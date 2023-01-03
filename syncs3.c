#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/inotify.h>
#include <unistd.h>
#include <dirent.h>
#include "jsmn.h"
#include <errno.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <math.h>
#include <time.h>

// command line arguments
char bucket[100];
char prefix[100];
int prefix_len;
char upload_path[100];
char download_path[100];
char logfile_path[100];

// when parsing the json returned by listing 100 S3 objects, the number of tokens was consistently 1704
#define MAX_NUMBER_OF_TOKENS (4096)

enum {
    string_buffer = 0,
    object_buffer,
    object_offset_buffer,
    stdout_buffer,
    fileA_buffer,
    fileB_buffer,
    number_of_buffers,
};

typedef struct {
    char *buf;
    int buf_len;
    int buf_size;
} buffer_t;

buffer_t bufs[number_of_buffers] = {0};

int append(int buf_idx, const char *something, int something_length)
{
    buffer_t *buf = bufs + buf_idx;
    while (buf->buf_len + something_length > buf->buf_size)
    {
        if (buf->buf_size == 0)
            buf->buf_size = 10000;
        buf->buf_size *= 2;
        buf->buf = realloc(buf->buf, buf->buf_size);
        assert(buf->buf);
    }

    if (something)
        memcpy(buf->buf + buf->buf_len, something, something_length);

    int offset_to_return = buf->buf_len;
    buf->buf_len += something_length;
    return offset_to_return;
}

typedef struct {
    int name_off;   // offset into string buffer to name
    int size_s3;    // the size in s3;
    int size_up;    // the size in upload directory;
    int size_down;    // the size in download directory;
    int wd; // inotify watch descriptor 
} obj_t;

int upload_path_wd; // inotify watch descriptor

obj_t *obj(const char *name)
{
    // first, a binary search We need to find the index 0..buf_len (inclusive)
    // for which all values less than name will be less than index
    // index *may* point to the same as name but all larger than name will be index or more
    int a = 0;
    int b = bufs[object_offset_buffer].buf_len / sizeof(int);
    assert(bufs[object_offset_buffer].buf_len % sizeof(int) == 0);
    
    if (a != b)    // is there an array to look in?
    {
        while (a != b)
        {
            int m = (a + b) / 2;
    
            obj_t *object_ptr = (obj_t *) (bufs[object_buffer].buf + *((int *) (bufs[object_offset_buffer].buf + m * sizeof(int))));
            if (strcmp(bufs[string_buffer].buf + object_ptr->name_off, name) < 0)
                a = m + 1;
            else
                b = m;
        }
        
        // we found the best location in the array, but is it exactly the same key?
        obj_t *object_ptr = (obj_t *) (bufs[object_buffer].buf + *((int *) (bufs[object_offset_buffer].buf + a * sizeof(int))));
        if (strcmp(bufs[string_buffer].buf + object_ptr->name_off, name) == 0)
            return object_ptr;   // we found it!
    }
    
    obj_t new_object;
    new_object.name_off = append(string_buffer, name, strlen(name) + 1);
    new_object.size_s3 = -1;
    new_object.size_up = -1;
    new_object.size_down = -1;
    new_object.wd = -1;
    
    // save the new object
    int object_offset = append(object_buffer, ((char *) (&new_object)), sizeof(obj_t));
    // and we'll save the new offset of the new object also. But first just make room for it
    assert(append(object_offset_buffer, NULL, sizeof(int)) + sizeof(int) == bufs[object_offset_buffer].buf_len);
  
    // but we have to put the offset at the right location
    int *offs = (int *)(bufs[object_offset_buffer].buf);
    memmove(offs + a + 1, offs + a, bufs[object_offset_buffer].buf_len - (a + 1) * sizeof(int));
    offs[a] = object_offset;

    return (obj_t *) (bufs[object_buffer].buf + object_offset);
}

void write_a_log_line(const char *mess)
{
    FILE *logfile = fopen(logfile_path, "a+");
    assert(logfile);
    fprintf(logfile, "%ld %s\n", time(NULL), mess);
    fclose(logfile);
}

void my_assert(int cond, const char *mess)
{
    if (!cond)
    {
        write_a_log_line(mess);
        exit(-1);
    }
}

// just make sure you have enough levels to cover the custom code, below
#define MAX_LEVELS 10
jsmntype_t tree_level_types[MAX_LEVELS];
int tree_level_start[MAX_LEVELS];
int filename_offset;
int object_size;

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_control_ListAccessPoints.html#API_control_ListAccessPoints_RequestSyntax
#define NEXTTOKEN_MAX_SIZE (1024)

jsmntok_t *climb_tree(int level, char *b, jsmntok_t *t, char *NextToken)
{
    jsmntok_t *save_t = t;
    
    if (level < MAX_LEVELS)
    {
        tree_level_types[level] = t->type;
        tree_level_start[level] = t->start;

        if ((t->type == JSMN_OBJECT) || (t->type == JSMN_ARRAY))
        {
            tree_level_start[level] = -1;
        }
        else if ((t->type == JSMN_STRING) || (t->type == JSMN_PRIMITIVE))
        {
            b[t->end] = '\0';
        }
        else
        {
            my_assert(0, "unrecognized token type");
        }
    
        if (
            level == 2 &&
            tree_level_types[0] == JSMN_OBJECT &&
            tree_level_start[0] == -1 &&
            tree_level_types[1] == JSMN_STRING &&
            strcmp(b + tree_level_start[1], "NextToken") == 0 &&
            tree_level_types[2] == JSMN_STRING
            )
            {   // We found a Next Token
                my_assert(NextToken[0] == '\0', "ensure only one NextToken in a single response");
                my_assert(strlen(b + tree_level_start[2]) <= NEXTTOKEN_MAX_SIZE, "the json contained a NextToken which was bigger than NEXTTOKEN_MAX_SIZE");
                strcpy(NextToken, b + tree_level_start[2]);
            }
            
    
        if (
            level == 3 &&
            tree_level_types[0] == JSMN_OBJECT &&
            tree_level_start[0] == -1 &&
            tree_level_types[1] == JSMN_STRING &&
            strcmp(b + tree_level_start[1], "Contents") == 0 &&
            tree_level_types[2] == JSMN_ARRAY &&
            tree_level_start[2] == -1 &&
            tree_level_types[3] == JSMN_OBJECT &&
            tree_level_start[3] == -1
            )
            {   // the beginning of an object in the array
                my_assert(filename_offset == -1 && object_size == -1, "list of objects wasn't parsed properly A");
            }
            
        if (
            level == 5 &&
            tree_level_types[0] == JSMN_OBJECT &&
            tree_level_start[0] == -1 &&
            tree_level_types[1] == JSMN_STRING &&
            strcmp(b + tree_level_start[1], "Contents") == 0 &&
            tree_level_types[2] == JSMN_ARRAY &&
            tree_level_start[2] == -1 &&
            tree_level_types[3] == JSMN_OBJECT &&
            tree_level_start[3] == -1 &&
            tree_level_types[4] == JSMN_STRING &&
            strcmp(b + tree_level_start[4], "Key") == 0 &&
            tree_level_types[5] == JSMN_STRING
            )
            {   // this is an object name
                
                my_assert(filename_offset == -1, "list of objects wasn't parsed properly B");
                filename_offset = tree_level_start[5];
                if (object_size != -1)
                {
                    char *filename = b + filename_offset;
                    my_assert(strncmp(filename, prefix, prefix_len) == 0, "ensure all s3 objects begin with prefix");
                    obj(filename + prefix_len)->size_s3 = object_size;
                    filename_offset = -1;
                    object_size = -1;
                }
            }
        
        if (
            level == 5 &&
            tree_level_types[0] == JSMN_OBJECT &&
            tree_level_start[0] == -1 &&
            tree_level_types[1] == JSMN_STRING &&
            strcmp(b + tree_level_start[1], "Contents") == 0 &&
            tree_level_types[2] == JSMN_ARRAY &&
            tree_level_start[2] == -1 &&
            tree_level_types[3] == JSMN_OBJECT &&
            tree_level_start[3] == -1 &&
            tree_level_types[4] == JSMN_STRING &&
            strcmp(b + tree_level_start[4], "Size") == 0 &&
            tree_level_types[5] == JSMN_PRIMITIVE
            )
            {   // this is an object size
                my_assert(object_size == -1, "list of objects wasn't parsed properly C");
                object_size = atoi(b + tree_level_start[5]);
                if (filename_offset != -1)
                {
                    char *filename = b + filename_offset;
                    my_assert(strncmp(filename, prefix, prefix_len) == 0, "ensure all s3 objects begin with prefix");
                    obj(filename + prefix_len)->size_s3 = object_size;
                    filename_offset = -1;
                    object_size = -1;
                }
            }
    }
    
    int this_object_size = t->size;
    while (this_object_size--)
    {
        my_assert(t + 1 - save_t < MAX_NUMBER_OF_TOKENS - 10, "while parsing json tree, ran off end of token array"); // the "- 10" is just a nice cushion
        t = climb_tree(level + 1, b, t + 1, NextToken);
    }
    
//    if (level == 0)
//      printf("num tokens: %d\n", t - save_t);
    return t;
}



int add_string_to_string_array(const char *thestr, char **b, int *bl, int *bs)
{
    int thestrlen = strlen(thestr);
    
    int offset = 0;
    while (*(*b + offset))
    {
        if (strcmp(*b + offset, thestr) == 0)
        {
            break;
        }
        offset += strlen(*b + offset) + 1;
    }
    if (!*(*b + offset))
    {
        if (*bl + thestrlen + 2 >= *bs) // add a little margin
        {
            write_a_log_line("doubling *bs");
            *bs *= 2;
            *b = realloc(*b, *bs);
            my_assert(*b != NULL, "realloc string array");
        }
        strcpy(*b + offset, thestr);
        *bl += thestrlen + 1;
        strcpy(*b + *bl, "");
    }
    return offset;
}

char *read_into_buffer(int fd, int buf_idx)
{
    buffer_t *buf = bufs + buf_idx;
    buf->buf_len = 0;
    if (buf->buf_size == 0)
    {
        buf->buf_size = 10000;
        buf->buf = realloc(buf->buf, buf->buf_size);
    }
    
    ssize_t r = 1;
    while (r > 0)
    {
        if (buf->buf_len + 1 >= buf->buf_size)
        {
            write_a_log_line("doubling stdout_buffer");
            buf->buf_size *= 2;
            buf->buf = realloc(buf->buf, buf->buf_size);
            my_assert(buf->buf != NULL, "realloc stdout_buffer");
        }
        
        r = read(fd, buf->buf + buf->buf_len, buf->buf_size - buf->buf_len - 1);
        if (r > 0)
        {
            buf->buf_len += r;
            buf->buf[buf->buf_len] = '\0';
        }
    }
    my_assert(r == 0, "read from process stdout");
    close(fd);
    
    return buf->buf;
}

void list_100_s3_objects(char *NextToken)
{
    int link[2];
    my_assert(pipe(link) == 0, "pipe");
    
    pid_t pid = fork();
    my_assert(pid != -1, "fork");
    if (pid == 0)
    {        
        dup2 (link[1], STDOUT_FILENO);
        close(link[0]);
        close(link[1]);
        
        if (NextToken[0])
            execlp("/usr/bin/aws", "/usr/bin/aws", "s3api", "list-objects", "--bucket", bucket, "--output", "json", "--max-items", "100", "--starting-token", NextToken, "--prefix", prefix, (char *)0);
        else
            execlp("/usr/bin/aws", "/usr/bin/aws", "s3api", "list-objects", "--bucket", bucket, "--output", "json", "--max-items", "100", "--prefix", prefix, (char *)0);

        exit(-1);
    }
    
    close(link[1]);
    
    // just read in stdout from process
    read_into_buffer(link[0], stdout_buffer);
    
    strcpy(NextToken, "");
    if (strstr(bufs[stdout_buffer].buf, "{"))
    {
        jsmn_parser p;
        jsmn_init(&p);
        jsmntok_t tokens[MAX_NUMBER_OF_TOKENS];
        my_assert(jsmn_parse(&p, bufs[stdout_buffer].buf, bufs[stdout_buffer].buf_len, tokens, MAX_NUMBER_OF_TOKENS) >= 0, "jsmn_parse");
        
        filename_offset = -1;
        object_size = -1;
        climb_tree(0, bufs[stdout_buffer].buf, tokens, NextToken);
        my_assert(filename_offset == -1 && object_size == -1, "list of objects wasn't parsed properly D");
        
        if (NextToken[0])
            write_a_log_line("parsed json from batch of objects");
        else
            write_a_log_line("parsed json from batch of objects (last)");        
    }
    else
    {
        write_a_log_line("list_100_s3_objects returns nothing");
    }
}

void put_s3_object(const char *key, const char *infile)
{
    char complete_key[100];
    my_assert(strlen(prefix) + strlen(key) + 1 <= sizeof(complete_key), "complete_key should be big enough in get_s3_object");
    strcpy(complete_key, prefix);
    strcat(complete_key, key);

    int link[2];
    my_assert(pipe(link) == 0, "pipe");
    
    pid_t pid = fork();
    my_assert(pid != -1, "fork");
    if (pid == 0)
    {        
        dup2 (link[1], STDOUT_FILENO);
        close(link[0]);
        close(link[1]);
        
        execlp("/usr/bin/aws", "/usr/bin/aws", "s3api", "put-object", "--bucket", bucket, "--output", "json", "--key", complete_key, "--body", infile, (char *)0);
        exit(-1);
    }
    else
    {
        close(link[1]);
        
        char *temp_buffer = read_into_buffer(link[0], stdout_buffer);
        
//        printf("put ->%s<-\n", temp_buffer);
    }
}

void get_s3_object(const char *key, const char *outfile)
{
    char complete_key[100];
    my_assert(strlen(prefix) + strlen(key) + 1 <= sizeof(complete_key), "complete_key should be big enough in get_s3_object");
    strcpy(complete_key, prefix);
    strcat(complete_key, key);
    
    int link[2];
    my_assert(pipe(link) == 0, "pipe");
    
    pid_t pid = fork();
    my_assert(pid != -1, "fork");
    if (pid == 0)
    {        
        dup2 (link[1], STDOUT_FILENO);
        close(link[0]);
        close(link[1]);
        
        execlp("/usr/bin/aws", "/usr/bin/aws", "s3api", "get-object", "--bucket", bucket, "--output", "json", "--key", complete_key, outfile, (char *)0);

        exit(-1);
    }
    else
    {
        close(link[1]);
        
        char *temp_buffer = read_into_buffer(link[0], stdout_buffer);
        
//        printf("get ->%s<-\n", temp_buffer);
    }
}

int get_missing_files()
{
    int something_was_fetched = 0;
    
    int i;
    for (i = 0; i < bufs[object_buffer].buf_len / sizeof(obj_t); i++)
    {
        obj_t *obj_ptr = ((obj_t *) (bufs[object_buffer].buf + i * sizeof(obj_t)));
        char pathname[128];
        my_assert(strlen(download_path) + strlen(bufs[string_buffer].buf + obj_ptr->name_off) + 1 <= sizeof(pathname), "pathname isn't big enough");
        strcpy(pathname, download_path);
        strcat(pathname, bufs[string_buffer].buf + obj_ptr->name_off);
        struct stat statbuf;
        if ((stat(pathname, &statbuf) != 0) || (statbuf.st_size != obj_ptr->size_s3))
        {
            something_was_fetched = 1;
            get_s3_object(bufs[string_buffer].buf + obj_ptr->name_off, pathname);
        }
    }
    
    if (something_was_fetched)
        write_a_log_line("get_missing_files - something_was_fetched");
    else
        write_a_log_line("get_missing_files - something_was NOT fetched");

    
    return something_was_fetched;
}

void download_all_from_s3(void)
{
    char NextToken[NEXTTOKEN_MAX_SIZE + 1];
    strcpy(NextToken, "");
    list_100_s3_objects(NextToken);
    while (NextToken[0])
        list_100_s3_objects(NextToken);
        
    if (get_missing_files())
        my_assert(get_missing_files() == 0, "called get_missing_files() twice and files were fetched both times");
}


int start_watching_upload_files(void)
{
    int fd_inotify = inotify_init();
    my_assert(fd_inotify > 0, "inotify_init");
    
    DIR *dd = opendir(upload_path);
    my_assert(dd != NULL, "opendir");
    
    upload_path_wd = inotify_add_watch(fd_inotify, upload_path, IN_CREATE);
    my_assert(upload_path_wd > 0, "inotify_add_watch upload_path");

    struct dirent *de;
    while ((de = readdir(dd)) != NULL)
    {
        if (de->d_name[0] != '.')
        {
            char pathname[1000];
            strcpy(pathname, upload_path);
            strcat(pathname, de->d_name);
            
            int wd = inotify_add_watch(fd_inotify, pathname, IN_MODIFY);
            my_assert(wd > 0, "inotify_add_watch individual file");
            obj(de->d_name)->wd = wd;
        }
    }
    my_assert(closedir(dd) == 0, "closedir");
    return fd_inotify;
}

char *read_file_into_buffer(int fd, ssize_t toread, int buf_idx)
{
    buffer_t *buf = bufs + buf_idx;
    buf->buf_len = 0;
    if (buf->buf_size < toread)
    {
        buf->buf_size = toread;
        buf->buf = realloc(buf->buf, buf->buf_size);
        my_assert(buf->buf != NULL, "realloc read_file_into_buffer");
    }
    
    while (toread)
    {        
        ssize_t thistime = read(fd, buf->buf + buf->buf_len, buf->buf_size - buf->buf_len);
        my_assert(thistime > 0, "read read_file_into_buffer");
        toread -= thistime;
        buf->buf_len += thistime;
    }

    close(fd);
    
    return buf->buf;
}


int files_not_equal(const char *a, const char *b)
{
    struct stat statbufa;
    struct stat statbufb;
    if (stat(a, &statbufa) != 0) return 1;
    if (stat(b, &statbufb) != 0) return 1;
    if (statbufa.st_size != statbufb.st_size) return 1;
    
    int fda = open(a, O_RDONLY);
    int fdb = open(b, O_RDONLY);
    my_assert(fda > 0 && fdb > 0, "open in files_not_equal");
    
    char *buffa = read_file_into_buffer(fda, statbufa.st_size, fileA_buffer);
    char *buffb = read_file_into_buffer(fdb, statbufa.st_size, fileB_buffer);
        
    return memcmp(buffa, buffb, statbufa.st_size) ? 1 : 0;
}

void upload_to_s3_and_download_from_s3(void)
{
    DIR *dd = opendir(upload_path);
    my_assert(dd != NULL, "opendir for upload");
    
    struct dirent *de;
    while ((de = readdir(dd)) != NULL)
    {
        if (de->d_name[0] != '.')
        {
            char up_pathname[1000];
            strcpy(up_pathname, upload_path);
            strcat(up_pathname, de->d_name);
            
            char down_pathname[1000];
            strcpy(down_pathname, download_path);
            strcat(down_pathname, de->d_name);
            
            printf("upload_to_s3_and_download_from_s3 %s\n", up_pathname);
            if (files_not_equal(up_pathname, down_pathname))
            {
                printf("not equal\n");
                put_s3_object(de->d_name, up_pathname);
                get_s3_object(de->d_name, down_pathname);
            }
        }
    }
    my_assert(closedir(dd) == 0, "closedir");
}

void wait_for_file_to_change(int fd_inotify)
{
    // Now wait for one of the files to change...
    struct inotify_event event[100];
    my_assert(read(fd_inotify, &event, sizeof(event)) > 0, "reading fd_inotify");
    
    // something changed. what happened?
    // actually, come to think of it... we don't care what the event was - let's just loop around
    my_assert(upload_path_wd != -1, "logic error. upload_path_wd was -1");
    my_assert(inotify_rm_watch(fd_inotify, upload_path_wd) == 0, "inotify_rm_watch upload_path_wd");
    upload_path_wd = -1;
        
    int i;
    for (i = 0; i < bufs[object_buffer].buf_len / sizeof(obj_t); i++)
    {
        obj_t *obj_ptr = ((obj_t *) (bufs[object_buffer].buf + i * sizeof(obj_t)));
        if (obj_ptr->wd != -1)
            my_assert(inotify_rm_watch(fd_inotify, obj_ptr->wd) == 0, "inotify_rm_watch");
        obj_ptr->wd = -1;
    }
    my_assert(close(fd_inotify) == 0, "close fd_inotify");
}

int child()
{
    write_a_log_line("child_start");
    
    download_all_from_s3();
    upload_to_s3_and_download_from_s3();
    while (1)
    {
        int fd_inotify = start_watching_upload_files();
        upload_to_s3_and_download_from_s3();
        // TODO pause here if the rolling average of bandwidth (over the last 24 hrs?) exceeds the limit
        // pause enough to bring the rolling average of the bandwidth back down below the limit
        wait_for_file_to_change(fd_inotify);
    }
}

void mommy()
{
    while (1)
    {
        if (!fork()) child();
        wait(NULL);
        usleep(1000000);    // wait for a second so we don't spin super quickly
    }
}

int main(int argc, char **argv)
{
    // e.g. syncs3 /home/pi/ joeruff.com 2D4090
    assert(argc == 4);
    
    char path[100];
    assert(strlen(argv[1]) + 1 + 1 <= sizeof(path));
    strcpy(path, argv[1]);
    strcat(path, "/");
    
    assert(mkdir(path, 0777) == 0 || errno == EEXIST);

    assert(strlen(argv[2]) + 1 <= sizeof(bucket));
    strcpy(bucket, argv[2]);

    char key[100];
    assert(strlen(argv[3]) + 1 <= sizeof(key));
    strcpy(key, argv[3]);

    assert(7 + strlen(key) + 7 + 1 <= sizeof(prefix));
    strcpy(prefix, "syncs3/");
    strcat(prefix, key);
    strcat(prefix, "/files/");
    prefix_len = strlen(prefix);
    
    char key_path[100];
    assert(strlen(path) + strlen(key) + 1 + 1 <= sizeof(key_path));
    strcpy(key_path, path);
    strcat(key_path, key);
    strcat(key_path, "/");
    
    assert(mkdir(key_path, 0777) == 0 || errno == EEXIST);
    
    assert(strlen(key_path) + 7 + 1 <= sizeof(upload_path));
    strcpy(upload_path, key_path);
    strcat(upload_path, "upload/");
    
    assert(mkdir(upload_path, 0777) == 0 || errno == EEXIST);
    
    assert(strlen(key_path) + 9 + 1 <= sizeof(download_path));
    strcpy(download_path, key_path);
    strcat(download_path, "download/");
    
    assert(mkdir(download_path, 0777) == 0 || errno == EEXIST);
    
    assert(strlen(key_path) + 7 + 1 <= sizeof(logfile_path));
    strcpy(logfile_path, key_path);
    strcat(logfile_path, "log.txt");
    
    if (!fork()) mommy();
        return 0;
}
