#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <dirent.h>
#include <poll.h>
#include <time.h>

#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/inotify.h>
#include <sys/socket.h>

#include <netinet/in.h>

#include <unistd.h>
#include <fcntl.h>

// The rules are:
// 1) no files can ever be removed from the directory
// 2) no file in the directory can ever shrink

#define MAXIMUM_NUMBER_OF_CLIENT_LISTENERS (10)
char *buffers[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS] = {NULL};
int bytes_to_send[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS] = {0};
int buffer_size[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS] = {0};
struct pollfd fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 2];

char upload_path[100];
char logfile_path[100];

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

struct {
    char d_name[256];
    int wd;
    char *buf;
    time_t mod_time;
    off_t st_size;
} *file_records = NULL;
int num_file_records = 0;

void broadcast_out(int only_one, const char *buf, off_t st_size)
{
    if (buf && st_size)
    {
        int i;
        int start = 0;
        int end = MAXIMUM_NUMBER_OF_CLIENT_LISTENERS;
        if (only_one != -1)
        {
            start = only_one;
            end = only_one + 1;
        }
        for (i = start; i < end; i++)
            if (fds[i].fd != -1)
            {
                if (bytes_to_send[i] + st_size > buffer_size[i])
                {
                    buffer_size[i] = bytes_to_send[i] + st_size;
                    buffers[i] = realloc(buffers[i], buffer_size[i]);
                    my_assert(buffers[i] != NULL, "realloc buffers");
                }
                memcpy(buffers[i] + bytes_to_send[i], buf, st_size);
                bytes_to_send[i] += st_size;
            }
    }
}

void grow_files_records_by_one()
{
    num_file_records++;
    file_records = realloc(file_records, num_file_records * sizeof(file_records[0]));
    my_assert(file_records != NULL, "realloc file_records");
    file_records[num_file_records - 1].wd = 0;
    file_records[num_file_records - 1].mod_time = 0;
    file_records[num_file_records - 1].buf = NULL;
}

int get_index(const char *d_name)
{
    int i = 0;
    while (i < num_file_records)
        if (strcmp(file_records[i].d_name, d_name) == 0)
            return i;
        else
            i++;
                    
    grow_files_records_by_one();
    strcpy(file_records[i].d_name, d_name);
    
    return i;
}

void read_all_files_into_memory()
{
    int dirfd = open(upload_path, O_RDONLY);
    my_assert(dirfd > 0, "read_all_files_into_memory open");
    
    DIR *dd = opendir(upload_path);
    my_assert(dd != NULL, "read_all_files_into_memory opendir");
    
    struct dirent *de;
    while ((de = readdir(dd)) != NULL)
        if (de->d_name[0] != '.')
        {
            int idx = get_index(de->d_name);
            
            struct stat statbuf;
            my_assert(fstatat(dirfd, de->d_name, &statbuf, 0) == 0, "read_all_files_into_memory fstatat");
            file_records[idx].mod_time = statbuf.st_mtime;
            
            if (file_records[idx].buf == NULL || file_records[idx].st_size < statbuf.st_size)
            {
                file_records[idx].st_size = statbuf.st_size;
                file_records[idx].buf = realloc(file_records[idx].buf, file_records[idx].st_size);
                my_assert(file_records[idx].buf != NULL, "read_all_files_into_memory realloc file_records");
                
                int fd = openat(dirfd, de->d_name, O_RDONLY);
                my_assert(fd > 0, "read_all_files_into_memory openat");
                
                ssize_t bytes_to_read = file_records[idx].st_size;
                while (bytes_to_read)
                {
                    ssize_t bytes_read_this_time = read(fd, file_records[idx].buf + file_records[idx].st_size - bytes_to_read, bytes_to_read);
                    my_assert(bytes_read_this_time > 0, "read_all_files_into_memory read");
                    bytes_to_read -= bytes_read_this_time;
                }
                close(fd);
                
                // OK, this is a new file which has been read into memory. Send this to ALL open connections
                broadcast_out(-1, file_records[idx].buf, file_records[idx].st_size);
            }

        }
    my_assert(closedir(dd) == 0, "read_all_files_into_memory closedir");
    
    close(dirfd);
}

int child()
{
    write_a_log_line("child start");
    int i;
    for (i = 0; i < MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 2; i++)
        fds[i].fd = -1;
    
    fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd = socket(AF_INET, SOCK_STREAM, 0);
    my_assert(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd != -1, "socket");


    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    
    int yes = 1;
    
    addr.sin_port = htons(5874);
    my_assert(setsockopt(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) != -1, "setsockopt");
    my_assert(bind(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd, (struct sockaddr *) &addr,  sizeof(addr)) == 0, "bind");
    my_assert(listen(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd, 5) == 0, "listen");

    fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].fd = inotify_init1(IN_NONBLOCK);
    my_assert(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].fd != -1, "inotify_init1");
    my_assert(inotify_add_watch(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].fd, upload_path, IN_CREATE | IN_MOVED_TO | IN_MODIFY | IN_MASK_ADD) > 0, "inotify_add_watch");
    
    read_all_files_into_memory();
    write_a_log_line("loop start");
    while (1)
    {
        for (i = 0; i < MAXIMUM_NUMBER_OF_CLIENT_LISTENERS; i++)
            fds[i].events = POLLIN | (bytes_to_send[i] ? POLLOUT : 0);
        fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].events = POLLIN;
        fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].events = POLLIN;

        my_assert(poll(fds, MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 2, -1) > 0, "poll");
        
        for (i = 0; i < MAXIMUM_NUMBER_OF_CLIENT_LISTENERS; i++)
        if (fds[i].fd != -1 && fds[i].revents)
        {
            if (fds[i].revents & POLLOUT)
            {
                ssize_t bytes_sent = send(fds[i].fd, buffers[i], bytes_to_send[i], 0);
                my_assert(bytes_sent > 0, "send");
                bytes_to_send[i] -= bytes_sent;
                memmove(buffers[i], buffers[i] + bytes_sent, bytes_to_send[i]);
            }
            if (fds[i].revents & POLLIN)
            {
                write_a_log_line("remote client closed conection");
                close(fds[i].fd);
                fds[i].fd = -1;
            }
            if (fds[i].revents & ( ~ (POLLOUT | POLLIN) ) )
                write_a_log_line("remote client conection revents something other than POLLIN or POLLOUT");
        }

        if (fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].revents)
        {
            if (fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].revents == POLLIN)
            {
                write_a_log_line("accept listener");
                for (i = 0; i < MAXIMUM_NUMBER_OF_CLIENT_LISTENERS; i++)
                    if (fds[i].fd == -1)
                        break;
                my_assert(i != MAXIMUM_NUMBER_OF_CLIENT_LISTENERS, "ran out of space for new listeners");
                        
                fds[i].fd = accept(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd, NULL, NULL);
                my_assert(fds[i].fd > 1, "accept");
                
                bytes_to_send[i] = 0;
                // start by sending ALL the file contents!
                int idx;
                for (idx = 0; idx < num_file_records; idx++)
                    broadcast_out(i, file_records[idx].buf, file_records[idx].st_size);
            }
            else
                write_a_log_line("listen revents != POLLIN");
        }
        
        if (fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].revents)
        {
            if (fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].revents == POLLIN)
            {
                struct inotify_event event[sizeof(struct inotify_event) + NAME_MAX + 1];
                ssize_t len = 1;
                while (len > 0)
                {
                    len = read(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].fd, &event, sizeof(event));
                    my_assert(len != -1 || errno == EAGAIN, "reading fd_inotify");
                }
                read_all_files_into_memory();
            }
            else
                write_a_log_line("inotify revents != POLLIN");
        }
    }
}

void mommy()
{
    write_a_log_line("mommy start");
    while (1)
    {
        write_a_log_line("about to start child");
        if (!fork()) child();
        wait(NULL);
        usleep(1000000);    // wait for a second so we don't spin super quickly
    }
}

int main(int argc, char **argv)
{
    fprintf(stderr, "usage: %s absolute_path random_string\n", argv[0]);
    fprintf(stderr, "     (e.g. %s /home/pi/ xxxxxx)\n", argv[0]);
    
    assert(argc == 3);
    
    char path[100];
    assert(strlen(argv[1]) + 1 + 1 <= sizeof(path));
    strcpy(path, argv[1]);
    if (path[strlen(path)-1] != '/')
        strcat(path, "/");
    
    char key_path[100];
    assert(strlen(path) + strlen(argv[2]) + 1 + 1 <= sizeof(key_path));
    strcpy(key_path, path);
    strcat(key_path, argv[2]);
    strcat(key_path, "/");
    
    assert(strlen(key_path) + 7 + 1 <= sizeof(upload_path));
    strcpy(upload_path, key_path);
    strcat(upload_path, "upload/");
    
    assert(strlen(key_path) + 7 + 1 <= sizeof(logfile_path));
    strcpy(logfile_path, key_path);
    strcat(logfile_path, "local_file_server.log");
    
    fprintf(stderr, "\n---> this run: %s %s %s\n", argv[0], argv[1], argv[2]);
    fprintf(stderr, "\nThe server watches the directory %s for new or modified files\n", upload_path);
    fprintf(stderr, "ls -l -t -r %s | tail -5\n", upload_path);
    fprintf(stderr, "\nThe log file is here: %s\n", logfile_path);
    fprintf(stderr, "\ntail -f %s\n", logfile_path);

    assert(mkdir(path, 0777) == 0 || errno == EEXIST);
    assert(mkdir(key_path, 0777) == 0 || errno == EEXIST);
    assert(mkdir(upload_path, 0777) == 0 || errno == EEXIST);

    fprintf(stderr, "serving up files in %s on port 5874\n", path);
    
    write_a_log_line("about to start mommy");
    if (!fork()) mommy();
        return 0;
}
