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
    

char path[100];

struct {
    char d_name[256];
    int wd;
    char *buf;
    time_t mod_time;
    off_t st_size;
} *file_records = NULL;
int num_file_records = 0;

void grow_files_records_by_one()
{
    num_file_records++;
    file_records = realloc(file_records, num_file_records * sizeof(file_records[0]));
    assert(file_records);
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

void start_watching_files(int *fd, int *path_inotify)
{
    assert(*fd == -1);
    *fd = inotify_init();
    assert(*fd != -1);
    
    assert(*path_inotify == 0);
    *path_inotify = inotify_add_watch(*fd, path, IN_CREATE);
    assert(*path_inotify > 0);
    
    DIR *dd = opendir(path);
    assert(dd != NULL);
    
    time_t now = time(NULL);
    
    struct dirent *de;
    while ((de = readdir(dd)) != NULL)
        if (de->d_name[0] != '.')
        {
            int idx = get_index(de->d_name);
            
            if ((file_records[idx].mod_time == 0) || (now < file_records[idx].mod_time) || (now - file_records[idx].mod_time < 30 * 24 * 60 * 60))
            {
                char fullpath[1000];
                strcpy(fullpath, path);
                strcat(fullpath, de->d_name);
                    
                file_records[idx].wd = inotify_add_watch(*fd, fullpath, IN_MODIFY);
                if (file_records[idx].wd <= 0)
                {
                    printf("inotify_add_watch failed\n");
                    printf("%d for %s\n", file_records[idx].wd, fullpath);
                    printf("errno: %d\n", errno);
                    if (errno == EACCES) printf("EACCES\n");
                    if (errno == EBADF) printf("EBADF\n");
                    if (errno == EFAULT) printf("EFAULT\n");
                    if (errno == EINVAL) printf("EINVAL\n");
                    if (errno == ENAMETOOLONG) printf("ENAMETOOLONG\n");
                    if (errno == ENOENT) printf("ENOENT\n");
                    if (errno == ENOMEM) printf("ENOMEM\n");
                    if (errno == ENOSPC) printf("ENOSPC\n");
                }
                assert(file_records[idx].wd > 0);
            }
        }
    assert(closedir(dd) == 0);
}

void stop_watching_files(int *fd, int *path_inotify)
{
    struct inotify_event event[100];
    assert(*fd != -1);
    assert(read(*fd, &event, sizeof(event)) > 0);
    
    // igore the actual event. Just reset everything...
    
    assert(*path_inotify > 0);
    assert(inotify_rm_watch(*fd, *path_inotify) == 0);
    *path_inotify = 0;
    
    int idx;
    for (idx = 0; idx < num_file_records; idx++)
    {
        // a file could have been added by read_all_files_into_memory() since the last start_watching_files()
        if (file_records[idx].wd > 0)
        {
            assert(inotify_rm_watch(*fd, file_records[idx].wd) == 0);
            file_records[idx].wd = 0;
        }
    }
    assert(close(*fd) == 0);
    *fd = -1;
}

void read_all_files_into_memory()
{
    int dirfd = open(path, O_RDONLY);
    assert(dirfd > 0);
    
    DIR *dd = opendir(path);
    assert(dd != NULL);
    
    struct dirent *de;
    while ((de = readdir(dd)) != NULL)
        if (de->d_name[0] != '.')
        {
            int idx = get_index(de->d_name);
            
            struct stat statbuf;
            assert(fstatat(dirfd, de->d_name, &statbuf, 0) == 0);
            file_records[idx].mod_time = statbuf.st_mtime;
            
            // a file could have been added by start_watching_files() since the last read_all_files_into_memory() 
            if (file_records[idx].buf == NULL || file_records[idx].st_size < statbuf.st_size)
            {
                file_records[idx].st_size = statbuf.st_size;
                file_records[idx].buf = realloc(file_records[idx].buf, file_records[idx].st_size);
                assert(file_records[idx].buf);
                
                int fd = openat(dirfd, de->d_name, O_RDONLY);
                assert(fd > 0);
                
                ssize_t bytes_to_read = file_records[idx].st_size;
                while (bytes_to_read)
                {
                    ssize_t bytes_read_this_time = read(fd, file_records[idx].buf + file_records[idx].st_size - bytes_to_read, bytes_to_read);
                    assert(bytes_read_this_time > 0);
                    bytes_to_read -= bytes_read_this_time;
                }
                close(fd);
                
                // OK, this is a new file which has been read into memory. Send this to ALL open connections
                int i;
                for (i = 0; i < MAXIMUM_NUMBER_OF_CLIENT_LISTENERS; i++)
                {
                    if (bytes_to_send[i] + file_records[idx].st_size > buffer_size[i])
                    {
                        buffer_size[i] = bytes_to_send[i] + file_records[idx].st_size;
                        buffers[i] = realloc(buffers[i], buffer_size[i]);
                        assert(buffers[i]);
                    }
                    memcpy(buffers[i] + bytes_to_send[i], file_records[idx].buf, file_records[idx].st_size);
                    bytes_to_send[i] += file_records[idx].st_size;
                }

            }

        }
    assert(closedir(dd) == 0);
    
    close(dirfd);
}

int child()
{
    int path_inotify = 0;
    struct pollfd fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 2];

    int i;
    for (i = 0; i < MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 2; i++)
        fds[i].fd = -1;
    
    fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd = socket(AF_INET, SOCK_STREAM, 0);
    fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].events = POLLIN;
    assert(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd != -1);


    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    
    int yes = 1;
    
    addr.sin_port = htons(5874);
    assert(setsockopt(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) != -1);
    assert(bind(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd, (struct sockaddr *) &addr,  sizeof(addr)) == 0);
    assert(listen(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd, 5) == 0);

    start_watching_files(&fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].fd, &path_inotify);
    fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].events = POLLIN;
    
    read_all_files_into_memory();
    while (1)
    {
        for (i = 0; i < MAXIMUM_NUMBER_OF_CLIENT_LISTENERS; i++)
            fds[i].events = POLLIN | (bytes_to_send[i] ? POLLOUT : 0);

        assert(poll(fds, MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 2, -1) > 0);
        
        for (i = 0; i < MAXIMUM_NUMBER_OF_CLIENT_LISTENERS; i++)
        if (fds[i].revents)
        {
            if (fds[i].revents == POLLOUT)
            {
                ssize_t bytes_sent = send(fds[i].fd, buffers[i], bytes_to_send[i], 0);
                assert(bytes_sent > 0);
                bytes_to_send[i] -= bytes_sent;
                memmove(buffers[i], buffers[i] + bytes_sent, bytes_to_send[i]);
            }
            if (fds[i].revents == POLLIN)
            {
                close(fds[i].fd);
                fds[i].fd = -1;
            }
        }

        if (fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].revents == POLLIN)
        {
            for (i = 0; i < MAXIMUM_NUMBER_OF_CLIENT_LISTENERS; i++)
                if (fds[i].fd == -1)
                    break;
            assert(i != MAXIMUM_NUMBER_OF_CLIENT_LISTENERS);
                    
            fds[i].fd = accept(fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 0].fd, NULL, NULL);
            assert(fds[i].fd > 1);
            
            bytes_to_send[i] = 0;
            // start by sending ALL the file contents!
            int idx;
            for (idx = 0; idx < num_file_records; idx++)
                if (file_records[idx].buf && file_records[idx].st_size)
                {
                    if (bytes_to_send[i] + file_records[idx].st_size > buffer_size[i])
                    {
                        buffer_size[i] = bytes_to_send[i] + file_records[idx].st_size;
                        buffers[i] = realloc(buffers[i], buffer_size[i]);
                        assert(buffers[i]);
                    }
                    memcpy(buffers[i] + bytes_to_send[i], file_records[idx].buf, file_records[idx].st_size);
                    bytes_to_send[i] += file_records[idx].st_size;
                }
        }
        
        if (fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].revents == POLLIN)
        {
            stop_watching_files(&fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].fd, &path_inotify);
            // is there a race condition here where a new file may go unnoticed?
            start_watching_files(&fds[MAXIMUM_NUMBER_OF_CLIENT_LISTENERS + 1].fd, &path_inotify);
            read_all_files_into_memory();
        }
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
    // e.g. local_file_server /path/
    assert(argc == 2);
    assert(strlen(argv[1]) + 1 + 1 <= sizeof(path));
    strcpy(path, argv[1]);
    strcat(path, "/");
    assert(mkdir(path, 0777) == 0 || errno == EEXIST);
    printf("serving up files in %s on port 5874\n", path);
    
    if (!fork()) mommy();
        return 0;
}
