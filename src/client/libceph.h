#ifndef __LIBCEPH_H
#define __LIBCEPH_H
#include <netinet/in.h>
#include <sys/statvfs.h>
#include <utime.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>

#ifdef __cplusplus
#include <list>
#include <string>
extern "C" {
#endif

int ceph_initialize(int argc, const char **argv);
void ceph_deinitialize();

int ceph_mount();
int ceph_umount();

int ceph_statfs(const char *path, struct statvfs *stbuf);

int ceph_chdir (const char *s);
const char *ceph_getcwd();

int ceph_opendir(const char *name, DIR **dirpp);
int ceph_closedir(DIR *dirp);
int ceph_readdir_r(DIR *dirp, struct dirent *de);
int ceph_readdirplus_r(DIR *dirp, struct dirent *de, struct stat *st, int *stmask);
void ceph_rewinddir(DIR *dirp); 
loff_t ceph_telldir(DIR *dirp);
void ceph_seekdir(DIR *dirp, loff_t offset);

int ceph_link (const char *existing, const char *newname);
int ceph_unlink (const char *path);
int ceph_rename(const char *from, const char *to);

// dirs
int ceph_mkdir(const char *path, mode_t mode);
int ceph_rmdir(const char *path);

// symlinks
int ceph_readlink(const char *path, char *buf, loff_t size);
int ceph_symlink(const char *existing, const char *newname);

// inode stuff
//  int ceph_lstat(const char *path, struct stat *stbuf, frag_info_t *dirstat=0);

int ceph_setattr(const char *relpath, struct stat *attr, int mask);
int ceph_chmod(const char *path, mode_t mode);
int ceph_chown(const char *path, uid_t uid, gid_t gid);
int ceph_utime(const char *path, struct utimbuf *buf);
int ceph_truncate(const char *path, loff_t size);

// file ops
int ceph_mknod(const char *path, mode_t mode, dev_t rdev=0);
int ceph_open(const char *path, int flags, mode_t mode=0);
int ceph_close(int fd);
loff_t ceph_lseek(int fd, loff_t offset, int whence);
int ceph_read(int fd, char *buf, loff_t size, loff_t offset=-1);
int ceph_write(int fd, const char *buf, loff_t size, loff_t offset=-1);
int ceph_ftruncate(int fd, loff_t size);
int ceph_fsync(int fd, bool syncdataonly);
int ceph_fstat(int fd, struct stat *stbuf);

int ceph_sync_fs();
#ifdef __cplusplus
int ceph_getdir(const char *relpath, std::list<std::string>& names); //not for C, sorry!
}
#endif

#endif
