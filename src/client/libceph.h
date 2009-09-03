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

struct stat_precise {
  ino_t st_ino;
  dev_t st_dev;
  mode_t st_mode;
  nlink_t st_nlink;
  uid_t st_uid;
  gid_t st_gid;
  dev_t st_rdev;
  off_t st_size;
  blksize_t st_blksize;
  blkcnt_t st_blocks;
  time_t st_atime_sec;
  time_t st_atime_micro;
  time_t st_mtime_sec;
  time_t st_mtime_micro;
  time_t st_ctime_sec;
  time_t st_ctime_micro;
};

extern "C" {

const char *ceph_version(int *major, int *minor, int *patch);

int ceph_initialize(int argc, const char **argv);
void ceph_deinitialize();

int ceph_mount();
int ceph_umount();

int ceph_statfs(const char *path, struct statvfs *stbuf);

int ceph_getcwd(char *buf, int buflen);
int ceph_chdir(const char *s);

int ceph_opendir(const char *name, DIR **dirpp);
int ceph_closedir(DIR *dirp);
int ceph_readdir_r(DIR *dirp, struct dirent *de);
int ceph_readdirplus_r(DIR *dirp, struct dirent *de, struct stat *st, int *stmask);
int ceph_getdents(DIR *dirp, char *name, int buflen);
int ceph_getdnames(DIR *dirp, char *name, int buflen);
void ceph_rewinddir(DIR *dirp); 
loff_t ceph_telldir(DIR *dirp);
void ceph_seekdir(DIR *dirp, loff_t offset);

int ceph_link (const char *existing, const char *newname);
int ceph_unlink (const char *path);
int ceph_rename(const char *from, const char *to);

// dirs
int ceph_mkdir(const char *path, mode_t mode);
int ceph_mkdirs(const char *path, mode_t mode);
int ceph_rmdir(const char *path);

// symlinks
int ceph_readlink(const char *path, char *buf, loff_t size);
int ceph_symlink(const char *existing, const char *newname);

// inode stuff
int ceph_lstat(const char *path, struct stat *stbuf);
int ceph_lstat_precise(const char *path, struct stat_precise *stbuf);

int ceph_setattr(const char *relpath, struct stat *attr, int mask);
int ceph_setattr_precise (const char *relpath, struct stat_precise *stbuf, int mask);
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
int ceph_get_file_stripe_unit(int fh);
int ceph_get_file_replication(const char *path);
int ceph_get_file_stripe_address(int fd, loff_t offset, char *buf, int buflen);
int ceph_set_default_file_stripe_unit(int stripe);
int ceph_set_default_file_stripe_count(int count);
int ceph_set_default_object_size(int size);
int ceph_set_default_file_replication(int replication);
}

#endif
