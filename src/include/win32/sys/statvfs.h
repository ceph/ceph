#ifndef _SYS_STATVFS_H
#define _SYS_STATVFS_H  1

typedef unsigned __int64 fsfilcnt64_t;
typedef unsigned __int64 fsblkcnt64_t;
typedef unsigned __int64 fsblkcnt_t;

struct statvfs
{
    unsigned long int f_bsize;
    unsigned long int f_frsize;
    fsblkcnt64_t f_blocks;
    fsblkcnt64_t f_bfree;
    fsblkcnt64_t f_bavail;
    fsfilcnt64_t f_files;
    fsfilcnt64_t f_ffree;
    fsfilcnt64_t f_favail;
    unsigned long int f_fsid;
    unsigned long int f_flag;
    unsigned long int f_namemax;
    int __f_spare[6];
};
struct flock {
    short l_type;
    short l_whence;
    off_t l_start;
    off_t l_len;
    pid_t l_pid;
};

#define F_RDLCK 0
#define F_WRLCK 1
#define F_UNLCK 2
#define F_SETLK 6

#endif /* _SYS_STATVFS_H */
