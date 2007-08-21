// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:t -*- 
// vim: ts=8 sw=4 smarttab

/*
    FUSE: Filesystem in Userspace
    Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

    This program can be distributed under the terms of the GNU GPL.
    See the file COPYING.

    gcc -Wall `pkg-config fuse --cflags --libs` -lulockmgr fusexmp_fh.c -o fusexmp_fh
*/

#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif


#include <fuse.h>
#include <ulockmgr.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif


#include <ext/hash_map>
using namespace __gnu_cxx;

#include <iostream>
#include <fstream>
#include <map>
using namespace std;

#include "common/Mutex.h"

Mutex trace_lock;
fstream traceout;

const char *basedir = 0;

struct Inode;
struct Dir {
    Inode *inode;
    map<string,Inode*> dentries;
};
struct Inode {
    Dir *parent_dir;
    Dir *dir;
};

hash_map<ino_t, Inode*> inode_map;




static int ft_getattr(const char *path, struct stat *stbuf)
{
    int res;

    trace_lock.Lock();
    traceout << "getattr" << endl << path << endl;
    trace_lock.Unlock();

    res = lstat(path, stbuf);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_fgetattr(const char *path, struct stat *stbuf,
                        struct fuse_file_info *fi)
{
    int res;

    (void) path;

    trace_lock.Lock();
    traceout << "fgetattr" << endl << fi->fh << endl;
    trace_lock.Unlock();

    res = fstat(fi->fh, stbuf);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_access(const char *path, int mask)
{
    int res;

    trace_lock.Lock();
    traceout << "access" << endl << path << endl;
    trace_lock.Unlock();

    res = access(path, mask);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_readlink(const char *path, char *buf, size_t size)
{
    int res;

    trace_lock.Lock();
    traceout << "readlink" << endl << path << endl;
    trace_lock.Unlock();

    res = readlink(path, buf, size - 1);
    if (res == -1)
        return -errno;

    buf[res] = '\0';
    return 0;
}

static int ft_opendir(const char *path, struct fuse_file_info *fi)
{
    DIR *dp = opendir(path);
    if (dp == NULL)
        return -errno;

    fi->fh = (unsigned long) dp;

    trace_lock.Lock();
    traceout << "opendir" << endl << path << endl << fi->fh << endl;
    trace_lock.Unlock();

    return 0;
}

static inline DIR *get_dirp(struct fuse_file_info *fi)
{
    return (DIR *) (uintptr_t) fi->fh;
}

static int ft_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi)
{
    DIR *dp = get_dirp(fi);
    struct dirent *de;

    (void) path;
    seekdir(dp, offset);
    while ((de = readdir(dp)) != NULL) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = de->d_ino;
        st.st_mode = de->d_type << 12;
        if (filler(buf, de->d_name, &st, telldir(dp)))
            break;
    }

    trace_lock.Lock();
    traceout << "readdir" << endl << fi->fh << endl;
    trace_lock.Unlock();

    return 0;
}

static int ft_releasedir(const char *path, struct fuse_file_info *fi)
{
    DIR *dp = get_dirp(fi);
    (void) path;
    closedir(dp);

    trace_lock.Lock();
    traceout << "releasedir" << endl << fi->fh << endl;
    trace_lock.Unlock();

    return 0;
}

static int ft_mknod(const char *path, mode_t mode, dev_t rdev)
{
    int res;

    trace_lock.Lock();
    traceout << "mknod" << endl << path << endl << mode << endl << rdev << endl;
    trace_lock.Unlock();

    if (S_ISFIFO(mode))
        res = mkfifo(path, mode);
    else
        res = mknod(path, mode, rdev);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_mkdir(const char *path, mode_t mode)
{
    int res;

    trace_lock.Lock();
    traceout << "mkdir" << endl << path << endl << mode << endl;
    trace_lock.Unlock();

    res = mkdir(path, mode);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_unlink(const char *path)
{
    int res;

    trace_lock.Lock();
    traceout << "unlink" << endl << path << endl;
    trace_lock.Unlock();

    res = unlink(path);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_rmdir(const char *path)
{
    int res;

    trace_lock.Lock();
    traceout << "rmdir" << endl << path << endl;
    trace_lock.Unlock();

    res = rmdir(path);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_symlink(const char *from, const char *to)
{
    int res;

    trace_lock.Lock();
    traceout << "symlink" << endl << from << endl << to << endl;
    trace_lock.Unlock();

    res = symlink(from, to);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_rename(const char *from, const char *to)
{
    int res;

    trace_lock.Lock();
    traceout << "rename" << endl << from << endl << to << endl;
    trace_lock.Unlock();

    res = rename(from, to);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_link(const char *from, const char *to)
{
    int res;

    trace_lock.Lock();
    traceout << "link" << endl << from << endl << to << endl;
    trace_lock.Unlock();

    res = link(from, to);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_chmod(const char *path, mode_t mode)
{
    int res;

    trace_lock.Lock();
    traceout << "chmod" << endl << path << endl << mode << endl;
    trace_lock.Unlock();

    res = chmod(path, mode);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_chown(const char *path, uid_t uid, gid_t gid)
{
    int res;

    trace_lock.Lock();
    traceout << "chown" << endl << path << endl << uid << endl << gid << endl;
    trace_lock.Unlock();

    res = lchown(path, uid, gid);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_truncate(const char *path, off_t size)
{
    int res;

    trace_lock.Lock();
    traceout << "truncate" << endl << path << endl << size << endl;
    trace_lock.Unlock();

    res = truncate(path, size);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_ftruncate(const char *path, off_t size,
                         struct fuse_file_info *fi)
{
    int res;

    (void) path;

    trace_lock.Lock();
    traceout << "ftruncate" << endl << fi->fh << endl << size << endl;
    trace_lock.Unlock();

    res = ftruncate(fi->fh, size);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_utimens(const char *path, const struct timespec ts[2])
{
    int res;
    struct timeval tv[2];

    trace_lock.Lock();
    traceout << "utimens" << endl << path
	     << tv[0].tv_sec << endl << tv[0].tv_usec << endl
	     << tv[1].tv_sec << endl << tv[1].tv_usec << endl;
    trace_lock.Unlock();

    tv[0].tv_sec = ts[0].tv_sec;
    tv[0].tv_usec = ts[0].tv_nsec / 1000;
    tv[1].tv_sec = ts[1].tv_sec;
    tv[1].tv_usec = ts[1].tv_nsec / 1000;

    res = utimes(path, tv);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    int fd;

    trace_lock.Lock();
    traceout << "create" << endl << path << endl << mode << endl << fi->flags << endl;
    trace_lock.Unlock();

    fd = open(path, fi->flags, mode);
    if (fd == -1)
        return -errno;

    fi->fh = fd;
    return 0;
}

static int ft_open(const char *path, struct fuse_file_info *fi)
{
    int fd;

    fd = open(path, fi->flags);

    trace_lock.Lock();
    traceout << "open" << endl << path << endl << fi->flags << endl << (fd > 0 ? fd:0) << endl;
    trace_lock.Unlock();

    if (fd == -1)
        return -errno;

    fi->fh = fd;
    return 0;
}

static int ft_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
    int res;

    trace_lock.Lock();
    traceout << "read" << endl << fi->fh << endl << offset << endl << size << endl;
    trace_lock.Unlock();

    (void) path;
    res = pread(fi->fh, buf, size, offset);
    if (res == -1)
        res = -errno;

    return res;
}

static int ft_write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
    int res;

    trace_lock.Lock();
    traceout << "write" << endl << fi->fh << endl << offset << endl << size << endl;
    trace_lock.Unlock();

    (void) path;
    res = pwrite(fi->fh, buf, size, offset);
    if (res == -1)
        res = -errno;

    return res;
}

static int ft_statfs(const char *path, struct statvfs *stbuf)
{
    int res;

    trace_lock.Lock();
    traceout << "statfs" << endl << path << endl;
    trace_lock.Unlock();

    res = statvfs(path, stbuf);
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_flush(const char *path, struct fuse_file_info *fi)
{
    int res;

    trace_lock.Lock();
    traceout << "flush" << endl << fi->fh << endl;
    trace_lock.Unlock();

    (void) path;
    /* This is called from every close on an open file, so call the
       close on the underlying filesystem.  But since flush may be
       called multiple times for an open file, this must not really
       close the file.  This is important if used on a network
       filesystem like NFS which flush the data/metadata on close() */
    res = close(dup(fi->fh));
    if (res == -1)
        return -errno;

    return 0;
}

static int ft_release(const char *path, struct fuse_file_info *fi)
{
    (void) path;
    close(fi->fh);

    trace_lock.Lock();
    traceout << "release" << endl << fi->fh << endl;
    trace_lock.Unlock();

    return 0;
}

static int ft_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
    int res;
    (void) path;

#ifndef HAVE_FDATASYNC
    (void) isdatasync;
#else
    if (isdatasync)
        res = fdatasync(fi->fh);
    else
#endif
        res = fsync(fi->fh);

    trace_lock.Lock();
    traceout << "fsync" << endl << fi->fh << endl << isdatasync << endl;
    trace_lock.Unlock();

    if (res == -1)
        return -errno;

    return 0;
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int ft_setxattr(const char *path, const char *name, const char *value,
                        size_t size, int flags)
{
    int res = lsetxattr(path, name, value, size, flags);
    if (res == -1)
        return -errno;
    return 0;
}

static int ft_getxattr(const char *path, const char *name, char *value,
                    size_t size)
{
    int res = lgetxattr(path, name, value, size);
    if (res == -1)
        return -errno;
    return res;
}

static int ft_listxattr(const char *path, char *list, size_t size)
{
    int res = llistxattr(path, list, size);
    if (res == -1)
        return -errno;
    return res;
}

static int ft_removexattr(const char *path, const char *name)
{
    int res = lremovexattr(path, name);
    if (res == -1)
        return -errno;
    return 0;
}
#endif /* HAVE_SETXATTR */

static int ft_lock(const char *path, struct fuse_file_info *fi, int cmd,
                    struct flock *lock)
{
    (void) path;

    /*
    trace_lock.Lock();
    traceout << "fsync" << endl << fi->fh << endl << isdatasync << endl;
    trace_lock.Unlock();
    */
    return ulockmgr_op(fi->fh, cmd, lock, &fi->lock_owner,
                       sizeof(fi->lock_owner));
}

static struct fuse_lowlevel_ops ft_oper = {
 init: 0,
 destroy: 0,
 lookup: ft_ll_lookup,
 forget: ft_ll_forget,
 getattr: ft_ll_getattr,
 setattr: ft_ll_setattr,
 readlink: ft_ll_readlink,
 mknod: ft_ll_mknod,
 mkdir: ft_ll_mkdir,
 unlink: ft_ll_unlink,
 rmdir: ft_ll_rmdir,
 symlink: ft_ll_symlink,
 rename: ft_ll_rename,
 link: ft_ll_link,
 open: ft_ll_open,
 read: ft_ll_read,
 write: ft_ll_write,
 flush: ft_ll_flush,
 release: ft_ll_release,
 fsync: ft_ll_fsync,
 opendir: ft_ll_opendir,
 readdir: ft_ll_readdir,
 releasedir: ft_ll_releasedir,
 fsyncdir: 0,
 statfs: ft_ll_statfs,
 setxattr: 0,
 getxattr: 0,
 listxattr: 0,
 removexattr: 0,
 access: 0,
 create: ft_ll_create,
 getlk: 0,
 setlk: 0,
 bmap: 0
};
/*static struct fuse_operations ft_oper = {
    .getattr	= ft_getattr,
    .fgetattr	= ft_fgetattr,
    //.access	= ft_access,
    .readlink	= ft_readlink,
    .opendir	= ft_opendir,
    .readdir	= ft_readdir,
    .releasedir	= ft_releasedir,
    .mknod	= ft_mknod,
    .mkdir	= ft_mkdir,
    .symlink	= ft_symlink,
    .unlink	= ft_unlink,
    .rmdir	= ft_rmdir,
    .rename	= ft_rename,
    .link	= ft_link,
    .chmod	= ft_chmod,
    .chown	= ft_chown,
    .truncate	= ft_truncate,
    .ftruncate	= ft_ftruncate,
    .utimens	= ft_utimens,
    .create	= ft_create,
    .open	= ft_open,
    .read	= ft_read,
    .write	= ft_write,
    .statfs	= ft_statfs,
    .flush	= ft_flush,
    .release	= ft_release,
    .fsync	= ft_fsync,
#ifdef HAVE_SETXATTR
    .setxattr	= ft_setxattr,
    .getxattr	= ft_getxattr,
    .listxattr	= ft_listxattr,
    .removexattr= ft_removexattr,
#endif
    .lock	= ft_lock,
    };*/

int main(int argc, char *argv[])
{
    // open trace
    

    umask(0);
    return fuse_main(argc, argv, &ft_oper, NULL);
}
