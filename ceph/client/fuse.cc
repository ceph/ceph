/*
    FUSE: Filesystem in Userspace
    Copyright (C) 2001-2005  Miklos Szeredi <miklos@szeredi.hu>

    This program can be distributed under the terms of the GNU GPL.
    See the file COPYING.
*/

#include <config.h>

#ifdef linux
/* For pread()/pwrite() */
#define _XOPEN_SOURCE 500
#endif

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/statfs.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include "Client.h"

// fuse globals
Client *client;     // ceph client


// ---
// fuse hooks

static int ceph_getattr(const char *path, struct stat *stbuf)
{
  return client->lstat(path, stbuf);
}

static int ceph_readlink(const char *path, char *buf, size_t size)
{
  int res;

  res = client->readlink(path, buf, size - 1);
  if (res < 0) return res;
  
  buf[res] = '\0';
  return 0;
}


static int ceph_getdir(const char *path, fuse_dirh_t h, fuse_dirfil_t filler)
{
  map<string, inode_t*> contents;

  int res = client->readdir(path, contents);
  
  if (res < 0) return res;

  // return contents to fuse via callback
  for (map<string, inodeno_t>::iterator it = contents.begin();
	   it != contents.end();
	   it++) {
	res = filler(h,                                    // fuse's handle
				 it->first.c_str(),                    // dentry as char*
				 it->second->mode & INODE_TYPE_MASK,   // mask type bits from mode
				 it->second->ino);                     // ino.  64->32 bit issue?  FIXME
	if (res != 0) break;   // fuse has had enough
  }
  return res;
}

static int ceph_mknod(const char *path, mode_t mode, dev_t rdev)
{
  return client->mknod(path, mode);
}

static int ceph_mkdir(const char *path, mode_t mode)
{
  return client->mkdir(path, mode);
}

static int ceph_unlink(const char *path)
{
  return client->unlink(path);
}

static int ceph_rmdir(const char *path)
{
  return client->rmdir(path);
}

static int ceph_symlink(const char *from, const char *to)
{
  return client->symlink(from, to);
}

static int ceph_rename(const char *from, const char *to)
{
  return client->rename(from, to);
}

static int ceph_link(const char *from, const char *to)
{
  return client->link(from, to);
}

static int ceph_chmod(const char *path, mode_t mode)
{
  return client->chmod(path, mode);
}

static int ceph_chown(const char *path, uid_t uid, gid_t gid)
{
  return client->lchown(path, uid, gid);
}

static int ceph_truncate(const char *path, off_t size)
{
  return truncate(path, size);      
}

static int ceph_utime(const char *path, struct utimbuf *buf)
{
  return client->utime(path, buf);
}


static int ceph_open(const char *path, struct fuse_file_info *fi)
{
  int res;
  
  res = client->open(path, fi->flags);
  if (res < 0) return res;
  
  fi->fh = res;
  return res;   // or 0?
}

static int ceph_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
  return client->read(fi->fh, buf, size, offset);
}

static int ceph_write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
  return client->write(fi->fh, buf, size, offset);
}

static int ceph_statfs(const char *path, struct statfs *stbuf)
{
  return client->statfs(path, stbuf);
}



static int ceph_release(const char *path, struct fuse_file_info *fi)
{
    /* Just a stub.  This method is optional and can safely be left
       unimplemented */

    (void) path;
    (void) fi;
    return 0;
}

static int ceph_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
    /* Just a stub.  This method is optional and can safely be left
       unimplemented */

    (void) path;
    (void) isdatasync;
    (void) fi;
    return 0;
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int ceph_setxattr(const char *path, const char *name, const char *value,
                        size_t size, int flags)
{
    int res = lsetxattr(path, name, value, size, flags);
    if(res == -1)
        return -errno;
    return 0;
}

static int ceph_getxattr(const char *path, const char *name, char *value,
                    size_t size)
{
    int res = lgetxattr(path, name, value, size);
    if(res == -1)
        return -errno;
    return res;
}

static int ceph_listxattr(const char *path, char *list, size_t size)
{
    int res = llistxattr(path, list, size);
    if(res == -1)
        return -errno;
    return res;
}

static int ceph_removexattr(const char *path, const char *name)
{
    int res = lremovexattr(path, name);
    if(res == -1)
        return -errno;
    return 0;
}
#endif /* HAVE_SETXATTR */

static struct fuse_operations ceph_oper = {
    .getattr	= ceph_getattr,
    .readlink	= ceph_readlink,
    .getdir	= ceph_getdir,
    .mknod	= ceph_mknod,
    .mkdir	= ceph_mkdir,
    .symlink	= ceph_symlink,
    .unlink	= ceph_unlink,
    .rmdir	= ceph_rmdir,
    .rename	= ceph_rename,
    .link	= ceph_link,
    .chmod	= ceph_chmod,
    .chown	= ceph_chown,
    .truncate	= ceph_truncate,
    .utime	= ceph_utime,
    .open	= ceph_open,
    .read	= ceph_read,
    .write	= ceph_write,
    .statfs	= ceph_statfs,
    .release	= ceph_release,
    .fsync	= ceph_fsync,
#ifdef HAVE_SETXATTR
    .setxattr	= ceph_setxattr,
    .getxattr	= ceph_getxattr,
    .listxattr	= ceph_listxattr,
    .removexattr= ceph_removexattr,
#endif
};

int main(int argc, char *argv[])
{
  // init client
  
  return fuse_main(argc, argv, &ceph_oper);
}
