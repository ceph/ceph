// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


/*
    FUSE: Filesystem in Userspace
    Copyright (C) 2001-2005  Miklos Szeredi <miklos@szeredi.hu>

    This program can be distributed under the terms of the GNU GPL.
    See the file COPYING.
*/


// fuse crap
#ifdef linux
/* For pread()/pwrite() */
#define _XOPEN_SOURCE 500
#endif

#define FUSE_USE_VERSION 25

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/statvfs.h>


// ceph stuff
#include "include/types.h"

#include "Client.h"

#include "config.h"

// stl
#include <map>
using namespace std;


// globals
Client *client;     // the ceph client



// ------
// fuse hooks

// checks fuse context or else returns the real getuid
//static int ceph_getuid() {
//}

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
  map<string, inode_t> contents;

  int res = client->getdir(path, contents);
  if (res < 0) return res;

  // return contents to fuse via callback
  for (map<string, inode_t>::iterator it = contents.begin();
       it != contents.end();
       it++) {
    // (immutable) inode contents too.
    res = filler(h,                                    // fuse's handle
                 it->first.c_str(),                    // dentry as char*
                 it->second.mode & INODE_TYPE_MASK,   // mask type bits from mode
                 it->second.ino);                     // ino.. 64->32 bit issue here? FIXME
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
  return client->chown(path, uid, gid);
}

static int ceph_truncate(const char *path, off_t size)
{
  return client->truncate(path, size);      
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
  return 0;  // fuse wants 0 onsucess
}

static int ceph_read(const char *path, char *buf, size_t size, off_t offset,
                     struct fuse_file_info *fi)
{
  fh_t fh = fi->fh;
  return client->read(fh, buf, size, offset);
}

static int ceph_write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
  fh_t fh = fi->fh;
  return client->write(fh, buf, size, offset);
}

/*
static int ceph_flush(const char *path, struct fuse_file_info *fi)
{
  fh_t fh = fi->fh;
  return client->flush(fh);
}
*/


static int ceph_statfs(const char *path, struct statvfs *stbuf)
{
  return client->statfs(path, stbuf);
}



static int ceph_release(const char *path, struct fuse_file_info *fi)
{
  fh_t fh = fi->fh;
  int r = client->close(fh);  // close the file
  return r;
}

static int ceph_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
  fh_t fh = fi->fh;
  return client->fsync(fh, isdatasync ? true:false);
}


static struct fuse_operations ceph_oper = {
  getattr: ceph_getattr,
  readlink: ceph_readlink,
  getdir: ceph_getdir,
  mknod: ceph_mknod,
  mkdir: ceph_mkdir,
  unlink: ceph_unlink,
  rmdir: ceph_rmdir,
  symlink: ceph_symlink,
  rename: ceph_rename,
  link: ceph_link,
  chmod: ceph_chmod,
  chown: ceph_chown,
  truncate: ceph_truncate,
  utime: ceph_utime,
  open: ceph_open,
  read: ceph_read,
  write: ceph_write,
  statfs: ceph_statfs,
  flush: 0, //ceph_flush,   
  release: ceph_release,
  fsync: ceph_fsync
};


int ceph_fuse_main(Client *c, int argc, char *argv[])
{
  // init client
  client = c;

  // set up fuse argc/argv
  int newargc = 0;
  char **newargv = (char **) malloc((argc + 10) * sizeof(char *));
  newargv[newargc++] = argv[0];
  
  // allow other (all!) users to see my file system
  // NOTE: echo user_allow_other >> /etc/fuse.conf
  // NB: seems broken on Darwin
#ifndef DARWIN
  newargv[newargc++] = "-o";
  newargv[newargc++] = "allow_other";
#endif // DARWIN
  
  // use inos
  newargv[newargc++] = "-o";
  newargv[newargc++] = "use_ino";

  // large reads, direct_io (no kernel cachine)
  //newargv[newargc++] = "-o";
  //newargv[newargc++] = "large_read";
  if (g_conf.fuse_direct_io) {
    newargv[newargc++] = "-o";
    newargv[newargc++] = "direct_io";
  }

  // disable stupid fuse unlink hiding thing
  newargv[newargc++] = "-o";
  newargv[newargc++] = "hard_remove";

  // force into foreground
  //   -> we can watch stdout this way!!
  newargv[newargc++] = "-f";
  
  // copy rest of cmdline (hopefully, the mount point!)
  for (int argctr = 1; argctr < argc; argctr++) newargv[newargc++] = argv[argctr];
  
  // go fuse go
  cout << "ok, calling fuse_main" << endl;
  int r = fuse_main(newargc, newargv, &ceph_oper);
  return r;
}
