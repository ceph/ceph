// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#define FUSE_USE_VERSION 26

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

// globals
static Client *client;     // the ceph client



// ------
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


// ------------------
// file i/o

static int ceph_open(const char *path, struct fuse_file_info *fi)
{
  int res;
  
  res = client->open(path, fi->flags, 0);
  if (res < 0) return res;
  fi->fh = res;
  return 0;  // fuse wants 0 onsucess
}

static int ceph_read(const char *path, char *buf, size_t size, off_t offset,
                     struct fuse_file_info *fi)
{
  int fd = fi->fh;
  return client->read(fd, buf, size, offset);
}

static int ceph_write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
  int fd = fi->fh;
  return client->write(fd, buf, size, offset);
}

static int ceph_flush(const char *path, struct fuse_file_info *fi)
{
  //int fh = fi->fh;
  //return client->flush(fh);
  return 0;
}

static int ceph_statfs(const char *path, struct statvfs *stbuf)
{
  return client->statfs(path, stbuf);
}

static int ceph_release(const char *path, struct fuse_file_info *fi)
{
  int fd = fi->fh;
  int r = client->close(fd);  // close the file
  return r;
}

static int ceph_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
  int fd = fi->fh;
  return client->fsync(fd, isdatasync ? true:false);
}


// ---------------------
// directory i/o

static int ceph_opendir(const char *path, struct fuse_file_info *fi)
{
  DIR *dirp;
  int r = client->opendir(path, &dirp);
  if (r < 0) return r;
  fi->fh = (uint64_t)(void*)dirp;
  return 0;
}

static int ceph_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t off, fuse_file_info *fi)
{
  DIR *dirp = (DIR*)fi->fh;
  
  client->seekdir(dirp, off);

  int res = 0;
  struct dirent de;
  struct stat st;
  int stmask = 0;
  while (res == 0) {
    int r = client->readdirplus_r(dirp, &de, &st, &stmask);
    if (r != 0) break;
    int stneed = CEPH_STAT_CAP_TYPE;
    res = filler(buf,
                 de.d_name,
		 ((stmask & stneed) == stneed) ? &st:0,
		 client->telldir(dirp));
  }
  return 0;
}

static int ceph_releasedir(const char *path, struct fuse_file_info *fi)
{
  DIR *dirp = (DIR*)fi->fh;
  int r = client->closedir(dirp);  // close the file
  return r;
}





static struct fuse_operations ceph_oper = {
  getattr: ceph_getattr,
  readlink: ceph_readlink,
  getdir: 0,
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
  flush: ceph_flush,   
  release: ceph_release,
  fsync: ceph_fsync,
  setxattr: 0,
  getxattr: 0,
  listxattr: 0,
  removexattr: 0,
  opendir: ceph_opendir,
  readdir: ceph_readdir,
  releasedir: ceph_releasedir  
};


int ceph_fuse_main(Client *c, int argc, const char *argv[])
{
  // init client
  client = c;

  // set up fuse argc/argv
  int newargc = 0;
  const char **newargv = (const char **) malloc((argc + 10) * sizeof(char *));
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
  cout << "ok, calling fuse_main" << std::endl;
  int r = fuse_main(newargc, (char**)newargv, &ceph_oper, 0);
  return r;
}
