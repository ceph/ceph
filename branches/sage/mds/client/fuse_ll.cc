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

#define FUSE_USE_VERSION 26

#include <fuse/fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

// ceph
#include "include/types.h"
#include "Client.h"
#include "config.h"

static Client *client;


static void ceph_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  struct fuse_entry_param fe;
  int stmask;

  memset(&fe, 0, sizeof(fe));
  stmask = client->ll_lookup(parent, name, &fe.attr, &fe.attr_timeout, &fe.entry_timeout);
  if (stmask >= 0) {
    fe.ino = fe.attr.st_ino;
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, ENOENT);
  }
}

static void ceph_ll_forget(fuse_req_t req, fuse_ino_t ino, long unsigned nlookup)
{
  client->ll_forget(ino, nlookup);
  fuse_reply_none(req);
}

static void ceph_ll_getattr(fuse_req_t req, fuse_ino_t ino,
			    struct fuse_file_info *fi)
{
  struct stat stbuf;
  
  (void) fi;

  double attr_timeout;
  if (client->ll_getattr(ino, &stbuf, &attr_timeout) == 0) 
    fuse_reply_attr(req, &stbuf, attr_timeout);
  else
    fuse_reply_err(req, ENOENT);
}

static void ceph_ll_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  void *dirp;
  int r = client->ll_opendir(ino, &dirp);
  if (r >= 0) {
    fi->fh = (long)dirp;
    fuse_reply_open(req, fi);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
			    off_t off, struct fuse_file_info *fi)
{
  (void) fi;
  
  // buffer
  char *buf;
  size_t pos = 0;

  buf = new char[size];
  if (!buf) {
    fuse_reply_err(req, ENOMEM);
    return;
  }
  
  // readdir
  struct dirent de;
  struct stat st;
  memset(&st, 0, sizeof(st));

  DIR *dirp = (DIR*)fi->fh;

  client->seekdir(dirp, off);

  while (1) {
    int r = client->readdir_r(dirp, &de);
    if (r < 0) break;
    st.st_ino = de.d_ino;
    st.st_mode = DT_TO_MODE(de.d_type);
    
    off_t off = client->telldir(dirp);
    size_t entrysize = fuse_add_direntry(req, buf + pos, size - pos,
					 de.d_name, &st, off);
    if (entrysize > size - pos) 
      break;  // didn't fit, done for now.
    pos += entrysize;
  }

  fuse_reply_buf(req, buf, pos);
  delete[] buf;
}

static void ceph_ll_releasedir(fuse_req_t req, fuse_ino_t ino,
			       struct fuse_file_info *fi)
{
  DIR *dirp = (DIR*)fi->fh;
  client->closedir(dirp);
  fuse_reply_err(req, 0);
}

static struct fuse_lowlevel_ops ceph_ll_oper = {
 init: 0,
 destroy: 0,
 lookup: ceph_ll_lookup,
 forget: ceph_ll_forget,
 getattr: ceph_ll_getattr,
 setattr: 0,
 readlink: 0,
 mknod: 0,
 mkdir: 0,
 unlink: 0,
 rmdir: 0,
 symlink: 0,
 rename: 0,
 link: 0,
 open: 0,
 read: 0,
 write: 0,
 flush: 0,
 release: 0,
 fsync: 0,
 opendir: ceph_ll_opendir,
 readdir: ceph_ll_readdir,
 releasedir: ceph_ll_releasedir,
 fsyncdir: 0,
 statfs: 0,
 setxattr: 0,
 getxattr: 0,
 listxattr: 0,
 removexattr: 0,
 access: 0,
 create: 0,
 getlk: 0,
 setlk: 0,
 bmap: 0
};

int ceph_fuse_ll_main(Client *c, int argc, char *argv[])
{
  cout << "ceph_fuse_ll_main starting fuse" << endl;

  client = c;

  // set up fuse argc/argv
  int newargc = 0;
  char **newargv = (char **) malloc((argc + 10) * sizeof(char *));
  newargv[newargc++] = argv[0];
  newargv[newargc++] = "-f";  // stay in foreground
  for (int argctr = 1; argctr < argc; argctr++) newargv[newargc++] = argv[argctr];

  // go go gadget fuse
  struct fuse_args args = FUSE_ARGS_INIT(newargc, newargv);
  struct fuse_chan *ch;
  char *mountpoint;
  int err = -1;
  
  if (fuse_parse_cmdline(&args, &mountpoint, NULL, NULL) != -1 &&
      (ch = fuse_mount(mountpoint, &args)) != NULL) {
    struct fuse_session *se;
    
    // init fuse
    se = fuse_lowlevel_new(&args, &ceph_ll_oper, sizeof(ceph_ll_oper),
			   NULL);
    if (se != NULL) {
      if (fuse_set_signal_handlers(se) != -1) {
	fuse_session_add_chan(se, ch);
	err = fuse_session_loop(se);
	fuse_remove_signal_handlers(se);
	fuse_session_remove_chan(ch);
      }
      fuse_session_destroy(se);
    }
    fuse_unmount(mountpoint, ch);
  }
  fuse_opt_free_args(&args);
  
  cout << "ceph_fuse_ll_main done, err=" << err << endl;
  return err ? 1 : 0;
}

