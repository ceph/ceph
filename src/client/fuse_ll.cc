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
  stmask = client->ll_lookup(parent, name, &fe.attr);
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

  if (client->ll_getattr(ino, &stbuf) == 0) 
    fuse_reply_attr(req, &stbuf, 0);
  else
    fuse_reply_err(req, ENOENT);
}

static void ceph_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
			    int to_set, struct fuse_file_info *fi)
{
  int r = client->ll_setattr(ino, attr, to_set);
  if (r == 0)
    fuse_reply_attr(req, attr, 0);
  else
    fuse_reply_err(req, -r);
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

static void ceph_ll_readlink(fuse_req_t req, fuse_ino_t ino)
{
  const char *value;
  int r = client->ll_readlink(ino, &value);
  if (r == 0) 
    fuse_reply_readlink(req, value);
  else
    fuse_reply_err(req, -r);
}

static void ceph_ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
			  mode_t mode, dev_t rdev)
{
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));

  int r = client->ll_mknod(parent, name, mode, rdev, &fe.attr);
  if (r == 0) {
    fe.ino = fe.attr.st_ino;
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
			  mode_t mode)
{
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));

  int r = client->ll_mkdir(parent, name, mode, &fe.attr);
  if (r == 0) {
    fe.ino = fe.attr.st_ino;
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  int r = client->ll_unlink(parent, name);
  fuse_reply_err(req, -r);
}

static void ceph_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  int r = client->ll_rmdir(parent, name);
  fuse_reply_err(req, -r);
}

static void ceph_ll_symlink(fuse_req_t req, const char *existing, fuse_ino_t parent, const char *name)
{
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));

  int r = client->ll_symlink(parent, name, existing, &fe.attr);
  if (r == 0) {
    fe.ino = fe.attr.st_ino;
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
			   fuse_ino_t newparent, const char *newname)
{
  int r = client->ll_rename(parent, name, newparent, newname);
  fuse_reply_err(req, -r);
}

static void ceph_ll_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
			 const char *newname)
{
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));
  
  int r = client->ll_link(ino, newparent, newname, &fe.attr);
  if (r == 0) {
    fe.ino = fe.attr.st_ino;
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  Fh *fh;
  int r = client->ll_open(ino, fi->flags, &fh);
  if (r == 0) {
    fi->fh = (long)fh;
    fuse_reply_open(req, fi);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
			 struct fuse_file_info *fi)
{
  Fh *fh = (Fh*)fi->fh;
  bufferlist bl;
  int r = client->ll_read(fh, off, size, &bl);
  if (r >= 0)
    fuse_reply_buf(req, bl.c_str(), bl.length());
  else
    fuse_reply_err(req, -r);
}

static void ceph_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
			   size_t size, off_t off, struct fuse_file_info *fi)
{
  Fh *fh = (Fh*)fi->fh;
  int r = client->ll_write(fh, off, size, buf);
  if (r >= 0)
    fuse_reply_write(req, r);
  else
    fuse_reply_err(req, -r);
}

static void ceph_ll_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  // NOOP
  fuse_reply_err(req, 0);
}

static void ceph_ll_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  Fh *fh = (Fh*)fi->fh;
  int r = client->ll_release(fh);
  fuse_reply_err(req, -r);
}

static void ceph_ll_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
			  struct fuse_file_info *fi)
{
  Fh *fh = (Fh*)fi->fh;
  int r = client->ll_fsync(fh, datasync);
  fuse_reply_err(req, -r);
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
  
  DIR *dirp = (DIR*)fi->fh;
  client->seekdir(dirp, off);

  struct dirent de;
  struct stat st;
  memset(&st, 0, sizeof(st));

  while (1) {
    int r = client->readdir_r(dirp, &de);
    if (r < 0) break;
    st.st_ino = de.d_ino;
    st.st_mode = DT_TO_MODE(de.d_type);

    off_t off = client->telldir(dirp);
    size_t entrysize = fuse_add_direntry(req, buf + pos, size - pos,
					 de.d_name, &st, off);

    /*
    cout << "ceph_ll_readdir added " << de.d_name << " at " << pos << " len " << entrysize
	 << " (buffer size is " << size << ")" 
	 << " .. off = " << off
	 << std::endl;
    */
    
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
  client->ll_releasedir(dirp);
  fuse_reply_err(req, 0);
}

static void ceph_ll_create(fuse_req_t req, fuse_ino_t parent, const char *name,
			   mode_t mode, struct fuse_file_info *fi)
{
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));
  Fh *fh;
  int r = client->ll_create(parent, name, mode, fi->flags, &fe.attr, &fh);
  if (r == 0) {
    fi->fh = (long)fh;
    fe.ino = fe.attr.st_ino;
    fuse_reply_create(req, &fe, fi);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_statfs(fuse_req_t req, fuse_ino_t ino)
{
  struct statvfs stbuf;
  int r = client->ll_statfs(ino, &stbuf);
  if (r == 0)
    fuse_reply_statfs(req, &stbuf);
  else
    fuse_reply_err(req, -r);
}

static struct fuse_lowlevel_ops ceph_ll_oper = {
 init: 0,
 destroy: 0,
 lookup: ceph_ll_lookup,
 forget: ceph_ll_forget,
 getattr: ceph_ll_getattr,
 setattr: ceph_ll_setattr,
 readlink: ceph_ll_readlink,
 mknod: ceph_ll_mknod,
 mkdir: ceph_ll_mkdir,
 unlink: ceph_ll_unlink,
 rmdir: ceph_ll_rmdir,
 symlink: ceph_ll_symlink,
 rename: ceph_ll_rename,
 link: ceph_ll_link,
 open: ceph_ll_open,
 read: ceph_ll_read,
 write: ceph_ll_write,
 flush: ceph_ll_flush,
 release: ceph_ll_release,
 fsync: ceph_ll_fsync,
 opendir: ceph_ll_opendir,
 readdir: ceph_ll_readdir,
 releasedir: ceph_ll_releasedir,
 fsyncdir: 0,
 statfs: ceph_ll_statfs,
 setxattr: 0,
 getxattr: 0,
 listxattr: 0,
 removexattr: 0,
 access: 0,
 create: ceph_ll_create,
 getlk: 0,
 setlk: 0,
 bmap: 0
};

int ceph_fuse_ll_main(Client *c, int argc, const char *argv[])
{
  cout << "ceph_fuse_ll_main starting fuse on pid " << getpid() << std::endl;

  client = c;

  // set up fuse argc/argv
  int newargc = 0;
  const char **newargv = (const char **) malloc((argc + 10) * sizeof(char *));
  newargv[newargc++] = argv[0];
  newargv[newargc++] = "-f";  // stay in foreground

  newargv[newargc++] = "-o";
  newargv[newargc++] = "allow_other";

  for (int argctr = 1; argctr < argc; argctr++) newargv[newargc++] = argv[argctr];

  // go go gadget fuse
  struct fuse_args args = FUSE_ARGS_INIT(newargc, (char**)newargv);
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
  
  cout << "ceph_fuse_ll_main done, err=" << err << std::endl;
  return err ? 1 : 0;
}

