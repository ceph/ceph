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

// ceph
#include "common/errno.h"
#include "common/safe_io.h"
#include "include/types.h"
#include "Client.h"
#include "common/config.h"

static Client *client;

Mutex stag_lock("fuse_ll.cc stag_lock");
int last_stag = 0;
hash_map<uint64_t,int> snap_stag_map;
hash_map<int,uint64_t> stag_snap_map;

#define FINO_INO(x) ((x) & ((1ull<<48)-1ull))
#define FINO_STAG(x) ((x) >> 48)
#define MAKE_FINO(i,s) ((i) | ((s) << 48))

static uint64_t fino_snap(uint64_t fino)
{
  Mutex::Locker l(stag_lock);
  uint64_t stag = FINO_STAG(fino);
  assert(stag_snap_map.count(stag));
  return stag_snap_map[stag];
}
static vinodeno_t fino_vino(inodeno_t fino)
{
  if (fino.val == 1) {
    fino = inodeno_t(client->get_root_ino());
  }
  vinodeno_t vino(FINO_INO(fino), fino_snap(fino));
  //cout << "fino_vino " << fino << " -> " << vino << std::endl;
  return vino;
}


static uint64_t make_fake_ino(inodeno_t ino, snapid_t snapid)
{
  Mutex::Locker l(stag_lock);
  uint64_t stag;
  if (snap_stag_map.count(snapid) == 0) {
    stag = ++last_stag;
    snap_stag_map[snapid] = stag;
    stag_snap_map[stag] = snapid;
  } else 
    stag = snap_stag_map[snapid];
  inodeno_t fino = MAKE_FINO(ino, stag);
  //cout << "make_fake_ino " << ino << "." << snapid << " -> " << fino << std::endl;
  return fino;
}

static void ceph_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  struct fuse_entry_param fe;
  int r;

  memset(&fe, 0, sizeof(fe));
  r = client->ll_lookup(fino_vino(parent), name, &fe.attr, ctx->uid, ctx->gid);
  if (r >= 0) {
    fe.ino = make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_forget(fuse_req_t req, fuse_ino_t ino, long unsigned nlookup)
{
  client->ll_forget(fino_vino(ino), nlookup);
  fuse_reply_none(req);
}

static void ceph_ll_getattr(fuse_req_t req, fuse_ino_t ino,
			    struct fuse_file_info *fi)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  struct stat stbuf;
  
  (void) fi;

  if (client->ll_getattr(fino_vino(ino), &stbuf, ctx->uid, ctx->gid) == 0) {
    stbuf.st_ino = make_fake_ino(stbuf.st_ino, stbuf.st_dev);
    fuse_reply_attr(req, &stbuf, 0);
  } else
    fuse_reply_err(req, ENOENT);
}

static void ceph_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
			    int to_set, struct fuse_file_info *fi)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);

  int mask = 0;
  if (to_set & FUSE_SET_ATTR_MODE) mask |= CEPH_SETATTR_MODE;
  if (to_set & FUSE_SET_ATTR_UID) mask |= CEPH_SETATTR_UID;
  if (to_set & FUSE_SET_ATTR_GID) mask |= CEPH_SETATTR_GID;
  if (to_set & FUSE_SET_ATTR_MTIME) mask |= CEPH_SETATTR_MTIME;
  if (to_set & FUSE_SET_ATTR_ATIME) mask |= CEPH_SETATTR_ATIME;
  if (to_set & FUSE_SET_ATTR_SIZE) mask |= CEPH_SETATTR_SIZE;

  int r = client->ll_setattr(fino_vino(ino), attr, mask, ctx->uid, ctx->gid);
  if (r == 0)
    fuse_reply_attr(req, attr, 0);
  else
    fuse_reply_err(req, -r);
}

// XATTRS

static void ceph_ll_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
			     const char *value, size_t size, int flags)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  int r = client->ll_setxattr(fino_vino(ino), name, value, size, flags, ctx->uid, ctx->gid);
  fuse_reply_err(req, -r);
}

static void ceph_ll_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  char buf[size];
  int r = client->ll_listxattr(fino_vino(ino), buf, size, ctx->uid, ctx->gid);
  if (size == 0 && r >= 0)
    fuse_reply_xattr(req, r);
  else if (r >= 0) 
    fuse_reply_buf(req, buf, r);
  else
    fuse_reply_err(req, -r);
}

static void ceph_ll_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
			     size_t size)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  char buf[size];
  int r = client->ll_getxattr(fino_vino(ino), name, buf, size, ctx->uid, ctx->gid);
  if (size == 0 && r >= 0)
    fuse_reply_xattr(req, r);
  else if (r >= 0)
    fuse_reply_buf(req, buf, r);
  else
    fuse_reply_err(req, -r);
}

static void ceph_ll_removexattr(fuse_req_t req, fuse_ino_t ino, const char *name)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  int r = client->ll_removexattr(fino_vino(ino), name, ctx->uid, ctx->gid);
  fuse_reply_err(req, -r);
}



static void ceph_ll_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  void *dirp;
  int r = client->ll_opendir(fino_vino(ino), &dirp, ctx->uid, ctx->gid);
  if (r >= 0) {
    fi->fh = (long)dirp;
    fuse_reply_open(req, fi);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_readlink(fuse_req_t req, fuse_ino_t ino)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  const char *value;
  int r = client->ll_readlink(fino_vino(ino), &value, ctx->uid, ctx->gid);
  if (r == 0) 
    fuse_reply_readlink(req, value);
  else
    fuse_reply_err(req, -r);
}

static void ceph_ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
			  mode_t mode, dev_t rdev)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));

  int r = client->ll_mknod(fino_vino(parent), name, mode, rdev, &fe.attr, ctx->uid, ctx->gid);
  if (r == 0) {
    fe.ino = make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
			  mode_t mode)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));

  int r = client->ll_mkdir(fino_vino(parent), name, mode, &fe.attr, ctx->uid, ctx->gid);
  if (r == 0) {
    fe.ino = make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  int r = client->ll_unlink(fino_vino(parent), name, ctx->uid, ctx->gid);
  fuse_reply_err(req, -r);
}

static void ceph_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  int r = client->ll_rmdir(fino_vino(parent), name, ctx->uid, ctx->gid);
  fuse_reply_err(req, -r);
}

static void ceph_ll_symlink(fuse_req_t req, const char *existing, fuse_ino_t parent, const char *name)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));

  int r = client->ll_symlink(fino_vino(parent), name, existing, &fe.attr, ctx->uid, ctx->gid);
  if (r == 0) {
    fe.ino = make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
			   fuse_ino_t newparent, const char *newname)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  int r = client->ll_rename(fino_vino(parent), name, fino_vino(newparent), newname, ctx->uid, ctx->gid);
  fuse_reply_err(req, -r);
}

static void ceph_ll_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
			 const char *newname)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));
  
  int r = client->ll_link(fino_vino(ino), fino_vino(newparent), newname, &fe.attr, ctx->uid, ctx->gid);
  if (r == 0) {
    fe.ino = make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Fh *fh;
  int r = client->ll_open(fino_vino(ino), fi->flags, &fh, ctx->uid, ctx->gid);
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

struct readdir_context {
  fuse_req_t req;
  char *buf;
  size_t size;
  size_t pos; /* in buf */
  uint64_t snap;
};

/*
 * return 0 on success, -1 if out of space
 */
static int ceph_ll_add_dirent(void *p, struct dirent *de, struct stat *st, int stmask, off_t next_off)
{
  struct readdir_context *c = (struct readdir_context *)p;

  st->st_ino = make_fake_ino(de->d_ino, c->snap);
  st->st_mode = DT_TO_MODE(de->d_type);

  size_t room = c->size - c->pos;
  size_t entrysize = fuse_add_direntry(c->req, c->buf + c->pos, room,
				       de->d_name, st, next_off);
  if (entrysize > room)
    return -ENOSPC;

  /* success */
  c->pos += entrysize;
  return 0;
}

static void ceph_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
			    off_t off, struct fuse_file_info *fi)
{
  (void) fi;

  DIR *dirp = (DIR*)fi->fh;
  client->seekdir(dirp, off);

  struct readdir_context rc;
  rc.req = req;
  rc.buf = new char[size];
  rc.size = size;
  rc.pos = 0;
  rc.snap = fino_snap(ino);

  int r;
  do {
    r = client->readdir_r_cb(dirp, ceph_ll_add_dirent, &rc);
  } while (r > 0);

  fuse_reply_buf(req, rc.buf, rc.pos);
  delete[] rc.buf;
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
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));
  Fh *fh;
  int r = client->ll_create(fino_vino(parent), name, mode, fi->flags, &fe.attr, &fh, ctx->uid, ctx->gid);
  if (r == 0) {
    fi->fh = (long)fh;
    fe.ino = make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fuse_reply_create(req, &fe, fi);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ceph_ll_statfs(fuse_req_t req, fuse_ino_t ino)
{
  struct statvfs stbuf;
  int r = client->ll_statfs(fino_vino(ino), &stbuf);
  if (r == 0)
    fuse_reply_statfs(req, &stbuf);
  else
    fuse_reply_err(req, -r);
}

int fd_on_success = 0;

static void do_init(void *foo, fuse_conn_info *bar)
{
  if (fd_on_success) {
    //cout << "fuse init signaling on fd " << fd_on_success << std::endl;
    uint32_t r = 0;
    int err = safe_write(fd_on_success, &r, sizeof(r));
    if (err) {
      derr << "fuse_ll: do_init: safe_write failed with error "
	   << cpp_strerror(err) << dendl;
      ceph_abort();
    }
    //cout << "fuse init done signaling on fd " << fd_on_success << std::endl;

    // close stdout, etc.
    ::close(0);
    ::close(1);
    ::close(2);
  }
}

const static struct fuse_lowlevel_ops ceph_ll_oper = {
 init: do_init,
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
 setxattr: ceph_ll_setxattr,
 getxattr: ceph_ll_getxattr,
 listxattr: ceph_ll_listxattr,
 removexattr: ceph_ll_removexattr,
 access: 0,
 create: ceph_ll_create,
 getlk: 0,
 setlk: 0,
 bmap: 0
};

int ceph_fuse_ll_main(Client *c, int argc, const char *argv[], int fd)
{
  //cout << "ceph_fuse_ll_main starting fuse on pid " << getpid() << std::endl;

  fd_on_success = fd;

  client = c;

  snap_stag_map[CEPH_NOSNAP] = 0;
  stag_snap_map[0] = CEPH_NOSNAP;
  
  // set up fuse argc/argv
  int newargc = 0;
  const char **newargv = (const char **) malloc((argc + 10) * sizeof(char *));
  newargv[newargc++] = argv[0];
  newargv[newargc++] = "-f";  // stay in foreground

  newargv[newargc++] = "-o";
  newargv[newargc++] = "allow_other";

  newargv[newargc++] = "-o";
  newargv[newargc++] = "default_permissions";

  //newargv[newargc++] = "-d";

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
    err = 0;
  } else {
    err = -errno;
  }
  fuse_opt_free_args(&args);
  
  //cout << "ceph_fuse_ll_main done, err=" << err << std::endl;
  return err;
}

