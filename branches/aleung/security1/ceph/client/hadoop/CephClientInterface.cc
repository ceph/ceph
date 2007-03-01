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


//#include <fuse.h>


using namespace std;

// globals
//Client *client;     // the ceph client
//this has to go - the real client will have to hold the pointer.
//Every function will need to take a Client pointer.

// ------
// fuse hooks

static int ceph_getattr(Client* client, const char *path, struct stat *stbuf)
{
  return client->lstat(path, stbuf);
}

static int ceph_readlink(Client* client, const char *path, char *buf, size_t size)
{
  int res;

  res = client->readlink(path, buf, size - 1);
  if (res < 0) return res;
  
  buf[res] = '\0';
  return 0;
}

// get rid of the callback thing, perhaps? and return the answer some other way?
/*
static int ceph_getdir(Client* client, const char *path, fuse_dirh_t h, fuse_dirfil_t filler)
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
*/

static int ceph_mknod(Client* client, const char *path, mode_t mode, dev_t rdev) 
{
  return client->mknod(path, mode);
}

static int ceph_mkdir(Client* client, const char *path, mode_t mode)
{
  return client->mkdir(path, mode);
}

static int ceph_unlink(Client* client, const char *path)
{
  return client->unlink(path);
}

static int ceph_rmdir(Client* client, const char *path)
{
  return client->rmdir(path);
}

static int ceph_symlink(Client* client, const char *from, const char *to)
{
  return client->symlink(from, to);
}


static int ceph_rename(Client* client, const char *from, const char *to)
{
  return client->rename(from, to);
}

static int ceph_link(Client* client, const char *from, const char *to)
{
  return client->link(from, to);
}

static int ceph_chmod(Client* client, const char *path, mode_t mode)
{
  return client->chmod(path, mode);
}

static int ceph_chown(Client* client, const char *path, uid_t uid, gid_t gid)
{
  return client->chown(path, uid, gid);
}

static int ceph_truncate(Client* client, const char *path, off_t size)
{
  return client->truncate(path, size);      
}

static int ceph_utime(Client* client, const char *path, struct utimbuf *buf)
{
  return client->utime(path, buf);
}


static int ceph_open(Client* client, const char *path, struct fuse_file_info *fi)
{
  int res;
  
  res = client->open(path, fi->flags);
  if (res < 0) return res;
  fi->fh = res;
  return 0;  // fuse wants 0 onsucess
}

static int ceph_read(Client* client, const char *path, char *buf, size_t size, off_t offset,
                     struct fuse_file_info *fi)
{
  fh_t fh = fi->fh;
  return client->read(fh, buf, size, offset);
}

static int ceph_write(Client* client, const char *path, const char *buf, size_t size,
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


#ifdef DARWIN
static int ceph_statfs(Client* client, const char *path, struct statvfs *stbuf)
{
  return client->statfs(path, stbuf);
}
#else
static int ceph_statfs(Client* client, const char *path, struct statfs *stbuf)
{
  return client->statfs(path, stbuf);
}
#endif


/* remove fuse stuff from these two
static int ceph_release(Client* client, const char *path, struct fuse_file_info *fi)
{
  fh_t fh = fi->fh;
  int r = client->close(fh);  // close the file
  return r;
}

static int ceph_fsync(Client* client, const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
  fh_t fh = fi->fh;
  return client->fsync(fh, isdatasync ? true:false);
}
*/

/*
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

*/


// Does this do anything we need? No. All it does is assemble a bunch of
// arguments and call fuse_main.

