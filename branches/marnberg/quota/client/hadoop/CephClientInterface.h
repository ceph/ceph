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

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#ifdef DARWIN
#include <sys/statvfs.h>
#else
#include <sys/statfs.h>
#endif // DARWIN

// ceph stuff
#include "include/types.h"

#include "Client.h"

#include "config.h"

// stl
#include <map>






// stbuf holds the attributes
static int ceph_getattr(Client* client, const char *path, struct stat *stbuf);

// reads a symlink
static int ceph_readlink(Client* client, const char *path, char *buf, size_t size);

// to do: remove fuse stuff from this one
//static int ceph_getdir(Client* client, const char *path, fuse_dirh_t h, fuse_dirfil_t filler);

// looks irrelevant - it's for special device files
static int ceph_mknod(Client* client, const char *path, mode_t mode, dev_t rdev);

// mode is the file permission bits
static int ceph_mkdir(Client* client, const char *path, mode_t mode);

// delete!
static int ceph_unlink(Client* client, const char *path);

// delete! if it's an empty directory
static int ceph_rmdir(Client* client, const char *path);

// make a symlink
static int ceph_symlink(Client* client, const char *from, const char *to);

// self-explanatory
static int ceph_rename(Client* client, const char *from, const char *to);

static int ceph_link(Client* client, const char *from, const char *to); //hard link

static int ceph_chmod(Client* client, const char *path, mode_t mode); //just chmod

static int ceph_chown(Client* client, const char *path, uid_t uid, gid_t gid); //duh

static int ceph_truncate(Client* client, const char *path, off_t size); //chop or zero-pad to size

// set file access/modification times
static int ceph_utime(Client* client, const char *path, struct utimbuf *buf);

// ok, gotta figure out what's in fuse_file_info and how to use it. Presumably it includes
// a file descriptor and the open flags?
static int ceph_open(Client* client, const char *path, struct fuse_file_info *fi);

// read!
static int ceph_read(Client* client, const char *path, char *buf, size_t size, off_t offset,
                     struct fuse_file_info *fi);

// write!
static int ceph_write(Client* client, const char *path, const char *buf, size_t size,
		      off_t offset, struct fuse_file_info *fi);

/* was already commented out
static int ceph_flush(const char *path, struct fuse_file_info *fi);
*/


// is this statvfs perhaps? we probably don't need it
#ifdef DARWIN
static int ceph_statfs(Client* client, const char *path, struct statvfs *stbuf);
#else
static int ceph_statfs(Client* client, const char *path, struct statfs *stbuf);
#endif

// Remove fuse stuff from these two
//static int ceph_release(Client* client, const char *path, struct fuse_file_info *fi);

//static int ceph_fsync(Client* client, const char *path, int isdatasync,	struct fuse_file_info *fi); //kinda like flush?

/* ceph_fuse_main
 * - start up fuse glue, attached to Client* cl.
 * - argc, argv should include a mount point, and 
 *   any weird fuse options you want.  by default,
 *   we will put fuse in the foreground so that it
 *   won't fork and we can see stdout.
 */
// int ceph_fuse_main(Client *cl, int argc, char *argv[]);
