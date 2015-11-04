// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * convert RGW commands to file commands
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef RADOS_RGW_FILE_H
#define RADOS_RGW_FILE_H

#include <sys/types.h>
#include <stdint.h>
#include <stdbool.h>

#include "include/rados/librgw.h"

#ifdef __cplusplus
extern "C" {
#endif


/*
 * object types
 */
enum rgw_fh_type {
  RGW_FS_TYPE_FILE = 0,
  RGW_FS_TYPE_DIRECTORY,
};

/*
 * dynamic allocated handle to support nfs handle
 */

/* content-addressable hash */
struct rgw_fh_hk {
  uint64_t bucket;
  uint64_t object;
};

struct rgw_file_handle
{
  /* content-addressable hash */
  struct rgw_fh_hk fh_hk;
  void *fh_private; /* librgw private data */
  /* object type */
  enum rgw_fh_type fh_type;
};

struct rgw_fs
{
  librgw_t rgw;
  void *fs_private;
  struct rgw_file_handle* root_fh;
};


/* XXX mount info hypothetical--emulate Unix, support at least
 * UUID-length fsid */
struct rgw_statvfs {
    uint64_t  f_bsize;    /* file system block size */
    uint64_t  f_frsize;   /* fragment size */
    uint64_t     f_blocks;   /* size of fs in f_frsize units */
    uint64_t     f_bfree;    /* # free blocks */
    uint64_t     f_bavail;   /* # free blocks for unprivileged users */
    uint64_t     f_files;    /* # inodes */
    uint64_t     f_ffree;    /* # free inodes */
    uint64_t     f_favail;   /* # free inodes for unprivileged users */
    uint64_t     f_fsid[2];     /* file system ID */
    uint64_t     f_flag;     /* mount flags */
    uint64_t     f_namemax;  /* maximum filename length */
};

/*
  lookup object by name (POSIX style)
*/
int rgw_lookup(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh, const char *path,
	      struct rgw_file_handle **fh, uint32_t flags);

/*
  lookup object by handle (NFS style)
*/
int rgw_lookup_handle(struct rgw_fs *rgw_fs, struct rgw_fh_hk *fh_hk,
		      struct rgw_file_handle **fh, uint32_t flags);

/*
 * release file handle
 */
int rgw_fh_rele(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		uint32_t flags);

/*
 attach rgw namespace
*/
int rgw_mount(librgw_t rgw, const char *uid, const char *key,
	      const char *secret, struct rgw_fs **rgw_fs);

/*
 detach rgw namespace
*/
int rgw_umount(struct rgw_fs *rgw_fs);


/*
  get filesystem attributes
*/
int rgw_statfs(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *parent_fh,
	       struct rgw_statvfs *vfs_st);


/*
  create file
*/
int rgw_create(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *parent_fh,
	       const char *name, mode_t mode, struct stat *st,
	       struct rgw_file_handle *fh);

/*
  create a new directory
*/
int rgw_mkdir(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh,
	      const char *name, mode_t mode, struct stat *st,
	      struct rgw_file_handle *fh);

/*
  rename object
*/
int rgw_rename(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *olddir, const char* old_name,
	       struct rgw_file_handle *newdir, const char* new_name);

/*
  remove file or directory
*/
int rgw_unlink(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *parent_fh, const char* path);

/*
    read  directory content
*/
typedef bool (*rgw_readdir_cb)(const char *name, void *arg, uint64_t offset);

int rgw_readdir(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *parent_fh, uint64_t *offset,
		rgw_readdir_cb rcb, void *cb_arg, bool *eof);

/* XXX (get|set)attr mask bits */
#define RGW_SETATTR_MODE   1
#define RGW_SETATTR_UID    2
#define RGW_SETATTR_GID    4
#define RGW_SETATTR_MTIME  8
#define RGW_SETATTR_ATIME 16
#define RGW_SETATTR_SIZE  32
#define RGW_SETATTR_CTIME 64

/*
   get unix attributes for object
*/
int rgw_getattr(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *fh, struct stat *st);

/*
   set unix attributes for object
*/
int rgw_setattr(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *fh, struct stat *st,
		uint32_t mask);

/*
   truncate file
*/
int rgw_truncate(struct rgw_fs *rgw_fs,
		 struct rgw_file_handle *fh, uint64_t size);

/*
   open file
*/
int rgw_open(struct rgw_fs *rgw_fs, struct rgw_file_handle *parent_fh,
	    uint32_t flags);

/*
   close file
*/

#define RGW_CLOSE_FLAG_NONE 0x0000
#define RGW_CLOSE_FLAG_RELE 0x0001
  
int rgw_close(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
	      uint32_t flags);

/*
   read data from file
*/
int rgw_read(struct rgw_fs *rgw_fs,
	    struct rgw_file_handle *fh, uint64_t offset,
	    size_t length, size_t *bytes_read, void *buffer);
  
/*
   write data to file
*/
int rgw_write(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, uint64_t offset,
	      size_t length, size_t *bytes_written, void *buffer);

#define RGW_UIO_NONE    0x0000
#define RGW_UIO_GIFT    0x0001
#define RGW_UIO_FREE    0x0002
#define RGW_UIO_BUFQ    0x0004

struct rgw_uio;
typedef void (*rgw_uio_release)(struct rgw_uio *, uint32_t);

/* buffer vector descriptors */
struct rgw_vio {
  void *vio_p1;
  void *vio_u1;
  void *vio_base;
  int32_t vio_len;
};
  
struct rgw_uio {
  rgw_uio_release uio_rele;
  void *uio_p1;
  void *uio_u1;
  uint64_t uio_offset;
  uint64_t uio_resid;
  uint32_t uio_cnt;
  uint32_t uio_flags;
  struct rgw_vio *uio_vio; /* appended vectors */
};

typedef struct rgw_uio rgw_uio;

int rgw_readv(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, rgw_uio *uio);
  
int rgw_writev(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, rgw_uio *uio);

/*
   sync written data
*/
int rgw_fsync(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh);

/* XXXX not implemented (needed?) */

int set_user_permissions(const char *uid);

int get_user_permissions(const char *uid);

int set_dir_permissions(const struct rgw_file_handle *fh);

int get_dir_permissions(const struct rgw_file_handle *fh);

int set_file_permissions(const struct rgw_file_handle *fh);

int get_file_permissions(const struct rgw_file_handle *fh);

int rgw_acl2perm();

int rgw_perm2acl();

#ifdef __cplusplus
}
#endif

#endif /* RADOS_RGW_FILE_H */
