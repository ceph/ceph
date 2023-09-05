// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * convert RGW commands to file commands
 *
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef RADOS_RGW_FILE_H
#define RADOS_RGW_FILE_H

#include <sys/stat.h>
#include <sys/types.h>
#include <stdint.h>
#include <stdbool.h>

#include "librgw.h"

#ifdef __cplusplus
extern "C" {
#endif

#define LIBRGW_FILE_VER_MAJOR 1
#define LIBRGW_FILE_VER_MINOR 2
#define LIBRGW_FILE_VER_EXTRA 1 /* version number needs to advance to
				 * match change in rgw_raddir2 signature */

#define LIBRGW_FILE_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)
#define LIBRGW_FILE_VERSION_CODE LIBRGW_FILE_VERSION(LIBRGW_FILE_VER_MAJOR, LIBRGW_FILE_VER_MINOR, LIBRGW_FILE_VER_EXTRA)

/*
 * object types
 */
enum rgw_fh_type {
  RGW_FS_TYPE_NIL = 0,
  RGW_FS_TYPE_FILE,
  RGW_FS_TYPE_DIRECTORY,
  RGW_FS_TYPE_SYMBOLIC_LINK,
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


void rgwfile_version(int *major, int *minor, int *extra);

/*
  lookup object by name (POSIX style)
*/
#define RGW_LOOKUP_FLAG_NONE    0x0000
#define RGW_LOOKUP_FLAG_CREATE  0x0001
#define RGW_LOOKUP_FLAG_RCB     0x0002 /* readdir callback hint */
#define RGW_LOOKUP_FLAG_DIR     0x0004
#define RGW_LOOKUP_FLAG_FILE    0x0008

#define RGW_LOOKUP_TYPE_FLAGS \
  (RGW_LOOKUP_FLAG_DIR|RGW_LOOKUP_FLAG_FILE)

int rgw_lookup(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh, const char *path,
	      struct rgw_file_handle **fh,
	      struct stat *st, uint32_t mask, uint32_t flags);

/*
  lookup object by handle (NFS style)
*/
int rgw_lookup_handle(struct rgw_fs *rgw_fs, struct rgw_fh_hk *fh_hk,
		      struct rgw_file_handle **fh, uint32_t flags);

/*
 * release file handle
 */
#define RGW_FH_RELE_FLAG_NONE   0x0000

int rgw_fh_rele(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		uint32_t flags);

/*
 attach rgw namespace
*/
#define RGW_MOUNT_FLAG_NONE     0x0000

int rgw_mount(librgw_t rgw, const char *uid, const char *key,
	      const char *secret, struct rgw_fs **rgw_fs,
	      uint32_t flags);

int rgw_mount2(librgw_t rgw, const char *uid, const char *key,
               const char *secret, const char *root, struct rgw_fs **rgw_fs,
               uint32_t flags);

/*
 register invalidate callbacks
*/
#define RGW_REG_INVALIDATE_FLAG_NONE    0x0000

typedef void (*rgw_fh_callback_t)(void *handle, struct rgw_fh_hk fh_hk);

int rgw_register_invalidate(struct rgw_fs *rgw_fs, rgw_fh_callback_t cb,
			    void *arg, uint32_t flags);

/*
 detach rgw namespace
*/
#define RGW_UMOUNT_FLAG_NONE    0x0000

int rgw_umount(struct rgw_fs *rgw_fs, uint32_t flags);


/*
  get filesystem attributes
*/
#define RGW_STATFS_FLAG_NONE     0x0000

int rgw_statfs(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *parent_fh,
	       struct rgw_statvfs *vfs_st,
	       uint32_t flags);


/* XXX (get|set)attr mask bits */
#define RGW_SETATTR_MODE   1
#define RGW_SETATTR_UID    2
#define RGW_SETATTR_GID    4
#define RGW_SETATTR_MTIME  8
#define RGW_SETATTR_ATIME 16
#define RGW_SETATTR_SIZE  32
#define RGW_SETATTR_CTIME 64

/*
  create file
*/
#define RGW_CREATE_FLAG_NONE     0x0000

int rgw_create(struct rgw_fs *rgw_fs, struct rgw_file_handle *parent_fh,
	       const char *name, struct stat *st, uint32_t mask,
	       struct rgw_file_handle **fh, uint32_t posix_flags,
	       uint32_t flags);

/*
  create a symbolic link
 */
#define RGW_CREATELINK_FLAG_NONE     0x0000
int rgw_symlink(struct rgw_fs *rgw_fs, struct rgw_file_handle *parent_fh,
               const char *name, const char *link_path, struct stat *st, 
               uint32_t mask, struct rgw_file_handle **fh, uint32_t posix_flags,
               uint32_t flags);

/*
  create a new directory
*/
#define RGW_MKDIR_FLAG_NONE      0x0000

int rgw_mkdir(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh,
	      const char *name, struct stat *st, uint32_t mask,
	      struct rgw_file_handle **fh, uint32_t flags);

/*
  rename object
*/
#define RGW_RENAME_FLAG_NONE      0x0000

int rgw_rename(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *olddir, const char* old_name,
	       struct rgw_file_handle *newdir, const char* new_name,
	       uint32_t flags);

/*
  remove file or directory
*/
#define RGW_UNLINK_FLAG_NONE      0x0000

int rgw_unlink(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *parent_fh, const char* path,
	       uint32_t flags);

/*
    read  directory content
*/
typedef int (*rgw_readdir_cb)(const char *name, void *arg, uint64_t offset,
			       struct stat *st, uint32_t mask,
			       uint32_t flags);

#define RGW_READDIR_FLAG_NONE      0x0000
#define RGW_READDIR_FLAG_DOTDOT    0x0001 /* send dot names */

int rgw_readdir(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *parent_fh, uint64_t *offset,
		rgw_readdir_cb rcb, void *cb_arg, bool *eof,
		uint32_t flags);

/* enumeration continuing from name */
int rgw_readdir2(struct rgw_fs *rgw_fs,
		 struct rgw_file_handle *parent_fh, const char *name,
		 rgw_readdir_cb rcb, void *cb_arg, bool *eof,
		 uint32_t flags);

/* project offset of dirent name */
#define RGW_DIRENT_OFFSET_FLAG_NONE 0x0000

int rgw_dirent_offset(struct rgw_fs *rgw_fs,
		      struct rgw_file_handle *parent_fh,
		      const char *name, int64_t *offset,
		      uint32_t flags);

/*
   get unix attributes for object
*/
#define RGW_GETATTR_FLAG_NONE      0x0000

int rgw_getattr(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *fh, struct stat *st,
		uint32_t flags);

/*
   set unix attributes for object
*/
#define RGW_SETATTR_FLAG_NONE      0x0000

int rgw_setattr(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *fh, struct stat *st,
		uint32_t mask, uint32_t flags);

/*
   truncate file
*/
#define RGW_TRUNCATE_FLAG_NONE     0x0000

int rgw_truncate(struct rgw_fs *rgw_fs,
		 struct rgw_file_handle *fh, uint64_t size,
		 uint32_t flags);

/*
   open file
*/
#define RGW_OPEN_FLAG_NONE         0x0000
#define RGW_OPEN_FLAG_CREATE       0x0001
#define RGW_OPEN_FLAG_V3           0x0002 /* ops have v3 semantics */
#define RGW_OPEN_FLAG_STATELESS    0x0002 /* alias it */

int rgw_open(struct rgw_fs *rgw_fs, struct rgw_file_handle *parent_fh,
	     uint32_t posix_flags, uint32_t flags);

/*
   close file
*/

#define RGW_CLOSE_FLAG_NONE        0x0000
#define RGW_CLOSE_FLAG_RELE        0x0001
  
int rgw_close(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
	      uint32_t flags);

/*
   read data from file
*/
#define RGW_READ_FLAG_NONE 0x0000

int rgw_read(struct rgw_fs *rgw_fs,
	     struct rgw_file_handle *fh, uint64_t offset,
	     size_t length, size_t *bytes_read, void *buffer,
	     uint32_t flags);

/*
   read symbolic link
*/
#define RGW_READLINK_FLAG_NONE 0x0000

int rgw_readlink(struct rgw_fs *rgw_fs,
	     struct rgw_file_handle *fh, uint64_t offset,
	     size_t length, size_t *bytes_read, void *buffer,
	     uint32_t flags);

/*
   write data to file
*/
#define RGW_WRITE_FLAG_NONE      0x0000

int rgw_write(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, uint64_t offset,
	      size_t length, size_t *bytes_written, void *buffer,
	      uint32_t flags);

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
	      struct rgw_file_handle *fh, rgw_uio *uio, uint32_t flags);

int rgw_writev(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *fh, rgw_uio *uio, uint32_t flags);

/*
   sync written data
*/
#define RGW_FSYNC_FLAG_NONE        0x0000

int rgw_fsync(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
	      uint32_t flags);

/*
   NFS commit operation
*/

#define RGW_COMMIT_FLAG_NONE        0x0000

int rgw_commit(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
	       uint64_t offset, uint64_t length, uint32_t flags);

/*
  extended attributes
 */
typedef struct rgw_xattrstr
{
  char *val;
  uint32_t len;
} rgw_xattrstr;

typedef struct rgw_xattr
{
  rgw_xattrstr key;
  rgw_xattrstr val;
} rgw_xattr;

typedef struct rgw_xattrlist
{
  rgw_xattr *xattrs;
  uint32_t xattr_cnt;
} rgw_xattrlist;

#define RGW_GETXATTR_FLAG_NONE      0x0000

typedef int (*rgw_getxattr_cb)(rgw_xattrlist *attrs, void *arg,
			       uint32_t flags);

int rgw_getxattrs(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		  rgw_xattrlist *attrs, rgw_getxattr_cb cb, void *cb_arg,
		  uint32_t flags);

#define RGW_LSXATTR_FLAG_NONE       0x0000
#define RGW_LSXATTR_FLAG_STOP       0x0001

int rgw_lsxattrs(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		 rgw_xattrstr *filter_prefix /* unimplemented for now */,
		 rgw_getxattr_cb cb, void *cb_arg, uint32_t flags);

#define RGW_SETXATTR_FLAG_NONE      0x0000

int rgw_setxattrs(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		 rgw_xattrlist *attrs, uint32_t flags);

#define RGW_RMXATTR_FLAG_NONE       0x0000

int rgw_rmxattrs(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		 rgw_xattrlist *attrs, uint32_t flags);

#ifdef __cplusplus
}
#endif

#endif /* RADOS_RGW_FILE_H */
