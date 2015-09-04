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
#ifndef RGW_FILE_H
#define RGW_FILE_H

#include <sys/types.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * dynamic allocated handle to support nfs handle
 * Currently we support only one instance of librgw.
 * In order to support several instance we will need to include an
 * instance id (16bit)
 */
struct rgw_file_handle
{
  uint64_t handle;
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
  get entity handle
*/
int rgw_get_handle(const char *uri, struct rgw_file_handle *handle);

/*
  check handle
*/
int rgw_check_handle(const struct rgw_file_handle *handle);

/*
 attach rgw namespace
*/
int rgw_mount(const char *uid, const char *key, const char *secret,
	      struct rgw_file_handle *handle);

/*
  get filesystem attributes
*/
int rgw_statfs(const struct rgw_file_handle *parent_handle,
	       struct rgw_statvfs *vfs_st);


/*
  create file
*/
int rgw_create(const struct rgw_file_handle *parent_handle,
	       const char *name, mode_t mode, struct stat *st,
	       struct rgw_file_handle *handle);

/*
  create a new directory
*/
int rgw_mkdir(const struct rgw_file_handle *parent_handle,
	      const char *name, mode_t mode, struct stat *st,
	      struct rgw_file_handle *handle);

/*
  rename object
*/
int rgw_rename(const struct rgw_file_handle *olddir, const char* old_name,
	       const struct rgw_file_handle *newdir, const char* new_name);

/*
  remove file or directory
*/
int rgw_unlink(const struct rgw_file_handle *parent_handle, const char* path);

/*
  lookup a directory or file
*/
int rgw_lookup(const struct rgw_file_handle *parent_handle, const char *path,
	       struct rgw_file_handle *handle);

/*
  read  directory content
*/
typedef bool (*rgw_readdir_cb)(const char *name, void *arg, uint64_t offset);

int rgw_readdir(const struct rgw_file_handle *parent_handle, uint64_t *offset,
		rgw_readdir_cb rcb, void *cb_arg, int *eof);

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
int rgw_getattr(const struct rgw_file_handle *handle, struct stat *st);

/*
   set unix attributes for object
*/
int rgw_setattr(const struct rgw_file_handle *handle, struct stat *st,
		uint32_t mask);

/*
   truncate file
*/
int rgw_truncate(const struct rgw_file_handle *handle, uint64_t size);

/*
   open file
*/
int rgw_open(const struct rgw_file_handle *handle, uint32_t flags);

/*
   close file
*/
int rgw_close(const struct rgw_file_handle *handle, uint32_t flags);

/*
   read data from file
*/
int rgw_read(const struct rgw_file_handle *handle, uint64_t offset,
	     size_t length, void *buffer);

/*
   write data to file
*/
int rgw_write(const struct rgw_file_handle *handle, uint64_t offset,
	      size_t length, void *buffer);

/*
   sync written data
*/
int rgw_fsync(const struct rgw_file_handle *handle);

int set_user_permissions(const char *uid);

int get_user_permissions(const char *uid);

int set_dir_permissions(const struct rgw_file_handle *handle);

int get_dir_permissions(const struct rgw_file_handle *handle);

int set_file_permissions(const struct rgw_file_handle *handle);

int get_file_permissions(const struct rgw_file_handle *handle);

int rgw_acl2perm();

int rgw_perm2acl();

#ifdef __cplusplus
}
#endif

#endif
