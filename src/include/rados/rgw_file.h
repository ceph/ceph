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

#ifdef __cplusplus
extern "C" {
#endif

/*
 * dynamic allocated handle to support nfs handle
 * Currently we support only one instance of librgw.
 * In order to support several instance we will need to include an
 * instance id (16bit)
 */
struct nfs_handle
{
  uint64_t handle;
};

/*
  get entity handle
*/
int rgw_get_handle(const char *uri, struct nfs_handle *handle);

/*
  check handle
*/
int rgw_check_handle(const struct nfs_handle *handle);

  int rgw_mount(const char *uid, const char *key, const char *secret,
		          const struct nfs_handle *handle);

/*
  create a new dirctory
*/
int rgw_create_directory(const struct nfs_handle *parent_handle, const char *name);

/*
  create a new file
*/
  int rgw_create_file(const struct nfs_handle *parent_handle, const char* name);

  int rgw_rename(const struct nfs_handle *parent_handle, const char* old_name, const char* new_name);

/*
  remove file or directory
*/
int rgw_unlink(const struct nfs_handle *parent_handle, const char* path);

/*
    lookup a directory or file
*/
int rgw_lookup(const struct nfs_handle *parent_handle, const char *path, uint64_t *handle);

/*
    read  directory content
*/
int rgw_readdir(const struct nfs_handle *parent_handle, const char *path);

int rgw_set_attributes(const struct nfs_handle *handle);

int rgw_get_attributes(const struct nfs_handle *handle);

int rgw_open(const struct nfs_handle *handle);

int rgw_close(const struct nfs_handle *handle);

int read(const struct nfs_handle *handle);

int write(const struct nfs_handle *handle);

int set_user_permissions(cosnt char *uid);

int get_user_permissions(const char *uid);

int set_dir_permissions(const struct nfs_handle *handle);

int get_dir_permissions(const struct nfs_handle *handle);

int set_file_permissions(const struct nfs_handle *handle);

int get_file_permissions(const struct nfs_handle *handle);

int acl2perm();

int perm2acl();

#ifdef __cplusplus
}
#endif

#endif
