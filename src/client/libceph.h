// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIB_H
#define CEPH_LIB_H

#include <utime.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ceph_mount_t;
struct ceph_dir_result_t;

const char *ceph_version(int *major, int *minor, int *patch);

/* initialization */
int ceph_create(ceph_mount_t **cmount, const char * const id);

/* initialization with an existing configuration */
int ceph_create_with_config(ceph_mount_t **cmount, struct md_config_t *conf);

/* Activate the mount */
int ceph_mount(ceph_mount_t *cmount, const char *root);

/* Destroy the ceph mount instance */
void ceph_shutdown(ceph_mount_t *cmount);

/* Config
 *
 * Functions for manipulating the Ceph configuration at runtime.
 */
int ceph_conf_read_file(ceph_mount_t *cmount, const char *path);

void ceph_conf_parse_argv(ceph_mount_t *cmount, int argc, const char **argv);

/* Sets a configuration value from a string.
 * Returns 0 on success, error code otherwise. */
int ceph_conf_set(ceph_mount_t *cmount, const char *option, const char *value);

/* Returns a configuration value as a string.
 * If len is positive, that is the maximum number of bytes we'll write into the
 * buffer. If len == -1, we'll call malloc() and set *buf.
 * Returns 0 on success, error code otherwise. Returns ENAMETOOLONG if the
 * buffer is too short. */
int ceph_conf_get(ceph_mount_t *cmount, const char *option, char *buf, size_t len);

int ceph_statfs(ceph_mount_t *cmount, const char *path, struct statvfs *stbuf);

/* Get the current working directory.
 *
 * The pointer you get back from this function will continue to be valid until
 * the *next* call you make to ceph_getcwd, at which point it will be invalidated.
 */
const char* ceph_getcwd(ceph_mount_t *cmount);

int ceph_chdir(ceph_mount_t *cmount, const char *s);

int ceph_opendir(ceph_mount_t *cmount, const char *name, ceph_dir_result_t **dirpp);
int ceph_closedir(ceph_mount_t *cmount, ceph_dir_result_t *dirp);
int ceph_readdir_r(ceph_mount_t *cmount, ceph_dir_result_t *dirp, struct dirent *de);
int ceph_readdirplus_r(ceph_mount_t *cmount, ceph_dir_result_t *dirp, struct dirent *de,
		       struct stat *st, int *stmask);
int ceph_getdents(ceph_mount_t *cmount, ceph_dir_result_t *dirp, char *name, int buflen);
int ceph_getdnames(ceph_mount_t *cmount, ceph_dir_result_t *dirp, char *name, int buflen);
void ceph_rewinddir(ceph_mount_t *cmount, ceph_dir_result_t *dirp);
loff_t ceph_telldir(ceph_mount_t *cmount, ceph_dir_result_t *dirp);
void ceph_seekdir(ceph_mount_t *cmount, ceph_dir_result_t *dirp, loff_t offset);

int ceph_link(ceph_mount_t *cmount, const char *existing, const char *newname);
int ceph_unlink(ceph_mount_t *cmount, const char *path);
int ceph_rename(ceph_mount_t *cmount, const char *from, const char *to);

/* dirs */
int ceph_mkdir(ceph_mount_t *cmount, const char *path, mode_t mode);
int ceph_mkdirs(ceph_mount_t *cmount, const char *path, mode_t mode);
int ceph_rmdir(ceph_mount_t *cmount, const char *path);

/* symlinks */
int ceph_readlink(ceph_mount_t *cmount, const char *path, char *buf, loff_t size);
int ceph_symlink(ceph_mount_t *cmount, const char *existing, const char *newname);

/* inode stuff */
int ceph_lstat(ceph_mount_t *cmount, const char *path, struct stat *stbuf);

int ceph_setattr(ceph_mount_t *cmount, const char *relpath, struct stat *attr, int mask);
int ceph_chmod(ceph_mount_t *cmount, const char *path, mode_t mode);
int ceph_chown(ceph_mount_t *cmount, const char *path, uid_t uid, gid_t gid);
int ceph_utime(ceph_mount_t *cmount, const char *path, struct utimbuf *buf);
int ceph_truncate(ceph_mount_t *cmount, const char *path, loff_t size);

/* file ops */
int ceph_mknod(ceph_mount_t *cmount, const char *path, mode_t mode, dev_t rdev);
int ceph_open(ceph_mount_t *cmount, const char *path, int flags, mode_t mode);
int ceph_close(ceph_mount_t *cmount, int fd);
loff_t ceph_lseek(ceph_mount_t *cmount, int fd, loff_t offset, int whence);
int ceph_read(ceph_mount_t *cmount, int fd, char *buf, loff_t size, loff_t offset);
int ceph_write(ceph_mount_t *cmount, int fd, const char *buf, loff_t size,
	       loff_t offset);
int ceph_ftruncate(ceph_mount_t *cmount, int fd, loff_t size);
int ceph_fsync(ceph_mount_t *cmount, int fd, int syncdataonly);
int ceph_fstat(ceph_mount_t *cmount, int fd, struct stat *stbuf);

int ceph_sync_fs(ceph_mount_t *cmount);


/* expose file layout */
int ceph_get_file_stripe_unit(ceph_mount_t *cmount, int fh);
int ceph_get_file_pool(ceph_mount_t *cmount, int fh);
int ceph_get_file_replication(ceph_mount_t *cmount, int fh);
int ceph_get_file_stripe_address(ceph_mount_t *cmount, int fd,
				 loff_t offset, char *buf, int buflen);

/* set default layout for new files */
int ceph_set_default_file_stripe_unit(ceph_mount_t *cmount, int stripe);
int ceph_set_default_file_stripe_count(ceph_mount_t *cmount, int count);
int ceph_set_default_object_size(ceph_mount_t *cmount, int size);
int ceph_set_default_preferred_pg(ceph_mount_t *cmount, int osd);
int ceph_set_default_file_replication(ceph_mount_t *cmount, int replication);

/* read from local replicas when possible */
int ceph_localize_reads(ceph_mount_t *cmount, int val);

/* return osd on local node, if any */
int ceph_get_local_osd(ceph_mount_t *cmount);

#ifdef __cplusplus
}
#endif

#endif
