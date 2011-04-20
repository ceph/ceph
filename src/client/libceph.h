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
#include <sys/types.h>
#include <dirent.h>

struct stat_precise {
  ino_t st_ino;
  dev_t st_dev;
  mode_t st_mode;
  nlink_t st_nlink;
  uid_t st_uid;
  gid_t st_gid;
  dev_t st_rdev;
  off_t st_size;
  blksize_t st_blksize;
  blkcnt_t st_blocks;
  time_t st_atime_sec;
  time_t st_atime_micro;
  time_t st_mtime_sec;
  time_t st_mtime_micro;
  time_t st_ctime_sec;
  time_t st_ctime_micro;
};

#ifdef __cplusplus
extern "C" {
#endif

struct ceph_cluster_t;

const char *ceph_version(int *major, int *minor, int *patch);

/* initialization */
int ceph_create(ceph_cluster_t **cluster, const char * const id);

/* initialization with an existing configuration */
int ceph_create_with_config(ceph_cluster_t **cluster, struct md_config_t *conf);

/* Connect to the cluster */
int ceph_connect(ceph_cluster_t *cluster);

/* Destroy the cluster instance */
void ceph_shutdown(ceph_cluster_t *cluster);

/* Config
 *
 * Functions for manipulating the Ceph configuration at runtime.
 */
int ceph_conf_read_file(ceph_cluster_t *cluster, const char *path);

void ceph_conf_parse_argv(ceph_cluster_t *cluster, int argc, const char **argv);

/* Sets a configuration value from a string.
 * Returns 0 on success, error code otherwise. */
int ceph_conf_set(ceph_cluster_t *cluster, const char *option, const char *value);

/* Returns a configuration value as a string.
 * If len is positive, that is the maximum number of bytes we'll write into the
 * buffer. If len == -1, we'll call malloc() and set *buf.
 * Returns 0 on success, error code otherwise. Returns ENAMETOOLONG if the
 * buffer is too short. */
int ceph_conf_get(ceph_cluster_t *cluster, const char *option, char *buf, size_t len);

int ceph_mount(ceph_cluster_t *cluster, const char *root);
void ceph_umount(ceph_cluster_t *cluster);

int ceph_statfs(ceph_cluster_t *cluster, const char *path, struct statvfs *stbuf);
int ceph_get_local_osd(ceph_cluster_t *cluster);

/* Get the current working directory.
 *
 * The pointer you get back from this function will continue to be valid until
 * the *next* call you make to ceph_getcwd, at which point it will be invalidated.
 */
const char* ceph_getcwd(ceph_cluster_t *cluster);

int ceph_chdir(ceph_cluster_t *cluster, const char *s);

int ceph_opendir(ceph_cluster_t *cluster, const char *name, DIR **dirpp);
int ceph_closedir(ceph_cluster_t *cluster, DIR *dirp);
int ceph_readdir_r(ceph_cluster_t *cluster, DIR *dirp, struct dirent *de);
int ceph_readdirplus_r(ceph_cluster_t *cluster, DIR *dirp, struct dirent *de,
		       struct stat *st, int *stmask);
int ceph_getdents(ceph_cluster_t *cluster, DIR *dirp, char *name, int buflen);
int ceph_getdnames(ceph_cluster_t *cluster, DIR *dirp, char *name, int buflen);
void ceph_rewinddir(ceph_cluster_t *cluster, DIR *dirp);
loff_t ceph_telldir(ceph_cluster_t *cluster, DIR *dirp);
void ceph_seekdir(ceph_cluster_t *cluster, DIR *dirp, loff_t offset);

int ceph_link(ceph_cluster_t *cluster, const char *existing, const char *newname);
int ceph_unlink(ceph_cluster_t *cluster, const char *path);
int ceph_rename(ceph_cluster_t *cluster, const char *from, const char *to);

// dirs
int ceph_mkdir(ceph_cluster_t *cluster, const char *path, mode_t mode);
int ceph_mkdirs(ceph_cluster_t *cluster, const char *path, mode_t mode);
int ceph_rmdir(ceph_cluster_t *cluster, const char *path);

// symlinks
int ceph_readlink(ceph_cluster_t *cluster, const char *path, char *buf, loff_t size);
int ceph_symlink(ceph_cluster_t *cluster, const char *existing, const char *newname);

// inode stuff
int ceph_lstat(ceph_cluster_t *cluster, const char *path, struct stat *stbuf);
int ceph_lstat_precise(ceph_cluster_t *cluster, const char *path, struct stat_precise *stbuf);

int ceph_setattr(ceph_cluster_t *cluster, const char *relpath, struct stat *attr, int mask);
int ceph_setattr_precise (ceph_cluster_t *cluster, const char *relpath,
			  struct stat_precise *stbuf, int mask);
int ceph_chmod(ceph_cluster_t *cluster, const char *path, mode_t mode);
int ceph_chown(ceph_cluster_t *cluster, const char *path, uid_t uid, gid_t gid);
int ceph_utime(ceph_cluster_t *cluster, const char *path, struct utimbuf *buf);
int ceph_truncate(ceph_cluster_t *cluster, const char *path, loff_t size);

// file ops
int ceph_mknod(ceph_cluster_t *cluster, const char *path, mode_t mode, dev_t rdev);
int ceph_open(ceph_cluster_t *cluster, const char *path, int flags, mode_t mode);
int ceph_close(ceph_cluster_t *cluster, int fd);
loff_t ceph_lseek(ceph_cluster_t *cluster, int fd, loff_t offset, int whence);
int ceph_read(ceph_cluster_t *cluster, int fd, char *buf, loff_t size, loff_t offset);
int ceph_write(ceph_cluster_t *cluster, int fd, const char *buf, loff_t size,
	       loff_t offset);
int ceph_ftruncate(ceph_cluster_t *cluster, int fd, loff_t size);
int ceph_fsync(ceph_cluster_t *cluster, int fd, int syncdataonly);
int ceph_fstat(ceph_cluster_t *cluster, int fd, struct stat *stbuf);

int ceph_sync_fs(ceph_cluster_t *cluster);
int ceph_get_file_stripe_unit(ceph_cluster_t *cluster, int fh);
int ceph_get_file_replication(ceph_cluster_t *cluster, const char *path);
int ceph_get_default_preferred_pg(ceph_cluster_t *cluster, int fd);
int ceph_get_file_stripe_address(ceph_cluster_t *cluster, int fd,
				 loff_t offset, char *buf, int buflen);
int ceph_set_default_file_stripe_unit(ceph_cluster_t *cluster, int stripe);
int ceph_set_default_file_stripe_count(ceph_cluster_t *cluster, int count);
int ceph_set_default_object_size(ceph_cluster_t *cluster, int size);
int ceph_set_default_file_replication(ceph_cluster_t *cluster, int replication);
int ceph_set_default_preferred_pg(ceph_cluster_t *cluster, int pg);
int ceph_localize_reads(ceph_cluster_t *cluster, int val);

#ifdef __cplusplus
}
#endif

#endif
