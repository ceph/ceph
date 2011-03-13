// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_LIBRBD_H
#define CEPH_LIBRBD_H

#ifdef __cplusplus
extern "C" {
#endif

#include <netinet/in.h>
#include <linux/types.h>
#include <string.h>
#include "../rados/librados.h"

#define LIBRBD_VER_MAJOR 0
#define LIBRBD_VER_MINOR 1
#define LIBRBD_VER_EXTRA 0

#define LIBRBD_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)

#define LIBRBD_VERSION_CODE LIBRBD_VERSION(LIBRBD_VER_MAJOR, LIBRBD_VER_MINOR, LIBRBD_VER_EXTRA)

#define LIBRBD_SUPPORTS_WATCH 0

typedef void *rbd_snap_t;
typedef void *rbd_image_t;

typedef struct {
  uint64_t id;
  uint64_t size;
  const char *name;
} rbd_snap_info_t;

#define RBD_MAX_IMAGE_NAME_SIZE 96
#define RBD_MAX_BLOCK_NAME_SIZE 24

typedef struct {
  uint64_t size;
  uint64_t obj_size;
  uint64_t num_objs;
  int order;
  char block_name_prefix[RBD_MAX_BLOCK_NAME_SIZE];
  int parent_pool;                            /* -1 if none */
  char parent_name[RBD_MAX_IMAGE_NAME_SIZE];  /* blank if none */
} rbd_image_info_t;

void rbd_version(int *major, int *minor, int *extra);

/* images */
int rbd_list(rados_ioctx_t io, char *names, size_t *size);
int rbd_create(rados_ioctx_t io, const char *name, size_t size, int *order);
int rbd_remove(rados_ioctx_t io, const char *name);
int rbd_copy(rados_ioctx_t src_io_ctx, const char *srcname, rados_ioctx_t dest_io_ctx, const char *destname);
int rbd_rename(rados_ioctx_t src_io_ctx, const char *srcname, const char *destname);

int rbd_open(rados_ioctx_t io, const char *name, rbd_image_t *image, const char *snap_name);
int rbd_close(rbd_image_t image);
int rbd_resize(rbd_image_t image, size_t size);
int rbd_stat(rbd_image_t image, rbd_image_info_t *info, size_t infosize);

/* snapshots */
int rbd_snap_list(rbd_image_t image, rbd_snap_info_t *snaps, int *max_snaps);
void rbd_snap_list_end(rbd_snap_info_t *snaps);
int rbd_snap_create(rbd_image_t image, const char *snapname);
int rbd_snap_remove(rbd_image_t image, const char *snapname);
int rbd_snap_rollback(rbd_image_t image, const char *snapname);
int rbd_snap_set(rbd_image_t image, const char *snapname);

/* I/O */
typedef void *rbd_completion_t;
typedef void (*rbd_callback_t)(rbd_completion_t cb, void *arg);
int rbd_read(rbd_image_t image, uint64_t ofs, size_t len, char *buf);
int rbd_read_iterate(rbd_image_t image, uint64_t ofs, size_t len,
		     int (*cb)(uint64_t, size_t, const char *, void *), void *arg);
int rbd_write(rbd_image_t image, uint64_t ofs, size_t len, const char *buf);
int rbd_aio_write(rbd_image_t image, uint64_t off, size_t len, const char *buf, rbd_completion_t c);
int rbd_aio_read(rbd_image_t image, uint64_t off, size_t len, char *buf, rbd_completion_t c);
int rbd_aio_create_completion(void *cb_arg, rbd_callback_t complete_cb, rbd_completion_t *c);
int rbd_aio_wait_for_complete(rbd_completion_t c);
int rbd_aio_get_return_value(rbd_completion_t c);
void rbd_aio_release(rbd_completion_t c);

#ifdef __cplusplus
}
#endif

#endif
