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
#if defined(__linux__)
#include <linux/types.h>
#elif defined(__FreeBSD__)
#include <sys/types.h>
#endif
#include <string.h>
#include "../rados/librados.h"

#define LIBRBD_VER_MAJOR 0
#define LIBRBD_VER_MINOR 1
#define LIBRBD_VER_EXTRA 4

#define LIBRBD_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)

#define LIBRBD_VERSION_CODE LIBRBD_VERSION(LIBRBD_VER_MAJOR, LIBRBD_VER_MINOR, LIBRBD_VER_EXTRA)

#define LIBRBD_SUPPORTS_WATCH 0

typedef void *rbd_snap_t;
typedef void *rbd_image_t;

typedef int (*librbd_progress_fn_t)(uint64_t offset, uint64_t total, void *ptr);

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
  int64_t parent_pool;			      /* deprecated */
  char parent_name[RBD_MAX_IMAGE_NAME_SIZE];  /* deprecated */
} rbd_image_info_t;

void rbd_version(int *major, int *minor, int *extra);

/* images */
int rbd_list(rados_ioctx_t io, char *names, size_t *size);
int rbd_create(rados_ioctx_t io, const char *name, uint64_t size, int *order);
int rbd_create2(rados_ioctx_t io, const char *name, uint64_t size,
		uint64_t features, int *order);
int rbd_clone(rados_ioctx_t p_ioctx, const char *p_name,
	      const char *p_snapname, rados_ioctx_t c_ioctx,
	      const char *c_name, uint64_t features, int *c_order);
int rbd_remove(rados_ioctx_t io, const char *name);
int rbd_remove_with_progress(rados_ioctx_t io, const char *name,
			     librbd_progress_fn_t cb, void *cbdata);
int rbd_rename(rados_ioctx_t src_io_ctx, const char *srcname, const char *destname);

int rbd_open(rados_ioctx_t io, const char *name, rbd_image_t *image, const char *snap_name);
int rbd_close(rbd_image_t image);
int rbd_resize(rbd_image_t image, uint64_t size);
int rbd_resize_with_progress(rbd_image_t image, uint64_t size,
			     librbd_progress_fn_t cb, void *cbdata);
int rbd_stat(rbd_image_t image, rbd_image_info_t *info, size_t infosize);
int rbd_get_old_format(rbd_image_t image, uint8_t *old);
int rbd_get_features(rbd_image_t image, uint64_t *features);
int rbd_get_overlap(rbd_image_t image, uint64_t *overlap);
int rbd_get_parent_info(rbd_image_t image,
			char *parent_poolname, size_t ppoolnamelen,
			char *parent_name, size_t pnamelen,
			char *parent_snapname, size_t psnapnamelen);
int rbd_copy(rbd_image_t image, rados_ioctx_t dest_io_ctx, const char *destname);
int rbd_copy_with_progress(rbd_image_t image, rados_ioctx_t dest_p, const char *destname,
			   librbd_progress_fn_t cb, void *cbdata);

/* snapshots */
int rbd_snap_list(rbd_image_t image, rbd_snap_info_t *snaps, int *max_snaps);
void rbd_snap_list_end(rbd_snap_info_t *snaps);
int rbd_snap_create(rbd_image_t image, const char *snapname);
int rbd_snap_remove(rbd_image_t image, const char *snapname);
int rbd_snap_rollback(rbd_image_t image, const char *snapname);
int rbd_snap_rollback_with_progress(rbd_image_t image, const char *snapname,
				    librbd_progress_fn_t cb, void *cbdata);
int rbd_snap_set(rbd_image_t image, const char *snapname);

/* cooperative locking */
/**
 * in params:
 * @param lockers_and_cookies: array of char* which will be filled in
 * @param max_entries: the size of the lockers_and_cookies array
 * out params:
 * @param exclusive: non-zero if the lock is an exclusive one. Only
 * meaningfull if there are a non-zero number of lockers.
 * @param lockers_and_cookies: alternating the address of the locker with
 * the locker's cookie.
 * @param max_entries: the number of lockers -- double for the number of
 * spaces required.
 */
int rbd_list_lockers(rbd_image_t image, int *exclusive,
                     char **lockers_and_cookies, int *max_entries);
int rbd_lock_exclusive(rbd_image_t image, const char *cookie);
int rbd_lock_shared(rbd_image_t image, const char *cookie);
int rbd_unlock(rbd_image_t image, const char *cookie);
int rbd_break_lock(rbd_image_t image, const char *locker, const char *cookie);


/* I/O */
typedef void *rbd_completion_t;
typedef void (*rbd_callback_t)(rbd_completion_t cb, void *arg);
ssize_t rbd_read(rbd_image_t image, uint64_t ofs, size_t len, char *buf);
int64_t rbd_read_iterate(rbd_image_t image, uint64_t ofs, size_t len,
			 int (*cb)(uint64_t, size_t, const char *, void *), void *arg);
ssize_t rbd_write(rbd_image_t image, uint64_t ofs, size_t len, const char *buf);
int rbd_discard(rbd_image_t image, uint64_t ofs, uint64_t len);
int rbd_aio_write(rbd_image_t image, uint64_t off, size_t len, const char *buf, rbd_completion_t c);
int rbd_aio_read(rbd_image_t image, uint64_t off, size_t len, char *buf, rbd_completion_t c);
int rbd_aio_discard(rbd_image_t image, uint64_t off, uint64_t len, rbd_completion_t c);
int rbd_aio_create_completion(void *cb_arg, rbd_callback_t complete_cb, rbd_completion_t *c);
int rbd_aio_wait_for_complete(rbd_completion_t c);
ssize_t rbd_aio_get_return_value(rbd_completion_t c);
void rbd_aio_release(rbd_completion_t c);
int rbd_flush(rbd_image_t image);

#ifdef __cplusplus
}
#endif

#endif
