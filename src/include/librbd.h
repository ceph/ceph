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

#define LIBRBD_VER_MAJOR 0
#define LIBRBD_VER_MINOR 1
#define LIBRBD_VER_EXTRA 0

#define LIBRBD_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)

#define LIBRBD_VERSION_CODE LIBRBD_VERSION(LIBRBD_VER_MAJOR, LIBRBD_VER_MINOR, LIBRBD_VER_EXTRA)

#define LIBRBD_SUPPORTS_WATCH 0

typedef void *rbd_snap_t;

typedef struct {
  uint64_t id;
  uint64_t size;
  const char *name;
} rbd_snap_info_t;

typedef struct {
  uint64_t size;
  uint64_t obj_size;
  uint64_t num_objs;
  int order;
} rbd_image_info_t;

/* initialization */
int rbd_initialize(int argc, const char **argv); /* arguments are optional */
void rbd_shutdown();

void librbd_version(int *major, int *minor, int *extra);

/* images */
int rbd_create_image(const char* pool, const char *name, size_t size);
int rbd_remove_image(const char* pool, const char *name);
int rbd_resize_image(const char* pool, const char *name, size_t size);
int rbd_stat_image(const char* pool, const char *name, rbd_image_info_t *info);
size_t rbd_list_images(const char* pool, char **names, size_t max_names);

/* snapshots */
int rbd_create_snap(const char* pool, const char *image,
		    const char *snapname);
int rbd_remove_snap(const char* pool, const char *image,
		    const char *snapname);
int rbd_rollback_snap(const char* pool, const char *image,
		      const char *snapname);
size_t rbd_list_snaps(const char* pool, const char *image,
		      rbd_snap_info_t *snaps, size_t max_snaps);

#ifdef __cplusplus
}
#endif

#endif
