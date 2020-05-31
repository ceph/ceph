// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
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
#include <stdbool.h>
#include <string.h>
#include <sys/uio.h>
#include "../rados/librados.h"
#include "features.h"

#define LIBRBD_VER_MAJOR 1
#define LIBRBD_VER_MINOR 15
#define LIBRBD_VER_EXTRA 0

#define LIBRBD_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)

#define LIBRBD_VERSION_CODE LIBRBD_VERSION(LIBRBD_VER_MAJOR, LIBRBD_VER_MINOR, LIBRBD_VER_EXTRA)

#define LIBRBD_SUPPORTS_AIO_FLUSH 1
#define LIBRBD_SUPPORTS_AIO_OPEN 1
#define LIBRBD_SUPPORTS_COMPARE_AND_WRITE 1
#define LIBRBD_SUPPORTS_LOCKING 1
#define LIBRBD_SUPPORTS_INVALIDATE 1
#define LIBRBD_SUPPORTS_IOVEC 1
#define LIBRBD_SUPPORTS_WATCH 0
#define LIBRBD_SUPPORTS_WRITESAME 1

#if __GNUC__ >= 4
  #define CEPH_RBD_API          __attribute__ ((visibility ("default")))
  #define CEPH_RBD_DEPRECATED   __attribute__((deprecated))
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#else
  #define CEPH_RBD_API
  #define CEPH_RBD_DEPRECATED
#endif

#define RBD_FLAG_OBJECT_MAP_INVALID   (1<<0)
#define RBD_FLAG_FAST_DIFF_INVALID    (1<<1)

#define RBD_MIRROR_IMAGE_STATUS_LOCAL_MIRROR_UUID ""

typedef void *rbd_image_t;
typedef void *rbd_image_options_t;
typedef void *rbd_pool_stats_t;

typedef void *rbd_completion_t;
typedef void (*rbd_callback_t)(rbd_completion_t cb, void *arg);

typedef int (*librbd_progress_fn_t)(uint64_t offset, uint64_t total, void *ptr);

typedef void (*rbd_update_callback_t)(void *arg);

typedef enum {
  RBD_SNAP_NAMESPACE_TYPE_USER   = 0,
  RBD_SNAP_NAMESPACE_TYPE_GROUP  = 1,
  RBD_SNAP_NAMESPACE_TYPE_TRASH  = 2,
  RBD_SNAP_NAMESPACE_TYPE_MIRROR = 3,
} rbd_snap_namespace_type_t;

typedef struct {
  char *id;
  char *name;
} rbd_image_spec_t;

typedef struct {
  int64_t pool_id;
  char *pool_name;
  char *pool_namespace;
  char *image_id;
  char *image_name;
  bool trash;
} rbd_linked_image_spec_t;

typedef struct {
  uint64_t id;
  rbd_snap_namespace_type_t namespace_type;
  char *name;
} rbd_snap_spec_t;

typedef struct {
  uint64_t id;
  uint64_t size;
  const char *name;
} rbd_snap_info_t;

typedef struct {
  const char *pool_name;
  const char *image_name;
  const char *image_id;
  bool trash;
} rbd_child_info_t;

#define RBD_MAX_IMAGE_NAME_SIZE 96
#define RBD_MAX_BLOCK_NAME_SIZE 24

#define RBD_SNAP_CREATE_SKIP_QUIESCE		1 << 0
#define RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR	1 << 1

#define RBD_SNAP_REMOVE_UNPROTECT	1 << 0
#define RBD_SNAP_REMOVE_FLATTEN		1 << 1
#define RBD_SNAP_REMOVE_FORCE		(RBD_SNAP_REMOVE_UNPROTECT | RBD_SNAP_REMOVE_FLATTEN)

/**
 * These types used to in set_image_notification to indicate the type of event
 * socket passed in.
 */
enum {
  EVENT_TYPE_PIPE = 1,
  EVENT_TYPE_EVENTFD = 2
};

typedef struct {
  uint64_t size;
  uint64_t obj_size;
  uint64_t num_objs;
  int order;
  char block_name_prefix[RBD_MAX_BLOCK_NAME_SIZE]; /* deprecated */
  int64_t parent_pool;                             /* deprecated */
  char parent_name[RBD_MAX_IMAGE_NAME_SIZE];       /* deprecated */
} rbd_image_info_t;

typedef enum {
  RBD_MIRROR_MODE_DISABLED, /* mirroring is disabled */
  RBD_MIRROR_MODE_IMAGE,    /* mirroring enabled on a per-image basis */
  RBD_MIRROR_MODE_POOL      /* mirroring enabled on all journaled images */
} rbd_mirror_mode_t;

typedef enum {
  RBD_MIRROR_PEER_DIRECTION_RX    = 0,
  RBD_MIRROR_PEER_DIRECTION_TX    = 1,
  RBD_MIRROR_PEER_DIRECTION_RX_TX = 2
} rbd_mirror_peer_direction_t;

typedef struct {
  char *uuid;
  char *cluster_name;
  char *client_name;
} rbd_mirror_peer_t CEPH_RBD_DEPRECATED;

typedef struct {
  char *uuid;
  rbd_mirror_peer_direction_t direction;
  char *site_name;
  char *mirror_uuid;
  char *client_name;
  time_t last_seen;
} rbd_mirror_peer_site_t;

#define RBD_MIRROR_PEER_ATTRIBUTE_NAME_MON_HOST "mon_host"
#define RBD_MIRROR_PEER_ATTRIBUTE_NAME_KEY      "key"

typedef enum {
  RBD_MIRROR_IMAGE_MODE_JOURNAL  = 0,
  RBD_MIRROR_IMAGE_MODE_SNAPSHOT = 1,
} rbd_mirror_image_mode_t;

typedef enum {
  RBD_MIRROR_IMAGE_DISABLING = 0,
  RBD_MIRROR_IMAGE_ENABLED = 1,
  RBD_MIRROR_IMAGE_DISABLED = 2
} rbd_mirror_image_state_t;

typedef struct {
  char *global_id;
  rbd_mirror_image_state_t state;
  bool primary;
} rbd_mirror_image_info_t;

typedef enum {
  MIRROR_IMAGE_STATUS_STATE_UNKNOWN         = 0,
  MIRROR_IMAGE_STATUS_STATE_ERROR           = 1,
  MIRROR_IMAGE_STATUS_STATE_SYNCING         = 2,
  MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY = 3,
  MIRROR_IMAGE_STATUS_STATE_REPLAYING       = 4,
  MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY = 5,
  MIRROR_IMAGE_STATUS_STATE_STOPPED         = 6,
} rbd_mirror_image_status_state_t;

typedef struct {
  char *name;
  rbd_mirror_image_info_t info;
  rbd_mirror_image_status_state_t state;
  char *description;
  time_t last_update;
  bool up;
} rbd_mirror_image_status_t CEPH_RBD_DEPRECATED;

typedef struct {
  char *mirror_uuid;
  rbd_mirror_image_status_state_t state;
  char *description;
  time_t last_update;
  bool up;
} rbd_mirror_image_site_status_t;

typedef struct {
  char *name;
  rbd_mirror_image_info_t info;
  uint32_t site_statuses_count;
  rbd_mirror_image_site_status_t *site_statuses;
} rbd_mirror_image_global_status_t;

typedef enum {
  RBD_GROUP_IMAGE_STATE_ATTACHED,
  RBD_GROUP_IMAGE_STATE_INCOMPLETE
} rbd_group_image_state_t;

typedef struct {
  char *name;
  int64_t pool;
  rbd_group_image_state_t state;
} rbd_group_image_info_t;

typedef struct {
  char *name;
  int64_t pool;
} rbd_group_info_t;

typedef enum {
  RBD_GROUP_SNAP_STATE_INCOMPLETE,
  RBD_GROUP_SNAP_STATE_COMPLETE
} rbd_group_snap_state_t;

typedef struct {
  char *name;
  rbd_group_snap_state_t state;
} rbd_group_snap_info_t;

typedef struct {
  int64_t group_pool;
  char *group_name;
  char *group_snap_name;
} rbd_snap_group_namespace_t;

typedef enum {
  RBD_SNAP_MIRROR_STATE_PRIMARY,
  RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED,
  RBD_SNAP_MIRROR_STATE_NON_PRIMARY,
  RBD_SNAP_MIRROR_STATE_NON_PRIMARY_DEMOTED
} rbd_snap_mirror_state_t;

typedef struct {
  rbd_snap_mirror_state_t state;
  size_t mirror_peer_uuids_count;
  char *mirror_peer_uuids;
  bool complete;
  char *primary_mirror_uuid;
  uint64_t primary_snap_id;
  uint64_t last_copied_object_number;
} rbd_snap_mirror_namespace_t;

typedef enum {
  RBD_LOCK_MODE_EXCLUSIVE = 0,
  RBD_LOCK_MODE_SHARED = 1,
} rbd_lock_mode_t;

CEPH_RBD_API void rbd_version(int *major, int *minor, int *extra);

/* image options */
enum {
  RBD_IMAGE_OPTION_FORMAT = 0,
  RBD_IMAGE_OPTION_FEATURES = 1,
  RBD_IMAGE_OPTION_ORDER = 2,
  RBD_IMAGE_OPTION_STRIPE_UNIT = 3,
  RBD_IMAGE_OPTION_STRIPE_COUNT = 4,
  RBD_IMAGE_OPTION_JOURNAL_ORDER = 5,
  RBD_IMAGE_OPTION_JOURNAL_SPLAY_WIDTH = 6,
  RBD_IMAGE_OPTION_JOURNAL_POOL = 7,
  RBD_IMAGE_OPTION_FEATURES_SET = 8,
  RBD_IMAGE_OPTION_FEATURES_CLEAR = 9,
  RBD_IMAGE_OPTION_DATA_POOL = 10,
  RBD_IMAGE_OPTION_FLATTEN = 11,
  RBD_IMAGE_OPTION_CLONE_FORMAT = 12,
  RBD_IMAGE_OPTION_MIRROR_IMAGE_MODE = 13,
};

typedef enum {
  RBD_TRASH_IMAGE_SOURCE_USER = 0,
  RBD_TRASH_IMAGE_SOURCE_MIRRORING = 1,
  RBD_TRASH_IMAGE_SOURCE_MIGRATION = 2,
  RBD_TRASH_IMAGE_SOURCE_REMOVING = 3,
  RBD_TRASH_IMAGE_SOURCE_USER_PARENT = 4,
} rbd_trash_image_source_t;

typedef struct {
  char *id;
  char *name;
  rbd_trash_image_source_t source;
  time_t deletion_time;
  time_t deferment_end_time;
} rbd_trash_image_info_t;

typedef struct {
  char *addr;
  int64_t id;
  uint64_t cookie;
} rbd_image_watcher_t;

typedef enum {
  RBD_IMAGE_MIGRATION_STATE_UNKNOWN = -1,
  RBD_IMAGE_MIGRATION_STATE_ERROR = 0,
  RBD_IMAGE_MIGRATION_STATE_PREPARING = 1,
  RBD_IMAGE_MIGRATION_STATE_PREPARED = 2,
  RBD_IMAGE_MIGRATION_STATE_EXECUTING = 3,
  RBD_IMAGE_MIGRATION_STATE_EXECUTED = 4,
} rbd_image_migration_state_t;

typedef struct {
  int64_t source_pool_id;
  char *source_pool_namespace;
  char *source_image_name;
  char *source_image_id;
  int64_t dest_pool_id;
  char *dest_pool_namespace;
  char *dest_image_name;
  char *dest_image_id;
  rbd_image_migration_state_t state;
  char *state_description;
} rbd_image_migration_status_t;

typedef enum {
  RBD_CONFIG_SOURCE_CONFIG = 0,
  RBD_CONFIG_SOURCE_POOL = 1,
  RBD_CONFIG_SOURCE_IMAGE = 2,
} rbd_config_source_t;

typedef struct {
  char *name;
  char *value;
  rbd_config_source_t source;
} rbd_config_option_t;

typedef enum {
  RBD_POOL_STAT_OPTION_IMAGES,
  RBD_POOL_STAT_OPTION_IMAGE_PROVISIONED_BYTES,
  RBD_POOL_STAT_OPTION_IMAGE_MAX_PROVISIONED_BYTES,
  RBD_POOL_STAT_OPTION_IMAGE_SNAPSHOTS,
  RBD_POOL_STAT_OPTION_TRASH_IMAGES,
  RBD_POOL_STAT_OPTION_TRASH_PROVISIONED_BYTES,
  RBD_POOL_STAT_OPTION_TRASH_MAX_PROVISIONED_BYTES,
  RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS
} rbd_pool_stat_option_t;

CEPH_RBD_API void rbd_image_options_create(rbd_image_options_t* opts);
CEPH_RBD_API void rbd_image_options_destroy(rbd_image_options_t opts);
CEPH_RBD_API int rbd_image_options_set_string(rbd_image_options_t opts,
					      int optname, const char* optval);
CEPH_RBD_API int rbd_image_options_set_uint64(rbd_image_options_t opts,
					      int optname, uint64_t optval);
CEPH_RBD_API int rbd_image_options_get_string(rbd_image_options_t opts,
					      int optname, char* optval,
					      size_t maxlen);
CEPH_RBD_API int rbd_image_options_get_uint64(rbd_image_options_t opts,
					      int optname, uint64_t* optval);
CEPH_RBD_API int rbd_image_options_is_set(rbd_image_options_t opts,
                                          int optname, bool* is_set);
CEPH_RBD_API int rbd_image_options_unset(rbd_image_options_t opts, int optname);
CEPH_RBD_API void rbd_image_options_clear(rbd_image_options_t opts);
CEPH_RBD_API int rbd_image_options_is_empty(rbd_image_options_t opts);

/* helpers */
CEPH_RBD_API void rbd_image_spec_cleanup(rbd_image_spec_t *image);
CEPH_RBD_API void rbd_image_spec_list_cleanup(rbd_image_spec_t *images,
                                              size_t num_images);
CEPH_RBD_API void rbd_linked_image_spec_cleanup(rbd_linked_image_spec_t *image);
CEPH_RBD_API void rbd_linked_image_spec_list_cleanup(
    rbd_linked_image_spec_t *images, size_t num_images);
CEPH_RBD_API void rbd_snap_spec_cleanup(rbd_snap_spec_t *snap);

/* images */
CEPH_RBD_API int rbd_list(rados_ioctx_t io, char *names, size_t *size)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_list2(rados_ioctx_t io, rbd_image_spec_t* images,
                           size_t *max_images);

CEPH_RBD_API int rbd_create(rados_ioctx_t io, const char *name, uint64_t size,
                            int *order);
CEPH_RBD_API int rbd_create2(rados_ioctx_t io, const char *name, uint64_t size,
		             uint64_t features, int *order);
/**
 * create new rbd image
 *
 * The stripe_unit must be a factor of the object size (1 << order).
 * The stripe_count can be one (no intra-object striping) or greater
 * than one.  The RBD_FEATURE_STRIPINGV2 must be specified if the
 * stripe_unit != the object size and the stripe_count is != 1.
 *
 * @param io ioctx
 * @param name image name
 * @param size image size in bytes
 * @param features initial feature bits
 * @param order object/block size, as a power of two (object size == 1 << order)
 * @param stripe_unit stripe unit size, in bytes.
 * @param stripe_count number of objects to stripe over before looping
 * @return 0 on success, or negative error code
 */
CEPH_RBD_API int rbd_create3(rados_ioctx_t io, const char *name, uint64_t size,
		             uint64_t features, int *order,
		             uint64_t stripe_unit, uint64_t stripe_count);
CEPH_RBD_API int rbd_create4(rados_ioctx_t io, const char *name, uint64_t size,
			     rbd_image_options_t opts);
CEPH_RBD_API int rbd_clone(rados_ioctx_t p_ioctx, const char *p_name,
	                   const char *p_snapname, rados_ioctx_t c_ioctx,
	                   const char *c_name, uint64_t features, int *c_order);
CEPH_RBD_API int rbd_clone2(rados_ioctx_t p_ioctx, const char *p_name,
	                    const char *p_snapname, rados_ioctx_t c_ioctx,
	                    const char *c_name, uint64_t features, int *c_order,
	                    uint64_t stripe_unit, int stripe_count);
CEPH_RBD_API int rbd_clone3(rados_ioctx_t p_ioctx, const char *p_name,
	                    const char *p_snapname, rados_ioctx_t c_ioctx,
	                    const char *c_name, rbd_image_options_t c_opts);
CEPH_RBD_API int rbd_remove(rados_ioctx_t io, const char *name);
CEPH_RBD_API int rbd_remove_with_progress(rados_ioctx_t io, const char *name,
			                  librbd_progress_fn_t cb,
                                          void *cbdata);
CEPH_RBD_API int rbd_rename(rados_ioctx_t src_io_ctx, const char *srcname,
                            const char *destname);

CEPH_RBD_API int rbd_trash_move(rados_ioctx_t io, const char *name,
                                uint64_t delay);
CEPH_RBD_API int rbd_trash_get(rados_ioctx_t io, const char *id,
                               rbd_trash_image_info_t *info);
CEPH_RBD_API void rbd_trash_get_cleanup(rbd_trash_image_info_t *info);
CEPH_RBD_API int rbd_trash_list(rados_ioctx_t io,
                                rbd_trash_image_info_t *trash_entries,
                                size_t *num_entries);
CEPH_RBD_API void rbd_trash_list_cleanup(rbd_trash_image_info_t *trash_entries,
                                         size_t num_entries);
CEPH_RBD_API int rbd_trash_purge(rados_ioctx_t io, time_t expire_ts, float threshold);
CEPH_RBD_API int rbd_trash_purge_with_progress(rados_ioctx_t io, time_t expire_ts,
                                               float threshold, librbd_progress_fn_t cb,
                                               void* cbdata);
CEPH_RBD_API int rbd_trash_remove(rados_ioctx_t io, const char *id, bool force);
CEPH_RBD_API int rbd_trash_remove_with_progress(rados_ioctx_t io,
                                                const char *id,
                                                bool force,
                                                librbd_progress_fn_t cb,
                                                void *cbdata);
CEPH_RBD_API int rbd_trash_restore(rados_ioctx_t io, const char *id,
                                   const char *name);

/* migration */
CEPH_RBD_API int rbd_migration_prepare(rados_ioctx_t ioctx,
                                       const char *image_name,
                                       rados_ioctx_t dest_ioctx,
                                       const char *dest_image_name,
                                       rbd_image_options_t opts);
CEPH_RBD_API int rbd_migration_execute(rados_ioctx_t ioctx,
                                       const char *image_name);
CEPH_RBD_API int rbd_migration_execute_with_progress(rados_ioctx_t ioctx,
                                                     const char *image_name,
                                                     librbd_progress_fn_t cb,
                                                     void *cbdata);
CEPH_RBD_API int rbd_migration_abort(rados_ioctx_t ioctx,
                                     const char *image_name);
CEPH_RBD_API int rbd_migration_abort_with_progress(rados_ioctx_t ioctx,
                                                   const char *image_name,
                                                   librbd_progress_fn_t cb,
                                                   void *cbdata);
CEPH_RBD_API int rbd_migration_commit(rados_ioctx_t ioctx,
                                      const char *image_name);
CEPH_RBD_API int rbd_migration_commit_with_progress(rados_ioctx_t ioctx,
                                                    const char *image_name,
                                                    librbd_progress_fn_t cb,
                                                    void *cbdata);
CEPH_RBD_API int rbd_migration_status(rados_ioctx_t ioctx,
                                      const char *image_name,
                                      rbd_image_migration_status_t *status,
                                      size_t status_size);
CEPH_RBD_API void rbd_migration_status_cleanup(
    rbd_image_migration_status_t *status);

/* pool mirroring */
CEPH_RBD_API int rbd_mirror_site_name_get(rados_t cluster,
                                          char *name, size_t *max_len);
CEPH_RBD_API int rbd_mirror_site_name_set(rados_t cluster,
                                          const char *name);

CEPH_RBD_API int rbd_mirror_mode_get(rados_ioctx_t io_ctx,
                                     rbd_mirror_mode_t *mirror_mode);
CEPH_RBD_API int rbd_mirror_mode_set(rados_ioctx_t io_ctx,
                                     rbd_mirror_mode_t mirror_mode);

CEPH_RBD_API int rbd_mirror_uuid_get(rados_ioctx_t io_ctx,
                                     char *uuid, size_t *max_len);

CEPH_RBD_API int rbd_mirror_peer_bootstrap_create(
    rados_ioctx_t io_ctx, char *token, size_t *max_len);
CEPH_RBD_API int rbd_mirror_peer_bootstrap_import(
    rados_ioctx_t io_ctx, rbd_mirror_peer_direction_t direction,
    const char *token);

CEPH_RBD_API int rbd_mirror_peer_site_add(
    rados_ioctx_t io_ctx, char *uuid, size_t uuid_max_length,
    rbd_mirror_peer_direction_t direction, const char *site_name,
    const char *client_name);
CEPH_RBD_API int rbd_mirror_peer_site_set_name(
    rados_ioctx_t io_ctx, const char *uuid, const char *site_name);
CEPH_RBD_API int rbd_mirror_peer_site_set_client_name(
    rados_ioctx_t io_ctx, const char *uuid, const char *client_name);
CEPH_RBD_API int rbd_mirror_peer_site_set_direction(
    rados_ioctx_t io_ctx, const char *uuid,
    rbd_mirror_peer_direction_t direction);
CEPH_RBD_API int rbd_mirror_peer_site_remove(
    rados_ioctx_t io_ctx, const char *uuid);
CEPH_RBD_API int rbd_mirror_peer_site_list(
    rados_ioctx_t io_ctx, rbd_mirror_peer_site_t *peers, int *max_peers);
CEPH_RBD_API void rbd_mirror_peer_site_list_cleanup(
    rbd_mirror_peer_site_t *peers, int max_peers);
CEPH_RBD_API int rbd_mirror_peer_site_get_attributes(
    rados_ioctx_t p, const char *uuid, char *keys, size_t *max_key_len,
    char *values, size_t *max_value_len, size_t *key_value_count);
CEPH_RBD_API int rbd_mirror_peer_site_set_attributes(
    rados_ioctx_t p, const char *uuid, const char *keys, const char *values,
    size_t key_value_count);

CEPH_RBD_API int rbd_mirror_image_global_status_list(
    rados_ioctx_t io_ctx, const char *start_id, size_t max, char **image_ids,
    rbd_mirror_image_global_status_t *images, size_t *len);
CEPH_RBD_API void rbd_mirror_image_global_status_list_cleanup(
    char **image_ids, rbd_mirror_image_global_status_t *images, size_t len);

/* rbd_mirror_peer_ commands are deprecated to rbd_mirror_peer_site_
 * equivalents */
CEPH_RBD_API int rbd_mirror_peer_add(
    rados_ioctx_t io_ctx, char *uuid, size_t uuid_max_length,
    const char *cluster_name, const char *client_name)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_mirror_peer_remove(
    rados_ioctx_t io_ctx, const char *uuid)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_mirror_peer_list(
    rados_ioctx_t io_ctx, rbd_mirror_peer_t *peers, int *max_peers)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API void rbd_mirror_peer_list_cleanup(
    rbd_mirror_peer_t *peers, int max_peers)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_mirror_peer_set_client(
    rados_ioctx_t io_ctx, const char *uuid, const char *client_name)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_mirror_peer_set_cluster(
    rados_ioctx_t io_ctx, const char *uuid, const char *cluster_name)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_mirror_peer_get_attributes(
    rados_ioctx_t p, const char *uuid, char *keys, size_t *max_key_len,
    char *values, size_t *max_value_len, size_t *key_value_count)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_mirror_peer_set_attributes(
    rados_ioctx_t p, const char *uuid, const char *keys, const char *values,
    size_t key_value_count)
  CEPH_RBD_DEPRECATED;

/* rbd_mirror_image_status_list_ commands are deprecard to
 * rbd_mirror_image_global_status_list_ commands */

CEPH_RBD_API int rbd_mirror_image_status_list(
    rados_ioctx_t io_ctx, const char *start_id, size_t max, char **image_ids,
    rbd_mirror_image_status_t *images, size_t *len)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API void rbd_mirror_image_status_list_cleanup(
    char **image_ids, rbd_mirror_image_status_t *images, size_t len)
  CEPH_RBD_DEPRECATED;

CEPH_RBD_API int rbd_mirror_image_status_summary(
    rados_ioctx_t io_ctx, rbd_mirror_image_status_state_t *states, int *counts,
    size_t *maxlen);

CEPH_RBD_API int rbd_mirror_image_instance_id_list(rados_ioctx_t io_ctx,
                                                   const char *start_id,
                                                   size_t max, char **image_ids,
                                                   char **instance_ids,
                                                   size_t *len);
CEPH_RBD_API void rbd_mirror_image_instance_id_list_cleanup(char **image_ids,
                                                            char **instance_ids,
                                                            size_t len);
CEPH_RBD_API int rbd_mirror_image_info_list(
    rados_ioctx_t io_ctx, rbd_mirror_image_mode_t *mode_filter,
    const char *start_id, size_t max, char **image_ids,
    rbd_mirror_image_mode_t *mode_entries,
    rbd_mirror_image_info_t *info_entries, size_t *num_entries);
CEPH_RBD_API void rbd_mirror_image_info_list_cleanup(
    char **image_ids, rbd_mirror_image_info_t *info_entries,
    size_t num_entries);

/* pool metadata */
CEPH_RBD_API int rbd_pool_metadata_get(rados_ioctx_t io_ctx, const char *key,
                                       char *value, size_t *val_len);
CEPH_RBD_API int rbd_pool_metadata_set(rados_ioctx_t io_ctx, const char *key,
                                       const char *value);
CEPH_RBD_API int rbd_pool_metadata_remove(rados_ioctx_t io_ctx,
                                          const char *key);
CEPH_RBD_API int rbd_pool_metadata_list(rados_ioctx_t io_ctx, const char *start,
                                        uint64_t max, char *keys,
                                        size_t *key_len, char *values,
                                        size_t *vals_len);

CEPH_RBD_API int rbd_config_pool_list(rados_ioctx_t io_ctx,
                                      rbd_config_option_t *options,
                                      int *max_options);
CEPH_RBD_API void rbd_config_pool_list_cleanup(rbd_config_option_t *options,
                                               int max_options);

CEPH_RBD_API int rbd_open(rados_ioctx_t io, const char *name,
                          rbd_image_t *image, const char *snap_name);
CEPH_RBD_API int rbd_open_by_id(rados_ioctx_t io, const char *id,
                                rbd_image_t *image, const char *snap_name);

CEPH_RBD_API int rbd_aio_open(rados_ioctx_t io, const char *name,
			      rbd_image_t *image, const char *snap_name,
			      rbd_completion_t c);
CEPH_RBD_API int rbd_aio_open_by_id(rados_ioctx_t io, const char *id,
                                    rbd_image_t *image, const char *snap_name,
                                    rbd_completion_t c);

/**
 * Open an image in read-only mode.
 *
 * This is intended for use by clients that cannot write to a block
 * device due to cephx restrictions. There will be no watch
 * established on the header object, since a watch is a write. This
 * means the metadata reported about this image (parents, snapshots,
 * size, etc.) may become stale. This should not be used for
 * long-running operations, unless you can be sure that one of these
 * properties changing is safe.
 *
 * Attempting to write to a read-only image will return -EROFS.
 *
 * @param io ioctx to determine the pool the image is in
 * @param name image name
 * @param image where to store newly opened image handle
 * @param snap_name name of snapshot to open at, or NULL for no snapshot
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_open_read_only(rados_ioctx_t io, const char *name,
                                    rbd_image_t *image, const char *snap_name);
CEPH_RBD_API int rbd_open_by_id_read_only(rados_ioctx_t io, const char *id,
                                          rbd_image_t *image, const char *snap_name);
CEPH_RBD_API int rbd_aio_open_read_only(rados_ioctx_t io, const char *name,
					rbd_image_t *image, const char *snap_name,
					rbd_completion_t c);
CEPH_RBD_API int rbd_aio_open_by_id_read_only(rados_ioctx_t io, const char *id,
                                              rbd_image_t *image, const char *snap_name,
                                              rbd_completion_t c);
CEPH_RBD_API int rbd_features_to_string(uint64_t features, char *str_features,
                                        size_t *size);
CEPH_RBD_API int rbd_features_from_string(const char *str_features, uint64_t *features);
CEPH_RBD_API int rbd_close(rbd_image_t image);
CEPH_RBD_API int rbd_aio_close(rbd_image_t image, rbd_completion_t c);
CEPH_RBD_API int rbd_resize(rbd_image_t image, uint64_t size);
CEPH_RBD_API int rbd_resize2(rbd_image_t image, uint64_t size, bool allow_shrink,
			     librbd_progress_fn_t cb, void *cbdata);
CEPH_RBD_API int rbd_resize_with_progress(rbd_image_t image, uint64_t size,
			     librbd_progress_fn_t cb, void *cbdata);
CEPH_RBD_API int rbd_stat(rbd_image_t image, rbd_image_info_t *info,
                          size_t infosize);
CEPH_RBD_API int rbd_get_old_format(rbd_image_t image, uint8_t *old);
CEPH_RBD_API int rbd_get_size(rbd_image_t image, uint64_t *size);
CEPH_RBD_API int rbd_get_features(rbd_image_t image, uint64_t *features);
CEPH_RBD_API int rbd_update_features(rbd_image_t image, uint64_t features,
                                     uint8_t enabled);
CEPH_RBD_API int rbd_get_op_features(rbd_image_t image, uint64_t *op_features);
CEPH_RBD_API int rbd_get_stripe_unit(rbd_image_t image, uint64_t *stripe_unit);
CEPH_RBD_API int rbd_get_stripe_count(rbd_image_t image,
                                      uint64_t *stripe_count);

CEPH_RBD_API int rbd_get_create_timestamp(rbd_image_t image,
                                          struct timespec *timestamp);
CEPH_RBD_API int rbd_get_access_timestamp(rbd_image_t image,
                                          struct timespec *timestamp);
CEPH_RBD_API int rbd_get_modify_timestamp(rbd_image_t image,
                                          struct timespec *timestamp);

CEPH_RBD_API int rbd_get_overlap(rbd_image_t image, uint64_t *overlap);
CEPH_RBD_API int rbd_get_name(rbd_image_t image, char *name, size_t *name_len);
CEPH_RBD_API int rbd_get_id(rbd_image_t image, char *id, size_t id_len);
CEPH_RBD_API int rbd_get_block_name_prefix(rbd_image_t image,
                                           char *prefix, size_t prefix_len);
CEPH_RBD_API int64_t rbd_get_data_pool_id(rbd_image_t image);

CEPH_RBD_API int rbd_get_parent_info(rbd_image_t image,
			             char *parent_poolname, size_t ppoolnamelen,
			             char *parent_name, size_t pnamelen,
			             char *parent_snapname,
                                     size_t psnapnamelen)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_get_parent_info2(rbd_image_t image,
                                      char *parent_poolname,
                                      size_t ppoolnamelen,
                                      char *parent_name, size_t pnamelen,
                                      char *parent_id, size_t pidlen,
                                      char *parent_snapname,
                                      size_t psnapnamelen)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_get_parent(rbd_image_t image,
                                rbd_linked_image_spec_t *parent_image,
                                rbd_snap_spec_t *parent_snap);

CEPH_RBD_API int rbd_get_flags(rbd_image_t image, uint64_t *flags);
CEPH_RBD_API int rbd_get_group(rbd_image_t image, rbd_group_info_t *group_info,
                               size_t group_info_size);
CEPH_RBD_API int rbd_set_image_notification(rbd_image_t image, int fd, int type);

/* exclusive lock feature */
CEPH_RBD_API int rbd_is_exclusive_lock_owner(rbd_image_t image, int *is_owner);
CEPH_RBD_API int rbd_lock_acquire(rbd_image_t image, rbd_lock_mode_t lock_mode);
CEPH_RBD_API int rbd_lock_release(rbd_image_t image);
CEPH_RBD_API int rbd_lock_get_owners(rbd_image_t image,
                                     rbd_lock_mode_t *lock_mode,
                                     char **lock_owners,
                                     size_t *max_lock_owners);
CEPH_RBD_API void rbd_lock_get_owners_cleanup(char **lock_owners,
                                              size_t lock_owner_count);
CEPH_RBD_API int rbd_lock_break(rbd_image_t image, rbd_lock_mode_t lock_mode,
                                const char *lock_owner);

/* object map feature */
CEPH_RBD_API int rbd_rebuild_object_map(rbd_image_t image,
                                        librbd_progress_fn_t cb, void *cbdata);

CEPH_RBD_API int rbd_copy(rbd_image_t image, rados_ioctx_t dest_io_ctx,
                          const char *destname);
CEPH_RBD_API int rbd_copy2(rbd_image_t src, rbd_image_t dest);
CEPH_RBD_API int rbd_copy3(rbd_image_t src, rados_ioctx_t dest_io_ctx,
			   const char *destname, rbd_image_options_t dest_opts);
CEPH_RBD_API int rbd_copy4(rbd_image_t src, rados_ioctx_t dest_io_ctx,
			   const char *destname, rbd_image_options_t dest_opts,
			   size_t sparse_size);
CEPH_RBD_API int rbd_copy_with_progress(rbd_image_t image, rados_ioctx_t dest_p,
                                        const char *destname,
                                        librbd_progress_fn_t cb, void *cbdata);
CEPH_RBD_API int rbd_copy_with_progress2(rbd_image_t src, rbd_image_t dest,
			                 librbd_progress_fn_t cb, void *cbdata);
CEPH_RBD_API int rbd_copy_with_progress3(rbd_image_t image,
					 rados_ioctx_t dest_p,
					 const char *destname,
					 rbd_image_options_t dest_opts,
					 librbd_progress_fn_t cb, void *cbdata);
CEPH_RBD_API int rbd_copy_with_progress4(rbd_image_t image,
					 rados_ioctx_t dest_p,
					 const char *destname,
					 rbd_image_options_t dest_opts,
					 librbd_progress_fn_t cb, void *cbdata,
					 size_t sparse_size);

/* deep copy */
CEPH_RBD_API int rbd_deep_copy(rbd_image_t src, rados_ioctx_t dest_io_ctx,
                               const char *destname,
                               rbd_image_options_t dest_opts);
CEPH_RBD_API int rbd_deep_copy_with_progress(rbd_image_t image,
                                             rados_ioctx_t dest_io_ctx,
                                             const char *destname,
                                             rbd_image_options_t dest_opts,
                                             librbd_progress_fn_t cb,
                                             void *cbdata);

/* snapshots */
CEPH_RBD_API int rbd_snap_list(rbd_image_t image, rbd_snap_info_t *snaps,
                               int *max_snaps);
CEPH_RBD_API void rbd_snap_list_end(rbd_snap_info_t *snaps);
CEPH_RBD_API int rbd_snap_exists(rbd_image_t image, const char *snapname, bool *exists);
CEPH_RBD_API int rbd_snap_create(rbd_image_t image, const char *snapname);
CEPH_RBD_API int rbd_snap_create2(rbd_image_t image, const char *snap_name,
                                  uint32_t flags, librbd_progress_fn_t cb,
                                  void *cbdata);
CEPH_RBD_API int rbd_snap_remove(rbd_image_t image, const char *snapname);
CEPH_RBD_API int rbd_snap_remove2(rbd_image_t image, const char *snap_name,
                                  uint32_t flags, librbd_progress_fn_t cb,
                                  void *cbdata);
CEPH_RBD_API int rbd_snap_remove_by_id(rbd_image_t image, uint64_t snap_id);
CEPH_RBD_API int rbd_snap_rollback(rbd_image_t image, const char *snapname);
CEPH_RBD_API int rbd_snap_rollback_with_progress(rbd_image_t image,
                                                 const char *snapname,
				                 librbd_progress_fn_t cb,
                                                 void *cbdata);
CEPH_RBD_API int rbd_snap_rename(rbd_image_t image, const char *snapname,
				 const char* dstsnapsname);
/**
 * Prevent a snapshot from being deleted until it is unprotected.
 *
 * @param snap_name which snapshot to protect
 * @returns 0 on success, negative error code on failure
 * @returns -EBUSY if snap is already protected
 */
CEPH_RBD_API int rbd_snap_protect(rbd_image_t image, const char *snap_name);
/**
 * Allow a snaphshot to be deleted.
 *
 * @param snap_name which snapshot to unprotect
 * @returns 0 on success, negative error code on failure
 * @returns -EINVAL if snap is not protected
 */
CEPH_RBD_API int rbd_snap_unprotect(rbd_image_t image, const char *snap_name);
/**
 * Determine whether a snapshot is protected.
 *
 * @param snap_name which snapshot query
 * @param is_protected where to store the result (0 or 1)
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_snap_is_protected(rbd_image_t image, const char *snap_name,
			               int *is_protected);
/**
 * Get the current snapshot limit for an image. If no limit is set,
 * UINT64_MAX is returned.
 *
 * @param limit pointer where the limit will be stored on success
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_snap_get_limit(rbd_image_t image, uint64_t *limit);

/**
 * Set a limit for the number of snapshots that may be taken of an image.
 *
 * @param limit the maximum number of snapshots allowed in the future.
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_snap_set_limit(rbd_image_t image, uint64_t limit);

/**
 * Get the timestamp of a snapshot for an image. 
 *
 * @param snap_id the snap id of a snapshot of input image.
 * @param timestamp the timestamp of input snapshot.
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_snap_get_timestamp(rbd_image_t image, uint64_t snap_id, struct timespec *timestamp);

CEPH_RBD_API int rbd_snap_set(rbd_image_t image, const char *snapname);
CEPH_RBD_API int rbd_snap_set_by_id(rbd_image_t image, uint64_t snap_id);
CEPH_RBD_API int rbd_snap_get_name(rbd_image_t image, uint64_t snap_id, char *snapname, size_t *name_len);
CEPH_RBD_API int rbd_snap_get_id(rbd_image_t image, const char *snapname, uint64_t *snap_id);

CEPH_RBD_API int rbd_snap_get_namespace_type(rbd_image_t image,
                                             uint64_t snap_id,
                                             rbd_snap_namespace_type_t *namespace_type);
CEPH_RBD_API int rbd_snap_get_group_namespace(rbd_image_t image,
                                              uint64_t snap_id,
                                              rbd_snap_group_namespace_t *group_snap,
                                              size_t group_snap_size);
CEPH_RBD_API int rbd_snap_group_namespace_cleanup(rbd_snap_group_namespace_t *group_snap,
                                                  size_t group_snap_size);
CEPH_RBD_API int rbd_snap_get_trash_namespace(rbd_image_t image,
                                              uint64_t snap_id,
                                              char* original_name,
                                              size_t max_length);
CEPH_RBD_API int rbd_snap_get_mirror_namespace(
    rbd_image_t image, uint64_t snap_id,
    rbd_snap_mirror_namespace_t *mirror_snap, size_t mirror_snap_size);
CEPH_RBD_API int rbd_snap_mirror_namespace_cleanup(
    rbd_snap_mirror_namespace_t *mirror_snap, size_t mirror_snap_size);

CEPH_RBD_API int rbd_flatten(rbd_image_t image);

CEPH_RBD_API int rbd_flatten_with_progress(rbd_image_t image,
                                           librbd_progress_fn_t cb,
                                           void *cbdata);

CEPH_RBD_API int rbd_sparsify(rbd_image_t image, size_t sparse_size);

CEPH_RBD_API int rbd_sparsify_with_progress(rbd_image_t image,
                                            size_t sparse_size,
                                            librbd_progress_fn_t cb,
                                            void *cbdata);

/**
 * List all images that are cloned from the image at the
 * snapshot that is set via rbd_snap_set().
 *
 * This iterates over all pools, so it should be run by a user with
 * read access to all of them. pools_len and images_len are filled in
 * with the number of bytes put into the pools and images buffers.
 *
 * If the provided buffers are too short, the required lengths are
 * still filled in, but the data is not and -ERANGE is returned.
 * Otherwise, the buffers are filled with the pool and image names
 * of the children, with a '\0' after each.
 *
 * @param image which image (and implicitly snapshot) to list clones of
 * @param pools buffer in which to store pool names
 * @param pools_len number of bytes in pools buffer
 * @param images buffer in which to store image names
 * @param images_len number of bytes in images buffer
 * @returns number of children on success, negative error code on failure
 * @returns -ERANGE if either buffer is too short
 */
CEPH_RBD_API ssize_t rbd_list_children(rbd_image_t image, char *pools,
                                       size_t *pools_len, char *images,
                                       size_t *images_len)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_list_children2(rbd_image_t image,
                                    rbd_child_info_t *children,
                                    int *max_children)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API void rbd_list_child_cleanup(rbd_child_info_t *child)
  CEPH_RBD_DEPRECATED;
CEPH_RBD_API void rbd_list_children_cleanup(rbd_child_info_t *children,
                                            size_t num_children)
  CEPH_RBD_DEPRECATED;

CEPH_RBD_API int rbd_list_children3(rbd_image_t image,
                                    rbd_linked_image_spec_t *images,
                                    size_t *max_images);

CEPH_RBD_API int rbd_list_descendants(rbd_image_t image,
                                      rbd_linked_image_spec_t *images,
                                      size_t *max_images);

/**
 * @defgroup librbd_h_locking Advisory Locking
 *
 * An rbd image may be locking exclusively, or shared, to facilitate
 * e.g. live migration where the image may be open in two places at once.
 * These locks are intended to guard against more than one client
 * writing to an image without coordination. They don't need to
 * be used for snapshots, since snapshots are read-only.
 *
 * Currently locks only guard against locks being acquired.
 * They do not prevent anything else.
 *
 * A locker is identified by the internal rados client id of the
 * holder and a user-defined cookie. This (client id, cookie) pair
 * must be unique for each locker.
 *
 * A shared lock also has a user-defined tag associated with it. Each
 * additional shared lock must specify the same tag or lock
 * acquisition will fail. This can be used by e.g. groups of hosts
 * using a clustered filesystem on top of an rbd image to make sure
 * they're accessing the correct image.
 *
 * @{
 */
/**
 * List clients that have locked the image and information about the lock.
 *
 * The number of bytes required in each buffer is put in the
 * corresponding size out parameter. If any of the provided buffers
 * are too short, -ERANGE is returned after these sizes are filled in.
 *
 * @param exclusive where to store whether the lock is exclusive (1) or shared (0)
 * @param tag where to store the tag associated with the image
 * @param tag_len number of bytes in tag buffer
 * @param clients buffer in which locker clients are stored, separated by '\0'
 * @param clients_len number of bytes in the clients buffer
 * @param cookies buffer in which locker cookies are stored, separated by '\0'
 * @param cookies_len number of bytes in the cookies buffer
 * @param addrs buffer in which locker addresses are stored, separated by '\0'
 * @param addrs_len number of bytes in the clients buffer
 * @returns number of lockers on success, negative error code on failure
 * @returns -ERANGE if any of the buffers are too short
 */
CEPH_RBD_API ssize_t rbd_list_lockers(rbd_image_t image, int *exclusive,
			              char *tag, size_t *tag_len,
			              char *clients, size_t *clients_len,
			              char *cookies, size_t *cookies_len,
			              char *addrs, size_t *addrs_len);

/**
 * Take an exclusive lock on the image.
 *
 * @param image the image to lock
 * @param cookie user-defined identifier for this instance of the lock
 * @returns 0 on success, negative error code on failure
 * @returns -EBUSY if the lock is already held by another (client, cookie) pair
 * @returns -EEXIST if the lock is already held by the same (client, cookie) pair
 */
CEPH_RBD_API int rbd_lock_exclusive(rbd_image_t image, const char *cookie);

/**
 * Take a shared lock on the image.
 *
 * Other clients may also take a shared lock, as lock as they use the
 * same tag.
 *
 * @param image the image to lock
 * @param cookie user-defined identifier for this instance of the lock
 * @param tag user-defined identifier for this shared use of the lock
 * @returns 0 on success, negative error code on failure
 * @returns -EBUSY if the lock is already held by another (client, cookie) pair
 * @returns -EEXIST if the lock is already held by the same (client, cookie) pair
 */
CEPH_RBD_API int rbd_lock_shared(rbd_image_t image, const char *cookie,
                                 const char *tag);

/**
 * Release a shared or exclusive lock on the image.
 *
 * @param image the image to unlock
 * @param cookie user-defined identifier for the instance of the lock
 * @returns 0 on success, negative error code on failure
 * @returns -ENOENT if the lock is not held by the specified (client, cookie) pair
 */
CEPH_RBD_API int rbd_unlock(rbd_image_t image, const char *cookie);

/**
 * Release a shared or exclusive lock that was taken by the specified client.
 *
 * @param image the image to unlock
 * @param client the entity holding the lock (as given by rbd_list_lockers())
 * @param cookie user-defined identifier for the instance of the lock to break
 * @returns 0 on success, negative error code on failure
 * @returns -ENOENT if the lock is not held by the specified (client, cookie) pair
 */
CEPH_RBD_API int rbd_break_lock(rbd_image_t image, const char *client,
                                const char *cookie);

/** @} locking */

/* I/O */
CEPH_RBD_API ssize_t rbd_read(rbd_image_t image, uint64_t ofs, size_t len,
                              char *buf);
/*
 * @param op_flags: see librados.h constants beginning with LIBRADOS_OP_FLAG
 */
CEPH_RBD_API ssize_t rbd_read2(rbd_image_t image, uint64_t ofs, size_t len,
                               char *buf, int op_flags);
/* DEPRECATED; use rbd_read_iterate2 */
CEPH_RBD_API int64_t rbd_read_iterate(rbd_image_t image, uint64_t ofs, size_t len,
			              int (*cb)(uint64_t, size_t, const char *, void *),
                                      void *arg);

/**
 * iterate read over an image
 *
 * Reads each region of the image and calls the callback.  If the
 * buffer pointer passed to the callback is NULL, the given extent is
 * defined to be zeros (a hole).  Normally the granularity for the
 * callback is the image stripe size.
 *
 * @param image image to read
 * @param ofs offset to start from
 * @param len bytes of source image to cover
 * @param cb callback for each region
 * @returns 0 success, error otherwise
 */
CEPH_RBD_API int rbd_read_iterate2(rbd_image_t image, uint64_t ofs, uint64_t len,
		                   int (*cb)(uint64_t, size_t, const char *, void *),
                                   void *arg);
/**
 * get difference between two versions of an image
 *
 * This will return the differences between two versions of an image
 * via a callback, which gets the offset and length and a flag
 * indicating whether the extent exists (1), or is known/defined to
 * be zeros (a hole, 0).  If the source snapshot name is NULL, we
 * interpret that as the beginning of time and return all allocated
 * regions of the image.  The end version is whatever is currently
 * selected for the image handle (either a snapshot or the writeable
 * head).
 *
 * @param fromsnapname start snapshot name, or NULL
 * @param ofs start offset
 * @param len len in bytes of region to report on
 * @param include_parent 1 if full history diff should include parent
 * @param whole_object 1 if diff extents should cover whole object
 * @param cb callback to call for each allocated region
 * @param arg argument to pass to the callback
 * @returns 0 on success, or negative error code on error
 */
CEPH_RBD_API int rbd_diff_iterate(rbd_image_t image,
		                  const char *fromsnapname,
		                  uint64_t ofs, uint64_t len,
		                  int (*cb)(uint64_t, size_t, int, void *),
                                  void *arg);
CEPH_RBD_API int rbd_diff_iterate2(rbd_image_t image,
		                   const char *fromsnapname,
		                   uint64_t ofs, uint64_t len,
                                   uint8_t include_parent, uint8_t whole_object,
		                   int (*cb)(uint64_t, size_t, int, void *),
                                   void *arg);
CEPH_RBD_API ssize_t rbd_write(rbd_image_t image, uint64_t ofs, size_t len,
                               const char *buf);
/*
 * @param op_flags: see librados.h constants beginning with LIBRADOS_OP_FLAG
 */
CEPH_RBD_API ssize_t rbd_write2(rbd_image_t image, uint64_t ofs, size_t len,
                                const char *buf, int op_flags);
CEPH_RBD_API int rbd_discard(rbd_image_t image, uint64_t ofs, uint64_t len);
CEPH_RBD_API ssize_t rbd_writesame(rbd_image_t image, uint64_t ofs, size_t len,
                                   const char *buf, size_t data_len, int op_flags);
CEPH_RBD_API ssize_t rbd_compare_and_write(rbd_image_t image, uint64_t ofs,
                                           size_t len, const char *cmp_buf,
                                           const char *buf, uint64_t *mismatch_off,
                                           int op_flags);

CEPH_RBD_API int rbd_aio_write(rbd_image_t image, uint64_t off, size_t len,
                               const char *buf, rbd_completion_t c);

/*
 * @param op_flags: see librados.h constants beginning with LIBRADOS_OP_FLAG
 */
CEPH_RBD_API int rbd_aio_write2(rbd_image_t image, uint64_t off, size_t len,
                                const char *buf, rbd_completion_t c,
                                int op_flags);
CEPH_RBD_API int rbd_aio_writev(rbd_image_t image, const struct iovec *iov,
                                int iovcnt, uint64_t off, rbd_completion_t c);
CEPH_RBD_API int rbd_aio_read(rbd_image_t image, uint64_t off, size_t len,
                              char *buf, rbd_completion_t c);
/*
 * @param op_flags: see librados.h constants beginning with LIBRADOS_OP_FLAG
 */
CEPH_RBD_API int rbd_aio_read2(rbd_image_t image, uint64_t off, size_t len,
                               char *buf, rbd_completion_t c, int op_flags);
CEPH_RBD_API int rbd_aio_readv(rbd_image_t image, const struct iovec *iov,
                               int iovcnt, uint64_t off, rbd_completion_t c);
CEPH_RBD_API int rbd_aio_discard(rbd_image_t image, uint64_t off, uint64_t len,
                                 rbd_completion_t c);
CEPH_RBD_API int rbd_aio_writesame(rbd_image_t image, uint64_t off, size_t len,
                                   const char *buf, size_t data_len,
                                   rbd_completion_t c, int op_flags);
CEPH_RBD_API ssize_t rbd_aio_compare_and_write(rbd_image_t image,
                                               uint64_t off, size_t len,
                                               const char *cmp_buf, const char *buf,
                                               rbd_completion_t c, uint64_t *mismatch_off,
                                               int op_flags);

CEPH_RBD_API int rbd_aio_create_completion(void *cb_arg,
                                           rbd_callback_t complete_cb,
                                           rbd_completion_t *c);
CEPH_RBD_API int rbd_aio_is_complete(rbd_completion_t c);
CEPH_RBD_API int rbd_aio_wait_for_complete(rbd_completion_t c);
CEPH_RBD_API ssize_t rbd_aio_get_return_value(rbd_completion_t c);
CEPH_RBD_API void *rbd_aio_get_arg(rbd_completion_t c);
CEPH_RBD_API void rbd_aio_release(rbd_completion_t c);
CEPH_RBD_API int rbd_flush(rbd_image_t image);
/**
 * Start a flush if caching is enabled. Get a callback when
 * the currently pending writes are on disk.
 *
 * @param image the image to flush writes to
 * @param c what to call when flushing is complete
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_aio_flush(rbd_image_t image, rbd_completion_t c);

/**
 * Drop any cached data for an image
 *
 * @param image the image to invalidate cached data for
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_invalidate_cache(rbd_image_t image);

CEPH_RBD_API int rbd_poll_io_events(rbd_image_t image, rbd_completion_t *comps, int numcomp);

CEPH_RBD_API int rbd_metadata_get(rbd_image_t image, const char *key, char *value, size_t *val_len);
CEPH_RBD_API int rbd_metadata_set(rbd_image_t image, const char *key, const char *value);
CEPH_RBD_API int rbd_metadata_remove(rbd_image_t image, const char *key);
/**
 * List all metadatas associated with this image.
 *
 * This iterates over all metadatas, key_len and val_len are filled in
 * with the number of bytes put into the keys and values buffers.
 *
 * If the provided buffers are too short, the required lengths are
 * still filled in, but the data is not and -ERANGE is returned.
 * Otherwise, the buffers are filled with the keys and values
 * of the image, with a '\0' after each.
 *
 * @param image which image (and implicitly snapshot) to list clones of
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max the maximum number of names to lis(if 0 means no limit)
 * @param keys buffer in which to store pool names
 * @param keys_len number of bytes in pools buffer
 * @param values buffer in which to store image names
 * @param vals_len number of bytes in images buffer
 * @returns number of children on success, negative error code on failure
 * @returns -ERANGE if either buffer is too short
 */
CEPH_RBD_API int rbd_metadata_list(rbd_image_t image, const char *start, uint64_t max,
    char *keys, size_t *key_len, char *values, size_t *vals_len);

// RBD image mirroring support functions
CEPH_RBD_API int rbd_mirror_image_enable(rbd_image_t image) CEPH_RBD_DEPRECATED;
CEPH_RBD_API int rbd_mirror_image_enable2(rbd_image_t image,
                                          rbd_mirror_image_mode_t mode);
CEPH_RBD_API int rbd_mirror_image_disable(rbd_image_t image, bool force);
CEPH_RBD_API int rbd_mirror_image_promote(rbd_image_t image, bool force);
CEPH_RBD_API int rbd_mirror_image_demote(rbd_image_t image);
CEPH_RBD_API int rbd_mirror_image_resync(rbd_image_t image);
CEPH_RBD_API int rbd_mirror_image_create_snapshot(rbd_image_t image,
                                                  uint64_t *snap_id);
CEPH_RBD_API int rbd_mirror_image_get_info(rbd_image_t image,
                                           rbd_mirror_image_info_t *mirror_image_info,
                                           size_t info_size);
CEPH_RBD_API void rbd_mirror_image_get_info_cleanup(
    rbd_mirror_image_info_t *mirror_image_info);
CEPH_RBD_API int rbd_mirror_image_get_mode(rbd_image_t image,
                                           rbd_mirror_image_mode_t *mode);

CEPH_RBD_API int rbd_mirror_image_get_global_status(
    rbd_image_t image,
    rbd_mirror_image_global_status_t *mirror_image_global_status,
    size_t status_size);
CEPH_RBD_API void rbd_mirror_image_global_status_cleanup(
    rbd_mirror_image_global_status_t *mirror_image_global_status);

CEPH_RBD_API int rbd_mirror_image_get_status(
    rbd_image_t image, rbd_mirror_image_status_t *mirror_image_status,
    size_t status_size)
  CEPH_RBD_DEPRECATED;

CEPH_RBD_API int rbd_mirror_image_get_instance_id(rbd_image_t image,
                                                  char *instance_id,
                                                  size_t *id_max_length);
CEPH_RBD_API int rbd_aio_mirror_image_promote(rbd_image_t image, bool force,
                                              rbd_completion_t c);
CEPH_RBD_API int rbd_aio_mirror_image_demote(rbd_image_t image,
                                             rbd_completion_t c);
CEPH_RBD_API int rbd_aio_mirror_image_get_info(rbd_image_t image,
                                               rbd_mirror_image_info_t *mirror_image_info,
                                               size_t info_size,
                                               rbd_completion_t c);

CEPH_RBD_API int rbd_aio_mirror_image_get_global_status(
    rbd_image_t image,
    rbd_mirror_image_global_status_t *mirror_global_image_status,
    size_t status_size, rbd_completion_t c);
CEPH_RBD_API int rbd_aio_mirror_image_get_status(
    rbd_image_t image, rbd_mirror_image_status_t *mirror_image_status,
    size_t status_size, rbd_completion_t c)
  CEPH_RBD_DEPRECATED;

// RBD groups support functions
CEPH_RBD_API int rbd_group_create(rados_ioctx_t p, const char *name);
CEPH_RBD_API int rbd_group_remove(rados_ioctx_t p, const char *name);
CEPH_RBD_API int rbd_group_list(rados_ioctx_t p, char *names, size_t *size);
CEPH_RBD_API int rbd_group_rename(rados_ioctx_t p, const char *src_name,
                                  const char *dest_name);
CEPH_RBD_API int rbd_group_info_cleanup(rbd_group_info_t *group_info,
                                        size_t group_info_size);

/**
 * Register an image metadata change watcher.
 *
 * @param image the image to watch
 * @param handle where to store the internal id assigned to this watch
 * @param watch_cb what to do when a notify is received on this image
 * @param arg opaque value to pass to the callback
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_update_watch(rbd_image_t image, uint64_t *handle,
				  rbd_update_callback_t watch_cb, void *arg);

/**
 * Unregister an image watcher.
 *
 * @param image the image to unwatch
 * @param handle which watch to unregister
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_update_unwatch(rbd_image_t image, uint64_t handle);

/**
 * List any watchers of an image.
 *
 * Watchers will be allocated and stored in the passed watchers array. If there
 * are more watchers than max_watchers, -ERANGE will be returned and the number
 * of watchers will be stored in max_watchers.
 *
 * The caller should call rbd_watchers_list_cleanup when finished with the list
 * of watchers.
 *
 * @param image the image to list watchers for.
 * @param watchers an array to store watchers in.
 * @param max_watchers capacity of the watchers array.
 * @returns 0 on success, negative error code on failure.
 * @returns -ERANGE if there are too many watchers for the passed array.
 * @returns the number of watchers in max_watchers.
 */
CEPH_RBD_API int rbd_watchers_list(rbd_image_t image,
				   rbd_image_watcher_t *watchers,
				   size_t *max_watchers);

CEPH_RBD_API void rbd_watchers_list_cleanup(rbd_image_watcher_t *watchers,
					    size_t num_watchers);

CEPH_RBD_API int rbd_config_image_list(rbd_image_t image,
                                       rbd_config_option_t *options,
                                       int *max_options);
CEPH_RBD_API void rbd_config_image_list_cleanup(rbd_config_option_t *options,
                                                int max_options);

CEPH_RBD_API int rbd_group_image_add(rados_ioctx_t group_p,
                                     const char *group_name,
                                     rados_ioctx_t image_p,
                                     const char *image_name);
CEPH_RBD_API int rbd_group_image_remove(rados_ioctx_t group_p,
                                        const char *group_name,
                                        rados_ioctx_t image_p,
                                        const char *image_name);
CEPH_RBD_API int rbd_group_image_remove_by_id(rados_ioctx_t group_p,
                                              const char *group_name,
                                              rados_ioctx_t image_p,
                                              const char *image_id);
CEPH_RBD_API int rbd_group_image_list(rados_ioctx_t group_p,
                                      const char *group_name,
                                      rbd_group_image_info_t *images,
                                      size_t group_image_info_size,
                                      size_t *num_entries);
CEPH_RBD_API int rbd_group_image_list_cleanup(rbd_group_image_info_t *images,
                                              size_t group_image_info_size,
                                              size_t num_entries);

CEPH_RBD_API int rbd_group_snap_create(rados_ioctx_t group_p,
                                       const char *group_name,
                                       const char *snap_name);
CEPH_RBD_API int rbd_group_snap_remove(rados_ioctx_t group_p,
                                       const char *group_name,
                                       const char *snap_name);
CEPH_RBD_API int rbd_group_snap_rename(rados_ioctx_t group_p,
                                       const char *group_name,
                                       const char *old_snap_name,
                                       const char *new_snap_name);
CEPH_RBD_API int rbd_group_snap_list(rados_ioctx_t group_p,
                                     const char *group_name,
                                     rbd_group_snap_info_t *snaps,
                                     size_t group_snap_info_size,
                                     size_t *num_entries);
CEPH_RBD_API int rbd_group_snap_list_cleanup(rbd_group_snap_info_t *snaps,
                                             size_t group_snap_info_size,
                                             size_t num_entries);
CEPH_RBD_API int rbd_group_snap_rollback(rados_ioctx_t group_p,
                                         const char *group_name,
                                         const char *snap_name);
CEPH_RBD_API int rbd_group_snap_rollback_with_progress(rados_ioctx_t group_p,
                                                       const char *group_name,
                                                       const char *snap_name,
                                                       librbd_progress_fn_t cb,
                                                       void *cbdata);

CEPH_RBD_API int rbd_namespace_create(rados_ioctx_t io,
                                      const char *namespace_name);
CEPH_RBD_API int rbd_namespace_remove(rados_ioctx_t io,
                                      const char *namespace_name);
CEPH_RBD_API int rbd_namespace_list(rados_ioctx_t io, char *namespace_names,
                                    size_t *size);
CEPH_RBD_API int rbd_namespace_exists(rados_ioctx_t io,
                                      const char *namespace_name,
                                      bool *exists);

CEPH_RBD_API int rbd_pool_init(rados_ioctx_t io, bool force);

CEPH_RBD_API void rbd_pool_stats_create(rbd_pool_stats_t *stats);
CEPH_RBD_API void rbd_pool_stats_destroy(rbd_pool_stats_t stats);
CEPH_RBD_API int rbd_pool_stats_option_add_uint64(rbd_pool_stats_t stats,
					          int stat_option,
                                                  uint64_t* stat_val);
CEPH_RBD_API int rbd_pool_stats_get(rados_ioctx_t io, rbd_pool_stats_t stats);

/**
 * Register a quiesce/unquiesce watcher.
 *
 * @param image the image to watch
 * @param quiesce_cb what to do when librbd wants to quiesce
 * @param unquiesce_cb what to do when librbd wants to unquiesce
 * @param arg opaque value to pass to the callbacks
 * @param handle where to store the internal id assigned to this watch
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_quiesce_watch(rbd_image_t image,
                                   rbd_update_callback_t quiesce_cb,
                                   rbd_update_callback_t unquiesce_cb,
                                   void *arg, uint64_t *handle);

/**
 * Notify quiesce is complete
 *
 * @param image the image to notify
 * @param r the return code
 */
CEPH_RADOS_API void rbd_quiesce_complete(rbd_image_t image, int r);

/**
 * Unregister a quiesce/unquiesce watcher.
 *
 * @param image the image to unwatch
 * @param handle which watch to unregister
 * @returns 0 on success, negative error code on failure
 */
CEPH_RBD_API int rbd_quiesce_unwatch(rbd_image_t image, uint64_t handle);

#if __GNUC__ >= 4
  #pragma GCC diagnostic pop
#endif

#ifdef __cplusplus
}
#endif

#endif /* CEPH_LIBRBD_H */
