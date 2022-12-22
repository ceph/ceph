/*
 * file:        fake_rbd.h
 * description: replacement for <rbd/librbd.h>
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#ifndef __FAKE_RBD_H__
#define __FAKE_RBD_H__

#if __GNUC__ >= 4
  #define CEPH_RBD_API          __attribute__ ((visibility ("default")))
  #define CEPH_RBD_DEPRECATED   __attribute__((deprecated))
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#else
  #define CEPH_RBD_API
  #define CEPH_RBD_DEPRECATED
#endif

#include <stdint.h>
#include <stddef.h>
#include <sys/uio.h>
#include <rados/librados.h>

/* the following types have to be compatible with the real librbd.h
 */
enum {
    EVENT_TYPE_PIPE = 1,
    EVENT_TYPE_EVENTFD = 2
};

typedef void *rbd_image_t;
typedef void *rbd_image_options_t;
typedef void *rbd_pool_stats_t;

typedef void *rbd_completion_t;
typedef void (*rbd_callback_t)(rbd_completion_t cb, void *arg);

#define RBD_MAX_BLOCK_NAME_SIZE 24
#define RBD_MAX_IMAGE_NAME_SIZE 96

/* fio only looks at 'size' */
typedef struct {
    uint64_t size;
    uint64_t obj_size;
    uint64_t num_objs;
    int order;
    char block_name_prefix[RBD_MAX_BLOCK_NAME_SIZE]; /* deprecated */
    int64_t parent_pool;                             /* deprecated */
    char parent_name[RBD_MAX_IMAGE_NAME_SIZE];       /* deprecated */
} rbd_image_info_t;

typedef struct {
    uint64_t id;
    uint64_t size;
    const char *name;
} rbd_snap_info_t;

extern "C" CEPH_RBD_API int rbd_poll_io_events(rbd_image_t image,
                                               rbd_completion_t *comps, int numcomp);

extern "C" CEPH_RBD_API int rbd_set_image_notification(rbd_image_t image, int fd, int type);

extern "C" CEPH_RBD_API int rbd_aio_create_completion(void *cb_arg,
                                                      rbd_callback_t complete_cb,
                                                      rbd_completion_t *c);

extern "C" CEPH_RBD_API void rbd_aio_release(rbd_completion_t c);

extern "C" CEPH_RBD_API int rbd_aio_discard(rbd_image_t image, uint64_t off,
                                            uint64_t len, rbd_completion_t c);

extern "C" CEPH_RBD_API int rbd_aio_flush(rbd_image_t image, rbd_completion_t c);

extern "C" CEPH_RBD_API int rbd_flush(rbd_image_t image);

extern "C" CEPH_RBD_API void *rbd_aio_get_arg(rbd_completion_t c);

extern "C" CEPH_RBD_API ssize_t rbd_aio_get_return_value(rbd_completion_t c);

extern "C" CEPH_RBD_API int rbd_aio_read(rbd_image_t image, uint64_t offset,
                                         size_t len, char *buf, rbd_completion_t c);

extern "C" CEPH_RBD_API int rbd_aio_readv(rbd_image_t image, const iovec *iov,
                                          int iovcnt, uint64_t off, rbd_completion_t c);

extern "C" CEPH_RBD_API int rbd_aio_writev(rbd_image_t image, const struct iovec *iov,
                                           int iovcnt, uint64_t off, rbd_completion_t c);

extern "C" CEPH_RBD_API int rbd_aio_write(rbd_image_t image, uint64_t off,
                                          size_t len, const char *buf, rbd_completion_t c);

extern "C" CEPH_RBD_API int rbd_read(rbd_image_t image, uint64_t off, size_t len, char *buf);

extern "C" CEPH_RBD_API int rbd_write(rbd_image_t image, uint64_t off, size_t len,
                                      const char *buf);

extern "C" CEPH_RBD_API int rbd_aio_writesame(rbd_image_t image, uint64_t off,
                                              size_t len,
                                              const char *buf, size_t data_len,
                                              rbd_completion_t c, int op_flags);

extern "C" CEPH_RBD_API int rbd_aio_write_zeroes(rbd_image_t image,
                                                 uint64_t off,
                                                 size_t len, rbd_completion_t c,
                                                 int zero_flags, int op_flags);

extern "C" CEPH_RBD_API int rbd_aio_wait_for_complete(rbd_completion_t c);

extern "C" CEPH_RBD_API int rbd_stat(rbd_image_t image, rbd_image_info_t *info,
                                     size_t infosize);

extern "C" CEPH_RBD_API int rbd_get_size(rbd_image_t image, uint64_t *size);

extern "C" CEPH_RBD_API int rbd_open(rados_ioctx_t io, const char *name,
                                     rbd_image_t *image, const char *snap_name);
extern "C" CEPH_RBD_API int rbd_close(rbd_image_t image);
extern "C" CEPH_RBD_API int rbd_invalidate_cache(rbd_image_t image);

extern "C" CEPH_RBD_API int rbd_create(rados_ioctx_t io, const char *name, uint64_t size, int *order);
extern "C" CEPH_RBD_API int rbd_remove(rados_ioctx_t io, const char *name);

typedef int (*librbd_progress_fn_t)(uint64_t offset, uint64_t total, void *ptr);
extern "C" CEPH_RBD_API int rbd_remove_with_progress(rados_ioctx_t io, const char *name,
                                                     librbd_progress_fn_t cb, void *cbdata);

/* These RBD functions are unimplemented and return errors
 */
extern "C" CEPH_RBD_API int rbd_resize(rbd_image_t image, uint64_t size);
extern "C" CEPH_RBD_API int rbd_snap_create(rbd_image_t image, const char *snapname);
extern "C" CEPH_RBD_API int rbd_snap_list(rbd_image_t image, rbd_snap_info_t *snaps, int *max_snaps);
extern "C" CEPH_RBD_API void rbd_snap_list_end(rbd_snap_info_t *snaps);
extern "C" CEPH_RBD_API int rbd_snap_remove(rbd_image_t image, const char *snapname);
extern "C" CEPH_RBD_API int rbd_snap_rollback(rbd_image_t image, const char *snapname);

extern "C" CEPH_RBD_API int rbd_diff_iterate2(rbd_image_t image,
                                   const char *fromsnapname,
                                   uint64_t ofs, uint64_t len,
                                   uint8_t include_parent, uint8_t whole_object,
                                   int (*cb)(uint64_t, size_t, int, void *),
                                   void *arg);

typedef enum {
    RBD_ENCRYPTION_FORMAT_LUKS1 = 0,
    RBD_ENCRYPTION_FORMAT_LUKS2 = 1,
    RBD_ENCRYPTION_FORMAT_LUKS  = 2
} rbd_encryption_format_t;

typedef void *rbd_encryption_options_t;
extern "C" CEPH_RBD_API int rbd_encryption_format(rbd_image_t image,
                                       rbd_encryption_format_t format,
                                       rbd_encryption_options_t opts,
                                       size_t opts_size);

extern "C" CEPH_RBD_API int rbd_encryption_load(rbd_image_t image,
                                     rbd_encryption_format_t format,
                                     rbd_encryption_options_t opts,
                                     size_t opts_size);
extern "C" CEPH_RBD_API int rbd_get_features(rbd_image_t image, uint64_t *features);
extern "C" CEPH_RBD_API int rbd_get_flags(rbd_image_t image, uint64_t *flags);

typedef struct {
  char *id;
  char *name;
} rbd_image_spec_t;

extern "C" CEPH_RBD_API void rbd_image_spec_cleanup(rbd_image_spec_t *image);

typedef struct {
  int64_t pool_id;
  char *pool_name;
  char *pool_namespace;
  char *image_id;
  char *image_name;
  bool trash;
} rbd_linked_image_spec_t;

extern "C" CEPH_RBD_API void rbd_linked_image_spec_cleanup(rbd_linked_image_spec_t *image);
extern "C" CEPH_RBD_API int rbd_mirror_image_enable(rbd_image_t image) CEPH_RBD_DEPRECATED;

typedef enum {
  RBD_MIRROR_IMAGE_MODE_JOURNAL  = 0,
  RBD_MIRROR_IMAGE_MODE_SNAPSHOT = 1,
} rbd_mirror_image_mode_t;

extern "C" CEPH_RBD_API int rbd_mirror_image_enable2(rbd_image_t image,
                                          rbd_mirror_image_mode_t mode);
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

extern "C" CEPH_RBD_API void rbd_mirror_image_get_info_cleanup(
    rbd_mirror_image_info_t *mirror_image_info);

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

extern "C" CEPH_RBD_API void rbd_mirror_image_global_status_cleanup(
    rbd_mirror_image_global_status_t *mirror_image_global_status);

typedef enum {
  RBD_MIRROR_PEER_DIRECTION_RX    = 0,
  RBD_MIRROR_PEER_DIRECTION_TX    = 1,
  RBD_MIRROR_PEER_DIRECTION_RX_TX = 2
} rbd_mirror_peer_direction_t;

extern "C" CEPH_RBD_API int rbd_mirror_peer_site_add(
    rados_ioctx_t io_ctx, char *uuid, size_t uuid_max_length,
    rbd_mirror_peer_direction_t direction, const char *site_name,
    const char *client_name);
extern "C" CEPH_RBD_API int rbd_mirror_peer_site_get_attributes(
    rados_ioctx_t p, const char *uuid, char *keys, size_t *max_key_len,
    char *values, size_t *max_value_len, size_t *key_value_count);
extern "C" CEPH_RBD_API int rbd_mirror_peer_site_remove(
    rados_ioctx_t io_ctx, const char *uuid);
extern "C" CEPH_RBD_API int rbd_mirror_peer_site_set_attributes(
    rados_ioctx_t p, const char *uuid, const char *keys, const char *values,
    size_t key_value_count);
extern "C" CEPH_RBD_API int rbd_mirror_peer_site_set_name(
    rados_ioctx_t io_ctx, const char *uuid, const char *site_name);
extern "C" CEPH_RBD_API int rbd_mirror_peer_site_set_client_name(
    rados_ioctx_t io_ctx, const char *uuid, const char *client_name);

typedef void *rbd_pool_stats_t;
extern "C" CEPH_RBD_API void rbd_pool_stats_create(rbd_pool_stats_t *stats);
extern "C" CEPH_RBD_API void rbd_pool_stats_destroy(rbd_pool_stats_t stats);
extern "C" CEPH_RBD_API int rbd_pool_stats_option_add_uint64(rbd_pool_stats_t stats,
                                                  int stat_option,
                                                  uint64_t* stat_val);
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

extern "C" CEPH_RBD_API void rbd_trash_get_cleanup(rbd_trash_image_info_t *info);
extern "C" CEPH_RBD_API void rbd_version(int *major, int *minor, int *extra);

#endif
