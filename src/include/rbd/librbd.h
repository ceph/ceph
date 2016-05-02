// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include "../rados/librados.h"
#include "features.h"

#define LIBRBD_VER_MAJOR 0
#define LIBRBD_VER_MINOR 1
#define LIBRBD_VER_EXTRA 10

#define LIBRBD_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)

#define LIBRBD_VERSION_CODE LIBRBD_VERSION(LIBRBD_VER_MAJOR, LIBRBD_VER_MINOR, LIBRBD_VER_EXTRA)

#define LIBRBD_SUPPORTS_WATCH 0
#define LIBRBD_SUPPORTS_AIO_FLUSH 1
#define LIBRBD_SUPPORTS_INVALIDATE 1
#define LIBRBD_SUPPORTS_AIO_OPEN 1

#if __GNUC__ >= 4
  #define CEPH_RBD_API    __attribute__ ((visibility ("default")))
#else
  #define CEPH_RBD_API
#endif

#define RBD_FLAG_OBJECT_MAP_INVALID   (1<<0)
#define RBD_FLAG_FAST_DIFF_INVALID    (1<<1)

typedef void *rbd_snap_t;
typedef void *rbd_image_t;
typedef void *rbd_image_options_t;

typedef void *rbd_completion_t;
typedef void (*rbd_callback_t)(rbd_completion_t cb, void *arg);

typedef int (*librbd_progress_fn_t)(uint64_t offset, uint64_t total, void *ptr);

typedef struct {
  uint64_t id;
  uint64_t size;
  const char *name;
} rbd_snap_info_t;

#define RBD_MAX_IMAGE_NAME_SIZE 96
#define RBD_MAX_BLOCK_NAME_SIZE 24

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
  char block_name_prefix[RBD_MAX_BLOCK_NAME_SIZE];
  int64_t parent_pool;			      /* deprecated */
  char parent_name[RBD_MAX_IMAGE_NAME_SIZE];  /* deprecated */
} rbd_image_info_t;

typedef enum {
  RBD_MIRROR_MODE_DISABLED, /* mirroring is disabled */
  RBD_MIRROR_MODE_IMAGE,    /* mirroring enabled on a per-image basis */
  RBD_MIRROR_MODE_POOL      /* mirroring enabled on all journaled images */
} rbd_mirror_mode_t;

typedef struct {
  char *uuid;
  char *cluster_name;
  char *client_name;
} rbd_mirror_peer_t;

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
};

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

/* images */
CEPH_RBD_API int rbd_list(rados_ioctx_t io, char *names, size_t *size);
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

/* pool mirroring */
CEPH_RBD_API int rbd_mirror_mode_get(rados_ioctx_t io_ctx,
                                     rbd_mirror_mode_t *mirror_mode);
CEPH_RBD_API int rbd_mirror_mode_set(rados_ioctx_t io_ctx,
                                     rbd_mirror_mode_t mirror_mode);
CEPH_RBD_API int rbd_mirror_peer_add(rados_ioctx_t io_ctx,
                                     char *uuid, size_t uuid_max_length,
                                     const char *cluster_name,
                                     const char *client_name);
CEPH_RBD_API int rbd_mirror_peer_remove(rados_ioctx_t io_ctx,
                                        const char *uuid);
CEPH_RBD_API int rbd_mirror_peer_list(rados_ioctx_t io_ctx,
                                      rbd_mirror_peer_t *peers, int *max_peers);
CEPH_RBD_API void rbd_mirror_peer_list_cleanup(rbd_mirror_peer_t *peers,
                                               int max_peers);
CEPH_RBD_API int rbd_mirror_peer_set_client(rados_ioctx_t io_ctx,
                                            const char *uuid,
                                            const char *client_name);
CEPH_RBD_API int rbd_mirror_peer_set_cluster(rados_ioctx_t io_ctx,
                                             const char *uuid,
                                             const char *cluster_name);

CEPH_RBD_API int rbd_open(rados_ioctx_t io, const char *name,
                          rbd_image_t *image, const char *snap_name);

CEPH_RBD_API int rbd_aio_open(rados_ioctx_t io, const char *name,
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
CEPH_RBD_API int rbd_aio_open_read_only(rados_ioctx_t io, const char *name,
					rbd_image_t *image, const char *snap_name,
					rbd_completion_t c);
CEPH_RBD_API int rbd_close(rbd_image_t image);
CEPH_RBD_API int rbd_aio_close(rbd_image_t image, rbd_completion_t c);
CEPH_RBD_API int rbd_resize(rbd_image_t image, uint64_t size);
CEPH_RBD_API int rbd_resize_with_progress(rbd_image_t image, uint64_t size,
			     librbd_progress_fn_t cb, void *cbdata);
CEPH_RBD_API int rbd_stat(rbd_image_t image, rbd_image_info_t *info,
                          size_t infosize);
CEPH_RBD_API int rbd_get_old_format(rbd_image_t image, uint8_t *old);
CEPH_RBD_API int rbd_get_size(rbd_image_t image, uint64_t *size);
CEPH_RBD_API int rbd_get_features(rbd_image_t image, uint64_t *features);
CEPH_RBD_API int rbd_update_features(rbd_image_t image, uint64_t features,
                                     uint8_t enabled);
CEPH_RBD_API int rbd_get_stripe_unit(rbd_image_t image, uint64_t *stripe_unit);
CEPH_RBD_API int rbd_get_stripe_count(rbd_image_t image,
                                      uint64_t *stripe_count);
CEPH_RBD_API int rbd_get_overlap(rbd_image_t image, uint64_t *overlap);
CEPH_RBD_API int rbd_get_parent_info(rbd_image_t image,
			             char *parent_poolname, size_t ppoolnamelen,
			             char *parent_name, size_t pnamelen,
			             char *parent_snapname,
                                     size_t psnapnamelen);
CEPH_RBD_API int rbd_get_flags(rbd_image_t image, uint64_t *flags);
CEPH_RBD_API int rbd_set_image_notification(rbd_image_t image, int fd, int type);

/* exclusive lock feature */
CEPH_RBD_API int rbd_is_exclusive_lock_owner(rbd_image_t image, int *is_owner);

/* object map feature */
CEPH_RBD_API int rbd_rebuild_object_map(rbd_image_t image,
                                        librbd_progress_fn_t cb, void *cbdata);

CEPH_RBD_API int rbd_copy(rbd_image_t image, rados_ioctx_t dest_io_ctx,
                          const char *destname);
CEPH_RBD_API int rbd_copy2(rbd_image_t src, rbd_image_t dest);
CEPH_RBD_API int rbd_copy3(rbd_image_t src, rados_ioctx_t dest_io_ctx,
			   const char *destname, rbd_image_options_t dest_opts);
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

/* snapshots */
CEPH_RBD_API int rbd_snap_list(rbd_image_t image, rbd_snap_info_t *snaps,
                               int *max_snaps);
CEPH_RBD_API void rbd_snap_list_end(rbd_snap_info_t *snaps);
CEPH_RBD_API int rbd_snap_create(rbd_image_t image, const char *snapname);
CEPH_RBD_API int rbd_snap_remove(rbd_image_t image, const char *snapname);
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
CEPH_RBD_API int rbd_snap_set(rbd_image_t image, const char *snapname);

CEPH_RBD_API int rbd_flatten(rbd_image_t image);

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
                                       size_t *images_len);

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
CEPH_RBD_API int rbd_aio_write(rbd_image_t image, uint64_t off, size_t len,
                               const char *buf, rbd_completion_t c);

/*
 * @param op_flags: see librados.h constants beginning with LIBRADOS_OP_FLAG
 */
CEPH_RBD_API int rbd_aio_write2(rbd_image_t image, uint64_t off, size_t len,
                                const char *buf, rbd_completion_t c, int op_flags);
CEPH_RBD_API int rbd_aio_read(rbd_image_t image, uint64_t off, size_t len,
                              char *buf, rbd_completion_t c);
/*
 * @param op_flags: see librados.h constants beginning with LIBRADOS_OP_FLAG
 */
CEPH_RBD_API int rbd_aio_read2(rbd_image_t image, uint64_t off, size_t len,
                               char *buf, rbd_completion_t c, int op_flags);
CEPH_RBD_API int rbd_aio_discard(rbd_image_t image, uint64_t off, uint64_t len,
                                 rbd_completion_t c);

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
CEPH_RBD_API int rbd_mirror_image_enable(rbd_image_t image);
CEPH_RBD_API int rbd_mirror_image_disable(rbd_image_t image, bool force);
CEPH_RBD_API int rbd_mirror_image_promote(rbd_image_t image, bool force);
CEPH_RBD_API int rbd_mirror_image_demote(rbd_image_t image);
CEPH_RBD_API int rbd_mirror_image_resync(rbd_image_t image);
CEPH_RBD_API int rbd_mirror_image_get_info(rbd_image_t image,
                                           rbd_mirror_image_info_t *mirror_image_info,
                                           size_t info_size);

#ifdef __cplusplus
}
#endif

#endif
