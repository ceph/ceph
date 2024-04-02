// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * scalable distributed file system
 *
 * Copyright (C) Jeff Layton <jlayton@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_CEPH_LL_CLIENT_H
#define CEPH_CEPH_LL_CLIENT_H
#include <stdint.h>

#ifdef _WIN32
#include "include/win32/fs_compat.h"
#endif

#ifdef __cplusplus
extern "C" {

class Fh;

struct inodeno_t;
struct vinodeno_t;
typedef struct vinodeno_t vinodeno;

#else /* __cplusplus */

typedef struct Fh Fh;

typedef struct inodeno_t {
  uint64_t val;
} inodeno_t;

typedef struct _snapid_t {
  uint64_t val;
} snapid_t;

typedef struct vinodeno_t {
  inodeno_t ino;
  snapid_t snapid;
} vinodeno_t;

#endif /* __cplusplus */

/*
 * Heavily borrowed from David Howells' draft statx patchset.
 *
 * Since the xstat patches are still a work in progress, we borrow its data
 * structures and #defines to implement ceph_getattrx. Once the xstat stuff
 * has been merged we should drop this and switch over to using that instead.
 */
struct ceph_statx {
	uint32_t	stx_mask;
	uint32_t	stx_blksize;
	uint32_t	stx_nlink;
	uint32_t	stx_uid;
	uint32_t	stx_gid;
	uint16_t	stx_mode;
	uint64_t	stx_ino;
	uint64_t	stx_size;
	uint64_t	stx_blocks;
	dev_t		stx_dev;
	dev_t		stx_rdev;
	struct timespec	stx_atime;
	struct timespec	stx_ctime;
	struct timespec	stx_mtime;
	struct timespec	stx_btime;
	uint64_t	stx_version;
};

#define CEPH_STATX_MODE		0x00000001U     /* Want/got stx_mode */
#define CEPH_STATX_NLINK	0x00000002U     /* Want/got stx_nlink */
#define CEPH_STATX_UID		0x00000004U     /* Want/got stx_uid */
#define CEPH_STATX_GID		0x00000008U     /* Want/got stx_gid */
#define CEPH_STATX_RDEV		0x00000010U     /* Want/got stx_rdev */
#define CEPH_STATX_ATIME	0x00000020U     /* Want/got stx_atime */
#define CEPH_STATX_MTIME	0x00000040U     /* Want/got stx_mtime */
#define CEPH_STATX_CTIME	0x00000080U     /* Want/got stx_ctime */
#define CEPH_STATX_INO		0x00000100U     /* Want/got stx_ino */
#define CEPH_STATX_SIZE		0x00000200U     /* Want/got stx_size */
#define CEPH_STATX_BLOCKS	0x00000400U     /* Want/got stx_blocks */
#define CEPH_STATX_BASIC_STATS	0x000007ffU     /* The stuff in the normal stat struct */
#define CEPH_STATX_BTIME	0x00000800U     /* Want/got stx_btime */
#define CEPH_STATX_VERSION	0x00001000U     /* Want/got stx_version */
#define CEPH_STATX_ALL_STATS	0x00001fffU     /* All supported stats */

/*
 * Compatibility macros until these defines make their way into glibc
 */
#ifndef AT_STATX_DONT_SYNC
#define AT_STATX_SYNC_TYPE	0x6000
#define AT_STATX_SYNC_AS_STAT	0x0000
#define AT_STATX_FORCE_SYNC	0x2000
#define AT_STATX_DONT_SYNC	0x4000 /* Don't sync attributes with the server */
#endif

/*
 * This is deprecated and just for backwards compatibility.
 * Please use AT_STATX_DONT_SYNC instead.
 */
#define AT_NO_ATTR_SYNC		AT_STATX_DONT_SYNC /* Deprecated */

/*
 * The statx interfaces only allow these flags. In order to allow us to add
 * others in the future, we disallow setting any that aren't recognized.
 */
#define CEPH_REQ_FLAG_MASK		(AT_SYMLINK_NOFOLLOW|AT_STATX_DONT_SYNC)

/* fallocate mode flags */
#ifndef FALLOC_FL_KEEP_SIZE
#define FALLOC_FL_KEEP_SIZE 0x01
#endif
#ifndef FALLOC_FL_PUNCH_HOLE
#define FALLOC_FL_PUNCH_HOLE 0x02
#endif

/** ceph_deleg_cb_t: Delegation recalls
 *
 * Called when there is an outstanding Delegation and there is conflicting
 * access, either locally or via cap activity.
 * @fh: open filehandle
 * @priv: private info registered when delegation was acquired
 */
typedef void (*ceph_deleg_cb_t)(Fh *fh, void *priv);

/**
 * client_ino_callback_t: Inode data/metadata invalidation
 *
 * Called when the client wants to invalidate the cached data for a range
 * in the file.
 * @handle: client callback handle
 * @ino: vino of inode to be invalidated
 * @off: starting offset of content to be invalidated
 * @len: length of region to invalidate
 */
typedef void (*client_ino_callback_t)(void *handle, vinodeno_t ino,
	      int64_t off, int64_t len);

/**
 * client_dentry_callback_t: Dentry invalidation
 *
 * Called when the client wants to purge a dentry from its cache.
 * @handle: client callback handle
 * @dirino: vino of directory that contains dentry to be invalidate
 * @ino: vino of inode attached to dentry to be invalidated
 * @name: name of dentry to be invalidated
 * @len: length of @name
 */
typedef void (*client_dentry_callback_t)(void *handle, vinodeno_t dirino,
					 vinodeno_t ino, const char *name,
					 size_t len);

/**
 * client_remount_callback_t: Remount entire fs
 *
 * Called when the client needs to purge the dentry cache and the application
 * doesn't have a way to purge an individual dentry. Mostly used for ceph-fuse
 * on older kernels.
 * @handle: client callback handle
 */

typedef int (*client_remount_callback_t)(void *handle);

/**
 * client_switch_interrupt_callback_t: Lock request interrupted
 *
 * Called before file lock request to set the interrupt handler while waiting
 * After the wait, called with "data" set to NULL pointer.
 * @handle: client callback handle
 * @data: opaque data passed to interrupt before call, NULL pointer after.
 */
typedef void (*client_switch_interrupt_callback_t)(void *handle, void *data);

/**
 * client_umask_callback_t: Fetch umask of actor
 *
 * Called when the client needs the umask of the requestor.
 * @handle: client callback handle
 */
typedef mode_t (*client_umask_callback_t)(void *handle);

/**
 * client_ino_release_t: Request that application release Inode references
 *
 * Called when the MDS wants to trim caps and Inode records.
 * @handle: client callback handle
 * @ino: vino of Inode being released
 */
typedef void (*client_ino_release_t)(void *handle, vinodeno_t ino);

/*
 * The handle is an opaque value that gets passed to some callbacks. Any fields
 * set to NULL will be left alone. There is no way to unregister callbacks.
 */
struct ceph_client_callback_args {
  void *handle;
  client_ino_callback_t ino_cb;
  client_dentry_callback_t dentry_cb;
  client_switch_interrupt_callback_t switch_intr_cb;
  client_remount_callback_t remount_cb;
  client_umask_callback_t umask_cb;
  client_ino_release_t ino_release_cb;
};

#ifdef __cplusplus
}
#endif

#endif /* CEPH_STATX_H */

