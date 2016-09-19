// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * scalable distributed file system
 *
 * Copyright (C) Jeff Layton <jlayton@redhat.com>
 *
 * Heavily borrowed from David Howells' draft statx patchset.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CEPH_STATX_H
#define CEPH_CEPH_STATX_H
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Since the xstat patches are still a work in progress, we borrow its data
 * structures and #defines to implement ceph_getattrx. Once the xstat stuff
 * has been merged we should drop this and switch over to using that instead.
 */
struct ceph_statx {
	uint32_t	stx_mask;
	uint32_t	stx_information;
	uint32_t	stx_blksize;
	uint32_t	stx_nlink;
	uint32_t	stx_gen;
	uint32_t	stx_uid;
	uint32_t	stx_gid;
	uint16_t	stx_mode;
	uint16_t	__spare0[1];
	uint64_t	stx_ino;
	uint64_t	stx_size;
	uint64_t	stx_blocks;
	uint64_t	stx_version;
	int64_t		stx_atime;
	int64_t		stx_btime;
	int64_t		stx_ctime;
	int64_t		stx_mtime;
	int32_t		stx_atime_ns;
	int32_t		stx_btime_ns;
	int32_t		stx_ctime_ns;
	int32_t		stx_mtime_ns;
	uint32_t	stx_rdev_major;
	uint32_t	stx_rdev_minor;
	uint32_t	stx_dev_major;
	uint32_t	stx_dev_minor;
	uint64_t	__spare1[16];
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
#define CEPH_STATX_GEN		0x00002000U     /* Want/got stx_gen */
#define CEPH_STATX_ALL_STATS	0x00003fffU     /* All supported stats */

/* statx request flags. Callers can set these in the "flags" field */
#ifndef AT_NO_ATTR_SYNC
#define AT_NO_ATTR_SYNC		0x4000 /* Don't sync attributes with the server */
#endif

#ifdef __cplusplus
}
#endif

#endif /* CEPH_STATX_H */

