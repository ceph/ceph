/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RBD_TYPES_H
#define CEPH_RBD_TYPES_H

#include "include/types.h"
#include "rbd/features.h"

/* New-style rbd image 'foo' consists of objects
 *   rbd_id.foo              - id of image
 *   rbd_header.<id>         - image metadata
 *   rbd_object_map.<id>     - optional image object map
 *   rbd_data.<id>.00000000
 *   rbd_data.<id>.00000001
 *   ...                     - data
 */

#define RBD_HEADER_PREFIX      "rbd_header."
#define RBD_OBJECT_MAP_PREFIX  "rbd_object_map."
#define RBD_DATA_PREFIX        "rbd_data."
#define RBD_ID_PREFIX          "rbd_id."

/*
 * old-style rbd image 'foo' consists of objects
 *   foo.rbd      - image metadata
 *   rb.<idhi>.<idlo>.00000000
 *   rb.<idhi>.<idlo>.00000001
 *   ...          - data
 */

#define RBD_SUFFIX	 	".rbd"
#define RBD_DIRECTORY           "rbd_directory"
#define RBD_INFO                "rbd_info"
#define RBD_NAMESPACE           "rbd_namespace"
#define RBD_TASK                "rbd_task"

/*
 * rbd_children object in each pool contains omap entries
 * that map parent (poolid, imageid, snapid) to a list of children
 * (imageids; snapids aren't required because we get all the snapshot
 * info from a read of the child's header object anyway).
 *
 * The clone operation writes a new item to this child list, and rm or
 * flatten removes an item, and may remove the whole entry if no children
 * exist after the rm/flatten.
 *
 * When attempting to remove a parent, all pools are searched for
 * rbd_children objects with entries referring to that parent; if any
 * exist (and those children exist), the parent removal is prevented.
 */
#define RBD_CHILDREN		"rbd_children"
#define RBD_LOCK_NAME		"rbd_lock"

/**
 * rbd_mirroring object in each pool contains pool-specific settings
 * for configuring mirroring.
 */
#define RBD_MIRRORING       "rbd_mirroring"

/**
 * rbd_mirror_leader and rbd_mirror_instance.<instance id> objects are used
 * for pool-level coordination between rbd-mirror daemons.
 */
#define RBD_MIRROR_LEADER               "rbd_mirror_leader"
#define RBD_MIRROR_INSTANCE_PREFIX      "rbd_mirror_instance."

#define RBD_MAX_OBJ_NAME_SIZE	96
#define RBD_MAX_BLOCK_NAME_SIZE 24

/**
 * Maximum string length of the RBD v2 image id (not including
 * null termination). This limit was derived from the existing
 * RBD_MAX_BLOCK_NAME_SIZE limit which needs to hold the "rbd_data."
 * prefix and null termination.
 */
#define RBD_MAX_IMAGE_ID_LENGTH 14

/**
 * Maximum string length of the RBD block object name prefix (not including
 * null termination).
 *
 * v1 format: rb.<max 8-byte high id>.<max 8-byte low id>.<max 8-byte extra>
 * v2 format: rbd_data.[<max 19-byte pool id>.]<max 14-byte image id>
 *
 * Note: new features might require increasing this maximum prefix length.
 */
#define RBD_MAX_BLOCK_NAME_PREFIX_LENGTH 43

#define RBD_COMP_NONE		0
#define RBD_CRYPT_NONE		0

#define RBD_HEADER_TEXT		"<<< Rados Block Device Image >>>\n"
#define RBD_MIGRATE_HEADER_TEXT	"<<< Migrating RBD Image      >>>\n"
#define RBD_HEADER_SIGNATURE	"RBD"
#define RBD_HEADER_VERSION	"001.005"

#define RBD_GROUP_INVALID_POOL (-1)

#define RBD_GROUP_HEADER_PREFIX "rbd_group_header."

#define RBD_GROUP_DIRECTORY "rbd_group_directory"

#define RBD_TRASH "rbd_trash"

/**
 * MON config-key prefix for storing optional remote cluster connectivity
 * parameters
 */
#define RBD_MIRROR_CONFIG_KEY_PREFIX          "rbd/mirror/"
#define RBD_MIRROR_SITE_NAME_CONFIG_KEY       RBD_MIRROR_CONFIG_KEY_PREFIX "site_name"
#define RBD_MIRROR_PEER_CLIENT_ID_CONFIG_KEY  RBD_MIRROR_CONFIG_KEY_PREFIX "peer_client_id"
#define RBD_MIRROR_PEER_CONFIG_KEY_PREFIX     RBD_MIRROR_CONFIG_KEY_PREFIX "peer/"

struct rbd_info {
	ceph_le64 max_id;
} __attribute__ ((packed));

struct rbd_obj_snap_ondisk {
	ceph_le64 id;
	ceph_le64 image_size;
} __attribute__((packed));

struct rbd_obj_header_ondisk {
	char text[40];
	char block_name[RBD_MAX_BLOCK_NAME_SIZE];
	char signature[4];
	char version[8];
	struct {
		__u8 order;
		__u8 crypt_type;
		__u8 comp_type;
		__u8 unused;
	} __attribute__((packed)) options;
	ceph_le64 image_size;
	ceph_le64 snap_seq;
	ceph_le32 snap_count;
	ceph_le32 reserved;
	ceph_le64 snap_names_len;
	struct rbd_obj_snap_ondisk snaps[0];
} __attribute__((packed));

enum {
  RBD_PROTECTION_STATUS_UNPROTECTED  = 0,
  RBD_PROTECTION_STATUS_UNPROTECTING = 1,
  RBD_PROTECTION_STATUS_PROTECTED    = 2,
  RBD_PROTECTION_STATUS_LAST         = 3
};

#endif
