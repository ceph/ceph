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

#define CG_ID_PREFIX           "cg_id."
#define CG_HEADER_PREFIX       "cg_header."
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

#define CG_DIRECTORY            "cg_directory"
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

#define RBD_MAX_OBJ_NAME_SIZE	96
#define RBD_MAX_BLOCK_NAME_SIZE 24

#define RBD_COMP_NONE		0
#define RBD_CRYPT_NONE		0

#define RBD_HEADER_TEXT		"<<< Rados Block Device Image >>>\n"
#define RBD_HEADER_SIGNATURE	"RBD"
#define RBD_HEADER_VERSION	"001.005"

struct rbd_info {
	__le64 max_id;
} __attribute__ ((packed));

struct rbd_obj_snap_ondisk {
	__le64 id;
	__le64 image_size;
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
	__le64 image_size;
	__le64 snap_seq;
	__le32 snap_count;
	__le32 reserved;
	__le64 snap_names_len;
	struct rbd_obj_snap_ondisk snaps[0];
} __attribute__((packed));


#endif
