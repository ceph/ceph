// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * snap_types.h - Contains types associated with CephFS snapshots that needs
 * to be imported in Cython (Python bindings). Placing these types in ceph_fs.h
 * (where it was initially place) causes an Cython import error.
 *
 * LGPL-2.1 or LGPL-3.0
 */


/*
 * snapshot metadata operations
 *
 * XXX: DON'T FORGET to update cephfs.pyx if any changes are made to this enum.
 */
enum {
        // allows both, snap MD create and update
	CEPH_SNAP_MD_OP_CREATE    = (1 << 0),
        // when passed with CREATE, allows only snap MD create and rejects
        // update
	CEPH_SNAP_MD_OP_EXCL      = (1 << 1),
	CEPH_SNAP_MD_OP_REMOVE    = (1 << 2),
};
