// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/* Based on QEMU block/qcow.cc and block/qcow2.h, which has this license: */

/*
 * Block driver for the QCOW version 2 format
 *
 * Copyright (c) 2004-2006 Fabrice Bellard
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#ifndef CEPH_LIBRBD_MIGRATION_QCOW2_H
#define CEPH_LIBRBD_MIGRATION_QCOW2_H

#include "include/ceph_assert.h"
#include "include/int_types.h"
#include "librbd/migration/QCOW.h"

#define QCOW_MAGIC (('Q' << 24) | ('F' << 16) | ('I' << 8) | 0xfb)

#define QCOW_CRYPT_NONE 0
#define QCOW_CRYPT_AES  1
#define QCOW_CRYPT_LUKS 2

#define QCOW_MAX_CRYPT_CLUSTERS 32
#define QCOW_MAX_SNAPSHOTS 65536

/* Field widths in qcow2 mean normal cluster offsets cannot reach
 * 64PB; depending on cluster size, compressed clusters can have a
 * smaller limit (64PB for up to 16k clusters, then ramps down to
 * 512TB for 2M clusters).  */
#define QCOW_MAX_CLUSTER_OFFSET ((1ULL << 56) - 1)

/* 8 MB refcount table is enough for 2 PB images at 64k cluster size
 * (128 GB for 512 byte clusters, 2 EB for 2 MB clusters) */
#define QCOW_MAX_REFTABLE_SIZE (1ULL << 23)

/* 32 MB L1 table is enough for 2 PB images at 64k cluster size
 * (128 GB for 512 byte clusters, 2 EB for 2 MB clusters) */
#define QCOW_MAX_L1_SIZE (1ULL << 25)

/* Allow for an average of 1k per snapshot table entry, should be plenty of
 * space for snapshot names and IDs */
#define QCOW_MAX_SNAPSHOTS_SIZE (1024 * QCOW_MAX_SNAPSHOTS)

/* Maximum amount of extra data per snapshot table entry to accept */
#define QCOW_MAX_SNAPSHOT_EXTRA_DATA 1024

/* Bitmap header extension constraints */
#define QCOW2_MAX_BITMAPS 65535
#define QCOW2_MAX_BITMAP_DIRECTORY_SIZE (1024 * QCOW2_MAX_BITMAPS)

/* Maximum of parallel sub-request per guest request */
#define QCOW2_MAX_WORKERS 8

/* indicate that the refcount of the referenced cluster is exactly one. */
#define QCOW_OFLAG_COPIED     (1ULL << 63)
/* indicate that the cluster is compressed (they never have the copied flag) */
#define QCOW_OFLAG_COMPRESSED (1ULL << 62)
/* The cluster reads as all zeros */
#define QCOW_OFLAG_ZERO (1ULL << 0)

#define QCOW_EXTL2_SUBCLUSTERS_PER_CLUSTER 32

/* The subcluster X [0..31] is allocated */
#define QCOW_OFLAG_SUB_ALLOC(X)   (1ULL << (X))
/* The subcluster X [0..31] reads as zeroes */
#define QCOW_OFLAG_SUB_ZERO(X)    (QCOW_OFLAG_SUB_ALLOC(X) << 32)
/* Subclusters [X, Y) (0 <= X <= Y <= 32) are allocated */
#define QCOW_OFLAG_SUB_ALLOC_RANGE(X, Y) \
    (QCOW_OFLAG_SUB_ALLOC(Y) - QCOW_OFLAG_SUB_ALLOC(X))
/* Subclusters [X, Y) (0 <= X <= Y <= 32) read as zeroes */
#define QCOW_OFLAG_SUB_ZERO_RANGE(X, Y) \
    (QCOW_OFLAG_SUB_ALLOC_RANGE(X, Y) << 32)
/* L2 entry bitmap with all allocation bits set */
#define QCOW_L2_BITMAP_ALL_ALLOC  (QCOW_OFLAG_SUB_ALLOC_RANGE(0, 32))
/* L2 entry bitmap with all "read as zeroes" bits set */
#define QCOW_L2_BITMAP_ALL_ZEROES (QCOW_OFLAG_SUB_ZERO_RANGE(0, 32))

/* Size of normal and extended L2 entries */
#define QCOW_L2E_SIZE_NORMAL   (sizeof(uint64_t))
#define QCOW_L2E_SIZE_EXTENDED (sizeof(uint64_t) * 2)

/* Size of L1 table entries */
#define QCOW_L1E_SIZE (sizeof(uint64_t))

/* Size of reftable entries */
#define QCOW_REFTABLE_ENTRY_SIZE (sizeof(uint64_t))

#define QCOW_MIN_CLUSTER_BITS 9
#define QCOW_MAX_CLUSTER_BITS 21

/* Defined in the qcow2 spec (compressed cluster descriptor) */
#define QCOW2_COMPRESSED_SECTOR_SIZE 512U
#define QCOW2_COMPRESSED_SECTOR_MASK (~(QCOW2_COMPRESSED_SECTOR_SIZE - 1ULL))

#define QCOW_L2_CACHE_SIZE 16

/* Must be at least 2 to cover COW */
#define QCOW_MIN_L2_CACHE_SIZE 2 /* cache entries */

/* Must be at least 4 to cover all cases of refcount table growth */
#define QCOW_MIN_REFCOUNT_CACHE_SIZE 4 /* clusters */

#define QCOW_DEFAULT_L2_CACHE_MAX_SIZE (1ULL << 25)
#define QCOW_DEFAULT_CACHE_CLEAN_INTERVAL 600  /* seconds */

#define QCOW_DEFAULT_CLUSTER_SIZE 65536

#define QCOW2_OPT_DATA_FILE "data-file"
#define QCOW2_OPT_LAZY_REFCOUNTS "lazy-refcounts"
#define QCOW2_OPT_DISCARD_REQUEST "pass-discard-request"
#define QCOW2_OPT_DISCARD_SNAPSHOT "pass-discard-snapshot"
#define QCOW2_OPT_DISCARD_OTHER "pass-discard-other"
#define QCOW2_OPT_OVERLAP "overlap-check"
#define QCOW2_OPT_OVERLAP_TEMPLATE "overlap-check.template"
#define QCOW2_OPT_OVERLAP_MAIN_HEADER "overlap-check.main-header"
#define QCOW2_OPT_OVERLAP_ACTIVE_L1 "overlap-check.active-l1"
#define QCOW2_OPT_OVERLAP_ACTIVE_L2 "overlap-check.active-l2"
#define QCOW2_OPT_OVERLAP_REFCOUNT_TABLE "overlap-check.refcount-table"
#define QCOW2_OPT_OVERLAP_REFCOUNT_BLOCK "overlap-check.refcount-block"
#define QCOW2_OPT_OVERLAP_SNAPSHOT_TABLE "overlap-check.snapshot-table"
#define QCOW2_OPT_OVERLAP_INACTIVE_L1 "overlap-check.inactive-l1"
#define QCOW2_OPT_OVERLAP_INACTIVE_L2 "overlap-check.inactive-l2"
#define QCOW2_OPT_OVERLAP_BITMAP_DIRECTORY "overlap-check.bitmap-directory"
#define QCOW2_OPT_CACHE_SIZE "cache-size"
#define QCOW2_OPT_L2_CACHE_SIZE "l2-cache-size"
#define QCOW2_OPT_L2_CACHE_ENTRY_SIZE "l2-cache-entry-size"
#define QCOW2_OPT_REFCOUNT_CACHE_SIZE "refcount-cache-size"
#define QCOW2_OPT_CACHE_CLEAN_INTERVAL "cache-clean-interval"

typedef struct QCowHeaderProbe {
    uint32_t magic;
    uint32_t version;
} __attribute__((__packed__)) QCowHeaderProbe;

typedef struct QCowHeaderV1
{
    uint32_t magic;
    uint32_t version;
    uint64_t backing_file_offset;
    uint32_t backing_file_size;
    uint32_t mtime;
    uint64_t size; /* in bytes */
    uint8_t cluster_bits;
    uint8_t l2_bits;
    uint16_t padding;
    uint32_t crypt_method;
    uint64_t l1_table_offset;
} __attribute__((__packed__)) QCowHeaderV1;

typedef struct QCowHeader {
    uint32_t magic;
    uint32_t version;
    uint64_t backing_file_offset;
    uint32_t backing_file_size;
    uint32_t cluster_bits;
    uint64_t size; /* in bytes */
    uint32_t crypt_method;
    uint32_t l1_size; /* XXX: save number of clusters instead ? */
    uint64_t l1_table_offset;
    uint64_t refcount_table_offset;
    uint32_t refcount_table_clusters;
    uint32_t nb_snapshots;
    uint64_t snapshots_offset;

    /* The following fields are only valid for version >= 3 */
    uint64_t incompatible_features;
    uint64_t compatible_features;
    uint64_t autoclear_features;

    uint32_t refcount_order;
    uint32_t header_length;

    /* Additional fields */
    uint8_t compression_type;

    /* header must be a multiple of 8 */
    uint8_t padding[7];
} __attribute__((__packed__)) QCowHeader;

typedef struct QCowSnapshotHeader {
    /* header is 8 byte aligned */
    uint64_t l1_table_offset;

    uint32_t l1_size;
    uint16_t id_str_size;
    uint16_t name_size;

    uint32_t date_sec;
    uint32_t date_nsec;

    uint64_t vm_clock_nsec;

    uint32_t vm_state_size;
    uint32_t extra_data_size; /* for extension */
    /* extra data follows */
    /* id_str follows */
    /* name follows  */
} __attribute__((__packed__)) QCowSnapshotHeader;

typedef struct QCowSnapshotExtraData {
    uint64_t vm_state_size_large;
    uint64_t disk_size;
    uint64_t icount;
} __attribute__((__packed__)) QCowSnapshotExtraData;


typedef struct QCowSnapshot {
    uint64_t l1_table_offset;
    uint32_t l1_size;
    char *id_str;
    char *name;
    uint64_t disk_size;
    uint64_t vm_state_size;
    uint32_t date_sec;
    uint32_t date_nsec;
    uint64_t vm_clock_nsec;
    /* icount value for the moment when snapshot was taken */
    uint64_t icount;
    /* Size of all extra data, including QCowSnapshotExtraData if available */
    uint32_t extra_data_size;
    /* Data beyond QCowSnapshotExtraData, if any */
    void *unknown_extra_data;
} QCowSnapshot;

typedef struct Qcow2CryptoHeaderExtension {
    uint64_t offset;
    uint64_t length;
} __attribute__((__packed__)) Qcow2CryptoHeaderExtension;

typedef struct Qcow2UnknownHeaderExtension {
    uint32_t magic;
    uint32_t len;
    uint8_t data[];
} Qcow2UnknownHeaderExtension;

enum {
    QCOW2_FEAT_TYPE_INCOMPATIBLE    = 0,
    QCOW2_FEAT_TYPE_COMPATIBLE      = 1,
    QCOW2_FEAT_TYPE_AUTOCLEAR       = 2,
};

/* Incompatible feature bits */
enum {
    QCOW2_INCOMPAT_DIRTY_BITNR      = 0,
    QCOW2_INCOMPAT_CORRUPT_BITNR    = 1,
    QCOW2_INCOMPAT_DATA_FILE_BITNR  = 2,
    QCOW2_INCOMPAT_COMPRESSION_BITNR = 3,
    QCOW2_INCOMPAT_EXTL2_BITNR      = 4,
    QCOW2_INCOMPAT_DIRTY            = 1 << QCOW2_INCOMPAT_DIRTY_BITNR,
    QCOW2_INCOMPAT_CORRUPT          = 1 << QCOW2_INCOMPAT_CORRUPT_BITNR,
    QCOW2_INCOMPAT_DATA_FILE        = 1 << QCOW2_INCOMPAT_DATA_FILE_BITNR,
    QCOW2_INCOMPAT_COMPRESSION      = 1 << QCOW2_INCOMPAT_COMPRESSION_BITNR,
    QCOW2_INCOMPAT_EXTL2            = 1 << QCOW2_INCOMPAT_EXTL2_BITNR,

    QCOW2_INCOMPAT_MASK             = QCOW2_INCOMPAT_DIRTY
                                    | QCOW2_INCOMPAT_CORRUPT
                                    | QCOW2_INCOMPAT_DATA_FILE
                                    | QCOW2_INCOMPAT_COMPRESSION
                                    | QCOW2_INCOMPAT_EXTL2,
};

/* Compatible feature bits */
enum {
    QCOW2_COMPAT_LAZY_REFCOUNTS_BITNR = 0,
    QCOW2_COMPAT_LAZY_REFCOUNTS       = 1 << QCOW2_COMPAT_LAZY_REFCOUNTS_BITNR,

    QCOW2_COMPAT_FEAT_MASK            = QCOW2_COMPAT_LAZY_REFCOUNTS,
};

/* Autoclear feature bits */
enum {
    QCOW2_AUTOCLEAR_BITMAPS_BITNR       = 0,
    QCOW2_AUTOCLEAR_DATA_FILE_RAW_BITNR = 1,
    QCOW2_AUTOCLEAR_BITMAPS             = 1 << QCOW2_AUTOCLEAR_BITMAPS_BITNR,
    QCOW2_AUTOCLEAR_DATA_FILE_RAW       = 1 << QCOW2_AUTOCLEAR_DATA_FILE_RAW_BITNR,

    QCOW2_AUTOCLEAR_MASK                = QCOW2_AUTOCLEAR_BITMAPS
                                        | QCOW2_AUTOCLEAR_DATA_FILE_RAW,
};

enum qcow2_discard_type {
    QCOW2_DISCARD_NEVER = 0,
    QCOW2_DISCARD_ALWAYS,
    QCOW2_DISCARD_REQUEST,
    QCOW2_DISCARD_SNAPSHOT,
    QCOW2_DISCARD_OTHER,
    QCOW2_DISCARD_MAX
};

typedef struct Qcow2Feature {
    uint8_t type;
    uint8_t bit;
    char    name[46];
} __attribute__((__packed__)) Qcow2Feature;

typedef struct Qcow2DiscardRegion {
    uint64_t offset;
    uint64_t bytes;
} Qcow2DiscardRegion;

typedef uint64_t Qcow2GetRefcountFunc(const void *refcount_array,
                                      uint64_t index);
typedef void Qcow2SetRefcountFunc(void *refcount_array,
                                  uint64_t index, uint64_t value);

typedef struct Qcow2BitmapHeaderExt {
    uint32_t nb_bitmaps;
    uint32_t reserved32;
    uint64_t bitmap_directory_size;
    uint64_t bitmap_directory_offset;
} __attribute__((__packed__)) Qcow2BitmapHeaderExt;

#define QCOW_RC_CACHE_SIZE QCOW_L2_CACHE_SIZE;

typedef struct Qcow2COWRegion {
    /**
     * Offset of the COW region in bytes from the start of the first cluster
     * touched by the request.
     */
    unsigned    offset;

    /** Number of bytes to copy */
    unsigned    nb_bytes;
} Qcow2COWRegion;

/**
 * Describes an in-flight (part of a) write request that writes to clusters
 * that are not referenced in their L2 table yet.
 */
typedef struct QCowL2Meta
{
    /** Guest offset of the first newly allocated cluster */
    uint64_t offset;

    /** Host offset of the first newly allocated cluster */
    uint64_t alloc_offset;

    /** Number of newly allocated clusters */
    int nb_clusters;

    /** Do not free the old clusters */
    bool keep_old_clusters;

    /**
     * The COW Region between the start of the first allocated cluster and the
     * area the guest actually writes to.
     */
    Qcow2COWRegion cow_start;

    /**
     * The COW Region between the area the guest actually writes to and the
     * end of the last allocated cluster.
     */
    Qcow2COWRegion cow_end;

    /*
     * Indicates that COW regions are already handled and do not require
     * any more processing.
     */
    bool skip_cow;

    /**
     * Indicates that this is not a normal write request but a preallocation.
     * If the image has extended L2 entries this means that no new individual
     * subclusters will be marked as allocated in the L2 bitmap (but any
     * existing contents of that bitmap will be kept).
     */
    bool prealloc;

    /** Pointer to next L2Meta of the same write request */
    struct QCowL2Meta *next;
} QCowL2Meta;

typedef enum QCow2ClusterType {
    QCOW2_CLUSTER_UNALLOCATED,
    QCOW2_CLUSTER_ZERO_PLAIN,
    QCOW2_CLUSTER_ZERO_ALLOC,
    QCOW2_CLUSTER_NORMAL,
    QCOW2_CLUSTER_COMPRESSED,
} QCow2ClusterType;

typedef enum QCow2MetadataOverlap {
    QCOW2_OL_MAIN_HEADER_BITNR      = 0,
    QCOW2_OL_ACTIVE_L1_BITNR        = 1,
    QCOW2_OL_ACTIVE_L2_BITNR        = 2,
    QCOW2_OL_REFCOUNT_TABLE_BITNR   = 3,
    QCOW2_OL_REFCOUNT_BLOCK_BITNR   = 4,
    QCOW2_OL_SNAPSHOT_TABLE_BITNR   = 5,
    QCOW2_OL_INACTIVE_L1_BITNR      = 6,
    QCOW2_OL_INACTIVE_L2_BITNR      = 7,
    QCOW2_OL_BITMAP_DIRECTORY_BITNR = 8,

    QCOW2_OL_MAX_BITNR              = 9,

    QCOW2_OL_NONE             = 0,
    QCOW2_OL_MAIN_HEADER      = (1 << QCOW2_OL_MAIN_HEADER_BITNR),
    QCOW2_OL_ACTIVE_L1        = (1 << QCOW2_OL_ACTIVE_L1_BITNR),
    QCOW2_OL_ACTIVE_L2        = (1 << QCOW2_OL_ACTIVE_L2_BITNR),
    QCOW2_OL_REFCOUNT_TABLE   = (1 << QCOW2_OL_REFCOUNT_TABLE_BITNR),
    QCOW2_OL_REFCOUNT_BLOCK   = (1 << QCOW2_OL_REFCOUNT_BLOCK_BITNR),
    QCOW2_OL_SNAPSHOT_TABLE   = (1 << QCOW2_OL_SNAPSHOT_TABLE_BITNR),
    QCOW2_OL_INACTIVE_L1      = (1 << QCOW2_OL_INACTIVE_L1_BITNR),
    /* NOTE: Checking overlaps with inactive L2 tables will result in bdrv
     * reads. */
    QCOW2_OL_INACTIVE_L2      = (1 << QCOW2_OL_INACTIVE_L2_BITNR),
    QCOW2_OL_BITMAP_DIRECTORY = (1 << QCOW2_OL_BITMAP_DIRECTORY_BITNR),
} QCow2MetadataOverlap;

/* Perform all overlap checks which can be done in constant time */
#define QCOW2_OL_CONSTANT \
    (QCOW2_OL_MAIN_HEADER | QCOW2_OL_ACTIVE_L1 | QCOW2_OL_REFCOUNT_TABLE | \
     QCOW2_OL_SNAPSHOT_TABLE | QCOW2_OL_BITMAP_DIRECTORY)

/* Perform all overlap checks which don't require disk access */
#define QCOW2_OL_CACHED \
    (QCOW2_OL_CONSTANT | QCOW2_OL_ACTIVE_L2 | QCOW2_OL_REFCOUNT_BLOCK | \
     QCOW2_OL_INACTIVE_L1)

/* Perform all overlap checks */
#define QCOW2_OL_ALL \
    (QCOW2_OL_CACHED | QCOW2_OL_INACTIVE_L2)

#define QCOW_L1E_OFFSET_MASK 0x00fffffffffffe00ULL
#define QCOW_L2E_OFFSET_MASK 0x00fffffffffffe00ULL
#define QCOW_L2E_COMPRESSED_OFFSET_SIZE_MASK 0x3fffffffffffffffULL

#define REFT_OFFSET_MASK 0xfffffffffffffe00ULL

#define INV_OFFSET (-1ULL)

static inline uint64_t l2meta_cow_start(QCowL2Meta *m)
{
    return m->offset + m->cow_start.offset;
}

static inline uint64_t l2meta_cow_end(QCowL2Meta *m)
{
    return m->offset + m->cow_end.offset + m->cow_end.nb_bytes;
}

static inline uint64_t refcount_diff(uint64_t r1, uint64_t r2)
{
    return r1 > r2 ? r1 - r2 : r2 - r1;
}

#endif // CEPH_LIBRBD_MIGRATION_QCOW2_H
