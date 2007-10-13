/* -*- mode:C++; tab-width:8; c-basic-offset:8; indent-tabs-mode:t -*- 
 * vim: ts=8 sw=8 smarttab
 */

/* ceph_fs.h
 *
 * C data types to share between kernel and userspace
 */

#ifndef _FS_CEPH_CEPH_FS_H
#define _FS_CEPH_CEPH_FS_H

typedef u64 ceph_ino_t;

/**
 * object id
 */
struct ceph_object {
  ceph_ino_t ino;  /* inode "file" identifier */
  u32 bno;  /* "block" (object) in that "file" */
  u32 rev;  /* revision.  normally ctime (as epoch). */
};
typedef struct ceph_object ceph_object_t;




/** object layout
 * how objects are mapped into PGs
 */
#define CEPH_OBJECT_LAYOUT_HASH     1
#define CEPH_OBJECT_LAYOUT_LINEAR   2
#define CEPH_OBJECT_LAYOUT_HASHINO  3

/**
 * pg layout -- how PGs are mapped into (sets of) OSDs
 */
#define CEPH_PG_LAYOUT_CRUSH  0   
#define CEPH_PG_LAYOUT_HASH   1
#define CEPH_PG_LAYOUT_LINEAR 2
#define CEPH_PG_LAYOUT_HYBRID 3


/**
 * ceph_file_layout - describe data layout for a file/inode
 */
struct ceph_file_layout {
  /* file -> object mapping */
  __u32 fl_stripe_unit;     /* stripe unit, in bytes.  must be multiple of page size. */
  __u32 fl_stripe_count;    /* over this many objects */
  __u32 fl_object_size;     /* until objects are this big, then move to new objects */

  /* object -> pg layout */
  __u8 fl_pg_type;      /* pg type; see PG_TYPE_* */
  __u8 fl_pg_size;      /* pg size (num replicas, raid stripe width, etc. */
  __u16 __pad;       
  __u32 fl_preferred;   /* preferred primary for pg */

  /* pg -> disk layout */
  __u32 fl_object_stripe_unit;   /* for per-object raid */
};
typedef struct ceph_file_layout ceph_file_layout_t;

#define ceph_file_layout_stripe_width(l) (l.fl_stripe_unit * l.fl_stripe_count)

/* period = bytes before i start on a new set of objects */
#define ceph_file_layout_period(l) (l.fl_object_size * l.fl_stripe_count)



/**
 * placement group id
 */
#define CEPH_PG_TYPE_REP   1
#define CEPH_PG_TYPE_RAID4 2

union ceph_pg {
  u64 pg64;
  struct {
    s32 preferred; /* preferred primary osd */
    u16 ps;        /* placement seed */
    u8 type;
    u8 size;
  } pg;
};
typedef union ceph_pg ceph_pg_t;

#define ceph_pg_is_rep(pg) (pg.pg.type == CEPH_PG_TYPE_REP)
#define ceph_pg_is_raid4(pg) (pg.pg.type == CEPH_PG_TYPE_RAID4)

/**
 * object layout
 *
 * describe how a given object should be stored.
 */
struct ceph_object_layout {
  ceph_pg_t ol_pgid;
  __u32 ol_stripe_unit;  
};
typedef struct ceph_object_layout ceph_object_layout_t;



/**
 * object extent
 */
struct ceph_object_extent {
  ceph_object_t oe_oid;
  u64 oe_start;
  u64 oe_length;
  ceph_object_layout_t oe_object_layout;
  
  /* buffer extent reverse mapping? */
};
typedef ceph_object_extent ceph_object_extent_t;



#endif
