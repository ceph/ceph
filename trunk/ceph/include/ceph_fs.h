/* ceph_fs.h
 *
 * C data types to share between kernel and userspace
 */

#ifndef _FS_CEPH_CEPH_FS_H
#define _FS_CEPH_CEPH_FS_H

#include <linux/types.h>


typedef __u64 ceph_ino_t;


/**
 * object id
 */
struct ceph_object {
	ceph_ino_t ino;  /* inode "file" identifier */
	__u32 bno;  /* "block" (object) in that "file" */
	__u32 rev;  /* revision.  normally ctime (as epoch). */
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
	
	/* pg -> disk layout */
	__u32 fl_object_stripe_unit;   /* for per-object raid */

	/* object -> pg layout */
	__s32 fl_pg_preferred; /* preferred primary for pg */
	__u8  fl_pg_type;      /* pg type; see PG_TYPE_* */
	__u8  fl_pg_size;      /* pg size (num replicas, raid stripe width, etc. */
};

#define ceph_file_layout_stripe_width(l) (l.fl_stripe_unit * l.fl_stripe_count)

/* period = bytes before i start on a new set of objects */
#define ceph_file_layout_period(l) (l.fl_object_size * l.fl_stripe_count)



/**
 * placement group id
 */
#define CEPH_PG_TYPE_REP   1
#define CEPH_PG_TYPE_RAID4 2

union ceph_pg {
	__u64 pg64;
	struct {
		__s32 preferred; /* preferred primary osd */
		__u16 ps;        /* placement seed */
		__u8 type;
		__u8 size;
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



/**
 * object extent
 */
struct ceph_object_extent {
	ceph_object_t oe_oid;
	__u64 oe_start;
	__u64 oe_length;
	struct ceph_object_layout oe_object_layout;
	
	/* buffer extent reverse mapping? */
};





/*********************************************
 * message types
 */

/*
 * entity_name
 */
struct ceph_entity_name {
	__u32 type;
	__u32 num;
};

#define CEPH_ENTITY_TYPE_MON    1
#define CEPH_ENTITY_TYPE_MDS    2
#define CEPH_ENTITY_TYPE_OSD    3
#define CEPH_ENTITY_TYPE_CLIENT 4
#define CEPH_ENTITY_TYPE_ADMIN  5


/*
 * entity_addr
 * ipv4 only for now
 */
struct ceph_entity_addr {
	__u64 nonce;
	__u32 port;
	__u8  ipq[4];
};


struct ceph_entity_inst {
	struct ceph_entity_name name;
	struct ceph_entity_addr addr;
};


/*
 * message header
 */
struct ceph_message_header {
	__u32 type;
	struct ceph_entity_inst src, dst;
	__u32 source_port, dest_port;
	__u32 nchunks;
};

#endif
