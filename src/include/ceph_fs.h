/* ceph_fs.h
 *
 * C data types to share between kernel and userspace
 */

#ifndef _FS_CEPH_CEPH_FS_H
#define _FS_CEPH_CEPH_FS_H

#ifdef __KERNEL__
# include <linux/in.h>
#else
# include <netinet/in.h>
#endif
#include <linux/types.h>

#define CEPH_MON_PORT 2138


typedef __u64 ceph_version_t;
typedef __u64 ceph_tid_t;
typedef __u32 ceph_epoch_t;


/*
 * fs id
 */
struct ceph_fsid {
	__u64 major;
	__u64 minor;
};
typedef struct ceph_fsid ceph_fsid_t;

static inline int ceph_fsid_equal(const ceph_fsid_t *a, const ceph_fsid_t *b) {
	return a->major == b->major && a->minor == b->minor;
}


/*
 * ino, object, etc.
 */
typedef __u64 ceph_ino_t;

struct ceph_object {
	ceph_ino_t ino;  /* inode "file" identifier */
	__u32 bno;  /* "block" (object) in that "file" */
	__u32 rev;  /* revision.  normally ctime (as epoch). */
};
typedef struct ceph_object ceph_object_t;

#define CEPH_INO_ROOT 1

struct ceph_timeval {
	__u32 tv_sec;
	__u32 tv_usec;
};

/*
 * dir fragments
 */ 
typedef __u32 ceph_frag_t;

static inline __u32 frag_make(__u32 b, __u32 v) { return (b << 24) | (v & (0xffffffu >> (24-b))); }
static inline __u32 frag_bits(__u32 f) { return f >> 24; }
static inline __u32 frag_value(__u32 f) { return f & 0xffffffu; }
static inline __u32 frag_mask(__u32 f) { return 0xffffffu >> (24-frag_bits(f)); }
static inline __u32 frag_next(__u32 f) { return (frag_bits(f) << 24) | (frag_value(f)+1); }

/*
 * file caps 
 */
#define CEPH_CAP_RDCACHE   1    // client can safely cache reads
#define CEPH_CAP_RD        2    // client can read
#define CEPH_CAP_WR        4    // client can write
#define CEPH_CAP_WREXTEND  8    // client can extend file
#define CEPH_CAP_WRBUFFER  16   // client can safely buffer writes
#define CEPH_CAP_LAZYIO    32   // client can perform lazy io


/*
 * object layout - how objects are mapped into PGs
 */
#define CEPH_OBJECT_LAYOUT_HASH     1
#define CEPH_OBJECT_LAYOUT_LINEAR   2
#define CEPH_OBJECT_LAYOUT_HASHINO  3

/*
 * pg layout -- how PGs are mapped into (sets of) OSDs
 */
#define CEPH_PG_LAYOUT_CRUSH  0   
#define CEPH_PG_LAYOUT_HASH   1
#define CEPH_PG_LAYOUT_LINEAR 2
#define CEPH_PG_LAYOUT_HYBRID 3

/*
 * ceph_file_layout - describe data layout for a file/inode
 */
struct ceph_file_layout {
	/* file -> object mapping */
	__u32 fl_stripe_unit;     /* stripe unit, in bytes.  must be multiple of page size. */
	__u32 fl_stripe_count;    /* over this many objects */
	__u32 fl_object_size;     /* until objects are this big, then move to new objects */
	
	/* pg -> disk layout */
	__u32 fl_object_stripe_unit;  /* for per-object parity, if any */

	/* object -> pg layout */
	__s32 fl_pg_preferred; /* preferred primary for pg, if any (-1 = none) */
	__u8  fl_pg_type;      /* pg type; see PG_TYPE_* */
	__u8  fl_pg_size;      /* pg size (num replicas, raid stripe width, etc. */
};

#define ceph_file_layout_stripe_width(l) (l.fl_stripe_unit * l.fl_stripe_count)

/* period = bytes before i start on a new set of objects */
#define ceph_file_layout_period(l) (l.fl_object_size * l.fl_stripe_count)

/*
 * placement group
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

#define ceph_pg_is_rep(pg)   (pg.pg.type == CEPH_PG_TYPE_REP)
#define ceph_pg_is_raid4(pg) (pg.pg.type == CEPH_PG_TYPE_RAID4)

/*
 * object layout - how a given object should be stored.
 */
struct ceph_object_layout {
	ceph_pg_t ol_pgid;
	__u32     ol_stripe_unit;  
};


/*
 * object extent
 */
struct ceph_object_extent {
	ceph_object_t oe_oid;
	__u64 oe_start;
	__u64 oe_length;
	struct ceph_object_layout oe_object_layout;
	
	/* buffer extent reverse mapping? */
};

/*
 * compound epoch+version, used by rados to serialize mutations
 */
struct ceph_eversion {
	ceph_epoch_t epoch;
	__u64        version;
} __attribute__ ((packed));
typedef struct ceph_eversion ceph_eversion_t;

/*
 * osd map bits
 */

/* status bits */
#define CEPH_OSD_EXISTS 1
#define CEPH_OSD_UP     2
#define CEPH_OSD_CLEAN  4  /* as in, clean shutdown */

/* offload weights */
#define CEPH_OSD_IN  0
#define CEPH_OSD_OUT 0x10000



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

#define CEPH_MSGR_TAG_READY   1  // server -> client + cseq: ready for messages
#define CEPH_MSGR_TAG_REJECT  2  // server -> client + cseq: decline socket
#define CEPH_MSGR_TAG_MSG     3  // message
#define CEPH_MSGR_TAG_ACK     4  // message ack
#define CEPH_MSGR_TAG_CLOSE   5  // closing pipe


/*
 * entity_addr
 */
struct ceph_entity_addr {
	__u32 erank;  /* entity's rank in process */
	__u32 nonce;  /* unique id for process (e.g. pid) */
	struct sockaddr_in ipaddr;
};

#define ceph_entity_addr_is_local(a,b)					\
	((a).nonce == (b).nonce &&					\
	 (a).ipaddr.sin_addr.s_addr == (b).ipaddr.sin_addr.s_addr)

#define compare_addr(a, b)			\
	((a)->erank == (b)->erank &&		\
	 (a)->nonce == (b)->nonce &&		\
	 memcmp((a), (b), sizeof(*(a)) == 0))



struct ceph_entity_inst {
	struct ceph_entity_name name;
	struct ceph_entity_addr addr;
};


/*
 * message header
 */
struct ceph_msg_header {
	__u32 seq;    /* message seq# for this session */
	__u32 type;   /* message type */
	struct ceph_entity_inst src, dst;
	__u32 front_len;
	__u32 data_off;  /* sender: include full offset; receiver: mask against PAGE_MASK */
	__u32 data_len;  /* bytes of data payload */
} __attribute__ ((packed));


/*
 * message types
 */

/* misc */
#define CEPH_MSG_SHUTDOWN               1
#define CEPH_MSG_PING                   2
#define CEPH_MSG_PING_ACK               3

/* client <-> monitor */
#define CEPH_MSG_MON_MAP                4
#define CEPH_MSG_CLIENT_MOUNT           10
#define CEPH_MSG_CLIENT_UNMOUNT         11
#define CEPH_MSG_STATFS                 12
#define CEPH_MSG_STATFS_REPLY           13

/* client <-> mds */
#define CEPH_MSG_MDS_GETMAP                  20
#define CEPH_MSG_MDS_MAP                     21

#define CEPH_MSG_CLIENT_SESSION         22   // start or stop
#define CEPH_MSG_CLIENT_RECONNECT       23

#define CEPH_MSG_CLIENT_REQUEST         24
#define CEPH_MSG_CLIENT_REQUEST_FORWARD 25
#define CEPH_MSG_CLIENT_REPLY           26
#define CEPH_MSG_CLIENT_FILECAPS        0x310  // 

/* osd */
#define CEPH_MSG_OSD_GETMAP       40
#define CEPH_MSG_OSD_MAP          41
#define CEPH_MSG_OSD_OP           42    // delete, etc.
#define CEPH_MSG_OSD_OPREPLY      43    // delete, etc.


/* client_session message op values */
enum {
	CEPH_SESSION_REQUEST_OPEN,
	CEPH_SESSION_OPEN,
	CEPH_SESSION_REQUEST_CLOSE,
	CEPH_SESSION_CLOSE,
	CEPH_SESSION_REQUEST_RENEWCAPS,
	CEPH_SESSION_RENEWCAPS,
	CEPH_SESSION_STALE,           // caps not renewed.
	CEPH_SESSION_REQUEST_RESUME,
	CEPH_SESSION_RESUME	
};

struct ceph_mds_session_head {
	__u32 op;
	__u64 seq;
	struct ceph_timeval stamp;
};

/* client_request */
enum {
	CEPH_MDS_OP_STAT = 100,
	CEPH_MDS_OP_LSTAT = 101,
	CEPH_MDS_OP_FSTAT = 102,
	CEPH_MDS_OP_UTIME = 1103,
	CEPH_MDS_OP_CHMOD = 1104,
	CEPH_MDS_OP_CHOWN = 1105,

	CEPH_MDS_OP_READDIR = 200,
	CEPH_MDS_OP_MKNOD = 1201,
	CEPH_MDS_OP_LINK = 1202,
	CEPH_MDS_OP_UNLINK = 1203,
	CEPH_MDS_OP_RENAME = 1204,

	CEPH_MDS_OP_MKDIR = 1220,
	CEPH_MDS_OP_RMDIR = 1221,
	CEPH_MDS_OP_SYMLINK = 1222,

	CEPH_MDS_OP_OPEN = 301,
	CEPH_MDS_OP_TRUNCATE = 1303,
	CEPH_MDS_OP_FSYNC = 303
};

struct ceph_mds_request_head {
	struct ceph_entity_inst client_inst;
	ceph_tid_t tid, oldest_client_tid;
	__u64 mdsmap_epoch; /* on client */
	__u32 num_fwd;
	__u32 retry_attempt;
	ceph_ino_t mds_wants_replica_in_dirino;
	__u32 op;
	__u32 caller_uid, caller_gid;
	ceph_ino_t cwd_ino;

	// fixed size arguments.  in a union.
	union { 
		struct {
			__u32 mask;
		} stat;
		struct {
			__u32 mask;
		} fstat;
		struct {
			ceph_frag_t frag;
		} readdir;
		struct {
			struct ceph_timeval mtime;
			struct ceph_timeval atime;
		} utime;
		struct {
			__u32 mode;
		} chmod; 
		struct {
			uid_t uid;
			gid_t gid;
		} chown;
		struct {
			__u32 mode;
			__u32 rdev;
		} mknod; 
		struct {
			__u32 mode;
		} mkdir; 
		struct {
			__u32 flags;
			__u32 mode;
		} open;
		struct {
			__s64 length;
		} truncate;
	} args;
} __attribute__ ((packed));


/* client reply */
struct ceph_mds_reply_head {
	ceph_tid_t tid;
	__u32 op;
	__s32 result;
	__u32 file_caps;
	__u32 file_caps_seq;
	__u64 mdsmap_epoch;
} __attribute__ ((packed));

struct ceph_frag_tree_head {
	__u32 nsplits;
	__s32 splits[0];
} __attribute__ ((packed));

struct ceph_mds_reply_inode {
	ceph_ino_t ino;
	struct ceph_file_layout layout;
	struct ceph_timeval ctime, mtime, atime;
	__u32 mode, uid, gid;
	__u32 nlink;
	__u64 size;
	__u32 rdev;
	__u32 mask;
	struct ceph_frag_tree_head fragtree;
} __attribute__ ((packed));
/* followed by frag array, then symlink string */

struct ceph_mds_reply_dirfrag {
	__u32 frag;
	__s32 auth;
	__u8 is_rep;
	__u32 ndist;
	__u32 dist[];
} __attribute__ ((packed));




/*
 * osd ops
 */
struct ceph_osd_reqid {
	struct ceph_entity_name name; /* who */
	__u32                   inc;  /* incarnation */
	ceph_tid_t              tid;
} __attribute__ ((packed));
typedef struct ceph_osd_reqid ceph_osd_reqid_t;

enum {
	CEPH_OSD_OP_READ       = 1,
	CEPH_OSD_OP_STAT       = 2,
	CEPH_OSD_OP_REPLICATE  = 3,
	CEPH_OSD_OP_UNREPLICATE = 4,
	CEPH_OSD_OP_WRNOOP     = 10,
	CEPH_OSD_OP_WRITE      = 11,
	CEPH_OSD_OP_DELETE     = 12,
	CEPH_OSD_OP_TRUNCATE   = 13,
	CEPH_OSD_OP_ZERO       = 14,

	CEPH_OSD_OP_WRLOCK     = 20,
	CEPH_OSD_OP_WRUNLOCK   = 21,
	CEPH_OSD_OP_RDLOCK     = 22,
	CEPH_OSD_OP_RDUNLOCK   = 23,
	CEPH_OSD_OP_UPLOCK     = 24,
	CEPH_OSD_OP_DNLOCK     = 25,
	CEPH_OSD_OP_MININCLOCK = 26, // minimum incarnation lock

	CEPH_OSD_OP_PULL       = 30,
	CEPH_OSD_OP_PUSH       = 31,

	CEPH_OSD_OP_BALANCEREADS   = 101,
	CEPH_OSD_OP_UNBALANCEREADS = 102
};

enum {
	CEPH_OSD_OP_WANT_ACK,
	CEPH_OSD_OP_WANT_SAFE,
	CEPH_OSD_OP_IS_RETRY
};

struct ceph_osd_request_head {
	struct ceph_entity_inst   client;
	ceph_osd_reqid_t          reqid;
	__u32                     op;
	__u64                     offset, length;
	ceph_object_t             oid;
	struct ceph_object_layout layout;
	ceph_epoch_t              osdmap_epoch;

	__u32                     flags;

	/* hack, fix me */
	ceph_tid_t      rep_tid;   
	ceph_eversion_t pg_trim_to;
	__u32 shed_count;
	//osd_peer_stat_t peer_stat;
} __attribute__ ((packed));

#endif
