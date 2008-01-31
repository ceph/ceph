/* ceph_fs.h
 *
 * C data types to share between kernel and userspace
 */

#ifndef _FS_CEPH_CEPH_FS_H
#define _FS_CEPH_CEPH_FS_H

#ifdef __KERNEL__
# include <linux/in.h>
# include <linux/types.h>
#else
# include <netinet/in.h>
# include "inttypes.h"
# include "byteorder.h"
#endif

#define CEPH_MON_PORT 12345


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

static inline int ceph_fsid_equal(const struct ceph_fsid *a, const struct ceph_fsid *b) {
	return a->major == b->major && a->minor == b->minor;
}


/*
 * ino, object, etc.
 */
typedef __u64 ceph_ino_t;

struct ceph_object {
	__le64 ino;  /* inode "file" identifier */
	__le32 bno;  /* "block" (object) in that "file" */
	__le32 rev;  /* revision.  normally ctime (as epoch). */
};

#define CEPH_INO_ROOT 1

struct ceph_timeval {
	__le32 tv_sec;
	__le32 tv_usec;
};

/*
 * dir fragments
 */ 
typedef __u32 ceph_frag_t;

static inline __u32 frag_make(__u32 b, __u32 v) { return (b << 24) | (v & (0xffffffu >> (24-b))); }
static inline __u32 frag_bits(__u32 f) { return f >> 24; }
static inline __u32 frag_value(__u32 f) { return f & 0xffffffu; }
static inline __u32 frag_mask(__u32 f) { return 0xffffffu >> (24-frag_bits(f)); }
static inline __u32 frag_next(__u32 f) { return frag_make(frag_bits(f), frag_value(f)+1); }

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

#define ceph_pg_is_rep(pg)   (pg.pg.type == CEPH_PG_TYPE_REP)
#define ceph_pg_is_raid4(pg) (pg.pg.type == CEPH_PG_TYPE_RAID4)

/*
 * crush rule ids.  fixme.
 */
#define CRUSH_REP_RULE(nrep) (nrep) 
#define CRUSH_RAID_RULE(num) (10+num)

/*
 * stable_mod func is used to control number of placement groups
 *  b <= bmask and bmask=(2**n)-1
 *  e.g., b=12 -> bmask=15, b=123 -> bmask=127
 */
static inline int ceph_stable_mod(int x, int b, int bmask) {
  if ((x & bmask) < b) 
    return x & bmask;
  else
    return (x & (bmask>>1));
}

/*
 * object layout - how a given object should be stored.
 */
struct ceph_object_layout {
	union ceph_pg ol_pgid;
	__u32         ol_stripe_unit;  
} __attribute__ ((packed));

/*
 * compound epoch+version, used by rados to serialize mutations
 */
struct ceph_eversion {
	ceph_epoch_t epoch;
	__u64        version;
} __attribute__ ((packed));

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

#define CEPH_MSGR_TAG_READY   1  /* server -> client + cseq: ready for messages */
#define CEPH_MSGR_TAG_REJECT  2  /* server -> client + cseq: decline socket */
#define CEPH_MSGR_TAG_MSG     3  /* message */
#define CEPH_MSGR_TAG_ACK     4  /* message ack */
#define CEPH_MSGR_TAG_CLOSE   5  /* closing pipe */


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

#define ceph_entity_addr_equal(a, b)		\
	(memcmp((a), (b), sizeof(*(a))) == 0)

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
	__u32 data_off;  /* sender: include full offset; receiver: mask against ~PAGE_MASK */
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

#define CEPH_MSG_CLIENT_SESSION         22
#define CEPH_MSG_CLIENT_RECONNECT       23

#define CEPH_MSG_CLIENT_REQUEST         24
#define CEPH_MSG_CLIENT_REQUEST_FORWARD 25
#define CEPH_MSG_CLIENT_REPLY           26
#define CEPH_MSG_CLIENT_FILECAPS        0x310

/* osd */
#define CEPH_MSG_OSD_GETMAP       40
#define CEPH_MSG_OSD_MAP          41
#define CEPH_MSG_OSD_OP           42
#define CEPH_MSG_OSD_OPREPLY      43


/* for statfs_reply.  units are KB, objects. */
struct ceph_statfs {
	__le64 f_total;
	__le64 f_free;  // used = total - free
	__le64 f_avail; // usable
	__le64 f_objects;
};

/*
 * mds states 
 *   > 0 -> in
 *  <= 0 -> out
 */
#define CEPH_MDS_STATE_DNE         0  /* down, does not exist. */
#define CEPH_MDS_STATE_STOPPED    -1  /* down, once existed, but no subtrees. empty log. */
#define CEPH_MDS_STATE_DESTROYING -2  /* down, existing, semi-destroyed. */
#define CEPH_MDS_STATE_FAILED      3  /* down, needs to be recovered. */

#define CEPH_MDS_STATE_BOOT       -4  /* up, boot announcement.  destiny unknown. */
#define CEPH_MDS_STATE_STANDBY    -5  /* up, idle.  waiting for assignment by monitor. */
#define CEPH_MDS_STATE_CREATING   -6  /* up, creating MDS instance (new journal, idalloc..). */
#define CEPH_MDS_STATE_STARTING   -7  /* up, starting prior stopped MDS instance. */

#define CEPH_MDS_STATE_REPLAY      8  /* up, starting prior failed instance. scanning journal. */
#define CEPH_MDS_STATE_RESOLVE     9  /* up, disambiguating distributed operations (import, rename, etc.) */
#define CEPH_MDS_STATE_RECONNECT   10 /* up, reconnect to clients */
#define CEPH_MDS_STATE_REJOIN      11 /* up, rejoining distributed cache */
#define CEPH_MDS_STATE_ACTIVE      12 /* up, active */
#define CEPH_MDS_STATE_STOPPING    13 /* up, exporting metadata */


/* client_session */
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
	__le32 op;
	__le64 seq;
	struct ceph_timeval stamp;
} __attribute__ ((packed));

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
	CEPH_MDS_OP_FSYNC = 303,
	CEPH_MDS_OP_CREATE = 304
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
			__s32 uid;
			__s32 gid;
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

/* client file caps */
#define CEPH_CAP_PIN       1  /* no specific capabilities beyond the pin */
#define CEPH_CAP_RDCACHE   2  /* client can cache reads */
#define CEPH_CAP_RD        4  /* client can read */
#define CEPH_CAP_WR        8  /* client can write */
#define CEPH_CAP_WRBUFFER 16  /* client can buffer writes */
#define CEPH_CAP_WREXTEND 32  /* client can extend eof */
#define CEPH_CAP_LAZYIO   64  /* client can perform lazy io */

enum {
	CEPH_CAP_OP_GRANT,   /* mds->client grant */
	CEPH_CAP_OP_ACK,     /* client->mds ack (if prior grant was a recall) */
	CEPH_CAP_OP_REQUEST, /* client->mds request (update wanted bits) */
	CEPH_CAP_OP_EXPORT,  /* mds has exported the cap */
	CEPH_CAP_OP_IMPORT   /* mds has imported the cap from specified mds */
};

struct ceph_mds_file_caps {
	__le32 op;
	__le32 seq;
	__le32 caps, wanted;
	__le64 ino;
	__le64 size;
	__le32 migrate_mds, migrate_seq;
	struct ceph_timeval mtime, atime;
} __attribute__ ((packed));

/* client reconnect */
struct ceph_mds_cap_reconnect {
	__le32 wanted;
	__le32 issued;
	__le64 size;
	struct ceph_timeval mtime, atime;
} __attribute__ ((packed));
/* followed by encoded string */



/*
 * osd ops
 */
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
	CEPH_OSD_OP_MININCLOCK = 26, /* minimum incarnation lock */

	CEPH_OSD_OP_PULL       = 30,
	CEPH_OSD_OP_PUSH       = 31,

	CEPH_OSD_OP_BALANCEREADS   = 101,
	CEPH_OSD_OP_UNBALANCEREADS = 102
};

/*
 * osd op flags
 */
enum {
	CEPH_OSD_OP_ACK = 1,   /* want (or is) "ack" ack */
	CEPH_OSD_OP_SAFE = 2,  /* want (or is) "safe" ack */
	CEPH_OSD_OP_RETRY = 4  /* resend attempt */
};

struct ceph_osd_peer_stat {
	struct ceph_timeval stamp;
	float oprate;
	float qlen;
	float recent_qlen;
	float read_latency;
	float read_latency_mine;
	float frac_rd_ops_shed_in;
	float frac_rd_ops_shed_out;
} __attribute__ ((packed));

struct ceph_osd_request_head {
	struct ceph_entity_inst   client_inst;
	ceph_tid_t                tid;
	__u32                     client_inc;
	__u32                     op;
	__u64                     offset, length;
	struct ceph_object        oid;
	struct ceph_object_layout layout;
	ceph_epoch_t              osdmap_epoch;

	__u32                     flags;

	struct ceph_eversion      reassert_version;

	/* semi-hack, fix me */
	__u32                     shed_count;
	struct ceph_osd_peer_stat peer_stat;
} __attribute__ ((packed));

struct ceph_osd_reply_head {
	ceph_tid_t           tid;
	__u32                op;
	__u32                flags;
	struct ceph_object   oid;
	struct ceph_object_layout layout;
	ceph_epoch_t         osdmap_epoch;
	__s32                result;
	__u64                offset, length;
	struct ceph_eversion reassert_version;
} __attribute__ ((packed));

#endif
