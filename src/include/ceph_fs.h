/* ceph_fs.h
 *
 * C data types to share between kernel and userspace
 */

#ifndef _FS_CEPH_CEPH_FS_H
#define _FS_CEPH_CEPH_FS_H

#ifdef __KERNEL__
# include <linux/in.h>
# include <linux/types.h>
# include <asm/fcntl.h>
#else
# include <netinet/in.h>
# include "inttypes.h"
# include "byteorder.h"
# include <fcntl.h>
#endif

#define CEPH_MON_PORT 12345
#define CEPH_FILE_MAX_SIZE (1ULL << 40) // 1 TB

/*
 * types in this file are defined as little-endian, and are
 * primarily intended to describe data structures that pass
 * over the wire or that are stored on disk.
 */


/*
 * some basics
 */
typedef __le64 ceph_version_t;
typedef __le64 ceph_tid_t;
typedef __le32 ceph_epoch_t;


/*
 * fs id
 */
struct ceph_fsid {
	__le64 major;
	__le64 minor;
} __attribute__ ((packed));

static inline int ceph_fsid_equal(const struct ceph_fsid *a, const struct ceph_fsid *b) {
	return a->major == b->major && a->minor == b->minor;
}


/*
 * ino, object, etc.
 */
typedef __le64 ceph_ino_t;

struct ceph_object {
	__le64 ino;  /* inode "file" identifier */
	__le32 bno;  /* "block" (object) in that "file" */
	__le32 rev;  /* revision.  normally ctime (as epoch). */
};

#define CEPH_INO_ROOT 1

struct ceph_timespec {
	__le32 tv_sec;
	__le32 tv_nsec;
} __attribute__ ((packed));

/*
 * dir fragments
 */
static inline __u32 frag_make(__u32 b, __u32 v) { return (b << 24) | (v & (0xffffffu >> (24-b))); }
static inline __u32 frag_bits(__u32 f) { return f >> 24; }
static inline __u32 frag_value(__u32 f) { return f & 0xffffffu; }
static inline __u32 frag_mask(__u32 f) { return 0xffffffu >> (24-frag_bits(f)); }
static inline __u32 frag_next(__u32 f) { return frag_make(frag_bits(f), frag_value(f)+1); }
static inline bool frag_is_leftmost(__u32 f) {
	return frag_value(f) == 0;
}
static inline bool frag_is_rightmost(__u32 f) {
	return frag_value(f) == frag_mask(f);
}
static inline int frag_compare(__u32 a, __u32 b) {
	unsigned va = frag_value(a);
	unsigned vb = frag_value(b);
	if (va < vb)
		return -1;
	if (va > vb)
		return 1;
	va = frag_bits(a);
	vb = frag_bits(b);
	if (va < vb)
		return -1;
	if (va > vb)
		return 1;
	return 0;
}
static inline bool frag_contains_value(__u32 f, __u32 v)
{
	return (v & frag_mask(f)) == frag_value(f);
}


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
	__le32 fl_stripe_unit;     /* stripe unit, in bytes.  must be multiple of page size. */
	__le32 fl_stripe_count;    /* over this many objects */
	__le32 fl_object_size;     /* until objects are this big, then move to new objects */
	__le32 fl_cas_hash;        /* 0 = none; 1 = sha256 */

	/* pg -> disk layout */
	__le32 fl_object_stripe_unit;  /* for per-object parity, if any */

	/* object -> pg layout */
	__le32 fl_pg_preferred; /* preferred primary for pg, if any (-1 = none) */
	__u8  fl_pg_type;      /* pg type; see PG_TYPE_* */
	__u8  fl_pg_size;      /* pg size (num replicas, raid stripe width, etc. */
	__u8  fl_pg_pool;      /* implies crush ruleset AND object namespace */
} __attribute__ ((packed));

#define ceph_file_layout_su(l) ((__s32)le32_to_cpu((l).fl_stripe_unit))
#define ceph_file_layout_stripe_count(l) ((__s32)le32_to_cpu((l).fl_stripe_count))
#define ceph_file_layout_object_size(l) ((__s32)le32_to_cpu((l).fl_object_size))
#define ceph_file_layout_cas_hash(l) ((__s32)le32_to_cpu((l).fl_cas_hash))
#define ceph_file_layout_object_su(l) ((__s32)le32_to_cpu((l).fl_object_stripe_unit))
#define ceph_file_layout_pg_preferred(l) ((__s32)le32_to_cpu((l).fl_pg_preferred))

#define ceph_file_layout_stripe_width(l) (le32_to_cpu((l).fl_stripe_unit) * \
					  le32_to_cpu((l).fl_stripe_count))

/* period = bytes before i start on a new set of objects */
#define ceph_file_layout_period(l) (le32_to_cpu((l).fl_object_size) *	\
				    le32_to_cpu((l).fl_stripe_count))

/*
 * placement group.
 * we encode this into one __le64.
 */
#define CEPH_PG_TYPE_REP   1
#define CEPH_PG_TYPE_RAID4 2
union ceph_pg {
	__u64 pg64;
	struct {
		__s16 preferred; /* preferred primary osd */
		__u16 ps;        /* placement seed */
		__u8 pool;       /* implies crush ruleset */
		__u8 type;
		__u8 size;
		__u8 __pad;
	} pg;
} __attribute__ ((packed));

#define ceph_pg_is_rep(pg)   (pg.pg.type == CEPH_PG_TYPE_REP)
#define ceph_pg_is_raid4(pg) (pg.pg.type == CEPH_PG_TYPE_RAID4)

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
	__le64 ol_pgid;           /* raw pg, with _full_ ps precision. */
	__le32 ol_stripe_unit;
} __attribute__ ((packed));

/*
 * compound epoch+version, used by rados to serialize mutations
 */
struct ceph_eversion {
	ceph_epoch_t epoch;
	__le64       version;
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



/*
 * string hash.
 *
 * taken from Linux, tho we should probably take care to use this one
 * in case the upstream hash changes.
 */

/* Name hashing routines. Initial hash value */
/* Hash courtesy of the R5 hash in reiserfs modulo sign bits */
#define ceph_init_name_hash()		0

/* partial hash update function. Assume roughly 4 bits per character */
static inline unsigned long
ceph_partial_name_hash(unsigned long c, unsigned long prevhash)
{
	return (prevhash + (c << 4) + (c >> 4)) * 11;
}

/*
 * Finally: cut down the number of bits to a int value (and try to avoid
 * losing bits)
 */
static inline unsigned long ceph_end_name_hash(unsigned long hash)
{
	return (unsigned int) hash;
}

/* Compute the hash for a name string. */
static inline unsigned int
ceph_full_name_hash(const unsigned char *name, unsigned int len)
{
	unsigned long hash = ceph_init_name_hash();
	while (len--)
		hash = ceph_partial_name_hash(*name++, hash);
	return ceph_end_name_hash(hash);
}



/*********************************************
 * message types
 */

/*
 * entity_name
 */
struct ceph_entity_name {
	__le32 type;
	__le32 num;
} __attribute__ ((packed));

#define CEPH_ENTITY_TYPE_MON    1
#define CEPH_ENTITY_TYPE_MDS    2
#define CEPH_ENTITY_TYPE_OSD    3
#define CEPH_ENTITY_TYPE_CLIENT 4
#define CEPH_ENTITY_TYPE_ADMIN  5

#define CEPH_MSGR_TAG_READY         1  /* server -> client: ready for messages */
#define CEPH_MSGR_TAG_RESETSESSION  2  /* server -> client: reset, try again */
#define CEPH_MSGR_TAG_WAIT          3  /* server -> client: wait for racing incoming connection */
#define CEPH_MSGR_TAG_RETRY         4  /* server -> client + cseq: try again with higher cseq */
#define CEPH_MSGR_TAG_CLOSE         5  /* closing pipe */
#define CEPH_MSGR_TAG_MSG          10  /* message */
#define CEPH_MSGR_TAG_ACK          11  /* message ack */


/*
 * entity_addr
 */
struct ceph_entity_addr {
	__le32 erank;  /* entity's rank in process */
	__le32 nonce;  /* unique id for process (e.g. pid) */
	struct sockaddr_in ipaddr;
} __attribute ((packed));

#define ceph_entity_addr_is_local(a,b)					\
	(le32_to_cpu((a).nonce) == le32_to_cpu((b).nonce) &&		\
	 (a).ipaddr.sin_addr.s_addr == (b).ipaddr.sin_addr.s_addr)

#define ceph_entity_addr_equal(a, b)		\
	(memcmp((a), (b), sizeof(*(a))) == 0)

struct ceph_entity_inst {
	struct ceph_entity_name name;
	struct ceph_entity_addr addr;
} __attribute__ ((packed));


/*
 * message header, footer
 */
struct ceph_msg_header {
	__le64 seq;    /* message seq# for this session */
	__le32 type;   /* message type */
	__le32 front_len;
	__le32 data_off;  /* sender: include full offset; receiver: mask against ~PAGE_MASK */
	__le32 data_len;  /* bytes of data payload */
	struct ceph_entity_inst src, dst;
} __attribute__ ((packed));

struct ceph_msg_footer {
	__le32 aborted;
	__le32 csum;
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
#define CEPH_MSG_MON_GET_MAP            5
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
#define CEPH_MSG_CLIENT_LEASE           0x311

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

struct ceph_osd_getmap {
	struct ceph_fsid fsid;
	__le32 start;
} __attribute__ ((packed));

struct ceph_mds_getmap {
	struct ceph_fsid fsid;
	__le32 want;
} __attribute__ ((packed));


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


/*
 * metadata lock types.
 *  - these are bitmasks.. we can compose them
 *  - they also define the lock ordering by the MDS
 *  - a few of these are internal to the mds
 */
#define CEPH_LOCK_DN          1
#define CEPH_LOCK_IVERSION    2     /* mds internal */
#define CEPH_LOCK_IFILE       4     /* mds internal */
#define CEPH_LOCK_IAUTH       8
#define CEPH_LOCK_ILINK       16
#define CEPH_LOCK_IDFT        32    /* dir frag tree */
#define CEPH_LOCK_IDIR        64    /* mds internal */
#define CEPH_LOCK_IXATTR      128
#define CEPH_LOCK_INO         256   /* immutable inode bits; not actually a lock */

#define CEPH_LOCK_ICONTENT    (CEPH_LOCK_IFILE|CEPH_LOCK_IDIR)  /* alias for either filelock or dirlock */

/*
 * stat masks are defined in terms of the locks that cover inode fields.
 */
#define CEPH_STAT_MASK_INODE    CEPH_LOCK_INO
#define CEPH_STAT_MASK_TYPE     CEPH_LOCK_INO  /* mode >> 12 */
#define CEPH_STAT_MASK_SYMLINK  CEPH_LOCK_INO
#define CEPH_STAT_MASK_LAYOUT   CEPH_LOCK_INO
#define CEPH_STAT_MASK_UID      CEPH_LOCK_IAUTH
#define CEPH_STAT_MASK_GID      CEPH_LOCK_IAUTH
#define CEPH_STAT_MASK_MODE     CEPH_LOCK_IAUTH
#define CEPH_STAT_MASK_NLINK    CEPH_LOCK_ILINK
#define CEPH_STAT_MASK_MTIME    CEPH_LOCK_ICONTENT
#define CEPH_STAT_MASK_SIZE     CEPH_LOCK_ICONTENT
#define CEPH_STAT_MASK_ATIME    CEPH_LOCK_ICONTENT  /* fixme */
#define CEPH_STAT_MASK_XATTR    CEPH_LOCK_IXATTR
#define CEPH_STAT_MASK_INODE_ALL (CEPH_LOCK_ICONTENT|CEPH_LOCK_IAUTH|CEPH_LOCK_ILINK|CEPH_LOCK_INO)

#define CEPH_UTIME_ATIME		1
#define CEPH_UTIME_MTIME		2
#define CEPH_UTIME_CTIME		4

/* client_session */
enum {
	CEPH_SESSION_REQUEST_OPEN,
	CEPH_SESSION_OPEN,
	CEPH_SESSION_REQUEST_CLOSE,
	CEPH_SESSION_CLOSE,
	CEPH_SESSION_REQUEST_RENEWCAPS,
	CEPH_SESSION_RENEWCAPS,
	CEPH_SESSION_STALE,
};

struct ceph_mds_session_head {
	__le32 op;
	__le64 seq;
	struct ceph_timespec stamp;
} __attribute__ ((packed));

/* client_request */
/*
 * mds ops.  
 *  & 0x1000  -> write op
 *  & 0x10000 -> follow symlink (e.g. stat(), not lstat()).
 &  & 0x100000 -> use weird ino/path trace
 */
#define CEPH_MDS_OP_WRITE        0x01000
#define CEPH_MDS_OP_FOLLOW_LINK  0x10000
#define CEPH_MDS_OP_INO_PATH  0x100000
enum {
	CEPH_MDS_OP_FINDINODE = 0x100100,

	CEPH_MDS_OP_LSTAT     = 0x00100,
	CEPH_MDS_OP_LUTIME    = 0x01101,
	CEPH_MDS_OP_LCHMOD    = 0x01102,
	CEPH_MDS_OP_LCHOWN    = 0x01103,

	CEPH_MDS_OP_STAT      = 0x10100,
	CEPH_MDS_OP_UTIME     = 0x11101,
	CEPH_MDS_OP_CHMOD     = 0x11102,
	CEPH_MDS_OP_CHOWN     = 0x11103,

	CEPH_MDS_OP_MKNOD     = 0x01201,
	CEPH_MDS_OP_LINK      = 0x01202,
	CEPH_MDS_OP_UNLINK    = 0x01203,
	CEPH_MDS_OP_RENAME    = 0x01204,
	CEPH_MDS_OP_MKDIR     = 0x01220,
	CEPH_MDS_OP_RMDIR     = 0x01221,
	CEPH_MDS_OP_SYMLINK   = 0x01222,

	CEPH_MDS_OP_OPEN      = 0x10302,
	CEPH_MDS_OP_TRUNCATE  = 0x11303,
	CEPH_MDS_OP_LTRUNCATE = 0x01303,
	CEPH_MDS_OP_FSYNC     = 0x00304,
	CEPH_MDS_OP_READDIR   = 0x00305,
};

struct ceph_mds_request_head {
	struct ceph_entity_inst client_inst;
	ceph_tid_t tid, oldest_client_tid;
	ceph_epoch_t mdsmap_epoch; /* on client */
	__le32 num_fwd;
	__le32 retry_attempt;
	ceph_ino_t mds_wants_replica_in_dirino;
	__le32 op;
	__le32 caller_uid, caller_gid;

	union {
		struct {
			__le32 mask;
		} __attribute__ ((packed)) stat;
		struct {
			__le32 mask;
		} __attribute__ ((packed)) fstat;
		struct {
			__le32 frag;
		} __attribute__ ((packed)) readdir;
		struct {
			struct ceph_timespec mtime;
			struct ceph_timespec atime;
			struct ceph_timespec ctime;
			__le32 mask;
		} __attribute__ ((packed)) utime;
		struct {
			__le32 mode;
		} __attribute__ ((packed)) chmod;
		struct {
			__le32 uid;
			__le32 gid;
		} __attribute__ ((packed)) chown;
		struct {
			__le32 mode;
			__le32 rdev;
		} __attribute__ ((packed)) mknod;
		struct {
			__le32 mode;
		} __attribute__ ((packed)) mkdir;
		struct {
			__le32 flags;
			__le32 mode;
		} __attribute__ ((packed)) open;
		struct {
			__le64 length;
		} __attribute__ ((packed)) truncate;
	} __attribute__ ((packed)) args;
} __attribute__ ((packed));

struct ceph_inopath_item {
	__le64 ino;
	__le32 dname_hash;
} __attribute__ ((packed));

/* client reply */
struct ceph_mds_reply_head {
	ceph_tid_t tid;
	__le32 op;
	__le32 result;
	__le32 file_caps;
	__le32 file_caps_seq;
	__le32 mdsmap_epoch;
} __attribute__ ((packed));

/*
 * one for each node split
 */
struct ceph_frag_tree_split {
	__le32 frag;
	__le32 by;
} __attribute__ ((packed));

struct ceph_frag_tree_head {
	__le32 nsplits;
	struct ceph_frag_tree_split splits[0];
} __attribute__ ((packed));

struct ceph_mds_reply_inode {
	ceph_ino_t ino;
	__le64 version;
	struct ceph_file_layout layout;
	struct ceph_timespec ctime, mtime, atime;
	__le64 time_warp_seq;
	__le32 mode, uid, gid;
	__le32 nlink;
	__le64 size, max_size;
	__le32 rdev;
	struct ceph_frag_tree_head fragtree;
} __attribute__ ((packed));
/* followed by frag array, then symlink string */

/* reply_lease follows dname, and reply_inode */
struct ceph_mds_reply_lease {
	__le16 mask;
	__le32 duration_ms;
} __attribute__ ((packed));

struct ceph_mds_reply_dirfrag {
	__le32 frag;
	__le32 auth;
	__le32 ndist;
	__le32 dist[];
} __attribute__ ((packed));

/* file access modes */
#define CEPH_FILE_MODE_PIN        0
#define CEPH_FILE_MODE_RD         1
#define CEPH_FILE_MODE_WR         2
#define CEPH_FILE_MODE_RDWR       3  /* RD | WR */
#define CEPH_FILE_MODE_LAZY       4
#define CEPH_FILE_MODE_NUM        8  /* bc these are bit fields.. mostly */

static inline int ceph_flags_to_mode(int flags)
{
	if ((flags & O_DIRECTORY) == O_DIRECTORY)
		return CEPH_FILE_MODE_PIN;
#ifdef O_LAZY
	if (flags & O_LAZY)
		return CEPH_FILE_MODE_LAZY;
#endif
	if ((flags & O_APPEND) == O_APPEND)
		flags |= O_WRONLY;

	flags &= O_ACCMODE;
	if ((flags & O_RDWR) == O_RDWR)
		return CEPH_FILE_MODE_RDWR;
	if ((flags & O_WRONLY) == O_WRONLY)
		return CEPH_FILE_MODE_WR;
	return CEPH_FILE_MODE_RD;
}

/* client file caps */
#define CEPH_CAP_PIN       1  /* no specific capabilities beyond the pin */
#define CEPH_CAP_RDCACHE   2  /* client can cache reads */
#define CEPH_CAP_RD        4  /* client can read */
#define CEPH_CAP_WR        8  /* client can write */
#define CEPH_CAP_WRBUFFER 16  /* client can buffer writes */
#define CEPH_CAP_WREXTEND 32  /* client can extend eof */
#define CEPH_CAP_LAZYIO   64  /* client can perform lazy io */
#define CEPH_CAP_EXCL    128  /* exclusive/loner access */

static inline int ceph_caps_for_mode(int mode)
{
	switch (mode) {
	case CEPH_FILE_MODE_PIN:
		return CEPH_CAP_PIN;
	case CEPH_FILE_MODE_RD:
		return CEPH_CAP_PIN |
			CEPH_CAP_RD | CEPH_CAP_RDCACHE;
	case CEPH_FILE_MODE_RDWR:
		return CEPH_CAP_PIN |
			CEPH_CAP_RD | CEPH_CAP_RDCACHE |
			CEPH_CAP_WR | CEPH_CAP_WRBUFFER |
			CEPH_CAP_EXCL;
	case CEPH_FILE_MODE_WR:
		return CEPH_CAP_PIN |
			CEPH_CAP_WR | CEPH_CAP_WRBUFFER |
			CEPH_CAP_EXCL;
	}
	return 0;
}

enum {
	CEPH_CAP_OP_GRANT,   /* mds->client grant */
	CEPH_CAP_OP_ACK,     /* client->mds ack (if prior grant was a recall) */
	CEPH_CAP_OP_REQUEST, /* client->mds request (update wanted bits) */
	CEPH_CAP_OP_TRUNC,   /* mds->client trunc notify (invalidate size+mtime) */
	CEPH_CAP_OP_EXPORT,  /* mds has exported the cap */
	CEPH_CAP_OP_IMPORT   /* mds has imported the cap from specified mds */
};

struct ceph_mds_file_caps {
	__le32 op;
	__le32 seq;
	__le32 caps, wanted;
	__le64 ino;
	__le64 size, max_size;
	__le32 migrate_mds, migrate_seq;
	struct ceph_timespec mtime, atime, ctime;
	__le64 time_warp_seq;
} __attribute__ ((packed));


#define CEPH_MDS_LEASE_REVOKE  1  /*    mds  -> client */
#define CEPH_MDS_LEASE_RELEASE 2  /* client  -> mds    */
#define CEPH_MDS_LEASE_RENEW   3  /* client <-> mds    */

struct ceph_mds_lease {
	__u8 action;
	__le16 mask;
	__le64 ino;
} __attribute__ ((packed));
/* followed by a __le32+string for dname */

/* client reconnect */
struct ceph_mds_cap_reconnect {
	__le32 wanted;
	__le32 issued;
	__le64 size;
	struct ceph_timespec mtime, atime;
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

	CEPH_OSD_OP_PULL       = 30,
	CEPH_OSD_OP_PUSH       = 31,

	CEPH_OSD_OP_BALANCEREADS   = 101,
	CEPH_OSD_OP_UNBALANCEREADS = 102
};

/*
 * osd op flags
 */
enum {
	CEPH_OSD_OP_ACK = 1,          /* want (or is) "ack" ack */
	CEPH_OSD_OP_SAFE = 2,         /* want (or is) "safe" ack */
	CEPH_OSD_OP_RETRY = 4,        /* resend attempt */
	CEPH_OSD_OP_INCLOCK_FAIL = 8, /* fail on inclock collision */
	CEPH_OSD_OP_BALANCE_READS = 16
};

struct ceph_osd_peer_stat {
	struct ceph_timespec stamp;
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
	__le32                    client_inc;
	__le32                    op;
	__le64                    offset, length;
	struct ceph_object        oid;
	struct ceph_object_layout layout;
	ceph_epoch_t              osdmap_epoch;

	__le32                    flags;
	__le32                    inc_lock;

	struct ceph_eversion      reassert_version;

	/* semi-hack, fix me */
	__le32                    shed_count;
	struct ceph_osd_peer_stat peer_stat;
} __attribute__ ((packed));

struct ceph_osd_reply_head {
	ceph_tid_t           tid;
	__le32               op;
	__le32               flags;
	struct ceph_object   oid;
	struct ceph_object_layout layout;
	ceph_epoch_t         osdmap_epoch;
	__le32               result;
	__le64               offset, length;
	struct ceph_eversion reassert_version;
} __attribute__ ((packed));

#endif
