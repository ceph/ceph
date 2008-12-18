/*
 * ceph_fs.h - Ceph constants and data types to share between kernel and
 * user space.
 *
 * LGPL2
 */

#ifndef _FS_CEPH_CEPH_FS_H
#define _FS_CEPH_CEPH_FS_H


#define CEPH_MON_PORT    6789  /* default monitor port */

/*
 * client-side processes will try to bind to ports in this
 * range, simply for the benefit of tools like nmap or wireshark
 * that would like to identify the protocol.
 */
#define CEPH_PORT_FIRST  6789
#define CEPH_PORT_START  6800  /* non-monitors start here */
#define CEPH_PORT_LAST   6900

/*
 * Max file size is a policy choice; in reality we are limited
 * by 2^64.
 */
#define CEPH_FILE_MAX_SIZE (1ULL << 40)   /* 1 TB */

/*
 * tcp connection banner.  include a protocol version. and adjust
 * whenever the wire protocol changes.  try to keep this string length
 * constant.
 */
#define CEPH_BANNER "ceph 012\n"
#define CEPH_BANNER_MAX_LEN 30

/*
 * subprotocol versions.  when specific messages types or high-level
 * protocols change, bump the affected components.  we keep rev
 * internal cluster protocols separately from the public,
 * client-facing protocol.
 */
#define CEPH_OSD_PROTOCOL     3 /* cluster internal */
#define CEPH_MDS_PROTOCOL     4 /* cluster internal */
#define CEPH_MON_PROTOCOL     4 /* cluster internal */
#define CEPH_OSDC_PROTOCOL    3 /* public/client */
#define CEPH_MDSC_PROTOCOL    4 /* public/client */
#define CEPH_MONC_PROTOCOL    4 /* public/client */


/*
 * types in this file are defined as little-endian, and are
 * primarily intended to describe data structures that pass
 * over the wire or that are stored on disk.
 */

/*
 * some basics
 */
typedef __le64 ceph_version_t;
typedef __le64 ceph_tid_t;      /* transaction id */
typedef __le32 ceph_epoch_t;


/*
 * fs id
 */
struct ceph_fsid {
	__le64 major;
	__le64 minor;
} __attribute__ ((packed));

static inline int ceph_fsid_equal(const struct ceph_fsid *a,
				  const struct ceph_fsid *b)
{
	return a->major == b->major && a->minor == b->minor;
}


/*
 * ino, object, etc.
 */
#define CEPH_INO_ROOT 1

typedef __le64 ceph_snapid_t;
#define CEPH_MAXSNAP ((__u64)(-3))
#define CEPH_SNAPDIR ((__u64)(-1))
#define CEPH_NOSNAP  ((__u64)(-2))

struct ceph_object {
	union {
		__u8 raw[20];        /* fits a sha1 hash */
		struct {
			__le64 ino;  /* inode "file" identifier */
			__le32 bno;  /* "block" (object) in that "file" */
			__le64 snap; /* snapshot id.  usually NOSNAP. */
		} __attribute__ ((packed));
	};
} __attribute__ ((packed));

struct ceph_timespec {
	__le32 tv_sec;
	__le32 tv_nsec;
} __attribute__ ((packed));


/*
 * "Frags" are a way to describe a subset of a 32-bit number space,
 * using a mask and a value to match against that mask.  Any given frag
 * (subset of the number space) can be partitioned into 2^n sub-frags.
 *
 * Frags are encoded into a 32-bit word:
 *   8 upper bits = "bits"
 *  24 lower bits = "value"
 * (We could go to 5+27 bits, but who cares.)
 *
 * We use the _most_ significant bits of the 24 bit value.  This makes
 * values logically sort.
 *
 * Unfortunately, because the "bits" field is still in the high bits, we
 * can't sort encoded frags numerically.  However, it does allow you
 * to feed encoded frags as values into frag_contains_value.
 */
static inline __u32 frag_make(__u32 b, __u32 v)
{
	return (b << 24) |
		(v & (0xffffffu << (24-b)) & 0xffffffu);
}
static inline __u32 frag_bits(__u32 f)
{
	return f >> 24;
}
static inline __u32 frag_value(__u32 f)
{
	return f & 0xffffffu;
}
static inline __u32 frag_mask(__u32 f)
{
	return (0xffffffu << (24-frag_bits(f))) & 0xffffffu;
}
static inline __u32 frag_mask_shift(__u32 f)
{
	return 24 - frag_bits(f);
}

static inline int frag_contains_value(__u32 f, __u32 v)
{
	return (v & frag_mask(f)) == frag_value(f);
}
static inline int frag_contains_frag(__u32 f, __u32 sub)
{
	/* is sub as specific as us, and contained by us? */
	return frag_bits(sub) >= frag_bits(f) &&
		(frag_value(sub) & frag_mask(f)) == frag_value(f);
}

static inline __u32 frag_parent(__u32 f)
{
	return frag_make(frag_bits(f) - 1,
			 frag_value(f) & (frag_mask(f) << 1));
}
static inline int frag_is_left_child(__u32 f)
{
	return frag_bits(f) > 0 &&
		(frag_value(f) & (0x1000000 >> frag_bits(f))) == 0;
}
static inline int frag_is_right_child(__u32 f)
{
	return frag_bits(f) > 0 &&
		(frag_value(f) & (0x1000000 >> frag_bits(f))) == 1;
}
static inline __u32 frag_sibling(__u32 f)
{
	return frag_make(frag_bits(f),
			 frag_value(f) ^ (0x1000000 >> frag_bits(f)));
}
static inline __u32 frag_left_child(__u32 f)
{
	return frag_make(frag_bits(f)+1, frag_value(f));
}
static inline __u32 frag_right_child(__u32 f)
{
	return frag_make(frag_bits(f)+1,
			 frag_value(f) | (0x1000000 >> (1+frag_bits(f))));
}
static inline __u32 frag_make_child(__u32 f, int by, int i)
{
	int newbits = frag_bits(f) + by;
	return frag_make(newbits,
			 frag_value(f) | (i << (24 - newbits)));
}
static inline int frag_is_leftmost(__u32 f)
{
	return frag_value(f) == 0;
}
static inline int frag_is_rightmost(__u32 f)
{
	return frag_value(f) == frag_mask(f);
}
static inline __u32 frag_next(__u32 f)
{
	return frag_make(frag_bits(f),
			 frag_value(f) + (0x1000000 >> frag_bits(f)));
}

/*
 * comparator to sort frags logically, as when traversing the
 * number space in ascending order...
 */
static inline int frag_compare(__u32 a, __u32 b)
{
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

/*
 * object layout - how objects are mapped into PGs
 */
#define CEPH_OBJECT_LAYOUT_HASH     1
#define CEPH_OBJECT_LAYOUT_LINEAR   2
#define CEPH_OBJECT_LAYOUT_HASHINO  3

/*
 * pg layout -- how PGs are mapped onto (sets of) OSDs
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
	__le32 fl_stripe_unit;     /* stripe unit, in bytes.  must be multiple
				      of page size. */
	__le32 fl_stripe_count;    /* over this many objects */
	__le32 fl_object_size;     /* until objects are this big, then move to
				      new objects */
	__le32 fl_cas_hash;        /* 0 = none; 1 = sha256 */

	/* pg -> disk layout */
	__le32 fl_object_stripe_unit;  /* for per-object parity, if any */

	/* object -> pg layout */
	__le32 fl_pg_preferred; /* preferred primary for pg (-1 for none) */
	__u8  fl_pg_type;       /* pg type; see PG_TYPE_* */
	__u8  fl_pg_size;       /* pg size (num replicas, etc.) */
	__u8  fl_pg_pool;       /* implies crush ruleset AND object namespace */
} __attribute__ ((packed));

#define ceph_file_layout_su(l) ((__s32)le32_to_cpu((l).fl_stripe_unit))
#define ceph_file_layout_stripe_count(l) \
	((__s32)le32_to_cpu((l).fl_stripe_count))
#define ceph_file_layout_object_size(l) ((__s32)le32_to_cpu((l).fl_object_size))
#define ceph_file_layout_cas_hash(l) ((__s32)le32_to_cpu((l).fl_cas_hash))
#define ceph_file_layout_object_su(l) \
	((__s32)le32_to_cpu((l).fl_object_stripe_unit))
#define ceph_file_layout_pg_preferred(l) \
	((__s32)le32_to_cpu((l).fl_pg_preferred))

#define ceph_file_layout_stripe_width(l) (le32_to_cpu((l).fl_stripe_unit) * \
					  le32_to_cpu((l).fl_stripe_count))

/* "period" == bytes before i start on a new set of objects */
#define ceph_file_layout_period(l) (le32_to_cpu((l).fl_object_size) *	\
				    le32_to_cpu((l).fl_stripe_count))

/*
 * placement group.
 * we encode this into one __le64.
 */
#define CEPH_PG_TYPE_REP     1
#define CEPH_PG_TYPE_RAID4   2
union ceph_pg {
	__u64 pg64;
	struct {
		__s16 preferred; /* preferred primary osd */
		__u16 ps;        /* placement seed */
		__u8 __pad;
		__u8 size;
		__u8 pool;       /* implies crush ruleset */
		__u8 type;
	} pg;
} __attribute__ ((packed));

#define ceph_pg_is_rep(pg)   ((pg).pg.type == CEPH_PG_TYPE_REP)
#define ceph_pg_is_raid4(pg) ((pg).pg.type == CEPH_PG_TYPE_RAID4)

/*
 * stable_mod func is used to control number of placement groups.
 * similar to straight-up modulo, but produces a stable mapping as b
 * increases over time.  b is the number of bins, and bmask is the
 * containing power of 2 minus 1.
 *
 * b <= bmask and bmask=(2**n)-1
 * e.g., b=12 -> bmask=15, b=123 -> bmask=127
 */
static inline int ceph_stable_mod(int x, int b, int bmask)
{
	if ((x & bmask) < b)
		return x & bmask;
	else
		return x & (bmask >> 1);
}

/*
 * object layout - how a given object should be stored.
 */
struct ceph_object_layout {
	__le64 ol_pgid;           /* raw pg, with _full_ ps precision. */
	__le32 ol_stripe_unit;
} __attribute__ ((packed));

/*
 * compound epoch+version, used by storage layer to serialize mutations
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

/* osd weights.  fixed point value: 0x10000 == 1.0 ("in"), 0 == "out" */
#define CEPH_OSD_IN  0x10000
#define CEPH_OSD_OUT 0


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
ceph_full_name_hash(const char *name, unsigned int len)
{
	unsigned long hash = ceph_init_name_hash();
	while (len--)
		hash = ceph_partial_name_hash(*name++, hash);
	return ceph_end_name_hash(hash);
}



/*********************************************
 * message layer
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

/* used by message exchange protocol */
#define CEPH_MSGR_TAG_READY         1  /* server->client: ready for messages */
#define CEPH_MSGR_TAG_RESETSESSION  2  /* server->client: reset, try again */
#define CEPH_MSGR_TAG_WAIT          3  /* server->client: wait for racing
					  incoming connection */
#define CEPH_MSGR_TAG_RETRY_SESSION 4  /* server->client + cseq: try again
					  with higher cseq */
#define CEPH_MSGR_TAG_RETRY_GLOBAL  5  /* server->client + gseq: try again
					  with higher gseq */
#define CEPH_MSGR_TAG_CLOSE         6  /* closing pipe */
#define CEPH_MSGR_TAG_MSG          10  /* message */
#define CEPH_MSGR_TAG_ACK          11  /* message ack */


/*
 * entity_addr -- network address
 */
struct ceph_entity_addr {
	__le32 erank;  /* entity's rank in process */
	__le32 nonce;  /* unique id for process (e.g. pid) */
	struct sockaddr_in ipaddr;
} __attribute__ ((packed));

static inline bool ceph_entity_addr_is_local(const struct ceph_entity_addr *a,
					     const struct ceph_entity_addr *b)
{
	return le32_to_cpu(a->nonce) == le32_to_cpu(b->nonce) &&
		a->ipaddr.sin_addr.s_addr == b->ipaddr.sin_addr.s_addr;
}

static inline bool ceph_entity_addr_equal(const struct ceph_entity_addr *a,
					  const struct ceph_entity_addr *b)
{
	return memcmp(a, b, sizeof(*a)) == 0;
}

struct ceph_entity_inst {
	struct ceph_entity_name name;
	struct ceph_entity_addr addr;
} __attribute__ ((packed));


/*
 * connection negotiation
 */
struct ceph_msg_connect {
	__le32 host_type;  /* CEPH_ENTITY_TYPE_* */
	__le32 global_seq;
	__le32 connect_seq;
	__u8  flags;
} __attribute__ ((packed));

struct ceph_msg_connect_reply {
	__u8 tag;
	__le32 global_seq;
	__le32 connect_seq;
	__u8 flags;
} __attribute__ ((packed));

#define CEPH_MSG_CONNECT_LOSSY  1  /* messages i send may be safely dropped */


/*
 * message header
 */
struct ceph_msg_header {
	__le64 seq;       /* message seq# for this session */
	__le16 type;      /* message type */
	__le16 priority;  /* priority.  higher value == higher priority */

	__le32 front_len; /* bytes in main payload */
	__le32 data_len;  /* bytes of data payload */
	__le16 data_off;  /* sender: include full offset;
			     receiver: mask against ~PAGE_MASK */

	__u8 mon_protocol, monc_protocol;  /* protocol versions, */
	__u8 osd_protocol, osdc_protocol;  /* internal and public */
	__u8 mds_protocol, mdsc_protocol;

	struct ceph_entity_inst src, orig_src, dst;
	__le32 crc;       /* header crc32c */
} __attribute__ ((packed));

#define CEPH_MSG_PRIO_LOW     64
#define CEPH_MSG_PRIO_DEFAULT 127
#define CEPH_MSG_PRIO_HIGH    196
#define CEPH_MSG_PRIO_HIGHEST 255

/*
 * follows data payload
 */
struct ceph_msg_footer {
	__le32 flags;
	__le32 front_crc;
	__le32 data_crc;
} __attribute__ ((packed));

#define CEPH_MSG_FOOTER_ABORTED   (1<<0)   /* drop this message */
#define CEPH_MSG_FOOTER_NOCRC     (1<<1)   /* no data crc */


/*
 * message types
 */

/* misc */
#define CEPH_MSG_SHUTDOWN               1
#define CEPH_MSG_PING                   2

/* client <-> monitor */
#define CEPH_MSG_MON_MAP                4
#define CEPH_MSG_MON_GET_MAP            5
#define CEPH_MSG_CLIENT_MOUNT           10
#define CEPH_MSG_CLIENT_UNMOUNT         11
#define CEPH_MSG_STATFS                 12
#define CEPH_MSG_STATFS_REPLY           13

/* client <-> mds */
#define CEPH_MSG_MDS_GETMAP             20
#define CEPH_MSG_MDS_MAP                21

#define CEPH_MSG_CLIENT_SESSION         22
#define CEPH_MSG_CLIENT_RECONNECT       23

#define CEPH_MSG_CLIENT_REQUEST         24
#define CEPH_MSG_CLIENT_REQUEST_FORWARD 25
#define CEPH_MSG_CLIENT_REPLY           26
#define CEPH_MSG_CLIENT_CAPS            0x310
#define CEPH_MSG_CLIENT_LEASE           0x311
#define CEPH_MSG_CLIENT_SNAP            0x312

/* osd */
#define CEPH_MSG_OSD_GETMAP       40
#define CEPH_MSG_OSD_MAP          41
#define CEPH_MSG_OSD_OP           42
#define CEPH_MSG_OSD_OPREPLY      43


struct ceph_mon_statfs {
	struct ceph_fsid fsid;
	__le64 tid;
};

struct ceph_statfs {
	__le64 f_total;
	__le64 f_free;  /* used = total - free (KB) */
	__le64 f_avail; /* usable */
	__le64 f_objects;
};

struct ceph_mon_statfs_reply {
	struct ceph_fsid fsid;
	__le64 tid;
	struct ceph_statfs st;
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
#define CEPH_MDS_STATE_STOPPED    -1  /* down, once existed, but no subtrees.
					 empty log. */
#define CEPH_MDS_STATE_DESTROYING -2  /* down, existing, semi-destroyed. */
#define CEPH_MDS_STATE_FAILED      3  /* down, needs to be recovered. */

#define CEPH_MDS_STATE_BOOT       -4  /* up, boot announcement. */
#define CEPH_MDS_STATE_STANDBY    -5  /* up, idle.  waiting for assignment. */
#define CEPH_MDS_STATE_CREATING   -6  /* up, creating MDS instance. */
#define CEPH_MDS_STATE_STARTING   -7  /* up, starting previously stopped mds. */
#define CEPH_MDS_STATE_STANDBY_REPLAY -8  /* up, tailing active node's journal. */

#define CEPH_MDS_STATE_REPLAY      8  /* up, replaying journal. */
#define CEPH_MDS_STATE_RESOLVE     9  /* up, disambiguating distributed
					 operations (import, rename, etc.) */
#define CEPH_MDS_STATE_RECONNECT   10 /* up, reconnect to clients */
#define CEPH_MDS_STATE_REJOIN      11 /* up, rejoining distributed cache */
#define CEPH_MDS_STATE_ACTIVE      12 /* up, active */
#define CEPH_MDS_STATE_STOPPING    13 /* up, but exporting metadata */


/*
 * metadata lock types.
 *  - these are bitmasks.. we can compose them
 *  - they also define the lock ordering by the MDS
 *  - a few of these are internal to the mds
 */
#define CEPH_LOCK_DN          1
#define CEPH_LOCK_ISNAP       2
#define CEPH_LOCK_IVERSION    4     /* mds internal */
#define CEPH_LOCK_IFILE       8     /* mds internal */
#define CEPH_LOCK_IAUTH       32
#define CEPH_LOCK_ILINK       64
#define CEPH_LOCK_IDFT        128   /* dir frag tree */
#define CEPH_LOCK_INEST       256   /* mds internal */
#define CEPH_LOCK_IXATTR      512
#define CEPH_LOCK_INO         2048  /* immutable inode bits; not a lock */

/*
 * stat masks are defined in terms of the locks that cover inode fields.
 */
#define CEPH_STAT_MASK_INODE    CEPH_LOCK_INO
#define CEPH_STAT_MASK_TYPE     CEPH_LOCK_INO  /* mode >> 12 */
#define CEPH_STAT_MASK_SYMLINK  CEPH_LOCK_INO
#define CEPH_STAT_MASK_UID      CEPH_LOCK_IAUTH
#define CEPH_STAT_MASK_GID      CEPH_LOCK_IAUTH
#define CEPH_STAT_MASK_MODE     CEPH_LOCK_IAUTH
#define CEPH_STAT_MASK_NLINK    CEPH_LOCK_ILINK
#define CEPH_STAT_MASK_LAYOUT   CEPH_LOCK_IFILE
#define CEPH_STAT_MASK_MTIME    CEPH_LOCK_IFILE
#define CEPH_STAT_MASK_SIZE     CEPH_LOCK_IFILE
#define CEPH_STAT_MASK_ATIME    CEPH_LOCK_IFILE  /* fixme */
#define CEPH_STAT_MASK_XATTR    CEPH_LOCK_IXATTR
#define CEPH_STAT_MASK_INODE_ALL (CEPH_LOCK_IFILE | CEPH_LOCK_IAUTH | \
				  CEPH_LOCK_ILINK | CEPH_LOCK_INO)

/* client_session ops */
enum {
	CEPH_SESSION_REQUEST_OPEN,
	CEPH_SESSION_OPEN,
	CEPH_SESSION_REQUEST_CLOSE,
	CEPH_SESSION_CLOSE,
	CEPH_SESSION_REQUEST_RENEWCAPS,
	CEPH_SESSION_RENEWCAPS,
	CEPH_SESSION_STALE,
};

static inline const char *ceph_session_op_name(int op)
{
	switch (op) {
	case CEPH_SESSION_REQUEST_OPEN: return "request_open";
	case CEPH_SESSION_OPEN: return "open";
	case CEPH_SESSION_REQUEST_CLOSE: return "request_close";
	case CEPH_SESSION_CLOSE: return "close";
	case CEPH_SESSION_REQUEST_RENEWCAPS: return "request_renewcaps";
	case CEPH_SESSION_RENEWCAPS: return "renewcaps";
	case CEPH_SESSION_STALE: return "stale";
	default: return "???";
	}
}

struct ceph_mds_session_head {
	__le32 op;
	__le64 seq;
	struct ceph_timespec stamp;
} __attribute__ ((packed));

/* client_request */
/*
 * metadata ops.
 *  & 0x001000 -> write op
 *  & 0x010000 -> follow symlink (e.g. stat(), not lstat()).
 &  & 0x100000 -> use weird ino/path trace
 */
#define CEPH_MDS_OP_WRITE        0x001000
#define CEPH_MDS_OP_FOLLOW_LINK  0x010000
#define CEPH_MDS_OP_INO_PATH     0x100000
enum {
	CEPH_MDS_OP_FINDINODE  = 0x100100,

	CEPH_MDS_OP_LSTAT      = 0x00100,
	CEPH_MDS_OP_LUTIME     = 0x01101,
	CEPH_MDS_OP_LCHMOD     = 0x01102,
	CEPH_MDS_OP_LCHOWN     = 0x01103,
	CEPH_MDS_OP_LSETXATTR  = 0x01104,
	CEPH_MDS_OP_LRMXATTR   = 0x01105,
	CEPH_MDS_OP_LSETLAYOUT = 0x01106,

	CEPH_MDS_OP_STAT       = 0x10100,
	CEPH_MDS_OP_UTIME      = 0x11101,
	CEPH_MDS_OP_CHMOD      = 0x11102,
	CEPH_MDS_OP_CHOWN      = 0x11103,
	CEPH_MDS_OP_SETXATTR   = 0x11104,
	CEPH_MDS_OP_RMXATTR    = 0x11105,

	CEPH_MDS_OP_MKNOD      = 0x01201,
	CEPH_MDS_OP_LINK       = 0x01202,
	CEPH_MDS_OP_UNLINK     = 0x01203,
	CEPH_MDS_OP_RENAME     = 0x01204,
	CEPH_MDS_OP_MKDIR      = 0x01220,
	CEPH_MDS_OP_RMDIR      = 0x01221,
	CEPH_MDS_OP_SYMLINK    = 0x01222,

	CEPH_MDS_OP_OPEN       = 0x10302,
	CEPH_MDS_OP_TRUNCATE   = 0x11303,
	CEPH_MDS_OP_LTRUNCATE  = 0x01303,
	CEPH_MDS_OP_FSYNC      = 0x00304,
	CEPH_MDS_OP_READDIR    = 0x00305,

	CEPH_MDS_OP_MKSNAP     = 0x01400,
	CEPH_MDS_OP_RMSNAP     = 0x01401,
	CEPH_MDS_OP_LSSNAP     = 0x00402,
};

static inline const char *ceph_mds_op_name(int op)
{
	switch (op) {
	case CEPH_MDS_OP_FINDINODE: return "findinode";
	case CEPH_MDS_OP_STAT:  return "stat";
	case CEPH_MDS_OP_LSTAT:  return "lstat";
	case CEPH_MDS_OP_UTIME: return "utime";
	case CEPH_MDS_OP_LUTIME: return "lutime";
	case CEPH_MDS_OP_CHMOD: return "chmod";
	case CEPH_MDS_OP_LCHMOD: return "lchmod";
	case CEPH_MDS_OP_CHOWN: return "chown";
	case CEPH_MDS_OP_LCHOWN: return "lchown";
	case CEPH_MDS_OP_LSETLAYOUT: return "lsetlayout";
	case CEPH_MDS_OP_SETXATTR: return "setxattr";
	case CEPH_MDS_OP_LSETXATTR: return "lsetxattr";
	case CEPH_MDS_OP_RMXATTR: return "rmxattr";
	case CEPH_MDS_OP_LRMXATTR: return "lrmxattr";
	case CEPH_MDS_OP_READDIR: return "readdir";
	case CEPH_MDS_OP_MKNOD: return "mknod";
	case CEPH_MDS_OP_LINK: return "link";
	case CEPH_MDS_OP_UNLINK: return "unlink";
	case CEPH_MDS_OP_RENAME: return "rename";
	case CEPH_MDS_OP_MKDIR: return "mkdir";
	case CEPH_MDS_OP_RMDIR: return "rmdir";
	case CEPH_MDS_OP_SYMLINK: return "symlink";
	case CEPH_MDS_OP_OPEN: return "open";
	case CEPH_MDS_OP_TRUNCATE: return "truncate";
	case CEPH_MDS_OP_LTRUNCATE: return "ltruncate";
	case CEPH_MDS_OP_FSYNC: return "fsync";
	case CEPH_MDS_OP_LSSNAP: return "lssnap";
	case CEPH_MDS_OP_MKSNAP: return "mksnap";
	case CEPH_MDS_OP_RMSNAP: return "rmsnap";
	default: return "???";
	}
}

struct ceph_mds_request_head {
	ceph_tid_t tid, oldest_client_tid;
	ceph_epoch_t mdsmap_epoch; /* on client */
	__le32 num_fwd;
	__le32 retry_attempt;
	__le64 mds_wants_replica_in_dirino;
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
			__le32 mask;
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
		struct {
			__le32 flags;
		} __attribute__ ((packed)) setxattr;
		struct {
			struct ceph_file_layout layout;
		} __attribute__ ((packed)) setlayout;
	} __attribute__ ((packed)) args;
} __attribute__ ((packed));

/* masks for utimes() */
#define CEPH_UTIME_ATIME		1
#define CEPH_UTIME_MTIME		2
#define CEPH_UTIME_CTIME		4

/* masks for chown */
#define CEPH_CHOWN_UID   1
#define CEPH_CHOWN_GID   2

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
	__le32 file_caps_mseq;
	__le32 mdsmap_epoch;
} __attribute__ ((packed));

/* one for each node split */
struct ceph_frag_tree_split {
	__le32 frag;      /* this frag splits... */
	__le32 by;        /* ...by this many bits */
} __attribute__ ((packed));

struct ceph_frag_tree_head {
	__le32 nsplits;
	struct ceph_frag_tree_split splits[];
} __attribute__ ((packed));

struct ceph_mds_reply_inode {
	__le64 ino;
	__le64 snapid;
	__le64 version;
	struct ceph_file_layout layout;
	struct ceph_timespec ctime, mtime, atime;
	__le64 time_warp_seq;
	__le32 rdev;
	__le32 mode, uid, gid;
	__le32 nlink;
	__le64 size, max_size, truncate_seq;
	__le64 files, subdirs, rbytes, rfiles, rsubdirs;  /* dir stats */
	struct ceph_timespec rctime;
	struct ceph_frag_tree_head fragtree;
} __attribute__ ((packed));
/* followed by frag array, then symlink string, then xattr blob */

/* reply_lease follows dname, and reply_inode */
struct ceph_mds_reply_lease {
	__le16 mask;
	__le32 duration_ms;
} __attribute__ ((packed));

struct ceph_mds_reply_dirfrag {
	__le32 frag;   /* fragment */
	__le32 auth;   /* auth mds, if this is a delegation point */
	__le32 ndist;  /* number of mds' this is replicated on */
	__le32 dist[];
} __attribute__ ((packed));

/* file access modes */
#define CEPH_FILE_MODE_PIN        0
#define CEPH_FILE_MODE_RD         1
#define CEPH_FILE_MODE_WR         2
#define CEPH_FILE_MODE_RDWR       3  /* RD | WR */
#define CEPH_FILE_MODE_LAZY       4  /* lazy io */
#define CEPH_FILE_MODE_NUM        8  /* bc these are bit fields.. mostly */

static inline int ceph_flags_to_mode(int flags)
{
#ifdef O_DIRECTORY  /* fixme */
	if ((flags & O_DIRECTORY) == O_DIRECTORY)
		return CEPH_FILE_MODE_PIN;
#endif
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
#define CEPH_CAP_WREXTEND 32  /* client can extend EOF */
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
	CEPH_CAP_OP_GRANT,     /* mds->client grant */
	CEPH_CAP_OP_TRUNC,     /* mds->client trunc notify */
	CEPH_CAP_OP_EXPORT,    /* mds has exported the cap */
	CEPH_CAP_OP_IMPORT,    /* mds has imported the cap from specified mds */
	CEPH_CAP_OP_RELEASED,    /* mds->client close out cap */
	CEPH_CAP_OP_FLUSHEDSNAP, /* mds->client flushed snap */
	CEPH_CAP_OP_ACK,       /* client->mds ack (if prior grant was recall) */
	CEPH_CAP_OP_REQUEST,   /* client->mds request (update wanted bits) */
	CEPH_CAP_OP_FLUSHSNAP, /* client->mds flush snapped metadata */
	CEPH_CAP_OP_RELEASE,   /* client->mds request release cap */
};

static inline const char *ceph_cap_op_name(int op)
{
	switch (op) {
	case CEPH_CAP_OP_GRANT: return "grant";
	case CEPH_CAP_OP_TRUNC: return "trunc";
	case CEPH_CAP_OP_EXPORT: return "export";
	case CEPH_CAP_OP_IMPORT: return "import";
	case CEPH_CAP_OP_RELEASED: return "released";
	case CEPH_CAP_OP_FLUSHEDSNAP: return "flushedsnap";
	case CEPH_CAP_OP_ACK: return "ack";
	case CEPH_CAP_OP_REQUEST: return "request";
	case CEPH_CAP_OP_FLUSHSNAP: return "flushsnap";
	case CEPH_CAP_OP_RELEASE: return "release";
	default: return "???";
	}
}

/*
 * caps message, used for capability callbacks, acks, requests, etc.
 */
struct ceph_mds_caps {
	__le32 op;
	__le64 ino;
	__le32 seq;
	__le32 caps, wanted;
	__le64 size, max_size;
	__le64 truncate_seq;
	__le32 migrate_seq;
	struct ceph_timespec mtime, atime, ctime;
	struct ceph_file_layout layout;
	__le64 time_warp_seq;
	__le64 snap_follows;
	__le32 snap_trace_len;
} __attribute__ ((packed));


#define CEPH_MDS_LEASE_REVOKE  1  /*    mds  -> client */
#define CEPH_MDS_LEASE_RELEASE 2  /* client  -> mds    */
#define CEPH_MDS_LEASE_RENEW   3  /* client <-> mds    */

struct ceph_mds_lease {
	__u8 action;
	__le16 mask;
	__le64 ino;
	__le64 first, last;
} __attribute__ ((packed));
/* followed by a __le32+string for dname */


/* client reconnect */
struct ceph_mds_cap_reconnect {
	__le32 wanted;
	__le32 issued;
	__le64 size;
	struct ceph_timespec mtime, atime;
	__le64 snaprealm;
} __attribute__ ((packed));
/* followed by encoded string */

struct ceph_mds_snaprealm_reconnect {
	__le64 ino;
	__le64 seq;
	__le64 parent;  /* parent realm */
} __attribute__ ((packed));

/*
 * snaps
 */
enum {
	CEPH_SNAP_OP_UPDATE,  /* CREATE or DESTROY */
	CEPH_SNAP_OP_CREATE,
	CEPH_SNAP_OP_DESTROY,
	CEPH_SNAP_OP_SPLIT,
};

static inline const char *ceph_snap_op_name(int o)
{
	switch (o) {
	case CEPH_SNAP_OP_UPDATE: return "update";
	case CEPH_SNAP_OP_CREATE: return "create";
	case CEPH_SNAP_OP_DESTROY: return "destroy";
	case CEPH_SNAP_OP_SPLIT: return "split";
	default: return "???";
	}
}

struct ceph_mds_snap_head {
	__le32 op;
	__le64 split;
	__le32 num_split_inos;
	__le32 num_split_realms;
	__le32 trace_len;
} __attribute__ ((packed));
/* followed by split ino list, then split realms, then the trace blob */

/*
 * encode info about a snaprealm, as viewed by a client
 */
struct ceph_mds_snap_realm {
	__le64 ino;           /* ino */
	__le64 created;       /* snap: when created */
	__le64 parent;        /* ino: parent realm */
	__le64 parent_since;  /* snap: same parent since */
	__le64 seq;           /* snap: version */
	__le32 num_snaps;
	__le32 num_prior_parent_snaps;
} __attribute__ ((packed));
/* followed by my snap list, then prior parent snap list */

/*
 * osd map flag bits
 */
#define CEPH_OSDMAP_NEARFULL (1<<0)  /* sync writes (near ENOSPC) */
#define CEPH_OSDMAP_FULL     (1<<1)  /* no data writes (ENOSPC) */

/*
 * osd ops
 */
#define CEPH_OSD_OP_MODE       0xf000
#define CEPH_OSD_OP_MODE_RD    0x1000
#define CEPH_OSD_OP_MODE_WR    0x2000
#define CEPH_OSD_OP_MODE_SUB   0x4000

#define CEPH_OSD_OP_TYPE       0x0f00
#define CEPH_OSD_OP_TYPE_LOCK  0x0100
#define CEPH_OSD_OP_TYPE_DATA  0x0200
#define CEPH_OSD_OP_TYPE_ATTR  0x0300

enum {
	/* read */
	CEPH_OSD_OP_READ       = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 1,
	CEPH_OSD_OP_STAT       = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 2,

	CEPH_OSD_OP_GETXATTR   = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_ATTR | 1,
	CEPH_OSD_OP_GETXATTRS  = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_ATTR | 2,

	/* subop */
	CEPH_OSD_OP_PULL           = CEPH_OSD_OP_MODE_SUB | 1,
	CEPH_OSD_OP_PUSH           = CEPH_OSD_OP_MODE_SUB | 2,
	CEPH_OSD_OP_BALANCEREADS   = CEPH_OSD_OP_MODE_SUB | 3,
	CEPH_OSD_OP_UNBALANCEREADS = CEPH_OSD_OP_MODE_SUB | 4,
	CEPH_OSD_OP_SCRUB          = CEPH_OSD_OP_MODE_SUB | 5,

	/* object data */
	CEPH_OSD_OP_WRITE      = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 1,
	CEPH_OSD_OP_WRITEFULL  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 2,
	CEPH_OSD_OP_TRUNCATE   = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 3,
	CEPH_OSD_OP_ZERO       = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 4,
	CEPH_OSD_OP_DELETE     = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 5,

	/* object attrs */
	CEPH_OSD_OP_SETXATTR   = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | 1,
	CEPH_OSD_OP_SETXATTRS  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | 2,
	CEPH_OSD_OP_RESETXATTRS= CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | 3,
	CEPH_OSD_OP_RMXATTR    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | 4,

	/* lock */
	CEPH_OSD_OP_WRLOCK     = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 1,
	CEPH_OSD_OP_WRUNLOCK   = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 2,
	CEPH_OSD_OP_RDLOCK     = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 3,
	CEPH_OSD_OP_RDUNLOCK   = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 4,
	CEPH_OSD_OP_UPLOCK     = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 5,
	CEPH_OSD_OP_DNLOCK     = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 6,

	/* fancy read */
	CEPH_OSD_OP_GREP       = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 3,

	/* fancy write */
	CEPH_OSD_OP_APPEND     = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 6,
};

static inline int ceph_osd_op_type_lock(int op)
{
	return (op & CEPH_OSD_OP_TYPE) == CEPH_OSD_OP_TYPE_LOCK;
}
static inline int ceph_osd_op_type_data(int op)
{
	return (op & CEPH_OSD_OP_TYPE) == CEPH_OSD_OP_TYPE_DATA;
}
static inline int ceph_osd_op_type_attr(int op)
{
	return (op & CEPH_OSD_OP_TYPE) == CEPH_OSD_OP_TYPE_ATTR;
}

static inline int ceph_osd_op_mode_subop(int op)
{
	return (op & CEPH_OSD_OP_MODE) == CEPH_OSD_OP_MODE_SUB;
}
static inline int ceph_osd_op_mode_read(int op)
{
	return (op & CEPH_OSD_OP_MODE) == CEPH_OSD_OP_MODE_RD;
}
static inline int ceph_osd_op_mode_modify(int op)
{
	return (op & CEPH_OSD_OP_MODE) == CEPH_OSD_OP_MODE_WR;
}

static inline const char *ceph_osd_op_name(int op)
{
	switch (op) {
	case CEPH_OSD_OP_READ: return "read";
	case CEPH_OSD_OP_STAT: return "stat";

	case CEPH_OSD_OP_WRITE: return "write";
	case CEPH_OSD_OP_DELETE: return "delete";
	case CEPH_OSD_OP_TRUNCATE: return "truncate";
	case CEPH_OSD_OP_ZERO: return "zero";
	case CEPH_OSD_OP_WRITEFULL: return "writefull";

	case CEPH_OSD_OP_SETXATTR: return "setxattr";
	case CEPH_OSD_OP_SETXATTRS: return "setxattrs";
	case CEPH_OSD_OP_RESETXATTRS: return "resetxattrs";
	case CEPH_OSD_OP_RMXATTR: return "rmxattr";

	case CEPH_OSD_OP_WRLOCK: return "wrlock";
	case CEPH_OSD_OP_WRUNLOCK: return "wrunlock";
	case CEPH_OSD_OP_RDLOCK: return "rdlock";
	case CEPH_OSD_OP_RDUNLOCK: return "rdunlock";
	case CEPH_OSD_OP_UPLOCK: return "uplock";
	case CEPH_OSD_OP_DNLOCK: return "dnlock";

	case CEPH_OSD_OP_PULL: return "pull";
	case CEPH_OSD_OP_PUSH: return "push";
	case CEPH_OSD_OP_BALANCEREADS: return "balance-reads";
	case CEPH_OSD_OP_UNBALANCEREADS: return "unbalance-reads";
	case CEPH_OSD_OP_SCRUB: return "scrub";

	default: return "???";
	}
}


/*
 * osd op flags
 */
enum {
	CEPH_OSD_OP_ACK = 1,          /* want (or is) "ack" ack */
	CEPH_OSD_OP_ONNVRAM = 2,      /* want (or is) "onnvram" ack */
	CEPH_OSD_OP_ONDISK = 4,       /* want (or is) "ondisk" ack */
	CEPH_OSD_OP_RETRY = 8,        /* resend attempt */
	CEPH_OSD_OP_INCLOCK_FAIL = 16, /* fail on inclock collision */
	CEPH_OSD_OP_MODIFY = 32,      /* op is/was a mutation */
	CEPH_OSD_OP_ORDERSNAP = 64,   /* EOLDSNAP if snapc is out of order */
	CEPH_OSD_OP_PEERSTAT = 128,   /* msg includes osd_peer_stat */
	CEPH_OSD_OP_BALANCE_READS = 256,
};

#define EOLDSNAPC    ERESTART  /* ORDERSNAP flag set and writer has old snap context*/
#define EBLACKLISTED ESHUTDOWN /* blacklisted */

struct ceph_osd_op {
	__le16 op;
	union {
		struct {
			__le64 offset, length;
		};
		struct {
			__le32 name_len;
			__le32 value_len;
		};
	};
} __attribute__ ((packed));

struct ceph_osd_request_head {
	ceph_tid_t                tid;
	__le32                    client_inc;
	struct ceph_object        oid;
	struct ceph_object_layout layout;
	ceph_epoch_t              osdmap_epoch;

	__le32                    flags;
	__le32                    inc_lock;

	struct ceph_eversion      reassert_version;

	/* writer's snap context */
	__le64 snap_seq;
	__le32 num_snaps;

	/* read or mutation */
	__le16 num_ops;
	__u16 object_type;
	struct ceph_osd_op ops[];  /* followed by snaps */
} __attribute__ ((packed));

struct ceph_osd_reply_head {
	ceph_tid_t           tid;
	__le32               flags;
	struct ceph_object   oid;
	struct ceph_object_layout layout;
	ceph_epoch_t         osdmap_epoch;
	struct ceph_eversion reassert_version;

	__le32 result;

	__le32 num_ops;
	struct ceph_osd_op ops[0];
} __attribute__ ((packed));

#endif
