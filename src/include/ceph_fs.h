/*
 * ceph_fs.h - Ceph constants and data types to share between kernel and
 * user space.
 *
 * LGPL2
 */

#ifndef _FS_CEPH_CEPH_FS_H
#define _FS_CEPH_CEPH_FS_H

#include "msgr.h"
#include "rados.h"

/*
 * Max file size is a policy choice; in reality we are limited
 * by 2^64.
 */
#define CEPH_FILE_MAX_SIZE (1ULL << 40)   /* 1 TB */

/*
 * subprotocol versions.  when specific messages types or high-level
 * protocols change, bump the affected components.  we keep rev
 * internal cluster protocols separately from the public,
 * client-facing protocol.
 */
#define CEPH_OSD_PROTOCOL     6 /* cluster internal */
#define CEPH_MDS_PROTOCOL     9 /* cluster internal */
#define CEPH_MON_PROTOCOL     4 /* cluster internal */
#define CEPH_OSDC_PROTOCOL   19 /* public/client */
#define CEPH_MDSC_PROTOCOL   26 /* public/client */
#define CEPH_MONC_PROTOCOL   14 /* public/client */



/*
 * types in this file are defined as little-endian, and are
 * primarily intended to describe data structures that pass
 * over the wire or that are stored on disk.
 */


#define CEPH_INO_ROOT  1


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
	__le32 fl_pg_pool;      /* namespace, crush ruleset, rep level */
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
 * message types
 */

/* misc */
#define CEPH_MSG_SHUTDOWN               1
#define CEPH_MSG_PING                   2

/* client <-> monitor */
#define CEPH_MSG_MON_MAP                4
#define CEPH_MSG_MON_GET_MAP            5
#define CEPH_MSG_CLIENT_MOUNT           10
#define CEPH_MSG_CLIENT_MOUNT_ACK       11
#define CEPH_MSG_CLIENT_UNMOUNT         12
#define CEPH_MSG_STATFS                 13
#define CEPH_MSG_STATFS_REPLY           14

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
#define CEPH_MSG_CLIENT_CAPRELEASE      0x313

/* osd */
#define CEPH_MSG_OSD_GETMAP       40
#define CEPH_MSG_OSD_MAP          41
#define CEPH_MSG_OSD_OP           42
#define CEPH_MSG_OSD_OPREPLY      43


struct ceph_mon_statfs {
	__le64 have_version;
	ceph_fsid_t fsid;
	__le64 tid;
};

struct ceph_statfs {
	__le64 kb, kb_used, kb_avail;
	__le64 num_objects;
};

struct ceph_mon_statfs_reply {
	ceph_fsid_t fsid;
	__le64 tid;
	__le64 version;
	struct ceph_statfs st;
};

struct ceph_osd_getmap {
	__le64 have_version;
	ceph_fsid_t fsid;
	__le32 start;
} __attribute__ ((packed));

struct ceph_mds_getmap {
	__le64 have_version;
	ceph_fsid_t fsid;
} __attribute__ ((packed));

struct ceph_client_mount {
	__le64 have_version;
} __attribute__ ((packed));

struct ceph_client_unmount {
	__le64 have_version;
} __attribute__ ((packed));

/*
 * client authentication ticket
 */
struct ceph_client_ticket {
	__u32 client;
	struct ceph_entity_addr addr;
	struct ceph_timespec created, expires;
	__u32 flags;
} __attribute__ ((packed));

/*
 * mds states
 *   > 0 -> in
 *  <= 0 -> out
 */
#define CEPH_MDS_STATE_DNE          0  /* down, does not exist. */
#define CEPH_MDS_STATE_STOPPED     -1  /* down, once existed, but no subtrees.
					  empty log. */
#define CEPH_MDS_STATE_BOOT        -4  /* up, boot announcement. */
#define CEPH_MDS_STATE_STANDBY     -5  /* up, idle.  waiting for assignment. */
#define CEPH_MDS_STATE_CREATING    -6  /* up, creating MDS instance. */
#define CEPH_MDS_STATE_STARTING    -7  /* up, starting previously stopped mds. */
#define CEPH_MDS_STATE_STANDBY_REPLAY -8 /* up, tailing active node's journal */

#define CEPH_MDS_STATE_REPLAY       8  /* up, replaying journal. */
#define CEPH_MDS_STATE_RESOLVE      9  /* up, disambiguating distributed
					  operations (import, rename, etc.) */
#define CEPH_MDS_STATE_RECONNECT    10 /* up, reconnect to clients */
#define CEPH_MDS_STATE_REJOIN       11 /* up, rejoining distributed cache */
#define CEPH_MDS_STATE_CLIENTREPLAY 12 /* up, replaying client operations */
#define CEPH_MDS_STATE_ACTIVE       13 /* up, active */
#define CEPH_MDS_STATE_STOPPING     14 /* up, but exporting metadata */

static inline const char *ceph_mds_state_name(int s)
{
	switch (s) {
		/* down and out */
	case CEPH_MDS_STATE_DNE:        return "down:dne";
	case CEPH_MDS_STATE_STOPPED:    return "down:stopped";
		/* up and out */
	case CEPH_MDS_STATE_BOOT:       return "up:boot";
	case CEPH_MDS_STATE_STANDBY:    return "up:standby";
	case CEPH_MDS_STATE_STANDBY_REPLAY:    return "up:standby-replay";
	case CEPH_MDS_STATE_CREATING:   return "up:creating";
	case CEPH_MDS_STATE_STARTING:   return "up:starting";
		/* up and in */
	case CEPH_MDS_STATE_REPLAY:     return "up:replay";
	case CEPH_MDS_STATE_RESOLVE:    return "up:resolve";
	case CEPH_MDS_STATE_RECONNECT:  return "up:reconnect";
	case CEPH_MDS_STATE_REJOIN:     return "up:rejoin";
	case CEPH_MDS_STATE_CLIENTREPLAY: return "up:clientreplay";
	case CEPH_MDS_STATE_ACTIVE:     return "up:active";
	case CEPH_MDS_STATE_STOPPING:   return "up:stopping";
	default: return "";
	}
	return NULL;
}


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

/* client_session ops */
enum {
	CEPH_SESSION_REQUEST_OPEN,
	CEPH_SESSION_OPEN,
	CEPH_SESSION_REQUEST_CLOSE,
	CEPH_SESSION_CLOSE,
	CEPH_SESSION_REQUEST_RENEWCAPS,
	CEPH_SESSION_RENEWCAPS,
	CEPH_SESSION_STALE,
	CEPH_SESSION_RECALL_STATE,
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
	case CEPH_SESSION_RECALL_STATE: return "recall_state";
	default: return "???";
	}
}

struct ceph_mds_session_head {
	__le32 op;
	__le64 seq;
	struct ceph_timespec stamp;
	__le32 max_caps, max_leases;
} __attribute__ ((packed));

/* client_request */
/*
 * metadata ops.
 *  & 0x001000 -> write op
 *  & 0x010000 -> follow symlink (e.g. stat(), not lstat()).
 &  & 0x100000 -> use weird ino/path trace
 */
#define CEPH_MDS_OP_WRITE        0x001000
enum {
	CEPH_MDS_OP_LOOKUP     = 0x00100,
	CEPH_MDS_OP_GETATTR    = 0x00101,
	CEPH_MDS_OP_LOOKUPHASH = 0x00102,
	CEPH_MDS_OP_SETXATTR   = 0x01105,
	CEPH_MDS_OP_RMXATTR    = 0x01106,
	CEPH_MDS_OP_SETLAYOUT  = 0x01107,
	CEPH_MDS_OP_SETATTR    = 0x01108,

	CEPH_MDS_OP_MKNOD      = 0x01201,
	CEPH_MDS_OP_LINK       = 0x01202,
	CEPH_MDS_OP_UNLINK     = 0x01203,
	CEPH_MDS_OP_RENAME     = 0x01204,
	CEPH_MDS_OP_MKDIR      = 0x01220,
	CEPH_MDS_OP_RMDIR      = 0x01221,
	CEPH_MDS_OP_SYMLINK    = 0x01222,

	CEPH_MDS_OP_CREATE     = 0x00301,
	CEPH_MDS_OP_OPEN       = 0x00302,
	CEPH_MDS_OP_READDIR    = 0x00305,

	CEPH_MDS_OP_LOOKUPSNAP = 0x00400,
	CEPH_MDS_OP_MKSNAP     = 0x01400,
	CEPH_MDS_OP_RMSNAP     = 0x01401,
	CEPH_MDS_OP_LSSNAP     = 0x00402,
};

static inline const char *ceph_mds_op_name(int op)
{
	switch (op) {
	case CEPH_MDS_OP_LOOKUP:  return "lookup";
	case CEPH_MDS_OP_LOOKUPHASH:  return "lookuphash";
	case CEPH_MDS_OP_GETATTR:  return "getattr";
	case CEPH_MDS_OP_SETXATTR: return "setxattr";
	case CEPH_MDS_OP_SETATTR: return "setattr";
	case CEPH_MDS_OP_RMXATTR: return "rmxattr";
	case CEPH_MDS_OP_READDIR: return "readdir";
	case CEPH_MDS_OP_MKNOD: return "mknod";
	case CEPH_MDS_OP_LINK: return "link";
	case CEPH_MDS_OP_UNLINK: return "unlink";
	case CEPH_MDS_OP_RENAME: return "rename";
	case CEPH_MDS_OP_MKDIR: return "mkdir";
	case CEPH_MDS_OP_RMDIR: return "rmdir";
	case CEPH_MDS_OP_SYMLINK: return "symlink";
	case CEPH_MDS_OP_CREATE: return "create";
	case CEPH_MDS_OP_OPEN: return "open";
	case CEPH_MDS_OP_LOOKUPSNAP: return "lookupsnap";
	case CEPH_MDS_OP_LSSNAP: return "lssnap";
	case CEPH_MDS_OP_MKSNAP: return "mksnap";
	case CEPH_MDS_OP_RMSNAP: return "rmsnap";
	default: return "???";
	}
}

#define CEPH_SETATTR_MODE   1
#define CEPH_SETATTR_UID    2
#define CEPH_SETATTR_GID    4
#define CEPH_SETATTR_MTIME  8
#define CEPH_SETATTR_ATIME 16
#define CEPH_SETATTR_SIZE  32

union ceph_mds_request_args {
	struct {
		__le32 mask;  /* CEPH_CAP_* */
	} __attribute__ ((packed)) getattr;
	struct {
		__le32 mode;
		__le32 uid;
		__le32 gid;
		struct ceph_timespec mtime;
		struct ceph_timespec atime;
		__le64 size, old_size;
		__le32 mask;  /* CEPH_SETATTR_* */
	} __attribute__ ((packed)) setattr;
	struct {
		__le32 frag;
		__le32 max_entries;
	} __attribute__ ((packed)) readdir;
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
		__le32 flags;
	} __attribute__ ((packed)) setxattr;
	struct {
		struct ceph_file_layout layout;
	} __attribute__ ((packed)) setlayout;
} __attribute__ ((packed));

#define CEPH_MDS_FLAG_REPLAY        1  /* this is a replayed op */
#define CEPH_MDS_FLAG_WANT_DENTRY   2  /* want dentry in reply */

struct ceph_mds_request_head {
	__le64 tid, oldest_client_tid;
	__le32 mdsmap_epoch; /* on client */
	__le32 flags;
	__u8 num_retry, num_fwd;
	__le16 num_releases;
	__le32 op;
	__le32 caller_uid, caller_gid;
	__le64 ino;    /* use this ino for openc, mkdir, mknod, etc. */
	union ceph_mds_request_args args;
} __attribute__ ((packed));

struct ceph_mds_request_release {
	__le64 ino, cap_id;
	__le32 caps, wanted;
	__le32 seq, issue_seq, mseq;
	__le32 dname_seq;
	__le32 dname_len;   /* if releasing a dentry lease; string follows. */
} __attribute__ ((packed));

/* client reply */
struct ceph_mds_reply_head {
	__le64 tid;
	__le32 op;
	__le32 result;
	__le32 mdsmap_epoch;
	__u8 safe;
	__u8 is_dentry, is_target;
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

struct ceph_mds_reply_cap {
	__le32 caps, wanted;
	__le64 cap_id;
	__le32 seq, mseq;
	__le64 realm;
	__le32 ttl_ms;  /* ttl, in ms.  if readonly and unwanted. */
	__u8 flags;
} __attribute__ ((packed));

#define CEPH_CAP_FLAG_AUTH  1

struct ceph_mds_reply_inode {
	__le64 ino;
	__le64 snapid;
	__le32 rdev;
	__le64 version;
	struct ceph_mds_reply_cap cap;
	struct ceph_file_layout layout;
	struct ceph_timespec ctime, mtime, atime;
	__le32 time_warp_seq;
	__le64 size, max_size, truncate_size;
	__le32 truncate_seq;
	__le32 mode, uid, gid;
	__le32 nlink;
	__le64 files, subdirs, rbytes, rfiles, rsubdirs;  /* dir stats */
	struct ceph_timespec rctime;
	struct ceph_frag_tree_head fragtree;
	__le64 xattr_version;
} __attribute__ ((packed));
/* followed by frag array, then symlink string, then xattr blob */

/* reply_lease follows dname, and reply_inode */
struct ceph_mds_reply_lease {
	__le16 mask;
	__le32 duration_ms;
	__le32 seq;
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


/* capability bits */
#define CEPH_CAP_PIN         1  /* no specific capabilities beyond the pin */

/* generic cap bits */
#define CEPH_CAP_GSHARED     1  /* client can reads */
#define CEPH_CAP_GEXCL       2  /* client can read and update */
#define CEPH_CAP_GCACHE      4  /* (file) client can cache reads */
#define CEPH_CAP_GRD         8  /* (file) client can read */
#define CEPH_CAP_GWR        16  /* (file) client can write */
#define CEPH_CAP_GBUFFER    32  /* (file) client can buffer writes */
#define CEPH_CAP_GWREXTEND  64  /* (file) client can extend EOF */
#define CEPH_CAP_GLAZYIO   128  /* (file) client can perform lazy io */

/* per-lock shift */
#define CEPH_CAP_SAUTH      2
#define CEPH_CAP_SLINK      4
#define CEPH_CAP_SXATTR     6
#define CEPH_CAP_SFILE      8   /* goes at the end (uses >2 cap bits) */

/* composed values */
#define CEPH_CAP_AUTH_SHARED  (CEPH_CAP_GSHARED  << CEPH_CAP_SAUTH)
#define CEPH_CAP_AUTH_EXCL     (CEPH_CAP_GEXCL     << CEPH_CAP_SAUTH)
#define CEPH_CAP_LINK_SHARED  (CEPH_CAP_GSHARED  << CEPH_CAP_SLINK)
#define CEPH_CAP_LINK_EXCL     (CEPH_CAP_GEXCL     << CEPH_CAP_SLINK)
#define CEPH_CAP_XATTR_SHARED (CEPH_CAP_GSHARED  << CEPH_CAP_SXATTR)
#define CEPH_CAP_XATTR_EXCL    (CEPH_CAP_GEXCL     << CEPH_CAP_SXATTR)
#define CEPH_CAP_FILE(x)    (x << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_SHARED   (CEPH_CAP_GSHARED   << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_EXCL     (CEPH_CAP_GEXCL     << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_CACHE    (CEPH_CAP_GCACHE    << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_RD       (CEPH_CAP_GRD       << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_WR       (CEPH_CAP_GWR       << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_BUFFER   (CEPH_CAP_GBUFFER   << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_WREXTEND (CEPH_CAP_GWREXTEND << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_LAZYIO   (CEPH_CAP_GLAZYIO   << CEPH_CAP_SFILE)

/* cap masks (for getattr) */
#define CEPH_STAT_CAP_INODE    CEPH_CAP_PIN
#define CEPH_STAT_CAP_TYPE     CEPH_CAP_PIN  /* mode >> 12 */
#define CEPH_STAT_CAP_SYMLINK  CEPH_CAP_PIN
#define CEPH_STAT_CAP_UID      CEPH_CAP_AUTH_SHARED
#define CEPH_STAT_CAP_GID      CEPH_CAP_AUTH_SHARED
#define CEPH_STAT_CAP_MODE     CEPH_CAP_AUTH_SHARED
#define CEPH_STAT_CAP_NLINK    CEPH_CAP_LINK_SHARED
#define CEPH_STAT_CAP_LAYOUT   CEPH_CAP_FILE_SHARED
#define CEPH_STAT_CAP_MTIME    CEPH_CAP_FILE_SHARED
#define CEPH_STAT_CAP_SIZE     CEPH_CAP_FILE_SHARED
#define CEPH_STAT_CAP_ATIME    CEPH_CAP_FILE_SHARED  /* fixme */
#define CEPH_STAT_CAP_XATTR    CEPH_CAP_XATTR_SHARED
#define CEPH_STAT_CAP_INODE_ALL (CEPH_CAP_PIN |			\
				 CEPH_CAP_AUTH_SHARED |	\
				 CEPH_CAP_LINK_SHARED |	\
				 CEPH_CAP_FILE_SHARED |	\
				 CEPH_CAP_XATTR_SHARED)

#define CEPH_CAP_ANY_SHARED (CEPH_CAP_AUTH_SHARED |			\
			      CEPH_CAP_LINK_SHARED |			\
			      CEPH_CAP_XATTR_SHARED |			\
			      CEPH_CAP_FILE_SHARED)
#define CEPH_CAP_ANY_RD   (CEPH_CAP_ANY_SHARED | CEPH_CAP_FILE_RD | \
			   CEPH_CAP_FILE_CACHE)

#define CEPH_CAP_ANY_EXCL (CEPH_CAP_AUTH_EXCL |		\
			   CEPH_CAP_LINK_EXCL |		\
			   CEPH_CAP_XATTR_EXCL |	\
			   CEPH_CAP_FILE_EXCL)
#define CEPH_CAP_ANY_FILE_WR (CEPH_CAP_FILE_WR|CEPH_CAP_FILE_BUFFER)
#define CEPH_CAP_ANY_WR   (CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_FILE_WR)
#define CEPH_CAP_ANY      (CEPH_CAP_ANY_RD | CEPH_CAP_ANY_EXCL | \
			   CEPH_CAP_ANY_FILE_WR | CEPH_CAP_PIN)

#define CEPH_CAP_LOCKS (CEPH_LOCK_IFILE | CEPH_LOCK_IAUTH | CEPH_LOCK_ILINK | \
			CEPH_LOCK_IXATTR)

static inline int ceph_caps_for_mode(int mode)
{
	switch (mode) {
	case CEPH_FILE_MODE_PIN:
		return CEPH_CAP_PIN;
	case CEPH_FILE_MODE_RD:
		return CEPH_CAP_PIN | CEPH_CAP_FILE_SHARED |
			CEPH_CAP_FILE_RD | CEPH_CAP_FILE_CACHE;
	case CEPH_FILE_MODE_RDWR:
		return CEPH_CAP_PIN | CEPH_CAP_FILE_SHARED |
			CEPH_CAP_FILE_EXCL |
			CEPH_CAP_FILE_RD | CEPH_CAP_FILE_CACHE |
			CEPH_CAP_FILE_WR | CEPH_CAP_FILE_BUFFER |
			CEPH_CAP_AUTH_SHARED | CEPH_CAP_AUTH_EXCL |
			CEPH_CAP_XATTR_SHARED | CEPH_CAP_XATTR_EXCL;
	case CEPH_FILE_MODE_WR:
		return CEPH_CAP_PIN | CEPH_CAP_FILE_SHARED |
			CEPH_CAP_FILE_EXCL |
			CEPH_CAP_FILE_WR | CEPH_CAP_FILE_BUFFER |
			CEPH_CAP_AUTH_SHARED | CEPH_CAP_AUTH_EXCL |
			CEPH_CAP_XATTR_SHARED | CEPH_CAP_XATTR_EXCL;
	}
	return 0;
}

enum {
	CEPH_CAP_OP_GRANT,     /* mds->client grant */
	CEPH_CAP_OP_REVOKE,     /* mds->client revoke */
	CEPH_CAP_OP_TRUNC,     /* mds->client trunc notify */
	CEPH_CAP_OP_EXPORT,    /* mds has exported the cap */
	CEPH_CAP_OP_IMPORT,    /* mds has imported the cap from specified mds */
	CEPH_CAP_OP_UPDATE,    /* client->mds update */
	CEPH_CAP_OP_DROP,      /* client->mds drop cap bits */
	CEPH_CAP_OP_FLUSH,     /* client->mds cap writeback */
	CEPH_CAP_OP_FLUSH_ACK, /* mds->client flushed */
	CEPH_CAP_OP_FLUSHSNAP, /* client->mds flush snapped metadata */
	CEPH_CAP_OP_FLUSHSNAP_ACK, /* mds->client flushed snapped metadata */
	CEPH_CAP_OP_RELEASE,   /* client->mds release (clean) cap */
	CEPH_CAP_OP_RENEW,     /* client->mds renewal request */
};

static inline const char *ceph_cap_op_name(int op)
{
	switch (op) {
	case CEPH_CAP_OP_GRANT: return "grant";
	case CEPH_CAP_OP_REVOKE: return "revoke";
	case CEPH_CAP_OP_TRUNC: return "trunc";
	case CEPH_CAP_OP_EXPORT: return "export";
	case CEPH_CAP_OP_IMPORT: return "import";
	case CEPH_CAP_OP_UPDATE: return "update";
	case CEPH_CAP_OP_DROP: return "drop";
	case CEPH_CAP_OP_FLUSH: return "flush";
	case CEPH_CAP_OP_FLUSH_ACK: return "flush_ack";
	case CEPH_CAP_OP_FLUSHSNAP: return "flushsnap";
	case CEPH_CAP_OP_FLUSHSNAP_ACK: return "flushsnap_ack";
	case CEPH_CAP_OP_RELEASE: return "release";
	case CEPH_CAP_OP_RENEW: return "renew";
	default: return "???";
	}
}

/*
 * caps message, used for capability callbacks, acks, requests, etc.
 */
struct ceph_mds_caps {
	__le32 op;
	__le64 ino, realm;
	__le64 cap_id;
	__le32 seq, issue_seq;
	__le32 caps, wanted, dirty;
	__le32 migrate_seq;
	__le64 snap_follows;
	__le32 snap_trace_len;
	__le32 ttl_ms;  /* for IMPORT op only */
	__le64 client_tid;  /* for FLUSH(SNAP) -> FLUSH(SNAP)_ACK */

	/* authlock */
	__le32 uid, gid, mode;

	/* linklock */
	__le32 nlink;

	/* xattrlock */
	__le32 xattr_len;
	__le64 xattr_version;

	/* filelock */
	__le64 size, max_size, truncate_size;
	__le32 truncate_seq;
	struct ceph_timespec mtime, atime, ctime;
	struct ceph_file_layout layout;
	__le32 time_warp_seq;
} __attribute__ ((packed));

struct ceph_mds_cap_release {
	__le32 num;
} __attribute__ ((packed));

struct ceph_mds_cap_item {
	__le64 ino;
	__le64 cap_id;
	__le32 migrate_seq, seq;
} __attribute__ ((packed));

#define CEPH_MDS_LEASE_REVOKE           1  /*    mds  -> client */
#define CEPH_MDS_LEASE_RELEASE          2  /* client  -> mds    */
#define CEPH_MDS_LEASE_RENEW            3  /* client <-> mds    */
#define CEPH_MDS_LEASE_REVOKE_ACK       4  /* client  -> mds    */

static inline const char *ceph_lease_op_name(int o)
{
	switch (o) {
	case CEPH_MDS_LEASE_REVOKE: return "revoke";
	case CEPH_MDS_LEASE_RELEASE: return "release";
	case CEPH_MDS_LEASE_RENEW: return "renew";
	case CEPH_MDS_LEASE_REVOKE_ACK: return "revoke_ack";
	default: return "???";
	}
}

struct ceph_mds_lease {
	__u8 action;
	__le16 mask;
	__le64 ino;
	__le64 first, last;
	__le32 seq;
	__le32 duration_ms;  /* duration of renewal */
} __attribute__ ((packed));
/* followed by a __le32+string for dname */


/* client reconnect */
struct ceph_mds_cap_reconnect {
	__le64 cap_id;
	__le32 wanted;
	__le32 issued;
	__le64 size;
	struct ceph_timespec mtime, atime;
	__le64 snaprealm;
	__le64 pathbase;
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

#endif
