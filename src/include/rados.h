#ifndef __RADOS_H
#define __RADOS_H

#include "msgr.h"


/*
 * fs id
 */
typedef struct { unsigned char fsid[16]; } ceph_fsid_t;

static inline int ceph_fsid_compare(const ceph_fsid_t *a,
				    const ceph_fsid_t *b)
{
	return memcmp(a, b, sizeof(*a));
}

/*
 * ino, object, etc.
 */
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
			__le64 pad;
		} __attribute__ ((packed));
	};
} __attribute__ ((packed));

struct ceph_timespec {
	__le32 tv_sec;
	__le32 tv_nsec;
} __attribute__ ((packed));


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
		__u32 pool;      /* implies crush ruleset */
	} pg;
} __attribute__ ((packed));

#define ceph_pg_is_rep(pg)   ((pg).pg.type == CEPH_PG_TYPE_REP)
#define ceph_pg_is_raid4(pg) ((pg).pg.type == CEPH_PG_TYPE_RAID4)

/*
 * pg_pool is a set of pgs storing a pool of objects
 *
 *  pg_num -- base number of pseudorandomly placed pgs
 *
 *  pgp_num -- effective number when calculating pg placement.  this
 * is used for pg_num increases.  new pgs result in data being "split"
 * into new pgs.  for this to proceed smoothly, new pgs are intiially
 * colocated with their parents; that is, pgp_num doesn't increase
 * until the new pgs have successfully split.  only _then_ are the new
 * pgs placed independently.
 *
 *  lpg_num -- localized pg count (per device).  replicas are randomly
 * selected.
 *
 *  lpgp_num -- as above.
 */
struct ceph_pg_pool {
	__u8 type;
	__u8 size;
	__u8 crush_ruleset;
	__le32 pg_num, pgp_num;
	__le32 lpg_num, lpgp_num;
	__le32 last_change;     /* most recent epoch changed */
} __attribute__ ((packed));

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
	__le32 epoch;
	__le64 version;
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
 * osd map flag bits
 */
#define CEPH_OSDMAP_NEARFULL (1<<0)  /* sync writes (near ENOSPC) */
#define CEPH_OSDMAP_FULL     (1<<1)  /* no data writes (ENOSPC) */
#define CEPH_OSDMAP_PAUSERD  (1<<2)  /* pause all reads */
#define CEPH_OSDMAP_PAUSEWR  (1<<3)  /* pause all writes */
#define CEPH_OSDMAP_PAUSEREC (1<<4)  /* pause recovery */

/*
 * osd ops
 */
#define CEPH_OSD_OP_MODE       0xf000
#define CEPH_OSD_OP_MODE_RD    0x1000
#define CEPH_OSD_OP_MODE_WR    0x2000
#define CEPH_OSD_OP_MODE_SUB   0x4000
#define CEPH_OSD_OP_MODE_EXEC  0x8000

#define CEPH_OSD_OP_TYPE       0x0f00
#define CEPH_OSD_OP_TYPE_LOCK  0x0100
#define CEPH_OSD_OP_TYPE_DATA  0x0200
#define CEPH_OSD_OP_TYPE_ATTR  0x0300
#define CEPH_OSD_OP_TYPE_EXEC  0x0400

enum {
	/** data **/
	/* read */
	CEPH_OSD_OP_READ      = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 1,
	CEPH_OSD_OP_STAT      = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 2,

	/* fancy read */
	CEPH_OSD_OP_GREP      = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 3,
	CEPH_OSD_OP_MASKTRUNC = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 4,

	/* write */
	CEPH_OSD_OP_WRITE     = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 1,
	CEPH_OSD_OP_WRITEFULL = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 2,
	CEPH_OSD_OP_TRUNCATE  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 3,
	CEPH_OSD_OP_ZERO      = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 4,
	CEPH_OSD_OP_DELETE    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 5,

	/* fancy write */
	CEPH_OSD_OP_APPEND    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 6,
	CEPH_OSD_OP_STARTSYNC = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 7,
	CEPH_OSD_OP_SETTRUNC  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 8,
	CEPH_OSD_OP_TRIMTRUNC = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 9,

	/** attrs **/
	/* read */
	CEPH_OSD_OP_GETXATTR  = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_ATTR | 1,
	CEPH_OSD_OP_GETXATTRS = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_ATTR | 2,

	/* write */
	CEPH_OSD_OP_SETXATTR  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | 1,
	CEPH_OSD_OP_SETXATTRS = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | 2,
	CEPH_OSD_OP_RESETXATTRS = CEPH_OSD_OP_MODE_WR|CEPH_OSD_OP_TYPE_ATTR | 3,
	CEPH_OSD_OP_RMXATTR   = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | 4,

	/** subop **/
	CEPH_OSD_OP_PULL           = CEPH_OSD_OP_MODE_SUB | 1,
	CEPH_OSD_OP_PUSH           = CEPH_OSD_OP_MODE_SUB | 2,
	CEPH_OSD_OP_BALANCEREADS   = CEPH_OSD_OP_MODE_SUB | 3,
	CEPH_OSD_OP_UNBALANCEREADS = CEPH_OSD_OP_MODE_SUB | 4,
	CEPH_OSD_OP_SCRUB          = CEPH_OSD_OP_MODE_SUB | 5,

	/** lock **/
	CEPH_OSD_OP_WRLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 1,
	CEPH_OSD_OP_WRUNLOCK  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 2,
	CEPH_OSD_OP_RDLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 3,
	CEPH_OSD_OP_RDUNLOCK  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 4,
	CEPH_OSD_OP_UPLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 5,
	CEPH_OSD_OP_DNLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 6,

	/** exec **/
	CEPH_OSD_OP_RDCALL    = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_EXEC | 1,
	CEPH_OSD_OP_LOAD_CLASS =  CEPH_OSD_OP_MODE_RD|CEPH_OSD_OP_TYPE_EXEC | 2,
	CEPH_OSD_OP_WRCALL    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_EXEC | 1,
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
static inline int ceph_osd_op_type_exec(int op)
{
	return (op & CEPH_OSD_OP_TYPE) == CEPH_OSD_OP_TYPE_EXEC;
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

	case CEPH_OSD_OP_GREP: return "grep";
	case CEPH_OSD_OP_MASKTRUNC: return "masktrunc";

	case CEPH_OSD_OP_WRITE: return "write";
	case CEPH_OSD_OP_DELETE: return "delete";
	case CEPH_OSD_OP_TRUNCATE: return "truncate";
	case CEPH_OSD_OP_ZERO: return "zero";
	case CEPH_OSD_OP_WRITEFULL: return "writefull";

	case CEPH_OSD_OP_APPEND: return "append";
	case CEPH_OSD_OP_STARTSYNC: return "startsync";
	case CEPH_OSD_OP_SETTRUNC: return "settrunc";
	case CEPH_OSD_OP_TRIMTRUNC: return "trimtrunc";

	case CEPH_OSD_OP_GETXATTR: return "getxattr";
	case CEPH_OSD_OP_GETXATTRS: return "getxattrs";
	case CEPH_OSD_OP_SETXATTR: return "setxattr";
	case CEPH_OSD_OP_SETXATTRS: return "setxattrs";
	case CEPH_OSD_OP_RESETXATTRS: return "resetxattrs";
	case CEPH_OSD_OP_RMXATTR: return "rmxattr";

	case CEPH_OSD_OP_PULL: return "pull";
	case CEPH_OSD_OP_PUSH: return "push";
	case CEPH_OSD_OP_BALANCEREADS: return "balance-reads";
	case CEPH_OSD_OP_UNBALANCEREADS: return "unbalance-reads";
	case CEPH_OSD_OP_SCRUB: return "scrub";

	case CEPH_OSD_OP_WRLOCK: return "wrlock";
	case CEPH_OSD_OP_WRUNLOCK: return "wrunlock";
	case CEPH_OSD_OP_RDLOCK: return "rdlock";
	case CEPH_OSD_OP_RDUNLOCK: return "rdunlock";
	case CEPH_OSD_OP_UPLOCK: return "uplock";
	case CEPH_OSD_OP_DNLOCK: return "dnlock";

	case CEPH_OSD_OP_RDCALL: return "rdcall";
	case CEPH_OSD_OP_WRCALL: return "wrcall";

	default: return "???";
	}
}


/*
 * osd op flags
 */
enum {
	CEPH_OSD_FLAG_ACK = 1,          /* want (or is) "ack" ack */
	CEPH_OSD_FLAG_ONNVRAM = 2,      /* want (or is) "onnvram" ack */
	CEPH_OSD_FLAG_ONDISK = 4,       /* want (or is) "ondisk" ack */
	CEPH_OSD_FLAG_RETRY = 8,        /* resend attempt */
	CEPH_OSD_FLAG_INCLOCK_FAIL = 16, /* fail on inclock collision */
	CEPH_OSD_FLAG_MODIFY = 32,      /* op is/was a mutation */
	CEPH_OSD_FLAG_ORDERSNAP = 64,   /* EOLDSNAP if snapc is out of order */
	CEPH_OSD_FLAG_PEERSTAT = 128,   /* msg includes osd_peer_stat */
	CEPH_OSD_FLAG_BALANCE_READS = 256,
};

#define EOLDSNAPC    ERESTART  /* ORDERSNAP flag set; writer has old snapc*/
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
		struct {
			__le64 truncate_size;
			__le32 truncate_seq;
		};
		struct {
			__u8 class_len;
			__u8 method_len;
			__u8 argc;
			__le32 indata_len;
		} __attribute__ ((packed));
	};
} __attribute__ ((packed));

struct ceph_osd_request_head {
	__le64                    tid;
	__le32                    client_inc;
	struct ceph_object        oid;
	struct ceph_object_layout layout;
	__le32                    osdmap_epoch;

	__le32                    flags;
	__le32                    inc_lock;

	struct ceph_timespec      mtime;
	struct ceph_eversion      reassert_version;

	/* writer's snap context */
	union {
		__le64 snap_seq;
		__le64 snapid;
	};
	__le32 num_snaps;

	/* read or mutation */
	__le16 num_ops;
	__u16 object_type;
	struct ceph_osd_op ops[];  /* followed by snaps */
} __attribute__ ((packed));

struct ceph_osd_reply_head {
	__le64               tid;
	__le32               client_inc;
	__le32               flags;
	struct ceph_object   oid;
	struct ceph_object_layout layout;
	__le32               osdmap_epoch;
	struct ceph_eversion reassert_version;

	__le32 result;

	__le32 num_ops;
	struct ceph_osd_op ops[0];
} __attribute__ ((packed));


#endif
