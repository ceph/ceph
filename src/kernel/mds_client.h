#ifndef _FS_CEPH_MDS_CLIENT_H
#define _FS_CEPH_MDS_CLIENT_H

#include <linux/completion.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/radix-tree.h>
#include <linux/spinlock.h>

#include "types.h"
#include "messenger.h"
#include "mdsmap.h"

/*
 * A cluster of MDS (metadata server) daemons is responsible for
 * managing the file system namespace (the directory hierarchy and
 * inodes) and for coordinating shared access to storage.  Metadata is
 * partitioning hierarchically across a number of servers, and that
 * partition varies over time as the cluster adjusts the distribution
 * in order to balance load.
 *
 * The MDS client is primarily responsible to managing synchronous
 * metadata requests for operations like open, unlink, and so forth.
 * If there is a MDS failure, we find out about it when we (possibly
 * request and) receive a new MDS map, and can resubmit affected
 * requests.
 *
 * For the most part, though, we take advantage of a lossless
 * communications channel to the MDS, and do not need to worry about
 * timing out or resubmitting requests.
 *
 * We maintain a stateful "session" with each MDS we interact with.
 * Within each session, we sent periodic heartbeat messages to ensure
 * any capabilities or leases we have been issues remain valid.  If
 * the session times out and goes stale, our leases and capabilities
 * are no longer valid.
 */

/*
 * Some lock dependencies:
 *
 * session->s_mutex
 *         mdsc->mutex
 *
 *         mdsc->snap_rwsem
 *
 *         inode->i_lock
 *                 mdsc->snap_flush_lock
 *                 mdsc->cap_delay_lock
 *
 *
 */

struct ceph_client;
struct ceph_cap;

/*
 * parsed info about a single inode.  pointers are into the encoded
 * on-wire structures within the mds reply message payload.
 */
struct ceph_mds_reply_info_in {
	struct ceph_mds_reply_inode *in;
	u32 symlink_len;
	char *symlink;
	u32 xattr_len;
	char *xattr_data;
};

/*
 * parsed info about an mds reply, including a "trace" from
 * the referenced inode, through its parents up to the root
 * directory, and directory contents (for readdir results).
 */
struct ceph_mds_reply_info_parsed {
	struct ceph_mds_reply_head    *head;

	struct ceph_mds_reply_info_in diri, targeti;
	struct ceph_mds_reply_dirfrag *dirfrag;
	char                          *dname;
	u32                           dname_len;
	struct ceph_mds_reply_lease   *dlease;

	struct ceph_mds_reply_dirfrag *dir_dir;
	int                           dir_nr;
	char                          **dir_dname;
	u32                           *dir_dname_len;
	struct ceph_mds_reply_lease   **dir_dlease;
	struct ceph_mds_reply_info_in *dir_in;
	u8                            dir_complete, dir_end;

	/* encoded blob describing snapshot contexts for certain
	   operations (e.g., open) */
	void *snapblob;
	int snapblob_len;
};

/*
 * state associated with each MDS<->client session
 */
enum {
	CEPH_MDS_SESSION_NEW = 1,
	CEPH_MDS_SESSION_OPENING = 2,
	CEPH_MDS_SESSION_OPEN = 3,
	CEPH_MDS_SESSION_CLOSING = 5,
	CEPH_MDS_SESSION_RECONNECTING = 6
};

#define CAPS_PER_RELEASE ((PAGE_CACHE_SIZE - \
			   sizeof(struct ceph_mds_cap_release)) /	\
			  sizeof(struct ceph_mds_cap_item))

struct ceph_mds_session {
	int               s_mds;
	int               s_state;
	unsigned long     s_ttl;      /* time until mds kills us */
	u64               s_seq;      /* incoming msg seq # */
	struct mutex      s_mutex;    /* serialize session messages */
	spinlock_t        s_cap_lock; /* protects s_caps, s_cap_{gen,ttl} */
	u32               s_cap_gen;  /* inc each time we get mds stale msg */
	unsigned long     s_cap_ttl;  /* when session caps expire */
	unsigned long     s_renew_requested; /* last time we sent a renew req */
	struct list_head  s_caps;     /* all caps issued by this session */
	int               s_nr_caps, s_trim_caps;
	atomic_t          s_ref;
	struct list_head  s_waiting;  /* waiting requests */
	struct list_head  s_unsafe;   /* unsafe requests */

	int               s_num_cap_releases;
	struct list_head  s_cap_releases; /* waiting cap_release messages */
	struct list_head  s_cap_releases_done; /* ready to send */

	struct list_head  s_cap_flushing;      /* inodes w/ flushing caps */
	u64               s_cap_flush_tid;
};

/*
 * modes of choosing which MDS to send a request to
 */
enum {
	USE_ANY_MDS,
	USE_RANDOM_MDS,
	USE_AUTH_MDS,   /* prefer authoritative mds for this metadata item */
};

struct ceph_mds_request;
struct ceph_mds_client;

typedef void (*ceph_mds_request_callback_t) (struct ceph_mds_client *mdsc,
					     struct ceph_mds_request *req);

struct ceph_mds_request_attr {
	struct attribute attr;
	ssize_t (*show)(struct ceph_mds_request *,
			struct ceph_mds_request_attr *,
			char *);
	ssize_t (*store)(struct ceph_mds_request *,
			 struct ceph_mds_request_attr *,
			 const char *, size_t);
};

/*
 * an in-flight mds request
 */
struct ceph_mds_request {
	u64 r_tid;                   /* transaction id */

	int r_op;
	struct inode *r_inode;
	struct dentry *r_dentry;
	struct dentry *r_old_dentry; /* rename from or link from */
	const char *r_path1, *r_path2;
	struct ceph_vino r_ino1, r_ino2;

	union ceph_mds_request_args r_args;
	struct page **r_pages;
	int r_num_pages;
	int r_data_len;

	int r_inode_drop, r_inode_unless;
	int r_dentry_drop, r_dentry_unless;
	int r_old_dentry_drop, r_old_dentry_unless;
	struct inode *r_old_inode;
	int r_old_inode_drop, r_old_inode_unless;

	struct inode *r_target_inode;

	struct ceph_msg  *r_request;  /* original request */
	struct ceph_msg  *r_reply;
	struct ceph_mds_reply_info_parsed r_reply_info;
	int r_err;
	unsigned long r_timeout;  /* optional.  jiffies */

	unsigned long r_started;  /* start time to measure timeout against */
	unsigned long r_request_started; /* start time for mds request only,
					    used to measure lease durations */

	/* for choosing which mds to send this request to */
	int r_direct_mode;
	u32 r_direct_hash;      /* choose dir frag based on this dentry hash */
	bool r_direct_is_hash;  /* true if r_direct_hash is valid */

	struct inode	*r_unsafe_dir;
	struct list_head r_unsafe_dir_item;

	/* references to the trailing dentry and inode from parsing the
	 * mds response.  also used to feed a VFS-provided dentry into
	 * the reply handler */
	int               r_fmode;        /* file mode, if expecting cap */
	struct ceph_mds_session *r_session;
	struct ceph_mds_session *r_fwd_session;  /* forwarded from */
	struct inode     *r_locked_dir; /* dir (if any) i_mutex locked by vfs */

	int               r_attempts;   /* resend attempts */
	int               r_num_fwd;    /* number of forward attempts */
	int               r_num_stale;
	int               r_resend_mds; /* mds to resend to next, if any*/

	atomic_t          r_ref;
	struct list_head  r_wait;
	struct completion r_completion;
	struct completion r_safe_completion;
	ceph_mds_request_callback_t r_callback;
	struct list_head  r_unsafe_item;  /* per-session unsafe list item */
	bool		  r_got_unsafe, r_got_safe;

	bool              r_did_prepopulate;
	u32               r_readdir_offset;

	struct ceph_cap_reservation r_caps_reservation;
	int r_num_caps;
};

/*
 * mds client state
 */
struct ceph_mds_client {
	struct ceph_client      *client;
	struct mutex            mutex;         /* all nested structures */

	struct ceph_mdsmap      *mdsmap;
	struct completion       safe_umount_waiters, session_close_waiters;
	struct list_head        waiting_for_map;

	struct ceph_mds_session **sessions;    /* NULL for mds if no session */
	int                     max_sessions;  /* len of s_mds_sessions */
	int                     stopping;      /* true if shutting down */

	/*
	 * snap_rwsem will cover cap linkage into snaprealms, and
	 * realm snap contexts.  (later, we can do per-realm snap
	 * contexts locks..)  the empty list contains realms with no
	 * references (implying they contain no inodes with caps) that
	 * should be destroyed.
	 */
	struct rw_semaphore     snap_rwsem;
	struct radix_tree_root  snap_realms;
	struct list_head        snap_empty;
	spinlock_t              snap_empty_lock;  /* protect snap_empty */

	u64                    last_tid;      /* most recent mds request */
	struct radix_tree_root request_tree;  /* pending mds requests */
	struct delayed_work    delayed_work;  /* delayed work */
	unsigned long    last_renew_caps;  /* last time we renewed our caps */
	struct list_head cap_delay_list;   /* caps with delayed release */
	spinlock_t       cap_delay_lock;   /* protects cap_delay_list */
	struct list_head snap_flush_list;  /* cap_snaps ready to flush */
	spinlock_t       snap_flush_lock;

	u64               cap_flush_seq;
	struct list_head  cap_dirty;        /* inodes with dirty caps */
	int               num_cap_flushing; /* # caps we are flushing */
	spinlock_t        cap_dirty_lock;   /* protects above items */
	wait_queue_head_t cap_flushing_wq;

	struct dentry 		*debugfs_file;

	spinlock_t		dentry_lru_lock;
	struct list_head	dentry_lru;
	int			num_dentry;
};

extern const char *ceph_mds_op_name(int op);

extern struct ceph_mds_session *__ceph_lookup_mds_session(struct ceph_mds_client *, int mds);

inline static struct ceph_mds_session *
ceph_get_mds_session(struct ceph_mds_session *s)
{
	atomic_inc(&s->s_ref);
	return s;
}

/*
 * requests
 */
static inline void ceph_mdsc_get_request(struct ceph_mds_request *req)
{
	atomic_inc(&req->r_ref);
}

extern void ceph_put_mds_session(struct ceph_mds_session *s);

extern void ceph_send_msg_mds(struct ceph_mds_client *mdsc,
			      struct ceph_msg *msg, int mds);

extern void ceph_mdsc_init(struct ceph_mds_client *mdsc,
			   struct ceph_client *client);
extern void ceph_mdsc_close_sessions(struct ceph_mds_client *mdsc);
extern void ceph_mdsc_stop(struct ceph_mds_client *mdsc);

extern void ceph_mdsc_sync(struct ceph_mds_client *mdsc);

extern void ceph_mdsc_handle_map(struct ceph_mds_client *mdsc,
				 struct ceph_msg *msg);
extern void ceph_mdsc_handle_session(struct ceph_mds_client *mdsc,
				     struct ceph_msg *msg);
extern void ceph_mdsc_handle_reply(struct ceph_mds_client *mdsc,
				   struct ceph_msg *msg);
extern void ceph_mdsc_handle_forward(struct ceph_mds_client *mdsc,
				     struct ceph_msg *msg);

extern void ceph_mdsc_handle_lease(struct ceph_mds_client *mdsc,
				   struct ceph_msg *msg);

extern void ceph_mdsc_lease_release(struct ceph_mds_client *mdsc,
				    struct inode *inode,
				    struct dentry *dn, int mask);

extern struct ceph_mds_request *
ceph_mdsc_create_request(struct ceph_mds_client *mdsc, int op, int mode);
extern void ceph_mdsc_submit_request(struct ceph_mds_client *mdsc,
				     struct ceph_mds_request *req);
extern int ceph_mdsc_do_request(struct ceph_mds_client *mdsc,
				struct inode *listener,
				struct ceph_mds_request *req);
extern void ceph_mdsc_put_request(struct ceph_mds_request *req);

extern void ceph_mdsc_pre_umount(struct ceph_mds_client *mdsc);

extern void ceph_mdsc_handle_reset(struct ceph_mds_client *mdsc, int mds);

extern struct ceph_mds_request *ceph_mdsc_get_listener_req(struct inode *inode,
							   u64 tid);
extern char *ceph_mdsc_build_path(struct dentry *dentry, int *plen, u64 *base,
				  int stop_on_nosnap);

extern void __ceph_mdsc_drop_dentry_lease(struct dentry *dentry);
extern void ceph_mdsc_lease_send_msg(struct ceph_mds_client *mdsc, int mds,
				     struct inode *inode,
				     struct dentry *dentry, char action,
				     u32 seq);

#endif
