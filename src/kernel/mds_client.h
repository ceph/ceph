#ifndef _FS_CEPH_MDS_CLIENT_H
#define _FS_CEPH_MDS_CLIENT_H

#include <linux/radix-tree.h>
#include <linux/list.h>
#include <linux/completion.h>
#include <linux/spinlock.h>

#include "messenger.h"
#include "mdsmap.h"

struct ceph_client;

/*
 * for mds reply parsing
 */
struct ceph_mds_reply_info_in {
	struct ceph_mds_reply_inode *in;
	__u32                       symlink_len;
	char                        *symlink;
};

struct ceph_mds_reply_info {
	struct ceph_mds_reply_head    *head;

	int trace_numi, trace_numd;
	struct ceph_mds_reply_info_in *trace_in;
	struct ceph_mds_reply_lease   **trace_ilease;
	struct ceph_mds_reply_dirfrag **trace_dir;
	char                          **trace_dname;
	__u32                         *trace_dname_len;
	struct ceph_mds_reply_lease   **trace_dlease;
	
	struct ceph_mds_reply_dirfrag *dir_dir;
	int                           dir_nr;
	struct ceph_mds_reply_lease   **dir_ilease;
	char                          **dir_dname;
	__u32                         *dir_dname_len;
	struct ceph_mds_reply_lease   **dir_dlease;
	struct ceph_mds_reply_info_in *dir_in;
};

/*
 * state associated with each MDS<->client session
 */
enum {
	CEPH_MDS_SESSION_NEW = 1,
	CEPH_MDS_SESSION_OPENING = 2,
	CEPH_MDS_SESSION_OPEN = 3,
	CEPH_MDS_SESSION_CLOSING = 4,
	CEPH_MDS_SESSION_RESUMING = 5,
	CEPH_MDS_SESSION_RECONNECTING = 6
};
struct ceph_mds_session {
	int               s_mds;
	int               s_state;
	__u64             s_cap_seq;    /* cap message count/seq from mds */
	spinlock_t        s_cap_lock;
	struct list_head  s_caps;
	int               s_nr_caps;
	atomic_t          s_ref;
	struct completion s_completion;
};

/*
 * an in-flight request
 */
struct ceph_mds_request {
	__u64             r_tid;
	struct ceph_msg * r_request;  /* original request */
	struct ceph_msg * r_reply;
	struct ceph_mds_reply_info r_reply_info;
	struct inode    * r_last_inode;
	struct dentry   * r_last_dentry;
	int				r_expects_cap;
	__u64             r_from_time;
	struct ceph_inode_cap * r_cap;
	struct ceph_mds_session * r_session;
	struct ceph_mds_session * r_mds[2];
	int               r_num_mds;    /* items in r_mds */

	int               r_attempts;   /* resend attempts */
	int               r_num_fwd;    /* number of forward attempts */
	int               r_resend_mds; /* mds to resend to next, if any*/

	atomic_t          r_ref;
	struct completion r_completion;
};

/*
 * mds client state
 */
struct ceph_mds_client {
	spinlock_t              lock;          /* all nested structures */
	struct ceph_client      *client;
	struct ceph_mdsmap      *mdsmap;
	struct ceph_mds_session **sessions;    /* NULL if no session */
	int                     max_sessions;  /* len of s_mds_sessions */
	__u64                   last_tid;      /* most recent mds request */
	struct radix_tree_root  request_tree;  /* pending mds requests */
	__u64                   last_requested_map;
	struct completion       map_waiters, session_close_waiters;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 20)
	struct delayed_work     delayed_work;  /* delayed work */
#else
	struct work_struct		delayed_work;  /* delayed work */
#endif
};

extern const char* ceph_mds_op_name(int op);

extern void ceph_mdsc_init(struct ceph_mds_client *mdsc,
			   struct ceph_client *client);
extern void ceph_mdsc_stop(struct ceph_mds_client *mdsc);

extern void ceph_mdsc_handle_map(struct ceph_mds_client *mdsc,
				 struct ceph_msg *msg);
extern void ceph_mdsc_handle_session(struct ceph_mds_client *mdsc,
				     struct ceph_msg *msg);
extern void ceph_mdsc_handle_reply(struct ceph_mds_client *mdsc,
				   struct ceph_msg *msg);
extern void ceph_mdsc_handle_forward(struct ceph_mds_client *mdsc,
				     struct ceph_msg *msg);

extern void ceph_mdsc_handle_filecaps(struct ceph_mds_client *mdsc,
				      struct ceph_msg *msg);
struct ceph_inode_info;
extern int ceph_mdsc_update_cap_wanted(struct ceph_inode_info *ci, int wanted);

extern struct ceph_mds_request *
ceph_mdsc_create_request(struct ceph_mds_client *mdsc, int op,
			 ceph_ino_t ino1, const char *path1,
			 ceph_ino_t ino2, const char *path2);
extern int ceph_mdsc_do_request(struct ceph_mds_client *mdsc,
				struct ceph_mds_request *req);
extern void ceph_mdsc_put_request(struct ceph_mds_request *req);

#endif
