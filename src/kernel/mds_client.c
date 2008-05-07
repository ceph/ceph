
#include <linux/wait.h>
#include <linux/sched.h>
#include "mds_client.h"
#include "mon_client.h"

#include "ceph_fs.h"

int ceph_debug_mdsc = -1;
#define DOUT_VAR ceph_debug_mdsc
#define DOUT_PREFIX "mds: "
#include "super.h"
#include "messenger.h"
#include "decode.h"

/*
 * note: this also appears in messages/MClientRequest.h,
 * but i don't want it inline in the kernel.
 */
const char *ceph_mds_op_name(int op)
{
  switch (op) {
  case CEPH_MDS_OP_STAT:  return "stat";
  case CEPH_MDS_OP_LSTAT: return "lstat";
  case CEPH_MDS_OP_UTIME: return "utime";
  case CEPH_MDS_OP_LUTIME: return "lutime";
  case CEPH_MDS_OP_CHMOD: return "chmod";
  case CEPH_MDS_OP_LCHMOD: return "lchmod";
  case CEPH_MDS_OP_CHOWN: return "chown";
  case CEPH_MDS_OP_LCHOWN: return "lchown";
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
  default: return "unknown";
  }
}

static void send_msg_mds(struct ceph_mds_client *mdsc, struct ceph_msg *msg,
			 int mds)
{
	msg->hdr.dst.addr = *ceph_mdsmap_get_addr(mdsc->mdsmap, mds);
	msg->hdr.dst.name.type = cpu_to_le32(CEPH_ENTITY_TYPE_MDS);
	msg->hdr.dst.name.num = cpu_to_le32(mds);
	ceph_msg_send(mdsc->client->msgr, msg, BASE_DELAY_INTERVAL);
}


/*
 * mds reply parsing
 */
static int parse_reply_info_in(void **p, void *end,
			       struct ceph_mds_reply_info_in *info)
{
	int err = -EINVAL;
	info->in = *p;
	*p += sizeof(struct ceph_mds_reply_inode) +
		sizeof(__u32)*le32_to_cpu(info->in->fragtree.nsplits);
	ceph_decode_32_safe(p, end, info->symlink_len, bad);
	ceph_decode_need(p, end, info->symlink_len, bad);
	info->symlink = *p;
	*p += info->symlink_len;
	return 0;
bad:
	return err;
}

static int parse_reply_info_trace(void **p, void *end,
				  struct ceph_mds_reply_info *info)
{
	__u16 numi, numd;
	int err = -EINVAL;

	ceph_decode_need(p, end, 2*sizeof(__u32), bad);
	ceph_decode_16(p, numi);
	ceph_decode_16(p, numd);
	info->trace_numi = numi;
	info->trace_numd = numd;
	if (numi == 0) {
		info->trace_in = 0;
		goto done;   /* hrm, this shouldn't actually happen, but.. */
	}

	/* alloc one big shared array */
	info->trace_in = kmalloc(numi * (sizeof(*info->trace_in) +
					 2*sizeof(*info->trace_ilease) +
					 sizeof(*info->trace_dir) +
					 sizeof(*info->trace_dname) +
					 sizeof(*info->trace_dname_len)),
				 GFP_NOFS);
	if (info->trace_in == NULL)
		goto badmem;

	info->trace_ilease = (void *)(info->trace_in + numi);
	info->trace_dir = (void *)(info->trace_ilease + numi);
	info->trace_dname = (void *)(info->trace_dir + numd);
	info->trace_dname_len = (void *)(info->trace_dname + numd);
	info->trace_dlease = (void *)(info->trace_dname_len + numd);

	if (numi == numd)
		goto dentry;
inode:
	if (!numi)
		goto done;
	numi--;
	err = parse_reply_info_in(p, end, &info->trace_in[numi]);
	if (err < 0)
		goto bad;
	info->trace_ilease[numi] = *p;
	*p += sizeof(struct ceph_mds_reply_lease);

dentry:
	if (!numd)
		goto done;
	numd--;
	ceph_decode_32_safe(p, end, info->trace_dname_len[numd], bad);
	ceph_decode_need(p, end, info->trace_dname_len[numd], bad);
	info->trace_dname[numd] = *p;
	*p += info->trace_dname_len[numd];
	info->trace_dlease[numd] = *p;
	*p += sizeof(struct ceph_mds_reply_lease);

	/* dir */
	info->trace_dir[numd] = *p;
	*p += sizeof(struct ceph_mds_reply_dirfrag) +
		sizeof(__u32)*le32_to_cpu(info->trace_dir[numd]->ndist);
	if (unlikely(*p > end))
		goto bad;
	goto inode;

done:
	if (unlikely(*p != end))
		return -EINVAL;
	return 0;

badmem:
	err = -ENOMEM;
bad:
	derr(1, "problem parsing trace %d\n", err);
	return err;
}

static int parse_reply_info_dir(void **p, void *end, 
				struct ceph_mds_reply_info *info)
{
	__u32 num, i = 0;
	int err = -EINVAL;

	info->dir_dir = *p;
	if (*p + sizeof(*info->dir_dir) > end)
		goto bad;
	*p += sizeof(*info->dir_dir) + sizeof(__u32)*info->dir_dir->ndist;
	if (*p > end)
		goto bad;

	ceph_decode_32_safe(p, end, num, bad);
	if (num == 0)
		goto done;

	/* alloc large array */
	info->dir_nr = num;
	info->dir_in = kmalloc(num * (sizeof(*info->dir_in) +
				      sizeof(*info->dir_ilease) +
				      sizeof(*info->dir_dname) +
				      sizeof(*info->dir_dname_len) +
				      sizeof(*info->dir_dlease)),
			       GFP_NOFS);
	if (info->dir_in == NULL)
		goto badmem;
	info->dir_ilease = (void *)(info->dir_in + num);
	info->dir_dname = (void *)(info->dir_ilease + num);
	info->dir_dname_len = (void *)(info->dir_dname + num);
	info->dir_dlease = (void *)(info->dir_dname_len + num);

	while (num) {
		/* dentry */
		ceph_decode_32_safe(p, end, info->dir_dname_len[i], bad);
		ceph_decode_need(p, end, info->dir_dname_len[i], bad);
		info->dir_dname[i] = *p;
		*p += info->dir_dname_len[i];
		dout(20, "parsed dir dname '%.*s'\n", info->dir_dname_len[i],
		     info->dir_dname[i]);
		info->dir_dlease[i] = *p;
		*p += sizeof(struct ceph_mds_reply_lease);

		/* inode */
		err = parse_reply_info_in(p, end, &info->dir_in[i]);
		if (err < 0)
			goto bad;
		info->dir_ilease[i] = *p;
		*p += sizeof(struct ceph_mds_reply_lease);
		i++;
		num--;
	}

done:
	return 0;

badmem:
	err = -ENOMEM;
bad:
	derr(1, "problem parsing dir contents %d\n", err);
	return err;
}

static int parse_reply_info(struct ceph_msg *msg, 
			    struct ceph_mds_reply_info *info)
{
	void *p, *end;
	__u32 len;
	int err = -EINVAL;

	memset(info, 0, sizeof(*info));
	info->head = msg->front.iov_base;

	/* trace */
	p = msg->front.iov_base + sizeof(struct ceph_mds_reply_head);
	end = p + msg->front.iov_len;
	ceph_decode_32_safe(&p, end, len, bad);
	if (len > 0) {
		err = parse_reply_info_trace(&p, p+len, info);
		if (err < 0)
			goto bad;
	}

	/* dir content */
	ceph_decode_32_safe(&p, end, len, bad);
	if (len > 0) {
		err = parse_reply_info_dir(&p, p+len, info);
		if (err < 0)
			goto bad;
	}

	return 0;
bad:
	derr(1, "parse_reply err %d\n", err);
	return err;
}

static void destroy_reply_info(struct ceph_mds_reply_info *info)
{
	kfree(info->trace_in);
	kfree(info->dir_in);
}


/*
 * sessions
 */

static struct ceph_mds_session *__get_session(struct ceph_mds_client *mdsc,
					      int mds)
{
	struct ceph_mds_session *session;
	if (mds >= mdsc->max_sessions || mdsc->sessions[mds] == 0)
		return NULL;
	session = mdsc->sessions[mds];
	atomic_inc(&session->s_ref);
	return session;
}

static void put_session(struct ceph_mds_session *s)
{
	BUG_ON(s == NULL);
	dout(30, "put_session %p %d -> %d\n", s,
	     atomic_read(&s->s_ref), atomic_read(&s->s_ref)-1);
	if (atomic_dec_and_test(&s->s_ref))
		kfree(s);
}

/*
 * create+register a new session for given mds.
 *  drop locks for kmalloc, check for races.
 */
static struct ceph_mds_session *
__register_session(struct ceph_mds_client *mdsc, int mds)
{
	struct ceph_mds_session *s;

	spin_unlock(&mdsc->lock);

	s = kmalloc(sizeof(struct ceph_mds_session), GFP_NOFS);
	s->s_mds = mds;
	s->s_state = CEPH_MDS_SESSION_NEW;
	s->s_seq = 0;
	mutex_init(&s->s_mutex);
	spin_lock_init(&s->s_cap_lock);
	s->s_cap_ttl = 0;
	s->s_cap_gen = 0;
	s->s_renew_requested = 0;
	INIT_LIST_HEAD(&s->s_caps);
	INIT_LIST_HEAD(&s->s_inode_leases);
	INIT_LIST_HEAD(&s->s_dentry_leases);
	s->s_nr_caps = 0;
	atomic_set(&s->s_ref, 1);
	init_completion(&s->s_completion);

	spin_lock(&mdsc->lock);

	/* register */
	dout(10, "register_session mds%d\n", mds);
	if (mds >= mdsc->max_sessions) {
		int newmax = 1 << get_count_order(mds+1);
		struct ceph_mds_session **sa;

		dout(50, "register_session realloc to %d\n", newmax);
		spin_unlock(&mdsc->lock);
		sa = kzalloc(newmax * sizeof(void *), GFP_NOFS);
		spin_lock(&mdsc->lock);
		if (sa == NULL)
			return ERR_PTR(-ENOMEM);
		if (mdsc->max_sessions < newmax) {
			if (mdsc->sessions) {
				memcpy(sa, mdsc->sessions,
				       mdsc->max_sessions *
				       sizeof(struct ceph_mds_session));
				kfree(mdsc->sessions);
			}
			mdsc->sessions = sa;
			mdsc->max_sessions = newmax;
		} else {
			kfree(sa);  /* lost race */
		}
	}
	if (mdsc->sessions[mds]) {
		put_session(s); /* lost race */
		return mdsc->sessions[mds];
	} else {
		mdsc->sessions[mds] = s;
		atomic_inc(&s->s_ref);
		return s;
	}
}

static void __unregister_session(struct ceph_mds_client *mdsc, int mds)
{
	dout(10, "__unregister_session mds%d %p\n", mds, mdsc->sessions[mds]);
	put_session(mdsc->sessions[mds]);
	mdsc->sessions[mds] = 0;
}


/*
 * requests
 */

static void get_request(struct ceph_mds_request *req)
{
	atomic_inc(&req->r_ref);
}

static void put_request_sessions(struct ceph_mds_request *req)
{
	if (req->r_session) {
		put_session(req->r_session);
		req->r_session = 0;
	}
	if (req->r_fwd_session) {
		put_session(req->r_fwd_session);
		req->r_fwd_session = 0;
	}
}

void ceph_mdsc_put_request(struct ceph_mds_request *req)
{
	dout(10, "put_request %p %d -> %d\n", req,
	     atomic_read(&req->r_ref), atomic_read(&req->r_ref)-1);
	if (atomic_dec_and_test(&req->r_ref)) {
		if (req->r_request)
			ceph_msg_put(req->r_request);
		if (req->r_reply) {
			ceph_msg_put(req->r_reply);
			destroy_reply_info(&req->r_reply_info);
		}
		if (req->r_last_inode)
			iput(req->r_last_inode);
		if (req->r_last_dentry)
			dput(req->r_last_dentry);
		put_request_sessions(req);
		kfree(req);
	}
}

static struct ceph_mds_request *
find_request_and_lock(struct ceph_mds_client *mdsc, __u64 tid)
{
	struct ceph_mds_request *req;
	spin_lock(&mdsc->lock);
	req = radix_tree_lookup(&mdsc->request_tree, tid);
	if (req)
		get_request(req);
	else
		spin_unlock(&mdsc->lock);
	return req;
}

static struct ceph_mds_request *new_request(struct ceph_msg *msg)
{
	struct ceph_mds_request *req;

	req = kmalloc(sizeof(*req), GFP_NOFS);
	req->r_request = msg;
	req->r_reply = 0;
	req->r_last_inode = 0;
	req->r_last_dentry = 0;
	req->r_old_dentry = 0;
	req->r_expects_cap = 0;
	req->r_fmode = 0;
	req->r_cap = 0;
	req->r_session = 0;
	req->r_fwd_session = 0;
	req->r_attempts = 0;
	req->r_num_fwd = 0;
	req->r_resend_mds = -1;
	atomic_set(&req->r_ref, 1);  /* one for request_tree, one for caller */
	init_completion(&req->r_completion);

	return req;
}


/*
 * register an in-flight request.
 * fill in tid in msg request header
 */
static void __register_request(struct ceph_mds_client *mdsc,
			       struct ceph_mds_request *req)
{
	struct ceph_mds_request_head *head = req->r_request->front.iov_base;
	req->r_tid = head->tid = ++mdsc->last_tid;
	dout(30, "__register_request %p tid %lld\n", req, req->r_tid);
	get_request(req);
	radix_tree_insert(&mdsc->request_tree, req->r_tid, (void *)req);
}

static void __unregister_request(struct ceph_mds_client *mdsc,
				 struct ceph_mds_request *req)
{
	dout(30, "unregister_request %p tid %lld\n", req, req->r_tid);
	radix_tree_delete(&mdsc->request_tree, req->r_tid);
	ceph_mdsc_put_request(req);
}


/*
 * choose mds to send request to next
 */
static int choose_mds(struct ceph_mds_client *mdsc,
		      struct ceph_mds_request *req)
{
	/* is there a specific mds we should try? */
	if (req->r_resend_mds >= 0 &&
	    ceph_mdsmap_get_state(mdsc->mdsmap, req->r_resend_mds) > 0)
		return req->r_resend_mds;

	/* pick one at random */
	return ceph_mdsmap_get_random_mds(mdsc->mdsmap);
}


/*
 * session messages
 */
static struct ceph_msg *create_session_msg(__u32 op, __u64 seq)
{
	struct ceph_msg *msg;
	struct ceph_mds_session_head *h;

	msg = ceph_msg_new(CEPH_MSG_CLIENT_SESSION, sizeof(*h), 0, 0, 0);
	if (IS_ERR(msg)) {
		derr("ENOMEM creating session msg\n");
		return ERR_PTR(-ENOMEM);
	}
	h = msg->front.iov_base;
	h->op = cpu_to_le32(op);
	h->seq = cpu_to_le64(seq);
	/*h->stamp = ....*/

	return msg;
}

static void wait_for_new_map(struct ceph_mds_client *mdsc)
{
	__u32 have;
	dout(30, "wait_for_new_map enter\n");
	have = mdsc->mdsmap->m_epoch;
	if (mdsc->last_requested_map < mdsc->mdsmap->m_epoch) {
		mdsc->last_requested_map = have;
		spin_unlock(&mdsc->lock);
		ceph_monc_request_mdsmap(&mdsc->client->monc, have);
	} else
		spin_unlock(&mdsc->lock);
	wait_for_completion(&mdsc->map_waiters);
	spin_lock(&mdsc->lock);
	dout(30, "wait_for_new_map exit\n");
}

static int open_session(struct ceph_mds_client *mdsc,
			struct ceph_mds_session *session)
{
	struct ceph_msg *msg;
	int mstate;
	int mds = session->s_mds;

	/* mds active? */
	mstate = ceph_mdsmap_get_state(mdsc->mdsmap, mds);
	dout(10, "open_session to mds%d, state %d\n", mds, mstate);
	if (mstate < CEPH_MDS_STATE_ACTIVE) {
		wait_for_new_map(mdsc);
		mstate = ceph_mdsmap_get_state(mdsc->mdsmap, mds);
		if (mstate < CEPH_MDS_STATE_ACTIVE) {
			dout(30, "open_session mds%d now %d still not active\n",
			     mds, mstate);
			return -EAGAIN;  /* hrm, try again? */
		}
	}

	session->s_state = CEPH_MDS_SESSION_OPENING;
	session->s_renew_requested = jiffies;
	spin_unlock(&mdsc->lock);

	/* send connect message */
	msg = create_session_msg(CEPH_SESSION_REQUEST_OPEN, session->s_seq);
	if (IS_ERR(msg))
		return PTR_ERR(msg);
	send_msg_mds(mdsc, msg, mds);

	/* wait for session to open (or fail, or close) */
	dout(30, "open_session waiting on session %p\n", session);
	wait_for_completion(&session->s_completion);
	dout(30, "open_session done waiting on session %p, state %d\n",
	     session, session->s_state);

	spin_lock(&mdsc->lock);
	return 0;
}

/*
 * caller must hold session s_mutex
 */
static void remove_session_caps(struct ceph_mds_session *session)
{
	struct ceph_inode_cap *cap;
	struct ceph_inode_info *ci;

	/*
	 * fixme: when we start locking the inode, make sure
	 * we don't deadlock with __remove_cap in inode.c.
	 */
	dout(10, "remove_session_caps on %p\n", session);
	while (session->s_nr_caps > 0) {
		cap = list_entry(session->s_caps.next, struct ceph_inode_cap,
				 session_caps);
		ci = cap->ci;
		dout(10, "removing cap %p, ci is %p, inode is %p\n",
		     cap, ci, &ci->vfs_inode);
		ceph_remove_cap(cap);
	}
	BUG_ON(session->s_nr_caps > 0);
}

/*
 * caller must hold session s_mutex
 */
static void revoke_dentry_lease(struct dentry *dentry)
{
	struct ceph_dentry_info *di;

	/* detach from dentry */
	spin_lock(&dentry->d_lock);
	di = ceph_dentry(dentry);
	if (di) {
		list_del(&di->lease_item);
		kfree(di);
		dentry->d_fsdata = 0;
	}
	spin_unlock(&dentry->d_lock);
	if (di)
		dput(dentry);
}

/*
 * caller must hold session s_mutex
 */
static void revoke_inode_lease(struct ceph_inode_info *ci, int mask)
{
	int drop = 0;

	spin_lock(&ci->vfs_inode.i_lock);
	dout(10, "revoke_inode_lease on inode %p, mask %d -> %d\n",
	     &ci->vfs_inode, ci->i_lease_mask, ci->i_lease_mask & ~mask);
	if (ci->i_lease_mask & mask) {
		ci->i_lease_mask &= ~mask;
		if (ci->i_lease_mask == 0) {
			list_del_init(&ci->i_lease_item);
			ci->i_lease_session = 0;
			drop = 1;
		}
	}
	spin_unlock(&ci->vfs_inode.i_lock);
	if (drop) {
		dout(10, "lease iput on %p\n", &ci->vfs_inode);
		iput(&ci->vfs_inode);
	}
}

/*
 * remove expire leases for this session.  unpint parent
 * inode/dentries, so that [di]cache can prune them.
 */
static void trim_session_leases(struct ceph_mds_session *session)
{
	struct ceph_inode_info *ci;
	struct ceph_dentry_info *di;
	struct dentry *dentry;

	dout(20, "trim_session_leases on session %p\n", session);

	/* inodes */
	while (!list_empty(&session->s_inode_leases)) {
		ci = list_first_entry(&session->s_inode_leases,
				      struct ceph_inode_info, i_lease_item);
		spin_lock(&ci->vfs_inode.i_lock);
		if (time_before(jiffies, ci->i_lease_ttl)) {
			spin_unlock(&ci->vfs_inode.i_lock);
			break;
		}
		dout(20, "trim_session_leases inode %p mask %d\n",
		     &ci->vfs_inode, ci->i_lease_mask);
		ci->i_lease_session = 0;
		ci->i_lease_mask = 0;
		list_del_init(&ci->i_lease_item);
		spin_unlock(&ci->vfs_inode.i_lock);
		iput(&ci->vfs_inode);
	}

	/* dentries */
	while (!list_empty(&session->s_dentry_leases)) {
		di = list_first_entry(&session->s_dentry_leases,
				      struct ceph_dentry_info, lease_item);
		dentry = di->dentry;
		spin_lock(&dentry->d_lock);
		if (time_before(jiffies, dentry->d_time)) {
			spin_unlock(&dentry->d_lock);
			break;
		}
		dout(20, "trim_session_leases dentry %p\n", dentry);
		list_del(&di->lease_item);
		kfree(di);
		dentry->d_fsdata = 0;
		spin_unlock(&dentry->d_lock);
		dput(dentry);
	}
}

/*
 * caller must hold session s_mutex
 */
static void remove_session_leases(struct ceph_mds_session *session)
{
	struct ceph_inode_info *ci;
	struct ceph_dentry_info *di;

	dout(10, "remove_session_leases on %p\n", session);

	/* inodes */
	while (!list_empty(&session->s_inode_leases)) {
		ci = list_entry(session->s_inode_leases.next,
				struct ceph_inode_info, i_lease_item);
		dout(10, "removing lease from inode %p\n", &ci->vfs_inode);
		revoke_inode_lease(ci, ci->i_lease_mask);
	}

	/* dentries */
	while (!list_empty(&session->s_dentry_leases)) {
		di = list_entry(session->s_dentry_leases.next,
				struct ceph_dentry_info, lease_item);
		dout(10, "removing lease from dentry %p\n", di->dentry);
		revoke_dentry_lease(di->dentry);
	}
}

/*
 * wake up any threads waiting on caps
 */
static void wake_up_session_caps(struct ceph_mds_session *session)
{
	struct list_head *p;
	struct ceph_inode_cap *cap;

	dout(10, "wake_up_session_caps %p mds%d\n", session, session->s_mds);
	list_for_each(p, &session->s_caps) {
		cap = list_entry(p, struct ceph_inode_cap, session_caps);
		dout(20, "waking up waiters on %p\n", &cap->ci->vfs_inode);
		wake_up(&cap->ci->i_cap_wq);
	}
}

/*
 * kick outstanding requests
 */
static void kick_requests(struct ceph_mds_client *mdsc, int mds, int all)
{
	struct ceph_mds_request *reqs[10];
	u64 nexttid = 0;
	int i, got;

	dout(20, "kick_requests mds%d\n", mds);
	while (nexttid < mdsc->last_tid) {
		got = radix_tree_gang_lookup(&mdsc->request_tree,
					     (void **)&reqs, nexttid, 10);
		if (got == 0)
			break;
		nexttid = reqs[got-1]->r_tid + 1;
		for (i = 0; i < got; i++) {
			if ((reqs[i]->r_session &&
			     reqs[i]->r_session->s_mds == mds) ||
			    (all && reqs[i]->r_fwd_session &&
			     reqs[i]->r_fwd_session->s_mds == mds)) {
				dout(10, " kicking req %llu\n", reqs[i]->r_tid);
				put_request_sessions(reqs[i]);
				complete(&reqs[i]->r_completion);
			}
		}
	}
}

/*
 * caller hols s_mutex
 */
static int send_renew_caps(struct ceph_mds_client *mdsc,
			   struct ceph_mds_session *session)
{
	struct ceph_msg *msg;

	if (time_after_eq(jiffies, session->s_cap_ttl) &&
	    time_after_eq(session->s_cap_ttl, session->s_renew_requested))
		dout(1, "mds%d session caps stale\n", session->s_mds);

	dout(10, "send_renew_caps to mds%d\n", session->s_mds);
	session->s_renew_requested = jiffies;
	msg = create_session_msg(CEPH_SESSION_REQUEST_RENEWCAPS, 0);
	if (IS_ERR(msg))
		return PTR_ERR(msg);
	send_msg_mds(mdsc, msg, session->s_mds);
	return 0;
}


void ceph_mdsc_handle_session(struct ceph_mds_client *mdsc,
			      struct ceph_msg *msg)
{
	__u32 op;
	__u64 seq;
	struct ceph_mds_session *session = 0;
	int mds = le32_to_cpu(msg->hdr.src.name.num);
	struct ceph_mds_session_head *h = msg->front.iov_base;
	int was_stale;

	if (msg->front.iov_len != sizeof(*h))
		goto bad;

	/* decode */
	op = le32_to_cpu(h->op);
	seq = le64_to_cpu(h->seq);

	/* handle */
	spin_lock(&mdsc->lock);
	session = __get_session(mdsc, mds);
	mutex_lock(&session->s_mutex);

	was_stale = session->s_cap_ttl == 0 ||
		time_after_eq(jiffies, session->s_cap_ttl);

	dout(2, "handle_session %p op %d seq %llu %s\n", session, op, seq, 
	     was_stale ? "stale":"not stale");
	switch (op) {
	case CEPH_SESSION_OPEN:
		dout(2, "session open from mds%d\n", mds);
		session->s_state = CEPH_MDS_SESSION_OPEN;
		spin_lock(&session->s_cap_lock);
		session->s_cap_ttl = session->s_renew_requested +
			mdsc->mdsmap->m_cap_bit_timeout*HZ;
		spin_unlock(&session->s_cap_lock);
		complete(&session->s_completion);
		break;

	case CEPH_SESSION_CLOSE:
		dout(2, "session close from mds%d\n", mds);
		complete(&session->s_completion); /* for good measure */
		__unregister_session(mdsc, mds);
		remove_session_caps(session);
		remove_session_leases(session);
		complete(&mdsc->session_close_waiters);
		kick_requests(mdsc, mds, 0); /* cur only */
		break;

	case CEPH_SESSION_RENEWCAPS:
		spin_lock(&session->s_cap_lock);
		session->s_cap_ttl = session->s_renew_requested +
			mdsc->mdsmap->m_cap_bit_timeout*HZ;
		spin_unlock(&session->s_cap_lock);
		dout(10, "session renewed caps from mds%d, ttl now %lu\n", mds,
			session->s_cap_ttl);
		if (was_stale && time_before(jiffies, session->s_cap_ttl)) {
			dout(1, "mds%d session caps renewed\n", session->s_mds);
			wake_up_session_caps(session);
		}
		break;

	case CEPH_SESSION_STALE:
		dout(1, "mds%d session caps went stale, renewing\n", 
		     session->s_mds);
		spin_lock(&session->s_cap_lock);
		session->s_cap_gen++;
		session->s_cap_ttl = 0;
		spin_unlock(&session->s_cap_lock);
		send_renew_caps(mdsc, session);
		break;

	default:
		derr(0, "bad session op %d from mds%d\n", op, mds);
		WARN_ON(1);
	}
	spin_unlock(&mdsc->lock);

	mutex_unlock(&session->s_mutex);
	put_session(session);
	return;

bad:
	derr(1, "corrupt session message, len %d, expected %d\n",
	     (int)msg->front.iov_len, (int)sizeof(*h));
	return;
}



/* exported functions */

struct ceph_mds_request *
ceph_mdsc_create_request(struct ceph_mds_client *mdsc, int op,
			 ceph_ino_t ino1, const char *path1,
			 ceph_ino_t ino2, const char *path2)
{
	struct ceph_msg *msg;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *head;
	void *p, *end;
	int pathlen;

	pathlen = 2*(sizeof(ino1) + sizeof(__u32));
	if (path1)
		pathlen += strlen(path1);
	if (path2)
		pathlen += strlen(path2);

	msg = ceph_msg_new(CEPH_MSG_CLIENT_REQUEST,
			   sizeof(struct ceph_mds_request_head) + pathlen,
			   0, 0, 0);
	if (IS_ERR(msg))
		return ERR_PTR(PTR_ERR(msg));
	req = new_request(msg);
	if (IS_ERR(req)) {
		ceph_msg_put(msg);
		return req;
	}
	head = msg->front.iov_base;
	p = msg->front.iov_base + sizeof(*head);
	end = msg->front.iov_base + msg->front.iov_len;

	/* encode head */
	head->client_inst = mdsc->client->msgr->inst;
	/* tid, oldest_client_tid set by do_request */
	head->mdsmap_epoch = cpu_to_le64(mdsc->mdsmap->m_epoch);
	head->num_fwd = 0;
	/* head->retry_attempt = 0; set by do_request */
	head->mds_wants_replica_in_dirino = 0;
	head->op = cpu_to_le32(op);
	head->caller_uid = cpu_to_le32(current->euid);
	head->caller_gid = cpu_to_le32(current->egid);

	/* encode paths */
	ceph_encode_filepath(&p, end, ino1, path1);
	ceph_encode_filepath(&p, end, ino2, path2);
	dout(10, "create_request op %d=%s -> %p\n", op,
	     ceph_mds_op_name(op), req);
	if (path1)
		dout(10, "create_request  path1 %llx/%s\n", ino1, path1);
	if (path2)
		dout(10, "create_request  path2 %llx/%s\n", ino2, path2);

	BUG_ON(p != end);
	return req;
}

/*
 * return oldest (lowest) tid in request tree, 0 if none.
 */
static __u64 __get_oldest_tid(struct ceph_mds_client *mdsc)
{
	struct ceph_mds_request *first;
	if (radix_tree_gang_lookup(&mdsc->request_tree,
				   (void **)&first, 0, 1) <= 0)
		return 0;
	return first->r_tid;
}

int ceph_mdsc_do_request(struct ceph_mds_client *mdsc,
			 struct ceph_mds_request *req)
{
	struct ceph_mds_session *session;
	struct ceph_mds_request_head *rhead;
	int err;
	int mds = -1;

	dout(30, "do_request on %p\n", req);
	BUG_ON(le32_to_cpu(req->r_request->hdr.type) !=
	       CEPH_MSG_CLIENT_REQUEST);

	radix_tree_preload(GFP_NOFS);
	spin_lock(&mdsc->lock);
	__register_request(mdsc, req);

retry:
	mds = choose_mds(mdsc, req);
	if (mds < 0) {
		dout(30, "do_request waiting for new mdsmap\n");
		wait_for_new_map(mdsc);
		goto retry;
	}

	/* get session */
	session = __get_session(mdsc, mds);
	if (!session)
		session = __register_session(mdsc, mds);
	dout(30, "do_request mds%d session %p state %d\n", mds, session, 
	     session->s_state);

	/* open? */
	err = 0;
	if (session->s_state == CEPH_MDS_SESSION_NEW ||
	    session->s_state == CEPH_MDS_SESSION_CLOSING) {
		err = open_session(mdsc, session);
		dout(30, "do_request open_session err=%d\n", err);
		BUG_ON(err && err != -EAGAIN);
	}
	if (session->s_state != CEPH_MDS_SESSION_OPEN ||
		err == -EAGAIN) {
		dout(30, "do_request session %p not open, state=%d, waiting\n",
		     session, session->s_state);
		put_session(session);
		goto retry;
	}

	/* make request? */
	if (req->r_session == 0)
		req->r_from_time = jiffies;
	BUG_ON(req->r_session);
	req->r_session = session;
	req->r_resend_mds = -1;  /* forget any specific mds hint */
	req->r_attempts++;
	rhead = req->r_request->front.iov_base;
	rhead->retry_attempt = cpu_to_le32(req->r_attempts-1);
	rhead->oldest_client_tid = cpu_to_le64(__get_oldest_tid(mdsc));

	/* send and wait */
	spin_unlock(&mdsc->lock);
	dout(10, "do_request %p r_expects_cap=%d\n", req, req->r_expects_cap);
	req->r_request = ceph_msg_maybe_dup(req->r_request);  
	ceph_msg_get(req->r_request);
	send_msg_mds(mdsc, req->r_request, mds);
	wait_for_completion(&req->r_completion);
	spin_lock(&mdsc->lock);
	if (req->r_reply == NULL)
		goto retry;

	/* clean up request, parse reply */
	__unregister_request(mdsc, req);
	spin_unlock(&mdsc->lock);

	if (IS_ERR(req->r_reply)) {
		err = PTR_ERR(req->r_reply);
		req->r_reply = 0;
		dout(10, "do_request returning err %d from reply handler\n",
		     err);
		return err;
	}

	ceph_msg_put(req->r_request);
	req->r_request = 0;

	err = le32_to_cpu(req->r_reply_info.head->result);
	dout(30, "do_request done on %p result %d\n", req, err);
	return err;
}

void ceph_mdsc_handle_reply(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	struct ceph_mds_request *req;
	struct ceph_mds_reply_head *head = msg->front.iov_base;
	struct ceph_mds_reply_info *rinfo;
	__u64 tid;
	int err, result;
	int mds;
	__u32 cap, capseq;

	/* extract tid */
	if (msg->front.iov_len < sizeof(*head)) {
		derr(1, "handle_reply got corrupt (short) reply\n");
		return;
	}
	tid = le64_to_cpu(head->tid);

	/* get request, session */
	req = find_request_and_lock(mdsc, tid);
	if (!req) {
		dout(1, "handle_reply on unknown tid %llu\n", tid);
		return;
	}
	dout(10, "handle_reply %p r_expects_cap=%d\n", req, req->r_expects_cap);
	mds = le32_to_cpu(msg->hdr.src.name.num);
	if (req->r_session && req->r_session->s_mds != mds) {
		put_session(req->r_session);
		req->r_session = __get_session(mdsc, mds);
	}
	if (req->r_session == 0) {
		derr(1, "got reply on %llu, but no session for mds%d\n",
		     tid, mds);
		spin_unlock(&mdsc->lock);
		return;
	}
	BUG_ON(req->r_reply);
	spin_unlock(&mdsc->lock);

	mutex_lock(&req->r_session->s_mutex);

	/* parse */
	rinfo = &req->r_reply_info;
	err = parse_reply_info(msg, rinfo);
	if (err < 0) {
		derr(0, "handle_reply got corrupt reply\n");
		goto done;
	}

	result = le32_to_cpu(rinfo->head->result);
	dout(10, "handle_reply tid %lld result %d\n", tid, result);

	/* insert trace into our cache */
	err = ceph_fill_trace(mdsc->client->sb, req, req->r_session);
	if (err)
		goto done;
	if (result == 0) {
		/* caps? */
		if (req->r_expects_cap && req->r_last_inode) {
			cap = le32_to_cpu(rinfo->head->file_caps);
			capseq = le32_to_cpu(rinfo->head->file_caps_seq);
			req->r_cap = ceph_add_cap(req->r_last_inode,
						  req->r_session,
						  req->r_fmode,
						  cap, capseq);
			if (IS_ERR(req->r_cap)) {
				err = PTR_ERR(req->r_cap);
				req->r_cap = 0;
				goto done;
			}
		}

		/* readdir result? */
		ceph_readdir_prepopulate(req);
	}

done:
	mutex_unlock(&req->r_session->s_mutex);
	spin_lock(&mdsc->lock);
	if (err) {
		req->r_reply = ERR_PTR(err);
	} else {
		req->r_reply = msg;
		ceph_msg_get(msg);
	}
	spin_unlock(&mdsc->lock);

	/* kick calling process */
	complete(&req->r_completion);
	ceph_mdsc_put_request(req);
	return;
}



/*
 * handle mds notification that our request has been forwarded.
 */
void ceph_mdsc_handle_forward(struct ceph_mds_client *mdsc,
			      struct ceph_msg *msg)
{
	struct ceph_mds_request *req;
	__u64 tid;
	__u32 next_mds;
	__u32 fwd_seq;
	int err = -EINVAL;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;
	int from_mds = le32_to_cpu(msg->hdr.src.name.num);

	/* decode */
	ceph_decode_need(&p, end, sizeof(__u64)+2*sizeof(__u32), bad);
	ceph_decode_64(&p, tid);
	ceph_decode_32(&p, next_mds);
	ceph_decode_32(&p, fwd_seq);

	/* handle */
	req = find_request_and_lock(mdsc, tid);
	if (!req)
		return;  /* dup reply? */

	/* do we have a session with the dest mds? */
	if (next_mds < mdsc->max_sessions &&
	    mdsc->sessions[next_mds] &&
	    mdsc->sessions[next_mds]->s_state == CEPH_MDS_SESSION_OPEN) {
		/* yes.  adjust mds set */
		if (fwd_seq > req->r_num_fwd) {
			req->r_num_fwd = fwd_seq;
			req->r_resend_mds = next_mds;
			put_request_sessions(req);
			req->r_session = __get_session(mdsc, next_mds);
			req->r_fwd_session = __get_session(mdsc, from_mds);
		}
		spin_unlock(&mdsc->lock);
	} else {
		/* no, resend. */
		/* forward race not possible; mds would drop */
		BUG_ON(fwd_seq <= req->r_num_fwd);
		put_request_sessions(req);
		req->r_resend_mds = next_mds;
		spin_unlock(&mdsc->lock);
		complete(&req->r_completion);
	}

	ceph_mdsc_put_request(req);
	return;

bad:
	derr(0, "problem decoding message, err=%d\n", err);
}



/*
 * send an reconnect to a recovering mds
 */
static void send_mds_reconnect(struct ceph_mds_client *mdsc, int mds)
{
	struct ceph_mds_session *session;
	struct ceph_msg *reply;
	int newlen, len = 4 + 1;
	void *p, *end;
	struct list_head *cp;
	struct ceph_inode_cap *cap;
	char *path;
	int pathlen, err;
	struct dentry *dentry;
	struct ceph_inode_info *ci;
	struct ceph_mds_cap_reconnect *rec;
	int count;

	dout(1, "reconnect to recovering mds%d\n", mds);

	/* find session */
	session = __get_session(mdsc, mds);
	if (session) {
		session->s_state = CEPH_MDS_SESSION_RECONNECTING;
		session->s_seq = 0;

		/* estimate needed space */
		len += session->s_nr_caps *
			sizeof(struct ceph_mds_cap_reconnect);
		len += session->s_nr_caps * (100); /* guess! */
		dout(40, "estimating i need %d bytes for %d caps\n",
		     len, session->s_nr_caps);
	} else {
		dout(20, "no session for mds%d, will send short reconnect\n",
		     mds);
	}

	spin_unlock(&mdsc->lock);  /* drop lock for duration */

	if (session)
		mutex_lock(&session->s_mutex);

retry:
	/* build reply */
	reply = ceph_msg_new(CEPH_MSG_CLIENT_RECONNECT, len, 0, 0, 0);
	if (IS_ERR(reply)) {
		err = PTR_ERR(reply);
		goto bad;
	}
	p = reply->front.iov_base;
	end = p + len;

	if (!session) {
		ceph_encode_8(&p, 1); /* session was closed */
		ceph_encode_32(&p, 0);
		goto send;
	}
	dout(10, "session %p state %d\n", session, session->s_state);

	/* traverse this session's caps */
	ceph_encode_8(&p, 0);
	ceph_encode_32(&p, session->s_nr_caps);
	count = 0;
	list_for_each(cp, &session->s_caps) {
		cap = list_entry(cp, struct ceph_inode_cap, session_caps);
		ci = cap->ci;
		ceph_decode_need(&p, end, sizeof(u64) +
				 sizeof(struct ceph_mds_cap_reconnect),
				 needmore);
		dout(10, " adding cap %p on ino %llx inode %p\n", cap,
		     ceph_ino(&ci->vfs_inode), &ci->vfs_inode);
		ceph_encode_64(&p, ceph_ino(&ci->vfs_inode));
		rec = p;
		p += sizeof(*rec);
		BUG_ON(p > end);
		spin_lock(&ci->vfs_inode.i_lock);
		cap->seq = 0;  /* reset cap seq */
		rec->wanted = cpu_to_le32(__ceph_caps_wanted(ci));
		rec->issued = cpu_to_le32(__ceph_caps_issued(ci));
		rec->size = cpu_to_le64(ci->vfs_inode.i_size);
		ceph_encode_timespec(&rec->mtime, &ci->vfs_inode.i_mtime);
		ceph_encode_timespec(&rec->atime, &ci->vfs_inode.i_atime);
		spin_unlock(&ci->vfs_inode.i_lock);

		dentry = d_find_alias(&ci->vfs_inode);
		if (dentry) {
			path = ceph_build_dentry_path(dentry, &pathlen);
			if (IS_ERR(path)) {
				err = PTR_ERR(path);
				BUG_ON(err);
			}
		} else {
			path = 0;
			pathlen = 0;
		}
		ceph_decode_need(&p, end, pathlen+4, needmore);
		ceph_encode_string(&p, end, path, pathlen);
		kfree(path);
		dput(dentry);
		count++;
	}

send:
	reply->front.iov_len = p - reply->front.iov_base;
	reply->hdr.front_len = cpu_to_le32(reply->front.iov_len);
	dout(10, "final len was %u (guessed %d)\n",
	     (unsigned)reply->front.iov_len, len);
	send_msg_mds(mdsc, reply, mds);

	spin_lock(&mdsc->lock);
	if (session) {
		if (session->s_state == CEPH_MDS_SESSION_RECONNECTING) {
			session->s_state = CEPH_MDS_SESSION_OPEN;
			complete(&session->s_completion);
		} else {
			dout(0, "WARNING: reconnect on %p raced and lost?\n",
			     session);
		}
	}
out:
	if (session) {
		mutex_unlock(&session->s_mutex);
		put_session(session);
	}
	return;

needmore:
	newlen = len * (session->s_nr_caps+1);
	do_div(newlen, (count+1));
	dout(30, "i guessed %d, and did %d of %d, retrying with %d\n",
	     len, count, session->s_nr_caps, newlen);
	len = newlen;
	ceph_msg_put(reply);
	goto retry;

bad:
	spin_lock(&mdsc->lock);
	derr(0, "error %d generating reconnect.  what to do?\n", err);
	/* fixme */
	WARN_ON(1);
	goto out;
}

/*
 * fixme: reconnect to an mds that closed our session
 */



/*
 * compare old and new mdsmaps, kicking requests
 * and closing out old connections as necessary
 */
static void check_new_map(struct ceph_mds_client *mdsc,
			  struct ceph_mdsmap *newmap,
			  struct ceph_mdsmap *oldmap)
{
	int i;
	int oldstate, newstate;
	struct ceph_mds_session *session;

	dout(20, "check_new_map new %u old %u\n",
	     newmap->m_epoch, oldmap->m_epoch);

	for (i = 0; i < oldmap->m_max_mds; i++) {
		if (mdsc->sessions[i] == 0)
			continue;
		oldstate = ceph_mdsmap_get_state(oldmap, i);
		newstate = ceph_mdsmap_get_state(newmap, i);

		dout(20, "check_new_map mds%d state %d -> %d\n",
		     i, oldstate, newstate);
		if (newstate >= oldstate)
			continue;  /* no problem */

		/* notify messenger */
		ceph_messenger_mark_down(mdsc->client->msgr,
					 &oldmap->m_addr[i]);

		/* kill session */
		session = mdsc->sessions[i];
		switch (session->s_state) {
		case CEPH_MDS_SESSION_OPENING:
			complete(&session->s_completion);
			__unregister_session(mdsc, i);
			break;
		case CEPH_MDS_SESSION_OPEN:
			kick_requests(mdsc, i, 1); /* cur or forwarder */
			break;
		}
	}
}


/* caps */

static void send_cap_ack(struct ceph_mds_client *mdsc, __u64 ino, int caps,
			 int wanted, __u32 seq, __u64 size, __u64 max_size,
			 struct timespec *mtime, struct timespec *atime,
			 u64 time_warp_seq, int mds)
{
	struct ceph_mds_file_caps *fc;
	struct ceph_msg *msg;

	dout(10, "send_cap_ack ino %llx caps %d wanted %d seq %u size %llu\n",
	     ino, caps, wanted, (unsigned)seq, size);

	msg = ceph_msg_new(CEPH_MSG_CLIENT_FILECAPS, sizeof(*fc), 0, 0, 0);
	if (IS_ERR(msg))
		return;

	fc = msg->front.iov_base;

	memset(fc, 0, sizeof(*fc));

	fc->op = cpu_to_le32(CEPH_CAP_OP_ACK);  /* misnomer */
	fc->seq = cpu_to_le64(seq);
	fc->caps = cpu_to_le32(caps);
	fc->wanted = cpu_to_le32(wanted);
	fc->ino = cpu_to_le64(ino);
	fc->size = cpu_to_le64(size);
	fc->max_size = cpu_to_le64(max_size);
	if (mtime)
		ceph_encode_timespec(&fc->mtime, mtime);
	if (atime)
		ceph_encode_timespec(&fc->atime, atime);
	fc->time_warp_seq = cpu_to_le64(time_warp_seq);

	send_msg_mds(mdsc, msg, mds);
}

void ceph_mdsc_handle_filecaps(struct ceph_mds_client *mdsc,
			       struct ceph_msg *msg)
{
	struct super_block *sb = mdsc->client->sb;
	struct ceph_client *client = ceph_sb_to_client(sb);
	struct ceph_mds_session *session;
	struct inode *inode;
	struct ceph_mds_file_caps *h;
	int mds = le32_to_cpu(msg->hdr.src.name.num);
	int op;
	u32 seq;
	u64 ino, size, max_size;
	ino_t inot;

	dout(10, "handle_filecaps from mds%d\n", mds);

	/* decode */
	if (msg->front.iov_len != sizeof(*h))
		goto bad;
	h = msg->front.iov_base;
	op = le32_to_cpu(h->op);
	ino = le64_to_cpu(h->ino);
	seq = le32_to_cpu(h->seq);
	size = le64_to_cpu(h->size);
	max_size = le64_to_cpu(h->max_size);

	/* find session */
	spin_lock(&mdsc->lock);
	session = __get_session(&client->mdsc, mds);
	spin_unlock(&mdsc->lock);
	if (!session) {
		dout(10, "WTF, got filecap but no session for mds%d\n", mds);
		return;
	}

	mutex_lock(&session->s_mutex);
	session->s_seq++;

	/* lookup ino */
	inot = ceph_ino_to_ino(ino);
#if BITS_PER_LONG == 64
	inode = ilookup(sb, ino);
#else
	inode = ilookup5(sb, inot, ceph_ino_compare, &ino);
#endif
	dout(20, "op %d ino %llx inode %p\n", op, ino, inode);

	if (!inode) {
		dout(10, "wtf, i don't have ino %lu=%llx?  closing out cap\n",
		     inot, ino);
		send_cap_ack(mdsc, ino, 0, 0, seq, size, 0, 0, 0, 0, mds);
		goto no_inode;
	}

	switch (op) {
	case CEPH_CAP_OP_GRANT:
		if (ceph_handle_cap_grant(inode, h, session) == 1) {
			dout(10, "sending reply back to mds%d\n", mds);
			ceph_msg_get(msg);
			send_msg_mds(mdsc, msg, mds);
		}
		break;

	case CEPH_CAP_OP_TRUNC:
		ceph_handle_cap_trunc(inode, h, session);
		break;

	case CEPH_CAP_OP_EXPORT:
	case CEPH_CAP_OP_IMPORT:
		dout(10, "cap export/import -- IMPLEMENT ME\n");
		break;
	}

	iput(inode);
no_inode:
	mutex_unlock(&session->s_mutex);
	put_session(session);
	return;

bad:
	dout(10, "corrupt filecaps message\n");
	return;
}

static void __cap_delay_cancel(struct ceph_mds_client *mdsc,
			       struct ceph_inode_info *ci)
{
	dout(10, "__cap_delay_cancel %p\n", &ci->vfs_inode);
	if (list_empty(&ci->i_cap_delay_list))
		return;
	spin_lock(&mdsc->cap_delay_lock);
	list_del_init(&ci->i_cap_delay_list);
	spin_unlock(&mdsc->cap_delay_lock);
	iput(&ci->vfs_inode);
}

/*
 * called with i_lock, then drops it.
 * caller should hold s_mutex.
 * 
 * returns true if we removed the last cap on this inode.
 */
int __ceph_mdsc_send_cap(struct ceph_mds_client *mdsc,
			 struct ceph_mds_session *session,
			 struct ceph_inode_cap *cap,
			 int used, int wanted, int cancel_work)
{
	struct ceph_inode_info *ci = cap->ci;
	struct inode *inode = &ci->vfs_inode;
	int revoking = cap->implemented & ~cap->issued;
	int dropping = cap->issued & ~wanted;
	int keep;
	__u64 seq, time_warp_seq;
	__u64 size, max_size;
	struct timespec mtime, atime;
	int removed_last = 0;

	dout(10, "__send_cap cap %p session %p %d -> %d\n", cap, cap->session,
	     cap->issued, cap->issued & wanted);
	cap->issued &= wanted;  /* drop bits we don't want */
	
	if (revoking && (revoking && used) == 0)
		cap->implemented = cap->issued;
	
	keep = cap->issued;
	seq = cap->seq;
	size = inode->i_size;
	ci->i_reported_size = size;
	max_size = ci->i_wanted_max_size;
	ci->i_requested_max_size = max_size;
	mtime = inode->i_mtime;
	atime = inode->i_atime;
	time_warp_seq = ci->i_time_warp_seq;
	if (wanted == 0) {
		__ceph_remove_cap(cap);
		removed_last = list_empty(&ci->i_caps);
		if (removed_last && cancel_work)
			__cap_delay_cancel(mdsc, ci);
	}
	spin_unlock(&inode->i_lock);

	if (dropping & CEPH_CAP_RDCACHE) {
		/*
		 * FIXME: this will block if there is a locked page..
		 */
		dout(20, "invalidating pages on %p\n", inode);
		invalidate_mapping_pages(&inode->i_data, 0, -1);
		dout(20, "done invalidating pages on %p\n", inode);
	}

	send_cap_ack(mdsc, ceph_ino(inode),
		     keep, wanted, seq,
		     size, max_size, &mtime, &atime, time_warp_seq,
		     session->s_mds);

	if (wanted == 0)
		iput(inode);  /* removed cap */

	return removed_last;
}

static void check_delayed_caps(struct ceph_mds_client *mdsc)
{
	struct ceph_inode_info *ci;

	dout(10, "check_delayed_caps\n");
	while (1) {
		spin_lock(&mdsc->cap_delay_lock);
		if (list_empty(&mdsc->cap_delay_list))
			break;
		ci = list_first_entry(&mdsc->cap_delay_list,
				      struct ceph_inode_info,
				      i_cap_delay_list);
		if (time_before(jiffies, ci->i_hold_caps_until))
			break;
		list_del_init(&ci->i_cap_delay_list);
		spin_unlock(&mdsc->cap_delay_lock);
		dout(10, "check_delayed_caps on %p\n", &ci->vfs_inode);
		ceph_check_caps(ci, 1);
		iput(&ci->vfs_inode);
	}
	spin_unlock(&mdsc->cap_delay_lock);
}

static void flush_write_caps(struct ceph_mds_client *mdsc,
			     struct ceph_mds_session *session, 
			     int purge)
{
	struct list_head *p, *n;
	
	list_for_each_safe (p, n, &session->s_caps) {
		struct ceph_inode_cap *cap = 
			list_entry(p, struct ceph_inode_cap, session_caps);
		struct inode *inode = &cap->ci->vfs_inode;
		int used, wanted;

		__ceph_do_pending_vmtruncate(inode);

		mutex_lock(&inode->i_mutex);
                filemap_write_and_wait(inode->i_mapping);
		mutex_unlock(&inode->i_mutex);

		spin_lock(&inode->i_lock);
		if ((cap->implemented & (CEPH_CAP_WR|CEPH_CAP_WRBUFFER)) == 0) {
			spin_unlock(&inode->i_lock);
			continue;
		}

		used = __ceph_caps_used(cap->ci);
		wanted = __ceph_caps_wanted(cap->ci);

		if (purge && (used || wanted)) {
			derr(0, "residual caps on %p used %d wanted %d s=%llu wrb=%d\n", 
			     inode, used, wanted, inode->i_size,
			     atomic_read(&cap->ci->i_wrbuffer_ref));
			used = wanted = 0;
		}

		__ceph_mdsc_send_cap(mdsc, session, cap, used, wanted, 1);
	}
}

static int close_session(struct ceph_mds_client *mdsc,
			 struct ceph_mds_session *session)
{
	int mds = session->s_mds;
	struct ceph_msg *msg;
	int err = 0;

	dout(10, "close_session mds%d\n", mds);
	mutex_lock(&session->s_mutex);
	
	if (session->s_state >= CEPH_MDS_SESSION_CLOSING)
		goto done;

	flush_write_caps(mdsc, session, 1);
	
	session->s_state = CEPH_MDS_SESSION_CLOSING;
	msg = create_session_msg(CEPH_SESSION_REQUEST_CLOSE,
				 session->s_seq);
	if (IS_ERR(msg)) {
		err = PTR_ERR(msg);
		goto done;
	}
	send_msg_mds(mdsc, msg, mds);

done:
	mutex_unlock(&session->s_mutex);
	return err;
}

/*
 * leases
 */

void ceph_mdsc_handle_lease(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	struct super_block *sb = mdsc->client->sb;
	struct inode *inode;
	struct ceph_mds_session *session;
	struct ceph_inode_info *ci;
	struct dentry *parent, *dentry;
	int mds = le32_to_cpu(msg->hdr.src.name.num);
	struct ceph_mds_lease *h = msg->front.iov_base;
	u64 ino;
	int mask;
	struct qstr dname;
	ino_t inot;

	dout(10, "handle_lease from mds%d\n", mds);

	/* decode */
	if (msg->front.iov_len < sizeof(*h) + sizeof(u32))
		goto bad;
	ino = le64_to_cpu(h->ino);
	mask = le16_to_cpu(h->mask);
	dname.name = (void *)h + sizeof(*h) + sizeof(u32);
	dname.len = msg->front.iov_len - sizeof(*h) - sizeof(u32);
	if (dname.len != le32_to_cpu(*(u32 *)(h+1)))
		goto bad;

	/* find session */
	spin_lock(&mdsc->lock);
	session = __get_session(mdsc, mds);
	spin_unlock(&mdsc->lock);
	if (!session) {
		dout(10, "WTF, got lease but no session for mds%d\n", mds);
		return;
	}
	session->s_seq++;

	mutex_lock(&session->s_mutex);

	/* lookup inode */
	inot = ceph_ino_to_ino(ino);
#if BITS_PER_LONG == 64
	inode = ilookup(sb, ino);
#else
	inode = ilookup5(sb, inot, ceph_ino_compare, &ino);
#endif
	dout(20, "action is %d, mask %d, ino %llx %p\n", h->action, 
	     mask, ino, inode);

	BUG_ON(h->action != CEPH_MDS_LEASE_REVOKE);  /* for now */

	if (inode == NULL) {
		dout(10, "i don't have inode %llx\n", ino);
		goto release;
	}

	/* inode */
	ci = ceph_inode(inode);
	revoke_inode_lease(ci, mask);

	/* dentry */
	if (mask & CEPH_LOCK_DN) {
		dout(10, "dname %.*s\n", dname.len, dname.name);
		parent = d_find_alias(inode);
		if (!parent) {
			dout(10, "no parent dentry on inode %p\n", inode);
			WARN_ON(1);
			goto release;  /* hrm... */
		}
		dname.hash = full_name_hash(dname.name, dname.len);
		dentry = d_lookup(parent, &dname);
		dput(parent);
		if (!dentry)
			goto release;
		revoke_dentry_lease(dentry);
		dout(10, "lease revoked on dentry %p\n", dentry);
		dput(dentry);
	}

release:
	iput(inode);
	dout(10, "sending release\n");
	h->action = CEPH_MDS_LEASE_RELEASE;
	ceph_msg_get(msg);
	send_msg_mds(mdsc, msg, mds);
	mutex_unlock(&session->s_mutex);
	return;

bad:
	dout(0, "corrupt lease message\n");
}


/*
 * pass inode always.  dentry is optional.
 */
void ceph_mdsc_lease_release(struct ceph_mds_client *mdsc, struct inode *inode,
			     struct dentry *dentry, int mask)
{
	struct ceph_msg *msg;
	struct ceph_mds_lease *lease;
	struct ceph_inode_info *ci;
	int origmask = mask;
	int mds = -1;
	int len = sizeof(*lease) + sizeof(__u32);
	int dnamelen = 0;
	__u64 ino;

	BUG_ON(inode == 0);
	if ((mask & CEPH_LOCK_DN) &&
	    dentry && dentry->d_fsdata &&
	    ceph_dentry(dentry)->lease_session->s_mds >= 0 &&
	    time_before(jiffies, dentry->d_time)) {
		dnamelen = dentry->d_name.len;
		len += dentry->d_name.len;
		mds = ceph_dentry(dentry)->lease_session->s_mds;
	} else
		mask &= ~CEPH_LOCK_DN;  /* nothing to release */
	ci = ceph_inode(inode);
	ino = ci->i_ceph_ino;
	if (ci->i_lease_session && 
	    time_before(jiffies, ci->i_lease_ttl) &&
	    ci->i_lease_session->s_mds >= 0) {
		mds = ci->i_lease_session->s_mds;
		mask &= CEPH_LOCK_DN | ci->i_lease_mask;  /* lease is valid */
		ci->i_lease_mask &= ~mask;
	} else
		mask &= CEPH_LOCK_DN;  /* no lease; clear all but DN bits */
	if (mask == 0) {
		dout(10, "lease_release inode %p (%d) dentry %p -- "
		     "no lease on %d\n",
		     inode, ci->i_lease_mask, dentry, origmask);
		return;  /* nothing to drop */
	}
	BUG_ON(mds < 0);

	dout(10, "lease_release inode %p dentry %p mask %d to mds%d\n", inode,
	     dentry, mask, mds);

	msg = ceph_msg_new(CEPH_MSG_CLIENT_LEASE, len, 0, 0, 0);
	if (IS_ERR(msg))
		return;
	lease = msg->front.iov_base;
	lease->action = CEPH_MDS_LEASE_RELEASE;
	lease->mask = mask;
	lease->ino = cpu_to_le64(ino); /* ?? */
	*(__le32 *)(lease+1) = cpu_to_le32(dnamelen);
	if (dentry)
		memcpy((void *)(lease + 1) + 4, dentry->d_name.name, dnamelen);

	send_msg_mds(mdsc, msg, mds);
}


/*
 * delayed work -- renew caps with mds
 */
static void schedule_delayed(struct ceph_mds_client *mdsc)
{
	/*
	 * renew at 1/2 the advertised timeout period.
	 */
	int delay = 5;
	unsigned hz = round_jiffies_relative(HZ * delay);
	dout(10, "schedule_delayed for %d seconds (%u hz)\n", delay, hz);
	schedule_delayed_work(&mdsc->delayed_work, hz);
}

static void delayed_work(struct work_struct *work)
{
	int i;
	struct ceph_mds_client *mdsc =
		container_of(work, struct ceph_mds_client, delayed_work.work);
	int renew_interval = mdsc->mdsmap->m_cap_bit_timeout >> 2;
	int renew_caps = time_after_eq(jiffies, HZ*renew_interval + 
				       mdsc->last_renew_caps);

	dout(10, "delayed_work on %p renew_caps=%d\n", mdsc, renew_caps);

	check_delayed_caps(mdsc);

	spin_lock(&mdsc->lock);
	if (renew_caps)
		mdsc->last_renew_caps = jiffies;

	for (i = 0; i < mdsc->max_sessions; i++) {
		struct ceph_mds_session *session = __get_session(mdsc, i);
		if (session == 0)
			continue;
		if (session->s_state < CEPH_MDS_SESSION_OPEN) {
			put_session(session);
			continue;
		}
		spin_unlock(&mdsc->lock);
		mutex_lock(&session->s_mutex);

		if (renew_caps)
			send_renew_caps(mdsc, session);
		trim_session_leases(session);

		mutex_unlock(&session->s_mutex);
		put_session(session);
		spin_lock(&mdsc->lock);
	}
	spin_unlock(&mdsc->lock);
	schedule_delayed(mdsc);
}


void ceph_mdsc_init(struct ceph_mds_client *mdsc, struct ceph_client *client)
{
	spin_lock_init(&mdsc->lock);
	mdsc->client = client;
	mdsc->mdsmap = 0;            /* none yet */
	mdsc->sessions = 0;
	mdsc->max_sessions = 0;
	mdsc->last_tid = 0;
	INIT_RADIX_TREE(&mdsc->request_tree, GFP_ATOMIC);
	mdsc->last_requested_map = 0;
	init_completion(&mdsc->map_waiters);
	init_completion(&mdsc->session_close_waiters);
	INIT_DELAYED_WORK(&mdsc->delayed_work, delayed_work);
	mdsc->last_renew_caps = jiffies;
	INIT_LIST_HEAD(&mdsc->cap_delay_list);
	spin_lock_init(&mdsc->cap_delay_lock);
}

/*
 * drop all leases (and dentry refs) in preparation for umount
 */
static void drop_leases(struct ceph_mds_client *mdsc)
{
	int i;
	
	spin_lock(&mdsc->lock);
	for (i = 0; i < mdsc->max_sessions; i++) {
		struct ceph_mds_session *session = __get_session(mdsc, i);
		if (!session)
			continue;
		spin_unlock(&mdsc->lock);
		remove_session_leases(session);
		spin_lock(&mdsc->lock);
	}
	spin_unlock(&mdsc->lock);
}

/*
 * called before mount is ro, and before dentries are torn down.
 * (hmm, does this still race with new lookups?)
 */
void ceph_mdsc_pre_umount(struct ceph_mds_client *mdsc)
{
	drop_leases(mdsc);
	check_delayed_caps(mdsc);
}

/*
 * called after sb is ro.
 */
void ceph_mdsc_stop(struct ceph_mds_client *mdsc)
{
	struct ceph_mds_session *session;
	int i;
	int n;

	dout(10, "stop\n");

	cancel_delayed_work_sync(&mdsc->delayed_work); /* cancel timer */

	spin_lock(&mdsc->cap_delay_lock);
	while (!list_empty(&mdsc->cap_delay_list)) {
		struct ceph_inode_info *ci;
		ci = list_first_entry(&mdsc->cap_delay_list,
				      struct ceph_inode_info,
				      i_cap_delay_list);
		list_del_init(&ci->i_cap_delay_list);
		spin_unlock(&mdsc->cap_delay_lock);
		iput(&ci->vfs_inode);
		spin_lock(&mdsc->cap_delay_lock);
	}
	spin_unlock(&mdsc->cap_delay_lock);

	spin_lock(&mdsc->lock);

	/* close sessions, caps */
	while (1) {
		dout(10, "closing sessions\n");
		n = 0;
		for (i = 0; i < mdsc->max_sessions; i++) {
			session = __get_session(mdsc, i);
			if (!session)
				continue;
			spin_unlock(&mdsc->lock);
			close_session(mdsc, session);
			spin_lock(&mdsc->lock);
			n++;
		}
		if (n == 0)
			break;

		dout(10, "waiting for sessions to close\n");
		spin_unlock(&mdsc->lock);
		wait_for_completion(&mdsc->session_close_waiters);
		spin_lock(&mdsc->lock);
	}

	WARN_ON(!list_empty(&mdsc->cap_delay_list));

	spin_unlock(&mdsc->lock);
	dout(10, "stopped\n");
}


/*
 * handle mds map update.
 */
void ceph_mdsc_handle_map(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	ceph_epoch_t epoch;
	__u32 maplen;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;
	struct ceph_mdsmap *newmap, *oldmap;
	int from = le32_to_cpu(msg->hdr.src.name.num);
	int newstate;
	struct ceph_fsid fsid;

	ceph_decode_need(&p, end, sizeof(fsid)+2*sizeof(__u32), bad);
	ceph_decode_64(&p, fsid.major);
	ceph_decode_64(&p, fsid.minor);
	if (!ceph_fsid_equal(&fsid, &mdsc->client->monc.monmap->fsid)) {
		derr(0, "got mdsmap with wrong fsid\n");
		return;
	}
	ceph_decode_32(&p, epoch);
	ceph_decode_32(&p, maplen);
	dout(2, "handle_map epoch %u len %d\n", epoch, (int)maplen);

	/* do we need it? */
	spin_lock(&mdsc->lock);
	if (mdsc->mdsmap && epoch <= mdsc->mdsmap->m_epoch) {
		dout(2, "ceph_mdsc_handle_map epoch %u < our %u\n",
		     epoch, mdsc->mdsmap->m_epoch);
		spin_unlock(&mdsc->lock);
		return;
	}
	spin_unlock(&mdsc->lock);

	/* decode */
	newmap = ceph_mdsmap_decode(&p, end);
	if (IS_ERR(newmap))
		goto bad2;

	/* swap into place */
	spin_lock(&mdsc->lock);
	if (mdsc->mdsmap) {
		if (mdsc->mdsmap->m_epoch < newmap->m_epoch) {
			dout(2, "got new mdsmap %u\n", newmap->m_epoch);
			oldmap = mdsc->mdsmap;
			mdsc->mdsmap = newmap;
			check_new_map(mdsc, newmap, oldmap);
			ceph_mdsmap_destroy(oldmap);

			/* reconnect? */
			if (from < newmap->m_max_mds) {
				newstate = ceph_mdsmap_get_state(newmap, from);
				if (newstate == CEPH_MDS_STATE_RECONNECT)
					send_mds_reconnect(mdsc, from);
			}
		} else {
			dout(2, "ceph_mdsc_handle_map lost decode race?\n");
			ceph_mdsmap_destroy(newmap);
			spin_unlock(&mdsc->lock);
			return;
		}
	} else {
		dout(2, "got first mdsmap %u\n", newmap->m_epoch);
		mdsc->mdsmap = newmap;
	}

	/* stop asking */
	ceph_monc_got_mdsmap(&mdsc->client->monc, newmap->m_epoch);

	spin_unlock(&mdsc->lock);

	/* (re)schedule work */
	schedule_delayed(mdsc);

	complete(&mdsc->map_waiters);
	return;

bad:
	derr(1, "corrupt mds map\n");
	return;
bad2:
	derr(1, "no memory to decode new mdsmap\n");
	return;
}



/* eof */
