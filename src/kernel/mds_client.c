
#include <linux/wait.h>
#include <linux/sched.h>
#include "mds_client.h"
#include "mon_client.h"

#include "ceph_debug.h"

int ceph_debug_mdsc = -1;
#define DOUT_VAR ceph_debug_mdsc
#define DOUT_MASK DOUT_MASK_MDSC
#include "super.h"
#include "messenger.h"
#include "decode.h"


/*
 * address and send message to a given mds
 */
void ceph_send_msg_mds(struct ceph_mds_client *mdsc, struct ceph_msg *msg,
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

/*
 * parse individual inode info
 */
static int parse_reply_info_in(void **p, void *end,
			       struct ceph_mds_reply_info_in *info)
{
	int err = -EINVAL;

	info->in = *p;
	*p += sizeof(struct ceph_mds_reply_inode) +
		sizeof(*info->in->fragtree.splits) *
		le32_to_cpu(info->in->fragtree.nsplits);

	ceph_decode_32_safe(p, end, info->symlink_len, bad);
	ceph_decode_need(p, end, info->symlink_len, bad);
	info->symlink = *p;
	*p += info->symlink_len;

	ceph_decode_32_safe(p, end, info->xattr_len, bad);
	ceph_decode_need(p, end, info->xattr_len, bad);
	info->xattr_data = *p;
	*p += info->xattr_len;
	return 0;
bad:
	return err;
}

/*
 * parse a full metadata trace from the mds: inode, dirinfo, dentry, inode...
 * sequence.
 */
static int parse_reply_info_trace(void **p, void *end,
				  struct ceph_mds_reply_info_parsed *info)
{
	u16 numi, numd, snapdirpos;
	int err;

	ceph_decode_need(p, end, 3*sizeof(u16), bad);
	ceph_decode_16(p, numi);
	ceph_decode_16(p, numd);
	ceph_decode_16(p, snapdirpos);
	info->trace_numi = numi;
	info->trace_numd = numd;
	info->trace_snapdirpos = snapdirpos;
	if (numi == 0) {
		info->trace_in = NULL;
		goto done;   /* hrm, this shouldn't actually happen, but.. */
	}

	/* alloc one big block of memory for all of these arrays */
	info->trace_in = kmalloc(numi * (sizeof(*info->trace_in) +
					 sizeof(*info->trace_dir) +
					 sizeof(*info->trace_dname) +
					 sizeof(*info->trace_dname_len) +
					 sizeof(*info->trace_dlease)),
				 GFP_NOFS);
	if (info->trace_in == NULL) {
		err = -ENOMEM;
		goto out_bad;
	}
	info->trace_dir = (void *)(info->trace_in + numi);
	info->trace_dname = (void *)(info->trace_dir + numd);
	info->trace_dname_len = (void *)(info->trace_dname + numd);
	info->trace_dlease = (void *)(info->trace_dname_len + numd);

	/*
	 * the trace starts at the deepest point, and works up toward
	 * the root inode.
	 */
	if (numi == numd)
		goto dentry;
inode:
	if (!numi)
		goto done;
	numi--;
	err = parse_reply_info_in(p, end, &info->trace_in[numi]);
	if (err < 0)
		goto out_bad;

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

	/* dir frag info */
	if (unlikely(*p + sizeof(struct ceph_mds_reply_dirfrag) > end))
		goto bad;
	info->trace_dir[numd] = *p;
	*p += sizeof(struct ceph_mds_reply_dirfrag) +
		sizeof(u32)*le32_to_cpu(info->trace_dir[numd]->ndist);
	if (unlikely(*p > end))
		goto bad;
	goto inode;

done:
	if (unlikely(*p != end))
		goto bad;
	return 0;

bad:
	err = -EINVAL;
out_bad:
	derr(1, "problem parsing trace %d\n", err);
	return err;
}

/*
 * parse readdir results
 */
static int parse_reply_info_dir(void **p, void *end,
				struct ceph_mds_reply_info_parsed *info)
{
	u32 num, i = 0;
	int err;

	info->dir_dir = *p;
	if (*p + sizeof(*info->dir_dir) > end)
		goto bad;
	*p += sizeof(*info->dir_dir) +
		sizeof(u32)*le32_to_cpu(info->dir_dir->ndist);
	if (*p > end)
		goto bad;

	ceph_decode_32_safe(p, end, num, bad);
	if (num == 0)
		goto done;

	/* alloc large array */
	info->dir_nr = num;
	info->dir_in = kmalloc(num * (sizeof(*info->dir_in) +
				      sizeof(*info->dir_dname) +
				      sizeof(*info->dir_dname_len) +
				      sizeof(*info->dir_dlease)),
			       GFP_NOFS);
	if (info->dir_in == NULL) {
		err = -ENOMEM;
		goto out_bad;
	}
	info->dir_dname = (void *)(info->dir_in + num);
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
			goto out_bad;
		i++;
		num--;
	}

done:
	return 0;

bad:
	err = -EINVAL;
out_bad:
	derr(1, "problem parsing dir contents %d\n", err);
	return err;
}

/*
 * parse entire mds reply
 */
static int parse_reply_info(struct ceph_msg *msg,
			    struct ceph_mds_reply_info_parsed *info)
{
	void *p, *end;
	u32 len;
	int err;

	info->head = msg->front.iov_base;
	p = msg->front.iov_base + sizeof(struct ceph_mds_reply_head);
	end = p + msg->front.iov_len - sizeof(struct ceph_mds_reply_head);

	/* trace */
	ceph_decode_32_safe(&p, end, len, bad);
	if (len > 0) {
		err = parse_reply_info_trace(&p, p+len, info);
		if (err < 0)
			goto out_bad;
	}

	/* dir content */
	ceph_decode_32_safe(&p, end, len, bad);
	if (len > 0) {
		err = parse_reply_info_dir(&p, p+len, info);
		if (err < 0)
			goto out_bad;
	}

	/* snap blob */
	ceph_decode_32_safe(&p, end, len, bad);
	info->snapblob_len = len;
	info->snapblob = p;
	p += len;

	if (p != end)
		goto bad;
	return 0;

bad:
	err = -EINVAL;
out_bad:
	derr(1, "parse_reply err %d\n", err);
	return err;
}

static void destroy_reply_info(struct ceph_mds_reply_info_parsed *info)
{
	kfree(info->trace_in);
	kfree(info->dir_in);
}


/*
 * sessions
 */
static const char *session_state_name(int s)
{
	switch (s) {
	case CEPH_MDS_SESSION_NEW: return "new";
	case CEPH_MDS_SESSION_OPENING: return "opening";
	case CEPH_MDS_SESSION_OPEN: return "open";
	case CEPH_MDS_SESSION_FLUSHING: return "flushing";
	case CEPH_MDS_SESSION_CLOSING: return "closing";
	case CEPH_MDS_SESSION_RECONNECTING: return "reconnecting";
	default: return "???";
	}
}

/*
 * called under mdsc->mutex
 */
struct ceph_mds_session *__ceph_lookup_mds_session(struct ceph_mds_client *mdsc,
						int mds)
{
	struct ceph_mds_session *session;

	if (mds >= mdsc->max_sessions || mdsc->sessions[mds] == NULL)
		return NULL;
	session = mdsc->sessions[mds];
	dout(30, "lookup_mds_session %p %d -> %d\n", session,
	     atomic_read(&session->s_ref), atomic_read(&session->s_ref)+1);
	atomic_inc(&session->s_ref);
	return session;
}

void ceph_put_mds_session(struct ceph_mds_session *s)
{
	dout(30, "put_mds_session %p %d -> %d\n", s,
	     atomic_read(&s->s_ref), atomic_read(&s->s_ref)-1);
	if (atomic_dec_and_test(&s->s_ref))
		kfree(s);
}

/*
 * create+register a new session for given mds.
 * called under mdsc->mutex.
 */
static struct ceph_mds_session *register_session(struct ceph_mds_client *mdsc,
						 int mds)
{
	struct ceph_mds_session *s;

	s = kmalloc(sizeof(*s), GFP_NOFS);
	s->s_mds = mds;
	s->s_state = CEPH_MDS_SESSION_NEW;
	s->s_ttl = 0;
	s->s_seq = 0;
	mutex_init(&s->s_mutex);
	spin_lock_init(&s->s_cap_lock);
	s->s_cap_gen = 0;
	s->s_cap_ttl = 0;
	s->s_renew_requested = 0;
	INIT_LIST_HEAD(&s->s_caps);
	INIT_LIST_HEAD(&s->s_rdcaps);
	s->s_nr_caps = 0;
	atomic_set(&s->s_ref, 1);
	init_completion(&s->s_completion);
	INIT_LIST_HEAD(&s->s_unsafe);

	dout(10, "register_session mds%d\n", mds);
	if (mds >= mdsc->max_sessions) {
		int newmax = 1 << get_count_order(mds+1);
		struct ceph_mds_session **sa;

		dout(50, "register_session realloc to %d\n", newmax);
		sa = kzalloc(newmax * sizeof(void *), GFP_NOFS);
		if (sa == NULL)
			return ERR_PTR(-ENOMEM);
		if (mdsc->sessions) {
			memcpy(sa, mdsc->sessions,
			       mdsc->max_sessions * sizeof(void *));
			kfree(mdsc->sessions);
		}
		mdsc->sessions = sa;
		mdsc->max_sessions = newmax;
	}
	mdsc->sessions[mds] = s;
	atomic_inc(&s->s_ref);  /* one ref to sessions[], one to caller */
	return s;
}

/*
 * called under mdsc->mutex
 */
static void unregister_session(struct ceph_mds_client *mdsc, int mds)
{
	dout(10, "unregister_session mds%d %p\n", mds, mdsc->sessions[mds]);
	ceph_put_mds_session(mdsc->sessions[mds]);
	mdsc->sessions[mds] = NULL;
}

/* drop session refs in request */
static void put_request_sessions(struct ceph_mds_request *req)
{
	if (req->r_session) {
		ceph_put_mds_session(req->r_session);
		req->r_session = NULL;
	}
	if (req->r_fwd_session) {
		ceph_put_mds_session(req->r_fwd_session);
		req->r_fwd_session = NULL;
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
		if (req->r_direct_dentry)
			dput(req->r_direct_dentry);
		if (req->r_last_inode)
			iput(req->r_last_inode);
		if (req->r_last_dentry)
			dput(req->r_last_dentry);
		if (req->r_old_dentry)
			dput(req->r_old_dentry);
		kfree(req->r_expected_cap);
		put_request_sessions(req);
		kfree(req);
	}
}

/*
 * lookup session, bump ref if found.
 *
 * called under mdsc->mutex.
 */
static struct ceph_mds_request *__lookup_request(struct ceph_mds_client *mdsc,
					     u64 tid)
{
	struct ceph_mds_request *req;
	req = radix_tree_lookup(&mdsc->request_tree, tid);
	if (req)
		ceph_mdsc_get_request(req);
	return req;
}

/*
 * allocate and initialize a new request.  mostly zeroed.
 */
static struct ceph_mds_request *new_request(struct ceph_msg *msg)
{
	struct ceph_mds_request *req;

	req = kzalloc(sizeof(*req), GFP_NOFS);
	req->r_request = msg;
	req->r_started = jiffies;
	req->r_resend_mds = -1;
	req->r_fmode = -1;
	atomic_set(&req->r_ref, 1);  /* one for request_tree, one for caller */
	init_completion(&req->r_completion);
	init_completion(&req->r_safe_completion);
	return req;
}

/*
 * Register an in-flight request, and assign a tid in msg request header.
 *
 * Called under mdsc->mutex.
 */
static void __register_request(struct ceph_mds_client *mdsc,
			       struct inode *listener,
			       struct ceph_mds_request *req)
{
	struct ceph_mds_request_head *head = req->r_request->front.iov_base;
	struct ceph_inode_info *ci;
	req->r_tid = ++mdsc->last_tid;
	head->tid = cpu_to_le64(req->r_tid);
	dout(30, "__register_request %p tid %lld\n", req, req->r_tid);
	ceph_mdsc_get_request(req);
	radix_tree_insert(&mdsc->request_tree, req->r_tid, (void *)req);
	req->r_listener = listener;
	if (listener) {
		ci = ceph_inode(listener);
		spin_lock(&ci->i_listener_lock);
		list_add_tail(&req->r_listener_item, &ci->i_listener_list);
		spin_unlock(&ci->i_listener_lock);
	}
}

static void __unregister_request(struct ceph_mds_client *mdsc,
				 struct ceph_mds_request *req)
{
	struct ceph_inode_info *ci;
	dout(30, "__unregister_request %p tid %lld\n", req, req->r_tid);
	radix_tree_delete(&mdsc->request_tree, req->r_tid);
	if (req->r_listener) {
		ci = ceph_inode(req->r_listener);
		spin_lock(&ci->i_listener_lock);
		list_del(&req->r_listener_item);
		spin_unlock(&ci->i_listener_lock);
	}
	ceph_mdsc_put_request(req);
}

static bool __have_session(struct ceph_mds_client *mdsc, int mds)
{
	if (mds >= mdsc->max_sessions)
		return false;
	return mdsc->sessions[mds];
}

/*
 * Choose mds to send request to next.  If there is a hint set in
 * the request (e.g., due to a prior forward hint from the mds), use
 * that.
 *
 * Called under mdsc->mutex.
 */
static int __choose_mds(struct ceph_mds_client *mdsc,
			struct ceph_mds_request *req)
{
	int mds = -1;
	u32 hash = req->r_direct_hash;
	bool is_hash = req->r_direct_is_hash;
	struct dentry *dentry = req->r_direct_dentry;
	struct ceph_inode_info *ci;
	int mode = req->r_direct_mode;

	/*
	 * is there a specific mds we should try?  ignore hint if we have
	 * no session and the mds is not up (active or recovering).
	 */
	if (req->r_resend_mds >= 0 &&
	    (__have_session(mdsc, req->r_resend_mds) ||
	     ceph_mdsmap_get_state(mdsc->mdsmap, req->r_resend_mds) > 0)) {
		dout(20, "choose_mds using resend_mds mds%d\n",
		     req->r_resend_mds);
		return req->r_resend_mds;
	}

	if (mode == USE_CAP_MDS) {
		mds = ceph_get_cap_mds(dentry->d_inode);
		if (mds >= 0) {
			dout(20, "choose_mds %p %llx.%llx mds%d (cap)\n",
			     dentry->d_inode, ceph_vinop(dentry->d_inode), mds);
			return mds;
		}
		derr(0, "choose_mds %p %llx.%llx has NO CAPS, using auth\n",
		     dentry->d_inode, ceph_vinop(dentry->d_inode));
		WARN_ON(1);
		mode = USE_AUTH_MDS;
	}

	if (mode == USE_RANDOM_MDS)
		goto random;

	/*
	 * try to find an appropriate mds to contact based on the
	 * given dentry.  walk up the tree until we find delegation info
	 * in the i_fragtree.
	 *
	 * if is_hash is true, direct request at the appropriate directory
	 * fragment (as with a readdir on a fragmented directory).
	 */
	while (dentry) {
		if (is_hash && dentry->d_inode &&
		    S_ISDIR(dentry->d_inode->i_mode)) {
			struct ceph_inode_frag frag;
			int found;

			ci = ceph_inode(dentry->d_inode);
			ceph_choose_frag(ci, hash, &frag, &found);
			if (found) {
				if (mode == USE_ANY_MDS && frag.ndist > 0) {
					u8 r;

					/* choose a random replica */
					get_random_bytes(&r, 1);
					r %= frag.ndist;
					mds = frag.dist[r];
					dout(20, "choose_mds %p %llx.%llx "
					     "frag %u mds%d (%d/%d)\n",
					     dentry->d_inode,
					     ceph_vinop(&ci->vfs_inode),
					     frag.frag, frag.mds,
					     (int)r, frag.ndist);
					return mds;
				}
				/* since the more deeply nested item wasn't
				 * known to be replicated, then we want to
				 * look for the authoritative mds. */
				mode = USE_AUTH_MDS;
				if (frag.mds >= 0) {
					/* choose auth mds */
					mds = frag.mds;
					dout(20, "choose_mds %p %llx.%llx "
					     "frag %u mds%d (auth)\n",
					     dentry->d_inode,
					     ceph_vinop(&ci->vfs_inode),
					     frag.frag, mds);
					return mds;
				}
			}
		}
		if (IS_ROOT(dentry))
			break;

		/* move up the hierarchy, but direct request based on the hash
		 * for the child's dentry name */
		hash = dentry->d_name.hash;
		is_hash = true;
		dentry = dentry->d_parent;
	}

	/* ok, just pick one at random */
random:
	mds = ceph_mdsmap_get_random_mds(mdsc->mdsmap);
	dout(20, "choose_mds chose random mds%d\n", mds);
	return mds;
}


/*
 * session messages
 */
static struct ceph_msg *create_session_msg(u32 op, u64 seq)
{
	struct ceph_msg *msg;
	struct ceph_mds_session_head *h;

	msg = ceph_msg_new(CEPH_MSG_CLIENT_SESSION, sizeof(*h), 0, 0, NULL);
	if (IS_ERR(msg)) {
		derr("ENOMEM creating session msg\n");
		return ERR_PTR(PTR_ERR(msg));
	}
	h = msg->front.iov_base;
	h->op = cpu_to_le32(op);
	h->seq = cpu_to_le64(seq);
	return msg;
}

/*
 * Register request with mon_client for a new mds map.  Wait until
 * we get one (or time out).
 *
 * called under mdsc->mutex (dropped while we wait)
 */
static int wait_for_new_map(struct ceph_mds_client *mdsc,
			     unsigned long timeout)
{
	u32 have;
	int err = 0;

	dout(30, "wait_for_new_map enter\n");
	have = mdsc->mdsmap->m_epoch;
	mutex_unlock(&mdsc->mutex);
	ceph_monc_request_mdsmap(&mdsc->client->monc, have+1);
	if (timeout) {
		err = wait_for_completion_timeout(&mdsc->map_waiters, timeout);
		if (err > 0)
			err = 0;
		else if (err == 0)
			err = -EIO;
	} else {
		wait_for_completion(&mdsc->map_waiters);
	}
	mutex_lock(&mdsc->mutex);
	dout(30, "wait_for_new_map err %d\n", err);
	return err;
}

/*
 * open a new session with the given mds, and wait for mds ack.  the
 * timeout is optional.
 *
 * called under mdsc->mutex
 */
static int open_session(struct ceph_mds_client *mdsc,
			struct ceph_mds_session *session, unsigned long timeout)
{
	struct ceph_msg *msg;
	int mstate;
	int mds = session->s_mds;
	int err = 0;

	/* wait for mds to go active? */
	mstate = ceph_mdsmap_get_state(mdsc->mdsmap, mds);
	dout(10, "open_session to mds%d, state %d\n", mds, mstate);
	if (mstate < CEPH_MDS_STATE_ACTIVE) {
		err = wait_for_new_map(mdsc, timeout);
		if (err)
			return err;
		mstate = ceph_mdsmap_get_state(mdsc->mdsmap, mds);
		if (mstate < CEPH_MDS_STATE_ACTIVE) {
			dout(30, "open_session mds%d now %d still not active\n",
			     mds, mstate);
			return -EAGAIN;  /* hrm, try again? */
		}
	}

	session->s_state = CEPH_MDS_SESSION_OPENING;
	session->s_renew_requested = jiffies;
	mutex_unlock(&mdsc->mutex);

	/* send connect message */
	msg = create_session_msg(CEPH_SESSION_REQUEST_OPEN, session->s_seq);
	if (IS_ERR(msg))
		return PTR_ERR(msg);
	ceph_send_msg_mds(mdsc, msg, mds);

	/* wait for session to open (or fail, or close) */
	dout(30, "open_session waiting on session %p\n", session);
	if (timeout) {
		err = wait_for_completion_timeout(&session->s_completion,
						  timeout);
		if (err > 0)
			err = 0;
		else if (err == 0)
			err = -EIO;
	} else {
		wait_for_completion(&session->s_completion);
	}
	dout(30, "open_session done waiting on session %p, state %d\n",
	     session, session->s_state);

	mutex_lock(&mdsc->mutex);
	return err;
}

/*
 * caller must hold session s_mutex
 */
static void remove_session_caps(struct ceph_mds_session *session)
{
	struct ceph_cap *cap;
	struct ceph_inode_info *ci;

	dout(10, "remove_session_caps on %p\n", session);
	while (session->s_nr_caps > 0) {
		cap = list_entry(session->s_caps.next, struct ceph_cap,
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

	spin_lock(&dentry->d_lock);
	di = ceph_dentry(dentry);
	if (di) {
		ceph_put_mds_session(di->lease_session);
		kfree(di);
		dentry->d_fsdata = NULL;
	}
	spin_unlock(&dentry->d_lock);
}

/*
 * wake up any threads waiting on this session's caps
 *
 * caller must hold s_mutex.
 */
static void wake_up_session_caps(struct ceph_mds_session *session)
{
	struct list_head *p;
	struct ceph_cap *cap;

	dout(10, "wake_up_session_caps %p mds%d\n", session, session->s_mds);
	list_for_each(p, &session->s_caps) {
		cap = list_entry(p, struct ceph_cap, session_caps);
		wake_up(&cap->ci->i_cap_wq);
	}
}

/*
 * Wake up threads with requests pending for @mds, so that they can
 * resubmit their requests to a possibly different mds.  If @all is set,
 * wake up if their requests has been forwarded to @mds, too.
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
			if (!reqs[i]->r_got_unsafe &&
			    ((reqs[i]->r_session &&
			      reqs[i]->r_session->s_mds == mds) ||
			     (all && reqs[i]->r_fwd_session &&
			      reqs[i]->r_fwd_session->s_mds == mds))) {
				dout(10, " kicking tid %llu\n", reqs[i]->r_tid);
				put_request_sessions(reqs[i]);
				complete(&reqs[i]->r_completion);
			}
		}
	}
}

/*
 * Send periodic message to MDS renewing all currently held caps.  The
 * ack will reset the expiration for all caps from this session.
 *
 * caller holds s_mutex
 */
static int send_renew_caps(struct ceph_mds_client *mdsc,
			   struct ceph_mds_session *session)
{
	struct ceph_msg *msg;

	if (time_after_eq(jiffies, session->s_cap_ttl) &&
	    time_after_eq(session->s_cap_ttl, session->s_renew_requested))
		dout(1, "mds%d session caps stale\n", session->s_mds);

	/* do not try to renew caps until a recovering mds has reconnected
	 * with its clients. */
	if (ceph_mdsmap_get_state(mdsc->mdsmap, session->s_mds) <
	    CEPH_MDS_STATE_RECONNECT) {
		dout(10, "send_renew_caps ignoring mds%d\n", session->s_mds);
		return 0;
	}

	dout(10, "send_renew_caps to mds%d\n", session->s_mds);
	session->s_renew_requested = jiffies;
	msg = create_session_msg(CEPH_SESSION_REQUEST_RENEWCAPS, 0);
	if (IS_ERR(msg))
		return PTR_ERR(msg);
	ceph_send_msg_mds(mdsc, msg, session->s_mds);
	return 0;
}

/*
 * Note new cap ttl, and any transition from stale -> not stale (fresh?).
 */
static void renewed_caps(struct ceph_mds_client *mdsc,
		  struct ceph_mds_session *session, int is_renew)
{
	int was_stale;
	int wake = 0;

	spin_lock(&session->s_cap_lock);
	was_stale = is_renew && (session->s_cap_ttl == 0 ||
				 time_after_eq(jiffies, session->s_cap_ttl));

	session->s_cap_ttl = session->s_renew_requested +
		mdsc->mdsmap->m_session_timeout*HZ;

	if (was_stale) {
		if (time_before(jiffies, session->s_cap_ttl)) {
			dout(1, "mds%d caps renewed\n", session->s_mds);
			wake = 1;
		} else {
			dout(1, "mds%d caps still stale\n", session->s_mds);
		}
	}
	dout(10, "renewed_caps mds%d ttl now %lu, was %s, now %s\n",
	     session->s_mds, session->s_cap_ttl, was_stale ? "stale" : "fresh",
	     time_before(jiffies, session->s_cap_ttl) ? "stale" : "fresh");
	spin_unlock(&session->s_cap_lock);

	if (wake)
		wake_up_session_caps(session);
}



static int request_close_session(struct ceph_mds_client *mdsc,
				 struct ceph_mds_session *session)
{
	struct ceph_msg *msg;
	int err = 0;

	msg = create_session_msg(CEPH_SESSION_REQUEST_CLOSE,
				 session->s_seq);
	if (IS_ERR(msg))
		err = PTR_ERR(msg);
	else
		ceph_send_msg_mds(mdsc, msg, session->s_mds);
	return err;
}

/*
 * check all caps on a session, without allowing release to
 * be delayed.
 */
static void check_all_caps(struct ceph_mds_client *mdsc,
			 struct ceph_mds_session *session)
{
	struct list_head *p, *n;

	list_for_each_safe(p, n, &session->s_caps) {
		struct ceph_cap *cap =
			list_entry(p, struct ceph_cap, session_caps);
		struct inode *inode = &cap->ci->vfs_inode;

		igrab(inode);
		mutex_unlock(&session->s_mutex);
		ceph_check_caps(ceph_inode(inode), 1, 0);
		mutex_lock(&session->s_mutex);
		iput(inode);
	}
}

/*
 * Called with s_mutex held.
 */
static int __close_session(struct ceph_mds_client *mdsc,
			 struct ceph_mds_session *session)
{
	int mds = session->s_mds;
	int err = 0;

	dout(10, "close_session mds%d state=%s\n", mds,
	     session_state_name(session->s_state));
	if (session->s_state >= CEPH_MDS_SESSION_CLOSING)
		return 0;

	check_all_caps(mdsc, session);

	if (list_empty(&session->s_caps)) {
		session->s_state = CEPH_MDS_SESSION_CLOSING;
		err = request_close_session(mdsc, session);
	} else {
		session->s_state = CEPH_MDS_SESSION_FLUSHING;
	}
	return err;
}

/*
 * Called when the last cap for a session has been flushed or
 * exported.
 */
void ceph_mdsc_flushed_all_caps(struct ceph_mds_client *mdsc,
				struct ceph_mds_session *session)
{
	dout(10, "flushed_all_caps for mds%d state %s\n", session->s_mds,
	     session_state_name(session->s_state));
	if (session->s_state == CEPH_MDS_SESSION_FLUSHING) {
		session->s_state = CEPH_MDS_SESSION_CLOSING;
		request_close_session(mdsc, session);
	}
}


/*
 * handle a mds session control message
 */
void ceph_mdsc_handle_session(struct ceph_mds_client *mdsc,
			      struct ceph_msg *msg)
{
	u32 op;
	u64 seq;
	struct ceph_mds_session *session = NULL;
	int mds;
	struct ceph_mds_session_head *h = msg->front.iov_base;

	if (le32_to_cpu(msg->hdr.src.name.type) != CEPH_ENTITY_TYPE_MDS)
		return;
	mds = le32_to_cpu(msg->hdr.src.name.num);

	/* decode */
	if (msg->front.iov_len != sizeof(*h))
		goto bad;
	op = le32_to_cpu(h->op);
	seq = le64_to_cpu(h->seq);

	mutex_lock(&mdsc->mutex);
	session = __ceph_lookup_mds_session(mdsc, mds);
	if (session && mdsc->mdsmap)
		/* FIXME: this ttl calculation is generous */
		session->s_ttl = jiffies + HZ*mdsc->mdsmap->m_session_autoclose;
	mutex_unlock(&mdsc->mutex);

	if (!session) {
		if (op != CEPH_SESSION_OPEN) {
			dout(10, "handle_session no session for mds%d\n", mds);
			return;
		}
		dout(10, "handle_session creating session for mds%d\n", mds);
		session = register_session(mdsc, mds);
	}

	mutex_lock(&session->s_mutex);

	dout(2, "handle_session mds%d %s %p state %s seq %llu\n",
	     mds, ceph_session_op_name(op), session,
	     session_state_name(session->s_state), seq);
	switch (op) {
	case CEPH_SESSION_OPEN:
		session->s_state = CEPH_MDS_SESSION_OPEN;
		renewed_caps(mdsc, session, 0);
		complete(&session->s_completion);
		if (mdsc->stopping)
			__close_session(mdsc, session);
		break;

	case CEPH_SESSION_RENEWCAPS:
		renewed_caps(mdsc, session, 1);
		break;

	case CEPH_SESSION_CLOSE:
		unregister_session(mdsc, mds);
		remove_session_caps(session);
		complete(&session->s_completion); /* for good measure */
		complete(&mdsc->session_close_waiters);
		kick_requests(mdsc, mds, 0);      /* cur only */
		break;

	case CEPH_SESSION_STALE:
		dout(1, "mds%d caps went stale, renewing\n", session->s_mds);
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

	mutex_unlock(&session->s_mutex);
	ceph_put_mds_session(session);
	return;

bad:
	derr(1, "corrupt mds%d session message, len %d, expected %d\n", mds,
	     (int)msg->front.iov_len, (int)sizeof(*h));
	return;
}


/*
 * create an mds request and message.
 *
 * slight hacky weirdness: if op is a FINDINODE, ino1 is the _length_
 * of path1, and path1 isn't null terminated (it's an nfs filehandle
 * fragment).  path2 is not used in that case.
 */
struct ceph_mds_request *
ceph_mdsc_create_request(struct ceph_mds_client *mdsc, int op,
			 u64 ino1, const char *path1,
			 u64 ino2, const char *path2,
			 struct dentry *ref, int mode)
{
	struct ceph_msg *msg;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *head;
	void *p, *end;
	int pathlen;

	if (op == CEPH_MDS_OP_FINDINODE) {
		pathlen = sizeof(u32) + ino1*sizeof(struct ceph_inopath_item);
	} else {
		pathlen = 2*(sizeof(ino1) + sizeof(u32));
		if (path1)
			pathlen += strlen(path1);
		if (path2)
			pathlen += strlen(path2);
	}

	msg = ceph_msg_new(CEPH_MSG_CLIENT_REQUEST,
			   sizeof(struct ceph_mds_request_head) + pathlen,
			   0, 0, NULL);
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

	/* dentry used to direct mds request? */
	req->r_direct_dentry = dget(ref);
	req->r_direct_mode = mode;

	/* tid, oldest_client_tid, retry_attempt set later. */
	head->mdsmap_epoch = cpu_to_le32(mdsc->mdsmap->m_epoch);
	head->num_fwd = 0;
	head->mds_wants_replica_in_dirino = 0;
	head->op = cpu_to_le32(op);
	head->caller_uid = cpu_to_le32(current->fsuid);
	head->caller_gid = cpu_to_le32(current->fsgid);
	memset(&head->args, 0, sizeof(head->args));

	/* encode paths */
	if (op == CEPH_MDS_OP_FINDINODE) {
		ceph_encode_32(&p, ino1);
		memcpy(p, path1, ino1 * sizeof(struct ceph_inopath_item));
		p += ino1 * sizeof(struct ceph_inopath_item);
	} else {
		ceph_encode_filepath(&p, end, ino1, path1);
		ceph_encode_filepath(&p, end, ino2, path2);
		if (path1)
			dout(10, "create_request path1 %llx/%s\n",
			     ino1, path1);
		if (path2)
			dout(10, "create_request path2 %llx/%s\n",
			     ino2, path2);
	}
	dout_flag(10, DOUT_MASK_PROTOCOL, "create_request op %d=%s -> %p\n", op,
	     ceph_mds_op_name(op), req);

	BUG_ON(p != end);
	return req;
}

/*
 * return oldest (lowest) tid in request tree, 0 if none.
 *
 * called under mdsc->mutex.
 */
static u64 __get_oldest_tid(struct ceph_mds_client *mdsc)
{
	struct ceph_mds_request *first;
	if (radix_tree_gang_lookup(&mdsc->request_tree,
				   (void **)&first, 0, 1) <= 0)
		return 0;
	return first->r_tid;
}

/*
 * called under mdsc->mutex
 */
static void __prepare_send_request(struct ceph_mds_client *mdsc,
				   struct ceph_mds_request *req)
{
	struct ceph_mds_request_head *rhead;

	/* if there are other references on this message, e.g., if we are
	 * told to forward it and the previous copy is still in flight, dup
	 * it. */
	req->r_request = ceph_msg_maybe_dup(req->r_request);

	req->r_attempts++;

	rhead = req->r_request->front.iov_base;
	rhead->retry_attempt = cpu_to_le32(req->r_attempts - 1);
	rhead->oldest_client_tid = cpu_to_le64(__get_oldest_tid(mdsc));
	rhead->num_fwd = cpu_to_le32(req->r_num_fwd);

	if (req->r_last_inode)
		rhead->ino = cpu_to_le64(ceph_ino(req->r_last_inode));
	else
		rhead->ino = 0;
}

/*
 * Synchrously perform an mds request.  Take care of all of the
 * session setup, forwarding, retry details.
 */
int ceph_mdsc_do_request(struct ceph_mds_client *mdsc,
			 struct inode *listener,
			 struct ceph_mds_request *req)
{
	struct ceph_mds_session *session = NULL;
	int err;
	int mds = -1;
	int safe = 0;

	dout(30, "do_request on %p\n", req);

	mutex_lock(&mdsc->mutex);
	__register_request(mdsc, listener, req);
retry:
	if (req->r_timeout &&
	    time_after_eq(jiffies, req->r_started + req->r_timeout)) {
		if (session && session->s_state == CEPH_MDS_SESSION_OPENING)
			unregister_session(mdsc, mds);
		dout(10, "do_request timed out\n");
		err = -EIO;
		goto finish;
	}

	mds = __choose_mds(mdsc, req);
	if (mds < 0 ||
	    ceph_mdsmap_get_state(mdsc->mdsmap, mds) < CEPH_MDS_STATE_ACTIVE) {
		dout(30, "do_request no mds or not active, waiting for map\n");
		err = wait_for_new_map(mdsc, req->r_timeout);
		if (err)
			goto finish;
		goto retry;
	}

	/* get session */
	session = __ceph_lookup_mds_session(mdsc, mds);
	if (!session)
		session = register_session(mdsc, mds);
	dout(30, "do_request mds%d session %p state %s\n", mds, session,
	     session_state_name(session->s_state));

	/* open? */
	err = 0;
	if (session->s_state == CEPH_MDS_SESSION_NEW ||
	    session->s_state == CEPH_MDS_SESSION_CLOSING)
		err = open_session(mdsc, session, req->r_timeout);
	if (session->s_state != CEPH_MDS_SESSION_OPEN ||
	    err == -EAGAIN) {
		dout(30, "do_request session %p not open, state=%s\n",
		     session, session_state_name(session->s_state));
		ceph_put_mds_session(session);
		goto retry;
	}

	BUG_ON(req->r_session);
	req->r_session = session; /* request now owns the session ref */
	req->r_resend_mds = -1;   /* forget any previous mds hint */

	if (req->r_request_started == 0)   /* note request start time */
		req->r_request_started = jiffies;

	__prepare_send_request(mdsc, req);
	mutex_unlock(&mdsc->mutex);

	ceph_msg_get(req->r_request);
	ceph_send_msg_mds(mdsc, req->r_request, mds);

	if (req->r_timeout) {
		err = wait_for_completion_timeout(&req->r_completion,
						  req->r_timeout);
		if (err > 0)
			err = 0;
		else if (err == 0)
			err = -EIO;  /* timed out */
	} else {
		err = 0;
		wait_for_completion(&req->r_completion);
	}
	mutex_lock(&mdsc->mutex);
	if (req->r_reply == NULL && !err) {
		put_request_sessions(req);
		goto retry;
	}
	if (IS_ERR(req->r_reply)) {
		err = PTR_ERR(req->r_reply);
		req->r_reply = NULL;
	}
	if (!err)
		/* all is well, reply has been parsed. */
		err = le32_to_cpu(req->r_reply_info.head->result);
	if (req)
		safe = req->r_reply_info.head->safe;
finish:
	if (safe) {
		complete(&req->r_safe_completion);
		__unregister_request(mdsc, req);
	}

	mutex_unlock(&mdsc->mutex);

	if (safe) {
		ceph_msg_put(req->r_request);
		req->r_request = NULL;
	}

	dout(30, "do_request %p done, result %d\n", req, err);
	return err;
}

/*
 * Handle mds reply.
 *
 * We take the session mutex and parse and process the reply immediately.
 * This preserves the logical ordering of replies, capabilities, etc., sent
 * by the MDS as they are applied to our local cache.
 */
void ceph_mdsc_handle_reply(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	struct ceph_mds_request *req;
	struct ceph_mds_reply_head *head = msg->front.iov_base;
	struct ceph_mds_reply_info_parsed *rinfo;  /* parsed reply info */
	u64 tid;
	int err, result;
	int mds;
	struct ceph_snap_realm *realm = NULL;

	if (le32_to_cpu(msg->hdr.src.name.type) != CEPH_ENTITY_TYPE_MDS)
		return;
	if (msg->front.iov_len < sizeof(*head)) {
		derr(1, "handle_reply got corrupt (short) reply\n");
		return;
	}

	/* get request, session */
	tid = le64_to_cpu(head->tid);
	mutex_lock(&mdsc->mutex);
	req = __lookup_request(mdsc, tid);
	if (!req) {
		dout(1, "handle_reply on unknown tid %llu\n", tid);
		mutex_unlock(&mdsc->mutex);
		return;
	}
	dout(10, "handle_reply %p expected_cap=%p\n", req, req->r_expected_cap);
	mds = le32_to_cpu(msg->hdr.src.name.num);

	/* dup? */
	if ((req->r_got_unsafe && !head->safe) ||
	    (req->r_got_safe && head->safe)) {
		dout(0, "got a dup %s reply on %llu from mds%d\n",
		     head->safe ? "safe":"unsafe", tid, mds);
		mutex_unlock(&mdsc->mutex);
		ceph_mdsc_put_request(req);
		return;
	}

	if (req->r_got_unsafe && head->safe) {
		/* 
		 * We already handled the unsafe response, now do the
		 * cleanup.  No need to examine the response; the MDS
		 * doesn't include any result info in the safe
		 * response.  And even if it did, there is nothing
		 * useful we could do with a revised return value.
		 */
		dout(10, "got safe reply %llu, mds%d\n", tid, mds);
		BUG_ON(req->r_session == NULL);
		complete(&req->r_safe_completion);
		__unregister_request(mdsc, req);
		req->r_got_safe = true;
		list_del_init(&req->r_unsafe_item);
		mutex_unlock(&mdsc->mutex);
		ceph_mdsc_put_request(req);
		return;
	}

	if (req->r_session && req->r_session->s_mds != mds) {
		ceph_put_mds_session(req->r_session);
		req->r_session = __ceph_lookup_mds_session(mdsc, mds);
	}
	if (req->r_session == NULL) {
		derr(1, "got reply on %llu, but no session for mds%d\n",
		     tid, mds);
		mutex_unlock(&mdsc->mutex);
		ceph_mdsc_put_request(req);
		return;
	}
	BUG_ON(req->r_reply);

	if (head->safe)
		req->r_got_safe = true;
	else {
		req->r_got_unsafe = true;
		list_add_tail(&req->r_unsafe_item, &req->r_session->s_unsafe);
	}

	/* take the snap sem -- we may be are adding a cap here */
	down_write(&mdsc->snap_rwsem);
	mutex_unlock(&mdsc->mutex);

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

	/* snap trace */
	if (rinfo->snapblob_len)
		realm = ceph_update_snap_trace(mdsc, rinfo->snapblob,
			       rinfo->snapblob + rinfo->snapblob_len,
			       le32_to_cpu(head->op) == CEPH_MDS_OP_RMSNAP);

	/* insert trace into our cache */
	err = ceph_fill_trace(mdsc->client->sb, req, req->r_session);
	if (err)
		goto done;
	if (result == 0) {
		/* readdir result? */
		if (rinfo->dir_nr)
			ceph_readdir_prepopulate(req, req->r_session);
	}


done:
	if (realm)
		ceph_put_snap_realm(mdsc, realm);
	up_write(&mdsc->snap_rwsem);

	if (err) {
		req->r_err = err;
	} else {
		req->r_reply = msg;
		ceph_msg_get(msg);
	}

	mutex_unlock(&req->r_session->s_mutex);

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
	u64 tid;
	u32 next_mds;
	u32 fwd_seq;
	u8 must_resend;
	int err = -EINVAL;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;
	int from_mds;

	if (le32_to_cpu(msg->hdr.src.name.type) != CEPH_ENTITY_TYPE_MDS)
		goto bad;
	from_mds = le32_to_cpu(msg->hdr.src.name.num);

	ceph_decode_need(&p, end, sizeof(u64)+2*sizeof(u32), bad);
	ceph_decode_64(&p, tid);
	ceph_decode_32(&p, next_mds);
	ceph_decode_32(&p, fwd_seq);
	ceph_decode_8(&p, must_resend);

	mutex_lock(&mdsc->mutex);
	req = __lookup_request(mdsc, tid);
	if (!req) {
		dout(10, "forward %llu dne\n", tid);
		goto out;  /* dup reply? */
	}

	if (fwd_seq <= req->r_num_fwd) {
		dout(10, "forward %llu to mds%d - old seq %d <= %d\n",
		     tid, next_mds, req->r_num_fwd, fwd_seq);
	} else if (!must_resend &&
		   __have_session(mdsc, next_mds) &&
		   mdsc->sessions[next_mds]->s_state == CEPH_MDS_SESSION_OPEN) {
		/* yes.  adjust our sessions, but that's all; the old mds
		 * forwarded our message for us. */
		dout(10, "forward %llu to mds%d (mds%d fwded)\n", tid, next_mds,
		     from_mds);
		req->r_num_fwd = fwd_seq;
		put_request_sessions(req);
		req->r_session = __ceph_lookup_mds_session(mdsc, next_mds);
		req->r_fwd_session = __ceph_lookup_mds_session(mdsc, from_mds);
	} else {
		/* no, resend. */
		/* forward race not possible; mds would drop */
		dout(10, "forward %llu to mds%d (we resend)\n", tid, next_mds);
		req->r_num_fwd = fwd_seq;
		req->r_resend_mds = next_mds;
		put_request_sessions(req);
		complete(&req->r_completion);  /* wake up do_request */
	}
	ceph_mdsc_put_request(req);
out:
	mutex_unlock(&mdsc->mutex);
	return;

bad:
	derr(0, "problem decoding message, err=%d\n", err);
}


/*
 * called under session->mutex.
 */
static void replay_unsafe_requests(struct ceph_mds_client *mdsc,
				   struct ceph_mds_session *session)
{
	struct list_head *p, *n;
	struct ceph_mds_request *req;

	dout(10, "replay_unsafe_requests mds%d\n", session->s_mds);

	mutex_lock(&mdsc->mutex);
	list_for_each_safe(p, n, &session->s_unsafe) {
		req = list_entry(p, struct ceph_mds_request, r_unsafe_item);
		__prepare_send_request(mdsc, req);
		ceph_msg_get(req->r_request);
		ceph_send_msg_mds(mdsc, req->r_request, session->s_mds);
	}
	mutex_unlock(&mdsc->mutex);
}

/*
 * If an MDS fails and recovers, it needs to reconnect with clients in order
 * to reestablish shared state.  This includes all caps issued through this
 * session _and_ the snap_realm hierarchy.  Because it's not clear which
 * snap realms the mds cares about, we send everything we know about.. that
 * ensures we'll then get any new info the recovering MDS might have.
 *
 * This is a relatively heavyweight operation, but it's rare.
 *
 * called with mdsc->mutex held.
 */
static void send_mds_reconnect(struct ceph_mds_client *mdsc, int mds)
{
	struct ceph_mds_session *session;
	struct ceph_msg *reply;
	int newlen, len = 4 + 1;
	void *p, *end;
	struct list_head *cp;
	struct ceph_cap *cap;
	char *path;
	int pathlen, err;
	u64 pathbase;
	struct dentry *dentry;
	struct ceph_inode_info *ci;
	int num_caps, num_realms = 0;
	int got;
	u64 next_snap_ino = 0;
	__le32 *pnum_caps, *pnum_realms;

	dout(1, "reconnect to recovering mds%d\n", mds);

	/* find session */
	session = __ceph_lookup_mds_session(mdsc, mds);
	down_read(&mdsc->snap_rwsem);
	mutex_unlock(&mdsc->mutex);    /* drop lock for duration */

	if (session) {
		mutex_lock(&session->s_mutex);

		session->s_state = CEPH_MDS_SESSION_RECONNECTING;
		session->s_seq = 0;

		/* replay unsafe requests */
		replay_unsafe_requests(mdsc, session);

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

retry:
	/* build reply */
	reply = ceph_msg_new(CEPH_MSG_CLIENT_RECONNECT, len, 0, 0, NULL);
	if (IS_ERR(reply)) {
		err = PTR_ERR(reply);
		derr(0, "ENOMEM trying to send mds reconnect to mds%d\n", mds);
		goto out;
	}
	p = reply->front.iov_base;
	end = p + len;

	if (!session) {
		ceph_encode_8(&p, 1); /* session was closed */
		ceph_encode_32(&p, 0);
		goto send;
	}
	dout(10, "session %p state %s\n", session,
	     session_state_name(session->s_state));

	/* traverse this session's caps */
	ceph_encode_8(&p, 0);
	pnum_caps = p;
	ceph_encode_32(&p, session->s_nr_caps);
	num_caps = 0;
	list_for_each(cp, &session->s_caps) {
		struct inode *inode;
		struct ceph_mds_cap_reconnect *rec;

		cap = list_entry(cp, struct ceph_cap, session_caps);
		ci = cap->ci;
		inode = &ci->vfs_inode;

		/* skip+drop expireable caps.  this is racy, but harmless. */
		if ((cap->issued & ~CEPH_CAP_EXPIREABLE) == 0) {
			dout(10, " skipping %p ino %llx.%llx cap %p %s\n",
			     inode, ceph_vinop(inode), cap,
			     ceph_cap_string(cap->issued));
			continue;
		}

		dout(10, " adding %p ino %llx.%llx cap %p %s\n",
		     inode, ceph_vinop(inode), cap,
		     ceph_cap_string(cap->issued));
		ceph_decode_need(&p, end, sizeof(u64), needmore);
		ceph_encode_64(&p, ceph_ino(inode));

		dentry = d_find_alias(inode);
		if (dentry) {
			path = ceph_build_path(dentry, &pathlen,
					       &pathbase, 9999);
			if (IS_ERR(path)) {
				err = PTR_ERR(path);
				BUG_ON(err);
			}
		} else {
			path = NULL;
			pathlen = 0;
		}
		ceph_decode_need(&p, end, pathlen+4, needmore);
		ceph_encode_string(&p, end, path, pathlen);

		ceph_decode_need(&p, end, sizeof(*rec), needmore);
		rec = p;
		p += sizeof(*rec);
		BUG_ON(p > end);
		spin_lock(&inode->i_lock);
		cap->seq = 0;  /* reset cap seq */
		rec->wanted = cpu_to_le32(__ceph_caps_wanted(ci));
		rec->issued = cpu_to_le32(cap->issued);
		rec->size = cpu_to_le64(inode->i_size);
		ceph_encode_timespec(&rec->mtime, &inode->i_mtime);
		ceph_encode_timespec(&rec->atime, &inode->i_atime);
		rec->snaprealm = cpu_to_le64(ci->i_snap_realm->ino);
		spin_unlock(&inode->i_lock);

		kfree(path);
		dput(dentry);
		num_caps++;
	}
	*pnum_caps = cpu_to_le32(num_caps);

	/*
	 * snaprealms.  we provide mds with the ino, seq (version), and
	 * parent for all of our realms.  If the mds has any newer info,
	 * it will tell us.
	 */
	next_snap_ino = 0;
	/* save some space for the snaprealm count */
	pnum_realms = p;
	ceph_decode_need(&p, end, sizeof(*pnum_realms), needmore);
	p += sizeof(*pnum_realms);
	num_realms = 0;
	while (1) {
		struct ceph_snap_realm *realm;
		struct ceph_mds_snaprealm_reconnect *sr_rec;
		got = radix_tree_gang_lookup(&mdsc->snap_realms,
					     (void **)&realm, next_snap_ino, 1);
		if (!got)
			break;

		dout(10, " adding snap realm %llx seq %lld parent %llx\n",
		     realm->ino, realm->seq, realm->parent_ino);
		ceph_decode_need(&p, end, sizeof(*sr_rec), needmore);
		sr_rec = p;
		sr_rec->ino = cpu_to_le64(realm->ino);
		sr_rec->seq = cpu_to_le64(realm->seq);
		sr_rec->parent = cpu_to_le64(realm->parent_ino);
		p += sizeof(*sr_rec);
		num_realms++;
		next_snap_ino = realm->ino + 1;
	}
	*pnum_realms = cpu_to_le32(num_realms);

send:
	reply->front.iov_len = p - reply->front.iov_base;
	reply->hdr.front_len = cpu_to_le32(reply->front.iov_len);
	dout(10, "final len was %u (guessed %d)\n",
	     (unsigned)reply->front.iov_len, len);
	ceph_send_msg_mds(mdsc, reply, mds);

	if (session) {
		session->s_state = CEPH_MDS_SESSION_OPEN;
		complete(&session->s_completion);
	}

out:
	if (session) {
		mutex_unlock(&session->s_mutex);
		ceph_put_mds_session(session);
	}
	up_read(&mdsc->snap_rwsem);
	mutex_lock(&mdsc->mutex);
	return;

needmore:
	/*
	 * we need a larger buffer.  this doesn't very accurately
	 * factor in snap realms, but it's safe.
	 */
	num_caps += num_realms;
	newlen = (len * (session->s_nr_caps+3)) / (num_caps + 1);
	dout(30, "i guessed %d, and did %d of %d caps, retrying with %d\n",
	     len, num_caps, session->s_nr_caps, newlen);
	len = newlen;
	ceph_msg_put(reply);
	goto retry;
}


/*
 * if the client is unresponsive for long enough, the mds will kill
 * the session entirely.
 */
void ceph_mdsc_handle_reset(struct ceph_mds_client *mdsc, int mds)
{
	derr(1, "mds%d gave us the boot.  IMPLEMENT RECONNECT.\n", mds);
}



/*
 * compare old and new mdsmaps, kicking requests
 * and closing out old connections as necessary
 *
 * called under mdsc->mutex.
 */
static void check_new_map(struct ceph_mds_client *mdsc,
			  struct ceph_mdsmap *newmap,
			  struct ceph_mdsmap *oldmap)
{
	int i;
	int oldstate, newstate;
	struct ceph_mds_session *s;

	dout(20, "check_new_map new %u old %u\n",
	     newmap->m_epoch, oldmap->m_epoch);

	for (i = 0; i < oldmap->m_max_mds && i < mdsc->max_sessions; i++) {
		if (mdsc->sessions[i] == NULL)
			continue;
		s = mdsc->sessions[i];
		oldstate = ceph_mdsmap_get_state(oldmap, i);
		newstate = ceph_mdsmap_get_state(newmap, i);

		dout(20, "check_new_map mds%d state %d -> %d (session %s)\n",
		     i, oldstate, newstate, session_state_name(s->s_state));
		if (newstate < oldstate) {
			/* if the state moved backwards, that means
			 * the old mds failed and/or a new mds is
			 * recovering in its place. */
			/* notify messenger to close out old messages,
			 * socket. */
			ceph_messenger_mark_down(mdsc->client->msgr,
						 &oldmap->m_addr[i]);

			if (s->s_state == CEPH_MDS_SESSION_OPENING) {
				/* the session never opened, just close it
				 * out now */
				complete(&s->s_completion);
				unregister_session(mdsc, i);
			}

			/* kick any requests waiting on the recovering mds */
			kick_requests(mdsc, i, 1);
			continue;
		}

		/*
		 * kick requests on any mds that has gone active.
		 *
		 * kick requests on cur or forwarder: we may have sent
		 * the request to mds1, mds1 told us it forwarded it
		 * to mds2, but then we learn mds1 failed and can't be
		 * sure it successfully forwarded our request before
		 * it died.
		 */
		if (oldstate < CEPH_MDS_STATE_ACTIVE &&
		    newstate >= CEPH_MDS_STATE_ACTIVE)
			kick_requests(mdsc, i, 1);
	}
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
	struct ceph_dentry_info *di;
	int mds;
	struct ceph_mds_lease *h = msg->front.iov_base;
	struct ceph_vino vino;
	int mask;
	struct qstr dname;

	if (le32_to_cpu(msg->hdr.src.name.type) != CEPH_ENTITY_TYPE_MDS)
		return;
	mds = le32_to_cpu(msg->hdr.src.name.num);
	dout(10, "handle_lease from mds%d\n", mds);

	/* decode */
	if (msg->front.iov_len < sizeof(*h) + sizeof(u32))
		goto bad;
	vino.ino = le64_to_cpu(h->ino);
	vino.snap = CEPH_NOSNAP;
	mask = le16_to_cpu(h->mask);
	dname.name = (void *)h + sizeof(*h) + sizeof(u32);
	dname.len = msg->front.iov_len - sizeof(*h) - sizeof(u32);
	if (dname.len != le32_to_cpu(*(__le32 *)(h+1)))
		goto bad;

	/* find session */
	mutex_lock(&mdsc->mutex);
	session = __ceph_lookup_mds_session(mdsc, mds);
	mutex_unlock(&mdsc->mutex);
	if (!session) {
		derr(0, "WTF, got lease but no session for mds%d\n", mds);
		return;
	}

	mutex_lock(&session->s_mutex);
	session->s_seq++;

	/* lookup inode */
	inode = ceph_find_inode(sb, vino);
	dout(20, "handle_lease action is %d, mask %d, ino %llx %p\n", h->action,
	     mask, vino.ino, inode);
	if (inode == NULL) {
		dout(10, "handle_lease no inode %llx\n", vino.ino);
		goto release;
	}

	BUG_ON(h->action != CEPH_MDS_LEASE_REVOKE);  /* for now */

	/* inode */
	ci = ceph_inode(inode);

	/* dentry */
	if (mask & CEPH_LOCK_DN) {
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
		di = ceph_dentry(dentry);
		if (di && di->lease_session == session) {
			h->seq = cpu_to_le32(di->lease_seq);
			revoke_dentry_lease(dentry);
		}
		dput(dentry);
	}

release:
	iput(inode);
	/* let's just reuse the same message */
	h->action = CEPH_MDS_LEASE_RELEASE;
	ceph_msg_get(msg);
	ceph_send_msg_mds(mdsc, msg, mds);
	mutex_unlock(&session->s_mutex);
	ceph_put_mds_session(session);
	return;

bad:
	dout(0, "corrupt lease message\n");
}


/*
 * Preemptively release a lease we expect to invalidate anyway.
 * Pass @inode always, @dentry is optional.
 */
void ceph_mdsc_lease_release(struct ceph_mds_client *mdsc, struct inode *inode,
			     struct dentry *dentry, int mask)
{
	struct ceph_msg *msg;
	struct ceph_mds_lease *lease;
	struct ceph_dentry_info *di;
	int origmask = mask;
	int mds = -1;
	int len = sizeof(*lease) + sizeof(u32);
	int dnamelen = 0;

	BUG_ON(inode == NULL);
	BUG_ON(dentry == NULL);

	/* is dentry lease valid? */
	if (mask & CEPH_LOCK_DN) {
		spin_lock(&dentry->d_lock);
		di = ceph_dentry(dentry);
		if (di &&
		    di->lease_session->s_mds >= 0 &&
		    di->lease_gen == di->lease_session->s_cap_gen &&
		    time_before(jiffies, dentry->d_time)) {
			/* we do have a lease on this dentry; note mds */
			mds = di->lease_session->s_mds;
			dnamelen = dentry->d_name.len;
			len += dentry->d_name.len;
		} else {
			mask &= ~CEPH_LOCK_DN;  /* no lease; clear DN bit */
		}
		spin_unlock(&dentry->d_lock);
	} else {
		mask &= ~CEPH_LOCK_DN;  /* no lease; clear DN bit */
	}

	if (mask == 0) {
		dout(10, "lease_release inode %p dentry %p -- "
		     "no lease on %d\n",
		     inode, dentry, origmask);
		return;  /* nothing to drop */
	}
	BUG_ON(mds < 0);

	dout(10, "lease_release inode %p dentry %p %d mask %d to mds%d\n",
	     inode, dentry, dnamelen, mask, mds);
	msg = ceph_msg_new(CEPH_MSG_CLIENT_LEASE, len, 0, 0, NULL);
	if (IS_ERR(msg))
		return;
	lease = msg->front.iov_base;
	lease->action = CEPH_MDS_LEASE_RELEASE;
	lease->mask = cpu_to_le16(mask);
	lease->ino = cpu_to_le64(ceph_vino(inode).ino);
	lease->first = lease->last = cpu_to_le64(ceph_vino(inode).snap);
	*(__le32 *)((void *)lease + sizeof(*lease)) = cpu_to_le32(dnamelen);
	if (dentry)
		memcpy((void *)lease + sizeof(*lease) + 4, dentry->d_name.name,
		       dnamelen);
	ceph_send_msg_mds(mdsc, msg, mds);
}


/*
 * delayed work -- periodically trim expired leases, renew caps with mds
 */
static void schedule_delayed(struct ceph_mds_client *mdsc)
{
	int delay = 5;
	unsigned hz = round_jiffies_relative(HZ * delay);
	schedule_delayed_work(&mdsc->delayed_work, hz);
}

static void delayed_work(struct work_struct *work)
{
	int i;
	struct ceph_mds_client *mdsc =
		container_of(work, struct ceph_mds_client, delayed_work.work);
	int renew_interval;
	int renew_caps;
	u32 want_map = 0;

	dout(30, "delayed_work\n");
	ceph_check_delayed_caps(mdsc);

	mutex_lock(&mdsc->mutex);
	renew_interval = mdsc->mdsmap->m_session_timeout >> 2;
	renew_caps = time_after_eq(jiffies, HZ*renew_interval +
				   mdsc->last_renew_caps);
	if (renew_caps)
		mdsc->last_renew_caps = jiffies;

	for (i = 0; i < mdsc->max_sessions; i++) {
		struct ceph_mds_session *s = __ceph_lookup_mds_session(mdsc, i);
		if (s == NULL)
			continue;
		if (s->s_state == CEPH_MDS_SESSION_CLOSING) {
			dout(10, "resending session close request for mds%d\n",
			     s->s_mds);
			request_close_session(mdsc, s);
			continue;
		}
		if (s->s_ttl && time_after(jiffies, s->s_ttl)) {
			derr(1, "mds%d session probably timed out, "
			     "requesting mds map\n", s->s_mds);
			want_map = mdsc->mdsmap->m_epoch;
		}
		if (s->s_state < CEPH_MDS_SESSION_OPEN) {
			/* this mds is failed or recovering, just wait */
			ceph_put_mds_session(s);
			continue;
		}
		mutex_unlock(&mdsc->mutex);

		mutex_lock(&s->s_mutex);
		if (renew_caps)
			send_renew_caps(mdsc, s);
		ceph_trim_session_rdcaps(s);
		mutex_unlock(&s->s_mutex);
		ceph_put_mds_session(s);

		mutex_lock(&mdsc->mutex);
	}
	mutex_unlock(&mdsc->mutex);

	if (want_map)
		ceph_monc_request_mdsmap(&mdsc->client->monc, want_map);

	schedule_delayed(mdsc);
}


void ceph_mdsc_init(struct ceph_mds_client *mdsc, struct ceph_client *client)
{
	mdsc->client = client;
	mutex_init(&mdsc->mutex);
	mdsc->mdsmap = NULL;            /* none yet */
	init_completion(&mdsc->map_waiters);
	init_completion(&mdsc->session_close_waiters);
	mdsc->sessions = NULL;
	mdsc->max_sessions = 0;
	mdsc->stopping = 0;
	init_rwsem(&mdsc->snap_rwsem);
	INIT_RADIX_TREE(&mdsc->snap_realms, GFP_NOFS);
	mdsc->last_tid = 0;
	INIT_RADIX_TREE(&mdsc->request_tree, GFP_NOFS);
	INIT_DELAYED_WORK(&mdsc->delayed_work, delayed_work);
	mdsc->last_renew_caps = jiffies;
	INIT_LIST_HEAD(&mdsc->cap_delay_list);
	spin_lock_init(&mdsc->cap_delay_lock);
	INIT_LIST_HEAD(&mdsc->snap_flush_list);
	spin_lock_init(&mdsc->snap_flush_lock);
}

/*
 * drop all leases (and dentry refs) in preparation for umount
 */
static void drop_leases(struct ceph_mds_client *mdsc)
{
	int i;

	dout(10, "drop_leases\n");
	mutex_lock(&mdsc->mutex);
	for (i = 0; i < mdsc->max_sessions; i++) {
		struct ceph_mds_session *s = __ceph_lookup_mds_session(mdsc, i);
		if (!s)
			continue;
		mutex_unlock(&mdsc->mutex);
		mutex_lock(&s->s_mutex);
		mutex_unlock(&s->s_mutex);
		ceph_put_mds_session(s);
		mutex_lock(&mdsc->mutex);
	}
	mutex_unlock(&mdsc->mutex);
}

/*
 * called before mount is ro, and before dentries are torn down.
 * (hmm, does this still race with new lookups?)
 */
void ceph_mdsc_pre_umount(struct ceph_mds_client *mdsc)
{
	drop_leases(mdsc);
	ceph_check_delayed_caps(mdsc);
}

/*
 * called after sb is ro.
 */
void ceph_mdsc_close_sessions(struct ceph_mds_client *mdsc)
{
	struct ceph_mds_session *session;
	int i;
	int n;
	unsigned long started, timeout = 60 * HZ;
	struct ceph_client *client = mdsc->client;

	dout(10, "close_sessions\n");
	mdsc->stopping = 1;

	/*
	 * clean out the delayed cap list; we will flush everything
	 * explicitly below.
	 */
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

	mutex_lock(&mdsc->mutex);

	/* close sessions, caps.
	 *
	 * WARNING the session close timeout (and forced unmount in
	 * general) is somewhat broken.. we'll leaved inodes pinned
	 * and other nastyness.
	 */
	started = jiffies;
	while (time_before(jiffies, started + timeout)) {
		dout(10, "closing sessions\n");
		n = 0;
		for (i = 0; i < mdsc->max_sessions; i++) {
			session = __ceph_lookup_mds_session(mdsc, i);
			if (!session)
				continue;
			mutex_unlock(&mdsc->mutex);
			mutex_lock(&session->s_mutex);
			__close_session(mdsc, session);
			mutex_unlock(&session->s_mutex);
			ceph_put_mds_session(session);
			mutex_lock(&mdsc->mutex);
			n++;
		}
		if (n == 0)
			break;

		if (client->mount_state == CEPH_MOUNT_SHUTDOWN)
			break;

		dout(10, "waiting for sessions to close\n");
		mutex_unlock(&mdsc->mutex);
		wait_for_completion_timeout(&mdsc->session_close_waiters,
					    timeout);
		mutex_lock(&mdsc->mutex);
	}

	WARN_ON(!list_empty(&mdsc->cap_delay_list));

	mutex_unlock(&mdsc->mutex);

	cancel_delayed_work_sync(&mdsc->delayed_work); /* cancel timer */

	dout(10, "stopped\n");
}

void ceph_mdsc_stop(struct ceph_mds_client *mdsc)
{
	dout(10, "stop\n");
	cancel_delayed_work_sync(&mdsc->delayed_work); /* cancel timer */
	if (mdsc->mdsmap)
		ceph_mdsmap_destroy(mdsc->mdsmap);
	kfree(mdsc->sessions);
}


/*
 * handle mds map update.
 */
void ceph_mdsc_handle_map(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	u32 epoch;
	u32 maplen;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;
	struct ceph_mdsmap *newmap, *oldmap;
	ceph_fsid_t fsid;
	int err = -EINVAL;
	int from;
	__le64 major, minor;

	if (le32_to_cpu(msg->hdr.src.name.type) == CEPH_ENTITY_TYPE_MDS)
		from = le32_to_cpu(msg->hdr.src.name.num);
	else
		from = -1;

	ceph_decode_need(&p, end, sizeof(fsid)+2*sizeof(u32), bad);
	ceph_decode_64_le(&p, major);
	__ceph_fsid_set_major(&fsid, major);
	ceph_decode_64_le(&p, minor);
	__ceph_fsid_set_minor(&fsid, minor);
	if (ceph_fsid_compare(&fsid, &mdsc->client->monc.monmap->fsid)) {
		derr(0, "got mdsmap with wrong fsid\n");
		return;
	}
	ceph_decode_32(&p, epoch);
	ceph_decode_32(&p, maplen);
	dout(2, "handle_map epoch %u len %d\n", epoch, (int)maplen);

	/* do we need it? */
	ceph_monc_got_mdsmap(&mdsc->client->monc, epoch);
	mutex_lock(&mdsc->mutex);
	if (mdsc->mdsmap && epoch <= mdsc->mdsmap->m_epoch) {
		dout(2, "handle_map epoch %u <= our %u\n",
		     epoch, mdsc->mdsmap->m_epoch);
		mutex_unlock(&mdsc->mutex);
		return;
	}

	newmap = ceph_mdsmap_decode(&p, end);
	if (IS_ERR(newmap)) {
		err = PTR_ERR(newmap);
		goto bad;
	}

	/* swap into place */
	if (mdsc->mdsmap) {
		oldmap = mdsc->mdsmap;
		mdsc->mdsmap = newmap;
		check_new_map(mdsc, newmap, oldmap);
		ceph_mdsmap_destroy(oldmap);

		/* reconnect?  a recovering mds will send us an mdsmap,
		 * indicating their state is RECONNECTING, if it wants us
		 * to reconnect. */
		if (from >= 0 && from < newmap->m_max_mds &&
		    ceph_mdsmap_get_state(newmap, from) ==
		    CEPH_MDS_STATE_RECONNECT)
			send_mds_reconnect(mdsc, from);
	} else {
		mdsc->mdsmap = newmap;  /* first mds map */
	}

	mutex_unlock(&mdsc->mutex);
	schedule_delayed(mdsc);
	complete(&mdsc->map_waiters);
	return;

bad:
	derr(1, "problem with mdsmap %d\n", err);
	return;
}


/* eof */
