
#include <linux/wait.h>
#include <linux/sched.h>
#include "mds_client.h"
#include "mon_client.h"

#include "ceph_debug.h"
#include "ceph_fs.h"

int ceph_debug_mdsc = -1;
#define DOUT_VAR ceph_debug_mdsc
#define DOUT_MASK DOUT_MASK_MDSC
#define DOUT_PREFIX "mds: "
#include "super.h"
#include "messenger.h"
#include "decode.h"

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

static int parse_reply_info_trace(void **p, void *end,
				  struct ceph_mds_reply_info *info)
{
	__u16 numi, numd, snapdirpos;
	int err = -EINVAL;

	ceph_decode_need(p, end, 3*sizeof(__u16), bad);
	ceph_decode_16(p, numi);
	ceph_decode_16(p, numd);
	ceph_decode_16(p, snapdirpos);
	info->trace_numi = numi;
	info->trace_numd = numd;
	info->trace_snapdirpos = snapdirpos;
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
	if (unlikely(*p + sizeof(struct ceph_mds_reply_dirfrag) > end))
		goto bad;
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
	p = msg->front.iov_base + sizeof(struct ceph_mds_reply_head);
	end = p + msg->front.iov_len;

	/* trace */
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

	/* snap blob */
	ceph_decode_32_safe(&p, end, len, bad);
	info->snapblob_len = len;
	info->snapblob = p;

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

struct ceph_mds_session *__ceph_get_mds_session(struct ceph_mds_client *mdsc,
						int mds)
{
	struct ceph_mds_session *session;
	if (mds >= mdsc->max_sessions || mdsc->sessions[mds] == 0)
		return NULL;
	session = mdsc->sessions[mds];
	dout(30, "get_mds_session %p %d -> %d\n", session,
	     atomic_read(&session->s_ref), atomic_read(&session->s_ref)+1);
	atomic_inc(&session->s_ref);
	return session;
}

void ceph_put_mds_session(struct ceph_mds_session *s)
{
	BUG_ON(s == NULL);
	dout(30, "put_mds_session %p %d -> %d\n", s,
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

	s = kmalloc(sizeof(*s), GFP_NOFS);
	s->s_mds = mds;
	s->s_ttl = 0;
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

	/* register */
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
			       mdsc->max_sessions * sizeof(void*));
			kfree(mdsc->sessions);
		}
		mdsc->sessions = sa;
		mdsc->max_sessions = newmax;
	}
	if (mdsc->sessions[mds]) {
		ceph_put_mds_session(s); /* lost race */
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
	ceph_put_mds_session(mdsc->sessions[mds]);
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
		ceph_put_mds_session(req->r_session);
		req->r_session = 0;
	}
	if (req->r_fwd_session) {
		ceph_put_mds_session(req->r_fwd_session);
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
		if (req->r_direct_dentry)
			dput(req->r_direct_dentry);
		if (req->r_last_inode)
			iput(req->r_last_inode);
		if (req->r_last_dentry)
			dput(req->r_last_dentry);
		if (req->r_old_dentry)
			dput(req->r_old_dentry);
		put_request_sessions(req);
		kfree(req);
	}
}

static struct ceph_mds_request *__get_request(struct ceph_mds_client *mdsc,
					     __u64 tid)
{
	struct ceph_mds_request *req;
	req = radix_tree_lookup(&mdsc->request_tree, tid);
	if (req)
		get_request(req);
	return req;
}

static struct ceph_mds_request *new_request(struct ceph_msg *msg)
{
	struct ceph_mds_request *req;

	req = kzalloc(sizeof(*req), GFP_NOFS);
	req->r_request = msg;
	req->r_reply = 0;
	req->r_timeout = 0;
	req->r_started = jiffies;
	req->r_err = 0;
	req->r_direct_dentry = 0;
	req->r_direct_mode = USE_ANY_MDS;
	req->r_direct_hash = 0;
	req->r_direct_is_hash = false;
	req->r_last_inode = 0;
	req->r_last_dentry = 0;
	req->r_old_dentry = 0;
	req->r_expects_cap = 0;
	req->r_fmode = 0;
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

static bool have_session(struct ceph_mds_client *mdsc, int mds)
{
	if (mds >= mdsc->max_sessions)
		return false;
	return mdsc->sessions[mds] ? true:false;
}


/*
 * choose mds to send request to next
 */
static int choose_mds(struct ceph_mds_client *mdsc,
		      struct ceph_mds_request *req)
{
	int mds = -1;
	u32 hash = req->r_direct_hash;
	bool is_hash = req->r_direct_is_hash;
	struct dentry *dentry = req->r_direct_dentry;
	struct ceph_inode_info *ci;
	struct ceph_inode_frag *frag = 0;
	int mode = req->r_direct_mode;

	/* is there a specific mds we should try? */
	if (req->r_resend_mds >= 0 &&
	    (!have_session(mdsc, req->r_resend_mds) ||
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

	while (dentry) {
		if (is_hash &&
		    dentry->d_inode &&
		    S_ISDIR(dentry->d_inode->i_mode)) {
			int found;
			ci = ceph_inode(dentry->d_inode);
			frag = kmalloc(sizeof(struct ceph_inode_frag), GFP_KERNEL);

			if (!frag)
				return -ENOMEM;

			ceph_choose_frag(ci, hash, frag, &found);
			if (found) {
				/* avoid hitting dir replicas on dir
				 * auth delegation point.. mds will
				 * likely forward anyway to avoid
				 * twiddling scatterlock */
				if (mode == USE_ANY_MDS && frag->ndist > 0 &&
				    dentry != req->r_direct_dentry) {
					u8 r;
					get_random_bytes(&r, 1);
					r %= frag->ndist;
					mds = frag->dist[r];
					dout(20, "choose_mds %p %llx.%llx "
					     "frag %u mds%d (%d/%d)\n",
					     dentry->d_inode,
					     ceph_vinop(&ci->vfs_inode),
					     frag->frag, frag->mds,
					     (int)r, frag->ndist);
					return mds;
				}
				mode = USE_AUTH_MDS;
				if (frag->mds >= 0) {
					mds = frag->mds;
					dout(20, "choose_mds %p %llx.%llx "
					     "frag %u mds%d (auth)\n",
					     dentry->d_inode,
					     ceph_vinop(&ci->vfs_inode),
					     frag->frag, mds);
					return mds;
				}
			}
			kfree(frag);
		}
		if (IS_ROOT(dentry))
			break;
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

static int wait_for_new_map(struct ceph_mds_client *mdsc,
			     unsigned long timeout)
{
	__u32 have;
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
	} else
		wait_for_completion(&mdsc->map_waiters);
	mutex_lock(&mdsc->mutex);
	dout(30, "wait_for_new_map err %d\n", err);
	return err;
}

static int open_session(struct ceph_mds_client *mdsc,
			struct ceph_mds_session *session, unsigned long timeout)
{
	struct ceph_msg *msg;
	int mstate;
	int mds = session->s_mds;
	int err = 0;

	/* mds active? */
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
	} else
		wait_for_completion(&session->s_completion);
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

	/*
	 * fixme: when we start locking the inode, make sure
	 * we don't deadlock with __remove_cap in inode.c.
	 */
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
	struct ceph_cap *cap;

	dout(10, "wake_up_session_caps %p mds%d\n", session, session->s_mds);
	list_for_each(p, &session->s_caps) {
		cap = list_entry(p, struct ceph_cap, session_caps);
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
 * caller holds s_mutex
 */
static int send_renew_caps(struct ceph_mds_client *mdsc,
			   struct ceph_mds_session *session)
{
	struct ceph_msg *msg;

	if (time_after_eq(jiffies, session->s_cap_ttl) &&
	    time_after_eq(session->s_cap_ttl, session->s_renew_requested))
		dout(1, "mds%d session caps stale\n", session->s_mds);

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
	mutex_lock(&mdsc->mutex);
	session = __ceph_get_mds_session(mdsc, mds);
	if (session && mdsc->mdsmap)
		session->s_ttl = jiffies + HZ*mdsc->mdsmap->m_session_autoclose;
	mutex_unlock(&mdsc->mutex);

	if (!session) {
		dout(10, "handle_session no session for mds%d\n", mds);
		return;
	}

	mutex_lock(&session->s_mutex);

	was_stale = session->s_cap_ttl == 0 ||
		time_after_eq(jiffies, session->s_cap_ttl);

	dout(2, "handle_session %s %p seq %llu (i was %s)\n",
	     ceph_session_op_name(op), session, seq,
	     was_stale ? "stale":"not stale");
	switch (op) {
	case CEPH_SESSION_OPEN:
		dout(2, "session open from mds%d\n", mds);
		session->s_state = CEPH_MDS_SESSION_OPEN;
		spin_lock(&session->s_cap_lock);
		session->s_cap_ttl = session->s_renew_requested +
			mdsc->mdsmap->m_session_timeout*HZ;
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
			mdsc->mdsmap->m_session_timeout*HZ;
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

	mutex_unlock(&session->s_mutex);
	ceph_put_mds_session(session);
	return;

bad:
	derr(1, "corrupt session message, len %d, expected %d\n",
	     (int)msg->front.iov_len, (int)sizeof(*h));
	return;
}

void ceph_mdsc_handle_reset(struct ceph_mds_client *mdsc, int mds)
{
	derr(1, "mds%d gave us the boot.  IMPLEMENT RECONNECT.\n", mds);
}


/* exported functions */

/*
 * slight hacky weirdness: if op is a FINDINODE, ino1 is the _length_
 * of path1, and path1 isn't null terminated (it's an nfs filehandle
 * fragment).  path2 is not used.
 */
struct ceph_mds_request *
ceph_mdsc_create_request(struct ceph_mds_client *mdsc, int op,
			 ceph_ino_t ino1, const char *path1,
			 ceph_ino_t ino2, const char *path2,
			 struct dentry *ref, int mode)
{
	struct ceph_msg *msg;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *head;
	void *p, *end;
	int pathlen;

	if (op == CEPH_MDS_OP_FINDINODE)
		pathlen = sizeof(u32) + ino1*sizeof(struct ceph_inopath_item);
	else {
		pathlen = 2*(sizeof(ino1) + sizeof(__u32));
		if (path1)
			pathlen += strlen(path1);
		if (path2)
			pathlen += strlen(path2);
	}

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

	/* direct request */
	if (ref)
		dget(ref);
	req->r_direct_dentry = ref;
	req->r_direct_mode = mode;
	req->r_direct_hash = -1;

	/* encode head */
	/* tid, oldest_client_tid set by do_request */
	head->mdsmap_epoch = cpu_to_le64(mdsc->mdsmap->m_epoch);
	head->num_fwd = 0;
	/* head->retry_attempt = 0; set by do_request */
	head->mds_wants_replica_in_dirino = 0;
	head->op = cpu_to_le32(op);
	head->caller_uid = cpu_to_le32(current->fsuid);
	head->caller_gid = cpu_to_le32(current->fsgid);

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

	mutex_lock(&mdsc->mutex);
	__register_request(mdsc, req);
retry:
	if (req->r_timeout &&
	    time_after_eq(jiffies, req->r_started + req->r_timeout)) {
		dout(10, "do_request timed out\n");
		err = -EIO;
		goto finish;
	}

	mds = choose_mds(mdsc, req);
	if (mds < 0) {
		dout(30, "do_request waiting for new mdsmap\n");
		err = wait_for_new_map(mdsc, req->r_timeout);
		if (err)
			goto finish;
		goto retry;
	}

	/* get session */
	session = __ceph_get_mds_session(mdsc, mds);
	if (!session)
		session = __register_session(mdsc, mds);
	dout(30, "do_request mds%d session %p state %d\n", mds, session,
	     session->s_state);

	/* open? */
	err = 0;
	if (session->s_state == CEPH_MDS_SESSION_NEW ||
	    session->s_state == CEPH_MDS_SESSION_CLOSING) {
		err = open_session(mdsc, session, req->r_timeout);
		dout(30, "do_request open_session err=%d\n", err);
	}
	if (session->s_state != CEPH_MDS_SESSION_OPEN ||
		err == -EAGAIN) {
		dout(30, "do_request session %p not open, state=%d, waiting\n",
		     session, session->s_state);
		ceph_put_mds_session(session);
		goto retry;
	}

	/* make request? */
	if (req->r_from_time == 0)
		req->r_from_time = jiffies;
	BUG_ON(req->r_session);
	req->r_session = session; /* request now owns the session ref */
	req->r_resend_mds = -1;  /* forget any specific mds hint */
	req->r_attempts++;
	rhead = req->r_request->front.iov_base;
	rhead->retry_attempt = cpu_to_le32(req->r_attempts-1);
	rhead->oldest_client_tid = cpu_to_le64(__get_oldest_tid(mdsc));

	/* send and wait */
	mutex_unlock(&mdsc->mutex);
	dout(10, "do_request %p r_expects_cap=%d\n", req, req->r_expects_cap);
	req->r_request = ceph_msg_maybe_dup(req->r_request);
	ceph_msg_get(req->r_request);
	ceph_send_msg_mds(mdsc, req->r_request, mds);
	if (req->r_timeout) {
		err = wait_for_completion_timeout(&req->r_completion,
						  req->r_timeout);
		if (err > 0)
			err = 0;
		else if (err == 0)
			err = -EIO;
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
		req->r_reply = 0;
	}
	if (!err)
		err = le32_to_cpu(req->r_reply_info.head->result);

	/* clean up request, parse reply */
finish:
	__unregister_request(mdsc, req);
	mutex_unlock(&mdsc->mutex);

	ceph_msg_put(req->r_request);
	req->r_request = 0;

	dout(30, "do_request done on %p result %d\n", req, err);
	return err;
}

void ceph_mdsc_handle_reply(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	struct ceph_mds_request *req;
	struct ceph_mds_reply_head *head = msg->front.iov_base;
	struct ceph_mds_reply_info *rinfo;
	u64 tid;
	int err, result;
	int mds;
	u32 cap, capseq, mseq;

	/* extract tid */
	if (msg->front.iov_len < sizeof(*head)) {
		derr(1, "handle_reply got corrupt (short) reply\n");
		return;
	}
	tid = le64_to_cpu(head->tid);

	/* get request, session */
	mutex_lock(&mdsc->mutex);
	req = __get_request(mdsc, tid);
	if (!req) {
		dout(1, "handle_reply on unknown tid %llu\n", tid);
		mutex_unlock(&mdsc->mutex);
		return;
	}
	dout(10, "handle_reply %p r_expects_cap=%d\n", req, req->r_expects_cap);
	mds = le32_to_cpu(msg->hdr.src.name.num);
	if (req->r_session && req->r_session->s_mds != mds) {
		ceph_put_mds_session(req->r_session);
		req->r_session = __ceph_get_mds_session(mdsc, mds);
	}
	if (req->r_session == 0) {
		derr(1, "got reply on %llu, but no session for mds%d\n",
		     tid, mds);
		mutex_unlock(&mdsc->mutex);
		return;
	}
	BUG_ON(req->r_reply);
	if (req->r_expects_cap)
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

	/* insert trace into our cache */
	err = ceph_fill_trace(mdsc->client->sb, req, req->r_session);
	if (err)
		goto done;
	if (result == 0) {
		/* caps? */
		if (req->r_expects_cap && req->r_last_inode) {
			cap = le32_to_cpu(rinfo->head->file_caps);
			capseq = le32_to_cpu(rinfo->head->file_caps_seq);
			mseq = le32_to_cpu(rinfo->head->file_caps_mseq);
			if (ceph_snap(req->r_last_inode) == CEPH_NOSNAP) {
				err = ceph_add_cap(req->r_last_inode,
						   req->r_session,
						   req->r_fmode,
						   cap, capseq, mseq,
						   rinfo->snapblob,
						   rinfo->snapblob_len);
				if (err)
					goto done;
			} else {
				struct ceph_inode_info *ci =
					ceph_inode(req->r_last_inode);
				spin_lock(&req->r_last_inode->i_lock);
				ci->i_snap_caps |= cap;
				__ceph_get_fmode(ci, req->r_fmode);
				spin_unlock(&req->r_last_inode->i_lock);
			}
		}

		/* readdir result? */
		if (rinfo->dir_nr)
			ceph_readdir_prepopulate(req);
	}

done:
	if (req->r_expects_cap)
		up_write(&mdsc->snap_rwsem);
	mutex_unlock(&req->r_session->s_mutex);
	mutex_lock(&mdsc->mutex);
	if (err) {
		req->r_err = err;
	} else {
		req->r_reply = msg;
		ceph_msg_get(msg);
	}
	mutex_unlock(&mdsc->mutex);

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
	__u8 must_resend;
	int err = -EINVAL;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;
	int from_mds = le32_to_cpu(msg->hdr.src.name.num);

	/* decode */
	ceph_decode_need(&p, end, sizeof(__u64)+2*sizeof(__u32), bad);
	ceph_decode_64(&p, tid);
	ceph_decode_32(&p, next_mds);
	ceph_decode_32(&p, fwd_seq);
	ceph_decode_8(&p, must_resend);

	/* handle */
	mutex_lock(&mdsc->mutex);
	req = __get_request(mdsc, tid);
	if (!req) {
		dout(10, "forward %llu dne\n", tid);
		goto out;  /* dup reply? */
	}

	/* do we have a session with the dest mds? */
	if (fwd_seq <= req->r_num_fwd) {
		dout(10, "forward %llu to mds%d - old seq %d <= %d\n",
		     tid, next_mds, req->r_num_fwd, fwd_seq);
	} else if (!must_resend &&
		   have_session(mdsc, next_mds) &&
		   mdsc->sessions[next_mds]->s_state == CEPH_MDS_SESSION_OPEN) {
		/* yes.  adjust mds set, but mds will do the forward. */
		dout(10, "forward %llu to mds%d (mds fwded)\n", tid, next_mds);
		req->r_num_fwd = fwd_seq;
		req->r_resend_mds = next_mds;
		put_request_sessions(req);
		req->r_session = __ceph_get_mds_session(mdsc, next_mds);
		req->r_fwd_session = __ceph_get_mds_session(mdsc, from_mds);
	} else {
		/* no, resend. */
		/* forward race not possible; mds would drop */
		dout(10, "forward %llu to mds%d (we resend)\n", tid, next_mds);
		BUG_ON(fwd_seq <= req->r_num_fwd);
		put_request_sessions(req);
		req->r_resend_mds = next_mds;
		complete(&req->r_completion);
	}

	ceph_mdsc_put_request(req);
out:
	mutex_unlock(&mdsc->mutex);
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
	struct ceph_cap *cap;
	char *path;
	int pathlen, err;
	u64 pathbase;
	struct dentry *dentry;
	struct ceph_inode_info *ci;
	struct ceph_mds_cap_reconnect *rec;
	int num_caps, num_realms = 0;
	int got;
	u64 next_snap_ino = 0;
	u32 *pnum_realms;

	dout(1, "reconnect to recovering mds%d\n", mds);

	/* find session */
	session = __ceph_get_mds_session(mdsc, mds);
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

	down_read(&mdsc->snap_rwsem);
	mutex_unlock(&mdsc->mutex);    /* drop lock for duration */
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
	num_caps = 0;
	list_for_each(cp, &session->s_caps) {
		struct inode *inode;

		cap = list_entry(cp, struct ceph_cap, session_caps);
		ci = cap->ci;
		inode = &ci->vfs_inode;

		dout(10, " adding cap %p on ino %llx.%llx inode %p\n", cap,
		     ceph_vinop(inode), inode);
		ceph_decode_need(&p, end, sizeof(u64), needmore);
		ceph_encode_64(&p, ceph_ino(inode));

		dentry = d_find_alias(inode);
		if (dentry) {
			path = ceph_build_dentry_path(dentry, &pathlen,
						      &pathbase, 9999);
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

		ceph_decode_need(&p, end, sizeof(*rec), needmore);
		rec = p;
		p += sizeof(*rec);
		BUG_ON(p > end);
		spin_lock(&inode->i_lock);
		cap->seq = 0;  /* reset cap seq */
		rec->wanted = cpu_to_le32(__ceph_caps_wanted(ci));
		rec->issued = cpu_to_le32(__ceph_caps_issued(ci, 0));
		rec->size = cpu_to_le64(inode->i_size);
		ceph_encode_timespec(&rec->mtime, &inode->i_mtime);
		ceph_encode_timespec(&rec->atime, &inode->i_atime);
		rec->snaprealm = cpu_to_le64(ci->i_snap_realm->ino);
		spin_unlock(&inode->i_lock);

		kfree(path);
		dput(dentry);
		num_caps++;
	}

	/* snaprealms */
	next_snap_ino = 0;
	pnum_realms = p;
	ceph_decode_need(&p, end, sizeof(pnum_realms), needmore);
	p += sizeof(*pnum_realms);
	num_realms = 0;
	while (1) {
		struct ceph_snap_realm *realm;
		struct ceph_mds_snaprealm_reconnect *rec;
		got = radix_tree_gang_lookup(&mdsc->snap_realms,
					     (void **)&realm, next_snap_ino, 1);
		if (!got)
			break;

		dout(10, " adding snap realm %llx seq %lld parent %llx\n",
		     realm->ino, realm->seq, realm->parent_ino);
		ceph_decode_need(&p, end, sizeof(*rec), needmore);
		rec = p;
		rec->ino = cpu_to_le64(realm->ino);
		rec->seq = cpu_to_le64(realm->seq);
		rec->parent = cpu_to_le64(realm->parent_ino);
		p += sizeof(*rec);
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
		if (session->s_state == CEPH_MDS_SESSION_RECONNECTING) {
			session->s_state = CEPH_MDS_SESSION_OPEN;
			complete(&session->s_completion);
		} else
			dout(0, "WARNING: reconnect on %p raced and lost?\n",
			     session);
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
	/* bleh, this doesn't very accurately factor in snap realms,
	   but it's safe. */
	num_caps += num_realms;
	newlen = len * (session->s_nr_caps+1);
	do_div(newlen, (num_caps+1));
	dout(30, "i guessed %d, and did %d of %d, retrying with %d\n",
	     len, num_caps, session->s_nr_caps, newlen);
	len = newlen;
	ceph_msg_put(reply);
	goto retry;

bad:
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

	for (i = 0; i < oldmap->m_max_mds && i < mdsc->max_sessions; i++) {
		if (mdsc->sessions[i] == 0)
			continue;
		session = mdsc->sessions[i];
		oldstate = ceph_mdsmap_get_state(oldmap, i);
		newstate = ceph_mdsmap_get_state(newmap, i);

		dout(20, "check_new_map mds%d state %d -> %d sess state %d\n",
		     i, oldstate, newstate, session->s_state);
		if (newstate < oldstate) {
			/* notify messenger */
			ceph_messenger_mark_down(mdsc->client->msgr,
						 &oldmap->m_addr[i]);

			/* kill session */
			if (session->s_state == CEPH_MDS_SESSION_OPENING) {
				complete(&session->s_completion);
				__unregister_session(mdsc, i);
				continue;
			}
		}

		kick_requests(mdsc, i, 1); /* cur or forwarder */
	}
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

static int close_session(struct ceph_mds_client *mdsc,
			 struct ceph_mds_session *session)
{
	int mds = session->s_mds;
	int err = 0;

	dout(10, "close_session mds%d\n", mds);
	mutex_lock(&session->s_mutex);

	if (session->s_state >= CEPH_MDS_SESSION_CLOSING)
		goto done;

	ceph_flush_write_caps(mdsc, session, 1);

	session->s_state = CEPH_MDS_SESSION_CLOSING;
	err = request_close_session(mdsc, session);

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
	struct ceph_vino vino;
	int mask;
	struct qstr dname;

	dout(10, "handle_lease from mds%d\n", mds);

	/* decode */
	if (msg->front.iov_len < sizeof(*h) + sizeof(u32))
		goto bad;
	vino.ino = le64_to_cpu(h->ino);
	vino.snap = CEPH_NOSNAP;
	mask = le16_to_cpu(h->mask);
	dname.name = (void *)h + sizeof(*h) + sizeof(u32);
	dname.len = msg->front.iov_len - sizeof(*h) - sizeof(u32);
	if (dname.len != le32_to_cpu(*(u32 *)(h+1)))
		goto bad;

	/* find session */
	mutex_lock(&mdsc->mutex);
	session = __ceph_get_mds_session(mdsc, mds);
	mutex_unlock(&mdsc->mutex);
	if (!session) {
		dout(10, "WTF, got lease but no session for mds%d\n", mds);
		return;
	}

	mutex_lock(&session->s_mutex);
	session->s_seq++;

	/* lookup inode */
	inode = ceph_find_inode(sb, vino);
	dout(20, "action is %d, mask %d, ino %llx %p\n", h->action,
	     mask, vino.ino, inode);
	if (inode == NULL) {
		dout(10, "i don't have inode %llx\n", vino.ino);
		goto release;
	}

	BUG_ON(h->action != CEPH_MDS_LEASE_REVOKE);  /* for now */

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
	ceph_send_msg_mds(mdsc, msg, mds);
	mutex_unlock(&session->s_mutex);
	ceph_put_mds_session(session);
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
	struct ceph_dentry_info *di;
	int origmask = mask;
	int mds = -1;
	int len = sizeof(*lease) + sizeof(__u32);
	int dnamelen = 0;

	BUG_ON(inode == 0);

	/* is dentry lease valid? */
	if ((mask & CEPH_LOCK_DN) && dentry) {
		spin_lock(&dentry->d_lock);
		di = ceph_dentry(dentry);
		if (di &&
		    di->lease_session->s_mds >= 0 &&
		    di->lease_gen == di->lease_session->s_cap_gen &&
		    time_before(jiffies, dentry->d_time)) {
			dnamelen = dentry->d_name.len;
			len += dentry->d_name.len;
			mds = di->lease_session->s_mds;
		} else
			mask &= ~CEPH_LOCK_DN;  /* no lease; clear DN bit */
		spin_unlock(&dentry->d_lock);
	} else
		mask &= ~CEPH_LOCK_DN;  /* no lease; clear DN bit */

	/* inode lease? */
	ci = ceph_inode(inode);
	spin_lock(&inode->i_lock);
	if (ci->i_lease_session &&
	    ci->i_lease_session->s_mds >= 0 &&
	    ci->i_lease_gen == ci->i_lease_session->s_cap_gen &&
	    time_before(jiffies, ci->i_lease_ttl)) {
		mds = ci->i_lease_session->s_mds;
		mask &= CEPH_LOCK_DN | ci->i_lease_mask;  /* lease is valid */
		ci->i_lease_mask &= ~mask;
	} else
		mask &= CEPH_LOCK_DN;  /* no lease; clear all but DN bits */
	spin_unlock(&inode->i_lock);

	if (mask == 0) {
		dout(10, "lease_release inode %p (%d) dentry %p -- "
		     "no lease on %d\n",
		     inode, ci->i_lease_mask, dentry, origmask);
		return;  /* nothing to drop */
	}
	BUG_ON(mds < 0);

	dout(10, "lease_release inode %p dentry %p %d mask %d to mds%d\n",
	     inode, dentry, dnamelen, mask, mds);

	msg = ceph_msg_new(CEPH_MSG_CLIENT_LEASE, len, 0, 0, 0);
	if (IS_ERR(msg))
		return;
	lease = msg->front.iov_base;
	lease->action = CEPH_MDS_LEASE_RELEASE;
	lease->mask = mask;
	lease->ino = cpu_to_le64(ceph_vino(inode).ino);
	lease->first = lease->last = cpu_to_le64(ceph_vino(inode).snap);
	*(__le32 *)((void *)lease + sizeof(*lease)) = cpu_to_le32(dnamelen);
	if (dentry)
		memcpy((void *)lease + sizeof(*lease) + 4, dentry->d_name.name,
		       dnamelen);

	ceph_send_msg_mds(mdsc, msg, mds);
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
	int renew_interval = mdsc->mdsmap->m_session_timeout >> 2;
	int renew_caps = time_after_eq(jiffies, HZ*renew_interval +
				       mdsc->last_renew_caps);
	u32 want_map = 0;

	dout(10, "delayed_work on %p renew_caps=%d\n", mdsc, renew_caps);

	ceph_check_delayed_caps(mdsc);

	mutex_lock(&mdsc->mutex);
	if (renew_caps)
		mdsc->last_renew_caps = jiffies;

	for (i = 0; i < mdsc->max_sessions; i++) {
		struct ceph_mds_session *session = __ceph_get_mds_session(mdsc, i);
		if (session == 0)
			continue;
		if (session->s_state == CEPH_MDS_SESSION_CLOSING) {
			dout(10, "resending session close request for mds%d\n",
			     session->s_mds);
			request_close_session(mdsc, session);
			continue;
		}
		if (session->s_ttl && time_after(jiffies, session->s_ttl)) {
			derr(1, "mds%d session probably timed out, "
			     "requesting mds map\n", session->s_mds);
			want_map = mdsc->mdsmap->m_epoch;
		}
		if (session->s_state < CEPH_MDS_SESSION_OPEN) {
			ceph_put_mds_session(session);
			continue;
		}
		//mutex_unlock(&mdsc->mutex);
		mutex_lock(&session->s_mutex);

		if (renew_caps)
			send_renew_caps(mdsc, session);
		trim_session_leases(session);

		mutex_unlock(&session->s_mutex);
		ceph_put_mds_session(session);
		//mutex_lock(&mdsc->mutex);
	}
	mutex_unlock(&mdsc->mutex);

	if (want_map)
		ceph_monc_request_mdsmap(&mdsc->client->monc, want_map);

	schedule_delayed(mdsc);
}


void ceph_mdsc_init(struct ceph_mds_client *mdsc, struct ceph_client *client)
{
	mutex_init(&mdsc->mutex);
	init_rwsem(&mdsc->snap_rwsem);
	mdsc->client = client;
	mdsc->mdsmap = 0;            /* none yet */
	mdsc->sessions = 0;
	mdsc->max_sessions = 0;
	mdsc->last_tid = 0;
	mdsc->stopping = 0;
	INIT_RADIX_TREE(&mdsc->snap_realms, GFP_NOFS);
	INIT_RADIX_TREE(&mdsc->request_tree, GFP_NOFS);
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

	mutex_lock(&mdsc->mutex);
	for (i = 0; i < mdsc->max_sessions; i++) {
		struct ceph_mds_session *session =
			__ceph_get_mds_session(mdsc, i);
		if (!session)
			continue;
		remove_session_leases(session);
		ceph_put_mds_session(session);
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
	unsigned long started, timeout = 30 * HZ;
	struct ceph_client *client = mdsc->client;

	dout(10, "close_sessions\n");
	mdsc->stopping = 1;

	/* clean out cap delay list */
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
	down_write(&mdsc->snap_rwsem);

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
			session = __ceph_get_mds_session(mdsc, i);
			if (!session)
				continue;
			close_session(mdsc, session);
			ceph_put_mds_session(session);
			n++;
		}
		if (n == 0)
			break;

		if (client->mount_state == CEPH_MOUNT_SHUTDOWN)
			break;

		dout(10, "waiting for sessions to close\n");
		mutex_unlock(&mdsc->mutex);
		up_write(&mdsc->snap_rwsem);

		wait_for_completion_timeout(&mdsc->session_close_waiters,
					    timeout);
		mutex_lock(&mdsc->mutex);
		down_write(&mdsc->snap_rwsem);
	}

	WARN_ON(!list_empty(&mdsc->cap_delay_list));

	up_write(&mdsc->snap_rwsem);
	mutex_unlock(&mdsc->mutex);

	cancel_delayed_work_sync(&mdsc->delayed_work); /* cancel timer */

	dout(10, "stopped\n");
}

void ceph_mdsc_stop(struct ceph_mds_client *mdsc)
{
	dout(10, "stop\n");
	cancel_delayed_work_sync(&mdsc->delayed_work); /* cancel timer */
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
	ceph_monc_got_mdsmap(&mdsc->client->monc, epoch);
	mutex_lock(&mdsc->mutex);
	if (mdsc->mdsmap && epoch <= mdsc->mdsmap->m_epoch) {
		dout(2, "ceph_mdsc_handle_map epoch %u < our %u\n",
		     epoch, mdsc->mdsmap->m_epoch);
		mutex_unlock(&mdsc->mutex);
		return;
	}

	/* decode */
	newmap = ceph_mdsmap_decode(&p, end);
	if (IS_ERR(newmap))
		goto bad2;

	/* swap into place */
	if (mdsc->mdsmap) {
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
		dout(2, "got first mdsmap %u\n", newmap->m_epoch);
		mdsc->mdsmap = newmap;
	}

	mutex_unlock(&mdsc->mutex);

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
