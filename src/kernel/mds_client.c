
#include <linux/ceph_fs.h>
#include <linux/wait.h>
#include <linux/sched.h>
#include "mds_client.h"
#include "mon_client.h"

int ceph_debug_mdsc = 50;
#define DOUT_VAR ceph_debug_mdsc
#define DOUT_PREFIX "mds: "
#include "super.h"
#include "messenger.h"

/*
 * note: this also appears in messages/MClientRequest.h,
 * but i don't want it inline in the kernel.
 */
const char *ceph_mds_op_name(int op)
{
  switch (op) {
  case CEPH_MDS_OP_STAT:  return "stat";
  case CEPH_MDS_OP_LSTAT: return "lstat";
  case CEPH_MDS_OP_FSTAT: return "fstat";
  case CEPH_MDS_OP_UTIME: return "utime";
  case CEPH_MDS_OP_CHMOD: return "chmod";
  case CEPH_MDS_OP_CHOWN: return "chown";
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
int parse_reply_info_in(void **p, void *end,
			struct ceph_mds_reply_info_in *info)
{
	int err;
	info->in = *p;
	*p += sizeof(struct ceph_mds_reply_inode) +
		sizeof(__u32)*le32_to_cpu(info->in->fragtree.nsplits);
	if ((err == ceph_decode_32(p, end, &info->symlink_len)) < 0)
		return err;
	info->symlink = *p;
	*p += info->symlink_len;
	if (unlikely(*p > end))
		return -EINVAL;
	return 0;
}

int parse_reply_info_trace(void **p, void *end,
			   struct ceph_mds_reply_info *info)
{
	__u32 numi;
	int err = -EINVAL;

	err = ceph_decode_32(p, end, &numi);
	if (err < 0)
		goto bad;
	if (numi == 0)
		goto done;   /* hrm, this shouldn't actually happen, but.. */

	/* alloc one longer shared array */
	info->trace_nr = numi;
	info->trace_in = kmalloc(numi * (sizeof(*info->trace_in) +
					 sizeof(*info->trace_dir) +
					 sizeof(*info->trace_dname) +
					 sizeof(*info->trace_dname_len)),
				 GFP_KERNEL);
	if (info->trace_in == NULL)
		return -ENOMEM;
	info->trace_dir = (void *)(info->trace_in + numi);
	info->trace_dname = (void *)(info->trace_dir + numi);
	info->trace_dname_len = (void *)(info->trace_dname + numi);

	while (1) {
		/* inode */
		err = parse_reply_info_in(p, end, &info->trace_in[numi-1]);
		if (err < 0)
			goto bad;
		if (--numi == 0)
			break;
		/* dentry */
		err = ceph_decode_32(p, end, &info->trace_dname_len[numi]);
		if (err < 0)
			goto bad;
		info->trace_dname[numi] = *p;
		*p += info->trace_dname_len[numi];
		if (*p > end)
			goto bad;
		/* dir */
		info->trace_dir[numi] = *p;
		*p += sizeof(struct ceph_mds_reply_dirfrag) +
			sizeof(__u32)*le32_to_cpu(info->trace_dir[numi]->ndist);
		if (unlikely(*p > end))
			goto bad;
	}

done:
	if (*p != end)
		return -EINVAL;
	return 0;

bad:
	derr(1, "problem parsing trace %d\n", err);
	return err;
}

int parse_reply_info_dir(void **p, void *end, struct ceph_mds_reply_info *info)
{
	__u32 num, i = 0;
	int err = -EINVAL;

	info->dir_dir = *p;
	if (*p + sizeof(*info->dir_dir) > end)
		goto bad;
	*p += sizeof(*info->dir_dir) + sizeof(__u32)*info->dir_dir->ndist;
	if (*p > end)
		goto bad;

	err = ceph_decode_32(p, end, &num);
	if (err < 0)
		goto bad;
	if (num == 0)
		goto done;

	/* alloc large array */
	info->dir_nr = num;
	info->dir_in = kmalloc(num * (sizeof(*info->dir_in) +
				      sizeof(*info->dir_dname) +
				      sizeof(*info->dir_dname_len)),
			       GFP_KERNEL);
	if (info->dir_in == NULL)
		return -ENOMEM;
	info->dir_dname = (void *)(info->dir_in + num);
	info->dir_dname_len = (void *)(info->dir_dname + num);

	while (num) {
		/* dentry, inode */
		err = ceph_decode_32(p, end, &info->dir_dname_len[i]);
		if (err < 0)
			goto bad;
		info->dir_dname[i] = *p;
		*p += info->dir_dname_len[i];
		if (*p > end)
			goto bad;
		err = parse_reply_info_in(p, end, &info->dir_in[i]);
		if (err < 0)
			goto bad;
		i++;
		num--;
	}

done:
	return 0;

bad:
	derr(1, "problem parsing dir contents %d\n", err);
	return err;
}

int parse_reply_info(struct ceph_msg *msg, struct ceph_mds_reply_info *info)
{
	void *p, *end;
	__u32 len;
	int err = -EINVAL;

	memset(info, 0, sizeof(*info));
	info->head = msg->front.iov_base;

	/* trace */
	p = msg->front.iov_base + sizeof(struct ceph_mds_reply_head);
	end = p + msg->front.iov_len;
	err = ceph_decode_32(&p, end, &len);
	if (err < 0)
		goto bad;
	if (len > 0) {
		if (p + len > end)
			goto bad;
		err = parse_reply_info_trace(&p, p+len, info);
		if (err < 0)
			goto bad;
	}

	/* dir content */
	err = ceph_decode_32(&p, end, &len);
	if (err < 0)
		goto bad;
	if (len > 0) {
		if (p + len > end)
			goto bad;
		err = parse_reply_info_dir(&p, p+len, info);
		if (err < 0)
			goto bad;
	}

	return 0;
bad:
	derr(1, "parse_reply err %d\n", err);
	return err;
}

void destroy_reply_info(struct ceph_mds_reply_info *info)
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

void put_session(struct ceph_mds_session *s)
{
	BUG_ON(s == NULL);
	dout(10, "put_session %p %d -> %d\n", s,
	     atomic_read(&s->s_ref), atomic_read(&s->s_ref)-1);
	if (atomic_dec_and_test(&s->s_ref)) {
		kfree(s);
		s = NULL;
	}
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

	s = kmalloc(sizeof(struct ceph_mds_session), GFP_KERNEL);
	s->s_mds = mds;
	s->s_state = CEPH_MDS_SESSION_NEW;
	s->s_cap_seq = 0;
	spin_lock_init(&s->s_cap_lock);
	INIT_LIST_HEAD(&s->s_caps);
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
		sa = kzalloc(newmax * sizeof(void *), GFP_KERNEL);
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

static void unregister_session(struct ceph_mds_client *mdsc, int mds)
{
	dout(10, "unregister_session mds%d %p\n", mds, mdsc->sessions[mds]);
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

static void drop_request_session_attempt_refs(struct ceph_mds_request *req)
{
	int i;
	for (i = 0; i < req->r_num_mds; i++)
		put_session(req->r_mds[i]);
	req->r_num_mds = 0;
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
		if (req->r_session)
			put_session(req->r_session);
		if (req->r_last_inode)
			iput(req->r_last_inode);
		if (req->r_last_dentry)
			dput(req->r_last_dentry);
		drop_request_session_attempt_refs(req);
		kfree(req);
	}
}

static struct ceph_mds_request *
find_request_and_lock(struct ceph_mds_client *mdsc, __u64 tid)
{
	struct ceph_mds_request *req;
	spin_lock(&mdsc->lock);
	req = radix_tree_lookup(&mdsc->request_tree, tid);
	if (!req) {
		spin_unlock(&mdsc->lock);
		return NULL;
	}
	get_request(req);
	return req;
}

static struct ceph_mds_request *new_request(struct ceph_msg *msg)
{
	struct ceph_mds_request *req;

	req = kmalloc(sizeof(*req), GFP_KERNEL);
	req->r_request = msg;
	req->r_reply = 0;
	req->r_last_inode = 0;
	req->r_last_dentry = 0;
	req->r_expects_cap = 0;
	req->r_cap = 0;
	req->r_session = 0;
	req->r_num_mds = 0;
	req->r_attempts = 0;
	req->r_num_fwd = 0;
	req->r_resend_mds = -1;
	atomic_set(&req->r_ref, 1);  /* one for request_tree, one for caller */
	init_completion(&req->r_completion);
	ceph_msg_get(msg);  /* grab reference */

	return req;
}


/*
 * register an in-flight request.
 * fill in tid in msg request header
 */
void __register_request(struct ceph_mds_client *mdsc,
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
	if (IS_ERR(msg))
		return ERR_PTR(-ENOMEM);
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
	spin_unlock(&mdsc->lock);

	/* send connect message */
	msg = create_session_msg(CEPH_SESSION_REQUEST_OPEN, session->s_cap_seq);
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

static int resume_session(struct ceph_mds_client *mdsc,
			  struct ceph_mds_session *session)
{
	int mds = session->s_mds;
	struct ceph_msg *msg;
	struct list_head *cp;
	struct ceph_inode_cap *cap;

	dout(10, "resume_session to mds%d\n", mds);

	/* note cap staleness */
	spin_lock(&session->s_cap_lock);
	list_for_each(cp, &session->s_caps) {
		cap = list_entry(cp, struct ceph_inode_cap, session_caps);
		cap->caps = 0;
	}
	spin_unlock(&session->s_cap_lock);

	session->s_state = CEPH_MDS_SESSION_RESUMING;

	/* send request_resume message */
	spin_unlock(&mdsc->lock);
	msg = create_session_msg(CEPH_SESSION_REQUEST_RESUME,
				 session->s_cap_seq);
	if (IS_ERR(msg)) {
		spin_lock(&mdsc->lock);
		return PTR_ERR(msg);
	}
	send_msg_mds(mdsc, msg, mds);
	spin_lock(&mdsc->lock);
	if (session->s_state == CEPH_MDS_SESSION_RESUMING) {
		session->s_state = CEPH_MDS_SESSION_OPEN;
		complete(&session->s_completion);
	} else {
		derr(0, "WARNING: resume_session on %p lost a race?\n",
		     session);
	}
	return 0;
}

static int close_session(struct ceph_mds_client *mdsc,
			 struct ceph_mds_session *session)
{
	int mds = session->s_mds;
	struct ceph_msg *msg;

	dout(10, "close_session to mds%d\n", mds);
	session->s_state = CEPH_MDS_SESSION_CLOSING;
	spin_unlock(&mdsc->lock);
	msg = create_session_msg(CEPH_SESSION_REQUEST_CLOSE,
				 session->s_cap_seq);
	if (IS_ERR(msg)) {
		spin_lock(&mdsc->lock);
		return PTR_ERR(msg);
	}
	send_msg_mds(mdsc, msg, mds);
	spin_lock(&mdsc->lock);
	return 0;
}

static void remove_session_caps(struct ceph_mds_session *session)
{
	struct ceph_inode_cap *cap;
	struct ceph_inode_info *ci;

	/*
	 * fixme: when we start locking the inode, make sure
	 * we don't deadlock with __remove_cap in inode.c.
	 */
	dout(10, "remove_session_caps on %p\n", session);
	spin_lock(&session->s_cap_lock);
	while (session->s_nr_caps > 0) {
		cap = list_entry(session->s_caps.next, struct ceph_inode_cap,
				 session_caps);
		ci = cap->ci;
		igrab(&ci->vfs_inode);
		dout(10, "removing cap %p, ci is %p, inode is %p\n",
		     cap, ci, &ci->vfs_inode);
		spin_unlock(&session->s_cap_lock);
		ceph_remove_cap(cap);
		spin_lock(&session->s_cap_lock);
		iput(&ci->vfs_inode);
	}
	BUG_ON(session->s_nr_caps > 0);
	spin_unlock(&session->s_cap_lock);
}

void ceph_mdsc_handle_session(struct ceph_mds_client *mdsc,
			      struct ceph_msg *msg)
{
	__u32 op;
	__u64 seq;
	struct ceph_mds_session *session;
	int mds = le32_to_cpu(msg->hdr.src.name.num);
	struct ceph_mds_session_head *h = msg->front.iov_base;

	if (msg->front.iov_len != sizeof(*h))
		goto bad;

	/* decode */
	op = le32_to_cpu(h->op);
	seq = le64_to_cpu(h->seq);

	/* handle */
	spin_lock(&mdsc->lock);
	session = __get_session(mdsc, mds);
	dout(1, "handle_session %p op %d seq %llu\n", session, op, seq);
	switch (op) {
	case CEPH_SESSION_OPEN:
		dout(1, "session open from mds%d\n", mds);
		session->s_state = CEPH_MDS_SESSION_OPEN;
		complete(&session->s_completion);
		break;

	case CEPH_SESSION_CLOSE:
		if (session->s_cap_seq == seq) {
			dout(1, "session close from mds%d\n", mds);
			complete(&session->s_completion); /* for good measure */
			unregister_session(mdsc, mds);
		} else {
			dout(1, "ignoring session close from mds%d, "
			     "seq %llu < my seq %llu\n",
			     le32_to_cpu(msg->hdr.src.name.num),
			     seq, session->s_cap_seq);
		}
		remove_session_caps(session);
		complete(&mdsc->session_close_waiters);
		break;

	case CEPH_SESSION_RENEWCAPS:
		dout(10, "session renewed caps from mds%d\n", mds);
		break;

	case CEPH_SESSION_STALE:
		dout(10, "session stale from mds%d\n", mds);
		resume_session(mdsc, session);
		break;

	case CEPH_SESSION_RESUME:
		dout(10, "session resumed by mds%d\n", mds);
		break;

	default:
		dout(0, "bad session op %d\n", op);
		BUG_ON(1);
	}
	put_session(session);
	spin_unlock(&mdsc->lock);

out:
	return;

bad:
	dout(1, "corrupt session message, len %d, expected %d\n",
	     (int)msg->front.iov_len, (int)sizeof(*h));
	goto out;
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
	head->caller_uid = cpu_to_le32(current->uid);
	head->caller_gid = cpu_to_le32(current->gid);

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
__u64 get_oldest_tid(struct ceph_mds_client *mdsc)
{
	struct ceph_mds_request *first;
	if (radix_tree_gang_lookup(&mdsc->request_tree,
				   (void **)&first, 0, 1) <= 0)
		return 0;
	dout(10, "oldest tid is %llu\n", first->r_tid);
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

	radix_tree_preload(GFP_KERNEL);
	spin_lock(&mdsc->lock);
	__register_request(mdsc, req);

retry:
	mds = choose_mds(mdsc, req);
	if (mds < 0) {
		dout(30, "do_request waiting for new mdsmap\n");
		wait_for_new_map(mdsc);
		goto retry;
	}
	dout(30, "do_request chose mds%d\n", mds);

	/* get session */
	session = __get_session(mdsc, mds);
	dout(30, "do_request __get_session returned %p state %d\n",
	     session, (session ? session->s_state:0));
	if (!session)
		session = __register_session(mdsc, mds);
	dout(30, "do_request session %p state %d\n", session, session->s_state);

	/* open? */
	if (session->s_state == CEPH_MDS_SESSION_NEW ||
	    session->s_state == CEPH_MDS_SESSION_CLOSING) {
		err = open_session(mdsc, session);
		dout(30, "do_request session err=%d\n", err);
		BUG_ON(err && err != -EAGAIN);
	}
	if (session->s_state != CEPH_MDS_SESSION_OPEN) {
		dout(30, "do_request session %p not open, state=%d, waiting\n",
		     session, session->s_state);
		put_session(session);
		goto retry;
	}

	/* make request? */
	BUG_ON(req->r_num_mds >= 2);
	req->r_mds[req->r_num_mds++] = session;
	req->r_resend_mds = -1;  /* forget any specific mds hint */
	req->r_attempts++;
	rhead = req->r_request->front.iov_base;
	rhead->retry_attempt = cpu_to_le32(req->r_attempts-1);
	rhead->oldest_client_tid = cpu_to_le64(get_oldest_tid(mdsc));

	/* send and wait */
	spin_unlock(&mdsc->lock);
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
	drop_request_session_attempt_refs(req);

	err = le32_to_cpu(req->r_reply_info.head->result);
	dout(30, "do_request done on %p result %d tracelen %d\n", req,
	     err, req->r_reply_info.trace_nr);
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
		dout(1, "handle_reply got corrupt (short) reply\n");
		return;
	}
	tid = le64_to_cpu(head->tid);

	/* get request, session */
	req = find_request_and_lock(mdsc, tid);
	if (!req) {
		dout(1, "handle_reply on unknown tid %llu\n", tid);
		return;
	}
	mds = le32_to_cpu(msg->hdr.src.name.num);
	req->r_session = __get_session(mdsc, mds);
	BUG_ON(req->r_session == 0);
	BUG_ON(req->r_reply);
	spin_unlock(&mdsc->lock);

	/* parse */
	rinfo = &req->r_reply_info;
	err = parse_reply_info(msg, rinfo);
	if (err < 0) {
		derr(0, "handle_reply got corrupt reply\n");
		goto done;
	}

	result = le32_to_cpu(rinfo->head->result);
	dout(10, "handle_reply tid %lld result %d\n", tid, result);
	if (result == 0) {
		err = ceph_fill_trace(mdsc->client->sb, req);
		if (err)
			goto done;
		if (req->r_expects_cap) {
			cap = le32_to_cpu(rinfo->head->file_caps);
			capseq = le32_to_cpu(rinfo->head->file_caps_seq);
			req->r_cap = ceph_add_cap(req->r_last_inode,
						  req->r_session,
						  cap, capseq);
			if (IS_ERR(req->r_cap)) {
				err = PTR_ERR(req->r_cap);
				req->r_cap = 0;
				goto done;
			}
		}
	}

done:
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
	int err;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;
	int frommds = le32_to_cpu(msg->hdr.src.name.num);

	/* decode */
	err = ceph_decode_64(&p, end, &tid);
	if (err != 0)
		goto bad;
	err = ceph_decode_32(&p, end, &next_mds);
	if (err != 0)
		goto bad;
	err = ceph_decode_32(&p, end, &fwd_seq);
	if (err != 0)
		goto bad;

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
			drop_request_session_attempt_refs(req);
			req->r_num_mds = 1;
			req->r_mds[0] = __get_session(mdsc, frommds);
		}
		spin_unlock(&mdsc->lock);
	} else {
		/* no, resend. */
		/* forward race not possible; mds would drop */
		BUG_ON(fwd_seq <= req->r_num_fwd);
		drop_request_session_attempt_refs(req);
		req->r_resend_mds = next_mds;
		spin_unlock(&mdsc->lock);
		complete(&req->r_completion);
	}

	ceph_mdsc_put_request(req);
	spin_unlock(&mdsc->lock);
	return;

bad:
	derr(0, "problem decoding message, err=%d\n", err);
}



/*
 * kick outstanding requests
 */
void kick_requests(struct ceph_mds_client *mdsc, int mds)
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
			if ((reqs[i]->r_num_mds >= 1 &&
			     reqs[i]->r_mds[0]->s_mds == mds) ||
			    (reqs[i]->r_num_mds >= 2 &&
			     reqs[i]->r_mds[1]->s_mds == mds)) {
				dout(10, " kicking req %llu\n", reqs[i]->r_tid);
				/* FIXME */
				complete(&reqs[i]->r_completion);
			}
		}
	}
}

/*
 * send an reconnect to a recovering mds
 */
void send_mds_reconnect(struct ceph_mds_client *mdsc, int mds)
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

	dout(10, "send_mds_reconnect mds%d\n", mds);

	/* find session */
	session = __get_session(mdsc, mds);
	if (session) {
		session->s_state = CEPH_MDS_SESSION_RECONNECTING;

		/* estimate needed space */
		spin_lock(&session->s_cap_lock);
		len += session->s_nr_caps *
			sizeof(struct ceph_mds_cap_reconnect);
		len += session->s_nr_caps * (100); /* guess! */
		dout(40, "estimating i need %d bytes for %d caps\n",
		     len, session->s_nr_caps);
		spin_unlock(&session->s_cap_lock);
	} else {
		dout(20, "no session for mds%d, will send short reconnect\n",
		     mds);
	}

	spin_unlock(&mdsc->lock);  /* drop lock for duration */

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
		ceph_encode_8(&p, end, 1); /* session was closed */
		ceph_encode_32(&p, end, 0);
		goto send;
	}
	dout(10, "session %p state %d\n", session, session->s_state);

	/* traverse this session's caps */
	spin_lock(&session->s_cap_lock);
	ceph_encode_8(&p, end, 0);
	ceph_encode_32(&p, end, session->s_nr_caps);
	count = 0;
	list_for_each(cp, &session->s_caps) {
		cap = list_entry(cp, struct ceph_inode_cap, session_caps);
		ci = cap->ci;
		if (p + sizeof(u64) +
		    sizeof(struct ceph_mds_cap_reconnect) > end)
			goto needmore;
		dout(10, " adding cap %p on ino %llx inode %p\n", cap,
		     ceph_ino(&ci->vfs_inode), &ci->vfs_inode);
		ceph_encode_64(&p, end, ceph_ino(&ci->vfs_inode));
		rec = p;
		p += sizeof(*rec);
		BUG_ON(p > end);
		rec->wanted = cpu_to_le32(ceph_caps_wanted(ci));
		rec->issued = cpu_to_le32(ceph_caps_issued(ci));
		rec->size = cpu_to_le64(ci->i_wr_size);
		ceph_encode_timespec(&rec->mtime, &ci->vfs_inode.i_mtime);
		ceph_encode_timespec(&rec->atime, &ci->vfs_inode.i_atime);
		dentry = d_find_alias(&ci->vfs_inode);
		path = ceph_build_dentry_path(dentry, &pathlen);
		if (IS_ERR(path)) {
			err = PTR_ERR(path);
			BUG_ON(err);
		}
		if (p + pathlen + 4 > end)
			goto needmore;
		ceph_encode_string(&p, end, path, pathlen);
		kfree(path);
		dput(dentry);
		count++;
		continue;

needmore:
		newlen = len * (session->s_nr_caps+1);
		do_div(newlen, (count+1));
		dout(30, "i guessed %d, and did %d of %d, retrying with %d\n",
		     len, count, session->s_nr_caps, newlen);
		len = newlen;
		spin_unlock(&session->s_cap_lock);
		ceph_msg_put(reply);
		goto retry;
	}
	spin_unlock(&session->s_cap_lock);

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
		put_session(session);
	}
	return;

bad:
	spin_lock(&mdsc->lock);
	derr(0, "error %d generating reconnect.  what to do?\n", err);
	/* fixme */
	BUG_ON(1);
}

/*
 * fixme: reconnect to an mds that closed our session
 */



/*
 * compare old and new mdsmaps, kicking requests
 * and closing out old connections as necessary
 */
void check_new_map(struct ceph_mds_client *mdsc,
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
			unregister_session(mdsc, i);
			break;
		case CEPH_MDS_SESSION_OPEN:
			kick_requests(mdsc, i);
			break;
		}
	}
}


/* caps */

void send_cap_ack(struct ceph_mds_client *mdsc, __u64 ino, int caps,
		  int wanted, __u32 seq, __u64 size, __u64 max_size, int mds)
{
	struct ceph_mds_file_caps *fc;
	struct ceph_msg *msg;

	dout(10, "send_cap_ack ino %llx caps %d wanted %d seq %u size %llu\n",
	     ino, caps, wanted, (unsigned)seq, size);

	msg = ceph_msg_new(CEPH_MSG_CLIENT_FILECAPS, sizeof(*fc), 0, 0, 0);
	if (IS_ERR(msg))
		return;

	fc = msg->front.iov_base;
	fc->op = cpu_to_le32(CEPH_CAP_OP_ACK);  /* misnomer */
	fc->seq = cpu_to_le64(seq);
	fc->caps = cpu_to_le32(caps);
	fc->wanted = cpu_to_le32(wanted);
	fc->ino = cpu_to_le64(ino);
	fc->size = cpu_to_le64(size);
	fc->max_size = cpu_to_le64(max_size);

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
	session = __get_session(&client->mdsc, mds);
	if (!session) {
		dout(10, "WTF, got filecap but no session for mds%d\n", mds);
		return;
	}
	session->s_cap_seq++;

	/* lookup ino */
	inot = ceph_ino_to_ino(ino);
#if BITS_PER_LONG == 64
	inode = ilookup(sb, ino);
#else
	inode = ilookup5(sb, inot, ceph_ino_compare, &ino);
#endif
	dout(20, "op is %d, ino %llx %p\n", op, ino, inode);

	if (inode && ceph_ino(inode) != ino) {
		BUG_ON(sizeof(ino_t) >= sizeof(u64));
		dout(10, "UH OH, lame ceph ino %llx -> %lu ino_t hash collided?"
		     "  inode is %llx\n", ino, inot, ceph_ino(inode));
		inode = 0;
	}

	if (!inode) {
		dout(10, "wtf, i don't have ino %lu=%llx?  closing out cap\n",
		     inot, ino);
		send_cap_ack(mdsc, ino, 0, 0, seq, size, max_size, mds);
		return;
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
	return;
bad:
	dout(10, "corrupt filecaps message\n");
	return;
}

int ceph_mdsc_update_cap_wanted(struct ceph_inode_info *ci, int wanted)
{
	struct ceph_client *client = ceph_inode_to_client(&ci->vfs_inode);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_inode_cap *cap;
	struct ceph_mds_session *session;
	struct list_head *p;

	dout(10, "update_cap_wanted %d -> %d\n", ci->i_cap_wanted, wanted);

	list_for_each(p, &ci->i_caps) {
		cap = list_entry(p, struct ceph_inode_cap, ci_caps);

		session = __get_session(mdsc, cap->mds);
		BUG_ON(!session);

		cap->caps &= wanted;  /* drop caps we don't want */
		send_cap_ack(mdsc, ceph_ino(&ci->vfs_inode), cap->caps, wanted,
			     cap->seq, ci->vfs_inode.i_size, ci->i_max_size,
			     cap->mds);
	}

	ci->i_cap_wanted = wanted;
	if (wanted == 0)
		ceph_remove_all_caps(ci);

	return 0;
}

int send_renewcaps(struct ceph_mds_client *mdsc, int mds)
{
	struct ceph_msg *msg;

	dout(10, "send_renew_caps to mds%d\n", mds);
	msg = create_session_msg(CEPH_SESSION_REQUEST_RENEWCAPS, 0);
	if (IS_ERR(msg))
		return PTR_ERR(msg);
	send_msg_mds(mdsc, msg, mds);
	return 0;
}

int ceph_mdsc_renew_caps(struct ceph_mds_client *mdsc)
{
	int i;
	dout(10, "renew_caps\n");
	for (i = 0; i < mdsc->max_sessions; i++) {
		if (mdsc->sessions[i] == 0 ||
		    mdsc->sessions[i]->s_state < CEPH_MDS_SESSION_OPEN)
			continue;
		send_renewcaps(mdsc, i);
	}
	return 0;
}


/*
 * delayed work -- renew caps with mds
 */
void schedule_delayed(struct ceph_mds_client *mdsc)
{
	/*
	 * renew at 1/2 the advertised timeout period.
	 */
	unsigned hz = (HZ * mdsc->mdsmap->m_cap_bit_timeout) >> 1;
	schedule_delayed_work(&mdsc->delayed_work, hz);
}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 20)
void delayed_work(struct work_struct *work)
#else
void delayed_work(void *arg)
#endif
{
	int i;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 20)
	struct ceph_mds_client *mdsc =
		container_of(work, struct ceph_mds_client, delayed_work.work);
#else
	struct ceph_mds_client *mdsc = arg;
#endif

	dout(10, "delayed_work on %p\n", mdsc);

	/* renew caps */
	spin_lock(&mdsc->lock);
	for (i = 0; i < mdsc->max_sessions; i++) {
		if (mdsc->sessions[i] == 0 ||
		    mdsc->sessions[i]->s_state < CEPH_MDS_SESSION_OPEN)
			continue;
		spin_unlock(&mdsc->lock);
		send_renewcaps(mdsc, i);
		spin_lock(&mdsc->lock);
	}

	schedule_delayed(mdsc);
	spin_unlock(&mdsc->lock);
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
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 20)
	INIT_DELAYED_WORK(&mdsc->delayed_work, delayed_work);
#else
	INIT_WORK(&mdsc->delayed_work, delayed_work, mdsc);
#endif
}

void ceph_mdsc_stop(struct ceph_mds_client *mdsc)
{
	int i;
	int n;

	dout(10, "stop\n");
	spin_lock(&mdsc->lock);

	/* close sessions, caps */
	while (1) {
		dout(10, "closing sessions\n");
		n = 0;
		for (i = 0; i < mdsc->max_sessions; i++) {
			if (mdsc->sessions[i] == 0 ||
			    mdsc->sessions[i]->s_state >=
			     CEPH_MDS_SESSION_CLOSING)
				continue;
			close_session(mdsc, mdsc->sessions[i]);
			n++;
		}
		if (n == 0)
			break;

		dout(10, "waiting for sessions to close\n");
		spin_unlock(&mdsc->lock);
		wait_for_completion(&mdsc->session_close_waiters);
		spin_lock(&mdsc->lock);
	}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 20)
	cancel_delayed_work_sync(&mdsc->delayed_work); /* cancel timer */
#else
	cancel_delayed_work(&mdsc->delayed_work); /* cancel timer */
	flush_scheduled_work();
#endif
	spin_unlock(&mdsc->lock);
}


/*
 * handle mds map update.
 */
void ceph_mdsc_handle_map(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	ceph_epoch_t epoch;
	__u32 maplen;
	int err;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;
	struct ceph_mdsmap *newmap, *oldmap;
	int from = le32_to_cpu(msg->hdr.src.name.num);
	int newstate;

	err = ceph_decode_32(&p, end, &epoch);
	if (err != 0)
		goto bad;
	err = ceph_decode_32(&p, end, &maplen);
	if (err != 0)
		goto bad;

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

	/* (re)schedule work */
	schedule_delayed(mdsc);

	spin_unlock(&mdsc->lock);
	complete(&mdsc->map_waiters);
	return;

bad:
	dout(1, "corrupt map\n");
	return;
bad2:
	dout(1, "no memory to decode new mdsmap\n");
	return;
}



/* eof */
