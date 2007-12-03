
#include <linux/ceph_fs.h>
#include <linux/wait.h>
#include <linux/sched.h>
#include "mds_client.h"
#include "mon_client.h"
#include "super.h"
#include "messenger.h"


static void send_msg_mds(struct ceph_mds_client *mdsc, struct ceph_msg *msg, int mds)
{
	msg->hdr.dst.addr = *ceph_mdsmap_get_addr(mdsc->mdsmap, mds);
	msg->hdr.dst.name.type = CEPH_ENTITY_TYPE_MDS;
	msg->hdr.dst.name.num = mds;
	ceph_msg_send(mdsc->client->msgr, msg);
}


/*
 * reference count request
 */
static void get_request(struct ceph_mds_request *req)
{
	atomic_inc(&req->r_ref);
}

static void put_request(struct ceph_mds_request *req)
{
	if (atomic_dec_and_test(&req->r_ref)) {
		ceph_msg_put(req->r_request);
		kfree(req);
	} 
}


/*
 * register an in-flight request
 */
static struct ceph_mds_request *
register_request(struct ceph_mds_client *mdsc, struct ceph_msg *msg, int mds)
{
	struct ceph_mds_request *req;

	req = kmalloc(sizeof(*req), GFP_KERNEL);
	req->r_tid = ++mdsc->last_tid;
	req->r_request = msg;
	req->r_reply = 0;
	req->r_num_mds = 0;
	req->r_attempts = 0;
	req->r_num_fwd = 0;
	req->r_resend_mds = mds;
	atomic_set(&req->r_ref, 2);  /* one for request_tree, one for caller */
	init_completion(&req->r_completion);

	radix_tree_insert(&mdsc->request_tree, req->r_tid, (void*)req);
	ceph_msg_get(msg);  /* grab reference */
	return req;
}

void
unregister_request(struct ceph_mds_client *mdsc, struct ceph_mds_request *req)
{
	radix_tree_delete(&mdsc->request_tree, req->r_tid);
	put_request(req);
}


/*
 * choose mds to send request to next
 */
static int choose_mds(struct ceph_mds_client *mdsc, struct ceph_mds_request *req)
{
	/* is there a specific mds we should try? */
	if (req->r_resend_mds >= 0 &&
	    ceph_mdsmap_get_state(mdsc->mdsmap, req->r_resend_mds) > 0)
		return req->r_resend_mds;

	/* pick one at random */
	return ceph_mdsmap_get_random_mds(mdsc->mdsmap);
}

static void register_session(struct ceph_mds_client *mdsc, int mds)
{
	struct ceph_mds_session *s;

	/* register */
	if (mds >= mdsc->max_sessions) {
		struct ceph_mds_session **sa;
		/* realloc */
		dout(50, "mdsc register_session realloc to %d\n", mds+1);
		sa = kzalloc((mds+1) * sizeof(struct ceph_mds_session), GFP_KERNEL);
		BUG_ON(sa == NULL);  /* i am lazy */
		if (mdsc->sessions) {
			memcpy(sa, mdsc->sessions, 
			       mdsc->max_sessions*sizeof(struct ceph_mds_session));
			kfree(mdsc->sessions);
		}
		mdsc->sessions = sa;
		mdsc->max_sessions = mds+1;
	}
	s = kmalloc(sizeof(struct ceph_mds_session), GFP_KERNEL);
	s->s_state = 0;
	s->s_cap_seq = 0;
	atomic_set(&s->s_ref, 1);
	init_completion(&s->s_completion);
	mdsc->sessions[mds] = s;
}

static struct ceph_mds_session *get_session(struct ceph_mds_client *mdsc, int mds)
{
	struct ceph_mds_session *session;
	
	if (mds >= mdsc->max_sessions || mdsc->sessions[mds] == 0) 
		register_session(mdsc, mds);
	session = mdsc->sessions[mds];

	atomic_inc(&session->s_ref);
	return session;
}

static void put_session(struct ceph_mds_session *s)
{
	if (atomic_dec_and_test(&s->s_ref)) 
		kfree(s);
}

static void unregister_session(struct ceph_mds_client *mdsc, int mds)
{
	put_session(mdsc->sessions[mds]);
	mdsc->sessions[mds] = 0;
}

static struct ceph_msg *create_session_msg(__u32 op, __u64 seq)
{
	struct ceph_msg *msg;
	void *p;

	msg = ceph_msg_new(CEPH_MSG_CLIENT_SESSION, sizeof(__u32)+sizeof(__u64), 0, 0);
	if (IS_ERR(msg))
		return ERR_PTR(-ENOMEM);  /* fixme */
	p = msg->front.iov_base;
	*(__le32*)p = cpu_to_le32(op);
	p += sizeof(__le32);
	*(__le64*)p = cpu_to_le64(seq);
	p += sizeof(__le64);

	return msg;
}

static void open_session(struct ceph_mds_client *mdsc, struct ceph_mds_session *session, int mds)
{
	struct ceph_msg *msg;

	/* connect */
	if (ceph_mdsmap_get_state(mdsc->mdsmap, mds) < CEPH_MDS_STATE_ACTIVE) {
		ceph_monc_request_mdsmap(&mdsc->client->monc, mdsc->mdsmap->m_epoch); /* race fixme */
		return;
	} 
	
	/* send connect message */
	msg = create_session_msg(CEPH_SESSION_REQUEST_OPEN, session->s_cap_seq);
	if (IS_ERR(msg))
		return;  /* fixme */
	send_msg_mds(mdsc, msg, mds);
}

void ceph_mdsc_handle_session(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	__u32 op;
	__u64 seq;
	int err;
	struct ceph_mds_session *session;
	int from = msg->hdr.src.name.num;
	void *p = msg->front.iov_base;
	void *end = msg->front.iov_base + msg->front.iov_len;

	/* decode */
	if ((err = ceph_decode_32(&p, end, &op)) != 0)
		goto bad;
	if ((err = ceph_decode_64(&p, end, &seq)) != 0)
		goto bad;
	
	/* handle */
	dout(1, "handle_session op %d seq %llu\n", op, seq);
	spin_lock(&mdsc->lock);
	switch (op) {
	case CEPH_SESSION_OPEN:
		dout(1, "session open from mds%d\n", from);
		session = get_session(mdsc, from);
		session->s_state = CEPH_MDS_SESSION_OPEN;
		complete(&session->s_completion);
		put_session(session);
		break;

	case CEPH_SESSION_CLOSE:
		session = get_session(mdsc, from);
		if (session->s_cap_seq == seq) {
			dout(1, "session close from mds%d\n", from);
			complete(&session->s_completion); /* for good measure */
			unregister_session(mdsc, from);
		} else {
			dout(1, "ignoring session close from mds%d, seq %llu < my seq %llu\n", 
			     msg->hdr.src.name.num, seq, session->s_cap_seq);
		}
		put_session(session);
		break;

	default:
		dout(0, "bad session op %d\n", op);
		BUG_ON(1);
	}
	spin_unlock(&mdsc->lock);

out:
	return;
	
bad:
	dout(1, "corrupt session message\n");
	goto out;
}


static void wait_for_new_map(struct ceph_mds_client *mdsc)
{
	dout(30, "wait_for_new_map enter\n");
	if (mdsc->last_requested_map < mdsc->mdsmap->m_epoch)
		ceph_monc_request_mdsmap(&mdsc->client->monc, mdsc->mdsmap->m_epoch);

	wait_for_completion(&mdsc->map_waiters);
	dout(30, "wait_for_new_map exit\n");
}

/* exported functions */

void ceph_mdsc_init(struct ceph_mds_client *mdsc, struct ceph_client *client)
{
	spin_lock_init(&mdsc->lock);
	mdsc->client = client;
	mdsc->mdsmap = 0;            /* none yet */
	mdsc->sessions = 0;
	mdsc->max_sessions = 0;
	mdsc->last_tid = 0;
	INIT_RADIX_TREE(&mdsc->request_tree, GFP_KERNEL);
	mdsc->last_requested_map = 0;
	init_completion(&mdsc->map_waiters);
}


struct ceph_msg *
ceph_mdsc_create_request_msg(struct ceph_mds_client *mdsc, int op, 
			     ceph_ino_t ino1, const char *path1, 
			     ceph_ino_t ino2, const char *path2)
{
	struct ceph_msg *req;
	struct ceph_client_request_head *head;
	void *p, *end;
	int pathlen = 2*(sizeof(ino1) + sizeof(__u32));
	if (path1) pathlen += strlen(path1);
	if (path2) pathlen += strlen(path2);

	req = ceph_msg_new(CEPH_MSG_CLIENT_REQUEST, 
			   sizeof(struct ceph_client_request_head) + pathlen,
			   0, 0);
	if (IS_ERR(req))
		return req;
	memset(req->front.iov_base, 0, req->front.iov_len);
	head = req->front.iov_base;
	p = req->front.iov_base + sizeof(*head);
	end = req->front.iov_base + req->front.iov_len;

	/* encode head */
	head->op = cpu_to_le32(op);
	ceph_encode_inst(&head->client_inst, &mdsc->client->msgr->inst);
	/*FIXME: head->oldest_client_tid = cpu_to_le64(....);*/  

	/* encode paths */
	ceph_encode_filepath(&p, end, ino1, path1);
	ceph_encode_filepath(&p, end, ino2, path2);

	BUG_ON(p != end);
	
	return req;
}

struct ceph_msg *
ceph_mdsc_do_request(struct ceph_mds_client *mdsc, struct ceph_msg *msg, int mds)
{
	struct ceph_mds_request *req;
	struct ceph_mds_session *session;
	struct ceph_msg *reply = 0;

	spin_lock(&mdsc->lock);
	req = register_request(mdsc, msg, mds);

retry:
	mds = choose_mds(mdsc, req);
	if (mds < 0) {
		/* wait for new mdsmap */
		spin_unlock(&mdsc->lock);
		dout(30, "mdsc_do_request waiting for new mdsmap\n");
		wait_for_new_map(mdsc);
		spin_lock(&mdsc->lock);
		goto retry;
	}
	dout(30, "mdsc_do_request chose mds%d\n", mds);

	/* get session */
	session = get_session(mdsc, mds);
	
	dout(30, "mdsc_do_request got session %p\n", session);

	/* open? */
	if (mdsc->sessions[mds]->s_state == CEPH_MDS_SESSION_IDLE) 
		open_session(mdsc, session, mds);
	if (mdsc->sessions[mds]->s_state != CEPH_MDS_SESSION_OPEN) {
		/* wait for session to open (or fail, or close) */
		spin_unlock(&mdsc->lock);
		wait_for_completion(&session->s_completion);
		put_session(session);
		spin_lock(&mdsc->lock);
		goto retry;
	}
	put_session(session);

	/* make request? */
	if (req->r_num_mds < 4) {
		req->r_mds[req->r_num_mds++] = mds;
		req->r_resend_mds = -1;  /* forget any specific mds hint */
		req->r_attempts++;
		send_msg_mds(mdsc, req->r_request, mds);
	}

	/* wait */
	dout(30, "mdsc_do_request 1\n");
	spin_unlock(&mdsc->lock);
	dout(30, "mdsc_do_request 2\n");
	wait_for_completion(&req->r_completion);
	dout(30, "mdsc_do_request 3\n");

	if (!req->r_reply) {
		dout(30, "mdsc_do_request 4\n");
		spin_lock(&mdsc->lock);
		dout(30, "mdsc_do_request 5\n");
		goto retry;
	}
	reply = req->r_reply;

	dout(30, "mdsc_do_request 6\n");
	spin_lock(&mdsc->lock);
	dout(30, "mdsc_do_request 7\n");
	unregister_request(mdsc, req);
	spin_unlock(&mdsc->lock);

	put_request(req);

	return reply;
}

int ceph_mdsc_do(struct ceph_mds_client *mdsc, int op, 
		 ceph_ino_t ino1, const char *path1, 
		 ceph_ino_t ino2, const char *path2)
{
	struct ceph_msg *req, *reply;
	struct ceph_client_reply_head *head;
	int ret;

	dout(30, "mdsc do 1\n");
	req = ceph_mdsc_create_request_msg(mdsc, op, ino1, path1, ino2, path2);
	if (IS_ERR(req)) 
		return PTR_ERR(req);
	dout(30, "mdsc do 2\n");

	reply = ceph_mdsc_do_request(mdsc, req, -1);
	if (IS_ERR(reply))
		return PTR_ERR(reply);
	dout(30, "mdsc do 3\n");
	head = reply->front.iov_base;
	ret = head->result;
	dout(30, "mdsc do 4\n");
	ceph_msg_put(reply);
	return ret;
}



void ceph_mdsc_handle_reply(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	struct ceph_mds_request *req;
	struct ceph_client_reply_head *head = msg->front.iov_base;
	__u64 tid;

	/* extract tid */
	if (msg->front.iov_len < sizeof(*head)) {
		dout(1, "got corrupt (short) reply\n");
		goto done;
	}
	tid = le64_to_cpu(head->tid);

	/* pass to blocked caller */
	spin_lock(&mdsc->lock);
	req = radix_tree_lookup(&mdsc->request_tree, tid);
	if (!req) {
		spin_unlock(&mdsc->lock);
		dout(1, "got reply on unknown tid %llu\n", tid);
		goto done;
	}

	get_request(req);
	BUG_ON(req->r_reply);
	req->r_reply = msg;
	spin_unlock(&mdsc->lock);

	complete(&req->r_completion);
	put_request(req);
	
done:
	return;
}


struct reply_info_in {
	struct ceph_client_reply_inode *in;
	__u32 symlink_len;
	char *symlink;
};

int parse_reply_info_in(void **p, void *end, struct reply_info_in *in)
{
	int err;
	in->in = *p;
	*p += sizeof(struct ceph_client_reply_inode) +
		sizeof(__u32)*le32_to_cpu(in->in->fragtree.nsplits);
	if ((err == ceph_decode_32(p, end, &in->symlink_len)) < 0)
		return err;
	in->symlink = *p;
	*p += in->symlink_len;
	if (unlikely(*p > end))
		return -EINVAL;
	return 0;
}

struct reply_info {
	int trace_nr;
	struct reply_info_in *trace_in;
	struct ceph_client_reply_dirfrag **trace_dir;
	char **trace_dname;
	__u32 *trace_dname_len;

	struct ceph_client_reply_dirfrag *dir_dir;
	int dir_nr;
	struct reply_info_in *dir_in;
	char **dir_dname;
	__u32 *dir_dname_len;
};

int parse_reply_info_trace(void **p, void *end, struct reply_info *info)
{
	__u32 numi;
	int err = -EINVAL;

	if ((err = ceph_decode_32(p, end, &numi)) < 0)
		goto bad;
	if (numi == 0) 
		goto done;   /* hrm, this shouldn't actually happen, but.. */
	
	/* alloc one longer shared array */
	info->trace_nr = numi;
	info->trace_in = kmalloc(numi * (2*sizeof(void*)+sizeof(*info->trace_in)), GFP_KERNEL);
	if (info->trace_in == NULL)
		return -ENOMEM;
	info->trace_dir = (void*)(info->trace_in + numi);
	info->trace_dname = (void*)(info->trace_dir + numi);
	info->trace_dname_len = (void*)(info->trace_dname + numi);

	while (1) {
		/* inode */
		if ((err = parse_reply_info_in(p, end, &info->trace_in[numi-1])) < 0)
			goto bad;
		if (--numi == 0)
			break;
		/* dentry */
		if ((err == ceph_decode_32(p, end, &info->trace_dname_len[numi])) < 0)
			goto bad;
		info->trace_dname[numi] = *p;
		*p += info->trace_dname_len[numi];
		if (*p > end)
			goto bad;
		/* dir */
		info->trace_dir[numi] = *p;
		*p += sizeof(struct ceph_client_reply_dirfrag) +
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

int parse_reply_info_dir(void **p, void *end, struct reply_info *info)
{
	__u32 num, i = 0;
	int err = -EINVAL;

	info->dir_dir = *p;
	*p += sizeof(*info->dir_dir) + sizeof(__u32)*info->dir_dir->ndist;
	if (*p > end) 
		goto bad;

	if ((err = ceph_decode_32(p, end, &num)) < 0)
		goto bad;
	if (num == 0)
		goto done;

	/* alloc large array */
	info->dir_nr = num;
	info->dir_in = kmalloc(3 * num * sizeof(void*), GFP_KERNEL);
	if (info->dir_in == NULL)
		return -ENOMEM;
	info->dir_dname = (void*)(info->trace_in + num);
	info->dir_dname_len = (void*)(info->trace_in + num*2);

	while (num) {
		/* dentry, inode */
		if ((err == ceph_decode_32(p, end, &info->dir_dname_len[i])) < 0)
			goto bad;
		info->dir_dname[i] = *p;
		*p += info->dir_dname_len[i];
		if (*p > end)
			goto bad;
		if ((err = parse_reply_info_in(p, end, &info->dir_in[i])) < 0)
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


int parse_reply_info(struct ceph_msg *msg, struct reply_info *info)
{
	void *p, *end;
	__u32 len;
	int err = -EINVAL;

	memset(info, 0, sizeof(*info));
	
	/* trace */
	p = msg->front.iov_base + sizeof(struct ceph_client_reply_head);
	end = p + msg->front.iov_len;
	if ((err = ceph_decode_32(&p, end, &len)) < 0)
		goto bad;
	if (len > 0 &&
	    (p + len > end ||
	     (err = parse_reply_info_trace(&p, p+len, info)) < 0))
		goto bad;

	/* dir content */
	if ((err = ceph_decode_32(&p, end, &len)) < 0)
		goto bad;
	if (len > 0 &&
	    (p + len > end ||
	     (err = parse_reply_info_dir(&p, p+len, info)) < 0))
		goto bad;

	return 0;
bad:
	derr(1, "problem parsing reply info %d\n", err);
	return err;
}

void destroy_reply_info(struct reply_info *info)
{
	if (info->trace_in) kfree(info->trace_in);
	if (info->dir_in) kfree(info->dir_in);
}

void ceph_mdsc_handle_forward(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	struct ceph_mds_request *req;
	__u64 tid;
	__u32 next_mds;
	__u32 fwd_seq;
	int err;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;
	
	/* decode */
	if ((err = ceph_decode_64(&p, end, &tid)) != 0)
		goto bad;
	if ((err = ceph_decode_32(&p, end, &next_mds)) != 0)
		goto bad;
	if ((err = ceph_decode_32(&p, end, &fwd_seq)) != 0)
		goto bad;

	/* handle */
	spin_lock(&mdsc->lock);
	req = radix_tree_lookup(&mdsc->request_tree, tid);
	if (req) get_request(req);
	if (!req) {
		spin_unlock(&mdsc->lock);
		return;  /* dup reply? */
	}
	
	/* do we have a session with the dest mds? */
	if (next_mds < mdsc->max_sessions &&
	    mdsc->sessions[next_mds] &&
	    mdsc->sessions[next_mds]->s_state == CEPH_MDS_SESSION_OPEN) {
		/* yes.  adjust mds set */
		if (fwd_seq > req->r_num_fwd) {
			req->r_num_fwd = fwd_seq;
			req->r_resend_mds = next_mds;
			req->r_num_mds = 1;
			req->r_mds[0] = msg->hdr.src.name.num;
		}
		spin_unlock(&mdsc->lock);
	} else {
		/* no, resend. */
		BUG_ON(fwd_seq <= req->r_num_fwd);  /* forward race not possible; mds would drop */

		req->r_num_mds = 0;
		req->r_resend_mds = next_mds;
		spin_unlock(&mdsc->lock);
		complete(&req->r_completion);
	}

	put_request(req);

out:
	return;

bad:
	derr(0, "corrupt forward message\n");
	goto out;
}


void ceph_mdsc_handle_map(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	ceph_epoch_t epoch;
	__u32 maplen;
	int err;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;
	struct ceph_mdsmap *newmap, *oldmap;

	if ((err = ceph_decode_32(&p, end, &epoch)) != 0)
		goto bad;
	if ((err = ceph_decode_32(&p, end, &maplen)) != 0)
		goto bad;

	dout(2, "ceph_mdsc_handle_map epoch %u len %d\n", epoch, (int)maplen);

	/* do we need it? */
	spin_lock(&mdsc->lock);
	if (mdsc->mdsmap && epoch <= mdsc->mdsmap->m_epoch) {
		dout(2, "ceph_mdsc_handle_map epoch %u < our %u\n", 
		     epoch, mdsc->mdsmap->m_epoch);
		spin_unlock(&mdsc->lock);
		goto out;
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
			oldmap = mdsc->mdsmap;
			mdsc->mdsmap = newmap;
			spin_unlock(&mdsc->lock);
			if (oldmap)
				ceph_mdsmap_destroy(oldmap);
		} else {
			spin_unlock(&mdsc->lock);
			dout(2, "ceph_mdsc_handle_map lost decode race?\n");
			ceph_mdsmap_destroy(newmap);
		}
	} else {
		mdsc->mdsmap = newmap;
		spin_unlock(&mdsc->lock);
	}
	complete(&mdsc->map_waiters);

out:
	return;
bad:
	dout(1, "corrupt map\n");
	goto out;
bad2:
	dout(1, "no memory to decode new mdsmap\n");
	goto out;
}
