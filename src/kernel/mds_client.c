
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


static void send_msg_mds(struct ceph_mds_client *mdsc, struct ceph_msg *msg, int mds)
{
	msg->hdr.dst.addr = *ceph_mdsmap_get_addr(mdsc->mdsmap, mds);
	msg->hdr.dst.name.type = CEPH_ENTITY_TYPE_MDS;
	msg->hdr.dst.name.num = mds;
	ceph_msg_send(mdsc->client->msgr, msg, BASE_DELAY_INTERVAL);
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
 * register an in-flight request.
 * fill in tid in msg request header
 */
static struct ceph_mds_request *
register_request(struct ceph_mds_client *mdsc, struct ceph_msg *msg, int mds)
{
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *head = msg->front.iov_base;

	req = kmalloc(sizeof(*req), GFP_KERNEL);
	req->r_tid = head->tid = ++mdsc->last_tid;
	req->r_request = msg;
	req->r_reply = 0;
	req->r_num_mds = 0;
	req->r_attempts = 0;
	req->r_num_fwd = 0;
	req->r_resend_mds = mds;
	atomic_set(&req->r_ref, 2);  /* one for request_tree, one for caller */
	init_completion(&req->r_completion);

	dout(30, "register_request %p tid %lld\n", req, req->r_tid);
	radix_tree_insert(&mdsc->request_tree, req->r_tid, (void*)req);
	ceph_msg_get(msg);  /* grab reference */
	return req;
}

static void unregister_request(struct ceph_mds_client *mdsc, 
			       struct ceph_mds_request *req)
{
	dout(30, "unregister_request %p tid %lld\n", req, req->r_tid);
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
	s->s_state = CEPH_MDS_SESSION_NEW;
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

	msg = ceph_msg_new(CEPH_MSG_CLIENT_SESSION, sizeof(__u32)+sizeof(__u64), 0, 0, 0);
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
		ceph_monc_request_mdsmap(&mdsc->client->monc, mdsc->mdsmap->m_epoch);
		return;
	} 
	
	/* send connect message */
	msg = create_session_msg(CEPH_SESSION_REQUEST_OPEN, session->s_cap_seq);
	if (IS_ERR(msg))
		return;  /* fixme */
	session->s_state = CEPH_MDS_SESSION_OPENING;
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
ceph_mdsc_create_request(struct ceph_mds_client *mdsc, int op, 
			 ceph_ino_t ino1, const char *path1, 
			 ceph_ino_t ino2, const char *path2)
{
	struct ceph_msg *req;
	struct ceph_mds_request_head *head;
	void *p, *end;
	int pathlen;

	pathlen = 2*(sizeof(ino1) + sizeof(__u32));
	if (path1) pathlen += strlen(path1);
	if (path2) pathlen += strlen(path2);

	req = ceph_msg_new(CEPH_MSG_CLIENT_REQUEST, 
			   sizeof(struct ceph_mds_request_head) + pathlen,
			   0, 0, 0);
	if (IS_ERR(req))
		return req;
	memset(req->front.iov_base, 0, req->front.iov_len);
	head = req->front.iov_base;
	p = req->front.iov_base + sizeof(*head);
	end = req->front.iov_base + req->front.iov_len;

	/* encode head */
	head->op = cpu_to_le32(op);
	ceph_encode_inst(&head->client_inst, &mdsc->client->msgr->inst);

	/* encode paths */
	ceph_encode_filepath(&p, end, ino1, path1);
	ceph_encode_filepath(&p, end, ino2, path2);
	dout(10, "create_request op %d -> %p\n", op, req);
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
				   (void**)&first, 0, 1) <= 0)
		return 0;
	dout(10, "oldest tid is %llu\n", first->r_tid);
	return first->r_tid;
}

int ceph_mdsc_do_request(struct ceph_mds_client *mdsc, struct ceph_msg *msg, 
			 struct ceph_mds_reply_info *rinfo, int mds)
{
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;
	struct ceph_mds_session *session;
	struct ceph_msg *reply = 0;
	int err;

	dout(30, "do_request on %p type %d\n", msg, msg->hdr.type);

	spin_lock(&mdsc->lock);
	req = register_request(mdsc, msg, mds);

retry:
	mds = choose_mds(mdsc, req);
	if (mds < 0) {
		/* wait for new mdsmap */
		spin_unlock(&mdsc->lock);
		dout(30, "do_request waiting for new mdsmap\n");
		wait_for_new_map(mdsc);
		spin_lock(&mdsc->lock);
		goto retry;
	}
	dout(30, "do_request chose mds%d\n", mds);

	/* get session */
	session = get_session(mdsc, mds);
	dout(30, "do_request session %p\n", session);

	/* open? */
	if (session->s_state == CEPH_MDS_SESSION_NEW ||
	    session->s_state == CEPH_MDS_SESSION_CLOSING) {
		open_session(mdsc, session, mds);
	}
	if (session->s_state == CEPH_MDS_SESSION_OPENING) {
		/* wait for session to open (or fail, or close) */
		spin_unlock(&mdsc->lock);
		dout(30, "do_request waiting on session %p\n", session);
		wait_for_completion(&session->s_completion);
		dout(30, "do_request done waiting on session %p\n", session);
		put_session(session);
		spin_lock(&mdsc->lock);
		goto retry;
	}
	BUG_ON(session->s_state != CEPH_MDS_SESSION_OPEN);
	put_session(session);

	/* make request? */
	BUG_ON(req->r_num_mds >= 2);
	req->r_mds[req->r_num_mds++] = mds;
	req->r_resend_mds = -1;  /* forget any specific mds hint */
	req->r_attempts++;
	rhead = req->r_request->front.iov_base;
	rhead->retry_attempt = cpu_to_le32(req->r_attempts-1);
	rhead->oldest_client_tid = cpu_to_le64(get_oldest_tid(mdsc));
	send_msg_mds(mdsc, req->r_request, mds);

	/* wait */
	spin_unlock(&mdsc->lock);
	wait_for_completion(&req->r_completion);
	spin_lock(&mdsc->lock);

	/* clean up request, parse reply */
	if (!req->r_reply) 
		goto retry;
	reply = req->r_reply;
	unregister_request(mdsc, req);
	spin_unlock(&mdsc->lock);
	put_request(req);
	if ((err = ceph_mdsc_parse_reply_info(reply, rinfo)) < 0)
		return err;

	dout(30, "do_request done on %p\n", msg);
	return 0;
}

void ceph_mdsc_handle_reply(struct ceph_mds_client *mdsc, struct ceph_msg *msg)
{
	struct ceph_mds_request *req;
	struct ceph_mds_reply_head *head = msg->front.iov_base;
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
	spin_unlock(&mdsc->lock);

	/* FIXME: locking on request? */
	BUG_ON(req->r_reply);
	req->r_reply = msg;
	ceph_msg_get(msg);
	complete(&req->r_completion);
	put_request(req);
	
done:
	return;
}

/*
 * mds reply parsing
 */
int parse_reply_info_in(void **p, void *end, struct ceph_mds_reply_info_in *info)
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

int parse_reply_info_trace(void **p, void *end, struct ceph_mds_reply_info *info)
{
	__u32 numi;
	int err = -EINVAL;

	if ((err = ceph_decode_32(p, end, &numi)) < 0)
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

	if ((err = ceph_decode_32(p, end, &num)) < 0)
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
	info->dir_dname = (void*)(info->dir_in + num);
	info->dir_dname_len = (void*)(info->dir_dname + num);

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


int ceph_mdsc_parse_reply_info(struct ceph_msg *msg, struct ceph_mds_reply_info *info)
{
	void *p, *end;
	__u32 len;
	int err = -EINVAL;

	memset(info, 0, sizeof(*info));
	info->head = msg->front.iov_base;

	/* trace */
	p = msg->front.iov_base + sizeof(struct ceph_mds_reply_head);
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

	info->reply = msg;
	return 0;
bad:
	derr(1, "parse_reply err %d\n", err);
	ceph_msg_put(msg);
	return err;
}

void ceph_mdsc_destroy_reply_info(struct ceph_mds_reply_info *info)
{
	if (info->trace_in) kfree(info->trace_in);
	if (info->dir_in) kfree(info->dir_in);
	ceph_msg_put(info->reply);
	info->reply = 0;
}


/*
 * handle mds notification that our request has been forwarded.
 */
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
	return;

bad:
	derr(0, "problem decoding message, err=%d\n", err);
}



/*
 * kick outstanding requests
 */
void kick_requests(struct ceph_mds_client *mdsc, int m)
{
	struct ceph_mds_request *reqs[10];
	u64 nexttid = 0;
	int i, got;
	
	dout(20, "kick_requests mds%d\n", m);
	while (nexttid < mdsc->last_tid) {
		got = radix_tree_gang_lookup(&mdsc->request_tree, (void**)&reqs,
					     nexttid, 10);
		if (got == 0) break;
		nexttid = reqs[got-1]->r_tid + 1;
		for (i=0; i<got; i++) {
			if ((reqs[i]->r_num_mds >= 1 && reqs[i]->r_mds[0] == m) ||
			    (reqs[i]->r_num_mds >= 2 && reqs[i]->r_mds[1] == m)) {
				dout(10, " kicking req %llu\n", reqs[i]->r_tid);
				complete(&reqs[i]->r_completion);
			}
		}
	}
}

void check_new_map(struct ceph_mds_client *mdsc,
		   struct ceph_mdsmap *newmap,
		   struct ceph_mdsmap *oldmap)
{
	int i;
	int oldstate, newstate;
	struct ceph_mds_session *session;

	dout(20, "check_new_map new %u old %u\n",
	     newmap->m_epoch, oldmap->m_epoch);
	
	for (i=0; i<oldmap->m_max_mds; i++) {
		if (mdsc->sessions[i] == 0)
			continue;
		oldstate = ceph_mdsmap_get_state(oldmap, i);
		newstate = ceph_mdsmap_get_state(newmap, i);
		if (newstate >= oldstate)
			continue;  /* no problem */
		
		dout(20, "check_new_map mds%d state %d -> %d\n",
		     i, oldstate, newstate);

		/* notify messenger */
		ceph_messenger_mark_down(mdsc->client->msgr,
					 &oldmap->m_addr[i]);

		/* kill session */
		session = mdsc->sessions[i];
		if (session->s_state == CEPH_MDS_SESSION_OPENING) 
			complete(&session->s_completion);
		if (session->s_state == CEPH_MDS_SESSION_OPEN)
			kick_requests(mdsc, i);
		unregister_session(mdsc, i);
	}
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
			oldmap = mdsc->mdsmap;
			mdsc->mdsmap = newmap;
			if (oldmap) {
				check_new_map(mdsc, newmap, oldmap);
				ceph_mdsmap_destroy(oldmap);
			}
		} else {
			dout(2, "ceph_mdsc_handle_map lost decode race?\n");
			ceph_mdsmap_destroy(newmap);
			spin_unlock(&mdsc->lock);
			return;
		}
	} else {
		mdsc->mdsmap = newmap;
	}
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
