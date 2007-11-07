
#include "mds_client.h"
#include "mon_client.h"
#include "super.h"
#include "kmsg.h"


static void send_msg_mds(struct ceph_mds_client *mdsc, struct ceph_message *msg, int mds)
{
	struct ceph_entity_inst inst;
	inst.addr = *ceph_mdsmap_get_addr(mdsc->mdsmap, mds);
	inst.name.type = CEPH_ENTITY_TYPE_MDS;
	inst.name.num = mds;
	ceph_kmsgr_send(mdsc->client->msgr, msg, &inst);
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
		ceph_put_msg(req->r_request);
		kfree(req);
	} 
}

static void get_session(struct ceph_mds_session *s)
{
	atomic_inc(&s->s_ref);
}

static void put_session(struct ceph_mds_session *s)
{
	if (atomic_dec_and_test(&s->s_ref)) 
		kfree(s);
}

/*
 * register an in-flight request
 */
static struct ceph_mds_request *
register_request(struct ceph_mds_client *mdsc, struct ceph_message *msg, int mds)
{
	struct ceph_mds_request *req;

	req = kmalloc(sizeof(*req), GFP_KERNEL);

	req->r_request = msg;
	ceph_get_msg(msg);  /* grab reference */
	req->r_reply = 0;
	req->r_num_mds = 0;
	req->r_attempts = 0;
	req->r_num_fwd = 0;
	req->r_resend_mds = mds;
	atomic_set(&req->r_ref, 2);  /* one for request_tree, one for caller */

	req->r_tid = ++mdsc->last_tid;
	radix_tree_insert(&mdsc->request_tree, req->r_tid, (void*)req);

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
	/* register */
	if (mds >= mdsc->max_sessions) {
		/* realloc */
	}
	mdsc->sessions[mds] = kmalloc(sizeof(struct ceph_mds_session), GFP_KERNEL);
	mdsc->sessions[mds]->s_state = 0;
	mdsc->sessions[mds]->s_cap_seq = 0;
	init_completion(&mdsc->sessions[mds]->s_completion);
	atomic_set(&mdsc->sessions[mds]->s_ref, 1);
}

static void open_session(struct ceph_mds_client *mdsc, struct ceph_mds_session *session, int mds)
{
	struct ceph_message *msg;

	/* connect */
	if (ceph_mdsmap_get_state(mdsc->mdsmap, mds) < CEPH_MDS_STATE_ACTIVE) {
		ceph_monc_request_mdsmap(&mdsc->client->mon_client, mdsc->mdsmap->m_epoch); /* race fixme */
		return;
	} 
	
	/* prepare connect message */
	
	/* send */
	send_msg_mds(mdsc, msg, mds);
}



static void wait_for_new_map(struct ceph_mds_client *mdsc)
{
	if (mdsc->last_requested_map < mdsc->mdsmap->m_epoch)
		ceph_monc_request_mdsmap(&mdsc->client->mon_client, mdsc->mdsmap->m_epoch);

	wait_for_completion(&mdsc->map_waiters);
}

/* exported functions */

void ceph_mdsc_init(struct ceph_mds_client *mdsc, struct ceph_client *client)
{
	mdsc->client = client;
	mdsc->mdsmap = 0;  /* none yet */
	mdsc->sessions = 0;
	mdsc->max_sessions = 0;
	mdsc->last_tid = 0;
	INIT_RADIX_TREE(&mdsc->request_tree, GFP_KERNEL);
	init_completion(&mdsc->map_waiters);
}


struct ceph_message *
ceph_mdsc_make_request(struct ceph_mds_client *mdsc, struct ceph_message *msg, int mds)
{
	struct ceph_mds_request *req;
	struct ceph_mds_session *session;
	struct ceph_message *reply = 0;

	spin_lock(&mdsc->lock);
	req = register_request(mdsc, msg, mds);

retry:
	mds = choose_mds(mdsc, req);
	if (mds < 0) {
		/* wait for new mdsmap */
		spin_unlock(&mdsc->lock);
		wait_for_new_map(mdsc);
		spin_lock(&mdsc->lock);
		goto retry;
	}

	/* get session */
	if (mds >= mdsc->max_sessions || mdsc->sessions[mds] == 0) 
		register_session(mdsc, mds);
	session = mdsc->sessions[mds];
	get_session(session);
	
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
	spin_unlock(&mdsc->lock);
	wait_for_completion(&req->r_completion);

	if (!req->r_reply) {
		spin_lock(&mdsc->lock);
		goto retry;
	}
	reply = req->r_reply;

	spin_lock(&mdsc->lock);
	unregister_request(mdsc, req);
	spin_unlock(&mdsc->lock);

	put_request(req);

	return reply;
}


void ceph_mdsc_handle_reply(struct ceph_mds_client *mdsc, struct ceph_message *msg)
{
	struct ceph_mds_request *req;
	__u64 tid;

	/* parse reply */

	spin_lock(&mdsc->lock);
	req = radix_tree_lookup(&mdsc->request_tree, tid);
	if (!req) {
		spin_unlock(&mdsc->lock);
		return;
	}

	get_request(req);
	BUG_ON(req->r_reply);
	req->r_reply = msg;
	spin_unlock(&mdsc->lock);

	complete(&req->r_completion);
	put_request(req);
}

void ceph_mdsc_handle_forward(struct ceph_mds_client *mdsc, struct ceph_message *msg)
{
	struct ceph_mds_request *req;
	int next_mds;
	int fwd_seq;
	__u64 tid;

	/* parse reply */
	

	
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
	ceph_put_msg(msg);
}


void ceph_mdsc_handle_map(struct ceph_mds_client *mdsc, 
			  struct ceph_message *msg)
{
	struct ceph_bufferlist_iterator bli;
	__u64 epoch;
	__u32 left;

	ceph_bl_iterator_init(&bli);
	epoch = ceph_bl_decode_u64(&msg->payload, &bli);
	left = ceph_bl_decode_u32(&msg->payload, &bli);

	dout(2, "ceph_mdsc_handle_map epoch %llu\n", epoch);

	spin_lock(&mdsc->lock);
	if (epoch > mdsc->mdsmap->m_epoch) {
		ceph_mdsmap_decode(mdsc->mdsmap, &msg->payload, &bli);
		spin_unlock(&mdsc->lock);
		complete(&mdsc->map_waiters);
	} else {
		spin_unlock(&mdsc->lock);
	}

	ceph_put_msg(msg);
}
