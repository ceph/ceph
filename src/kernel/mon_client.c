
#include <linux/types.h>
#include <linux/random.h>
#include <linux/sched.h>
#include "mon_client.h"

#include "ceph_debug.h"
#include "super.h"
#include "decode.h"

/*
 * Interact with Ceph monitor cluster.  Handle requests for new map
 * versions, and periodically resend as needed.  Also implement
 * statfs() and umount().
 *
 * A small cluster of Ceph "monitors" are responsible for managing critical
 * cluster configuration and state information.  An odd number (e.g., 3, 5)
 * of cmon daemons use a modified version of the Paxos part-time parliament
 * algorithm to manage the MDS map (mds cluster membership), OSD map, and
 * list of clients who have mounted the file system.
 *
 * Communication with the monitor cluster is lossy, so requests for
 * information may have to be resent if we time out waiting for a response.
 * As long as we do not time out, we continue to send all requests to the
 * same monitor.  If there is a problem, we randomly pick a new monitor from
 * the cluster to try.
 */

/*
 * Decode a monmap blob (e.g., during mount).
 */
struct ceph_monmap *ceph_monmap_decode(void *p, void *end)
{
	struct ceph_monmap *m = 0;
	int i, err = -EINVAL;
	ceph_fsid_t fsid;
	u32 epoch, num_mon;
	u16 version;

	dout("monmap_decode %p %p len %d\n", p, end, (int)(end-p));

	ceph_decode_16_safe(&p, end, version, bad);

	ceph_decode_need(&p, end, sizeof(fsid) + 2*sizeof(u32), bad);
	ceph_decode_copy(&p, &fsid, sizeof(fsid));
	ceph_decode_32(&p, epoch);

	ceph_decode_32(&p, num_mon);
	ceph_decode_need(&p, end, num_mon*sizeof(m->mon_inst[0]), bad);

	if (num_mon >= CEPH_MAX_MON)
		goto bad;
	m = kmalloc(sizeof(*m) + sizeof(m->mon_inst[0])*num_mon, GFP_NOFS);
	if (m == NULL)
		return ERR_PTR(-ENOMEM);
	m->fsid = fsid;
	m->epoch = epoch;
	m->num_mon = num_mon;
	ceph_decode_copy(&p, m->mon_inst, num_mon*sizeof(m->mon_inst[0]));

	if (p != end)
		goto bad;

	dout("monmap_decode epoch %d, num_mon %d\n", m->epoch,
	     m->num_mon);
	for (i = 0; i < m->num_mon; i++)
		dout("monmap_decode  mon%d is %u.%u.%u.%u:%u\n", i,
		     IPQUADPORT(m->mon_inst[i].addr.ipaddr));
	return m;

bad:
	dout("monmap_decode failed with %d\n", err);
	kfree(m);
	return ERR_PTR(err);
}

/*
 * return true if *addr is included in the monmap.
 */
int ceph_monmap_contains(struct ceph_monmap *m, struct ceph_entity_addr *addr)
{
	int i;

	for (i = 0; i < m->num_mon; i++)
		if (ceph_entity_addr_equal(addr, &m->mon_inst[i].addr))
			return 1;
	return 0;
}

/*
 * Open a session with a (new) monitor.
 */
static int open_session(struct ceph_mon_client *monc, int newmon)
{
	char r;

	if (!newmon && monc->con) {
		dout("open_session mon%d already open\n", monc->last_mon);
		return 0;
	}

	if (monc->con) {
		dout("open_session closing mon%d\n", monc->last_mon);
		monc->con->put(monc->con);
	}

	get_random_bytes(&r, 1);
	monc->last_mon = r % monc->monmap->num_mon;

	monc->con = kzalloc(sizeof(*monc->con), GFP_NOFS);
	if (!monc->con) {
		pr_err("open_session mon%d ENOMEM\n", monc->last_mon);
		return -ENOMEM;
	}

	dout("open_session mon%d opened\n", monc->last_mon);
	ceph_con_init(monc->client->msgr, monc->con,
		      &monc->monmap->mon_inst[monc->last_mon].addr);
	monc->con->peer_name.type = cpu_to_le32(CEPH_ENTITY_TYPE_MON);
	monc->con->peer_name.num = cpu_to_le32(monc->last_mon);
	return 0;
}

/*
 * Generic timeout mechanism for monitor requests, so we can resend if
 * we don't get a timely reply.  Exponential backoff.
 */
static void reschedule_timeout(struct ceph_mon_request *req)
{
	schedule_delayed_work(&req->delayed_work, req->delay);
	if (req->delay < MAX_DELAY_INTERVAL)
		req->delay *= 2;
	else
		req->delay = MAX_DELAY_INTERVAL;
}

static void retry_request(struct work_struct *work)
{
	struct ceph_mon_request *req =
		container_of(work, struct ceph_mon_request,
			     delayed_work.work);

	/*
	 * if lock is contended, reschedule sooner.  we can't wait for
	 * mutex because we cancel the timeout sync with lock held.
	 */
	if (mutex_trylock(&req->monc->req_mutex)) {
		req->do_request(req->monc, 1);
		reschedule_timeout(req);
		mutex_unlock(&req->monc->req_mutex);
	} else
		schedule_delayed_work(&req->delayed_work, BASE_DELAY_INTERVAL);
}

static void cancel_timeout(struct ceph_mon_request *req)
{
	cancel_delayed_work_sync(&req->delayed_work);
	req->delay = BASE_DELAY_INTERVAL;
}

static void init_request_type(struct ceph_mon_client *monc,
			      struct ceph_mon_request *req,
			      ceph_monc_request_func_t func)
{
	req->monc = monc;
	INIT_DELAYED_WORK(&req->delayed_work, retry_request);
	req->delay = 0;
	req->do_request = func;
}


/*
 * Request a new mds map.
 */
static void request_mdsmap(struct ceph_mon_client *monc, int newmon)
{
	struct ceph_msg *msg;
	struct ceph_mds_getmap *h;
	int err;

	dout("request_mdsmap want %u\n", monc->want_mdsmap);
	err = open_session(monc, newmon);
	if (err)
		return;
	msg = ceph_msg_new(CEPH_MSG_MDS_GETMAP, sizeof(*h), 0, 0, NULL);
	if (IS_ERR(msg))
		return;
	h = msg->front.iov_base;
	h->fsid = monc->monmap->fsid;
	h->have_version = cpu_to_le64(monc->want_mdsmap - 1);
	ceph_con_send(monc->con, msg);
}

/*
 * Register our desire for an mdsmap epoch >= @want.
 */
void ceph_monc_request_mdsmap(struct ceph_mon_client *monc, u32 want)
{
	dout("request_mdsmap want %u\n", want);
	mutex_lock(&monc->req_mutex);
	if (want > monc->want_mdsmap) {
		monc->want_mdsmap = want;
		monc->mdsreq.delay = BASE_DELAY_INTERVAL;
		request_mdsmap(monc, 0);
		reschedule_timeout(&monc->mdsreq);
	}
	mutex_unlock(&monc->req_mutex);
}

/*
 * Possibly cancel our desire for a new map
 */
int ceph_monc_got_mdsmap(struct ceph_mon_client *monc, u32 got)
{
	int ret = 0;

	mutex_lock(&monc->req_mutex);
	if (got < monc->want_mdsmap) {
		dout("got_mdsmap %u < wanted %u\n", got, monc->want_mdsmap);
		ret = -EAGAIN;
	} else {
		dout("got_mdsmap %u >= wanted %u\n", got, monc->want_mdsmap);
		monc->want_mdsmap = 0;
		cancel_timeout(&monc->mdsreq);
	}
	mutex_unlock(&monc->req_mutex);
	return ret;
}


/*
 * osd map
 */
static void request_osdmap(struct ceph_mon_client *monc, int newmon)
{
	struct ceph_msg *msg;
	struct ceph_osd_getmap *h;
	int err;

	dout("request_osdmap want %u\n", monc->want_osdmap);
	err = open_session(monc, newmon);
	if (err)
		return;
	msg = ceph_msg_new(CEPH_MSG_OSD_GETMAP, sizeof(*h), 0, 0, NULL);
	if (IS_ERR(msg))
		return;
	h = msg->front.iov_base;
	h->fsid = monc->monmap->fsid;
	h->start = cpu_to_le32(monc->want_osdmap);
	h->have_version = cpu_to_le64(monc->want_osdmap ?
				      monc->want_osdmap-1 : 0);
	ceph_con_send(monc->con, msg);
}

void ceph_monc_request_osdmap(struct ceph_mon_client *monc, u32 want)
{
	dout("request_osdmap want %u\n", want);
	mutex_lock(&monc->req_mutex);
	monc->osdreq.delay = BASE_DELAY_INTERVAL;
	monc->want_osdmap = want;
	request_osdmap(monc, 0);
	reschedule_timeout(&monc->osdreq);
	mutex_unlock(&monc->req_mutex);
}

int ceph_monc_got_osdmap(struct ceph_mon_client *monc, u32 got)
{
	int ret = 0;

	mutex_lock(&monc->req_mutex);
	if (got < monc->want_osdmap) {
		dout("got_osdmap %u < wanted %u\n", got, monc->want_osdmap);
		ret = -EAGAIN;
	} else {
		dout("got_osdmap %u >= wanted %u\n", got, monc->want_osdmap);
		monc->want_osdmap = 0;
		cancel_timeout(&monc->osdreq);
	}
	mutex_unlock(&monc->req_mutex);
	return ret;
}


/*
 * mount
 */
static void request_mount(struct ceph_mon_client *monc, int newmon)
{
	struct ceph_msg *msg;
	struct ceph_client_mount *h;
	int err;

	dout("request_mount\n");
	err = open_session(monc, newmon);
	if (err)
		return;
	msg = ceph_msg_new(CEPH_MSG_CLIENT_MOUNT, sizeof(*h), 0, 0, NULL);
	if (IS_ERR(msg))
		return;
	h = msg->front.iov_base;
	h->have_version = 0;
	ceph_con_send(monc->con, msg);
}

void ceph_monc_request_mount(struct ceph_mon_client *monc)
{
	mutex_lock(&monc->req_mutex);
	monc->mountreq.delay = BASE_DELAY_INTERVAL;
	request_mount(monc, 0);
	reschedule_timeout(&monc->mountreq);
	mutex_unlock(&monc->req_mutex);
}

/*
 * The monitor responds with mount ack indicate mount success.  The
 * included client ticket allows the client to talk to MDSs and OSDs.
 */
void ceph_handle_mount_ack(struct ceph_client *client, struct ceph_msg *msg)
{
	struct ceph_mon_client *monc = &client->monc;
	struct ceph_monmap *monmap = NULL, *old = client->monc.monmap;
	void *p, *end;
	s32 result;
	u32 len;
	int err = -EINVAL;

	if (client->signed_ticket) {
		dout("handle_mount_ack - already mounted\n");
		return;
	}

	dout("handle_mount_ack\n");
	p = msg->front.iov_base;
	end = p + msg->front.iov_len;

	ceph_decode_32_safe(&p, end, result, bad);
	ceph_decode_32_safe(&p, end, len, bad);
	if (result) {
		pr_err("ceph mount denied: %.*s (%d)\n", len, (char *)p,
		       result);
		err = result;
		goto out;
	}
	p += len;

	ceph_decode_32_safe(&p, end, len, bad);
	ceph_decode_need(&p, end, len, bad);
	monmap = ceph_monmap_decode(p, p + len);
	if (IS_ERR(monmap)) {
		pr_err("ceph problem decoding monmap, %d\n",
		       (int)PTR_ERR(monmap));
		err = -EINVAL;
		goto out;
	}
	p += len;

	ceph_decode_32_safe(&p, end, len, bad);
	dout("ticket len %d\n", len);
	ceph_decode_need(&p, end, len, bad);

	client->signed_ticket = kmalloc(len, GFP_KERNEL);
	if (!client->signed_ticket) {
		pr_err("ceph ENOMEM allocating %d bytes for client ticket\n",
		       len);
		err = -ENOMEM;
		goto out_free;
	}

	memcpy(client->signed_ticket, p, len);
	client->signed_ticket_len = len;

	client->monc.monmap = monmap;
	kfree(old);

	client->whoami = le32_to_cpu(msg->hdr.dst.name.num);
	client->msgr->inst.name = msg->hdr.dst.name;
	pr_info("ceph mount as client%d fsid is %llx.%llx\n", client->whoami,
		le64_to_cpu(__ceph_fsid_major(&client->monc.monmap->fsid)),
		le64_to_cpu(__ceph_fsid_minor(&client->monc.monmap->fsid)));
	ceph_debugfs_client_init(client);

	err = 0;
	goto out;

bad:
	pr_err("ceph error decoding mount_ack message\n");
out_free:
	kfree(monmap);
out:
	client->mount_err = err;
	mutex_lock(&monc->req_mutex);
	cancel_timeout(&monc->mountreq);
	mutex_unlock(&monc->req_mutex);
	wake_up(&client->mount_wq);
}



/*
 * umount
 */
static void request_umount(struct ceph_mon_client *monc, int newmon)
{
	struct ceph_msg *msg;
	struct ceph_client_mount *h;
	int err;

	dout("request_umount\n");
	err = open_session(monc, newmon);
	if (err)
		return;
	msg = ceph_msg_new(CEPH_MSG_CLIENT_UNMOUNT, sizeof(*h), 0, 0, NULL);
	if (IS_ERR(msg))
		return;
	h = msg->front.iov_base;
	h->have_version = 0;
	ceph_con_send(monc->con, msg);
}

void ceph_monc_request_umount(struct ceph_mon_client *monc)
{
	struct ceph_client *client = monc->client;

	/* don't bother if forced unmount */
	if (client->mount_state == CEPH_MOUNT_SHUTDOWN)
		return;

	mutex_lock(&monc->req_mutex);
	monc->umountreq.delay = BASE_DELAY_INTERVAL;
	request_umount(monc, 0);
	reschedule_timeout(&monc->umountreq);
	mutex_unlock(&monc->req_mutex);
}

void ceph_monc_handle_umount(struct ceph_mon_client *monc,
			     struct ceph_msg *msg)
{
	dout("handle_umount\n");
	mutex_lock(&monc->req_mutex);
	cancel_timeout(&monc->umountreq);
	monc->client->mount_state = CEPH_MOUNT_UNMOUNTED;
	mutex_unlock(&monc->req_mutex);
	wake_up(&monc->client->mount_wq);
}


/*
 * statfs
 */
void ceph_monc_handle_statfs_reply(struct ceph_mon_client *monc,
				   struct ceph_msg *msg)
{
	struct ceph_mon_statfs_request *req;
	struct ceph_mon_statfs_reply *reply = msg->front.iov_base;
	u64 tid;

	if (msg->front.iov_len != sizeof(*reply))
		goto bad;
	tid = le64_to_cpu(reply->tid);
	dout("handle_statfs_reply %p tid %llu\n", msg, tid);

	mutex_lock(&monc->statfs_mutex);
	req = radix_tree_lookup(&monc->statfs_request_tree, tid);
	if (req) {
		*req->buf = reply->st;
		req->result = 0;
	}
	mutex_unlock(&monc->statfs_mutex);
	if (req)
		complete(&req->completion);
	return;

bad:
	pr_err("ceph corrupt statfs reply, no tid\n");
}

/*
 * (re)send a statfs request
 */
static int send_statfs(struct ceph_mon_client *monc,
		       struct ceph_mon_statfs_request *req,
		       int newmon)
{
	struct ceph_msg *msg;
	struct ceph_mon_statfs *h;
	int err;

	dout("send_statfs tid %llu\n", req->tid);
	err = open_session(monc, newmon);
	if (err)
		return err;
	msg = ceph_msg_new(CEPH_MSG_STATFS, sizeof(*h), 0, 0, NULL);
	if (IS_ERR(msg))
		return PTR_ERR(msg);
	req->request = msg;
	h = msg->front.iov_base;
	h->have_version = 0;
	h->fsid = monc->monmap->fsid;
	h->tid = cpu_to_le64(req->tid);
	ceph_con_send(monc->con, msg);
	return 0;
}

/*
 * Do a synchronous statfs().
 */
int ceph_monc_do_statfs(struct ceph_mon_client *monc, struct ceph_statfs *buf)
{
	struct ceph_mon_statfs_request req;
	int err;

	req.buf = buf;
	init_completion(&req.completion);

	/* register request */
	mutex_lock(&monc->statfs_mutex);
	req.tid = ++monc->last_tid;
	req.last_attempt = jiffies;
	req.delay = BASE_DELAY_INTERVAL;
	if (radix_tree_insert(&monc->statfs_request_tree, req.tid, &req) < 0) {
		mutex_unlock(&monc->statfs_mutex);
		pr_err("ceph ENOMEM in do_statfs\n");
		return -ENOMEM;
	}
	if (monc->num_statfs_requests == 0)
		schedule_delayed_work(&monc->statfs_delayed_work,
				      round_jiffies_relative(1*HZ));
	monc->num_statfs_requests++;
	ceph_msgpool_resv(&monc->client->msgpool_statfs_reply, 1);
	mutex_unlock(&monc->statfs_mutex);

	/* send request and wait */
	err = send_statfs(monc, &req, 0);
	if (!err)
		err = wait_for_completion_interruptible(&req.completion);

	mutex_lock(&monc->statfs_mutex);
	radix_tree_delete(&monc->statfs_request_tree, req.tid);
	monc->num_statfs_requests--;
	if (monc->num_statfs_requests == 0)
		cancel_delayed_work(&monc->statfs_delayed_work);
	ceph_msgpool_resv(&monc->client->msgpool_statfs_reply, -1);
	mutex_unlock(&monc->statfs_mutex);

	if (!err)
		err = req.result;
	return err;
}

/*
 * Resend any statfs requests that have timed out.
 */
static void do_statfs_check(struct work_struct *work)
{
	struct ceph_mon_client *monc =
		container_of(work, struct ceph_mon_client,
			     statfs_delayed_work.work);
	u64 next_tid = 0;
	int got;
	int did = 0;
	int newmon = 1;
	struct ceph_mon_statfs_request *req;

	dout("do_statfs_check\n");
	mutex_lock(&monc->statfs_mutex);
	while (1) {
		got = radix_tree_gang_lookup(&monc->statfs_request_tree,
					     (void **)&req,
					     next_tid, 1);
		if (got == 0)
			break;
		did++;
		next_tid = req->tid + 1;
		if (time_after(jiffies, req->last_attempt + req->delay)) {
			req->last_attempt = jiffies;
			if (req->delay < MAX_DELAY_INTERVAL)
				req->delay *= 2; /* exponential backoff */
			send_statfs(monc, req, newmon);
			newmon = 0;
		}
	}
	mutex_unlock(&monc->statfs_mutex);

	if (did)
		schedule_delayed_work(&monc->statfs_delayed_work,
				      round_jiffies_relative(1*HZ));
}


int ceph_monc_init(struct ceph_mon_client *monc, struct ceph_client *cl)
{
	dout("init\n");
	memset(monc, 0, sizeof(*monc));
	monc->client = cl;
	monc->monmap = NULL;
	mutex_init(&monc->statfs_mutex);
	INIT_RADIX_TREE(&monc->statfs_request_tree, GFP_NOFS);
	monc->num_statfs_requests = 0;
	monc->last_tid = 0;
	INIT_DELAYED_WORK(&monc->statfs_delayed_work, do_statfs_check);
	init_request_type(monc, &monc->mdsreq, request_mdsmap);
	init_request_type(monc, &monc->osdreq, request_osdmap);
	init_request_type(monc, &monc->mountreq, request_mount);
	init_request_type(monc, &monc->umountreq, request_umount);
	mutex_init(&monc->req_mutex);
	monc->want_mdsmap = 0;
	monc->want_osdmap = 0;
	monc->con = NULL;
	return 0;
}

void ceph_monc_stop(struct ceph_mon_client *monc)
{
	dout("stop\n");
	cancel_timeout(&monc->mdsreq);
	cancel_timeout(&monc->osdreq);
	cancel_timeout(&monc->mountreq);
	cancel_timeout(&monc->umountreq);
	cancel_delayed_work_sync(&monc->statfs_delayed_work);
	if (monc->con) {
		monc->con->put(monc->con);
		monc->con = NULL;
	}
	kfree(monc->monmap);
}
