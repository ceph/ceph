
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

const static struct ceph_connection_operations mon_con_ops;

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
static int __open_session(struct ceph_mon_client *monc, int newmon)
{
	char r;

	if (!monc->con || newmon) {
		if (monc->con) {
			dout("open_session closing mon%d\n", monc->cur_mon);
			monc->con->ops->put(monc->con);
		}

		get_random_bytes(&r, 1);
		monc->cur_mon = r % monc->monmap->num_mon;
		monc->subscribed = false;
		monc->sub_sent = 0;

		monc->con = kzalloc(sizeof(*monc->con), GFP_NOFS);
		if (!monc->con) {
			pr_err("open_session mon%d ENOMEM\n", monc->cur_mon);
			return -ENOMEM;
		}

		dout("open_session mon%d opened\n", monc->cur_mon);
		ceph_con_init(monc->client->msgr, monc->con,
			      &monc->monmap->mon_inst[monc->cur_mon].addr);
		monc->con->private = monc;
		monc->con->ops = &mon_con_ops;
		monc->con->peer_name.type = cpu_to_le32(CEPH_ENTITY_TYPE_MON);
		monc->con->peer_name.num = cpu_to_le32(monc->cur_mon);
	} else {
		dout("open_session mon%d already open\n", monc->cur_mon);
	}
	return 0;
}

/*
 * Reschedule delayed work timer.
 */
static void __schedule_delayed(struct ceph_mon_client *monc)
{
	unsigned delay;

	if (monc->want_mount)
		delay = 10 * HZ;
	else
		delay = 20 * HZ;
	dout("__schedule_delayed after %u\n", delay);
	schedule_delayed_work(&monc->delayed_work, delay);
}

/*
 * Send subscribe request for mdsmap and/or osdmap.
 */
static void __send_subscribe(struct ceph_mon_client *monc)
{
	if ((!monc->subscribed && !monc->sub_sent) ||
	    time_before(jiffies, monc->sub_renew_after) ||
	    !monc->want_next_osdmap) {
		struct ceph_msg *msg;
		struct ceph_mon_subscribe_item *i;
		void *p, *end;

		msg = ceph_msg_new(CEPH_MSG_MON_SUBSCRIBE, 64, 0, 0, 0);
		if (!msg)
			return;

		dout("open_session subscribing to 'mdsmap' at %u\n",
		     (unsigned)monc->have_mdsmap);
		p = msg->front.iov_base;
		end = p + msg->front.iov_len;

		ceph_encode_32(&p, 1 + monc->want_next_osdmap);
		ceph_encode_string(&p, end, "mdsmap", 6);
		i = p;
		i->have = cpu_to_le64(monc->have_mdsmap);
		i->onetime = 0;
		p += sizeof(*i);
		if (monc->want_next_osdmap) {
			ceph_encode_string(&p, end, "osdmap", 6);
			i = p;
			i->have = cpu_to_le64(monc->have_osdmap);
			i->onetime = 1;
			p += sizeof(*i);
		}

		msg->front.iov_len = p - msg->front.iov_base;
		msg->hdr.front_len = cpu_to_le32(msg->front.iov_len);
		ceph_con_send(monc->con, msg);

		monc->sub_sent = jiffies;
	}
}

static void handle_subscribe_ack(struct ceph_mon_client *monc,
				 struct ceph_msg *msg)
{
	unsigned seconds;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;

	dout("handle_subscribe_ack\n");
	ceph_decode_64_safe(&p, end, seconds, bad);
	monc->subscribed = true;
	monc->sub_renew_after = monc->sub_sent + seconds*HZ;
	
bad:
	pr_err("ceph got corrupt subscribe-ack msg\n");
}

/*
 * Keep track of which maps we have
 */
int ceph_monc_got_mdsmap(struct ceph_mon_client *monc, u32 got)
{
	mutex_lock(&monc->mutex);
	monc->have_mdsmap = got;
	mutex_unlock(&monc->mutex);
	return 0;
}

int ceph_monc_got_osdmap(struct ceph_mon_client *monc, u32 got)
{
	mutex_lock(&monc->mutex);
	monc->have_osdmap = got;
	monc->want_next_osdmap = false;
	mutex_unlock(&monc->mutex);
	return 0;
}

/*
 * Register interest in the next osdmap
 */
void ceph_monc_request_next_osdmap(struct ceph_mon_client *monc)
{
	dout("request_next_osdmap have %u\n", monc->have_osdmap);
	mutex_lock(&monc->mutex);
	monc->want_next_osdmap = true;
	__send_subscribe(monc);
	mutex_unlock(&monc->mutex);
}


/*
 * mount
 */
static void __request_mount(struct ceph_mon_client *monc)
{
	struct ceph_msg *msg;
	struct ceph_client_mount *h;
	int err;

	dout("__request_mount\n");
	err = __open_session(monc, 1);
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
	mutex_lock(&monc->mutex);
	__request_mount(monc);
	__schedule_delayed(monc);
	mutex_unlock(&monc->mutex);
}

/*
 * The monitor responds with mount ack indicate mount success.  The
 * included client ticket allows the client to talk to MDSs and OSDs.
 */
static void handle_mount_ack(struct ceph_mon_client *monc, struct ceph_msg *msg)
{
	struct ceph_client *client = monc->client;
	struct ceph_monmap *monmap = NULL, *old = monc->monmap;
	void *p, *end;
	s32 result;
	u32 len;
	s64 cnum;
	struct ceph_entity_addr addr;
	int err = -EINVAL;

	if (client->whoami >= 0) {
		dout("handle_mount_ack - already mounted\n");
		return;
	}

	mutex_lock(&monc->mutex);

	dout("handle_mount_ack\n");
	p = msg->front.iov_base;
	end = p + msg->front.iov_len;

	ceph_decode_64_safe(&p, end, cnum, bad);
	ceph_decode_need(&p, end, sizeof(addr), bad);
	ceph_decode_copy(&p, &addr, sizeof(addr));
	
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

	client->monc.monmap = monmap;
	kfree(old);

	client->signed_ticket = NULL;
	client->signed_ticket_len = 0;

	monc->want_mount = false;

	client->whoami = cnum;
	client->msgr->inst.name.num = cpu_to_le32(cnum);
	client->msgr->inst.name.type = CEPH_ENTITY_TYPE_CLIENT;
	memcpy(&client->msgr->inst.addr, &addr, sizeof(addr));
	pr_info("ceph client%d %u.%u.%u.%u:%u fsid %llx.%llx\n", client->whoami,
		IPQUADPORT(addr.ipaddr),
		le64_to_cpu(__ceph_fsid_major(&client->monc.monmap->fsid)),
		le64_to_cpu(__ceph_fsid_minor(&client->monc.monmap->fsid)));

	ceph_debugfs_client_init(client);
	__send_subscribe(monc);

	err = 0;
	goto out;

bad:
	pr_err("ceph error decoding mount_ack message\n");
out:
	client->mount_err = err;
	mutex_unlock(&monc->mutex);
	wake_up(&client->mount_wq);
}




/*
 * statfs
 */
static void handle_statfs_reply(struct ceph_mon_client *monc,
				struct ceph_msg *msg)
{
	struct ceph_mon_statfs_request *req;
	struct ceph_mon_statfs_reply *reply = msg->front.iov_base;
	u64 tid;

	if (msg->front.iov_len != sizeof(*reply))
		goto bad;
	tid = le64_to_cpu(reply->tid);
	dout("handle_statfs_reply %p tid %llu\n", msg, tid);

	mutex_lock(&monc->mutex);
	req = radix_tree_lookup(&monc->statfs_request_tree, tid);
	if (req) {
		*req->buf = reply->st;
		req->result = 0;
	}
	mutex_unlock(&monc->mutex);
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
		       struct ceph_mon_statfs_request *req)
{
	struct ceph_msg *msg;
	struct ceph_mon_statfs *h;
	int err;

	dout("send_statfs tid %llu\n", req->tid);
	err = __open_session(monc, 0);
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
	mutex_lock(&monc->mutex);
	req.tid = ++monc->last_tid;
	req.last_attempt = jiffies;
	req.delay = BASE_DELAY_INTERVAL;
	if (radix_tree_insert(&monc->statfs_request_tree, req.tid, &req) < 0) {
		mutex_unlock(&monc->mutex);
		pr_err("ceph ENOMEM in do_statfs\n");
		return -ENOMEM;
	}
	monc->num_statfs_requests++;
	ceph_msgpool_resv(&monc->client->msgpool_statfs_reply, 1);
	mutex_unlock(&monc->mutex);

	/* send request and wait */
	err = send_statfs(monc, &req);
	if (!err)
		err = wait_for_completion_interruptible(&req.completion);

	mutex_lock(&monc->mutex);
	radix_tree_delete(&monc->statfs_request_tree, req.tid);
	monc->num_statfs_requests--;
	ceph_msgpool_resv(&monc->client->msgpool_statfs_reply, -1);
	mutex_unlock(&monc->mutex);

	if (!err)
		err = req.result;
	return err;
}

/*
 * Resend pending statfs requests.
 */
static void resend_statfs(struct ceph_mon_client *monc)
{
	u64 next_tid = 0;
	int got;
	int did = 0;
	struct ceph_mon_statfs_request *req;

	mutex_lock(&monc->mutex);
	while (1) {
		got = radix_tree_gang_lookup(&monc->statfs_request_tree,
					     (void **)&req,
					     next_tid, 1);
		if (got == 0)
			break;
		did++;
		next_tid = req->tid + 1;

		send_statfs(monc, req);
	}
	mutex_unlock(&monc->mutex);
}

/*
 * Delayed work.  If we haven't mounted yet, retry.  Otherwise,
 * renew/retry subscription as needed (in case it is timing out, or we
 * got an ENOMEM).  And keep the monitor connection alive.
 */
static void delayed_work(struct work_struct *work)
{
	struct ceph_mon_client *monc =
		container_of(work, struct ceph_mon_client, delayed_work.work);

	dout("monc delayed_work\n");
	mutex_lock(&monc->mutex);
	if (monc->want_mount) {
		__request_mount(monc);
	} else {
		ceph_con_keepalive(monc->con);
		__send_subscribe(monc);
	}
	__schedule_delayed(monc);
	mutex_unlock(&monc->mutex);
}

int ceph_monc_init(struct ceph_mon_client *monc, struct ceph_client *cl)
{
	dout("init\n");
	memset(monc, 0, sizeof(*monc));
	monc->client = cl;
	monc->monmap = NULL;
	mutex_init(&monc->mutex);
	monc->con = NULL;

	INIT_DELAYED_WORK(&monc->delayed_work, delayed_work);
	INIT_RADIX_TREE(&monc->statfs_request_tree, GFP_NOFS);
	monc->num_statfs_requests = 0;
	monc->last_tid = 0;

	monc->have_mdsmap = 0;
	monc->have_osdmap = 0;
	monc->want_next_osdmap = true;
	monc->want_mount = true;
	return 0;
}

void ceph_monc_stop(struct ceph_mon_client *monc)
{
	dout("stop\n");
	cancel_delayed_work_sync(&monc->delayed_work);
	if (monc->con) {
		monc->con->ops->put(monc->con);
		monc->con = NULL;
	}
	kfree(monc->monmap);
}


/*
 * handle incoming message
 */
static void dispatch(struct ceph_connection *con, struct ceph_msg *msg)
{
	struct ceph_mon_client *monc = con->private;
	int type = le16_to_cpu(msg->hdr.type);

	switch (type) {
	case CEPH_MSG_CLIENT_MOUNT_ACK:
		handle_mount_ack(monc, msg);
		break;

	case CEPH_MSG_MON_SUBSCRIBE_ACK:
		handle_subscribe_ack(monc, msg);
		break;

	case CEPH_MSG_STATFS_REPLY:
		handle_statfs_reply(monc, msg);
		break;

	case CEPH_MSG_MDS_MAP:
		ceph_mdsc_handle_map(&monc->client->mdsc, msg);
		break;

	case CEPH_MSG_OSD_MAP:
		ceph_osdc_handle_map(&monc->client->osdc, msg);
		break;

	default:
		pr_err("ceph received unknown message type %d %s\n", type,
		       ceph_msg_type_name(type));
	}
	ceph_msg_put(msg);
}

/*
 * If the monitor connection resets, pick a new monitor and resubmit
 * any pending requests.
 */
static void mon_reset(struct ceph_connection *con)
{
	struct ceph_mon_client *monc = con->private;

	dout("mon_reset\n");
	if (__open_session(monc, 1) < 0)
		return;   /* delayed work handler will retry */

	__send_subscribe(monc);
	resend_statfs(monc);
}


const static struct ceph_connection_operations mon_con_ops = {
	.get = ceph_con_get,
	.put = ceph_con_put,
	.dispatch = dispatch,
	.peer_reset = mon_reset,
	.alloc_msg = ceph_alloc_msg,
	.alloc_middle = ceph_alloc_middle,
};
