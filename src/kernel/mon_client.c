
#include <linux/types.h>
#include <linux/random.h>
#include <linux/sched.h>
#include "mon_client.h"

#include "ceph_debug.h"

int ceph_debug_mon __read_mostly = -1;
#define DOUT_MASK DOUT_MASK_MON
#define DOUT_VAR ceph_debug_mon
#include "super.h"
#include "decode.h"

/*
 * Decode a monmap blob (e.g., during mount).
 */
struct ceph_monmap *ceph_monmap_decode(void *p, void *end)
{
	struct ceph_monmap *m;
	int i, err = -EINVAL;
	ceph_fsid_t fsid;

	dout(30, "monmap_decode %p %p len %d\n", p, end, (int)(end-p));

	/* The encoded and decoded sizes match. */
	m = kmalloc(end-p, GFP_NOFS);
	if (m == NULL)
		return ERR_PTR(-ENOMEM);

	ceph_decode_need(&p, end, 2*sizeof(u32) + 2*sizeof(u64), bad);
	ceph_decode_copy(&p, &m->fsid, sizeof(fsid));
	ceph_decode_32(&p, m->epoch);
	ceph_decode_32(&p, m->num_mon);
	ceph_decode_need(&p, end, m->num_mon*sizeof(m->mon_inst[0]), bad);
	ceph_decode_copy(&p, m->mon_inst, m->num_mon*sizeof(m->mon_inst[0]));
	if (p != end)
		goto bad;

	dout(30, "monmap_decode epoch %d, num_mon %d\n", m->epoch,
	     m->num_mon);
	for (i = 0; i < m->num_mon; i++)
		dout(30, "monmap_decode  mon%d is %u.%u.%u.%u:%u\n", i,
		     IPQUADPORT(m->mon_inst[i].addr.ipaddr));
	return m;

bad:
	dout(30, "monmap_decode failed with %d\n", err);
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
 * Choose a monitor.  If @notmon >= 0, choose a different monitor than
 * last time.
 */
static int pick_mon(struct ceph_mon_client *monc, int newmon)
{
	char r;

	if (!newmon && monc->last_mon >= 0)
		return monc->last_mon;
	get_random_bytes(&r, 1);
	monc->last_mon = r % monc->monmap->num_mon;
	return monc->last_mon;
}

/*
 * Generic timeout mechanism for monitor requests
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
 * mds map
 */
static void request_mdsmap(struct ceph_mon_client *monc, int newmon)
{
	struct ceph_msg *msg;
	struct ceph_mds_getmap *h;
	int mon = pick_mon(monc, newmon);

	dout(5, "request_mdsmap from mon%d want %u\n", mon, monc->want_mdsmap);
	msg = ceph_msg_new(CEPH_MSG_MDS_GETMAP, sizeof(*h), 0, 0, NULL);
	if (IS_ERR(msg))
		return;
	h = msg->front.iov_base;
	h->fsid = monc->monmap->fsid;
	h->have_version = cpu_to_le64(monc->want_mdsmap - 1);
	msg->hdr.dst = monc->monmap->mon_inst[mon];
	ceph_msg_send(monc->client->msgr, msg, 0);
}

/*
 * Register our desire for an mdsmap >= epoch @want.
 */
void ceph_monc_request_mdsmap(struct ceph_mon_client *monc, u32 want)
{
	dout(5, "request_mdsmap want %u\n", want);
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
		dout(5, "got_mdsmap %u < wanted %u\n", got, monc->want_mdsmap);
		ret = -EAGAIN;
	} else {
		dout(5, "got_mdsmap %u >= wanted %u\n", got, monc->want_mdsmap);
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
	int mon = pick_mon(monc, newmon);

	dout(5, "request_osdmap from mon%d want %u\n", mon, monc->want_osdmap);
	msg = ceph_msg_new(CEPH_MSG_OSD_GETMAP, sizeof(*h), 0, 0, NULL);
	if (IS_ERR(msg))
		return;
	h = msg->front.iov_base;
	h->fsid = monc->monmap->fsid;
	h->start = cpu_to_le32(monc->want_osdmap);
	h->have_version = cpu_to_le64(monc->want_osdmap ?
				      monc->want_osdmap-1 : 0);
	msg->hdr.dst = monc->monmap->mon_inst[mon];
	ceph_msg_send(monc->client->msgr, msg, 0);
}

void ceph_monc_request_osdmap(struct ceph_mon_client *monc, u32 want)
{
	dout(5, "request_osdmap want %u\n", want);
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
		dout(5, "got_osdmap %u < wanted %u\n", got, monc->want_osdmap);
		ret = -EAGAIN;
	} else {
		dout(5, "got_osdmap %u >= wanted %u\n", got, monc->want_osdmap);
		monc->want_osdmap = 0;
		cancel_timeout(&monc->osdreq);
	}
	mutex_unlock(&monc->req_mutex);
	return ret;
}


/*
 * umount
 */
static void request_umount(struct ceph_mon_client *monc, int newmon)
{
	struct ceph_msg *msg;
	int mon = pick_mon(monc, newmon);
	struct ceph_client_mount *h;

	dout(5, "request_umount from mon%d\n", mon);
	msg = ceph_msg_new(CEPH_MSG_CLIENT_UNMOUNT, sizeof(*h), 0, 0, NULL);
	if (IS_ERR(msg))
		return;
	h = msg->front.iov_base;
	h->have_version = 0;
	msg->hdr.dst = monc->monmap->mon_inst[mon];
	ceph_msg_send(monc->client->msgr, msg, 0);
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
	dout(5, "handle_umount\n");
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
	dout(10, "handle_statfs_reply %p tid %llu\n", msg, tid);

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
	derr(10, "corrupt statfs reply, no tid\n");
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
	int mon = pick_mon(monc, newmon ? 1 : -1);

	dout(10, "send_statfs to mon%d tid %llu\n", mon, req->tid);
	msg = ceph_msg_new(CEPH_MSG_STATFS, sizeof(*h), 0, 0, NULL);
	if (IS_ERR(msg))
		return PTR_ERR(msg);
	req->request = msg;
	h = msg->front.iov_base;
	h->have_version = 0;
	h->fsid = monc->monmap->fsid;
	h->tid = cpu_to_le64(req->tid);
	msg->hdr.dst = monc->monmap->mon_inst[mon];
	ceph_msg_send(monc->client->msgr, msg, 0);
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
	memset(&req.kobj, 0, sizeof(req.kobj));
	if (radix_tree_insert(&monc->statfs_request_tree, req.tid, &req) < 0) {
		mutex_unlock(&monc->statfs_mutex);
		derr(10, "ENOMEM in do_statfs\n");
		return -ENOMEM;
	}
	if (monc->num_statfs_requests == 0)
		schedule_delayed_work(&monc->statfs_delayed_work,
				      round_jiffies_relative(1*HZ));
	monc->num_statfs_requests++;
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

	dout(10, "do_statfs_check\n");
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
				req->delay *= 2;
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
	dout(5, "init\n");
	memset(monc, 0, sizeof(*monc));
	monc->client = cl;
	monc->monmap = kzalloc(sizeof(struct ceph_monmap) +
		       sizeof(struct ceph_entity_addr) * MAX_MON_MOUNT_ADDR,
		       GFP_KERNEL);
	if (monc->monmap == NULL)
		return -ENOMEM;
	mutex_init(&monc->statfs_mutex);
	INIT_RADIX_TREE(&monc->statfs_request_tree, GFP_NOFS);
	monc->num_statfs_requests = 0;
	monc->last_tid = 0;
	INIT_DELAYED_WORK(&monc->statfs_delayed_work, do_statfs_check);
	init_request_type(monc, &monc->mdsreq, request_mdsmap);
	init_request_type(monc, &monc->osdreq, request_osdmap);
	init_request_type(monc, &monc->umountreq, request_umount);
	mutex_init(&monc->req_mutex);
	monc->want_mdsmap = 0;
	monc->want_osdmap = 0;
	return 0;
}

void ceph_monc_stop(struct ceph_mon_client *monc)
{
	dout(5, "stop\n");
	cancel_timeout(&monc->mdsreq);
	cancel_timeout(&monc->osdreq);
	cancel_timeout(&monc->umountreq);
	cancel_delayed_work_sync(&monc->statfs_delayed_work);
	kfree(monc->monmap);
}
