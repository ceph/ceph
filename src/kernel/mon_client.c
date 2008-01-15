
#include <linux/types.h>
#include <linux/random.h>
#include "mon_client.h"

int ceph_mon_debug = 50;
#define DOUT_VAR ceph_mon_debug
#define DOUT_PREFIX "mon: "
#include "super.h"


int ceph_monmap_decode(struct ceph_monmap *m, void *p, void *end)
{
	int err;
	void *old;

	dout(30, "monmap_decode %p %p\n", p, end);

	if ((err = ceph_decode_32(&p, end, &m->epoch)) < 0)
		goto bad;
	if ((err = ceph_decode_64(&p, end, &m->fsid.major)) < 0)
		goto bad;
	if ((err = ceph_decode_64(&p, end, &m->fsid.minor)) < 0)
		goto bad;
	if ((err = ceph_decode_32(&p, end, &m->num_mon)) < 0)
		return err;

	old = m->mon_inst;
	m->mon_inst = kmalloc(m->num_mon*sizeof(*m->mon_inst), GFP_KERNEL);
	if (m->mon_inst == NULL) {
		m->mon_inst = old;
		return -ENOMEM;
	}
	kfree(old);

	if ((err = ceph_decode_copy(&p, end, m->mon_inst, m->num_mon*sizeof(m->mon_inst[0]))) < 0)
		goto bad;

	dout(30, "monmap_decode got epoch %d, num_mon %d\n", m->epoch, m->num_mon);
	return 0;

bad:
	dout(30, "monmap_decode failed with %d\n", err);
	return err;
}

/*
 * return true if *addr is included in the monmap
 */
int ceph_monmap_contains(struct ceph_monmap *m, struct ceph_entity_addr *addr)
{
	int i;
	for (i=0; i<m->num_mon; i++) 
		if (ceph_entity_addr_equal(addr, &m->mon_inst[i].addr)) 
			return 1;
	return 0;
}


static int pick_mon(struct ceph_mon_client *monc, int notmon)
{
	char r;
	if (notmon < 0 && monc->last_mon >= 0)
		return monc->last_mon;
	get_random_bytes(&r, 1);
	monc->last_mon = r % monc->monmap.num_mon;
	return monc->last_mon;
}


void ceph_monc_init(struct ceph_mon_client *monc, struct ceph_client *cl)
{
	dout(5, "ceph_monc_init\n");
	memset(monc, 0, sizeof(*monc));
	monc->client = cl;
	spin_lock_init(&monc->lock);
	INIT_RADIX_TREE(&monc->statfs_request_tree, GFP_KERNEL);
}



void ceph_monc_request_mdsmap(struct ceph_mon_client *monc, __u64 have)
{
	dout(5, "ceph_monc_request_mdsmap -- IMPLEMENT ME\n");
	
}


/*
 * statfs
 */

void ceph_monc_handle_statfs_reply(struct ceph_mon_client *monc, struct ceph_msg *msg)
{
	__u64 tid;
	struct ceph_mon_statfs_request *req;
	void *p = msg->front.iov_base;
	void *end = p + msg->front.iov_len;
	int err;
	
	if ((err = ceph_decode_64(&p, end, &tid)) < 0)
		goto bad;
	dout(10, "handle_statfs_reply %p tid %llu\n", msg, tid);

	spin_lock(&monc->lock);
	req = radix_tree_lookup(&monc->statfs_request_tree, tid);
	dout(30, "got req %p\n", req);
	if (req) {
		if ((err = ceph_decode_64(&p, end, &req->buf->f_total)) < 0)
			goto bad;
		if ((err = ceph_decode_64(&p, end, &req->buf->f_free)) < 0)
			goto bad;
		if ((err = ceph_decode_64(&p, end, &req->buf->f_avail)) < 0)
			goto bad;
		if ((err = ceph_decode_64(&p, end, &req->buf->f_objects)) < 0)
			goto bad;
		dout(30, "decoded ok\n");
	}
	radix_tree_delete(&monc->statfs_request_tree, tid);
	spin_unlock(&monc->lock);
	if (req)
		complete(&req->completion);
	return;
bad:
	dout(10, "corrupt statfs reply\n");
}

int send_statfs(struct ceph_mon_client *monc, u64 tid)
{
	struct ceph_msg *msg;
	int mon = pick_mon(monc, -1);

	dout(10, "send_statfs to mon%d tid %llu\n", mon, tid);
	msg = ceph_msg_new(CEPH_MSG_STATFS, sizeof(tid), 0, 0, 0);
	if (IS_ERR(msg))
		return PTR_ERR(msg);
	*(__le64*)msg->front.iov_base = cpu_to_le64(tid);
	msg->hdr.dst = monc->monmap.mon_inst[mon];
	ceph_msg_send(monc->client->msgr, msg, BASE_DELAY_INTERVAL);
	return 0;
}

int ceph_monc_do_statfs(struct ceph_mon_client *monc, struct ceph_statfs *buf)
{
	struct ceph_mon_statfs_request req;
	int err;

	req.buf = buf;
	init_completion(&req.completion);

	/* register request */
	spin_lock(&monc->lock);
	req.tid = ++monc->last_tid;
	req.last_attempt = jiffies;
	radix_tree_insert(&monc->statfs_request_tree, req.tid, &req);
	spin_unlock(&monc->lock);
	
	/* send request */
	if ((err = send_statfs(monc, req.tid)) < 0)
		return err;

	dout(20, "do_statfs waiting for reply\n");
	wait_for_completion(&req.completion);
	return 0;
}
