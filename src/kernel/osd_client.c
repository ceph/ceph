#include <linux/slab.h>
#include <linux/err.h>
#include <asm/uaccess.h>
#include <linux/mm.h>
#include <linux/highmem.h>	/* kmap, kunmap */
#include <linux/pagemap.h>	/* read_cache_pages */

int ceph_debug_osdc = -1;
#define DOUT_VAR ceph_debug_osdc
#define DOUT_PREFIX "osdc: "
#include "super.h"

#include "osd_client.h"
#include "messenger.h"
#include "crush/mapper.h"
#include "decode.h"

struct ceph_readdesc {
	struct ceph_osd_client *osdc;
	struct ceph_file_layout *layout;
	loff_t off;
	loff_t len;
};


/*
 * requests
 */

static void get_request(struct ceph_osd_request *req)
{
	atomic_inc(&req->r_ref);
}

static void put_request(struct ceph_osd_request *req)
{
	dout(10, "put_request %p %d -> %d\n", req, atomic_read(&req->r_ref),
	     atomic_read(&req->r_ref)-1);
	BUG_ON(atomic_read(&req->r_ref) <= 0);
	if (atomic_dec_and_test(&req->r_ref)) {
		ceph_msg_put(req->r_request);
		kfree(req);
	}
}

struct ceph_msg *new_request_msg(struct ceph_osd_client *osdc, int op,
				 struct ceph_snap_context *snapc)
{
	struct ceph_msg *req;
	struct ceph_osd_request_head *head;
	size_t size = sizeof(struct ceph_osd_request_head);

	if (snapc)
		size += sizeof(u64) * snapc->num_snaps;
	req = ceph_msg_new(CEPH_MSG_OSD_OP, size, 0, 0, 0);
	if (IS_ERR(req))
		return req;
	memset(req->front.iov_base, 0, req->front.iov_len);
	head = req->front.iov_base;

	/* encode head */
	head->op = cpu_to_le32(op);
	head->client_inc = 1; /* always, for now. */
	head->flags = 0;

	/* snaps */
	if (snapc) {
		head->snap_seq = cpu_to_le64(snapc->seq);
		head->num_snaps = cpu_to_le32(snapc->num_snaps);
		memcpy(req->front.iov_base + sizeof(*head), snapc->snaps,
		       snapc->num_snaps*sizeof(u64));
		dout(10, "snapc seq %lld %d snaps\n", snapc->seq,
		     snapc->num_snaps);
	}
	return req;
}

static struct ceph_osd_request *alloc_request(int num_pages,
					      struct ceph_msg *msg)
{
	struct ceph_osd_request *req;

	req = kzalloc(sizeof(*req) + num_pages*sizeof(void *), GFP_NOFS);
	if (req == NULL)
		return ERR_PTR(-ENOMEM);
	req->r_request = msg;
	req->r_num_pages = num_pages;
	atomic_set(&req->r_ref, 1);
	return req;
}

static void reschedule_timeout(struct ceph_osd_client *osdc)
{
	int timeout = osdc->client->mount_args.osd_timeout;
	dout(10, "reschedule timeout (%d seconds)\n", timeout);
	schedule_delayed_work(&osdc->timeout_work, 
			      round_jiffies_relative(timeout*HZ));
}

static int register_request(struct ceph_osd_client *osdc,
			    struct ceph_osd_request *req)
{
	struct ceph_osd_request_head *head = req->r_request->front.iov_base;
	int rc;

	rc = radix_tree_preload(GFP_NOFS);
	if (rc < 0) {
		derr(10, "ENOMEM in register_request\n");
		return rc;
	}

	spin_lock(&osdc->request_lock);
	req->r_tid = head->tid = ++osdc->last_tid;
	req->r_flags = 0;
	req->r_pgid.pg64 = le64_to_cpu(head->layout.ol_pgid);
	req->r_reply = 0;
	req->r_result = 0;
	init_completion(&req->r_completion);

	dout(30, "register_request %p tid %lld\n", req, req->r_tid);
	get_request(req);
	rc = radix_tree_insert(&osdc->request_tree, req->r_tid, (void *)req);

	if (osdc->nr_requests == 0) 
		reschedule_timeout(osdc);
	osdc->nr_requests++;

	spin_unlock(&osdc->request_lock);
	radix_tree_preload_end();
	
	return rc;
}

static void unregister_request(struct ceph_osd_client *osdc,
			       struct ceph_osd_request *req)
{
	dout(30, "unregister_request %p tid %lld\n", req, req->r_tid);
	spin_lock(&osdc->request_lock);
	radix_tree_delete(&osdc->request_tree, req->r_tid);

	osdc->nr_requests--;
	cancel_delayed_work(&osdc->timeout_work);
	if (osdc->nr_requests)
		reschedule_timeout(osdc);

	spin_unlock(&osdc->request_lock);
	put_request(req);
}

/*
 * pick an osd.  the pg primary, namely.
 */
static int pick_osd(struct ceph_osd_client *osdc,
		    struct ceph_osd_request *req)
{
	int ruleno;
	int i;
	int pps; /* placement ps */
	int osds[10];
	int nr_osds;
	
	ruleno = crush_find_rule(osdc->osdmap->crush, req->r_pgid.pg.pool,
				 req->r_pgid.pg.type, req->r_pgid.pg.size);
	if (ruleno < 0) {
		derr(0, "pick_osd no crush rule for pool %d type %d size %d\n",
		     req->r_pgid.pg.pool, req->r_pgid.pg.type, 
		     req->r_pgid.pg.size);
		return -1;
	}

	if (req->r_pgid.pg.preferred >= 0)
		pps = ceph_stable_mod(req->r_pgid.pg.ps,
				     osdc->osdmap->lpgp_num,
				     osdc->osdmap->lpgp_num_mask);
	else
		pps = ceph_stable_mod(req->r_pgid.pg.ps,
				     osdc->osdmap->pgp_num,
				     osdc->osdmap->pgp_num_mask);
	nr_osds = crush_do_rule(osdc->osdmap->crush, ruleno, pps, osds,
				req->r_pgid.pg.size, req->r_pgid.pg.preferred);

	for (i = 0; i < nr_osds; i++)
		if (ceph_osd_is_up(osdc->osdmap, osds[i]))
			return osds[i];
	return -1;
}

/*
 * caller should hold map_sem (for read)
 */
static void send_request(struct ceph_osd_client *osdc,
			 struct ceph_osd_request *req,
			 int osd)
{
	struct ceph_osd_request_head *reqhead;

	if (osd < 0)
		osd = pick_osd(osdc, req);
	if (osd < 0) {
		dout(10, "send_request %p no up osds in pg\n", req);
		ceph_monc_request_osdmap(&osdc->client->monc,
					 osdc->osdmap->epoch);
		return;
	}

	dout(10, "send_request %p tid %llu to osd%d flags %d\n",
	     req, req->r_tid, osd, req->r_flags);

	reqhead = req->r_request->front.iov_base;
	reqhead->osdmap_epoch = cpu_to_le32(osdc->osdmap->epoch);

	req->r_request->hdr.dst.name.type =
		cpu_to_le32(CEPH_ENTITY_TYPE_OSD);
	req->r_request->hdr.dst.name.num = cpu_to_le32(osd);
	req->r_request->hdr.dst.addr = osdc->osdmap->osd_addr[osd];
	req->r_last_osd = req->r_request->hdr.dst.addr;
	
	ceph_msg_get(req->r_request); /* send consumes a ref */
	ceph_msg_send(osdc->client->msgr, req->r_request, 0);
}

/*
 * handle osd op reply
 */
void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	struct ceph_osd_reply_head *rhead = msg->front.iov_base;
	struct ceph_osd_request *req;
	u64 tid;

	if (msg->front.iov_len != sizeof(*rhead))
		goto bad;
	tid = le64_to_cpu(rhead->tid);
	dout(10, "handle_reply %p tid %llu\n", msg, tid);

	/* lookup */
	spin_lock(&osdc->request_lock);
	req = radix_tree_lookup(&osdc->request_tree, tid);
	if (req == NULL) {
		dout(10, "handle_reply tid %llu dne\n", tid);
		spin_unlock(&osdc->request_lock);
		return;
	}
	get_request(req);

	if (req->r_reply == NULL) {
		dout(10, "handle_reply tid %llu saving reply\n", tid);
		ceph_msg_get(msg);
		req->r_reply = msg;
	} else if (req->r_reply == msg)
		dout(10, "handle_reply tid %llu already had this reply\n", tid);
	else {
		dout(10, "handle_reply tid %llu had OTHER reply?\n", tid);
		goto done;
	}
	dout(10, "handle_reply tid %llu flags %d |= %d\n", tid, req->r_flags,
	     rhead->flags);
	req->r_flags |= rhead->flags;
	spin_unlock(&osdc->request_lock);
	complete(&req->r_completion);
done:
	put_request(req);
	return;

bad:
	derr(0, "got corrupt osd_op_reply got %d %d expected %d\n",
	     (int)msg->front.iov_len, (int)msg->hdr.front_len,
	     (int)sizeof(*rhead));
}


/*
 * caller should hold read sem
 */
static int kick_requests(struct ceph_osd_client *osdc)
{
	u64 next_tid = 0;
	struct ceph_osd_request *req;
	int got;
	int osd;
	int ret = 0;
	
more:
	spin_lock(&osdc->request_lock);
more_locked:
	got = radix_tree_gang_lookup(&osdc->request_tree, (void **)&req, 
				     next_tid, 1);
	if (got == 0)
		goto done;

	next_tid = req->r_tid + 1;
	osd = pick_osd(osdc, req);
	if (osd < 0) {
		ret = 1;  /* request a newer map */
		memset(&req->r_last_osd, 0, sizeof(req->r_last_osd));
	} else if (!ceph_entity_addr_equal(&req->r_last_osd,
					   &osdc->osdmap->osd_addr[osd])) {
		dout(20, "kicking tid %llu osd%d\n", req->r_tid, osd);
		get_request(req);
		spin_unlock(&osdc->request_lock);
		req->r_request = ceph_msg_maybe_dup(req->r_request);
		if (!req->r_aborted) {
			req->r_flags |= CEPH_OSD_OP_RETRY;
			send_request(osdc, req, osd);
		}
		put_request(req);
		goto more;
	}
	goto more_locked;

done:
	spin_unlock(&osdc->request_lock);
	if (ret)
		dout(10, "%d requests still pending on down osds\n", ret);
	return ret;
}

void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	void *p, *end, *next;
	__u32 nr_maps, maplen;
	__u32 epoch;
	struct ceph_osdmap *newmap = 0;
	int err;
	struct ceph_fsid fsid;

	dout(2, "handle_map, have %u\n", osdc->osdmap ? osdc->osdmap->epoch:0);
	p = msg->front.iov_base;
	end = p + msg->front.iov_len;

	/* verify fsid */
	ceph_decode_need(&p, end, sizeof(fsid), bad);
	ceph_decode_64(&p, fsid.major);
	ceph_decode_64(&p, fsid.minor);
	if (!ceph_fsid_equal(&fsid, &osdc->client->monc.monmap->fsid)) {
		derr(0, "got map with wrong fsid, ignoring\n");
		return;
	}

	down_write(&osdc->map_sem);

	/* incremental maps */
	ceph_decode_32_safe(&p, end, nr_maps, bad);
	dout(10, " %d inc maps\n", nr_maps);
	while (nr_maps > 0) {
		ceph_decode_need(&p, end, 2*sizeof(__u32), bad);
		ceph_decode_32(&p, epoch);
		ceph_decode_32(&p, maplen);
		ceph_decode_need(&p, end, maplen, bad);
		next = p + maplen;
		if (osdc->osdmap && osdc->osdmap->epoch+1 == epoch) {
			dout(10, "applying incremental map %u len %d\n",
			     epoch, maplen);
			newmap = apply_incremental(&p, next, osdc->osdmap,
						   osdc->client->msgr);
			if (IS_ERR(newmap)) {
				err = PTR_ERR(newmap);
				goto bad;
			}
			if (newmap != osdc->osdmap) {
				osdmap_destroy(osdc->osdmap);
				osdc->osdmap = newmap;
			}
		} else {
			dout(10, "ignoring incremental map %u len %d\n",
			     epoch, maplen);
		}
		p = next;
		nr_maps--;
	}
	if (newmap)
		goto done;

	/* full maps */
	ceph_decode_32_safe(&p, end, nr_maps, bad);
	dout(30, " %d full maps\n", nr_maps);
	while (nr_maps > 1) {
		ceph_decode_need(&p, end, 2*sizeof(__u32), bad);
		ceph_decode_32(&p, epoch);
		ceph_decode_32(&p, maplen);
		ceph_decode_need(&p, end, maplen, bad);
		dout(5, "skipping non-latest full map %u len %d\n",
		     epoch, maplen);
		p += maplen;
		nr_maps--;
	}
	if (nr_maps) {
		ceph_decode_need(&p, end, 2*sizeof(__u32), bad);
		ceph_decode_32(&p, epoch);
		ceph_decode_32(&p, maplen);
		ceph_decode_need(&p, end, maplen, bad);
		if (osdc->osdmap && osdc->osdmap->epoch >= epoch) {
			dout(10, "skipping full map %u len %d, "
			     "older than our %u\n", epoch, maplen,
			     osdc->osdmap->epoch);
			p += maplen;
		} else {
			dout(10, "taking full map %u len %d\n", epoch, maplen);
			newmap = osdmap_decode(&p, p+maplen);
			if (IS_ERR(newmap)) {
				err = PTR_ERR(newmap);
				goto bad;
			}
			if (osdc->osdmap)
				osdmap_destroy(osdc->osdmap);
			osdc->osdmap = newmap;
		}
	}

done:
	downgrade_write(&osdc->map_sem);
	ceph_monc_got_osdmap(&osdc->client->monc, osdc->osdmap->epoch);
	if (newmap && kick_requests(osdc))
		ceph_monc_request_osdmap(&osdc->client->monc,
					 osdc->osdmap->epoch);
	up_read(&osdc->map_sem);
	return;

bad:
	derr(1, "handle_map corrupt msg\n");
	up_write(&osdc->map_sem);
	return;
}



/*
 * find pages for message payload to be read into.
 *  0 = success, -1 failure.
 */
int ceph_osdc_prepare_pages(void *p, struct ceph_msg *m, int want)
{
	struct ceph_client *client = p;
	struct ceph_osd_client *osdc = &client->osdc;
	struct ceph_osd_reply_head *rhead = m->front.iov_base;
	struct ceph_osd_request *req;
	__u64 tid;
	int ret = -1;
	int type = le32_to_cpu(m->hdr.type);

	dout(10, "prepare_pages on msg %p want %d\n", m, want);
	if (unlikely(type != CEPH_MSG_OSD_OPREPLY))
		return -1;  /* hmm! */

	tid = le64_to_cpu(rhead->tid);
	spin_lock(&osdc->request_lock);
	req = radix_tree_lookup(&osdc->request_tree, tid);
	if (!req) {
		dout(10, "prepare_pages unknown tid %llu\n", tid);
		goto out;
	}
	dout(10, "prepare_pages tid %llu have %d pages, want %d\n",
	     tid, req->r_num_pages, want);
	if (likely(req->r_num_pages >= want)) {
		m->pages = req->r_pages;
		m->nr_pages = req->r_num_pages;
		ceph_msg_get(m);
		req->r_reply = m;
		ret = 0; /* success */
	}
out:
	spin_unlock(&osdc->request_lock);
	return ret;
}

/*
 * synchronously do an osd request.
 */
int do_request(struct ceph_osd_client *osdc, struct ceph_osd_request *req)
{
	struct ceph_osd_reply_head *replyhead;
	__s32 rc;
	int bytes;

	/* register+send request */
	register_request(osdc, req);
	down_read(&osdc->map_sem);
	send_request(osdc, req, -1);
	up_read(&osdc->map_sem);

	rc = wait_for_completion_interruptible(&req->r_completion);

	unregister_request(osdc, req);
	if (rc < 0) {
		struct ceph_msg *msg;
		dout(0, "tid %llu err %d, revoking %p pages\n", req->r_tid,
		     rc, req->r_request);
		/* 
		 * mark req aborted _before_ revoking pages, so that
		 * if a racing kick_request _does_ dup the page vec
		 * pointer, it will definitely then see the aborted
		 * flag and not send the request.
		 */
		req->r_aborted = 1;
		msg = req->r_request;
		mutex_lock(&msg->page_mutex);
		msg->pages = 0;
		mutex_unlock(&msg->page_mutex);
		if (req->r_reply) {
			mutex_lock(&req->r_reply->page_mutex);
			req->r_reply->pages = 0;
			mutex_unlock(&req->r_reply->page_mutex);
		}
		return rc;
	}

	/* parse reply */
	replyhead = req->r_reply->front.iov_base;
	rc = le32_to_cpu(replyhead->result);
	bytes = le32_to_cpu(req->r_reply->hdr.data_len);
	dout(10, "do_request tid %llu result %d, %d bytes\n",
	     req->r_tid, rc, bytes);
	if (rc < 0)
		return rc;
	return bytes;
}

void handle_timeout(struct work_struct *work)
{
	struct ceph_osd_client *osdc = 
		container_of(work, struct ceph_osd_client, timeout_work.work);
	dout(10, "timeout\n");
	down_read(&osdc->map_sem);
	ceph_monc_request_osdmap(&osdc->client->monc, osdc->osdmap->epoch);
	up_read(&osdc->map_sem);
}

void ceph_osdc_init(struct ceph_osd_client *osdc, struct ceph_client *client)
{
	dout(5, "init\n");
	osdc->client = client;

	osdc->osdmap = NULL;
	init_rwsem(&osdc->map_sem);
	init_completion(&osdc->map_waiters);
	osdc->last_requested_map = 0;

	spin_lock_init(&osdc->request_lock);
	osdc->last_tid = 0;
	osdc->nr_requests = 0;
	INIT_RADIX_TREE(&osdc->request_tree, GFP_ATOMIC);
	INIT_DELAYED_WORK(&osdc->timeout_work, handle_timeout);
}

void ceph_osdc_stop(struct ceph_osd_client *osdc)
{
	cancel_delayed_work_sync(&osdc->timeout_work);

	if (osdc->osdmap) {
		osdmap_destroy(osdc->osdmap);
		osdc->osdmap = 0;
	}
}



/*
 * calculate the mapping of an extent onto an object, and fill out the
 * request accordingly.  shorten extent as necessary if it hits an
 * object boundary.
 */
static __u64 calc_layout(struct ceph_osd_client *osdc,
			 struct ceph_vino vino, struct ceph_file_layout *layout,
			 __u64 off, __u64 len,
			 struct ceph_osd_request *req)
{
	struct ceph_osd_request_head *reqhead = req->r_request->front.iov_base;
	__u64 toff = off, tlen = len;

	reqhead->oid.ino = vino.ino;
	reqhead->oid.snap = vino.snap;

	calc_file_object_mapping(layout, &toff, &tlen, &reqhead->oid,
				 &off, &len);
	if (tlen != 0)
		dout(10, " skipping last %llu, writing  %llu~%llu\n",
		     tlen, off, len);
	reqhead->offset = cpu_to_le64(off);
	reqhead->length = cpu_to_le64(len);

	calc_object_layout(&reqhead->layout, &reqhead->oid, layout,
			   osdc->osdmap);

	dout(10, "calc_layout bno %u on %llu~%llu pgid %llx\n",
	     le32_to_cpu(reqhead->oid.bno), off, len,
	     le64_to_cpu(reqhead->layout.ol_pgid));

	return len;
}

/*
 * synchronous read direct to user buffer.
 *
 * FIXME: if read spans object boundary, just do two separate reads.
 * for a correct atomic read, we should take read locks on all
 * objects.
 */
int ceph_osdc_sync_read(struct ceph_osd_client *osdc, struct ceph_vino vino,
			struct ceph_file_layout *layout,
			__u64 off, __u64 len,
			char __user *data)
{
	struct ceph_msg *reqm;
	struct ceph_osd_request *req;
	int num_pages, i, po, left, l;
	__s32 rc;
	int finalrc = 0;
	u64 rlen;

	dout(10, "sync_read on vino %llx.%llx at %llu~%llu\n", vino.ino,
	     vino.snap, off, len);

more:
	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_READ, 0);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);

	num_pages = calc_pages_for(off, len);
	req = alloc_request(num_pages, reqm);
	if (IS_ERR(req))
		return PTR_ERR(req);

	rlen = calc_layout(osdc, vino, layout, off, len, req);
	num_pages = calc_pages_for(off, rlen);  /* recalc */
	dout(10, "sync_read %llu~%llu -> %d pages\n", off, rlen, num_pages);

	/* allocate temp pages to hold data */
	for (i = 0; i < num_pages; i++) {
		req->r_pages[i] = alloc_page(GFP_NOFS);
		if (req->r_pages[i] == NULL) {
			req->r_num_pages = i+1;
			put_request(req);
			return -ENOMEM;
		}
	}
	reqm->nr_pages = num_pages;
	reqm->pages = req->r_pages;
	reqm->hdr.data_len = cpu_to_le32(rlen);
	reqm->hdr.data_off = cpu_to_le32(off);

	rc = do_request(osdc, req);
	if (rc > 0) {
		/* copy into user buffer */
		po = off & ~PAGE_CACHE_MASK;
		left = rc;
		i = 0;
		while (left > 0) {
			int bad;
			l = min_t(int, left, PAGE_CACHE_SIZE-po);
			dout(20, "copy po %d left %d l %d page %d\n",
			     po, left, l, i);
			bad = copy_to_user(data,
					   page_address(req->r_pages[i]) + po,
					   l);
			if (bad == l) {
				rc = -EFAULT;
				goto out;
			}
			data += l - bad;
			left -= l - bad;
			if (po) {
				po += l - bad;
				if (po == PAGE_CACHE_SIZE)
					po = 0;
			}
			i++;
		}
	}
out:
	put_request(req);
	if (rc > 0) {
		finalrc += rc;
		off += rc;
		len -= rc;
		if (len > 0)
			goto more;
	} else
		finalrc = rc;
	dout(10, "sync_read result %d\n", finalrc);
	return finalrc;
}

/*
 * read a single page.
 */
int ceph_osdc_readpage(struct ceph_osd_client *osdc, struct ceph_vino vino,
		       struct ceph_file_layout *layout,
		       loff_t off, loff_t len,
		       struct page *page)
{
	struct ceph_msg *reqm;
	struct ceph_osd_request_head *reqhead;
	struct ceph_osd_request *req;
	__s32 rc;

	dout(10, "readpage on ino %llx.%llx at %lld~%lld\n", vino.ino,
	     vino.snap, off, len);

	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_READ, 0);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	reqhead = reqm->front.iov_base;

	req = alloc_request(1, reqm);
	if (IS_ERR(req)) {
		ceph_msg_put(reqm);
		return PTR_ERR(req);
	}
	req->r_pages[0] = page;

	len = calc_layout(osdc, vino, layout, off, len, req);
	BUG_ON(len != PAGE_CACHE_SIZE);

	rc = do_request(osdc, req);
	put_request(req);
	dout(10, "readpage result %d\n", rc);
	if (rc == -ENOENT)
		rc = 0;		/* object page dne; zero it */
	return rc;
}

/*
 * read some contiguous pages from page_list.
 *  - we stop if pages aren't contiguous, or when we hit an object boundary
 */
int ceph_osdc_readpages(struct ceph_osd_client *osdc,
			struct address_space *mapping,
			struct ceph_vino vino, struct ceph_file_layout *layout,
			__u64 off, __u64 len,
			struct list_head *page_list, int num_pages)
{
	struct ceph_msg *reqm;
	struct ceph_osd_request *req;
	struct page *page;
	pgoff_t next_index;
	int contig_pages;
	int rc = 0;

	/*
	 * for now, our strategy is simple: start with the
	 * initial page, and fetch as much of that object as
	 * we can that falls within the range specified by
	 * num_pages.
	 */
	dout(10, "readpages on ino %llx.%llx on %llu~%llu\n", vino.ino,
	     vino.snap, off, len);

	/* alloc request, w/ optimistically-sized page vector */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_READ, 0);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	req = alloc_request(num_pages, reqm);
	if (req == 0) {
		ceph_msg_put(reqm);
		return -ENOMEM;
	}

	/* find adjacent pages */
	next_index = list_entry(page_list->prev, struct page, lru)->index;
	contig_pages = 0;
	list_for_each_entry_reverse(page, page_list, lru) {
		if (page->index == next_index) {
			req->r_pages[contig_pages] = page;
			contig_pages++;
			next_index++;
		} else
			break;
	}
	dout(10, "readpages found %d/%d contig\n", contig_pages, num_pages);
	if (contig_pages == 0)
		goto out;
	len = min((contig_pages << PAGE_CACHE_SHIFT) - (off & ~PAGE_CACHE_MASK),
		  len);
	dout(10, "readpages contig page extent is %llu~%llu\n", off, len);

	/* request msg */
	len = calc_layout(osdc, vino, layout, off, len, req);
	req->r_num_pages = calc_pages_for(off, len);
	dout(10, "readpages final extent is %llu~%llu -> %d pages\n",
	     off, len, req->r_num_pages);
	rc = do_request(osdc, req);

out:
	put_request(req);
	dout(10, "readpages result %d\n", rc);
	if (rc < 0)
		dout(10, "hrm!\n");
	return rc;
}


/*
 * synchronous write.  from userspace.
 *
 * FIXME: if write spans object boundary, just do two separate write.
 * for a correct atomic write, we should take write locks on all
 * objects, rollback on failure, etc.
 */
int ceph_osdc_sync_write(struct ceph_osd_client *osdc, struct ceph_vino vino,
			 struct ceph_file_layout *layout,
			 struct ceph_snap_context *snapc,
			 __u64 off, __u64 len, const char __user *data)
{
	struct ceph_msg *reqm;
	struct ceph_osd_request_head *reqhead;
	struct ceph_osd_request *req;
	int num_pages, i, po, l, left;
	__s32 rc;
	u64 rlen;
	int finalrc = 0;

	dout(10, "sync_write on ino %llx.%llx at %llu~%llu\n", vino.ino,
	     vino.snap, off, len);

more:
	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_WRITE, snapc);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	reqhead = reqm->front.iov_base;
	reqhead->flags = CEPH_OSD_OP_ACK | /* just ack for now... FIXME */
		CEPH_OSD_OP_ORDERSNAP;     /* get EOLDSNAPC if out of order */

	/* how many pages? */
	num_pages = calc_pages_for(off, len);
	req = alloc_request(num_pages, reqm);
	if (IS_ERR(req)) {
		ceph_msg_put(reqm);
		return PTR_ERR(req);
	}

	rlen = calc_layout(osdc, vino, layout, off, len, req);
	num_pages = calc_pages_for(off, rlen);  /* recalc */
	dout(10, "sync_write %llu~%llu -> %d pages\n", off, rlen, num_pages);

	/* copy data into a set of pages */
	left = rlen;
	po = off & ~PAGE_MASK;
	for (i = 0; i < num_pages; i++) {
		int bad;
		req->r_pages[i] = alloc_page(GFP_NOFS);
		if (req->r_pages[i] == NULL) {
			req->r_num_pages = i+1;
			rc = -ENOMEM;
			goto out;
		}
		l = min_t(int, PAGE_SIZE-po, left);
		bad = copy_from_user(page_address(req->r_pages[i]) + po, data,
				     l);
		if (bad == l) {
			req->r_num_pages = i+1;
			rc = -EFAULT;
			goto out;
		}
		data += l - bad;
		left -= l - bad;
		if (po) {
			po += l - bad;
			if (po == PAGE_CACHE_SIZE)
				po = 0;
		}
	}
	reqm->pages = req->r_pages;
	reqm->nr_pages = num_pages;
	reqm->hdr.data_len = cpu_to_le32(rlen);
	reqm->hdr.data_off = cpu_to_le32(off);

	rc = do_request(osdc, req);
out:
	for (i = 0; i < req->r_num_pages; i++)
		__free_pages(req->r_pages[i], 0);
	put_request(req);
	if (rc == 0) {
		finalrc += rlen;
		off += rlen;
		len -= rlen;
		if (len > 0)
			goto more;
	} else
		finalrc = rc;
	dout(10, "sync_write result %d\n", finalrc);
	return finalrc;
}

/*
 * do a write job for N pages
 */
int ceph_osdc_writepages(struct ceph_osd_client *osdc, struct ceph_vino vino,
			 struct ceph_file_layout *layout,
			 struct ceph_snap_context *snapc,
			 loff_t off, loff_t len,
			 struct page **pages, int num_pages)
{
	struct ceph_msg *reqm;
	struct ceph_osd_request_head *reqhead;
	struct ceph_osd_request *req;
	int rc = 0;

	BUG_ON(vino.snap != CEPH_NOSNAP);

	/* request + msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_WRITE, snapc);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	req = alloc_request(num_pages, reqm);
	if (IS_ERR(req)) {
		ceph_msg_put(reqm);
		return PTR_ERR(req);
	}

	reqhead = reqm->front.iov_base;
	if (osdc->client->mount_args.flags & CEPH_MOUNT_UNSAFE_WRITES)
		reqhead->flags = CEPH_OSD_OP_ACK;
	else
		reqhead->flags = CEPH_OSD_OP_SAFE;

	len = calc_layout(osdc, vino, layout, off, len, req);
	num_pages = calc_pages_for(off, len);
	dout(10, "writepages %llu~%llu -> %d pages\n", off, len, num_pages);
	
	/* copy pages */
	memcpy(req->r_pages, pages, num_pages * sizeof(struct page *));
	reqm->pages = req->r_pages;
	reqm->nr_pages = req->r_num_pages = num_pages;
	reqm->hdr.data_len = len;
	reqm->hdr.data_off = off;

	rc = do_request(osdc, req);
	put_request(req);
	if (rc == 0)
		rc = len;
	dout(10, "writepages result %d\n", rc);
	return rc;
}


