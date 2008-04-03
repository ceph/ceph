#include <linux/slab.h>
#include <linux/err.h>
#include <asm/uaccess.h>
#include <linux/mm.h>
#include <linux/highmem.h>	/* kmap, kunmap */
#include <linux/pagemap.h>	/* read_cache_pages */

int ceph_debug_osdc = 50;
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

void ceph_osdc_init(struct ceph_osd_client *osdc, struct ceph_client *client)
{
	dout(5, "init\n");
	spin_lock_init(&osdc->lock);
	osdc->client = client;
	osdc->osdmap = NULL;
	osdc->last_requested_map = 0;
	osdc->last_tid = 0;
	INIT_RADIX_TREE(&osdc->request_tree, GFP_KERNEL);
	init_completion(&osdc->map_waiters);
}

void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	void *p, *end, *next;
	__u32 nr_maps, maplen;
	__u32 epoch;
	struct ceph_osdmap *newmap = 0;
	int err;

	dout(1, "handle_map, have %u\n", osdc->osdmap ? osdc->osdmap->epoch:0);
	p = msg->front.iov_base;
	end = p + msg->front.iov_len;

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
			newmap = apply_incremental(&p, p+maplen, osdc->osdmap);
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
		goto out;
	
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
	dout(1, "handle_map done\n");
	
	ceph_monc_got_osdmap(&osdc->client->monc, osdc->osdmap->epoch);

	/* kick any pending requests that need kicking */
	/* WRITE ME */

out:
	return;

bad:
	derr(1, "handle_map corrupt msg\n");
	goto out;
}



/* 
 * requests
 */

static void get_request(struct ceph_osd_request *req)
{
	atomic_inc(&req->r_ref);
}

static void put_request(struct ceph_osd_request *req)
{
	BUG_ON(atomic_read(&req->r_ref) <= 0);
	if (atomic_dec_and_test(&req->r_ref)) {
		ceph_msg_put(req->r_request);
		kfree(req);
	}
}

struct ceph_msg *new_request_msg(struct ceph_osd_client *osdc, int op)
{
	struct ceph_msg *req;
	struct ceph_osd_request_head *head;
	
	req = ceph_msg_new(CEPH_MSG_OSD_OP, sizeof(struct ceph_osd_request_head), 0, 0, 0);
	if (IS_ERR(req))
		return req;
	memset(req->front.iov_base, 0, req->front.iov_len);
	head = req->front.iov_base;

	/* encode head */
	head->op = cpu_to_le32(op);
	head->client_inst = osdc->client->msgr->inst;
	head->client_inc = 1; /* always, for now. */
	head->flags = 0;   

	return req;
}

static struct ceph_osd_request *alloc_request(int nr_pages, 
					      struct ceph_msg *msg)
{
	struct ceph_osd_request *req;

	req = kmalloc(sizeof(*req) + nr_pages*sizeof(void*), GFP_KERNEL);
	if (req == NULL)
		return ERR_PTR(-ENOMEM);
	req->r_request = msg;
	req->r_nr_pages = nr_pages;
	return req;
}

static int register_request(struct ceph_osd_client *osdc,
			    struct ceph_osd_request *req)
{
	struct ceph_osd_request_head *head = req->r_request->front.iov_base;

	req->r_tid = head->tid = ++osdc->last_tid;
	req->r_flags = 0;
	req->r_pgid.pg64 = le64_to_cpu(head->layout.ol_pgid);
	req->r_reply = 0;
	req->r_result = 0;
	atomic_set(&req->r_ref, 2);  /* one for request_tree, one for caller */
	init_completion(&req->r_completion);

	dout(30, "register_request %p tid %lld\n", req, req->r_tid);
	return radix_tree_insert(&osdc->request_tree, req->r_tid, (void*)req);
}

static void send_request(struct ceph_osd_client *osdc, 
			 struct ceph_osd_request *req)
{
	int ruleno;
	int osds[10];
	int nr_osds;
	int i;
	int pps; /* placement ps */

	dout(30, "send_request %p\n", req);
	
	ruleno = crush_find_rule(osdc->osdmap->crush, req->r_pgid.pg.pool, 
				 req->r_pgid.pg.type, req->r_pgid.pg.size);
	BUG_ON(ruleno < 0);  /* fixme, need some proper error handling here */
	dout(30, "using crush rule %d\n", ruleno);
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
	for (i=0; i<nr_osds; i++) {
		if (ceph_osd_is_up(osdc->osdmap, osds[i]))
			break;
	}
	if (i < nr_osds) {
		dout(10, "send_request %p tid %llu to osd%d flags %d\n", req, req->r_tid, osds[i], req->r_flags);
		req->r_request->hdr.dst.name.type = cpu_to_le32(CEPH_ENTITY_TYPE_OSD);
		req->r_request->hdr.dst.name.num = cpu_to_le32(osds[i]);
		req->r_request->hdr.dst.addr = osdc->osdmap->osd_addr[osds[i]];
		ceph_msg_get(req->r_request); /* send consumes a ref */
		ceph_msg_send(osdc->client->msgr, req->r_request, 0);
	} else {
		dout(10, "send_request no osds in this pg are up\n");
		ceph_monc_request_osdmap(&osdc->client->monc, 
					 osdc->osdmap->epoch, 0);
	}
}

static void unregister_request(struct ceph_osd_client *osdc,
			       struct ceph_osd_request *req)
{
	dout(30, "unregister_request %p tid %lld\n", req, req->r_tid);
	radix_tree_delete(&osdc->request_tree, req->r_tid);
	put_request(req);
}

/*
 * handle osd op reply
 */
void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	struct ceph_osd_reply_head *rhead = msg->front.iov_base;
	struct ceph_osd_request *req;
	ceph_tid_t tid;

	dout(10, "handle_reply %p tid %llu\n", msg, le64_to_cpu(rhead->tid));
	
	/* lookup */
	tid = le64_to_cpu(rhead->tid);
	spin_lock(&osdc->lock);
	req = radix_tree_lookup(&osdc->request_tree, tid);
	if (req == NULL) {
		dout(10, "handle_reply tid %llu dne\n", tid);
		spin_unlock(&osdc->lock);
		return;
	}
	get_request(req);
	
	if (req->r_reply == NULL) {
		dout(10, "handle_reply tid %llu saving reply\n", tid);
		ceph_msg_get(msg);
		req->r_reply = msg;
	} else {
		dout(10, "handle_reply tid %llu already had a reply\n", tid);
	}
	dout(10, "handle_reply tid %llu flags %d |= %d\n", tid, req->r_flags, rhead->flags);
	req->r_flags |= rhead->flags;
	spin_unlock(&osdc->lock);
	complete(&req->r_completion);
	put_request(req);
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
	spin_lock(&osdc->lock);
	req = radix_tree_lookup(&osdc->request_tree, tid);
	if (!req) {
		dout(10, "prepare_pages unknown tid %llu\n", tid);
		goto out;
	}
	dout(10, "prepare_pages tid %llu have %d pages, want %d\n", 
	     tid, req->r_nr_pages, want);
	if (likely(req->r_nr_pages >= want)) {
		m->pages = req->r_pages;
		m->nr_pages = req->r_nr_pages;
		ret = 0; /* success */
	}
out:
	spin_unlock(&osdc->lock);
	return ret;
}


int do_request(struct ceph_osd_client *osdc, struct ceph_osd_request *req)
{
	struct ceph_osd_request_head *reqhead = req->r_request->front.iov_base;
	struct ceph_osd_reply_head *replyhead;
	__s32 rc;
	int bytes;

	/* register+send request */
	spin_lock(&osdc->lock);
	rc = register_request(osdc, req);
	if (rc < 0) {
		spin_unlock(&osdc->lock);
		return rc;
	}
	reqhead->osdmap_epoch = osdc->osdmap->epoch;
	send_request(osdc, req);
	spin_unlock(&osdc->lock);
	
	/* wait */
	dout(10, "do_request tid %llu waiting on %p\n", req->r_tid, req);
	wait_for_completion(&req->r_completion);
	dout(10, "do_request tid %llu got reply on %p\n", req->r_tid, req);

	spin_lock(&osdc->lock);
	unregister_request(osdc, req);
	spin_unlock(&osdc->lock);

	/* parse reply */
	replyhead = req->r_reply->front.iov_base;
	rc = le32_to_cpu(replyhead->result);
	bytes = le32_to_cpu(req->r_reply->hdr.data_len);
	dout(10, "do_request result %d, %d bytes\n", rc, bytes); 
	if (rc < 0)
		return rc;
	return bytes;
}

/*
 * calculate the mapping of an extent onto an object, and fill out the
 * request accordingly.  shorten extent as necessary if it hits an
 * object boundary.
 */
__u64 calc_layout(struct ceph_osd_client *osdc, 
		  ceph_ino_t ino, struct ceph_file_layout *layout,
		  __u64 off, __u64 len,
		  struct ceph_osd_request *req)
{
	struct ceph_osd_request_head *reqhead = req->r_request->front.iov_base;
	__u64 toff = off, tlen = len;

	reqhead->oid.ino = ino;
	reqhead->oid.rev = 0;

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
 * synchronous read direct to user buffer
 */
int ceph_osdc_sync_read(struct ceph_osd_client *osdc, ceph_ino_t ino,
			struct ceph_file_layout *layout, 
			__u64 off, __u64 len,
			char __user *data)
{
	struct ceph_msg *reqm;
	struct ceph_osd_request *req;
	int nr_pages, i, po, left, l;
	__s32 rc;

	dout(10, "sync_read on ino %llx at %llu~%llu\n", ino, off, len);

	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_READ);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);

	nr_pages = calc_pages_for(off, len);
	req = alloc_request(nr_pages, reqm);
	if (IS_ERR(req))
		return PTR_ERR(req);

	len = calc_layout(osdc, ino, layout, off, len, req);
	nr_pages = calc_pages_for(off, len);  /* recalc */
	dout(10, "sync_read %llu~%llu -> %d pages\n", off, len, nr_pages);

	/* allocate temp pages to hold data */
	for (i=0; i<nr_pages; i++) {
		req->r_pages[i] = alloc_page(GFP_KERNEL);
		if (req->r_pages[i] == NULL) {
			req->r_nr_pages = i+1;
			put_request(req);
			return -ENOMEM;
		}
	}
	reqm->nr_pages = nr_pages;
	reqm->pages = req->r_pages;
	reqm->hdr.data_len = cpu_to_le32(len);
	reqm->hdr.data_off = cpu_to_le32(off);

	rc = do_request(osdc, req);
	if (rc > 0) {
		/* copy into user buffer */
		po = off & ~PAGE_CACHE_MASK;
		left = rc;
		i = 0;
		while (left > 0) {
			l = min_t(int, left, PAGE_CACHE_SIZE-po);
			dout(20, "copy po %d left %d l %d page %d\n",
			     po, left, l, i);
			copy_to_user(data, page_address(req->r_pages[i]) + po,
				     l);
			data += l;
			left -= l;
			po = 0;
			i++;
		}
	}
	put_request(req);
	dout(10, "sync_read result %d\n", rc); 
	return rc;
}

/*
 * read a single page.
 */
int ceph_osdc_readpage(struct ceph_osd_client *osdc, ceph_ino_t ino,
		       struct ceph_file_layout *layout, 
		       loff_t off, loff_t len,
		       struct page *page)
{
	struct ceph_msg *reqm;
 	struct ceph_osd_request_head *reqhead;
	struct ceph_osd_request *req;
	__s32 rc;

	dout(10, "readpage on ino %llx at %lld~%lld\n", ino, off, len);

	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_READ);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	reqhead = reqm->front.iov_base;

	req = alloc_request(1, reqm);
	if (IS_ERR(req)) {
		ceph_msg_put(reqm);
		return PTR_ERR(req);
	}
	req->r_pages[0] = page;

	len = calc_layout(osdc, ino, layout, off, len, req);
	BUG_ON(len != PAGE_CACHE_SIZE);
		
	rc = do_request(osdc, req);
	put_request(req);
	dout(10, "readpage result %d\n", rc); 
	return rc;
}

/*
 * read some contiguous pages from page_list.  
 *  - we stop if pages aren't contiguous, or when we hit an object boundary
 */
int ceph_osdc_readpages(struct ceph_osd_client *osdc,
			struct address_space *mapping,
			ceph_ino_t ino, struct ceph_file_layout *layout, 
			__u64 off, __u64 len,
			struct list_head *page_list, int nr_pages)
{
	struct ceph_msg *reqm;
	struct ceph_osd_request *req;
	struct page *page;
	pgoff_t next_index;
	int contig_pages;
	__s32 rc;

	/*
	 * for now, our strategy is simple: start with the 
	 * initial page, and fetch as much of that object as
	 * we can that falls within the range specified by 
	 * nr_pages.
	 */

	dout(10, "readpages on ino %llx on %llu~%llu\n", ino, off, len);

	/* alloc request, w/ page vector */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_READ);
	if (IS_ERR(reqm)) 
		return PTR_ERR(reqm);
	req = alloc_request(nr_pages, reqm);
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
	dout(10, "readpages found %d/%d contig\n", contig_pages, nr_pages);
	if (contig_pages == 0) {
		put_request(req);
		return 0;
	}
	len = min((contig_pages << PAGE_CACHE_SHIFT) - (off & ~PAGE_CACHE_MASK),
		  len);
	dout(10, "readpages contig page extent is %llu~%llu\n", off, len);
	
	/* request msg */
	len = calc_layout(osdc, ino, layout, off, len, req);
	req->r_nr_pages = calc_pages_for(off, len);

	dout(10, "readpages final extent is %llu~%llu -> %d pages\n", 
	     off, len, req->r_nr_pages);
	
	rc = do_request(osdc, req);
	put_request(req);
	dout(10, "readpages result %d\n", rc);
	return rc;
}


/*
 * synchronous write.  from userspace.
 */
int ceph_osdc_sync_write(struct ceph_osd_client *osdc, ceph_ino_t ino,
			 struct ceph_file_layout *layout, 
			 __u64 off, __u64 len, const char __user *data)
{
	struct ceph_msg *reqm;
	struct ceph_osd_request_head *reqhead;
	struct ceph_osd_request *req;
	int nr_pages, i, po, l, left;
	__s32 rc;

	dout(10, "sync_write on ino %llx at %llu~%llu\n", ino, off, len);

	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_WRITE);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	reqhead = reqm->front.iov_base;
	reqhead->flags = CEPH_OSD_OP_ACK;  /* just ack.. FIXME */

	/* how many pages? */
	nr_pages = calc_pages_for(off, len);
	req = alloc_request(nr_pages, reqm);
	if (IS_ERR(req)) {
		ceph_msg_put(reqm);
		return PTR_ERR(req);
	}

	len = calc_layout(osdc, ino, layout, off, len, req);
	nr_pages = calc_pages_for(off, len);  /* recalc */
	dout(10, "sync_write %llu~%llu -> %d pages\n", off, len, nr_pages);

	/* copy data into a set of pages */
	left = len;
	po = off & ~PAGE_MASK;
	for (i=0; i<nr_pages; i++) {
		req->r_pages[i] = alloc_page(GFP_KERNEL);
		if (req->r_pages[i] == NULL) {
			req->r_nr_pages = i+1;
			put_request(req);
			return -ENOMEM;
		}
		l = min_t(int, PAGE_SIZE-po, left);
		copy_from_user(page_address(req->r_pages[i]) + po, data, l);
		data += l;
		left -= l;
		po = 0;
	}
	reqm->pages = req->r_pages;
	reqm->nr_pages = nr_pages;
	reqm->hdr.data_len = cpu_to_le32(len);
	reqm->hdr.data_off = cpu_to_le32(off);

	rc = do_request(osdc, req);
	put_request(req);
	if (rc == 0)
		rc = len;
	dout(10, "sync_write result %d\n", rc); 
	return rc;
}

/*
 * do a write job for N pages
 */
int ceph_osdc_writepages(struct ceph_osd_client *osdc, ceph_ino_t ino,
			 struct ceph_file_layout *layout, 
			 loff_t off, loff_t len,
			 struct page **pages, int nr_pages)
{
	struct ceph_msg *reqm;
	struct ceph_osd_request_head *reqhead;
	struct ceph_osd_request *req;
	int rc = 0;
	
	/* request + msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_WRITE);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	req = alloc_request(nr_pages, reqm);
	if (IS_ERR(req)) {
		ceph_msg_put(reqm);
		return PTR_ERR(req);
	}

	reqhead = reqm->front.iov_base;
	reqhead->flags = CEPH_OSD_OP_ACK;    /* just ACK for now */

	len = calc_layout(osdc, ino, layout, off, len, req);
	nr_pages = calc_pages_for(off, len);
	dout(10, "writepages %llu~%llu -> %d pages\n", off, len, nr_pages);
	
	/* copy pages */
	memcpy(req->r_pages, pages, nr_pages * sizeof(struct page *));
	reqm->pages = req->r_pages;
	reqm->nr_pages = req->r_nr_pages = nr_pages;
	reqm->hdr.data_len = len;
	reqm->hdr.data_off = off;

	rc = do_request(osdc, req);
	put_request(req);
	if (rc == 0)
		rc = len;
	dout(10, "writepages result %d\n", rc);
	return rc;
}


