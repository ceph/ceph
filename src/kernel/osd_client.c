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

static int ceph_readpage_async(struct ceph_osd_client *osdc,
			  struct ceph_msg *reqm, 
			  struct page *page);
static int ceph_writepage_async(struct ceph_osd_client *osdc, ceph_ino_t ino,
			  struct ceph_file_layout *layout, 
			  loff_t off, loff_t len,
			  char *write_data);

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

	dout(1, "handle_map\n");
	p = msg->front.iov_base;
	end = p + msg->front.iov_len;

	/* incremental maps */
	ceph_decode_32_safe(&p, end, nr_maps, bad);
	dout(10, " %d inc maps\n", nr_maps);
	while (nr_maps--) {
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
			dout(10, "ignoring incremental map %u len %d\n", epoch, maplen);
		}
		p = next;
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

	return req;
}

struct ceph_osd_request *alloc_request(int nr_pages)
{
	struct ceph_osd_request *req;

	req = kmalloc(sizeof(*req) + nr_pages*sizeof(void*), GFP_KERNEL);
	if (req == NULL)
		return ERR_PTR(-ENOMEM);

	return req;
}

struct ceph_osd_request *register_request(struct ceph_osd_client *osdc,
					  struct ceph_msg *msg,
					  int nr_pages,
					  struct ceph_osd_request *req)
{
	struct ceph_osd_request_head *head = msg->front.iov_base;

	req->r_tid = head->tid = ++osdc->last_tid;
	req->r_flags = 0;
	req->r_request = msg;
	req->r_pgid = head->layout.ol_pgid;
	req->r_reply = 0;
	req->r_result = 0;
	atomic_set(&req->r_ref, 2);  /* one for request_tree, one for caller */
	init_completion(&req->r_completion);
	req->r_nr_pages = nr_pages;

	dout(30, "register_request %p tid %lld with %d pages\n", req, req->r_tid, nr_pages);
	radix_tree_insert(&osdc->request_tree, req->r_tid, (void*)req);
	return req;
}

static void send_request(struct ceph_osd_client *osdc, struct ceph_osd_request *req)
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
		dout(10, "send_request no osds in pg are up\n");
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
	if (likely(req->r_nr_pages == want)) {
		dout(10, "prepare_pages tid %llu using existing page vec\n", tid);
		m->pages = req->r_pages;
		m->nr_pages = req->r_nr_pages;
		ret = 0; /* success */
	} else {
		dout(10, "prepare_pages tid %llu have %d pages, reply wants %d\n", 
		     tid, req->r_nr_pages, want);
	}
out:
	spin_unlock(&osdc->lock);
	return ret;
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
	int ret;

	dout(10, "readpage on ino %llx at %lld~%lld\n", ino, off, len);

	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_READ);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	reqhead = reqm->front.iov_base;
	reqhead->oid.ino = ino;
	reqhead->oid.rev = 0;
	calc_file_object_mapping(layout, &off, &len, &reqhead->oid,
				 &reqhead->offset, &reqhead->length);
	BUG_ON(len != 0);
	calc_object_layout(&reqhead->layout, &reqhead->oid, layout, osdc->osdmap);
	dout(10, "readpage object block %u %llu~%llu\n", reqhead->oid.bno, reqhead->offset, reqhead->length);
	
	ret = ceph_readpage_async(osdc, reqm, page);
	if (ret < 0)
		return ret;
	return 0;
}

/*
 * read multiple pages (readahead)
 */

int ceph_osdc_readpages_filler(void *data, struct page *page)
{
	struct ceph_readdesc *pdesc = (struct ceph_readdesc *)data;
	struct ceph_osd_client *osdc = pdesc->osdc;
	struct ceph_file_layout *layout = pdesc->layout;
	loff_t off = pdesc->off;
	loff_t len = pdesc->len;

	struct ceph_osd_request_head *reqhead;
	struct ceph_msg *reqm;
	struct inode *inode = page->mapping->host;
	ceph_ino_t ino = ceph_ino(inode);
	int ret;

	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_READ);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	reqhead = reqm->front.iov_base;
	reqhead->oid.ino = ino;
	reqhead->oid.rev = 0;
	calc_file_object_mapping(layout, &off, &len, &reqhead->oid,
				 &reqhead->offset, &reqhead->length);
	BUG_ON(len != 0);
	calc_object_layout(&reqhead->layout, &reqhead->oid, layout, osdc->osdmap);
	dout(10, "readpage object block %u %llu~%llu\n", reqhead->oid.bno, reqhead->offset, reqhead->length);
	
	ret = ceph_readpage_async(osdc, reqm, page);
	if (ret < 0)
		return ret;

	return 0;
}

/* FIXME: Since this macro list_to_page is already defined in mm/readahead.c,
 *        it's not good to define it again at this point.
 */
#define list_to_page(head) (list_entry((head)->prev, struct page, lru))

int ceph_osdc_readpages(struct ceph_osd_client *osdc,
			struct address_space *mapping,
			ceph_ino_t ino,
			struct ceph_file_layout *layout, 
			struct list_head *pagels, int nr_pages)
{
	struct ceph_msg *reqm, *reply;
	struct ceph_osd_request_head *reqhead;
	struct ceph_osd_request *req;
	struct ceph_osd_reply_head *replyhead;
	__u64 off, len;
	struct ceph_readdesc desc = {
		.osdc = osdc,
		.layout = layout
	};

	/*
	 * for now, our strategy is simple: start with the 
	 * initial page, and fetch as much of that object as
	 * we can that falls within the range specified by 
	 * nr_pages.
	 */

	dout(10, "readpage on ino %llx at %lld~%lld\n", ino, off, len);

	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_READ);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	reqhead = reqm->front.iov_base;
	reqhead->oid.ino = ino;
	reqhead->oid.rev = 0;
	off = list_to_page(pagels)->index << PAGE_SHIFT;
	len = nr_pages << PAGE_SHIFT;
	calc_file_object_mapping(layout, &off, &len, &reqhead->oid,
				 &reqhead->offset, &reqhead->length);
	BUG_ON(len != 0);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 19)
	nr_pages = DIV_ROUND_UP(reqhead->length, PAGE_SIZE);
#else
	nr_pages = (reqhead->length + PAGE_SIZE - 1) / PAGE_SIZE;
#endif
	calc_object_layout(&reqhead->layout, &reqhead->oid, layout, osdc->osdmap);
	dout(10, "readpage object block %u %llu~%llu\n", reqhead->oid.bno, reqhead->offset, reqhead->length);
	
	/* register request */
	req = alloc_request(nr_pages);
	if (IS_ERR(req))
		return PTR_ERR(req);

	spin_lock(&osdc->lock);
	req = register_request(osdc, reqm, nr_pages, req);
	if (IS_ERR(req)) {
		ceph_msg_put(reqm);
		spin_unlock(&osdc->lock);
		return PTR_ERR(req);
	}

	/* prepage pages (add to page cache, request vector) */
	desc.off = off;
	desc.len = len;
	read_cache_pages(mapping, pagels, ceph_osdc_readpages_filler, &desc);

	/* send request */
	reqhead->osdmap_epoch = osdc->osdmap->epoch;
	send_request(osdc, req);
	spin_unlock(&osdc->lock);
	
	/* wait */
	dout(10, "readpage tid %llu waiting for reply on %p\n", req->r_tid, req);
	wait_for_completion(&req->r_completion);
	dout(10, "readpage tid %llu got reply on %p\n", req->r_tid, req);

	spin_lock(&osdc->lock);
	unregister_request(osdc, req);
	spin_unlock(&osdc->lock);

	reply = req->r_reply;
	replyhead = reply->front.iov_base;
	dout(10, "readpage result %d\n", replyhead->result);
	ceph_msg_put(reply);
	put_request(req);
	return 0;
}


/*
 * silly hack.
 */
int ceph_osdc_silly_write(struct ceph_osd_client *osdc, ceph_ino_t ino,
			  struct ceph_file_layout *layout, 
			  __u64 len, __u64 off, const char __user *data)
{
	struct ceph_msg *reqm, *reply;
	struct ceph_osd_request_head *reqhead;
	struct ceph_osd_request *req;
	struct ceph_osd_reply_head *replyhead;
	__u64 toff = off, tlen = len;
	int nrp, i, po, l, left;

	dout(10, "silly_write on ino %llx at %llu~%llu\n", ino, off, len);

	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_WRITE);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	reqhead = reqm->front.iov_base;
	reqhead->oid.ino = ino;
	reqhead->oid.rev = 0;
	reqhead->flags = CEPH_OSD_OP_ACK|CEPH_OSD_OP_SAFE;  /* want them both */
	calc_file_object_mapping(layout, &toff, &tlen, &reqhead->oid,
				 &reqhead->offset, &reqhead->length);
	if (tlen != 0) {
		dout(10, "not writing complete bit.. skipping last %llu, doing %llu~%llu\n", tlen, off, len);
		len -= tlen;
	}
	calc_object_layout(&reqhead->layout, &reqhead->oid, layout, osdc->osdmap);
	dout(10, "silly_write object block %u %llu~%llu\n", reqhead->oid.bno, reqhead->offset, reqhead->length);
	
	/* how many pages? */
	nrp = calc_pages_for(len, off);
	dout(10, "%d~%d -> %d pages\n", (int)off, (int)len, nrp);

	req = alloc_request(nrp);
	if (IS_ERR(req))
		return PTR_ERR(req);

	/* register+send request */
	spin_lock(&osdc->lock);
	req = register_request(osdc, reqm, nrp, req);
	if (IS_ERR(req)) {
		ceph_msg_put(reqm);
		spin_unlock(&osdc->lock);
		return PTR_ERR(req);
	}

	/* data into a set of pages */
	for (i=0; i<nrp; i++)
		req->r_pages[i] = alloc_page(GFP_KERNEL);
	left = len;
	po = off & ~PAGE_MASK;
	for (i=0; i<nrp; i++) {
		l = min_t(int, PAGE_SIZE-po, left);
		copy_from_user(page_address(req->r_pages[i]) + po, data, l);
		data += l;
		left -= l;
		po = 0;
	}
	req->r_request->pages = req->r_pages;
	req->r_request->nr_pages = req->r_nr_pages;
	req->r_request->hdr.data_len = cpu_to_le32(len);
	req->r_request->hdr.data_off = cpu_to_le32(off);

	reqhead->osdmap_epoch = osdc->osdmap->epoch;
	send_request(osdc, req);
	spin_unlock(&osdc->lock);
	
	/* wait */
	dout(10, "silly_write tid %llu waiting for reply on %p\n", req->r_tid, req);
	wait_for_completion(&req->r_completion);
	dout(10, "silly_write tid %llu got reply on %p\n", req->r_tid, req);

	spin_lock(&osdc->lock);
	unregister_request(osdc, req);
	spin_unlock(&osdc->lock);

	reply = req->r_reply;
	replyhead = reply->front.iov_base;
	dout(10, "silly_write result %d, returning %d\n", replyhead->result, (int)len);
	ceph_msg_put(reply);
	put_request(req);
	return (int)len;
}



/*
 * Make a page up to date to be written later to remote OSDs
 */
int ceph_osdc_prepare_write(struct ceph_osd_client *osdc, ceph_ino_t ino,
                      struct ceph_file_layout *layout, 
                      loff_t off, loff_t len,
                      struct page *page)
{
	int rc;
	char *read_data;
	struct ceph_msg *reqm;
	struct ceph_osd_request_head *reqhead;
	
	dout(10, "osdc_prepare_write on ino %llx at %lld~%lld\n", ino, off, len);
	
	get_page(page);
	read_data = kmap(page);
	
	/*
	 *  1. Read a page by calling ceph_osdc_readpage()
	 *  2. Fill the rest bytes by 0, to align by PAGE_SIZE
	 */
	
	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_READ);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	reqhead = reqm->front.iov_base;
	reqhead->oid.ino = ino;
	reqhead->oid.rev = 0;
	calc_file_object_mapping(layout, &off, &len, &reqhead->oid,
				 &reqhead->offset, &reqhead->length);
	BUG_ON(len != 0);
	calc_object_layout(&reqhead->layout, &reqhead->oid, layout, osdc->osdmap);
	dout(10, "readpage object block %u %llu~%llu\n", reqhead->oid.bno, reqhead->offset, reqhead->length);
	
	rc = ceph_readpage_async(osdc, reqm, page);
	if (rc < 0)
		goto io_error;
	
	if (rc < PAGE_SIZE)
		memset(read_data + rc, 0, PAGE_SIZE - rc);
	
	SetPageUptodate(page);
	
	rc = 0;
	
io_error:
	kunmap(page);
	put_page(page);
	return rc;
}

/*
 * Mark a page as dirty to be written into remote OSDs
 */
int ceph_osdc_commit_write(struct ceph_osd_client *osdc, ceph_ino_t ino,
			   struct ceph_file_layout *layout, 
			   loff_t off, loff_t len,
			   struct page *page)
{
	char *write_data;
	int ret = 0;
	
	dout(10, "osdc_commit_write on ino %llx at %lld~%lld\n", ino, off, len);
	
	/*
	 * if it is not up-to-date, then force it to be so.
	 */
	
	write_data = kmap(page);
	write_data += off;
	
	if (len > PAGE_SIZE) {
		kunmap(page);
		return -EIO;
	}
	
	ret = ceph_writepage_async(osdc, ino, layout, off, len, write_data);
	if (ret) {
		kunmap(page);
		return ret;
	}
	
	kunmap(page);
	return ret;
}

/*
 * Do write a single page, sending data to remote OSDs.
 */
int ceph_osdc_writepage(struct ceph_osd_client *osdc, ceph_ino_t ino,
                         struct ceph_file_layout *layout, 
                         loff_t off, loff_t len,
                         struct page *page)
{
	int ret = 0;
	char *write_data;
	
	dout(10, "writepage on ino %llx at %lld~%lld\n", ino, off, len);
	
	write_data = kmap(page);
	write_data += off;
	
	if (len > PAGE_SIZE) {
		kunmap(page);
		return -EIO;
	}
	
	ret = ceph_writepage_async(osdc, ino, layout, off, len, write_data);
	if (ret) {
		kunmap(page);
		return ret;
	}

	kunmap(page);
	return ret;
}


/*
 * do a read job for one page
 */
static int ceph_readpage_async(struct ceph_osd_client *osdc,
			       struct ceph_msg *reqm, 
			       struct page *page)
{
	struct ceph_msg *reply;
	struct ceph_osd_request_head *reqhead;
	struct ceph_osd_request *req;
	struct ceph_osd_reply_head *replyhead;

	reqhead = reqm->front.iov_base;

	req = alloc_request(1);
	if (IS_ERR(req))
		return PTR_ERR(req);

	/* register+send request */
	spin_lock(&osdc->lock);
	req = register_request(osdc, reqm, 1, req);
	if (IS_ERR(req)) {
		ceph_msg_put(reqm);
		spin_unlock(&osdc->lock);
		return PTR_ERR(req);
	}
	req->r_pages[0] = page;
	reqhead->osdmap_epoch = osdc->osdmap->epoch;
	send_request(osdc, req);
	spin_unlock(&osdc->lock);
	
	/* wait */
	dout(10, "readpage tid %llu waiting for reply on %p\n", req->r_tid, req);
	wait_for_completion(&req->r_completion);
	dout(10, "readpage tid %llu got reply on %p\n", req->r_tid, req);

	spin_lock(&osdc->lock);
	unregister_request(osdc, req);
	spin_unlock(&osdc->lock);

	reply = req->r_reply;
	replyhead = reply->front.iov_base;
	dout(10, "readpage result %d\n", replyhead->result);
	ceph_msg_put(reply);
	put_request(req);

	return 0;
}

/*
 * do a write job for one page
 */
static int ceph_writepage_async(struct ceph_osd_client *osdc, ceph_ino_t ino,
			  struct ceph_file_layout *layout, 
			  loff_t off, loff_t len,
			  char *write_data)
{
	struct ceph_msg *reqm, *reply;
	struct ceph_osd_request_head *reqhead;
	struct ceph_osd_request *req;
	struct ceph_osd_reply_head *replyhead;
	__u64 toff = off, tlen = len;
	int po, l;
	int ret = 0;
	
	/* request msg */
	reqm = new_request_msg(osdc, CEPH_OSD_OP_WRITE);
	if (IS_ERR(reqm))
		return PTR_ERR(reqm);
	reqhead = reqm->front.iov_base;
	reqhead->oid.ino = ino;
	reqhead->oid.rev = 0;
	reqhead->flags = CEPH_OSD_OP_ACK|CEPH_OSD_OP_SAFE;  /* want them both */
	dout(10, "writepage_async getting object mapping on %llx %llu~%llu\n",
	     ino, off, len);
	calc_file_object_mapping(layout, &toff, &tlen, &reqhead->oid,
				 &reqhead->offset, &reqhead->length);
	if (tlen != 0) {
		dout(10, "not writing complete bit.. "
		     "skipping last %llu, doing %llu~%llu\n", tlen, off, len);
		len -= tlen;
	}
	calc_object_layout(&reqhead->layout, &reqhead->oid, layout,
			   osdc->osdmap);
	dout(10, "writepage_async object block %u %llu~%llu\n",
	     reqhead->oid.bno, reqhead->offset, reqhead->length);
	
	req = alloc_request(1);
	if (IS_ERR(req))
		return PTR_ERR(req);

	/* register+send request */
	spin_lock(&osdc->lock);
	req = register_request(osdc, reqm, 1, req);
	if (IS_ERR(req)) {
		ceph_msg_put(reqm);
		spin_unlock(&osdc->lock);
		return PTR_ERR(req);
	}
	
	/* copy data into a page in a request message */
	dout(10, "writepage_async alloc\n");
	req->r_pages[0] = alloc_pages(GFP_KERNEL, 1);
	po = off & ~PAGE_MASK;
	
	dout(10, "writepage_async copy\n");
	l = min_t(int, PAGE_SIZE-po, len);
	if (copy_from_user(page_address(req->r_pages[0]) + po, write_data, l)) {
		dout(10, "writepage_async fault\n");
		ret = -EFAULT;
		unregister_request(osdc, req);
		spin_unlock(&osdc->lock);
		goto out;
	}
	
	req->r_request->pages = req->r_pages;
	req->r_request->nr_pages = req->r_nr_pages;
	req->r_request->hdr.data_len = len;
	req->r_request->hdr.data_off = off;
	
	reqhead->osdmap_epoch = osdc->osdmap->epoch;
	dout(10, "writepage_async send\n");
	send_request(osdc, req);
	spin_unlock(&osdc->lock);
	
	/* wait */
	dout(10, "writepage_async tid %llu waiting for reply on %p\n", 
	     req->r_tid, req);
	wait_for_completion(&req->r_completion);
	dout(10, "writepage_async tid %llu got reply on %p\n", req->r_tid, req);
	
	spin_lock(&osdc->lock);
	unregister_request(osdc, req);
	spin_unlock(&osdc->lock);
	
	reply = req->r_reply;
	replyhead = reply->front.iov_base;
	dout(10, "writepage_async result %d, returning %d\n", 
	     replyhead->result, (int)len);
	ceph_msg_put(reply);
	put_request(req);
	
	ret = len;
	
out:
	put_request(req);
	return ret;
}
