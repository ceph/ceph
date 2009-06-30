
#include <linux/backing-dev.h>
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/pagemap.h>
#include <linux/writeback.h>	/* generic_writepages */
#include <linux/pagevec.h>
#include <linux/task_io_accounting_ops.h>

#include "ceph_debug.h"
int ceph_debug_addr __read_mostly = -1;
#define DOUT_MASK DOUT_MASK_ADDR
#define DOUT_VAR ceph_debug_addr
#include "super.h"

#include "osd_client.h"

/*
 * There are a few funny things going on here.
 *
 * The page->private field is used to reference a struct
 * ceph_snap_context for _every_ dirty page.  This indicates which
 * snapshot the page was logically dirtied in, and thus which snap
 * context needs to be associated with the osd write during writeback.
 *
 * Similarly, struct ceph_inode_info maintains a set of counters to
 * count dirty pages on the inode.  In the absense of snapshots,
 * i_wrbuffer_ref == i_wrbuffer_ref_head == the dirty page count.
 *
 * A snapshot is taken (that is, when the client receives notification
 * that a snapshot was taken), each inode with caps and with dirty
 * pages (dirty pages implies there is a cap) gets a new ceph_cap_snap
 * in the i_cap_snaps (which is sorted in ascending order, new snaps
 * go to the tail).  The i_wrbuffer_ref_head count is moved to
 * capsnap->dirty. (Unless a sync write is currently in progress.  In
 * that case, the capsnap is said to be "pending", new writes cannot
 * start, and the capsnap isn't "finalized" until the write completes
 * (or fails) and a final size/mtime for the inode for that snap can
 * be settled upon.)  i_wrbuffer_ref_head is reset to 0.
 *
 * On writeback, we must submit writes to the osd IN SNAP ORDER.  So,
 * we look for the first capsnap in i_cap_snaps and write out pages in
 * that snap context _only_.  Then we move on to the next capsnap,
 * eventually reachings the "live" or "head" context (i.e., pages that
 * are not yet snapped) and are writing the most recently dirtied
 * pages.
 *
 * Invalidate and so forth must take care to ensure the dirty page
 * accounting is preserved.
 */


/*
 * Dirty a page.  If @snapc is NULL, use the current snap context for
 * i_snap_realm.  Otherwise, redirty a page within the context of
 * the given *snapc.
 *
 * Caller may or may not have locked *page.  That means we can race
 * with truncate_complete_page and end up with a non-dirty page with
 * private data.
 */
static int ceph_set_page_dirty(struct page *page)
{
	struct address_space *mapping = page->mapping;
	struct inode *inode;
	struct ceph_inode_info *ci;
	int undo = 0;
	struct ceph_snap_context *snapc;

	if (unlikely(!mapping))
		return !TestSetPageDirty(page);

	if (TestSetPageDirty(page)) {
		dout(20, "%p set_page_dirty %p idx %lu -- already dirty\n",
		     mapping->host, page, page->index);
		return 0;
	}

	/*
	 * optimistically adjust accounting, on the assumption that
	 * we won't race with invalidate.
	 */
	inode = mapping->host;
	ci = ceph_inode(inode);

	/*
	 * Note that we're grabbing a snapc ref here without holding
	 * any locks!
	 */
	snapc = ceph_get_snap_context(ci->i_snap_realm->cached_context);

	/* dirty the head */
	spin_lock(&inode->i_lock);
	if (ci->i_wrbuffer_ref_head == 0)
		ci->i_head_snapc = ceph_get_snap_context(snapc);
	++ci->i_wrbuffer_ref_head;
	++ci->i_wrbuffer_ref;
	dout(20, "%p set_page_dirty %p idx %lu head %d/%d -> %d/%d "
	     "snapc %p seq %lld (%d snaps)\n",
	     mapping->host, page, page->index,
	     ci->i_wrbuffer_ref-1, ci->i_wrbuffer_ref_head-1,
	     ci->i_wrbuffer_ref, ci->i_wrbuffer_ref_head,
	     snapc, snapc->seq, snapc->num_snaps);
	spin_unlock(&inode->i_lock);

	/* now adjust page */
	spin_lock_irq(&mapping->tree_lock);
	if (page->mapping) {	/* Race with truncate? */
		WARN_ON_ONCE(!PageUptodate(page));

		if (mapping_cap_account_dirty(mapping)) {
			__inc_zone_page_state(page, NR_FILE_DIRTY);
			__inc_bdi_stat(mapping->backing_dev_info,
					BDI_RECLAIMABLE);
			task_io_account_write(PAGE_CACHE_SIZE);
		}
		radix_tree_tag_set(&mapping->page_tree,
				page_index(page), PAGECACHE_TAG_DIRTY);

		/*
		 * Reference snap context in page->private.  Also set
		 * PagePrivate so that we get invalidatepage callback.
		 */
		page->private = (unsigned long)snapc;
		SetPagePrivate(page);
	} else {
		dout(20, "ANON set_page_dirty %p (raced truncate?)\n", page);
		undo = 1;
	}

	spin_unlock_irq(&mapping->tree_lock);

	if (undo)
		ceph_put_wrbuffer_cap_refs(ci, 1, snapc);

	__mark_inode_dirty(mapping->host, I_DIRTY_PAGES);

	BUG_ON(!PageDirty(page));
	return 1;
}

/*
 * If we are truncating the full page (i.e. offset == 0), adjust the
 * dirty page counters appropriately.  Only called if there is private
 * data on the page.
 */
static void ceph_invalidatepage(struct page *page, unsigned long offset)
{
	struct inode *inode = page->mapping->host;
	struct ceph_inode_info *ci;
	struct ceph_snap_context *snapc = (void *)page->private;

	BUG_ON(!PageLocked(page));
	BUG_ON(!page->private);
	BUG_ON(!PagePrivate(page));
	BUG_ON(!page->mapping);

	/*
	 * We can get non-dirty pages here due to races between
	 * set_page_dirty and truncate_complete_page; just spit out a
	 * warning, in case we end up with accounting problems later.
	 */
	if (!PageDirty(page))
		dout(0, "%p invalidatepage %p page not dirty\n", inode, page);

	if (offset == 0)
		ClearPageChecked(page);

	ci = ceph_inode(inode);
	if (offset == 0) {
		dout(20, "%p invalidatepage %p idx %lu full dirty page %lu\n",
		     inode, page, page->index, offset);
		ceph_put_wrbuffer_cap_refs(ci, 1, snapc);
		ceph_put_snap_context(snapc);
		page->private = 0;
		ClearPagePrivate(page);
	} else {
		dout(20, "%p invalidatepage %p idx %lu partial dirty page\n",
		     inode, page, page->index);
	}
}

/* just a sanity check */
static int ceph_releasepage(struct page *page, gfp_t g)
{
	struct inode *inode = page->mapping ? page->mapping->host : NULL;
	dout(20, "%p releasepage %p idx %lu\n", inode, page, page->index);
	WARN_ON(PageDirty(page));
	WARN_ON(page->private);
	WARN_ON(PagePrivate(page));
	return 0;
}

/*
 * read a single page, without unlocking it.
 */
static int readpage_nounlock(struct file *filp, struct page *page)
{
	struct inode *inode = filp->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	int err = 0;

	dout(10, "readpage inode %p file %p page %p index %lu\n",
	     inode, filp, page, page->index);
	err = ceph_osdc_readpages(osdc, ceph_vino(inode), &ci->i_layout,
				  page->index << PAGE_SHIFT, PAGE_SIZE,
				  ci->i_truncate_seq, ci->i_truncate_size,
				  &page, 1);
	if (unlikely(err < 0)) {
		SetPageError(page);
		goto out;
	}
	SetPageUptodate(page);

out:
	return err < 0 ? err : 0;
}

static int ceph_readpage(struct file *filp, struct page *page)
{
	int r = readpage_nounlock(filp, page);
	unlock_page(page);
	return r;
}

/*
 * Build a vector of contiguous pages from the provided page list.
 */
static struct page **page_vector_from_list(struct list_head *page_list,
					   unsigned *nr_pages)
{
	struct page **pages;
	struct page *page;
	int next_index, contig_pages = 0;

	/* build page vector */
	pages = kmalloc(sizeof(*pages) * *nr_pages, GFP_NOFS);
	if (!pages)
		return ERR_PTR(-ENOMEM);

	BUG_ON(list_empty(page_list));
	next_index = list_entry(page_list->prev, struct page, lru)->index;
	list_for_each_entry_reverse(page, page_list, lru) {
		if (page->index == next_index) {
			dout(20, "readpages page %d %p\n", contig_pages, page);
			pages[contig_pages] = page;
			contig_pages++;
			next_index++;
		} else {
			break;
		}
	}
	*nr_pages = contig_pages;
	return pages;
}

/*
 * Read multiple pages.  Leave pages we don't read + unlock in page_list;
 * the caller (VM) cleans them up.
 */
static int ceph_readpages(struct file *file, struct address_space *mapping,
			  struct list_head *page_list, unsigned nr_pages)
{
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	int rc = 0;
	struct page **pages;
	struct pagevec pvec;
	loff_t offset;

	dout(10, "readpages %p file %p nr_pages %d\n",
	     inode, file, nr_pages);

	pages = page_vector_from_list(page_list, &nr_pages);
	if (IS_ERR(pages))
		return PTR_ERR(pages);

	/* guess read extent */
	offset = pages[0]->index << PAGE_CACHE_SHIFT;
	rc = ceph_osdc_readpages(osdc, ceph_vino(inode), &ci->i_layout,
				 offset, nr_pages << PAGE_CACHE_SHIFT,
				 ci->i_truncate_seq, ci->i_truncate_size,
				 pages, nr_pages);
	if (rc < 0)
		goto out;

	/* set uptodate and add to lru in pagevec-sized chunks */
	pagevec_init(&pvec, 0);
	for (; rc > 0; rc -= PAGE_CACHE_SIZE) {
		struct page *page;

		BUG_ON(list_empty(page_list));
		page = list_entry(page_list->prev, struct page, lru);
		list_del(&page->lru);

		if (add_to_page_cache(page, mapping, page->index, GFP_NOFS)) {
			page_cache_release(page);
			dout(20, "readpages %p add_to_page_cache failed %p\n",
			     inode, page);
			continue;
		}
		dout(10, "readpages %p adding %p idx %lu\n", inode, page,
		     page->index);
		flush_dcache_page(page);
		SetPageUptodate(page);
		unlock_page(page);
		if (pagevec_add(&pvec, page) == 0)
			pagevec_lru_add_file(&pvec);   /* add to lru */
	}
	pagevec_lru_add_file(&pvec);
	rc = 0;

out:
	kfree(pages);
	return rc;
}

/*
 * Get ref for the oldest snapc for an inode with dirty data... that is, the
 * only snap context we are allowed to write back.
 *
 * Caller holds i_lock.
 */
static struct ceph_snap_context *__get_oldest_context(struct inode *inode,
						      u64 *snap_size)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_snap_context *snapc = NULL;
	struct ceph_cap_snap *capsnap = NULL;

	list_for_each_entry(capsnap, &ci->i_cap_snaps, ci_item) {
		dout(20, " cap_snap %p snapc %p has %d dirty pages\n", capsnap,
		     capsnap->context, capsnap->dirty_pages);
		if (capsnap->dirty_pages) {
			snapc = ceph_get_snap_context(capsnap->context);
			if (snap_size)
				*snap_size = capsnap->size;
			break;
		}
	}
	if (!snapc && ci->i_snap_realm) {
		snapc = ceph_get_snap_context(ci->i_snap_realm->cached_context);
		dout(20, " head snapc %p has %d dirty pages\n",
		     snapc, ci->i_wrbuffer_ref_head);
	}
	return snapc;
}

static struct ceph_snap_context *get_oldest_context(struct inode *inode,
						    u64 *snap_size)
{
	struct ceph_snap_context *snapc = NULL;

	spin_lock(&inode->i_lock);
	snapc = __get_oldest_context(inode, snap_size);
	spin_unlock(&inode->i_lock);
	return snapc;
}

/*
 * Write a single page, but leave the page locked.
 *
 * If we get a write error, set the page error bit, but still adjust the
 * dirty page accounting (i.e., page is no longer dirty).
 *
 * FIXME: Is that the right thing to do?
 */
static int writepage_nounlock(struct page *page, struct writeback_control *wbc)
{
	struct inode *inode;
	struct ceph_inode_info *ci;
	struct ceph_osd_client *osdc;
	loff_t page_off = page->index << PAGE_CACHE_SHIFT;
	int len = PAGE_CACHE_SIZE;
	loff_t i_size;
	int err = 0;
	struct ceph_snap_context *snapc;
	u64 snap_size = 0;

	dout(10, "writepage %p idx %lu\n", page, page->index);

	if (!page->mapping || !page->mapping->host) {
		dout(10, "writepage %p - no mapping\n", page);
		return -EFAULT;
	}
	inode = page->mapping->host;
	ci = ceph_inode(inode);
	osdc = &ceph_inode_to_client(inode)->osdc;

	/* verify this is a writeable snap context */
	snapc = (void *)page->private;
	if (snapc == NULL) {
		dout(20, "writepage %p page %p not dirty?\n", inode, page);
		goto out;
	}
	if (snapc != get_oldest_context(inode, &snap_size)) {
		dout(10, "writepage %p page %p snapc %p not writeable - noop\n",
		     inode, page, (void *)page->private);
		/* we should only noop if called by kswapd */
		WARN_ON((current->flags & PF_MEMALLOC) == 0);
		goto out;
	}

	/* is this a partial page at end of file? */
	if (snap_size)
		i_size = snap_size;
	else
		i_size = i_size_read(inode);
	if (i_size < page_off + len)
		len = i_size - page_off;

	dout(10, "writepage %p page %p index %lu on %llu~%u\n",
	     inode, page, page->index, page_off, len);

	set_page_writeback(page);
	err = ceph_osdc_writepages(osdc, ceph_vino(inode),
				   &ci->i_layout, snapc,
				   page_off, len,
				   ci->i_truncate_seq, ci->i_truncate_size,
				   &inode->i_mtime,
				   &page, 1, 0, 0);
	if (err < 0) {
		dout(20, "writepage setting page error %p\n", page);
		SetPageError(page);
		if (wbc)
			wbc->pages_skipped++;
	} else {
		dout(20, "writepage cleaned page %p\n", page);
		err = 0;  /* vfs expects us to return 0 */
	}
	page->private = 0;
	ClearPagePrivate(page);
	end_page_writeback(page);
	ceph_put_wrbuffer_cap_refs(ci, 1, snapc);
	ceph_put_snap_context(snapc);
out:
	return err;
}

static int ceph_writepage(struct page *page, struct writeback_control *wbc)
{
	int err = writepage_nounlock(page, wbc);
	unlock_page(page);
	return err;
}


/*
 * lame release_pages helper.  release_pages() isn't exported to
 * modules.
 */
static void ceph_release_pages(struct page **pages, int num)
{
	struct pagevec pvec;
	int i;
	pagevec_init(&pvec, 0);
	for (i = 0; i < num; i++) {
		if (pagevec_add(&pvec, pages[i]) == 0)
			pagevec_release(&pvec);
	}
	pagevec_release(&pvec);
}


/*
 * async writeback completion handler.
 *
 * If we get an error, set the mapping error bit, but not the individual
 * page error bits.
 *
 * FIXME: What should we be doing here?
 */
static void writepages_finish(struct ceph_osd_request *req)
{
	struct inode *inode = req->r_inode;
	struct ceph_osd_reply_head *replyhead;
	struct ceph_osd_op *op;
	struct ceph_inode_info *ci = ceph_inode(inode);
	unsigned wrote;
	loff_t offset = req->r_pages[0]->index << PAGE_CACHE_SHIFT;
	struct page *page;
	int i;
	struct ceph_snap_context *snapc = req->r_snapc;
	struct address_space *mapping = inode->i_mapping;
	struct writeback_control *wbc = req->r_wbc;
	__s32 rc = -EIO;
	u64 bytes = 0;

	/* parse reply */
	if (req->r_reply) {
		replyhead = req->r_reply->front.iov_base;
		WARN_ON(le32_to_cpu(replyhead->num_ops) == 0);
		op = (void *)(replyhead + 1);
		rc = le32_to_cpu(replyhead->result);
		bytes = le64_to_cpu(op->length);
	}

	if (rc >= 0) {
		wrote = (bytes + (offset & ~PAGE_CACHE_MASK) + ~PAGE_CACHE_MASK)
			>> PAGE_CACHE_SHIFT;
		WARN_ON(wrote != req->r_num_pages);
	} else {
		wrote = 0;
		mapping_set_error(mapping, rc);
	}
	dout(10, "writepages_finish %p rc %d bytes %llu wrote %d (pages)\n",
	     inode, rc, bytes, wrote);

	/* clean all pages */
	for (i = 0; i < req->r_num_pages; i++) {
		page = req->r_pages[i];
		BUG_ON(!page);
		WARN_ON(!PageUptodate(page));

		if (i >= wrote) {
			dout(20, "inode %p skipping page %p\n", inode, page);
			wbc->pages_skipped++;
		}
		page->private = 0;
		ClearPagePrivate(page);
		ceph_put_snap_context(snapc);
		dout(50, "unlocking %d %p\n", i, page);
		end_page_writeback(page);
		unlock_page(page);
	}
	dout(20, "%p wrote+cleaned %d pages\n", inode, wrote);
	ceph_put_wrbuffer_cap_refs(ci, req->r_num_pages, snapc);

	ceph_release_pages(req->r_pages, req->r_num_pages);
	kfree(req->r_pages);
	ceph_osdc_put_request(req);
}

/*
 * initiate async writeback
 */
static int ceph_writepages_start(struct address_space *mapping,
				 struct writeback_control *wbc)
{
	struct inode *inode = mapping->host;
	struct backing_dev_info *bdi = mapping->backing_dev_info;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_client *client = ceph_inode_to_client(inode);
	pgoff_t index, start, end;
	int range_whole = 0;
	int should_loop = 1;
	pgoff_t max_pages = 0, max_pages_ever = 0;
	struct ceph_snap_context *snapc = NULL, *last_snapc = NULL;
	struct pagevec *pvec;
	int done = 0;
	int rc = 0;
	unsigned wsize = 1 << inode->i_blkbits;
	struct ceph_osd_request *req = NULL;
	int do_sync;
	u64 snap_size = 0;

	/*
	 * Include a 'sync' in the OSD request if this is a data
	 * integrity write (e.g., O_SYNC write or fsync()), or if our
	 * cap is being revoked.
	 */
	do_sync = wbc->sync_mode == WB_SYNC_ALL;
	if (ceph_caps_revoking(ci, CEPH_CAP_FILE_BUFFER))
		do_sync = 1;
	dout(10, "writepages_start %p dosync=%d (pdflush=%d mode=%s)\n",
	     inode, do_sync, current_is_pdflush(),
	     wbc->sync_mode == WB_SYNC_NONE ? "NONE" :
	     (wbc->sync_mode == WB_SYNC_ALL ? "ALL" : "HOLD"));

	client = ceph_inode_to_client(inode);
	if (client->mount_state == CEPH_MOUNT_SHUTDOWN) {
		dout(1, "writepage_start %p on forced umount\n", inode);
		return -EIO; /* we're in a forced umount, don't write! */
	}
	if (client->mount_args.wsize && client->mount_args.wsize < wsize)
		wsize = client->mount_args.wsize;
	if (wsize < PAGE_CACHE_SIZE)
		wsize = PAGE_CACHE_SIZE;
	max_pages_ever = wsize >> PAGE_CACHE_SHIFT;

	pvec = kmalloc(sizeof(*pvec), GFP_KERNEL);
	pagevec_init(pvec, 0);

	/* ?? */
	if (wbc->nonblocking && bdi_write_congested(bdi)) {
		dout(20, " writepages congested\n");
		wbc->encountered_congestion = 1;
		goto out_free;
	}

	/* where to start/end? */
	if (wbc->range_cyclic) {
		start = mapping->writeback_index; /* Start from prev offset */
		end = -1;
		dout(20, " cyclic, start at %lu\n", start);
	} else {
		start = wbc->range_start >> PAGE_CACHE_SHIFT;
		end = wbc->range_end >> PAGE_CACHE_SHIFT;
		if (wbc->range_start == 0 && wbc->range_end == LLONG_MAX)
			range_whole = 1;
		should_loop = 0;
		dout(20, " not cyclic, %lu to %lu\n", start, end);
	}
	index = start;

retry:
	/* find oldest snap context with dirty data */
	ceph_put_snap_context(snapc);
	snapc = get_oldest_context(inode, &snap_size);
	if (!snapc) {
		/* hmm, why does writepages get called when there
		   is no dirty data? */
		dout(20, " no snap context with dirty data?\n");
		goto out;
	}
	dout(20, " oldest snapc is %p seq %lld (%d snaps)\n",
	     snapc, snapc->seq, snapc->num_snaps);
	if (last_snapc && snapc != last_snapc) {
		/* if we switched to a newer snapc, restart our scan at the
		 * start of the original file range. */
		dout(20, "  snapc differs from last pass, restarting at %lu\n",
		     index);
		index = start;
	}
	last_snapc = snapc;

	while (!done && index <= end) {
		unsigned i;
		int first;
		pgoff_t next;
		int pvec_pages, locked_pages;
		struct page *page;
		int want;
		u64 offset, len;
		struct ceph_osd_request_head *reqhead;
		struct ceph_osd_op *op;

		next = 0;
		locked_pages = 0;
		max_pages = max_pages_ever;

get_more_pages:
		first = -1;
		want = min(end - index,
			   min((pgoff_t)PAGEVEC_SIZE,
			       max_pages - (pgoff_t)locked_pages) - 1)
			+ 1;
		pvec_pages = pagevec_lookup_tag(pvec, mapping, &index,
						PAGECACHE_TAG_DIRTY,
						want);
		dout(20, "pagevec_lookup_tag got %d\n", pvec_pages);
		if (!pvec_pages && !locked_pages)
			break;
		for (i = 0; i < pvec_pages && locked_pages < max_pages; i++) {
			page = pvec->pages[i];
			dout(20, "? %p idx %lu\n", page, page->index);
			if (locked_pages == 0)
				lock_page(page);  /* first page */
			else if (!trylock_page(page))
				break;

			/* only dirty pages, or our accounting breaks */
			if (unlikely(!PageDirty(page)) ||
			    unlikely(page->mapping != mapping)) {
				dout(20, "!dirty or !mapping %p\n", page);
				unlock_page(page);
				break;
			}
			if (!wbc->range_cyclic && page->index > end) {
				dout(20, "end of range %p\n", page);
				done = 1;
				unlock_page(page);
				break;
			}
			if (next && (page->index != next)) {
				dout(20, "not consecutive %p\n", page);
				unlock_page(page);
				break;
			}
			if (wbc->sync_mode != WB_SYNC_NONE) {
				dout(20, "waiting on writeback %p\n", page);
				wait_on_page_writeback(page);
			}
			if ((snap_size && page_offset(page) > snap_size) ||
			    (!snap_size &&
			     page_offset(page) > i_size_read(inode))) {
				dout(20, "%p page eof %llu\n", page, snap_size ?
				     snap_size : i_size_read(inode));
				done = 1;
				unlock_page(page);
				break;
			}
			if (PageWriteback(page)) {
				dout(20, "%p under writeback\n", page);
				unlock_page(page);
				break;
			}

			/* only if matching snap context */
			if (snapc != (void *)page->private) {
				dout(20, "page snapc %p != oldest %p\n",
				     (void *)page->private, snapc);
				unlock_page(page);
				if (!locked_pages)
					continue; /* keep looking for snap */
				break;
			}

			if (!clear_page_dirty_for_io(page)) {
				dout(20, "%p !clear_page_dirty_for_io\n", page);
				unlock_page(page);
				break;
			}

			/* ok */
			if (locked_pages == 0) {
				/* prepare async write request */
				offset = page->index << PAGE_CACHE_SHIFT;
				len = wsize;
				req = ceph_osdc_new_request(&client->osdc,
					    &ci->i_layout,
					    ceph_vino(inode),
					    offset, &len,
					    CEPH_OSD_OP_WRITE,
					    CEPH_OSD_FLAG_WRITE |
						    CEPH_OSD_FLAG_ONDISK,
					    snapc, do_sync,
					    ci->i_truncate_seq,
					    ci->i_truncate_size,
					    &inode->i_mtime);
				max_pages = req->r_num_pages;

				rc = -ENOMEM;
				req->r_pages = kmalloc(sizeof(*req->r_pages) *
						       max_pages, GFP_NOFS);
				if (req->r_pages == NULL)
					goto out;
				req->r_callback = writepages_finish;
				req->r_inode = inode;
				req->r_wbc = wbc;
			}

			/* note position of first page in pvec */
			if (first < 0)
				first = i;
			dout(20, "%p will write page %p idx %lu\n",
			     inode, page, page->index);
			set_page_writeback(page);
			req->r_pages[locked_pages] = page;
			locked_pages++;
			next = page->index + 1;
		}

		/* did we get anything? */
		if (!locked_pages)
			goto release_pvec_pages;
		if (i) {
			int j;
			BUG_ON(!locked_pages || first < 0);

			if (pvec_pages && i == pvec_pages &&
			    locked_pages < max_pages) {
				dout(50, "reached end pvec, trying for more\n");
				pagevec_reinit(pvec);
				goto get_more_pages;
			}

			/* shift unused pages over in the pvec...  we
			 * will need to release them below. */
			for (j = i; j < pvec_pages; j++) {
				dout(50, " pvec leftover page %p\n",
				     pvec->pages[j]);
				pvec->pages[j-i+first] = pvec->pages[j];
			}
			pvec->nr -= i-first;
		}

		/* submit the write */
		offset = req->r_pages[0]->index << PAGE_CACHE_SHIFT;
		len = min((snap_size ? snap_size : i_size_read(inode)) - offset,
			  (u64)locked_pages << PAGE_CACHE_SHIFT);
		dout(10, "writepages got %d pages at %llu~%llu\n",
		     locked_pages, offset, len);

		/* revise final length, page count */
		req->r_num_pages = locked_pages;
		reqhead = req->r_request->front.iov_base;
		op = (void *)(reqhead + 1);
		op->length = cpu_to_le64(len);
		op->payload_len = op->length;
		req->r_request->hdr.data_len = cpu_to_le32(len);

		rc = ceph_osdc_start_request(&client->osdc, req);
		req = NULL;
		/*
		 * FIXME: if writepages_start fails (ENOMEM?) we should
		 * really redirty all those pages and release req..
		 */

		/* continue? */
		index = next;
		wbc->nr_to_write -= locked_pages;
		if (wbc->nr_to_write <= 0)
			done = 1;

	release_pvec_pages:
		dout(50, "pagevec_release on %d pages (%p)\n", (int)pvec->nr,
		     pvec->nr ? pvec->pages[0] : NULL);
		pagevec_release(pvec);

		if (locked_pages && !done)
			goto retry;
	}

	if (should_loop && !done) {
		/* more to do; loop back to beginning of file */
		dout(40, "writepages looping back to beginning of file\n");
		should_loop = 0;
		index = 0;
		goto retry;
	}

	if (wbc->range_cyclic || (range_whole && wbc->nr_to_write > 0))
		mapping->writeback_index = index;

out:
	if (req)
		ceph_osdc_put_request(req);
	if (rc > 0)
		rc = 0;  /* vfs expects us to return 0 */
	ceph_put_snap_context(snapc);
	dout(10, "writepages done, rc = %d\n", rc);
out_free:
	kfree(pvec);
	return rc;
}



/*
 * See if a given @snapc is either writeable, or already written.
 */
static int context_is_writeable_or_written(struct inode *inode,
					   struct ceph_snap_context *snapc)
{
	struct ceph_snap_context *oldest = get_oldest_context(inode, NULL);
	return !oldest || snapc->seq <= oldest->seq;
}

/*
 * We are only allowed to write into/dirty the page if the page is
 * clean, or already dirty within the same snap context.
 */
static int ceph_write_begin(struct file *file, struct address_space *mapping,
			    loff_t pos, unsigned len, unsigned flags,
			    struct page **pagep, void **fsdata)
{
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;
	struct page *page;
	pgoff_t index = pos >> PAGE_CACHE_SHIFT;
	loff_t page_off = pos & PAGE_CACHE_MASK;
	int pos_in_page = pos & ~PAGE_CACHE_MASK;
	int end_in_page = pos_in_page + len;
	loff_t i_size;
	struct ceph_snap_context *snapc;
	int r;

	/* get a page*/
retry:
	page = grab_cache_page_write_begin(mapping, index, 0);
	if (!page)
		return -ENOMEM;
	*pagep = page;

	dout(10, "write_begin file %p inode %p page %p %d~%d\n", file,
	     inode, page, (int)pos, (int)len);

retry_locked:
	/* writepages currently holds page lock, but if we change that later, */
	wait_on_page_writeback(page);

	/* check snap context */
	BUG_ON(!ci->i_snap_realm);
	down_read(&mdsc->snap_rwsem);
	BUG_ON(!ci->i_snap_realm->cached_context);
	if (page->private &&
	    (void *)page->private != ci->i_snap_realm->cached_context) {
		/*
		 * this page is already dirty in another (older) snap
		 * context!  is it writeable now?
		 */
		snapc = get_oldest_context(inode, NULL);
		up_read(&mdsc->snap_rwsem);

		if (snapc != (void *)page->private) {
			dout(10, " page %p snapc %p not current or oldest\n",
			     page, (void *)page->private);
			/*
			 * queue for writeback, and wait for snapc to
			 * be writeable or written
			 */
			snapc = ceph_get_snap_context((void *)page->private);
			unlock_page(page);
			if (ceph_queue_writeback(inode))
				igrab(inode);
			wait_event_interruptible(ci->i_cap_wq,
			       context_is_writeable_or_written(inode, snapc));
			ceph_put_snap_context(snapc);
			goto retry;
		}

		/* yay, writeable, do it now (without dropping page lock) */
		dout(10, " page %p snapc %p not current, but oldest\n",
		     page, snapc);
		if (!clear_page_dirty_for_io(page))
			goto retry_locked;
		r = writepage_nounlock(page, NULL);
		if (r < 0)
			goto fail_nosnap;
		goto retry_locked;
	}

	if (PageUptodate(page)) {
		dout(20, " page %p already uptodate\n", page);
		return 0;
	}

	/* full page? */
	if (pos_in_page == 0 && len == PAGE_CACHE_SIZE)
		return 0;

	/* past end of file? */
	i_size = inode->i_size;   /* caller holds i_mutex */

	if (i_size + len > CEPH_FILE_MAX_SIZE) {
		/* file is too big */
		r = -EINVAL;
		goto fail;
	}

	if (page_off >= i_size ||
	    (pos_in_page == 0 && (pos+len) >= i_size &&
	     end_in_page - pos_in_page != PAGE_CACHE_SIZE)) {
		dout(20, " zeroing %p 0 - %d and %d - %d\n",
		     page, pos_in_page, end_in_page, (int)PAGE_CACHE_SIZE);
		zero_user_segments(page,
				   0, pos_in_page,
				   end_in_page, PAGE_CACHE_SIZE);
		return 0;
	}

	/* we need to read it. */
	up_read(&mdsc->snap_rwsem);
	r = readpage_nounlock(file, page);
	if (r < 0)
		goto fail;
	goto retry_locked;

fail:
	up_read(&mdsc->snap_rwsem);
fail_nosnap:
	unlock_page(page);
	return r;
}

/*
 * we don't do anything in here that simple_write_end doesn't do
 * except adjust dirty page accounting and drop read lock on
 * mdsc->snap_rwsem.
 */
static int ceph_write_end(struct file *file, struct address_space *mapping,
			  loff_t pos, unsigned len, unsigned copied,
			  struct page *page, void *fsdata)
{
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;
	unsigned from = pos & (PAGE_CACHE_SIZE - 1);
	int check_cap = 0;

	dout(10, "write_end file %p inode %p page %p %d~%d (%d)\n", file,
	     inode, page, (int)pos, (int)copied, (int)len);

	/* zero the stale part of the page if we did a short copy */
	if (copied < len)
		zero_user_segment(page, from+copied, len);

	/* did file size increase? */
	/* (no need for i_size_read(); we caller holds i_mutex */
	if (pos+copied > inode->i_size)
		check_cap = ceph_inode_set_size(inode, pos+copied);

	if (!PageUptodate(page))
		SetPageUptodate(page);

	set_page_dirty(page);

	unlock_page(page);
	up_read(&mdsc->snap_rwsem);
	page_cache_release(page);

	if (check_cap)
		ceph_check_caps(ceph_inode(inode), CHECK_CAPS_AUTHONLY, NULL);

	return copied;
}

/*
 * we set .direct_IO to indicate direct io is supported, but since we
 * intercept O_DIRECT reads and writes early, this function should
 * never get called.
 */
static ssize_t ceph_direct_io(int rw, struct kiocb *iocb,
			      const struct iovec *iov,
			      loff_t pos, unsigned long nr_segs)
{
	WARN_ON(1);
	return -EINVAL;
}

const struct address_space_operations ceph_aops = {
	.readpage = ceph_readpage,
	.readpages = ceph_readpages,
	.writepage = ceph_writepage,
	.writepages = ceph_writepages_start,
	.write_begin = ceph_write_begin,
	.write_end = ceph_write_end,
	.set_page_dirty = ceph_set_page_dirty,
	.invalidatepage = ceph_invalidatepage,
	.releasepage = ceph_releasepage,
	.direct_IO = ceph_direct_io,
};


/*
 * vm ops
 */

/*
 * Reuse write_{begin,end} here for simplicity.
 */
static int ceph_page_mkwrite(struct vm_area_struct *vma, struct vm_fault *vmf)
{
	struct inode *inode = vma->vm_file->f_dentry->d_inode;
	struct page *page = vmf->page;
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;
	loff_t off = page->index << PAGE_CACHE_SHIFT;
	loff_t size, len;
	struct page *locked_page = NULL;
	void *fsdata = NULL;
	int ret;

	size = i_size_read(inode);
	if (off + PAGE_CACHE_SIZE <= size)
		len = PAGE_CACHE_SIZE;
	else
		len = size & ~PAGE_CACHE_MASK;

	dout(10, "page_mkwrite %p %llu~%llu page %p idx %lu\n", inode,
	     off, len, page, page->index);
	ret = ceph_write_begin(vma->vm_file, inode->i_mapping, off, len, 0,
			       &locked_page, &fsdata);
	WARN_ON(page != locked_page);
	if (!ret) {
		/*
		 * doing the following, instead of calling
		 * ceph_write_end. Note that we keep the
		 * page locked
		 */
		set_page_dirty(page);
		up_read(&mdsc->snap_rwsem);
		page_cache_release(page);
		ret = VM_FAULT_LOCKED;
	} else {
		ret = VM_FAULT_SIGBUS;
	}
	dout(10, "page_mkwrite %p %llu~%llu = %d\n", inode, off, len, ret);
	return ret;
}

static struct vm_operations_struct ceph_vmops = {
	.fault		= filemap_fault,
	.page_mkwrite	= ceph_page_mkwrite,
};

int ceph_mmap(struct file *file, struct vm_area_struct *vma)
{
	struct address_space *mapping = file->f_mapping;

	if (!mapping->a_ops->readpage)
		return -ENOEXEC;
	file_accessed(file);
	vma->vm_ops = &ceph_vmops;
	vma->vm_flags |= VM_CAN_NONLINEAR;
	return 0;
}
