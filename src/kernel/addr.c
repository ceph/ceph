
#include <linux/backing-dev.h>
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/pagemap.h>
#include <linux/writeback.h>	/* generic_writepages */
#include <linux/pagevec.h>
#include <linux/task_io_accounting_ops.h>

int ceph_debug_addr = -1;
#define DOUT_VAR ceph_debug_addr
#define DOUT_PREFIX "addr: "
#include "super.h"

#include "osd_client.h"

static int ceph_readpage(struct file *filp, struct page *page)
{
	struct inode *inode = filp->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	void *kaddr;
	int err = 0;

	dout(10, "readpage inode %p file %p page %p index %lu\n",
	     inode, filp, page, page->index);
	kaddr = kmap(page);
	err = ceph_osdc_readpage(osdc, ceph_ino(inode), &ci->i_layout,
				 page->index << PAGE_SHIFT, PAGE_SIZE, page);
	if (err < 0)
		goto out;

	if (err < PAGE_CACHE_SIZE) {
		dout(10, "readpage zeroing tail %d bytes of page %p\n",
		     (int)PAGE_CACHE_SIZE - err, page);
		memset(kaddr + err, 0, PAGE_CACHE_SIZE - err);
	}
	SetPageUptodate(page);

out:
	kunmap(page);
	unlock_page(page);
	return err;
}

static int ceph_readpages(struct file *file, struct address_space *mapping,
			  struct list_head *page_list, unsigned nr_pages)
{
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	int rc = 0;
	struct page *page;
	struct pagevec pvec;
	loff_t offset;

	dout(10, "readpages inode %p file %p nr_pages %d\n",
	     inode, file, nr_pages);

	/* guess read extent */
	BUG_ON(list_empty(page_list));
	page = list_entry(page_list->prev, struct page, lru);
	offset = page->index << PAGE_CACHE_SHIFT;
	rc = ceph_osdc_readpages(osdc, mapping, ceph_ino(inode), &ci->i_layout,
				 offset, nr_pages << PAGE_CACHE_SHIFT,
				 page_list, nr_pages);
	if (rc < 0)
		return rc;

	/* set uptodate and add to lru in pagevec-sized chunks */
	pagevec_init(&pvec, 0);
	if (rc > 0)
		rc += offset & ~PAGE_CACHE_MASK;
	for (; rc > 0; rc -= PAGE_CACHE_SIZE) {
		if (list_empty(page_list))
			break;  /* WTF */
		page = list_entry(page_list->prev, struct page, lru);
		list_del(&page->lru);

		if (add_to_page_cache(page, mapping, page->index,
				      GFP_NOFS)) {
			page_cache_release(page);
			dout(20, "readpages add_to_page_cache failed on %p\n",
			     page);
			continue;
		}
		dout(10, "readpages adding page %p\n", page);
		kunmap(page);
		flush_dcache_page(page);
		SetPageUptodate(page);
		unlock_page(page);
		if (pagevec_add(&pvec, page) == 0)
			pagevec_lru_add(&pvec);
	}
	pagevec_lru_add(&pvec);
	return 0;
}


/*
 * ceph_writepage:
 *  clear dirty page, and set the writeback flag in the radix tree.
 *  to actually write data to the remote OSDs.
 */
static int ceph_writepage(struct page *page, struct writeback_control *wbc)
{
	struct inode *inode;
	struct ceph_inode_info *ci;
	struct ceph_osd_client *osdc;
	loff_t page_off = page->index << PAGE_CACHE_SHIFT;
	int len = PAGE_CACHE_SIZE;
	loff_t i_size;
	int err = 0;
	int was_dirty;

	if (!page->mapping || !page->mapping->host)
		return -EFAULT;
	inode = page->mapping->host;
	ci = ceph_inode(inode);
	osdc = &ceph_inode_to_client(inode)->osdc;

	/* is this a partial page at end of file? */
	i_size = i_size_read(inode);
	if (i_size < page_off + len)
		len = i_size - page_off;

	dout(10, "ceph_writepage inode %p page %p index %lu on %llu~%u\n",
	     inode, page, page->index, page_off, len);

	page_cache_get(page);
	was_dirty = PageDirty(page);
	set_page_writeback(page);
	err = ceph_osdc_writepages(osdc, ceph_ino(inode), &ci->i_layout,
				   page_off, len, &page, 1);
	if (err >= 0) {
		if (was_dirty) {
			dout(20, "cleaned page %p\n", page);
			ceph_put_wrbuffer_cap_refs(ci, 1);
		}
		SetPageUptodate(page);
		err = 0;  /* vfs expects us to return 0 */
	} else
		redirty_page_for_writepage(wbc, page);  /* is this right?? */
	unlock_page(page);
	end_page_writeback(page);
	page_cache_release(page);
	return err;
}


/*
 * lame release_pages helper.  release_pages() isn't exported to
 * modules.
 */
void ceph_release_pages(struct page **pages, int num)
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
 * ceph_writepages:
 *  do write jobs for several pages
 */
static int ceph_writepages(struct address_space *mapping,
			   struct writeback_control *wbc)
{
	struct inode *inode = mapping->host;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_client *client = ceph_inode_to_client(inode);
	pgoff_t index, end;
	int range_whole = 0;
	int should_loop = 1;
	struct page **pages;
	pgoff_t max_pages = 0;
	struct pagevec pvec;
	int done = 0;
	int rc = 0;
	unsigned wsize = 1 << inode->i_blkbits;

	if (client->mount_args.wsize && client->mount_args.wsize < wsize)
		wsize = client->mount_args.wsize;

	dout(10, "writepages on %p, wsize %u\n", inode, wsize);

	/* if wsize is small, write 1 page at a time */
	if (wsize < PAGE_CACHE_SIZE)
		return generic_writepages(mapping, wbc);

	/* larger page vector? */
	max_pages = wsize >> PAGE_CACHE_SHIFT;
	if (max_pages > PAGEVEC_SIZE) {
		pages = kmalloc(max_pages * sizeof(*pages), GFP_NOFS);
		if (!pages)
			return generic_writepages(mapping, wbc);
	} else
		pages = 0;
	pagevec_init(&pvec, 0);

	/* ?? from cifs. */
	/*
	if (wbc->nonblocking && bdi_write_congested(bdi)) {
		wbc->encountered_congestions = 1;
		return 0;
	}
	*/

	/* where to start? */
	if (wbc->range_cyclic) {
		index = mapping->writeback_index; /* Start from prev offset */
		end = -1;
		dout(10, "cyclic, start at %lu\n", index);
	} else {
		index = wbc->range_start >> PAGE_CACHE_SHIFT;
		end = wbc->range_end >> PAGE_CACHE_SHIFT;
		if (wbc->range_start == 0 && wbc->range_end == LLONG_MAX)
			range_whole = 1;
		should_loop = 0;
		dout(10, "not cyclic, %lu to %lu\n", index, end);
	}


retry:
	while (!done && index <= end) {
		unsigned i;
		pgoff_t next;
		struct page *page, **pagep;
		int pvec_pages, locked_pages;
		int want;
		int cleaned;

		pagep = pages;
		next = 0;
		locked_pages = 0;
		cleaned = 0;

get_more_pages:
		want = min(end - index,
			   min((pgoff_t)PAGEVEC_SIZE,
			       max_pages - (pgoff_t)locked_pages) - 1) + 1;
		pvec_pages = pagevec_lookup_tag(&pvec, mapping, &index,
						PAGECACHE_TAG_DIRTY,
						want);
		for (i = 0; i < pvec_pages && locked_pages < max_pages; i++) {
			page = pvec.pages[i];
			
			if (locked_pages == 0)
				lock_page(page);
			else if (TestSetPageLocked(page))
				break;

			/* only dirty pages, or wrbuffer accounting breaks! */
			if (unlikely(!PageDirty(page)) ||
			    unlikely(page->mapping != mapping)) {
				unlock_page(page);
				break;
			}
			if (!wbc->range_cyclic && page->index > end) {
				done = 1;
				unlock_page(page);
				break;
			}
			if (next && (page->index != next)) {
				/* Not next consecutive page */
				unlock_page(page);
				break;
			}
			if (wbc->sync_mode != WB_SYNC_NONE)
				wait_on_page_writeback(page);

			if (page_offset(page) >= i_size_read(inode)) {
				done = 1;
				unlock_page(page);
				break;
			}

			if (PageWriteback(page) ||
			    !clear_page_dirty_for_io(page)) {
				unlock_page(page);
				break;
			}

			/* ok */
			set_page_writeback(page);
			cleaned++;

			dout(20, "%p locked+cleaned page %p idx %lu\n",
			     inode, page, page->index);
			
			if (pages)
				pages[locked_pages] = page;
			else if (locked_pages == 0)
				pagep = pvec.pages + i;
			locked_pages++;
			next = page->index + 1;
		}

		if (locked_pages == 0)
			break;

		if (pages) {
			int j;
			if (pvec_pages && i == pvec_pages &&
			    locked_pages && locked_pages < max_pages) {
				dout(50, "reached end pvec, trying for more\n");
				pagevec_reinit(&pvec);
				goto get_more_pages;
			}

			/* shift unused pages over in the pvec...  we
			 * will need to release them below. */
			for (j = i; j < pvec_pages; j++) {
				dout(50, " pvec leftover page %p\n",
				     pvec.pages[j]);
				pvec.pages[j-i] = pvec.pages[j];
			}
			pvec.nr -= i;
		}

		/* did we get anything? */
		if (locked_pages > 0) {
			loff_t offset = pagep[0]->index << PAGE_CACHE_SHIFT;
			loff_t len = min(i_size_read(inode) - offset,
				 (loff_t)locked_pages << PAGE_CACHE_SHIFT);
			unsigned wrote;
			dout(10, "writepages got %d pages at %llu~%llu\n",
			     locked_pages, offset, len);
			rc = ceph_osdc_writepages(&client->osdc,
						  ceph_ino(inode),
						  &ci->i_layout,
						  offset, len,
						  pagep,
						  locked_pages);
			if (rc >= 0)
				wrote = (rc + (offset & ~PAGE_CACHE_MASK)
					 + ~PAGE_CACHE_MASK)
					>> PAGE_CACHE_SHIFT;
			else
				wrote = 0;
			dout(20, "writepages rc %d wrote %d\n", rc, wrote);

			/* unmap+unlock pages */
			for (i = 0; i < locked_pages; i++) {
				page = pagep[i];
				if (i < wrote)
					SetPageUptodate(page);
				else if (rc < 0) {
					dout(20, "%p redirtying page %p\n", 
					     inode, page);
					redirty_page_for_writepage(wbc, page);
				}
				dout(50, "unlocking %d %p\n", i, page);
				unlock_page(page);
				end_page_writeback(page);
			}
			dout(20, "%p cleaned %d pages\n", inode, cleaned);
			ceph_put_wrbuffer_cap_refs(ci, cleaned);

			/* continue? */
			index = next;
			wbc->nr_to_write -= locked_pages;
			if (wbc->nr_to_write <= 0)
				done = 1;
		}

		if (pages) {
			/* hmm, pagevec_release also does lru_add_drain()...? */
			dout(50, "release_pages on %d\n", locked_pages);
			ceph_release_pages(pages, locked_pages);
		}
		dout(50, "pagevec_release on %d pages\n", (int)pvec.nr);
		pagevec_release(&pvec);
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

	kfree(pages);
	if (rc > 0)
		rc = 0;  /* vfs expects us to return 0 */
	dout(10, "writepages done, rc = %d\n", rc);
	return rc;
}

/*
 * newer write interface
 */
static int ceph_write_begin(struct file *file, struct address_space *mapping,
			    loff_t pos, unsigned len, unsigned flags,
			    struct page **pagep, void **fsdata)
{
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_inode_info *ci;
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	struct page *page;
	pgoff_t index = pos >> PAGE_CACHE_SHIFT;
	loff_t page_off = pos & PAGE_MASK;
	int pos_in_page = pos & ~PAGE_MASK;
	int end_in_page = pos_in_page + len;
	loff_t i_size;
	int r;

	/* get a page*/
	page = __grab_cache_page(mapping, index);
	if (!page)
		return -ENOMEM;
	*pagep = page;

	dout(10, "write_begin file %p inode %p page %p %d~%d\n", file,
	     inode, page, (int)pos, (int)len);

	if (PageUptodate(page))
		return 0;

	/* full page? */
	if (pos_in_page == 0 && len == PAGE_SIZE)
		return 0;

	/* past end of file? */
	i_size = inode->i_size;   /* caller holds i_mutex */
	if (page_off >= i_size ||
	    (pos_in_page == 0 && (pos+len) >= i_size)) {
		simple_prepare_write(file, page, pos_in_page, end_in_page);
		return 0;
	}

	/* we need to read it. */
	/* or, do sub-page granularity dirty accounting? */
	/* try to read the full page */
	ci = ceph_inode(inode);
	r = ceph_osdc_readpage(osdc, ceph_ino(inode), &ci->i_layout,
			       page_off, PAGE_SIZE, page);
	if (r < 0)
		return r;
	if (r < pos_in_page) {
		void *kaddr = kmap_atomic(page, KM_USER1);
		dout(20, "write_begin zeroing pre %d~%d\n", r, pos_in_page-r);
		memset(kaddr+r, 0, pos_in_page-r);
		flush_dcache_page(page);
		kunmap_atomic(kaddr, KM_USER1);
	}
	end_in_page = pos_in_page + len;
	if (end_in_page < PAGE_SIZE && r < PAGE_SIZE) {
		void *kaddr = kmap_atomic(page, KM_USER1);
		dout(20, "write_begin zeroing post %d~%d\n", end_in_page,
		     (int)PAGE_SIZE - end_in_page);
		memset(kaddr+end_in_page, 0, PAGE_SIZE-end_in_page);
		flush_dcache_page(page);
		kunmap_atomic(kaddr, KM_USER1);
	}
	return 0;
}

/*
 * we don't do anything in here that simple_write_end doesn't do
 * except adjust dirty page accounting.
 */
static int ceph_write_end(struct file *file, struct address_space *mapping,
			  loff_t pos, unsigned len, unsigned copied,
			  struct page *page, void *fsdata)
{
	struct inode *inode = file->f_dentry->d_inode;
	unsigned from = pos & (PAGE_CACHE_SIZE - 1);

	dout(10, "write_end file %p inode %p page %p %d~%d (%d)\n", file,
	     inode, page, (int)pos, (int)copied, (int)len);

	/* zero the stale part of the page if we did a short copy */
	if (copied < len) {
		void *kaddr = kmap_atomic(page, KM_USER0);
		memset(kaddr + from + copied, 0, len - copied);
		flush_dcache_page(page);
		kunmap_atomic(kaddr, KM_USER0);
	}

	/* did file size increase? */
	/* (no need for i_size_read(); we caller holds i_mutex */
	if (pos+copied > inode->i_size)
		ceph_inode_set_size(inode, pos+copied);

	if (!PageUptodate(page))
		SetPageUptodate(page);

	set_page_dirty(page);

	unlock_page(page);
	page_cache_release(page);

	return copied;
}


static int ceph_set_page_dirty(struct page *page)
{
	struct address_space *mapping = page->mapping;
	struct ceph_inode_info *ci;

	if (unlikely(!mapping))
		return !TestSetPageDirty(page);

	if (TestSetPageDirty(page)) {
		dout(20, "set_page_dirty %p -- already dirty\n", page);
		return 0;
	}

	write_lock_irq(&mapping->tree_lock);
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

		ci = ceph_inode(mapping->host);
		atomic_inc(&ci->i_wrbuffer_ref);
		dout(20, "set_page_dirty %p %p %d -> %d (?)\n", page,
		     &ci->vfs_inode,
		     atomic_read(&ci->i_wrbuffer_ref)-1,
		     atomic_read(&ci->i_wrbuffer_ref));
	}
	write_unlock_irq(&mapping->tree_lock);
	__mark_inode_dirty(mapping->host, I_DIRTY_PAGES);

	return 1;
}


const struct address_space_operations ceph_aops = {
	.readpage = ceph_readpage,
	.readpages = ceph_readpages,
	.writepage = ceph_writepage,
	.writepages = ceph_writepages,
	.write_begin = ceph_write_begin,
	.write_end = ceph_write_end,
	.set_page_dirty = ceph_set_page_dirty,
};
