
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/pagemap.h>
#include <linux/writeback.h>	/* generic_writepages */
#include <linux/pagevec.h>

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
	int err = 0;

	dout(10, "ceph_readpage inode %p file %p page %p index %lu\n",
	     inode, filp, page, page->index);
	err = ceph_osdc_readpage(osdc, ceph_ino(inode), &ci->i_layout,
				 page->index << PAGE_SHIFT, PAGE_SIZE, page);
	if (err < 0)
		goto out;

	if (err < PAGE_CACHE_SIZE) {
		void *kaddr;
		dout(10, "readpage zeroing tail %d bytes of page %p\n",
		     (int)PAGE_CACHE_SIZE - err, page);
		kaddr = kmap(page);
		memset(kaddr + err, 0, PAGE_CACHE_SIZE - err);
		kunmap(page);
	}
	SetPageUptodate(page);

	/* TODO: update info in ci? */
out:
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
				      GFP_KERNEL)) {
			page_cache_release(page);
			dout(20, "readpages add_to_page_cache failed on %p\n",
			     page);
			continue;
		}
		dout(10, "readpages adding page %p\n", page);
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
	set_page_writeback(page);
	kmap(page);
	err = ceph_osdc_writepages(osdc, ceph_ino(inode), &ci->i_layout,
				   page_off, len, &page, 1);
	kunmap(page);
	if (err >= 0)
		SetPageUptodate(page);
	else
		redirty_page_for_writepage(wbc, page);  /* is this right?? */
	unlock_page(page);
	end_page_writeback(page);
	page_cache_release(page);
	return err;
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
		pages = kmalloc(max_pages * sizeof(*pages), GFP_KERNEL);
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

		pagep = pages;
		next = 0;
		locked_pages = 0;

	get_more_pages:
		want = min(end - index,
			   min((pgoff_t)PAGEVEC_SIZE, 
			       max_pages - (pgoff_t)locked_pages) - 1) + 1;
		pvec_pages = pagevec_lookup_tag(&pvec, mapping, &index,
						PAGECACHE_TAG_DIRTY,
						want);
		dout(30, "lookup_tag want %d got %d pages\n", want, pvec_pages);
	
		for (i = 0; i < pvec_pages; i++) {
			page = pvec.pages[i];
			if (locked_pages == 0) 
				lock_page(page);
			else if (TestSetPageLocked(page))
				break;
			if (unlikely(page->mapping != mapping)) {
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
			if (PageWriteback(page) ||
			    !clear_page_dirty_for_io(page)) {
				unlock_page(page);
				break;
			}

			/* ok */
			set_page_writeback(page);
			if (page_offset(page) >= i_size_read(inode)) {
				done = 1;
				unlock_page(page);
				end_page_writeback(page);
				break;
			}
			/*
			dout(20, "writepages locked page %p index %lu\n",
			     page, page->index);
			*/
			kmap(page);
			if (pages)
				pages[locked_pages] = page;
			else if (locked_pages == 0)
				pagep = pvec.pages + i;
			locked_pages++;
			if (locked_pages == max_pages)
				break;
			next = page->index + 1;
		}

		if (locked_pages == 0)
			break;

		if (pages && pvec_pages && i == pvec_pages &&
		    locked_pages &&
		    locked_pages < max_pages) {
			dout(10, "reached end pvec, trying for more\n");
			pagevec_reinit(&pvec);
			goto get_more_pages;
		}

		/* did we get anything? */
		if (locked_pages > 0) {
			loff_t offset = pagep[0]->index << PAGE_CACHE_SHIFT;
			loff_t len = min(i_size_read(inode) - offset,
				 (loff_t)locked_pages << PAGE_CACHE_SHIFT);
			dout(10, "writepages got %d pages at %llu~%llu\n",
			     locked_pages, offset, len);
			rc = ceph_osdc_writepages(&client->osdc, 
						  ceph_ino(inode), 
						  &ci->i_layout,
						  offset, len,
						  pagep,
						  locked_pages);
			dout(20, "writepages rc %d\n", rc);
						     
			/* unmap+unlock pages */
			if (rc >= 0)
				rc += offset & ~PAGE_CACHE_MASK;
			for (i = 0; i < locked_pages; i++) {
				page = pagep[i];
				if (rc > (i << PAGE_CACHE_SHIFT))
					SetPageUptodate(page);
				else if (rc < 0)
					SetPageError(page);
				kunmap(page);
				unlock_page(page);
				end_page_writeback(page);
			}				

			/* continue? */
			index = next;
			wbc->nr_to_write -= locked_pages;
			if (wbc->nr_to_write <= 0)
				done = 1;
		}
		
		if (pages) {
			/* pagevec_release does lru_add_drain(); ...? */
			release_pages(pages, locked_pages, 0);  /* cold? */
			pagevec_reinit(&pvec);
		} else
			pagevec_release(&pvec);
	}
	
	if (should_loop && !done) {
		/* more to do; loop back to beginning of file */
		dout(10, "writepages looping back to beginning of file\n");
		should_loop = 0;
		index = 0;
		goto retry;
	}
	if (wbc->range_cyclic || (range_whole && wbc->nr_to_write > 0))
		mapping->writeback_index = index;

	kfree(pages);
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
	if (pos_in_page == 0 && len == PAGE_SIZE) {  
		SetPageUptodate(page);
		return 0;
	}
	
	/* past end of file? */
	i_size = i_size_read(inode);
	if (page_off >= i_size || 
	    (pos_in_page == 0 && (pos+len) >= i_size)) {
		simple_prepare_write(file, page, pos_in_page, pos_in_page+len);
		SetPageUptodate(page);
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
		/* we didn't read up to our write start pos, zero the gap */
		void *kaddr = kmap_atomic(page, KM_USER0);
		memset(kaddr+r, 0, pos_in_page-r);
		flush_dcache_page(page);
		kunmap_atomic(kaddr, KM_USER0);
	}
	return 0;
}

static int ceph_write_end(struct file *file, struct address_space *mapping,
			  loff_t pos, unsigned len, unsigned copied,
			  struct page *page, void *fsdata)
{
	struct inode *inode = file->f_dentry->d_inode;

	dout(10, "write_end file %p inode %p page %p %d~%d (%d)\n", file,
	     inode, page, (int)pos, (int)copied, (int)len);

	/* did file size increase? */
	spin_lock(&inode->i_lock);
	if (pos+copied > inode->i_size)
		i_size_write(inode, pos + copied);
	spin_unlock(&inode->i_lock);

	SetPageUptodate(page);
	set_page_dirty(page);
	unlock_page(page);

	return copied;
}



/* generic_perform_write
 * page accounting
 */

static int ceph_set_page_dirty(struct page *page)
{
	struct ceph_inode_info *ci = ceph_inode(page->mapping->host);
	spin_lock(&ci->vfs_inode.i_lock);
	dout(10, "set_page_dirty %p : %d -> %d \n", page,
	     ci->i_nr_dirty_pages, ci->i_nr_dirty_pages + 1);
	ci->i_nr_dirty_pages++;
	spin_lock(&ci->vfs_inode.i_lock);
	return 0;
}

static int ceph_releasepage(struct page *page, gfp_t gfpmask)
{
	struct ceph_inode_info *ci = ceph_inode(page->mapping->host);
	int last = 0;
	spin_lock(&ci->vfs_inode.i_lock);
	dout(10, "releasepage %p gfpmask %d : %d -> %d \n", page, gfpmask,
	     ci->i_nr_pages, ci->i_nr_pages - 1);
	if (--ci->i_nr_pages == 0)
		last++;
	spin_lock(&ci->vfs_inode.i_lock);
	if (last)
		ceph_check_caps_wanted(ci, gfpmask);
	return 0;
}


const struct address_space_operations ceph_aops = {
	.readpage = ceph_readpage,
	.readpages = ceph_readpages,
	.writepage = ceph_writepage,
	.writepages = ceph_writepages,
	.write_begin = ceph_write_begin,
	.write_end = ceph_write_end,
//	.set_page_dirty = ceph_set_page_dirty,
	.releasepage = ceph_releasepage,
};
