
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/pagemap.h>
#include <linux/writeback.h>	/* generic_writepages */

int ceph_debug_addr = 50;
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
	if (err)
		goto out_unlock;

	SetPageUptodate(page);

	/* TODO: update info in ci? */
out_unlock:
	return err;
}

static int ceph_readpages(struct file *file, struct address_space *mapping,
			  struct list_head *pages, unsigned nr_pages)
{
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	int err = 0;

	dout(10, "ceph_readpages inode %p file %p nr_pages %d\n",
	     inode, file, nr_pages);

	err = ceph_osdc_readpages(osdc, mapping, ceph_ino(inode), &ci->i_layout,
				  pages, nr_pages);
	if (err < 0)
		goto out_unlock;

out_unlock:
	return err;


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
	err = ceph_osdc_writepage(osdc, ceph_ino(inode), &ci->i_layout,
				  page_off, len, page);
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
	struct ceph_client *client = ceph_inode_to_client(inode);
	pgoff_t index, end;
	int range_whole = 0;
	int scanned = 0;

	dout(10, "writepages on %p\n", inode);

	/* if wsize is small, write 1 page at a time */
	if (client->mount_args.wsize &&
	    client->mount_args.wsize < PAGE_CACHE_SIZE)
		return generic_writepages(mapping, wbc);

	/* ?? from cifs. */
	/*
	if (wbc->nonblocking && bdi_write_congested(bdi)) {
		wbc->encountered_congestions = 1;
		return 0;
	}
	*/

	return generic_writepages(mapping, wbc);

	/* where to start? */
	if (wbc->range_cyclic) {
		index = mapping->writeback_index; /* Start from prev offset */
		end = -1;
	} else {
		index = wbc->range_start >> PAGE_CACHE_SHIFT;
		end = wbc->range_end >> PAGE_CACHE_SHIFT;
		if (wbc->range_start == 0 && wbc->range_end == LLONG_MAX)
			range_whole = 1;
		scanned = 1;
	}

		

}


/*
 * ceph_prepare_write:
 *  allocate and initialize buffer heads for each page
 */
static int ceph_prepare_write(struct file *filp, struct page *page,
       unsigned from, unsigned to)
{
/*    struct inode *inode = filp->f_dentry->d_inode;*/
	struct inode *inode = page->mapping->host;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	int err = 0;
	loff_t offset, i_size;

	dout(10, "prepare_write file %p inode %p page %p %d~%d\n", filp,
	     inode, page, from, (to-from));

	/*
	err = ceph_wait_for_cap(inode, CEPH_CAP_WR);
	if (err)
		return err;
	*/

	/*
	 *  1. check if page is up to date
	 *  2. If not, read a page to be up to date
	 */

	if (PageUptodate(page))
		return 0;

	/* The given page is already up to date if it's a full page */
	if ((to == PAGE_SIZE) && (from == 0)) {
		SetPageUptodate(page);
		return 0;
	}

	offset = (loff_t)page->index << PAGE_SHIFT;
	i_size = i_size_read(inode);

	if ((offset >= i_size) ||
	    ((from == 0) && (offset + to) >= i_size)) {
		/* data beyond the file end doesn't need to be read */
		simple_prepare_write(filp, page, from, to);
		SetPageUptodate(page);
		return 0;
	}

	/* Now it's clear that the page is not up to date */

	err = ceph_osdc_prepare_write(osdc, ceph_ino(inode), &ci->i_layout,
				      page->index << PAGE_SHIFT, PAGE_SIZE,
				      page);
	if (err)
		goto out_unlock;

	/* TODO: update info in ci? */

out_unlock:
	return err;
}

/*
 * ceph_commit_write:
 *  mark the page as dirty, so that it is written to the disk later
 */
static int ceph_commit_write(struct file *filp, struct page *page,
       unsigned from, unsigned to)
{
/*    struct inode *inode = filp->f_dentry->d_inode;*/
	struct inode *inode = page->mapping->host;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	loff_t position = ((loff_t)page->index << PAGE_SHIFT) + to;
	int err = 0;
	char *page_data;

	dout(10, "commit_write file %p inode %p page %p %d~%d\n", filp,
	     inode, page, from, (to-from));

	spin_lock(&inode->i_lock);
	if (position > inode->i_size)
		i_size_write(inode, position);
	spin_unlock(&inode->i_lock);

	/*
	 *  1. check if page is up to date
	 *  2. If not, make the page up to date by writing a page
	 *  3. If yes, just set the page as dirty
	 */

	if (!PageUptodate(page)) {
		position = ((loff_t)page->index << PAGE_SHIFT) + from;

		page_data = kmap(page);
		err = ceph_osdc_commit_write(osdc, ceph_ino(inode), &ci->i_layout,
					     page->index << PAGE_SHIFT,
					     PAGE_SIZE,
					     page);
		if (err)
			err = 0; /* FIXME: more sophisticated error handling */
		kunmap(page);

		/* TODO: update info in ci? */
	} else {
		/* set the page as up-to-date and mark it as dirty */
		SetPageUptodate(page);
		set_page_dirty(page);
		ci->i_nr_dirty_pages++;
	}

/*out_unlock:*/
	return err;
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
	.write_begin = ceph_write_begin,
	.write_end = ceph_write_end,
	//.prepare_write = ceph_prepare_write,
	//.commit_write = ceph_commit_write,
	.writepage = ceph_writepage,
//     .writepages = ceph_writepages,
//	.set_page_dirty = ceph_set_page_dirty,
	.releasepage = ceph_releasepage,
};
