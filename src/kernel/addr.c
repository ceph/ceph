
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/pagemap.h>

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
out_unlock:
	unlock_page(page);
	return err;
}

#if 0
static int ceph_readpages(struct file *file, struct address_space *mapping,
			  struct list_head *pages, unsigned nr_pages)
{
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	int err = 0;

	dout(10, "ceph_readpages inode %p file %p nr_pages %d\n",
	     inode, file, nr_pages);

	err = ceph_osdc_readpages(osdc, ceph_ino(inode), &ci->i_layout,
				  pages, nr_pages);
	if (err < 0)
		goto out_unlock;

	// hrm
	//SetPageUptodate(page);
out_unlock:
	return err;


}
#endif


/*
 * ceph_writepage:
 *  clear dirty page, and set the writeback flag in the radix tree.
 *  to actually write data to the remote OSDs.
 */
static int ceph_writepage(struct page *page, struct writeback_control *wbc)
{
       struct inode *inode = page->mapping->host;
       struct ceph_inode_info *ci;
       struct ceph_osd_client *osdc;
       int err = 0;

       if (!page->mapping || !page->mapping->host)
               return -EFAULT;

       ci = ceph_inode(inode);
       osdc = &ceph_inode_to_client(inode)->osdc;

       get_page(page);
       set_page_writeback(page);
       SetPageUptodate(page);

       dout(10, "ceph_writepage inode %p page %p index %lu\n",
               inode, page, page->index);

       err = ceph_osdc_silly_write(osdc, inode->i_ino, &ci->i_layout,
                               PAGE_SIZE, 0, (const char *)page->first_page);
/*    err = ceph_osdc_writepage(osdc, inode->i_ino, &ci->i_layout,*/
/*                page->index << PAGE_SHIFT, PAGE_SIZE, page);*/
       if (err)
               goto out_unlock;

out_unlock:
       unlock_page(page);
       end_page_writeback(page);
       put_page(page);

       return err;
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
	
	/*
	 * TODO:
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
	
	err = ceph_osdc_prepare_write(osdc, inode->i_ino, &ci->i_layout,
				      page->index << PAGE_SHIFT, PAGE_SIZE, page);
	if (err)
		goto out_unlock;
	
out_unlock:
	unlock_page(page);
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
	
	spin_lock(&inode->i_lock);
	if (position > inode->i_size) {
		i_size_write(inode, position);
	}
	spin_unlock(&inode->i_lock);
	
	/*
	 *  1. check if page is up to date
	 *  2. If not, make the page up to date by writing a page
	 *  3. If yes, just set the page as dirty
	 */
	
	if (!PageUptodate(page)) {
		position = ((loff_t)page->index << PAGE_SHIFT) + from;
		
		page_data = kmap(page);
		err = ceph_osdc_commit_write(osdc, inode->i_ino, &ci->i_layout,
					     page->index << PAGE_SHIFT, PAGE_SIZE, page);
		if (err)
			err = 0;        /* FIXME: more sophisticated error handling */
		kunmap(page);
	}
	else {
		/* set the page as up-to-date and mark it as dirty */
		SetPageUptodate(page);
		set_page_dirty(page);
	}
	
/*out_unlock:*/
	unlock_page(page);
	return err;
}


const struct address_space_operations ceph_aops = {
	.readpage = ceph_readpage,
//	.readpages = ceph_readpages
//	.writepage = ceph_writepage,
	.prepare_write = ceph_prepare_write,
	.commit_write = ceph_commit_write,
	.writepage = ceph_writepage,
//     .writepages = ceph_writepages,
};
