
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
	err = ceph_osdc_readpage(osdc, inode->i_ino, &ci->i_layout,
				 page->index << PAGE_SHIFT, PAGE_SIZE, page);
	if (err) goto out_unlock;

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
	
	err = ceph_osdc_readpages(osdc, inode->i_ino, &ci->i_layout,
				  pages, nr_pages);
	if (err < 0) goto out_unlock;

	// hrm
	//SetPageUptodate(page);
out_unlock:
	return err;


}
#endif

const struct address_space_operations ceph_aops = {
	.readpage = ceph_readpage,
//	.readpages = ceph_readpages
	.writepage = ceph_writepage,
};
