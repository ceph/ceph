
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

	dout(10, "ceph_readpage file %p page %p index %lu\n", filp, 
	     page, page->index);
	dout(10, " inode %p\n", inode);
	err = ceph_osdc_readpage(osdc, inode->i_ino, &ci->i_layout,
				 page->index << PAGE_SHIFT, PAGE_SIZE, page);
	if (err) goto out_unlock;

	SetPageUptodate(page);
out_unlock:
	unlock_page(page);
	return err;
}


const struct address_space_operations ceph_aops = {
	.readpage = ceph_readpage,
};
