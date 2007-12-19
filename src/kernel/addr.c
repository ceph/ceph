
#include "super.h"

/*
static int ceph_vfs_readpage(struct file *filp, struct page *page)
{

}
*/

const struct address_space_operations ceph_aops = {
//      .readpage = ceph_vfs_readpage,
};
