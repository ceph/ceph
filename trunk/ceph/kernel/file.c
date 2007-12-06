
#include "super.h"

/*
static ssize_t
ceph_file_read(struct file *filp, char __user * data, size_t count,
	       loff_t * offset)
{
}


static ssize_t
ceph_file_write(struct file *filp, const char __user * data,
		size_t count, loff_t * offset)
{
}


int ceph_file_open(struct inode *inode, struct file *file)
{
}


static int ceph_file_lock(struct file *filp, int cmd, struct file_lock *fl)
{
}
*/

const struct inode_operations ceph_file_iops = {
/*	.getattr = ceph_vfs_getattr,
	.setattr = ceph_vfs_setattr,
*/
};

const struct file_operations ceph_file_fops = {
/*	.llseek = generic_file_llseek,
	.read = ceph_file_read,
	.write = ceph_file_write,
	.open = ceph_file_open,
//	.release = ceph_dir_release,
	.lock = ceph_file_lock,
	.mmap = generic_file_mmap,
*/
};
