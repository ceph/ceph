
#include "super.h"

const struct inode_operations ceph_dir_iops;
const struct file_operations ceph_dir_fops;

/*
static int ceph_dir_readdir(struct file *filp, void *dirent, filldir_t filldir)
{
}

int ceph_dir_release(struct inode *inode, struct file *filp)
{
}
*/

const struct inode_operations ceph_dir_iops = {
/*	.create = ceph_vfs_create,
	.lookup = ceph_vfs_lookup,
	.unlink = ceph_vfs_unlink,
	.mkdir = ceph_vfs_mkdir,
	.rmdir = ceph_vfs_rmdir,
	.mknod = ceph_vfs_mknod,
	.rename = ceph_vfs_rename,
	.getattr = ceph_vfs_getattr,
	.setattr = ceph_vfs_setattr,
*/
};

const struct file_operations ceph_dir_fops = {
/*	.read = generic_read_dir,
	.readdir = ceph_dir_readdir,
//	.open = ceph_file_open,
	.release = ceph_dir_release,
*/
};
