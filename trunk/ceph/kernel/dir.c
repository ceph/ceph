
#include "super.h"

const struct inode_operations ceph_dir_iops;
const struct file_operations ceph_dir_fops;

static int ceph_dir_open(struct inode *inode, struct file *file)
{
	struct ceph_client *cl = ceph_inode_to_client(inode);
	struct ceph_inode_info *ci = ceph_inode(inode);

	dout(5, "dir_open inode %p (%lu) file %p\n", inode, inode->i_ino, file);
	
	return 0;
}

static int ceph_dir_readdir(struct file *filp, void *dirent, filldir_t filldir)
{
	dout(5, "dir_readdir filp %p\n", filp);

	return 0;
}

int ceph_dir_release(struct inode *inode, struct file *filp)
{
	struct ceph_client *cl = ceph_inode_to_client(inode);
	struct ceph_inode_info *ci = ceph_inode(inode);

	dout(5, "dir_release inode %p filp %p\n", inode, filp);

	return 0;
}

const struct file_operations ceph_dir_fops = {
	.read = generic_read_dir,
	.readdir = ceph_dir_readdir,
	.open = ceph_dir_open,
	.release = ceph_dir_release,
};

static struct dentry *ceph_dir_lookup(struct inode *dir, struct dentry *dentry,
				      struct nameidata *nameidata)
{
	dout(5, "dir_lookup inode %p dentry %p\n", dir, dentry);
	return NULL;
}

/*
static int
ceph_dir_create(struct inode *dir, struct dentry *dentry, int mode,
		struct nameidata *nd)
{
}


static int ceph_dir_unlink(struct inode *i, struct dentry *d)
{
}

static int ceph_dir_mkdir(struct inode *dir, struct dentry *dentry, int mode)
{
}


static int ceph_dir_rmdir(struct inode *i, struct dentry *d)
{
}

static int ceph_dir_rename(struct inode *old_dir, struct dentry *old_dentry,
			   struct inode *new_dir, struct dentry *new_dentry)
{
}
*/

const struct inode_operations ceph_dir_iops = {
	.lookup = ceph_dir_lookup,
//	.getattr = ceph_inode_getattr,
/*	.create = ceph_dir_create,
	.unlink = ceph_dir_unlink,
	.mkdir = ceph_vfs_mkdir,
	.rmdir = ceph_vfs_rmdir,
	.mknod = ceph_vfs_mknod,
	.rename = ceph_vfs_rename,
	.getattr = ceph_vfs_getattr,
	.setattr = ceph_vfs_setattr,
*/
};

