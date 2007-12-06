#include <linux/module.h>
#include <linux/fs.h>
#include <linux/smp_lock.h>
#include <linux/slab.h>

#include <linux/ceph_fs.h>

#include "super.h"

const struct inode_operations ceph_symlink_iops;

struct inode *ceph_new_inode(struct super_block *sb, 
			     struct ceph_mds_reply_inode *info) {
	struct inode *inode;
	
	inode = new_inode(sb);
	if (inode == NULL)
		return ERR_PTR(-ENOMEM);
	
	inode->i_ino = le64_to_cpu(info->ino);
	inode->i_mode = le32_to_cpu(info->mode) | S_IFDIR;
	inode->i_uid = le32_to_cpu(info->uid);
	inode->i_gid = le32_to_cpu(info->gid);
	inode->i_nlink = le32_to_cpu(info->nlink);
	inode->i_size = le64_to_cpu(info->size);
	inode->i_rdev = le32_to_cpu(info->rdev);
	inode->i_blocks = 0;
	inode->i_rdev = 0;
	dout(30, "new_inode ino=%lx by %d.%d sz=%llu\n", inode->i_ino,
	     inode->i_uid, inode->i_gid, inode->i_size);
	
	ceph_decode_timespec(&inode->i_atime, &info->atime);
	ceph_decode_timespec(&inode->i_mtime, &info->mtime);
	ceph_decode_timespec(&inode->i_ctime, &info->ctime);

#if 0
	/* ceph inode */
	ci->i_layout = info->layout;  /* swab? */

	if (le32_to_cpu(info->fragtree.nsplits) == 0) {
		ci->i_fragtree = ci->i_fragtree_static;
	} else {
		//ci->i_fragtree = kmalloc(...);
		BUG_ON(1); // write me
	}
	ci->i_fragtree->nsplits = le32_to_cpu(info->fragtree.nsplits);
	for (i=0; i<ci->i_fragtree->nsplits; i++)
		ci->i_fragtree->splits[i] = le32_to_cpu(info->fragtree.splits[i]);

	ci->i_frag_map_nr = 1;
	ci->i_frag_map = ci->i_frag_map_static;
	ci->i_frag_map[0].frag = 0;
	ci->i_frag_map[0].mds = 0; /* fixme */
	
	ci->i_nr_caps = 0;
	ci->i_caps = ci->i_caps_static;
	ci->i_wr_size = 0;
	ci->i_wr_mtime.tv_sec = 0;
	ci->i_wr_mtime.tv_usec = 0;
#endif

	inode->i_mapping->a_ops = &ceph_aops;

	switch (inode->i_mode & S_IFMT) {
	case S_IFIFO:
	case S_IFBLK:
	case S_IFCHR:
	case S_IFSOCK:
		init_special_inode(inode, inode->i_mode, inode->i_rdev);
		break;
	case S_IFREG:
		inode->i_op = &ceph_file_iops;
		inode->i_fop = &ceph_file_fops;
		break;
	case S_IFLNK:
		inode->i_op = &ceph_symlink_iops;
		break;
	case S_IFDIR:
		inc_nlink(inode);
		inode->i_op = &ceph_dir_iops;
		inode->i_fop = &ceph_dir_fops;
		break;
	default:
		derr(0, "BAD mode 0x%x S_IFMT 0x%x\n",
		     inode->i_mode, inode->i_mode & S_IFMT);
		return ERR_PTR(-EINVAL);
	}

	return inode;
}

/*
static int
ceph_vfs_create(struct inode *dir, struct dentry *dentry, int mode,
		struct nameidata *nd)
{
}

static int ceph_vfs_mkdir(struct inode *dir, struct dentry *dentry, int mode)
{
}

static struct dentry *ceph_vfs_lookup(struct inode *dir, struct dentry *dentry,
				      struct nameidata *nameidata)
{
}

static int ceph_vfs_unlink(struct inode *i, struct dentry *d)
{
}

static int ceph_vfs_rmdir(struct inode *i, struct dentry *d)
{
}

static int
ceph_vfs_rename(struct inode *old_dir, struct dentry *old_dentry,
		struct inode *new_dir, struct dentry *new_dentry)
{
}

static int
ceph_vfs_getattr(struct vfsmount *mnt, struct dentry *dentry,
		 struct kstat *stat)
{
}

static int ceph_vfs_setattr(struct dentry *dentry, struct iattr *iattr)
{
}

static int ceph_vfs_readlink(struct dentry *dentry, char __user * buffer,
			     int buflen)
{
}

static void *ceph_vfs_follow_link(struct dentry *dentry, struct nameidata *nd)
{
}

static void ceph_vfs_put_link(struct dentry *dentry, struct nameidata *nd, void *p)
{
}

static int
ceph_vfs_symlink(struct inode *dir, struct dentry *dentry, const char *symname)
{
}

static int
ceph_vfs_link(struct dentry *old_dentry, struct inode *dir,
	      struct dentry *dentry)
{
}

static int
ceph_vfs_mknod(struct inode *dir, struct dentry *dentry, int mode, dev_t rdev)
{
}
*/

const struct inode_operations ceph_symlink_iops = {
/*	.readlink = ceph_vfs_readlink,
	.follow_link = ceph_vfs_follow_link,
	.put_link = ceph_vfs_put_link,
	.getattr = ceph_vfs_getattr,
	.setattr = ceph_vfs_setattr,
*/
};
