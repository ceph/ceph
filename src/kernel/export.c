#include <linux/exportfs.h>

#include "ceph_fs.h"

int ceph_debug_export = -1;
#define DOUT_VAR ceph_debug_export
#define DOUT_PREFIX "export: "
#include "super.h"


int ceph_encode_fh(struct dentry *dentry, __u32 *fh, int *max_len, 
		   int connectable)
{
	struct inode *inode = dentry->d_inode;
	int len = *max_len;
	int type = 1;

	dout(10, "encode_fh %p max_len %d%s\n", dentry, *max_len,
	     connectable ? " connectable":"");
	
	if (len < 2 || (connectable && len < 4))
		return -ENOSPC;

	len = 3;
	*(u64 *)fh = ceph_ino(inode);
	fh[3] = inode->i_generation;
	if (connectable) {
		struct inode *parent;
		spin_lock(&dentry->d_lock);
		parent = dentry->d_parent->d_inode;
		*(u64 *)(fh + 3) = ceph_ino(parent);
		fh[5] = parent->i_generation;
		spin_unlock(&dentry->d_lock);
		len = 6;
		type = 2;
	}
	*max_len = len;
	return type;
}

struct dentry *ceph_fh_to_dentry(struct super_block *sb, struct fid *fid,
				 int fh_len, int fh_type)
{
	u32 *fh = fid->raw;
	u64 ino = *(u64 *)fh;
	u32 gen = fh[2];
	struct inode *inode;
	struct dentry *dentry;

	inode = ceph_find_inode(sb, ino);
	if (!inode) {
		derr(10, "fh_to_dentry %llx.%d -- no inode\n", ino, gen);
		return ERR_PTR(-ESTALE);
	}
	if (inode->i_generation != fh[2]) {
		derr(10, "fh_to_dentry %llx.%d -- %p gen is %d\n", ino, gen,
		     inode, inode->i_generation);
		iput(inode);
		return ERR_PTR(-ESTALE);
	}
	
	dentry = d_alloc_anon(inode);
	if (!dentry) {
		derr(10, "fh_to_dentry %llx.%d -- inode %p but ENOMEM\n", 
		     ino, gen, inode);
		iput(inode);
		return ERR_PTR(-ENOMEM);
	}
	dout(10, "fh_to_dentry %llx.%d -- inode %p dentry %p\n", ino, gen,
	     inode, dentry);
	return dentry;	
}

struct dentry *ceph_fh_to_parent(struct super_block *sb, struct fid *fid,
				 int fh_len, int fh_type)
{
	u32 *fh = fid->raw;
	u64 ino = *(u64 *)(fh + 3);
	u32 gen;
	struct inode *inode;
	struct dentry *dentry;
	
	if (fh_len < 6)
		return ERR_PTR(-ESTALE);
	gen = fh[5];

	inode = ceph_find_inode(sb, ino);
	if (!inode) {
		derr(10, "fh_to_parent %llx.%d -- no inode\n", ino, gen);
		return ERR_PTR(-ESTALE);
	}
	if (inode->i_generation != gen) {
		derr(10, "fh_to_parent %llx.%d -- %p gen is %d\n", ino, gen,
		     inode, inode->i_generation);
		iput(inode);
		return ERR_PTR(-ESTALE);
	}
	
	dentry = d_alloc_anon(inode);
	if (!dentry) {
		derr(10, "fh_to_parent %llx.%d -- inode %p but ENOMEM\n", 
		     ino, gen, inode);
		iput(inode);
		return ERR_PTR(-ENOMEM);
	}
	dout(10, "fh_to_parent %llx.%d -- inode %p dentry %p\n", ino, gen,
	     inode, dentry);
	return dentry;	
}

const struct export_operations ceph_export_ops = {
	.encode_fh = ceph_encode_fh,
	.fh_to_dentry = ceph_fh_to_dentry,
	.fh_to_parent = ceph_fh_to_parent,
};
