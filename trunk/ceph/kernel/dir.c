int ceph_dir_debug = 50;
#define DOUT_VAR ceph_dir_debug
#define DOUT_PREFIX "dir: "
#include "super.h"

const struct inode_operations ceph_dir_iops;
const struct file_operations ceph_dir_fops;

static int ceph_dir_open(struct inode *inode, struct file *file)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_cap *cap;
	struct ceph_file_info *fi;

	dout(5, "dir_open inode %p (%lu) file %p\n", inode, inode->i_ino, file);
	cap = ceph_find_cap(inode, 0);
	if (!cap) {
		/* open this inode */
		BUG_ON(1);  // writeme
	}
	
	fi = kzalloc(sizeof(*fi), GFP_KERNEL);
	if (fi == NULL)
		return -ENOMEM;
	file->private_data = fi;

	atomic_inc(&ci->i_cap_count);
	dout(5, "open_dir success\n");
	return 0;
}

static int ceph_dir_readdir(struct file *filp, void *dirent, filldir_t filldir)
{
	struct ceph_file_info *fi = filp->private_data;
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(filp->f_dentry->d_inode)->mdsc;
	u32 frag = filp->f_pos >> 32;
	u32 off = filp->f_pos & 0xfffffffful;
	int err;

	dout(5, "dir_readdir filp %p at frag %u off %u\n", filp, frag, off);
	if (fi->frag != frag || fi->rinfo.reply == NULL) {
		struct ceph_msg *req, *reply;
		/* query mds */
		if (fi->rinfo.reply) 
			ceph_mdsc_destroy_reply_info(&fi->rinfo);
		
		dout(10, "dir_readdir querying mds for frag %u\n", frag);
		req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_READDIR, 
					       filp->f_dentry->d_inode->i_ino, "", 0, 0);
		if (IS_ERR(req)) 
			return PTR_ERR(req);
		reply = ceph_mdsc_do_request(mdsc, req, -1);
		if (IS_ERR(reply))
			return PTR_ERR(reply);
		if ((err = ceph_mdsc_parse_reply_info(reply, &fi->rinfo)) < 0) {
			ceph_mdsc_destroy_reply_info(&fi->rinfo);
			return err;
		}
		dout(10, "dir_readdir got and parse readdir result on frag %u\n", frag);
	}
	
	
	
	return 0;
}

int ceph_dir_release(struct inode *inode, struct file *filp)
{
	struct ceph_inode_info *ci = ceph_inode(inode);

	dout(5, "dir_release inode %p filp %p\n", inode, filp);
	atomic_dec(&ci->i_cap_count);
	kfree(filp->private_data);

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

