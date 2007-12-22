
int ceph_debug_file = 50;
#define DOUT_VAR ceph_debug_file
#define DOUT_PREFIX "file: "
#include "super.h"

#include "mds_client.h"

struct ceph_inode_cap *ceph_do_open(struct inode *inode, struct file *file)
{
	int flags = file->f_flags;
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;
	ceph_ino_t pathbase;
	char path[PATH_MAX];
	int pathlen;
	struct ceph_msg *req;
	struct ceph_mds_request_head *rhead;
	struct ceph_mds_reply_info rinfo;
	struct dentry *dentry;
	int frommds;
	struct ceph_inode_cap *cap;
	int err;

	dentry = list_entry(inode->i_dentry.next, struct dentry, d_alias);

	dout(5, "open inode %p dentry %p name '%s' flags %d\n", inode, dentry, dentry->d_name.name, flags);
	pathbase = inode->i_sb->s_root->d_inode->i_ino;
	pathlen = ceph_get_dentry_path(dentry, path, inode->i_sb->s_root);

	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_OPEN, pathbase, path, 0, 0);
	if (IS_ERR(req)) 
		return ERR_PTR(PTR_ERR(req));
	rhead = req->front.iov_base;
	rhead->args.open.flags = cpu_to_le32(flags);
	if ((err = ceph_mdsc_do_request(mdsc, req, &rinfo, -1)) < 0)
		return ERR_PTR(err);
	
	dout(10, "open got and parsed result\n");
	frommds = rinfo.reply->hdr.src.name.num;
	cap = ceph_add_cap(inode, frommds, 
			   le32_to_cpu(rinfo.head->file_caps), 
			   le32_to_cpu(rinfo.head->file_caps_seq));
	return cap;
}

int ceph_open(struct inode *inode, struct file *file)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_cap *cap;
	struct ceph_file_info *fi;

	dout(5, "dir_open inode %p (%lu) file %p\n", inode, inode->i_ino, file);
	cap = ceph_find_cap(inode, 0);
	if (!cap) {
		cap = ceph_do_open(inode, file);
		if (IS_ERR(cap))
			return PTR_ERR(cap);
	}
	
	fi = kzalloc(sizeof(*fi), GFP_KERNEL);
	if (fi == NULL)
		return -ENOMEM;
	file->private_data = fi;

	atomic_inc(&ci->i_cap_count);
	dout(5, "open_dir success\n");
	return 0;
}

int ceph_release(struct inode *inode, struct file *filp)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_file_info *fi = filp->private_data;

	dout(5, "dir_release inode %p filp %p\n", inode, filp);
	atomic_dec(&ci->i_cap_count);

	if (fi->rinfo.reply) 
		ceph_mdsc_destroy_reply_info(&fi->rinfo);
	kfree(fi);

	return 0;
}

const struct inode_operations ceph_file_iops = {
/*	.getattr = ceph_vfs_getattr,
	.setattr = ceph_vfs_setattr,
*/
};

const struct file_operations ceph_file_fops = {
	.open = ceph_open,
	.release = ceph_release,
/*	.llseek = generic_file_llseek,
	.read = ceph_file_read,
	.write = ceph_file_write,
	.open = ceph_file_open,
//	.release = ceph_dir_release,
	.lock = ceph_file_lock,
	.mmap = generic_file_mmap,
*/
};
