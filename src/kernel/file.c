
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
	char *path;
	int pathlen;
	struct ceph_msg *req;
	struct ceph_mds_request_head *rhead;
	struct ceph_mds_reply_info rinfo;
	struct ceph_mds_session *session;
	struct dentry *dentry;
	int frommds;
	struct ceph_inode_cap *cap;
	int err;

	dentry = list_entry(inode->i_dentry.next, struct dentry, d_alias);

	dout(5, "open inode %p dentry %p name '%s' flags %d\n", inode, dentry, dentry->d_name.name, flags);
	pathbase = inode->i_sb->s_root->d_inode->i_ino;
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path)) 
		return ERR_PTR(PTR_ERR(path));
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_OPEN, pathbase, path, 0, 0);
	kfree(path);
	if (IS_ERR(req)) 
		return ERR_PTR(PTR_ERR(req));
	rhead = req->front.iov_base;
	rhead->args.open.flags = cpu_to_le32(flags);
	if ((err = ceph_mdsc_do_request(mdsc, req, &rinfo, &session)) < 0)
		return ERR_PTR(err);
	
	dout(10, "open got and parsed result\n");
	frommds = rinfo.reply->hdr.src.name.num;
	cap = ceph_add_cap(inode, session, 
			   le32_to_cpu(rinfo.head->file_caps), 
			   le32_to_cpu(rinfo.head->file_caps_seq));
	ceph_mdsc_put_session(session);
	return cap;
}

int ceph_open(struct inode *inode, struct file *file)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_cap *cap = 0;
	struct ceph_file_info *cf;
	int mode;
	int wanted;

	dout(5, "ceph_open inode %p (%lu) file %p\n", inode, inode->i_ino, file);
	/*
	if (file->f_flags == O_DIRECTORY && ... ) 
		cap = ceph_find_cap(inode, 0);
	*/
	if (!cap) {
		cap = ceph_do_open(inode, file);
		if (IS_ERR(cap))
			return PTR_ERR(cap);
	}
	
	cf = kzalloc(sizeof(*cf), GFP_KERNEL);
	if (cf == NULL)
		return -ENOMEM;
	file->private_data = cf;

	atomic_inc(&ci->i_cap_count);

	mode = ceph_file_mode(file->f_flags);
	ci->i_nr_by_mode[mode]++;
	wanted = ceph_caps_wanted(ci);
	ci->i_cap_wanted |= wanted;   /* FIXME this isn't quite right */

	dout(5, "ceph_open success, %lx\n", inode->i_ino);
	return 0;
}

int ceph_release(struct inode *inode, struct file *file)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_file_info *cf = file->private_data;
	int mode, wanted;
	
	dout(5, "ceph_release inode %p file %p\n", inode, file);
	atomic_dec(&ci->i_cap_count);
	
	if (cf->rinfo.reply) 
		ceph_mdsc_destroy_reply_info(&cf->rinfo);
	kfree(cf);

	mode = ceph_file_mode(file->f_flags);
	ci->i_nr_by_mode[mode]--;
	wanted = ceph_caps_wanted(ci);
	dout(10, "mode %d wanted %d was %d\n", mode, wanted, ci->i_cap_wanted);
	if (wanted != ci->i_cap_wanted)
		ceph_mdsc_update_cap_wanted(ci, wanted);

	return 0;
}

const struct inode_operations ceph_file_iops = {
	.setattr = ceph_setattr,
/*	.getattr = ceph_vfs_getattr,
	.setattr = ceph_vfs_setattr,
*/
};


/*
 * totally naive write.  just to get things sort of working.
 */
int ceph_silly_write(struct file *filp, const char __user * data,
		     size_t count, loff_t * offset)
{
	struct inode *inode = filp->f_path.dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_file_info *cf = file->private_data;
	int ret = 0;

	dout(10, "silly_write on file %p %lld~%u\n", filp, offset, count);
	
	/* ignore caps, for now. */
	
	if (ret > 0)
		*offset += ret;

	if (*offset > inode->i_size) {
		inode->i_size = *offset;
		inode->i_blocks = (inode->i_size + 512 - 1) >> 9;
	}	
	invalidate_inode_pages2(inode->i_mapping);
	return ret;
}

const struct file_operations ceph_file_fops = {
	.open = ceph_open,
	.release = ceph_release,
	.llseek = generic_file_llseek,
	.read = do_sync_read,
	.write = do_sync_write,
	.aio_read = generic_file_aio_read,
	.aio_write = generic_file_aio_write,
	.mmap = generic_file_mmap,
/*	.read = ceph_file_read,
	.write = ceph_file_write,
	.open = ceph_file_open,
//	.release = ceph_dir_release,
	.lock = ceph_file_lock,
	.mmap = generic_file_mmap,
*/
};
