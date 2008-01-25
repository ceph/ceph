
int ceph_debug_file = 50;
#define DOUT_VAR ceph_debug_file
#define DOUT_PREFIX "file: "
#include "super.h"

#include "mds_client.h"

/*
 * if err==0, caller is responsible for a put_session on *psession
 */
int do_open_request(struct super_block *sb, struct dentry *dentry, int flags, int create_mode, 
		    struct ceph_mds_session **psession, struct ceph_mds_reply_info *rinfo)
{
	struct ceph_client *client = ceph_sb_to_client(sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	ceph_ino_t pathbase;
	char *path;
	int pathlen;
	struct ceph_msg *req;
	struct ceph_mds_request_head *rhead;
	int err;

	dout(5, "open dentry %p name '%s' flags %d\n", dentry, dentry->d_name.name, flags);
	pathbase = sb->s_root->d_inode->i_ino;
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path)) 
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_OPEN, pathbase, path, 0, 0);
	kfree(path);
	if (IS_ERR(req)) 
		return PTR_ERR(req);
	rhead = req->front.iov_base;
	rhead->args.open.flags = cpu_to_le32(flags);
	rhead->args.open.mode = cpu_to_le32(create_mode);
	if ((err = ceph_mdsc_do_request(mdsc, req, rinfo, psession)) < 0)
		return err;
	return 0;
}

/*
 * add cap.  also set up private_data for holding readdir results
 * if O_DIRECTORY.
 */
int proc_open_reply(struct inode *inode, struct file *file, 
		    struct ceph_mds_session *session, struct ceph_mds_reply_info *rinfo)
{
	struct ceph_inode_cap *cap;
	struct ceph_file_info *cf;

	cap = ceph_add_cap(inode, session, 
			   le32_to_cpu(rinfo->head->file_caps), 
			   le32_to_cpu(rinfo->head->file_caps_seq));

	if (file->f_flags & O_DIRECTORY) {
		cf = kzalloc(sizeof(*cf), GFP_KERNEL);
		if (cf == NULL)
			return -ENOMEM;
		file->private_data = cf;
	}

	return 0;
}

static int ceph_open_init_private_data(struct inode *inode, struct file *file)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_file_info *cf;
	int mode;
	int wanted;

	cf = kzalloc(sizeof(*cf), GFP_KERNEL);
	if (cf == NULL)
		return -ENOMEM;
	file->private_data = cf;

	mode = ceph_file_mode(file->f_flags);
	ci->i_nr_by_mode[mode]++;
	wanted = ceph_caps_wanted(ci);
	ci->i_cap_wanted |= wanted;   /* FIXME this isn't quite right */

	return 0;
}

int ceph_open(struct inode *inode, struct file *file)
{
	struct dentry *dentry;
	struct ceph_mds_reply_info rinfo;
	struct ceph_mds_session *session;
	struct ceph_inode_cap *cap = 0;
	struct ceph_file_info *cf = file->private_data;
	int err;

	dout(5, "ceph_open inode %p (%lu) file %p\n", inode, inode->i_ino, file);

	if (cf) {
		/* the file is already opened */
		return 0;
	}
	
	/*
	if (file->f_flags == O_DIRECTORY && ... ) 
		cap = ceph_find_cap(inode, 0);
	*/
	if (!cap) {
		dentry = list_entry(inode->i_dentry.next, struct dentry, d_alias);
		err = do_open_request(inode->i_sb, dentry, file->f_flags, 0, &session, &rinfo);
		if (err < 0) 
			return err;
		err = proc_open_reply(inode, file, session, &rinfo);
		ceph_mdsc_put_session(session);
		if (err < 0)
			return err;
	}

	err = ceph_open_init_private_data(inode, file);

	if (err < 0) {
		return err;
	}
	
	dout(5, "ceph_open success, %lx\n", inode->i_ino);
	return 0;
}

int ceph_lookup_open(struct inode *dir, struct dentry *dentry, struct nameidata *nd)
{
	struct ceph_mds_reply_info rinfo;
	struct ceph_mds_session *session;
	struct file *file = nd->intent.open.file;
	struct inode *inode;
	ino_t ino;
	int found = 0;
	int err;

	dout(5, "ceph_lookup_open in dir %p dentry %p '%s'\n", dir, dentry, dentry->d_name.name);
	err = do_open_request(dir->i_sb, dentry, nd->intent.open.flags, nd->intent.open.create_mode,
			      &session, &rinfo);
	if (err < 0)
		return err;
	err = le32_to_cpu(rinfo.head->result);
	dout(20, "ceph_lookup_open result=%d\n", err);

	/* if there was a previous inode associated with this dentry, now there isn't one */
	if (err == -ENOENT) 
		d_add(dentry, NULL);
	if (err < 0) 
		goto out;
	if (rinfo.trace_nr == 0) {
		derr(0, "wtf, no trace from mds\n");
		err = -EIO;
		goto out;
	}

	/* create the inode */
	ino = le64_to_cpu(rinfo.trace_in[rinfo.trace_nr-1].in->ino);
	dout(10, "got and parsed stat result, ino %lu\n", ino);
	inode = ilookup(dir->i_sb, ino);
	if (!inode) 
		inode = new_inode(dir->i_sb);
	else
		found++;
	if (!inode) {
		err = -EACCES;
		goto out;
	}
	
	if ((err = ceph_fill_inode(inode, rinfo.trace_in[rinfo.trace_nr-1].in)) < 0) 
		goto out;
	d_add(dentry, inode);

	if (found) 
		iput(inode);

	/* finish the open */
	err = proc_open_reply(inode, file, session, &rinfo);
	if (err == 0) {
		err = ceph_open_init_private_data(inode, file);
	}
out:
	ceph_mdsc_put_session(session);
	return err;
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
 * ugly hack!
 */
ssize_t ceph_silly_write(struct file *file, const char __user * data,
		     size_t count, loff_t * offset)
{
	struct inode *inode = file->f_path.dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	int ret = 0;
	int did = 0;
	
	dout(10, "silly_write on file %p %lld~%lu\n", file, *offset, count);
	
	/* ignore caps, for now. */
	/* this is an ugly hack */

	while (count > 0) {
		ret = ceph_osdc_silly_write(osdc, inode->i_ino, &ci->i_layout, count, *offset, data);
		dout(10, "ret is %d\n", ret);
		if (ret > 0) {
			did += ret;
			*offset += ret;
			data += ret;
			count -= ret;
			dout(10, "did %d bytes, ret now %d, %lu left\n", did, ret, count);
		} else if (did) 
			break;
		else 
			return ret;
	}

	spin_lock(&inode->i_lock);
	if (*offset > inode->i_size) {
		ci->i_wr_size = inode->i_size = *offset;
		inode->i_blocks = (inode->i_size + 512 - 1) >> 9;
		dout(10, "extending file size to %d\n", (int)inode->i_size);
	}	
	spin_unlock(&inode->i_lock);
	invalidate_inode_pages2(inode->i_mapping);
	return ret;
}

const struct file_operations ceph_file_fops = {
	.open = ceph_open,
	.release = ceph_release,
	.llseek = generic_file_llseek,
	.read = do_sync_read,
	.write = ceph_silly_write,//do_sync_write,
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
