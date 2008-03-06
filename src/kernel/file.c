
int ceph_debug_file = 50;
#define DOUT_VAR ceph_debug_file
#define DOUT_PREFIX "file: "
#include "super.h"

#include "mds_client.h"

/*
 * if err==0, caller is responsible for a put_session on *psession
 */
struct ceph_mds_request *
prepare_open_request(struct super_block *sb, struct dentry *dentry,
		     int flags, int create_mode)
{
	struct ceph_client *client = ceph_sb_to_client(sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	u64 pathbase;
	char *path;
	int pathlen;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;

	dout(5, "prepare_open_request dentry %p name '%s' flags %d\n", dentry,
	     dentry->d_name.name, flags);
	pathbase = ceph_ino(sb->s_root->d_inode);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return ERR_PTR(PTR_ERR(path));
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_OPEN, pathbase, path,
				       0, 0);
	req->r_expects_cap = true;
	kfree(path);
	if (!IS_ERR(req)) {
		rhead = req->r_request->front.iov_base;
		rhead->args.open.flags = cpu_to_le32(flags);
		rhead->args.open.mode = cpu_to_le32(create_mode);
	}
	return req;
}


static int ceph_open_init_private_data(struct inode *inode, struct file *file, int flags)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_file_info *cf;
	int mode;
	int wanted;

	cf = kzalloc(sizeof(*cf), GFP_KERNEL);
	if (cf == NULL)
		return -ENOMEM;
	file->private_data = cf;

	spin_lock(&inode->i_lock);
	mode = ceph_file_mode(flags);
	cf->mode = mode;
	ci->i_nr_by_mode[mode]++;
	wanted = ceph_caps_wanted(ci);
	dout(10, "opened %p flags 0%o mode %d nr now %d.  wanted %d -> %d\n",
	     file, flags,
	     mode, ci->i_nr_by_mode[mode], 
	     ci->i_cap_wanted, ci->i_cap_wanted|wanted);
	ci->i_cap_wanted |= wanted;   /* FIXME this isn't quite right */
	spin_unlock(&inode->i_lock);

	return 0;
}

int ceph_open(struct inode *inode, struct file *file)
{
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct dentry *dentry;
	struct ceph_mds_request *req;
	struct ceph_file_info *cf = file->private_data;
	int err;

	dout(5, "ceph_open inode %p ino %llx file %p\n", inode,
	     ceph_ino(inode), file);

	if (cf) {
		/* the file is already opened */
		return 0;
	}

	/*
	if (file->f_flags == O_DIRECTORY && ... )
		cap = ceph_find_cap(inode, 0);
	*/

	dentry = list_entry(inode->i_dentry.next, struct dentry,
			    d_alias);
	req = prepare_open_request(inode->i_sb, dentry, file->f_flags, 0);
	if (IS_ERR(req))
		return PTR_ERR(req);
	err = ceph_mdsc_do_request(mdsc, req);
	if (err == 0) 
		err = ceph_open_init_private_data(inode, file, file->f_flags);
	ceph_mdsc_put_request(req);
	dout(5, "ceph_open result=%d on %llx\n", err, ceph_ino(inode));
	return err;
}


/*
 * FIXME 
 *
 * this whole thing needs some fixing.  we need to be sure that
 * a successful lookup (that does an open) will be followed by
 * a VFS open (that sets up teh file*?).  i think there are
 * lots of things that can ahppen in between, mainly security 
 * checks that may not match the server's checks. 
 * otherwise, it's not safe to just go ahead and do the open
 * here or you won't get an eventual release...
 */
#if 0

int ceph_lookup_open(struct inode *dir, struct dentry *dentry,
		     struct nameidata *nd)
{
	struct ceph_mds_reply_info rinfo;
	struct ceph_mds_session *session;
	struct file *file = nd->intent.open.file;
	struct inode *inode;
	ino_t ino;
	int found = 0;
	int err;

	dout(5, "ceph_lookup_open in dir %p dentry %p '%s'\n", dir, dentry,
	     dentry->d_name.name);
/* fixme
	err = prepare_open_request(dir->i_sb, dentry, nd->intent.open.flags,
			      nd->intent.open.create_mode,
			      &session, &rinfo);
*/
	if (err < 0)
		return err;
	err = le32_to_cpu(rinfo.head->result);
	dout(20, "ceph_lookup_open result=%d\n", err);

	/* if there was a previous inode associated with this dentry,
	 * now there isn't one */
	if (err == -ENOENT) {
		ceph_touch_dentry(dentry);
		d_add(dentry, NULL);
	} 
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

	err = ceph_fill_inode(inode, rinfo.trace_in[rinfo.trace_nr-1].in);
	if (err < 0)
		goto out;
	ceph_touch_dentry(dentry);
	d_add(dentry, inode);

	if (found)
		iput(inode);

	/* finish the open */
	err = proc_open_reply(inode, file, session, &rinfo);
	if (err == 0)
		err = ceph_open_init_private_data(inode, file, nd->intent.open.flags);
out:
	//ceph_mdsc_put_session(session);
	return err;
}
#endif

int ceph_release(struct inode *inode, struct file *file)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_file_info *cf = file->private_data;
	int mode = cf->mode;
	int wanted;

	dout(5, "release inode %p file %p\n", inode, file);
	atomic_dec(&ci->i_cap_count);

	/*
	 * FIXME mystery: why is file->f_flags now different than
	 * file->f_flags (actually, nd->intent.open.flags) on
	 * open?  e.g., on ceph_lookup_open,
	 *   ceph_file: opened 000000006fa3ebd0 flags 0101102 mode 2 nr now 1.  wanted 0 -> 30
	 * and on release,
	 *   ceph_file: released 000000006fa3ebd0 flags 0100001 mode 3 nr now -1.  wanted 30 was 30
	 * for now, store the open mode in ceph_file_info.
	 */
	mode = cf->mode;
	ci->i_nr_by_mode[mode]--;
	wanted = ceph_caps_wanted(ci);
	dout(10, "released %p flags 0%o mode %d nr now %d.  wanted %d was %d\n", 
	     file, file->f_flags, mode, 
	     ci->i_nr_by_mode[mode], wanted, ci->i_cap_wanted);
	if (wanted != ci->i_cap_wanted)
		ceph_mdsc_update_cap_wanted(ci, wanted);
	
	if (cf->last_readdir)
		ceph_mdsc_put_request(cf->last_readdir);
	kfree(cf);

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
ssize_t ceph_silly_write(struct file *file, const char __user *data,
			 size_t count, loff_t *offset)
{
	struct inode *inode = file->f_path.dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_inode_to_client(inode)->osdc;
	int ret = 0;
	int did = 0;
	off_t pos = *offset;

	dout(10, "silly_write on file %p %lld~%u\n", file, *offset,
	     (unsigned)count);

	/* ignore caps, for now. */
	/* this is an ugly hack */

	if (file->f_flags & O_APPEND)
		pos = inode->i_size;

	while (count > 0) {
		ret = ceph_osdc_silly_write(osdc, ceph_ino(inode),
					    &ci->i_layout,
					    count, pos, data);
		dout(10, "ret is %d\n", ret);
		if (ret > 0) {
			did += ret;
			pos += ret;
			data += ret;
			count -= ret;
			dout(10, "did %d bytes, ret now %d, %u left\n",
			     did, ret, (unsigned)count);
		} else if (did)
			break;
		else
			return ret;
	}

	spin_lock(&inode->i_lock);
	if (pos > inode->i_size) {
		ci->i_wr_size = inode->i_size = pos;
		inode->i_blocks = (inode->i_size + 512 - 1) >> 9;
		dout(10, "extending file size to %d\n", (int)inode->i_size);
	}
	spin_unlock(&inode->i_lock);
	invalidate_inode_pages2(inode->i_mapping);

	*offset = pos;

	return ret;
}

const struct file_operations ceph_file_fops = {
	.open = ceph_open,
	.release = ceph_release,
	.llseek = generic_file_llseek,
	.read = do_sync_read,
	.write = ceph_silly_write,
	//.write = do_sync_write,
	.aio_read = generic_file_aio_read,
	.aio_write = generic_file_aio_write,
	.mmap = generic_file_mmap,
};
