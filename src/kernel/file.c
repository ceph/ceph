
#include <linux/sched.h>
#include <linux/file.h>

int ceph_debug_file = -1;
#define DOUT_VAR ceph_debug_file
#define DOUT_PREFIX "file: "
#include "super.h"

#include "mds_client.h"

#include <linux/namei.h>


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
	req->r_expects_cap = 1;
	kfree(path);
	if (!IS_ERR(req)) {
		rhead = req->r_request->front.iov_base;
		rhead->args.open.flags = cpu_to_le32(flags);
		rhead->args.open.mode = cpu_to_le32(create_mode);
	}
	return req;
}


static int ceph_init_file(struct inode *inode, struct file *file,
				       int flags)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_file_info *cf;
	int mode = ceph_file_mode(flags);

	cf = kzalloc(sizeof(*cf), GFP_KERNEL);
	if (cf == NULL)
		return -ENOMEM;
	file->private_data = cf;

	cf->mode = mode;
	ceph_get_mode(ci, mode);
	return 0;
}

int ceph_open(struct inode *inode, struct file *file)
{
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct dentry *dentry = list_entry(inode->i_dentry.next, struct dentry,
					   d_alias);
	struct ceph_mds_request *req;
	struct ceph_file_info *cf = file->private_data;
	int err;

	/* filter out O_CREAT|O_EXCL; vfs did that already.  yuck. */
	int flags = file->f_flags & ~(O_CREAT|O_EXCL);

	dout(5, "open inode %p ino %llx file %p\n", inode,
	     ceph_ino(inode), file);
	if (cf) {
		dout(5, "open file %p is already opened\n", file);
		return 0;
	}

	req = prepare_open_request(inode->i_sb, dentry, flags, 0);
	if (IS_ERR(req))
		return PTR_ERR(req);
	err = ceph_mdsc_do_request(mdsc, req);
	if (err == 0) 
		err = ceph_init_file(inode, file, flags);
	ceph_mdsc_put_request(req);
	dout(5, "ceph_open result=%d on %llx\n", err, ceph_ino(inode));
	return err;
}


/*
 * so, if this succeeds, but some subsequent check in the vfs
 * may_open() fails, the struct *fiel gets cleaned up (i.e.
 * ceph_release gets called).  so fear not!
 */
/*
 * flags
 *  path_lookup_open   -> LOOKUP_OPEN
 *  path_lookup_create -> LOOKUP_OPEN|LOOKUP_CREATE
 */
int ceph_lookup_open(struct inode *dir, struct dentry *dentry,
		     struct nameidata *nd, int mode)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct file *file = nd->intent.open.file;
	struct ceph_mds_request *req;
	int err;
	int flags = nd->intent.open.flags;
	dout(5, "ceph_lookup_open dentry %p '%.*s' flags %d mode 0%o\n", 
	     dentry, dentry->d_name.len, dentry->d_name.name, flags, mode);
	
	req = prepare_open_request(dir->i_sb, dentry, flags, mode);
	if (IS_ERR(req))
		return PTR_ERR(req);
	if (flags & O_CREAT)
		ceph_mdsc_lease_release(mdsc, dir, 0, CEPH_LOCK_ICONTENT);
	dget(dentry);                /* to match put_request below */
	req->r_last_dentry = dentry; /* use this dentry in fill_trace */
	err = ceph_mdsc_do_request(mdsc, req);
	if (err == 0)
		err = ceph_init_file(req->r_last_inode, file, flags);
	else if (err == -ENOENT) {
		ceph_init_dentry(dentry);
		d_add(dentry, NULL);
	}
	ceph_mdsc_put_request(req);
	dout(5, "ceph_lookup_open result=%d\n", err);
	return err;
}

int ceph_release(struct inode *inode, struct file *file)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_file_info *cf = file->private_data;

	dout(5, "release inode %p file %p\n", inode, file);

	/*
	 * FIXME mystery: why is file->f_flags now different than
	 * file->f_flags (actually, nd->intent.open.flags) on
	 * open?  e.g., on ceph_lookup_open,
	 *   ceph_file: opened 000000006fa3ebd0 flags 0101102 mode 2 nr now 1.  wanted 0 -> 30
	 * and on release,
	 *   ceph_file: released 000000006fa3ebd0 flags 0100001 mode 3 nr now -1.  wanted 30 was 30
	 * for now, store the open mode in ceph_file_info.
	 */

	ceph_put_mode(ci, cf->mode);
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
 * completely synchronous read and write methods.  direct from __user
 * buffer to osd.
 */
static ssize_t ceph_sync_read(struct file *file, char __user *data,
			       size_t count, loff_t *offset)
{
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_client *client = ceph_inode_to_client(inode);
	int ret = 0;
	off_t pos = *offset;

	dout(10, "sync_read on file %p %lld~%u\n", file, *offset,
	     (unsigned)count);
	
	ret = ceph_osdc_sync_read(&client->osdc, ceph_ino(inode),
				  &ci->i_layout,
				  pos, count, data);
	if (ret > 0)
		*offset = pos + ret;
	return ret;
}

static ssize_t ceph_sync_write(struct file *file, const char __user *data,
			       size_t count, loff_t *offset)
{
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_client *client = ceph_inode_to_client(inode);
	int ret = 0;
	off_t pos = *offset;

	dout(10, "sync_write on file %p %lld~%u\n", file, *offset,
	     (unsigned)count);

	if (file->f_flags & O_APPEND)
		pos = i_size_read(inode);

	ret = ceph_osdc_sync_write(&client->osdc, ceph_ino(inode),
				   &ci->i_layout,
				   pos, count, data);
	if (ret > 0) {
		pos += ret;
		*offset = pos;

		spin_lock(&inode->i_lock);
		if (pos > inode->i_size) {
			inode->i_size = pos;
			inode->i_blocks = (inode->i_size + 512 - 1) >> 9;
			dout(10, "extending file size to %lu\n", pos);
		}
		spin_unlock(&inode->i_lock);
	}
	return ret;
}

/* 
 * wrap do_sync_read and friends with checks for cap bits on the inode.
 * atomically grab references, so that those bits are released mid-read.
 */
ssize_t ceph_read(struct file *filp, char __user *buf, size_t len, loff_t *ppos)
{
	struct inode *inode = filp->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	ssize_t ret;
	int got = 0;

	dout(10, "read %llx %llu~%u trying to get caps\n",
	     ceph_ino(inode), *ppos, (unsigned)len);
	ret = wait_event_interruptible(ci->i_cap_wq,
				       ceph_get_cap_refs(ci, CEPH_CAP_RD, 
							 CEPH_CAP_RDCACHE, 
							 &got));
	if (ret < 0) 
		goto out;
	dout(10, "read %llx %llu~%u got cap refs %d\n",
	     ceph_ino(inode), *ppos, (unsigned)len, got);

	if ((got & CEPH_CAP_RDCACHE) == 0 ||
	    (inode->i_sb->s_flags & MS_SYNCHRONOUS)) 
		ret = ceph_sync_read(filp, buf, len, ppos);
	else
		ret = do_sync_read(filp, buf, len, ppos);

out:
	dout(10, "read %llx dropping cap refs on %d\n", ceph_ino(inode), got);
	ceph_put_cap_refs(ci, got);
	return ret;
}

/*
 * ditto
 */
ssize_t ceph_write(struct file *filp, const char __user *buf, 
		   size_t len, loff_t *ppos)
{
	struct inode *inode = filp->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	ssize_t ret;
	int got = 0;

	dout(10, "write trying to get caps\n");
	ret = wait_event_interruptible(ci->i_cap_wq,
				       ceph_get_cap_refs(ci, CEPH_CAP_WR, 
							 CEPH_CAP_WRBUFFER,
							 &got));
	if (ret < 0) 
		goto out;
	dout(10, "write got cap refs on %d\n", got);

	if ((got & CEPH_CAP_WRBUFFER) == 0 ||
	    (inode->i_sb->s_flags & MS_SYNCHRONOUS)) 
		ret = ceph_sync_write(filp, buf, len, ppos);
	else
		ret = do_sync_write(filp, buf, len, ppos);

out:
	dout(10, "write dropping cap refs on %d\n", got);
	ceph_put_cap_refs(ci, got);
	return ret;
}



const struct file_operations ceph_file_fops = {
	.open = ceph_open,
	.release = ceph_release,
	.llseek = generic_file_llseek,
	.read = ceph_read,
	.write = ceph_write,
	.aio_read = generic_file_aio_read,
	.aio_write = generic_file_aio_write,
	.mmap = generic_file_mmap,
};
