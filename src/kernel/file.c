
#include <linux/sched.h>
#include <linux/file.h>
#include <linux/writeback.h>

#include "ceph_debug.h"

int ceph_debug_file __read_mostly = -1;
#define DOUT_MASK DOUT_MASK_FILE
#define DOUT_VAR ceph_debug_file
#include "super.h"

#include "mds_client.h"

#include <linux/namei.h>


/*
 * Prepare an open request.  Preallocate ceph_cap to avoid an
 * inopportune ENOMEM later.
 */
static struct ceph_mds_request *
prepare_open_request(struct super_block *sb, struct dentry *dentry,
		     int flags, int create_mode)
{
	struct ceph_client *client = ceph_sb_to_client(sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	int want_auth = USE_ANY_MDS;

	if (flags & (O_WRONLY|O_RDWR|O_CREAT|O_TRUNC))
		want_auth = USE_AUTH_MDS;

	dout(5, "prepare_open_request dentry %p name '%s' flags %d\n", dentry,
	     dentry->d_name.name, flags);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_OPEN, dentry, NULL,
				       NULL, NULL, want_auth);
	if (IS_ERR(req))
		goto out;
	req->r_expected_cap = kmalloc(sizeof(struct ceph_cap), GFP_NOFS);
	if (!req->r_expected_cap) {
		ceph_mdsc_put_request(req);
		return ERR_PTR(-ENOMEM);
	}
	req->r_fmode = ceph_flags_to_mode(flags);
	req->r_args.open.flags = cpu_to_le32(flags);
	req->r_args.open.mode = cpu_to_le32(create_mode);
out:
	return req;
}

/*
 * initialize private struct file data.
 * if we fail, clean up by dropping fmode reference on the ceph_inode
 */
static int ceph_init_file(struct inode *inode, struct file *file, int fmode)
{
	struct ceph_file_info *cf;

	cf = kzalloc(sizeof(*cf), GFP_NOFS);
	if (cf == NULL) {
		ceph_put_fmode(ceph_inode(inode), fmode); /* clean up */
		return -ENOMEM;
	}
	cf->fmode = fmode;
	file->private_data = cf;
	return 0;
}

/*
 * If the filp already has private_data, that means the file was
 * already opened by intent during lookup, and we do nothing.
 *
 * If we already have the requisite capabilities, we can satisfy
 * the open request locally (no need to request new caps from the
 * MDS).
 */
int ceph_open(struct inode *inode, struct file *file)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct dentry *dentry;
	struct ceph_mds_request *req;
	struct ceph_file_info *cf = file->private_data;
	struct inode *parent_inode = file->f_dentry->d_parent->d_inode;
	int err;
	int flags, fmode, wantcaps;

	if (ceph_snap(inode) != CEPH_NOSNAP && (file->f_mode & FMODE_WRITE))
		return -EROFS;

	if (cf) {
		dout(5, "open file %p is already opened\n", file);
		return 0;
	}

	/* filter out O_CREAT|O_EXCL; vfs did that already.  yuck. */
	flags = file->f_flags & ~(O_CREAT|O_EXCL);
	if (S_ISDIR(inode->i_mode))
		flags = O_DIRECTORY;  /* mds likes to know */

	dout(5, "open inode %p ino %llx.%llx file %p flags %d (%d)\n", inode,
	     ceph_vinop(inode), file, flags, file->f_flags);
	fmode = ceph_flags_to_mode(flags);
	wantcaps = ceph_caps_for_mode(fmode);

	/* can we re-use existing caps? */
	spin_lock(&inode->i_lock);
	if ((__ceph_caps_issued(ci, NULL) & wantcaps) == wantcaps) {
		dout(10, "open fmode %d caps %d using existing on %p\n",
		     fmode, wantcaps, inode);
		__ceph_get_fmode(ci, fmode);
		spin_unlock(&inode->i_lock);
		return ceph_init_file(inode, file, fmode);
	}
	spin_unlock(&inode->i_lock);
	dout(10, "open fmode %d, don't have caps %s\n", fmode,
	     ceph_cap_string(wantcaps));

	dentry = d_find_alias(inode);
	if (!dentry)
		return -ESTALE;  /* yuck */
	if (!ceph_caps_issued_mask(ceph_inode(inode), CEPH_CAP_FILE_EXCL))
		ceph_release_caps(inode, CEPH_CAP_FILE_RDCACHE);
	req = prepare_open_request(inode->i_sb, dentry, flags, 0);
	if (IS_ERR(req)) {
		err = PTR_ERR(req);
		goto out;
	}
	err = ceph_mdsc_do_request(mdsc, parent_inode, req);
	if (!err)
		err = ceph_init_file(inode, file, req->r_fmode);
	ceph_mdsc_put_request(req);
	dout(5, "open result=%d on %llx.%llx\n", err, ceph_vinop(inode));
out:
	dput(dentry);
	return err;
}


/*
 * Do a lookup + open with a single request.
 *
 * If this succeeds, but some subsequent check in the vfs
 * may_open() fails, the struct *file gets cleaned up (i.e.
 * ceph_release gets called).  So fear not!
 */
/*
 * flags
 *  path_lookup_open   -> LOOKUP_OPEN
 *  path_lookup_create -> LOOKUP_OPEN|LOOKUP_CREATE
 */
struct dentry *ceph_lookup_open(struct inode *dir, struct dentry *dentry,
				struct nameidata *nd, int mode,
				int locked_dir)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct file *file = nd->intent.open.file;
	struct inode *parent_inode = get_dentry_parent_inode(file->f_dentry);
	struct ceph_mds_request *req;
	int err;
	int flags = nd->intent.open.flags - 1;  /* silly vfs! */

	dout(5, "ceph_lookup_open dentry %p '%.*s' flags %d mode 0%o\n",
	     dentry, dentry->d_name.len, dentry->d_name.name, flags, mode);

	/* do the open */
	req = prepare_open_request(dir->i_sb, dentry, flags, mode);
	if (IS_ERR(req))
		return ERR_PTR(PTR_ERR(req));
	if ((flags & O_CREAT) &&
	    (!ceph_caps_issued_mask(ceph_inode(dir), CEPH_CAP_FILE_EXCL)))
		ceph_release_caps(dir, CEPH_CAP_FILE_RDCACHE);
	req->r_locked_dir = dir;           /* caller holds dir->i_mutex */
	err = ceph_mdsc_do_request(mdsc, parent_inode, req);
	dentry = ceph_finish_lookup(req, dentry, err);
	if (!err)
		err = ceph_init_file(req->r_last_inode, file, req->r_fmode);
	ceph_mdsc_put_request(req);
	dout(5, "ceph_lookup_open result=%p\n", dentry);
	return dentry;
}

int ceph_release(struct inode *inode, struct file *file)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_file_info *cf = file->private_data;

	dout(5, "release inode %p file %p\n", inode, file);
	ceph_put_fmode(ci, cf->fmode);
	if (cf->last_readdir)
		ceph_mdsc_put_request(cf->last_readdir);
	kfree(cf->dir_info);
	kfree(cf);
	return 0;
}

/*
 * Completely synchronous read and write methods.  Direct from __user
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

	ret = ceph_osdc_sync_read(&client->osdc, ceph_vino(inode),
				  &ci->i_layout,
				  pos, count, ci->i_truncate_seq,
				  ci->i_truncate_size, data);
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

	if (ceph_snap(file->f_dentry->d_inode) != CEPH_NOSNAP)
		return -EROFS;

	dout(10, "sync_write on file %p %lld~%u\n", file, *offset,
	     (unsigned)count);

	if (file->f_flags & O_APPEND)
		pos = i_size_read(inode);

	ret = ceph_osdc_sync_write(&client->osdc, ceph_vino(inode),
				   &ci->i_layout,
				   ci->i_snap_realm->cached_context,
				   pos, count, ci->i_truncate_seq,
				   ci->i_truncate_size, data);
	if (ret > 0) {
		pos += ret;
		*offset = pos;
		if (pos > i_size_read(inode))
			ceph_inode_set_size(inode, pos);
	}

	return ret;
}

/*
 * Wrap generic_file_aio_read with checks for cap bits on the inode.
 * Atomically grab references, so that those bits are not released
 * back to the MDS mid-read.
 *
 * Hmm, the sync reach case isn't actually async... should it be?
 */
static ssize_t ceph_aio_read(struct kiocb *iocb, const struct iovec *iov,
		      unsigned long nr_segs, loff_t pos)
{
	struct file *filp = iocb->ki_filp;
	loff_t *ppos = &iocb->ki_pos;
	size_t len = iov->iov_len;
	struct inode *inode = filp->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	ssize_t ret;
	int got = 0;

	__ceph_do_pending_vmtruncate(inode);

	dout(10, "aio_read %llx.%llx %llu~%u trying to get caps on %p\n",
	     ceph_vinop(inode), pos, (unsigned)len, inode);
	ret = ceph_get_caps(ci,
				 CEPH_CAP_FILE_RD,
				 CEPH_CAP_FILE_RDCACHE,
				 &got, -1);
	if (ret < 0)
		goto out;
	dout(10, "aio_read %llx.%llx %llu~%u got cap refs %d\n",
	     ceph_vinop(inode), pos, (unsigned)len, got);

	if ((got & CEPH_CAP_FILE_RDCACHE) == 0 ||
	    (inode->i_sb->s_flags & MS_SYNCHRONOUS))
		/* hmm, this isn't really async... */
		ret = ceph_sync_read(filp, iov->iov_base, len, ppos);
	else
		ret = generic_file_aio_read(iocb, iov, nr_segs, pos);

out:
	dout(10, "aio_read %llx.%llx dropping cap refs on %d\n",
	     ceph_vinop(inode), got);
	ceph_put_cap_refs(ci, got);
	return ret;
}

/*
 * Check the offset we are writing up to against our current
 * max_size.  If necessary, tell the MDS we want to write to
 * a larger offset.
 */
static void check_max_size(struct inode *inode, loff_t endoff)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int check = 0;

	/* do we need to explicitly request a larger max_size? */
	spin_lock(&inode->i_lock);
	if ((endoff >= ci->i_max_size ||
	     endoff > (inode->i_size << 1)) &&
	    endoff > ci->i_wanted_max_size) {
		dout(10, "write %p at large endoff %llu, req max_size\n",
		     inode, endoff);
		ci->i_wanted_max_size = endoff;
		check = 1;
	}
	spin_unlock(&inode->i_lock);
	if (check)
		ceph_check_caps(ci, 0, 0, NULL);
}

/*
 * Take cap references to avoid releasing caps to MDS mid-write.
 *
 * If we are synchronous, and write with an old snap context, the OSD
 * may return EOLDSNAPC.  In that case, retry the write.. _after_
 * dropping our cap refs and allowing the pending snap to logically
 * complete _before_ this write occurs.
 *
 * If we are near ENOSPC, write synchronously.
 */
static ssize_t ceph_aio_write(struct kiocb *iocb, const struct iovec *iov,
		       unsigned long nr_segs, loff_t pos)
{
	struct file *file = iocb->ki_filp;
	struct address_space *mapping = file->f_mapping;
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_osd_client *osdc = &ceph_client(inode->i_sb)->osdc;
	loff_t endoff = pos + iov->iov_len;
	int got = 0;
	int ret;

	if (ceph_snap(inode) != CEPH_NOSNAP)
		return -EROFS;

retry_snap:
	if (ceph_osdmap_flag(osdc->osdmap, CEPH_OSDMAP_FULL))
		return -ENOSPC;
	__ceph_do_pending_vmtruncate(inode);
	check_max_size(inode, endoff);
	dout(10, "aio_write %p %llu~%u getting caps. i_size %llu\n",
	     inode, pos, (unsigned)iov->iov_len, inode->i_size);
	ret = ceph_get_caps(ci,
				 CEPH_CAP_FILE_WR,
				 CEPH_CAP_FILE_WRBUFFER,
				 &got, endoff);
	if (ret < 0)
		goto out;

	dout(10, "aio_write %p %llu~%u  got cap refs on %d\n",
	     inode, pos, (unsigned)iov->iov_len, got);

	if ((got & CEPH_CAP_FILE_WRBUFFER) == 0) {
		ret = ceph_sync_write(file, iov->iov_base, iov->iov_len,
			&iocb->ki_pos);
	} else {
		ret = generic_file_aio_write(iocb, iov, nr_segs, pos);

		if (ret >= 0 &&
	    	    ceph_osdmap_flag(osdc->osdmap, CEPH_OSDMAP_NEARFULL)) {
			ret = sync_page_range(inode, mapping, pos, ret);
		}
	}
	if (ret >= 0)
		ci->i_dirty_caps |= CEPH_CAP_FILE_WR;

out:
	dout(10, "aio_write %p %llu~%u  dropping cap refs on %d\n",
	     inode, pos, (unsigned)iov->iov_len, got);
	ceph_put_cap_refs(ci, got);

	if (ret == -EOLDSNAPC) {
		dout(10, "aio_write %p %llu~%u got EOLDSNAPC, retrying\n",
		     inode, pos, (unsigned)iov->iov_len);
		goto retry_snap;
	}

	return ret;
}

static int ceph_fsync(struct file *file, struct dentry *dentry, int datasync)
{
	struct inode *inode = dentry->d_inode;
	int ret;

	dout(10, "fsync on inode %p\n", inode);
	ret = filemap_write_and_wait(inode->i_mapping);
	if (ret < 0)
		return ret;
	/*
	 * HMM: should we also ensure that caps are flushed to mds?
	 * It's not strictly necessary, since with the data on the
	 * osds the mds can/will always reconstruct the file size.
	 * Not mtime, though.
	 */

	return ret;
}

const struct file_operations ceph_file_fops = {
	.open = ceph_open,
	.release = ceph_release,
	.llseek = generic_file_llseek,
	.read = do_sync_read,
	.write = do_sync_write,
	.aio_read = ceph_aio_read,
	.aio_write = ceph_aio_write,
	.mmap = ceph_mmap,
	.fsync = ceph_fsync,
	.splice_read = generic_file_splice_read,
	.splice_write = generic_file_splice_write,
	.unlocked_ioctl = ceph_ioctl,
};


