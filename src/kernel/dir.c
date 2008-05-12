int ceph_debug_dir = -1;
#define DOUT_VAR ceph_debug_dir
#define DOUT_PREFIX "dir: "
#include "super.h"

#include <linux/namei.h>
#include <linux/sched.h>

const struct inode_operations ceph_dir_iops;
const struct file_operations ceph_dir_fops;
struct dentry_operations ceph_dentry_ops;

/*
 * build a dentry's path, relative to sb root.  allocate on
 * heap; caller must kfree.
 * (based on build_path_from_dentry in fs/cifs/dir.c)
 */
char *ceph_build_dentry_path(struct dentry *dentry, int *plen)
{
	struct dentry *temp;
	char *path;
	int len, pos;

	if (dentry == NULL)
		return ERR_PTR(-EINVAL);

retry:
	len = 0;
	for (temp = dentry; !IS_ROOT(temp);) {
		len += 1 + temp->d_name.len;
		temp = temp->d_parent;
		if (temp == NULL) {
			derr(1, "corrupt dentry %p\n", dentry);
			return ERR_PTR(-EINVAL);
		}
	}
	if (len)
		len--;  /* no leading '/' */

	path = kmalloc(len+1, GFP_NOFS);
	if (path == NULL)
		return ERR_PTR(-ENOMEM);
	pos = len;
	path[pos] = 0;	/* trailing null */
	for (temp = dentry; !IS_ROOT(temp);) {
		pos -= temp->d_name.len;
		if (pos < 0) {
			break;
		} else {
			strncpy(path + pos, temp->d_name.name,
				temp->d_name.len);
			dout(50, "build_path_dentry path+%d: %p '%.*s'\n",
			     pos, temp, temp->d_name.len, path + pos);
			if (pos)
				path[--pos] = '/';
		}
		temp = temp->d_parent;
		if (temp == NULL) {
			derr(1, "corrupt dentry\n");
			kfree(path);
			return ERR_PTR(-EINVAL);
		}
	}
	if (pos != 0) {
		derr(1, "did not end path lookup where expected, "
		     "namelen is %d\n", len);
		/* presumably this is only possible if racing with a
		   rename of one of the parent directories (we can not
		   lock the dentries above us to prevent this, but
		   retrying should be harmless) */
		kfree(path);
		goto retry;
	}

	dout(10, "build_path_dentry on %p %d build '%.*s'\n",
	     dentry, atomic_read(&dentry->d_count), len, path);
	*plen = len;
	return path;
}


/*
 * build fpos from fragment id and offset within that fragment.
 */
static loff_t make_fpos(unsigned frag, unsigned off)
{
	return ((loff_t)frag << 32) | (loff_t)off;
}
static unsigned fpos_frag(loff_t p)
{
	return p >> 32;
}
static unsigned fpos_off(loff_t p)
{
	return p & 0xffffffff;
}

static int ceph_readdir(struct file *filp, void *dirent, filldir_t filldir)
{
	struct ceph_file_info *fi = filp->private_data;
	struct inode *inode = filp->f_dentry->d_inode;
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;
	unsigned frag = fpos_frag(filp->f_pos);
	unsigned off = fpos_off(filp->f_pos);
	unsigned skew;
	int err;
	__u32 ftype;
	struct ceph_mds_reply_info *rinfo;

nextfrag:
	dout(5, "dir_readdir filp %p at frag %u off %u\n", filp, frag, off);
	if (fi->frag != frag || fi->last_readdir == NULL) {
		struct ceph_mds_request *req;
		struct ceph_mds_request_head *rhead;

		frag = ceph_choose_frag(ceph_inode(inode), frag);

		/* query mds */
		dout(10, "dir_readdir querying mds for ino %llx frag %x\n",
		     ceph_ino(inode), frag);
		req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_READDIR,
					       ceph_ino(inode), "", 0, 0,
					       filp->f_dentry, 1, frag);
		if (IS_ERR(req))
			return PTR_ERR(req);
		rhead = req->r_request->front.iov_base;
		rhead->args.readdir.frag = cpu_to_le32(frag);
		err = ceph_mdsc_do_request(mdsc, req);
		if (err < 0) {
			ceph_mdsc_put_request(req);
			return err;
		}
		dout(10, "dir_readdir got and parsed readdir result=%d"
		     " on frag %x\n", err, frag);
		if (fi->last_readdir)
			ceph_mdsc_put_request(fi->last_readdir);
		fi->last_readdir = req;
	}

	/* include . and .. with first fragment */
	if (frag_is_leftmost(frag)) {
		switch (off) {
		case 0:
			dout(10, "dir_readdir off 0 -> '.'\n");
			if (filldir(dirent, ".", 1, make_fpos(0, 0),
				    inode->i_ino, inode->i_mode >> 12) < 0)
				return 0;
			off++;
			filp->f_pos++;
		case 1:
			dout(10, "dir_readdir off 1 -> '..'\n");
			if (filp->f_dentry->d_parent != NULL &&
			    filldir(dirent, "..", 2, make_fpos(0, 1),
				    filp->f_dentry->d_parent->d_inode->i_ino,
				    inode->i_mode >> 12) < 0)
				return 0;
			off++;
			filp->f_pos++;
		}
		skew = -2;
	} else
		skew = 0;

	rinfo = &fi->last_readdir->r_reply_info;
	dout(10, "dir_readdir frag %x num %d off %d skew %d\n", frag,
	     rinfo->dir_nr, off, skew);
	while (off+skew < rinfo->dir_nr) {
		dout(10, "dir_readdir off %d -> %d / %d name '%.*s'\n",
		     off, off+skew,
		     rinfo->dir_nr, rinfo->dir_dname_len[off+skew],
		     rinfo->dir_dname[off+skew]);
		ftype = le32_to_cpu(rinfo->dir_in[off+skew].in->mode >> 12);
		if (filldir(dirent,
			    rinfo->dir_dname[off+skew],
			    rinfo->dir_dname_len[off+skew],
			    make_fpos(frag, off),
			    le64_to_cpu(rinfo->dir_in[off+skew].in->ino),
			    ftype) < 0) {
			dout(20, "filldir stopping us...\n");
			return 0;
		}
		off++;
		filp->f_pos++;
	}

	/* more frags? */
	if (frag_value(frag) != frag_mask(frag)) {
		frag = frag_next(frag);
		off = 0;
		filp->f_pos = make_fpos(frag, off);
		dout(10, "dir_readdir next frag is %x\n", frag);
		goto nextfrag;
	}

	dout(20, "dir_readdir done.\n");
	return 0;
}

loff_t ceph_dir_llseek(struct file *file, loff_t offset, int origin)
{
	struct ceph_file_info *fi = file->private_data;
	struct inode *inode = file->f_mapping->host;
	loff_t retval;

	mutex_lock(&inode->i_mutex);
	switch (origin) {
	case SEEK_END:
		offset += inode->i_size;
		break;
	case SEEK_CUR:
		offset += file->f_pos;
	}
	retval = -EINVAL;
	if (offset >= 0 && offset <= inode->i_sb->s_maxbytes) {
		if (offset != file->f_pos) {
			file->f_pos = offset;
			file->f_version = 0;
		}
		retval = offset;
		if (offset == 0 && fi->last_readdir) {
			dout(10, "llseek dropping %p readdir content\n", file);
			ceph_mdsc_put_request(fi->last_readdir);
			fi->last_readdir = 0;
		}
	}
	mutex_unlock(&inode->i_mutex);
	return retval;
}

/*
 * do a lookup / lstat (same thing).
 * @on_inode indicates that we should stat the ino, and not a path
 * built from @dentry.
 */
struct dentry *ceph_do_lookup(struct super_block *sb, struct dentry *dentry, 
			      int mask, int on_inode)
{
	struct ceph_client *client = ceph_sb_to_client(sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	char *path;
	int pathlen;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;
	int err;

	if (dentry->d_name.len > NAME_MAX)
		return ERR_PTR(-ENAMETOOLONG);

	dout(10, "do_lookup %p mask %d\n", dentry, mask);
	if (on_inode) {
		/* stat ino directly */
		req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LSTAT,
					       ceph_ino(dentry->d_inode), 0,
					       0, 0,
					       dentry, 0, -1);
	} else {
		/* build path */
		path = ceph_build_dentry_path(dentry, &pathlen);
		if (IS_ERR(path))
			return ERR_PTR(PTR_ERR(path));
		req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LSTAT,
					       ceph_ino(sb->s_root->d_inode),
					       path, 0, 0,
					       dentry, 0, -1);
		kfree(path);
	}
	if (IS_ERR(req))
		return ERR_PTR(PTR_ERR(req));
	rhead = req->r_request->front.iov_base;
	rhead->args.stat.mask = cpu_to_le32(mask);
	dget(dentry);                /* to match put_request below */
	req->r_last_dentry = dentry; /* use this dentry in fill_trace */
	err = ceph_mdsc_do_request(mdsc, req);
	if (err == -ENOENT) {
		/* no trace? */
		if (req->r_reply_info.trace_numd == 0) {
			dout(20, "ENOENT and no trace, dentry %p inode %p\n",
			     dentry, dentry->d_inode);
			ceph_init_dentry(dentry);
			if (dentry->d_inode) {
				d_drop(dentry);
				req->r_last_dentry = d_alloc(dentry->d_parent,
							     &dentry->d_name);
				d_rehash(req->r_last_dentry);
			} else
				d_add(dentry, NULL);
		}
		err = 0;
	}
	if (err)
		dentry = ERR_PTR(err);
	else if (dentry != req->r_last_dentry)
		dentry = req->r_last_dentry;   /* we got d_splice_alias'd */
	else
		dentry = 0;
	ceph_mdsc_put_request(req);  /* will dput(dentry) */
	dout(20, "do_lookup result=%p\n", dentry);
	return dentry;
}

static struct dentry *ceph_lookup(struct inode *dir, struct dentry *dentry,
				      struct nameidata *nd)
{
	dout(5, "dir_lookup in dir %p dentry %p '%.*s'\n",
	     dir, dentry, dentry->d_name.len, dentry->d_name.name);

	/* open (but not create!) intent? */
	if (nd && nd->flags & LOOKUP_OPEN &&
	    !(nd->intent.open.flags & O_CREAT)) {
		int mode = nd->intent.open.create_mode & ~current->fs->umask;
		int err = ceph_lookup_open(dir, dentry, nd, mode);
		return ERR_PTR(err);
	}

	return ceph_do_lookup(dir->i_sb, dentry, CEPH_STAT_MASK_INODE_ALL, 0);
}

static int ceph_mknod(struct inode *dir, struct dentry *dentry,
			  int mode, dev_t rdev)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;
	char *path;
	int pathlen;
	int err;

	dout(5, "dir_mknod in dir %p dentry %p mode 0%o rdev %d\n",
	     dir, dentry, mode, rdev);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_MKNOD,
				       ceph_ino(dir->i_sb->s_root->d_inode),
				       path, 0, 0,
				       dentry, 1, -1);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}
	ceph_mdsc_lease_release(mdsc, dir, 0, CEPH_LOCK_ICONTENT);
	rhead = req->r_request->front.iov_base;
	rhead->args.mknod.mode = cpu_to_le32(mode);
	rhead->args.mknod.rdev = cpu_to_le32(rdev);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	if (err < 0)
		d_drop(dentry);
	return err;
}

static int ceph_create(struct inode *dir, struct dentry *dentry, int mode,
			   struct nameidata *nd)
{
	int err;

	dout(5, "create in dir %p dentry %p name '%.*s'\n",
	     dir, dentry, dentry->d_name.len, dentry->d_name.name);
	if (nd) {
		BUG_ON((nd->flags & LOOKUP_OPEN) == 0);
		err = ceph_lookup_open(dir, dentry, nd, mode);
		return err;
	}

	/* fall back to mknod */
	return ceph_mknod(dir, dentry, (mode & ~S_IFMT) | S_IFREG, 0);
}

static int ceph_symlink(struct inode *dir, struct dentry *dentry,
			    const char *dest)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	char *path;
	int pathlen;
	int err;

	dout(5, "dir_symlink in dir %p dentry %p to '%s'\n", dir, dentry, dest);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_SYMLINK,
				       ceph_ino(dir->i_sb->s_root->d_inode),
				       path, 0, dest,
				       dentry, 1, -1);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}
	ceph_mdsc_lease_release(mdsc, dir, 0, CEPH_LOCK_ICONTENT);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	if (err < 0)
		d_drop(dentry);
	return err;
}

static int ceph_mkdir(struct inode *dir, struct dentry *dentry, int mode)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;
	char *path;
	int pathlen;
	int err;

	dout(5, "dir_mkdir in dir %p dentry %p mode 0%o\n", dir, dentry, mode);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_MKDIR,
				       ceph_ino(dir->i_sb->s_root->d_inode),
				       path, 0, 0,
				       dentry, 1, -1);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}
	ceph_mdsc_lease_release(mdsc, dir, 0, CEPH_LOCK_ICONTENT);
	rhead = req->r_request->front.iov_base;
	rhead->args.mkdir.mode = cpu_to_le32(mode);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	if (err < 0)
		d_drop(dentry);
	return err;
}

static int ceph_link(struct dentry *old_dentry, struct inode *dir,
			 struct dentry *dentry)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	char *oldpath, *path;
	int oldpathlen, pathlen;
	int err;

	dout(5, "dir_link in dir %p old_dentry %p dentry %p\n", dir,
	     old_dentry, dentry);
	oldpath = ceph_build_dentry_path(old_dentry, &oldpathlen);
	if (IS_ERR(oldpath))
		return PTR_ERR(oldpath);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path)) {
		kfree(oldpath);
		return PTR_ERR(path);
	}
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LINK,
				       ceph_ino(dir->i_sb->s_root->d_inode),
				       path,
				       ceph_ino(dir->i_sb->s_root->d_inode),
				       oldpath,
				       dentry, 1, -1);
	kfree(oldpath);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}

	dget(dentry);                /* to match put_request below */
	req->r_last_dentry = dentry; /* use this dentry in fill_trace */

	ceph_mdsc_lease_release(mdsc, dir, 0, CEPH_LOCK_ICONTENT);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	if (err)
		d_drop(dentry);
	else if (req->r_reply_info.trace_numd == 0) {
		/* no trace */
		inc_nlink(old_dentry->d_inode);
		d_instantiate(dentry, old_dentry->d_inode);
	}
	return err;
}

static int ceph_unlink(struct inode *dir, struct dentry *dentry)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct inode *inode = dentry->d_inode;
	struct ceph_mds_request *req;
	char *path;
	int pathlen;
	int err;
	int op = ((dentry->d_inode->i_mode & S_IFMT) == S_IFDIR) ?
		CEPH_MDS_OP_RMDIR : CEPH_MDS_OP_UNLINK;

	dout(5, "dir_unlink/rmdir in dir %p dentry %p inode %p\n",
	     dir, dentry, inode);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, op,
				       ceph_ino(dir->i_sb->s_root->d_inode),
				       path, 0, 0,
				       dentry, 1, -1);
	kfree(path);
	if (IS_ERR(req))
		return PTR_ERR(req);
	ceph_mdsc_lease_release(mdsc, dir, dentry,
				CEPH_LOCK_DN|CEPH_LOCK_ICONTENT);
	ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_ILINK);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);

	if (err == -ENOENT)
		dout(10, "HMMM!\n");

	return err;
}

static int ceph_rename(struct inode *old_dir, struct dentry *old_dentry,
			   struct inode *new_dir, struct dentry *new_dentry)
{
	struct ceph_client *client = ceph_sb_to_client(old_dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	struct dentry *root = old_dir->i_sb->s_root;
	char *oldpath, *newpath;
	int oldpathlen, newpathlen;
	int err;

	dout(5, "dir_rename in dir %p dentry %p to dir %p dentry %p\n",
	     old_dir, old_dentry, new_dir, new_dentry);
	oldpath = ceph_build_dentry_path(old_dentry, &oldpathlen);
	if (IS_ERR(oldpath))
		return PTR_ERR(oldpath);
	newpath = ceph_build_dentry_path(new_dentry, &newpathlen);
	if (IS_ERR(newpath)) {
		kfree(oldpath);
		return PTR_ERR(newpath);
	}
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_RENAME,
				       ceph_ino(root->d_inode), oldpath,
				       ceph_ino(root->d_inode), newpath,
				       new_dentry, 1, -1);
	kfree(oldpath);
	kfree(newpath);
	if (IS_ERR(req))
		return PTR_ERR(req);
	dget(old_dentry);
	req->r_old_dentry = old_dentry;
	dget(new_dentry);
	req->r_last_dentry = new_dentry;
	ceph_mdsc_lease_release(mdsc, old_dir, old_dentry,
				CEPH_LOCK_DN|CEPH_LOCK_ICONTENT);
	if (new_dentry->d_inode)
		ceph_mdsc_lease_release(mdsc, new_dentry->d_inode, 0,
					CEPH_LOCK_ILINK);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	return err;
}



/*
 * check if dentry lease, or parent directory inode lease or cap says
 * this dentry is still valid
 */
static int ceph_dentry_revalidate(struct dentry *dentry, struct nameidata *nd)
{
	struct inode *dir = dentry->d_parent->d_inode;

	dout(10, "d_revalidate %p '%.*s' inode %p\n", dentry,
	     dentry->d_name.len, dentry->d_name.name, dentry->d_inode);

	if (ceph_inode_lease_valid(dir, CEPH_LOCK_ICONTENT)) {
		dout(20, "dentry_revalidate %p have ICONTENT on dir inode %p\n",
		     dentry, dir);
		return 1;
	}
	if (ceph_dentry_lease_valid(dentry)) {
		dout(20, "dentry_revalidate %p lease valid\n", dentry);
		return 1;
	}

	dout(20, "dentry_revalidate %p no lease\n", dentry);
	d_drop(dentry);
	return 0;
}

static void ceph_dentry_release(struct dentry *dentry)
{
	BUG_ON(dentry->d_fsdata);
}

const struct file_operations ceph_dir_fops = {
	.read = generic_read_dir,
	.readdir = ceph_readdir,
	.llseek = ceph_dir_llseek,
	.open = ceph_open,
	.release = ceph_release,
};

const struct inode_operations ceph_dir_iops = {
	.lookup = ceph_lookup,
	.getattr = ceph_getattr,
	.setattr = ceph_setattr,
	.mknod = ceph_mknod,
	.symlink = ceph_symlink,
	.mkdir = ceph_mkdir,
	.link = ceph_link,
	.unlink = ceph_unlink,
	.rmdir = ceph_unlink,
	.rename = ceph_rename,
	.create = ceph_create,
};

struct dentry_operations ceph_dentry_ops = {
	.d_revalidate = ceph_dentry_revalidate,
	.d_release = ceph_dentry_release,
};

