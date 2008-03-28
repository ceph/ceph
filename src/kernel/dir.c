int ceph_dir_debug = 50;
#define DOUT_VAR ceph_dir_debug
#define DOUT_PREFIX "dir: "
#include "super.h"

#include <linux/namei.h>

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

	path = kmalloc(len+1, GFP_KERNEL);
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
			dout(30, "build_path_dentry path+%d: '%.*s'\n",
			     pos, temp->d_name.len, path + pos);
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

	dout(10, "build_path_dentry on %p build '%.*s'\n",
	     dentry, len, path);
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


static int prepopulate_dir(struct dentry *parent,
			   struct ceph_mds_reply_info *rinfo,
			   int from_mds, unsigned long from_time)
{
	struct qstr dname;
	struct dentry *dn;
	struct inode *in;
	int i;

	for (i = 0; i < rinfo->dir_nr; i++) {
		/* dentry */
		dname.name = rinfo->dir_dname[i];
		dname.len = rinfo->dir_dname_len[i];
		dname.hash = full_name_hash(dname.name, dname.len);

		dn = d_lookup(parent, &dname);
		dout(30, "calling d_lookup on parent=%p name=%.*s"
		     " returned %p\n", parent, dname.len, dname.name, dn);

		if (!dn) {
			dn = d_alloc(parent, &dname);
			if (dn == NULL) {
				dout(30, "d_alloc badness\n");
				return -1;
			}
			ceph_init_dentry(dn);
		}
		ceph_update_dentry_lease(dn, rinfo->dir_dlease[i], 
					 from_mds, from_time);

		/* inode */
		if (dn->d_inode == NULL) {
			in = new_inode(parent->d_sb);
			if (in == NULL) {
				dout(30, "new_inode badness\n");
				d_delete(dn);
				return -1;
			}
		} else {
			in = dn->d_inode;
		}

		if (ceph_ino(in) !=
		    le64_to_cpu(rinfo->dir_in[i].in->ino)) {
			if (ceph_fill_inode(in, rinfo->dir_in[i].in) < 0) {
				dout(30, "ceph_fill_inode badness\n");
				iput(in);
				d_delete(dn);
				return -1;
			}
			d_instantiate(dn, in);
			if (d_unhashed(dn))
				d_rehash(dn);
			dout(10, "dir_readdir added dentry %p ino %llx %d/%d\n",
			     dn, ceph_ino(in), i, rinfo->dir_nr);
		} else {
			if (ceph_fill_inode(in, rinfo->dir_in[i].in) < 0) {
				dout(30, "ceph_fill_inode badness\n");
				return -1;
			}
		}
		ceph_update_inode_lease(in, rinfo->dir_ilease[i], from_mds,
					from_time);

		dput(dn);
	}
	return 0;
}

static int ceph_dir_readdir(struct file *filp, void *dirent, filldir_t filldir)
{
	struct ceph_file_info *fi = filp->private_data;
	struct inode *inode = filp->f_dentry->d_inode;
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;
	unsigned frag = fpos_frag(filp->f_pos);
	unsigned off = fpos_off(filp->f_pos);
	unsigned skew = -2;
	int err;
	__u32 ftype;
	struct ceph_mds_reply_info *rinfo;

nextfrag:
	dout(5, "dir_readdir filp %p at frag %u off %u\n", filp, frag, off);
	if (fi->frag != frag || fi->last_readdir == NULL) {
		struct ceph_mds_request *req;
		struct ceph_mds_request_head *rhead;

		/* query mds */
		if (fi->last_readdir) {
			ceph_mdsc_put_request(fi->last_readdir);
			fi->last_readdir = 0;
		}
		dout(10, "dir_readdir querying mds for ino %llx frag %u\n",
		     ceph_ino(inode), frag);
		req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_READDIR,
					       ceph_ino(inode), "", 0, 0);
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
		     " on frag %u\n", err, frag);
		fi->last_readdir = req;

		/* pre-populate dentry cache */
		prepopulate_dir(filp->f_dentry, &req->r_reply_info, 
				le32_to_cpu(req->r_reply->hdr.src.name.num),
				req->r_from_time);
	}

	/* include . and .. with first fragment */
	if (frag == 0) {
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
	} else
		skew = -2;

	rinfo = &fi->last_readdir->r_reply_info;
	while (off+skew < rinfo->dir_nr) {
		dout(10, "dir_readdir off %d -> %d / %d name '%s'\n",
		     off, off+skew,
		     rinfo->dir_nr, rinfo->dir_dname[off+skew]);
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
		dout(10, "dir_readdir next frag is %u\n", frag);
		goto nextfrag;
	}

	dout(20, "dir_readdir done.\n");
	return 0;
}


const struct file_operations ceph_dir_fops = {
	.read = generic_read_dir,
	.readdir = ceph_dir_readdir,
	.open = ceph_open,
	.release = ceph_release,
};


int ceph_do_lookup(struct super_block *sb, struct dentry *dentry, int mask)
{
	struct ceph_client *client = ceph_sb_to_client(sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	char *path;
	int pathlen;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;
	int err;

	dout(10, "do_lookup %p mask %d\n", dentry, CEPH_STAT_MASK_INODE_ALL);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LSTAT,
				       ceph_ino(sb->s_root->d_inode),
				       path, 0, 0);
	kfree(path);
	if (IS_ERR(req))
		return PTR_ERR(req);
	rhead = req->r_request->front.iov_base;
	rhead->args.stat.mask = cpu_to_le32(mask);
	dget(dentry);                /* to match put_request below */
	req->r_last_dentry = dentry; /* use this dentry in fill_trace */
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);  /* will dput(dentry) */
	if (err == -ENOENT) {
		ceph_init_dentry(dentry);
		d_add(dentry, NULL);
		err = 0;
	}
	dout(20, "do_lookup result=%d\n", err);
	return err;
}

static struct dentry *ceph_dir_lookup(struct inode *dir, struct dentry *dentry,
				      struct nameidata *nd)
{
	int err;

	dout(5, "dir_lookup in dir %p dentry %p '%.*s'\n",
	     dir, dentry, dentry->d_name.len, dentry->d_name.name);

	/* open (but not create!) intent? */
	if (nd->flags & LOOKUP_OPEN &&
	    !(nd->intent.open.flags & O_CREAT)) {
		err = ceph_lookup_open(dir, dentry, nd);
		return ERR_PTR(err);
	}

	err = ceph_do_lookup(dir->i_sb, dentry, CEPH_STAT_MASK_INODE_ALL);
	if (err == -ENOENT)
		d_add(dentry, NULL);
	else if (err < 0)
		return ERR_PTR(err);

	return NULL;
}

static int ceph_dir_create(struct inode *dir, struct dentry *dentry, int mode,
			   struct nameidata *nd)
{
	int err;
	dout(5, "create in dir %p dentry %p name '%.*s'\n",
	     dir, dentry, dentry->d_name.len, dentry->d_name.name);
	BUG_ON((nd->flags & LOOKUP_OPEN) == 0);
	err = ceph_lookup_open(dir, dentry, nd);
	return err;
}

static int ceph_dir_mknod(struct inode *dir, struct dentry *dentry,
			  int mode, dev_t rdev)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;
	char *path;
	int pathlen;
	int err;

	dout(5, "dir_mknod in dir %p dentry %p mode %d rdev %d\n",
	     dir, dentry, mode, rdev);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_MKNOD,
				       ceph_ino(dir->i_sb->s_root->d_inode),
				       path, 0, 0);
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

static int ceph_dir_symlink(struct inode *dir, struct dentry *dentry,
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
				       path, 0, dest);
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

static int ceph_dir_mkdir(struct inode *dir, struct dentry *dentry, int mode)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;
	char *path;
	int pathlen;
	int err;

	dout(5, "dir_mkdir in dir %p dentry %p mode %d\n", dir, dentry, mode);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_MKDIR,
				       ceph_ino(dir->i_sb->s_root->d_inode),
				       path, 0, 0);
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

static int ceph_dir_link(struct dentry *old_dentry, struct inode *dir,
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
				       oldpath);
	kfree(oldpath);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}
	ceph_mdsc_lease_release(mdsc, dir, 0, CEPH_LOCK_ICONTENT);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	if (!err) {
		igrab(old_dentry->d_inode);
		inc_nlink(old_dentry->d_inode);
		d_instantiate(dentry, old_dentry->d_inode);
	} else
		d_drop(dentry);
	
	return err;
}

static int ceph_dir_unlink(struct inode *dir, struct dentry *dentry)
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
				       path, 0, 0);
	kfree(path);
	if (IS_ERR(req))
		return PTR_ERR(req);
	ceph_mdsc_lease_release(mdsc, dir, dentry, 
				CEPH_LOCK_DN|CEPH_LOCK_ICONTENT);
	ceph_mdsc_lease_release(mdsc, dentry->d_inode, 0, CEPH_LOCK_ILINK);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);

	if (!err) {
		if (dentry->d_inode)
			drop_nlink(dentry->d_inode);
	}

	return err;
}

static int ceph_dir_rename(struct inode *old_dir, struct dentry *old_dentry,
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
				       ceph_ino(root->d_inode), newpath);
	kfree(oldpath);
	kfree(newpath);
	if (IS_ERR(req))
		return PTR_ERR(req);
	ceph_mdsc_lease_release(mdsc, old_dir, old_dentry, 
				CEPH_LOCK_DN|CEPH_LOCK_ICONTENT);
	if (new_dentry->d_inode)
		ceph_mdsc_lease_release(mdsc, new_dentry->d_inode, 0, 
					CEPH_LOCK_ILINK);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	return err;
}

static int ceph_d_revalidate(struct dentry *dentry, struct nameidata *nd)
{
	int mds = (long)dentry->d_fsdata;
	struct inode *dir = dentry->d_parent->d_inode;
	struct ceph_inode_info *dirci = ceph_inode(dir);

	dout(20, "d_revalidate ttl %lu mds %d now %lu\n", dentry->d_time, 
	     mds, jiffies);
	
	/* does dir inode lease or cap cover it? */
	if (dirci->i_lease_mds >= 0 &&
	    time_after(dirci->i_lease_ttl, jiffies) &&
	    (dirci->i_lease_mask & CEPH_LOCK_ICONTENT)) {
		dout(20, "d_revalidate have ICONTENT on dir inode %p, ok\n",
		     dir);
		return 1;
	}
	if (ceph_caps_issued(dirci) & (CEPH_CAP_EXCL|CEPH_CAP_RDCACHE)) {
		dout(20, "d_revalidate have EXCL|RDCACHE caps on dir inode %p"
		     ", ok\n", dir);
		return 1;
	}

	/* dentry lease? */
	if (mds >= 0 && time_after(dentry->d_time, jiffies)) {
		dout(20, "d_revalidate - dentry %p lease valid\n", dentry);
		return 1;
	}

	dout(20, "d_revalidate - dentry %p expired\n", dentry);
	d_drop(dentry);
	return 0;
}


const struct inode_operations ceph_dir_iops = {
	.lookup = ceph_dir_lookup,
	.getattr = ceph_inode_getattr,
	.setattr = ceph_setattr,
	.mknod = ceph_dir_mknod,
	.symlink = ceph_dir_symlink,
	.mkdir = ceph_dir_mkdir,
	.link = ceph_dir_link,
	.unlink = ceph_dir_unlink,
	.rmdir = ceph_dir_unlink,
	.rename = ceph_dir_rename,
	.create = ceph_dir_create,
};

struct dentry_operations ceph_dentry_ops = {
	.d_revalidate = ceph_d_revalidate,
};

