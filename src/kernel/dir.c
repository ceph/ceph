int ceph_dir_debug = 50;
#define DOUT_VAR ceph_dir_debug
#define DOUT_PREFIX "dir: "
#include "super.h"

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
			dout(30, "build_path_dentry path+%d: '%s'\n",
			     pos, path + pos);
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
		/* presumably this is only possible if racing with a rename
		   of one of the parent directories  (we can not lock the dentries
		   above us to prevent this, but retrying should be harmless) */
		kfree(path);
		goto retry;
	}

	dout(10, "build_path_dentry on %p build '%s' len %d\n",
	     dentry, path, len);
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

static int ceph_dir_readdir(struct file *filp, void *dirent, filldir_t filldir)
{
	struct ceph_file_info *fi = filp->private_data;
	struct inode *inode = filp->f_dentry->d_inode;
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;
	unsigned frag = fpos_frag(filp->f_pos);
	unsigned off = fpos_off(filp->f_pos);
	unsigned skew = -2;
	int err;
	int i;
	struct qstr dname;
	struct dentry *parent, *dn;
	struct inode *in;
	struct ceph_mds_reply_info *rinfo;

nextfrag:
	dout(5, "dir_readdir filp %p at frag %u off %u\n", filp, frag, off);
	if (fi->frag != frag || fi->req == NULL) {
		struct ceph_mds_request *req;
		struct ceph_mds_request_head *rhead;
		struct ceph_mds_reply_info *rinfo;

		/* query mds */
		if (fi->req)
			ceph_mdsc_put_request(fi->req);

		dout(10, "dir_readdir querying mds for ino %llx frag %u\n",
		     ceph_ino(inode), frag);
		req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_READDIR,
					       ceph_ino(inode), "", 0, 0);
		if (IS_ERR(req))
			return PTR_ERR(req);
		fi->req = req;
		rhead = req->r_request->front.iov_base;
		rhead->args.readdir.frag = cpu_to_le32(frag);
		err = ceph_mdsc_do_request(mdsc, req);
		if (err < 0)
		    return err;
		rinfo = &fi->req->r_reply_info;
		err = le32_to_cpu(rinfo->head->result);
		dout(10, "dir_readdir got and parsed readdir result=%d"
		     " on frag %u\n", err, frag);
		if (err < 0) {
			ceph_mdsc_put_request(req);
			fi->req = 0;
			return err;
		}

		/* pre-populate dentry cache */
		parent = filp->f_dentry;
		for (i = 0; i < rinfo->dir_nr; i++) {
			dname.name = rinfo->dir_dname[i];
			dname.len = rinfo->dir_dname_len[i];
			dname.hash = full_name_hash(dname.name, dname.len);

			dn = d_lookup(parent, &dname);
			dout(30, "calling d_lookup on parent=%p name=%s"
			     " returned %p\n", parent, dname.name, dn);

			if (!dn) {
				dn = d_alloc(parent, &dname);
				if (dn == NULL) {
					dout(30, "d_alloc badness\n");
					break;
				}
			}

			if (dn->d_inode == NULL) {
				in = new_inode(parent->d_sb);
				if (in == NULL) {
					dout(30, "new_inode badness\n");
					d_delete(dn);
					break;
				}
			} else {
				in = dn->d_inode;
			}

			ceph_touch_dentry(dn);
			if (ceph_ino(in) !=
			    le64_to_cpu(rinfo->dir_in[i].in->ino)) {
				if (ceph_fill_inode(in, rinfo->dir_in[i].in) < 0) {
					dout(30, "ceph_fill_inode badness\n");
					iput(in);
					d_delete(dn);
					break;
				}
				d_add(dn, in);
				dout(10, "dir_readdir added dentry %p inode %llx %d/%d\n",
				     dn, ceph_ino(in), i, rinfo->dir_nr);
			} else {
				if (ceph_fill_inode(in, rinfo->dir_in[i].in) < 0) {
					dout(30, "ceph_fill_inode badness\n");
					break;
				}
			}


			dput(dn);
		}
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

	rinfo = &fi->req->r_reply_info;
	while (off+skew < rinfo->dir_nr) {
		dout(10, "dir_readdir off %d -> %d / %d name '%s'\n", off, off+skew,
		     rinfo->dir_nr, rinfo->dir_dname[off+skew]);
		if (filldir(dirent,
			    rinfo->dir_dname[off+skew],
			    rinfo->dir_dname_len[off+skew],
			    make_fpos(frag, off),
			    le64_to_cpu(rinfo->dir_in[off+skew].in->ino),
			    le32_to_cpu(rinfo->dir_in[off+skew].in->mode >> 12)) < 0) {
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


int ceph_request_lookup(struct super_block *sb, struct dentry *dentry)
{
	struct ceph_client *client = ceph_sb_to_client(sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	char *path;
	int pathlen;
	struct ceph_mds_request *req;
	int err;

	/* regular lookup */
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LSTAT,
				       ceph_ino(sb->s_root->d_inode), path, 0, 0);
	kfree(path);
	if (IS_ERR(req))
		return PTR_ERR(req);
	err = ceph_mdsc_do_request(mdsc, req);
	if (err < 0)
		return err;
	err = le32_to_cpu(req->r_reply_info.head->result);
	ceph_mdsc_put_request(req);
	dout(20, "dir_lookup result=%d\n", err);
	return err;
}

void ceph_touch_dentry(struct dentry *dentry)
{
	dentry->d_time = jiffies;
	dentry->d_op = &ceph_dentry_ops;
}

static struct dentry *ceph_dir_lookup(struct inode *dir, struct dentry *dentry,
				      struct nameidata *nd)
{
	int err;

	dout(5, "dir_lookup in dir %p dentry %p '%s'\n", dir, dentry, dentry->d_name.name);

	/* open(|create) intent? */
	/*
	if (nd->flags & LOOKUP_OPEN) {
		err = ceph_lookup_open(dir, dentry, nd);
		return ERR_PTR(err);
	}
	*/

	err = ceph_request_lookup(dir->i_sb, dentry);
	if (err == -ENOENT) {
		dout(10, "ENOENT, adding a null dentry\n");
		ceph_touch_dentry(dentry);
		d_add(dentry, NULL);
	} else if (err < 0)
		return ERR_PTR(err);

	return NULL;
}

static int ceph_dir_mknod(struct inode *dir, struct dentry *dentry, int mode, dev_t rdev)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;
	char *path;
	int pathlen;
	int err;

	dout(5, "dir_mknod in dir %p dentry %p mode %d rdev %d\n", dir, dentry, mode, rdev);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_MKNOD,
				       ceph_ino(dir->i_sb->s_root->d_inode), path, 0, 0);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}
	rhead = req->r_request->front.iov_base;
	rhead->args.mknod.mode = cpu_to_le32(mode);
	rhead->args.mknod.rdev = cpu_to_le32(rdev);
	err = ceph_mdsc_do_request(mdsc, req);
	if (err < 0) {
		d_drop(dentry);
		return err;
	}

	err = le32_to_cpu(req->r_reply_info.head->result);
	if (err == 0) {
		if (req->r_last_inode == NULL) {
			/* TODO handle this one */
			err = -ENOMEM;
			goto done;
		}
		//dout(10, "rinfo.dir_in=%p rinfo.trace_nr=%d\n", rinfo.trace_in, rinfo.trace_nr);
	}
done:
	ceph_mdsc_put_request(req);
	return err;
}

static int ceph_dir_symlink(struct inode *dir, struct dentry *dentry, const char *dest)
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
				       ceph_ino(dir->i_sb->s_root->d_inode), path, 0, dest);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}
	err = ceph_mdsc_do_request(mdsc, req);
	if (err < 0) {
		d_drop(dentry);
		return err;
	}

	err = le32_to_cpu(req->r_reply_info.head->result);
	if (err == 0) {
		if (req->r_last_inode == NULL) {
			/* TODO handle this one */
			err = -ENOMEM;
			goto done;
		}
	}
done:
	ceph_mdsc_put_request(req);
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
				       ceph_ino(dir->i_sb->s_root->d_inode), path, 0, 0);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}
	rhead = req->r_request->front.iov_base;
	rhead->args.mkdir.mode = cpu_to_le32(mode);
	err = ceph_mdsc_do_request(mdsc, req);
	if (err < 0) {
		d_drop(dentry);
		return err;
	}

	err = le32_to_cpu(req->r_reply_info.head->result);
	if (err == 0) {
		if (req->r_last_inode == NULL) {
			/* TODO handle this one */
			err = -ENOMEM;
			goto done_mkdir;
		}
	}
done_mkdir:
	ceph_mdsc_put_request(req);
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

	dout(5, "dir_unlink/rmdir in dir %p dentry %p inode %p\n", dir, dentry, inode);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, op,
				       ceph_ino(dir->i_sb->s_root->d_inode), path, 0, 0);
	kfree(path);
	if (IS_ERR(req))
		return PTR_ERR(req);
	err = ceph_mdsc_do_request(mdsc, req);
	if (err < 0)
		return err;

	err = le32_to_cpu(req->r_reply_info.head->result);
	if (err == 0) {
		inode_dec_link_count(req->r_last_inode);
		/* FIXME update dir mtime etc. from reply trace */
	}
	ceph_mdsc_put_request(req);
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
	err = ceph_mdsc_do_request(mdsc, req);
	if (err < 0)
		return err;

	err = le32_to_cpu(req->r_reply_info.head->result);
	if (err == 0) {
		/* FIXME update dir mtime etc. from reply trace */
	}
	ceph_mdsc_put_request(req);
	return err;
}

static int
ceph_dir_create(struct inode *dir, struct dentry *dentry, int mode,
		struct nameidata *nd)
{
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(dir)->mdsc;
	ceph_ino_t pathbase;
	char *path;
	int pathlen;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;
	int err;
	
	dout(5, "create in dir %p dentry %p name '%s' flags %d\n", dir, dentry, dentry->d_name.name, mode);
	pathbase = ceph_ino(dir->i_sb->s_root->d_inode);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_MKNOD, pathbase, path, 0, 0);
	kfree(path);
	if (IS_ERR(req))
		return PTR_ERR(req);
	rhead = req->r_request->front.iov_base;
	rhead->args.mknod.mode = cpu_to_le32(mode);
	rhead->args.mknod.rdev = 0;
	err = ceph_mdsc_do_request(mdsc, req);
	if (err < 0)
		return err;
	
	dout(10, "create got and parsed result\n");
	
	err = le32_to_cpu(req->r_reply_info.head->result);
	if (err == 0) {
		if (req->r_last_inode == NULL) {
			err = -ENOMEM;
			goto done_create;
		}
	}
done_create:
	ceph_mdsc_put_request(req);
	return err;
}

static int ceph_d_revalidate(struct dentry *dentry, struct nameidata *nd)
{
	dout(20, "ceph_d_revalidate\n");
	if (dentry->d_inode) {
		if (ceph_inode_revalidate(dentry)) {
			dout(20, "ceph_d_revalidate (invalid entry)\n");
			return 0;
		}
	} else {
		if (!ceph_lookup_cache || time_after(jiffies, dentry->d_time+CACHE_HZ)) {
			d_drop(dentry);
			return 0;
		}
	}
	return 1;
}


const struct inode_operations ceph_dir_iops = {
	.lookup = ceph_dir_lookup,
	.getattr = ceph_inode_getattr,
	.setattr = ceph_setattr,
	.mknod = ceph_dir_mknod,
	.symlink = ceph_dir_symlink,
	.mkdir = ceph_dir_mkdir,
	.unlink = ceph_dir_unlink,
	.rmdir = ceph_dir_unlink,
	.rename = ceph_dir_rename,
	.create = ceph_dir_create,
};

struct dentry_operations ceph_dentry_ops = {
	.d_revalidate = ceph_d_revalidate,
};

