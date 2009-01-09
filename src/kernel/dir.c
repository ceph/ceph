#include <linux/namei.h>
#include <linux/sched.h>

#include "ceph_debug.h"

int ceph_debug_dir = -1;
#define DOUT_MASK DOUT_MASK_DIR
#define DOUT_VAR ceph_debug_dir
#include "super.h"

/*
 * Ceph MDS operations are specified in terms of a base ino and
 * relative path.  Thus, the client can specify an operation on a
 * specific inode (e.g., a getattr due to fstat(2)), or as a path
 * relative to, say, the root directory.
 *
 * Because the MDS does not statefully track which inodes the client
 * has in its cache, the client has to take care to only specify
 * operations relative to inodes it knows the MDS has cached.  (The
 * MDS cannot do a lookup by ino.)
 *
 * So, in general, we try to specify operations in terms of generate
 * path names relative to the root.
 */

const struct inode_operations ceph_dir_iops;
const struct file_operations ceph_dir_fops;
struct dentry_operations ceph_dentry_ops;

static int ceph_dentry_revalidate(struct dentry *dentry, struct nameidata *nd);

/*
 * build a dentry's path.  allocate on heap; caller must kfree.  based
 * on build_path_from_dentry in fs/cifs/dir.c.
 *
 * encode hidden .snap dirs as a double /, i.e.
 *   foo/.snap/bar -> foo//bar
 */
char *ceph_build_path(struct dentry *dentry, int *plen, u64 *base, int min)
{
	struct dentry *temp;
	char *path;
	int len, pos;

	if (dentry == NULL)
		return ERR_PTR(-EINVAL);

retry:
	len = 0;
	for (temp = dentry; !IS_ROOT(temp);) {
		struct inode *inode = temp->d_inode;
		if (inode && ceph_snap(inode) == CEPH_SNAPDIR)
			len++;  /* slash only */
		else
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
	for (temp = dentry; !IS_ROOT(temp) && pos != 0; ) {
		if (temp->d_inode &&
		    ceph_snap(temp->d_inode) == CEPH_SNAPDIR) {
			dout(50, "build_path_dentry path+%d: %p SNAPDIR\n",
			     pos, temp);
		} else {
			pos -= temp->d_name.len;
			if (pos < 0)
				break;
			strncpy(path + pos, temp->d_name.name,
				temp->d_name.len);
			dout(50, "build_path_dentry path+%d: %p '%.*s'\n",
			     pos, temp, temp->d_name.len, path + pos);
		}
		if (pos)
			path[--pos] = '/';
		temp = temp->d_parent;
		if (temp == NULL) {
			derr(1, "corrupt dentry\n");
			kfree(path);
			return ERR_PTR(-EINVAL);
		}
	}
	if (pos != 0) {
		derr(1, "did not end path lookup where expected, "
		     "namelen is %d, pos is %d\n", len, pos);
		/* presumably this is only possible if racing with a
		   rename of one of the parent directories (we can not
		   lock the dentries above us to prevent this, but
		   retrying should be harmless) */
		kfree(path);
		goto retry;
	}

	*base = ceph_ino(temp->d_inode);
	*plen = len;
	dout(10, "build_path_dentry on %p %d built %llx '%.*s'\n",
	     dentry, atomic_read(&dentry->d_count), *base, len, path);
	return path;
}


/*
 * for readdir, encoding the directory frag and offset within that frag
 * into f_pos.
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
	u32 ftype;
	struct ceph_mds_reply_info_parsed *rinfo;

nextfrag:
	dout(5, "readdir filp %p at frag %u off %u\n", filp, frag, off);

	/* do we have the correct frag content buffered? */
	if (fi->frag != frag || fi->last_readdir == NULL) {
		struct ceph_mds_request *req;
		struct ceph_mds_request_head *rhead;
		char *path;
		int pathlen;
		u64 pathbase;
		int op = ceph_snap(inode) == CEPH_SNAPDIR ?
			CEPH_MDS_OP_LSSNAP : CEPH_MDS_OP_READDIR;

		/* discard old result, if any */
		if (fi->last_readdir)
			ceph_mdsc_put_request(fi->last_readdir);

		/* requery frag tree, as the frag topology may have changed */
		frag = ceph_choose_frag(ceph_inode(inode), frag, NULL, NULL);

		dout(10, "readdir querying mds for ino %llx.%llx frag %x\n",
		     ceph_vinop(inode), frag);
		path = ceph_build_path(filp->f_dentry, &pathlen, &pathbase, 1);
		req = ceph_mdsc_create_request(mdsc, op,
					       pathbase, path, 0, NULL,
					       filp->f_dentry, USE_AUTH_MDS);
		kfree(path);
		if (IS_ERR(req))
			return PTR_ERR(req);
		/* hints to request -> mds selection code */
		req->r_direct_mode = USE_AUTH_MDS;
		req->r_direct_hash = frag_value(frag);
		req->r_direct_is_hash = true;
		rhead = req->r_request->front.iov_base;
		rhead->args.readdir.frag = cpu_to_le32(frag);
		err = ceph_mdsc_do_request(mdsc, NULL, req);
		if (err < 0) {
			ceph_mdsc_put_request(req);
			return err;
		}
		dout(10, "readdir got and parsed readdir result=%d"
		     " on frag %x\n", err, frag);
		fi->last_readdir = req;
	}

	/* include . and .. with first fragment */
	if (frag_is_leftmost(frag)) {
		switch (off) {
		case 0:
			dout(10, "readdir off 0 -> '.'\n");
			if (filldir(dirent, ".", 1, make_fpos(0, 0),
				    inode->i_ino, inode->i_mode >> 12) < 0)
				return 0;
			off++;
			filp->f_pos++;
		case 1:
			dout(10, "readdir off 1 -> '..'\n");
			if (filp->f_dentry->d_parent != NULL &&
			    filldir(dirent, "..", 2, make_fpos(0, 1),
				    filp->f_dentry->d_parent->d_inode->i_ino,
				    inode->i_mode >> 12) < 0)
				return 0;
			off++;
			filp->f_pos++;
		}
		skew = -2;  /* compensate for . and .. */
	} else {
		skew = 0;
	}

	rinfo = &fi->last_readdir->r_reply_info;
	dout(10, "readdir frag %x num %d off %d skew %d\n", frag,
	     rinfo->dir_nr, off, skew);
	while (off+skew < rinfo->dir_nr) {
		dout(10, "readdir off %d -> %d / %d name '%.*s'\n",
		     off, off+skew,
		     rinfo->dir_nr, rinfo->dir_dname_len[off+skew],
		     rinfo->dir_dname[off+skew]);
		ftype = le32_to_cpu(rinfo->dir_in[off+skew].in->mode) >> 12;
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
	if (!frag_is_rightmost(frag)) {
		frag = frag_next(frag);
		off = 0;
		filp->f_pos = make_fpos(frag, off);
		dout(10, "readdir next frag is %x\n", frag);
		goto nextfrag;
	}

	dout(20, "readdir done.\n");
	return 0;
}

static loff_t ceph_dir_llseek(struct file *file, loff_t offset, int origin)
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
		/*
		 * discard buffered readdir content on seekdir(0)
		 */
		if (offset == 0 && fi->last_readdir) {
			dout(10, "dir_llseek dropping %p content\n", file);
			ceph_mdsc_put_request(fi->last_readdir);
			fi->last_readdir = NULL;
		}
	}
	mutex_unlock(&inode->i_mutex);
	return retval;
}


/*
 * Process result of a lookup/open request.
 *
 * Mainly, make sure that return r_last_dentry (the dentry
 * the MDS trace ended on) in place of the original VFS-provided
 * dentry, if they differ.
 *
 * Gracefully handle the case where the MDS replies with -ENOENT and
 * no trace (which it may do, at its discretion, e.g., if it doesn't
 * care to issue a lease on the negative dentry).
 */
struct dentry *ceph_finish_lookup(struct ceph_mds_request *req,
				  struct dentry *dentry, int err)
{
	struct ceph_client *client = ceph_client(dentry->d_sb);
	struct inode *parent = dentry->d_parent->d_inode;

	/* snap dir? */
	if (err == -ENOENT &&
	    ceph_vino(parent).ino != 1 &&  /* no .snap in root dir */
	    strcmp(dentry->d_name.name, client->mount_args.snapdir_name) == 0) {
		struct inode *inode = ceph_get_snapdir(parent);
		dout(10, "ENOENT on snapdir %p '%.*s', linking to snapdir %p\n",
		     dentry, dentry->d_name.len, dentry->d_name.name, inode);
		d_add(dentry, inode);
		err = 0;
	}

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
			} else {
				d_add(dentry, NULL);
			}
		}
		err = 0;
	}
	if (err)
		dentry = ERR_PTR(err);
	else if (dentry != req->r_last_dentry)
		dentry = dget(req->r_last_dentry);   /* we got spliced */
	else
		dentry = NULL;
	return dentry;
}

/*
 * Do a lookup / lstat (same thing, modulo the metadata @mask).
 * @on_inode indicates that we should stat the ino directly, and not a
 * path built from @dentry.
 */
struct dentry *ceph_do_lookup(struct super_block *sb, struct dentry *dentry,
			      int mask, int on_inode, int locked_dir)
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
		WARN_ON(ceph_snap(dentry->d_inode) != CEPH_NOSNAP);
		req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LSTAT,
					       ceph_ino(dentry->d_inode), NULL,
					       0, NULL,
					       dentry, USE_CAP_MDS);
	} else {
		/* build path */
		u64 pathbase;
		path = ceph_build_path(dentry, &pathlen, &pathbase, 1);
		if (IS_ERR(path))
			return ERR_PTR(PTR_ERR(path));
		req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LSTAT,
					       pathbase, path, 0, NULL,
					       dentry, USE_ANY_MDS);
		kfree(path);
	}
	if (IS_ERR(req))
		return ERR_PTR(PTR_ERR(req));
	rhead = req->r_request->front.iov_base;
	rhead->args.stat.mask = cpu_to_le32(mask);
	req->r_last_dentry = dget(dentry); /* try to use this in fill_trace */
	req->r_locked_dir = dentry->d_parent->d_inode;  /* by the VFS */
	err = ceph_mdsc_do_request(mdsc, NULL, req);
	dentry = ceph_finish_lookup(req, dentry, err);
	ceph_mdsc_put_request(req);  /* will dput(dentry) */
	dout(20, "do_lookup result=%p\n", dentry);
	return dentry;
}

/*
 * Try to do a lookup+open, if possible.
 */
static struct dentry *ceph_lookup(struct inode *dir, struct dentry *dentry,
				  struct nameidata *nd)
{
	dout(5, "lookup in dir %p dentry %p '%.*s'\n",
	     dir, dentry, dentry->d_name.len, dentry->d_name.name);

	/* open (but not create!) intent? */
	if (nd &&
	    (nd->flags & LOOKUP_OPEN) &&
	    (nd->flags & LOOKUP_CONTINUE) == 0 && /* only open last component */
	    !(nd->intent.open.flags & O_CREAT)) {
		int mode = nd->intent.open.create_mode & ~current->fs->umask;
		return ceph_lookup_open(dir, dentry, nd, mode, 1);
	}

	return ceph_do_lookup(dir->i_sb, dentry, CEPH_STAT_CAP_INODE_ALL,
			      0, 1);
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
	u64 pathbase;
	int err;

	if (ceph_snap(dir) != CEPH_NOSNAP)
		return -EROFS;

	dout(5, "mknod in dir %p dentry %p mode 0%o rdev %d\n",
	     dir, dentry, mode, rdev);
	path = ceph_build_path(dentry, &pathlen, &pathbase, 1);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_MKNOD,
				       pathbase, path, 0, NULL,
				       dentry, USE_AUTH_MDS);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}
	req->r_locked_dir = dir;
	rhead = req->r_request->front.iov_base;
	rhead->args.mknod.mode = cpu_to_le32(mode);
	rhead->args.mknod.rdev = cpu_to_le32(rdev);
	ceph_release_caps(dir, CEPH_CAP_FILE_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, dir, req);
	if (!err && req->r_reply_info.trace_numd == 0) {
		/*
		 * no trace.  do lookup, in case we are called from create
		 * and the VFS needs a valid dentry.
		 */
		struct dentry *d;
		d = ceph_do_lookup(dir->i_sb, dentry, CEPH_STAT_CAP_INODE_ALL,
				   0, 0);
		if (d) {
			/* ick.  this is untested, and inherently racey... i
			   suppose we _did_ create the file, but it has since
			   been deleted?  hrm. */
			dput(d);
			err = -ESTALE;
			dentry = NULL;
		}
	}
	ceph_mdsc_put_request(req);
	if (err)
		d_drop(dentry);
	return err;
}

static int ceph_create(struct inode *dir, struct dentry *dentry, int mode,
			   struct nameidata *nd)
{
	dout(5, "create in dir %p dentry %p name '%.*s'\n",
	     dir, dentry, dentry->d_name.len, dentry->d_name.name);

	if (ceph_snap(dir) != CEPH_NOSNAP)
		return -EROFS;

	if (nd) {
		BUG_ON((nd->flags & LOOKUP_OPEN) == 0);
		dentry = ceph_lookup_open(dir, dentry, nd, mode, 0);
		/* hrm, what should i do here if we get aliased? */
		if (IS_ERR(dentry))
			return PTR_ERR(dentry);
		return 0;
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
	u64 pathbase;
	int err;

	if (ceph_snap(dir) != CEPH_NOSNAP)
		return -EROFS;

	dout(5, "symlink in dir %p dentry %p to '%s'\n", dir, dentry, dest);
	path = ceph_build_path(dentry, &pathlen, &pathbase, 1);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_SYMLINK,
				       pathbase, path, 0, dest,
				       dentry, USE_AUTH_MDS);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}
	req->r_locked_dir = dir;
	ceph_release_caps(dir, CEPH_CAP_FILE_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, dir, req);
	ceph_mdsc_put_request(req);
	if (err)
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
	u64 pathbase;
	int err;
	char *snap = NULL;
	int snaplen;
	struct dentry *pathdentry = dentry;
	int op = CEPH_MDS_OP_MKDIR;

	if (ceph_snap(dir) == CEPH_SNAPDIR) {
		/* mkdir .snap/foo is a MKSNAP */
		op = CEPH_MDS_OP_MKSNAP;
		snaplen = dentry->d_name.len;
		snap = kmalloc(snaplen + 1, GFP_NOFS);
		memcpy(snap, dentry->d_name.name, snaplen);
		snap[snaplen] = 0;
		pathdentry = d_find_alias(dir);
		dout(5, "mksnap dir %p snap '%s' dn %p\n", dir, snap, dentry);
	} else if (ceph_snap(dir) != CEPH_NOSNAP) {
		return -EROFS;
	} else {
		dout(5, "mkdir dir %p dn %p mode 0%o\n", dir, dentry, mode);
	}
	path = ceph_build_path(pathdentry, &pathlen, &pathbase, 1);
	if (pathdentry != dentry)
		dput(pathdentry);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, op,
				       pathbase, path, 0, snap,
				       dentry, USE_AUTH_MDS);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}

	req->r_last_dentry = dget(dentry); /* use this dentry in fill_trace */
	req->r_locked_dir = dir;
	rhead = req->r_request->front.iov_base;
	rhead->args.mkdir.mode = cpu_to_le32(mode);

	ceph_release_caps(dir, CEPH_CAP_FILE_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, dir, req);
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
	u64 oldpathbase, pathbase;
	int err;

	if (ceph_snap(dir) != CEPH_NOSNAP)
		return -EROFS;

	dout(5, "link in dir %p old_dentry %p dentry %p\n", dir,
	     old_dentry, dentry);
	oldpath = ceph_build_path(old_dentry, &oldpathlen, &oldpathbase, 1);
	if (IS_ERR(oldpath))
		return PTR_ERR(oldpath);
	path = ceph_build_path(dentry, &pathlen, &pathbase, 1);
	if (IS_ERR(path)) {
		kfree(oldpath);
		return PTR_ERR(path);
	}
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LINK,
				       pathbase, path,
				       oldpathbase, oldpath,
				       dentry, USE_AUTH_MDS);
	kfree(oldpath);
	kfree(path);
	if (IS_ERR(req)) {
		d_drop(dentry);
		return PTR_ERR(req);
	}

	req->r_last_dentry = dget(dentry); /* use this dentry in fill_trace */
	req->r_locked_dir = old_dentry->d_inode;

	ceph_release_caps(dir, CEPH_CAP_FILE_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, dir, req);
	if (err) {
		d_drop(dentry);
	} else if (req->r_reply_info.trace_numd == 0) {
		/* no trace */
		struct inode *inode = old_dentry->d_inode;
		inc_nlink(inode);
		atomic_inc(&inode->i_count);
		dget(dentry);
		d_instantiate(dentry, inode);
	}
	ceph_mdsc_put_request(req);
	return err;
}

/*
 * rmdir and unlink are differ only by the metadata op code
 */
static int ceph_unlink(struct inode *dir, struct dentry *dentry)
{
	struct ceph_client *client = ceph_sb_to_client(dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct inode *inode = dentry->d_inode;
	struct ceph_mds_request *req;
	char *path;
	int pathlen;
	u64 pathbase;
	char *snap = NULL;
	int snaplen;
	int err;
	int op = ((dentry->d_inode->i_mode & S_IFMT) == S_IFDIR) ?
		CEPH_MDS_OP_RMDIR : CEPH_MDS_OP_UNLINK;
	struct dentry *pathdentry = dentry;

	if (ceph_snap(dir) == CEPH_SNAPDIR) {
		/* rmdir .snap/foo is RMSNAP */
		op = CEPH_MDS_OP_RMSNAP;
		snaplen = dentry->d_name.len;
		snap = kmalloc(snaplen + 1, GFP_NOFS);
		memcpy(snap, dentry->d_name.name, snaplen);
		snap[snaplen] = 0;
		pathdentry = d_find_alias(dir);
		dout(5, "rmsnap dir %p '%s' dn %p\n", dir, snap, dentry);
	} else if (ceph_snap(dir) != CEPH_NOSNAP) {
		return -EROFS;
	} else {
		dout(5, "unlink/rmdir dir %p dn %p inode %p\n",
		     dir, dentry, inode);
	}
	path = ceph_build_path(pathdentry, &pathlen, &pathbase, 1);
	if (pathdentry != dentry)
		dput(pathdentry);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, op,
				       pathbase, path, 0, snap,
				       dentry, USE_AUTH_MDS);
	kfree(path);
	if (IS_ERR(req))
		return PTR_ERR(req);

	req->r_locked_dir = dir;  /* by VFS */

	ceph_release_caps(dir, CEPH_CAP_FILE_RDCACHE);
	ceph_mdsc_lease_release(mdsc, dir, dentry,
				CEPH_LOCK_DN);
	ceph_release_caps(inode, CEPH_CAP_LINK_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, dir, req);
	ceph_mdsc_put_request(req);

	return err;
}

static int ceph_rename(struct inode *old_dir, struct dentry *old_dentry,
			   struct inode *new_dir, struct dentry *new_dentry)
{
	struct ceph_client *client = ceph_sb_to_client(old_dir->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	char *oldpath, *newpath;
	int oldpathlen, newpathlen;
	u64 oldpathbase, newpathbase;
	int err;

	if (ceph_snap(old_dir) != ceph_snap(new_dir))
		return -EXDEV;
	if (ceph_snap(old_dir) != CEPH_NOSNAP ||
	    ceph_snap(new_dir) != CEPH_NOSNAP)
		return -EROFS;

	dout(5, "dir_rename in dir %p dentry %p to dir %p dentry %p\n",
	     old_dir, old_dentry, new_dir, new_dentry);
	oldpath = ceph_build_path(old_dentry, &oldpathlen, &oldpathbase, 1);
	if (IS_ERR(oldpath))
		return PTR_ERR(oldpath);
	newpath = ceph_build_path(new_dentry, &newpathlen, &newpathbase, 1);
	if (IS_ERR(newpath)) {
		kfree(oldpath);
		return PTR_ERR(newpath);
	}
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_RENAME,
				       oldpathbase, oldpath,
				       newpathbase, newpath,
				       new_dentry, USE_AUTH_MDS);
	kfree(oldpath);
	kfree(newpath);
	if (IS_ERR(req))
		return PTR_ERR(req);
	req->r_old_dentry = dget(old_dentry);
	req->r_last_dentry = dget(new_dentry);
	req->r_locked_dir = new_dir;
	ceph_release_caps(old_dir, CEPH_CAP_FILE_RDCACHE);
	ceph_mdsc_lease_release(mdsc, old_dir, old_dentry,
				CEPH_LOCK_DN);
	if (new_dentry->d_inode)
		ceph_release_caps(new_dentry->d_inode, CEPH_CAP_FILE_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, old_dir, req);
	if (!err && req->r_reply_info.trace_numd == 0) {
		/*
		 * no trace
		 *
		 * Normally d_move() is done by fill_trace (called by
		 * do_request, above).  If there is no trace, we need
		 * to do it here.
		 */
		if (new_dentry->d_inode)
			dput(new_dentry);
		d_move(old_dentry, new_dentry);
	}
	ceph_mdsc_put_request(req);
	return err;
}



/*
 * check if dentry lease, or parent directory inode lease/cap says
 * this dentry is still valid
 */
static int ceph_dentry_revalidate(struct dentry *dentry, struct nameidata *nd)
{
	struct inode *dir = dentry->d_parent->d_inode;

	/* always trust cached snapped metadata... for now */
	if (ceph_snap(dir) != CEPH_NOSNAP) {
		dout(10, "d_revalidate %p '%.*s' inode %p is SNAPPED\n", dentry,
		     dentry->d_name.len, dentry->d_name.name, dentry->d_inode);
		return 1;
	}

	dout(10, "d_revalidate %p '%.*s' inode %p\n", dentry,
	     dentry->d_name.len, dentry->d_name.name, dentry->d_inode);

	if (ceph_ino(dir) != 1 &&  /* ICONTENT is meaningless on root inode */
	    ceph_inode(dir)->i_version == dentry->d_time &&
	    ceph_inode_holds_cap(dir, CEPH_CAP_FILE_RDCACHE)) {
		dout(20, "dentry_revalidate %p %lu file RDCACHE dir %p %llu\n",
		     dentry, dentry->d_time, dir, ceph_inode(dir)->i_version);
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
	struct ceph_dentry_info *di = ceph_dentry(dentry);
	
	if (di) {
		ceph_put_mds_session(di->lease_session);
		kfree(di);
		dentry->d_fsdata = NULL;
	}
}

static int ceph_snapdir_dentry_revalidate(struct dentry *dentry,
					  struct nameidata *nd)
{
	/*
	 * Eventually, we'll want to revalidate snapped metadata
	 * too... probably.
	 */
	return 1;
}



/*
 * read() on a dir.  This weird interface hack only works if mounted
 * with '-o dirstat'.
 */
static ssize_t ceph_read_dir(struct file *file, char __user *buf, size_t size,
			     loff_t *ppos)
{
	struct ceph_file_info *cf = file->private_data;
	struct inode *inode = file->f_dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	int left;

	if (!(ceph_client(inode->i_sb)->mount_args.flags & CEPH_MOUNT_DIRSTAT))
		return -EISDIR;

	if (!cf->dir_info) {
		cf->dir_info = kmalloc(1024, GFP_NOFS);
		if (!cf->dir_info)
			return -ENOMEM;
		cf->dir_info_len =
			sprintf(cf->dir_info,
				"entries:   %20lld\n"
				" files:    %20lld\n"
				" subdirs:  %20lld\n"
				"rentries:  %20lld\n"
				" rfiles:   %20lld\n"
				" rsubdirs: %20lld\n"
				"rbytes:    %20lld\n"
				"rctime:    %10ld.%09ld\n",
				ci->i_files + ci->i_subdirs,
				ci->i_files,
				ci->i_subdirs,
				ci->i_rfiles + ci->i_rsubdirs,
				ci->i_rfiles,
				ci->i_rsubdirs,
				ci->i_rbytes,
				(long)ci->i_rctime.tv_sec,
				(long)ci->i_rctime.tv_nsec);
	}

	if (*ppos >= cf->dir_info_len)
		return 0;
	size = min_t(unsigned, size, cf->dir_info_len-*ppos);
	left = copy_to_user(buf, cf->dir_info + *ppos, size);
	if (left == size)
		return -EFAULT;
	*ppos += (size - left);
	return size - left;
}

static int ceph_dir_fsync(struct file *file, struct dentry *dentry, int datasync)
{
	struct inode *inode = dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	int ret, err;
	struct list_head *p, *n;

	ret = 0;
	dout(0, "sync on directory\n");
	spin_lock(&ci->i_listener_lock);
	list_for_each_safe(p, n, &ci->i_listener_list) {
		struct ceph_mds_request *req =
                        list_entry(p, struct ceph_mds_request, r_listener_item);
		ceph_mdsc_get_request(req);
		spin_unlock(&ci->i_listener_lock);
		if (req->r_timeout) {
			err = wait_for_completion_timeout(&req->r_safe_completion,
							req->r_timeout);
			if (err == 0)
				ret = -EIO;  /* timed out */
		} else {
			wait_for_completion(&req->r_safe_completion);
		}
		spin_lock(&ci->i_listener_lock);

		ceph_mdsc_put_request(req);
	}
	spin_unlock(&ci->i_listener_lock);

	return ret;
}

const struct file_operations ceph_dir_fops = {
	.read = ceph_read_dir,
	.readdir = ceph_readdir,
	.llseek = ceph_dir_llseek,
	.open = ceph_open,
	.release = ceph_release,
	.unlocked_ioctl = ceph_ioctl,
	.fsync = ceph_dir_fsync,
};

const struct inode_operations ceph_dir_iops = {
	.lookup = ceph_lookup,
	.getattr = ceph_getattr,
	.setattr = ceph_setattr,
	.setxattr = ceph_setxattr,
	.getxattr = ceph_getxattr,
	.listxattr = ceph_listxattr,
	.removexattr = ceph_removexattr,
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

struct dentry_operations ceph_snapdir_dentry_ops = {
	.d_revalidate = ceph_snapdir_dentry_revalidate,
};

struct dentry_operations ceph_snap_dentry_ops = {
};
