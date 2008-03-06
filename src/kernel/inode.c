#include <linux/module.h>
#include <linux/fs.h>
#include <linux/smp_lock.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/uaccess.h>
#include <linux/kernel.h>
#include <linux/ceph_fs.h>

int ceph_inode_debug = 50;
#define DOUT_VAR ceph_inode_debug
#define DOUT_PREFIX "inode: "
#include "super.h"

const struct inode_operations ceph_symlink_iops;

int ceph_get_inode(struct super_block *sb, __u64 ino, struct inode **pinode)
{
	struct ceph_inode_info *ci;

	BUG_ON(pinode == NULL);

	*pinode = iget_locked(sb, ino);
	if (*pinode == NULL) 
		return -ENOMEM;
	if ((*pinode)->i_state & I_NEW)
		unlock_new_inode(*pinode);

	ci = ceph_inode(*pinode);
	ceph_set_ino(*pinode, ino);

	ci->i_hashval = (*pinode)->i_ino;

	dout(30, "get_inode on %lu=%llx got %p\n", (*pinode)->i_ino, ino, *pinode);
	return 0;
}

/*
 * populate an inode based on info from mds.
 * may be called on new or existing inodes.
 */
int ceph_fill_inode(struct inode *inode, struct ceph_mds_reply_inode *info) 
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int i;
	int symlen;
	u32 su = le32_to_cpu(info->layout.fl_stripe_unit);
	int blkbits = fls(su)-1;
	unsigned blksize = 1 << blkbits;
	u64 size = le64_to_cpu(info->size);
	u64 blocks = size + blksize - 1;
	do_div(blocks, blksize);

	dout(30, "fill_inode %p ino %lu/%llx by %d.%d sz=%llu mode %o nlink %d\n", 
	     inode, inode->i_ino, ceph_ino(inode), inode->i_uid, inode->i_gid, 
	     inode->i_size, inode->i_mode, inode->i_nlink);
	dout(30, " su %d, blkbits %d, blksize %u, blocks %llu\n",
	     su, blkbits, blksize, blocks);	

	ceph_set_ino(inode, le64_to_cpu(info->ino));
	inode->i_mode = le32_to_cpu(info->mode);
	inode->i_uid = le32_to_cpu(info->uid);
	inode->i_gid = le32_to_cpu(info->gid);
	inode->i_nlink = le32_to_cpu(info->nlink);
	inode->i_rdev = le32_to_cpu(info->rdev);
	spin_lock(&inode->i_lock);
	inode->i_size = size;
	inode->i_blkbits = blkbits;
	inode->i_blocks = blocks;
	spin_unlock(&inode->i_lock);

	if (ci->i_hashval != inode->i_ino) {
		insert_inode_hash(inode);
		ci->i_hashval = inode->i_ino;
	}

	ceph_decode_timespec(&inode->i_atime, &info->atime);
	ceph_decode_timespec(&inode->i_mtime, &info->mtime);
	ceph_decode_timespec(&inode->i_ctime, &info->ctime);

	/* ceph inode */
	ci->i_layout = info->layout; 
	if (ci->i_symlink)
		kfree(ci->i_symlink);
	ci->i_symlink = 0;

	if (le32_to_cpu(info->fragtree.nsplits) > 0) {
		//ci->i_fragtree = kmalloc(...);
		BUG_ON(1); // write me
	}
	ci->i_fragtree->nsplits = le32_to_cpu(info->fragtree.nsplits);
	for (i=0; i<ci->i_fragtree->nsplits; i++)
		ci->i_fragtree->splits[i] = le32_to_cpu(info->fragtree.splits[i]);

	ci->i_frag_map_nr = 1;
	ci->i_frag_map[0].frag = 0;
	ci->i_frag_map[0].mds = 0; // FIXME
	
	ci->i_old_atime = inode->i_atime;

	ci->i_max_size = le64_to_cpu(info->max_size);

	inode->i_mapping->a_ops = &ceph_aops;

	switch (inode->i_mode & S_IFMT) {
	case S_IFIFO:
	case S_IFBLK:
	case S_IFCHR:
	case S_IFSOCK:
		dout(20, "%p is special\n", inode);
		init_special_inode(inode, inode->i_mode, inode->i_rdev);
		break;
	case S_IFREG:
		dout(20, "%p is a file\n", inode);
		inode->i_op = &ceph_file_iops;
		inode->i_fop = &ceph_file_fops;
		break;
	case S_IFLNK:
		dout(20, "%p is a symlink\n", inode);
		inode->i_op = &ceph_symlink_iops;
		symlen = le32_to_cpu(*(__u32*)(info->fragtree.splits+ci->i_fragtree->nsplits));
		dout(20, "symlink len is %d\n", symlen);
		BUG_ON(symlen != ci->vfs_inode.i_size);
		ci->i_symlink = kmalloc(symlen+1, GFP_KERNEL);
		if (ci->i_symlink == NULL)
			return -ENOMEM;
		memcpy(ci->i_symlink, 
		       (char*)(info->fragtree.splits+ci->i_fragtree->nsplits) + 4,
		       symlen);
		ci->i_symlink[symlen] = 0;
		dout(20, "symlink is '%s'\n", ci->i_symlink);
		break;
	case S_IFDIR:
		dout(20, "%p is a dir\n", inode);
		inode->i_op = &ceph_dir_iops;
		inode->i_fop = &ceph_dir_fops;
		break;
	default:
		derr(0, "BAD mode 0x%x S_IFMT 0x%x\n",
		     inode->i_mode, inode->i_mode & S_IFMT);
		return -EINVAL;
	}

	ci->time = jiffies;

	return 0;
}

int ceph_fill_trace(struct super_block *sb, struct ceph_mds_reply_info *prinfo, 
		struct inode **lastinode, struct dentry **lastdentry)
{
	int err = 0;
	struct qstr dname;
	struct dentry *dn, *parent = NULL;
	struct inode *in;
	int i = 0;

	BUG_ON(sb == NULL);

	if (lastinode)
		*lastinode = NULL;

	if (lastdentry)
		*lastdentry = NULL;

	dn = sb->s_root;
	dget(dn);
	in = dn->d_inode;

	for (i=0; i<prinfo->trace_nr; i++)
		if (ceph_ino(in) == prinfo->trace_in[i].in->ino)
			break;

	if (i == prinfo->trace_nr) {
		dout(10, "ceph_fill_trace did not locate mounted root!\n");
		return -ENOENT;
	}

	if ((err = ceph_fill_inode(in, prinfo->trace_in[i].in)) < 0)
		return err;

	for (++i; i<prinfo->trace_nr; i++) {
		dput(parent);
		parent = dn;

		dname.name = prinfo->trace_dname[i];
		dname.len = prinfo->trace_dname_len[i];
		dname.hash = full_name_hash(dname.name, dname.len);

		dn = d_lookup(parent, &dname);

		dout(30, "calling d_lookup on parent=%p name=%s returned %p\n", parent, dname.name, dn);

		if (!dn) {
			dn = d_alloc(parent, &dname);
			if (dn == NULL) {
				dout(30, "d_alloc badness\n");
				break; 
			}
		}

		if (!prinfo->trace_in[i].in) {
			err = -ENOENT;
			d_delete(dn);
			dn = NULL;
			break;
		}

		if ((!dn->d_inode) ||
		    (ceph_ino(dn->d_inode) != prinfo->trace_in[i].in->ino)) {
			in = new_inode(parent->d_sb);
			if (in == NULL) {
				dout(30, "new_inode badness\n");
				d_delete(dn);
				dn = NULL;
				break;
			}
			if (ceph_fill_inode(in, prinfo->trace_in[i].in) < 0) {
				dout(30, "ceph_fill_inode badness\n");
				iput(in);
				d_delete(dn);
				dn = NULL;
				break;
			}
			ceph_touch_dentry(dn);
			d_add(dn, in);
			dout(10, "ceph_fill_trace added dentry %p inode %llx %d/%d\n",
			     dn, ceph_ino(in), i, prinfo->trace_nr);
		} else {
			in = dn->d_inode;
			if (ceph_fill_inode(in, prinfo->trace_in[i].in) < 0) {
				dout(30, "ceph_fill_inode badness\n");
				break;
			}

		}
	
	}

	dput(parent);

	if (lastdentry)
		*lastdentry = dn;
	else
		dput(dn);
	
	if (lastinode) {
		*lastinode = in;
		igrab(in);
	}

	return err;
}


/*
 * capabilities
 */

struct ceph_inode_cap *ceph_find_cap(struct inode *inode, int want)
{
	struct ceph_inode_info *ci = ceph_inode(inode);

	int i;
	for (i=0; i<ci->i_nr_caps; i++) 
		if ((ci->i_caps[i].caps & want) == want) {
			dout(40, "find_cap found i=%d cap %d want %d\n", i, ci->i_caps[i].caps, want);
			return &ci->i_caps[i];
		}
	return 0;
}

static struct ceph_inode_cap *get_cap_for_mds(struct inode *inode, int mds)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int i;
	for (i=0; i<ci->i_nr_caps; i++) 
		if (ci->i_caps[i].mds == mds) 
			return &ci->i_caps[i];
	return 0;
}


struct ceph_inode_cap *ceph_add_cap(struct inode *inode, struct ceph_mds_session *session, u32 cap, u32 seq)
{
	int mds = session->s_mds;
	struct ceph_inode_info *ci = ceph_inode(inode);
	int i;

	dout(10, "ceph_add_cap on %p mds%d cap %d seq %d\n", inode, session->s_mds, cap, seq);
	
	for (i=0; i<ci->i_nr_caps; i++) 
		if (ci->i_caps[i].mds == mds) break;
	if (i == ci->i_nr_caps) {
		for (i=0; i<ci->i_nr_caps; i++) 
			if (ci->i_caps[i].mds < 0) break;
		if (i == ci->i_max_caps) {
			/* realloc */
			void *o = ci->i_caps;
			ci->i_caps = kmalloc(ci->i_max_caps*2*sizeof(*ci->i_caps), GFP_KERNEL);
			if (ci->i_caps == NULL) {
				ci->i_caps = o;
				derr(0, "add_cap enomem\n");
				return ERR_PTR(-ENOMEM);
			}
			memcpy(ci->i_caps, o, ci->i_nr_caps*sizeof(*ci->i_caps));
			if (o != ci->i_caps_static)
				kfree(o);
			ci->i_max_caps *= 2;
		}
		if (i == ci->i_nr_caps)
			ci->i_nr_caps++;

		ci->i_caps[i].ci = ci;
		ci->i_caps[i].caps = 0;
		ci->i_caps[i].mds = mds;
		ci->i_caps[i].seq = 0;
		ci->i_caps[i].flags = 0;

		ci->i_caps[i].session = session;
		spin_lock(&session->s_cap_lock);
		list_add(&ci->i_caps[i].session_caps, &session->s_caps);
		session->s_nr_caps++;
		spin_unlock(&session->s_cap_lock);

		if (ci->i_nr_caps == 1) {
			dout(10, "igrab on %p\n", inode);
			igrab(inode);
		}
	}

	dout(10, "add_cap inode %p (%llx) got cap %d %xh now %xh seq %d from %d\n",
	     inode, ceph_ino(inode), i, cap, cap|ci->i_caps[i].caps, seq, mds);
	ci->i_caps[i].caps |= cap;
	ci->i_caps[i].seq = seq;
	return &ci->i_caps[i];
}

int ceph_get_caps(struct ceph_inode_info *ci)
{
	int i;
	int have = 0;
	for (i=0; i<ci->i_nr_caps; i++)
		have |= ci->i_caps[i].caps;
	return have;
}

void __remove_cap(struct ceph_inode_cap *cap)
{
	/* remove from session list */
	struct ceph_mds_session *session = cap->session;
	spin_lock(&session->s_cap_lock);
	list_del(&cap->session_caps);
	session->s_nr_caps--;
	cap->session = 0;
	spin_unlock(&session->s_cap_lock);
}

void ceph_remove_cap(struct ceph_inode_info *ci, int mds)
{
	int i;
	int was = ci->i_nr_caps;
	dout(10, "remove_cap on %p for mds%d\n", &ci->vfs_inode, mds);
	for (i=0; i<ci->i_nr_caps; i++) {
		if (ci->i_caps[i].mds != mds) 
			continue;
		dout(10, "remove_cap removing %p\n", &ci->i_caps[i]);
		__remove_cap(&ci->i_caps[i]);  /* remove from list */
		if (i == ci->i_nr_caps-1) {
			do {
				ci->i_nr_caps--;
			} while (ci->i_nr_caps &&
				 ci->i_caps[ci->i_nr_caps-1].mds < 0);
			break;
		}
		ci->i_caps[i].mds = -1;
		ci->i_caps[i].caps = 0;
		ci->i_caps[i].seq = 0;
	}
	if (was > 0 && ci->i_nr_caps == 0) {
		dout(10, "iput on %p\n", &ci->vfs_inode);
		iput(&ci->vfs_inode);
	}
}

void ceph_remove_caps(struct ceph_inode_info *ci)
{
	int i;
	dout(10, "remove_caps on %p nr %d\n", &ci->vfs_inode, ci->i_nr_caps);
	if (ci->i_nr_caps) {
		for (i=0; i<ci->i_nr_caps; i++) 
			__remove_cap(&ci->i_caps[i]);
		ci->i_nr_caps = 0;
		if (ci->i_caps != ci->i_caps_static) {
			kfree(ci->i_caps);
			ci->i_caps = ci->i_caps_static;
			ci->i_max_caps = STATIC_CAPS;
		}
		dout(10, "iput on %p\n", &ci->vfs_inode);
		iput(&ci->vfs_inode);
	}
}

/*
 * 0 - ok
 * 1 - send the msg back to mds
 */
int ceph_handle_cap_grant(struct inode *inode, struct ceph_mds_file_caps *grant, struct ceph_mds_session *session)
{
	struct ceph_inode_cap *cap;
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	int seq = le32_to_cpu(grant->seq);
	int newcaps;
	int used;
	int wanted = ceph_caps_wanted(ci);
	int ret = 0;
	u64 size = le64_to_cpu(grant->size);
	u64 max_size = le64_to_cpu(grant->max_size);

	dout(10, "handle_cap_grant inode %p ci %p mds%d seq %d\n", inode, ci, mds, seq);
	dout(10, " my wanted = %d\n", wanted);
	dout(10, " size %llu max_size %llu\n", size, max_size);

	/* size change? */
	spin_lock(&inode->i_lock);
	if (size > inode->i_size) {
		dout(10, "size %lld -> %llu\n", inode->i_size, size);
		inode->i_size = size;
	}
	spin_unlock(&inode->i_lock);

	/* max size increase? */
	if (max_size != ci->i_max_size) {
		dout(10, "max_size %lld -> %llu\n", ci->i_max_size, max_size);
		ci->i_max_size = max_size;
	}

	cap = get_cap_for_mds(inode, mds);

	/* new cap? */
	if (!cap) {
		/* unwanted? */
		if (wanted == 0) {
			dout(10, "wanted=0, reminding mds\n");
			grant->wanted = cpu_to_le32(0);
			return 1; /* ack */
		}
		/* hrm */
		BUG_ON(1);
		dout(10, "adding new cap inode %p for mds%d\n", inode, mds);
		cap = ceph_add_cap(inode, session, 
				   le32_to_cpu(grant->caps), 
				   le32_to_cpu(grant->seq));
		return ret;
	} 

	cap->seq = seq;

	if (wanted != le32_to_cpu(grant->wanted)) {
		dout(10, "wanted %d -> %d\n", le32_to_cpu(grant->wanted), wanted);
		grant->wanted = cpu_to_le32(wanted);
	}

	/* revocation? */
	newcaps = le32_to_cpu(grant->caps);
	if (cap->caps & ~newcaps) {
		used = ceph_caps_used(ci);
		dout(10, "revocation: %d -> %d, used %d\n", cap->caps, newcaps, used);
		if (newcaps & used) {
			/* FIXME FIXME FIXME DO STUFF HERE */
			/* but blindly ack for now... */
		}
		cap->caps = newcaps;
		return 1; /* ack */
	}
	
	/* grant or no-op */
	if (cap->caps == newcaps) {
		dout(10, "caps unchanged: %d -> %d\n", cap->caps, newcaps);
	} else {
		dout(10, "grant: %d -> %d\n", cap->caps, newcaps);
		cap->caps = newcaps;
	}
	return ret;	
}

int ceph_handle_cap_trunc(struct inode *inode, struct ceph_mds_file_caps *trunc, struct ceph_mds_session *session)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	int seq = le32_to_cpu(trunc->seq);
	u64 size = le64_to_cpu(trunc->size);
	dout(10, "handle_cap_trunc inode %p ci %p mds%d seq %d\n", inode, ci, mds, seq);

	spin_lock(&inode->i_lock);
	dout(10, "size %lld -> %llu\n", inode->i_size, size);
	inode->i_size = size;
	spin_unlock(&inode->i_lock);

	/*
	 * FIXME: how to truncate the page cache here?
	 */
	return 0;
}

/*
 * symlinks
 */
static void * ceph_sym_follow_link(struct dentry *dentry, struct nameidata *nd)
{
	struct ceph_inode_info *ci = ceph_inode(dentry->d_inode);
	nd_set_link(nd, ci->i_symlink);
	return NULL;
}

const struct inode_operations ceph_symlink_iops = {
	.readlink = generic_readlink,
	.follow_link = ceph_sym_follow_link,
};


/*
 * generics
 */
struct ceph_mds_request *prepare_setattr(struct ceph_mds_client *mdsc, struct dentry *dentry, int op)
{
	char *path;
	int pathlen;
	struct ceph_mds_request *req;

	dout(5, "prepare_setattr dentry %p\n", dentry);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return ERR_PTR(PTR_ERR(path));
	req = ceph_mdsc_create_request(mdsc, op, ceph_ino(dentry->d_inode->i_sb->s_root->d_inode), path, 0, 0);
	kfree(path);
	return req;
}

int ceph_setattr(struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
        const unsigned int ia_valid = attr->ia_valid;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *reqh;
	int err;

	/* gratuitous debug output */
        if (ia_valid & ATTR_UID)
		dout(10, "setattr: uid %d -> %d\n", inode->i_uid, attr->ia_uid);
        if (ia_valid & ATTR_GID)
		dout(10, "setattr: gid %d -> %d\n", inode->i_uid, attr->ia_uid);
        if (ia_valid & ATTR_MODE)
		dout(10, "setattr: mode %d -> %d\n", inode->i_mode, attr->ia_mode);
        if (ia_valid & ATTR_SIZE)
		dout(10, "setattr: size %lld -> %lld\n", inode->i_size, attr->ia_size);
        if (ia_valid & ATTR_ATIME)
		dout(10, "setattr: atime %ld.%ld -> %ld.%ld\n", 
		     inode->i_atime.tv_sec, inode->i_atime.tv_nsec, 
		     attr->ia_atime.tv_sec, attr->ia_atime.tv_nsec);
        if (ia_valid & ATTR_MTIME)
		dout(10, "setattr: mtime %ld.%ld -> %ld.%ld\n", 
		     inode->i_mtime.tv_sec, inode->i_mtime.tv_nsec, 
		     attr->ia_mtime.tv_sec, attr->ia_mtime.tv_nsec);
        if (ia_valid & ATTR_FILE)
		dout(10, "setattr: ATTR_FILE ... hrm!\n");

	/* chown */
        if (ia_valid & (ATTR_UID|ATTR_GID)) {
		req = prepare_setattr(mdsc, dentry, CEPH_MDS_OP_CHOWN);
		if (IS_ERR(req)) 
			return PTR_ERR(req);
		reqh = req->r_request->front.iov_base;
		if (ia_valid & ATTR_UID)
			reqh->args.chown.uid = cpu_to_le32(attr->ia_uid);
		else
			reqh->args.chown.uid = cpu_to_le32(-1);
		if (ia_valid & ATTR_GID)
			reqh->args.chown.gid = cpu_to_le32(attr->ia_gid);
		else
			reqh->args.chown.gid = cpu_to_le32(-1);
		err = ceph_mdsc_do_request(mdsc, req);
		ceph_mdsc_put_request(req);
		dout(10, "chown result %d\n", err);
		if (err)
			return err;
	}
	
	/* chmod? */
	if (ia_valid & ATTR_MODE) {
		req = prepare_setattr(mdsc, dentry, CEPH_MDS_OP_CHMOD);
		if (IS_ERR(req)) 
			return PTR_ERR(req);
		reqh = req->r_request->front.iov_base;
		reqh->args.chmod.mode = cpu_to_le32(attr->ia_mode);
		err = ceph_mdsc_do_request(mdsc, req);
		ceph_mdsc_put_request(req);
		dout(10, "chmod result %d\n", err);
		if (err)
			return err;
	}

	/* utimes */
	/* FIXME: second resolution here is a hack to avoid setattr on open... :/ */
	if (((ia_valid & ATTR_ATIME) && inode->i_atime.tv_sec != attr->ia_atime.tv_sec) ||
	    ((ia_valid & ATTR_MTIME) && inode->i_mtime.tv_sec != attr->ia_mtime.tv_sec)) {
		req = prepare_setattr(mdsc, dentry, CEPH_MDS_OP_UTIME);
		if (IS_ERR(req)) 
			return PTR_ERR(req);
		reqh = req->r_request->front.iov_base;
		ceph_encode_timespec(&reqh->args.utime.mtime, &attr->ia_mtime);
		ceph_encode_timespec(&reqh->args.utime.atime, &attr->ia_atime);
		err = ceph_mdsc_do_request(mdsc, req);
		ceph_mdsc_put_request(req);
		dout(10, "utime result %d\n", err);
		if (err)
			return err;
	}

	/* truncate? */
	if (ia_valid & ATTR_SIZE &&
	    attr->ia_size < inode->i_size) {  /* fixme? */
		dout(10, "truncate: ia_size %d i_size %d ci->i_wr_size %d\n",
		     (int)attr->ia_size, (int)inode->i_size, (int)ci->i_wr_size);
		if (ia_valid & ATTR_FILE) 
			req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_TRUNCATE, 
						       ceph_ino(dentry->d_inode), "", 0, 0);
		else
			req = prepare_setattr(mdsc, dentry, CEPH_MDS_OP_TRUNCATE);
		if (IS_ERR(req)) 
			return PTR_ERR(req);
		reqh = req->r_request->front.iov_base;
		reqh->args.truncate.length = cpu_to_le64(attr->ia_size);
		err = ceph_mdsc_do_request(mdsc, req);
		ceph_mdsc_put_request(req);
		dout(10, "truncate result %d\n", err);
		if (err)
			return err;
	}

	return 0;
}

int ceph_inode_revalidate(struct dentry *dentry)
{
	struct ceph_inode_info *ci;

	if (dentry->d_inode == NULL)
		return -ENOENT;

	ci = ceph_inode(dentry->d_inode);
	if (!ci)
		return -ENOENT;

	if (ceph_lookup_cache && time_before(jiffies, ci->time+CACHE_HZ))
		return 0;

	return ceph_request_lookup(dentry->d_inode->i_sb, dentry);
}

int ceph_inode_getattr(struct vfsmount *mnt, struct dentry *dentry, struct kstat *stat)
{
	int err;
	dout(30, "ceph_inode_getattr\n");

	err = ceph_inode_revalidate(dentry);

	dout(30, "ceph_inode_getattr returned %d\n", err);
	if (!err) 
		generic_fillattr(dentry->d_inode, stat);
	return err;
}

