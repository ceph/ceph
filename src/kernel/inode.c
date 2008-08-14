#include <linux/module.h>
#include <linux/fs.h>
#include <linux/smp_lock.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/uaccess.h>
#include <linux/kernel.h>
#include <linux/namei.h>
#include <linux/writeback.h>

#include "ceph_fs.h"

int ceph_debug_inode = -1;
#define DOUT_VAR ceph_debug_inode
#define DOUT_PREFIX "inode: "
#include "super.h"
#include "decode.h"

const struct inode_operations ceph_symlink_iops;

/*
 * find or create an inode, given the ceph ino number
 */
struct inode *ceph_get_inode(struct super_block *sb, struct ceph_vino vino)
{
	struct inode *inode;
	struct ceph_inode_info *ci;
	ino_t t = ceph_vino_to_ino(vino);

	inode = iget5_locked(sb, t, ceph_ino_compare, ceph_set_ino_cb, &vino);
	if (inode == NULL)
		return ERR_PTR(-ENOMEM);
	if (inode->i_state & I_NEW) {
		dout(40, "get_inode created new inode %p %llx.%llx ino %llx\n",
		     inode, ceph_vinop(inode), (u64)inode->i_ino);
		unlock_new_inode(inode);
	}

	ci = ceph_inode(inode);

	dout(30, "get_inode on %lu=%llx.%llx got %p\n", inode->i_ino, vino.ino,
	     vino.snap, inode);
	return inode;
}

struct inode *ceph_get_snapdir(struct inode *parent)
{
	struct ceph_vino vino = {
		.ino = ceph_ino(parent),
		.snap = CEPH_SNAPDIR,
	};
	struct inode *inode = ceph_get_inode(parent->i_sb, vino);
	if (IS_ERR(inode))
		return ERR_PTR(PTR_ERR(inode));
	inode->i_mode = parent->i_mode;
	inode->i_uid = parent->i_uid;
	inode->i_gid = parent->i_gid;
	inode->i_op = &ceph_dir_iops;
	inode->i_fop = &ceph_dir_fops;
	ceph_inode(inode)->i_snap_caps = CEPH_CAP_PIN; /* so we can open */
	return inode;
}


const struct inode_operations ceph_file_iops = {
	.setattr = ceph_setattr,
	.getattr = ceph_getattr,
	.setxattr = ceph_setxattr,
	.getxattr = ceph_getxattr,
	.listxattr = ceph_listxattr,
	.removexattr = ceph_removexattr,
};

const struct inode_operations ceph_special_iops = {
	.setattr = ceph_setattr,
	.getattr = ceph_getattr,
	.setxattr = ceph_setxattr,
	.getxattr = ceph_getxattr,
	.listxattr = ceph_listxattr,
	.removexattr = ceph_removexattr
};


/*
 * find (or create) a frag in the tree
 */
struct ceph_inode_frag *__ceph_get_frag(struct ceph_inode_info *ci, u32 f)
{
	struct rb_node **p;
	struct rb_node *parent = NULL;
	struct ceph_inode_frag *frag, *newfrag = 0;
	int c;

retry:
	p = &ci->i_fragtree.rb_node;
	while (*p) {
		parent = *p;
		frag = rb_entry(parent, struct ceph_inode_frag, node);
		c = frag_compare(f, frag->frag);
		if (c < 0)
			p = &(*p)->rb_left;
		else if (c > 0)
			p = &(*p)->rb_right;
		else
			goto out;
	}

	if (!newfrag) {
		spin_unlock(&ci->vfs_inode.i_lock);
		newfrag = kmalloc(sizeof(*frag), GFP_NOFS);
		spin_lock(&ci->vfs_inode.i_lock);
		if (!newfrag)
			return ERR_PTR(-ENOMEM);
		goto retry;
	}

	frag = newfrag;
	newfrag = 0;
	frag->frag = f;
	frag->split_by = 0;
	frag->mds = -1;
	frag->ndist = 0;

	rb_link_node(&frag->node, parent, p);
	rb_insert_color(&frag->node, &ci->i_fragtree);

	dout(20, "get_frag added %llx.%llx frag %x\n",
	     ceph_vinop(&ci->vfs_inode), f);

out:
	kfree(newfrag);
	return frag;
}

__u32 ceph_choose_frag(struct ceph_inode_info *ci, u32 v,
		       struct ceph_inode_frag **pfrag)
{
	u32 t = frag_make(0, 0);
	struct ceph_inode_frag *frag;
	unsigned nway, i;
	u32 n;

	spin_lock(&ci->vfs_inode.i_lock);
	while (1) {
		WARN_ON(!frag_contains_value(t, v));
		frag = __ceph_find_frag(ci, t);
		if (!frag)
			break; /* t is a leaf */
		if (frag->split_by == 0) {
			if (pfrag)
				*pfrag = frag;
			break;
		}

		/* choose child */
		nway = 1 << frag->split_by;
		dout(30, "choose_frag(%x) %x splits by %d (%d ways)\n", v, t,
		     frag->split_by, nway);
		for (i = 0; i < nway; i++) {
			n = frag_make(frag_bits(t) + frag->split_by,
				      frag_value(t) | (i << frag_bits(t)));
			if (frag_contains_value(n, v)) {
				t = n;
				break;
			}
		}
		BUG_ON(i == nway);
	}
	dout(30, "choose_frag(%x) = %x\n", v, t);

	spin_unlock(&ci->vfs_inode.i_lock);
	return t;
}

/*
 * process dirfrag (delegation) info.  include leaf fragment in tree
 * ONLY if mds >= 0 || ndist > 0.  (otherwise, only branches/splits
 * are included in i_fragtree)
 */
static int ceph_fill_dirfrag(struct inode *inode,
			     struct ceph_mds_reply_dirfrag *dirinfo)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_frag *frag;
	u32 id = le32_to_cpu(dirinfo->frag);
	int mds = le32_to_cpu(dirinfo->auth);
	int ndist = le32_to_cpu(dirinfo->ndist);
	int i;
	int err = 0;

	spin_lock(&inode->i_lock);
	if (mds < 0 && ndist == 0) {
		/* no delegation info needed. */
		frag = __ceph_find_frag(ci, id);
		if (!frag)
			goto out;
		if (frag->split_by == 0) {
			/* tree leaf, remove */
			dout(20, "fill_dirfrag removed %llx.%llx frag %x"
			     " (no ref)\n", ceph_vinop(inode), id);
			rb_erase(&frag->node, &ci->i_fragtree);
			kfree(frag);
		} else {
			/* tree branch, keep */
			dout(20, "fill_dirfrag cleared %llx.%llx frag %x"
			     " referral\n", ceph_vinop(inode), id);
			frag->mds = -1;
			frag->ndist = 0;
		}
		goto out;
	}


	/* find/add this frag to store mds delegation info */
	frag = __ceph_get_frag(ci, id);
	if (IS_ERR(frag)) {
		derr(0, "fill_dirfrag ENOMEM on mds ref %llx.%llx frag %x\n",
		     ceph_vinop(inode), le32_to_cpu(dirinfo->frag));
		err = -ENOMEM;
		goto out;
	}

	frag->mds = mds;
	frag->ndist = min_t(u32, ndist, MAX_DIRFRAG_REP);
	for (i = 0; i < frag->ndist; i++)
		frag->dist[i] = le32_to_cpu(dirinfo->dist[i]);
	dout(20, "fill_dirfrag %llx.%llx frag %x referral mds %d ndist=%d\n",
	     ceph_vinop(inode), frag->frag, frag->mds, frag->ndist);

out:
	spin_unlock(&inode->i_lock);
	return err;
}

/*
 * helper to fill in size, ctime, mtime, and atime.  we have to be
 * careful because the client or MDS may have more up to date info,
 * depending on which capabilities/were help, and on the time_warp_seq
 * (which we increment on utimes()).
 */
static void fill_file_bits(struct inode *inode, int issued, u64 time_warp_seq,
			   u64 size, struct timespec *ctime,
			   struct timespec *mtime, struct timespec *atime)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	u64 blocks = (size + (1<<9) - 1) >> 9;
	int warn = 0;

	if (issued & CEPH_CAP_EXCL) {
		if (timespec_compare(ctime, &inode->i_ctime) > 0)
			inode->i_ctime = *ctime;
		if (time_warp_seq > ci->i_time_warp_seq)
			derr(0, "WARNING: %p mds time_warp_seq %llu > %llu\n",
			     inode, time_warp_seq, ci->i_time_warp_seq);
	} else if (issued & (CEPH_CAP_WR|CEPH_CAP_WRBUFFER)) {
		if (size > inode->i_size) {
			dout(10, "size %lld -> %llu\n", inode->i_size, size);
			inode->i_size = size;
			inode->i_blocks = blocks;
			ci->i_reported_size = size;
		}
		if (time_warp_seq > ci->i_time_warp_seq) {
			inode->i_ctime = *ctime;
			inode->i_mtime = *mtime;
			inode->i_atime = *atime;
			ci->i_time_warp_seq = time_warp_seq;
		} else if (time_warp_seq == ci->i_time_warp_seq) {
			if (timespec_compare(ctime, &inode->i_ctime) > 0)
				inode->i_ctime = *ctime;
			if (timespec_compare(mtime, &inode->i_mtime) > 0)
				inode->i_mtime = *mtime;
			if (timespec_compare(atime, &inode->i_atime) > 0)
				inode->i_atime = *atime;
		} else
			warn = 1;
	} else {
		inode->i_size = size;
		inode->i_blocks = blocks;
		ci->i_reported_size = size;
		if (time_warp_seq >= ci->i_time_warp_seq) {
			inode->i_ctime = *ctime;
			inode->i_mtime = *mtime;
			inode->i_atime = *atime;
			ci->i_time_warp_seq = time_warp_seq;
		} else
			warn = 1;
	}
	if (warn)
		dout(10, "%p mds time_warp_seq %llu < %llu\n",
		     inode, time_warp_seq, ci->i_time_warp_seq);
}

/*
 * populate an inode based on info from mds.
 * may be called on new or existing inodes.
 */
int ceph_fill_inode(struct inode *inode,
		    struct ceph_mds_reply_info_in *iinfo,
		    struct ceph_mds_reply_dirfrag *dirinfo)
{
	struct ceph_mds_reply_inode *info = iinfo->in;
	struct ceph_inode_info *ci = ceph_inode(inode);
	int i;
	int symlen;
	int issued;
	struct timespec mtime, atime, ctime;
	u64 size = le64_to_cpu(info->size);
	u32 su = le32_to_cpu(info->layout.fl_stripe_unit);
	int blkbits = fls(su) - 1;
	u32 nsplits;

	dout(30, "fill_inode %p ino %llx by %d.%d sz=%llu mode 0%o nlink %d\n",
	     inode, info->ino, inode->i_uid, inode->i_gid,
	     inode->i_size, inode->i_mode, inode->i_nlink);

	spin_lock(&inode->i_lock);
	dout(30, " su %d, blkbits %d.  v %llu, had %llu\n",
	     su, blkbits, le64_to_cpu(info->version), ci->i_version);
	if (le64_to_cpu(info->version) > 0 &&
	    ci->i_version == le64_to_cpu(info->version))
		goto no_change;

	/* update inode */
	ci->i_version = le64_to_cpu(info->version);
	inode->i_version++;
	inode->i_mode = le32_to_cpu(info->mode);
	inode->i_uid = le32_to_cpu(info->uid);
	inode->i_gid = le32_to_cpu(info->gid);
	inode->i_nlink = le32_to_cpu(info->nlink);
	inode->i_rdev = le32_to_cpu(info->rdev);

	/* be careful with mtime, atime, size */
	ceph_decode_timespec(&atime, &info->atime);
	ceph_decode_timespec(&mtime, &info->mtime);
	ceph_decode_timespec(&ctime, &info->ctime);
	issued = __ceph_caps_issued(ci, 0);

	fill_file_bits(inode, issued, le64_to_cpu(info->time_warp_seq), size,
		       &ctime, &mtime, &atime);

	inode->i_blkbits = blkbits;

	ci->i_max_size = le64_to_cpu(info->max_size);

	ci->i_layout = info->layout;
	kfree(ci->i_symlink);
	ci->i_symlink = 0;

	/* xattrs */
	if (iinfo->xattr_len) {
		if (ci->i_xattr_len != iinfo->xattr_len) {
			kfree(ci->i_xattr_data);
			ci->i_xattr_len = iinfo->xattr_len;
			ci->i_xattr_data = kmalloc(ci->i_xattr_len, GFP_NOFS);
			if (!ci->i_xattr_data) {
				derr(10, "ENOMEM on xattr blob %d bytes\n",
				     ci->i_xattr_len);
				ci->i_xattr_len = 0;  /* hrmpf */
			}
		}
		if (ci->i_xattr_len)
			memcpy(ci->i_xattr_data, iinfo->xattr_data,
			       ci->i_xattr_len);
	}

	ci->i_old_atime = inode->i_atime;

	inode->i_mapping->a_ops = &ceph_aops;

no_change:
	/* populate frag tree */
	/* FIXME: move me up, if/when version reflects fragtree changes */
	nsplits = le32_to_cpu(info->fragtree.nsplits);
	for (i = 0; i < nsplits; i++) {
		u32 id = le32_to_cpu(info->fragtree.splits[i].frag);
		struct ceph_inode_frag *frag = __ceph_get_frag(ci, id);
		if (IS_ERR(frag)) {
			derr(0,"ENOMEM on ino %llx.%llx frag %x\n",
			     ceph_vinop(inode), id);
			continue;
		}
		frag->split_by =
			le32_to_cpu(info->fragtree.splits[i].by);
		dout(20, " frag %x split by %d\n", frag->frag,
		     frag->split_by);
	}

	spin_unlock(&inode->i_lock);

	/* set dirinfo? */
	if (dirinfo)
		ceph_fill_dirfrag(inode, dirinfo);

	switch (inode->i_mode & S_IFMT) {
	case S_IFIFO:
	case S_IFBLK:
	case S_IFCHR:
	case S_IFSOCK:
		init_special_inode(inode, inode->i_mode, inode->i_rdev);
		inode->i_op = &ceph_special_iops;
		break;
	case S_IFREG:
		inode->i_op = &ceph_file_iops;
		inode->i_fop = &ceph_file_fops;
		break;
	case S_IFLNK:
		inode->i_op = &ceph_symlink_iops;
		symlen = iinfo->symlink_len;
		BUG_ON(symlen != ci->vfs_inode.i_size);
		ci->i_symlink = kmalloc(symlen+1, GFP_NOFS);
		if (ci->i_symlink == NULL)
			return -ENOMEM;
		memcpy(ci->i_symlink, iinfo->symlink, symlen);
		ci->i_symlink[symlen] = 0;
		break;
	case S_IFDIR:
		inode->i_op = &ceph_dir_iops;
		inode->i_fop = &ceph_dir_fops;

		ci->i_files = le64_to_cpu(info->files);
		ci->i_subdirs = le64_to_cpu(info->subdirs);
		ci->i_rbytes = le64_to_cpu(info->rbytes);
		ci->i_rfiles = le64_to_cpu(info->rfiles);
		ci->i_rsubdirs = le64_to_cpu(info->rsubdirs);
		ceph_decode_timespec(&ci->i_rctime, &info->rctime);

		if (ceph_client(inode->i_sb)->mount_args.flags &
		    CEPH_MOUNT_RBYTES)
			inode->i_size = ci->i_rbytes;
		break;
	default:
		derr(0, "BAD mode 0x%x S_IFMT 0x%x\n",
		     inode->i_mode, inode->i_mode & S_IFMT);
		return -EINVAL;
	}

	return 0;
}

/*
 * caller must hold session s_mutex.
 */
static int update_inode_lease(struct inode *inode,
			      struct ceph_mds_reply_lease *lease,
			      struct ceph_mds_session *session,
			      unsigned long from_time)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int is_new = 0;
	int mask = le16_to_cpu(lease->mask);
	long unsigned duration = le32_to_cpu(lease->duration_ms);
	long unsigned ttl = from_time + (duration * HZ) / 1000;

	dout(10, "update_inode_lease %p mask %d duration %lu ms ttl %lu\n",
	     inode, mask, duration, ttl);

	if (mask == 0)
		return 0;

	spin_lock(&inode->i_lock);
	/*
	 * be careful: we can't remove lease from a different session
	 * without holding that other session's s_mutex.  so don't.
	 */
	if ((ci->i_lease_ttl == 0 || !time_before(ttl, ci->i_lease_ttl) ||
	     ci->i_lease_gen != session->s_cap_gen) &&
	    (!ci->i_lease_session || ci->i_lease_session == session)) {
		ci->i_lease_ttl = ttl;
		ci->i_lease_gen = session->s_cap_gen;
		ci->i_lease_mask = mask;
		if (!ci->i_lease_session) {
			ci->i_lease_session = session;
			is_new = 1;
		}
		list_move_tail(&ci->i_lease_item, &session->s_inode_leases);
	} else
		mask = 0;
	spin_unlock(&inode->i_lock);
	if (is_new) {
		dout(10, "lease iget on %p\n", inode);
		igrab(inode);
	}

	return mask;
}

/*
 * check if inode lease is valid for a given mask
 */
int ceph_inode_lease_valid(struct inode *inode, int mask)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int havemask;
	int valid = 0;
	int ret = 0;

	spin_lock(&inode->i_lock);
	havemask = ci->i_lease_mask;

	/* EXCL cap counts for an ICONTENT lease... check caps? */
	if ((mask & CEPH_LOCK_ICONTENT) &&
	    __ceph_caps_issued(ci, 0) & CEPH_CAP_EXCL) {
		dout(20, "lease_valid inode %p EXCL cap -> ICONTENT\n", inode);
		havemask |= CEPH_LOCK_ICONTENT;
	}
	/* any ICONTENT bits imply all ICONTENT bits */
	if (havemask & CEPH_LOCK_ICONTENT)
		havemask |= CEPH_LOCK_ICONTENT;

	if (ci->i_lease_session) {
		struct ceph_mds_session *s = ci->i_lease_session;
		spin_lock(&s->s_cap_lock);
		if (s->s_cap_gen == ci->i_lease_gen &&
		    time_before(jiffies, s->s_cap_ttl) &&
		    time_before(jiffies, ci->i_lease_ttl))
			valid = 1;
		spin_unlock(&s->s_cap_lock);
	}
	spin_unlock(&inode->i_lock);

	if (valid && (havemask & mask) == mask)
		ret = 1;

	dout(10, "lease_valid inode %p have %d want %d valid %d = %d\n", inode,
	     havemask, mask, valid, ret);
	return ret;
}


/*
 * caller should hold session s_mutex.
 */
static void update_dentry_lease(struct dentry *dentry,
				struct ceph_mds_reply_lease *lease,
				struct ceph_mds_session *session,
				unsigned long from_time)
{
	struct ceph_dentry_info *di;
	int is_new = 0;
	long unsigned duration = le32_to_cpu(lease->duration_ms);
	long unsigned ttl = from_time + (duration * HZ) / 1000;

	dout(10, "update_dentry_lease %p mask %d duration %lu ms ttl %lu\n",
	     dentry, le16_to_cpu(lease->mask), duration, ttl);
	if (lease->mask == 0) {
		/*
		 * no per-dentry lease.  so, set d_time to match
		 * parent directory version.  if/when we get an
		 * ICONTENT cap (implicit directory-wide lease), we'll
		 * know whether it covers this dentry.
		 */
		struct inode *dir = dentry->d_parent->d_inode;
		dentry->d_time = ceph_inode(dir)->i_version;
		dout(20, " no lease, setting d_time to %lu\n", dentry->d_time);
		return;
	}

	spin_lock(&dentry->d_lock);
	di = ceph_dentry(dentry);
	if (dentry->d_time != 0 &&
	    di && di->lease_gen == session->s_cap_gen &&
	    time_before(ttl, dentry->d_time))
		goto fail_unlock;  /* we already have a newer lease. */

	if (!di) {
		spin_unlock(&dentry->d_lock);
		di = kmalloc(sizeof(struct ceph_dentry_info),
			     GFP_NOFS);
		if (!di)
			return;          /* oh well */
		spin_lock(&dentry->d_lock);
		if (dentry->d_fsdata) {
			kfree(di);   /* lost a race! */
			goto fail_unlock;
		}
		di->dentry = dentry;
		dentry->d_fsdata = di;
		di->lease_session = session;
		di->lease_gen = session->s_cap_gen;
		list_add(&di->lease_item, &session->s_dentry_leases);
		is_new = 1;
	} else {
		/* touch existing */
		if (di->lease_session != session)
			goto fail_unlock;
		list_move_tail(&di->lease_item, &session->s_dentry_leases);
	}
	dentry->d_time = ttl;
	spin_unlock(&dentry->d_lock);
	if (is_new) {
		dout(10, "lease dget on %p\n", dentry);
		dget(dentry);
	}
	return;

fail_unlock:
	spin_unlock(&dentry->d_lock);
}

/*
 * check if dentry lease is valid
 */
int ceph_dentry_lease_valid(struct dentry *dentry)
{
	struct ceph_dentry_info *di;
	struct ceph_mds_session *session;
	int valid = 0;
	u32 gen;
	unsigned long ttl;

	spin_lock(&dentry->d_lock);
	di = ceph_dentry(dentry);
	if (di) {
		session = di->lease_session;
		spin_lock(&session->s_cap_lock);
		gen = session->s_cap_gen;
		ttl = session->s_cap_ttl;
		spin_unlock(&session->s_cap_lock);

		if (di->lease_gen == gen &&
		    time_before(jiffies, dentry->d_time) &&
		    time_before(jiffies, ttl))
			valid = 1;
	}
	spin_unlock(&dentry->d_lock);
	dout(20, "dentry_lease_valid - dentry %p = %d\n", dentry, valid);
	return valid;
}


/*
 * splice a dentry to an inode.
 * caller must hold directory i_mutex for this to be safe.
 */
static struct dentry *splice_dentry(struct dentry *dn, struct inode *in,
				    bool *prehash)
{
	struct dentry *realdn;

	/* dn must be unhashed */
	if (!d_unhashed(dn))
		d_drop(dn);
	realdn = d_materialise_unique(dn, in);
	if (IS_ERR(realdn)) {
		derr(0, "error splicing %p (%d) "
		     "inode %p ino %llx.%llx\n",
		     dn, atomic_read(&dn->d_count), in,
		     ceph_vinop(in));
		if (prehash)
			*prehash = false; /* don't rehash on error */
		goto out;
	} else if (realdn) {
		dout(10, "dn %p (%d) spliced with %p (%d) "
		     "inode %p ino %llx.%llx\n",
		     dn, atomic_read(&dn->d_count),
		     realdn, atomic_read(&realdn->d_count),
		     realdn->d_inode,
		     ceph_vinop(realdn->d_inode));
		dput(dn);
		dn = realdn;
		ceph_init_dentry(dn);
	} else
		dout(10, "dn %p attached to %p ino %llx.%llx\n",
		     dn, dn->d_inode, ceph_vinop(dn->d_inode));
	if ((!prehash || *prehash) && d_unhashed(dn))
		d_rehash(dn);
out:
	return dn;
}

/*
 * assimilate a full trace of inodes and dentries, from the root to
 * the item relevant for this reply, into our cache.  make any dcache
 * changes needed to properly reflect the completed operation (e.g.,
 * call d_move).  make note of the distribution of metadata across the
 * mds cluster.
 *
 * FIXME: we should check inode.version to avoid races between traces
 * from multiple MDSs after, say, a ancestor directory is renamed.
 */
int ceph_fill_trace(struct super_block *sb, struct ceph_mds_request *req,
		    struct ceph_mds_session *session)
{
	struct ceph_mds_reply_info *rinfo = &req->r_reply_info;
	int err = 0, mask;
	struct qstr dname;
	struct dentry *dn = sb->s_root;
	struct dentry *parent = NULL;
	struct dentry *existing;
	struct inode *in;
	struct ceph_mds_reply_inode *ininfo;
	int d = 0;
	struct ceph_vino vino;
	int have_icontent = 0;
	bool have_lease = 0;

	if (rinfo->trace_numi == 0) {
		dout(10, "fill_trace reply has empty trace!\n");
		return 0;
	}

#if 0
	/*
	 * if we resend completed ops to a recovering mds, we get no
	 * trace.  pretend this is the case to ensure the 'no trace'
	 * handlers behave.
	 */
	if (rinfo->head->op & CEPH_MDS_OP_WRITE) {
		dout(0, "fill_trace faking empty trace on %d %s\n",
		     rinfo->head->op,
		     ceph_mds_op_name(rinfo->head->op));
		rinfo->trace_numi = 0;
		rinfo->trace_numd = 0;
		return 0;
	}
#endif

	vino.ino = le64_to_cpu(rinfo->trace_in[0].in->ino);
	vino.snap = le64_to_cpu(rinfo->trace_in[0].in->snapid);
	if (likely(dn)) {
		in = dn->d_inode;
		/* trace should start at root, or have only 1 dentry
		 * (if it is in mds stray dir) */
		WARN_ON(vino.ino != 1 && rinfo->trace_numd != 1);
	} else {
		/* first reply (i.e. mount) */
		in = ceph_get_inode(sb, vino);
		if (IS_ERR(in))
			return PTR_ERR(in);
		dn = d_alloc_root(in);
		if (dn == NULL) {
			derr(0, "d_alloc_root enomem badness on root dentry\n");
			return -ENOMEM;
		}
	}

	if (vino.ino == 1) {
		err = ceph_fill_inode(in, &rinfo->trace_in[0],
				      rinfo->trace_numd ?
				      rinfo->trace_dir[0]:0);
		if (err < 0)
			return err;
		mask = update_inode_lease(in, rinfo->trace_ilease[0],
					 session, req->r_from_time);
		//have_icontent = mask & CEPH_LOCK_ICONTENT;
		if (sb->s_root == NULL)
			sb->s_root = dn;
	}

	dget(dn);
	for (d = 0; d < rinfo->trace_numd; d++) {
		dname.name = rinfo->trace_dname[d];
		dname.len = rinfo->trace_dname_len[d];
		parent = dn;
		dn = 0;

		dout(10, "fill_trace %d/%d parent %p inode %p: '%.*s'"
		     " ic %d dmask %d\n",
		     (d+1), rinfo->trace_numd, parent, parent->d_inode,
		     (int)dname.len, dname.name,
		     have_icontent, rinfo->trace_dlease[d]->mask);

		/* try to take dir i_mutex */
		if (req->r_locked_dir != parent->d_inode &&
		    mutex_trylock(&parent->d_inode->i_mutex) == 0) {
			dout(0, "fill_trace  FAILED to take %p i_mutex\n",
			     parent->d_inode);
			goto no_dir_mutex;
		}

		/* do we have a dn lease? */
		have_lease = have_icontent ||
			(rinfo->trace_dlease[d]->mask & CEPH_LOCK_DN);
		if (!have_lease)
			dout(0, "fill_trace  no icontent|dentry lease\n");

		dout(10, "fill_trace  took %p i_mutex\n", parent->d_inode);

		dname.hash = full_name_hash(dname.name, dname.len);
	retry_lookup:
		/* existing dentry? */
		dn = d_lookup(parent, &dname);
		dout(10, "fill_trace d_lookup of '%.*s' got %p\n",
		     (int)dname.len, dname.name, dn);

		/* use caller provided dentry?  for simplicity,
		 *  - only if there is no existing dn, and
		 *  - only if parent is correct
		 */
		if (d == rinfo->trace_numd-1 && req->r_last_dentry) {
			if (!dn && req->r_last_dentry->d_parent == parent) {
				dn = req->r_last_dentry;
				dout(10, "fill_trace provided dn %p '%.*s'\n",
				     dn, dn->d_name.len, dn->d_name.name);
				ceph_init_dentry(dn);  /* just in case */
			} else {
				dout(10, "fill_trace NOT using provided dn %p "
				     "(parent %p)\n", req->r_last_dentry,
				     req->r_last_dentry->d_parent);
				dput(req->r_last_dentry);
			}
			req->r_last_dentry = NULL;
		}

		if (!dn) {
			dn = d_alloc(parent, &dname);
			if (!dn) {
				derr(0, "d_alloc enomem\n");
				err = -ENOMEM;
				goto out_dir;
			}
			dout(10, "fill_trace d_alloc %p '%.*s'\n", dn,
			     dn->d_name.len, dn->d_name.name);
			ceph_init_dentry(dn);
		}
		BUG_ON(!dn);

		/* null dentry? */
		if (d+1 == rinfo->trace_numi) {
			dout(10, "fill_trace null dentry\n");
			if (dn->d_inode) {
				dout(20, "d_delete %p\n", dn);
				d_delete(dn);
				dput(dn);
				goto retry_lookup;
			}
			dout(20, "d_instantiate %p NULL\n", dn);
			d_instantiate(dn, NULL);
			if (have_lease && d_unhashed(dn))
				d_rehash(dn);
			in = 0;
			update_dentry_lease(dn, rinfo->trace_dlease[d],
					    session, req->r_from_time);
			goto out_dir;
		}

		/* rename? */
		if (d == rinfo->trace_numd-1 && req->r_old_dentry) {
			dout(10, " src %p '%.*s' dst %p '%.*s'\n",
			     req->r_old_dentry,
			     req->r_old_dentry->d_name.len,
			     req->r_old_dentry->d_name.name,
			     dn, dn->d_name.len, dn->d_name.name);
			dout(10, "fill_trace doing d_move %p -> %p\n",
			     req->r_old_dentry, dn);
			d_move(req->r_old_dentry, dn);
			dout(10, " src %p '%.*s' dst %p '%.*s'\n",
			     req->r_old_dentry,
			     req->r_old_dentry->d_name.len,
			     req->r_old_dentry->d_name.name,
			     dn, dn->d_name.len, dn->d_name.name);
			dput(dn);  /* dn is dropped */
			dn = req->r_old_dentry;  /* use old_dentry */
			req->r_old_dentry = 0;
		}

		/* attach proper inode */
		ininfo = rinfo->trace_in[d+1].in;
		vino.ino = le64_to_cpu(ininfo->ino);
		vino.snap = le64_to_cpu(ininfo->snapid);
		if (dn->d_inode) {
			if (ceph_ino(dn->d_inode) != vino.ino ||
			    ceph_snap(dn->d_inode) != vino.snap) {
				dout(10, "dn %p wrong inode %p ino %llx.%llx\n",
				     dn, dn->d_inode, ceph_vinop(dn->d_inode));
				d_delete(dn);
				dput(dn);
				goto retry_lookup;
			}
			dout(10, "dn %p correct %p ino %llx.%llx\n",
			     dn, dn->d_inode, ceph_vinop(dn->d_inode));
			in = dn->d_inode;
		} else {
			in = ceph_get_inode(dn->d_sb, vino);
			if (IS_ERR(in)) {
				derr(30, "get_inode badness\n");
				err = PTR_ERR(in);
				d_delete(dn);
				dn = NULL;
				in = NULL;
				goto out_dir;
			}
			dn = splice_dentry(dn, in, &have_lease);
		}

		if (have_lease)
			update_dentry_lease(dn, rinfo->trace_dlease[d],
					    session, req->r_from_time);

		/* done with dn update */
		if (req->r_locked_dir != parent->d_inode)
			mutex_unlock(&parent->d_inode->i_mutex);

	update_inode:
		err = ceph_fill_inode(in,
				      &rinfo->trace_in[d+1],
				      rinfo->trace_numd <= d ?
				      rinfo->trace_dir[d+1]:0);
		if (err < 0) {
			derr(30, "ceph_fill_inode badness\n");
			iput(in);
			d_delete(dn);
			dn = NULL;
			break;
		}
		mask = update_inode_lease(dn->d_inode,
					 rinfo->trace_ilease[d+1],
					 session, req->r_from_time);
		have_icontent = mask & CEPH_LOCK_ICONTENT;

		dput(parent);
		parent = NULL;

		if (d == rinfo->trace_numi-rinfo->trace_snapdirpos-1) {
			struct inode *snapdir = ceph_get_snapdir(dn->d_inode);
			dput(dn);
			dn = d_find_alias(snapdir);
			iput(snapdir);
			dout(10, " snapdir dentry is %p\n", dn);
		}
		continue;


	out_dir:
		/* drop i_mutex */
		if (req->r_locked_dir != parent->d_inode)
			mutex_unlock(&parent->d_inode->i_mutex);
		break;


	no_dir_mutex:
		/*
		 * we couldn't take i_mutex for this dir, so do not
		 * lookup or relink any existing dentry.
		 */
		if (d == rinfo->trace_numd-1 && req->r_last_dentry) {
			dn = req->r_last_dentry;
			dout(10, "fill_trace using provided dn %p\n", dn);
			ceph_init_dentry(dn);
			req->r_last_dentry = NULL;
		}

		/* null dentry? */
		if (d+1 == rinfo->trace_numi) {
			if (dn && dn->d_inode)
				d_delete(dn);
			break;
		}

		/* find existing inode */
		ininfo = rinfo->trace_in[d+1].in;
		vino.ino = le64_to_cpu(ininfo->ino);
		vino.snap = le64_to_cpu(ininfo->snapid);
		in = ceph_get_inode(parent->d_sb, vino);
		if (IS_ERR(in)) {
			derr(30, "ceph_get_inode badness\n");
			err = PTR_ERR(in);
			in = NULL;
			break;
		}
		existing = d_find_alias(in);
		if (existing) {
			if (dn)
				dput(dn);
			dn = existing;
			dout(10, " using existing %p\n", dn);
		} else {
			if (dn && dn->d_inode == NULL) {
				dout(10, " instantiating provided %p\n", dn);
				d_instantiate(dn, in);
			} else {
				if (dn) {
					dout(10, " ignoring provided dn %p\n",
					     dn);
					dput(dn);
				}
				dn = d_alloc_anon(in);
				dout(10, " d_alloc_anon new dn %p\n", dn);
			}
		}
		goto update_inode;
	}
	if (parent)
		dput(parent);

	dout(10, "fill_trace done err=%d, last dn %p in %p\n", err, dn, in);
	if (req->r_old_dentry)
		dput(req->r_old_dentry);
	if (req->r_last_dentry)
		dput(req->r_last_dentry);
	if (req->r_last_inode)
		iput(req->r_last_inode);
	req->r_last_dentry = dn;
	req->r_last_inode = in;
	if (in)
		igrab(in);
	return err;
}

/*
 * prepopulate cache with readdir results
 */
int ceph_readdir_prepopulate(struct ceph_mds_request *req)
{
	struct dentry *parent = req->r_last_dentry;
	struct ceph_mds_reply_info *rinfo = &req->r_reply_info;
	struct qstr dname;
	struct dentry *dn;
	struct inode *in;
	int i;
	struct inode *snapdir = 0;

	if (le32_to_cpu(rinfo->head->op) == CEPH_MDS_OP_LSSNAP) {
		snapdir = ceph_get_snapdir(parent->d_inode);
		parent = d_find_alias(snapdir);
		dput(parent);
		dout(10, "readdir_prepopulate %d items under SNAPDIR dn %p\n",
		     rinfo->dir_nr, parent);
	} else {
		dout(10, "readdir_prepopulate %d items under dn %p\n",
		     rinfo->dir_nr, parent);
		if (rinfo->dir_dir)
			ceph_fill_dirfrag(parent->d_inode, rinfo->dir_dir);
	}

	for (i = 0; i < rinfo->dir_nr; i++) {
		struct ceph_vino vino;
		/* dentry */
		dname.name = rinfo->dir_dname[i];
		dname.len = le32_to_cpu(rinfo->dir_dname_len[i]);
		dname.hash = full_name_hash(dname.name, dname.len);

		vino.ino = le64_to_cpu(rinfo->dir_in[i].in->ino);
		vino.snap = le64_to_cpu(rinfo->dir_in[i].in->snapid);

retry_lookup:
		dn = d_lookup(parent, &dname);
		dout(30, "calling d_lookup on parent=%p name=%.*s"
		     " returned %p\n", parent, dname.len, dname.name, dn);

		if (!dn) {
			dn = d_alloc(parent, &dname);
			dout(40, "d_alloc %p '%.*s'\n", parent,
			     dname.len, dname.name);
			if (dn == NULL) {
				dout(30, "d_alloc badness\n");
				return -1;
			}
			ceph_init_dentry(dn);
		} else if (dn->d_inode &&
			   (ceph_ino(dn->d_inode) != vino.ino ||
			    ceph_snap(dn->d_inode) != vino.snap)) {
			dout(10, " dn %p points to wrong inode %p\n",
			     dn, dn->d_inode);
			d_delete(dn);
			dput(dn);
			goto retry_lookup;
		}

		/* inode */
		if (dn->d_inode)
			in = dn->d_inode;
		else {
			in = ceph_get_inode(parent->d_sb, vino);
			if (in == NULL) {
				dout(30, "new_inode badness\n");
				d_delete(dn);
				dput(dn);
				return -ENOMEM;
			}
			dn = splice_dentry(dn, in, NULL);
		}

		if (ceph_fill_inode(in, &rinfo->dir_in[i], 0) < 0) {
			dout(0, "ceph_fill_inode badness on %p\n", in);
			dput(dn);
			continue;
		}
		update_dentry_lease(dn, rinfo->dir_dlease[i],
				    req->r_session, req->r_from_time);
		update_inode_lease(in, rinfo->dir_ilease[i],
				   req->r_session, req->r_from_time);
		dput(dn);
	}

	iput(snapdir);
	dout(10, "readdir_prepopulate done\n");
	return 0;
}


/*
 * capabilities
 */

static struct ceph_inode_cap *__get_cap_for_mds(struct inode *inode, int mds)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_cap *cap;
	struct list_head *p;

	list_for_each(p, &ci->i_caps) {
		cap = list_entry(p, struct ceph_inode_cap, ci_caps);
		if (cap->mds == mds)
			return cap;
	}
	return 0;
}

int ceph_get_cap_mds(struct inode *inode)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_cap *cap;
	int mds = -1;

	spin_lock(&inode->i_lock);
	if (!list_empty(&ci->i_caps)) {
		cap = list_first_entry(&ci->i_caps, struct ceph_inode_cap,
				       ci_caps);
		mds = cap->mds;
	}
	spin_unlock(&inode->i_lock);
	return mds;
}

/*
 * caller should hold session snap_mutex, s_mutex.
 *
 * @fmode can be negative, in which case it is ignored.
 */
int ceph_add_cap(struct inode *inode,
		 struct ceph_mds_session *session,
		 int fmode, unsigned issued,
		 unsigned seq, unsigned mseq,
		 void *snapblob, int snapblob_len)
{
	int mds = session->s_mds;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_cap *cap, *new_cap = 0;
	int i;
	int is_new = 0;
	struct ceph_snaprealm *realm = 0;
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;

	if (snapblob_len)
		realm = ceph_update_snap_trace(mdsc,
					       snapblob, snapblob+snapblob_len,
					       0);

	dout(10, "ceph_add_cap on %p mds%d cap %d seq %d\n", inode,
	     session->s_mds, issued, seq);
	spin_lock(&inode->i_lock);
	cap = __get_cap_for_mds(inode, mds);
	if (!cap) {
		for (i = 0; i < STATIC_CAPS; i++)
			if (ci->i_static_caps[i].mds == -1) {
				cap = &ci->i_static_caps[i];
				break;
			}
		if (!cap) {
			if (new_cap)
				cap = new_cap;
			else {
				spin_unlock(&inode->i_lock);
				new_cap = kmalloc(sizeof(*cap), GFP_NOFS);
				if (new_cap == 0)
					return -ENOMEM;
				spin_lock(&inode->i_lock);
			}
		}

		is_new = 1;    /* grab inode later */
		cap->issued = cap->implemented = 0;
		cap->mds = mds;
		cap->flags = 0;
		cap->flushed_snap = 0;

		cap->ci = ci;
		list_add(&cap->ci_caps, &ci->i_caps);

		/* add to session cap list */
		cap->session = session;
		list_add(&cap->session_caps, &session->s_caps);
		session->s_nr_caps++;

		/* clear out old exporting info? */
		if (ci->i_cap_exporting_mds == mds) {
			ci->i_cap_exporting_issued = 0;
			ci->i_cap_exporting_mseq = 0;
			ci->i_cap_exporting_mds = -1;
		}
	}
	if (!ci->i_snaprealm) {
		ci->i_snaprealm = realm;
		list_add(&ci->i_snaprealm_item, &realm->inodes_with_caps);
	} else
		ceph_put_snaprealm(realm);

	dout(10, "add_cap inode %p (%llx.%llx) cap %xh now %xh seq %d mds%d\n",
	     inode, ceph_vinop(inode), issued, issued|cap->issued, seq, mds);
	cap->issued |= issued;
	cap->implemented |= issued;
	cap->seq = seq;
	cap->mseq = mseq;
	cap->gen = session->s_cap_gen;
	if (fmode >= 0)
		__ceph_get_fmode(ci, fmode);
	spin_unlock(&inode->i_lock);
	if (is_new)
		igrab(inode);
	return 0;
}

int __ceph_caps_issued(struct ceph_inode_info *ci, int *implemented)
{
	int have = ci->i_snap_caps;
	struct ceph_inode_cap *cap;
	struct list_head *p;
	u32 gen;
	unsigned long ttl;

	list_for_each(p, &ci->i_caps) {
		cap = list_entry(p, struct ceph_inode_cap, ci_caps);

		spin_lock(&cap->session->s_cap_lock);
		gen = cap->session->s_cap_gen;
		ttl = cap->session->s_cap_ttl;
		spin_unlock(&cap->session->s_cap_lock);

		if (cap->gen < gen || time_after_eq(jiffies, ttl)) {
			dout(30, "__ceph_caps_issued %p cap %p issued %d "
			     "but STALE (gen %u vs %u)\n", &ci->vfs_inode,
			     cap, cap->issued, cap->gen, gen);
			continue;
		}
		dout(30, "__ceph_caps_issued %p cap %p issued %d\n",
		     &ci->vfs_inode, cap, cap->issued);
		have |= cap->issued;
		if (implemented)
			*implemented |= cap->implemented;
	}
	return have;
}

/*
 * caller should hold i_lock, snap_mutex, and session s_mutex.
 * returns true if this is the last cap.  if so, caller should iput.
 */
int __ceph_remove_cap(struct ceph_inode_cap *cap)
{
	struct ceph_mds_session *session = cap->session;
	struct ceph_inode_info *ci = cap->ci;

	dout(20, "__ceph_remove_cap %p from %p\n", cap, &ci->vfs_inode);

	/* remove from session list */
	list_del_init(&cap->session_caps);
	session->s_nr_caps--;

	/* remove from inode list */
	list_del_init(&cap->ci_caps);
	cap->session = 0;
	cap->mds = -1;  /* mark unused */

	if (cap < ci->i_static_caps ||
	    cap >= ci->i_static_caps + STATIC_CAPS)
		kfree(cap);

	if (list_empty(&ci->i_caps)) {
		list_del_init(&ci->i_snaprealm_item);
		return 1;
	}
	return 0;
}

/*
 * caller should hold snap_mutex and session s_mutex.
 */
void ceph_remove_cap(struct ceph_inode_cap *cap)
{
	struct inode *inode = &cap->ci->vfs_inode;
	int was_last;

	spin_lock(&inode->i_lock);
	was_last = __ceph_remove_cap(cap);
	spin_unlock(&inode->i_lock);
	if (was_last)
		iput(inode);
}

/*
 * caller holds i_lock
 *    -> client->cap_delay_lock
 */
void __ceph_cap_delay_requeue(struct ceph_mds_client *mdsc,
			      struct ceph_inode_info *ci)
{
	ci->i_hold_caps_until = round_jiffies(jiffies + HZ * 5);
	dout(10, "__cap_delay_requeue %p at %lu\n", &ci->vfs_inode,
	     ci->i_hold_caps_until);
	spin_lock(&mdsc->cap_delay_lock);
	if (list_empty(&ci->i_cap_delay_list))
		igrab(&ci->vfs_inode);
	else
		list_del_init(&ci->i_cap_delay_list);
	list_add_tail(&ci->i_cap_delay_list, &mdsc->cap_delay_list);
	spin_unlock(&mdsc->cap_delay_lock);
}


/*
 * examine currently used, wanted versus held caps.
 *  release, ack revoked caps to mds as appropriate.
 * @is_delayed if caller just dropped a cap ref, and we probably want to delay
 */
void ceph_check_caps(struct ceph_inode_info *ci, int is_delayed, int flush_snap)
{
	struct ceph_client *client = ceph_inode_to_client(&ci->vfs_inode);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct inode *inode = &ci->vfs_inode;
	struct ceph_inode_cap *cap;
	struct list_head *p;
	int wanted, used;
	struct ceph_mds_session *session = 0;  /* if non-NULL, i hold s_mutex */
	int took_snap_mutex = 0;             /* true if mdsc->snap_mutex held */
	int removed_last;

retry:
	spin_lock(&inode->i_lock);
	wanted = __ceph_caps_wanted(ci);
	used = __ceph_caps_used(ci);
	dout(10, "check_caps %p wanted %d used %d issued %d\n", inode,
	     wanted, used, __ceph_caps_issued(ci, 0));

	if (!is_delayed)
		__ceph_cap_delay_requeue(mdsc, ci);

	list_for_each(p, &ci->i_caps) {
		int revoking;
		cap = list_entry(p, struct ceph_inode_cap, ci_caps);

		/* note: no side-effects allowed, until we take s_mutex */
		revoking = cap->implemented & ~cap->issued;

		if (ci->i_wanted_max_size > ci->i_max_size &&
		    ci->i_wanted_max_size > ci->i_requested_max_size)
			goto ack;

		/* completed revocation? */
		if (revoking && (revoking && used) == 0) {
			dout(10, "completed revocation of %d\n",
			     cap->implemented & ~cap->issued);
			goto ack;
		}

		/* approaching file_max? */
		if ((cap->issued & CEPH_CAP_WR) &&
		    (inode->i_size << 1) >= ci->i_max_size &&
		    (ci->i_reported_size << 1) < ci->i_max_size) {
			dout(10, "i_size approaching max_size\n");
			goto ack;
		}

		/* flush snap? */
		if (flush_snap &&
		    (cap->issued & (CEPH_CAP_WR|CEPH_CAP_WRBUFFER))) {
			if (cap->flushed_snap >=
			    ci->i_snaprealm->cached_context->seq) {
				dout(10, "flushed_snap %llu >= seq %lld, "
				     "not flushing mds%d\n",
				     cap->flushed_snap,
				     ci->i_snaprealm->cached_context->seq,
				     cap->session->s_mds);
				continue;  /* already flushed for this snap */
			}
			goto ack;
		}

		if ((cap->issued & ~wanted) == 0)
			continue;     /* nothing extra, all good */

		if (time_before(jiffies, ci->i_hold_caps_until)) {
			/* delaying cap release for a bit */
			dout(30, "delaying cap release\n");
			continue;
		}

ack:
		/* take s_mutex, one way or another */
		if (session && session != cap->session) {
			dout(30, "oops, wrong session %p mutex\n", session);
			mutex_unlock(&session->s_mutex);
			session = 0;
		}
		/* take snap_mutex before session mutex */
		if (!flush_snap && !took_snap_mutex) {
			if (mutex_trylock(&mdsc->snap_mutex) == 0) {
				dout(10, "inverting snap/in locks on %p\n",
				     inode);
				spin_unlock(&inode->i_lock);
				mutex_lock(&mdsc->snap_mutex);
				took_snap_mutex = 1;
				goto retry;
			}
			took_snap_mutex = 1;
		}
		if (!session) {
			session = cap->session;
			if (mutex_trylock(&session->s_mutex) == 0) {
				dout(10, "inverting session/ino locks on %p\n",
				     session);
				spin_unlock(&inode->i_lock);
				mutex_lock(&session->s_mutex);
				goto retry;
			}
		}

		/* send_cap drops i_lock */
		removed_last = __ceph_mdsc_send_cap(mdsc, session, cap,
						    used, wanted, !is_delayed,
						    flush_snap);
		if (removed_last)
			goto out;
		/* retake i_lock and restart our cap scan. */
		goto retry;
	}

	/* okay */
	spin_unlock(&inode->i_lock);

out:
	if (session)
		mutex_unlock(&session->s_mutex);
	if (took_snap_mutex)
		mutex_unlock(&mdsc->snap_mutex);
}

void ceph_inode_set_size(struct inode *inode, loff_t size)
{
	struct ceph_inode_info *ci = ceph_inode(inode);

	spin_lock(&inode->i_lock);
	dout(30, "set_size %p %llu -> %llu\n", inode, inode->i_size, size);
	inode->i_size = size;
	inode->i_blocks = (size + (1 << 9) - 1) >> 9;

	if ((size << 1) >= ci->i_max_size &&
	    (ci->i_reported_size << 1) < ci->i_max_size) {
		spin_unlock(&inode->i_lock);
		ceph_check_caps(ci, 0, 0);
	} else
		spin_unlock(&inode->i_lock);
}

void ceph_put_fmode(struct ceph_inode_info *ci, int fmode)
{
	int last = 0;

	spin_lock(&ci->vfs_inode.i_lock);
	dout(20, "put_mode %p fmode %d %d -> %d\n", &ci->vfs_inode, fmode,
	     ci->i_nr_by_mode[fmode], ci->i_nr_by_mode[fmode]-1);
	if (--ci->i_nr_by_mode[fmode] == 0)
		last++;
	spin_unlock(&ci->vfs_inode.i_lock);

	if (last && ci->i_vino.snap == CEPH_NOSNAP)
		ceph_check_caps(ci, 0, 0);
}


/*
 * caller holds s_mutex.  NOT snap_mutex.
 * return value:
 *  0 - ok
 *  1 - send the msg back to mds
 */
int ceph_handle_cap_grant(struct inode *inode, struct ceph_mds_caps *grant,
			  struct ceph_mds_session *session)
{
	struct ceph_inode_cap *cap;
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	int seq = le32_to_cpu(grant->seq);
	int newcaps = le32_to_cpu(grant->caps);
	int used;
	int issued; /* to me, before */
	int wanted;
	int reply = 0;
	u64 size = le64_to_cpu(grant->size);
	u64 max_size = le64_to_cpu(grant->max_size);
	struct timespec mtime, atime, ctime;
	int wake = 0;
	int writeback_now = 0;
	int invalidate = 0;

	dout(10, "handle_cap_grant inode %p ci %p mds%d seq %d\n",
	     inode, ci, mds, seq);
	dout(10, " size %llu max_size %llu, i_size %llu\n", size, max_size,
		inode->i_size);

	spin_lock(&inode->i_lock);

	/* do we have this cap? */
	cap = __get_cap_for_mds(inode, mds);
	if (!cap) {
		/*
		 * then ignore.  never reply to cap messages out of turn,
		 * or we'll be mixing up different instances of caps on the
		 * same inode, and confuse the mds.
		 */
		dout(10, "no cap on %p ino %llx.%llx from mds%d, ignoring\n",
		     inode, ci->i_vino.ino, ci->i_vino.snap, mds);
		goto out;
	}
	dout(10, " cap %p\n", cap);
	cap->gen = session->s_cap_gen;

	/* size/ctime/mtime/atime? */
	issued = __ceph_caps_issued(ci, 0);
	ceph_decode_timespec(&mtime, &grant->mtime);
	ceph_decode_timespec(&atime, &grant->atime);
	ceph_decode_timespec(&ctime, &grant->ctime);
	fill_file_bits(inode, issued, le64_to_cpu(grant->time_warp_seq),
		       size, &ctime, &mtime, &atime);

	/* max size increase? */
	if (max_size != ci->i_max_size) {
		dout(10, "max_size %lld -> %llu\n", ci->i_max_size, max_size);
		ci->i_max_size = max_size;
		if (max_size >= ci->i_wanted_max_size) {
			ci->i_wanted_max_size = 0;  /* reset */
			ci->i_requested_max_size = 0;
		}
		wake = 1;
	}

	/* check cap bits */
	wanted = __ceph_caps_wanted(ci);
	used = __ceph_caps_used(ci);
	dout(10, " my wanted = %d, used = %d\n", wanted, used);
	if (wanted != le32_to_cpu(grant->wanted)) {
		dout(10, "mds wanted %d -> %d\n", le32_to_cpu(grant->wanted),
		     wanted);
		grant->wanted = cpu_to_le32(wanted);
	}

	cap->seq = seq;

	/* revocation? */
	if (cap->issued & ~newcaps) {
		dout(10, "revocation: %d -> %d\n", cap->issued, newcaps);
		if ((cap->issued & ~newcaps) & CEPH_CAP_RDCACHE)
			invalidate = 1;
		if ((used & ~newcaps) & CEPH_CAP_WRBUFFER)
			writeback_now = 1; /* will delay ack */
		else {
			cap->implemented = newcaps;
			/* ack now.  re-use incoming message. */
			grant->size = le64_to_cpu(inode->i_size);
			grant->max_size = 0;  /* don't re-request */
			ceph_encode_timespec(&grant->mtime, &inode->i_mtime);
			ceph_encode_timespec(&grant->atime, &inode->i_atime);
			grant->time_warp_seq = cpu_to_le64(ci->i_time_warp_seq);
			grant->snap_follows =
			     cpu_to_le64(ci->i_snaprealm->cached_context->seq);
			reply = 1;
			wake = 1;
		}
		cap->issued = newcaps;
		goto out;
	}

	/* grant or no-op */
	if (cap->issued == newcaps) {
		dout(10, "caps unchanged: %d -> %d\n", cap->issued, newcaps);
	} else {
		dout(10, "grant: %d -> %d\n", cap->issued, newcaps);
		cap->implemented = cap->issued = newcaps;
		wake = 1;
	}

out:
	spin_unlock(&inode->i_lock);
	if (wake)
		wake_up(&ci->i_cap_wq);
	if (writeback_now) {
		/*
		 * queue inode for writeback; we can't actually call
		 * write_inode_now, writepages, etc. from this
		 * context.
		 */
		dout(10, "queueing %p for writeback\n", inode);
		ceph_queue_writeback(ceph_client(inode->i_sb), ci);
	}
	if (invalidate)
		invalidate_mapping_pages(&inode->i_data, 0, -1);
	return reply;
}

void ceph_inode_writeback(struct work_struct *work)
{
	struct ceph_inode_info *ci = container_of(work, struct ceph_inode_info,
						  i_wb_work);
	dout(10, "writeback %p\n", &ci->vfs_inode);
	write_inode_now(&ci->vfs_inode, 0);
}


/*
 * called by setattr
 */
static int apply_truncate(struct inode *inode, loff_t size)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int rc;

	rc = vmtruncate(inode, size);
	if (rc == 0) {
		spin_lock(&inode->i_lock);
		ci->i_reported_size = size;
		spin_unlock(&inode->i_lock);
	}
	if (atomic_read(&ci->i_wrbuffer_ref) == 0)
		ceph_check_caps(ci, 0, 0);
	return rc;
}

/*
 * called by trunc_wq; take i_mutex ourselves
 */
void ceph_vmtruncate_work(struct work_struct *work)
{
	struct ceph_inode_info *ci = container_of(work, struct ceph_inode_info,
						  i_vmtruncate_work);
	struct inode *inode = &ci->vfs_inode;

	dout(10, "vmtruncate_work %p\n", inode);
	mutex_lock(&inode->i_mutex);
	__ceph_do_pending_vmtruncate(inode);
	mutex_unlock(&inode->i_mutex);
}

void __ceph_do_pending_vmtruncate(struct inode *inode)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	loff_t to;

	spin_lock(&inode->i_lock);
	to = ci->i_vmtruncate_to;
	ci->i_vmtruncate_to = -1;
	spin_unlock(&inode->i_lock);

	if (to >= 0) {
		dout(10, "__do_pending_vmtruncate %p to %lld\n", inode, to);
		vmtruncate(inode, to);
		if (atomic_read(&ci->i_wrbuffer_ref) == 0)
			ceph_check_caps(ci, 0, 0);
	} else
		dout(10, "__do_pending_vmtruncate %p nothing to do\n", inode);
}

/*
 * caller hold s_mutex, NOT snap_mutex.
 */
void ceph_handle_cap_trunc(struct inode *inode,
			   struct ceph_mds_caps *trunc,
			   struct ceph_mds_session *session)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	int seq = le32_to_cpu(trunc->seq);
	u64 size = le64_to_cpu(trunc->size);
	int queue_trunc = 0;

	dout(10, "handle_cap_trunc inode %p ci %p mds%d seq %d\n", inode, ci,
	     mds, seq);

	/*
	 * vmtruncate lazily; we can't block on i_mutex in the message
	 * handler path, or we deadlock against osd op replies needed
	 * to complete the writes holding i_lock.  vmtruncate will
	 * also block on page locks held by writes...
	 *
	 * if its an expansion, and there is no truncate pending, we
	 * don't need to truncate.
	 */

	spin_lock(&inode->i_lock);
	if (ci->i_vmtruncate_to < 0 && size > inode->i_size)
		dout(10, "clean fwd truncate, no vmtruncate needed\n");
	else if (ci->i_vmtruncate_to >= 0 && size >= ci->i_vmtruncate_to)
		dout(10, "trunc to %lld < %lld already queued\n",
		     ci->i_vmtruncate_to, size);
	else {
		/* we need to trunc even smaller */
		dout(10, "queueing trunc %lld -> %lld\n", inode->i_size, size);
		ci->i_vmtruncate_to = size;
		queue_trunc = 1;
	}
	i_size_write(inode, size);
	ci->i_reported_size = size;
	spin_unlock(&inode->i_lock);

	if (queue_trunc)
		queue_work(ceph_client(inode->i_sb)->trunc_wq,
			   &ci->i_vmtruncate_work);
}

void ceph_handle_cap_export(struct inode *inode, struct ceph_mds_caps *ex,
			    struct ceph_mds_session *session)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	unsigned mseq = le32_to_cpu(ex->migrate_seq);
	struct ceph_inode_cap *cap = 0, *t;
	struct list_head *p;
	int was_last = 0;

	dout(10, "handle_cap_export inode %p ci %p mds%d mseq %d\n",
	     inode, ci, mds, mseq);

	spin_lock(&inode->i_lock);

	/* make sure we haven't seen a higher mseq */
	list_for_each(p, &ci->i_caps) {
		t = list_entry(p, struct ceph_inode_cap, ci_caps);
		if (t->mseq > mseq) {
			dout(10, " higher mseq on cap from mds%d\n",
			     t->session->s_mds);
			goto out;
		}
		if (t->session->s_mds == mds)
			cap = t;
	}

	if (cap) {
		/* make note, and remove */
		ci->i_cap_exporting_mds = mds;
		ci->i_cap_exporting_mseq = mseq;
		ci->i_cap_exporting_issued = cap->issued;
		was_last = __ceph_remove_cap(cap);
	}

out:
	spin_unlock(&inode->i_lock);
	if (was_last)
		iput(inode);
}

void ceph_handle_cap_import(struct inode *inode, struct ceph_mds_caps *im,
			    struct ceph_mds_session *session,
			    void *snaptrace, int snaptrace_len)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	unsigned issued = le32_to_cpu(im->caps);
	unsigned seq = le32_to_cpu(im->seq);
	unsigned mseq = le32_to_cpu(im->migrate_seq);

	if (ci->i_cap_exporting_mds >= 0 &&
	    ci->i_cap_exporting_mseq < mseq) {
		dout(10, "handle_cap_import inode %p ci %p mds%d mseq %d"
		     " - cleared exporting from mds%d\n",
		     inode, ci, mds, mseq,
		     ci->i_cap_exporting_mds);
		ci->i_cap_exporting_issued = 0;
		ci->i_cap_exporting_mseq = 0;
		ci->i_cap_exporting_mds = -1;
	} else {
		dout(10, "handle_cap_import inode %p ci %p mds%d mseq %d\n",
		     inode, ci, mds, mseq);
	}

	ceph_add_cap(inode, session, -1, issued, seq, mseq,
		     snaptrace, snaptrace_len);
}


static void __take_cap_refs(struct ceph_inode_info *ci, int got)
{
	if (got & CEPH_CAP_RD)
		ci->i_rd_ref++;
	if (got & CEPH_CAP_RDCACHE)
		ci->i_rdcache_ref++;
	if (got & CEPH_CAP_WR)
		ci->i_wr_ref++;
	if (got & CEPH_CAP_WRBUFFER) {
		atomic_inc(&ci->i_wrbuffer_ref);
		dout(30, "__take_cap_refs %p wrbuffer %d -> %d (?)\n",
		     &ci->vfs_inode, atomic_read(&ci->i_wrbuffer_ref)-1,
		     atomic_read(&ci->i_wrbuffer_ref));
	}
}

void ceph_take_cap_refs(struct ceph_inode_info *ci, int got)
{
	dout(30, "take_cap_refs on %p taking %d\n", &ci->vfs_inode, got);
	spin_lock(&ci->vfs_inode.i_lock);
	__take_cap_refs(ci, got);
	spin_unlock(&ci->vfs_inode.i_lock);
}

int ceph_get_cap_refs(struct ceph_inode_info *ci, int need, int want, int *got,
		      loff_t endoff)
{
	int ret = 0;
	int have, implemented;

	dout(30, "get_cap_refs on %p need %d want %d\n", &ci->vfs_inode,
	     need, want);
	spin_lock(&ci->vfs_inode.i_lock);
	if (endoff >= 0 && endoff > (loff_t)ci->i_max_size) {
		dout(20, "get_cap_refs endoff %llu > max_size %llu\n",
		     endoff, ci->i_max_size);
		goto sorry;
	}
	have = __ceph_caps_issued(ci, &implemented);
	if ((have & need) == need) {
		/*
		 * look at (implemented & ~have & not) so that we keep waiting
		 * on transition from wanted -> needed caps.  this is needed
		 * for WRBUFFER|WR -> WR to avoid a new WR sync write from
		 * going before a prior buffered writeback happens.
		 */
		int not = want & ~(have & need);
		int revoking = implemented & ~have;
		dout(30, "get_cap_refs have %d but not %d (revoking %d)\n",
		     have, not, revoking);
		if ((revoking & not) == 0) {
			*got = need | (have & want);
			__take_cap_refs(ci, *got);
			ret = 1;
		}
	} else
		dout(30, "get_cap_refs have %d needed %d\n", have, need);
sorry:
	spin_unlock(&ci->vfs_inode.i_lock);
	dout(30, "get_cap_refs on %p ret %d got %d\n", &ci->vfs_inode,
	     ret, *got);
	return ret;
}

void ceph_put_cap_refs(struct ceph_inode_info *ci, int had)
{
	int last = 0;

	spin_lock(&ci->vfs_inode.i_lock);
	if (had & CEPH_CAP_RD)
		if (--ci->i_rd_ref == 0)
			last++;
	if (had & CEPH_CAP_RDCACHE)
		if (--ci->i_rdcache_ref == 0)
			last++;
	if (had & CEPH_CAP_WR)
		if (--ci->i_wr_ref == 0)
			last++;
	if (had & CEPH_CAP_WRBUFFER) {
		if (atomic_dec_and_test(&ci->i_wrbuffer_ref))
			last++;
		dout(30, "put_cap_refs %p wrbuffer %d -> %d (?)\n",
		     &ci->vfs_inode, atomic_read(&ci->i_wrbuffer_ref)+1,
		     atomic_read(&ci->i_wrbuffer_ref));
	}
	spin_unlock(&ci->vfs_inode.i_lock);

	dout(30, "put_cap_refs on %p had %d %s\n", &ci->vfs_inode, had,
	     last ? "last":"");

	if (last)
		ceph_check_caps(ci, 0, 0);
}

void ceph_put_wrbuffer_cap_refs(struct ceph_inode_info *ci, int nr)
{
	int was_last;
	int v;

	spin_lock(&ci->vfs_inode.i_lock);
	was_last = atomic_sub_and_test(nr, &ci->i_wrbuffer_ref);
	v = atomic_read(&ci->i_wrbuffer_ref);
	spin_unlock(&ci->vfs_inode.i_lock);

	dout(30, "put_wrbuffer_cap_refs on %p %d -> %d (?)%s\n",
	     &ci->vfs_inode, v+nr, v, was_last ? " LAST":"");
	WARN_ON(v < 0);

	if (was_last)
		ceph_check_caps(ci, 0, 0);
}


/*
 * symlinks
 */
static void *ceph_sym_follow_link(struct dentry *dentry, struct nameidata *nd)
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
static struct ceph_mds_request *prepare_setattr(struct ceph_mds_client *mdsc,
						struct dentry *dentry,
						int ia_valid, int op)
{
	char *path;
	int pathlen;
	struct ceph_mds_request *req;
	u64 pathbase;

	if (ia_valid & ATTR_FILE) {
		dout(5, "prepare_setattr dentry %p (inode %llx.%llx)\n", dentry,
		     ceph_vinop(dentry->d_inode));
		req = ceph_mdsc_create_request(mdsc, op,
					       ceph_ino(dentry->d_inode),
					       "", 0, 0,
					       dentry, USE_CAP_MDS);
	} else {
		dout(5, "prepare_setattr dentry %p (full path)\n", dentry);
		path = ceph_build_dentry_path(dentry, &pathlen, &pathbase, 0);
		if (IS_ERR(path))
			return ERR_PTR(PTR_ERR(path));
		req = ceph_mdsc_create_request(mdsc, op, pathbase, path, 0, 0,
					       dentry, USE_ANY_MDS);
		kfree(path);
	}
	return req;
}

static int ceph_setattr_chown(struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = dentry->d_inode;
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	const unsigned int ia_valid = attr->ia_valid;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *reqh;
	int err;

	req = prepare_setattr(mdsc, dentry, ia_valid, CEPH_MDS_OP_CHOWN);
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
	ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_IAUTH);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	dout(10, "chown result %d\n", err);
	if (err)
		return err;

	return 0;
}

static int ceph_setattr_chmod(struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = dentry->d_inode;
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *reqh;
	int err;

	req = prepare_setattr(mdsc, dentry, attr->ia_valid, CEPH_MDS_OP_LCHMOD);
	if (IS_ERR(req))
		return PTR_ERR(req);
	reqh = req->r_request->front.iov_base;
	reqh->args.chmod.mode = cpu_to_le32(attr->ia_mode);
	ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_IAUTH);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	dout(10, "chmod result %d\n", err);
	if (err)
		return err;

	return 0;
}

static int ceph_setattr_time(struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	const unsigned int ia_valid = attr->ia_valid;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *reqh;
	int err;

	/* if i hold CAP_EXCL, i can change [am]time any way i like */
	if (ceph_caps_issued(ci) & CEPH_CAP_EXCL) {
		dout(10, "utime holding EXCL, doing locally\n");
		ci->i_time_warp_seq++;
		if (ia_valid & ATTR_ATIME)
			inode->i_atime = attr->ia_atime;
		if (ia_valid & ATTR_MTIME)
			inode->i_ctime = inode->i_mtime = attr->ia_mtime;
		return 0;
	}

	/* if i hold CAP_WR, i can _increase_ [am]time safely */
	if ((ceph_caps_issued(ci) & CEPH_CAP_WR) &&
	    ((ia_valid & ATTR_MTIME) == 0 ||
	     timespec_compare(&inode->i_mtime, &attr->ia_mtime) < 0) &&
	    ((ia_valid & ATTR_ATIME) == 0 ||
	     timespec_compare(&inode->i_atime, &attr->ia_atime) < 0)) {
		dout(10, "utime holding WR, doing [am]time increase locally\n");
		if (ia_valid & ATTR_ATIME)
			inode->i_atime = attr->ia_atime;
		if (ia_valid & ATTR_MTIME)
			inode->i_ctime = inode->i_mtime = attr->ia_mtime;
		return 0;
	}

	/* if i have valid values, this may be a no-op */
	if (ceph_inode_lease_valid(inode, CEPH_LOCK_ICONTENT) &&
	    !(((ia_valid & ATTR_ATIME) &&
	       !timespec_equal(&inode->i_atime, &attr->ia_atime)) ||
			((ia_valid & ATTR_MTIME) &&
	       !timespec_equal(&inode->i_mtime, &attr->ia_mtime)))) {
		dout(10, "lease indicates utimes is a no-op\n");
		return 0;
	}

	req = prepare_setattr(mdsc, dentry, ia_valid, CEPH_MDS_OP_LUTIME);
	if (IS_ERR(req))
		return PTR_ERR(req);
	reqh = req->r_request->front.iov_base;
	ceph_encode_timespec(&reqh->args.utime.mtime, &attr->ia_mtime);
	ceph_encode_timespec(&reqh->args.utime.atime, &attr->ia_atime);

	reqh->args.utime.mask = 0;
	if (ia_valid & ATTR_ATIME)
		reqh->args.utime.mask |= CEPH_UTIME_ATIME;
	if (ia_valid & ATTR_MTIME)
		reqh->args.utime.mask |= CEPH_UTIME_MTIME;

	ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_ICONTENT);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	dout(10, "utime result %d\n", err);
	if (err)
		return err;

	return 0;
}

static int ceph_setattr_size(struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	const unsigned int ia_valid = attr->ia_valid;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *reqh;
	int err;

	dout(10, "truncate: ia_size %d i_size %d\n",
	     (int)attr->ia_size, (int)inode->i_size);
	if (ceph_caps_issued(ci) & CEPH_CAP_EXCL &&
	    attr->ia_size > inode->i_size) {
		dout(10, "holding EXCL, doing truncate (fwd) locally\n");
		inode->i_ctime = attr->ia_ctime;
		err = apply_truncate(inode, attr->ia_size);
		if (err)
			return err;
		return 0;
	}
	if (ceph_inode_lease_valid(inode, CEPH_LOCK_ICONTENT) &&
	    attr->ia_size == inode->i_size) {
		dout(10, "lease indicates truncate is a no-op\n");
		return 0;
	}
	req = prepare_setattr(mdsc, dentry, ia_valid, CEPH_MDS_OP_LTRUNCATE);
	if (IS_ERR(req))
		return PTR_ERR(req);
	reqh = req->r_request->front.iov_base;
	reqh->args.truncate.length = cpu_to_le64(attr->ia_size);
	ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_ICONTENT);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	dout(10, "truncate result %d\n", err);
	__ceph_do_pending_vmtruncate(inode);
	if (err)
		return err;

	return 0;
}


int ceph_setattr(struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = dentry->d_inode;
	const unsigned int ia_valid = attr->ia_valid;
	int err;

	if (ceph_snap(inode) != CEPH_NOSNAP)
		return -EROFS;

	__ceph_do_pending_vmtruncate(inode);

	err = inode_change_ok(inode, attr);
	if (err != 0)
		return err;

	/* gratuitous debug output */
	if (ia_valid & ATTR_UID)
		dout(10, "setattr: %p uid %d -> %d\n", inode,
		     inode->i_uid, attr->ia_uid);
	if (ia_valid & ATTR_GID)
		dout(10, "setattr: %p gid %d -> %d\n", inode,
		     inode->i_uid, attr->ia_uid);
	if (ia_valid & ATTR_MODE)
		dout(10, "setattr: %p mode 0%o -> 0%o\n", inode, inode->i_mode,
		     attr->ia_mode);
	if (ia_valid & ATTR_SIZE)
		dout(10, "setattr: %p size %lld -> %lld\n", inode,
		     inode->i_size, attr->ia_size);
	if (ia_valid & ATTR_ATIME)
		dout(10, "setattr: %p atime %ld.%ld -> %ld.%ld\n", inode,
		     inode->i_atime.tv_sec, inode->i_atime.tv_nsec,
		     attr->ia_atime.tv_sec, attr->ia_atime.tv_nsec);
	if (ia_valid & ATTR_MTIME)
		dout(10, "setattr: %p mtime %ld.%ld -> %ld.%ld\n", inode,
		     inode->i_mtime.tv_sec, inode->i_mtime.tv_nsec,
		     attr->ia_mtime.tv_sec, attr->ia_mtime.tv_nsec);
	if (ia_valid & ATTR_MTIME)
		dout(10, "setattr: %p ctime %ld.%ld -> %ld.%ld\n", inode,
		     inode->i_ctime.tv_sec, inode->i_ctime.tv_nsec,
		     attr->ia_ctime.tv_sec, attr->ia_ctime.tv_nsec);
	if (ia_valid & ATTR_FILE)
		dout(10, "setattr: %p ATTR_FILE ... hrm!\n", inode);

	/* chown */
	if (ia_valid & (ATTR_UID|ATTR_GID)) {
		err = ceph_setattr_chown(dentry, attr);
		if (err)
			return err;
	}

	/* chmod? */
	if (ia_valid & ATTR_MODE) {
		err = ceph_setattr_chmod(dentry, attr);
		if (err)
			return err;
	}

	/* utimes */
	if (ia_valid & (ATTR_ATIME|ATTR_MTIME)) {
		err = ceph_setattr_time(dentry, attr);
		if (err)
			return err;
	}

	/* truncate? */
	if (ia_valid & ATTR_SIZE) {
		err = ceph_setattr_size(dentry, attr);
		if (err)
			return err;
	}

	return 0;
}

static int do_getattr(struct dentry *dentry, int mask)
{
	int want_inode = 0;
	struct dentry *ret;

	if (ceph_snap(dentry->d_inode) == CEPH_SNAPDIR) {
		dout(30, "getattr dentry %p inode %p SNAPDIR\n", dentry,
		     dentry->d_inode);
		return 0;
	}

	dout(30, "getattr dentry %p inode %p\n", dentry,
	     dentry->d_inode);

	if (ceph_inode_lease_valid(dentry->d_inode, mask))
		return 0;

	/*
	 * if the dentry is unhashed AND we have a cap, stat
	 * the ino directly.  (if its unhashed and we don't have a
	 * cap, we may be screwed anyway.)
	 */
	if (d_unhashed(dentry)) {
		if (ceph_get_cap_mds(dentry->d_inode) >= 0)
			want_inode = 1;
		else
			derr(10, "WARNING: getattr on unhashed cap-less"
			     " dentry %p %.*s\n", dentry,
			     dentry->d_name.len, dentry->d_name.name);
	}
	ret = ceph_do_lookup(dentry->d_inode->i_sb, dentry, mask,
			     want_inode, 0);
	if (IS_ERR(ret))
		return PTR_ERR(ret);
	if (ret)
		dentry = ret;
	if (!dentry->d_inode)
		return -ENOENT;
	return 0;
}

int ceph_getattr(struct vfsmount *mnt, struct dentry *dentry,
		 struct kstat *stat)
{
	int err;

	err = do_getattr(dentry, CEPH_STAT_MASK_INODE_ALL);
	dout(30, "getattr returned %d\n", err);
	if (!err) {
		generic_fillattr(dentry->d_inode, stat);
		stat->ino = ceph_ino(dentry->d_inode);
		if (ceph_snap(dentry->d_inode) != CEPH_NOSNAP)
			stat->dev = ceph_snap(dentry->d_inode);
		else
			stat->dev = 0;
	}
	return err;
}

ssize_t ceph_getxattr(struct dentry *dentry, const char *name, void *value,
		      size_t size)
{
	struct inode *inode = dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	int namelen = strlen(name);
	u32 numattr;
	int err;
	void *p, *end;

	err = do_getattr(dentry, CEPH_STAT_MASK_XATTR);
	if (err)
		return err;

	spin_lock(&inode->i_lock);

	err = -ENODATA;
	if (!ci->i_xattr_len)
		goto out;

	/* find attr */
	p = ci->i_xattr_data;
	end = p + ci->i_xattr_len;
	ceph_decode_32_safe(&p, end, numattr, bad);
	err = -ENODATA;  /* == ENOATTR */
	while (numattr--) {
		u32 len;
		int match;
		ceph_decode_32_safe(&p, end, len, bad);
		match = (len == namelen && strncmp(name, p, len) == 0);
		p += len;
		ceph_decode_32_safe(&p, end, len, bad);
		if (match) {
			err = -ERANGE;
			if (size && size < len)
				goto out;
			err = len;
			if (size == 0)
				goto out;
			memcpy(value, p, len);
			goto out;
		}
		p += len;
	}

out:
	spin_unlock(&inode->i_lock);
	return err;

bad:
	derr(10, "corrupt xattr info on %p %llx.%llx\n", dentry->d_inode,
	     ceph_vinop(dentry->d_inode));
	err = -EIO;
	goto out;
}

ssize_t ceph_listxattr(struct dentry *dentry, char *names, size_t size)
{
	struct inode *inode = dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	int namelen = 0;
	u32 numattr = 0;
	void *p, *end;
	int err;

	err = do_getattr(dentry, CEPH_STAT_MASK_XATTR);
	if (err)
		return err;

	spin_lock(&inode->i_lock);

	/* measure len */
	if (ci->i_xattr_len) {
		p = ci->i_xattr_data;
		end = p + ci->i_xattr_len;
		ceph_decode_32_safe(&p, end, numattr, bad);
		while (numattr--) {
			u32 len;
			ceph_decode_32_safe(&p, end, len, bad);
			namelen += len + 1;
			p += len;
			ceph_decode_32_safe(&p, end, len, bad);
			p += len;
		}
	} else
		namelen = 1; /* for \0 */

	err = -ERANGE;
	if (size && namelen > size)
		goto out;
	err = namelen;
	if (size == 0)
		goto out;

	/* copy names */
	if (ci->i_xattr_len) {
		p = ci->i_xattr_data;
		ceph_decode_32(&p, numattr);
		while (numattr--) {
			u32 len;
			ceph_decode_32(&p, len);
			memcpy(names, p, len);
			names[len] = '\0';
			names += len + 1;
			p += len;
			ceph_decode_32(&p, len);
			p += len;
		}
	} else
		names[0] = 0;

out:
	spin_unlock(&inode->i_lock);
	return err;

bad:
	derr(10, "corrupt xattr info on %p %llx.%llx\n", dentry->d_inode,
	     ceph_vinop(dentry->d_inode));
	err = -EIO;
	goto out;
}

int ceph_setxattr(struct dentry *dentry, const char *name,
		  const void *value, size_t size, int flags)
{
	struct ceph_client *client = ceph_client(dentry->d_sb);
	struct inode *inode = dentry->d_inode;
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	struct ceph_mds_request_head *rhead;
	char *path;
	int pathlen;
	u64 pathbase;
	int err;
	int i, nr_pages;
	struct page **pages = 0;
	void *kaddr;

	if (ceph_snap(inode) != CEPH_NOSNAP)
		return -EROFS;

	/* only support user.* xattrs, for now */
	if (strncmp(name, "user.", 5) != 0)
		return -EOPNOTSUPP;

	/* copy value into some pages */
	nr_pages = calc_pages_for(0, size);
	if (nr_pages) {
		pages = kmalloc(sizeof(pages)*nr_pages, GFP_NOFS);
		if (!pages)
			return -ENOMEM;
		err = -ENOMEM;
		for (i = 0; i < nr_pages; i++) {
			pages[i] = alloc_page(GFP_NOFS);
			if (!pages[i]) {
				nr_pages = i;
				goto out;
			}
			kaddr = kmap(pages[i]);
			memcpy(kaddr, value + i*PAGE_CACHE_SIZE,
			       min(PAGE_CACHE_SIZE, size-i*PAGE_CACHE_SIZE));
		}
	}

	/* do request */
	path = ceph_build_dentry_path(dentry, &pathlen, &pathbase, 0);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LSETXATTR,
				       pathbase, path, 0, name,
				       dentry, USE_AUTH_MDS);
	kfree(path);
	if (IS_ERR(req))
		return PTR_ERR(req);

	rhead = req->r_request->front.iov_base;
	rhead->args.setxattr.flags = cpu_to_le32(flags);

	req->r_request->pages = pages;
	req->r_request->nr_pages = nr_pages;
	req->r_request->hdr.data_len = cpu_to_le32(size);
	req->r_request->hdr.data_off = cpu_to_le32(0);

	ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_IXATTR);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);

out:
	if (pages) {
		for (i = 0; i < nr_pages; i++)
			__free_page(pages[i]);
		kfree(pages);
	}
	return err;
}

int ceph_removexattr(struct dentry *dentry, const char *name)
{
	struct ceph_client *client = ceph_client(dentry->d_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct inode *inode = dentry->d_inode;
	struct ceph_mds_request *req;
	char *path;
	int pathlen;
	u64 pathbase;
	int err;

	if (ceph_snap(inode) != CEPH_NOSNAP)
		return -EROFS;

	/* only support user.* xattrs, for now */
	if (strncmp(name, "user.", 5) != 0)
		return -EOPNOTSUPP;

	path = ceph_build_dentry_path(dentry, &pathlen, &pathbase, 0);
	if (IS_ERR(path))
		return PTR_ERR(path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LRMXATTR,
				       pathbase, path, 0, name,
				       dentry, USE_AUTH_MDS);
	kfree(path);
	if (IS_ERR(req))
		return PTR_ERR(req);

	ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_IXATTR);
	err = ceph_mdsc_do_request(mdsc, req);
	ceph_mdsc_put_request(req);
	return err;
}

