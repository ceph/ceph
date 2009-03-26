#include <linux/module.h>
#include <linux/fs.h>
#include <linux/smp_lock.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/uaccess.h>
#include <linux/kernel.h>
#include <linux/namei.h>
#include <linux/writeback.h>

#include "ceph_debug.h"

int ceph_debug_inode __read_mostly = -1;
#define DOUT_MASK DOUT_MASK_INODE
#define DOUT_VAR ceph_debug_inode
#include "super.h"
#include "decode.h"

static const struct inode_operations ceph_symlink_iops;

static void ceph_inode_invalidate_pages(struct work_struct *work);


/*
 * find or create an inode, given the ceph ino number
 */
struct inode *ceph_get_inode(struct super_block *sb, struct ceph_vino vino)
{
	struct inode *inode;
	ino_t t = ceph_vino_to_ino(vino);

	inode = iget5_locked(sb, t, ceph_ino_compare, ceph_set_ino_cb, &vino);
	if (inode == NULL)
		return ERR_PTR(-ENOMEM);
	if (inode->i_state & I_NEW) {
		dout(40, "get_inode created new inode %p %llx.%llx ino %llx\n",
		     inode, ceph_vinop(inode), (u64)inode->i_ino);
		unlock_new_inode(inode);
	}

	dout(30, "get_inode on %lu=%llx.%llx got %p\n", inode->i_ino, vino.ino,
	     vino.snap, inode);
	return inode;
}

/*
 * get/constuct snapdir inode for a given directory
 */
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


/*
 * find/create a frag in the tree
 */
static struct ceph_inode_frag *__get_or_create_frag(struct ceph_inode_info *ci,
						    u32 f)
{
	struct rb_node **p;
	struct rb_node *parent = NULL;
	struct ceph_inode_frag *frag;
	int c;

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
			return frag;
	}

	frag = kmalloc(sizeof(*frag), GFP_NOFS);
	if (!frag) {
		derr(0, "ENOMEM on %p %llx.%llx frag %x\n", &ci->vfs_inode,
		     ceph_vinop(&ci->vfs_inode), f);
		return ERR_PTR(-ENOMEM);
	}
	frag->frag = f;
	frag->split_by = 0;
	frag->mds = -1;
	frag->ndist = 0;

	rb_link_node(&frag->node, parent, p);
	rb_insert_color(&frag->node, &ci->i_fragtree);

	dout(20, "get_or_create_frag added %llx.%llx frag %x\n",
	     ceph_vinop(&ci->vfs_inode), f);

	return frag;
}

/*
 * Choose frag containing the given value @v.  If @pfrag is
 * specified, copy the frag delegation info to the caller if
 * it is present.
 */
u32 ceph_choose_frag(struct ceph_inode_info *ci, u32 v,
		       struct ceph_inode_frag *pfrag,
		       int *found)
{
	u32 t = frag_make(0, 0);
	struct ceph_inode_frag *frag;
	unsigned nway, i;
	u32 n;

	if (found)
		*found = 0;

	mutex_lock(&ci->i_fragtree_mutex);
	while (1) {
		WARN_ON(!frag_contains_value(t, v));
		frag = __ceph_find_frag(ci, t);
		if (!frag)
			break; /* t is a leaf */
		if (frag->split_by == 0) {
			if (pfrag)
				memcpy(pfrag, frag, sizeof(*pfrag));
			if (found)
				*found = 1;
			break;
		}

		/* choose child */
		nway = 1 << frag->split_by;
		dout(30, "choose_frag(%x) %x splits by %d (%d ways)\n", v, t,
		     frag->split_by, nway);
		for (i = 0; i < nway; i++) {
			n = frag_make_child(t, frag->split_by, i);
			if (frag_contains_value(n, v)) {
				t = n;
				break;
			}
		}
		BUG_ON(i == nway);
	}
	dout(30, "choose_frag(%x) = %x\n", v, t);

	mutex_unlock(&ci->i_fragtree_mutex);
	return t;
}

/*
 * Process dirfrag (delegation) info from the mds.  Include leaf
 * fragment in tree ONLY if mds >= 0 || ndist > 0.  Otherwise, only
 * branches/splits are included in i_fragtree)
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

	mutex_lock(&ci->i_fragtree_mutex);
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
			/* tree branch, keep and clear */
			dout(20, "fill_dirfrag cleared %llx.%llx frag %x"
			     " referral\n", ceph_vinop(inode), id);
			frag->mds = -1;
			frag->ndist = 0;
		}
		goto out;
	}


	/* find/add this frag to store mds delegation info */
	frag = __get_or_create_frag(ci, id);
	if (IS_ERR(frag)) {
		/* this is not the end of the world; we can continue
		   with bad/inaccurate delegation info */
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
	mutex_unlock(&ci->i_fragtree_mutex);
	return err;
}


/*
 * initialize a newly allocated inode.
 */
struct inode *ceph_alloc_inode(struct super_block *sb)
{
	struct ceph_inode_info *ci;
	int i;

	ci = kmem_cache_alloc(ceph_inode_cachep, GFP_NOFS);
	if (!ci)
		return NULL;

	dout(10, "alloc_inode %p\n", &ci->vfs_inode);

	ci->i_version = 0;
	ci->i_time_warp_seq = 0;
	ci->i_ceph_flags = 0;
	ci->i_symlink = NULL;

	ci->i_fragtree = RB_ROOT;
	mutex_init(&ci->i_fragtree_mutex);

	ci->i_xattr_len = 0;
	ci->i_xattr_data = NULL;

	ci->i_caps = RB_ROOT;
	ci->i_dirty_caps = 0;
	init_waitqueue_head(&ci->i_cap_wq);
	ci->i_hold_caps_until = 0;
	INIT_LIST_HEAD(&ci->i_cap_delay_list);
	ci->i_cap_exporting_mds = 0;
	ci->i_cap_exporting_mseq = 0;
	ci->i_cap_exporting_issued = 0;
	INIT_LIST_HEAD(&ci->i_cap_snaps);
	ci->i_head_snapc = NULL;
	ci->i_snap_caps = 0;

	for (i = 0; i < CEPH_FILE_MODE_NUM; i++)
		ci->i_nr_by_mode[i] = 0;

	ci->i_truncate_seq = 0;
	ci->i_truncate_size = 0;
	ci->i_truncate_pending = 0;

	ci->i_max_size = 0;
	ci->i_reported_size = 0;
	ci->i_wanted_max_size = 0;
	ci->i_requested_max_size = 0;

	ci->i_rd_ref = 0;
	ci->i_rdcache_ref = 0;
	ci->i_wr_ref = 0;
	ci->i_wrbuffer_ref = 0;
	ci->i_wrbuffer_ref_head = 0;
	ci->i_rdcache_gen = 0;
	ci->i_rdcache_revoking = 0;

	INIT_LIST_HEAD(&ci->i_unsafe_writes);
	INIT_LIST_HEAD(&ci->i_unsafe_dirops);
	spin_lock_init(&ci->i_unsafe_lock);

	ci->i_snap_realm = NULL;
	INIT_LIST_HEAD(&ci->i_snap_realm_item);
	INIT_LIST_HEAD(&ci->i_snap_flush_item);

	INIT_WORK(&ci->i_wb_work, ceph_inode_writeback);
	INIT_WORK(&ci->i_pg_inv_work, ceph_inode_invalidate_pages);

	INIT_WORK(&ci->i_vmtruncate_work, ceph_vmtruncate_work);

	return &ci->vfs_inode;
}

void ceph_destroy_inode(struct inode *inode)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_frag *frag;
	struct rb_node *n;

	dout(30, "destroy_inode %p ino %llx.%llx\n", inode, ceph_vinop(inode));
	kfree(ci->i_symlink);
	while ((n = rb_first(&ci->i_fragtree)) != NULL) {
		frag = rb_entry(n, struct ceph_inode_frag, node);
		rb_erase(n, &ci->i_fragtree);
		kfree(frag);
	}
	kfree(ci->i_xattr_data);
	kmem_cache_free(ceph_inode_cachep, ci);
}


/*
 * Helpers to fill in size, ctime, mtime, and atime.  We have to be
 * careful because either the client or MDS may have more up to date
 * info, depending on which capabilities are held, and whether
 * time_warp_seq or truncate_seq have increased.  Ordinarily, mtime
 * and size are monotonically increasing, except when utimes() or
 * truncate() increments the corresponding _seq values on the MDS.
 */
int ceph_fill_file_size(struct inode *inode, int issued,
			u32 truncate_seq, u64 truncate_size, u64 size)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int queue_trunc = 0;

	if (ceph_seq_cmp(truncate_seq, ci->i_truncate_seq) > 0 ||
	    (truncate_seq == ci->i_truncate_seq && size > inode->i_size)) {
		dout(10, "size %lld -> %llu\n", inode->i_size, size);
		inode->i_size = size;
		inode->i_blocks = (size + (1<<9) - 1) >> 9;
		ci->i_reported_size = size;
		if (truncate_seq != ci->i_truncate_seq) {
			dout(10, "truncate_seq %u -> %u\n",
			     ci->i_truncate_seq, truncate_seq);
			ci->i_truncate_seq = truncate_seq;
			ci->i_truncate_pending++;
			if (issued & (CEPH_CAP_FILE_RDCACHE|CEPH_CAP_FILE_RD|
				      CEPH_CAP_FILE_WR|CEPH_CAP_FILE_WRBUFFER|
				      CEPH_CAP_FILE_EXCL))
				queue_trunc = 1;
		}
	}
	if (ceph_seq_cmp(truncate_seq, ci->i_truncate_seq) >= 0 &&
	    ci->i_truncate_size != truncate_size) {
		dout(10, "truncate_size %lld -> %llu\n", ci->i_truncate_size,
		     truncate_size);
		ci->i_truncate_size = truncate_size;
	}
	return queue_trunc;
}

void ceph_fill_file_time(struct inode *inode, int issued,
			 u64 time_warp_seq, struct timespec *ctime,
			 struct timespec *mtime, struct timespec *atime)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int warn = 0;

	if (issued & (CEPH_CAP_FILE_EXCL|
		      CEPH_CAP_FILE_WR|
		      CEPH_CAP_FILE_WRBUFFER)) {
		if (timespec_compare(ctime, &inode->i_ctime) > 0) {
			dout(20, "ctime %ld.%09ld -> %ld.%09ld inc w/ cap\n",
			     inode->i_ctime.tv_sec, inode->i_ctime.tv_nsec,
			     ctime->tv_sec, ctime->tv_nsec);
			inode->i_ctime = *ctime;
		}
		if (ceph_seq_cmp(time_warp_seq, ci->i_time_warp_seq) > 0) {
			/* the MDS did a utimes() */
			dout(20, "mtime %ld.%09ld -> %ld.%09ld "
			     "tw %d -> %d\n",
			     inode->i_mtime.tv_sec, inode->i_mtime.tv_nsec,
			     mtime->tv_sec, mtime->tv_nsec,
			     ci->i_time_warp_seq, (int)time_warp_seq);

			inode->i_mtime = *mtime;
			inode->i_atime = *atime;
			ci->i_time_warp_seq = time_warp_seq;
		} else if (time_warp_seq == ci->i_time_warp_seq) {
			/* nobody did utimes(); take the max */
			if (timespec_compare(mtime, &inode->i_mtime) > 0) {
				dout(20, "mtime %ld.%09ld -> %ld.%09ld inc\n",
				     inode->i_mtime.tv_sec,
				     inode->i_mtime.tv_nsec,
				     mtime->tv_sec, mtime->tv_nsec);
				inode->i_mtime = *mtime;
			}
			if (timespec_compare(atime, &inode->i_atime) > 0) {
				dout(20, "atime %ld.%09ld -> %ld.%09ld inc\n",
				     inode->i_atime.tv_sec,
				     inode->i_atime.tv_nsec,
				     atime->tv_sec, atime->tv_nsec);
				inode->i_atime = *atime;
			}
		} else if (issued & CEPH_CAP_FILE_EXCL) {
			/* we did a utimes(); ignore mds values */
		} else {
			warn = 1;
		}
	} else {
		/* we have no write caps; whatever the MDS says is true */
		if (ceph_seq_cmp(time_warp_seq, ci->i_time_warp_seq) >= 0) {
			inode->i_ctime = *ctime;
			inode->i_mtime = *mtime;
			inode->i_atime = *atime;
			ci->i_time_warp_seq = time_warp_seq;
		} else {
			warn = 1;
		}
	}
	if (warn) /* time_warp_seq shouldn't go backwards */
		dout(10, "%p mds time_warp_seq %llu < %u\n",
		     inode, time_warp_seq, ci->i_time_warp_seq);
}

/*
 * populate an inode based on info from mds.
 * may be called on new or existing inodes.
 */
static int fill_inode(struct inode *inode,
		      struct ceph_mds_reply_info_in *iinfo,
		      struct ceph_mds_reply_dirfrag *dirinfo,
		      struct ceph_mds_session *session,
		      unsigned long ttl_from, int cap_fmode)
{
	struct ceph_mds_reply_inode *info = iinfo->in;
	struct ceph_inode_info *ci = ceph_inode(inode);
	int i;
	int issued, implemented;
	struct timespec mtime, atime, ctime;
	u32 nsplits;
	void *xattr_data = NULL;
	int err = 0;
	int queue_trunc = 0;

	dout(30, "fill_inode %p ino %llx.%llx v %llu had %llu\n",
	     inode, ceph_vinop(inode), le64_to_cpu(info->version),
	     ci->i_version);

	/*
	 * prealloc xattr data, if it looks like we'll need it.  only
	 * if len > 4 (meaning there are actually xattrs; the first 4
	 * bytes are the xattr count).
	 */
	if (iinfo->xattr_len > 4 && iinfo->xattr_len != ci->i_xattr_len) {
		xattr_data = kmalloc(iinfo->xattr_len, GFP_NOFS);
		if (!xattr_data)
			derr(10, "ENOMEM on xattr blob %d bytes\n",
			     ci->i_xattr_len);
	}

	spin_lock(&inode->i_lock);

	/*
	 * provided version will be odd if inode value is projected,
	 * even if stable.  skip the update if we have a newer info
	 * (e.g., due to inode info racing form multiple MDSs), or if
	 * we are getting projected (unstable) inode info.
	 */
	if (le64_to_cpu(info->version) > 0 &&
	    (ci->i_version & ~1) > le64_to_cpu(info->version))
		goto no_change;

	issued = __ceph_caps_issued(ci, &implemented);
	issued |= implemented | __ceph_caps_dirty(ci);

	/* update inode */
	ci->i_version = le64_to_cpu(info->version);
	inode->i_version++;
	inode->i_rdev = le32_to_cpu(info->rdev);

	if ((issued & CEPH_CAP_AUTH_EXCL) == 0) {
		inode->i_mode = le32_to_cpu(info->mode);
		inode->i_uid = le32_to_cpu(info->uid);
		inode->i_gid = le32_to_cpu(info->gid);
		dout(20, "%p mode 0%o uid.gid %d.%d\n", inode, inode->i_mode, 
		     inode->i_uid, inode->i_gid);
	}

	if ((issued & CEPH_CAP_LINK_EXCL) == 0) {
		inode->i_nlink = le32_to_cpu(info->nlink);
	}

	/* be careful with mtime, atime, size */
	ceph_decode_timespec(&atime, &info->atime);
	ceph_decode_timespec(&mtime, &info->mtime);
	ceph_decode_timespec(&ctime, &info->ctime);
	queue_trunc = ceph_fill_file_size(inode, issued,
					  le32_to_cpu(info->truncate_seq),
					  le64_to_cpu(info->truncate_size),
					  le64_to_cpu(info->size));
	ceph_fill_file_time(inode, issued,
			    le32_to_cpu(info->time_warp_seq),
			    &ctime, &mtime, &atime);

	ci->i_max_size = le64_to_cpu(info->max_size);
	ci->i_layout = info->layout;
	inode->i_blkbits = fls(le32_to_cpu(info->layout.fl_stripe_unit)) - 1;

	/* xattrs */
	/* note that if i_xattr_len <= 4, i_xattr_data will still be NULL. */
	if (iinfo->xattr_len && (issued & CEPH_CAP_XATTR_EXCL) == 0 &&
	    le64_to_cpu(info->xattr_version) > ci->i_xattr_version) {
		if (ci->i_xattr_len != iinfo->xattr_len) {
			kfree(ci->i_xattr_data);
			ci->i_xattr_len = iinfo->xattr_len;
			ci->i_xattr_version = le64_to_cpu(info->xattr_version);
			ci->i_xattr_data = xattr_data;
			xattr_data = NULL;
		}
		if (ci->i_xattr_len > 4)
			memcpy(ci->i_xattr_data, iinfo->xattr_data,
			       ci->i_xattr_len);
	}

	ci->i_old_atime = inode->i_atime;

	inode->i_mapping->a_ops = &ceph_aops;
	inode->i_mapping->backing_dev_info =
		&ceph_client(inode->i_sb)->backing_dev_info;

no_change:
	spin_unlock(&inode->i_lock);

	/* queue truncate if we saw i_size decrease */
	if (queue_trunc)
		if (queue_work(ceph_client(inode->i_sb)->trunc_wq,
			       &ci->i_vmtruncate_work))
			igrab(inode);

	/* populate frag tree */
	/* FIXME: move me up, if/when version reflects fragtree changes */
	nsplits = le32_to_cpu(info->fragtree.nsplits);
	mutex_lock(&ci->i_fragtree_mutex);
	for (i = 0; i < nsplits; i++) {
		u32 id = le32_to_cpu(info->fragtree.splits[i].frag);
		struct ceph_inode_frag *frag = __get_or_create_frag(ci, id);

		if (IS_ERR(frag))
			continue;
		frag->split_by = le32_to_cpu(info->fragtree.splits[i].by);
		dout(20, " frag %x split by %d\n", frag->frag, frag->split_by);
	}
	mutex_unlock(&ci->i_fragtree_mutex);

	/* were we issued a capability? */
	if (info->cap.caps) {
		if (ceph_snap(inode) == CEPH_NOSNAP) {
			ceph_add_cap(inode, session,
				     le64_to_cpu(info->cap.cap_id),
				     cap_fmode,
				     le32_to_cpu(info->cap.caps),
				     le32_to_cpu(info->cap.wanted),
				     le32_to_cpu(info->cap.seq),
				     le32_to_cpu(info->cap.mseq),
				     le64_to_cpu(info->cap.realm),
				     le32_to_cpu(info->cap.ttl_ms),
				     ttl_from,
				     NULL);
		} else {
			spin_lock(&inode->i_lock);
			ci->i_snap_caps |= le32_to_cpu(info->cap.caps);
			if (cap_fmode >= 0)
				__ceph_get_fmode(ci, cap_fmode);
			spin_unlock(&inode->i_lock);
		}
	}

	/* update delegation info? */
	if (dirinfo)
		ceph_fill_dirfrag(inode, dirinfo);

	switch (inode->i_mode & S_IFMT) {
	case S_IFIFO:
	case S_IFBLK:
	case S_IFCHR:
	case S_IFSOCK:
		init_special_inode(inode, inode->i_mode, inode->i_rdev);
		inode->i_op = &ceph_file_iops;
		break;
	case S_IFREG:
		inode->i_op = &ceph_file_iops;
		inode->i_fop = &ceph_file_fops;
		break;
	case S_IFLNK:
		inode->i_op = &ceph_symlink_iops;
		if (!ci->i_symlink) {
			int symlen = iinfo->symlink_len;

			BUG_ON(symlen != inode->i_size);
			err = -ENOMEM;
			ci->i_symlink = kmalloc(symlen+1, GFP_NOFS);
			if (!ci->i_symlink)
				goto out;
			memcpy(ci->i_symlink, iinfo->symlink, symlen);
			ci->i_symlink[symlen] = 0;
		}
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

		/* it may be better to set st_size in getattr instead? */
		if (ceph_client(inode->i_sb)->mount_args.flags &
		    CEPH_MOUNT_RBYTES)
			inode->i_size = ci->i_rbytes;

		/* set dir completion flag? */
		if (ci->i_files == 0 && ci->i_subdirs == 0 &&
		    ceph_ino(inode) != CEPH_INO_ROOT &&
		    (le32_to_cpu(info->cap.caps) & CEPH_CAP_FILE_RDCACHE)) {
			dout(10, " marking %p complete (empty)\n", inode);
			ci->i_ceph_flags |= CEPH_I_COMPLETE;
		}
		break;
	default:
		derr(0, "BAD mode 0%o S_IFMT 0%o\n", inode->i_mode,
		     inode->i_mode & S_IFMT);
		err = -EINVAL;
		goto out;
	}
	err = 0;

out:
	kfree(xattr_data);
	return err;
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
	long unsigned half_ttl = from_time + (duration * HZ / 2) / 1000;

	/* only track leases on regular dentries */
	if (dentry->d_op != &ceph_dentry_ops)
		return;	
	
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
		goto out_unlock;  /* we already have a newer lease. */

	if (!di) {
		spin_unlock(&dentry->d_lock);
		di = kmalloc(sizeof(struct ceph_dentry_info),
			     GFP_NOFS);
		if (!di)
			return;          /* oh well */
		spin_lock(&dentry->d_lock);
		if (dentry->d_fsdata) {
			kfree(di);   /* lost a race! */
			goto out_unlock;
		}
		dentry->d_fsdata = di;
		di->lease_session = ceph_get_mds_session(session);
		di->lease_gen = session->s_cap_gen;
		di->lease_seq = le32_to_cpu(lease->seq);
		is_new = 1;
	} else if (di->lease_session != session)
		goto out_unlock;
	di->lease_renew_after = half_ttl;
	di->lease_renew_from = 0;
	dentry->d_time = ttl;
out_unlock:
	spin_unlock(&dentry->d_lock);
	return;
}

/*
 * splice a dentry to an inode.
 * caller must hold directory i_mutex for this to be safe.
 *
 * we will only rehash the resulting dentry if @prehash is
 * true; @prehash will be set to false (for the benefit of
 * the caller) if we fail.
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
		derr(0, "error splicing %p (%d) inode %p ino %llx.%llx\n",
		     dn, atomic_read(&dn->d_count), in, ceph_vinop(in));
		if (prehash)
			*prehash = false; /* don't rehash on error */
		dn = realdn; /* note realdn contains the error */
		goto out;
	} else if (realdn) {
		dout(10, "dn %p (%d) spliced with %p (%d) "
		     "inode %p ino %llx.%llx\n",
		     dn, atomic_read(&dn->d_count),
		     realdn, atomic_read(&realdn->d_count),
		     realdn->d_inode, ceph_vinop(realdn->d_inode));
		dput(dn);
		dn = realdn;
		ceph_init_dentry(dn);
	} else {
		dout(10, "dn %p attached to %p ino %llx.%llx\n",
		     dn, dn->d_inode, ceph_vinop(dn->d_inode));
	}
	if ((!prehash || *prehash) && d_unhashed(dn))
		d_rehash(dn);
out:
	return dn;
}

/*
 * Assimilate a full trace of inodes and dentries, from the root to
 * the item relevant for this reply, into our cache.  Make any dcache
 * changes needed to properly reflect the completed operation (e.g.,
 * call d_move).  Make note of the distribution of metadata across the
 * mds cluster.
 *
 * Care is taken to (attempt to) take i_mutex before adjusting dentry
 * linkages or leases.
 *
 * FIXME: we should check inode.version to avoid races between traces
 * from multiple MDSs after, say, a ancestor directory is renamed.
 *
 * Called with snap_rwsem (read).
 */
int ceph_fill_trace(struct super_block *sb, struct ceph_mds_request *req,
		    struct ceph_mds_session *session)
{
	struct ceph_mds_reply_info_parsed *rinfo = &req->r_reply_info;
	struct inode *in = NULL;
	struct ceph_mds_reply_inode *ininfo;
	struct ceph_vino vino;
	int i = 0;
	int err = 0;

	dout(10, "fill_trace %p is_dentry %d is_target %d\n", req,
	     rinfo->head->is_dentry, rinfo->head->is_target);

#if 0
	/*
	 * if we resend completed ops to a recovering mds, we get no
	 * trace.  pretend this is the case to ensure the 'no trace'
	 * handlers in the callers behave.
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

	if (!rinfo->head->is_target && !rinfo->head->is_dentry) {
		dout(10, "fill_trace reply has empty trace!\n");
		if (rinfo->head->result == 0 && req->r_locked_dir) {
			struct ceph_inode_info *ci =
				ceph_inode(req->r_locked_dir);
			dout(10, " clearing %p complete (empty trace)\n",
			     req->r_locked_dir);
			ci->i_ceph_flags &= ~(CEPH_I_READDIR | CEPH_I_COMPLETE);
		}
		return 0;
	}

	/*
	 * update a dentry?
	 */
	if (req->r_locked_dir) {
		/*
		 * lookup link rename   : null -> possibly existing inode
		 * mknod symlink mkdir  : null -> new inode
		 * unlink               : linked -> null
		 */
		struct inode *dir = req->r_locked_dir;
		struct dentry *dn = req->r_dentry;
		bool have_dir_cap, have_lease;

		BUG_ON(!dn);
		BUG_ON(!rinfo->head->is_dentry);
		BUG_ON(dn->d_parent->d_inode != dir);
		BUG_ON(ceph_ino(dir) !=
		       le64_to_cpu(rinfo->diri.in->ino));
		BUG_ON(ceph_snap(dir) !=
		       le64_to_cpu(rinfo->diri.in->snapid));

		err = fill_inode(dir, &rinfo->diri, rinfo->dirfrag,
				 session, req->r_request_started, -1);
		if (err < 0)
			return err;

		/* do we have a lease on the whole dir? */
		have_dir_cap =
			(le32_to_cpu(rinfo->diri.in->cap.caps) &
			 CEPH_CAP_FILE_RDCACHE);

		/* do we have a dn lease? */
		have_lease = have_dir_cap ||
			(le16_to_cpu(rinfo->dlease->mask) &
			 CEPH_LOCK_DN);

		if (!have_lease)
			dout(10, "fill_trace  no dentry lease or dir cap\n");

		/* rename? */
		if (req->r_old_dentry) {
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
			//dput(dn);  /* dn is dropped */
			dn = req->r_old_dentry;  /* use old_dentry */
			in = dn->d_inode;
		}

		/* null dentry? */
		if (!rinfo->head->is_target) {
			dout(10, "fill_trace null dentry\n");
			if (dn->d_inode) {
				dout(20, "d_delete %p\n", dn);
				d_delete(dn);
			} else {
				dout(20, "d_instantiate %p NULL\n", dn);
				d_instantiate(dn, NULL);
				if (have_lease && d_unhashed(dn))
					d_rehash(dn);
				update_dentry_lease(dn, rinfo->dlease,
						    session,
						    req->r_request_started);
			}
			goto done;
		}

		/* attach proper inode */
		ininfo = rinfo->targeti.in;
		vino.ino = le64_to_cpu(ininfo->ino);
		vino.snap = le64_to_cpu(ininfo->snapid);
		if (!dn->d_inode) {
			in = ceph_get_inode(dn->d_sb, vino);
			if (IS_ERR(in)) {
				derr(30, "get_inode badness\n");
				err = PTR_ERR(in);
				d_delete(dn);
				goto done;
			}
			dn = splice_dentry(dn, in, &have_lease);
			if (IS_ERR(dn)) {
				err = PTR_ERR(dn);
				goto done;
			}
			req->r_dentry = dn;  /* may have spliced */
		} else if (ceph_ino(in) != vino.ino ||
			   ceph_snap(in) != vino.snap) {
			dout(10, " %p links to %p %llx.%llx, not %llx.%llx\n",
			     dn, in, ceph_ino(in), ceph_snap(in),
			     vino.ino, vino.snap);
			have_lease = false;
			in = NULL;
		}

		if (have_lease)
			update_dentry_lease(dn, rinfo->dlease, session,
					    req->r_request_started);
		dout(10, " final dn %p\n", dn);
		i++;
	}

	/* update any additional inodes */
	if (rinfo->head->is_target) {
		vino.ino = le64_to_cpu(rinfo->targeti.in->ino);
		vino.snap = le64_to_cpu(rinfo->targeti.in->snapid);

		if (in != NULL &&
		    ceph_ino(in) == vino.ino && ceph_snap(in) == vino.snap) {
			igrab(in);
		} else {
			in = ceph_get_inode(sb, vino);
			if (IS_ERR(in)) {
				err = PTR_ERR(in);
				goto done;
			}
		}

		err = fill_inode(in,
				 &rinfo->targeti, NULL,
				 session, req->r_request_started,
				 (le32_to_cpu(rinfo->head->result) == 0) ?
				 req->r_fmode : -1);
		if (err < 0) {
			derr(30, "fill_inode badness\n");
			goto done;
		}
		req->r_target_inode = in;
	}

done:
	dout(10, "fill_trace done err=%d\n", err);
	return err;
}

/*
 * prepopulate cache with readdir results, leases, etc.
 */
int ceph_readdir_prepopulate(struct ceph_mds_request *req,
			     struct ceph_mds_session *session)
{
	struct dentry *parent = req->r_dentry;
	struct ceph_mds_reply_info_parsed *rinfo = &req->r_reply_info;
	struct qstr dname;
	struct dentry *dn;
	struct inode *in;
	int err = 0, i;
	struct inode *snapdir = NULL;

	if (le32_to_cpu(rinfo->head->op) == CEPH_MDS_OP_LSSNAP) {
		snapdir = ceph_get_snapdir(parent->d_inode);
		parent = d_find_alias(snapdir);
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

		dname.name = rinfo->dir_dname[i];
		dname.len = rinfo->dir_dname_len[i];
		dname.hash = full_name_hash(dname.name, dname.len);

		vino.ino = le64_to_cpu(rinfo->dir_in[i].in->ino);
		vino.snap = le64_to_cpu(rinfo->dir_in[i].in->snapid);

retry_lookup:
		dn = d_lookup(parent, &dname);
		dout(30, "d_lookup on parent=%p name=%.*s got %p\n",
		     parent, dname.len, dname.name, dn);

		if (!dn) {
			dn = d_alloc(parent, &dname);
			dout(40, "d_alloc %p '%.*s' = %p\n", parent,
			     dname.len, dname.name, dn);
			if (dn == NULL) {
				dout(30, "d_alloc badness\n");
				err = -ENOMEM;
				goto out;
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
		if (dn->d_inode) {
			in = dn->d_inode;
		} else {
			in = ceph_get_inode(parent->d_sb, vino);
			if (in == NULL) {
				dout(30, "new_inode badness\n");
				d_delete(dn);
				dput(dn);
				err = -ENOMEM;
				goto out;
			}
			dn = splice_dentry(dn, in, NULL);
		}

		if (fill_inode(in, &rinfo->dir_in[i], NULL, session,
			       req->r_request_started, -1) < 0) {
			dout(0, "fill_inode badness on %p\n", in);
			dput(dn);
			continue;
		}
		update_dentry_lease(dn, rinfo->dir_dlease[i],
				    req->r_session, req->r_request_started);
		dput(dn);
	}

out:
	if (snapdir) {
		iput(snapdir);
		dput(parent);
	}
	dout(10, "readdir_prepopulate done\n");
	return err;
}


void ceph_inode_set_size(struct inode *inode, loff_t size)
{
	struct ceph_inode_info *ci = ceph_inode(inode);

	spin_lock(&inode->i_lock);
	dout(30, "set_size %p %llu -> %llu\n", inode, inode->i_size, size);
	inode->i_size = size;
	inode->i_blocks = (size + (1 << 9) - 1) >> 9;

	/* tell the MDS if we are approaching max_size */
	if ((size << 1) >= ci->i_max_size &&
	    (ci->i_reported_size << 1) < ci->i_max_size) {
		spin_unlock(&inode->i_lock);
		ceph_check_caps(ci, 0, 0, NULL);
	} else {
		spin_unlock(&inode->i_lock);
	}
}

/*
 * Write back inode data in a worker thread.  (This can't be done
 * in the message handler context.)
 */
void ceph_inode_writeback(struct work_struct *work)
{
	struct ceph_inode_info *ci = container_of(work, struct ceph_inode_info,
						  i_wb_work);
	struct inode *inode = &ci->vfs_inode;

	dout(10, "writeback %p\n", inode);
	filemap_fdatawrite(&inode->i_data);
	iput(inode);
}

/*
 * Invalidate inode pages in a worker thread.  (This can't be done
 * in the message handler context.)
 */
static void ceph_inode_invalidate_pages(struct work_struct *work)
{
	struct ceph_inode_info *ci = container_of(work, struct ceph_inode_info,
						  i_pg_inv_work);
	struct inode *inode = &ci->vfs_inode;
	u32 orig_gen;
	int check = 0;

	spin_lock(&inode->i_lock);
	dout(10, "invalidate_pages %p gen %d revoking %d\n", inode,
	     ci->i_rdcache_gen, ci->i_rdcache_revoking);
	if (ci->i_rdcache_gen == 0 ||
	    ci->i_rdcache_revoking != ci->i_rdcache_gen) {
		BUG_ON(ci->i_rdcache_revoking > ci->i_rdcache_gen);
		/* nevermind! */
		ci->i_rdcache_revoking = 0;
		spin_unlock(&inode->i_lock);
		goto out;
	}
	orig_gen = ci->i_rdcache_gen;
	spin_unlock(&inode->i_lock);

	truncate_inode_pages(&inode->i_data, 0);

	spin_lock(&inode->i_lock);
	if (orig_gen == ci->i_rdcache_gen) {
		dout(10, "invalidate_pages %p gen %d successful\n", inode,
		     ci->i_rdcache_gen);
		ci->i_rdcache_gen = 0;
		ci->i_rdcache_revoking = 0;
		check = 1;
	} else {
		dout(10, "invalidate_pages %p gen %d raced, gen now %d\n",
		     inode, orig_gen, ci->i_rdcache_gen);
	}
	spin_unlock(&inode->i_lock);

	if (check)
		ceph_check_caps(ci, 0, 0, NULL);
out:
	iput(inode);
}


/*
 * called by trunc_wq; take i_mutex ourselves
 *
 * We also truncation in a separate thread as well.
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
	iput(inode);
}

/*
 * called with i_mutex held.
 *
 * Make sure any pending truncation is applied before doing anything
 * that may depend on it.
 */
void __ceph_do_pending_vmtruncate(struct inode *inode)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	u64 to;
	int wrbuffer_refs, wake = 0;

	spin_lock(&inode->i_lock);
	if (ci->i_truncate_pending == 0) {
		dout(10, "__do_pending_vmtruncate %p none pending\n", inode);
		spin_unlock(&inode->i_lock);
		return;
	}
	to = ci->i_truncate_size;
	wrbuffer_refs = ci->i_wrbuffer_ref;
	dout(10, "__do_pending_vmtruncate %p (%d) to %lld\n", inode,
	     ci->i_truncate_pending, to);
	spin_unlock(&inode->i_lock);

	truncate_inode_pages(inode->i_mapping, to);

	spin_lock(&inode->i_lock);
	ci->i_truncate_pending--;
	if (ci->i_truncate_pending == 0)
		wake = 1;
	spin_unlock(&inode->i_lock);

	if (wrbuffer_refs == 0)
		ceph_check_caps(ci, 0, 0, NULL);
	if (wake)
		wake_up(&ci->i_cap_wq);
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

static const struct inode_operations ceph_symlink_iops = {
	.readlink = generic_readlink,
	.follow_link = ceph_sym_follow_link,
};

/*
 * setattr
 */
static int ceph_setattr_chown(struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct inode *parent_inode = dentry->d_parent->d_inode;
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	const unsigned int ia_valid = attr->ia_valid;
	struct ceph_mds_request *req;
	int err;
	int mask = 0;

	spin_lock(&inode->i_lock);
	if (__ceph_caps_issued(ci, NULL) & CEPH_CAP_AUTH_EXCL) {
		dout(10, "chown holding auth EXCL, doing locally\n");
		if (ia_valid & ATTR_UID)
			inode->i_uid = attr->ia_uid;
		if (ia_valid & ATTR_GID)
			inode->i_gid = attr->ia_gid;
		inode->i_ctime = CURRENT_TIME;
		ci->i_dirty_caps |= CEPH_CAP_AUTH_EXCL;
		spin_unlock(&inode->i_lock);
		return 0;
	}
	spin_unlock(&inode->i_lock);

	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LCHOWN, USE_AUTH_MDS);
	if (IS_ERR(req))
		return PTR_ERR(req);
	req->r_inode = igrab(inode);
	if (ia_valid & ATTR_UID) {
		req->r_args.chown.uid = cpu_to_le32(attr->ia_uid);
		mask |= CEPH_CHOWN_UID;
	}
	if (ia_valid & ATTR_GID) {
		req->r_args.chown.gid = cpu_to_le32(attr->ia_gid);
		mask |= CEPH_CHOWN_GID;
	}
	req->r_args.chown.mask = cpu_to_le32(mask);
	ceph_release_caps(inode, CEPH_CAP_AUTH_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, parent_inode, req);
	ceph_mdsc_put_request(req);
	dout(10, "chown result %d\n", err);
	return err;
}

static int ceph_setattr_chmod(struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = dentry->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct inode *parent_inode = dentry->d_parent->d_inode;
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	int err;

	spin_lock(&inode->i_lock);
	if (__ceph_caps_issued(ci, NULL) & CEPH_CAP_AUTH_EXCL) {
		dout(10, "chmod holding auth EXCL, doing locally\n");
		inode->i_mode = attr->ia_mode;
		inode->i_ctime = CURRENT_TIME;
		ci->i_dirty_caps |= CEPH_CAP_AUTH_EXCL;
		spin_unlock(&inode->i_lock);
		return 0;
	}
	spin_unlock(&inode->i_lock);

	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LCHMOD, USE_AUTH_MDS);
	if (IS_ERR(req))
		return PTR_ERR(req);
	req->r_inode = igrab(inode);
	req->r_args.chmod.mode = cpu_to_le32(attr->ia_mode);
	ceph_release_caps(inode, CEPH_CAP_AUTH_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, parent_inode, req);
	ceph_mdsc_put_request(req);
	dout(10, "chmod result %d\n", err);
	return err;
}

static int ceph_setattr_time(struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = dentry->d_inode;
	struct inode *parent_inode = dentry->d_parent->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	const unsigned int ia_valid = attr->ia_valid;
	struct ceph_mds_request *req;
	int err;
	int issued;

	spin_lock(&inode->i_lock);
	issued = __ceph_caps_issued(ci, NULL);

	/* if i hold CAP_EXCL, i can change [am]time any way i like */
	if (issued & CEPH_CAP_FILE_EXCL) {
		dout(10, "utime holding EXCL, doing locally\n");
		ci->i_time_warp_seq++;
		if (ia_valid & ATTR_ATIME)
			inode->i_atime = attr->ia_atime;
		if (ia_valid & ATTR_MTIME)
			inode->i_mtime = attr->ia_mtime;
		inode->i_ctime = CURRENT_TIME;
		ci->i_dirty_caps |= CEPH_CAP_FILE_EXCL;
		spin_unlock(&inode->i_lock);
		return 0;
	}

	/* if i hold CAP_WR, i can _increase_ [am]time safely */
	if ((issued & CEPH_CAP_FILE_WR) &&
	    ((ia_valid & ATTR_MTIME) == 0 ||
	     timespec_compare(&inode->i_mtime, &attr->ia_mtime) < 0) &&
	    ((ia_valid & ATTR_ATIME) == 0 ||
	     timespec_compare(&inode->i_atime, &attr->ia_atime) < 0)) {
		dout(10, "utime holding WR, doing [am]time increase locally\n");
		if (ia_valid & ATTR_ATIME)
			inode->i_atime = attr->ia_atime;
		if (ia_valid & ATTR_MTIME)
			inode->i_mtime = attr->ia_mtime;
		inode->i_ctime = CURRENT_TIME;
		ci->i_dirty_caps |= CEPH_CAP_FILE_WR;
		spin_unlock(&inode->i_lock);
		return 0;
	}

	/* if i have valid values, this may be a no-op */
	if ((issued & CEPH_CAP_FILE_RDCACHE) &&
	    !(((ia_valid & ATTR_ATIME) &&
	       !timespec_equal(&inode->i_atime, &attr->ia_atime)) ||
	      ((ia_valid & ATTR_MTIME) &&
	       !timespec_equal(&inode->i_mtime, &attr->ia_mtime)))) {
		dout(10, "lease indicates utimes is a no-op\n");
		spin_unlock(&inode->i_lock);
		return 0;
	}

	spin_unlock(&inode->i_lock);

	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LUTIME, USE_AUTH_MDS);
	if (IS_ERR(req))
		return PTR_ERR(req);
	req->r_inode = igrab(inode);
	ceph_encode_timespec(&req->r_args.utime.mtime, &attr->ia_mtime);
	ceph_encode_timespec(&req->r_args.utime.atime, &attr->ia_atime);

	req->r_args.utime.mask = 0;
	if (ia_valid & ATTR_ATIME)
		req->r_args.utime.mask |= cpu_to_le32(CEPH_UTIME_ATIME);
	if (ia_valid & ATTR_MTIME)
		req->r_args.utime.mask |= cpu_to_le32(CEPH_UTIME_MTIME);

	ceph_release_caps(inode, CEPH_CAP_FILE_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, parent_inode, req);
	ceph_mdsc_put_request(req);
	dout(10, "utime result %d\n", err);
	return err;
}

static int ceph_setattr_size(struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = dentry->d_inode;
	struct inode *parent_inode = dentry->d_parent->d_inode;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_client *client = ceph_sb_to_client(inode->i_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	int err;
	int issued;

	dout(10, "truncate: ia_size %d i_size %d\n",
	     (int)attr->ia_size, (int)inode->i_size);

	spin_lock(&inode->i_lock);
	issued = __ceph_caps_issued(ci, NULL);

	if ((issued & CEPH_CAP_FILE_EXCL) &&
	    attr->ia_size > inode->i_size) {
		dout(10, "holding EXCL, doing truncate (fwd) locally\n");
		err = vmtruncate(inode, attr->ia_size);
		if (!err) {
			inode->i_size = attr->ia_size;
			inode->i_blocks = (attr->ia_size + (1 << 9) - 1) >> 9;
			inode->i_ctime = attr->ia_ctime;
			ci->i_reported_size = attr->ia_size;
		}
		spin_unlock(&inode->i_lock);
		return err;
	}
	if ((issued & CEPH_CAP_FILE_RDCACHE) &&
	    attr->ia_size == inode->i_size) {
		dout(10, "lease indicates truncate is a no-op\n");
		spin_unlock(&inode->i_lock);
		return 0;
	}
	spin_unlock(&inode->i_lock);

	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LTRUNCATE,
				       USE_AUTH_MDS);
	if (IS_ERR(req))
		return PTR_ERR(req);
	req->r_inode = igrab(inode);
	req->r_args.truncate.length = cpu_to_le64(attr->ia_size);
	req->r_args.truncate.old_length = cpu_to_le64(inode->i_size);
	//ceph_release_caps(inode, CEPH_CAP_FILE_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, parent_inode, req);
	ceph_mdsc_put_request(req);
	dout(10, "truncate result %d\n", err);
	__ceph_do_pending_vmtruncate(inode);
	return err;
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
		     inode->i_gid, attr->ia_gid);
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

	if (ia_valid & (ATTR_UID|ATTR_GID))
		err = ceph_setattr_chown(dentry, attr);
	if (ia_valid & ATTR_MODE)
		err = ceph_setattr_chmod(dentry, attr);
	if (ia_valid & (ATTR_ATIME|ATTR_MTIME))
		err = ceph_setattr_time(dentry, attr);
	if (ia_valid & ATTR_SIZE)
		err = ceph_setattr_size(dentry, attr);
	return err;
}

/*
 * Verify that we have a lease on the given mask.  If not,
 * do a getattr against an mds.
 */
int ceph_do_getattr(struct dentry *dentry, int mask)
{
	struct ceph_client *client = ceph_sb_to_client(dentry->d_sb);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct inode *inode = dentry->d_inode;
	struct ceph_mds_request *req;
	int err;

	if (ceph_snap(inode) == CEPH_SNAPDIR) {
		dout(30, "do_getattr dentry %p inode %p SNAPDIR\n", dentry,
		     inode);
		return 0;
	}

	dout(30, "do_getattr dentry %p inode %p mask %s\n", dentry,
	     inode, ceph_cap_string(mask));
	if (ceph_caps_issued_mask(ceph_inode(inode), mask))
		return 0;

	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LSTAT, USE_ANY_MDS);
	if (IS_ERR(req))
		return PTR_ERR(req);
	req->r_inode = igrab(inode);
	req->r_args.stat.mask = cpu_to_le32(mask);
	err = ceph_mdsc_do_request(mdsc, NULL, req);
	ceph_mdsc_put_request(req);  /* will dput(dentry) */
	dout(20, "do_getattr result=%d\n", err);
	return err;
}

/*
 * Get all attributes.  Hopefully somedata we'll have a statlite()
 * and can limit the fields we require to be accurate.
 */
int ceph_getattr(struct vfsmount *mnt, struct dentry *dentry,
		 struct kstat *stat)
{
	int err;

	err = ceph_do_getattr(dentry, CEPH_STAT_CAP_INODE_ALL);
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

/*
 * (virtual) xattrs
 *
 * These define virtual xattrs exposing the recursive directory statistics.
 */
struct _ceph_vir_xattr_cb {
	char *name;
	size_t (*getxattr_cb)(struct ceph_inode_info *ci, char *val,
			      size_t size);
};

static size_t _ceph_vir_xattrcb_entries(struct ceph_inode_info *ci, char *val,
					size_t size)
{
	return snprintf(val, size, "%lld", ci->i_files + ci->i_subdirs);
}

static size_t _ceph_vir_xattrcb_files(struct ceph_inode_info *ci, char *val,
				      size_t size)
{
	return snprintf(val, size, "%lld", ci->i_files);
}

static size_t _ceph_vir_xattrcb_subdirs(struct ceph_inode_info *ci, char *val,
					size_t size)
{
	return snprintf(val, size, "%lld", ci->i_subdirs);
}

static size_t _ceph_vir_xattrcb_rentries(struct ceph_inode_info *ci, char *val,
					 size_t size)
{
	return snprintf(val, size, "%lld", ci->i_rfiles + ci->i_rsubdirs);
}

static size_t _ceph_vir_xattrcb_rfiles(struct ceph_inode_info *ci, char *val,
				       size_t size)
{
	return snprintf(val, size, "%lld", ci->i_rfiles);
}

static size_t _ceph_vir_xattrcb_rsubdirs(struct ceph_inode_info *ci, char *val,
					 size_t size)
{
	return snprintf(val, size, "%lld", ci->i_rsubdirs);
}

static size_t _ceph_vir_xattrcb_rbytes(struct ceph_inode_info *ci, char *val,
				       size_t size)
{
	return snprintf(val, size, "%lld", ci->i_rbytes);
}

static size_t _ceph_vir_xattrcb_rctime(struct ceph_inode_info *ci, char *val,
				       size_t size)
{
	return snprintf(val, size, "%ld.%ld", (long)ci->i_rctime.tv_sec,
			(long)ci->i_rctime.tv_nsec);
}

static struct _ceph_vir_xattr_cb _ceph_vir_xattr_recs[] = {
	{ "user.ceph.dir.entries", _ceph_vir_xattrcb_entries},
	{ "user.ceph.dir.files", _ceph_vir_xattrcb_files},
	{ "user.ceph.dir.subdirs", _ceph_vir_xattrcb_subdirs},
	{ "user.ceph.dir.rentries", _ceph_vir_xattrcb_rentries},
	{ "user.ceph.dir.rfiles", _ceph_vir_xattrcb_rfiles},
	{ "user.ceph.dir.rsubdirs", _ceph_vir_xattrcb_rsubdirs},
	{ "user.ceph.dir.rbytes", _ceph_vir_xattrcb_rbytes},
	{ "user.ceph.dir.rctime", _ceph_vir_xattrcb_rctime},
	{ NULL, NULL }
};

static struct _ceph_vir_xattr_cb *_ceph_match_vir_xattr(const char *name)
{
	struct _ceph_vir_xattr_cb *xattr_rec = _ceph_vir_xattr_recs;

	do {
		if (strcmp(xattr_rec->name, name) == 0)
			return xattr_rec;
		xattr_rec++;
	} while (xattr_rec->name);

	return NULL;
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
	struct _ceph_vir_xattr_cb *vir_xattr;

	/* let's see if a virtual xattr was requested */
	vir_xattr = _ceph_match_vir_xattr(name);
	if (vir_xattr)
		return (vir_xattr->getxattr_cb)(ci, value, size);

	/* get xattrs from mds (if we don't already have them) */
	err = ceph_do_getattr(dentry, CEPH_STAT_CAP_XATTR);
	if (err)
		return err;

	spin_lock(&inode->i_lock);

	err = -ENODATA;  /* == ENOATTR */
	if (ci->i_xattr_len <= 4)
		goto out;

	/* find attr name */
	p = ci->i_xattr_data;
	end = p + ci->i_xattr_len;
	ceph_decode_32_safe(&p, end, numattr, bad);
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
	u32 len;
	int i;

	err = ceph_do_getattr(dentry, CEPH_STAT_CAP_XATTR);
	if (err)
		return err;

	spin_lock(&inode->i_lock);

	/* measure len of names */
	if (ci->i_xattr_len > 4) {
		p = ci->i_xattr_data;
		end = p + ci->i_xattr_len;
		ceph_decode_32_safe(&p, end, numattr, bad);
		while (numattr--) {
			ceph_decode_32_safe(&p, end, len, bad);
			namelen += len + 1;
			p += len;
			ceph_decode_32_safe(&p, end, len, bad);
			p += len;
		}
	} else {
		namelen = 0;
	}

	/* include virtual dir xattrs */
	if ((inode->i_mode & S_IFMT) == S_IFDIR)
		for (i = 0; _ceph_vir_xattr_recs[i].name; i++)
			namelen += strlen(_ceph_vir_xattr_recs[i].name) + 1;

	err = -ERANGE;
	if (size && namelen > size)
		goto out;
	err = namelen;
	if (size == 0)
		goto out;

	/* copy names */
	if (ci->i_xattr_len> 4) {
		p = ci->i_xattr_data;
		ceph_decode_32(&p, numattr);
		while (numattr--) {
			ceph_decode_32(&p, len);
			memcpy(names, p, len);
			names[len] = '\0';
			names += len + 1;
			p += len;
			ceph_decode_32(&p, len);
			p += len;
		}
	} else {
		names[0] = 0;
	}

	/* virtual xattr names, too */
	if ((inode->i_mode & S_IFMT) == S_IFDIR)
		for (i = 0; _ceph_vir_xattr_recs[i].name; i++) {
			len = sprintf(names, "%s",
				      _ceph_vir_xattr_recs[i].name);
			names += len + 1;
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

int ceph_setxattr(struct dentry *dentry, const char *name,
		  const void *value, size_t size, int flags)
{
	struct ceph_client *client = ceph_client(dentry->d_sb);
	struct inode *inode = dentry->d_inode;
	struct inode *parent_inode = dentry->d_parent->d_inode;
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req;
	int err;
	int i, nr_pages;
	struct page **pages = NULL;
	void *kaddr;

	if (ceph_snap(inode) != CEPH_NOSNAP)
		return -EROFS;

	/* only support user.* xattrs, for now */
	if (strncmp(name, "user.", 5) != 0)
		return -EOPNOTSUPP;

	if (_ceph_match_vir_xattr(name) != NULL)
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
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LSETXATTR,
				       USE_AUTH_MDS);
	if (IS_ERR(req))
		return PTR_ERR(req);
	req->r_inode = igrab(inode);
	req->r_args.setxattr.flags = cpu_to_le32(flags);

	req->r_request->pages = pages;
	req->r_request->nr_pages = nr_pages;
	req->r_request->hdr.data_len = cpu_to_le32(size);
	req->r_request->hdr.data_off = cpu_to_le16(0);

	ceph_release_caps(inode, CEPH_CAP_XATTR_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, parent_inode, req);
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
	struct inode *parent_inode = dentry->d_parent->d_inode;
	struct ceph_mds_request *req;
	int err;

	if (ceph_snap(inode) != CEPH_NOSNAP)
		return -EROFS;

	/* only support user.* xattrs, for now */
	if (strncmp(name, "user.", 5) != 0)
		return -EOPNOTSUPP;

	if (_ceph_match_vir_xattr(name) != NULL)
		return -EOPNOTSUPP;

	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LRMXATTR,
				       USE_AUTH_MDS);
	if (IS_ERR(req))
		return PTR_ERR(req);
	req->r_inode = igrab(inode);
	ceph_release_caps(inode, CEPH_CAP_XATTR_RDCACHE);
	err = ceph_mdsc_do_request(mdsc, parent_inode, req);
	ceph_mdsc_put_request(req);
	return err;
}

