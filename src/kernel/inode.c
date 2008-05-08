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
struct inode *ceph_get_inode(struct super_block *sb, __u64 ino)
{
	struct inode *inode;
	struct ceph_inode_info *ci;
	ino_t inot;

	inot = ceph_ino_to_ino(ino);
#if BITS_PER_LONG == 64
	inode = iget_locked(sb, ino);
#else
	inode = iget5_locked(sb, inot, ceph_ino_compare, ceph_set_ino_cb, &ino);
#endif
	if (inode == NULL)
		return ERR_PTR(-ENOMEM);
	if (inode->i_state & I_NEW) {
		dout(40, "get_inode created new inode %p %llx\n", inode,
		     ceph_ino(inode));
		unlock_new_inode(inode);
	}

	ci = ceph_inode(inode);
	ceph_set_ino(inode, ino);
	ci->i_hashval = inode->i_ino;

	dout(30, "get_inode on %lu=%llx got %p\n", inode->i_ino, ino, inode);
	return inode;
}


const struct inode_operations ceph_file_iops = {
	.setattr = ceph_setattr,
	.getattr = ceph_getattr,
};

const struct inode_operations ceph_special_iops = {
	.setattr = ceph_setattr,
	.getattr = ceph_getattr,
};


/*
 * populate an inode based on info from mds.
 * may be called on new or existing inodes.
 */
int ceph_fill_inode(struct inode *inode, struct ceph_mds_reply_inode *info)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int i;
	int symlen;
	int issued;
	struct timespec mtime, atime, ctime;
	u64 size = le64_to_cpu(info->size);
	u32 su = le32_to_cpu(info->layout.fl_stripe_unit);
	int blkbits = fls(su) - 1;
	u64 blocks = (size + (1<<9) - 1) >> 9;
	u64 time_warp_seq;

	dout(30, "fill_inode %p ino %llx by %d.%d sz=%llu mode 0%o nlink %d\n",
	     inode, info->ino, inode->i_uid, inode->i_gid,
	     inode->i_size, inode->i_mode, inode->i_nlink);

	spin_lock(&inode->i_lock);
	dout(30, " su %d, blkbits %d, blocks %llu.  v %llu, had %llu\n",
	     su, blkbits, blocks, le64_to_cpu(info->version), ci->i_version);
	if (le64_to_cpu(info->version) > 0 &&
	    ci->i_version == le64_to_cpu(info->version))
		goto no_change;
	ci->i_version = le64_to_cpu(info->version);
	inode->i_mode = le32_to_cpu(info->mode);
	inode->i_uid = le32_to_cpu(info->uid);
	inode->i_gid = le32_to_cpu(info->gid);
	inode->i_nlink = le32_to_cpu(info->nlink);
	inode->i_rdev = le32_to_cpu(info->rdev);

	/* be careful with mtime, atime, size */
	ceph_decode_timespec(&atime, &info->atime);
	ceph_decode_timespec(&mtime, &info->mtime);
	ceph_decode_timespec(&ctime, &info->ctime);
	time_warp_seq = le64_to_cpu(info->time_warp_seq);

	issued = __ceph_caps_issued(ci);
	if (issued & CEPH_CAP_EXCL) {
		if (timespec_compare(&ctime, &inode->i_ctime) > 0)
			inode->i_ctime = ctime;
		if (time_warp_seq > ci->i_time_warp_seq)
			derr(0, "WARNING: %p mds time_warp_seq %llu > %llu\n",
			     inode, time_warp_seq, ci->i_time_warp_seq);
	} else if (issued & (CEPH_CAP_WR|CEPH_CAP_WRBUFFER)) {
		if (size > inode->i_size) {
			inode->i_size = size;
			inode->i_blkbits = blkbits;
			inode->i_blocks = blocks;
		}
		if (time_warp_seq >= ci->i_time_warp_seq) {
			if (timespec_compare(&mtime, &inode->i_mtime) > 0)
				inode->i_mtime = mtime;
			if (timespec_compare(&atime, &inode->i_atime) > 0)
				inode->i_atime = atime;
			if (timespec_compare(&ctime, &inode->i_ctime) > 0)
				inode->i_ctime = ctime;
			ci->i_time_warp_seq = time_warp_seq;
		} else
			dout(10, " mds time_warp_seq %llu < %llu\n",
			     time_warp_seq, ci->i_time_warp_seq);
	} else {
		if (timespec_compare(&mtime, &inode->i_mtime) != 0)
			dout(10, "%p issued %d .. full update, "
			     "mtime %lu.%lu -> %lu.%lu\n", inode, issued,
			     inode->i_mtime.tv_sec, inode->i_mtime.tv_nsec,
			     mtime.tv_sec, mtime.tv_nsec);
		inode->i_size = size;
		inode->i_blkbits = blkbits;
		inode->i_blocks = blocks;
		if (time_warp_seq >= ci->i_time_warp_seq) {
			inode->i_mtime = mtime;
			inode->i_atime = atime;
			inode->i_ctime = ctime;
			ci->i_time_warp_seq = time_warp_seq;
		} else
			dout(10, " mds time_warp_seq %llu < %llu\n",
			     time_warp_seq, ci->i_time_warp_seq);
	}

	/* ceph inode */
	ci->i_layout = info->layout;
	kfree(ci->i_symlink);
	ci->i_symlink = 0;

	if (le32_to_cpu(info->fragtree.nsplits) > 0) {
		/*ci->i_fragtree = kmalloc(...); */
		BUG_ON(1); /* write me */
	}
	ci->i_fragtree->nsplits = le32_to_cpu(info->fragtree.nsplits);
	for (i = 0; i < ci->i_fragtree->nsplits; i++)
		ci->i_fragtree->splits[i] =
			le32_to_cpu(info->fragtree.splits[i]);

	ci->i_frag_map_nr = 1;
	ci->i_frag_map[0].frag = 0;
	ci->i_frag_map[0].mds = 0; /* FIXME */

	ci->i_old_atime = inode->i_atime;

	ci->i_max_size = le64_to_cpu(info->max_size);
	ci->i_reported_size = inode->i_size;  /* reset */

	inode->i_mapping->a_ops = &ceph_aops;

no_change:
	spin_unlock(&inode->i_lock);

	if (ci->i_hashval != inode->i_ino) {
		insert_inode_hash(inode);
		ci->i_hashval = inode->i_ino;
	}

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
		symlen = le32_to_cpu(*(__u32 *)(info->fragtree.splits +
						ci->i_fragtree->nsplits));
		BUG_ON(symlen != ci->vfs_inode.i_size);
		ci->i_symlink = kmalloc(symlen+1, GFP_NOFS);
		if (ci->i_symlink == NULL)
			return -ENOMEM;
		memcpy(ci->i_symlink,
		       (char *)(info->fragtree.splits +
				ci->i_fragtree->nsplits) + 4,
		       symlen);
		ci->i_symlink[symlen] = 0;
		break;
	case S_IFDIR:
		inode->i_op = &ceph_dir_iops;
		inode->i_fop = &ceph_dir_fops;
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
void ceph_update_inode_lease(struct inode *inode,
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
		return;

	spin_lock(&inode->i_lock);
	/*
	 * be careful: we can't remove lease from a different session
	 * without holding that other session's s_mutex.  so don't.
	 */
	if ((ci->i_lease_ttl == 0 || !time_before(ttl, ci->i_lease_ttl)) &&
	    (!ci->i_lease_session || ci->i_lease_session == session)) {
		ci->i_lease_ttl = ttl;
		ci->i_lease_mask = mask;
		if (!ci->i_lease_session) {
			ci->i_lease_session = session;
			is_new = 1;
		}
		list_move_tail(&ci->i_lease_item, &session->s_inode_leases);
	}
	spin_unlock(&inode->i_lock);
	if (is_new) {
		dout(10, "lease iget on %p\n", inode);
		igrab(inode);
	}
}

/*
 * check if inode lease is valid for a given mask
 */
int ceph_inode_lease_valid(struct inode *inode, int mask)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int havemask;
	int valid;
	int ret = 0;

	spin_lock(&inode->i_lock);
	havemask = ci->i_lease_mask;
	/* EXCL cap counts for an ICONTENT lease */
	if (__ceph_caps_issued(ci) & CEPH_CAP_EXCL) {
		dout(20, "lease_valid inode %p EXCL cap -> ICONTENT\n", inode);
		havemask |= CEPH_LOCK_ICONTENT;
	}
	/* any ICONTENT bits imply all ICONTENT bits */
	if (havemask & CEPH_LOCK_ICONTENT)
		havemask |= CEPH_LOCK_ICONTENT;

	valid = time_before(jiffies, ci->i_lease_ttl);
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
void ceph_update_dentry_lease(struct dentry *dentry,
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
	if (lease->mask == 0)
		return;

	spin_lock(&dentry->d_lock);
	if (dentry->d_time != 0 && time_before(ttl, dentry->d_time))
		goto fail_unlock;  /* we already have a newer lease. */

	di = ceph_dentry(dentry);
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
	int valid = 0;
	spin_lock(&dentry->d_lock);
	di = ceph_dentry(dentry);
	if (di && time_before(jiffies, dentry->d_time))
		valid = 1;
	spin_unlock(&dentry->d_lock);
	dout(20, "dentry_lease_valid - dentry %p = %d\n", dentry, valid);
	return valid;
}




int ceph_fill_trace(struct super_block *sb, struct ceph_mds_request *req,
		    struct ceph_mds_session *session)
{
	struct ceph_mds_reply_info *rinfo = &req->r_reply_info;
	int err = 0;
	struct qstr dname;
	struct dentry *dn = sb->s_root, *realdn;
	struct dentry *parent = NULL;
	struct inode *in;
	struct ceph_mds_reply_inode *ininfo;
	int d = 0;
	u64 ino;

	if (rinfo->trace_numi == 0) {
		dout(10, "fill_trace reply has empty trace!\n");
		return 0;
	}

	ino = le64_to_cpu(rinfo->trace_in[0].in->ino);
	if (dn) {
		in = dn->d_inode;
		/* trace should start at root, or have only 1 dentry
		 * (if it is in mds stray dir) */
		WARN_ON(ino != 1 && rinfo->trace_numd != 1);
	} else {
		/* first reply (i.e. mount) */
		in = ceph_get_inode(sb, ino);
		if (IS_ERR(in))
			return PTR_ERR(in);
		dn = d_alloc_root(in);
		if (dn == NULL) {
			derr(0, "d_alloc_root enomem badness on root dentry\n");
			return -ENOMEM;
		}
	}

	if (ino == 1) {
		err = ceph_fill_inode(in, rinfo->trace_in[0].in);
		if (err < 0)
			return err;
		ceph_update_inode_lease(in, rinfo->trace_ilease[0], session,
					req->r_from_time);
		if (sb->s_root == NULL)
			sb->s_root = dn;
	}

	dget(dn);
	for (d = 0; d < rinfo->trace_numd; d++) {
		dname.name = rinfo->trace_dname[d];
		dname.len = rinfo->trace_dname_len[d];
		parent = dn;

		dout(10, "fill_trace %d/%d parent %p in %p d '%.*s'\n",
		     (d+1), rinfo->trace_numd, parent, parent->d_inode,
		     (int)dname.len, dname.name);

		/* dentry */
		dn = 0;
		if (d == rinfo->trace_numd-1 && req->r_last_dentry) {
			dn = req->r_last_dentry;
			dout(10, "fill_trace provided dn %p '%.*s'\n", dn,
			     dn->d_name.len, dn->d_name.name);
			ceph_init_dentry(dn);  /* just in case */
			req->r_last_dentry = NULL;

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
				dput(dn);  /* dn is dropped */
				dn = req->r_old_dentry;  /* use old_dentry */
				req->r_old_dentry = 0;
			}
			if (dn->d_parent != parent)
				dout(10, "warning: d_parent %p != %p\n",
				     dn->d_parent, parent);
		}
		
		if (!dn) {
			dname.hash = full_name_hash(dname.name, dname.len);
		retry_lookup:
			dn = d_lookup(parent, &dname);
			dout(10, "fill_trace d_lookup of '%.*s' got %p\n",
			     (int)dname.len, dname.name, dn);
			if (!dn) {
				dn = d_alloc(parent, &dname);
				if (!dn) {
					derr(0, "d_alloc enomem\n");
					err = -ENOMEM;
					break;
				}
				ceph_init_dentry(dn);
			}
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
			if (d_unhashed(dn))
				d_rehash(dn);
			in = 0;
			ceph_update_dentry_lease(dn, rinfo->trace_dlease[d],
						 session, req->r_from_time);
			break;
		}

		/* attach proper inode */
		ininfo = rinfo->trace_in[d+1].in;
		if (dn->d_inode) {
			if (ceph_ino(dn->d_inode) != le64_to_cpu(ininfo->ino)) {
				dout(10, "dn %p wrong %p ino %llx\n",
				     dn, dn->d_inode, ceph_ino(dn->d_inode));
				d_delete(dn);
				dput(dn);
				goto retry_lookup;
			}
			dout(10, "dn %p correct %p ino %llx\n",
			     dn, dn->d_inode, ceph_ino(dn->d_inode));
			in = dn->d_inode;
		} else {
			in = ceph_get_inode(dn->d_sb, le64_to_cpu(ininfo->ino));
			if (IS_ERR(in)) {
				derr(30, "get_inode badness\n");
				err = PTR_ERR(in);
				d_delete(dn);
				dn = NULL;
				in = NULL;
				break;
			}
			/* d_splice_alias wants dn unhashed */
			if (!d_unhashed(dn)) {
				dout(20, "d_drop %p\n", dn);
				d_drop(dn);
			}
			realdn = d_splice_alias(in, dn);
			if (realdn) {
				derr(10, "dn %p spliced with %p inode %p "
				     "ino %llx\n", dn, realdn, realdn->d_inode, 
				     ceph_ino(realdn->d_inode));
				if (dn == req->r_last_dentry) {
					dput(dn);
					req->r_last_dentry = realdn;
				}
				dn = realdn;
				ceph_init_dentry(dn);
			} else
				dout(10, "dn %p attached to %p ino %llx\n",
				     dn, dn->d_inode, ceph_ino(dn->d_inode));
		}
		if (dn->d_parent == parent)
			ceph_update_dentry_lease(dn, rinfo->trace_dlease[d],
						 session, req->r_from_time);
		else
			derr(10, "sloppy tailing dentry %p, not doing lease\n",
			     dn);
		BUG_ON(d_unhashed(dn));
		
		err = ceph_fill_inode(in, ininfo);
		if (err < 0) {
			derr(30, "ceph_fill_inode badness\n");
			iput(in);
			d_delete(dn);
			dn = NULL;
			break;
		}
		ceph_update_inode_lease(dn->d_inode, rinfo->trace_ilease[d+1],
					session, req->r_from_time);
		dput(parent);
		parent = NULL;
	}
	if (parent)
		dput(parent);

	dout(10, "fill_trace done, last dn %p in %p\n", dn, in);
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

	dout(10, "readdir_prepopulate %d items under dentry %p\n",
	     rinfo->dir_nr, parent);
	for (i = 0; i < rinfo->dir_nr; i++) {
		/* dentry */
		dname.name = rinfo->dir_dname[i];
		dname.len = rinfo->dir_dname_len[i];
		dname.hash = full_name_hash(dname.name, dname.len);

retry_lookup:
		dn = d_lookup(parent, &dname);
		dout(30, "calling d_lookup on parent=%p name=%.*s"
		     " returned %p\n", parent, dname.len, dname.name, dn);

		if (!dn) {
			dn = d_alloc(parent, &dname);
			dout(40, "d_alloc %p/%.*s\n", parent,
			     dname.len, dname.name);
			if (dn == NULL) {
				dout(30, "d_alloc badness\n");
				return -1;
			}
			ceph_init_dentry(dn);
		} else if (dn->d_inode &&
			   ceph_ino(dn->d_inode) !=
			   le64_to_cpu(rinfo->dir_in[i].in->ino)) {
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
			struct dentry *new;
			in = ceph_get_inode(parent->d_sb,
					    rinfo->dir_in[i].in->ino);
			if (in == NULL) {
				dout(30, "new_inode badness\n");
				d_delete(dn);
				return -1;
			}			
			if (!d_unhashed(dn))
				d_drop(dn);
			new = d_splice_alias(in, dn);
			if (new)
				dn = new;
		}
		BUG_ON(d_unhashed(dn));

		if (ceph_fill_inode(in, rinfo->dir_in[i].in) < 0) {
			dout(0, "ceph_fill_inode badness on %p\n", in);
			dput(dn);
			continue;
		}
		ceph_update_dentry_lease(dn, rinfo->dir_dlease[i],
					 req->r_session, req->r_from_time);
		ceph_update_inode_lease(in, rinfo->dir_ilease[i],
					req->r_session, req->r_from_time);
		dput(dn);
	}
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

/*
 * caller shoudl hold session s_mutex.
 */
struct ceph_inode_cap *ceph_add_cap(struct inode *inode,
				    struct ceph_mds_session *session,
				    int fmode,
				    u32 issued, u32 seq)
{
	int mds = session->s_mds;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_cap *cap, *new_cap = 0;
	int i;
	int is_new = 0;

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
					return ERR_PTR(-ENOMEM);
				spin_lock(&inode->i_lock);
			}
		}

		is_new = 1;    /* grab inode later */
		cap->issued = cap->implemented = 0;
		cap->mds = mds;
		cap->seq = 0;
		cap->flags = 0;

		cap->ci = ci;
		list_add(&cap->ci_caps, &ci->i_caps);

		/* add to session cap list */
		cap->session = session;
		list_add(&cap->session_caps, &session->s_caps);
		session->s_nr_caps++;
	}

	dout(10, "add_cap inode %p (%llx) got cap %xh now %xh seq %d from %d\n",
	     inode, ceph_ino(inode), issued, issued|cap->issued, seq, mds);
	cap->issued |= issued;
	cap->implemented |= issued;
	cap->seq = seq;
	cap->gen = session->s_cap_gen;
	__ceph_get_fmode(ci, fmode);
	spin_unlock(&inode->i_lock);
	if (is_new)
		igrab(inode);
	return cap;
}

int __ceph_caps_issued(struct ceph_inode_info *ci)
{
	int have = 0;
	struct ceph_inode_cap *cap;
	struct list_head *p;
	u64 gen;
	unsigned long ttl;

	list_for_each(p, &ci->i_caps) {
		cap = list_entry(p, struct ceph_inode_cap, ci_caps);

		spin_lock(&cap->session->s_cap_lock);
		gen = cap->session->s_cap_gen;
		ttl = cap->session->s_cap_ttl;
		spin_unlock(&cap->session->s_cap_lock);

		if (cap->gen < gen || time_after_eq(jiffies, ttl)) {
			dout(30, "__ceph_caps_issued %p cap %p issued %d "
			     "but STALE (gen %llu vs %llu)\n", &ci->vfs_inode,
			     cap, cap->issued, cap->gen, gen);
			continue;
		}
		dout(30, "__ceph_caps_issued %p cap %p issued %d\n",
		     &ci->vfs_inode, cap, cap->issued);
		have |= cap->issued;
	}
	return have;
}

/*
 * caller should hold i_lock and session s_mutex.
 */
void __ceph_remove_cap(struct ceph_inode_cap *cap)
{
	struct ceph_mds_session *session = cap->session;

	dout(20, "__ceph_remove_cap %p from %p\n", cap, &cap->ci->vfs_inode);

	/* remove from session list */
	list_del_init(&cap->session_caps);
	session->s_nr_caps--;

	/* remove from inode list */
	list_del_init(&cap->ci_caps);
	cap->session = 0;
	cap->mds = -1;  /* mark unused */

	if (cap < cap->ci->i_static_caps ||
	    cap >= cap->ci->i_static_caps + STATIC_CAPS)
		kfree(cap);
}

/*
 * caller should hold session s_mutex.
 */
void ceph_remove_cap(struct ceph_inode_cap *cap)
{
	struct inode *inode = &cap->ci->vfs_inode;

	spin_lock(&inode->i_lock);
	__ceph_remove_cap(cap);
	spin_unlock(&inode->i_lock);
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
void ceph_check_caps(struct ceph_inode_info *ci, int is_delayed)
{
	struct ceph_client *client = ceph_inode_to_client(&ci->vfs_inode);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct inode *inode = &ci->vfs_inode;
	struct ceph_inode_cap *cap;
	struct list_head *p;
	int wanted, used;
	struct ceph_mds_session *session = 0;  /* if non-NULL, i hold s_mutex */
	int removed_last;

retry:
	spin_lock(&inode->i_lock);
	wanted = __ceph_caps_wanted(ci);
	used = __ceph_caps_used(ci);
	dout(10, "check_caps %p wanted %d used %d issued %d\n", inode,
	     wanted, used, __ceph_caps_issued(ci));

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
						    used, wanted, !is_delayed);
		if (removed_last)
			goto out;
		goto retry;
	}

	/* okay */
	spin_unlock(&inode->i_lock);

out:
	if (session)
		mutex_unlock(&session->s_mutex);
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
		ceph_check_caps(ci, 0);
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

	if (last)
		ceph_check_caps(ci, 0);
}


/*
 * caller holds s_mutex.
 * return value:
 *  0 - ok
 *  1 - send the msg back to mds
 */
int ceph_handle_cap_grant(struct inode *inode, struct ceph_mds_file_caps *grant,
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
	u64 time_warp_seq;
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
		dout(10, "no cap on %p ino %llx from mds%d, ignoring\n",
		     inode, ci->i_ceph_ino, mds);
		goto out;
	}
	dout(10, " cap %p\n", cap);
	cap->gen = session->s_cap_gen;

	/* size change? */
	if (size > inode->i_size) {
		dout(10, "size %lld -> %llu\n", inode->i_size, size);
		inode->i_size = size;
	}

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

	/* ctime? */
	ceph_decode_timespec(&ctime, &grant->ctime);
	if (timespec_compare(&ctime, &inode->i_ctime) > 0) {
		dout(10, "ctime %lu.%09ld -> %lu.%.09ld\n",
		     ctime.tv_sec, ctime.tv_nsec,
		     inode->i_ctime.tv_sec, inode->i_ctime.tv_nsec);
		inode->i_ctime = ctime;
	}

	/* mtime/atime? */
	issued = __ceph_caps_issued(ci);
	time_warp_seq = le64_to_cpu(grant->time_warp_seq);
	if (issued & CEPH_CAP_EXCL) {
		if (time_warp_seq > ci->i_time_warp_seq)
			derr(0, "WARNING: %p mds time_warp_seq %llu > %llu\n",
			     inode, time_warp_seq, ci->i_time_warp_seq);
	} else {
		ceph_decode_timespec(&mtime, &grant->mtime);
		ceph_decode_timespec(&atime, &grant->atime);
		if (time_warp_seq >= ci->i_time_warp_seq) {
			if (timespec_compare(&mtime, &inode->i_mtime) > 0) {
				dout(10, "%p mtime %lu.%09ld -> %lu.%.09ld\n",
				     inode, mtime.tv_sec, mtime.tv_nsec,
				     inode->i_mtime.tv_sec,
				     inode->i_mtime.tv_nsec);
				inode->i_mtime = mtime;
			}
			if (timespec_compare(&atime, &inode->i_atime) > 0) {
				dout(10, "%p atime %lu.%09ld -> %lu.%09ld\n",
				     inode,
				     atime.tv_sec, atime.tv_nsec,
				     inode->i_atime.tv_sec,
				     inode->i_atime.tv_nsec);
				inode->i_atime = atime;
			}
			ci->i_time_warp_seq = time_warp_seq;
		} else
			dout(10, " mds time_warp_seq %llu < %llu\n",
			     time_warp_seq, ci->i_time_warp_seq);
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
			reply = 1;
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
		ceph_check_caps(ci, 0);
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
			ceph_check_caps(ci, 0);
	} else
		dout(10, "__do_pending_vmtruncate %p nothing to do\n", inode);
}

int ceph_handle_cap_trunc(struct inode *inode, struct ceph_mds_file_caps *trunc,
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
	return 0;
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
	int have;

	dout(30, "get_cap_refs on %p need %d want %d\n", &ci->vfs_inode,
	     need, want);
	spin_lock(&ci->vfs_inode.i_lock);
	if (endoff >= 0 && endoff > (loff_t)ci->i_max_size) {
		dout(20, "get_cap_refs endoff %llu > max_size %llu\n",
		     endoff, ci->i_max_size);
		goto sorry;
	}
	have = __ceph_caps_issued(ci);
	dout(30, "get_cap_refs have %d\n", have);
	if ((have & need) == need) {
		*got = need | (have & want);
		__take_cap_refs(ci, *got);
		ret = 1;
	}
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
		ceph_check_caps(ci, 0);
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
		ceph_check_caps(ci, 0);
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
	__u64 baseino = ceph_ino(dentry->d_inode->i_sb->s_root->d_inode);

	if (ia_valid & ATTR_FILE) {
		dout(5, "prepare_setattr dentry %p (inode %llx)\n", dentry,
		     ceph_ino(dentry->d_inode));
		req = ceph_mdsc_create_request(mdsc, op,
					       ceph_ino(dentry->d_inode), "",
					       0, 0);
	} else {
		dout(5, "prepare_setattr dentry %p (full path)\n", dentry);
		path = ceph_build_dentry_path(dentry, &pathlen);
		if (IS_ERR(path))
			return ERR_PTR(PTR_ERR(path));
		req = ceph_mdsc_create_request(mdsc, op, baseino, path, 0, 0);
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

int ceph_getattr(struct vfsmount *mnt, struct dentry *dentry,
		 struct kstat *stat)
{
	int err = 0;
	int mask = CEPH_STAT_MASK_INODE_ALL;

	dout(30, "getattr dentry %p inode %p\n", dentry,
	     dentry->d_inode);

	if (!ceph_inode_lease_valid(dentry->d_inode, mask)) {
		/*
		 * if the dentry is unhashed, stat the ino directly: we
		 * presumably have an open capability.
		 */
		struct dentry *ret =
			ceph_do_lookup(dentry->d_inode->i_sb, dentry, mask,
				       d_unhashed(dentry));
		if (IS_ERR(ret))
			return PTR_ERR(ret);
		if (ret)
			dentry = ret;
		if (!dentry->d_inode)
			return -ENOENT;
	}

	dout(30, "getattr returned %d\n", err);
	if (!err)
		generic_fillattr(dentry->d_inode, stat);
	return err;
}

