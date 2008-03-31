#include <linux/module.h>
#include <linux/fs.h>
#include <linux/smp_lock.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/uaccess.h>
#include <linux/kernel.h>
#include <linux/ceph_fs.h>
#include <linux/namei.h>

int ceph_inode_debug = 50;
#define DOUT_VAR ceph_inode_debug
#define DOUT_PREFIX "inode: "
#include "super.h"
#include "decode.h"

const struct inode_operations ceph_symlink_iops;

int ceph_get_inode(struct super_block *sb, __u64 ino, struct inode **pinode)
{
	struct ceph_inode_info *ci;
	ino_t inot;

	BUG_ON(pinode == NULL);

	inot = ceph_ino_to_ino(ino);
#if BITS_PER_LONG == 64
	*pinode = iget_locked(sb, ino);
#else
	*pinode = iget5_locked(sb, inot, ceph_ino_compare, ceph_set_ino_cb,
			       &ino);
#endif
	if (*pinode == NULL) 
		return -ENOMEM;
	if ((*pinode)->i_state & I_NEW)
		unlock_new_inode(*pinode);

	ci = ceph_inode(*pinode);
#if BITS_PER_LONG == 64
	ceph_set_ino(*pinode, ino);
#endif
	ci->i_hashval = (*pinode)->i_ino;

	dout(30, "get_inode on %lu=%llx got %p\n", (*pinode)->i_ino, ino, 
	     *pinode);
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
	int issued;
	struct timespec mtime, atime;
	u64 blocks = size + blksize - 1;
	do_div(blocks, blksize);

	dout(30, "fill_inode %p ino %llx by %d.%d sz=%llu mode %o nlink %d\n", 
	     inode, info->ino, inode->i_uid, inode->i_gid, 
	     inode->i_size, inode->i_mode, inode->i_nlink);
	dout(30, " su %d, blkbits %d, blksize %u, blocks %llu\n",
	     su, blkbits, blksize, blocks);	

	ceph_set_ino(inode, le64_to_cpu(info->ino));
	inode->i_mode = le32_to_cpu(info->mode);
	inode->i_uid = le32_to_cpu(info->uid);
	inode->i_gid = le32_to_cpu(info->gid);
	inode->i_nlink = le32_to_cpu(info->nlink);
	inode->i_rdev = le32_to_cpu(info->rdev);
	ceph_decode_timespec(&inode->i_ctime, &info->ctime);

	/* be careful with mtime, atime, size */
	ceph_decode_timespec(&atime, &info->atime);
	ceph_decode_timespec(&mtime, &info->mtime);
	issued = ceph_caps_issued(ci);
	spin_lock(&inode->i_lock);
	if (issued & (CEPH_CAP_WR|CEPH_CAP_WRBUFFER)) {
		if ((issued & CEPH_CAP_EXCL) == 0) {
			if (size > inode->i_size) {
				inode->i_size = size;
				inode->i_blkbits = blkbits;
				inode->i_blocks = blocks;
			}
			if (timespec_compare(&mtime, &inode->i_mtime) > 0)
				inode->i_mtime = mtime;
			if (timespec_compare(&atime, &inode->i_atime) > 0)
				inode->i_atime = atime;
		}
	} else {
		inode->i_size = size;
		inode->i_blkbits = blkbits;
		inode->i_blocks = blocks;
		inode->i_mtime = mtime;
		inode->i_atime = atime;
	}
	spin_unlock(&inode->i_lock);

	if (ci->i_hashval != inode->i_ino) {
		insert_inode_hash(inode);
		ci->i_hashval = inode->i_ino;
	}

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
		ci->i_fragtree->splits[i] = 
			le32_to_cpu(info->fragtree.splits[i]);

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

	return 0;
}

/* 
 * inode lease lock order is
 *     inode->i_lock
 *          session->s_cap_lock
 */
void ceph_update_inode_lease(struct inode *inode, 
			     struct ceph_mds_reply_lease *lease,
			     struct ceph_mds_session *session,
			     unsigned long from_time) 
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	__u64 ttl = le32_to_cpu(lease->duration_ms) * HZ;
	int is_new = 0;

	do_div(ttl, 1000);
	ttl += from_time;

	dout(10, "update_inode_lease %p mask %d duration %d ms ttl %llu\n",
	     inode, le16_to_cpu(lease->mask), le32_to_cpu(lease->duration_ms),
	     ttl);

	if (lease->mask == 0)
		return;

	spin_lock(&inode->i_lock);
	if (ttl > ci->i_lease_ttl) {
		ci->i_lease_ttl = ttl;
		ci->i_lease_mask = le16_to_cpu(lease->mask);
		if (ci->i_lease_session) {
			spin_lock(&ci->i_lease_session->s_cap_lock);
			list_del(&ci->i_lease_item);
			spin_unlock(&ci->i_lease_session->s_cap_lock);
		} else
			is_new = 1;
		ci->i_lease_session = session;
		spin_lock(&session->s_cap_lock);
		list_add(&ci->i_lease_item, &session->s_inode_leases);
		spin_unlock(&session->s_cap_lock);
	}
	spin_unlock(&inode->i_lock);
	if (is_new) {
		dout(10, "lease iget on %p\n", inode);
		igrab(inode);
	}
}

void ceph_revoke_inode_lease(struct ceph_inode_info *ci, int mask)
{
	int drop = 0;
	spin_lock(&ci->vfs_inode.i_lock);
	ci->i_lease_mask &= ~mask;
	if (ci->i_lease_mask == 0) {
		spin_lock(&ci->i_lease_session->s_cap_lock);
		list_del(&ci->i_lease_item);
		spin_unlock(&ci->i_lease_session->s_cap_lock);
		ci->i_lease_session = 0;
		drop = 1;
	}
	spin_unlock(&ci->vfs_inode.i_lock);
	if (drop) {
		dout(10, "lease iput on %p\n", &ci->vfs_inode);
		iput(&ci->vfs_inode);
	}
}

/*
 * dentry lease lock order is
 *     dentry->d_lock
 *         session->s_cap_lock
 */
void ceph_update_dentry_lease(struct dentry *dentry, 
			      struct ceph_mds_reply_lease *lease,
			      struct ceph_mds_session *session,
			      unsigned long from_time) 
{
	__u64 ttl = le32_to_cpu(lease->duration_ms) * HZ;
	struct ceph_dentry_info *di;
	int is_new = 0;

	do_div(ttl, 1000);
	ttl += from_time;

	dout(10, "update_dentry_lease %p mask %d duration %d ms ttl %llu\n",
	     dentry, le16_to_cpu(lease->mask), le32_to_cpu(lease->duration_ms),
	     ttl);
	if (lease->mask == 0)
		return;

	spin_lock(&dentry->d_lock);
	if (ttl < dentry->d_time) 
		goto fail_unlock;  /* older. */

	di = ceph_dentry(dentry);
	if (!di) {
		spin_unlock(&dentry->d_lock);
		di = kmalloc(sizeof(struct ceph_dentry_info),
			     GFP_KERNEL);
		if (!di)
			return;          /* oh well */
		spin_lock(&dentry->d_lock);
		if (dentry->d_fsdata) {  /* lost a race! */
			kfree(di);
			goto fail_unlock;
		}
		is_new = 1;
		di->dentry = dentry;
		dentry->d_fsdata = di;
	}

	/* update */
	dentry->d_time = ttl;

	/* (re)add to session lru */
	if (!is_new && di->lease_session) {
		spin_lock(&di->lease_session->s_cap_lock);
		list_del(&di->lease_item);
		spin_unlock(&di->lease_session->s_cap_lock);
	}
	di->lease_session = session;	      
	spin_lock(&session->s_cap_lock);
	list_add(&di->lease_item, &session->s_dentry_leases);
	spin_unlock(&session->s_cap_lock);
	
	spin_unlock(&dentry->d_lock);
	if (is_new) {
		dout(10, "lease dget on %p\n", dentry);
		dget(dentry);
	}
	return;

fail_unlock:
	spin_unlock(&dentry->d_lock);    
}

void ceph_revoke_dentry_lease(struct dentry *dentry)
{
	struct ceph_dentry_info *di;
	struct ceph_mds_session *session;
	int drop = 0;

	/* detach from dentry */
	spin_lock(&dentry->d_lock);
	di = ceph_dentry(dentry);
	if (di) {
		session = di->lease_session;
		spin_lock(&session->s_cap_lock);
		list_del(&di->lease_item);
		spin_unlock(&session->s_cap_lock);
		kfree(di);
		drop = 1;
		dentry->d_fsdata = 0;
	}
	spin_unlock(&dentry->d_lock);
	if (drop) {
		dout(10, "lease dput on %p\n", dentry);
		dput(dentry);
	}
}



int ceph_fill_trace(struct super_block *sb, struct ceph_mds_request *req, 
		    struct ceph_mds_session *session)
{
	struct ceph_mds_reply_info *rinfo = &req->r_reply_info;
	int err = 0;
	struct qstr dname;
	struct dentry *dn = sb->s_root;
	struct dentry *parent = NULL;
	struct inode *in;
	struct ceph_mds_reply_inode *ininfo;
	int d = 0;

	if (rinfo->trace_numi == 0) {
		dout(10, "fill_trace reply has empty trace!\n");
		return 0;
	}

	if (dn) {
		in = dn->d_inode;
	} else {
		/* first reply (i.e. mount) */
		err = ceph_get_inode(sb, 
				     le64_to_cpu(rinfo->trace_in[0].in->ino),
				     &in);
		if (err < 0) 
			return err;
		dn = d_alloc_root(in);
		if (dn == NULL) {
			derr(0, "d_alloc_root enomem badness on root dentry\n");
			return -ENOMEM;
		}
	}

	err = ceph_fill_inode(in, rinfo->trace_in[0].in);
	if (err < 0)
		return err;
	ceph_update_inode_lease(in, rinfo->trace_ilease[0], session, 
				req->r_from_time);

	if (sb->s_root == NULL)
		sb->s_root = dn;

	dget(dn);
	for (d = 0; d < rinfo->trace_numd; d++) {
		dout(10, "fill_trace dn %d/%d dn %p in %p\n", 
		     (d+1), rinfo->trace_numd,
		     dn, dn->d_inode);
		parent = dn;
		
		/* dentry */	       
		if (d == rinfo->trace_numd-1 &&
		    req->r_last_dentry) {
			dout(10, "fill_trace using provided dentry\n");
			dn = req->r_last_dentry;
			ceph_init_dentry(dn);  /* just in case */
			req->r_last_dentry = NULL;
		} else {
			dname.name = rinfo->trace_dname[d];
			dname.len = rinfo->trace_dname_len[d];
			dname.hash = full_name_hash(dname.name, dname.len);
			dn = d_lookup(parent, &dname);
			dout(10, "fill_trace d_lookup of '%.*s' got %p\n", 
			     (int)dname.len, dname.name, dn);
			if (!dn) {
				dout(10, "fill_trace calling d_alloc\n");
				dn = d_alloc(parent, &dname);
				if (!dn) {
					derr(0, "d_alloc enomem\n");
					err = -ENOMEM;
					break;
				}
				ceph_init_dentry(dn);
			}
		}
		ceph_update_dentry_lease(dn, rinfo->trace_dlease[d], 
					 session, req->r_from_time);

		/* inode */
		if (d+1 == rinfo->trace_numi) {
			dout(10, "fill_trace has dentry but no inode\n");
			if (dn->d_inode)
				d_delete(dn);  /* is this right? */
			else {
				d_instantiate(dn, NULL);
				if (d_unhashed(dn))
					d_rehash(dn);
			}
			break;
		}
		ininfo = rinfo->trace_in[d+1].in;

		if ((!dn->d_inode) ||
		    (ceph_ino(dn->d_inode) != ininfo->ino)) {
			dout(10, "fill_trace new_inode\n");
			in = new_inode(dn->d_sb);
			if (in == NULL) {
				dout(30, "new_inode badness\n");
				err = -ENOMEM;
				d_delete(dn);
				dn = NULL;
				break;
			}
			if ((err = ceph_fill_inode(in, ininfo)) < 0) {
				dout(30, "ceph_fill_inode badness\n");
				iput(in);
				d_delete(dn);
				dn = NULL;
				break;
			}
			dout(10, "fill_trace d_instantiate\n");
			d_instantiate(dn, in);
			if (d_unhashed(dn))
				d_rehash(dn);
			dout(10, "ceph_fill_trace added dentry %p"
			     " inode %llx\n", dn, ceph_ino(in));
		} else {
			in = dn->d_inode;
			if ((err = ceph_fill_inode(in, ininfo)) < 0) {
				dout(30, "ceph_fill_inode badness\n");
				break;
			}
		}
		ceph_update_inode_lease(dn->d_inode, rinfo->trace_ilease[d+1],
					session, req->r_from_time);
		dput(parent);
		parent = NULL;
	}
	if (parent)
		dput(parent);

	dout(10, "fill_trace done, last dn %p in %p\n", dn, in);
	if (req->r_last_dentry)
		dput(req->r_last_dentry);
	if (req->r_last_inode)
		iput(req->r_last_inode);
	req->r_last_dentry = dn;
	req->r_last_inode = in;
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
					 req->r_session, req->r_from_time);

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
			dout(10, "added dentry %p ino %llx %d/%d\n",
			     dn, ceph_ino(in), i, rinfo->dir_nr);
		} else {
			if (ceph_fill_inode(in, rinfo->dir_in[i].in) < 0) {
				dout(30, "ceph_fill_inode badness\n");
				return -1;
			}
		}
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

struct ceph_inode_cap *ceph_find_cap(struct inode *inode, int want)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_cap *cap;
	struct list_head *p;

	list_for_each(p, &ci->i_caps) {
		cap = list_entry(p, struct ceph_inode_cap, ci_caps);
		if ((cap->caps & want) == want) {
			dout(40, "find_cap found %p caps %d want %d\n", cap, 
			     cap->caps, want);
			return cap;
		}
	}
	return 0;
}

static struct ceph_inode_cap *get_cap_for_mds(struct inode *inode, int mds)
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


struct ceph_inode_cap *ceph_add_cap(struct inode *inode, struct ceph_mds_session *session, u32 caps, u32 seq)
{
	int mds = session->s_mds;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_cap *cap;
	int i;

	dout(10, "ceph_add_cap on %p mds%d cap %d seq %d\n", inode, session->s_mds, caps, seq);
	cap = get_cap_for_mds(inode, mds);
	if (!cap) {
		for (i=0; i<STATIC_CAPS; i++)
			if (ci->i_static_caps[i].mds == -1) {
				cap = &ci->i_static_caps[i];
				break;
			}
		if (!cap)
			cap = kmalloc(sizeof(*cap), GFP_KERNEL);
		if (cap == 0)
			return ERR_PTR(-ENOMEM);
		
		cap->caps = 0;
		cap->mds = mds;
		cap->seq = 0;
		cap->flags = 0;
		
		if (list_empty(&ci->i_caps))
			igrab(inode);
		cap->ci = ci;
		list_add(&cap->ci_caps, &ci->i_caps);

		cap->session = session;
		spin_lock(&session->s_cap_lock);
		list_add(&cap->session_caps, &session->s_caps);
		session->s_nr_caps++;
		spin_unlock(&session->s_cap_lock);
	}

	dout(10, "add_cap inode %p (%llx) got cap %xh now %xh seq %d from %d\n",
	     inode, ceph_ino(inode), caps, caps|cap->caps, seq, mds);
	cap->caps |= caps;
	cap->seq = seq;
	return cap;
}

int ceph_caps_issued(struct ceph_inode_info *ci)
{
	int have = 0;
	struct ceph_inode_cap *cap;
	struct list_head *p;

	list_for_each(p, &ci->i_caps) {
		cap = list_entry(p, struct ceph_inode_cap, ci_caps);
		have |= cap->caps;
	}
	return have;
}

void ceph_remove_cap(struct ceph_inode_cap *cap)
{
	struct ceph_mds_session *session = cap->session;
	struct ceph_inode_info *ci = cap->ci;

	dout(10, "ceph_remove_cap %p from %p\n", cap, &ci->vfs_inode);

	/* remove from session list */
	spin_lock(&session->s_cap_lock);
	list_del(&cap->session_caps);
	session->s_nr_caps--;
	cap->session = 0;
	spin_unlock(&session->s_cap_lock);

	/* remove from inode list */
	spin_lock(&ci->vfs_inode.i_lock);
	list_del(&cap->ci_caps);
	if (list_empty(&ci->i_caps))
		iput(&ci->vfs_inode);
	cap->ci = 0;
	cap->mds = -1;  /* mark unused */
	spin_unlock(&ci->vfs_inode.i_lock);
		
	if (cap < ci->i_static_caps || cap >= ci->i_static_caps + STATIC_CAPS) 
		kfree(cap);
}

void ceph_remove_all_caps(struct ceph_inode_info *ci)
{
	struct ceph_inode_cap *cap;
	dout(10, "remove_caps on %p\n", &ci->vfs_inode);
	while (!list_empty(&ci->i_caps)) {
		cap = list_entry(ci->i_caps.next, struct ceph_inode_cap,
				 ci_caps);
		ceph_remove_cap(cap);
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
	int issued; /* to me, before */
	int wanted = ceph_caps_wanted(ci) | ceph_caps_used(ci);
	int ret = 0;
	u64 size = le64_to_cpu(grant->size);
	u64 max_size = le64_to_cpu(grant->max_size);
	struct timespec mtime, atime;
	int wake = 0;
	
	dout(10, "handle_cap_grant inode %p ci %p mds%d seq %d\n", 
	     inode, ci, mds, seq);
	dout(10, " my wanted = %d\n", wanted);
	dout(10, " size %llu max_size %llu\n", size, max_size);

	/* size change? */
	spin_lock(&inode->i_lock);
	if (size > inode->i_size) {
		dout(10, "size %lld -> %llu\n", inode->i_size, size);
		inode->i_size = size;
	}

	/* max size increase? */
	if (max_size != ci->i_max_size) {
		dout(10, "max_size %lld -> %llu\n", ci->i_max_size, max_size);
		ci->i_max_size = max_size;
	}

	/* mtime/atime? */
	issued = ceph_caps_issued(ci);
	if ((issued & CEPH_CAP_EXCL) == 0) {
		ceph_decode_timespec(&mtime, &grant->mtime);
		ceph_decode_timespec(&atime, &grant->atime);
		if (timespec_compare(&mtime, &inode->i_mtime) > 0) {
			dout(10, "mtime %lu.%09ld -> %lu.%.09ld\n", 
			     mtime.tv_sec, mtime.tv_nsec,
			     inode->i_mtime.tv_sec, inode->i_mtime.tv_nsec);
			inode->i_mtime = mtime;
		}
		if (timespec_compare(&mtime, &inode->i_mtime) > 0) {
			dout(10, "atime %lu.%09ld -> %lu.%09ld\n", 
			     atime.tv_sec, atime.tv_nsec,
			     inode->i_atime.tv_sec, inode->i_atime.tv_nsec);
			inode->i_atime = atime;
		}
	}

	cap = get_cap_for_mds(inode, mds);

	/* do we have this cap? */
	if (!cap) {
		/*
		 * then ignore.  never reply to cap messages out of turn,
		 * or we'll be mixing up different instances of caps on the
		 * same inode, and confuse the mds.
		 */
		dout(10, "no cap on ino %llx from mds%d, ignoring\n",
		     ci->i_ceph_ino, mds);
		goto out;
	} 

	cap->seq = seq;

	if (wanted != le32_to_cpu(grant->wanted)) {
		dout(10, "wanted %d -> %d\n", le32_to_cpu(grant->wanted), 
		     wanted);
		grant->wanted = cpu_to_le32(wanted);
	}

	/* revocation? */
	newcaps = le32_to_cpu(grant->caps);
	if (cap->caps & ~newcaps) {
		used = ceph_caps_used(ci);
		dout(10, "revocation: %d -> %d, used %d\n", cap->caps,
		     newcaps, used);
		if (newcaps & used) {
			/* FIXME FIXME FIXME DO STUFF HERE */
			/* but blindly ack for now... */
		}
		cap->caps = newcaps;
		ret = 1; /* ack */
		goto out;
	}
	
	/* grant or no-op */
	if (cap->caps == newcaps) {
		dout(10, "caps unchanged: %d -> %d\n", cap->caps, newcaps);
	} else {
		dout(10, "grant: %d -> %d\n", cap->caps, newcaps);
		cap->caps = newcaps;
		wake = 1;
	}

out:
	spin_unlock(&inode->i_lock);
	if (wake)
		wake_up(&ci->i_cap_wq);
	return ret;	
}

int ceph_handle_cap_trunc(struct inode *inode, struct ceph_mds_file_caps *trunc,
			  struct ceph_mds_session *session)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	int seq = le32_to_cpu(trunc->seq);
	u64 size = le64_to_cpu(trunc->size);
	dout(10, "handle_cap_trunc inode %p ci %p mds%d seq %d\n", inode, ci, 
	     mds, seq);

	spin_lock(&inode->i_lock);
	dout(10, "trunc size %lld -> %llu\n", inode->i_size, size);
	inode->i_size = size;
	spin_unlock(&inode->i_lock);

	/*
	 * FIXME: how to truncate the page cache here?
	 */
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
	if (got & CEPH_CAP_WRBUFFER)
		ci->i_wrbuffer_ref++;
}

int ceph_get_cap_refs(struct ceph_inode_info *ci, int need, int want, int *got)
{
	int ret = 0;
	int have;
	dout(10, "get_cap_refs on %p need %d want %d\n", &ci->vfs_inode,
	     need, want);
	spin_lock(&ci->vfs_inode.i_lock);
	have = ceph_caps_issued(ci);
	if ((have & need) == need) {
		*got = need | (have & want);
		__take_cap_refs(ci, *got);
		ret = 1;
	}
	spin_unlock(&ci->vfs_inode.i_lock);
	dout(10, "get_cap_refs on %p ret %d got %d\n", &ci->vfs_inode,
	     ret, *got);
	return ret;
}

void ceph_put_cap_refs(struct ceph_inode_info *ci, int had)
{
	int last = 0, wanted;

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
	if (had & CEPH_CAP_WRBUFFER)
		if (--ci->i_wrbuffer_ref)
			last++;
	spin_unlock(&ci->vfs_inode.i_lock);

	if (last) {
		wanted = ceph_caps_wanted(ci);
		wanted |= ceph_caps_used(ci);
		if (wanted != ci->i_cap_wanted)
			ceph_mdsc_update_cap_wanted(ci, wanted);
	}
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
struct ceph_mds_request *prepare_setattr(struct ceph_mds_client *mdsc, 
					 struct dentry *dentry, int op)
{
	char *path;
	int pathlen;
	struct ceph_mds_request *req;
	__u64 baseino = ceph_ino(dentry->d_inode->i_sb->s_root->d_inode);

	dout(5, "prepare_setattr dentry %p\n", dentry);
	path = ceph_build_dentry_path(dentry, &pathlen);
	if (IS_ERR(path))
		return ERR_PTR(PTR_ERR(path));
	req = ceph_mdsc_create_request(mdsc, op, baseino, path, 0, 0);
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
		dout(10, "setattr: mode %d -> %d\n", inode->i_mode, 
		     attr->ia_mode);
        if (ia_valid & ATTR_SIZE)
		dout(10, "setattr: size %lld -> %lld\n", inode->i_size, 
		     attr->ia_size);
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
		ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_IAUTH);
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
		ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_IAUTH);
		err = ceph_mdsc_do_request(mdsc, req);
		ceph_mdsc_put_request(req);
		dout(10, "chmod result %d\n", err);
		if (err)
			return err;
	}

	/* utimes */
	if (((ia_valid & ATTR_ATIME) && 
	     !timespec_equal(&inode->i_atime, &attr->ia_atime)) ||
	    ((ia_valid & ATTR_MTIME) && 
	     !timespec_equal(&inode->i_mtime, &attr->ia_mtime))) {
		/* do i hold CAP_EXCL? */
		if (ceph_caps_issued(ci) & CEPH_CAP_EXCL) {
			dout(10, "utime holding EXCL, doing nothing\n");
			inode->i_atime = attr->ia_atime;
			inode->i_mtime = attr->ia_mtime;
			return 0;
		}
		req = prepare_setattr(mdsc, dentry, CEPH_MDS_OP_UTIME);
		if (IS_ERR(req)) 
			return PTR_ERR(req);
		reqh = req->r_request->front.iov_base;
		ceph_encode_timespec(&reqh->args.utime.mtime, &attr->ia_mtime);
		ceph_encode_timespec(&reqh->args.utime.atime, &attr->ia_atime);
		ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_ICONTENT);
		err = ceph_mdsc_do_request(mdsc, req);
		ceph_mdsc_put_request(req);
		dout(10, "utime result %d\n", err);
		if (err)
			return err;
	}

	/* truncate? */
	if (ia_valid & ATTR_SIZE &&
	    attr->ia_size < inode->i_size) {  /* fixme? */
		dout(10, "truncate: ia_size %d i_size %d\n",
		     (int)attr->ia_size, (int)inode->i_size);
		if (ia_valid & ATTR_FILE) 
			req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_TRUNCATE, 
						       ceph_ino(dentry->d_inode), "", 0, 0);
		else
			req = prepare_setattr(mdsc, dentry, CEPH_MDS_OP_TRUNCATE);
		if (IS_ERR(req)) 
			return PTR_ERR(req);
		reqh = req->r_request->front.iov_base;
		reqh->args.truncate.length = cpu_to_le64(attr->ia_size);
		ceph_mdsc_lease_release(mdsc, inode, 0, CEPH_LOCK_ICONTENT);
		err = ceph_mdsc_do_request(mdsc, req);
		ceph_mdsc_put_request(req);
		dout(10, "truncate result %d\n", err);
		if (err)
			return err;
	}

	return 0;
}

int ceph_inode_revalidate(struct inode *inode, int mask)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int havemask = ci->i_lease_mask;

	/* EXCL cap counts for an ICONTENT lease */
	if (ceph_caps_issued(ci) & CEPH_CAP_EXCL)
		havemask |= CEPH_LOCK_ICONTENT;
	/* any ICONTENT bits imply all bits */
	if (havemask & CEPH_LOCK_ICONTENT)  
		havemask |= CEPH_LOCK_ICONTENT;
	
	if (time_before(jiffies, ci->i_lease_ttl)) {
		if ((havemask & mask) == mask) {
			dout(10, "inode_revalidate %p mask %d still valid\n", 
			     inode, mask);
			return 0;
		} 
		dout(10, "inode_revalidate %p mask %d by only have %d\n", inode,
		     mask, havemask);
	} else {
		dout(10, "inode_revalidate %p have %d want %d, lease expired\n",
		     inode, havemask, mask);
		ceph_revoke_inode_lease(ci, mask);
	}
	return ceph_do_lookup(inode->i_sb, d_find_alias(inode), mask);
}

int ceph_inode_getattr(struct vfsmount *mnt, struct dentry *dentry, 
		       struct kstat *stat)
{
	int err = 0;
	dout(30, "ceph_inode_getattr\n");

	err = ceph_inode_revalidate(dentry->d_inode, CEPH_STAT_MASK_INODE_ALL);

	dout(30, "ceph_inode_getattr returned %d\n", err);
	if (!err) 
		generic_fillattr(dentry->d_inode, stat);
	return err;
}

