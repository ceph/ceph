#include <linux/module.h>
#include <linux/fs.h>
#include <linux/smp_lock.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/uaccess.h>

#include <linux/ceph_fs.h>

int ceph_inode_debug = 50;
#define DOUT_VAR ceph_inode_debug
#define DOUT_PREFIX "inode: "
#include "super.h"

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
 * populate an inode based on info from mds
 */
int ceph_fill_inode(struct inode *inode, struct ceph_mds_reply_inode *info) 
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int i;
	int symlen;

	inode->i_ino = le64_to_cpu(info->ino);
	inode->i_mode = le32_to_cpu(info->mode);
	inode->i_uid = le32_to_cpu(info->uid);
	inode->i_gid = le32_to_cpu(info->gid);
	inode->i_nlink = le32_to_cpu(info->nlink);
	inode->i_size = le64_to_cpu(info->size);
	inode->i_rdev = le32_to_cpu(info->rdev);
	inode->i_blocks = 1;

	insert_inode_hash(inode);

	dout(30, "fill_inode %p ino=%lx by %d.%d sz=%llu mode %o nlink %d\n", inode,
	     inode->i_ino, inode->i_uid, inode->i_gid, inode->i_size, inode->i_mode, 
	     inode->i_nlink);
	
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
	
	for (i=0; i<ci->i_nr_caps; i++) 
		if (ci->i_caps[i].mds == mds) break;
	if (i == ci->i_nr_caps) {
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
		ci->i_caps[i].ci = ci;
		ci->i_caps[i].caps = 0;
		ci->i_caps[i].mds = mds;
		ci->i_caps[i].seq = 0;
		ci->i_caps[i].flags = 0;
		ci->i_nr_caps++;

		ci->i_caps[i].session = session;
		spin_lock(&session->s_cap_lock);
		list_add(&ci->i_caps[i].session_caps, &session->s_caps);
		session->s_nr_caps++;
		spin_unlock(&session->s_cap_lock);
	}

	dout(10, "add_cap inode %p (%lu) got cap %d %xh now %xh seq %d from %d\n",
	     inode, inode->i_ino, i, cap, cap|ci->i_caps[i].caps, seq, mds);
	ci->i_caps[i].caps |= cap;
	ci->i_caps[i].seq = seq;
	if (ci->i_nr_caps == 1)
		igrab(inode);
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
	spin_unlock(&session->s_cap_lock);
	cap->session = 0;
}

void ceph_remove_caps(struct ceph_inode_info *ci)
{
	int i;
	dout(10, "remove_caps on %p nr %d\n", &ci->vfs_inode, ci->i_nr_caps);
	if (ci->i_nr_caps) {
		for (i=0; i<ci->i_nr_caps; i++) 
			__remove_cap(&ci->i_caps[i]);
		iput(&ci->vfs_inode);
		ci->i_nr_caps = 0;
		if (ci->i_caps != ci->i_caps_static) {
			kfree(ci->i_caps);
			ci->i_caps = ci->i_caps_static;
			ci->i_max_caps = STATIC_CAPS;
		}
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

	dout(10, "handle_cap_grant inode %p ci %p mds%d seq %d\n", inode, ci, mds, seq);

	/* unwanted? */
	if (ceph_caps_wanted(ci) == 0) {
		dout(10, "wanted=0, reminding mds\n");
		grant->wanted = cpu_to_le32(0);
		return 1; /* ack */
	}

	/* new cap? */
	cap = get_cap_for_mds(inode, mds);
	if (!cap) {
		dout(10, "adding new cap inode %p for mds%d\n", inode, mds);
		cap = ceph_add_cap(inode, session, le32_to_cpu(grant->caps), le32_to_cpu(grant->seq));
		return 0;
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
		dout(10, "no-op: %d -> %d\n", cap->caps, newcaps);
	} else {
		dout(10, "grant: %d -> %d\n", cap->caps, newcaps);
		cap->caps = newcaps;
	}
	return 0;	
}

