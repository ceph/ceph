#include <linux/exportfs.h>
#include <asm/unaligned.h>

#include "super.h"
#include "ceph_debug.h"

int ceph_debug_export __read_mostly = -1;
#define DOUT_MASK DOUT_MASK_EXPORT
#define DOUT_VAR ceph_debug_export

/*
 * fh is N tuples of
 *  <ino, parent's d_name.hash>
 *
 * This is only a semi-reliable strategy.  The fundamental issue is
 * that ceph doesn't not have a way to locate an arbitrary inode by
 * ino.  Keeping a few parents in the handle increases the probability
 * that we'll find it in one of the MDS caches, but it is by no means
 * a guarantee.
 *
 * Also, the FINDINODE request is currently directed at a single MDS.
 * It should probably try all MDS's before giving up.  For a single MDS
 * system that isn't a problem.
 *
 * In the meantime, this works reasonably well for basic usage.
 */


struct ceph_export_item {
	struct ceph_vino ino;
	struct ceph_vino parent_ino;
	u32 parent_name_hash;
} __attribute__ ((packed));

#define IPSZ ((sizeof(struct ceph_export_item) + sizeof(u32) + 1) / sizeof(u32))

static int ceph_encode_fh(struct dentry *dentry, u32 *rawfh, int *max_len,
		   int connectable)
{
	int type = 1;
	struct ceph_export_item *fh =
		(struct ceph_export_item *)rawfh;
	int max = *max_len / IPSZ;
	int len;
	struct dentry *d_parent;

	dout(10, "encode_fh %p max_len %d u32s (%d export items)%s\n", dentry,
	     *max_len, max, connectable ? " connectable" : "");

	if (max < 1 || (connectable && max < 2))
		return -ENOSPC;

	for (len = 0; len < max; len++) {
		d_parent = dentry->d_parent;
		fh[len].ino = ceph_vino(dentry->d_inode);
		fh[len].parent_ino = ceph_vino(d_parent->d_inode);
		fh[len].parent_name_hash = dentry->d_parent->d_name.hash;

		if (IS_ROOT(dentry))
			break;

		dentry = dentry->d_parent;

		if (!dentry)
			break;
	}

	if (len > 1)
		type = 2;

	*max_len = len * IPSZ;
	return type;
}

static struct dentry *__fh_to_dentry(struct super_block *sb,
			      struct ceph_export_item *fh, int len)
{
	struct ceph_mds_client *mdsc = &ceph_client(sb)->mdsc;
	struct inode *inode;
	struct dentry *dentry;
	int err;
#define BUF_SIZE 16
	char path2[BUF_SIZE];
	u32 hash = fh->parent_name_hash;

	inode = ceph_find_inode(sb, fh->ino);
	if (!inode) {
		struct ceph_mds_request *req;
		derr(10, "fh_to_dentry %llx.%x -- no inode\n", fh->ino.ino,
		     hash);
		req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LOOKUPHASH,
					       USE_ANY_MDS);
		if (IS_ERR(req))
			return ERR_PTR(PTR_ERR(req));

		req->r_ino1 = fh->ino;
		snprintf(path2, BUF_SIZE, "%d", hash);
		req->r_ino2 = fh->parent_ino;
		req->r_num_caps = 1;
		err = ceph_mdsc_do_request(mdsc, NULL, req);
		ceph_mdsc_put_request(req);
		inode = ceph_find_inode(sb, fh->ino);
		if (!inode)
			return ERR_PTR(err ? err : -ESTALE);
	}

	dentry = d_obtain_alias(inode);

	if (!dentry) {
		derr(10, "fh_to_dentry %llx.%x -- inode %p but ENOMEM\n",
		     fh->ino.ino,
		     hash, inode);
		iput(inode);
		return ERR_PTR(-ENOMEM);
	}
	err = ceph_init_dentry(dentry);

	if (err < 0) {
		iput(inode);
		return ERR_PTR(err);
	}
	dout(10, "fh_to_dentry %llx.%x -- inode %p dentry %p\n", fh->ino.ino,
	     hash, inode, dentry);
	return dentry;

}

static struct dentry *ceph_fh_to_dentry(struct super_block *sb, struct fid *fid,
				 int fh_len, int fh_type)
{
	u32 *fh = fid->raw;
	return __fh_to_dentry(sb, (struct ceph_export_item *)fh, fh_len/IPSZ);
}

static struct dentry *ceph_fh_to_parent(struct super_block *sb, struct fid *fid,
				 int fh_len, int fh_type)
{
	u32 *fh = fid->raw;
	u64 ino = get_unaligned((u64 *)fh);
	u32 hash = fh[2];

	derr(10, "fh_to_parent %llx.%x\n", (unsigned long long)ino, hash);

	if (fh_len < 6)
		return ERR_PTR(-ESTALE);

	return __fh_to_dentry(sb, (struct ceph_export_item *)fh + 1,
			      fh_len/IPSZ - 1);
}

const struct export_operations ceph_export_ops = {
	.encode_fh = ceph_encode_fh,
	.fh_to_dentry = ceph_fh_to_dentry,
	.fh_to_parent = ceph_fh_to_parent,
};
