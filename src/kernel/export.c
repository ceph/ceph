#include <linux/exportfs.h>

#include "super.h"
#include "ceph_debug.h"

int ceph_debug_export = -1;
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

#define IPSZ (sizeof(struct ceph_inopath_item) / sizeof(u32))

static int ceph_encode_fh(struct dentry *dentry, u32 *rawfh, int *max_len,
		   int connectable)
{
	int type = 1;
	struct ceph_inopath_item *fh =
		(struct ceph_inopath_item *)rawfh;
	int max = *max_len / IPSZ;
	int len;

	dout(10, "encode_fh %p max_len %d u32s (%d inopath items)%s\n", dentry,
	     *max_len, max, connectable ? " connectable" : "");

	if (max < 1 || (connectable && max < 2))
		return -ENOSPC;

	/*
	 * pretty sure this is racy.  caller holds dentry->d_lock, but
	 * not parents'.
	 */
	fh[0].ino = cpu_to_le64(ceph_vino(dentry->d_inode).ino);
	fh[0].dname_hash = cpu_to_le32(dentry->d_name.hash);
	len = 1;
	while (len < max) {
		dentry = dentry->d_parent;
		if (!dentry)
			break;
		fh[len].ino = cpu_to_le64(ceph_vino(dentry->d_inode).ino);
		fh[len].dname_hash = cpu_to_le32(dentry->d_name.hash);
		len++;
		type = 2;
		if (IS_ROOT(dentry))
			break;
	}

	*max_len = len * IPSZ;
	return type;
}

static struct dentry *__fh_to_dentry(struct super_block *sb,
			      struct ceph_inopath_item *fh, int len)
{
	struct ceph_mds_client *mdsc = &ceph_client(sb)->mdsc;
	struct inode *inode;
	struct dentry *dentry;
	int err;
	struct ceph_vino vino = {
		.ino = le64_to_cpu(fh[0].ino),
		.snap = CEPH_NOSNAP,   /* FIXME */
	};
	u32 hash = le32_to_cpu(fh[0].dname_hash);

	inode = ceph_find_inode(sb, vino);
	if (!inode) {
		struct ceph_mds_request *req;
		derr(10, "fh_to_dentry %llx.%x -- no inode\n", vino.ino, hash);
		req = ceph_mdsc_create_request(mdsc,
					       CEPH_MDS_OP_FINDINODE,
					       NULL, NULL,
					       (char *)fh, (void *)&len,
					       USE_ANY_MDS);
		if (IS_ERR(req))
			return ERR_PTR(PTR_ERR(req));
		err = ceph_mdsc_do_request(mdsc, NULL, req);
		ceph_mdsc_put_request(req);
		inode = ceph_find_inode(sb, vino);
		if (!inode)
			return ERR_PTR(err ? err : -ESTALE);
	}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 28)
	dentry = d_obtain_alias(inode);
#else
	dentry = d_alloc_anon(inode);
#endif
	if (!dentry) {
		derr(10, "fh_to_dentry %llx.%x -- inode %p but ENOMEM\n",
		     vino.ino,
		     hash, inode);
		iput(inode);
		return ERR_PTR(-ENOMEM);
	}
	dout(10, "fh_to_dentry %llx.%x -- inode %p dentry %p\n", vino.ino,
	     hash, inode, dentry);
	return dentry;

}

static struct dentry *ceph_fh_to_dentry(struct super_block *sb, struct fid *fid,
				 int fh_len, int fh_type)
{
	u32 *fh = fid->raw;
	return __fh_to_dentry(sb, (struct ceph_inopath_item *)fh, fh_len/IPSZ);
}

static struct dentry *ceph_fh_to_parent(struct super_block *sb, struct fid *fid,
				 int fh_len, int fh_type)
{
	u32 *fh = fid->raw;
	u64 ino = *(u64 *)fh;
	u32 hash = fh[2];

	derr(10, "fh_to_parent %llx.%x\n", ino, hash);

	if (fh_len < 6)
		return ERR_PTR(-ESTALE);

	return __fh_to_dentry(sb, (struct ceph_inopath_item *)fh + 1,
			      fh_len/IPSZ - 1);
}

const struct export_operations ceph_export_ops = {
	.encode_fh = ceph_encode_fh,
	.fh_to_dentry = ceph_fh_to_dentry,
	.fh_to_parent = ceph_fh_to_parent,
};
