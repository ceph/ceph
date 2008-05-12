#include <linux/exportfs.h>

#include "ceph_fs.h"

int ceph_debug_export = -1;
#define DOUT_VAR ceph_debug_export
#define DOUT_PREFIX "export: "
#include "super.h"

/*
 * fh is N tuples of
 *  <ino, parent's d_name.hash>
 */

#define IPSZ (sizeof(struct ceph_inopath_item) / sizeof(u32))

int ceph_encode_fh(struct dentry *dentry, __u32 *rawfh, int *max_len, 
		   int connectable)
{
	int type = 1;
	struct ceph_inopath_item *fh =
		(struct ceph_inopath_item *)rawfh;
	int max = *max_len / IPSZ;
	int len;

	dout(10, "encode_fh %p max_len %d u32s (%d inopath items)%s\n", dentry,
	     *max_len, max, connectable ? " connectable":"");
	
	if (max < 1 || (connectable && max < 2))
		return -ENOSPC;

	/*
	 * pretty sure this is racy
	 */
	/* note: caller holds dentry->d_lock */
	fh[0].ino = cpu_to_le64(ceph_ino(dentry->d_inode));
	fh[0].dname_hash = cpu_to_le32(dentry->d_name.hash);
	len = 1;
	while (len < max) {
		dentry = dentry->d_parent;
		if (!dentry)
			break;
		fh[len].ino = cpu_to_le64(ceph_ino(dentry->d_inode));
		fh[len].dname_hash = cpu_to_le32(dentry->d_name.hash);
		len++;
		type = 2;
		if (IS_ROOT(dentry))
			break;
	}

	*max_len = len * IPSZ;
	return type;
}

struct dentry *__fh_to_dentry(struct super_block *sb,
			      struct ceph_inopath_item *fh, int len)
{
	struct ceph_mds_client *mdsc = &ceph_client(sb)->mdsc;
	struct inode *inode;
	struct dentry *dentry;
	int err;
	u64 ino = le64_to_cpu(fh[0].ino);
	u32 hash = le32_to_cpu(fh[0].dname_hash);

	inode = ceph_find_inode(sb, ino);
	if (!inode) {
		struct ceph_mds_request *req;
		derr(10, "__fh_to_dentry %llx.%x -- no inode\n", ino, hash);
		
		req = ceph_mdsc_create_request(mdsc,
					       CEPH_MDS_OP_FINDINODE,
					       len, (char *)fh, 0, 0,
					       NULL, 0, -1);
		if (IS_ERR(req))
			return ERR_PTR(PTR_ERR(req));
		err = ceph_mdsc_do_request(mdsc, req);
		ceph_mdsc_put_request(req);
		
		inode = ceph_find_inode(sb, ino);
		if (!inode)
			return ERR_PTR(err ? err : -ESTALE);
	}

	dentry = d_alloc_anon(inode);
	if (!dentry) {
		derr(10, "__fh_to_dentry %llx.%x -- inode %p but ENOMEM\n", ino,
		     hash, inode);
		iput(inode);
		return ERR_PTR(-ENOMEM);
	}
	dout(10, "__fh_to_dentry %llx.%x -- inode %p dentry %p\n", ino, hash,
	     inode, dentry);
	return dentry;	

}

struct dentry *ceph_fh_to_dentry(struct super_block *sb, struct fid *fid,
				 int fh_len, int fh_type)
{
	u32 *fh = fid->raw;
	return __fh_to_dentry(sb, (struct ceph_inopath_item *)fh, fh_len/IPSZ);
}

struct dentry *ceph_fh_to_parent(struct super_block *sb, struct fid *fid,
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
