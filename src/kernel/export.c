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

int ceph_encode_fh(struct dentry *dentry, __u32 *fh, int *max_len, 
		   int connectable)
{
	int len;
	int type = 1;

	dout(10, "encode_fh %p max_len %d%s\n", dentry, *max_len,
	     connectable ? " connectable":"");
	
	if (*max_len < 3 || (connectable && *max_len < 6))
		return -ENOSPC;

	/*
	 * pretty sure this is racy
	 */
	/* note: caller holds dentry->d_lock */
	*(u64 *)fh = ceph_ino(dentry->d_inode);
	fh[2] = dentry->d_name.hash;
	len = 3;
	while (len + 3 <= *max_len) {
		dentry = dentry->d_parent;
		if (!dentry)
			break;
		*(u64 *)(fh + len) = ceph_ino(dentry->d_inode);
		fh[len + 2] = dentry->d_name.hash;
		len += 3;
		type = 2;
		if (IS_ROOT(dentry))
			break;
	}

	*max_len = len;
	return type;
}

struct dentry *__fh_to_dentry(struct super_block *sb, u32 *fh, int fh_len)
{
	struct ceph_mds_client *mdsc = &ceph_client(sb)->mdsc;
	struct inode *inode;
	struct dentry *dentry;
	u64 ino = *(u64 *)fh;
	u32 hash = fh[2];
	int err;

	inode = ceph_find_inode(sb, ino);
	if (!inode) {
		struct ceph_mds_request *req;
		derr(10, "__fh_to_dentry %llx.%x -- no inode\n", ino, hash);
		
		req = ceph_mdsc_create_request(mdsc,
					       CEPH_MDS_OP_FINDINODE,
					       fh_len/3, (char *)fh, 0, 0);
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
		derr(10, "__fh_to_dentry %llx.%x -- inode %p but ENOMEM\n", 
		     ino, hash, inode);
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
	return __fh_to_dentry(sb, fh, fh_len);
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

	return __fh_to_dentry(sb, fh + 3, fh_len - 3);
}

const struct export_operations ceph_export_ops = {
	.encode_fh = ceph_encode_fh,
	.fh_to_dentry = ceph_fh_to_dentry,
	.fh_to_parent = ceph_fh_to_parent,
};
