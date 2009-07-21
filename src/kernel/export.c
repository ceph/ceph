#include <linux/exportfs.h>
#include <asm/unaligned.h>

#include "super.h"
#include "ceph_debug.h"

/*
 * fh is the ino, the parent ino, and the parent dentry hash.  we'll
 * make them all connectable.
 */
struct ceph_nfs_fh {
	struct ceph_vino ino;
	struct ceph_vino parent_ino;
	u32 parent_name_hash;
} __attribute__ ((packed));

static int ceph_encode_fh(struct dentry *dentry, u32 *rawfh, int *max_len,
			  int connectable)
{
	struct ceph_nfs_fh *fh = (void *)rawfh;
	struct dentry *parent = dentry->d_parent;

	if (*max_len < sizeof(*fh))
		return -ENOSPC;

	dout("encode_fh %p%s\n", dentry, connectable ? " connectable" : "");
	fh->ino = ceph_vino(dentry->d_inode);
	fh->parent_ino = ceph_vino(parent->d_inode);
	fh->parent_name_hash = parent->d_name.hash;
	return 2;
}

static struct dentry *__fh_to_dentry(struct super_block *sb,
				     struct ceph_nfs_fh *fh)
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
		pr_err("ceph fh_to_dentry %llx.%x -- no inode\n", fh->ino.ino,
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

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 28)
	dentry = d_obtain_alias(inode);
#else
	dentry = d_alloc_anon(inode);
#endif

	if (!dentry) {
		pr_err("ceph fh_to_dentry %llx.%x -- inode %p but ENOMEM\n",
		       fh->ino.ino, hash, inode);
		iput(inode);
		return ERR_PTR(-ENOMEM);
	}
	err = ceph_init_dentry(dentry);

	if (err < 0) {
		iput(inode);
		return ERR_PTR(err);
	}
	dout("fh_to_dentry %llx.%x -- inode %p dentry %p\n", fh->ino.ino,
	     hash, inode, dentry);
	return dentry;

}

static struct dentry *ceph_fh_to_dentry(struct super_block *sb, struct fid *fid,
					int fh_len, int fh_type)
{
	return __fh_to_dentry(sb, (struct ceph_nfs_fh *)fid->raw);
}

static struct dentry *ceph_fh_to_parent(struct super_block *sb, struct fid *fid,
					int fh_len, int fh_type)
{
	struct ceph_nfs_fh *fh = (void *)fid->raw;
	struct inode *inode;
	
	pr_debug("ceph_fh_to_parent %llx\n", fh->parent_ino.ino);

	/* TODO: this is a feeble attempt.  we should query the mds, too. */
	inode = ceph_find_inode(sb, fh->ino);
	if (!inode)
		return ERR_PTR(-ESTALE);

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 28)
	return d_obtain_alias(inode);
#else
	return d_alloc_anon(inode);
#endif
}

const struct export_operations ceph_export_ops = {
	.encode_fh = ceph_encode_fh,
	.fh_to_dentry = ceph_fh_to_dentry,
	.fh_to_parent = ceph_fh_to_parent,
};
