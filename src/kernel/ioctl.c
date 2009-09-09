#include "ioctl.h"
#include "super.h"
#include "ceph_debug.h"


/*
 * ioctls
 */

/*
 * get and set the file layout
 */
static long ceph_ioctl_get_layout(struct file *file, void __user *arg)
{
	struct ceph_inode_info *ci = ceph_inode(file->f_dentry->d_inode);
	struct ceph_ioctl_layout l;
	int err;

	err = ceph_do_getattr(file->f_dentry->d_inode, CEPH_STAT_CAP_LAYOUT);
	if (!err) {
		l.stripe_unit = ceph_file_layout_su(ci->i_layout);
		l.stripe_count = ceph_file_layout_stripe_count(ci->i_layout);
		l.object_size = ceph_file_layout_object_size(ci->i_layout);
		l.data_pool = le32_to_cpu(ci->i_layout.fl_pg_pool);
		if (copy_to_user(arg, &l, sizeof(l)))
			return -EFAULT;
	}

	return err;
}

static long ceph_ioctl_set_layout(struct file *file, void __user *arg)
{
	struct inode *inode = file->f_dentry->d_inode;
	struct inode *parent_inode = file->f_dentry->d_parent->d_inode;
	struct ceph_mds_client *mdsc = &ceph_sb_to_client(inode->i_sb)->mdsc;
	struct ceph_mds_request *req;
	struct ceph_ioctl_layout l;
	int err, i;

	/* copy and validate */
	if (copy_from_user(&l, arg, sizeof(l)))
		return -EFAULT;

	if ((l.object_size & ~PAGE_MASK) ||
	    (l.stripe_unit & ~PAGE_MASK) ||
	    !l.stripe_unit ||
	    (l.object_size &&
	     (unsigned)l.object_size % (unsigned)l.stripe_unit))
		return -EINVAL;

	/* make sure it's a valid data pool */
	if (l.data_pool > 0) {
		mutex_lock(&mdsc->mutex);
		err = -EINVAL;
		for (i = 0; i < mdsc->mdsmap->m_num_data_pg_pools; i++)
			if (mdsc->mdsmap->m_data_pg_pools[i] == l.data_pool) {
				err = 0;
				break;
			}
		mutex_unlock(&mdsc->mutex);
		if (err)
			return err;
	}

	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_SETLAYOUT,
				       USE_AUTH_MDS);
	if (IS_ERR(req))
		return PTR_ERR(req);
	req->r_inode = igrab(inode);
	req->r_inode_drop = CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_EXCL;

	req->r_args.setlayout.layout.fl_stripe_unit =
		cpu_to_le32(l.stripe_unit);
	req->r_args.setlayout.layout.fl_stripe_count =
		cpu_to_le32(l.stripe_count);
	req->r_args.setlayout.layout.fl_object_size =
		cpu_to_le32(l.object_size);
	req->r_args.setlayout.layout.fl_pg_pool = cpu_to_le32(l.data_pool);
	req->r_args.setlayout.layout.fl_pg_preferred = cpu_to_le32((s32)-1);

	err = ceph_mdsc_do_request(mdsc, parent_inode, req);
	ceph_mdsc_put_request(req);
	return err;
}

long ceph_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
	dout("ioctl file %p cmd %u arg %lu\n", file, cmd, arg);
	switch (cmd) {
	case CEPH_IOC_GET_LAYOUT:
		return ceph_ioctl_get_layout(file, (void __user *)arg);

	case CEPH_IOC_SET_LAYOUT:
		return ceph_ioctl_set_layout(file, (void __user *)arg);
	}
	return -ENOTTY;
}
