#include <linux/module.h>
#include <linux/fs.h>
#include <linux/smp_lock.h>
#include <linux/slab.h>

#include <linux/ceph_fs.h>

MODULE_AUTHOR("Patience Warnick <patience@newdream.net>");
MODULE_DESCRIPTION("Ceph filesystem for Linux");
MODULE_LICENSE("GPL");


static void ceph_read_inode(struct inode * inode)
{
	return;
}

static int ceph_write_inode(struct inode * inode, int unused)
{
	lock_kernel();
	unlock_kernel();
	return 0;
}

static void ceph_delete_inode(struct inode * inode)
{
	return;
}

static void ceph_put_super(struct super_block *s)
{
	return;
}

static int ceph_statfs(struct dentry *dentry, struct kstatfs *buf)
{
	return 0;
}

static void ceph_write_super(struct super_block *s)
{
	lock_kernel();
	unlock_kernel();
	return;
}

static struct kmem_cache *ceph_inode_cachep;

static struct inode *ceph_alloc_inode(struct super_block *sb)
{
	struct ceph_inode_info *ci;
	ci = kmem_cache_alloc(ceph_inode_cachep, GFP_KERNEL);
	if (!ci)
		return NULL;
	return &ci->vfs_inode;
}

static void ceph_destroy_inode(struct inode *inode)
{
	kmem_cache_free(ceph_inode_cachep, CEPH_I(inode));
}

static void init_once(void *foo, struct kmem_cache *cachep, unsigned long flags)
{
	struct ceph_inode_info *ci = (struct ceph_inode_info *) foo;

	if ((flags & (SLAB_CTOR_VERIFY|SLAB_CTOR_CONSTRUCTOR)) ==
	    SLAB_CTOR_CONSTRUCTOR)
		inode_init_once(&ci->vfs_inode);
}
 
static int init_inodecache(void)
{
	ceph_inode_cachep = kmem_cache_create("ceph_inode_cache",
					     sizeof(struct ceph_inode_info),
					     0, (SLAB_RECLAIM_ACCOUNT|
						SLAB_MEM_SPREAD),
					     init_once, NULL);
	if (ceph_inode_cachep == NULL)
		return -ENOMEM;
	return 0;
}

static void destroy_inodecache(void)
{
	kmem_cache_destroy(ceph_inode_cachep);
}

static const struct super_operations ceph_sops = {
	.alloc_inode	= ceph_alloc_inode,
	.destroy_inode	= ceph_destroy_inode,
	.read_inode	= ceph_read_inode,
	.write_inode	= ceph_write_inode,
	.delete_inode	= ceph_delete_inode,
	.put_super	= ceph_put_super,
	.write_super	= ceph_write_super,
	.statfs		= ceph_statfs,
};

static int ceph_get_sb(struct file_system_type *fs_type,
	int flags, const char *dev_name, void *data, struct vfsmount *mnt)
{
	printk(KERN_INFO "entered ceph_get_sb\n");
        return 0;
}

static struct file_system_type ceph_fs_type = {
	.owner		= THIS_MODULE,
	.name		= "ceph",
	.get_sb		= ceph_get_sb,
	.kill_sb	= kill_block_super,
/*	.fs_flags	=   */
};

static int __init init_ceph(void)
{
	int ret = 0;

	printk(KERN_INFO "ceph init\n");
	if (!(ret = init_inodecache())) {
		if ((ret = register_filesystem(&ceph_fs_type))) {
			destroy_inodecache();
        	}
        }
	return ret;
}

static void __exit exit_ceph(void)
{
	printk(KERN_INFO "ceph exit\n");

	unregister_filesystem(&ceph_fs_type);
}


module_init(init_ceph);
module_exit(exit_ceph);
