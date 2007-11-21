
#include <linux/module.h>
#include <linux/parser.h>
#include <linux/fs.h>
#include <linux/mount.h>
#include <linux/seq_file.h>
#include <linux/string.h>
#include "super.h"




/*
 * super ops
 */

static void ceph_read_inode(struct inode * inode)
{
	return;
}

static int ceph_write_inode(struct inode * inode, int unused)
{
	return 0;
}

static void ceph_delete_inode(struct inode * inode)
{
	return;
}

static void ceph_clear_inode(struct inode * inode)
{

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
	return;
}


/**
 * ceph_show_options - Show mount options in /proc/mounts
 * @m: seq_file to write to
 * @mnt: mount descriptor
 */
static int ceph_show_options(struct seq_file *m, struct vfsmount *mnt)
{
	struct ceph_super_info *sbinfo = mnt->mnt_sb->s_fs_info;
	struct ceph_mount_args *args = &sbinfo->mount_args;

	if (ceph_debug != 0)
		seq_printf(m, ",debug=%d", ceph_debug);		
	if (args->flags & CEPH_MOUNT_FSID) 
		seq_printf(m, ",fsidmajor=%llu,fsidminor%llu", 
			   args->fsid.major, args->fsid.minor);
	if (args->flags & CEPH_MOUNT_NOSHARE)
		seq_puts(m, ",noshare");
	seq_printf(m, ",monport=%d", args->mon_port);
	return 0;
}


/*
 * inode cache
 */
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
	struct ceph_inode_info *ci = foo;
	inode_init_once(&ci->vfs_inode);
}

static int init_inodecache(void)
{
	ceph_inode_cachep = kmem_cache_create("ceph_inode_cache",
					      sizeof(struct ceph_inode_info),
					      0, (SLAB_RECLAIM_ACCOUNT|
						  SLAB_MEM_SPREAD),
					      init_once);
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
	.clear_inode    = ceph_clear_inode,
	.delete_inode	= ceph_delete_inode,
	.put_super	= ceph_put_super,
	.write_super	= ceph_write_super,
	.show_options   = ceph_show_options,
	.statfs		= ceph_statfs,
};

static int ceph_set_super(struct super_block *s, void *data)
{
	struct ceph_mount_args *args = data;
	struct ceph_super_info *sbinfo;
	int ret;

	s->s_flags = args->mntflags;
	
	sbinfo = kzalloc(sizeof(struct ceph_super_info), GFP_KERNEL);
	if (!sbinfo)
		return -ENOMEM;
	s->s_fs_info = sbinfo;

	memcpy(&sbinfo->mount_args, args, sizeof(*args));
	
	ret = set_anon_super(s, 0);  /* what is the second arg for? */
	if (ret != 0)
		goto out; 
	
	return ret;

out:
	kfree(s->s_fs_info);
	s->s_fs_info = 0;
	return ret;
}

/*
 * share superblock if same fs AND path AND options
 */
static int ceph_compare_super(struct super_block *sb, void *data)
{
	struct ceph_mount_args *args = (struct ceph_mount_args*)data;
	struct ceph_super_info *other = ceph_sbinfo(sb);
	
	/* either compare fsid, or specified mon_hostname */
	if (args->flags & CEPH_MOUNT_FSID) {
		if (!ceph_fsid_equal(&args->fsid, &other->sb_client->fsid))
			return 1;
	} else {
		/*if (strcmp(args->mon_hostname, other->mount_args.mon_hostname) != 0)
			return 1;
		*/
	}
	if (strcmp(args->path, other->mount_args.path) != 0)
		return 1;
	if (args->mntflags != other->mount_args.mntflags)
		return 1;
	return 0;
}


enum {
	Opt_fsidmajor, 
	Opt_fsidminor,
	Opt_debug,
	Opt_monport	
};

static match_table_t arg_tokens = {
	{Opt_fsidmajor, "fsidmajor=%ld"},
	{Opt_fsidminor, "fsidminor=%ld"},
	{Opt_debug, "debug=%d"},
	{Opt_monport, "monport=%d"}
};

static int parse_ip(char *c, int len, struct ceph_entity_addr *addr)
{
	dout(1, "parse_ip on %s len %d\n", c, len);

	return 0;
}

static int parse_mount_args(int flags, char *options, const char *dev_name, struct ceph_mount_args *args)
{
	char *c;
	int len;
	substring_t argstr[MAX_OPT_ARGS];
		
	dout(1, "parse_mount_args dev_name %s\n", dev_name);

	/* defaults */
	args->mntflags = flags;
	args->flags = 0;
	args->mon_port = CEPH_MON_PORT;

	/* ip1[,ip2...]:/server/path */
	c = strchr(dev_name, ':');
	if (c == NULL)
		return -EINVAL;

	/* get mon ip */
	/* er, just one for now. later, comma-separate... */	
	len = c - dev_name;
	parse_ip(c, len, &args->mon_addr[0]);
	args->mon_addr[0].ipaddr.sin_family = AF_INET;
	args->mon_addr[0].ipaddr.sin_port = CEPH_MON_PORT;
	args->mon_addr[0].erank = 0;
	args->mon_addr[0].nonce = 0;
	args->num_mon = 1;
	
	/* path on server */
	c++;
	if (strlen(c) >= sizeof(args->path))
		return -ENAMETOOLONG;
	strcpy(args->path, c);
	
	dout(1, "server path %s\n", args->path);
	
	/* parse mount options */
	while ((c = strsep(&options, ",")) != NULL) {
		int token;
		int intval;
		int ret;
		if (!*c) 
			continue;
		token = match_token(c, arg_tokens, argstr);
		ret = match_int(&argstr[0], &intval);
		if (ret < 0) {
			dout(0, "bad mount arg, not int\n");
			continue;
		}
		switch (token) {
		case Opt_fsidmajor:
			args->fsid.major = intval;
			break;
		case Opt_fsidminor:
			args->fsid.minor = intval;
			break;
		case Opt_monport:
			args->mon_port = intval;
			break;
		case Opt_debug:
			ceph_debug = intval;
			break;
		default:
			derr(1, "bad mount option %s\n", c);
			continue;
		}
	}
	
	return 0;
}


static int ceph_get_sb(struct file_system_type *fs_type,
		       int flags, const char *dev_name, void *data, struct vfsmount *mnt)
{
	struct super_block *s;
	struct ceph_mount_args mount_args;
	struct ceph_super_info *sbinfo;
	int error;
	int (*compare_super)(struct super_block *, void *) = ceph_compare_super;

	dout(1, "ceph_get_sb\n");
	
	error = parse_mount_args(flags, data, dev_name, &mount_args);
	if (error < 0) 
		goto out;

	if (mount_args.flags & CEPH_MOUNT_NOSHARE)
		compare_super = 0;
	
	/* superblock */
	s = sget(fs_type, compare_super, ceph_set_super, &mount_args);
	if (IS_ERR(s)) {
		error = PTR_ERR(s);
		goto out;
	}
	sbinfo = ceph_sbinfo(s);

	/* client */
	if (!sbinfo->sb_client) {
		sbinfo->sb_client = ceph_get_client(&mount_args);
		if (PTR_ERR(sbinfo->sb_client)) {
			error = PTR_ERR(sbinfo->sb_client);
			goto out_splat;
		}
	}
        return 0;
	
out_splat:
	up_write(&s->s_umount);
	deactivate_super(s);
out:
	kfree(data);
	return error;
}

static void ceph_kill_sb(struct super_block *s)
{
	struct ceph_super_info *sbinfo = ceph_sbinfo(s);
	dout(1, "ceph_kill_sb\n");

	kill_anon_super(s);

	ceph_put_client(sbinfo->sb_client);
	kfree(sbinfo);
}



/************************************/

static struct file_system_type ceph_fs_type = {
	.owner		= THIS_MODULE,
	.name		= "ceph",
	.get_sb		= ceph_get_sb,
	.kill_sb	= ceph_kill_sb,
/*	.fs_flags	=   */
};

static int __init init_ceph(void)
{
	int ret = 0;

	dout(1, "init_ceph\n");
	if (!(ret = init_inodecache())) {
		if ((ret = register_filesystem(&ceph_fs_type))) {
			destroy_inodecache();
        	}
        }
	return ret;
}

static void __exit exit_ceph(void)
{
	dout(1, "exit_ceph\n");

	unregister_filesystem(&ceph_fs_type);
}


module_init(init_ceph);
module_exit(exit_ceph);
