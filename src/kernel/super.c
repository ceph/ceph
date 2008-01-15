#include <linux/module.h>
#include <linux/parser.h>
#include <linux/fs.h>
#include <linux/mount.h>
#include <linux/seq_file.h>
#include <linux/string.h>

int ceph_super_debug = 50;
#define DOUT_VAR ceph_super_debug
#define DOUT_PREFIX "super: "
#include "super.h"
#include "ktcp.h"

#include <linux/statfs.h>
#include "mon_client.h"

/*
 * super ops
 */

static int ceph_write_inode(struct inode * inode, int unused)
{
	struct ceph_inode_info *ci = ceph_inode(inode);

	if (memcmp(&ci->i_old_atime, &inode->i_atime, sizeof(struct timeval))) {
		dout(30, "ceph_write_inode %lx .. atime updated\n", inode->i_ino);
		/* eventually push this async to mds ... */
	}
	return 0;
}

static void ceph_put_super(struct super_block *s)
{
	dout(30, "ceph_put_super\n");
	return;
}

static int ceph_statfs(struct dentry *dentry, struct kstatfs *buf)
{
	struct ceph_client *client = ceph_inode_to_client(dentry->d_inode);
	struct ceph_statfs st;
	int err;

	dout(30, "ceph_statfs\n");
	if ((err = ceph_monc_do_statfs(&client->monc, &st)) < 0)
		return err;

	/* fill in kstatfs */
	buf->f_type = CEPH_SUPER_MAGIC;  /* ?? */
	buf->f_bsize = 4096;
	buf->f_blocks = st.f_total / 4;
	buf->f_bfree = st.f_free / 4;
	buf->f_bavail = st.f_avail / 4;
	buf->f_files = st.f_objects;
	buf->f_ffree = -1;
	/* fsid? */
	buf->f_namelen = 1024;
	buf->f_frsize = 4096;
	
	return 0;
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

	ci->i_symlink = 0;

	ci->i_fragtree = ci->i_fragtree_static;
	ci->i_fragtree->nsplits = 0;

	ci->i_frag_map_nr = 0;
	ci->i_frag_map = ci->i_frag_map_static;

	ci->i_nr_caps = 0;
	ci->i_max_caps = STATIC_CAPS;
	ci->i_caps = ci->i_caps_static;
	atomic_set(&ci->i_cap_count, 0);

	return &ci->vfs_inode;
}

static void ceph_destroy_inode(struct inode *inode)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	
	if (ci->i_caps != ci->i_caps_static) 
		kfree(ci->i_caps);
	if (ci->i_symlink)
		kfree(ci->i_symlink);

	kmem_cache_free(ceph_inode_cachep, ci);
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
	.write_inode    = ceph_write_inode,
//	.sync_fs        = ceph_syncfs,
	.put_super	= ceph_put_super,
	.show_options   = ceph_show_options,
	.statfs		= ceph_statfs,
};


/* 
 * mount options
 */

enum {
	Opt_fsidmajor, 
	Opt_fsidminor,
	Opt_debug,
	Opt_debug_msgr,
	Opt_debug_mdsc,
	Opt_debug_osdc,
	Opt_monport,
	Opt_port,
	Opt_ip
};

static match_table_t arg_tokens = {
	{Opt_fsidmajor, "fsidmajor=%ld"},
	{Opt_fsidminor, "fsidminor=%ld"},
	{Opt_debug, "debug=%d"},
	{Opt_debug_msgr, "debug_msgr=%d"},
	{Opt_debug_mdsc, "debug_mdsc=%d"},
	{Opt_debug_osdc, "debug_osdc=%d"},
	{Opt_monport, "monport=%d"},
	{Opt_port, "port=%d"},
	{Opt_ip, "ip=%s"}
};

/*
 * FIXME: add error checking to ip parsing
 */
static int parse_ip(const char *c, int len, struct ceph_entity_addr *addr)
{
	int i;
	int v;
	unsigned ip = 0;
	const char *p = c;

	dout(15, "parse_ip on '%s' len %d\n", c, len);
	for (i=0; *p && i<4; i++) {
		v = 0;
		//dout(30, " i=%d at %s\n", i, p);
		while (*p && *p != '.' && p < c+len) {
			if (*p < '0' || *p > '9')
				goto bad;
			v = (v * 10) + (*p - '0');
			p++;
		}
		//dout(30, " v = %d\n", v);
		ip = (ip << 8) + v;
		if (!*p) 
			break;
		p++;
	}
	if (p < c+len) 
		goto bad;

	*(__u32*)&addr->ipaddr.sin_addr.s_addr = htonl(ip);
	dout(15, "parse_ip got %u.%u.%u.%u\n", ip >> 24, (ip >> 16) & 0xff,
	     (ip >> 8) & 0xff, ip & 0xff);
	return 0;

bad:
	dout(1, "parse_ip bad ip '%s'\n", c);
	return -EINVAL;
}

static int parse_mount_args(int flags, char *options, const char *dev_name, struct ceph_mount_args *args)
{
	char *c;
	int len;
	substring_t argstr[MAX_OPT_ARGS];
		
	dout(15, "parse_mount_args dev_name '%s'\n", dev_name);
	memset(args, 0, sizeof(*args));

	/* defaults */
	args->mntflags = flags;
	args->flags = 0;

	/* ip1[,ip2...]:/server/path */
	c = strchr(dev_name, ':');
	if (c == NULL)
		return -EINVAL;

	/* get mon ip */
	/* er, just one for now. later, comma-separate... */	
	len = c - dev_name;
	parse_ip(dev_name, len, &args->mon_addr[0]);
	args->mon_addr[0].ipaddr.sin_family = AF_INET;
	args->mon_addr[0].ipaddr.sin_port = htons(CEPH_MON_PORT);
	args->mon_addr[0].erank = 0;
	args->mon_addr[0].nonce = 0;
	args->num_mon = 1;
	
	/* path on server */
	c++;
	while (*c == '/') c++;  /* remove leading '/'(s) */
	if (strlen(c) >= sizeof(args->path))
		return -ENAMETOOLONG;
	strcpy(args->path, c);
	
	dout(15, "server path '%s'\n", args->path);
	
	/* parse mount options */
	while ((c = strsep(&options, ",")) != NULL) {
		int token, intval, ret, i;
		if (!*c) 
			continue;
		token = match_token(c, arg_tokens, argstr);
		if (token < Opt_ip) {
			ret = match_int(&argstr[0], &intval);
			if (ret < 0) {
				dout(0, "bad mount arg, not int\n");
				continue;
			}
			dout(30, "got token %d intval %d\n", token, intval);
		}
		switch (token) {
		case Opt_fsidmajor:
			args->fsid.major = intval;
			break;
		case Opt_fsidminor:
			args->fsid.minor = intval;
			break;
		case Opt_monport:
			dout(25, "parse_mount_args monport=%d\n", intval);
			for (i=0; i<args->num_mon; i++)
				args->mon_addr[i].ipaddr.sin_port = htons(intval);
			break;
		case Opt_debug:
			ceph_debug = intval;
			break;
		case Opt_debug_msgr:
			ceph_debug_msgr = intval;
			break;
		case Opt_debug_mdsc:
			ceph_debug_mdsc = intval;
			break;
		case Opt_debug_osdc:
			ceph_debug_osdc = intval;
			break;
		case Opt_port:
			args->my_addr.ipaddr.sin_port = htons(intval);
			break;
		case Opt_ip:
			parse_ip(argstr[0].from, argstr[0].to-argstr[0].from, &args->my_addr);
			break;
		default:
			derr(1, "parse_mount_args bad token %d\n", token);
			continue;
		}
	}

	return 0;
}

int parse_open_reply(struct ceph_msg *reply, struct inode *inode)
{
	struct ceph_mds_reply_head *head;
	struct ceph_mds_reply_info rinfo;
	int frommds = reply->hdr.src.name.num;
	int err;
	struct ceph_inode_cap *cap;

	/* parse reply */
	head = reply->front.iov_base;
	err = le32_to_cpu(head->result);
	dout(30, "parse_open_reply mds%d reports %d\n", frommds, err);
	if (err < 0) 
		return err;
	if ((err = ceph_mdsc_parse_reply_info(reply, &rinfo)) < 0)
		return err;
	BUG_ON(rinfo.trace_nr == 0);
	if ((err = ceph_fill_inode(inode, rinfo.trace_in[rinfo.trace_nr-1].in)) < 0) 
		return err;

	/* fill in cap */
	cap = ceph_add_cap(inode, frommds, 
			   le32_to_cpu(head->file_caps), 
			   le32_to_cpu(head->file_caps_seq));
	if (IS_ERR(cap))
		return PTR_ERR(cap);

	ceph_mdsc_destroy_reply_info(&rinfo);
	return 0;
}


static int open_root_inode(struct super_block *sb, struct ceph_mount_args *args)
{
	struct ceph_super_info *sbinfo = ceph_sbinfo(sb);
	struct ceph_mds_client *mdsc = &sbinfo->sb_client->mdsc;
	struct inode *inode;
	struct dentry *root;
	struct ceph_msg *req = 0;
	struct ceph_mds_request_head *reqhead;
	struct ceph_mds_reply_info rinfo;
	int frommds;
	int err;
	struct ceph_inode_cap *cap;
	struct ceph_inode_info *ci;
	
	/* open dir */
	dout(30, "open_root_inode opening '%s'\n", args->path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_OPEN, 1, args->path, 0, 0);
	if (IS_ERR(req)) 
		return PTR_ERR(req);
	reqhead = req->front.iov_base;
	reqhead->args.open.flags = O_DIRECTORY;
	reqhead->args.open.mode = 0;
	if ((err = ceph_mdsc_do_request(mdsc, req, &rinfo, -1)) < 0)
		return err;
	
	err = le32_to_cpu(rinfo.head->result);
	if (err != 0)
		return err;
	if (rinfo.trace_nr == 0) {
		dout(10, "open_root_inode wtf, mds returns 0 but no trace\n");
		err = -EINVAL;
	}

	/* create root inode */
	inode = iget_locked(sb, rinfo.trace_in[rinfo.trace_nr-1].in->ino);
	if (inode == NULL) 
		return -ENOMEM;
	if (inode->i_state & I_NEW)
		unlock_new_inode(inode);

	if ((err = ceph_fill_inode(inode, rinfo.trace_in[rinfo.trace_nr-1].in)) < 0) 
		goto out;

	/* fill in cap */
	frommds = rinfo.reply->hdr.src.name.num;
	cap = ceph_add_cap(inode, frommds, 
			   le32_to_cpu(rinfo.head->file_caps), 
			   le32_to_cpu(rinfo.head->file_caps_seq));
	if (IS_ERR(cap)) {
		err = PTR_ERR(cap);
		goto out;
	}
	ci = ceph_inode(inode);
	ci->i_nr_by_mode[FILE_MODE_PIN]++;

	root = d_alloc_root(inode);
	if (root == NULL) {
		err = -ENOMEM;
		/* fixme: also close? */
		goto out;
	}
	sb->s_root = root;
	dout(30, "open_root_inode success.\n");
	return 0;

out:
	dout(30, "open_root_inode failure %d\n", err);
	iput(inode);
	return err;
}

static int ceph_set_super(struct super_block *s, void *data)
{
	struct ceph_mount_args *args = data;
	struct ceph_super_info *sbinfo;
	int ret;

	dout(10, "set_super %p data %p\n", s, data);

	s->s_flags = args->mntflags;

	sbinfo = kzalloc(sizeof(struct ceph_super_info), GFP_KERNEL);
	if (!sbinfo)
		return -ENOMEM;
	s->s_fs_info = sbinfo;

	/* fill sbinfo */
	s->s_op = &ceph_sops;
	memcpy(&sbinfo->mount_args, args, sizeof(*args));

	/* create client */
	sbinfo->sb_client = ceph_create_client(args, s);
	if (IS_ERR(sbinfo->sb_client)) {
		ret = PTR_ERR(sbinfo->sb_client);
		sbinfo->sb_client = 0;
		goto bail2;
	}

	ret = set_anon_super(s, 0);  /* what is the second arg for? */
	if (ret != 0)
		goto bail;

	return ret;

bail2:
	ceph_put_client(sbinfo->sb_client);
bail:
	kfree(s->s_fs_info);
	s->s_fs_info = 0;
	return ret;
}

/*
 * share superblock if same fs AND options
 */
static int ceph_compare_super(struct super_block *sb, void *data)
{
	struct ceph_mount_args *args = (struct ceph_mount_args*)data;
	struct ceph_super_info *other = ceph_sbinfo(sb);
	int i;
	dout(10, "ceph_compare_super %p\n", sb);

	/* either compare fsid, or specified mon_hostname */
	if (args->flags & CEPH_MOUNT_FSID) {
		if (!ceph_fsid_equal(&args->fsid, &other->sb_client->fsid)) {
			dout(30, "fsid doesn't match\n");
			return 0;
		}
	} else {
		/* do we share (a) monitor? */
		for (i=0; i<args->num_mon; i++)
			if (ceph_monmap_contains(other->sb_client->monc.monmap, 
						 &args->mon_addr[i]))
				break;
		if (i == args->num_mon) {
			dout(30, "mon ip not part of monmap\n");
			return 0;
		}
		dout(10, "mon ip matches existing sb %p\n", sb);
	}
	if (args->mntflags != other->mount_args.mntflags) {
		dout(30, "flags differ\n");
		return 0;
	}
	return 1;
}

static int ceph_get_sb(struct file_system_type *fs_type,
		       int flags, const char *dev_name, void *data, struct vfsmount *mnt)
{
	struct super_block *sb;
	struct ceph_mount_args mount_args;
	struct ceph_super_info *sbinfo;
	int err;
	int (*compare_super)(struct super_block *, void *) = ceph_compare_super;

	dout(25, "ceph_get_sb\n");
	
	err = parse_mount_args(flags, data, dev_name, &mount_args);
	if (err < 0) 
		goto out;

	if (mount_args.flags & CEPH_MOUNT_NOSHARE)
		compare_super = 0;
	
	/* superblock */
	sb = sget(fs_type, compare_super, ceph_set_super, &mount_args);
	if (IS_ERR(sb)) {
		err = PTR_ERR(sb);
		goto out;
	}
	sbinfo = ceph_sbinfo(sb);

	/* open root */
	dout(30, "ceph_get_sb opening base mountpoint\n");
	if ((err = open_root_inode(sb, &mount_args)) < 0) 
		goto out_splat;
	
	dout(30, "ceph_get_sb %p finishing\n", sb);
        return simple_set_mnt(mnt, sb);
	
out_splat:
	up_write(&sb->s_umount);
	deactivate_super(sb);
out:
	dout(25, "ceph_get_sb fail %d\n", err);
	return err;
}

static void ceph_kill_sb(struct super_block *s)
{
	struct ceph_super_info *sbinfo = ceph_sbinfo(s);
	dout(1, "kill_sb %p\n", s);
	kill_anon_super(s);
	if (sbinfo->sb_client)
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
	destroy_inodecache();
}

module_init(init_ceph);
module_exit(exit_ceph);

MODULE_AUTHOR("Patience Warnick <patience@newdream.net>");
MODULE_AUTHOR("Sage Weil <sage@newdream.net>");
MODULE_DESCRIPTION("Ceph filesystem for Linux");
MODULE_LICENSE("GPL");
