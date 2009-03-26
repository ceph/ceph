#include <linux/module.h>
#include <linux/parser.h>
#include <linux/fs.h>
#include <linux/mount.h>
#include <linux/rwsem.h>
#include <linux/seq_file.h>
#include <linux/sched.h>
#include <linux/string.h>
#include <linux/version.h>
#include <linux/backing-dev.h>
#include <linux/statfs.h>

/* debug levels; defined in super.h */

#include "ceph_debug.h"
#include "ceph_ver.h"
#include "bookkeeper.h"
#include "decode.h"

/*
 * global debug value.
 *  0 = quiet.
 *
 * if the per-file debug level >= 0, then that overrides this  global
 * debug level.
 */
int ceph_debug __read_mostly = 1;
int ceph_debug_mask __read_mostly = 0xffffffff;
/* if true, send output to KERN_INFO (console) instead of KERN_DEBUG. */
int ceph_debug_console __read_mostly;
int ceph_debug_super __read_mostly = -1;   /* for this file */

#define DOUT_MASK DOUT_MASK_SUPER
#define DOUT_VAR ceph_debug_super
#include "super.h"

#include "mon_client.h"

void ceph_dispatch(void *p, struct ceph_msg *msg);
void ceph_peer_reset(void *p, struct ceph_entity_addr *peer_addr,
		     struct ceph_entity_name *peer_name);

/*
 * super ops
 */
static int ceph_write_inode(struct inode *inode, int unused)
{
	struct ceph_inode_info *ci = ceph_inode(inode);

	if (memcmp(&ci->i_old_atime, &inode->i_atime, sizeof(struct timeval))) {
		dout(30, "ceph_write_inode %llx.%llx .. atime updated\n",
		     ceph_vinop(inode));
		/* maybe someday we will push this async to mds? */
	}
	return 0;
}

static void ceph_put_super(struct super_block *s)
{
	struct ceph_client *cl = ceph_client(s);
	int rc;
	int seconds = 15;

	dout(30, "put_super\n");
	ceph_mdsc_close_sessions(&cl->mdsc);
	ceph_monc_request_umount(&cl->monc);

	if (cl->mount_state != CEPH_MOUNT_SHUTDOWN) {
		rc = wait_event_timeout(cl->mount_wq,
				(cl->mount_state == CEPH_MOUNT_UNMOUNTED),
				seconds*HZ);
		if (rc == 0)
			derr(0, "umount timed out after %d seconds\n", seconds);
	}

	return;
}

static int ceph_statfs(struct dentry *dentry, struct kstatfs *buf)
{
	struct ceph_client *client = ceph_inode_to_client(dentry->d_inode);
	struct ceph_monmap *monmap = client->monc.monmap;
	struct ceph_statfs st;
	__le64 fsid;
	int err;

	dout(30, "statfs\n");
	err = ceph_monc_do_statfs(&client->monc, &st);
	if (err < 0)
		return err;

	/* fill in kstatfs */
	buf->f_type = CEPH_SUPER_MAGIC;  /* ?? */

	/*
	 * express utilization in terms of large blocks to avoid
	 * overflow on 32-bit machines.
	 */
	buf->f_bsize = 1 << CEPH_BLOCK_SHIFT;     /* 1 MB */
	buf->f_blocks = le64_to_cpu(st.f_total) >> (CEPH_BLOCK_SHIFT-10);
	buf->f_bfree = le64_to_cpu(st.f_free) >> (CEPH_BLOCK_SHIFT-10);
	buf->f_bavail = le64_to_cpu(st.f_avail) >> (CEPH_BLOCK_SHIFT-10);

	buf->f_files = le64_to_cpu(st.f_objects);
	buf->f_ffree = -1;
	buf->f_namelen = PATH_MAX;
	buf->f_frsize = PAGE_CACHE_SIZE;

	/* leave in little-endian, regardless of host endianness */
	fsid = __ceph_fsid_major(&monmap->fsid) ^ __ceph_fsid_minor(&monmap->fsid);
	buf->f_fsid.val[0] = le64_to_cpu(fsid) & 0xffffffff;
	buf->f_fsid.val[1] = le64_to_cpu(fsid) >> 32;

	return 0;
}


static int ceph_syncfs(struct super_block *sb, int wait)
{
	dout(10, "sync_fs %d\n", wait);
	ceph_osdc_sync(&ceph_client(sb)->osdc);
	return 0;
}


/**
 * ceph_show_options - Show mount options in /proc/mounts
 * @m: seq_file to write to
 * @mnt: mount descriptor
 */
static int ceph_show_options(struct seq_file *m, struct vfsmount *mnt)
{
	struct ceph_client *client = ceph_sb_to_client(mnt->mnt_sb);
	struct ceph_mount_args *args = &client->mount_args;

	if (ceph_debug != 0)
		seq_printf(m, ",debug=%d", ceph_debug);
	if (args->flags & CEPH_MOUNT_FSID)
		seq_printf(m, ",fsidmajor=%llu,fsidminor%llu",
			   __ceph_fsid_major(&args->fsid), __ceph_fsid_minor(&args->fsid));
	if (args->flags & CEPH_MOUNT_NOSHARE)
		seq_puts(m, ",noshare");
	if (args->flags & CEPH_MOUNT_UNSAFE_WRITEBACK)
		seq_puts(m, ",unsafewriteback");
	if (args->flags & CEPH_MOUNT_DIRSTAT)
		seq_puts(m, ",dirstat");
	else
		seq_puts(m, ",nodirstat");
	if (args->flags & CEPH_MOUNT_RBYTES)
		seq_puts(m, ",rbytes");
	else
		seq_puts(m, ",norbytes");
	if (args->flags & CEPH_MOUNT_NOCRC)
		seq_puts(m, ",nocrc");
	return 0;
}


/*
 * inode cache
 */
struct kmem_cache *ceph_inode_cachep;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 27)
static void init_once(void *foo)
#else
static void init_once(struct kmem_cache *cachep, void *foo)
#endif
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

#if LINUX_VERSION_CODE < KERNEL_VERSION(2, 6, 26)
static void ceph_umount_begin(struct vfsmount *vfsmnt, int flags)
#else
static void ceph_umount_begin(struct super_block *sb)
#endif
{
#if LINUX_VERSION_CODE < KERNEL_VERSION(2, 6, 26)
	struct ceph_client *client = ceph_sb_to_client(vfsmnt->mnt_sb);
#else
	struct ceph_client *client = ceph_sb_to_client(sb);
#endif

	dout(30, "ceph_umount_begin\n");

#if LINUX_VERSION_CODE < KERNEL_VERSION(2, 6, 26)
	if (!(flags & MNT_FORCE))
		return;
#endif

	if (!client)
		return;

	client->mount_state = CEPH_MOUNT_SHUTDOWN;
	return;
}


static const struct super_operations ceph_super_ops = {
	.alloc_inode	= ceph_alloc_inode,
	.destroy_inode	= ceph_destroy_inode,
	.write_inode    = ceph_write_inode,
	.sync_fs        = ceph_syncfs,
	.put_super	= ceph_put_super,
	.show_options   = ceph_show_options,
	.statfs		= ceph_statfs,
	.umount_begin   = ceph_umount_begin,
};



/*
 * The monitor responds with mount ack indicate mount success.  The
 * included client ticket allows the client to talk to MDSs and OSDs.
 */
static int handle_mount_ack(struct ceph_client *client, struct ceph_msg *msg)
{
	struct ceph_monmap *monmap = NULL, *old = client->monc.monmap;
	void *p, *end;
	s32 result;
	u32 len;
	int err = -EINVAL;

	if (client->signed_ticket) {
		dout(2, "handle_mount_ack - already mounted\n");
		return 0;
	}

	dout(2, "handle_mount_ack\n");
	p = msg->front.iov_base;
	end = p + msg->front.iov_len;

	ceph_decode_32_safe(&p, end, result, bad);
	ceph_decode_32_safe(&p, end, len, bad);
	if (result) {
		dout(0, "mount denied: %.*s (%d)\n", len, (char *)p, result);
		return result;
	}
	p += len;

	ceph_decode_32_safe(&p, end, len, bad);
	ceph_decode_need(&p, end, len, bad);
	monmap = ceph_monmap_decode(p, p + len);
	if (IS_ERR(monmap)) {
		derr(0, "problem decoding monmap, %d\n", (int)PTR_ERR(monmap));
		return -EINVAL;
	}
	p += len;

	ceph_decode_32_safe(&p, end, len, bad);
	dout(0, "ticket len %d\n", len);
	ceph_decode_need(&p, end, len, bad);

	client->signed_ticket = kmalloc(len, GFP_KERNEL);
	if (!client->signed_ticket) {
		derr(0, "problem allocating %d bytes for client ticket\n",
		     len);
		err = -ENOMEM;
		goto out;
	}

	memcpy(client->signed_ticket, p, len);
	client->signed_ticket_len = len;

	client->monc.monmap = monmap;
	kfree(old);

	client->whoami = le32_to_cpu(msg->hdr.dst.name.num);
	client->msgr->inst.name = msg->hdr.dst.name;
	dout(1, "i am client%d, fsid is %llx.%llx\n", client->whoami,
	     le64_to_cpu(__ceph_fsid_major(&client->monc.monmap->fsid)),
	     le64_to_cpu(__ceph_fsid_minor(&client->monc.monmap->fsid)));
	ceph_debugfs_client_init(client);
	return 0;

bad:
	derr(0, "error decoding mount_ack message\n");
out:
	kfree(monmap);
	return err;
}

const char *ceph_msg_type_name(int type)
{
	switch (type) {
	case CEPH_MSG_SHUTDOWN: return "shutdown";
	case CEPH_MSG_PING: return "ping";
	case CEPH_MSG_MON_MAP: return "mon_map";
	case CEPH_MSG_MON_GET_MAP: return "mon_get_map";
	case CEPH_MSG_CLIENT_MOUNT: return "client_mount";
	case CEPH_MSG_CLIENT_MOUNT_ACK: return "client_mount_ack";
	case CEPH_MSG_CLIENT_UNMOUNT: return "client_unmount";
	case CEPH_MSG_STATFS: return "statfs";
	case CEPH_MSG_STATFS_REPLY: return "statfs_reply";
	case CEPH_MSG_MDS_GETMAP: return "mds_getmap";
	case CEPH_MSG_MDS_MAP: return "mds_map";
	case CEPH_MSG_CLIENT_SESSION: return "client_session";
	case CEPH_MSG_CLIENT_RECONNECT: return "client_reconnect";
	case CEPH_MSG_CLIENT_REQUEST: return "client_request";
	case CEPH_MSG_CLIENT_REQUEST_FORWARD: return "client_request_forward";
	case CEPH_MSG_CLIENT_REPLY: return "client_reply";
	case CEPH_MSG_CLIENT_CAPS: return "client_caps";
	case CEPH_MSG_CLIENT_SNAP: return "client_snap";
	case CEPH_MSG_CLIENT_LEASE: return "client_lease";
	case CEPH_MSG_OSD_GETMAP: return "osd_getmap";
	case CEPH_MSG_OSD_MAP: return "osd_map";
	case CEPH_MSG_OSD_OP: return "osd_op";
	case CEPH_MSG_OSD_OPREPLY: return "osd_opreply";
	default: return "unknown";
	}
}

/*
 * Called when a message socket is explicitly reset by a peer.
 */
void ceph_peer_reset(void *p, struct ceph_entity_addr *peer_addr,
		     struct ceph_entity_name *peer_name)
{
	struct ceph_client *client = p;

	dout(30, "ceph_peer_reset %s%d\n", ENTITY_NAME(*peer_name));
	switch (le32_to_cpu(peer_name->type)) {
	case CEPH_ENTITY_TYPE_MDS:
		ceph_mdsc_handle_reset(&client->mdsc,
					      le32_to_cpu(peer_name->num));
		break;
	case CEPH_ENTITY_TYPE_OSD:
		ceph_osdc_handle_reset(&client->osdc, peer_addr);
		break;
	}
}


/*
 * mount options
 */
enum {
	Opt_fsidmajor,
	Opt_fsidminor,
	Opt_debug,
	Opt_debug_console,
	Opt_debug_msgr,
	Opt_debug_mdsc,
	Opt_debug_osdc,
	Opt_debug_addr,
	Opt_debug_inode,
	Opt_debug_snap,
	Opt_debug_ioctl,
	Opt_debug_caps,
	Opt_monport,
	Opt_port,
	Opt_wsize,
	Opt_rsize,
	Opt_osdtimeout,
	Opt_mount_timeout,
	Opt_caps_delay,
	/* int args above */
	Opt_ip,
	Opt_noshare,
	Opt_unsafewriteback,
	Opt_safewriteback,
	Opt_dirstat,
	Opt_nodirstat,
	Opt_rbytes,
	Opt_norbytes,
	Opt_nocrc,
};

static match_table_t arg_tokens = {
	{Opt_fsidmajor, "fsidmajor=%ld"},
	{Opt_fsidminor, "fsidminor=%ld"},
	{Opt_debug, "debug=%d"},
	{Opt_debug_msgr, "debug_msgr=%d"},
	{Opt_debug_mdsc, "debug_mdsc=%d"},
	{Opt_debug_osdc, "debug_osdc=%d"},
	{Opt_debug_addr, "debug_addr=%d"},
	{Opt_debug_inode, "debug_inode=%d"},
	{Opt_debug_snap, "debug_snap=%d"},
	{Opt_debug_ioctl, "debug_ioctl=%d"},
	{Opt_debug_caps, "debug_caps=%d"},
	{Opt_monport, "monport=%d"},
	{Opt_port, "port=%d"},
	{Opt_wsize, "wsize=%d"},
	{Opt_rsize, "rsize=%d"},
	{Opt_osdtimeout, "osdtimeout=%d"},
	{Opt_mount_timeout, "mount_timeout=%d"},
	{Opt_caps_delay, "caps_delay=%d"},
	/* int args above */
	{Opt_ip, "ip=%s"},
	{Opt_debug_console, "debug_console"},
	{Opt_noshare, "noshare"},
	{Opt_unsafewriteback, "unsafewriteback"},
	{Opt_safewriteback, "safewriteback"},
	{Opt_dirstat, "dirstat"},
	{Opt_nodirstat, "nodirstat"},
	{Opt_rbytes, "rbytes"},
	{Opt_norbytes, "norbytes"},
	{Opt_nocrc, "nocrc"},
	{-1, NULL}
};

#define ADDR_DELIM(c) ((!c) || (c == ':') || (c == ','))

/*
 * FIXME: add error checking to ip parsing
 */
static int parse_ip(const char *c, int len, struct ceph_entity_addr *addr,
		    int max_count, int *count)
{
	int i;
	int v;
	int mon_count;
	unsigned ip = 0;
	const char *p = c, *numstart;

	dout(15, "parse_ip on '%s' len %d\n", c, len);
	for (mon_count = 0; mon_count < max_count; mon_count++) {
		for (i = 0; !ADDR_DELIM(*p) && i < 4; i++) {
			v = 0;
			numstart = p;
			while (!ADDR_DELIM(*p) && *p != '.' && p < c+len) {
				if (*p < '0' || *p > '9')
					goto bad;
				v = (v * 10) + (*p - '0');
				p++;
			}
			if (v > 255 || numstart == p)
				goto bad;
			ip = (ip << 8) + v;

			if (*p == '.')
				p++;
		}
		if (i != 4)
			goto bad;
		*(__be32 *)&addr[mon_count].ipaddr.sin_addr.s_addr = htonl(ip);

		/* port? */
		if (*p == ':') {
			p++;
			numstart = p;
			v = 0;
			while (!ADDR_DELIM(*p) && *p != '.' && p < c+len) {
				if (*p < '0' || *p > '9')
					goto bad;
				v = (v * 10) + (*p - '0');
				p++;
			}
			if (v > 65535 || numstart == p)
				goto bad;
			addr[mon_count].ipaddr.sin_port = htons(v);
		} else
			addr[mon_count].ipaddr.sin_port = htons(CEPH_MON_PORT);

		dout(15, "parse_ip got %u.%u.%u.%u:%u\n",
		     IPQUADPORT(addr[mon_count].ipaddr));

		if (*p != ',')
			break;
		p++;
	}

	if (p < c+len)
		goto bad;

	if (count)
		*count = mon_count + 1;

	return 0;

bad:
	derr(1, "parse_ip bad ip '%s'\n", c);
	return -EINVAL;
}

static int parse_mount_args(int flags, char *options, const char *dev_name,
			    struct ceph_mount_args *args, const char **path)
{
	char *c;
	int len, err;
	substring_t argstr[MAX_OPT_ARGS];
	int i;

	dout(15, "parse_mount_args dev_name '%s'\n", dev_name);
	memset(args, 0, sizeof(*args));

	/* defaults */
	args->sb_flags = flags;
	args->flags = CEPH_MOUNT_DEFAULT;
	args->osd_timeout = 5;    /* seconds */
	args->mount_timeout = CEPH_MOUNT_TIMEOUT_DEFAULT; /* seconds */
	args->caps_delay = CEPH_CAP_DELAY_DEFAULT; /* seconds */
	args->snapdir_name = ".snap";

	/* ip1[:port1][,ip2[:port2]...]:/subdir/in/fs */
	c = strstr(dev_name, ":/");
	if (c == NULL)
		return -EINVAL;
	*c = 0;

	/* get mon ip(s) */
	len = c - dev_name;
	err = parse_ip(dev_name, len, args->mon_addr, MAX_MON_MOUNT_ADDR,
		       &args->num_mon);
	if (err < 0)
		return err;

	for (i = 0; i < args->num_mon; i++) {
		args->mon_addr[i].ipaddr.sin_family = AF_INET;
		args->mon_addr[i].erank = 0;
		args->mon_addr[i].nonce = 0;
	}
	args->my_addr.ipaddr.sin_family = AF_INET;
	args->my_addr.ipaddr.sin_addr.s_addr = htonl(0);
	args->my_addr.ipaddr.sin_port = htons(0);

	/* path on server */
	c++;
	while (*c == '/')
		c++;  /* remove leading '/'(s) */
	*path = c;
	dout(15, "server path '%s'\n", *path);

	/* parse mount options */
	while ((c = strsep(&options, ",")) != NULL) {
		int token, intval, ret;
		if (!*c)
			continue;
		token = match_token(c, arg_tokens, argstr);
		if (token < 0) {
			derr(0, "bad mount option at '%s'\n", c);
			return -EINVAL;

		}
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
			__ceph_fsid_set_major(&args->fsid, cpu_to_le64(intval));
			break;
		case Opt_fsidminor:
			__ceph_fsid_set_minor(&args->fsid, cpu_to_le64(intval));
			break;
		case Opt_port:
			args->my_addr.ipaddr.sin_port = htons(intval);
			break;
		case Opt_ip:
			err = parse_ip(argstr[0].from,
					argstr[0].to-argstr[0].from,
					&args->my_addr,
					1, NULL);
			if (err < 0)
				return err;
			args->flags |= CEPH_MOUNT_MYIP;
			break;

			/* debug levels */
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
		case Opt_debug_addr:
			ceph_debug_addr = intval;
			break;
		case Opt_debug_inode:
			ceph_debug_inode = intval;
			break;
		case Opt_debug_snap:
			ceph_debug_snap = intval;
			break;
		case Opt_debug_ioctl:
			ceph_debug_ioctl = intval;
			break;
		case Opt_debug_caps:
			ceph_debug_caps = intval;
			break;
		case Opt_debug_console:
			ceph_debug_console = 1;
			break;

			/* misc */
		case Opt_wsize:
			args->wsize = intval;
			break;
		case Opt_rsize:
			args->rsize = intval;
			break;
		case Opt_osdtimeout:
			args->osd_timeout = intval;
			break;
		case Opt_mount_timeout:
			args->mount_timeout = intval;
			break;
		case Opt_caps_delay:
			args->caps_delay = intval;
			break;

		case Opt_noshare:
			args->flags |= CEPH_MOUNT_NOSHARE;
			break;
		case Opt_unsafewriteback:
			args->flags |= CEPH_MOUNT_UNSAFE_WRITEBACK;
			break;
		case Opt_safewriteback:
			args->flags &= ~CEPH_MOUNT_UNSAFE_WRITEBACK;
			break;

		case Opt_dirstat:
			args->flags |= CEPH_MOUNT_DIRSTAT;
			break;
		case Opt_nodirstat:
			args->flags &= ~CEPH_MOUNT_DIRSTAT;
			break;
		case Opt_rbytes:
			args->flags |= CEPH_MOUNT_RBYTES;
			break;
		case Opt_norbytes:
			args->flags &= ~CEPH_MOUNT_RBYTES;
			break;
		case Opt_nocrc:
			args->flags |= CEPH_MOUNT_NOCRC;
			break;

		default:
			BUG_ON(token);
		}
	}

	return 0;
}

/*
 * create a fresh client instance
 */
static struct ceph_client *ceph_create_client(void)
{
	struct ceph_client *client;
	int err = -ENOMEM;

	client = kzalloc(sizeof(*client), GFP_KERNEL);
	if (client == NULL)
		return ERR_PTR(-ENOMEM);

	mutex_init(&client->mount_mutex);

	init_waitqueue_head(&client->mount_wq);

	client->sb = NULL;
	client->mount_state = CEPH_MOUNT_MOUNTING;
	client->whoami = -1;

	client->msgr = NULL;

	client->mount_err = 0;
	client->signed_ticket = NULL;
	client->signed_ticket_len = 0;

	client->wb_wq = create_workqueue("ceph-writeback");
	if (client->wb_wq == NULL)
		goto fail;
	client->pg_inv_wq = create_workqueue("ceph-pg-invalid");
	if (client->pg_inv_wq == NULL)
		goto fail;
	client->trunc_wq = create_workqueue("ceph-trunc");
	if (client->trunc_wq == NULL)
		goto fail;

	/* subsystems */
	err = ceph_monc_init(&client->monc, client);
	if (err < 0)
		return ERR_PTR(err);
	ceph_mdsc_init(&client->mdsc, client);
	ceph_osdc_init(&client->osdc, client);

	return client;

fail:
	return ERR_PTR(-ENOMEM);
}

static void ceph_destroy_client(struct ceph_client *client)
{
	dout(10, "destroy_client %p\n", client);

	/* unmount */
	ceph_mdsc_stop(&client->mdsc);
	ceph_monc_stop(&client->monc);
	ceph_osdc_stop(&client->osdc);

	kfree(client->signed_ticket);

	ceph_debugfs_client_cleanup(client);
	if (client->wb_wq)
		destroy_workqueue(client->wb_wq);
	if (client->pg_inv_wq)
		destroy_workqueue(client->pg_inv_wq);
	if (client->trunc_wq)
		destroy_workqueue(client->trunc_wq);
	if (client->msgr)
		ceph_messenger_destroy(client->msgr);
	kfree(client);
	dout(10, "destroy_client %p done\n", client);
}

/*
 * true if we have the mon, osd, and mds maps, and are thus
 * fully "mounted".
 */
static int have_all_maps(struct ceph_client *client)
{
	return client->osdc.osdmap && client->osdc.osdmap->epoch &&
		client->monc.monmap && client->monc.monmap->epoch &&
		client->mdsc.mdsmap && client->mdsc.mdsmap->m_epoch;
}

/*
 * Bootstrap mount by opening the root directory.  Note the mount
 * @started time from caller, and time out if this takes too long.
 */
static struct dentry *open_root_dentry(struct ceph_client *client,
				       const char *path,
				       unsigned long started)
{
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req = NULL;
	int err;
	struct dentry *root;

	/* open dir */
	dout(30, "open_root_inode opening '%s'\n", path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_LSTAT, USE_ANY_MDS);
	if (IS_ERR(req))
		return ERR_PTR(PTR_ERR(req));
	req->r_path1 = path;
	req->r_started = started;
	req->r_timeout = client->mount_args.mount_timeout * HZ;
	req->r_args.stat.mask = cpu_to_le32(CEPH_STAT_CAP_INODE);
	err = ceph_mdsc_do_request(mdsc, NULL, req);
	if (err == 0) {
		root = d_obtain_alias(req->r_target_inode);
		dout(30, "open_root_inode success, root dentry is %p\n", root);
	} else {
		root = ERR_PTR(err);
	}
	ceph_mdsc_put_request(req);
	return root;
}

/*
 * mount: join the ceph cluster.
 */
static int ceph_mount(struct ceph_client *client, struct vfsmount *mnt,
		      const char *path)
{
	struct ceph_entity_addr *myaddr = NULL;
	struct ceph_msg *mount_msg;
	int err;
	int request_interval = 5 * HZ;
	unsigned long timeout = client->mount_args.mount_timeout * HZ;
	unsigned long started = jiffies;  /* note the start time */
	int which;
	struct dentry *root;
	unsigned char r;

	dout(10, "mount start\n");
	mutex_lock(&client->mount_mutex);

	/* initialize the messenger */
	if (client->msgr == NULL) {
		if (client->mount_args.flags & CEPH_MOUNT_MYIP)
			myaddr = &client->mount_args.my_addr;
		client->msgr = ceph_messenger_create(myaddr);
		if (IS_ERR(client->msgr)) {
			err = PTR_ERR(client->msgr);
			client->msgr = NULL;
			goto out;
		}
		client->msgr->parent = client;
		client->msgr->dispatch = ceph_dispatch;
		client->msgr->prepare_pages = ceph_osdc_prepare_pages;
		client->msgr->peer_reset = ceph_peer_reset;
	}

	/* send mount request, and wait for mon, mds, and osd maps */
	while (!have_all_maps(client)) {
		err = -EIO;
		if (timeout && time_after_eq(jiffies, started + timeout))
			goto out;
		dout(10, "mount sending mount request\n");
		get_random_bytes(&r, 1);
		which = r % client->mount_args.num_mon;
		mount_msg = ceph_msg_new(CEPH_MSG_CLIENT_MOUNT, 0, 0, 0, NULL);
		if (IS_ERR(mount_msg)) {
			err = PTR_ERR(mount_msg);
			goto out;
		}
		mount_msg->hdr.dst.name.type =
			cpu_to_le32(CEPH_ENTITY_TYPE_MON);
		mount_msg->hdr.dst.name.num = cpu_to_le32(which);
		mount_msg->hdr.dst.addr = client->mount_args.mon_addr[which];

		ceph_msg_send(client->msgr, mount_msg, 0);

		/* wait */
		dout(10, "mount sent to mon%d, waiting for maps\n", which);
		err = wait_event_interruptible_timeout(client->mount_wq,
						       have_all_maps(client),
						       request_interval);
		if (err == -EINTR)
			goto out;
		if (client->mount_err) {
			err = client->mount_err;
			goto out;
		}
	}

	dout(30, "mount opening base mountpoint\n");
	root = open_root_dentry(client, path, started);
	if (IS_ERR(root)) {
		err = PTR_ERR(root);
		goto out;
	}

	mnt->mnt_root = root;
	mnt->mnt_sb = client->sb;
	client->mount_state = CEPH_MOUNT_MOUNTED;
	dout(10, "mount success\n");
	err = 0;

out:
	mutex_unlock(&client->mount_mutex);
	return err;
}


/*
 * Process an incoming message.
 *
 * This should be relatively fast and must not do any work that waits
 * on other messages to be received.
 */
void ceph_dispatch(void *p, struct ceph_msg *msg)
{
	struct ceph_client *client = p;
	int had;
	int type = le16_to_cpu(msg->hdr.type);

	switch (type) {
	case CEPH_MSG_CLIENT_MOUNT_ACK:
		had = client->signed_ticket ? 1 : 0;
		client->mount_err = handle_mount_ack(client, msg);
		if (client->mount_err ||
		    (!had && client->signed_ticket && have_all_maps(client)))
			wake_up(&client->mount_wq);
		break;

		/* mon client */
	case CEPH_MSG_STATFS_REPLY:
		ceph_monc_handle_statfs_reply(&client->monc, msg);
		break;
	case CEPH_MSG_CLIENT_UNMOUNT:
		ceph_monc_handle_umount(&client->monc, msg);
		break;

		/* mds client */
	case CEPH_MSG_MDS_MAP:
		had = client->mdsc.mdsmap ? 1 : 0;
		ceph_mdsc_handle_map(&client->mdsc, msg);
		if (!had && client->mdsc.mdsmap && have_all_maps(client))
			wake_up(&client->mount_wq);
		break;
	case CEPH_MSG_CLIENT_SESSION:
		ceph_mdsc_handle_session(&client->mdsc, msg);
		break;
	case CEPH_MSG_CLIENT_REPLY:
		ceph_mdsc_handle_reply(&client->mdsc, msg);
		break;
	case CEPH_MSG_CLIENT_REQUEST_FORWARD:
		ceph_mdsc_handle_forward(&client->mdsc, msg);
		break;
	case CEPH_MSG_CLIENT_CAPS:
		ceph_handle_caps(&client->mdsc, msg);
		break;
	case CEPH_MSG_CLIENT_SNAP:
		ceph_handle_snap(&client->mdsc, msg);
		break;
	case CEPH_MSG_CLIENT_LEASE:
		ceph_mdsc_handle_lease(&client->mdsc, msg);
		break;

		/* osd client */
	case CEPH_MSG_OSD_MAP:
		had = client->osdc.osdmap ? 1 : 0;
		ceph_osdc_handle_map(&client->osdc, msg);
		if (!had && client->osdc.osdmap && have_all_maps(client))
			wake_up(&client->mount_wq);
		break;
	case CEPH_MSG_OSD_OPREPLY:
		ceph_osdc_handle_reply(&client->osdc, msg);
		break;

	default:
		derr(0, "received unknown message type %d %s\n", type,
		     ceph_msg_type_name(type));
	}

	ceph_msg_put(msg);
}


static int ceph_set_super(struct super_block *s, void *data)
{
	struct ceph_client *client = data;
	int ret;

	dout(10, "set_super %p data %p\n", s, data);

	s->s_flags = client->mount_args.sb_flags;
	s->s_maxbytes = min((u64)MAX_LFS_FILESIZE, CEPH_FILE_MAX_SIZE);

	s->s_fs_info = client;
	client->sb = s;

	s->s_op = &ceph_super_ops;
	s->s_export_op = &ceph_export_ops;

	s->s_time_gran = 1000;  /* 1000 ns == 1 us */

	ret = set_anon_super(s, NULL);  /* what is that second arg for? */
	if (ret != 0)
		goto fail;

	return ret;

fail:
	s->s_fs_info = NULL;
	client->sb = NULL;
	return ret;
}

/*
 * share superblock if same fs AND options
 */
static int ceph_compare_super(struct super_block *sb, void *data)
{
	struct ceph_client *new = data;
	struct ceph_mount_args *args = &new->mount_args;
	struct ceph_client *other = ceph_sb_to_client(sb);
	int i;
	dout(10, "ceph_compare_super %p\n", sb);

	/* either compare fsid, or specified mon_hostname */
	if (args->flags & CEPH_MOUNT_FSID) {
		if (ceph_fsid_compare(&args->fsid, &other->fsid)) {
			dout(30, "fsid doesn't match\n");
			return 0;
		}
	} else {
		/* do we share (a) monitor? */
		for (i = 0; i < args->num_mon; i++)
			if (ceph_monmap_contains(other->monc.monmap,
						 &args->mon_addr[i]))
				break;
		if (i == args->num_mon) {
			dout(30, "mon ip not part of monmap\n");
			return 0;
		}
		dout(10, "mon ip matches existing sb %p\n", sb);
	}
	if (args->sb_flags != other->mount_args.sb_flags) {
		dout(30, "flags differ\n");
		return 0;
	}
	return 1;
}

/*
 * construct our own bdi so we can control readahead
 */
static int ceph_init_bdi(struct super_block *sb, struct ceph_client *client)
{
	int err;

	if (client->mount_args.rsize)
		client->backing_dev_info.ra_pages =
			(client->mount_args.rsize + PAGE_CACHE_SIZE - 1)
			>> PAGE_SHIFT;

	if (client->backing_dev_info.ra_pages < (PAGE_CACHE_SIZE >> PAGE_SHIFT))
		client->backing_dev_info.ra_pages =
			CEPH_DEFAULT_READ_SIZE >> PAGE_SHIFT;

	err = bdi_init(&client->backing_dev_info);

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 26)
	if (err < 0)
		return err;

	err = bdi_register_dev(&client->backing_dev_info, sb->s_dev);
#endif

	return err;
}

static int ceph_get_sb(struct file_system_type *fs_type,
		       int flags, const char *dev_name, void *data,
		       struct vfsmount *mnt)
{
	struct super_block *sb;
	struct ceph_client *client;
	int err;
	int (*compare_super)(struct super_block *, void *) = ceph_compare_super;
	const char *path;

	dout(25, "ceph_get_sb\n");

	/* create client (which we may/may not use) */
	client = ceph_create_client();
	if (IS_ERR(client))
		return PTR_ERR(client);

	err = parse_mount_args(flags, data, dev_name,
			       &client->mount_args, &path);
	if (err < 0)
		goto out;

	if (client->mount_args.flags & CEPH_MOUNT_NOSHARE)
		compare_super = NULL;

	sb = sget(fs_type, compare_super, ceph_set_super, client);
	if (IS_ERR(sb)) {
		err = PTR_ERR(sb);
		goto out;
	}

	if (ceph_client(sb) != client) {
		ceph_destroy_client(client);
		client = ceph_client(sb);
		dout(20, "get_sb got existing client %p\n", client);
	} else {
		dout(20, "get_sb using new client %p\n", client);
		err = ceph_init_bdi(sb, client);
		if (err < 0)
			goto out_splat;
	}

	err = ceph_mount(client, mnt, path);
	if (err < 0)
		goto out_splat;
	dout(22, "hi mnt_root %p\n", mnt->mnt_root);
	dout(22, "root ino %llx.%llx\n", ceph_vinop(mnt->mnt_root->d_inode));
	return 0;

out_splat:
	ceph_mdsc_close_sessions(&client->mdsc);
	up_write(&sb->s_umount);
	deactivate_super(sb);
	goto out_final;
out:
	ceph_destroy_client(client);
out_final:
	dout(25, "ceph_get_sb fail %d\n", err);
	return err;
}

static void ceph_kill_sb(struct super_block *s)
{
	struct ceph_client *client = ceph_sb_to_client(s);
	dout(1, "kill_sb %p\n", s);
	ceph_mdsc_pre_umount(&client->mdsc);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 26)
	bdi_unregister(&client->backing_dev_info);
#endif
	kill_anon_super(s);    /* will call put_super after sb is r/o */
	bdi_destroy(&client->backing_dev_info);
	ceph_destroy_client(client);
}


/************************************/

static struct file_system_type ceph_fs_type = {
	.owner		= THIS_MODULE,
	.name		= "ceph",
	.get_sb		= ceph_get_sb,
	.kill_sb	= ceph_kill_sb,
	.fs_flags	= FS_RENAME_DOES_D_MOVE,
};

static int __init init_ceph(void)
{
	int ret = 0;

	dout(1, "init_ceph\n");
	dout(0, "ceph (%s)\n", STRINGIFY(CEPH_GIT_VER));

#ifdef CONFIG_CEPH_BOOKKEEPER
	ceph_bookkeeper_init();
#endif

	ret = ceph_debugfs_init();
	if (ret < 0)
		goto out;

	ret = ceph_msgr_init();
	if (ret < 0)
		goto out_debugfs;

	ret = init_inodecache();
	if (ret)
		goto out_msgr;

	ret = register_filesystem(&ceph_fs_type);
	if (ret)
		goto out_icache;
	return 0;

out_icache:
	destroy_inodecache();
out_msgr:
	ceph_msgr_exit();
out_debugfs:
	ceph_debugfs_cleanup();
out:
	return ret;
}

static void __exit exit_ceph(void)
{
	dout(1, "exit_ceph\n");
	unregister_filesystem(&ceph_fs_type);
	destroy_inodecache();
	ceph_msgr_exit();
	ceph_debugfs_cleanup();
#ifdef CONFIG_CEPH_BOOKKEEPER
	ceph_bookkeeper_finalize();
#endif
}

module_init(init_ceph);
module_exit(exit_ceph);

MODULE_AUTHOR("Patience Warnick <patience@newdream.net>");
MODULE_AUTHOR("Sage Weil <sage@newdream.net>");
MODULE_AUTHOR("Yehuda Sadeh <yehuda@hq.newdream.net>");
MODULE_DESCRIPTION("Ceph filesystem for Linux");
MODULE_LICENSE("GPL");
