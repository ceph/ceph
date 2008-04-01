
#include <linux/ceph_fs.h>
#include <linux/wait.h>
#include <linux/sched.h>
#include <linux/random.h>
#include <linux/fs.h>
#include <linux/mount.h>

int ceph_client_debug = 50;
#define DOUT_VAR ceph_client_debug
#define DOUT_PREFIX "client: "
#include "super.h"
#include "ktcp.h"


void ceph_dispatch(void *p, struct ceph_msg *msg);
void ceph_peer_reset(void *p, struct ceph_entity_name *peer_name);


/*
 * share work queue between clients.
 */
static spinlock_t ceph_client_spinlock = SPIN_LOCK_UNLOCKED;
static int ceph_num_clients = 0;

static void get_client_counter(void) 
{
	spin_lock(&ceph_client_spinlock);
	if (ceph_num_clients == 0) {
		dout(1, "first client, setting up workqueues\n");
		ceph_workqueue_init();
	}
	ceph_num_clients++;
	spin_unlock(&ceph_client_spinlock);
}

static void put_client_counter(void) 
{
	spin_lock(&ceph_client_spinlock);
	ceph_num_clients--;
	if (ceph_num_clients == 0) {
		dout(1, "last client, shutting down workqueues\n");
		ceph_workqueue_shutdown();
	}
	spin_unlock(&ceph_client_spinlock);
}

static struct dentry *open_root_dentry(struct ceph_client *client, struct ceph_mount_args *args)
{
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req = 0;
	struct ceph_mds_request_head *reqhead;
	int err;
	struct dentry *root;

	/* open dir */
	dout(30, "open_root_inode opening '%s'\n", args->path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_OPEN, 1, args->path, 0, 0);
	if (IS_ERR(req)) 
		return ERR_PTR(PTR_ERR(req));
	req->r_expects_cap = 1;
	reqhead = req->r_request->front.iov_base;
	reqhead->args.open.flags = O_DIRECTORY;
	reqhead->args.open.mode = 0;
	err = ceph_mdsc_do_request(mdsc, req);
	if (err == 0) {
		root = req->r_last_dentry;
		dget(root);
		dout(30, "open_root_inode success, root dentry is %p\n", root);
	} else
		root = ERR_PTR(err);
	ceph_mdsc_put_request(req);
	return root;
}

/*
 * mount: join the ceph cluster.
 */
int ceph_mount(struct ceph_client *client, struct ceph_mount_args *args, struct vfsmount *mnt)
{
	struct ceph_msg *mount_msg;
	struct dentry *root;
	int err;
	int attempts = 10;
	int which;
	char r;

	dout(10, "mount start\n");
	while (client->mounting < 7) {
		get_random_bytes(&r, 1);
		which = r % args->num_mon;
		mount_msg = ceph_msg_new(CEPH_MSG_CLIENT_MOUNT, 0, 0, 0, 0);
		if (IS_ERR(mount_msg))
			return PTR_ERR(mount_msg);
		mount_msg->hdr.dst.name.type = cpu_to_le32(CEPH_ENTITY_TYPE_MON);
		mount_msg->hdr.dst.name.num = cpu_to_le32(which);
		mount_msg->hdr.dst.addr = args->mon_addr[which];
		
		ceph_msg_send(client->msgr, mount_msg, 0);
		dout(10, "mount from mon%d, %d attempts left\n", which, attempts);
		
		/* wait */
		dout(10, "mount sent mount request, waiting for maps\n");
		err = wait_for_completion_timeout(&client->mount_completion, 6*HZ);
		if (err == -EINTR)
			return err; 
		if (client->mounting == 7) 
			break;  /* success */
		dout(10, "mount still waiting for mount, attempts=%d\n", attempts);
		if (--attempts == 0)
			return -EIO;
	}

	dout(30, "mount opening base mountpoint\n");
	root = open_root_dentry(client, args);
	if (IS_ERR(root))
		return PTR_ERR(root);
	mnt->mnt_root = root;
	mnt->mnt_sb = client->sb;
	dout(10, "mount success\n");
	return 0;
}


/*
 * the monitor responds to monmap to indicate mount success.
 * (or, someday, to indicate a change in the monitor cluster?)
 */
static void handle_monmap(struct ceph_client *client, struct ceph_msg *msg)
{
	int err;
	int first = (client->monc.monmap->epoch == 0);
	void *new;

	dout(1, "handle_monmap had epoch %d\n", client->monc.monmap->epoch);
	new = ceph_monmap_decode(msg->front.iov_base, 
				 msg->front.iov_base + msg->front.iov_len);
	if (IS_ERR(new)) {
		err = PTR_ERR(new);
		derr(0, "problem decoding monmap, %d\n", err);
		return;
	}
	kfree(client->monc.monmap);
	client->monc.monmap = new;

	if (first) {
		client->whoami = le32_to_cpu(msg->hdr.dst.name.num);
		client->msgr->inst.name = msg->hdr.dst.name;
		dout(1, "i am client%d\n", client->whoami);
	}
}


void got_first_map(struct ceph_client *client, int num)
{
	set_bit(num, &client->mounting);
	dout(10, "got_first_map num %d mounting now %lu bits %d\n", 
	     num, client->mounting, (int)find_first_zero_bit(&client->mounting, 4));
	if (find_first_zero_bit(&client->mounting, 4) == 3) {
		dout(10, "got_first_map kicking mount\n");
		complete(&client->mount_completion);
	}
}



/*
 * create a fresh client instance
 */
struct ceph_client *ceph_create_client(struct ceph_mount_args *args, struct super_block *sb)
{
	struct ceph_client *cl;
	struct ceph_entity_addr *myaddr = 0;
	int err;

	cl = kzalloc(sizeof(*cl), GFP_KERNEL);
	if (cl == NULL)
		return ERR_PTR(-ENOMEM);

	init_completion(&cl->mount_completion);
	spin_lock_init(&cl->sb_lock);
	get_client_counter();

	/* messenger */
	if (args->flags & CEPH_MOUNT_MYIP)
		myaddr = &args->my_addr;
	cl->msgr = ceph_messenger_create(myaddr);
	if (IS_ERR(cl->msgr)) {
		err = PTR_ERR(cl->msgr);
		goto fail;
	}
	cl->msgr->parent = cl;
	cl->msgr->dispatch = ceph_dispatch;
	cl->msgr->prepare_pages = ceph_osdc_prepare_pages;
	cl->msgr->peer_reset = ceph_peer_reset;
	
	cl->whoami = -1;
	if ((err = ceph_monc_init(&cl->monc, cl)) < 0)
		goto fail;
	ceph_mdsc_init(&cl->mdsc, cl);
	ceph_osdc_init(&cl->osdc, cl);

	cl->sb = sb;
	cl->mounting = 0;  /* wait for mon+mds+osd */

	return cl;

fail:
	put_client_counter();
	kfree(cl);
	return ERR_PTR(err);
}

void ceph_destroy_client(struct ceph_client *cl)
{
	dout(10, "destroy_client %p\n", cl);

	/* unmount */
	/* ... */

	ceph_mdsc_stop(&cl->mdsc);	

	ceph_messenger_destroy(cl->msgr);
	put_client_counter();
	kfree(cl);
	dout(10, "destroy_client %p done\n", cl);
}


/*
 * dispatch -- called with incoming messages.
 *
 * should be fast and non-blocking, as it is called with locks held.
 */
void ceph_dispatch(void *p, struct ceph_msg *msg)
{
	struct ceph_client *client = p;
	int had;
	int type = le32_to_cpu(msg->hdr.type);

	/* deliver the message */
	switch (type) {
		/* me */
	case CEPH_MSG_MON_MAP:
		had = client->monc.monmap->epoch ? 1:0;
		handle_monmap(client, msg);
		if (!had && client->monc.monmap->epoch)
			got_first_map(client, 0);
		break;

		/* mon client */
	case CEPH_MSG_STATFS_REPLY:
		ceph_monc_handle_statfs_reply(&client->monc, msg);
		break;

		/* mds client */
	case CEPH_MSG_MDS_MAP:
		had = client->mdsc.mdsmap ? 1:0;
		ceph_mdsc_handle_map(&client->mdsc, msg);
		if (!had && client->mdsc.mdsmap) 
			got_first_map(client, 1);
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
	case CEPH_MSG_CLIENT_FILECAPS:
		ceph_mdsc_handle_filecaps(&client->mdsc, msg);
		break;
	case CEPH_MSG_CLIENT_LEASE:
		ceph_mdsc_handle_lease(&client->mdsc, msg);
		break;

		/* osd client */
	case CEPH_MSG_OSD_MAP:
		had = client->osdc.osdmap ? 1:0;
		ceph_osdc_handle_map(&client->osdc, msg);
		if (!had && client->osdc.osdmap) 
			got_first_map(client, 2);
		break;
	case CEPH_MSG_OSD_OPREPLY:
		ceph_osdc_handle_reply(&client->osdc, msg);
		break;

	default:
		derr(1, "dispatch unknown message type %d\n", type);
	}

	ceph_msg_put(msg);
}

const char *ceph_msg_type_name(int type) 
{
	switch (type) {
	case CEPH_MSG_SHUTDOWN: return "shutdown";
	case CEPH_MSG_PING: return "ping";
	case CEPH_MSG_PING_ACK: return "ping_ack";
	case CEPH_MSG_MON_MAP: return "mon_map";
	case CEPH_MSG_CLIENT_MOUNT: return "client_mount";
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
	case CEPH_MSG_CLIENT_FILECAPS: return "client_filecaps";
	case CEPH_MSG_CLIENT_LEASE: return "client_lease";
	case CEPH_MSG_OSD_GETMAP: return "osd_getmap";
	case CEPH_MSG_OSD_MAP: return "osd_map";
	case CEPH_MSG_OSD_OP: return "osd_op";
	case CEPH_MSG_OSD_OPREPLY: return "osd_opreply";
	}
	return "unknown";
}

void ceph_peer_reset(void *p, struct ceph_entity_name *peer_name)
{
	struct ceph_client *client = p;
	
	dout(30, "ceph_peer_reset peer_name = %s%d\n", 
	     ceph_name_type_str(peer_name->type), le32_to_cpu(peer_name->num));

	/* write me */
}
