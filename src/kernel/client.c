
#include <linux/wait.h>
#include <linux/sched.h>
#include <linux/random.h>
#include <linux/fs.h>
#include <linux/mount.h>

#include "ceph_fs.h"

int ceph_debug_client = -1;
#define DOUT_VAR ceph_debug_client
#define DOUT_PREFIX "client: "
#include "super.h"
#include "ktcp.h"


void ceph_dispatch(void *p, struct ceph_msg *msg);
void ceph_peer_reset(void *p, struct ceph_entity_name *peer_name);


/*
 * share work queue between clients.
 */
spinlock_t ceph_client_spinlock;
int ceph_num_clients = 0;

static void get_client_counter(void)
{
	spin_lock(&ceph_client_spinlock);
	if (ceph_num_clients == 0) {
		dout(10, "first client, setting up workqueues\n");
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
		dout(10, "last client, shutting down workqueues\n");
		ceph_workqueue_shutdown();
	}
	spin_unlock(&ceph_client_spinlock);
}

static struct dentry *open_root_dentry(struct ceph_client *client,
				       struct ceph_mount_args *args)
{
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_mds_request *req = 0;
	struct ceph_mds_request_head *reqhead;
	int err;
	struct dentry *root;

	/* open dir */
	dout(30, "open_root_inode opening '%s'\n", args->path);
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_OPEN,
				       1, args->path, 0, 0);
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

static int have_all_maps(struct ceph_client *client)
{
	return client->osdc.osdmap && client->osdc.osdmap->epoch &&
		client->monc.monmap && client->monc.monmap->epoch &&
		client->mdsc.mdsmap && client->mdsc.mdsmap->m_epoch;
}

/*
 * mount: join the ceph cluster.
 */
int ceph_mount(struct ceph_client *client, struct ceph_mount_args *args,
	       struct vfsmount *mnt)
{
	struct ceph_msg *mount_msg;
	struct dentry *root;
	int err;
	int attempts = 10;
	int which;
	char r;

	dout(10, "mount start\n");
	while (1) {
		get_random_bytes(&r, 1);
		which = r % args->num_mon;
		mount_msg = ceph_msg_new(CEPH_MSG_CLIENT_MOUNT, 0, 0, 0, 0);
		if (IS_ERR(mount_msg))
			return PTR_ERR(mount_msg);
		mount_msg->hdr.dst.name.type =
			cpu_to_le32(CEPH_ENTITY_TYPE_MON);
		mount_msg->hdr.dst.name.num = cpu_to_le32(which);
		mount_msg->hdr.dst.addr = args->mon_addr[which];

		ceph_msg_send(client->msgr, mount_msg, 0);
		dout(10, "mount from mon%d, %d attempts left\n",
		     which, attempts);

		/* wait */
		dout(10, "mount sent mount request, waiting for maps\n");
		err = wait_event_interruptible_timeout(client->mount_wq,
						       have_all_maps(client),
						       6*HZ);
		dout(10, "mount wait got %d\n", err);
		if (err == -EINTR)
			return err;
		if (have_all_maps(client))
			break;  /* success */
		dout(10, "mount still waiting for mount, attempts=%d\n",
		     attempts);
		if (--attempts == 0)
			return -EIO;
	}

	dout(30, "mount opening base mountpoint\n");
	root = open_root_dentry(client, args);
	if (IS_ERR(root))
		return PTR_ERR(root);
	mnt->mnt_root = root;
	mnt->mnt_sb = client->sb;
	client->mount_state = CEPH_MOUNT_MOUNTED;
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

	dout(2, "handle_monmap had epoch %d\n", client->monc.monmap->epoch);
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
		char name[10];
		client->whoami = le32_to_cpu(msg->hdr.dst.name.num);
		client->msgr->inst.name = msg->hdr.dst.name;
		sprintf(name, "client%d", client->whoami);
		dout(1, "i am %s, fsid is %llx.%llx\n", name,
		     le64_to_cpu(client->monc.monmap->fsid.major),
		     le64_to_cpu(client->monc.monmap->fsid.minor));

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,25)
		client->client_kobj = kobject_create_and_add(name, ceph_kobj);
		//client->fsid_kobj = kobject_create_and_add("fsid", 
		//client->client_kobj);
#endif
	}
}

/*
 * create a fresh client instance
 */
struct ceph_client *ceph_create_client(struct ceph_mount_args *args,
				       struct super_block *sb)
{
	struct ceph_client *cl;
	struct ceph_entity_addr *myaddr = 0;
	int err = -ENOMEM;

	cl = kzalloc(sizeof(*cl), GFP_KERNEL);
	if (cl == NULL)
		return ERR_PTR(-ENOMEM);

	init_waitqueue_head(&cl->mount_wq);
	spin_lock_init(&cl->sb_lock);
	get_client_counter();

	cl->wb_wq = create_workqueue("ceph-writeback");
	if (cl->wb_wq == 0)
		goto fail;

	/* messenger */
	if (args->flags & CEPH_MOUNT_MYIP)
		myaddr = &args->my_addr;
	cl->msgr = ceph_messenger_create(myaddr);
	if (IS_ERR(cl->msgr)) {
		err = PTR_ERR(cl->msgr);
		cl->msgr = 0;
		goto fail;
	}
	cl->msgr->parent = cl;
	cl->msgr->dispatch = ceph_dispatch;
	cl->msgr->prepare_pages = ceph_osdc_prepare_pages;
	cl->msgr->peer_reset = ceph_peer_reset;

	cl->whoami = -1;
	err = ceph_monc_init(&cl->monc, cl);
	if (err < 0)
		goto fail;
	ceph_mdsc_init(&cl->mdsc, cl);
	ceph_osdc_init(&cl->osdc, cl);

	cl->sb = sb;
	cl->mount_state = CEPH_MOUNT_MOUNTING;

	return cl;

fail:
	put_client_counter();
	kfree(cl);
	return ERR_PTR(err);
}

void ceph_umount_start(struct ceph_client *cl)
{
	int rc;
	int seconds = 15;

	ceph_mdsc_stop(&cl->mdsc);
	ceph_monc_request_umount(&cl->monc);
	rc = wait_event_timeout(cl->mount_wq,
				(cl->mount_state == CEPH_MOUNT_UNMOUNTED),
				seconds*HZ);
	if (rc == 0)
		derr(0, "umount timed out after %d seconds\n", seconds);
}

void ceph_destroy_client(struct ceph_client *cl)
{
	dout(10, "destroy_client %p\n", cl);

	/* unmount */
	/* ... */

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,25)
	if (cl->client_kobj)
		kobject_put(cl->client_kobj);
#endif
	if (cl->wb_wq)
		destroy_workqueue(cl->wb_wq);
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
		if (!had && client->monc.monmap->epoch && have_all_maps(client))
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
		had = client->mdsc.mdsmap ? 1:0;
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
		if (!had && client->osdc.osdmap && have_all_maps(client))
			wake_up(&client->mount_wq);
		break;
	case CEPH_MSG_OSD_OPREPLY:
		ceph_osdc_handle_reply(&client->osdc, msg);
		break;

	default:
		derr(0, "received unknown message type %d\n", type);
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

	dout(30, "ceph_peer_reset peer_name = %s%d\n", ENTITY_NAME(*peer_name));

	/* write me */
}
