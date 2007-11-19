
#include <linux/ceph_fs.h>
#include <linux/wait.h>
#include <linux/sched.h>
#include "client.h"
#include "super.h"


/* debug level; defined in include/ceph_fs.h */
int ceph_debug = 10;


void ceph_dispatch(struct ceph_client *client, struct ceph_msg *msg);


/*
 * create a fresh client instance
 */
static struct ceph_client *create_client(struct ceph_mount_args *args)
{
	struct ceph_client *cl;
	int err;

	cl = kzalloc(sizeof(*cl), GFP_KERNEL);
	if (cl == NULL)
		return ERR_PTR(-ENOMEM);

	atomic_set(&cl->nref, 0);
	init_waitqueue_head(&cl->mount_wq);
	spin_lock_init(&cl->sb_lock);

	/* messenger */
	cl->msgr = ceph_messenger_create();
	if (IS_ERR(cl->msgr)) {
		err = PTR_ERR(cl->msgr);
		goto fail;
	}
	cl->msgr->parent = cl;
	cl->msgr->dispatch = (ceph_messenger_dispatch_t)ceph_dispatch;
	
	cl->whoami = -1;
	ceph_monc_init(&cl->monc);
	ceph_mdsc_init(&cl->mdsc, cl);
	ceph_osdc_init(&cl->osdc);

	return cl;

fail:
	kfree(cl);
	return ERR_PTR(err);
}

/*
 * try to mount
 */
static int mount(struct ceph_client *client, struct ceph_mount_args *args)
{
	struct ceph_msg *mount_msg;
	int err;
	int attempts = 10;
	int which;
	
	client->mounting = 6;  /* FIXME don't wait for osd map, for now */

	/* send mount request */
	mount_msg = ceph_msg_new(CEPH_MSG_CLIENT_MOUNT, 0, 0, 0);
	if (IS_ERR(mount_msg))
		return PTR_ERR(mount_msg);
	ceph_msg_get(mount_msg);  /* grab ref; we may retry */
trymount:
	which = get_random_int() % args->num_mon;
	mount_msg->hdr.dst.name.type = CEPH_ENTITY_TYPE_MON;
	mount_msg->hdr.dst.name.num = which;
	mount_msg->hdr.dst.addr = args->mon_addr[which];
	dout(1, "mount from mon%d, %d attempts left\n", which, attempts);
	
	ceph_msg_send(client->msgr, mount_msg);

	/* wait */
	err = wait_event_interruptible_timeout(client->mount_wq, 
					       (client->mounting == 0),
					       6*HZ);
	if (err == -EINTR)
		return err; 
	if (client->mounting) {
		dout(1, "ceph_get_client still waiting for mount, attempts=%d\n", attempts);
		if (--attempts)
			goto trymount;
		return -EIO;
	}

	/* get handle for mount path */
	/*err = ceph_open_dir(CEPH_INO_ROOT, args->path);
	if (err)
		return err;
	*/
	
	return 0;
}

/*
 * the monitor responds to monmap to indicate mount success.
 * (or, someday, to indicate a change in the monitor cluster?)
 */
static void handle_mon_map(struct ceph_client *client, struct ceph_msg *msg)
{
	int err;
	
	dout(1, "handle_mon_map");

	/* parse */
	err = ceph_monmap_decode(&client->monc.monmap, msg->front.iov_base, 
				 msg->front.iov_base + msg->front.iov_len);
	if (err != 0) 
		return;
	
	if (client->whoami < 0) {
		client->whoami = msg->hdr.dst.name.num;
		client->msgr->inst.name = msg->hdr.dst.name;
	}

	clear_bit(4, &client->mounting);
	if (client->mounting == 0)
		wake_up_all(&client->mount_wq);
}



/*
 * directory of filesystems mounted by this host
 *
 *   key: fsid.major ^ fsid.minor
 * value: struct ceph_client.fsid_item
 */

/* ignore all this until later
RADIX_TREE(ceph_clients, GFP_KERNEL);

static struct ceph_client *get_client_fsid(struct ceph_fsid *fsid)
{

}

static struct ceph_client *get_client_monaddr(struct ceph_entity_addr *monaddr)
{

}
*/

struct ceph_client *ceph_get_client(struct ceph_mount_args *args)
{
	struct ceph_client *client = 0;
	int ret;

	/* existing, by fsid? */
	/*
	if (args->flags & CEPH_MOUNT_FSID) 
		client = ceph_get_client_fsid(&args->fsid);
	if (client)
		return client;
	*/
	/* existing, by monitors? */
	/* write me. */

	/* create new client */
	client = create_client(args);
	if (IS_ERR(client))
		return client;
	atomic_inc(&client->nref);
	
	/* request mount */
	ret = mount(client, args);
	if (ret < 0) {
		ceph_put_client(client);
		return ERR_PTR(ret);
	}
	return client;
}

void ceph_put_client(struct ceph_client *cl)
{
	if (atomic_dec_and_test(&cl->nref)) {
		dout(1, "ceph_put_client last put on %p\n", cl);

		/* unmount */
		/* ... */
		kfree(cl);
	}
}





/*
 * dispatch -- called with incoming messages.
 *
 * should be fast and non-blocking, as it is called with locks held.
 */
void ceph_dispatch(struct ceph_client *client, struct ceph_msg *msg)
{
	dout(5, "dispatch %p type %d\n", (void*)msg, msg->hdr.type);

	/* deliver the message */
	switch (msg->hdr.type) {
		/* me */
	case CEPH_MSG_MON_MAP:
		handle_mon_map(client, msg);
		break;

		/* mds client */
	case CEPH_MSG_MDS_MAP:
		ceph_mdsc_handle_map(&client->mdsc, msg);
		break;
	case CEPH_MSG_CLIENT_REPLY:
		ceph_mdsc_handle_reply(&client->mdsc, msg);
		break;
	case CEPH_MSG_CLIENT_REQUEST_FORWARD:
		ceph_mdsc_handle_forward(&client->mdsc, msg);
		break;

		/* osd client */
	case CEPH_MSG_OSD_MAP:
		ceph_osdc_handle_map(&client->osdc, msg);
		break;
	case CEPH_MSG_OSD_OPREPLY:
		ceph_osdc_handle_reply(&client->osdc, msg);
		break;

	default:
		derr(1, "dispatch unknown message type %d\n", msg->hdr.type);
		ceph_msg_put(msg);
	}
}
