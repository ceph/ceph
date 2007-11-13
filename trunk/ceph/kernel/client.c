
#include "client.h"
#include "super.h"
#include <include/ceph_msgs.h>


/* debug level; defined in include/ceph_fs.h */
int ceph_debug = 10;



/*
 * create a fresh client instance
 */
static struct ceph_client *create_client(ceph_mount_args *args)
{
	struct ceph_client *cl;

	cl = kmalloc(sizeof(*cl), GFP_KERNEL);
	if (cl == NULL)
		return ERR_PTR(-ENOMEM);
	memset(cl, 0, sizeof(*cl));

	cl->whoami = -1;
	ceph_monc_init(&cl->monc);
	ceph_mdsc_init(&cl->mdsc, cl);
	ceph_osdc_init(&cl->osdc);

	return cl;
}

/*
 * try to mount
 */
static int mount(struct ceph_client *client, struct ceph_mount_args *args)
{
	struct ceph_message *mount_msg;
	struct ceph_entity_inst inst;
	int ret;
	int attempts = 10;
	
	atomic_set(&client->mounting, 1);

	/* send mount request */
	mount_msg = ceph_new_message(CEPH_MSG_CLIENT_MOUNT, 0);
	if (IS_ERR(mount_msg))
		return PTR_ERR(mount_msg);
	ceph_get_msg(mount_msg);  /* grab ref; we may retry */
trymount:
	inst.name.type = CEPH_ENTITY_TYPE_MON;
	inst.name.num = get_random_int() % args->num_mon;
	inst.addr = args->mon_addr[inst.name.num];
	dout(1, "ceph_get_client requesting mount from mon%d, %d attempts left\n", 
	     inst.name.num, attempts);
	ceph_messenger_send(client->msgr, mount_msg, &inst);

	/* wait */
	err = wait_event_interruptible_timeout(client->mounted_wq, 
					       atomic_read(&client->mounting) == 0,
					       6*HZ);
	if (err == -EINTR)
		return err; 
	if (atomic_read(&client->mounting)) {
		dout(1, "ceph_get_client still waiting for mount, attempts=%d\n", attempts);
		if (--attempts)
			goto trymount;
		return -EIO;
	}
	
	return 0;
}

/*
 * the monitor responds to monmap to indicate mount success.
 * (or, someday, to indicate a change in the monitor cluster?)
 */
static void handle_mon_map(struct ceph_client *client, struct ceph_message *msg)
{
	int err;
	
	dout(1, "handle_mon_map");

	/* parse */
	err = ceph_monmap_decode(&client->monc->monmap, &msg->payload);
	if (err != 0) 
		return;
	
	/* mounted! */
	client->whoami = msg->dst.name.num;
	if (atomic_dec_and_test(&client->mounting))
		wake_up(&client->mount_wq);
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

struct ceph_client *ceph_get_client(ceph_mount_args *args)
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
	client = create_client();
	if (IS_ERR(client))
		return PTR_ERR(client);
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
static void dispatch(struct ceph_client *client, struct ceph_message *msg)
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
		ceph_mdsc_handle_map(&client->mds_client, msg);
		break;
	case CEPH_MSG_CLIENT_REPLY:
		ceph_mdsc_handle_reply(&client->mds_client, msg);
		break;
	case CEPH_MSG_CLIENT_REQUEST_FORWARD:
		ceph_mdsc_handle_forward(&client->mds_client, msg);
		break;

		/* osd client */
	case CEPH_MSG_OSD_MAP:
		ceph_osdc_handle_map(&client->osd_client, msg);
		break;
	case CEPH_MSG_OSD_OPREPLY:
		ceph_osdc_handle_reply(&client->osd_client, msg);
		break;

	default:
		derr(1, "dispatch unknown message type %d\n", msg->hdr.type);
		ceph_put_msg(msg);
	}
}
