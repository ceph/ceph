
#include <linux/ceph_fs.h>
#include <linux/wait.h>
#include <linux/sched.h>
#include <linux/random.h>
#include "client.h"

int ceph_client_debug = 50;
#define DOUT_VAR ceph_client_debug
#define DOUT_PREFIX "client: "
#include "super.h"
#include "ktcp.h"


void ceph_dispatch(void *p, struct ceph_msg *msg);


/* debug level; defined in include/ceph_fs.h */
int ceph_debug = 0;

/*
 * directory of filesystems mounted by this host
 *
 *   key: fsid.major ^ fsid.minor
 * value: struct ceph_client.fsid_item
 */
static spinlock_t ceph_client_spinlock = SPIN_LOCK_UNLOCKED;
static int ceph_num_clients = 0;

RADIX_TREE(ceph_clients, GFP_KERNEL);

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
	init_completion(&cl->mount_completion);
	spin_lock_init(&cl->sb_lock);
	get_client_counter();

	/* messenger */
	cl->msgr = ceph_messenger_create(&args->my_addr);
	if (IS_ERR(cl->msgr)) {
		err = PTR_ERR(cl->msgr);
		goto fail;
	}
	cl->msgr->parent = cl;
	cl->msgr->dispatch = ceph_dispatch;
	cl->msgr->prepare_pages = ceph_osdc_prepare_pages;
	
	cl->whoami = -1;
	if ((err = ceph_monc_init(&cl->monc, cl)) < 0)
		goto fail;
	ceph_mdsc_init(&cl->mdsc, cl);
	ceph_osdc_init(&cl->osdc, cl);

	return cl;

fail:
	put_client_counter();
	kfree(cl);
	return ERR_PTR(err);
}

/*
 * mount: join the ceph cluster.
 */
static int mount(struct ceph_client *client, struct ceph_mount_args *args)
{
	struct ceph_msg *mount_msg;
	int err;
	int attempts = 3;
	int which;
	char r;
	
	client->mounting = 0;  /* wait for mon+mds+osd */

	/* send mount request */
trymount:
	get_random_bytes(&r, 1);
	which = r % args->num_mon;
	mount_msg = ceph_msg_new(CEPH_MSG_CLIENT_MOUNT, 0, 0, 0, 0);
	if (IS_ERR(mount_msg))
		return PTR_ERR(mount_msg);
	mount_msg->hdr.dst.name.type = CEPH_ENTITY_TYPE_MON;
	mount_msg->hdr.dst.name.num = which;
	mount_msg->hdr.dst.addr = args->mon_addr[which];

	ceph_msg_send(client->msgr, mount_msg, 0);
	dout(10, "mount from mon%d, %d attempts left\n", which, attempts);

	/* wait */
	dout(10, "mount waiting\n");
	err = wait_for_completion_timeout(&client->mount_completion, 6*HZ);
	if (err == -EINTR)
		return err; 
	if (client->mounting < 7) {
		dout(10, "mount still waiting for mount, attempts=%d\n", attempts);
		if (--attempts)
			goto trymount;
		return -EIO;
	}
	dout(10, "mount got all maps.\n");
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
		client->whoami = msg->hdr.dst.name.num;
		client->msgr->inst.name = msg->hdr.dst.name;
		dout(1, "i am client%d\n", client->whoami);
	}
}



/*
static struct ceph_client *get_client_fsid(struct ceph_fsid *fsid)
{

}

static struct ceph_client *get_client_monaddr(struct ceph_entity_addr *monaddr)
{

}
*/

struct ceph_client *ceph_create_client(struct ceph_mount_args *args, struct super_block *sb)
{
	struct ceph_client *client = 0;
	int ret;

	/* create new client */
	client = create_client(args);
	if (IS_ERR(client))
		return client;
	atomic_inc(&client->nref);
	client->sb = sb;

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
	dout(10, "ceph_put_client %p\n", cl);
	if (atomic_dec_and_test(&cl->nref)) {
		dout(1, "ceph_put_client last put on %p\n", cl);

		/* unmount */
		/* ... */

		ceph_messenger_destroy(cl->msgr);
		put_client_counter();
		kfree(cl);
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
 * dispatch -- called with incoming messages.
 *
 * should be fast and non-blocking, as it is called with locks held.
 */
void ceph_dispatch(void *p, struct ceph_msg *msg)
{
	struct ceph_client *client = p;
	int had;

	dout(5, "dispatch from %s%d type %d len %d+%d\n",
	     ceph_name_type_str(msg->hdr.src.name.type), msg->hdr.src.name.num,
	     msg->hdr.type, msg->hdr.front_len, msg->hdr.data_len);

	/* deliver the message */
	switch (msg->hdr.type) {
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
		derr(1, "dispatch unknown message type %d\n", msg->hdr.type);
	}

	ceph_msg_put(msg);
}
