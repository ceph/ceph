
#include "client.h"
#include "super.h"
#include <include/ceph_msgs.h>


/* debug level; defined in include/ceph_fs.h */
int ceph_debug = 10;

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







/*
 * directory of filesystems mounted by this host
 *
 *   key: fsid.major ^ fsid.minor
 * value: struct ceph_client.fsid_item
 */
RADIX_TREE(ceph_clients, GFP_KERNEL);

struct ceph_client *ceph_get_client(ceph_mount_args *args)
{
	struct ceph_client *client = 0;
	struct sockaddr_in monaddr;
	struct ceph_message *mount_msg;
	int which;

	/* existing, by fsid? */
	if (args->flags & CEPH_MOUNT_FSID) 
		client = ceph_get_client_fsid(&args->fsid);
	if (client)
		return client;

	/* existing, by monitors? */
	/* write me. */

	/* create new client */
	client = create_client();
	if (IS_ERR(client))
		return PTR_ERR(client);
	
	/* send mount request */
	mount_msg = prepare_mount_request(client);
	ceph_get_msg(mount_msg);  /* grab ref; we may retry */
	which = 0;
trymount:
	ceph_msgr_send(client->msgr, mount_msg, args->mon_addr[which]);
	
	/* ... wait ... */
	
		       
	
	
	
	
}

void ceph_put_client(struct ceph_client *cl)
{
	
}





