
#include <linux/ceph_fs_msgs.h>
#include "super.h"


/*
 * dispatch -- called with incoming messages.
 *
 * should be fast and non-blocking, as it is called with locks held.
 */
void dispatch(struct ceph_client *client, struct ceph_message *msg)
{
	/* deliver the message */
	switch (msg->hdr.type) {
		/* mds client */
	case CEPH_MSG_MDSMAP:
		ceph_mdsc_handle_map(client->mds_client, msg);
		break;
	case CEPH_MSG_CLIENT_REPLY:
		ceph_mdsc_handle_reply(client->mds_client, msg);
		break;
	case CEPH_MSG_CLIENT_FORWARD:
		ceph_mdsc_handle_forward(client->mds_client, msg);
		break;

		/* osd client */
	case CEPH_MSG_OSDMAP:
		ceph_osdc_handle_map(client->osd_client, msg);
		break;
	case CEPH_MSG_OSD_OPREPLY:
		ceph_osdc_handle_reply(client->osd_client, msg);
		break;

	default:
		printk(KERN_INFO "dispatch: unknown message type %d\n", msg->hdr.type);
		ceph_put_msg(msg);
	}
}

