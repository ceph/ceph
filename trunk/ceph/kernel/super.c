
#include <linux/ceph_fs_msgs.h>
#include "super.h"

/* debug level */
int ceph_debug = 10;

/*
 * dispatch -- called with incoming messages.
 *
 * should be fast and non-blocking, as it is called with locks held.
 */
void dispatch(struct ceph_client *client, struct ceph_message *msg)
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

