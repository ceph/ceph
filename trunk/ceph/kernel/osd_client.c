
#include "osd_client.h"
#include "messenger.h"


void ceph_osdc_init(struct ceph_osd_client *osdc)
{
	osdc->osdmap = NULL;
}

void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	dout(1, "ceph_osdc_handle_map - implement me\n");
	ceph_msg_put(msg);
}
