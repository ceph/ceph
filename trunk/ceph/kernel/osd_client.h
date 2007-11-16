#ifndef _FS_CEPH_OSD_CLIENT_H
#define _FS_CEPH_OSD_CLIENT_H

/* this will be equivalent to osdc/Objecter.h */

/* do these later
#include "osdmap.h"
*/
struct ceph_osdmap;
struct ceph_msg;

struct ceph_osd_client {
	struct ceph_osdmap *osdmap;  /* current osd map */

};

extern void ceph_osdc_init(struct ceph_osd_client *osdc);
extern void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg);
extern void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg);

#endif

