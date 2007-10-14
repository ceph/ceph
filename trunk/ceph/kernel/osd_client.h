#ifndef _FS_CEPH_OSD_CLIENT_H
#define _FS_CEPH_OSD_CLIENT_H

/* this will be equivalent to osdc/Objecter.h */


/* do these later
#include "osdmap.h"
*/
struct ceph_osdmap;


struct ceph_osd_client {
	struct ceph_osdmap *s_osdmap;  /* osd map */

};

#endif
