#ifndef _FS_CEPH_OSD_CLIENT_H
#define _FS_CEPH_OSD_CLIENT_H

/* this will be equivalent to osdc/Objecter.h */

#include <linux/ceph_fs.h>

struct ceph_msg;

struct ceph_osdmap {
	struct ceph_fsid fsid;
	__u64 epoch;
	__u64 mon_epoch;
	struct ceph_timeval ctime, mtime;
	
	__u32 pg_num, pg_num_mask;
	__u32 localized_pg_num, localized_pg_num_mask;
	
	__u32 max_osd;
	__u8 *osd_state;
	__u32 *osd_offload;  /* 0 = normal, 0x10000 = 100% offload (failed) */
	struct ceph_entity_addr *osd_addr;
	struct crush_map *crush;
};

struct ceph_osd_client {
	struct ceph_osdmap *osdmap;  /* current osd map */

};

extern void ceph_osdc_init(struct ceph_osd_client *osdc);
extern void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg);
extern void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg);

#endif

