#ifndef _FS_CEPH_OSDMAP_H
#define _FS_CEPH_OSDMAP_H

#include <linux/ceph_fs.h>

#include "crush/crush.h"

struct ceph_osdmap {
	struct ceph_fsid fsid;
	ceph_epoch_t epoch;
	ceph_epoch_t mon_epoch;
	struct ceph_timeval ctime, mtime;
	
	__u32 pg_num, pg_num_mask;
	__u32 localized_pg_num, localized_pg_num_mask;
	
	__u32 max_osd;
	__u8 *osd_state;
	__u32 *osd_offload;  /* 0 = normal, 0x10000 = 100% offload (failed) */
	struct ceph_entity_addr *osd_addr;
	struct crush_map *crush;

	__u32 num_pg_swap_primary;
	struct {
		ceph_pg_t pg;
		__u32 osd;
	} *pg_swap_primary;
};

extern struct ceph_osdmap *apply_incremental(void **p, void *end, struct ceph_osdmap *map);
extern void osdmap_destroy(struct ceph_osdmap *map);
extern struct ceph_osdmap *osdmap_decode(void **p, void *end);

#endif
