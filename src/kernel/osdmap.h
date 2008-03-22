#ifndef _FS_CEPH_OSDMAP_H
#define _FS_CEPH_OSDMAP_H

#include <linux/ceph_fs.h>

#include "crush/crush.h"

struct ceph_osdmap {
	struct ceph_fsid fsid;
	ceph_epoch_t epoch;
	ceph_epoch_t mkfs_epoch;
	struct ceph_timeval ctime, mtime;
	
	__u32 pg_num, pg_num_mask;
	__u32 pgp_num, pgp_num_mask;
	__u32 lpg_num, lpg_num_mask;
	__u32 lpgp_num, lpgp_num_mask;
	ceph_epoch_t last_pg_change;
	
	__u32 max_osd;
	__u8 *osd_state;
	__u32 *osd_offload;  /* 0 = normal, 0x10000 = 100% offload (failed) */
	struct ceph_entity_addr *osd_addr;
	struct crush_map *crush;

	__u32 num_pg_swap_primary;
	struct {
		union ceph_pg pg;
		__u32 osd;
	} *pg_swap_primary;
};

static inline int ceph_osd_is_up(struct ceph_osdmap *map, int osd)
{
	return (osd < map->max_osd) && (map->osd_state[osd] & CEPH_OSD_UP);
}

extern struct ceph_osdmap *apply_incremental(void **p, void *end, struct ceph_osdmap *map);
extern void osdmap_destroy(struct ceph_osdmap *map);
extern struct ceph_osdmap *osdmap_decode(void **p, void *end);

extern void calc_file_object_mapping(struct ceph_file_layout *layout, 
				     loff_t *off, loff_t *len,
				     struct ceph_object *oid, __u64 *oxoff, __u64 *oxlen);
extern void calc_object_layout(struct ceph_object_layout *ol,
			       struct ceph_object *oid,
			       struct ceph_file_layout *fl,
			       struct ceph_osdmap *osdmap);

#endif
