#ifndef _FS_CEPH_OSDMAP_H
#define _FS_CEPH_OSDMAP_H

#include <linux/rbtree.h>
#include "types.h"
#include "ceph_fs.h"
#include "crush/crush.h"

/*
 * The osd map describes the current membership of the osd cluster and
 * specifies the mapping of objects to placement groups and placement
 * groups to (sets of) osds.  That is, it completely specifies the
 * (desired) distribution of all data objects in the system at some
 * point in time.
 *
 * Each map version is identified by an epoch, which increases monotonically.
 *
 * The map can be updated either via an incremental map (diff) describing
 * the change between two successive epochs, or as a fully encoded map.
 */
struct ceph_pg_pool_info {
	struct ceph_pg_pool v;
	int pg_num_mask, pgp_num_mask, lpg_num_mask, lpgp_num_mask;
};

struct ceph_pg_mapping {
	struct rb_node node;
	u64 pgid;
	int len;
	int osds[];
};

struct ceph_osdmap {
	struct ceph_fsid fsid;
	u32 epoch;
	u32 mkfs_epoch;
	struct ceph_timespec created, modified;

	u32 flags;         /* CEPH_OSDMAP_* */

	u32 max_osd;       /* size of osd_state, _offload, _addr arrays */
	u8 *osd_state;     /* CEPH_OSD_* */
	u32 *osd_weight;   /* 0 = failed, 0x10000 = 100% normal */
	struct ceph_entity_addr *osd_addr;

	struct rb_root pg_temp;

	u32 num_pools;
	struct ceph_pg_pool_info *pg_pool;

	/* the CRUSH map specifies the mapping of placement groups to
	 * the list of osds that store+replicate them. */
	struct crush_map *crush;
};

static inline int ceph_osd_is_up(struct ceph_osdmap *map, int osd)
{
	return (osd < map->max_osd) && (map->osd_state[osd] & CEPH_OSD_UP);
}

static inline bool ceph_osdmap_flag(struct ceph_osdmap *map, int flag)
{
	return map && (map->flags & flag);
}

extern char *ceph_osdmap_state_str(char *str, int len, int state);

static inline struct ceph_entity_addr *ceph_osd_addr(struct ceph_osdmap *map,
						     int osd)
{
	if (osd >= map->max_osd)
		return NULL;
	return &map->osd_addr[osd];
}

extern struct ceph_osdmap *osdmap_decode(void **p, void *end);
extern struct ceph_osdmap *osdmap_apply_incremental(void **p, void *end,
					    struct ceph_osdmap *map,
					    struct ceph_messenger *msgr);
extern void ceph_osdmap_destroy(struct ceph_osdmap *map);

/* calculate mapping of a file extent to an object */
extern void ceph_calc_file_object_mapping(struct ceph_file_layout *layout,
					  u64 off, u64 *plen,
					  u64 *bno, u64 *oxoff, u64 *oxlen);

/* calculate mapping of object to a placement group */
extern int ceph_calc_object_layout(struct ceph_object_layout *ol,
				   const char *oid,
				   struct ceph_file_layout *fl,
				   struct ceph_osdmap *osdmap);
extern int ceph_calc_pg_primary(struct ceph_osdmap *osdmap, union ceph_pg pgid);

#endif
