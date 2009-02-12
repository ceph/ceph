#ifndef _FS_CEPH_OSDMAP_H
#define _FS_CEPH_OSDMAP_H

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
struct ceph_osdmap {
	ceph_fsid_t fsid;
	u32 epoch;
	u32 mkfs_epoch;
	struct ceph_timespec ctime, mtime;

	/* these parameters describe the number of placement groups
	 * in the system.  foo_mask is the smallest value (2**n-1) >= foo. */
	u32 pg_num, pg_num_mask;
	u32 pgp_num, pgp_num_mask;
	u32 lpg_num, lpg_num_mask;
	u32 lpgp_num, lpgp_num_mask;
	u32 last_pg_change;   /* epoch of last pg count change */

	u32 flags;         /* CEPH_OSDMAP_* */

	u32 max_osd;       /* size of osd_state, _offload, _addr arrays */
	u8 *osd_state;     /* CEPH_OSD_* */
	u32 *osd_offload;  /* 0 = normal, 0x10000 = 100% offload (failed) */
	u32 *osd_weight;
	struct ceph_entity_addr *osd_addr;

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

static inline char *ceph_osdmap_state_str(char *str, int len, int state)
{
	int flag = 0;

	if (!len)
		goto done;

	*str = '\0';
	if (state) {
		if (state & CEPH_OSD_EXISTS) {
			snprintf(str, len, "exists");
			flag = 1;
		}
		if (state & CEPH_OSD_UP) {
			snprintf(str, len, "%s%s%s", str, (flag ? ", " : ""), "up");
			flag = 1;
		}
	} else {
		snprintf(str, len, "doesn't exist");
	}
done:
	return str;
}

static inline struct ceph_entity_addr *ceph_osd_addr(struct ceph_osdmap *map,
						     int osd)
{
	if (osd >= map->max_osd)
		return 0;
	return &map->osd_addr[osd];
}

extern struct ceph_osdmap *osdmap_decode(void **p, void *end);
extern struct ceph_osdmap *apply_incremental(void **p, void *end,
					     struct ceph_osdmap *map,
					     struct ceph_messenger *msgr);
extern void osdmap_destroy(struct ceph_osdmap *map);

/* calculate mapping of a file extent to an object */
extern void calc_file_object_mapping(struct ceph_file_layout *layout,
				     u64 off, u64 *plen,
				     struct ceph_object *oid,
				     u64 *oxoff, u64 *oxlen);

/* calculate mapping of object to a placement group */
extern void calc_object_layout(struct ceph_object_layout *ol,
			       struct ceph_object *oid,
			       struct ceph_file_layout *fl,
			       struct ceph_osdmap *osdmap);

#endif
