#ifndef _FS_CEPH_MDSMAP_H
#define _FS_CEPH_MDSMAP_H

#include <linux/ceph_fs.h>

/* see mds/MDSMap.h */
#define CEPH_MDS_STATE_DNE         0  /* down, never existed. */
#define CEPH_MDS_STATE_STOPPED    -1  /* down, once existed, but no subtrees. empty log. */
#define CEPH_MDS_STATE_FAILED      2  /* down, active subtrees needs to be recovered. */

#define CEPH_MDS_STATE_BOOT       -3  /* up, boot announcement.  destiny unknown. */
#define CEPH_MDS_STATE_STANDBY    -4  /* up, idle.  waiting for assignment by monitor. */
#define CEPH_MDS_STATE_CREATING   -5  /* up, creating MDS instance (new journal, idalloc..). */
#define CEPH_MDS_STATE_STARTING   -6  /* up, starting prior stopped MDS instance. */

#define CEPH_MDS_STATE_REPLAY      7  /* up, starting prior failed instance. scanning journal. */
#define CEPH_MDS_STATE_RESOLVE     8  /* up, disambiguating distributed operations (import, rename, etc.) */
#define CEPH_MDS_STATE_RECONNECT   9  /* up, reconnect to clients */
#define CEPH_MDS_STATE_REJOIN      10 /* up, replayed journal, rejoining distributed cache */
#define CEPH_MDS_STATE_ACTIVE      11 /* up, active */
#define CEPH_MDS_STATE_STOPPING    12 /* up, exporting metadata (-> standby or out) */

/*
 * mds map
 *
 * fields limited to those the client cares about
 */
struct ceph_mdsmap {
	__u64 m_epoch;
	struct ceph_timeval m_created;
	__u32 m_anchortable;
	__u32 m_root;
	__u32 m_max_mds;  /* size of m_addr, m_state arrays */
	struct ceph_entity_addr *m_addr;  /* array of addresses */
	__u8 *m_state;                    /* array of states */
};

extern int ceph_mdsmap_get_random_mds(ceph_mdsmap *m);
extern int ceph_mdsmap_get_state(ceph_mdsmap *m, int w);
extern struct ceph_entity_addr *ceph_mdsmap_get_addr(ceph_mdsmap *m, int w);

extern int ceph_mdsmap_decode(struct ceph_mdsmap *m, 
			      struct ceph_bufferlist *bl, 
			      struct ceph_bufferlist_iterator *bli);

#endif
