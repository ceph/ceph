#ifndef _FS_CEPH_MDSMAP_H
#define _FS_CEPH_MDSMAP_H

#include <linux/ceph_fs.h>

/*
 * mds map
 *
 * fields limited to those the client cares about
 */
struct ceph_mdsmap {
	ceph_epoch_t m_epoch, m_client_epoch, m_last_failure;
	struct ceph_timespec m_created;
	__u32 m_anchortable;
	__u32 m_root;
	__u32 m_cap_bit_timeout;
	__u32 m_session_autoclose;
	__u32 m_max_mds;  /* size of m_addr, m_state arrays */
	struct ceph_entity_addr *m_addr;  /* array of addresses */
	__s32 *m_state;                   /* array of states */
};

extern int ceph_mdsmap_get_random_mds(struct ceph_mdsmap *m);
extern int ceph_mdsmap_get_state(struct ceph_mdsmap *m, int w);
extern struct ceph_entity_addr *ceph_mdsmap_get_addr(struct ceph_mdsmap *m, int w);

extern struct ceph_mdsmap * ceph_mdsmap_decode(void **p, void *end);
extern void ceph_mdsmap_destroy(struct ceph_mdsmap *m);


#endif
