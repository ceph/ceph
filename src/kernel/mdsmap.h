#ifndef _FS_CEPH_MDSMAP_H
#define _FS_CEPH_MDSMAP_H

#include "types.h"

/*
 * mds map
 *
 * fields limited to those the client cares about
 */
struct ceph_mdsmap {
	u32 m_epoch, m_client_epoch, m_last_failure;
	u32 m_root;
	u32 m_session_timeout;          /* seconds */
	u32 m_session_autoclose;        /* seconds */
	u32 m_max_mds;                  /* size of m_addr, m_state arrays */
	struct ceph_entity_addr *m_addr;  /* mds addrs */
	s32 *m_state;                   /* states */
};

static inline struct ceph_entity_addr *
ceph_mdsmap_get_addr(struct ceph_mdsmap *m, int w)
{
	if (w >= m->m_max_mds)
		return NULL;
	return &m->m_addr[w];
}

static inline int ceph_mdsmap_get_state(struct ceph_mdsmap *m, int w)
{
	BUG_ON(w < 0);
	if (w >= m->m_max_mds)
		return CEPH_MDS_STATE_DNE;
	return m->m_state[w];
}

static inline char *ceph_mdsmap_state_str(int state)
{
	switch (state) {
	case CEPH_MDS_STATE_DNE:
		return "dne";
		break;
	case CEPH_MDS_STATE_STOPPED:
		return "stopped";
		break;
	case CEPH_MDS_STATE_DESTROYING:
		return "destroying";
		break;
	case CEPH_MDS_STATE_FAILED:
		return "failed";
		break;
	case CEPH_MDS_STATE_BOOT:
		return "boot";
		break;
	case CEPH_MDS_STATE_STANDBY:
		return "standby";
		break;
	case CEPH_MDS_STATE_CREATING:
		return "creating";
		break;
	case CEPH_MDS_STATE_STARTING:
		return "starting";
		break;
	case CEPH_MDS_STATE_STANDBY_REPLAY:
		return "standby replay";
		break;
	case CEPH_MDS_STATE_REPLAY:
		return "replay";
		break;
	case CEPH_MDS_STATE_RESOLVE:
		return "resolve";
		break;
	case CEPH_MDS_STATE_RECONNECT:
		return "reconnect";
		break;
	case CEPH_MDS_STATE_REJOIN:
		return "rejoin";
		break;
	case CEPH_MDS_STATE_ACTIVE:
		return "active";
		break;
	case CEPH_MDS_STATE_STOPPING:
		return "stopping";
		break;
	}

	return "unknown";
}

extern int ceph_mdsmap_get_random_mds(struct ceph_mdsmap *m);
extern struct ceph_mdsmap *ceph_mdsmap_decode(void **p, void *end);
extern void ceph_mdsmap_destroy(struct ceph_mdsmap *m);

#endif
