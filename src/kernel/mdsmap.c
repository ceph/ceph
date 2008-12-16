#include <linux/bug.h>
#include <linux/err.h>
#include <linux/random.h>
#include <linux/slab.h>
#include <linux/types.h>

#include "mdsmap.h"
#include "messenger.h"
#include "decode.h"

#include "ceph_debug.h"

int ceph_debug_mdsmap = -1;
#define DOUT_MASK DOUT_MASK_MDSMAP
#define DOUT_VAR ceph_debug_mdsmap
#include "super.h"


/*
 * choose a random mds that is "up" (i.e. has a state > 0), or -1.
 */
int ceph_mdsmap_get_random_mds(struct ceph_mdsmap *m)
{
	int n = 0;
	int i;
	char r;

	/* count */
	for (i = 0; i < m->m_max_mds; i++)
		if (m->m_state[i] > 0)
			n++;
	if (n == 0)
		return -1;

	/* pick */
	get_random_bytes(&r, 1);
	n = r % n;
	i = 0;
	for (i = 0; n > 0; i++, n--)
		while (m->m_state[i] <= 0)
			i++;

	return i;
}

/*
 * Ignore any fields we don't care about in the MDS map (there are quite
 * a few of them).
 */
struct ceph_mdsmap *ceph_mdsmap_decode(void **p, void *end)
{
	struct ceph_mdsmap *m;
	int i, n;
	int err = -EINVAL;

	m = kzalloc(sizeof(*m), GFP_NOFS);
	if (m == NULL)
		return ERR_PTR(-ENOMEM);

	ceph_decode_need(p, end, 8*sizeof(u32), bad);
	ceph_decode_32(p, m->m_epoch);
	ceph_decode_32(p, m->m_client_epoch);
	ceph_decode_32(p, m->m_last_failure);
	ceph_decode_32(p, m->m_root);
	ceph_decode_32(p, m->m_session_timeout);
	ceph_decode_32(p, m->m_session_autoclose);
	ceph_decode_32(p, m->m_max_mds);

	m->m_addr = kzalloc(m->m_max_mds*sizeof(*m->m_addr), GFP_NOFS);
	m->m_state = kzalloc(m->m_max_mds*sizeof(*m->m_state), GFP_NOFS);
	if (m->m_addr == NULL || m->m_state == NULL)
		goto badmem;

	/* pick out active nodes from mds_info (state > 0) */
	ceph_decode_32(p, n);
	ceph_decode_need(p, end,
			 n * (3*sizeof(u32) + sizeof(u64) +
			      2*sizeof(*m->m_addr) +
			      sizeof(struct ceph_timespec)),
			 bad);
	for (i = 0; i < n; i++) {
		s32 mds, inc, state;
		u64 state_seq;

		*p += sizeof(m->m_addr[0]);  /* skip addr key */
		ceph_decode_32(p, mds);
		ceph_decode_32(p, inc);
		ceph_decode_32(p, state);
		ceph_decode_64(p, state_seq);
		dout(10, "mdsmap_decode %d/%d mds%d.%d state %d\n",
		     i+1, n, mds, inc, state);
		if (mds >= 0 && mds < m->m_max_mds && state > 0) {
			m->m_state[mds] = state;
			ceph_decode_copy(p, &m->m_addr[mds],
					 sizeof(*m->m_addr));
		} else {
			*p += sizeof(m->m_addr[0]);  /* skip it */
		}
		*p += sizeof(struct ceph_timespec);
	}

	/* ok, we don't care about the rest. */
	dout(30, "mdsmap_decode success epoch %u\n", m->m_epoch);
	return m;

badmem:
	err = -ENOMEM;
bad:
	derr(0, "corrupt mdsmap");
	ceph_mdsmap_destroy(m);
	return ERR_PTR(-EINVAL);
}

void ceph_mdsmap_destroy(struct ceph_mdsmap *m)
{
	kfree(m->m_addr);
	kfree(m->m_state);
	kfree(m);
}
