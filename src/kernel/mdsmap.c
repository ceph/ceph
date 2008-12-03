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
	u32 mds;
	int err = -EINVAL;

	m = kzalloc(sizeof(*m), GFP_NOFS);
	if (m == NULL)
		return ERR_PTR(-ENOMEM);

	ceph_decode_need(p, end, 10*sizeof(u32), bad);
	ceph_decode_32(p, m->m_epoch);
	ceph_decode_32(p, m->m_client_epoch);
	ceph_decode_32(p, m->m_last_failure);
	*p += sizeof(struct ceph_timespec);  /* ignore map timestamp */
	*p += sizeof(u32);                 /* skip anchortable */
	ceph_decode_32(p, m->m_root);
	ceph_decode_32(p, m->m_session_timeout);
	ceph_decode_32(p, m->m_session_autoclose);
	ceph_decode_32(p, m->m_max_mds);

	m->m_addr = kmalloc(m->m_max_mds*sizeof(*m->m_addr), GFP_NOFS);
	m->m_state = kzalloc(m->m_max_mds*sizeof(*m->m_state), GFP_NOFS);
	if (m->m_addr == NULL || m->m_state == NULL)
		goto badmem;

	/* state */
	ceph_decode_32(p, n);
	ceph_decode_need(p, end, n*2*sizeof(u32), bad);
	for (i = 0; i < n; i++) {
		ceph_decode_32(p, mds);
		if (mds >= m->m_max_mds)
			goto bad;
		ceph_decode_32(p, m->m_state[mds]);
	}

	/* state_seq */
	ceph_decode_32_safe(p, end, n, bad);
	*p += n*(sizeof(u32)+sizeof(u64));

	/* mds_inst */
	ceph_decode_32_safe(p, end, n, bad);
	ceph_decode_need(p, end,
			 n*(sizeof(u32)+sizeof(struct ceph_entity_name)+
			    sizeof(struct ceph_entity_addr)),
			 bad);
	for (i = 0; i < n; i++) {
		ceph_decode_32(p, mds);
		if (mds >= m->m_max_mds)
			goto bad;
		*p += sizeof(struct ceph_entity_name);
		ceph_decode_copy(p, &m->m_addr[mds], sizeof(*m->m_addr));
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
