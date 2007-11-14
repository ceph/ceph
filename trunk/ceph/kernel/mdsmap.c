
#include <linux/types.h>
#include <linux/random.h>
#include <linux/slab.h>
#include <asm/bug.h>

#include "mdsmap.h"
#include "messenger.h"

int ceph_mdsmap_get_state(struct ceph_mdsmap *m, int w)
{
	BUG_ON(w < 0);
	if (w >= m->m_max_mds)
		return CEPH_MDS_STATE_DNE;
	return m->m_state[w];
}

int ceph_mdsmap_get_random_mds(struct ceph_mdsmap *m)
{
	int n = 0;
	int i;

	/* count */
	for (i=0; i<m->m_max_mds; i++)
		if (m->m_state > 0) n++;
	if (n == 0) 
		return -1;
	
	/* pick */
	n = get_random_int() % n;
	i = 0;
	for (i=0; n>0; i++, n--)
		while (m->m_state[i] <= 0) 
			i++;

	return i;
}


struct ceph_entity_addr *ceph_mdsmap_get_addr(struct ceph_mdsmap *m, int w)
{
	if (w >= m->m_max_mds)
		return NULL;
	return &m->m_addr[w];
}

int ceph_mdsmap_decode(struct ceph_mdsmap *m, 
		       struct ceph_bufferlist *bl, 
		       struct ceph_bufferlist_iterator *bli)
{
	int i, n;
	__u32 mds;
	int err;
	
	if ((err = ceph_bl_decode_64(bl, bli, &m->m_epoch)) != 0)
		goto bad;
	if ((err = ceph_bl_decode_64(bl, bli, &m->m_client_epoch)) != 0)
		goto bad;
	if ((err = ceph_bl_decode_32(bl, bli, &m->m_created.tv_sec)) != 0)
		goto bad;
	if ((err = ceph_bl_decode_32(bl, bli, &m->m_created.tv_usec)) != 0)
		goto bad;
	if ((err = ceph_bl_decode_32(bl, bli, &m->m_anchortable)) != 0)
		goto bad;
	if ((err = ceph_bl_decode_32(bl, bli, &m->m_root)) != 0)
		goto bad;
	if ((err = ceph_bl_decode_32(bl, bli, &m->m_max_mds)) != 0)
		goto bad;

	m->m_addr = kmalloc(m->m_max_mds*sizeof(*m->m_addr), GFP_KERNEL);
	m->m_state = kmalloc(m->m_max_mds*sizeof(*m->m_state), GFP_KERNEL);
	memset(m->m_state, 0, m->m_max_mds);
	
	/* state */
	if ((err = ceph_bl_decode_32(bl, bli, &n)) != 0)
		goto bad;
	for (i=0; i<n; i++) {
		if ((err = ceph_bl_decode_32(bl, bli, &mds)) != 0)
			goto bad;
		if ((err = ceph_bl_decode_32(bl, bli, &m->m_state[mds])) != 0)
			goto bad;
	}

	/* state_seq */
	if ((err = ceph_bl_decode_32(bl, bli, &n)) != 0)
		goto bad;
	ceph_bl_iterator_advance(bl, bli, n*(sizeof(__u32)+sizeof(__u64)));
	
	/* mds_inst */
	if ((err = ceph_bl_decode_32(bl, bli, &n)) != 0)
		goto bad;
	for (i=0; i<n; i++) {
		if ((err = ceph_bl_decode_32(bl, bli, &mds)) != 0)
			goto bad;
		ceph_bl_iterator_advance(bl, bli, sizeof(struct ceph_entity_name));
		if ((err = ceph_bl_decode_addr(bl, bli, &m->m_addr[mds])) != 0)
			goto bad;
	}

	/* ok, we don't care about the rest. */
	return 0;

bad:
	derr(0, "corrupt mdsmap");
	return -EINVAL;
}


