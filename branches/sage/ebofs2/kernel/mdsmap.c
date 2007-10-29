
#include "mdsmap.h"
#include <linux/random.h>

int ceph_mdsmap_get_state(ceph_mdsmap *m, int w)
{
	BUG_ON(w < 0);
	if (w >= m->m_max_mds)
		return CEPH_MDS_STATE_DNE;
	return = m->m_state[w];
}

int ceph_mdsmap_get_random_mds(ceph_mdsmap *m)
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
		while (m->state[i] <= 0) i++;

	return i;
}


struct ceph_entity_addr *ceph_mdsmap_get_addr(ceph_mdsmap *m, int w)
{
	if (w >= m->m_max_mds)
		return NULL;
	return m->m_addr[w];
}

int ceph_mdsmap_decode(struct ceph_mdsmap *m, 
		       struct ceph_bufferlist *bl, 
		       struct ceph_bufferlist_iterator *bli)
{
	int i, n;
	__u32 mds;
	struct ceph_entity_inst *inst;
	
	m->m_epoch = ceph_bl_decode_u64(bl, bli);
	ceph_bl_decode_u32(bl, bli); /* target_num */
	m->m_created.tv_sec = ceph_bl_decode_u32(bl, bli);
	m->m_created.tv_usec = ceph_bl_decode_u32(bl, bli);
	ceph_bl_decode_u64(bl, bli); /* same_in_set_since */
	m->m_anchortable = ceph_bl_decode_s32(bl, bli);
	m->m_root = ceph_bl_decode_s32(bl, bli);
	m->m_max_mds = ceph_bl_decode_u32(bl, bli);

	m->m_addr = kmalloc(sizeof(struct ceph_entity_addr)*m->m_max_mds, GFP_KERNEL);
	m->m_state = kmalloc(sizeof(__u8)*m->m_max_mds, GFP_KERNEL);
	memset(m->m_state, 0, sizeof(__u8)*m->m_max_mds);
	
	/* created */
	n = ceph_bl_decode_u32(bl, bli);
	ceph_bl_iterator_advance(bli, n*sizeof(__u32));
	
	/* state */
	n = ceph_bl_decode_u32(bl, bli);
	for (i=0; i<n; i++) {
		mds = ceph_bl_decode_u32(bl, bli);
		m->m_state[mds] = ceph_bl_decode_s32(bl, bli);
	}

	/* state_seq */
	n = ceph_bl_decode_u32(bl, bli);
	ceph_bl_iterator_advance(bli, n*2*sizeof(__u32));

	/* mds_inst */
	n = ceph_bl_decode_u32(bl, bli);
	for (i=0; i<n; i++) {
		mds = ceph_bl_decode_u32(bl, bli);
		inst = ceph
		ceph_bl_iterator_advance(bli, sizeof(struct ceph_entity_name));
		m->m_addr[mds].nonce = ceph_bl_decode_u64(bl, bli);
		m->m_addr[mds].port = ceph_bl_decode_u32(bl, bli);
		m->m_addr[mds].ipq[0] = ceph_bl_decode_u8(bl, bli);
		m->m_addr[mds].ipq[1] = ceph_bl_decode_u8(bl, bli);
		m->m_addr[mds].ipq[2] = ceph_bl_decode_u8(bl, bli);
		m->m_addr[mds].ipq[3] = ceph_bl_decode_u8(bl, bli);
	}

	/* mds_inc */

	return 0;
}


