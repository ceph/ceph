
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

int ceph_mdsmap_decode(ceph_mdsmap *m, ceph_bufferlist *bl)
{
	/* write me */
}
