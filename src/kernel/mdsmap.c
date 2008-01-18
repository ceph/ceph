#include <linux/err.h>
#include <linux/types.h>
#include <linux/random.h>
#include <linux/slab.h>
#include <asm/bug.h>

#include "mdsmap.h"
#include "messenger.h"

int ceph_mdsmap_debug = 50;
#define DOUT_VAR ceph_mdsmap_debug
#define DOUT_PREFIX "mdsmap: "
#include "super.h"

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
	char r;

	/* count */
	for (i=0; i<m->m_max_mds; i++)
		if (m->m_state > 0) n++;
	if (n == 0) 
		return -1;
	
	/* pick */
	get_random_bytes(&r, 1);
	n = r % n;
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

struct ceph_mdsmap *ceph_mdsmap_decode(void **p, void *end)
{
	struct ceph_mdsmap *m;
	int i, n;
	__u32 mds;
	int err;
	
	m = kzalloc(sizeof(*m), GFP_KERNEL);
	if (m == NULL) 
		return ERR_PTR(-ENOMEM);

	if ((err = ceph_decode_32(p, end, &m->m_epoch)) != 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &m->m_client_epoch)) != 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &m->m_created.tv_sec)) != 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &m->m_created.tv_usec)) != 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &m->m_anchortable)) != 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &m->m_root)) != 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &m->m_max_mds)) != 0)
		goto bad;

	m->m_addr = kmalloc(m->m_max_mds*sizeof(*m->m_addr), GFP_KERNEL);
	m->m_state = kzalloc(m->m_max_mds*sizeof(*m->m_state), GFP_KERNEL);
	
	/* state */
	if ((err = ceph_decode_32(p, end, &n)) != 0)
		goto bad;
	for (i=0; i<n; i++) {
		if ((err = ceph_decode_32(p, end, &mds)) != 0)
			goto bad;
		if (mds >= m->m_max_mds)
			goto bad;
		if ((err = ceph_decode_32(p, end, &m->m_state[mds])) != 0)
			goto bad;
	}

	/* state_seq */
	if ((err = ceph_decode_32(p, end, &n)) != 0)
		goto bad;
	*p += n*(sizeof(__u32)+sizeof(__u64));
	
	/* mds_inst */
	if ((err = ceph_decode_32(p, end, &n)) != 0)
		goto bad;
	for (i=0; i<n; i++) {
		if ((err = ceph_decode_32(p, end, &mds)) != 0)
			goto bad;
		if (mds >= m->m_max_mds)
			goto bad;
		*p += sizeof(struct ceph_entity_name);
		if ((err = ceph_decode_addr(p, end, &m->m_addr[mds])) != 0)
			goto bad;
	}

	/* ok, we don't care about the rest. */
	dout(30, "mdsmap_decode success epoch %u\n", m->m_epoch);
	return m;

bad:
	derr(0, "corrupt mdsmap");
	ceph_mdsmap_destroy(m);
	return ERR_PTR(-EINVAL);
}

void ceph_mdsmap_destroy(struct ceph_mdsmap *m)
{
	if (m->m_addr) kfree(m->m_addr);
	if (m->m_state) kfree(m->m_state);
	kfree(m);
}
