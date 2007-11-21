#include <linux/slab.h>
#include "monmap.h"
#include "messenger.h"

int ceph_monmap_decode(struct ceph_monmap *m, void **p, void *end)
{
	int err;

	if ((err = ceph_decode_64(p, end, &m->epoch)) < 0)
		return err;
	if ((err = ceph_decode_32(p, end, &m->num_mon)) < 0)
		return err;

	m->mon_inst = kmalloc(m->num_mon*sizeof(*m->mon_inst), GFP_KERNEL);
	if (m->mon_inst == NULL)
		return -ENOMEM;
	if ((err = ceph_decode_copy(p, end, m->mon_inst, m->num_mon*sizeof(m->mon_inst[0]))) < 0)
		goto bad;

	return 0;

bad:
	kfree(m->mon_inst);
	m->mon_inst = 0;
	return err;
}
