
#include <linux/types.h>
#include <linux/random.h>
#include "mon_client.h"

int ceph_mon_debug = 50;
#define DOUT_VAR ceph_mon_debug
#define DOUT_PREFIX "mon: "
#include "super.h"


int ceph_monmap_decode(struct ceph_monmap *m, void *p, void *end)
{
	int err;
	void *old;

	dout(30, "monmap_decode %p %p\n", p, end);

	if ((err = ceph_decode_32(&p, end, &m->epoch)) < 0)
		goto bad;
	if ((err = ceph_decode_64(&p, end, &m->fsid.major)) < 0)
		goto bad;
	if ((err = ceph_decode_64(&p, end, &m->fsid.minor)) < 0)
		goto bad;
	if ((err = ceph_decode_32(&p, end, &m->num_mon)) < 0)
		return err;

	old = m->mon_inst;
	m->mon_inst = kmalloc(m->num_mon*sizeof(*m->mon_inst), GFP_KERNEL);
	if (m->mon_inst == NULL) {
		m->mon_inst = old;
		return -ENOMEM;
	}
	kfree(old);

	if ((err = ceph_decode_copy(&p, end, m->mon_inst, m->num_mon*sizeof(m->mon_inst[0]))) < 0)
		goto bad;

	dout(30, "monmap_decode got epoch %d, num_mon %d\n", m->epoch, m->num_mon);
	return 0;

bad:
	dout(30, "monmap_decode failed with %d\n", err);
	return err;
}

static int pick_mon(struct ceph_mon_client *monc, int notmon)
{
	char r;
	if (notmon < 0 && monc->last_mon >= 0)
		return monc->last_mon;
	get_random_bytes(&r, 1);
	monc->last_mon = r % monc->monmap.num_mon;
	return monc->last_mon;
}


void ceph_monc_init(struct ceph_mon_client *monc)
{
	dout(5, "ceph_monc_init\n");
	memset(monc, 0, sizeof(*monc));
}



void ceph_monc_request_mdsmap(struct ceph_mon_client *monc, __u64 have)
{
	dout(5, "ceph_monc_request_mdsmap -- IMPLEMENT ME\n");
	
}
