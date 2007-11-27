
#include <linux/types.h>
#include <linux/random.h>
#include "mon_client.h"


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
	dout(5, "ceph_monc_request_mdsmap\n");
}
