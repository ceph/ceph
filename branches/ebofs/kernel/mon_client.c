
#include <linux/types.h>
#include <linux/random.h>
#include "mon_client.h"


static int pick_mon(struct ceph_mon_client *monc, int notmon)
{
	if (notmon < 0 && monc->last_mon >= 0)
		return monc->last_mon;
	monc->last_mon = get_random_int() % monc->monmap.num_mon;
	return monc->last_mon;
}



