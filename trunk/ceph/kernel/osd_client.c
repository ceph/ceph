
#include <linux/slab.h>
#include <linux/err.h>

#include "../crush/crush.h"
#include "osd_client.h"
#include "messenger.h"

/* maps */

static int calc_bits_of(int t) 
{
	int b = 0;
	while (t) {
		t = t >> 1;
		b++;
	}
	return b;
}

static void calc_pg_masks(struct ceph_osdmap *map)
{
	map->pg_num_mask = (1 << calc_bits_of(map->pg_num-1)) - 1;
	map->localized_pg_num_mask = (1 << calc_bits_of(map->localized_pg_num-1)) - 1;
}

static struct crush_map *crush_decode(void **p, void *end)
{
	struct crush_map *c;
	int err = -EINVAL;
	
	c = kmalloc(sizeof(*c), GFP_KERNEL);
	if (c == NULL)
		return ERR_PTR(-ENOMEM);

	/* ... */

	return c;

bad:
	return ERR_PTR(err);
}

void osdmap_destroy(struct ceph_osdmap *map)
{
	if (map->osd_state) kfree(map->osd_state);
	if (map->crush) kfree(map->crush);
	kfree(map);
}

static struct ceph_osdmap *osdmap_decode(void **p, void *end)
{
	struct ceph_osdmap *map;
	__u32 crushlen;
	int err;

	map = kmalloc(sizeof(*map), GFP_KERNEL);
	if (map == NULL) 
		return ERR_PTR(-ENOMEM);

	if ((err = ceph_decode_64(p, end, &map->fsid.major)) < 0)
		goto bad;
	if ((err = ceph_decode_64(p, end, &map->fsid.minor)) < 0)
		goto bad;
	if ((err = ceph_decode_64(p, end, &map->epoch)) < 0)
		goto bad;
	if ((err = ceph_decode_64(p, end, &map->mon_epoch)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &map->ctime.tv_sec)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &map->ctime.tv_usec)) < 0)
		goto bad;

	if ((err = ceph_decode_32(p, end, &map->pg_num)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &map->localized_pg_num)) < 0)
		goto bad;
	calc_pg_masks(map);

	if ((err = ceph_decode_32(p, end, &map->max_osd)) < 0)
		goto bad;

	/* alloc */
	map->osd_state = kmalloc(map->max_osd * (sizeof(__u32)*2 + 
						 sizeof(struct ceph_entity_addr)),
				 GFP_KERNEL);
	if (map->osd_state == NULL) 
		goto bad;
	map->osd_offload = (void*)((__u32*)map->osd_state + map->max_osd);
	map->osd_addr = (void*)(map->osd_offload + map->max_osd);
	
	/* osds */
	if ((err = ceph_decode_copy(p, end, &map->osd_state, map->max_osd)) < 0)
		goto bad;
	if ((err = ceph_decode_copy(p, end, &map->osd_offload, map->max_osd*sizeof(*map->osd_offload))) < 0)
		goto bad;
	if ((err = ceph_decode_copy(p, end, &map->osd_addr, map->max_osd*sizeof(*map->osd_addr))) < 0)
		goto bad;

	/* crush */
	if ((err = ceph_decode_32(p, end, &crushlen)) < 0)
		goto bad;
	map->crush = crush_decode(p, end);
	if (IS_ERR(map->crush)) {
		err = PTR_ERR(map->crush);
		map->crush = 0;
		goto bad;
	}

	return map;

bad:
	osdmap_destroy(map);
	return ERR_PTR(err);
}




void ceph_osdc_init(struct ceph_osd_client *osdc)
{
	osdc->osdmap = NULL;
}

void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	dout(1, "ceph_osdc_handle_map - implement me\n");
	ceph_msg_put(msg);
}


