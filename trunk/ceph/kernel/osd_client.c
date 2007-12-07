#include <linux/slab.h>
#include <linux/err.h>

#include "crush/crush.h"
#include "osd_client.h"
#include "messenger.h"

int ceph_osdc_debug = 50;
#define DOUT_VAR ceph_osdc_debug
#define DOUT_PREFIX "osdc: "
#include "super.h"

/* maps */

static int calc_bits_of(unsigned t) 
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

static int crush_decode_uniform_bucket(void **p, void *end, struct crush_bucket_uniform *b)
{
	int j, err;
	dout(30, "crush_decode_uniform_bucket %p to %p\n", *p, end);
	b->primes = kmalloc(b->h.size * sizeof(__u32), GFP_KERNEL);
	if (b->primes == NULL)
		return -ENOMEM;
	for (j=0; j<b->h.size; j++)
		if ((err = ceph_decode_32(p, end, &b->primes[j])) < 0)
			return err;
	if ((err = ceph_decode_32(p, end, &b->item_weight)) < 0)
		return -EINVAL;
	return 0;
}

static int crush_decode_list_bucket(void **p, void *end, struct crush_bucket_list *b)
{
	int j, err;
	dout(30, "crush_decode_list_bucket %p to %p\n", *p, end);
	b->item_weights = kmalloc(b->h.size * sizeof(__u32), GFP_KERNEL);
	if (b->item_weights == NULL)
		return -ENOMEM;
	b->sum_weights = kmalloc(b->h.size * sizeof(__u32), GFP_KERNEL);
	if (b->sum_weights == NULL)
		return -ENOMEM;
	for (j=0; j<b->h.size; j++) {
		if ((err = ceph_decode_32(p, end, &b->item_weights[j])) < 0)
			return err;
		if ((err = ceph_decode_32(p, end, &b->sum_weights[j])) < 0)
			return err;
	}
	return 0;
}

static int crush_decode_tree_bucket(void **p, void *end, struct crush_bucket_tree *b)
{
	int j, err;
	dout(30, "crush_decode_tree_bucket %p to %p\n", *p, end);
	b->node_weights = kmalloc(b->h.size * sizeof(__u32), GFP_KERNEL);
	if (b->node_weights == NULL)
		return -ENOMEM;
	for (j=0; j<b->h.size; j++) 
		if ((err = ceph_decode_32(p, end, &b->node_weights[j])) < 0)
			return err;
	return 0;
}

static int crush_decode_straw_bucket(void **p, void *end, struct crush_bucket_straw *b)
{
	int j, err;
	dout(30, "crush_decode_straw_bucket %p to %p\n", *p, end);
	b->straws = kmalloc(b->h.size * sizeof(__u32), GFP_KERNEL);
	if (b->straws == NULL)
		return -ENOMEM;
	for (j=0; j<b->h.size; j++) 
		if ((err = ceph_decode_32(p, end, &b->straws[j])) < 0)
			return err;
	return 0;
}

static struct crush_map *crush_decode(void **p, void *end)
{
	struct crush_map *c;
	int err = -EINVAL;
	int i, j;
	void *start = *p;

	dout(30, "crush_decode %p to %p\n", *p, end);

	c = kzalloc(sizeof(*c), GFP_KERNEL);
	if (c == NULL)
		return ERR_PTR(-ENOMEM);

	if ((err = ceph_decode_32(p, end, &c->max_buckets)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &c->max_rules)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &c->max_devices)) < 0)
		goto bad;

	c->device_offload = kmalloc(c->max_devices * sizeof(__u32), GFP_KERNEL);
	if (c->device_offload == NULL) 
		goto badmem;
	c->device_parents = kmalloc(c->max_devices * sizeof(__u32), GFP_KERNEL);
	if (c->device_parents == NULL) 
		goto badmem;
	c->bucket_parents = kmalloc(c->max_buckets * sizeof(__u32), GFP_KERNEL);
	if (c->bucket_parents == NULL) 
		goto badmem;

	c->buckets = kmalloc(c->max_buckets * sizeof(*c->buckets), GFP_KERNEL);
	if (c->buckets == NULL) 
		goto badmem;
	c->rules = kmalloc(c->max_rules * sizeof(*c->rules), GFP_KERNEL);
	if (c->rules == NULL)
		goto badmem;

	for (i=0; i<c->max_devices; i++)
		if ((err = ceph_decode_32(p, end, &c->device_offload[i])) < 0)
			goto bad;

	/* buckets */
	for (i=0; i<c->max_buckets; i++) {
		int size = 0;
		__u32 type;
		struct crush_bucket *b;

		//dout(30, "crush_decode bucket %d off %x %p to %p\n", i, (int)(*p-start), *p, end);

		if ((err = ceph_decode_32(p, end, &type)) < 0)
			goto bad;
		//dout(30, "crush_decode type %d\n", type);
		if (type == 0) {
			c->buckets[i] = 0;
			continue;
		}

		switch (type) {
		case CRUSH_BUCKET_UNIFORM:
			size = sizeof(struct crush_bucket_uniform);
			break;
		case CRUSH_BUCKET_LIST:
			size = sizeof(struct crush_bucket_list);
			break;
		case CRUSH_BUCKET_TREE:
			size = sizeof(struct crush_bucket_tree);
			break;
		case CRUSH_BUCKET_STRAW:
			size = sizeof(struct crush_bucket_straw);
			break;
		}
		BUG_ON(size == 0);
		b = c->buckets[i] = kzalloc(size, GFP_KERNEL);
		if (b == NULL)
			goto badmem;

		if ((err = ceph_decode_32(p, end, &b->id)) < 0)
			goto bad;
		if ((err = ceph_decode_16(p, end, &b->type)) < 0)
			goto bad;
		if ((err = ceph_decode_16(p, end, &b->bucket_type)) < 0)
			goto bad;
		if ((err = ceph_decode_32(p, end, &b->weight)) < 0)
			goto bad;
		if ((err = ceph_decode_32(p, end, &b->size)) < 0)
			goto bad;

		//dout(30, "crush_decode bucket size %d off %x %p to %p\n", b->size, (int)(*p-start), *p, end);

		b->items = kmalloc(b->size * sizeof(__s32), GFP_KERNEL);
		if (b->items == NULL)
			goto badmem;
		for (j=0; j<b->size; j++)
			if ((err = ceph_decode_32(p, end, &b->items[j])) < 0)
				goto bad;
		
		switch (b->type) {
		case CRUSH_BUCKET_UNIFORM:
			if ((err = crush_decode_uniform_bucket(p, end, 
				      (struct crush_bucket_uniform*)b)) < 0)
				goto bad;
			break;
		case CRUSH_BUCKET_LIST:
			if ((err = crush_decode_list_bucket(p, end, 
				      (struct crush_bucket_list*)b)) < 0)
				goto bad;
			break;
		case CRUSH_BUCKET_TREE:
			if ((err = crush_decode_tree_bucket(p, end, 
				      (struct crush_bucket_tree*)b)) < 0)
				goto bad;
			break;
		case CRUSH_BUCKET_STRAW:
			if ((err = crush_decode_straw_bucket(p, end, 
				      (struct crush_bucket_straw*)b)) < 0)
				goto bad;
			break;
		}
	}

	/* rules */
	for (i=0; i<c->max_rules; i++) {
		__u32 yes;
		struct crush_rule *r;

		dout(30, "crush_decode rule %d off %x %p to %p\n", i, (int)(*p-start), *p, end);

		if ((err = ceph_decode_32(p, end, &yes)) < 0)
			goto bad;
		//dout(30, "crush_decode yes = %d off %x %p to %p\n", yes, (int)(*p-start), *p, end);
		if (!yes) {
			c->rules[i] = 0;
			continue;
		}

		if ((err = ceph_decode_32(p, end, &yes)) < 0)
			goto bad;

		r = c->rules[i] = kmalloc(sizeof(**c->rules) + yes*sizeof(struct crush_rule_step),
					  GFP_KERNEL);
		if (r == NULL)
			goto badmem;
		r->len = yes;
		for (j=0; j<r->len; j++) {
			if ((err = ceph_decode_32(p, end, &r->steps[j].op)) < 0)
				goto bad;
			if ((err = ceph_decode_32(p, end, &r->steps[j].arg1)) < 0)
				goto bad;
			if ((err = ceph_decode_32(p, end, &r->steps[j].arg2)) < 0)
				goto bad;
		}
	}

	
	dout(30, "crush_decode success\n");
	return c;
	
badmem:
	err = -ENOMEM;
bad:
	dout(30, "crush_decode fail %d\n", err);
	crush_destroy(c);
	return ERR_PTR(err);
}

static void osdmap_destroy(struct ceph_osdmap *map)
{
	if (map->osd_state) kfree(map->osd_state);
	if (map->crush) kfree(map->crush);
	kfree(map);
}

static int osdmap_set_max_osd(struct ceph_osdmap *map, int max)
{
	__u8 *state;
	struct ceph_entity_addr *addr;

	state = kzalloc(max * (sizeof(__u32) + 
			       sizeof(struct ceph_entity_addr)),
			GFP_KERNEL);
	if (state == NULL) 
		return -ENOMEM;
	addr = (void*)((__u32*)state + max);

	/* copy old? */
	if (map->osd_state) {
		memcpy(state, map->osd_state, map->max_osd);
		memcpy(addr, map->osd_addr, map->max_osd);
		kfree(map->osd_state);
	}

	map->osd_state = state;
	map->osd_addr = addr;
	map->max_osd = max;
	return 0;
}

static struct ceph_osdmap *osdmap_decode(void **p, void *end)
{
	struct ceph_osdmap *map;
	__u32 len, max;
	int i;
	int err;
	void *start = *p;

	dout(30, "osdmap_decode from %p to %p\n", *p, end);

	map = kzalloc(sizeof(*map), GFP_KERNEL);
	if (map == NULL) 
		return ERR_PTR(-ENOMEM);

	if ((err = ceph_decode_64(p, end, &map->fsid.major)) < 0)
		goto bad;
	if ((err = ceph_decode_64(p, end, &map->fsid.minor)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &map->epoch)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &map->mon_epoch)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &map->ctime.tv_sec)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &map->ctime.tv_usec)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &map->mtime.tv_sec)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &map->mtime.tv_usec)) < 0)
		goto bad;

	if ((err = ceph_decode_32(p, end, &map->pg_num)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &map->localized_pg_num)) < 0)
		goto bad;

	calc_pg_masks(map);

	if ((err = ceph_decode_32(p, end, &max)) < 0)
		goto bad;

	/* (re)alloc osd arrays */
	if ((err = osdmap_set_max_osd(map, max)) < 0)
	    goto bad;
	dout(30, "osdmap_decode max_osd = %d\n", map->max_osd);
	
	/* osds */
	*p += 4; /* skip length field (should match max) */
	if ((err = ceph_decode_copy(p, end, map->osd_state, map->max_osd)) < 0)
		goto bad;
	*p += 4; /* skip length field (should match max) */
	if ((err = ceph_decode_copy(p, end, map->osd_addr, map->max_osd*sizeof(*map->osd_addr))) < 0)
		goto bad;

	/* pg primary swapping */
	if ((err = ceph_decode_32(p, end, &len)) < 0)
		goto bad;
	if (len) {
		map->pg_swap_primary = kmalloc(len * sizeof(*map->pg_swap_primary), GFP_KERNEL);
		if (map->pg_swap_primary == NULL) {
			err = -ENOMEM;
			goto bad;
		}
		map->num_pg_swap_primary = len;
		for (i=0; i<len; i++) {
			if ((err = ceph_decode_64(p, end, &map->pg_swap_primary[i].pg.pg64)) < 0)
				goto bad;
			if ((err = ceph_decode_32(p, end, &map->pg_swap_primary[i].osd)) < 0)
				goto bad;
		}
	}

	/* crush */
	if ((err = ceph_decode_32(p, end, &len)) < 0)
		goto bad;
	dout(30, "osdmap_decode crush len %d from off %x\n", len, (int)(*p - start));
	map->crush = crush_decode(p, end);
	if (IS_ERR(map->crush)) {
		err = PTR_ERR(map->crush);
		map->crush = 0;
		goto bad;
	}

	dout(30, "osdmap_decode done %p %p\n", *p, end);
	BUG_ON(*p < end);

	return map;

bad:
	dout(30, "osdmap_decode fail\n");
	osdmap_destroy(map);
	return ERR_PTR(err);
}

static struct ceph_osdmap *apply_incremental(void **p, void *end, struct ceph_osdmap *map)
{
	struct ceph_osdmap *newmap = map;
	struct crush_map *newcrush = 0;
	struct ceph_fsid fsid;
	__u32 epoch, mon_epoch;
	struct ceph_timeval ctime;
	__u32 len;
	__u32 max;
	int err;

	if ((err = ceph_decode_64(p, end, &fsid.major)) < 0)
		goto bad;
	if ((err = ceph_decode_64(p, end, &fsid.minor)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &epoch)) < 0)
		goto bad;
	BUG_ON(epoch != map->epoch+1);
	if ((err = ceph_decode_32(p, end, &mon_epoch)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &ctime.tv_sec)) < 0)
		goto bad;
	if ((err = ceph_decode_32(p, end, &ctime.tv_usec)) < 0)
		goto bad;

	/* full map? */
	if ((err = ceph_decode_32(p, end, &len)) < 0)
		goto bad;
	if (len > 0) {
		newmap = osdmap_decode(p, min(*p+len, end));
		return newmap;  /* error or not */
	}

	/* new crush? */
	if ((err = ceph_decode_32(p, end, &len)) < 0)
		goto bad;
	if (len > 0) {
		newcrush = crush_decode(p, min(*p+len, end));
		if (IS_ERR(newcrush))
			return ERR_PTR(PTR_ERR(newcrush));
	}

	/* FIXME: from this point on i'm optimisticaly assuming the message is complete */

	/* new max? */
	if ((err = ceph_decode_32(p, end, &max)) < 0)
		goto bad;
	if (max > 0) {
		if ((err = osdmap_set_max_osd(map, max)) < 0)
			goto bad;
	}

	map->epoch++;
	map->mon_epoch = mon_epoch;
	map->ctime = map->ctime;
	if (newcrush) {
		if (map->crush) 
			crush_destroy(map->crush);
		map->crush = newcrush;
	}

	/* new_up */
	if ((err = ceph_decode_32(p, end, &len)) < 0)
		goto bad;
	while (len--) {
		__u32 osd;
		struct ceph_entity_addr addr;
		if ((err = ceph_decode_32(p, end, &osd)) < 0)
			goto bad;
		if ((err = ceph_decode_addr(p, end, &addr)) < 0)
			goto bad;
		dout(1, "osd%d up\n", osd);
		BUG_ON(osd >= map->max_osd);
		map->osd_state[osd] |= CEPH_OSD_UP;
		map->osd_addr[osd] = addr;
	}

	/* new_down */
	if ((err = ceph_decode_32(p, end, &len)) < 0)
		goto bad;
	while (len--) {
		__u32 osd;
		if ((err = ceph_decode_32(p, end, &osd)) < 0)
			goto bad;
		dout(1, "osd%d down\n", osd);
		if (osd < map->max_osd) 
			map->osd_state[osd] &= ~CEPH_OSD_UP;
	}

	/* new_offload */
	if ((err = ceph_decode_32(p, end, &len)) < 0)
		goto bad;
	while (len--) {
		__u32 osd, off;
		if ((err = ceph_decode_32(p, end, &osd)) < 0)
			goto bad;
		if ((err = ceph_decode_32(p, end, &off)) < 0)
			goto bad;
		dout(1, "osd%d offload %x\n", osd, off);
		if (osd < map->max_osd) 
			map->crush->device_offload[osd] = off;
	}
	
	return map;

bad:
	return ERR_PTR(err);
}

void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	void *p, *end, *next;
	__u32 nr_maps, maplen;
	__u32 epoch;
	struct ceph_osdmap *newmap = 0;
	int err;

	dout(1, "handle_map\n");
	p = msg->front.iov_base;
	end = p + msg->front.iov_len;

	/* incremental maps */
	if ((err = ceph_decode_32(&p, end, &nr_maps)) < 0)
		goto bad;
	dout(10, " %d inc maps\n", nr_maps);
	while (nr_maps--) {
		if ((err = ceph_decode_32(&p, end, &epoch)) < 0)
			goto bad;
		if ((err = ceph_decode_32(&p, end, &maplen)) < 0)
			goto bad;
		next = p + maplen;
		if (osdc->osdmap && osdc->osdmap->epoch+1 == epoch) {
			dout(10, "applying incremental map %u len %d\n", epoch, maplen);
			newmap = apply_incremental(p, min(p+maplen,end), osdc->osdmap);
			if (IS_ERR(newmap)) {
				err = PTR_ERR(newmap);
				goto bad;
			}
			if (newmap != osdc->osdmap) {
				osdmap_destroy(osdc->osdmap);
				osdc->osdmap = newmap;
			}
		} else {
			dout(10, "ignoring incremental map %u len %d\n", epoch, maplen);
		}
		p = next;
	}
	if (newmap) 
		goto out;
	
	/* full maps */
	if ((err = ceph_decode_32(&p, end, &nr_maps)) < 0)
		goto bad;
	dout(30, " %d full maps\n", nr_maps);
	while (nr_maps > 1) {
		if ((err = ceph_decode_32(&p, end, &epoch)) < 0)
			goto bad;
		if ((err = ceph_decode_32(&p, end, &maplen)) < 0)
			goto bad;
		dout(5, "skipping non-latest full map %u len %d\n", epoch, maplen);
		p += maplen;
	}
	if (nr_maps) {
		if ((err = ceph_decode_32(&p, end, &epoch)) < 0)
			goto bad;
		if ((err = ceph_decode_32(&p, end, &maplen)) < 0)
			goto bad;
		if (osdc->osdmap && osdc->osdmap->epoch >= epoch) {
			dout(10, "skipping full map %u len %d, older than our %u\n", 
			     epoch, maplen, osdc->osdmap->epoch);
			p += maplen;
		} else {
			dout(10, "taking full map %u len %d\n", epoch, maplen);
			newmap = osdmap_decode(&p, min(p+maplen,end));
			if (IS_ERR(newmap)) {
				err = PTR_ERR(newmap);
				goto bad;
			}
			if (osdc->osdmap)
				osdmap_destroy(osdc->osdmap);
			osdc->osdmap = newmap;
		}
	}
	dout(1, "osdc handle_map done\n");
	
out:
	return;

bad:
	derr(1, "osdc handle_map corrupt msg\n");
	goto out;
}



void ceph_osdc_init(struct ceph_osd_client *osdc)
{
	dout(5, "init\n");
	osdc->osdmap = NULL;
	osdc->last_tid = 0;
	INIT_RADIX_TREE(&osdc->request_tree, GFP_KERNEL);
	osdc->last_requested_map = 0;
	init_completion(&osdc->map_waiters);
}


void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	dout(5, "handle_reply\n");
}

