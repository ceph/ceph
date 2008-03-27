
#include <asm/div64.h>

int ceph_osdmap_debug = 50;
#define DOUT_VAR ceph_osdmap_debug
#define DOUT_PREFIX "osdmap: "
#include "super.h"

#include "osdmap.h"
#include "crush/hash.h"

#include "decode.h"

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
	map->pgp_num_mask = (1 << calc_bits_of(map->pgp_num-1)) - 1;
	map->lpg_num_mask = (1 << calc_bits_of(map->lpg_num-1)) - 1;
	map->lpgp_num_mask = (1 << calc_bits_of(map->lpgp_num-1)) - 1;
}

static int crush_decode_uniform_bucket(void **p, void *end, 
				       struct crush_bucket_uniform *b)
{
	int j;
	dout(30, "crush_decode_uniform_bucket %p to %p\n", *p, end);
	b->primes = kmalloc(b->h.size * sizeof(__u32), GFP_KERNEL);
	if (b->primes == NULL)
		return -ENOMEM;
	ceph_decode_need(p, end, (1+b->h.size) * sizeof(__u32), bad);
	for (j=0; j<b->h.size; j++)
		ceph_decode_32(p, b->primes[j]);
	ceph_decode_32(p, b->item_weight);
	return 0;
bad:
	return -EINVAL;
}

static int crush_decode_list_bucket(void **p, void *end,
				    struct crush_bucket_list *b)
{
	int j;
	dout(30, "crush_decode_list_bucket %p to %p\n", *p, end);
	b->item_weights = kmalloc(b->h.size * sizeof(__u32), GFP_KERNEL);
	if (b->item_weights == NULL)
		return -ENOMEM;
	b->sum_weights = kmalloc(b->h.size * sizeof(__u32), GFP_KERNEL);
	if (b->sum_weights == NULL)
		return -ENOMEM;
	ceph_decode_need(p, end, 2 * b->h.size * sizeof(__u32), bad);
	for (j=0; j<b->h.size; j++) {
		ceph_decode_32(p, b->item_weights[j]);
		ceph_decode_32(p, b->sum_weights[j]);
	}
	return 0;
bad:
	return -EINVAL;
}

static int crush_decode_tree_bucket(void **p, void *end, 
				    struct crush_bucket_tree *b)
{
	int j;
	dout(30, "crush_decode_tree_bucket %p to %p\n", *p, end);
	b->node_weights = kmalloc(b->h.size * sizeof(__u32), GFP_KERNEL);
	if (b->node_weights == NULL)
		return -ENOMEM;
	ceph_decode_need(p, end, b->h.size * sizeof(__u32), bad);
	for (j=0; j<b->h.size; j++) 
		ceph_decode_32(p, b->node_weights[j]);
	return 0;
bad:
	return -EINVAL;
}

static int crush_decode_straw_bucket(void **p, void *end, 
				     struct crush_bucket_straw *b)
{
	int j;
	dout(30, "crush_decode_straw_bucket %p to %p\n", *p, end);
	b->straws = kmalloc(b->h.size * sizeof(__u32), GFP_KERNEL);
	if (b->straws == NULL)
		return -ENOMEM;
	ceph_decode_need(p, end, 2 * b->h.size * sizeof(__u32), bad);
	for (j=0; j<b->h.size; j++) {
		ceph_decode_32(p, b->item_weights[j]);
		ceph_decode_32(p, b->straws[j]);
	}
	return 0;
bad:
	return -EINVAL;
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

	ceph_decode_need(p, end, 3*sizeof(__u32), bad);
	ceph_decode_32(p, c->max_buckets);
	ceph_decode_32(p, c->max_rules);
	ceph_decode_32(p, c->max_devices);

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

	ceph_decode_need(p, end, c->max_devices * sizeof(__u32), bad);
	for (i=0; i<c->max_devices; i++)
		ceph_decode_32(p, c->device_offload[i]);
	
	/* buckets */
	for (i=0; i<c->max_buckets; i++) {
		int size = 0;
		__u32 type;
		struct crush_bucket *b;

		dout(30, "crush_decode bucket %d off %x %p to %p\n",
		     i, (int)(*p-start), *p, end); 

		ceph_decode_32_safe(p, end, type, bad);
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

		ceph_decode_need(p, end, 4*sizeof(__u32), bad);
		ceph_decode_32(p, b->id);
		ceph_decode_16(p, b->type);
		ceph_decode_16(p, b->alg);
		ceph_decode_32(p, b->weight);
		ceph_decode_32(p, b->size);

		dout(30, "crush_decode bucket size %d off %x %p to %p\n", 
		     b->size, (int)(*p-start), *p, end);

		b->items = kmalloc(b->size * sizeof(__s32), GFP_KERNEL);
		if (b->items == NULL)
			goto badmem;

		ceph_decode_need(p, end, b->size*sizeof(__u32), bad);
		for (j=0; j<b->size; j++)
			ceph_decode_32(p, b->items[j]);
		
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

		dout(30, "crush_decode rule %d off %x %p to %p\n", 
		     i, (int)(*p-start), *p, end);

		ceph_decode_32_safe(p, end, yes, bad);
		if (!yes) {
			c->rules[i] = 0;
			continue;
		}

		// len
		ceph_decode_32_safe(p, end, yes, bad);

		r = c->rules[i] = kmalloc(sizeof(**c->rules) +
					  yes*sizeof(struct crush_rule_step),
					  GFP_KERNEL);
		if (r == NULL)
			goto badmem;
		r->len = yes;
		ceph_decode_copy_safe(p, end, &r->mask, 4, bad); /* 4 u8's */
		ceph_decode_need(p, end, r->len*3*sizeof(__u32), bad);
		for (j=0; j<r->len; j++) {
			ceph_decode_32(p, r->steps[j].op);
			ceph_decode_32(p, r->steps[j].arg1);
			ceph_decode_32(p, r->steps[j].arg2);
		}
	}

	/* ignore trailing name maps */
		
	dout(30, "crush_decode success\n");
	return c;
	
badmem:
	err = -ENOMEM;
bad:
	dout(30, "crush_decode fail %d\n", err);
	crush_destroy(c);
	return ERR_PTR(err);
}

void osdmap_destroy(struct ceph_osdmap *map)
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

struct ceph_osdmap *osdmap_decode(void **p, void *end)
{
	struct ceph_osdmap *map;
	__u32 len, max;
	int i;
	int err = -EINVAL;
	void *start = *p;

	dout(30, "osdmap_decode from %p to %p\n", *p, end);

	map = kzalloc(sizeof(*map), GFP_KERNEL);
	if (map == NULL) 
		return ERR_PTR(-ENOMEM);

	ceph_decode_need(p, end, 2*sizeof(__u64)+9*sizeof(__u32), bad);
	ceph_decode_64(p, map->fsid.major);
	ceph_decode_64(p, map->fsid.minor);
	ceph_decode_32(p, map->epoch);
	ceph_decode_32(p, map->ctime.tv_sec);
	ceph_decode_32(p, map->ctime.tv_nsec);
	ceph_decode_32(p, map->mtime.tv_sec);
	ceph_decode_32(p, map->mtime.tv_nsec);
	ceph_decode_32(p, map->pg_num);
	ceph_decode_32(p, map->pgp_num);
	ceph_decode_32(p, map->lpg_num);
	ceph_decode_32(p, map->lpgp_num);
	ceph_decode_32(p, map->last_pg_change);

	calc_pg_masks(map);

	ceph_decode_32(p, max);

	/* (re)alloc osd arrays */
	if ((err = osdmap_set_max_osd(map, max)) < 0)
		goto bad;
	dout(30, "osdmap_decode max_osd = %d\n", map->max_osd);
	
	/* osds */
	err = -EINVAL;
	ceph_decode_need(p, end, 2*sizeof(__u32) + 
			 map->max_osd*(1+sizeof(*map->osd_addr)), bad);
	*p += 4; /* skip length field (should match max) */
	ceph_decode_copy(p, map->osd_state, map->max_osd);
	*p += 4; /* skip length field (should match max) */
	ceph_decode_copy(p, map->osd_addr, map->max_osd*sizeof(*map->osd_addr));

	/* pg primary swapping */
	ceph_decode_32_safe(p, end, len, bad);
	if (len) {
		map->pg_swap_primary = kmalloc(len * 
					       sizeof(*map->pg_swap_primary), 
					       GFP_KERNEL);
		if (map->pg_swap_primary == NULL) 
			goto badmem;
		map->num_pg_swap_primary = len;
		ceph_decode_need(p, end, sizeof(__u64)+sizeof(__u32), bad);
		for (i=0; i<len; i++) {
			ceph_decode_64(p, map->pg_swap_primary[i].pg.pg64);
			ceph_decode_32(p, map->pg_swap_primary[i].osd);
		}
	}

	/* crush */
	ceph_decode_32_safe(p, end, len, bad);
	dout(30, "osdmap_decode crush len %d from off %x\n", 
	     len, (int)(*p - start));
	map->crush = crush_decode(p, end);
	if (IS_ERR(map->crush)) {
		err = PTR_ERR(map->crush);
		map->crush = 0;
		goto bad;
	}

	dout(30, "osdmap_decode done %p %p\n", *p, end);
	/* ignore trailing bits of crush map */
	/* BUG_ON(*p < end); */

	return map;

badmem:
	err = -ENOMEM;
bad:
	dout(30, "osdmap_decode fail\n");
	osdmap_destroy(map);
	return ERR_PTR(err);
}

struct ceph_osdmap *apply_incremental(void **p, void *end,
				      struct ceph_osdmap *map)
{
	struct ceph_osdmap *newmap = map;
	struct crush_map *newcrush = 0;
	struct ceph_fsid fsid;
	__u32 epoch;
	struct ceph_timespec ctime;
	__u32 len;
	__u32 max;
	int err = -EINVAL;

	ceph_decode_need(p, end, 2*sizeof(__u64)+4*sizeof(__u32), bad);
	ceph_decode_64(p, fsid.major);
	ceph_decode_64(p, fsid.minor);
	ceph_decode_32(p, epoch);
	BUG_ON(epoch != map->epoch+1);
	ceph_decode_32(p, ctime.tv_sec);
	ceph_decode_32(p, ctime.tv_nsec);

	/* full map? */
	ceph_decode_32(p, len);
	if (len > 0) {
		newmap = osdmap_decode(p, min(*p+len, end));
		return newmap;  /* error or not */
	}

	/* new crush? */
	ceph_decode_32_safe(p, end, len, bad);
	if (len > 0) {
		newcrush = crush_decode(p, min(*p+len, end));
		if (IS_ERR(newcrush))
			return ERR_PTR(PTR_ERR(newcrush));
	}

	/*
	 * FIXME: from this point on i'm optimisticaly assuming the message
	 * is complete 
	 */

	/* new max? */
	ceph_decode_need(p, end, 3*sizeof(__u32), bad);
	ceph_decode_32(p, max);
	*p += 4*sizeof(__u32);  /* skip new_pg_num et al for now.  FIXME. */
	if (max > 0) {
		if ((err = osdmap_set_max_osd(map, max)) < 0)
			goto bad;
	}

	map->epoch++;
	map->ctime = map->ctime;
	if (newcrush) {
		if (map->crush) 
			crush_destroy(map->crush);
		map->crush = newcrush;
	}

	/* new_up */
	err = -EINVAL;
	ceph_decode_32_safe(p, end, len, bad);
	while (len--) {
		__u32 osd;
		struct ceph_entity_addr addr;
		ceph_decode_32_safe(p, end, osd, bad);
		ceph_decode_copy_safe(p, end, &addr, sizeof(addr), bad);
		dout(1, "osd%d up\n", osd);
		BUG_ON(osd >= map->max_osd);
		map->osd_state[osd] |= CEPH_OSD_UP;
		map->osd_addr[osd] = addr;
	}

	/* new_down */
	ceph_decode_32_safe(p, end, len, bad);
	while (len--) {
		__u32 osd;
		ceph_decode_32_safe(p, end, osd, bad);
		dout(1, "osd%d down\n", osd);
		if (osd < map->max_osd) 
			map->osd_state[osd] &= ~CEPH_OSD_UP;
	}

	/* new_offload */
	ceph_decode_32_safe(p, end, len, bad);
	while (len--) {
		__u32 osd, off;
		ceph_decode_need(p, end, sizeof(__u32)*2, bad);
		ceph_decode_32(p, osd);
		ceph_decode_32(p, off);
		dout(1, "osd%d offload %x\n", osd, off);
		if (osd < map->max_osd) 
			map->crush->device_offload[osd] = off;
	}
	
	return map;

bad:
	return ERR_PTR(err);
}


/*
 * calculate file layout from given offset, length.
 * fill in correct oid and off,len within object.
 * update file offset,length to end of extent, or
 * the next file extent not included in current mapping.
 */
void calc_file_object_mapping(struct ceph_file_layout *layout, 
			      loff_t *off, loff_t *len,
			      struct ceph_object *oid,
			      __u64 *oxoff, __u64 *oxlen)
{
	unsigned su, stripeno, stripepos, objsetno;
	unsigned su_per_object;
	unsigned stripe_len = layout->fl_stripe_count * layout->fl_stripe_unit;
	unsigned first_oxlen;
	loff_t t;

	/*su_per_object = layout->fl_object_size / layout->fl_stripe_unit; */
	su_per_object = layout->fl_object_size;
	do_div(su_per_object, layout->fl_stripe_unit);

	BUG_ON((layout->fl_stripe_unit & ~PAGE_MASK) != 0);
	/* su = *off / layout->fl_stripe_unit; */
	su = *off;
	do_div(su, layout->fl_stripe_unit);
	/* stripeno = su / layout->fl_stripe_count;
	   stripepos = su % layout->fl_stripe_count; */
	stripeno = su;
	stripepos = do_div(stripeno, layout->fl_stripe_count);
	/* objsetno = stripeno / su_per_object; */
	objsetno = stripeno;
	do_div(objsetno, su_per_object);

	oid->bno = objsetno * layout->fl_stripe_count + stripepos;
	/* *oxoff = *off / layout->fl_stripe_unit; */
	t = *off;
	*oxoff = do_div(t, layout->fl_stripe_unit);
	first_oxlen = min_t(loff_t, *len, layout->fl_stripe_unit);
	*oxlen = first_oxlen;
	
	/* multiple stripe units across this object? */
	t = *len;
	while (t > stripe_len && *oxoff + *oxlen < layout->fl_object_size) {
		*oxlen += min_t(loff_t, layout->fl_stripe_unit, t);
		t -= stripe_len;
	}
		
	*off += first_oxlen;
	*len -= *oxlen;
}

/*
 * calculate an object layout (i.e. pgid) from an oid, 
 * file_layout, and osdmap
 */
void calc_object_layout(struct ceph_object_layout *ol,
			struct ceph_object *oid,
			struct ceph_file_layout *fl,
			struct ceph_osdmap *osdmap)
{
	unsigned num, num_mask;
	if (fl->fl_pg_preferred >= 0) {
		num = osdmap->lpg_num;
		num_mask = osdmap->lpg_num_mask;
	} else {
		num = osdmap->pg_num;
		num_mask = osdmap->pg_num_mask;
	}
	ol->ol_pgid.pg.ps = 
		ceph_stable_mod(oid->bno + crush_hash32_2(oid->ino, 
							  oid->ino>>32), 
				num, num_mask);
	ol->ol_pgid.pg.preferred = fl->fl_pg_preferred;
	ol->ol_pgid.pg.type = fl->fl_pg_type;
	ol->ol_pgid.pg.size = fl->fl_pg_size;
	ol->ol_stripe_unit = fl->fl_object_stripe_unit;
}
