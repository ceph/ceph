
#include <linux/slab.h>
#include <linux/err.h>

#include "crush/crush.h"
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
	__u32 crushlen, max;
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

	if ((err = ceph_decode_32(p, end, &max)) < 0)
		goto bad;

	/* alloc */
	if ((err = osdmap_set_max_osd(map, max)) < 0)
	    goto bad;
	
	/* osds */
	if ((err = ceph_decode_copy(p, end, &map->osd_state, map->max_osd)) < 0)
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

static struct ceph_osdmap *apply_incremental(void **p, void *end, struct ceph_osdmap *map)
{
	struct ceph_osdmap *newmap = map;
	struct crush_map *newcrush = 0;
	struct ceph_fsid fsid;
	__u64 epoch, mon_epoch;
	struct ceph_timeval ctime;
	__u32 len;
	__u32 max;
	int err;

	if ((err = ceph_decode_64(p, end, &fsid.major)) < 0)
		goto bad;
	if ((err = ceph_decode_64(p, end, &fsid.minor)) < 0)
		goto bad;
	if ((err = ceph_decode_64(p, end, &epoch)) < 0)
		goto bad;
	if ((err = ceph_decode_64(p, end, &mon_epoch)) < 0)
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
	if (!map || epoch != map->epoch+1) 
		return 0; /* old or new, or no existing; done */

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
	struct ceph_osdmap *newmap = 0;
	int err;

	dout(1, "ceph_osdc_handle_map\n");
	p = msg->front.iov_base;
	end = p + msg->front.iov_len;

	/* incremental maps */
	if ((err = ceph_decode_32(&p, end, &nr_maps)) < 0)
		goto bad;
	while (nr_maps--) {
		if ((err = ceph_decode_32(&p, end, &maplen)) < 0)
			goto bad;
		next = p + maplen;
		newmap = apply_incremental(p, min(p+maplen,end), osdc->osdmap);
		if (IS_ERR(newmap)) {
			err = PTR_ERR(newmap);
			goto bad;
		}
		if (newmap != osdc->osdmap) {
			osdmap_destroy(osdc->osdmap);
			osdc->osdmap = newmap;
		}
		dout(1, "got incremental map %llu\n", newmap->epoch);
		p = next;
	}
	if (newmap) 
		goto out;
	
	/* full maps */
	if ((err = ceph_decode_32(&p, end, &nr_maps)) < 0)
		goto bad;
	while (nr_maps > 1) {
		dout(5, "ignoring non-latest full map\n");
		if ((err = ceph_decode_32(&p, end, &maplen)) < 0)
			goto bad;
		p += maplen;
	}
	if (nr_maps) {
		if ((err = ceph_decode_32(&p, end, &maplen)) < 0)
			goto bad;
		newmap = osdmap_decode(&p, min(p+maplen,end));
		if (IS_ERR(newmap)) {
			err = PTR_ERR(newmap);
			goto bad;
		}
		osdmap_destroy(osdc->osdmap);
		osdc->osdmap = newmap;
		dout(1, "got full map %llu\n", newmap->epoch);
	}
	
out:
	ceph_msg_put(msg);
	return;

bad:
	derr(1, "corrupt osd map message\n");
	goto out;
}



void ceph_osdc_init(struct ceph_osd_client *osdc)
{
	dout(5, "ceph_osdc_init\n");
	osdc->osdmap = NULL;
}


void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	dout(5, "ceph_osdc_handle_reply\n");
}

