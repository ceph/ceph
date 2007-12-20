#include <linux/slab.h>
#include <linux/err.h>

#include "osd_client.h"
#include "messenger.h"

int ceph_osdc_debug = 50;
#define DOUT_VAR ceph_osdc_debug
#define DOUT_PREFIX "osdc: "
#include "super.h"

void ceph_osdc_init(struct ceph_osd_client *osdc)
{
	dout(5, "init\n");
	spin_lock_init(&osdc->lock);
	osdc->osdmap = NULL;
	osdc->last_requested_map = 0;
	osdc->last_tid = 0;
	INIT_RADIX_TREE(&osdc->request_tree, GFP_KERNEL);
	init_completion(&osdc->map_waiters);
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



void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	dout(5, "handle_reply\n");
}



struct ceph_msg *
ceph_osdc_create_request(struct ceph_osd_client *osdc, int op)
{
	struct ceph_msg *req;
	struct ceph_osd_request_head *head;

}
