#include <linux/err.h>
#include <linux/highmem.h>
#include <linux/mm.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#include <linux/uaccess.h>

#include "ceph_debug.h"

int ceph_debug_osdc __read_mostly = -1;
#define DOUT_MASK DOUT_MASK_OSDC
#define DOUT_VAR ceph_debug_osdc
#include "super.h"

#include "osd_client.h"
#include "messenger.h"
#include "crush/mapper.h"
#include "decode.h"


/*
 * calculate the mapping of a file extent onto an object, and fill out the
 * request accordingly.  shorten extent as necessary if it crosses an
 * object boundary.
 */
static void calc_layout(struct ceph_osd_client *osdc,
			struct ceph_vino vino, struct ceph_file_layout *layout,
			u64 off, u64 *plen,
			struct ceph_osd_request *req)
{
	struct ceph_osd_request_head *reqhead = req->r_request->front.iov_base;
	struct ceph_osd_op *op = (void *)(reqhead + 1);
	u64 orig_len = *plen;
	u64 objoff, objlen;    /* extent in object */
	u64 bno;

	reqhead->snapid = cpu_to_le64(vino.snap);

	/* object extent? */
	ceph_calc_file_object_mapping(layout, off, plen, &bno,
				      &objoff, &objlen);
	if (*plen < orig_len)
		dout(10, " skipping last %llu, final file extent %llu~%llu\n",
		     orig_len - *plen, off, *plen);

	sprintf(req->r_oid, "%llx.%08llx", vino.ino, bno);
	req->r_oid_len = strlen(req->r_oid);


	op->offset = cpu_to_le64(objoff);
	op->length = cpu_to_le64(objlen);
	req->r_num_pages = calc_pages_for(off, *plen);

	dout(10, "calc_layout %s (%d) %llu~%llu (%d pages)\n",
	     req->r_oid, req->r_oid_len, objoff, objlen, req->r_num_pages);
}


/*
 * requests
 */
void ceph_osdc_put_request(struct ceph_osd_request *req)
{
	dout(10, "put_request %p %d -> %d\n", req, atomic_read(&req->r_ref),
	     atomic_read(&req->r_ref)-1);
	BUG_ON(atomic_read(&req->r_ref) <= 0);
	if (atomic_dec_and_test(&req->r_ref)) {
		if (req->r_request)
			ceph_msg_put(req->r_request);
		if (req->r_reply)
			ceph_msg_put(req->r_reply);
		if (req->r_own_pages)
			ceph_release_page_vector(req->r_pages,
						 req->r_num_pages);
		ceph_put_snap_context(req->r_snapc);
		kfree(req);
	}
}

/*
 * build new request AND message, calculate layout, and adjust file
 * extent as needed.  include addition truncate or sync osd ops.
 */
struct ceph_osd_request *ceph_osdc_new_request(struct ceph_osd_client *osdc,
					       struct ceph_file_layout *layout,
					       struct ceph_vino vino,
					       u64 off, u64 *plen,
					       int opcode, int flags,
					       struct ceph_snap_context *snapc,
					       int do_sync,
					       u32 truncate_seq,
					       u64 truncate_size,
					       struct timespec *mtime)
{
	struct ceph_osd_request *req;
	struct ceph_msg *msg;
	struct ceph_osd_request_head *head;
	struct ceph_osd_op *op;
	void *p;
	int do_trunc = truncate_seq && (off + *plen > truncate_size);
	int num_op = 1 + do_sync + do_trunc;
	size_t msg_size = sizeof(*head) + num_op*sizeof(*op);
	int i;
	u64 prevofs;

	/* we may overallocate here, if our write extent is shortened below */
	req = kzalloc(sizeof(*req), GFP_NOFS);
	if (req == NULL)
		return ERR_PTR(-ENOMEM);

	atomic_set(&req->r_ref, 1);
	init_completion(&req->r_completion);
	init_completion(&req->r_safe_completion);
	INIT_LIST_HEAD(&req->r_unsafe_item);
	req->r_flags = flags;
	req->r_last_osd = -1;

	WARN_ON((flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE)) == 0);

	/* create message; allow space for oid */
	msg_size += 40 + osdc->client->signed_ticket_len;
	if (snapc)
		msg_size += sizeof(u64) * snapc->num_snaps;
	msg = ceph_msg_new(CEPH_MSG_OSD_OP, msg_size, 0, 0, NULL);
	if (IS_ERR(msg)) {
		kfree(req);
		return ERR_PTR(PTR_ERR(msg));
	}
	memset(msg->front.iov_base, 0, msg->front.iov_len);
	head = msg->front.iov_base;
	op = (void *)(head + 1);
	p = (void *)(op + num_op);

	req->r_request = msg;
	req->r_snapc = ceph_get_snap_context(snapc);

	head->client_inc = cpu_to_le32(1); /* always, for now. */
	head->flags = cpu_to_le32(flags);
	if (flags & CEPH_OSD_FLAG_WRITE)
		ceph_encode_timespec(&head->mtime, mtime);
	head->num_ops = cpu_to_le16(num_op);
	op->op = cpu_to_le16(opcode);

	/* calculate max write size */
	calc_layout(osdc, vino, layout, off, plen, req);
	req->r_file_layout = *layout;  /* keep a copy */

	if (flags & CEPH_OSD_FLAG_WRITE) {
		req->r_request->hdr.data_off = cpu_to_le16(off);
		req->r_request->hdr.data_len = cpu_to_le32(*plen);
		op->payload_len = cpu_to_le32(*plen);
	}


	/* fill in oid, ticket */
	head->object_len = cpu_to_le32(req->r_oid_len);
	memcpy(p, req->r_oid, req->r_oid_len);
	p += req->r_oid_len;

	head->ticket_len = cpu_to_le32(osdc->client->signed_ticket_len);
	memcpy(p, osdc->client->signed_ticket,
	       osdc->client->signed_ticket_len);
	p += osdc->client->signed_ticket_len;


	/* additional ops */
	if (do_trunc) {
		op++;
		op->op = cpu_to_le16(opcode == CEPH_OSD_OP_READ ?
			     CEPH_OSD_OP_MASKTRUNC : CEPH_OSD_OP_SETTRUNC);
		op->truncate_seq = cpu_to_le32(truncate_seq);
		prevofs =  le64_to_cpu((op-1)->offset);
		op->truncate_size = cpu_to_le64(truncate_size - (off-prevofs));
	}
	if (do_sync) {
		op++;
		op->op = cpu_to_le16(CEPH_OSD_OP_STARTSYNC);
	}
	if (snapc) {
		head->snap_seq = cpu_to_le64(snapc->seq);
		head->num_snaps = cpu_to_le32(snapc->num_snaps);
		for (i = 0; i < snapc->num_snaps; i++) {
			put_unaligned_le64(snapc->snaps[i], p);
			p += sizeof(u64);
		}
	}

	BUG_ON(p > msg->front.iov_base + msg->front.iov_len);
	return req;
}

/*
 * Register request, assign tid.  If this is the first request, set up
 * the timeout event.
 */
static int register_request(struct ceph_osd_client *osdc,
			    struct ceph_osd_request *req)
{
	struct ceph_osd_request_head *head = req->r_request->front.iov_base;
	int rc;

	mutex_lock(&osdc->request_mutex);
	req->r_tid = ++osdc->last_tid;
	head->tid = cpu_to_le64(req->r_tid);

	dout(30, "register_request %p tid %lld\n", req, req->r_tid);
	rc = radix_tree_insert(&osdc->request_tree, req->r_tid, (void *)req);
	if (rc < 0)
		goto out;

	ceph_osdc_get_request(req);
	osdc->num_requests++;

	req->r_timeout_stamp =
		jiffies + osdc->client->mount_args.osd_timeout*HZ;

	if (osdc->num_requests == 1) {
		osdc->timeout_tid = req->r_tid;
		dout(30, "  timeout on tid %llu at %lu\n", req->r_tid,
		     req->r_timeout_stamp);
		schedule_delayed_work(&osdc->timeout_work,
		      round_jiffies_relative(req->r_timeout_stamp - jiffies));
	}
out:
	mutex_unlock(&osdc->request_mutex);
	return rc;
}

/*
 * Timeout callback, called every N seconds when 1 or more osd
 * requests has been active for more than N seconds.  When this
 * happens, we ping all OSDs with requests who have timed out to
 * ensure any communications channel reset is detected.  Reset the
 * request timeouts another N seconds in the future as we go.
 * Reschedule the timeout event another N seconds in future (unless
 * there are no open requests).
 */
static void handle_timeout(struct work_struct *work)
{
	struct ceph_osd_client *osdc =
		container_of(work, struct ceph_osd_client, timeout_work.work);
	struct ceph_osd_request *req;
	unsigned long timeout = osdc->client->mount_args.osd_timeout * HZ;
	unsigned long next_timeout = timeout + jiffies;
	RADIX_TREE(pings, GFP_NOFS);  /* only send 1 ping per osd */
	u64 next_tid = 0;
	int got;

	dout(10, "timeout\n");
	down_read(&osdc->map_sem);

	ceph_monc_request_osdmap(&osdc->client->monc, osdc->osdmap->epoch+1);

	mutex_lock(&osdc->request_mutex);
	while (1) {
		got = radix_tree_gang_lookup(&osdc->request_tree, (void **)&req,
					     next_tid, 1);
		if (got == 0)
			break;
		next_tid = req->r_tid + 1;
		if (time_before(jiffies, req->r_timeout_stamp))
			goto next;

		req->r_timeout_stamp = next_timeout;
		if (req->r_last_osd >= 0 &&
		    radix_tree_lookup(&pings, req->r_last_osd) == NULL) {
			struct ceph_entity_name n = {
				.type = cpu_to_le32(CEPH_ENTITY_TYPE_OSD),
				.num = cpu_to_le32(req->r_last_osd)
			};
			dout(20, " tid %llu (at least) timed out on osd%d\n",
			     req->r_tid, req->r_last_osd);
			radix_tree_insert(&pings, req->r_last_osd, req);
			ceph_ping(osdc->client->msgr, n, &req->r_last_osd_addr);
		}

	next:
		got = radix_tree_gang_lookup(&osdc->request_tree, (void **)&req,
					     next_tid, 1);
	}

	while (radix_tree_gang_lookup(&pings, (void **)&req, 0, 1))
		radix_tree_delete(&pings, req->r_last_osd);

	if (osdc->timeout_tid)
		schedule_delayed_work(&osdc->timeout_work,
				      round_jiffies_relative(timeout));

	mutex_unlock(&osdc->request_mutex);

	up_read(&osdc->map_sem);
}

/*
 * called under osdc->request_mutex
 */
static void __unregister_request(struct ceph_osd_client *osdc,
				 struct ceph_osd_request *req)
{
	dout(30, "__unregister_request %p tid %lld\n", req, req->r_tid);
	radix_tree_delete(&osdc->request_tree, req->r_tid);

	osdc->num_requests--;
	ceph_osdc_put_request(req);

	if (req->r_tid == osdc->timeout_tid) {
		if (osdc->num_requests == 0) {
			dout(30, "no requests, canceling timeout\n");
			osdc->timeout_tid = 0;
			cancel_delayed_work(&osdc->timeout_work);
		} else {
			int ret;

			ret = radix_tree_gang_lookup(&osdc->request_tree,
						     (void **)&req, 0, 1);
			BUG_ON(ret != 1);
			osdc->timeout_tid = req->r_tid;
			dout(30, "rescheduled timeout on tid %llu at %lu\n",
			     req->r_tid, req->r_timeout_stamp);
			schedule_delayed_work(&osdc->timeout_work,
			      round_jiffies_relative(req->r_timeout_stamp -
						     jiffies));
		}
	}
}

/*
 * Pick an osd, and put result in req->r_last_osd[_addr].  The first
 * up osd in the pg.  or -1.
 *
 * Caller should hold map_sem for read.
 *
 * return 0 if unchanged, 1 if changed.
 */
static int map_osds(struct ceph_osd_client *osdc,
		    struct ceph_osd_request *req)
{
	struct ceph_osd_request_head *reqhead = req->r_request->front.iov_base;
	union ceph_pg pgid;
	struct ceph_pg_pool_info *pool;
	int ruleno;
	unsigned pps; /* placement ps */
	int osds[10], osd = -1;
	int i, num;
	int err;

	err = ceph_calc_object_layout(&reqhead->layout, req->r_oid,
				      &req->r_file_layout, osdc->osdmap);
	if (err)
		return err;
	pgid.pg64 = le64_to_cpu(reqhead->layout.ol_pgid);
	if (pgid.pg.pool >= osdc->osdmap->num_pools)
		return -1;
	pool = &osdc->osdmap->pg_pool[pgid.pg.pool];
	ruleno = crush_find_rule(osdc->osdmap->crush, pool->v.crush_ruleset,
				 pool->v.type, pool->v.size);
	if (ruleno < 0) {
		derr(0, "map_osds no crush rule for pool %d type %d size %d\n",
		     pgid.pg.pool, pool->v.type, pool->v.size);
		return -1;
	}

	if (pgid.pg.preferred >= 0)
		pps = ceph_stable_mod(pgid.pg.ps,
				      le32_to_cpu(pool->v.lpgp_num),
				      pool->lpgp_num_mask);
	else
		pps = ceph_stable_mod(pgid.pg.ps,
				      le32_to_cpu(pool->v.pgp_num),
				      pool->pgp_num_mask);
	pps += pgid.pg.pool;
	num = crush_do_rule(osdc->osdmap->crush, ruleno, pps, osds,
			    min_t(int, pool->v.size, ARRAY_SIZE(osds)),
			    pgid.pg.preferred, osdc->osdmap->osd_weight);

	/* primary is first up osd */
	for (i = 0; i < num; i++)
		if (ceph_osd_is_up(osdc->osdmap, osds[i])) {
			osd = osds[i];
			break;
		}
	dout(20, "map_osds tid %llu pgid %llx pool %d osd%d (was osd%d)\n",
	     req->r_tid, pgid.pg64, pgid.pg.pool, osd, req->r_last_osd);
	if (req->r_last_osd == osd &&
	    (osd < 0 || ceph_entity_addr_equal(&osdc->osdmap->osd_addr[osd],
					       &req->r_last_osd_addr)))
		return 0;
	req->r_last_osd = osd;
	if (osd >= 0)
		req->r_last_osd_addr = osdc->osdmap->osd_addr[osd];
	return 1;
}

/*
 * caller should hold map_sem (for read)
 */
static int send_request(struct ceph_osd_client *osdc,
			struct ceph_osd_request *req)
{
	struct ceph_osd_request_head *reqhead;
	int osd;

	map_osds(osdc, req);
	if (req->r_last_osd < 0) {
		dout(10, "send_request %p no up osds in pg\n", req);
		ceph_monc_request_osdmap(&osdc->client->monc,
					 osdc->osdmap->epoch+1);
		return 0;
	}
	osd = req->r_last_osd;

	dout(10, "send_request %p tid %llu to osd%d flags %d\n",
	     req, req->r_tid, osd, req->r_flags);

	reqhead = req->r_request->front.iov_base;
	reqhead->osdmap_epoch = cpu_to_le32(osdc->osdmap->epoch);
	reqhead->flags |= cpu_to_le32(req->r_flags);  /* e.g., RETRY */
	reqhead->reassert_version = req->r_reassert_version;

	req->r_request->hdr.dst.name.type =
		cpu_to_le32(CEPH_ENTITY_TYPE_OSD);
	req->r_request->hdr.dst.name.num = cpu_to_le32(osd);
	req->r_request->hdr.dst.addr = req->r_last_osd_addr;

	req->r_timeout_stamp = jiffies+osdc->client->mount_args.osd_timeout*HZ;

	ceph_msg_get(req->r_request); /* send consumes a ref */
	return ceph_msg_send(osdc->client->msgr, req->r_request,
			     BASE_DELAY_INTERVAL);
}

/*
 * handle osd op reply.  either call the callback if it is specified,
 * or do the completion to wake up the waiting thread.
 */
void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	struct ceph_osd_reply_head *rhead = msg->front.iov_base;
	struct ceph_osd_request *req;
	u64 tid;
	int numops, object_len, flags;

	if (msg->front.iov_len < sizeof(*rhead))
		goto bad;
	tid = le64_to_cpu(rhead->tid);
	numops = le32_to_cpu(rhead->num_ops);
	object_len = le32_to_cpu(rhead->object_len);
	if (msg->front.iov_len != sizeof(*rhead) + object_len +
	    numops * sizeof(struct ceph_osd_op))
		goto bad;
	dout(10, "handle_reply %p tid %llu\n", msg, tid);

	/* lookup */
	mutex_lock(&osdc->request_mutex);
	req = radix_tree_lookup(&osdc->request_tree, tid);
	if (req == NULL) {
		dout(10, "handle_reply tid %llu dne\n", tid);
		mutex_unlock(&osdc->request_mutex);
		return;
	}
	ceph_osdc_get_request(req);
	flags = le32_to_cpu(rhead->flags);

	if (req->r_aborted) {
		dout(10, "handle_reply tid %llu aborted\n", tid);
		goto done;
	}

	if (req->r_reassert_version.epoch == 0) {
		/* first ack */
		if (req->r_reply == NULL) {
			/* no data payload, or r_reply would have been set by
			   prepare_pages. */
			ceph_msg_get(msg);
			req->r_reply = msg;
		} else {
			/* r_reply was set by prepare_pages */
			BUG_ON(req->r_reply != msg);
		}

		/* in case we need to replay this op, */
		req->r_reassert_version = rhead->reassert_version;
	} else if ((flags & CEPH_OSD_FLAG_ONDISK) == 0) {
		dout(10, "handle_reply tid %llu dup ack\n", tid);
		goto done;
	}

	dout(10, "handle_reply tid %llu flags %d\n", tid, flags);

	/* either this is a read, or we got the safe response */
	if ((flags & CEPH_OSD_FLAG_ONDISK) ||
	    ((flags & CEPH_OSD_FLAG_WRITE) == 0))
		__unregister_request(osdc, req);

	mutex_unlock(&osdc->request_mutex);

	if (req->r_callback)
		req->r_callback(req);
	else
		complete(&req->r_completion);

	if (flags & CEPH_OSD_FLAG_ONDISK) {
		if (req->r_safe_callback)
			req->r_safe_callback(req);
		complete(&req->r_safe_completion);  /* fsync waiter */
	}

done:
	ceph_osdc_put_request(req);
	return;

bad:
	derr(0, "got corrupt osd_op_reply got %d %d expected %d\n",
	     (int)msg->front.iov_len, le32_to_cpu(msg->hdr.front_len),
	     (int)sizeof(*rhead));
}


/*
 * Resubmit osd requests whose osd or osd address has changed.  Request
 * a new osd map if osds are down, or we are otherwise unable to determine
 * how to direct a request.
 *
 * If @who is specified, resubmit requests for that specific osd.
 *
 * Caller should hold map_sem for read.
 */
static void kick_requests(struct ceph_osd_client *osdc,
			  struct ceph_entity_addr *who)
{
	struct ceph_osd_request *req;
	u64 next_tid = 0;
	int got;
	int needmap = 0;

	mutex_lock(&osdc->request_mutex);
	while (1) {
		got = radix_tree_gang_lookup(&osdc->request_tree, (void **)&req,
					     next_tid, 1);
		if (got == 0)
			break;
		next_tid = req->r_tid + 1;

		if (who && ceph_entity_addr_equal(who, &req->r_last_osd_addr))
			goto kick;

		if (map_osds(osdc, req) == 0)
			continue;  /* no change */

		if (req->r_last_osd < 0) {
			dout(20, "tid %llu maps to no valid osd\n", req->r_tid);
			needmap++;  /* request a newer map */
			memset(&req->r_last_osd_addr, 0,
			       sizeof(req->r_last_osd_addr));
			continue;
		}

	kick:
		dout(20, "kicking tid %llu osd%d\n", req->r_tid,
		     req->r_last_osd);
		ceph_osdc_get_request(req);
		mutex_unlock(&osdc->request_mutex);
		req->r_request = ceph_msg_maybe_dup(req->r_request);
		if (!req->r_aborted) {
			req->r_flags |= CEPH_OSD_FLAG_RETRY;
			send_request(osdc, req);
		}
		ceph_osdc_put_request(req);
		mutex_lock(&osdc->request_mutex);
	}
	mutex_unlock(&osdc->request_mutex);

	if (needmap) {
		dout(10, "%d requests for down osds, need new map\n", needmap);
		ceph_monc_request_osdmap(&osdc->client->monc,
					 osdc->osdmap->epoch+1);
	}
}

/*
 * Process updated osd map.
 *
 * The message contains any number of incremental and full maps.
 */
void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg)
{
	void *p, *end, *next;
	u32 nr_maps, maplen;
	u32 epoch;
	struct ceph_osdmap *newmap = NULL, *oldmap;
	int err;
	ceph_fsid_t fsid;

	dout(2, "handle_map have %u\n", osdc->osdmap ? osdc->osdmap->epoch : 0);
	p = msg->front.iov_base;
	end = p + msg->front.iov_len;

	/* verify fsid */
	ceph_decode_need(&p, end, sizeof(fsid), bad);
	ceph_decode_copy(&p, &fsid, sizeof(fsid));
	if (ceph_fsid_compare(&fsid, &osdc->client->monc.monmap->fsid)) {
		derr(0, "got map with wrong fsid, ignoring\n");
		return;
	}

	down_write(&osdc->map_sem);

	/* incremental maps */
	ceph_decode_32_safe(&p, end, nr_maps, bad);
	dout(10, " %d inc maps\n", nr_maps);
	while (nr_maps > 0) {
		ceph_decode_need(&p, end, 2*sizeof(u32), bad);
		ceph_decode_32(&p, epoch);
		ceph_decode_32(&p, maplen);
		ceph_decode_need(&p, end, maplen, bad);
		next = p + maplen;
		if (osdc->osdmap && osdc->osdmap->epoch+1 == epoch) {
			dout(10, "applying incremental map %u len %d\n",
			     epoch, maplen);
			newmap = apply_incremental(&p, next, osdc->osdmap,
						   osdc->client->msgr);
			if (IS_ERR(newmap)) {
				err = PTR_ERR(newmap);
				goto bad;
			}
			if (newmap != osdc->osdmap) {
				ceph_osdmap_destroy(osdc->osdmap);
				osdc->osdmap = newmap;
			}
		} else {
			dout(10, "ignoring incremental map %u len %d\n",
			     epoch, maplen);
		}
		p = next;
		nr_maps--;
	}
	if (newmap)
		goto done;

	/* full maps */
	ceph_decode_32_safe(&p, end, nr_maps, bad);
	dout(30, " %d full maps\n", nr_maps);
	while (nr_maps) {
		ceph_decode_need(&p, end, 2*sizeof(u32), bad);
		ceph_decode_32(&p, epoch);
		ceph_decode_32(&p, maplen);
		ceph_decode_need(&p, end, maplen, bad);
		if (nr_maps > 1) {
			dout(5, "skipping non-latest full map %u len %d\n",
			     epoch, maplen);
		} else if (osdc->osdmap && osdc->osdmap->epoch >= epoch) {
			dout(10, "skipping full map %u len %d, "
			     "older than our %u\n", epoch, maplen,
			     osdc->osdmap->epoch);
		} else {
			dout(10, "taking full map %u len %d\n", epoch, maplen);
			newmap = osdmap_decode(&p, p+maplen);
			if (IS_ERR(newmap)) {
				err = PTR_ERR(newmap);
				goto bad;
			}
			oldmap = osdc->osdmap;
			osdc->osdmap = newmap;
			if (oldmap)
				ceph_osdmap_destroy(oldmap);
		}
		p += maplen;
		nr_maps--;
	}

done:
	downgrade_write(&osdc->map_sem);
	ceph_monc_got_osdmap(&osdc->client->monc, osdc->osdmap->epoch);
	if (newmap)
		kick_requests(osdc, NULL);
	up_read(&osdc->map_sem);
	return;

bad:
	derr(1, "handle_map corrupt msg\n");
	up_write(&osdc->map_sem);
	return;
}

/*
 * If we detect that a tcp connection to an osd resets, we need to
 * resubmit all requests for that osd.  That's because although we reliably
 * deliver our requests, the osd doesn't not try as hard to deliver the
 * reply (because it does not get notification when clients, mds' leave
 * the cluster).
 */
void ceph_osdc_handle_reset(struct ceph_osd_client *osdc,
			    struct ceph_entity_addr *addr)
{
	down_read(&osdc->map_sem);
	kick_requests(osdc, addr);
	up_read(&osdc->map_sem);
}


/*
 * A read request prepares specific pages that data is to be read into.
 * When a message is being read off the wire, we call prepare_pages to
 * find those pages.
 *  0 = success, -1 failure.
 */
int ceph_osdc_prepare_pages(void *p, struct ceph_msg *m, int want)
{
	struct ceph_client *client = p;
	struct ceph_osd_client *osdc = &client->osdc;
	struct ceph_osd_reply_head *rhead = m->front.iov_base;
	struct ceph_osd_request *req;
	u64 tid;
	int ret = -1;
	int type = le16_to_cpu(m->hdr.type);

	dout(10, "prepare_pages on msg %p want %d\n", m, want);
	if (unlikely(type != CEPH_MSG_OSD_OPREPLY))
		return -1;  /* hmm! */

	tid = le64_to_cpu(rhead->tid);
	mutex_lock(&osdc->request_mutex);
	req = radix_tree_lookup(&osdc->request_tree, tid);
	if (!req) {
		dout(10, "prepare_pages unknown tid %llu\n", tid);
		goto out;
	}
	dout(10, "prepare_pages tid %llu has %d pages, want %d\n",
	     tid, req->r_num_pages, want);
	if (likely(req->r_num_pages >= want && req->r_reply == NULL &&
		    !req->r_aborted)) {
		m->pages = req->r_pages;
		m->nr_pages = req->r_num_pages;
		ceph_msg_get(m);
		req->r_reply = m;
		ret = 0; /* success */
	}
out:
	mutex_unlock(&osdc->request_mutex);
	return ret;
}

/*
 * Register request, send initial attempt.
 */
int ceph_osdc_start_request(struct ceph_osd_client *osdc,
			    struct ceph_osd_request *req)
{
	int rc;

	req->r_request->pages = req->r_pages;
	req->r_request->nr_pages = req->r_num_pages;

	rc = register_request(osdc, req);
	if (rc < 0)
		return rc;

	down_read(&osdc->map_sem);
	rc = send_request(osdc, req);
	up_read(&osdc->map_sem);
	return rc;
}

int ceph_osdc_wait_request(struct ceph_osd_client *osdc,
			   struct ceph_osd_request *req)
{
	struct ceph_osd_reply_head *replyhead;
	__s32 rc;
	int bytes;

	rc = wait_for_completion_interruptible(&req->r_completion);
	if (rc < 0) {
		ceph_osdc_abort_request(osdc, req);
		return rc;
	}

	/* parse reply */
	replyhead = req->r_reply->front.iov_base;
	rc = le32_to_cpu(replyhead->result);
	bytes = le32_to_cpu(req->r_reply->hdr.data_len);
	dout(10, "wait_request tid %llu result %d, %d bytes\n",
	     req->r_tid, rc, bytes);
	if (rc < 0)
		return rc;
	return bytes;
}

/*
 * To abort an in-progress request, take pages away from outgoing or
 * incoming message.
 */
void ceph_osdc_abort_request(struct ceph_osd_client *osdc,
			     struct ceph_osd_request *req)
{
	struct ceph_msg *msg;

	dout(0, "abort_request tid %llu, revoking %p pages\n", req->r_tid,
	     req->r_request);
	/*
	 * mark req aborted _before_ revoking pages, so that
	 * if a racing kick_request _does_ dup the page vec
	 * pointer, it will definitely then see the aborted
	 * flag and not send the request.
	 */
	req->r_aborted = 1;
	msg = req->r_request;
	mutex_lock(&msg->page_mutex);
	msg->pages = NULL;
	mutex_unlock(&msg->page_mutex);
	if (req->r_reply) {
		mutex_lock(&req->r_reply->page_mutex);
		req->r_reply->pages = NULL;
		mutex_unlock(&req->r_reply->page_mutex);
	}
}

void ceph_osdc_sync(struct ceph_osd_client *osdc)
{
	struct ceph_osd_request *req;
	u64 last_tid, next_tid = 0;
	int got;

	mutex_lock(&osdc->request_mutex);
	last_tid = osdc->last_tid;
	while (1) {
		got = radix_tree_gang_lookup(&osdc->request_tree, (void **)&req,
					     next_tid, 1);
		if (!got)
			break;
		if (req->r_tid > last_tid)
			break;

		next_tid = req->r_tid + 1;
		if ((req->r_flags & CEPH_OSD_FLAG_WRITE) == 0)
			continue;

		ceph_osdc_get_request(req);
		mutex_unlock(&osdc->request_mutex);
		dout(10, "sync waiting on tid %llu (last is %llu)\n",
		     req->r_tid, last_tid);
		wait_for_completion(&req->r_safe_completion);
		mutex_lock(&osdc->request_mutex);
		ceph_osdc_put_request(req);
	}
	mutex_unlock(&osdc->request_mutex);
	dout(10, "sync done (thru tid %llu)\n", last_tid);
}

/*
 * init, shutdown
 */
void ceph_osdc_init(struct ceph_osd_client *osdc, struct ceph_client *client)
{
	dout(5, "init\n");
	osdc->client = client;
	osdc->osdmap = NULL;
	init_rwsem(&osdc->map_sem);
	init_completion(&osdc->map_waiters);
	osdc->last_requested_map = 0;
	mutex_init(&osdc->request_mutex);
	osdc->timeout_tid = 0;
	osdc->last_tid = 0;
	INIT_RADIX_TREE(&osdc->request_tree, GFP_NOFS);
	osdc->num_requests = 0;
	INIT_DELAYED_WORK(&osdc->timeout_work, handle_timeout);
}

void ceph_osdc_stop(struct ceph_osd_client *osdc)
{
	cancel_delayed_work_sync(&osdc->timeout_work);
	if (osdc->osdmap) {
		ceph_osdmap_destroy(osdc->osdmap);
		osdc->osdmap = NULL;
	}
}

/*
 * Read some contiguous pages.  Return number of bytes read (or
 * zeroed).
 */
int ceph_osdc_readpages(struct ceph_osd_client *osdc,
			struct ceph_vino vino, struct ceph_file_layout *layout,
			u64 off, u64 len,
			u32 truncate_seq, u64 truncate_size,
			struct page **pages, int num_pages)
{
	struct ceph_osd_request *req;
	int i;
	struct page *page;
	int rc = 0, read = 0;

	dout(10, "readpages on ino %llx.%llx on %llu~%llu\n", vino.ino,
	     vino.snap, off, len);
	req = ceph_osdc_new_request(osdc, layout, vino, off, &len,
				    CEPH_OSD_OP_READ, CEPH_OSD_FLAG_READ,
				    NULL, 0, truncate_seq, truncate_size, NULL);
	if (IS_ERR(req))
		return PTR_ERR(req);

	/* it may be a short read due to an object boundary */
	req->r_pages = pages;
	num_pages = calc_pages_for(off, len);
	req->r_num_pages = num_pages;

	dout(10, "readpages final extent is %llu~%llu (%d pages)\n",
	     off, len, req->r_num_pages);

	rc = ceph_osdc_start_request(osdc, req);
	if (!rc)
		rc = ceph_osdc_wait_request(osdc, req);

	if (rc >= 0) {
		read = rc;
		rc = len;
	} else if (rc == -ENOENT) {
		rc = len;
	}

	/* zero trailing pages on success */
	if (read < (num_pages << PAGE_CACHE_SHIFT)) {
		if (read & ~PAGE_CACHE_MASK) {
			i = read >> PAGE_CACHE_SHIFT;
			page = pages[i];
			dout(20, "readpages zeroing %d %p from %d\n", i, page,
			     (int)(read & ~PAGE_CACHE_MASK));
			zero_user_segment(page, read & ~PAGE_CACHE_MASK,
					  PAGE_CACHE_SIZE);
			read += PAGE_CACHE_SIZE;
		}
		for (i = read >> PAGE_CACHE_SHIFT; i < num_pages; i++) {
			page = req->r_pages[i];
			dout(20, "readpages zeroing %d %p\n", i, page);
			zero_user_segment(page, 0, PAGE_CACHE_SIZE);
		}
	}

	ceph_osdc_put_request(req);
	dout(10, "readpages result %d\n", rc);
	return rc;
}

/*
 * do a sync write on N pages
 */
int ceph_osdc_writepages(struct ceph_osd_client *osdc, struct ceph_vino vino,
			 struct ceph_file_layout *layout,
			 struct ceph_snap_context *snapc,
			 u64 off, u64 len,
			 u32 truncate_seq, u64 truncate_size,
			 struct timespec *mtime,
			 struct page **pages, int num_pages,
			 int flags, int do_sync)
{
	struct ceph_osd_request *req;
	int rc = 0;

	BUG_ON(vino.snap != CEPH_NOSNAP);
	req = ceph_osdc_new_request(osdc, layout, vino, off, &len,
				    CEPH_OSD_OP_WRITE,
				    flags | CEPH_OSD_FLAG_ONDISK |
					    CEPH_OSD_FLAG_WRITE,
				    snapc, do_sync,
				    truncate_seq, truncate_size, mtime);
	if (IS_ERR(req))
		return PTR_ERR(req);

	/* it may be a short write due to an object boundary */
	req->r_pages = pages;
	req->r_num_pages = calc_pages_for(off, len);
	dout(10, "writepages %llu~%llu (%d pages)\n", off, len,
	     req->r_num_pages);

	rc = ceph_osdc_start_request(osdc, req);
	if (!rc)
		rc = ceph_osdc_wait_request(osdc, req);

	ceph_osdc_put_request(req);
	if (rc == 0)
		rc = len;
	dout(10, "writepages result %d\n", rc);
	return rc;
}

