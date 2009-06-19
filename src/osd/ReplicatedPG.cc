// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "ReplicatedPG.h"
#include "OSD.h"
#include "PGLS.h"

#include "common/arch.h"
#include "common/Logger.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGTrim.h"

#include "messages/MOSDPing.h"

#include "config.h"

#define DOUT_SUBSYS osd
#define DOUT_PREFIX_ARGS this, osd->whoami, osd->osdmap
#undef dout_prefix
#define dout_prefix _prefix(this, osd->whoami, osd->osdmap)
static ostream& _prefix(PG *pg, int whoami, OSDMap *osdmap) {
  return *_dout << dbeginl << pthread_self() << " osd" << whoami
		<< " " << (osdmap ? osdmap->get_epoch():0) << " " << *pg << " ";
}


#include <sstream>

#include <errno.h>

static const int LOAD_LATENCY    = 1;
static const int LOAD_QUEUE_SIZE = 2;
static const int LOAD_HYBRID     = 3;


// =======================
// pg changes

bool ReplicatedPG::same_for_read_since(epoch_t e)
{
  return (e >= info.history.same_primary_since);
}

bool ReplicatedPG::same_for_modify_since(epoch_t e)
{
  return (e >= info.history.same_primary_since);
}

bool ReplicatedPG::same_for_rep_modify_since(epoch_t e)
{
  // check osd map: same set, or primary+acker?
  return e >= info.history.same_primary_since;
}

// ====================
// missing objects

bool ReplicatedPG::is_missing_object(const sobject_t& soid)
{
  return missing.missing.count(soid);
}
 

void ReplicatedPG::wait_for_missing_object(const sobject_t& soid, Message *m)
{
  assert(is_missing_object(soid));

  // we don't have it (yet).
  eversion_t v = missing.missing[soid].need;
  if (pulling.count(soid)) {
    dout(7) << "missing "
	    << soid 
	    << " v " << v
	    << ", already pulling"
	    << dendl;
  } else {
    dout(7) << "missing " 
	    << soid 
	    << " v " << v
	    << ", pulling"
	    << dendl;
    pull(soid);
    osd->start_recovery_op(this, 1);
  }
  waiting_for_missing_object[soid].push_back(m);
}


// ==========================================================

/** preprocess_op - preprocess an op (before it gets queued).
 * fasttrack read
 */
bool ReplicatedPG::preprocess_op(MOSDOp *op, utime_t now)
{
#if 0
  // we only care about reads here on out..
  if (op->may_write() ||
      op->ops.size() < 1)
    return false;

  ceph_osd_op& readop = op->ops[0];

  object_t oid = op->get_oid();
  sobject_t soid(oid, op->get_snapid());

  // -- load balance reads --
  if (is_primary()) {
    // -- read on primary+acker ---
    
    // test
    if (false) {
      if (acting.size() > 1) {
	int peer = acting[1];
	dout(-10) << "preprocess_op fwd client read op to osd" << peer
		  << " for " << op->get_orig_source() << " " << op->get_orig_source_inst() << dendl;
	osd->messenger->forward_message(op, osd->osdmap->get_inst(peer));
	return true;
      }
    }
    
#if 0
    // -- balance reads?
    if (g_conf.osd_balance_reads &&
	!op->get_source().is_osd()) {
      // flash crowd?
      bool is_flash_crowd_candidate = false;
      if (g_conf.osd_flash_crowd_iat_threshold > 0) {
	osd->iat_averager.add_sample( oid, (double)g_clock.now() );
	is_flash_crowd_candidate = osd->iat_averager.is_flash_crowd_candidate( oid );
      }

      // hot?
      double temp = 0;
      if (stat_object_temp_rd.count(soid))
	temp = stat_object_temp_rd[soid].get(op->get_recv_stamp());
      bool is_hotly_read = temp > g_conf.osd_balance_reads_temp;

      dout(20) << "balance_reads oid " << oid << " temp " << temp 
		<< (is_hotly_read ? " hotly_read":"")
		<< (is_flash_crowd_candidate ? " flash_crowd_candidate":"")
		<< dendl;

      bool should_balance = is_flash_crowd_candidate || is_hotly_read;
      bool is_balanced = false;
      bool b;
      // *** FIXME *** this may block, and we're in the fast path! ***
      if (g_conf.osd_balance_reads &&
	  osd->store->getattr(info.pgid.to_coll(), soid, "balance-reads", &b, 1) >= 0)
	is_balanced = true;
      
      if (!is_balanced && should_balance &&
	  balancing_reads.count(soid) == 0) {
	dout(-10) << "preprocess_op balance-reads on " << oid << dendl;
	balancing_reads.insert(soid);
	ceph_object_layout layout;
	layout.ol_pgid = info.pgid.u.pg64;
	layout.ol_stripe_unit = 0;
	MOSDOp *pop = new MOSDOp(0, osd->get_tid(),
				 oid,
				 layout,
				 osd->osdmap->get_epoch(),
				 CEPH_OSD_FLAG_MODIFY);
	pop->add_simple_op(CEPH_OSD_OP_BALANCEREADS, 0, 0);
	do_op(pop);
      }
      if (is_balanced && !should_balance &&
	  !unbalancing_reads.count(soid) == 0) {
	dout(-10) << "preprocess_op unbalance-reads on " << oid << dendl;
	unbalancing_reads.insert(soid);
	ceph_object_layout layout;
	layout.ol_pgid = info.pgid.u.pg64;
	layout.ol_stripe_unit = 0;
	MOSDOp *pop = new MOSDOp(0, osd->get_tid(),
				 oid,
				 layout,
				 osd->osdmap->get_epoch(),
				 CEPH_OSD_FLAG_MODIFY);
	pop->add_simple_op(CEPH_OSD_OP_UNBALANCEREADS, 0, 0);
	do_op(pop);
      }
    }
#endif
    
    // -- read shedding
    if (g_conf.osd_shed_reads &&
	g_conf.osd_stat_refresh_interval > 0 &&
	!op->get_source().is_osd()) {        // no re-shedding!
      Mutex::Locker lock(osd->peer_stat_lock);

      osd->_refresh_my_stat(now);

      // check my load. 
      // TODO xxx we must also compare with our own load
      // if i am x percentage higher than replica , 
      // redirect the read 

      int shedto = -1;
      double bestscore = 0.0;  // highest positive score wins
      
      // we calculate score values such that we can interpret them as a probability.

      switch (g_conf.osd_shed_reads) {
      case LOAD_LATENCY:
	// above some minimum?
	if (osd->my_stat.read_latency >= g_conf.osd_shed_reads_min_latency) {  
	  for (unsigned i=1; i<acting.size(); ++i) {
	    int peer = acting[i];
	    if (osd->peer_stat.count(peer) == 0) continue;

	    // assume a read_latency of 0 (technically, undefined) is OK, since
	    // we'll be corrected soon enough if we're wrong.

	    double plat = osd->peer_stat[peer].read_latency_mine;

	    double diff = osd->my_stat.read_latency - plat;
	    if (diff < g_conf.osd_shed_reads_min_latency_diff) continue;

	    double c = .002; // add in a constant to smooth it a bit
	    double latratio = 
	      (c+osd->my_stat.read_latency) /   
	      (c+plat);
	    double p = (latratio - 1.0) / 2.0 / latratio;
	    dout(15) << "preprocess_op " << op->get_reqid() 
		      << " my read latency " << osd->my_stat.read_latency
		      << ", peer osd" << peer << " is " << plat << " (" << osd->peer_stat[peer].read_latency << ")"
		      << ", latratio " << latratio
		      << ", p=" << p
		      << dendl;
	    if (latratio > g_conf.osd_shed_reads_min_latency_ratio &&
		p > bestscore &&
		drand48() < p) {
	      shedto = peer;
	      bestscore = p;
	    }
	  }
	}
	break;
	
      case LOAD_HYBRID:
	// dumb mostly
	if (osd->my_stat.read_latency >= g_conf.osd_shed_reads_min_latency) {
	  for (unsigned i=1; i<acting.size(); ++i) {
	    int peer = acting[i];
	    if (osd->peer_stat.count(peer) == 0/* ||
		osd->peer_stat[peer].read_latency <= 0*/) continue;

	    if (osd->peer_stat[peer].qlen < osd->my_stat.qlen) {
	      
	      if (osd->my_stat.read_latency - osd->peer_stat[peer].read_latency >
		  g_conf.osd_shed_reads_min_latency_diff) continue;

	      double qratio = osd->pending_ops / osd->peer_stat[peer].qlen;
	      
	      double c = .002; // add in a constant to smooth it a bit
	      double latratio = 
		(c+osd->my_stat.read_latency)/   
		(c+osd->peer_stat[peer].read_latency);
	      double p = (latratio - 1.0) / 2.0 / latratio;
	      
	      dout(-15) << "preprocess_op " << op->get_reqid() 
			<< " my qlen / rdlat " 
			<< osd->pending_ops << " " << osd->my_stat.read_latency
			<< ", peer osd" << peer << " is "
			<< osd->peer_stat[peer].qlen << " " << osd->peer_stat[peer].read_latency
			<< ", qratio " << qratio
			<< ", latratio " << latratio
			<< ", p=" << p
			<< dendl;
	      if (latratio > g_conf.osd_shed_reads_min_latency_ratio &&
		  p > bestscore &&
		  drand48() < p) {
		shedto = peer;
		bestscore = p;
	      }
	    }
	  }
	}
	break;

	/*
      case LOAD_QUEUE_SIZE:
	// am i above my average?  -- dumb
	if (osd->pending_ops > osd->my_stat.qlen) {
	  // yes. is there a peer who is below my average?
	  for (unsigned i=1; i<acting.size(); ++i) {
	    int peer = acting[i];
	    if (osd->peer_stat.count(peer) == 0) continue;
	    if (osd->peer_stat[peer].qlen < osd->my_stat.qlen) {
	      // calculate a probability that we should redirect
	      float p = (osd->my_stat.qlen - osd->peer_stat[peer].qlen) / osd->my_stat.qlen;  // this is dumb.
	      float v = 1.0 - p;
	      
	      dout(10) << "my qlen " << osd->pending_ops << " > my_avg " << osd->my_stat.qlen
		       << ", peer osd" << peer << " has qlen " << osd->peer_stat[peer].qlen
		       << ", p=" << p
		       << ", v= "<< v
		       << dendl;
	      
	      if (v > bestscore) {
		shedto = peer;
		bestscore = v;
	      }
	    }
	  }
	}
	break;*/

      }
	
      // shed?
      if (shedto >= 0) {
	dout(10) << "preprocess_op shedding read to peer osd" << shedto
		  << " " << op->get_reqid()
		  << dendl;
	op->set_peer_stat(osd->my_stat);
	osd->messenger->forward_message(op, osd->osdmap->get_inst(shedto));
	osd->stat_rd_ops_shed_out++;
	osd->logger->inc(l_osd_shdout);
	return true;
      }
    }
  } // endif balance reads


  // -- fastpath read?
  // if this is a read and the data is in the cache, do an immediate read.. 
  if ( g_conf.osd_immediate_read_from_cache ) {
    if (osd->store->is_cached(info.pgid.to_coll(), soid,
			      readop.offset,
			      readop.length) == 0) {
      if (!is_primary() && !op->get_source().is_osd()) {
	// am i allowed?
	bool v;
	if (osd->store->getattr(info.pgid.to_coll(), soid, "balance-reads", &v, 1) < 0) {
	  dout(-10) << "preprocess_op in-cache but no balance-reads on " << oid
		    << ", fwd to primary" << dendl;
	  osd->messenger->forward_message(op, osd->osdmap->get_inst(get_primary()));
	  return true;
	}
      }

      // do it now
      dout(10) << "preprocess_op data is in cache, reading from cache" << *op <<  dendl;
      do_op(op);
      return true;
    }
  }
#endif
  return false;
}

void ReplicatedPG::do_pg_op(MOSDOp *op)
{
  dout(0) << "do_pg_op " << *op << dendl;

  //bufferlist& indata = op->get_data();
  bufferlist outdata;
  int result = 0;

  for (vector<OSDOp>::iterator p = op->ops.begin(); p != op->ops.end(); p++) {
    switch (p->op.op) {
    case CEPH_OSD_OP_PGLS:
      {
        dout(10) << " pgls pg=" << op->get_pg() << dendl;
	// read into a buffer
        PGLSResponse response;
        response.handle = (collection_list_handle_t)(__u64)(p->op.pgls_cookie);
        vector<sobject_t> sentries;
	result = osd->store->collection_list_partial(op->get_pg().to_coll(), op->get_snapid(), sentries, p->op.length,
	                                             &response.handle);
	if (!result) {
          vector<sobject_t>::iterator iter;
          for (iter = sentries.begin(); iter != sentries.end(); ++iter) {
            response.entries.push_back(iter->oid);
          }
	  ::encode(response, outdata);
        }
	dout(10) << " pgls result=" << result << " outdata.length()=" << outdata.length() << dendl;
      }
      break;

    default:
      result = -EINVAL;
      break;
    }
  }

  // reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(),
				       CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK); 
  reply->set_data(outdata);
  reply->set_result(result);
  osd->messenger->send_message(reply, op->get_orig_source_inst());
  delete op;
}

void ReplicatedPG::calc_trim_to()
{
  if (!is_degraded() &&
      (is_clean() ||
       log.top.version - log.bottom.version > info.stats.num_objects)) {
    if (min_last_complete_ondisk != eversion_t() &&
	min_last_complete_ondisk != pg_trim_to) {
      dout(10) << "calc_trim_to " << pg_trim_to << " -> " << min_last_complete_ondisk << dendl;
      pg_trim_to = min_last_complete_ondisk;
      assert(pg_trim_to <= log.top);
    }
  } else
    pg_trim_to = eversion_t();
}

/** do_op - do an op
 * pg lock will be held (if multithreaded)
 * osd_lock NOT held.
 */
void ReplicatedPG::do_op(MOSDOp *op) 
{
  osd->logger->inc(l_osd_op);

  if ((op->get_rmw_flags() & CEPH_OSD_FLAG_PGOP))
    return do_pg_op(op);

  dout(0) << "do_op " << *op << dendl;
  dout(0) << "do_op flags=" << hex << op->get_flags() << dec << dendl;

  entity_inst_t client = op->get_source_inst();

  ObjectContext *obc;
  bool can_create = op->may_write();
  int r = find_object_context(op->get_oid(), op->get_snapid(), &obc, can_create);
  if (r) {
    osd->reply_op_error(op, r);
    return;
  }    
  
  bool ok;
  if (op->may_read() && op->may_write()) 
    ok = obc->try_rmw(client);
  else if (op->may_write())
    ok = obc->try_write(client);
  else if (op->may_read())
    ok = obc->try_read(client);
  else
    assert(0);
  if (!ok) {
    dout(10) << "do_op waiting on obc " << *obc << dendl;
    obc->waiting.push_back(op);
    return;
  }

  if (!op->may_write() && !obc->obs.exists) {
    osd->reply_op_error(op, -ENOENT);
    put_object_context(obc);
    return;
  }

  const sobject_t& soid = obc->obs.oi.soid;
  OpContext *ctx = new OpContext(op, op->get_reqid(), op->ops, op->get_data(),
				 obc->state, &obc->obs, this);
  bool noop = false;

  if (op->may_write()) {
    // version
    ctx->at_version = log.top;
    if (!noop) {
      ctx->at_version.epoch = osd->osdmap->get_epoch();
      ctx->at_version.version++;
      assert(ctx->at_version > info.last_update);
      assert(ctx->at_version > log.top);
    }

    ctx->mtime = op->get_mtime();
    
    // snap
    if (op->get_snap_seq()) {
      // client specified snapc
      ctx->snapc.seq = op->get_snap_seq();
      ctx->snapc.snaps = op->get_snaps();
    } else {
      // use pool's snapc
      ctx->snapc = pool->snapc;
    }

    // set version in op, for benefit of client and our eventual reply
    op->set_version(ctx->at_version);

    dout(10) << "do_op " << soid << " " << ctx->ops
	     << " ov " << obc->obs.oi.version << " av " << ctx->at_version 
	     << " snapc " << ctx->snapc
	     << " snapset " << obc->obs.oi.snapset
	     << dendl;  

    if (is_dup(ctx->reqid)) {
      dout(3) << "do_op dup " << ctx->reqid << ", doing WRNOOP" << dendl;
      noop = true;
    }
  } else {
    dout(10) << "do_op " << soid << " " << ctx->ops
	     << " ov " << obc->obs.oi.version
	     << dendl;  
  }

  // verify snap ordering
  if ((op->get_flags() & CEPH_OSD_FLAG_ORDERSNAP) &&
      ctx->snapc.seq < obc->obs.oi.snapset.seq) {
    dout(10) << " ORDERSNAP flag set and snapc seq " << ctx->snapc.seq
	     << " < snapset seq " << obc->obs.oi.snapset.seq
	     << " on " << soid << dendl;
    delete ctx;
    put_object_context(obc);
    osd->reply_op_error(op, -EOLDSNAPC);
    return;
  }

  // note my stats
  utime_t now = g_clock.now();

  // note some basic context for op replication that prepare_transaction may clobber
  eversion_t old_last_update = ctx->at_version;
  bool old_exists = obc->obs.exists;
  __u64 old_size = obc->obs.oi.size;
  eversion_t old_version = obc->obs.oi.version;

  // we are acker.
  if (!noop) {
    int result = prepare_transaction(ctx);

    if (result >= 0)
      log_op_stats(soid, ctx);

    if (result == -EAGAIN)
      return;

    // read or error?
    if (ctx->op_t.empty() || result < 0) {
      MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(),
					   CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK); 
      reply->set_data(ctx->outdata);
      reply->get_header().data_off = ctx->data_off;
      reply->set_result(result);
      osd->messenger->send_message(reply, op->get_orig_source_inst());
      delete op;
      delete ctx;
      put_object_context(obc);
      return;
    }

    assert(op->may_write());

    // trim log?
    calc_trim_to();

    log_op(ctx->log, pg_trim_to, ctx->local_t);
  }
  
  // continuing on to write path, make sure object context is registered
  register_object_context(obc);

  // issue replica writes
  tid_t rep_tid = osd->get_tid();
  RepGather *repop = new_repop(ctx, obc, noop, rep_tid);

  // note: repop now owns ctx AND ctx->op

  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];

    if (peer_missing.count(peer) &&
        peer_missing[peer].is_missing(soid)) {
      // push it before this update. 
      // FIXME, this is probably extra much work (eg if we're about to overwrite)
      push_to_replica(soid, peer);
      osd->start_recovery_op(this, 1);
    }
    
    issue_repop(repop, peer, now, old_exists, old_size, old_version);

    if (!noop) {
      // keep peer_info up to date
      Info &in = peer_info[peer];
      in.last_update = ctx->at_version;
      if (in.last_complete == old_last_update)
	in.last_update = ctx->at_version;
    }
  }

  // apply immediately?
  if (obc->is_rmw_mode())
    apply_repop(repop);

  // (logical) local ack.
  //  (if alone and delayed, this will apply the update.)
  int whoami = osd->get_nodeid();
  assert(repop->waitfor_ack.count(whoami));
  repop->waitfor_ack.erase(whoami);
  eval_repop(repop);
  repop->put();

  // drop my obc reference.
  put_object_context(obc);
}


void ReplicatedPG::log_op_stats(const sobject_t& soid, OpContext *ctx)
{
  osd->logger->inc(l_osd_op);

  if (ctx->op_t.empty()) {
    osd->logger->inc(l_osd_c_rd);
    osd->logger->inc(l_osd_c_rdb, ctx->outdata.length());

    utime_t now = g_clock.now();
    utime_t diff = now;
    diff -= ctx->op->get_recv_stamp();
    //dout(20) <<  "do_op " << ctx->reqid << " total op latency " << diff << dendl;
    Mutex::Locker lock(osd->peer_stat_lock);
    osd->stat_rd_ops_in_queue--;
    osd->read_latency_calc.add(diff);
	
    if (is_primary() &&
	g_conf.osd_balance_reads)
      stat_object_temp_rd[soid].hit(now);  // hit temp.
  } else {
    osd->logger->inc(l_osd_c_wr);
    osd->logger->inc(l_osd_c_wrb, ctx->indata.length());
  }
}


void ReplicatedPG::do_sub_op(MOSDSubOp *op)
{
  dout(15) << "do_sub_op " << *op << dendl;

  osd->logger->inc(l_osd_subop);

  if (op->ops.size() >= 1) {
    OSDOp& first = op->ops[0];
    switch (first.op.op) {
      // rep stuff
    case CEPH_OSD_OP_PULL:
      sub_op_pull(op);
      return;
    case CEPH_OSD_OP_PUSH:
      sub_op_push(op);
      return;
    case CEPH_OSD_OP_SCRUB:
      sub_op_scrub(op);
      return;
    }
  }

  sub_op_modify(op);
}

void ReplicatedPG::do_sub_op_reply(MOSDSubOpReply *r)
{
  if (r->ops.size() >= 1) {
    OSDOp& first = r->ops[0];
    if (first.op.op == CEPH_OSD_OP_PUSH) {
      // continue peer recovery
      sub_op_push_reply(r);
      return;
    }
    if (first.op.op == CEPH_OSD_OP_SCRUB) {
      sub_op_scrub_reply(r);
      return;
    }
  }

  sub_op_modify_reply(r);
}


bool ReplicatedPG::snap_trimmer()
{
  lock();
  dout(10) << "snap_trimmer start" << dendl;

  while (info.snap_trimq.size() &&
	 is_active()) {
    snapid_t sn = *info.snap_trimq.begin();
    coll_t c = info.pgid.to_snap_coll(sn);
    vector<sobject_t> ls;
    osd->store->collection_list(c, ls);
    if (ls.size() != info.stats.num_objects)
      dout(10) << " WARNING: " << ls.size() << " != num_objects " << info.stats.num_objects << dendl;

    dout(10) << "snap_trimmer collection " << c << " has " << ls.size() << " items" << dendl;

    for (vector<sobject_t>::iterator p = ls.begin(); p != ls.end(); p++) {
      const sobject_t& coid = *p;

      ObjectStore::Transaction t;

      // load clone info
      bufferlist bl;
      osd->store->getattr(info.pgid.to_coll(), coid, OI_ATTR, bl);
      object_info_t coi(bl);

      // load head info
      sobject_t head = coid;
      head.snap = CEPH_NOSNAP;
      bl.clear();
      osd->store->getattr(info.pgid.to_coll(), head, OI_ATTR, bl);
      object_info_t hoi(bl);

      vector<snapid_t>& snaps = coi.snaps;
      SnapSet& snapset = hoi.snapset;

      dout(10) << coid << " old head " << head << " snapset " << snapset << dendl;

      // remove snaps
      vector<snapid_t> newsnaps;
      for (unsigned i=0; i<snaps.size(); i++)
	if (!osd->_lookup_pool(info.pgid.pool())->info.is_removed_snap(snaps[i]))
	  newsnaps.push_back(snaps[i]);
	else {
	  vector<snapid_t>::iterator q = snapset.snaps.begin();
	  while (*q != snaps[i]) q++;  // it should be in there
	  snapset.snaps.erase(q);
	}
      if (newsnaps.empty()) {
	// remove clone
	dout(10) << coid << " snaps " << snaps << " -> " << newsnaps << " ... deleting" << dendl;
	t.remove(info.pgid.to_coll(), coid);
	t.collection_remove(info.pgid.to_snap_coll(snaps[0]), coid);
	if (snaps.size() > 1)
	  t.collection_remove(info.pgid.to_snap_coll(snaps[snaps.size()-1]), coid);

	// ...from snapset
	snapid_t last = coid.snap;
	vector<snapid_t>::iterator p;
	for (p = snapset.clones.begin(); p != snapset.clones.end(); p++)
	  if (*p == last)
	    break;
	if (p != snapset.clones.begin()) {
	  // not the oldest... merge overlap into next older clone
	  vector<snapid_t>::iterator n = p - 1;
	  interval_set<__u64> keep;
	  keep.union_of(snapset.clone_overlap[*n], snapset.clone_overlap[*p]);
	  add_interval_usage(keep, info.stats);  // not deallocated
 	  snapset.clone_overlap[*n].intersection_of(snapset.clone_overlap[*p]);
	} else {
	  add_interval_usage(snapset.clone_overlap[last], info.stats);  // not deallocated
	}
	info.stats.num_objects--;
	info.stats.num_object_clones--;
	info.stats.num_bytes -= snapset.clone_size[last];
	info.stats.num_kb -= SHIFT_ROUND_UP(snapset.clone_size[last], 10);
	snapset.clones.erase(p);
	snapset.clone_overlap.erase(last);
	snapset.clone_size.erase(last);
      } else {
	// save adjusted snaps for this object
	dout(10) << coid << " snaps " << snaps << " -> " << newsnaps << dendl;
	coi.snaps.swap(newsnaps);
	bl.clear();
	::encode(coi, bl);
	t.setattr(info.pgid.to_coll(), coid, OI_ATTR, bl);

	if (snaps[0] != newsnaps[0]) {
	  t.collection_remove(info.pgid.to_snap_coll(snaps[0]), coid);
	  t.collection_add(info.pgid.to_snap_coll(newsnaps[0]), info.pgid.to_coll(), coid);
	}
	if (snaps.size() > 1 && snaps[snaps.size()-1] != newsnaps[newsnaps.size()-1]) {
	  t.collection_remove(info.pgid.to_snap_coll(snaps[snaps.size()-1]), coid);
	  if (newsnaps.size() > 1)
	    t.collection_add(info.pgid.to_snap_coll(newsnaps[newsnaps.size()-1]), info.pgid.to_coll(), coid);
	}	      
      }

      // save head snapset
      dout(10) << coid << " new head " << head << " snapset " << snapset << dendl;
      
      if (snapset.clones.empty() && !snapset.head_exists) {
	dout(10) << coid << " removing head " << head << dendl;
	t.remove(info.pgid.to_coll(), head);
	info.stats.num_objects--;
      } else {
	bl.clear();
	::encode(hoi, bl);
	t.setattr(info.pgid.to_coll(), head, OI_ATTR, bl);
      }

      osd->store->apply_transaction(t);

      // give other threads a chance at this pg
      unlock();
      lock();
    }

    // remove snap collection
    ObjectStore::Transaction t;
    dout(10) << "removing snap " << sn << " collection " << c << dendl;
    snap_collections.erase(sn);
    write_info(t);
    t.remove_collection(c);
    osd->store->apply_transaction(t);
 
    info.snap_trimq.erase(sn);
  }  

  // done
  dout(10) << "snap_trimmer done" << dendl;

  ObjectStore::Transaction t;
  write_info(t);
  osd->store->apply_transaction(t);
  unlock();
  return true;
}


// ========================================================================
// low level osd ops

int ReplicatedPG::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops,
			     bufferlist& odata)
{
  int result = 0;

  object_info_t& oi = ctx->obs->oi;

  const sobject_t& soid = oi.soid;

  ObjectStore::Transaction& t = ctx->op_t;

  dout(10) << "do_osd_op " << soid << " " << ops << dendl;

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); p++) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op; 
    bool is_modify;
    string cname, mname;
    bufferlist::iterator bp = osd_op.data.begin();

    switch (op.op) {
    case CEPH_OSD_OP_CALL:
      bp.copy(op.class_len, cname);
      bp.copy(op.method_len, mname);
      is_modify = osd->class_handler->get_method_flags(cname, mname) & CLS_METHOD_WR;
      break;

    default:
      is_modify = (op.op & CEPH_OSD_OP_MODE_WR);
      break;
    }

    // munge ZERO -> TRUNCATE?  (don't munge to DELETE or we risk hosing attributes)
    if (op.op == CEPH_OSD_OP_ZERO &&
	oi.snapset.head_exists &&
	op.offset + op.length >= oi.size) {
      dout(10) << " munging ZERO " << op.offset << "~" << op.length
	       << " -> TRUNCATE " << op.offset << " (old size is " << oi.size << ")" << dendl;
      op.op = CEPH_OSD_OP_TRUNCATE;
      oi.snapset.head_exists = true;
    }
    // munge DELETE -> TRUNCATE?
    if (op.op == CEPH_OSD_OP_DELETE &&
	oi.snapset.clones.size()) {
      dout(10) << " munging DELETE -> TRUNCATE 0 bc of clones " << oi.snapset.clones << dendl;
      op.op = CEPH_OSD_OP_TRUNCATE;
      op.offset = 0;
      oi.snapset.head_exists = false;
    }

    dout(10) << "do_osd_op  " << osd_op << dendl;

    // make writeable (i.e., clone if necessary)
    if (is_modify)
      make_writeable(ctx);

    switch (op.op) {
      
      // --- READS ---

    case CEPH_OSD_OP_READ:
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->read(info.pgid.to_coll(), soid, op.offset, op.length, bl);
	if (odata.length() == 0)
	  ctx->data_off = op.offset;
	odata.claim(bl);
	if (r >= 0) 
	  op.length = r;
	else {
	  result = r;
	  op.length = 0;
	}
	dout(10) << " read got " << r << " / " << op.length << " bytes from obj " << soid << dendl;
      }
      break;

    case CEPH_OSD_OP_CALL:
      {
	bufferlist indata;
	bp.copy(op.indata_len, indata);
	
	ClassHandler::ClassData *cls;
        ClassVersion version;
        version.set_arch(get_arch());
        result = osd->get_class(cname, version, info.pgid, ctx->op, &cls);
	if (result) {
	  dout(10) << "call class " << cname << " does not exist" << dendl;
          if (result == -EAGAIN)
            return result;
	} else {
	  bufferlist outdata;
	  ClassHandler::ClassMethod *method = cls->get_method(mname.c_str());
	  if (!method) {
	    dout(10) << "call method " << cname << "." << mname << " does not exist" << dendl;
	    result = -EINVAL;
	  } else {
	    dout(10) << "call method " << cname << "." << mname << dendl;
	    result = method->exec((cls_method_context_t)&ctx, indata, outdata);
	    dout(10) << "method called response length=" << outdata.length() << dendl;
	    op.length = outdata.length();
	    odata.claim_append(outdata);
	  }
	}
      }
      break;

    case CEPH_OSD_OP_STAT:
      {
	struct stat st;
	memset(&st, sizeof(st), 0);
	result = osd->store->stat(info.pgid.to_coll(), soid, &st);
	if (result >= 0) {
	  __u64 size = st.st_size;
	  ::encode(size, odata);
	  ::encode(oi.mtime, odata);
	}
      }
      break;

    case CEPH_OSD_OP_GETXATTR:
      {
	nstring name(op.name_len + 1);
	name[0] = '_';
	bp.copy(op.name_len, name.data()+1);
	int r = osd->store->getattr(info.pgid.to_coll(), soid, name.c_str(), odata);
	if (r >= 0) {
	  op.value_len = r;
	  result = 0;
	} else
	  result = r;
      }
      break;

    case CEPH_OSD_OP_MASKTRUNC:
      if (p != ops.begin()) {
	OSDOp& rd = *(p - 1);
	OSDOp& m = *p;
	
	// are we beyond truncate_size?
	if (rd.op.offset + rd.op.length > m.op.truncate_size) {	
	  __u32 seq = 0;
	  interval_set<__u64> tm;
	  if (oi.truncate_info.length()) {
	    bufferlist::iterator p = oi.truncate_info.begin();
	    ::decode(seq, p);
	    ::decode(tm, p);
	  }
	  
	  // truncated portion of the read
	  unsigned from = MAX(rd.op.offset, m.op.truncate_size);  // also end of data
	  unsigned to = rd.op.offset + rd.op.length;
	  unsigned trim = to-from;
	  
	  rd.op.length = rd.op.length - trim;
	  
	  dout(10) << " masktrunc " << m << ": overlap " << from << "~" << trim << dendl;
	  
	  bufferlist keep;
	  keep.substr_of(odata, 0, odata.length() - trim);
	  bufferlist truncated;  // everthing after 'from'
	  truncated.substr_of(odata, odata.length() - trim, trim);
	  keep.swap(odata);
	  
	  if (seq == rd.op.truncate_seq) {
	    // keep any valid extents beyond 'from'
	    unsigned data_end = from;
	    for (map<__u64,__u64>::iterator q = tm.m.begin();
		 q != tm.m.end();
		 q++) {
	      unsigned s = MAX(q->first, from);
	      unsigned e = MIN(q->first+q->second, to);
	      if (e > s) {
		unsigned l = e-s;
		dout(10) << "   " << q->first << "~" << q->second << " overlap " << s << "~" << l << dendl;

		// add in zeros?
		if (s > data_end) {
		  bufferptr bp(s-from);
		  bp.zero();
		  odata.push_back(bp);
		  dout(20) << "  adding " << bp.length() << " zeros" << dendl;
		  rd.op.length = rd.op.length + bp.length();
		  data_end += bp.length();
		}

		bufferlist b;
		b.substr_of(truncated, s-from, l);
		dout(20) << "  adding " << b.length() << " bytes from " << s << "~" << l << dendl;
		odata.claim_append(b);
		rd.op.length = rd.op.length + l;
		data_end += l;
	      }
	    } // for
	  } // seq == rd.truncate_eq
	}
      }
      break;


      // --- WRITES ---

      // -- object data --

    case CEPH_OSD_OP_WRITE:
      { // write
	assert(op.length);
	bufferlist nbl;
	bp.copy(op.length, nbl);
	t.write(info.pgid.to_coll(), soid, op.offset, op.length, nbl);
	if (oi.snapset.clones.size()) {
	  snapid_t newest = *oi.snapset.clones.rbegin();
	  interval_set<__u64> ch;
	  ch.insert(op.offset, op.length);
	  ch.intersection_of(oi.snapset.clone_overlap[newest]);
	  oi.snapset.clone_overlap[newest].subtract(ch);
	  add_interval_usage(ch, info.stats);
	}
	if (op.offset + op.length > oi.size) {
	  __u64 new_size = op.offset + op.length;
	  info.stats.num_bytes += new_size - oi.size;
	  info.stats.num_kb += SHIFT_ROUND_UP(new_size, 10) - SHIFT_ROUND_UP(oi.size, 10);
	  oi.size = new_size;
	}
	oi.snapset.head_exists = true;
      }
      break;
      
    case CEPH_OSD_OP_WRITEFULL:
      { // write full object
	bufferlist nbl;
	bp.copy(op.length, nbl);
	t.truncate(info.pgid.to_coll(), soid, 0);
	t.write(info.pgid.to_coll(), soid, op.offset, op.length, nbl);
	if (oi.snapset.clones.size()) {
	  snapid_t newest = *oi.snapset.clones.rbegin();
	  oi.snapset.clone_overlap.erase(newest);
	  oi.size = 0;
	}
	if (op.length != oi.size) {
	  info.stats.num_bytes -= oi.size;
	  info.stats.num_kb -= SHIFT_ROUND_UP(oi.size, 10);
	  info.stats.num_bytes += op.length;
	  info.stats.num_kb += SHIFT_ROUND_UP(op.length, 10);
	  oi.size = op.length;
	}
	oi.snapset.head_exists = true;
      }
      break;

    case CEPH_OSD_OP_ZERO:
      { // zero
	assert(op.length);
	if (!ctx->obs->exists)
	  t.touch(info.pgid.to_coll(), soid);
	t.zero(info.pgid.to_coll(), soid, op.offset, op.length);
	if (oi.snapset.clones.size()) {
	  snapid_t newest = *oi.snapset.clones.rbegin();
	  interval_set<__u64> ch;
	  ch.insert(op.offset, op.length);
	  ch.intersection_of(oi.snapset.clone_overlap[newest]);
	  oi.snapset.clone_overlap[newest].subtract(ch);
	  add_interval_usage(ch, info.stats);
	}
	oi.snapset.head_exists = true;
      }
      break;
      
    case CEPH_OSD_OP_TRUNCATE:
      { // truncate
	if (!ctx->obs->exists)
	  t.touch(info.pgid.to_coll(), soid);
	t.truncate(info.pgid.to_coll(), soid, op.offset);
	if (oi.snapset.clones.size()) {
	  snapid_t newest = *oi.snapset.clones.rbegin();
	  interval_set<__u64> trim;
	  if (oi.size > op.offset) {
	    trim.insert(op.offset, oi.size-op.offset);
	    trim.intersection_of(oi.snapset.clone_overlap[newest]);
	    add_interval_usage(trim, info.stats);
	  }
	  interval_set<__u64> keep;
	  if (op.offset)
	    keep.insert(0, op.offset);
	  oi.snapset.clone_overlap[newest].intersection_of(keep);
	}
	if (op.offset != oi.size) {
	  info.stats.num_bytes -= oi.size;
	  info.stats.num_kb -= SHIFT_ROUND_UP(oi.size, 10);
	  info.stats.num_bytes += op.offset;
	  info.stats.num_kb += SHIFT_ROUND_UP(op.offset, 10);
	  oi.size = op.offset;
	}
	// do no set head_exists, or we will break above DELETE -> TRUNCATE munging.
      }
      break;
    
    case CEPH_OSD_OP_DELETE:
      { // delete
	t.remove(info.pgid.to_coll(), soid);
	if (oi.snapset.clones.size()) {
	  snapid_t newest = *oi.snapset.clones.rbegin();
	  add_interval_usage(oi.snapset.clone_overlap[newest], info.stats);
	  oi.snapset.clone_overlap.erase(newest);  // ok, redundant.
	}
	if (ctx->obs->exists) {
	  info.stats.num_objects--;
	  info.stats.num_bytes -= oi.size;
	  info.stats.num_kb -= SHIFT_ROUND_UP(oi.size, 10);
	  oi.size = 0;
	  ctx->obs->exists = false;
	  oi.snapset.head_exists = false;
	}      
      }
      break;
    

      // -- object attrs --
      
    case CEPH_OSD_OP_SETXATTR:
      {
	if (!ctx->obs->exists)
	  t.touch(info.pgid.to_coll(), soid);
	nstring name(op.name_len + 1);
	name[0] = '_';
	bp.copy(op.name_len, name.data()+1);
	bufferlist bl;
	bp.copy(op.value_len, bl);
	if (!oi.snapset.head_exists)  // create object if it doesn't yet exist.
	  t.touch(info.pgid.to_coll(), soid);
	t.setattr(info.pgid.to_coll(), soid, name, bl);
	oi.snapset.head_exists = true;
      }
      break;

    case CEPH_OSD_OP_RMXATTR:
      {
	nstring name(op.name_len + 1);
	name[0] = '_';
	bp.copy(op.name_len, name.data()+1);
	t.rmattr(info.pgid.to_coll(), soid, name);
      }
      break;
    

      // -- fancy writers --
    case CEPH_OSD_OP_APPEND:
      {
	// just do it inline; this works because we are happy to execute
	// fancy op on replicas as well.
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITE;
	newop.op.offset = oi.size;
	newop.op.length = op.length;
        newop.data = osd_op.data;
	do_osd_ops(ctx, nops, odata);
      }
      break;

    case CEPH_OSD_OP_STARTSYNC:
      t.start_sync();
      break;

    case CEPH_OSD_OP_SETTRUNC:
      if (p != ops.begin()) {
	// set truncate seq over preceeding write's range
	OSDOp& wr = *(p - 1);
	
	__u32 seq = 0;
	interval_set<__u64> tm;
	bufferlist::iterator p;
	if (oi.truncate_info.length()) {
	  p = oi.truncate_info.begin();
	  ::decode(seq, p);
	}
	if (seq < op.truncate_seq) {
	  seq = op.truncate_seq;
	  tm.insert(wr.op.offset, wr.op.length);
	} else {
	  if (oi.truncate_info.length())
	    ::decode(tm, p);
	  interval_set<__u64> n;
	  n.insert(wr.op.offset, wr.op.length);
	  tm.union_of(n);
	}
	dout(10) << " settrunc seq " << seq << " map " << tm << dendl;
	oi.truncate_info.clear();
	::encode(seq, oi.truncate_info);
	::encode(tm, oi.truncate_info);
      }
      break;

    case CEPH_OSD_OP_TRIMTRUNC:
      if (ctx->obs->exists) {
	__u32 old_seq = 0;
	bufferlist::iterator p;
	if (oi.truncate_info.length()) {
	  p = oi.truncate_info.begin();
	  ::decode(old_seq, p);
	}
	
	if (op.truncate_seq > old_seq) {
	  // just truncate/delete.
	  vector<OSDOp> nops(1);
	  OSDOp& newop = nops[0];
	  newop.op.op = CEPH_OSD_OP_TRUNCATE;
	  newop.op.offset = op.truncate_size;
          newop.data = osd_op.data;
	  dout(10) << " seq " << op.truncate_seq << " > old_seq " << old_seq
		   << ", truncating with " << newop << dendl;
	  do_osd_ops(ctx, nops, odata);
	} else {
	  // do smart truncate
	  interval_set<__u64> tm;
	  ::decode(tm, p);
	  
	  interval_set<__u64> zero;
	  zero.insert(0, oi.size);
	  tm.intersection_of(zero);
	  zero.subtract(tm);
	  
	  dout(10) << " seq " << op.truncate_seq << " == old_seq " << old_seq
		   << ", tm " << tm << ", zeroing " << zero << dendl;
	  for (map<__u64,__u64>::iterator p = zero.m.begin();
	       p != zero.m.end();
	       p++) {
	    vector<OSDOp> nops(1);
	    OSDOp& newop = nops[0];
	    newop.op.op = CEPH_OSD_OP_ZERO;
	    newop.op.offset = p->first;
	    newop.op.length = p->second;
            newop.data = osd_op.data;
	    do_osd_ops(ctx, nops, odata);
	  }
	  
	  oi.truncate_info.clear();
	}
      }
      break;


    default:
      dout(1) << "unrecognized osd op " << op.op
	      << " " << ceph_osd_op_name(op.op)
	      << dendl;
      result = -EOPNOTSUPP;
      assert(0);  // for now
    }

    if ((is_modify) &&
	!ctx->obs->exists && oi.snapset.head_exists) {
      dout(20) << " num_objects " << info.stats.num_objects << " -> " << (info.stats.num_objects+1) << dendl;
      info.stats.num_objects++;
      ctx->obs->exists = true;
    }

    if (result)
      break;
  }
  return result;
}



void ReplicatedPG::_make_clone(ObjectStore::Transaction& t,
			       const sobject_t& head, const sobject_t& coid,
			       object_info_t *poi)
{
  bufferlist bv;
  ::encode(*poi, bv);

  t.clone(info.pgid.to_coll(), head, coid);
  t.setattr(info.pgid.to_coll(), coid, OI_ATTR, bv);
}

void ReplicatedPG::make_writeable(OpContext *ctx)
{
  object_info_t& oi = ctx->obs->oi;
  const sobject_t& soid = oi.soid;
  SnapContext& snapc = ctx->snapc;
  ObjectStore::Transaction& t = ctx->op_t;

  // clone?
  assert(soid.snap == CEPH_NOSNAP);
  dout(20) << "make_writeable " << soid << " snapset=" << oi.snapset
	   << "  snapc=" << snapc << dendl;;
  
  // use newer snapc?
  if (oi.snapset.seq > snapc.seq) {
    snapc.seq = oi.snapset.seq;
    snapc.snaps = oi.snapset.snaps;
    dout(10) << " using newer snapc " << snapc << dendl;
  }
  
  if (oi.snapset.head_exists &&           // head exists
      snapc.snaps.size() &&            // there are snaps
      snapc.snaps[0] > oi.snapset.seq) {  // existing object is old
    // clone
    sobject_t coid = soid;
    coid.snap = snapc.seq;
    
    unsigned l;
    for (l=1; l<snapc.snaps.size() && snapc.snaps[l] > oi.snapset.seq; l++) ;
    
    vector<snapid_t> snaps(l);
    for (unsigned i=0; i<l; i++)
      snaps[i] = snapc.snaps[i];
    
    // prepare clone
    ctx->clone_obc = new ObjectContext(coid);
    ctx->clone_obc->state = ctx->mode;   // take state from head obc's
    ctx->clone_obc->obs.oi.version = ctx->at_version;
    ctx->clone_obc->obs.oi.prior_version = oi.version;
    ctx->clone_obc->obs.oi.last_reqid = oi.last_reqid;
    ctx->clone_obc->obs.oi.mtime = oi.mtime;
    ctx->clone_obc->obs.oi.snaps = snaps;
    ctx->clone_obc->obs.exists = true;
    ctx->clone_obc->get();

    ctx->clone_obc->force_start_write();
    if (is_primary())
      register_object_context(ctx->clone_obc);
    
    _make_clone(t, soid, coid, &ctx->clone_obc->obs.oi);
    
    // add to snap bound collections
    coll_t fc = make_snap_collection(t, snaps[0]);
    t.collection_add(fc, info.pgid.to_coll(), coid);
    if (snaps.size() > 1) {
      coll_t lc = make_snap_collection(t, snaps[snaps.size()-1]);
      t.collection_add(lc, info.pgid.to_coll(), coid);
    }
    
    info.stats.num_objects++;
    info.stats.num_object_clones++;
    oi.snapset.clones.push_back(coid.snap);
    oi.snapset.clone_size[coid.snap] = ctx->obs->oi.size;
    oi.snapset.clone_overlap[coid.snap].insert(0, ctx->obs->oi.size);
    
    // log clone
    dout(10) << " cloning v " << oi.version
	     << " to " << coid << " v " << ctx->at_version
	     << " snaps=" << snaps << dendl;
    ctx->log.push_back(Log::Entry(PG::Log::Entry::CLONE, coid, ctx->at_version,
				  oi.version, ctx->reqid, oi.mtime));
    ::encode(snaps, ctx->log.back().snaps);

    ctx->at_version.version++;
  }
  
  // update snapset with latest snap context
  oi.snapset.seq = snapc.seq;
  oi.snapset.snaps = snapc.snaps;
}


void ReplicatedPG::add_interval_usage(interval_set<__u64>& s, pg_stat_t& stats)
{
  for (map<__u64,__u64>::iterator p = s.m.begin(); p != s.m.end(); p++) {
    stats.num_bytes += p->second;
    stats.num_kb += SHIFT_ROUND_UP(p->first+p->second, 10) - (p->first >> 10);
  }
}


int ReplicatedPG::prepare_transaction(OpContext *ctx)
{
  assert(!ctx->ops.empty());
  
  object_info_t *poi = &ctx->obs->oi;

  const sobject_t& soid = poi->soid;

  // we'll need this to log
  eversion_t old_version = poi->version;

  // prepare the actual mutation
  int result = do_osd_ops(ctx, ctx->ops, ctx->outdata);

  if (result < 0 || ctx->op_t.empty())
    return result;  // error, or read op.

  // finish and log the op.
  poi->version = ctx->at_version;
  if (ctx->obs->exists) {
    poi->version = ctx->at_version;
    poi->prior_version = old_version;
    poi->last_reqid = ctx->reqid;
    if (ctx->mtime != utime_t()) {
      poi->mtime = ctx->mtime;
      dout(10) << " set mtime to " << poi->mtime << dendl;
    } else {
      dout(10) << " mtime unchanged at " << poi->mtime << dendl;
    }

    bufferlist bv(sizeof(*poi));
    ::encode(*poi, bv);
    ctx->op_t.setattr(info.pgid.to_coll(), soid, OI_ATTR, bv);
  }

  // append to log
  int logopcode = Log::Entry::MODIFY;
  if (!ctx->obs->exists)
    logopcode = Log::Entry::DELETE;
  ctx->log.push_back(Log::Entry(logopcode, soid, ctx->at_version, old_version,
				ctx->reqid, ctx->mtime));
  return result;
}

void ReplicatedPG::log_op(vector<Log::Entry>& logv, eversion_t trim_to,
			  ObjectStore::Transaction& t)
{
  dout(10) << "log_op " << log << dendl;

  bufferlist log_bl;
  for (vector<Log::Entry>::iterator p = logv.begin();
       p != logv.end();
       p++)
    add_log_entry(*p, log_bl);
  append_log(t, log_bl, logv[0].version);
  trim(t, trim_to);

  // update the local pg, pg log
  write_info(t);
}






// ========================================================================
// rep op gather

class C_OSD_OpCommit : public Context {
public:
  ReplicatedPG *pg;
  ReplicatedPG::RepGather *repop;

  C_OSD_OpCommit(ReplicatedPG *p, ReplicatedPG::RepGather *rg) :
    pg(p), repop(rg) {
    repop->get();
    pg->get();    // we're copying the pointer
  }
  void finish(int r) {
    pg->lock();
    if (!pg->is_deleted()) 
      pg->op_ondisk(repop);
    repop->put();
    pg->unlock();
    pg->put();
  }
};

/** op_commit
 * transaction commit on the acker.
 */
void ReplicatedPG::op_ondisk(RepGather *repop)
{
  if (repop->aborted) {
    dout(10) << "op_ondisk " << *repop << " -- aborted" << dendl;
  } else if (repop->waitfor_disk.count(osd->get_nodeid()) == 0) {
    dout(10) << "op_ondisk " << *repop << " -- already marked ondisk" << dendl;
  } else {
    dout(10) << "op_ondisk " << *repop << dendl;
    repop->waitfor_disk.erase(osd->get_nodeid());
    repop->waitfor_nvram.erase(osd->get_nodeid());
    last_complete_ondisk = repop->pg_local_last_complete;
    eval_repop(repop);
  }
}


void ReplicatedPG::apply_repop(RepGather *repop)
{
  dout(10) << "apply_repop  applying update on " << *repop << dendl;
  assert(!repop->applied);

  Context *oncommit = new C_OSD_OpCommit(this, repop);

  list<ObjectStore::Transaction*> tls;
  tls.push_back(&repop->ctx->op_t);
  tls.push_back(&repop->ctx->local_t);
  unsigned r = osd->store->apply_transactions(tls, oncommit);
  if (r)
    dout(-10) << "apply_repop  apply transaction return " << r << " on " << *repop << dendl;
  
  // discard my reference to the buffer
  repop->ctx->op->get_data().clear();
  tls.clear();
  repop->ctx->op_t.clear_data();
  
  repop->applied = true;
  
  if (repop->ctx->clone_obc) {
   repop->ctx->clone_obc->finish_write();
   put_object_context(repop->ctx->clone_obc);
   repop->ctx->clone_obc = 0;
  }

  repop->obc->finish_write();

  put_object_context(repop->obc);
  repop->obc = 0;

  update_stats();

  // any completion stuff to do here?
  const sobject_t& soid = repop->ctx->obs->oi.soid;
  OSDOp& first = repop->ctx->ops[0];

  switch (first.op.op) { 
#if 0
  case CEPH_OSD_OP_UNBALANCEREADS:
    dout(-10) << "apply_repop  completed unbalance-reads on " << oid << dendl;
    unbalancing_reads.erase(oid);
    if (waiting_for_unbalanced_reads.count(oid)) {
      osd->take_waiters(waiting_for_unbalanced_reads[oid]);
      waiting_for_unbalanced_reads.erase(oid);
    }
    break;

  case CEPH_OSD_OP_BALANCEREADS:
    dout(-10) << "apply_repop  completed balance-reads on " << oid << dendl;
    /*
    if (waiting_for_balanced_reads.count(oid)) {
      osd->take_waiters(waiting_for_balanced_reads[oid]);
      waiting_for_balanced_reads.erase(oid);
    }
    */
    break;
#endif

  case CEPH_OSD_OP_WRUNLOCK:
    dout(-10) << "apply_repop  completed wrunlock on " << soid << dendl;
    if (waiting_for_wr_unlock.count(soid)) {
      osd->take_waiters(waiting_for_wr_unlock[soid]);
      waiting_for_wr_unlock.erase(soid);
    }
    break;
  }   
  
}

void ReplicatedPG::eval_repop(RepGather *repop)
{
  dout(10) << "eval_repop " << *repop << dendl;
  
  MOSDOp *op = (MOSDOp *)repop->ctx->op;

  ObjectContext *obc = repop->obc;

  // apply?
  if (!repop->applied &&
      obc->is_delayed_mode() &&
      repop->waitfor_ack.empty())  // all replicas have acked
    apply_repop(repop);

  // disk?
  if (repop->can_send_disk()) {
    if (op->wants_ondisk()) {
      // send commit.
      MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ONDISK);
      dout(10) << " sending commit on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, op->get_orig_source_inst());
      repop->sent_disk = true;
    }
  }

  // nvram?
  else if (repop->can_send_nvram()) {
    if (op->wants_onnvram()) {
      // send commit.
      MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ONNVRAM);
      dout(10) << " sending onnvram on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, op->get_orig_source_inst());
      repop->sent_nvram = true;
    }
  }

  // ack?
  else if (repop->can_send_ack()) {
    if (op->wants_ack()) {
      // send ack
      MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK);
      dout(10) << " sending ack on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, op->get_orig_source_inst());
      repop->sent_ack = true;
    }

    utime_t now = g_clock.now();
    now -= repop->start;
    osd->logger->finc(l_osd_rlsum, now);
    osd->logger->inc(l_osd_rlnum, 1);
  }

  // done.
  if (repop->can_delete()) {
    calc_min_last_complete_ondisk();

    dout(10) << " removing " << *repop << dendl;
    assert(!repop_queue.empty());
    assert(repop_queue.front() == repop);
    repop_queue.pop_front();
    repop_map.erase(repop->rep_tid);
    repop->put();
  }
}


void ReplicatedPG::issue_repop(RepGather *repop, int dest, utime_t now,
			       bool old_exists, __u64 old_size, eversion_t old_version)
{
  const sobject_t& soid = repop->ctx->obs->oi.soid;
  dout(7) << " issue_repop rep_tid " << repop->rep_tid
          << " o " << soid
          << " to osd" << dest
          << dendl;
  
  MOSDOp *op = (MOSDOp *)repop->ctx->op;

  // forward the write/update/whatever
  int acks_wanted = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;
  MOSDSubOp *wr = new MOSDSubOp(repop->ctx->reqid, info.pgid, soid,
				repop->noop, acks_wanted,
				osd->osdmap->get_epoch(), 
				repop->rep_tid, repop->ctx->at_version);

  if (op->get_flags() & CEPH_OSD_FLAG_PARALLELEXEC) {
    // replicate original op for parallel execution on replica
    wr->ops = repop->ctx->ops;
    wr->mtime = repop->ctx->mtime;
    wr->old_exists = old_exists;
    wr->old_size = old_size;
    wr->old_version = old_version;
    wr->snapset = repop->obc->obs.oi.snapset;
    wr->snapc = repop->ctx->snapc;
    wr->get_data() = repop->ctx->op->get_data();   // _copy_ bufferlist
  } else {
    // ship resulting transaction and log entries
    wr->ops = repop->ctx->ops;   // just fyi
    ::encode(repop->ctx->op_t, wr->get_data());
    ::encode(repop->ctx->log, wr->logbl);
  }

  wr->pg_trim_to = pg_trim_to;
  wr->peer_stat = osd->get_my_stat_for(now, dest);
  osd->messenger->send_message(wr, osd->osdmap->get_inst(dest));
}

ReplicatedPG::RepGather *ReplicatedPG::new_repop(OpContext *ctx, ObjectContext *obc,
						 bool noop, tid_t rep_tid)
{
  dout(10) << "new_repop rep_tid " << rep_tid << " on " << *ctx->op << dendl;

  RepGather *repop = new RepGather(ctx, obc, noop, rep_tid, info.last_complete);

  obc->start_write();
  obc->get();  // we take a ref

  // initialize gather sets
  for (unsigned i=0; i<acting.size(); i++) {
    int osd = acting[i];
    repop->waitfor_ack.insert(osd);
    repop->waitfor_disk.insert(osd);
  }

  repop->start = g_clock.now();

  repop_queue.push_back(&repop->queue_item);
  repop_map[repop->rep_tid] = repop;
  repop->get();

  return repop;
}
 

void ReplicatedPG::repop_ack(RepGather *repop, int result, int ack_type,
			     int fromosd, eversion_t peer_lcod)
{
  MOSDOp *op = (MOSDOp *)repop->ctx->op;

  dout(7) << "repop_ack rep_tid " << repop->rep_tid << " op " << *op
          << " result " << result
	  << " ack_type " << ack_type
	  << " from osd" << fromosd
          << dendl;
  
  if (ack_type & CEPH_OSD_FLAG_ONDISK) {
    // disk
    if (repop->waitfor_disk.count(fromosd)) {
      repop->waitfor_disk.erase(fromosd);
      repop->waitfor_nvram.erase(fromosd);
      repop->waitfor_ack.erase(fromosd);
      peer_last_complete_ondisk[fromosd] = peer_lcod;
    }
  } else if (ack_type & CEPH_OSD_FLAG_ONNVRAM) {
    // nvram
    repop->waitfor_nvram.erase(fromosd);
    repop->waitfor_ack.erase(fromosd);
  } else {
    // ack
    repop->waitfor_ack.erase(fromosd);
  }

  eval_repop(repop);
}







// -------------------------------------------------------


ReplicatedPG::ObjectContext *ReplicatedPG::get_object_context(const sobject_t& soid, bool can_create)
{
  map<sobject_t, ObjectContext*>::iterator p = object_contexts.find(soid);
  ObjectContext *obc;
  if (p != object_contexts.end()) {
    obc = p->second;
    dout(10) << "get_object_context " << soid << " " << obc->ref
	     << " -> " << (obc->ref+1) << dendl;
  } else {
    // check disk
    bufferlist bv;
    int r = osd->store->getattr(info.pgid.to_coll(), soid, OI_ATTR, bv);
    if (r < 0 && !can_create)
      return 0;   // -ENOENT!

    obc = new ObjectContext(soid);

    if (r >= 0) {
      obc->obs.oi.decode(bv);

      if (soid.snap == CEPH_NOSNAP && !obc->obs.oi.snapset.head_exists) {
	obc->obs.exists = false;
      } else {
	obc->obs.exists = true;
      }
    } else {
      obc->obs.exists = false;
    }
    dout(10) << "get_object_context " << soid << " read " << obc->obs.oi << dendl;
  }
  obc->ref++;
  return obc;
}


int ReplicatedPG::find_object_context(const object_t& oid, snapid_t snapid,
				      ObjectContext **pobc,
				      bool can_create)
{
  // first look at the head
  sobject_t head(oid, CEPH_NOSNAP);
  ObjectContext *hobc = get_object_context(head, can_create);
  if (!hobc)
    return -ENOENT;

  dout(10) << "find_object_context " << oid << " @" << snapid
	   << " snapset " << hobc->obs.oi.snapset << dendl;
 
  // head?
  if (snapid > hobc->obs.oi.snapset.seq) {
    dout(10) << "find_object_context  " << head
	     << " want " << snapid << " > snapset seq " << hobc->obs.oi.snapset.seq
	     << " -- HIT " << hobc->obs
	     << dendl;
    *pobc = hobc;
    return 0;
  }

  // which clone would it be?
  unsigned k = 0;
  while (k < hobc->obs.oi.snapset.clones.size() &&
	 hobc->obs.oi.snapset.clones[k] < snapid)
    k++;
  if (k == hobc->obs.oi.snapset.clones.size()) {
    dout(10) << "get_object_context  no clones with last >= snapid " << snapid << " -- DNE" << dendl;
    put_object_context(hobc);
    return -ENOENT;
  }
  sobject_t soid(oid, hobc->obs.oi.snapset.clones[k]);

  put_object_context(hobc); // we're done with head obc
  hobc = 0;

  if (missing.is_missing(soid)) {
    dout(20) << "get_object_context  " << soid << " missing, try again later" << dendl;
    return -EAGAIN;
  }

  ObjectContext *obc = get_object_context(soid);

  // clone
  dout(20) << "get_object_context  " << soid << " snaps " << obc->obs.oi.snaps << dendl;
  snapid_t first = obc->obs.oi.snaps[obc->obs.oi.snaps.size()-1];
  snapid_t last = obc->obs.oi.snaps[0];
  if (first <= snapid) {
    dout(20) << "get_object_context  " << soid << " [" << first << "," << last
	     << "] contains " << snapid << " -- HIT " << obc->obs << dendl;
    *pobc = obc;
    return 0;
  } else {
    dout(20) << "get_object_context  " << soid << " [" << first << "," << last
	     << "] does not contain " << snapid << " -- DNE" << dendl;
    put_object_context(obc);
    return -ENOENT;
  }
}

void ReplicatedPG::put_object_context(ObjectContext *obc)
{
  dout(10) << "put_object_context " << obc->obs.oi.soid << " "
	   << obc->ref << " -> " << (obc->ref-1) << dendl;

  if (obc->wake) {
    osd->take_waiters(obc->waiting);
    obc->wake = false;
  }

  --obc->ref;
  if (obc->ref == 0) {
    assert(obc->waiting.empty());

    if (obc->registered)
      object_contexts.erase(obc->obs.oi.soid);
    delete obc;

    if (object_contexts.empty())
      kick();
  }
}



// ----------------------
// balance reads cruft

// for reads
#if 0
  // wrlocked?
  if ((op->get_snapid() == 0 || op->get_snapid() == CEPH_NOSNAP) &&
      block_if_wrlocked(op, *ctx.poi)) 
    return;
#endif

#if 0
  // !primary and unbalanced?
  //  (ignore ops forwarded from the primary)
  if (!is_primary()) {
    if (op->get_source().is_osd() &&
	op->get_source().num() == get_primary()) {
      // read was shed to me by the primary
      int from = op->get_source().num();
      assert(op->get_flags() & CEPH_OSD_FLAG_PEERSTAT);
      osd->take_peer_stat(from, op->get_peer_stat());
      dout(10) << "read shed IN from " << op->get_source() 
		<< " " << op->get_reqid()
		<< ", me = " << osd->my_stat.read_latency_mine
		<< ", them = " << op->get_peer_stat().read_latency
		<< (osd->my_stat.read_latency_mine > op->get_peer_stat().read_latency ? " WTF":"")
		<< dendl;
      osd->logger->inc(l_osd_shdin);

      // does it look like they were wrong to do so?
      Mutex::Locker lock(osd->peer_stat_lock);
      if (osd->my_stat.read_latency_mine > op->get_peer_stat().read_latency &&
	  osd->my_stat_on_peer[from].read_latency_mine < op->get_peer_stat().read_latency) {
	dout(-10) << "read shed IN from " << op->get_source() 
		  << " " << op->get_reqid()
		  << " and me " << osd->my_stat.read_latency_mine
		  << " > them " << op->get_peer_stat().read_latency
		  << ", but they didn't know better, sharing" << dendl;
	osd->my_stat_on_peer[from] = osd->my_stat;
	/*
	osd->messenger->send_message(new MOSDPing(osd->osdmap->get_fsid(), osd->osdmap->get_epoch(),
						  osd->my_stat),
				     osd->osdmap->get_inst(from));
	*/
      }
    } else {
      // make sure i exist and am balanced, otherwise fw back to acker.
      bool b;
      if (!osd->store->exists(info.pgid.to_coll(), soid) || 
	  osd->store->getattr(info.pgid.to_coll(), soid, "balance-reads", &b, 1) < 0) {
	dout(-10) << "read on replica, object " << soid 
		  << " dne or no balance-reads, fw back to primary" << dendl;
	osd->messenger->forward_message(op, osd->osdmap->get_inst(get_primary()));
	return;
      }
    }
  }
#endif

// for writes
#if 0
  // balance-reads set?
  char v;
  if ((op->get_op() != CEPH_OSD_OP_BALANCEREADS && op->get_op() != CEPH_OSD_OP_UNBALANCEREADS) &&
      (osd->store->getattr(info.pgid.to_coll(), soid, "balance-reads", &v, 1) >= 0 ||
       balancing_reads.count(soid.oid))) {
    
    if (!unbalancing_reads.count(soid.oid)) {
      // unbalance
      dout(-10) << "preprocess_op unbalancing-reads on " << soid.oid << dendl;
      unbalancing_reads.insert(soid.oid);
      
      ceph_object_layout layout;
      layout.ol_pgid = info.pgid.u.pg64;
      layout.ol_stripe_unit = 0;
      MOSDOp *pop = new MOSDOp(0, osd->get_tid(),
			       soid.oid,
			       layout,
			       osd->osdmap->get_epoch(),
			       CEPH_OSD_OP_UNBALANCEREADS, 0);
      do_op(pop);
    }

    // add to wait queue
    dout(-10) << "preprocess_op waiting for unbalance-reads on " << soid.oid << dendl;
    waiting_for_unbalanced_reads[soid.oid].push_back(op);
    delete ctx;
    return;
  }
#endif

#if 0
  // wrlock?
  if (!ctx->ops.empty() &&  // except noop; we just want to flush
      block_if_wrlocked(op, obc->oi)) {
    put_object_context(obc);
    delete ctx;
    return; // op will be handled later, after the object unlocks
  }
#endif






// sub op modify


// commit (to disk) callback
class C_OSD_RepModifyCommit : public Context {
public:
  ReplicatedPG *pg;

  MOSDSubOp *op;
  int destosd;

  eversion_t pg_last_complete;

  Mutex lock;
  Cond cond;
  bool acked;
  bool waiting;

  C_OSD_RepModifyCommit(ReplicatedPG *p, MOSDSubOp *oo, int dosd, eversion_t lc) : 
    pg(p), op(oo), destosd(dosd), pg_last_complete(lc),
    lock("C_OSD_RepModifyCommit::lock"),
    acked(false), waiting(false) { 
    pg->get();  // we're copying the pointer.
  }
  void finish(int r) {
    lock.Lock();
    assert(!waiting);
    while (!acked) {
      waiting = true;
      cond.Wait(lock);
    }
    assert(acked);
    lock.Unlock();

    pg->lock();
    pg->sub_op_modify_ondisk(op, destosd, pg_last_complete);
    pg->unlock();
    pg->put();
  }
  void ack() {
    lock.Lock();
    assert(!acked);
    acked = true;
    if (waiting) cond.Signal();

    // discard my reference to buffer
    op->get_data().clear();

    lock.Unlock();
  }
};

void ReplicatedPG::sub_op_modify(MOSDSubOp *op)
{
  const sobject_t& soid = op->poid;

  const char *opname;
  if (op->noop)
    opname = "no-op";
  else
    opname = ceph_osd_op_name(op->ops[0].op.op);

  dout(10) << "sub_op_modify " << opname 
           << " " << soid 
           << " v " << op->version
	   << (op->noop ? " NOOP" : "")
	   << (op->logbl.length() ? " (transaction)" : " (parallel exec")
	   << " " << op->logbl.length()
	   << dendl;  

  // sanity checks
  assert(op->map_epoch >= info.history.same_primary_since);
  assert(is_active());
  assert(is_replica());
  
  // note peer's stat
  int fromosd = op->get_source().num();
  osd->take_peer_stat(fromosd, op->peer_stat);

  // we better not be missing this.
  assert(!missing.is_missing(soid));

  int ackerosd = acting[0];
  osd->logger->inc(l_osd_r_wr);
  osd->logger->inc(l_osd_r_wrb, op->get_data().length());
  
  list<ObjectStore::Transaction*> tls;
  ObjectStore::Transaction opt, localt;
  OpContext *ctx = 0;

  if (!op->noop) {
    if (op->logbl.length()) {
      // shipped transaction and log entries
      vector<Log::Entry> log;
      
      bufferlist::iterator p = op->get_data().begin();
      ::decode(opt, p);
      p = op->logbl.begin();
      ::decode(log, p);
      
      log_op(log, op->pg_trim_to, localt);

      tls.push_back(&opt);
      tls.push_back(&localt);
    } else {
      // do op
      ObjectState obs(op->poid);
      obs.oi.version = op->old_version;
      obs.oi.snapset = op->snapset;
      obs.oi.size = op->old_size;
      obs.exists = op->old_exists;
      
      ctx = new OpContext(op, op->reqid, op->ops, op->get_data(), ObjectContext::RMW, &obs, this);
      
      ctx->mtime = op->mtime;
      ctx->at_version = op->version;
      ctx->snapc = op->snapc;
      
      prepare_transaction(ctx);
      log_op(ctx->log, op->pg_trim_to, ctx->local_t);
    
      tls.push_back(&ctx->op_t);
      tls.push_back(&ctx->local_t);
    }
  } else {
    // just trim the log
    if (op->pg_trim_to != eversion_t()) {
      trim(ctx->local_t, op->pg_trim_to);
      tls.push_back(&ctx->local_t);
    }
  }
    
  C_OSD_RepModifyCommit *oncommit = new C_OSD_RepModifyCommit(this, op, ackerosd,
							      info.last_complete);
  unsigned r = osd->store->apply_transactions(tls, oncommit);
  if (r) {
    derr(0) << "error applying transaction: r = " << r << dendl;
  }

  // ack myself.
  oncommit->ack(); 

  delete ctx;

  // send ack to acker
  MOSDSubOpReply *ack = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK);
  ack->set_peer_stat(osd->get_my_stat_for(g_clock.now(), ackerosd));
  osd->messenger->send_message(ack, osd->osdmap->get_inst(ackerosd));
}

void ReplicatedPG::sub_op_modify_ondisk(MOSDSubOp *op, int ackerosd, eversion_t last_complete)
{
  // send commit.
  dout(10) << "rep_modify_commit on op " << *op
           << ", sending commit to osd" << ackerosd
           << dendl;
  if (osd->osdmap->is_up(ackerosd)) {
    last_complete_ondisk = last_complete;
    MOSDSubOpReply *commit = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ONDISK);
    commit->set_last_complete_ondisk(last_complete);
    commit->set_peer_stat(osd->get_my_stat_for(g_clock.now(), ackerosd));
    osd->messenger->send_message(commit, osd->osdmap->get_inst(ackerosd));
  }
  
  delete op;
}

void ReplicatedPG::sub_op_modify_reply(MOSDSubOpReply *r)
{
  // must be replication.
  tid_t rep_tid = r->get_rep_tid();
  int fromosd = r->get_source().num();
  
  osd->take_peer_stat(fromosd, r->get_peer_stat());
  
  if (repop_map.count(rep_tid)) {
    // oh, good.
    repop_ack(repop_map[rep_tid], 
	      r->get_result(), r->ack_type,
	      fromosd, 
	      r->get_last_complete_ondisk());
  }

  delete r;
}










// ===========================================================

void ReplicatedPG::calc_head_subsets(SnapSet& snapset, const sobject_t& head,
				     Missing& missing,
				     interval_set<__u64>& data_subset,
				     map<sobject_t, interval_set<__u64> >& clone_subsets)
{
  dout(10) << "calc_head_subsets " << head
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  struct stat st;
  osd->store->stat(info.pgid.to_coll(), head, &st);

  interval_set<__u64> cloning;
  interval_set<__u64> prev;
  if (st.st_size)
    prev.insert(0, st.st_size);    
  
  for (int j=snapset.clones.size()-1; j>=0; j--) {
    sobject_t c = head;
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c)) {
      dout(10) << "calc_head_subsets " << head << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_head_subsets " << head << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }

  // what's left for us to push?
  if (st.st_size)
    data_subset.insert(0, st.st_size);
  data_subset.subtract(cloning);

  dout(10) << "calc_head_subsets " << head
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

void ReplicatedPG::calc_clone_subsets(SnapSet& snapset, const sobject_t& soid,
				      Missing& missing,
				      interval_set<__u64>& data_subset,
				      map<sobject_t, interval_set<__u64> >& clone_subsets)
{
  dout(10) << "calc_clone_subsets " << soid
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  __u64 size = snapset.clone_size[soid.snap];

  unsigned i;
  for (i=0; i < snapset.clones.size(); i++)
    if (snapset.clones[i] == soid.snap)
      break;

  // any overlap with next older clone?
  interval_set<__u64> cloning;
  interval_set<__u64> prev;
  if (size)
    prev.insert(0, size);    
  for (int j=i-1; j>=0; j--) {
    sobject_t c = soid;
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c)) {
      dout(10) << "calc_clone_subsets " << soid << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }
  
  // overlap with next newest?
  interval_set<__u64> next;
  if (size)
    next.insert(0, size);    
  for (unsigned j=i+1; j<snapset.clones.size(); j++) {
    sobject_t c = soid;
    c.snap = snapset.clones[j];
    next.intersection_of(snapset.clone_overlap[snapset.clones[j-1]]);
    if (!missing.is_missing(c)) {
      dout(10) << "calc_clone_subsets " << soid << " has next " << c
	       << " overlap " << next << dendl;
      clone_subsets[c] = next;
      cloning.union_of(next);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have next " << c
	     << " overlap " << next << dendl;
  }
  
  // what's left for us to push?
  if (size)
    data_subset.insert(0, size);
  data_subset.subtract(cloning);

  dout(10) << "calc_clone_subsets " << soid
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}


/** pull - request object from a peer
 */
bool ReplicatedPG::pull(const sobject_t& soid)
{
  eversion_t v = missing.missing[soid].need;

  int fromosd = -1;
  assert(missing_loc.count(soid));
  for (set<int>::iterator p = missing_loc[soid].begin();
       p != missing_loc[soid].end();
       p++) {
    if (osd->osdmap->is_up(*p)) {
      fromosd = *p;
      break;
    }
  }
  
  dout(7) << "pull " << soid
          << " v " << v 
	  << " on osds " << missing_loc[soid]
	  << " from osd" << fromosd
	  << dendl;

  if (fromosd < 0)
    return false;

  map<sobject_t, interval_set<__u64> > clone_subsets;
  interval_set<__u64> data_subset;

  // is this a snapped object?  if so, consult the snapset.. we may not need the entire object!
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    sobject_t head = soid;
    head.snap = CEPH_NOSNAP;
    
    // do we have the head?
    if (missing.is_missing(head)) {
      if (pulling.count(head)) {
	dout(10) << " missing but already pulling head " << head << dendl;
	return false;
      } else {
	return pull(head);
      }
    }

    // check snapset
    bufferlist bl;
    int r = osd->store->getattr(info.pgid.to_coll(), head, OI_ATTR, bl);
    assert(r >= 0);
    object_info_t oi(bl);
    dout(10) << " snapset " << oi.snapset << dendl;
    
    calc_clone_subsets(oi.snapset, soid, missing,
		       data_subset, clone_subsets);
    // FIXME: this may overestimate if we are pulling multiple clones in parallel...
    dout(10) << " pulling " << data_subset << ", will clone " << clone_subsets
	     << dendl;
  } else {
    // pulling head or unversioned object.
    // always pull the whole thing.
  }

  // send op
  osd_reqid_t rid;
  tid_t tid = osd->get_tid();
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, soid, false, CEPH_OSD_FLAG_ACK,
				   osd->osdmap->get_epoch(), tid, v);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PULL;
  subop->data_subset.swap(data_subset);
  // do not include clone_subsets in pull request; we will recalculate this
  // when the object is pushed back.
  //subop->clone_subsets.swap(clone_subsets);
  osd->messenger->send_message(subop, osd->osdmap->get_inst(fromosd));
  
  // take note
  assert(pulling.count(soid) == 0);
  pulling[soid].first = v;
  pulling[soid].second = fromosd;
  return true;
}


/*
 * intelligently push an object to a replica.  make use of existing
 * clones/heads and dup data ranges where possible.
 */
void ReplicatedPG::push_to_replica(const sobject_t& soid, int peer)
{
  dout(10) << "push_to_replica " << soid << " osd" << peer << dendl;

  // get size
  struct stat st;
  int r = osd->store->stat(info.pgid.to_coll(), soid, &st);
  assert(r == 0);
  
  map<sobject_t, interval_set<__u64> > clone_subsets;
  interval_set<__u64> data_subset;

  bufferlist bv;
  r = osd->store->getattr(info.pgid.to_coll(), soid, OI_ATTR, bv);
  assert(r >= 0);
  object_info_t oi(bv);
  
  // are we doing a clone on the replica?
  if (soid.snap && soid.snap < CEPH_NOSNAP) {	
    sobject_t head = soid;
    head.snap = CEPH_NOSNAP;
    if (peer_missing[peer].is_missing(head) &&
	peer_missing[peer].have_old(head) == oi.prior_version) {
      dout(10) << "push_to_replica osd" << peer << " has correct old " << head
	       << " v" << oi.prior_version 
	       << ", pushing " << soid << " attrs as a clone op" << dendl;
      interval_set<__u64> data_subset;
      map<sobject_t, interval_set<__u64> > clone_subsets;
      clone_subsets[head].insert(0, st.st_size);
      push(soid, peer, data_subset, clone_subsets);
      return;
    }

    // try to base push off of clones that succeed/preceed poid
    // we need the head (and current SnapSet) to do that.
    if (missing.is_missing(head)) {
      dout(15) << "push_to_replica missing head " << head << ", pushing raw clone" << dendl;
      return push(soid, peer);  // no head.  push manually.
    }
    
    bufferlist bl;
    r = osd->store->getattr(info.pgid.to_coll(), head, OI_ATTR, bl);
    assert(r >= 0);
    object_info_t hoi(bl);
    dout(15) << "push_to_replica head snapset is " << hoi.snapset << dendl;

    calc_clone_subsets(hoi.snapset, soid, peer_missing[peer],
		       data_subset, clone_subsets);
  } else {
    // pushing head or unversioned object.
    // base this on partially on replica's clones?
    dout(15) << "push_to_replica head snapset is " << oi.snapset << dendl;
    calc_head_subsets(oi.snapset, soid, peer_missing[peer], data_subset, clone_subsets);
  }

  dout(10) << "push_to_replica " << soid << " pushing " << data_subset
	   << " cloning " << clone_subsets << dendl;    
  push(soid, peer, data_subset, clone_subsets);
}

/*
 * push - send object to a peer
 */
void ReplicatedPG::push(const sobject_t& soid, int peer)
{
  interval_set<__u64> subset;
  map<sobject_t, interval_set<__u64> > clone_subsets;
  push(soid, peer, subset, clone_subsets);
}

void ReplicatedPG::push(const sobject_t& soid, int peer, 
			interval_set<__u64> &data_subset,
			map<sobject_t, interval_set<__u64> >& clone_subsets)
{
  // read data+attrs
  bufferlist bl;
  map<nstring,bufferptr> attrset;
  __u64 size;

  if (data_subset.size() || clone_subsets.size()) {
    struct stat st;
    int r = osd->store->stat(info.pgid.to_coll(), soid, &st);
    assert(r == 0);
    size = st.st_size;

    for (map<__u64,__u64>::iterator p = data_subset.m.begin();
	 p != data_subset.m.end();
	 p++) {
      bufferlist bit;
      osd->store->read(info.pgid.to_coll(), soid, p->first, p->second, bit);
      bl.claim_append(bit);
    }
  } else {
    osd->store->read(info.pgid.to_coll(), soid, 0, 0, bl);
    size = bl.length();
  }

  osd->store->getattrs(info.pgid.to_coll(), soid, attrset);

  bufferlist bv;
  bv.push_back(attrset[OI_ATTR]);
  object_info_t oi(bv);

  // ok
  dout(7) << "push " << soid << " v " << oi.version 
	  << " size " << size
	  << " subset " << data_subset
          << " data " << bl.length()
          << " to osd" << peer
          << dendl;

  osd->logger->inc(l_osd_r_push);
  osd->logger->inc(l_osd_r_pushb, bl.length());
  
  // send
  osd_reqid_t rid;  // useless?
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, soid, false, 0,
				   osd->osdmap->get_epoch(), osd->get_tid(), oi.version);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PUSH;
  subop->ops[0].op.offset = 0;
  subop->ops[0].op.length = size;
  subop->ops[0].data = bl;
  subop->data_subset.swap(data_subset);
  subop->clone_subsets.swap(clone_subsets);
  subop->attrset.swap(attrset);
  osd->messenger->send_message(subop, osd->osdmap->get_inst(peer));
  
  if (is_primary()) {
    pushing[soid].insert(peer);
    peer_missing[peer].got(soid, oi.version);
  }
}

void ReplicatedPG::sub_op_push_reply(MOSDSubOpReply *reply)
{
  dout(10) << "sub_op_push_reply from " << reply->get_source() << " " << *reply << dendl;
  
  int peer = reply->get_source().num();
  const sobject_t& soid = reply->get_poid();
  
  if (pushing.count(soid) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd" << peer
	     << ", or anybody else"
	     << dendl;
  } else if (pushing[soid].count(peer) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd" << peer
	     << ", only " << pushing[soid]
	     << dendl;
  } else {
    pushing[soid].erase(peer);

    if (peer_missing.count(peer) == 0 ||
        peer_missing[peer].num_missing() == 0) 
      uptodate_set.insert(peer);

    update_stats();

    if (pushing[soid].empty()) {
      dout(10) << "pushed " << soid << " to all replicas" << dendl;
      finish_recovery_op();
    } else {
      dout(10) << "pushed " << soid << ", still waiting for push ack from " 
	       << pushing[soid] << dendl;
    }
  }
  delete reply;
}


/** op_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void ReplicatedPG::sub_op_pull(MOSDSubOp *op)
{
  const sobject_t soid = op->poid;
  const eversion_t v = op->version;

  dout(7) << "op_pull " << soid << " v " << op->version
          << " from " << op->get_source()
          << dendl;

  assert(!is_primary());  // we should be a replica or stray.

  push(soid, op->get_source().num(), op->data_subset, op->clone_subsets);
  delete op;
}


struct C_OSD_Commit : public Context {
  ReplicatedPG *pg;
  epoch_t same_since;
  eversion_t last_complete;
  C_OSD_Commit(ReplicatedPG *p, epoch_t ss, eversion_t lc) : pg(p), same_since(ss), last_complete(lc) {
    pg->get();
  }
  void finish(int r) {
    pg->lock();
    pg->_committed(same_since, last_complete);
    pg->unlock();
    pg->put();
  }
};

void ReplicatedPG::_committed(epoch_t same_since, eversion_t last_complete)
{
  if (same_since == info.history.same_since) {
    dout(10) << "_committed last_complete " << last_complete << " now ondisk" << dendl;
    last_complete_ondisk = last_complete;

    if (last_complete_ondisk == info.last_update) {
      if (is_replica()) {
	// we are fully up to date.  tell the primary!
	osd->messenger->send_message(new MOSDPGTrim(osd->osdmap->get_epoch(), info.pgid,
						    last_complete_ondisk),
				     osd->osdmap->get_inst(get_primary()));
      } else if (is_primary()) {
	// we are the primary.  tell replicas to trim?
	if (calc_min_last_complete_ondisk())
	  trim_peers();
      }
    }

  } else {
    dout(10) << "_committed pg has changed, not touching last_complete_ondisk" << dendl;
  }
}

/** op_push
 * NOTE: called from opqueue.
 */
void ReplicatedPG::sub_op_push(MOSDSubOp *op)
{
  const sobject_t& soid = op->poid;
  eversion_t v = op->version;
  OSDOp& push = op->ops[0];

  dout(7) << "op_push " 
          << soid 
          << " v " << v 
	  << " len " << push.op.length
	  << " data_subset " << op->data_subset
	  << " clone_subsets " << op->clone_subsets
	  << " data len " << op->get_data().length()
          << dendl;

  interval_set<__u64> data_subset;
  map<sobject_t, interval_set<__u64> > clone_subsets;

  bufferlist data;
  data.claim(op->get_data());

  // determine data/clone subsets
  data_subset = op->data_subset;
  if (data_subset.empty() && push.op.length && push.op.length == data.length())
    data_subset.insert(0, push.op.length);
  clone_subsets = op->clone_subsets;

  if (is_primary()) {
    if (soid.snap && soid.snap < CEPH_NOSNAP) {
      // clone.  make sure we have enough data.
      sobject_t head = soid;
      head.snap = CEPH_NOSNAP;
      assert(!missing.is_missing(head));

      bufferlist bl;
      int r = osd->store->getattr(info.pgid.to_coll(), head, OI_ATTR, bl);
      assert(r >= 0);
      object_info_t hoi(bl);
      
      clone_subsets.clear();   // forget what pusher said; recalculate cloning.

      interval_set<__u64> data_needed;
      calc_clone_subsets(hoi.snapset, soid, missing, data_needed, clone_subsets);
      
      dout(10) << "sub_op_push need " << data_needed << ", got " << data_subset << dendl;
      if (!data_needed.subset_of(data_subset)) {
	dout(0) << " we did not get enough of " << soid << " object data" << dendl;
	delete op;
	return;
      }

      // did we get more data than we need?
      if (!data_subset.subset_of(data_needed)) {
	interval_set<__u64> extra = data_subset;
	extra.subtract(data_needed);
	dout(10) << " we got some extra: " << extra << dendl;

	bufferlist result;
	int off = 0;
	for (map<__u64,__u64>::iterator p = data_subset.m.begin();
	     p != data_subset.m.end();
	     p++) {
	  interval_set<__u64> x;
	  x.insert(p->first, p->second);
	  x.intersection_of(data_needed);
	  dout(20) << " data_subset object extent " << p->first << "~" << p->second << " need " << x << dendl;
	  if (!x.empty()) {
	    __u64 first = x.m.begin()->first;
	    __u64 len = x.m.begin()->second;
	    bufferlist sub;
	    int boff = off + (first - p->first);
	    dout(20) << "   keeping buffer extent " << boff << "~" << len << dendl;
	    sub.substr_of(data, boff, len);
	    result.claim_append(sub);
	  }
	  off += p->second;
	}
	data.claim(result);
	dout(20) << " new data len is " << data.length() << dendl;
      }
    } else {
      // head|unversioned. for now, primary will _only_ pull full copies of the head.
      assert(op->clone_subsets.empty());
    }
  }
  dout(15) << " data_subset " << data_subset
	   << " clone_subsets " << clone_subsets
	   << dendl;

  // write object and add it to the PG
  ObjectStore::Transaction t;
  t.remove(info.pgid.to_coll(), soid);  // in case old version exists

  __u64 boff = 0;
  for (map<sobject_t, interval_set<__u64> >::iterator p = clone_subsets.begin();
       p != clone_subsets.end();
       p++)
    for (map<__u64,__u64>::iterator q = p->second.m.begin();
	 q != p->second.m.end(); 
	 q++) {
      dout(15) << " clone_range " << p->first << " " << q->first << "~" << q->second << dendl;
      t.clone_range(info.pgid.to_coll(), soid, p->first, q->first, q->second);
    }
  for (map<__u64,__u64>::iterator p = data_subset.m.begin();
       p != data_subset.m.end(); 
       p++) {
    bufferlist bit;
    bit.substr_of(data, boff, p->second);
    t.write(info.pgid.to_coll(), soid, p->first, p->second, bit);
    dout(15) << " write " << p->first << "~" << p->second << dendl;
    boff += p->second;
  }

  if (data_subset.empty())
    t.touch(info.pgid.to_coll(), soid);

  t.setattrs(info.pgid.to_coll(), soid, op->attrset);
  if (soid.snap && soid.snap < CEPH_NOSNAP &&
      op->attrset.count(OI_ATTR)) {
    bufferlist bl;
    bl.push_back(op->attrset[OI_ATTR]);
    object_info_t oi(bl);
    if (oi.snaps.size()) {
      coll_t lc = make_snap_collection(t, oi.snaps[0]);
      t.collection_add(lc, info.pgid.to_coll(), soid);
      if (oi.snaps.size() > 1) {
	coll_t hc = make_snap_collection(t, oi.snaps[oi.snaps.size()-1]);
	t.collection_add(hc, info.pgid.to_coll(), soid);
      }
    }
  }

  if (missing.is_missing(soid, v)) {
    dout(10) << "got missing " << soid << " v " << v << dendl;
    missing.got(soid, v);

    // raise last_complete?
    while (log.complete_to != log.log.end()) {
      if (missing.missing.count(log.complete_to->soid))
	break;
      if (info.last_complete < log.complete_to->version)
	info.last_complete = log.complete_to->version;
      log.complete_to++;
    }
    dout(10) << "last_complete now " << info.last_complete << dendl;
  }

  // apply to disk!
  write_info(t);
  unsigned r = osd->store->apply_transaction(t, new C_OSD_Commit(this, info.history.same_since,
								 info.last_complete));
  assert(r == 0);

  osd->logger->inc(l_osd_r_pull);
  osd->logger->inc(l_osd_r_pullb, data.length());

  if (is_primary()) {

    missing_loc.erase(soid);

    // close out pull op?
    if (pulling.count(soid)) {
      pulling.erase(soid);
      finish_recovery_op();
    }

    update_stats();

    if (info.is_uptodate())
      uptodate_set.insert(osd->get_nodeid());
    
    if (is_active()) {
      // are others missing this too?  (only if we're active.. skip
      // this part if we're still repeering, it'll just confuse us)
      for (unsigned i=1; i<acting.size(); i++) {
	int peer = acting[i];
	assert(peer_missing.count(peer));
	if (peer_missing[peer].is_missing(soid)) {
	  push_to_replica(soid, peer);  // ok, push it, and they (will) have it now.
	  osd->start_recovery_op(this, 1);
	}
      }
    }

  } else {
    // ack if i'm a replica and being pushed to.
    MOSDSubOpReply *reply = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK); 
    osd->messenger->send_message(reply, op->get_source_inst());
  }

  // kick waiters
  if (waiting_for_missing_object.count(soid)) {
    dout(20) << " kicking waiters on " << soid << dendl;
    osd->take_waiters(waiting_for_missing_object[soid]);
    waiting_for_missing_object.erase(soid);
  } else {
    dout(20) << " no waiters on " << soid << dendl;
    /*for (hash_map<sobject_t,list<class Message*> >::iterator p = waiting_for_missing_object.begin();
	 p != waiting_for_missing_object.end();
	 p++)
      dout(20) << "   " << p->first << dendl;
    */
  }

  delete op;  // at the end... soid is a ref to op->soid!
}



/*
 * pg status change notification
 */

void ReplicatedPG::on_osd_failure(int o)
{
  //dout(10) << "on_osd_failure " << o << dendl;
}

void ReplicatedPG::apply_and_flush_repops(bool requeue)
{
  list<Message*> rq;

  // apply all repops
  while (!repop_queue.empty()) {
    RepGather *repop = repop_queue.front();
    repop_queue.pop_front();
    dout(10) << " applying repop tid " << repop->rep_tid << dendl;
    if (!repop->applied)
      apply_repop(repop);
    repop->aborted = true;
    repop_map.erase(repop->rep_tid);

    if (requeue) {
      dout(10) << " requeuing " << *repop->ctx->op << dendl;
      rq.push_back(repop->ctx->op);
      repop->ctx->op = 0;
    }

    repop->put();
  }

  if (requeue)
    osd->push_waiters(rq);
}

void ReplicatedPG::on_shutdown()
{
  dout(10) << "on_shutdown" << dendl;
  apply_and_flush_repops(false);
}

void ReplicatedPG::on_change()
{
  dout(10) << "on_change" << dendl;
  apply_and_flush_repops(is_primary());
  
  // clear pushing/pulling maps
  pushing.clear();
  pulling.clear();
}

void ReplicatedPG::on_role_change()
{
  dout(10) << "on_role_change" << dendl;

  // take object waiters
  for (hash_map<sobject_t, list<Message*> >::iterator it = waiting_for_missing_object.begin();
       it != waiting_for_missing_object.end();
       it++)
    osd->take_waiters(it->second);
  waiting_for_missing_object.clear();
}


// clear state.  called on recovery completion AND cancellation.
void ReplicatedPG::_clear_recovery_state()
{
  missing_loc.clear();
  pulling.clear();
  pushing.clear();
}


int ReplicatedPG::start_recovery_ops(int max)
{
  int started = 0;
  assert(is_primary());
  
  while (max > 0) {
    int n;
    if (uptodate_set.count(osd->whoami))
      n = recover_replicas(max);
    else
      n = recover_primary(max);
    started += n;
    osd->logger->inc(l_osd_rop, n);
    if (n < max)
      break;
    max -= n;
  }
  return started;
}

void ReplicatedPG::finish_recovery_op()
{
  dout(10) << "finish_recovery_op" << dendl;
  osd->finish_recovery_op(this, 1, false);
}


/**
 * do one recovery op.
 * return true if done, false if nothing left to do.
 */
int ReplicatedPG::recover_primary(int max)
{
  assert(is_primary());

  dout(10) << "recover_primary pulling " << pulling.size() << " in pg" << dendl;
  dout(10) << "recover_primary " << missing << dendl;
  dout(15) << "recover_primary " << missing.missing << dendl;

  // look at log!
  Log::Entry *latest = 0;
  int started = 0;
  int skipped = 0;

  map<sobject_t, Missing::item>::iterator p = missing.missing.lower_bound(log.last_requested);
  while (p != missing.missing.end()) {
    assert(log.objects.count(p->first));
    latest = log.objects[p->first];
    assert(latest);

    const sobject_t& soid(latest->soid);
    sobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    dout(10) << "recover_primary "
             << *latest
	     << (latest->is_update() ? " (update)":"")
	     << (missing.is_missing(latest->soid) ? " (missing)":"")
	     << (missing.is_missing(head) ? " (missing head)":"")
             << (pulling.count(latest->soid) ? " (pulling)":"")
	     << (pulling.count(head) ? " (pulling head)":"")
             << dendl;
    
    assert(latest->is_update());

    if (!pulling.count(latest->soid)) {
      if (pulling.count(head)) {
	++skipped;
      } else {
	// is this a clone operation that we can do locally?
	if (latest->op == Log::Entry::CLONE) {
	  if (missing.is_missing(head) &&
	      missing.have_old(head) == latest->prior_version) {
	    dout(10) << "recover_primary cloning " << head << " v" << latest->prior_version
		     << " to " << soid << " v" << latest->version
		     << " snaps " << latest->snaps << dendl;
	    ObjectStore::Transaction t;

	    ObjectContext *headobc = get_object_context(head);

	    object_info_t oi(soid);
	    oi.version = latest->version;
	    oi.prior_version = latest->prior_version;
	    oi.last_reqid = headobc->obs.oi.last_reqid;
	    oi.mtime = headobc->obs.oi.mtime;
	    ::decode(oi.snaps, latest->snaps);
	    _make_clone(t, head, soid, &oi);

	    put_object_context(headobc);

	    osd->store->apply_transaction(t);
	    missing.got(latest->soid, latest->version);
	    missing_loc.erase(latest->soid);
	    continue;
	  }
	}
	
	if (pull(soid))
	  ++started;
	else
	  ++skipped;
	if (started >= max)
	  return started;
      }
    }
    
    p++;

    // only advance last_requested if we haven't skipped anything
    if (!skipped)
      log.last_requested = latest->soid;
  }

  // done?
  if (!pulling.empty()) {
    dout(7) << "recover_primary requested everything, still waiting" << dendl;
    return started;
  }
  if (missing.num_missing()) {
    dout(7) << "recover_primary still missing " << missing << dendl;
    return started;
  }

  // done.
  if (info.last_complete != info.last_update) {
    dout(7) << "recover_primary last_complete " << info.last_complete << " -> " << info.last_update << dendl;
    info.last_complete = info.last_update;
  }

  log.reset_recovery_pointers();

  uptodate_set.insert(osd->whoami);
  if (is_all_uptodate()) {
    dout(-7) << "recover_primary complete" << dendl;
    finish_recovery();
  } else {
    dout(-10) << "recover_primary primary now complete, starting peer recovery" << dendl;
  }

  return started;
}

int ReplicatedPG::recover_replicas(int max)
{
  int started = 0;
  dout(-10) << "recover_replicas" << dendl;

  // this is FAR from an optimal recovery order.  pretty lame, really.
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    assert(peer_missing.count(peer));

    dout(10) << " peer osd" << peer << " missing " << peer_missing[peer] << dendl;
    dout(20) << "   " << peer_missing[peer].missing << dendl;

    if (peer_missing[peer].num_missing() == 0) 
      continue;
    
    // oldest first!
    sobject_t soid = peer_missing[peer].rmissing.begin()->second;
    eversion_t v = peer_missing[peer].rmissing.begin()->first;

    push_to_replica(soid, peer);

    // do other peers need it too?
    for (i++; i<acting.size(); i++) {
      int peer = acting[i];
      if (peer_missing.count(peer) &&
          peer_missing[peer].is_missing(soid)) 
	push_to_replica(soid, peer);
    }

    if (++started >= max)
      return started;
  }
  
  // nothing to do!
  dout(-10) << "recover_replicas - nothing to do!" << dendl;

  if (is_all_uptodate()) {
    finish_recovery();
  } else {
    dout(10) << "recover_replicas not all uptodate, acting " << acting << ", uptodate " << uptodate_set << dendl;
  }

  return started;
}



/** clean_up_local
 * remove any objects that we're storing but shouldn't.
 * as determined by log.
 */
void ReplicatedPG::clean_up_local(ObjectStore::Transaction& t)
{
  dout(10) << "clean_up_local" << dendl;

  assert(info.last_update >= log.bottom);  // otherwise we need some help!

  if (log.backlog) {

    // FIXME: sloppy pobject vs object conversions abound!  ***
    
    // be thorough.
    vector<sobject_t> ls;
    osd->store->collection_list(info.pgid.to_coll(), ls);
    if (ls.size() != info.stats.num_objects)
      dout(10) << " WARNING: " << ls.size() << " != num_objects " << info.stats.num_objects << dendl;

    set<sobject_t> s;
    
    for (vector<sobject_t>::iterator i = ls.begin();
         i != ls.end();
         i++) 
      s.insert(*i);

    set<sobject_t> did;
    for (list<Log::Entry>::reverse_iterator p = log.log.rbegin();
         p != log.log.rend();
         p++) {
      if (did.count(p->soid)) continue;
      did.insert(p->soid);
      
      if (p->is_delete()) {
        if (s.count(p->soid)) {
          dout(10) << " deleting " << p->soid
                   << " when " << p->version << dendl;
          t.remove(info.pgid.to_coll(), p->soid);
        }
        s.erase(p->soid);
      } else {
        // just leave old objects.. they're missing or whatever
        s.erase(p->soid);
      }
    }

    for (set<sobject_t>::iterator i = s.begin(); 
         i != s.end();
         i++) {
      dout(10) << " deleting stray " << *i << dendl;
      t.remove(info.pgid.to_coll(), *i);
    }

  } else {
    // just scan the log.
    set<sobject_t> did;
    for (list<Log::Entry>::reverse_iterator p = log.log.rbegin();
         p != log.log.rend();
         p++) {
      if (did.count(p->soid)) continue;
      did.insert(p->soid);

      if (p->is_delete()) {
        dout(10) << " deleting " << p->soid
                 << " when " << p->version << dendl;
        t.remove(info.pgid.to_coll(), p->soid);
      } else {
        // keep old(+missing) objects, just for kicks.
      }
    }
  }
}




// ==========================================================================================
// SCRUB


int ReplicatedPG::_scrub(ScrubMap& scrubmap, int& errors, int& fixed)
{
  dout(10) << "_scrub" << dendl;

  coll_t c = info.pgid.to_coll();
  bool repair = state_test(PG_STATE_REPAIR);
  const char *mode = repair ? "repair":"scrub";

  // traverse in reverse order.
  sobject_t head;
  SnapSet snapset;
  unsigned curclone = 0;

  pg_stat_t stat;

  bufferlist last_data;

  for (vector<ScrubMap::object>::reverse_iterator p = scrubmap.objects.rbegin(); 
       p != scrubmap.objects.rend(); 
       p++) {
    const sobject_t& soid = p->poid;
    stat.num_objects++;

    // basic checks.
    if (p->attrs.count(OI_ATTR) == 0) {
      dout(0) << mode << " no '" << OI_ATTR << "' attr on " << soid << dendl;
      errors++;
      continue;
    }
    bufferlist bv;
    bv.push_back(p->attrs[OI_ATTR]);
    object_info_t oi(bv);

    dout(20) << mode << "  " << soid << " " << oi << dendl;

    stat.num_bytes += p->size;
    stat.num_kb += SHIFT_ROUND_UP(p->size, 10);

    //bufferlist data;
    //osd->store->read(c, poid, 0, 0, data);
    //assert(data.length() == p->size);

    // new head?
    if (soid.snap == CEPH_NOSNAP) {
      // it's a head.
      if (head != sobject_t()) {
	derr(0) << " missing clone(s) for " << head << dendl;
	assert(head == sobject_t());  // we had better be done
	errors++;
      }

      snapset = oi.snapset;
      if (!snapset.head_exists)
	assert(p->size == 0); // make sure object is 0-sized.

      // what will be next?
      if (snapset.clones.empty())
	head = sobject_t();  // no clones.
      else
	curclone = snapset.clones.size()-1;

      // subtract off any clone overlap
      for (map<snapid_t,interval_set<__u64> >::iterator q = snapset.clone_overlap.begin();
	   q != snapset.clone_overlap.end();
	   q++) {
	for (map<__u64,__u64>::iterator r = q->second.m.begin();
	     r != q->second.m.end();
	     r++) {
	  stat.num_bytes -= r->second;
	  stat.num_kb -= SHIFT_ROUND_UP(r->first+r->second, 10) - (r->first >> 10);
	}	  
      }

    } else if (soid.snap) {
      // it's a clone
      assert(head != sobject_t());

      stat.num_object_clones++;
      
      assert(soid.snap == snapset.clones[curclone]);

      assert(p->size == snapset.clone_size[curclone]);

      // verify overlap?
      // ...

      // what's next?
      curclone++;
      if (curclone == snapset.clones.size())
	head = sobject_t();

    } else {
      // it's unversioned.
    }
  }  
  
  dout(10) << mode << " got "
	   << stat.num_objects << "/" << info.stats.num_objects << " objects, "
	   << stat.num_object_clones << "/" << info.stats.num_object_clones << " clones, "
	   << stat.num_bytes << "/" << info.stats.num_bytes << " bytes, "
	   << stat.num_kb << "/" << info.stats.num_kb << " kb."
	   << dendl;

  stringstream ss;
  if (stat.num_objects != info.stats.num_objects ||
      stat.num_object_clones != info.stats.num_object_clones ||
      stat.num_bytes != info.stats.num_bytes ||
      stat.num_kb != info.stats.num_kb) {
    ss << info.pgid << " " << mode << " stat mismatch, got "
       << stat.num_objects << "/" << info.stats.num_objects << " objects, "
       << stat.num_object_clones << "/" << info.stats.num_object_clones << " clones, "
       << stat.num_bytes << "/" << info.stats.num_bytes << " bytes, "
       << stat.num_kb << "/" << info.stats.num_kb << " kb.";
    osd->get_logclient()->log(LOG_ERROR, ss);
    errors++;

    if (repair) {
      fixed++;
      info.stats.num_objects = stat.num_objects;
      info.stats.num_object_clones = stat.num_object_clones;
      info.stats.num_bytes = stat.num_bytes;
      info.stats.num_kb = stat.num_kb;
      update_stats();
    }
  }

  dout(10) << "_scrub (" << mode << ") finish" << dendl;
  return errors;
}
