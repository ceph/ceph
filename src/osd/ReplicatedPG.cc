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
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGTrim.h"

#include "messages/MOSDPing.h"

#include "config.h"

#define DOUT_SUBSYS osd
#define DOUT_PREFIX_ARGS this, osd->whoami, osd->osdmap
#undef dout_prefix
#define dout_prefix _prefix(this, osd->whoami, osd->osdmap)
static ostream& _prefix(PG *pg, int whoami, OSDMap *osdmap) {
  return *_dout << dbeginl << "osd" << whoami
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
    start_recovery_op(soid);
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
			      readop.extent.offset,
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

  snapid_t snapid = op->get_snapid();

  for (vector<OSDOp>::iterator p = op->ops.begin(); p != op->ops.end(); p++) {
    switch (p->op.op) {
    case CEPH_OSD_OP_PGLS:
      if (op->get_pg() != info.pgid) {
        dout(10) << " pgls pg=" << op->get_pg() << " != " << info.pgid << dendl;
	result = 0; // hmm?
      } else {
        dout(10) << " pgls pg=" << op->get_pg() << dendl;
	// read into a buffer
        PGLSResponse response;
        response.handle = (collection_list_handle_t)(__u64)(p->op.pgls.cookie);
        vector<sobject_t> sentries;
	result = osd->store->collection_list_partial(coll_t::build_pg_coll(info.pgid), snapid,
						     sentries, p->op.pgls.count,
	                                             &response.handle);
	if (result == 0) {
          vector<sobject_t>::iterator iter;
          for (iter = sentries.begin(); iter != sentries.end(); ++iter) {
	    // skip snapdir objects
	    if (iter->snap == CEPH_SNAPDIR)
	      continue;

	    if (snapid != CEPH_NOSNAP) {
	      // skip items not defined for this snapshot
	      if (iter->snap == CEPH_NOSNAP) {
		bufferlist bl;
		osd->store->getattr(coll_t::build_pg_coll(info.pgid), *iter, SS_ATTR, bl);
		SnapSet snapset(bl);
		if (snapid <= snapset.seq)
		  continue;
	      } else {
		bufferlist bl;
		osd->store->getattr(coll_t::build_pg_coll(info.pgid), *iter, OI_ATTR, bl);
		object_info_t oi(bl);
		bool exists = false;
		for (vector<snapid_t>::iterator i = oi.snaps.begin(); i != oi.snaps.end(); ++i)
		  if (*i == snapid) {
		    exists = true;
		    break;
		  }
		dout(10) << *iter << " has " << oi.snaps << " .. exists=" << exists << dendl;
		if (!exists)
		  continue;
	      }
	    }
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
  //if the message came from an OSD, it needs to go back to originator,
  //but if the connection ISN't an OSD that connection is the originator
  if (op->get_connection()->get_peer_type() != CEPH_ENTITY_TYPE_OSD)
    osd->messenger->send_message(reply, op->get_connection());
  else
    osd->messenger->send_message(reply, op->get_orig_source_inst());
  op->put();
}

void ReplicatedPG::calc_trim_to()
{
  if (!is_degraded() &&
      (is_clean() ||
       log.head.version - log.tail.version > info.stats.num_objects)) {
    if (min_last_complete_ondisk != eversion_t() &&
	min_last_complete_ondisk != pg_trim_to) {
      dout(10) << "calc_trim_to " << pg_trim_to << " -> " << min_last_complete_ondisk << dendl;
      pg_trim_to = min_last_complete_ondisk;
      assert(pg_trim_to <= log.head);
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

  dout(10) << "do_op " << *op << dendl;

  entity_inst_t client = op->get_source_inst();

  ObjectContext *obc;
  bool can_create = op->may_write();
  int r = find_object_context(op->get_oid(), op->get_snapid(), &obc, can_create);
  if (r) {
    osd->reply_op_error(op, r);
    return;
  }    
  
  bool ok;
  dout(10) << "do_op mode is " << mode << dendl;
  assert(!mode.wake);   // we should never have woken waiters here.
  if (op->may_read() && op->may_write())
    ok = mode.try_rmw(client);
  else if (op->may_write())
    ok = mode.try_write(client);
  else if (op->may_read())
    ok = mode.try_read(client);
  else
    assert(0);
  if (!ok) {
    dout(10) << "do_op waiting on mode " << mode << dendl;
    mode.waiting.push_back(op);
    return;
  }

  if (!op->may_write() && !obc->obs.exists) {
    osd->reply_op_error(op, -ENOENT);
    put_object_context(obc);
    return;
  }

  dout(10) << "do_op mode now " << mode << dendl;

  const sobject_t& soid = obc->obs.oi.soid;
  OpContext *ctx = new OpContext(op, op->get_reqid(), op->ops, op->get_data(),
				 &obc->obs, this);
  bool noop = false;

  if (op->may_write()) {
    // snap
    if (op->get_snap_seq()) {
      // client specified snapc
      ctx->snapc.seq = op->get_snap_seq();
      ctx->snapc.snaps = op->get_snaps();
    } else {
      // use pool's snapc
      ctx->snapc = pool->snapc;
    }
    if ((op->get_flags() & CEPH_OSD_FLAG_ORDERSNAP) &&
	ctx->snapc.seq < obc->obs.ssc->snapset.seq) {
      dout(10) << " ORDERSNAP flag set and snapc seq " << ctx->snapc.seq
	       << " < snapset seq " << obc->obs.ssc->snapset.seq
	       << " on " << soid << dendl;
      delete ctx;
      put_object_context(obc);
      osd->reply_op_error(op, -EOLDSNAPC);
      return;
    }

    if (is_dup(ctx->reqid)) {
      dout(3) << "do_op dup " << ctx->reqid << ", doing WRNOOP" << dendl;
      noop = true;
    }

    // version
    ctx->at_version = log.head;
    if (!noop) {
      ctx->at_version.epoch = osd->osdmap->get_epoch();
      ctx->at_version.version++;
      assert(ctx->at_version > info.last_update);
      assert(ctx->at_version > log.head);

      // set version in op, for benefit of client and our eventual reply.  if !noop!
      op->set_version(ctx->at_version);
    }

    ctx->mtime = op->get_mtime();
    
    dout(10) << "do_op " << soid << " " << ctx->ops
	     << " ov " << obc->obs.oi.version << " av " << ctx->at_version 
	     << " snapc " << ctx->snapc
	     << " snapset " << obc->obs.ssc->snapset
	     << dendl;  
  } else {
    dout(10) << "do_op " << soid << " " << ctx->ops
	     << " ov " << obc->obs.oi.version
	     << dendl;  
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

    if (op->may_read()) {
      dout(10) << " taking ondisk_read_lock" << dendl;
      obc->ondisk_read_lock();
    }
    int result = prepare_transaction(ctx);
    if (op->may_read()) {
      dout(10) << " dropping ondisk_read_lock" << dendl;
      obc->ondisk_read_unlock();
    }

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
      //if the message came from an OSD, it needs to go back to originator,
      //but if the connection ISN't an OSD that connection is the originator
      if (op->get_connection()->get_peer_type() != CEPH_ENTITY_TYPE_OSD)
	osd->messenger->send_message(reply, op->get_connection());
      else
	osd->messenger->send_message(reply, op->get_orig_source_inst());
      op->put();
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
  repop->v = ctx->at_version;

  // note: repop now owns ctx AND ctx->op

  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];

    if (peer_missing.count(peer) &&
        peer_missing[peer].is_missing(soid)) {
      // push it before this update. 
      // FIXME, this is probably extra much work (eg if we're about to overwrite)
      obc->ondisk_read_lock();
      push_to_replica(soid, peer);
      start_recovery_op(soid);
      obc->ondisk_read_unlock();
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
      stat_object_temp_rd[soid].hit(now, osd->decayrate);  // hit temp.
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
    coll_t c = coll_t::build_snap_pg_coll(info.pgid, sn);
    vector<sobject_t> ls;
    osd->store->collection_list(c, ls);

    dout(10) << "snap_trimmer collection " << c << " has " << ls.size() << " items" << dendl;

    for (vector<sobject_t>::iterator p = ls.begin(); p != ls.end(); p++) {
      const sobject_t& coid = *p;

      ObjectStore::Transaction *t = new ObjectStore::Transaction;

      // load clone info
      bufferlist bl;
      osd->store->getattr(coll_t::build_pg_coll(info.pgid), coid, OI_ATTR, bl);
      object_info_t coi(bl);

      // get snap set context
      SnapSetContext *ssc = get_snapset_context(coid.oid, false);
      assert(ssc);
      SnapSet& snapset = ssc->snapset;
      vector<snapid_t>& snaps = coi.snaps;

      dout(10) << coid << " snaps " << snaps << " old snapset " << snapset << dendl;
      assert(snapset.seq);

      // trim clone's snaps
      vector<snapid_t> newsnaps;
      for (unsigned i=0; i<snaps.size(); i++)
	if (!pool->info.is_removed_snap(snaps[i]))
	  newsnaps.push_back(snaps[i]);

      if (newsnaps.empty()) {
	// remove clone
	dout(10) << coid << " snaps " << snaps << " -> " << newsnaps << " ... deleting" << dendl;
	t->remove(coll_t::build_pg_coll(info.pgid), coid);
	t->collection_remove(coll_t::build_snap_pg_coll(info.pgid, snaps[0]), coid);
	if (snaps.size() > 1)
	  t->collection_remove(coll_t::build_snap_pg_coll(info.pgid, snaps[snaps.size()-1]), coid);

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
	t->setattr(coll_t::build_pg_coll(info.pgid), coid, OI_ATTR, bl);

	if (snaps[0] != newsnaps[0]) {
	  t->collection_remove(coll_t::build_snap_pg_coll(info.pgid, snaps[0]), coid);
	  t->collection_add(coll_t::build_snap_pg_coll(info.pgid, newsnaps[0]), coll_t::build_pg_coll(info.pgid), coid);
	}
	if (snaps.size() > 1 && snaps[snaps.size()-1] != newsnaps[newsnaps.size()-1]) {
	  t->collection_remove(coll_t::build_snap_pg_coll(info.pgid, snaps[snaps.size()-1]), coid);
	  if (newsnaps.size() > 1)
	    t->collection_add(coll_t::build_snap_pg_coll(info.pgid, newsnaps[newsnaps.size()-1]), coll_t::build_pg_coll(info.pgid), coid);
	}	      
      }

      // save head snapset
      dout(10) << coid << " new snapset " << snapset << dendl;

      sobject_t snapoid(coid.oid, snapset.head_exists ? CEPH_NOSNAP:CEPH_SNAPDIR);
      if (snapset.clones.empty() && !snapset.head_exists) {
	dout(10) << coid << " removing " << snapoid << dendl;
	t->remove(coll_t::build_pg_coll(info.pgid), snapoid);
      } else {
	bl.clear();
	::encode(snapset, bl);
	t->setattr(coll_t::build_pg_coll(info.pgid), snapoid, SS_ATTR, bl);
      }

      int tr = osd->store->queue_transaction(&osr, t);
      assert(tr == 0);

      // give other threads a chance at this pg
      unlock();
      lock();
    }

    // remove snap collection
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    dout(10) << "removing snap " << sn << " collection " << c << dendl;
    snap_collections.erase(sn);
    write_info(*t);
    t->remove_collection(c);
    int tr = osd->store->queue_transaction(&osr, t);
    assert(tr == 0);
 
    info.snap_trimq.erase(sn);
  }  

  // done
  dout(10) << "snap_trimmer done" << dendl;

  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  write_info(*t);
  int tr = osd->store->queue_transaction(&osr, t);
  assert(tr == 0);
  unlock();
  return true;
}


// ========================================================================
// low level osd ops

int ReplicatedPG::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops,
			     bufferlist& odata)
{
  int result = 0;
  SnapSetContext *ssc = ctx->obs->ssc;
  object_info_t& oi = ctx->obs->oi;

  const sobject_t& soid = oi.soid;

  ObjectStore::Transaction& t = ctx->op_t;

  dout(10) << "do_osd_op " << soid << " " << ops << dendl;

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); p++) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op; 

    dout(10) << "do_osd_op  " << osd_op << dendl;

    // modify?
    bool is_modify;
    string cname, mname;
    bufferlist::iterator bp = osd_op.data.begin();
    switch (op.op) {
    case CEPH_OSD_OP_CALL:
      bp.copy(op.cls.class_len, cname);
      bp.copy(op.cls.method_len, mname);
      is_modify = osd->class_handler->get_method_flags(cname, mname) & CLS_METHOD_WR;
      break;

    default:
      is_modify = (op.op & CEPH_OSD_OP_MODE_WR);
      break;
    }

    // make writeable (i.e., clone if necessary)
    if (is_modify)
      make_writeable(ctx);

    // munge ZERO -> TRUNCATE?  (don't munge to DELETE or we risk hosing attributes)
    if (op.op == CEPH_OSD_OP_ZERO &&
	ctx->obs->exists &&
	op.extent.offset + op.extent.length >= oi.size) {
      dout(10) << " munging ZERO " << op.extent.offset << "~" << op.extent.length
	       << " -> TRUNCATE " << op.extent.offset << " (old size is " << oi.size << ")" << dendl;
      op.op = CEPH_OSD_OP_TRUNCATE;
    }

    switch (op.op) {
      
      // --- READS ---

    case CEPH_OSD_OP_READ:
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->read(coll_t::build_pg_coll(info.pgid), soid, op.extent.offset, op.extent.length, bl);
	if (odata.length() == 0)
	  ctx->data_off = op.extent.offset;
	odata.claim(bl);
	if (r >= 0) 
	  op.extent.length = r;
	else {
	  result = r;
	  op.extent.length = 0;
	}
	info.stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	info.stats.num_rd++;
	dout(0) << " read got " << r << " / " << op.extent.length << " bytes from obj " << soid << dendl;

	__u32 seq = oi.truncate_seq;
	// are we beyond truncate_size?
	if ( (seq < op.extent.truncate_seq) &&
	     (op.extent.offset + op.extent.length > op.extent.truncate_size) ) {

	  // truncated portion of the read
	  unsigned from = MAX(op.extent.offset, op.extent.truncate_size);  // also end of data
	  unsigned to = op.extent.offset + op.extent.length;
	  unsigned trim = to-from;

	  op.extent.length = op.extent.length - trim;

	  bufferlist keep;

	  // keep first part of odata; trim at truncation point
	  dout(0) << " obj " << soid << " seq " << seq
	           << ": trimming overlap " << from << "~" << trim << dendl;
	  keep.substr_of(odata, 0, odata.length() - trim);
          odata.claim(keep);
	}
      }
      break;

    case CEPH_OSD_OP_CALL:
      {
	bufferlist indata;
	bp.copy(op.cls.indata_len, indata);
	
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
	    op.extent.length = outdata.length();
	    odata.claim_append(outdata);
	  }
	}
      }
      break;

    case CEPH_OSD_OP_STAT:
      {
	struct stat st;
	memset(&st, 0, sizeof(st));
	result = osd->store->stat(coll_t::build_pg_coll(info.pgid), soid, &st);
	if (result >= 0) {
	  __u64 size = st.st_size;
	  ::encode(size, odata);
	  ::encode(oi.mtime, odata);
	}
	info.stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_GETXATTR:
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	int r = osd->store->getattr(coll_t::build_pg_coll(info.pgid), soid, name.c_str(), odata);
	if (r >= 0) {
	  op.xattr.value_len = r;
	  result = 0;
	} else
	  result = r;
	info.stats.num_rd++;
      }
      break;

   case CEPH_OSD_OP_GETXATTRS:
      {
	map<string,bufferptr> attrset;
        result = osd->store->getattrs(coll_t::build_pg_coll(info.pgid), soid, attrset, true);
        map<string, bufferptr>::iterator iter;
        map<string, bufferlist> newattrs;
        for (iter = attrset.begin(); iter != attrset.end(); ++iter) {
           bufferlist bl;
           bl.append(iter->second);
           newattrs[iter->first] = bl;
        }
        
        bufferlist bl;
        ::encode(newattrs, bl);
        odata.claim_append(bl);
      }
      break;


      // --- WRITES ---

      // -- object data --

    case CEPH_OSD_OP_WRITE:
      { // write
        __u32 seq = oi.truncate_seq;
        if (seq && (seq > op.extent.truncate_seq) &&
            (op.extent.offset + op.extent.length > oi.size)) {
	  // old write, arrived after trimtrunc
	  op.extent.length = (op.extent.offset > oi.size ? 0 : oi.size - op.extent.offset);
	  dout(10) << " old truncate_seq " << op.extent.truncate_seq << " < current " << seq
		   << ", adjusting write length to " << op.extent.length << dendl;
        }
	if (op.extent.truncate_seq > seq) {
	  // write arrives before trimtrunc
	  dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
		   << ", truncating to " << op.extent.truncate_size << dendl;
	  t.truncate(coll_t::build_pg_coll(info.pgid), soid, op.extent.truncate_size);
	  oi.truncate_seq = op.extent.truncate_seq;
	  oi.truncate_size = op.extent.truncate_size;
	}
        if (op.extent.length) {
	  bufferlist nbl;
	  bp.copy(op.extent.length, nbl);
	  t.write(coll_t::build_pg_coll(info.pgid), soid, op.extent.offset, op.extent.length, nbl);
        } else {
          t.touch(coll_t::build_pg_coll(info.pgid), soid);
        }
	if (ssc->snapset.clones.size()) {
	  snapid_t newest = *ssc->snapset.clones.rbegin();
	  interval_set<__u64> ch;
	  if (op.extent.length)
	    ch.insert(op.extent.offset, op.extent.length);
	  ch.intersection_of(ssc->snapset.clone_overlap[newest]);
	  ssc->snapset.clone_overlap[newest].subtract(ch);
	  add_interval_usage(ch, info.stats);
	}
	if (op.extent.length && (op.extent.offset + op.extent.length > oi.size)) {
	  __u64 new_size = op.extent.offset + op.extent.length;
	  info.stats.num_bytes += new_size - oi.size;
	  info.stats.num_kb += SHIFT_ROUND_UP(new_size, 10) - SHIFT_ROUND_UP(oi.size, 10);
	  oi.size = new_size;
	}
	info.stats.num_wr++;
	info.stats.num_wr_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ssc->snapset.head_exists = true;
      }
      break;
      
    case CEPH_OSD_OP_WRITEFULL:
      { // write full object
	bufferlist nbl;
	bp.copy(op.extent.length, nbl);
	if (ctx->obs->exists)
	  t.truncate(coll_t::build_pg_coll(info.pgid), soid, 0);
	t.write(coll_t::build_pg_coll(info.pgid), soid, op.extent.offset, op.extent.length, nbl);
	if (ssc->snapset.clones.size()) {
	  snapid_t newest = *ssc->snapset.clones.rbegin();
	  ssc->snapset.clone_overlap.erase(newest);
	  oi.size = 0;
	}
	if (op.extent.length != oi.size) {
	  info.stats.num_bytes -= oi.size;
	  info.stats.num_kb -= SHIFT_ROUND_UP(oi.size, 10);
	  info.stats.num_bytes += op.extent.length;
	  info.stats.num_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	  oi.size = op.extent.length;
	}
	info.stats.num_wr++;
	info.stats.num_wr_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ssc->snapset.head_exists = true;
      }
      break;

    case CEPH_OSD_OP_ZERO:
      { // zero
	assert(op.extent.length);
	if (!ctx->obs->exists)
	  t.touch(coll_t::build_pg_coll(info.pgid), soid);
	t.zero(coll_t::build_pg_coll(info.pgid), soid, op.extent.offset, op.extent.length);
	if (ssc->snapset.clones.size()) {
	  snapid_t newest = *ssc->snapset.clones.rbegin();
	  interval_set<__u64> ch;
	  ch.insert(op.extent.offset, op.extent.length);
	  ch.intersection_of(ssc->snapset.clone_overlap[newest]);
	  ssc->snapset.clone_overlap[newest].subtract(ch);
	  add_interval_usage(ch, info.stats);
	}
	info.stats.num_wr++;
	ssc->snapset.head_exists = true;
      }
      break;
    case CEPH_OSD_OP_CREATE:
      { // zero
        int flags = le32_to_cpu(op.flags);
	if (ctx->obs->exists && (flags & CEPH_OSD_OP_FLAG_EXCL))
          result = -EEXIST; /* this is an exclusive create */
        else {
          t.touch(coll_t::build_pg_coll(info.pgid), soid);
          ssc->snapset.head_exists = true;
        }
      }
      break;
      
    case CEPH_OSD_OP_TRIMTRUNC:
      op.extent.offset = op.extent.truncate_size;
      // falling through

    case CEPH_OSD_OP_TRUNCATE:
      { // truncate
	if (!ctx->obs->exists) {
	  dout(10) << " object dne, truncate is a no-op" << dendl;
	  break;
	}

	if (op.extent.truncate_seq) {
	  assert(op.extent.offset == op.extent.truncate_size);
	  if (op.extent.truncate_seq <= oi.truncate_seq) {
	    dout(10) << " truncate seq " << op.extent.truncate_seq << " <= current " << oi.truncate_seq
		     << ", no-op" << dendl;
	    break; // old
	  }
	  dout(10) << " truncate seq " << op.extent.truncate_seq << " > current " << oi.truncate_seq
		   << ", truncating" << dendl;
	  oi.truncate_seq = op.extent.truncate_seq;
	  oi.truncate_size = op.extent.truncate_size;
	}

	t.truncate(coll_t::build_pg_coll(info.pgid), soid, op.extent.offset);
	if (ssc->snapset.clones.size()) {
	  snapid_t newest = *ssc->snapset.clones.rbegin();
	  interval_set<__u64> trim;
	  if (oi.size > op.extent.offset) {
	    trim.insert(op.extent.offset, oi.size-op.extent.offset);
	    trim.intersection_of(ssc->snapset.clone_overlap[newest]);
	    add_interval_usage(trim, info.stats);
	  }
	  interval_set<__u64> keep;
	  if (op.extent.offset)
	    keep.insert(0, op.extent.offset);
	  ssc->snapset.clone_overlap[newest].intersection_of(keep);
	}
	if (op.extent.offset != oi.size) {
	  info.stats.num_bytes -= oi.size;
	  info.stats.num_kb -= SHIFT_ROUND_UP(oi.size, 10);
	  info.stats.num_bytes += op.extent.offset;
	  info.stats.num_kb += SHIFT_ROUND_UP(op.extent.offset, 10);
	  oi.size = op.extent.offset;
	}
	info.stats.num_wr++;
	// do no set head_exists, or we will break above DELETE -> TRUNCATE munging.
      }
      break;
    
    case CEPH_OSD_OP_DELETE:
      { // delete
	if (ctx->obs->exists)
	  t.remove(coll_t::build_pg_coll(info.pgid), soid);  // no clones, delete!
	if (ssc->snapset.clones.size()) {
	  snapid_t newest = *ssc->snapset.clones.rbegin();
	  add_interval_usage(ssc->snapset.clone_overlap[newest], info.stats);
	  ssc->snapset.clone_overlap.erase(newest);  // ok, redundant.
	}
	if (ctx->obs->exists) {
	  info.stats.num_objects--;
	  info.stats.num_bytes -= oi.size;
	  info.stats.num_kb -= SHIFT_ROUND_UP(oi.size, 10);
	  oi.size = 0;
	  ssc->snapset.head_exists = false;
	  ctx->obs->exists = false;
	}      
	info.stats.num_wr++;
      }
      break;


      // -- object attrs --
      
    case CEPH_OSD_OP_SETXATTR:
      {
	if (!ctx->obs->exists)
	  t.touch(coll_t::build_pg_coll(info.pgid), soid);
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	bufferlist bl;
	bp.copy(op.xattr.value_len, bl);
	if (!ctx->obs->exists)  // create object if it doesn't yet exist.
	  t.touch(coll_t::build_pg_coll(info.pgid), soid);
	t.setattr(coll_t::build_pg_coll(info.pgid), soid, name, bl);
	ssc->snapset.head_exists = true;
 	info.stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_RMXATTR:
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	t.rmattr(coll_t::build_pg_coll(info.pgid), soid, name);
 	info.stats.num_wr++;
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
	newop.op.extent.offset = oi.size;
	newop.op.extent.length = op.extent.length;
        newop.data = osd_op.data;
	do_osd_ops(ctx, nops, odata);
      }
      break;

    case CEPH_OSD_OP_STARTSYNC:
      t.start_sync();
      break;


      // -- trivial map --
    case CEPH_OSD_OP_TMAPGET:
      {
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_READ;
	newop.op.extent.offset = 0;
	newop.op.extent.length = 0;
	do_osd_ops(ctx, nops, odata);
      }
      break;

    case CEPH_OSD_OP_TMAPPUT:
      {
	//_dout_lock.Lock();
	//osd_op.data.hexdump(*_dout);
	//_dout_lock.Unlock();

	// verify
	if (0) {
	  bufferlist header;
	  map<string, bufferlist> m;
	  ::decode(header, bp);
	  ::decode(m, bp);
	  assert(bp.end());
	}

	// write it
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITEFULL;
	newop.op.extent.offset = 0;
	newop.op.extent.length = osd_op.data.length();
	newop.data = osd_op.data;
	bufferlist r;
	do_osd_ops(ctx, nops, r);
      }
      break;

    case CEPH_OSD_OP_TMAPUP:
      if (bp.end()) {
	dout(10) << "tmapup is a no-op" << dendl;
      } else {
	// read the whole object
	bufferlist ibl;
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_READ;
	newop.op.extent.offset = 0;
	newop.op.extent.length = 0;
	do_osd_ops(ctx, nops, ibl);
	dout(10) << "tmapup read " << ibl.length() << dendl;

	dout(30) << " starting is \n";
	ibl.hexdump(*_dout);
	*_dout << dendl;

	bufferlist::iterator ip = ibl.begin();
	bufferlist obl;
	bool changed = false;

	dout(30) << "the update command is: \n";
	osd_op.data.hexdump(*_dout);
	*_dout << dendl;

	if (0) {
	  // parse
	  bufferlist header;
	  map<string, bufferlist> m;
	  //ibl.hexdump(*_dout);
	  if (ibl.length()) {
	    ::decode(header, ip);
	    ::decode(m, ip);
	    //dout(0) << "m is " << m.size() << " " << m << dendl;
	    assert(ip.end());
	  }
	  
	  // do the update(s)
	  while (!bp.end()) {
	    __u8 op;
	    string key;
	    ::decode(op, bp);
	    
	    switch (op) {
	    case CEPH_OSD_TMAP_SET: // insert key
	      {
		::decode(key, bp);
		bufferlist data;
		::decode(data, bp);
		m[key] = data;
		changed = true;
	      }
	      break;
	      
	    case CEPH_OSD_TMAP_RM: // remove key
	      ::decode(key, bp);
	      if (m.count(key)) {
		m.erase(key);
		changed = true;
	      }
	      break;
	      
	    case CEPH_OSD_TMAP_HDR: // update header
	      {
		::decode(header, bp);
		changed = true;
	      }
	      break;
	      
	    default:
	      return -EINVAL;
	    }
	  }

	  // reencode
	  ::encode(header, obl);
	  ::encode(m, obl);
	} else {
	  // header
	  bufferlist header;
	  __u32 nkeys = 0;
	  if (ibl.length()) {
	    ::decode(header, ip);
	    ::decode(nkeys, ip);
	  }
	  dout(10) << "tmapup header " << header.length() << dendl;

	  if (!bp.end() && *bp == CEPH_OSD_TMAP_HDR) {
	    ++bp;
	    ::decode(header, bp);
	    changed = true;
	    dout(10) << "tmapup new header " << header.length() << dendl;
	  }
	  
	  ::encode(header, obl);

	  dout(20) << "tmapup initial nkeys " << nkeys << dendl;

	  // update keys
	  bufferlist newkeydata;
	  string nextkey;
	  bufferlist nextval;
	  bool have_next = false;
	  if (!ip.end()) {
	    have_next = true;
	    ::decode(nextkey, ip);
	    ::decode(nextval, ip);
	  }
	  while (!bp.end()) {
	    __u8 op;
	    string key;
	    ::decode(op, bp);
	    ::decode(key, bp);
	    
	    dout(10) << "tmapup op " << (int)op << " key " << key << dendl;

	    // skip existing intervening keys
	    bool stop = false;
	    while (have_next && !stop) {
	      dout(20) << "  (have_next=" << have_next << " nextkey=" << nextkey << ")" << dendl;
	      if (nextkey > key)
		break;
	      if (nextkey < key) {
		// copy untouched.
		::encode(nextkey, newkeydata);
		::encode(nextval, newkeydata);
		dout(20) << "  keep " << nextkey << " " << nextval.length() << dendl;
	      } else {
		// don't copy; discard old value.  and stop.
		dout(20) << "  drop " << nextkey << " " << nextval.length() << dendl;
		stop = true;
		nkeys--;
	      }	      
	      if (!ip.end()) {
		::decode(nextkey, ip);
		::decode(nextval, ip);
	      } else
		have_next = false;
	    }
	    
	    if (op == CEPH_OSD_TMAP_SET) {
	      bufferlist val;
	      ::decode(val, bp);
	      ::encode(key, newkeydata);
	      ::encode(val, newkeydata);
	      dout(20) << "   set " << key << " " << val.length() << dendl;
	      nkeys++;
	    } else if (op == CEPH_OSD_TMAP_RM) {
	      // do nothing.
	    }
	    changed = true;
	  }

	  // copy remaining
	  if (have_next) {
	    ::encode(nextkey, newkeydata);
	    ::encode(nextval, newkeydata);
	    dout(20) << "  keep " << nextkey << " " << nextval.length() << dendl;
	  }
	  if (!ip.end()) {
	    bufferlist rest;
	    rest.substr_of(ibl, ip.get_off(), ibl.length() - ip.get_off());
	    dout(20) << "  keep trailing " << rest.length()
		     << " at " << newkeydata.length() << dendl;
	    newkeydata.claim_append(rest);
	  }

	  // encode final key count + key data
	  dout(20) << "tmapup final nkeys " << nkeys << dendl;
	  ::encode(nkeys, obl);
	  obl.claim_append(newkeydata);
	}

	if (0) {
	  dout(30) << " final is \n";
	  obl.hexdump(*_dout);
	  *_dout << dendl;

	  // sanity check
	  bufferlist::iterator tp = obl.begin();
	  bufferlist h;
	  ::decode(h, tp);
	  map<string,bufferlist> d;
	  ::decode(d, tp);
	  assert(tp.end());
	  dout(0) << " **** debug sanity check, looks ok ****" << dendl;
	}

	// write it out
	dout(20) << "tmapput write " << obl.length() << dendl;
	newop.op.op = CEPH_OSD_OP_WRITEFULL;
	newop.op.extent.offset = 0;
	newop.op.extent.length = obl.length();
        newop.data = obl;
	do_osd_ops(ctx, nops, odata);
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
	!ctx->obs->exists && ssc->snapset.head_exists) {
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

  t.clone(coll_t::build_pg_coll(info.pgid), head, coid);
  t.setattr(coll_t::build_pg_coll(info.pgid), coid, OI_ATTR, bv);
}

void ReplicatedPG::make_writeable(OpContext *ctx)
{
  SnapSetContext *ssc = ctx->obs->ssc;
  object_info_t& oi = ctx->obs->oi;
  const sobject_t& soid = oi.soid;
  SnapContext& snapc = ctx->snapc;
  ObjectStore::Transaction& t = ctx->op_t;

  // clone?
  assert(soid.snap == CEPH_NOSNAP);
  dout(20) << "make_writeable " << soid << " snapset=" << ssc->snapset
	   << "  snapc=" << snapc << dendl;;
  
  // use newer snapc?
  if (ssc->snapset.seq > snapc.seq) {
    snapc.seq = ssc->snapset.seq;
    snapc.snaps = ssc->snapset.snaps;
    dout(10) << " using newer snapc " << snapc << dendl;
  }
  
  if (ssc->snapset.head_exists &&           // head exists
      snapc.snaps.size() &&            // there are snaps
      snapc.snaps[0] > ssc->snapset.seq) {  // existing object is old
    // clone
    sobject_t coid = soid;
    coid.snap = snapc.seq;
    
    unsigned l;
    for (l=1; l<snapc.snaps.size() && snapc.snaps[l] > ssc->snapset.seq; l++) ;
    
    vector<snapid_t> snaps(l);
    for (unsigned i=0; i<l; i++)
      snaps[i] = snapc.snaps[i];
    
    // prepare clone
    ctx->clone_obc = new ObjectContext(coid);
    ctx->clone_obc->obs.oi.version = ctx->at_version;
    ctx->clone_obc->obs.oi.prior_version = oi.version;
    ctx->clone_obc->obs.oi.last_reqid = oi.last_reqid;
    ctx->clone_obc->obs.oi.mtime = oi.mtime;
    ctx->clone_obc->obs.oi.snaps = snaps;
    ctx->clone_obc->obs.exists = true;
    ctx->clone_obc->get();

    if (is_primary())
      register_object_context(ctx->clone_obc);
    
    _make_clone(t, soid, coid, &ctx->clone_obc->obs.oi);
    
    // add to snap bound collections
    coll_t fc = make_snap_collection(t, snaps[0]);
    t.collection_add(fc, coll_t::build_pg_coll(info.pgid), coid);
    if (snaps.size() > 1) {
      coll_t lc = make_snap_collection(t, snaps[snaps.size()-1]);
      t.collection_add(lc, coll_t::build_pg_coll(info.pgid), coid);
    }
    
    info.stats.num_objects++;
    info.stats.num_object_clones++;
    ssc->snapset.clones.push_back(coid.snap);
    ssc->snapset.clone_size[coid.snap] = ctx->obs->oi.size;
    if (ctx->obs->oi.size)
      ssc->snapset.clone_overlap[coid.snap].insert(0, ctx->obs->oi.size);
    
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
  ssc->snapset.seq = snapc.seq;
  ssc->snapset.snaps = snapc.snaps;
  dout(20) << "make_writeable " << soid << " done, snapset=" << ssc->snapset << dendl;
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

  bool head_existed = ctx->obs->exists;

  // prepare the actual mutation
  int result = do_osd_ops(ctx, ctx->ops, ctx->outdata);

  if (result < 0 || ctx->op_t.empty())
    return result;  // error, or read op.

  // finish and log the op.
  poi->version = ctx->at_version;
  
  bufferlist bss;
  ::encode(ctx->obs->ssc->snapset, bss);
  assert(ctx->obs->exists == ctx->obs->ssc->snapset.head_exists);

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
    ctx->op_t.setattr(coll_t::build_pg_coll(info.pgid), soid, OI_ATTR, bv);

    dout(10) << " final snapset " << ctx->obs->ssc->snapset
	     << " in " << soid << dendl;
    ctx->op_t.setattr(coll_t::build_pg_coll(info.pgid), soid, SS_ATTR, bss);   
    if (!head_existed) {
      // if we logically recreated the head, remove old _snapdir object
      sobject_t snapoid(soid.oid, CEPH_SNAPDIR);
      ctx->op_t.remove(coll_t::build_pg_coll(info.pgid), snapoid);
      dout(10) << " removing old " << snapoid << dendl;
    }
  } else if (ctx->obs->ssc->snapset.clones.size()) {
    // save snapset on _snap
    sobject_t snapoid(soid.oid, CEPH_SNAPDIR);
    dout(10) << " final snapset " << ctx->obs->ssc->snapset
	     << " in " << snapoid << dendl;
    ctx->op_t.touch(coll_t::build_pg_coll(info.pgid), snapoid);
    ctx->op_t.setattr(coll_t::build_pg_coll(info.pgid), snapoid, SS_ATTR, bss);
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

class C_OSD_OpApplied : public Context {
public:
  ReplicatedPG *pg;
  ReplicatedPG::RepGather *repop;

  C_OSD_OpApplied(ReplicatedPG *p, ReplicatedPG::RepGather *rg) :
    pg(p), repop(rg) {
    repop->get();
    pg->get();    // we're copying the pointer
  }
  void finish(int r) {
    pg->op_applied(repop);
    pg->put();
  }
};

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
    pg->op_commit(repop);
    pg->put();
  }
};

void ReplicatedPG::apply_repop(RepGather *repop)
{
  dout(10) << "apply_repop  applying update on " << *repop << dendl;
  assert(!repop->applying);
  assert(!repop->applied);

  repop->applying = true;

  repop->tls.push_back(&repop->ctx->op_t);
  repop->tls.push_back(&repop->ctx->local_t);

  repop->obc->ondisk_write_lock();

  Context *oncommit = new C_OSD_OpCommit(this, repop);
  Context *onapplied = new C_OSD_OpApplied(this, repop);
  Context *onapplied_sync = new C_OSD_OndiskWriteUnlock(repop->obc);
  int r = osd->store->queue_transactions(&osr, repop->tls, onapplied, oncommit, onapplied_sync);
  if (r) {
    dout(-10) << "apply_repop  queue_transactions returned " << r << " on " << *repop << dendl;
    assert(0);
  }
}

void ReplicatedPG::op_applied(RepGather *repop)
{
  lock();
  dout(10) << "op_applied " << *repop << dendl;

  // discard my reference to the buffer
  repop->ctx->op->get_data().clear();
  
  repop->applying = false;
  repop->applied = true;

  // (logical) local ack.
  int whoami = osd->get_nodeid();

  if (!repop->aborted) {
    assert(repop->waitfor_ack.count(whoami));
    repop->waitfor_ack.erase(whoami);
  }
  
  if (repop->ctx->clone_obc) {
    put_object_context(repop->ctx->clone_obc);
    repop->ctx->clone_obc = 0;
  }

  dout(10) << "op_applied mode was " << mode << dendl;
  mode.write_applied();
  dout(10) << "op_applied mode now " << mode << " (finish_write)" << dendl;

  put_object_context(repop->obc);
  repop->obc = 0;

  update_stats();

  // any completion stuff to do here?
  const sobject_t& soid = repop->ctx->obs->oi.soid;
  OSDOp& first = repop->ctx->ops[0];

  switch (first.op.op) { 
#if 0
  case CEPH_OSD_OP_UNBALANCEREADS:
    dout(-10) << "op_applied  completed unbalance-reads on " << oid << dendl;
    unbalancing_reads.erase(oid);
    if (waiting_for_unbalanced_reads.count(oid)) {
      osd->take_waiters(waiting_for_unbalanced_reads[oid]);
      waiting_for_unbalanced_reads.erase(oid);
    }
    break;

  case CEPH_OSD_OP_BALANCEREADS:
    dout(-10) << "op_applied  completed balance-reads on " << oid << dendl;
    /*
    if (waiting_for_balanced_reads.count(oid)) {
      osd->take_waiters(waiting_for_balanced_reads[oid]);
      waiting_for_balanced_reads.erase(oid);
    }
    */
    break;
#endif

  case CEPH_OSD_OP_WRUNLOCK:
    dout(-10) << "op_applied  completed wrunlock on " << soid << dendl;
    if (waiting_for_wr_unlock.count(soid)) {
      osd->take_waiters(waiting_for_wr_unlock[soid]);
      waiting_for_wr_unlock.erase(soid);
    }
    break;
  }   

  if (!repop->aborted)
    eval_repop(repop);

  repop->put();
  unlock();
}

void ReplicatedPG::op_commit(RepGather *repop)
{
  lock();

  if (repop->aborted) {
    dout(10) << "op_commit " << *repop << " -- aborted" << dendl;
  } else if (repop->waitfor_disk.count(osd->get_nodeid()) == 0) {
    dout(10) << "op_commit " << *repop << " -- already marked ondisk" << dendl;
  } else {
    dout(10) << "op_commit " << *repop << dendl;
    repop->waitfor_disk.erase(osd->get_nodeid());
    //repop->waitfor_nvram.erase(osd->get_nodeid());
    last_complete_ondisk = repop->pg_local_last_complete;
    eval_repop(repop);
  }

  repop->put();
  unlock();
}



void ReplicatedPG::eval_repop(RepGather *repop)
{
  MOSDOp *op = (MOSDOp *)repop->ctx->op;

  dout(10) << "eval_repop " << *repop
	   << " wants=" << (op->wants_ack() ? "a":"") << (op->wants_ondisk() ? "d":"")
	   << dendl;
 
  // apply?
  if (!repop->applied && !repop->applying &&
      ((mode.is_delayed_mode() &&
	repop->waitfor_ack.size() == 1) ||  // all other replicas have acked
       mode.is_rmw_mode()))
    apply_repop(repop);
  
  // disk?
  if (repop->can_send_disk() && op->wants_ondisk()) {
    // send commit.
    MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ONDISK);
    dout(10) << " sending commit on " << *repop << " " << reply << dendl;
    //if the message came from an OSD, it needs to go back to originator,
    //but if the connection ISN't an OSD that connection is the originator
    if (op->get_connection()->get_peer_type() != CEPH_ENTITY_TYPE_OSD)
      osd->messenger->send_message(reply, op->get_connection());
    else
      osd->messenger->send_message(reply, op->get_orig_source_inst());
    repop->sent_disk = true;
  }

  /*
  // nvram?
  else if (repop->can_send_nvram()) {
    if (op->wants_onnvram()) {
      // send commit.
      MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ONNVRAM);
      dout(10) << " sending onnvram on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, op->get_orig_source_inst());
      repop->sent_nvram = true;
    }
    }*/

  // ack?
  else if (repop->can_send_ack()) {
    if (op->wants_ack()) {
      // send ack
      MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK);
      dout(10) << " sending ack on " << *repop << " " << reply << dendl;
      //if the message came from an OSD, it needs to go back to originator,
      //but if the connection ISN't an OSD that connection is the originator
      if (op->get_connection()->get_peer_type() != CEPH_ENTITY_TYPE_OSD)
	osd->messenger->send_message(reply, op->get_connection());
      else
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
    dout(20) << "   q front is " << *repop_queue.front() << dendl; 
    if (repop_queue.front() != repop) {
      dout(0) << " removing " << *repop << dendl;
      dout(0) << "   q front is " << *repop_queue.front() << dendl; 
      assert(repop_queue.front() == repop);
    }
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
    wr->snapset = repop->obc->obs.ssc->snapset;
    wr->snapc = repop->ctx->snapc;
    wr->get_data() = repop->ctx->op->get_data();   // _copy_ bufferlist
  } else {
    // ship resulting transaction, log entries, and pg_stats
    ::encode(repop->ctx->op_t, wr->get_data());
    ::encode(repop->ctx->log, wr->logbl);
    wr->pg_stats = info.stats;
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

  dout(10) << "new_repop mode was " << mode << dendl;
  mode.write_start();
  obc->get();  // we take a ref
  dout(10) << "new_repop mode now " << mode << " (start_write)" << dendl;

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
      //repop->waitfor_nvram.erase(fromosd);
      repop->waitfor_ack.erase(fromosd);
      peer_last_complete_ondisk[fromosd] = peer_lcod;
    }
/*} else if (ack_type & CEPH_OSD_FLAG_ONNVRAM) {
    // nvram
    repop->waitfor_nvram.erase(fromosd);
    repop->waitfor_ack.erase(fromosd);*/
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
    int r = osd->store->getattr(coll_t::build_pg_coll(info.pgid), soid, OI_ATTR, bv);
    if (r < 0 && !can_create)
      return 0;   // -ENOENT!

    obc = new ObjectContext(soid);

    if (can_create)
      obc->obs.ssc = get_snapset_context(soid.oid, true);

    if (r >= 0) {
      obc->obs.oi.decode(bv);
      obc->obs.exists = true;
      assert(!obc->obs.ssc || obc->obs.ssc->snapset.head_exists);
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
  // want the head?
  sobject_t head(oid, CEPH_NOSNAP);
  if (snapid == CEPH_NOSNAP) {
    ObjectContext *obc = get_object_context(head, can_create);
    if (!obc)
      return -ENOENT;
    dout(10) << "find_object_context " << oid << " @" << snapid << dendl;
    *pobc = obc;
    return 0;
  }

  // we want a snap
  SnapSetContext *ssc = get_snapset_context(oid, can_create);
  if (!ssc)
    return -ENOENT;

  dout(10) << "find_object_context " << oid << " @" << snapid
	   << " snapset " << ssc->snapset << dendl;
 
  // head?
  if (snapid > ssc->snapset.seq) {
    if (ssc->snapset.head_exists) {
      ObjectContext *obc = get_object_context(head, false);
      dout(10) << "find_object_context  " << head
	       << " want " << snapid << " > snapset seq " << ssc->snapset.seq
	       << " -- HIT " << obc->obs
	       << dendl;
      if (!obc->obs.ssc)
	obc->obs.ssc = ssc;
      else {
	assert(ssc == obc->obs.ssc);
	put_snapset_context(ssc);
      }
      *pobc = obc;
      return 0;
    }
    dout(10) << "find_object_context  " << head
	     << " want " << snapid << " > snapset seq " << ssc->snapset.seq
	     << " but head dne -- DNE"
	     << dendl;
    put_snapset_context(ssc);
    return -ENOENT;
  }

  // which clone would it be?
  unsigned k = 0;
  while (k < ssc->snapset.clones.size() &&
	 ssc->snapset.clones[k] < snapid)
    k++;
  if (k == ssc->snapset.clones.size()) {
    dout(10) << "get_object_context  no clones with last >= snapid " << snapid << " -- DNE" << dendl;
    put_snapset_context(ssc);
    return -ENOENT;
  }
  sobject_t soid(oid, ssc->snapset.clones[k]);

  put_snapset_context(ssc); // we're done with ssc
  ssc = 0;

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

  if (mode.wake) {
    osd->take_waiters(mode.waiting);
    mode.wake = false;
  }

  --obc->ref;
  if (obc->ref == 0) {
    if (obc->obs.ssc)
      put_snapset_context(obc->obs.ssc);

    if (obc->registered)
      object_contexts.erase(obc->obs.oi.soid);
    delete obc;

    if (object_contexts.empty())
      kick();
  }
}


ReplicatedPG::SnapSetContext *ReplicatedPG::get_snapset_context(const object_t& oid, bool can_create)
{
  SnapSetContext *ssc;
  map<object_t, SnapSetContext*>::iterator p = snapset_contexts.find(oid);
  if (p != snapset_contexts.end()) {
    ssc = p->second;
  } else {
    bufferlist bv;
    sobject_t head(oid, CEPH_NOSNAP);
    int r = osd->store->getattr(coll_t::build_pg_coll(info.pgid), head, SS_ATTR, bv);
    if (r < 0) {
      // try _snapset
      sobject_t snapdir(oid, CEPH_SNAPDIR);
      r = osd->store->getattr(coll_t::build_pg_coll(info.pgid), snapdir, SS_ATTR, bv);
      if (r < 0 && !can_create)
	return NULL;
    }
    ssc = new SnapSetContext(oid);
    if (r >= 0) {
      bufferlist::iterator bvp = bv.begin();
      ssc->snapset.decode(bvp);
    }
  }
  assert(ssc);
  dout(10) << "get_snapset_context " << ssc->oid << " "
	   << ssc->ref << " -> " << (ssc->ref+1) << dendl;
  ssc->ref++;
  return ssc;
}

void ReplicatedPG::put_snapset_context(SnapSetContext *ssc)
{
  dout(10) << "put_snapset_context " << ssc->oid << " "
	   << ssc->ref << " -> " << (ssc->ref-1) << dendl;

  --ssc->ref;
  if (ssc->ref == 0) {
    if (ssc->registered)
      snapset_contexts.erase(ssc->oid);
    delete ssc;
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
      if (!osd->store->exists(coll_t::build_pg_coll(info.pgid), soid) || 
	  osd->store->getattr(coll_t::build_pg_coll(info.pgid), soid, "balance-reads", &b, 1) < 0) {
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
      (osd->store->getattr(coll_t::build_pg_coll(info.pgid), soid, "balance-reads", &v, 1) >= 0 ||
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

void ReplicatedPG::sub_op_modify(MOSDSubOp *op)
{
  const sobject_t& soid = op->poid;

  const char *opname;
  if (op->noop)
    opname = "no-op";
  else if (op->ops.size())
    opname = ceph_osd_op_name(op->ops[0].op.op);
  else
    opname = "trans";

  dout(10) << "sub_op_modify " << opname 
           << " " << soid 
           << " v " << op->version
	   << (op->noop ? " NOOP" : "")
	   << (op->logbl.length() ? " (transaction)" : " (parallel exec")
	   << " " << op->logbl.length()
	   << dendl;  

  // sanity checks
  assert(op->map_epoch >= info.history.same_acting_since);
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
  
  RepModify *rm = new RepModify;
  rm->pg = this;
  rm->op = op;
  rm->ctx = 0;
  rm->ackerosd = ackerosd;
  rm->last_complete = info.last_complete;

  if (!op->noop) {
    if (op->logbl.length()) {
      // shipped transaction and log entries
      vector<Log::Entry> log;
      
      bufferlist::iterator p = op->get_data().begin();
      ::decode(rm->opt, p);
      p = op->logbl.begin();
      ::decode(log, p);
      
      info.stats = op->pg_stats;
      log_op(log, op->pg_trim_to, rm->localt);

      rm->tls.push_back(&rm->opt);
      rm->tls.push_back(&rm->localt);
    } else {
      // do op
      ObjectState obs(op->poid);
      obs.oi.version = op->old_version;
      obs.oi.size = op->old_size;
      obs.exists = op->old_exists;
      
      rm->ctx = new OpContext(op, op->reqid, op->ops, op->get_data(), &obs, this);
      
      rm->ctx->mtime = op->mtime;
      rm->ctx->at_version = op->version;
      rm->ctx->snapc = op->snapc;

      SnapSetContext ssc(op->poid.oid);
      ssc.snapset = op->snapset;
      rm->ctx->obs->ssc = &ssc;
      
      prepare_transaction(rm->ctx);
      log_op(rm->ctx->log, op->pg_trim_to, rm->ctx->local_t);
    
      rm->tls.push_back(&rm->ctx->op_t);
      rm->tls.push_back(&rm->ctx->local_t);
    }
  } else {
    // just trim the log
    if (op->pg_trim_to != eversion_t()) {
      trim(rm->localt, op->pg_trim_to);
      rm->tls.push_back(&rm->localt);
    }
  }
  
  Context *oncommit = new C_OSD_RepModifyCommit(rm);
  Context *onapply = new C_OSD_RepModifyApply(rm);
  int r = osd->store->queue_transactions(&osr, rm->tls, onapply, oncommit);
  if (r) {
    derr(0) << "error applying transaction: r = " << r << dendl;
    assert(0);
  }
}

void ReplicatedPG::sub_op_modify_applied(RepModify *rm)
{
  lock();
  dout(10) << "sub_op_modify_applied on " << rm << " op " << *rm->op << dendl;

  if (!rm->committed) {
    // send ack to acker only if we haven't sent a commit already
    MOSDSubOpReply *ack = new MOSDSubOpReply(rm->op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK);
    ack->set_peer_stat(osd->get_my_stat_for(g_clock.now(), rm->ackerosd));
    ack->set_priority(CEPH_MSG_PRIO_HIGH);
    osd->messenger->send_message(ack, osd->osdmap->get_inst(rm->ackerosd));
  }

  rm->applied = true;
  bool done = rm->applied && rm->committed;

  unlock();
  if (done) {
    delete rm->ctx;
    rm->op->put();
    delete rm;
    put();
  }
}

void ReplicatedPG::sub_op_modify_commit(RepModify *rm)
{
  lock();

  // send commit.
  dout(10) << "sub_op_modify_commit on op " << *rm->op
           << ", sending commit to osd" << rm->ackerosd
           << dendl;
  if (osd->osdmap->is_up(rm->ackerosd)) {
    last_complete_ondisk = rm->last_complete;
    MOSDSubOpReply *commit = new MOSDSubOpReply(rm->op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ONDISK);
    commit->set_last_complete_ondisk(rm->last_complete);
    commit->set_peer_stat(osd->get_my_stat_for(g_clock.now(), rm->ackerosd));
    osd->messenger->send_message(commit, osd->osdmap->get_inst(rm->ackerosd));
  }
  
  rm->committed = true;
  bool done = rm->applied && rm->committed;

  unlock();
  if (done) {
    delete rm->ctx;
    rm->op->put();
    delete rm;
    put();
  }
}

void ReplicatedPG::sub_op_modify_reply(MOSDSubOpReply *r)
{
  // must be replication.
  tid_t rep_tid = r->get_tid();
  int fromosd = r->get_source().num();
  
  osd->take_peer_stat(fromosd, r->get_peer_stat());
  
  if (repop_map.count(rep_tid)) {
    // oh, good.
    repop_ack(repop_map[rep_tid], 
	      r->get_result(), r->ack_type,
	      fromosd, 
	      r->get_last_complete_ondisk());
  }

  r->put();
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
  osd->store->stat(coll_t::build_pg_coll(info.pgid), head, &st);

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
    // do we have the head and/or snapdir?
    sobject_t head = soid;
    head.snap = CEPH_NOSNAP;
    if (missing.is_missing(head)) {
      if (pulling.count(head)) {
	dout(10) << " missing but already pulling head " << head << dendl;
	return false;
      } else {
	return pull(head);
      }
    }
    head.snap = CEPH_SNAPDIR;
    if (missing.is_missing(head)) {
      if (pulling.count(head)) {
	dout(10) << " missing but already pulling snapdir " << head << dendl;
	return false;
      } else {
	return pull(head);
      }
    }

    // check snapset
    SnapSetContext *ssc = get_snapset_context(soid.oid, false);
    dout(10) << " snapset " << ssc->snapset << dendl;
    calc_clone_subsets(ssc->snapset, soid, missing,
		       data_subset, clone_subsets);
    put_snapset_context(ssc);
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
  int r = osd->store->stat(coll_t::build_pg_coll(info.pgid), soid, &st);
  assert(r == 0);
  
  map<sobject_t, interval_set<__u64> > clone_subsets;
  interval_set<__u64> data_subset;

  bufferlist bv;
  r = osd->store->getattr(coll_t::build_pg_coll(info.pgid), soid, OI_ATTR, bv);
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
      if (st.st_size)
	clone_subsets[head].insert(0, st.st_size);
      push(soid, peer, data_subset, clone_subsets);
      return;
    }

    // try to base push off of clones that succeed/preceed poid
    // we need the head (and current SnapSet) to do that.
    if (missing.is_missing(head)) {
      dout(15) << "push_to_replica missing head " << head << ", pushing raw clone" << dendl;
      return push(soid, peer);
    }
    sobject_t snapdir = head;
    snapdir.snap = CEPH_SNAPDIR;
    if (missing.is_missing(snapdir)) {
      dout(15) << "push_to_replica missing snapdir " << snapdir << ", pushing raw clone" << dendl;
      return push(snapdir, peer);
    }
    
    SnapSetContext *ssc = get_snapset_context(soid.oid, false);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    calc_clone_subsets(ssc->snapset, soid, peer_missing[peer],
		       data_subset, clone_subsets);
    put_snapset_context(ssc);
  } else if (soid.snap == CEPH_NOSNAP) {
    // pushing head or unversioned object.
    // base this on partially on replica's clones?
    SnapSetContext *ssc = get_snapset_context(soid.oid, false);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    calc_head_subsets(ssc->snapset, soid, peer_missing[peer], data_subset, clone_subsets);
    put_snapset_context(ssc);
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
  map<string,bufferptr> attrset;
  __u64 size;

  if (data_subset.size() || clone_subsets.size()) {
    struct stat st;
    int r = osd->store->stat(coll_t::build_pg_coll(info.pgid), soid, &st);
    assert(r == 0);
    size = st.st_size;

    for (map<__u64,__u64>::iterator p = data_subset.m.begin();
	 p != data_subset.m.end();
	 p++) {
      bufferlist bit;
      osd->store->read(coll_t::build_pg_coll(info.pgid), soid, p->first, p->second, bit);
      bl.claim_append(bit);
    }
  } else {
    osd->store->read(coll_t::build_pg_coll(info.pgid), soid, 0, 0, bl);
    size = bl.length();
  }

  osd->store->getattrs(coll_t::build_pg_coll(info.pgid), soid, attrset);

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
  subop->ops[0].op.extent.offset = 0;
  subop->ops[0].op.extent.length = size;
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
      finish_recovery_op(soid);
    } else {
      dout(10) << "pushed " << soid << ", still waiting for push ack from " 
	       << pushing[soid] << dendl;
    }
  }
  reply->put();
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
  op->put();
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
  if (same_since == info.history.same_acting_since) {
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

void ReplicatedPG::_wrote_pushed_object(ObjectStore::Transaction *t, ObjectContext *obc)
{
  dout(10) << "_wrote_pushed_object " << *obc << dendl;
  lock();
  put_object_context(obc);
  unlock();
  delete t;
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
	  << " len " << push.op.extent.length
	  << " data_subset " << op->data_subset
	  << " clone_subsets " << op->clone_subsets
	  << " data len " << op->get_data().length()
          << dendl;

  interval_set<__u64> data_subset;
  map<sobject_t, interval_set<__u64> > clone_subsets;

  bufferlist data;
  data.claim(op->get_data());

  // we need this later, and it gets clobbered by t.setattrs()
  bufferlist oibl;
  if (op->attrset.count(OI_ATTR))
    oibl.push_back(op->attrset[OI_ATTR]);

  // determine data/clone subsets
  data_subset = op->data_subset;
  if (data_subset.empty() && push.op.extent.length && push.op.extent.length == data.length())
    data_subset.insert(0, push.op.extent.length);
  clone_subsets = op->clone_subsets;

  if (is_primary()) {
    if (soid.snap && soid.snap < CEPH_NOSNAP) {
      // clone.  make sure we have enough data.
      SnapSetContext *ssc = get_snapset_context(soid.oid, false);
      assert(ssc);

      clone_subsets.clear();   // forget what pusher said; recalculate cloning.

      interval_set<__u64> data_needed;
      calc_clone_subsets(ssc->snapset, soid, missing, data_needed, clone_subsets);
      put_snapset_context(ssc);
      
      dout(10) << "sub_op_push need " << data_needed << ", got " << data_subset << dendl;
      if (!data_needed.subset_of(data_subset)) {
	dout(0) << " we did not get enough of " << soid << " object data" << dendl;
	op->put();
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
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  t->remove(coll_t::build_pg_coll(info.pgid), soid);  // in case old version exists

  __u64 boff = 0;
  for (map<sobject_t, interval_set<__u64> >::iterator p = clone_subsets.begin();
       p != clone_subsets.end();
       p++)
    for (map<__u64,__u64>::iterator q = p->second.m.begin();
	 q != p->second.m.end(); 
	 q++) {
      dout(15) << " clone_range " << p->first << " " << q->first << "~" << q->second << dendl;
      t->clone_range(coll_t::build_pg_coll(info.pgid), p->first, soid, q->first, q->second);
    }
  for (map<__u64,__u64>::iterator p = data_subset.m.begin();
       p != data_subset.m.end(); 
       p++) {
    bufferlist bit;
    bit.substr_of(data, boff, p->second);
    t->write(coll_t::build_pg_coll(info.pgid), soid, p->first, p->second, bit);
    dout(15) << " write " << p->first << "~" << p->second << dendl;
    boff += p->second;
  }

  if (data_subset.empty())
    t->touch(coll_t::build_pg_coll(info.pgid), soid);

  t->setattrs(coll_t::build_pg_coll(info.pgid), soid, op->attrset);
  if (soid.snap && soid.snap < CEPH_NOSNAP &&
      op->attrset.count(OI_ATTR)) {
    bufferlist bl;
    bl.push_back(op->attrset[OI_ATTR]);
    object_info_t oi(bl);
    if (oi.snaps.size()) {
      coll_t lc = make_snap_collection(*t, oi.snaps[0]);
      t->collection_add(lc, coll_t::build_pg_coll(info.pgid), soid);
      if (oi.snaps.size() > 1) {
	coll_t hc = make_snap_collection(*t, oi.snaps[oi.snaps.size()-1]);
	t->collection_add(hc, coll_t::build_pg_coll(info.pgid), soid);
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
    if (log.complete_to != log.log.end())
      dout(10) << " log.complete_to = " << log.complete_to->version << dendl;
  }

  // track ObjectContext
  Context *onreadable = 0;
  Context *onreadable_sync = 0;
  if (is_primary()) {
    dout(10) << " setting up obc for " << soid << dendl;
    ObjectContext *obc = 0;
    find_object_context(soid.oid, soid.snap, &obc, true);
    register_object_context(obc);
    obc->ondisk_write_lock();
    
    obc->obs.oi.decode(oibl);

    onreadable = new C_OSD_WrotePushedObject(this, t, obc);
    onreadable_sync = new C_OSD_OndiskWriteUnlock(obc);
  } else {
    onreadable = new ObjectStore::C_DeleteTransaction(t);
  }

  // apply to disk!
  write_info(*t);
  int r = osd->store->queue_transaction(&osr, t,
					onreadable,
					new C_OSD_Commit(this, info.history.same_acting_since,
							 info.last_complete),
					onreadable_sync);
  assert(r == 0);

  osd->logger->inc(l_osd_r_pull);
  osd->logger->inc(l_osd_r_pullb, data.length());

  if (is_primary()) {

    missing_loc.erase(soid);

    // close out pull op?
    if (pulling.count(soid)) {
      pulling.erase(soid);
      finish_recovery_op(soid);
    }

    update_stats();

    if (info.is_uptodate())
      uptodate_set.insert(osd->get_nodeid());
    
    /*
    if (is_active()) {
      // are others missing this too?  (only if we're active.. skip
      // this part if we're still repeering, it'll just confuse us)
      for (unsigned i=1; i<acting.size(); i++) {
	int peer = acting[i];
	assert(peer_missing.count(peer));
	if (peer_missing[peer].is_missing(soid)) {
	  push_to_replica(soid, peer);  // ok, push it, and they (will) have it now.
	  start_recovery_op(soid);
	}
      }
    }
    */

  } else {
    // ack if i'm a replica and being pushed to.
    MOSDSubOpReply *reply = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK); 
    osd->messenger->send_message(reply, op->get_connection());
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

  op->put();  // at the end... soid is a ref to op->soid!
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
    if (!repop->applied && !repop->applying)
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
#ifdef DEBUG_RECOVERY_OIDS
  recovering_oids.clear();
#endif
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




/**
 * do one recovery op.
 * return true if done, false if nothing left to do.
 */
int ReplicatedPG::recover_primary(int max)
{
  assert(is_primary());

  dout(10) << "recover_primary pulling " << pulling.size() << " in pg" << dendl;
  dout(10) << "recover_primary " << missing << dendl;
  dout(25) << "recover_primary " << missing.missing << dendl;

  // look at log!
  Log::Entry *latest = 0;
  int started = 0;
  int skipped = 0;

  map<eversion_t, sobject_t>::iterator p = missing.rmissing.lower_bound(log.last_requested);
  while (p != missing.rmissing.end()) {
    sobject_t soid;
    eversion_t v = p->first;

    if (log.objects.count(p->second)) {
      latest = log.objects[p->second];
      assert(latest->is_update());
      soid = latest->soid;
    } else {
      latest = 0;
      soid = p->second;
    }
    Missing::item& item = missing.missing[p->second];

    sobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    dout(10) << "recover_primary "
             << soid << " " << item.need
	     << (missing.is_missing(soid) ? " (missing)":"")
	     << (missing.is_missing(head) ? " (missing head)":"")
             << (pulling.count(soid) ? " (pulling)":"")
	     << (pulling.count(head) ? " (pulling head)":"")
             << dendl;
    
    if (!pulling.count(soid)) {
      if (pulling.count(head)) {
	++skipped;
      } else {
	// is this a clone operation that we can do locally?
	if (latest && latest->op == Log::Entry::CLONE) {
	  if (missing.is_missing(head) &&
	      missing.have_old(head) == latest->prior_version) {
	    dout(10) << "recover_primary cloning " << head << " v" << latest->prior_version
		     << " to " << soid << " v" << latest->version
		     << " snaps " << latest->snaps << dendl;
	    ObjectStore::Transaction *t = new ObjectStore::Transaction;

	    ObjectContext *headobc = get_object_context(head);

	    object_info_t oi(soid);
	    oi.version = latest->version;
	    oi.prior_version = latest->prior_version;
	    oi.last_reqid = headobc->obs.oi.last_reqid;
	    oi.mtime = headobc->obs.oi.mtime;
	    ::decode(oi.snaps, latest->snaps);
	    _make_clone(*t, head, soid, &oi);

	    put_object_context(headobc);

	    // XXX: track objectcontext!
	    int tr = osd->store->queue_transaction(&osr, t);
	    assert(tr == 0);
	    missing.got(latest->soid, latest->version);
	    missing_loc.erase(latest->soid);
	    continue;
	  }
	}
	
	if (pull(soid)) {
	  ++started;
	  start_recovery_op(soid);
	} else
	  ++skipped;
	if (started >= max)
	  return started;
      }
    }
    
    p++;

    // only advance last_requested if we haven't skipped anything
    if (!skipped)
      log.last_requested = v;
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
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    C_Contexts *fin = new C_Contexts;
    finish_recovery(*t, fin->contexts);
    int tr = osd->store->queue_transaction(&osr, t, new ObjectStore::C_DeleteTransaction(t), fin);
    assert(tr == 0);
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

    ObjectContext *obc = lookup_object_context(soid);
    if (obc) {
      dout(10) << " ondisk_read_lock for " << soid << dendl;
      obc->ondisk_read_lock();
    }

    start_recovery_op(soid);
    started++;

    push_to_replica(soid, peer);

    // do other peers need it too?
    for (i++; i<acting.size(); i++) {
      int peer = acting[i];
      if (peer_missing.count(peer) &&
          peer_missing[peer].is_missing(soid)) 
	push_to_replica(soid, peer);
    }

    if (obc) {
      dout(10) << " ondisk_read_unlock on " << soid << dendl;
      obc->ondisk_read_unlock();
      put_object_context(obc);
    }

    if (started >= max)
      return started;
  }
  
  // nothing to do!
  dout(-10) << "recover_replicas - nothing to do!" << dendl;

  if (is_all_uptodate()) {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    C_Contexts *fin = new C_Contexts;
    finish_recovery(*t, fin->contexts);
    int tr = osd->store->queue_transaction(&osr, t, new ObjectStore::C_DeleteTransaction(t), fin);
    assert(tr == 0);
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

  assert(info.last_update >= log.tail);  // otherwise we need some help!

  if (log.backlog) {

    // FIXME: sloppy pobject vs object conversions abound!  ***
    
    // be thorough.
    vector<sobject_t> ls;
    osd->store->collection_list(coll_t::build_pg_coll(info.pgid), ls);
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
          t.remove(coll_t::build_pg_coll(info.pgid), p->soid);
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
      t.remove(coll_t::build_pg_coll(info.pgid), *i);
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
        t.remove(coll_t::build_pg_coll(info.pgid), p->soid);
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

  coll_t c = coll_t::build_pg_coll(info.pgid);
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

    // new snapset?
    if (soid.snap == CEPH_SNAPDIR ||
	soid.snap == CEPH_NOSNAP) {
      if (p->attrs.count(SS_ATTR) == 0) {
	dout(0) << mode << " no '" << SS_ATTR << "' attr on " << soid << dendl;
	errors++;
	continue;
      }
      bufferlist bl;
      bl.push_back(p->attrs[SS_ATTR]);
      bufferlist::iterator blp = bl.begin();
      ::decode(snapset, blp);

      // did we finish the last oid?
      if (head != sobject_t()) {
	derr(0) << " missing clone(s) for " << head << dendl;
	assert(head == sobject_t());  // we had better be done
	errors++;
      }
      
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
    }
    if (soid.snap == CEPH_SNAPDIR)
      continue;

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

    if (soid.snap == CEPH_NOSNAP) {
      if (!snapset.head_exists) {
	dout(0) << mode << "  snapset.head_exists=false, but " << soid << " exists" << dendl;
	errors++;
	continue;
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

      // tell replicas
      for (unsigned i=1; i<acting.size(); i++) {
	MOSDPGInfo *m = new MOSDPGInfo(osd->osdmap->get_epoch());
	m->pg_info.push_back(info);
	osd->messenger->send_message(m, osd->osdmap->get_inst(acting[i]));
      }
    }
  }

  dout(10) << "_scrub (" << mode << ") finish" << dendl;
  return errors;
}
