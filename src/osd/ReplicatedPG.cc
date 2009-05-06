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

#include "common/Logger.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGRemove.h"

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

bool ReplicatedPG::is_missing_object(sobject_t soid)
{
  return missing.missing.count(soid);
}
 

void ReplicatedPG::wait_for_missing_object(sobject_t soid, Message *m)
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
  // we only care about reads here on out..
  if (op->is_modify() ||
      op->ops.size() < 1 ||
      op->is_modify())
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

  return false;
}


/** do_op - do an op
 * pg lock will be held (if multithreaded)
 * osd_lock NOT held.
 */
void ReplicatedPG::do_op(MOSDOp *op) 
{
  //dout(15) << "do_op " << *op << dendl;

  osd->logger->inc(l_osd_op);

  if (op->is_modify())
    op_modify(op);
  else
    op_read(op);
}


void ReplicatedPG::do_sub_op(MOSDSubOp *op)
{
  dout(15) << "do_sub_op " << *op << dendl;

  osd->logger->inc(l_osd_subop);

  if (op->ops.size() >= 1) {
    ceph_osd_op& first = op->ops[0];
    switch (first.op) {
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
    ceph_osd_op& first = r->ops[0];
    if (first.op == CEPH_OSD_OP_PUSH) {
      // continue peer recovery
      sub_op_push_reply(r);
      return;
    }
    if (first.op == CEPH_OSD_OP_SCRUB) {
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

  while (info.dead_snaps.size() &&
	 is_active()) {
    snapid_t sn = *info.dead_snaps.begin();
    coll_t c = info.pgid.to_snap_coll(sn);
    vector<pobject_t> ls;
    osd->store->collection_list(c, ls);
    if (ls.size() != info.stats.num_objects)
      dout(10) << " WARNING: " << ls.size() << " != num_objects " << info.stats.num_objects << dendl;

    dout(10) << "snap_trimmer collection " << c << " has " << ls.size() << " items" << dendl;

    for (vector<pobject_t>::iterator p = ls.begin(); p != ls.end(); p++) {
      sobject_t coid = *p;

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
	if (!osd->osdmap->is_removed_snap(snaps[i]))
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
 
    info.dead_snaps.erase(sn);
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
// READS


/*
 * return false if object doesn't (logically) exist
 */
int ReplicatedPG::pick_read_snap(sobject_t& soid, object_info_t& coi)
{
  sobject_t head = soid;
  head.snap = CEPH_NOSNAP;

  bufferlist bl;
  int r = osd->store->getattr(info.pgid.to_coll(), head, OI_ATTR, bl);
  if (r < 0)
    return -ENOENT;  // if head doesn't exist, no snapped version will either.
  object_info_t hoi(bl);

  dout(10) << "pick_read_snap " << soid << " snapset " << hoi.snapset << dendl;

  snapid_t want = soid.snap;

  // head?
  if (soid.snap > hoi.snapset.seq) {
    if (hoi.snapset.head_exists) {
      dout(10) << "pick_read_snap  " << head
	       << " want " << want << " > snapset seq " << hoi.snapset.seq
	       << " -- HIT" << dendl;
      soid = head;
      coi = hoi;
      return 0;
    } else {
      dout(10) << "pick_read_snap  " << head
	       << " want " << want << " > snapset seq " << hoi.snapset.seq
	       << " but head_exists = false -- DNE" << dendl;
      return -ENOENT;
    }
  }

  // which clone would it be?
  unsigned k = 0;
  while (k<hoi.snapset.clones.size() && hoi.snapset.clones[k] < want)
    k++;
  if (k == hoi.snapset.clones.size()) {
    dout(10) << "pick_read_snap  no clones with last >= want " << want << " -- DNE" << dendl;
    return -ENOENT;
  }
  
  // check clone
  soid.snap = hoi.snapset.clones[k];
  if (missing.is_missing(soid)) {
    dout(20) << "pick_read_snap  " << soid << " missing, try again later" << dendl;
    return -EAGAIN;
  }

  // load clone info
  bl.clear();
  osd->store->getattr(info.pgid.to_coll(), soid, OI_ATTR, bl);
  coi.decode(bl);

  // clone
  dout(20) << "pick_read_snap  " << soid << " snaps " << coi.snaps << dendl;
  snapid_t first = coi.snaps[coi.snaps.size()-1];
  snapid_t last = coi.snaps[0];
  if (first <= want) {
    dout(20) << "pick_read_snap  " << soid << " [" << first << "," << last << "] contains " << want << " -- HIT" << dendl;
    return 0;
  } else {
    dout(20) << "pick_read_snap  " << soid << " [" << first << "," << last << "] does not contain " << want << " -- DNE" << dendl;
    return -ENOENT;
  }
} 


void ReplicatedPG::op_read(MOSDOp *op)
{
  object_t oid = op->get_oid();
  sobject_t soid(oid, op->get_snapid());

  dout(10) << "op_read " << soid << " " << op->ops << dendl;

  bufferlist::iterator bp = op->get_data().begin();
  bufferlist data;
  int data_off = 0;
  int result = 0;

  // pick revision
  object_info_t oi(soid);
  if (soid.snap) {
    result = pick_read_snap(soid, oi);
    if (result == -EAGAIN) {
      wait_for_missing_object(soid, op);
      return;
    }
    if (result != 0)
      goto done;    // we have no revision for this request.
  } 

  // wrlocked?
  if (op->get_snapid() == CEPH_NOSNAP &&
      block_if_wrlocked(op, oi)) 
    return;


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

  // do it.
  for (vector<ceph_osd_op>::iterator p = op->ops.begin(); p != op->ops.end(); p++) {
    switch (p->op) {
    case CEPH_OSD_OP_READ:
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->read(info.pgid.to_coll(), soid, p->offset, p->length, bl);
	if (data.length() == 0)
	  data_off = p->offset;
	data.claim(bl);
	if (r >= 0) 
	  p->length = r;
	else {
	  result = r;
	  p->length = 0;
	}
	dout(10) << " read got " << r << " / " << p->length << " bytes from obj " << oid << dendl;
      }
      osd->logger->inc(l_osd_c_rd);
      osd->logger->inc(l_osd_c_rdb, p->length);
      break;

   case CEPH_OSD_OP_EXEC:
    {
	bufferlist bl;
	int r = osd->store->read(info.pgid.to_coll(), poid, p->offset, p->length, bl);

	if (data.length() == 0)
	  data_off = p->offset;

	if (r >= 0)  {
	  p->length = r;
	  char *buf = bl.c_str();
          for (unsigned int i=0; i<bl.length(); i++)
	     if (isdigit(buf[i]))
		buf[i] = '?';

	  data.claim(bl);
	} else {
	  result = r;
	  p->length = 0;
	}
	dout(10) << " exec got " << r << " / " << p->length << " bytes from obj " << oid << dendl;
	dout(10) << " exec reply=" << data.c_str() << dendl;
    }
    break;
    
    case CEPH_OSD_OP_STAT:
      {
	struct stat st;
	memset(&st, sizeof(st), 0);
	int r = osd->store->stat(info.pgid.to_coll(), soid, &st);
	if (r >= 0)
	  p->length = st.st_size;
	else
	  result = r;
      }
      break;

    case CEPH_OSD_OP_GETXATTR:
      {
	nstring name(p->name_len + 1);
	name[0] = '_';
	bp.copy(p->name_len, name.data()+1);
	int r = osd->store->getattr(info.pgid.to_coll(), soid, name.c_str(), data);
	if (r >= 0) {
	  p->value_len = r;
	  result = 0;
	} else
	  result = r;
      }
      break;
      
    case CEPH_OSD_OP_GREP:
      {
	
      }
      break;
      
    case CEPH_OSD_OP_MASKTRUNC:
      if (p != op->ops.begin()) {
	ceph_osd_op& rd = *(p - 1);
	ceph_osd_op& m = *p;

	// are we beyond truncate_size?
	if (rd.offset + rd.length > m.truncate_size) {	
	  __u32 seq = 0;
	  interval_set<__u64> tm;
	  if (oi.truncate_info.length()) {
	    bufferlist::iterator p = oi.truncate_info.begin();
	    ::decode(seq, p);
	    ::decode(tm, p);
	  }

	  // truncated portion of the read
	  unsigned from = MAX(rd.offset, m.truncate_size);  // also end of data
	  unsigned to = rd.offset + rd.length;
  	  unsigned trim = to-from;

	  rd.length = rd.length - trim;

	  dout(10) << " masktrunc " << m << ": overlap " << from << "~" << trim << dendl;

	  bufferlist keep;
	  keep.substr_of(data, 0, data.length() - trim);
	  bufferlist truncated;  // everthing after 'from'
	  truncated.substr_of(data, data.length() - trim, trim);
	  keep.swap(data);

	  if (seq == rd.truncate_seq) {
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
		  data.push_back(bp);
		  dout(20) << "  adding " << bp.length() << " zeros" << dendl;
		  rd.length = rd.length + bp.length();
		  data_end += bp.length();
		}
		
		bufferlist b;
		b.substr_of(truncated, s-from, l);
		dout(20) << "  adding " << b.length() << " bytes from " << s << "~" << l << dendl;
		data.claim_append(b);
		rd.length = rd.length + l;
		data_end += l;
	      }
	    } // for
	  } // seq == rd.truncate_eq
	}
      }
      break;

    default:
      dout(1) << "unrecognized osd op " << p->op
	      << " " << ceph_osd_op_name(p->op)
	      << dendl;
      result = -EOPNOTSUPP;
      assert(0);  // for now
    }
  }
  
 done:
  // reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK); 
  reply->set_data(data);
  reply->get_header().data_off = data_off;
  reply->set_result(result);

  if (result >= 0) {
    utime_t now = g_clock.now();
    utime_t diff = now;
    diff -= op->get_recv_stamp();
    dout(10) <<  "op_read " << op->get_reqid() << " total op latency " << diff << dendl;
    Mutex::Locker lock(osd->peer_stat_lock);
    osd->stat_rd_ops_in_queue--;
    osd->read_latency_calc.add(diff);

    if (is_primary() &&
	g_conf.osd_balance_reads)
      stat_object_temp_rd[soid].hit(now);  // hit temp.
  }

  osd->messenger->send_message(reply, op->get_orig_source_inst());
  
  delete op;
}






// ========================================================================
// MODIFY



void ReplicatedPG::_make_clone(ObjectStore::Transaction& t,
			       sobject_t head, pobject_t coid,
			       eversion_t ov, eversion_t v, osd_reqid_t& reqid, utime_t mtime, vector<snapid_t>& snaps)
{
  object_info_t pi(coid);
  pi.version = v;
  pi.prior_version = ov;
  pi.last_reqid = reqid;
  pi.mtime = mtime;
  pi.snaps.swap(snaps);
  bufferlist bv;
  ::encode(pi, bv);

  t.clone(info.pgid.to_coll(), head, coid);
  t.setattr(info.pgid.to_coll(), coid, OI_ATTR, bv);
}

void ReplicatedPG::prepare_clone(ObjectStore::Transaction& t, bufferlist& logbl, osd_reqid_t reqid, pg_stat_t& stats,
				 sobject_t soid, loff_t old_size, object_info_t& oi,
				 eversion_t& at_version, SnapContext& snapc)
{
  // clone?
  assert(soid.snap == CEPH_NOSNAP);
  dout(20) << "snapset=" << oi.snapset << "  snapc=" << snapc << dendl;;
  
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
    _make_clone(t, soid, coid, oi.version, at_version, reqid, oi.mtime, snaps);
    
    // add to snap bound collections
    coll_t fc = make_snap_collection(t, snaps[0]);
    t.collection_add(fc, info.pgid.to_coll(), coid);
    if (snaps.size() > 1) {
      coll_t lc = make_snap_collection(t, snaps[snaps.size()-1]);
      t.collection_add(lc, info.pgid.to_coll(), coid);
    }
    
    stats.num_objects++;
    stats.num_object_clones++;
    oi.snapset.clones.push_back(coid.snap);
    oi.snapset.clone_size[coid.snap] = old_size;
    oi.snapset.clone_overlap[coid.snap].insert(0, old_size);
    
    // log clone
    dout(10) << "cloning v " << oi.version
	     << " to " << coid << " v " << at_version
	     << " snaps=" << snaps << dendl;
    Log::Entry cloneentry(PG::Log::Entry::CLONE, coid, at_version, oi.version, reqid, oi.mtime);
    ::encode(snaps, cloneentry.snaps);
    add_log_entry(cloneentry, logbl);

    at_version.version++;
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

// low level object operations
int ReplicatedPG::prepare_simple_op(ObjectStore::Transaction& t, osd_reqid_t reqid, pg_stat_t& st,
				    sobject_t soid, __u64& old_size, bool& exists, object_info_t& oi,
				    vector<ceph_osd_op>& ops, int opn, bufferlist::iterator& bp,
				    SnapContext& snapc)
{
  ceph_osd_op& op = ops[opn];
  int eop = op.op;

  dout(15) << "prepare_simple_op " << reqid << " " << opn << ": " << ops[opn] << " on " << oi << dendl;

  // munge ZERO -> DELETE or TRUNCATE?
  if (eop == CEPH_OSD_OP_ZERO &&
      oi.snapset.head_exists &&
      op.offset + op.length >= old_size) {
    if (op.offset == 0) {
      // FIXME: no, this will zap object attributes... do we really want
      // to do this?  ...
      //dout(10) << " munging ZERO " << op.offset << "~" << op.length
      //<< " -> DELETE (size is " << old_size << ")" << dendl;
      //eop = CEPH_OSD_OP_DELETE;
    } else {
      dout(10) << " munging ZERO " << op.offset << "~" << op.length
	       << " -> TRUNCATE " << op.offset << " (old size is " << old_size << ")" << dendl;
      eop = CEPH_OSD_OP_TRUNCATE;
      oi.snapset.head_exists = true;
    }
  }
  // munge DELETE -> TRUNCATE?
  if (eop == CEPH_OSD_OP_DELETE &&
      oi.snapset.clones.size()) {
    dout(10) << " munging DELETE -> TRUNCATE 0 bc of clones " << oi.snapset.clones << dendl;
    eop = CEPH_OSD_OP_TRUNCATE;
    op.offset = 0;
    oi.snapset.head_exists = false;
  }

  switch (eop) {

    // -- locking --
  case CEPH_OSD_OP_WRLOCK:
    oi.wrlock_by = reqid;
    break;

  case CEPH_OSD_OP_WRUNLOCK:
    oi.wrlock_by = osd_reqid_t();
    break;

  case CEPH_OSD_OP_BALANCEREADS:
    {
      bool bal = true;
      t.setattr(info.pgid.to_coll(), soid, "balance-reads", &bal, sizeof(bal));
    }
    break;
  case CEPH_OSD_OP_UNBALANCEREADS:
    {
      t.rmattr(info.pgid.to_coll(), soid, "balance-reads");
    }
    break;


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
	add_interval_usage(ch, st);
      }
      if (op.offset + op.length > old_size) {
	__u64 new_size = op.offset + op.length;
	st.num_bytes += new_size - old_size;
	st.num_kb += SHIFT_ROUND_UP(new_size, 10) - SHIFT_ROUND_UP(old_size, 10);
	old_size = new_size;
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
	old_size = 0;
      }
      if (op.length != old_size) {
	st.num_bytes -= old_size;
	st.num_kb -= SHIFT_ROUND_UP(old_size, 10);
	st.num_bytes += op.length;
	st.num_kb += SHIFT_ROUND_UP(op.length, 10);
	old_size = op.length;
      }
      oi.snapset.head_exists = true;
    }
    break;

  case CEPH_OSD_OP_ZERO:
    { // zero
      assert(op.length);
      if (!exists)
	t.touch(info.pgid.to_coll(), soid);
      t.zero(info.pgid.to_coll(), soid, op.offset, op.length);
      if (oi.snapset.clones.size()) {
	snapid_t newest = *oi.snapset.clones.rbegin();
	interval_set<__u64> ch;
	ch.insert(op.offset, op.length);
	ch.intersection_of(oi.snapset.clone_overlap[newest]);
	oi.snapset.clone_overlap[newest].subtract(ch);
	add_interval_usage(ch, st);
      }
      oi.snapset.head_exists = true;
    }
    break;

  case CEPH_OSD_OP_TRUNCATE:
    { // truncate
      if (!exists)
	t.touch(info.pgid.to_coll(), soid);
      t.truncate(info.pgid.to_coll(), soid, op.offset);
      if (oi.snapset.clones.size()) {
	snapid_t newest = *oi.snapset.clones.rbegin();
	interval_set<__u64> trim;
	if (old_size > op.offset) {
	  trim.insert(op.offset, old_size-op.offset);
	  trim.intersection_of(oi.snapset.clone_overlap[newest]);
	  add_interval_usage(trim, st);
	}
	interval_set<__u64> keep;
	if (op.offset)
	  keep.insert(0, op.offset);
	oi.snapset.clone_overlap[newest].intersection_of(keep);
      }
      if (op.offset != old_size) {
	st.num_bytes -= old_size;
	st.num_kb -= SHIFT_ROUND_UP(old_size, 10);
	st.num_bytes += op.offset;
	st.num_kb += SHIFT_ROUND_UP(op.offset, 10);
	old_size = op.offset;
      }
      // do no set head_exists, or we will break above DELETE -> TRUNCATE munging.
    }
    break;
    
  case CEPH_OSD_OP_DELETE:
    { // delete
      t.remove(info.pgid.to_coll(), soid);
      if (oi.snapset.clones.size()) {
	snapid_t newest = *oi.snapset.clones.rbegin();
	add_interval_usage(oi.snapset.clone_overlap[newest], st);
	oi.snapset.clone_overlap.erase(newest);  // ok, redundant.
      }
      if (exists) {
	st.num_objects--;
	st.num_bytes -= old_size;
	st.num_kb -= SHIFT_ROUND_UP(old_size, 10);
	old_size = 0;
	exists = false;
	oi.snapset.head_exists = false;
      }      
    }
    break;
    

    // -- object attrs --

  case CEPH_OSD_OP_SETXATTR:
    {
      if (!exists)
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
      vector<ceph_osd_op> nops(1);
      ceph_osd_op& newop = nops[0];
      newop.op = CEPH_OSD_OP_WRITE;
      newop.offset = old_size;
      newop.length = op.length;
      prepare_simple_op(t, reqid, st, soid, old_size, exists, oi, nops, 0, bp, snapc);
    }
    break;

  case CEPH_OSD_OP_STARTSYNC:
    t.start_sync();
    break;

  case CEPH_OSD_OP_SETTRUNC:
    if (opn > 0 && ops[opn-1].op == CEPH_OSD_OP_WRITE) {
      // set truncate seq over preceeding write's range
      ceph_osd_op& wr = ops[opn-1];

      __u32 seq = 0;
      interval_set<__u64> tm;
      bufferlist::iterator p;
      if (oi.truncate_info.length()) {
	p = oi.truncate_info.begin();
	::decode(seq, p);
      }
      if (seq < op.truncate_seq) {
	seq = op.truncate_seq;
	tm.insert(wr.offset, wr.length);
      } else {
	if (oi.truncate_info.length())
	  ::decode(tm, p);
	interval_set<__u64> n;
	n.insert(wr.offset, wr.length);
	tm.union_of(n);
      }
      dout(10) << " settrunc seq " << seq << " map " << tm << dendl;
      oi.truncate_info.clear();
      ::encode(seq, oi.truncate_info);
      ::encode(tm, oi.truncate_info);
    }
    break;

  case CEPH_OSD_OP_TRIMTRUNC:
    if (exists) {
      __u32 old_seq = 0;
      bufferlist::iterator p;
      if (oi.truncate_info.length()) {
	p = oi.truncate_info.begin();
	::decode(old_seq, p);
      }
      
      if (op.truncate_seq > old_seq) {
	// just truncate/delete.
	vector<ceph_osd_op> nops(1);
	ceph_osd_op& newop = nops[0];
	newop.op = CEPH_OSD_OP_TRUNCATE;
	newop.offset = op.truncate_size;
	dout(10) << " seq " << op.truncate_seq << " > old_seq " << old_seq
		 << ", truncating with " << newop << dendl;
	prepare_simple_op(t, reqid, st, soid, old_size, exists, oi, nops, 0, bp, snapc);
      } else {
	// do smart truncate
	interval_set<__u64> tm;
	::decode(tm, p);

	interval_set<__u64> zero;
	zero.insert(0, old_size);
	tm.intersection_of(zero);
	zero.subtract(tm);
	
	dout(10) << " seq " << op.truncate_seq << " == old_seq " << old_seq
		 << ", tm " << tm << ", zeroing " << zero << dendl;
	for (map<__u64,__u64>::iterator p = zero.m.begin();
	     p != zero.m.end();
	     p++) {
	  vector<ceph_osd_op> nops(1);
	  ceph_osd_op& newop = nops[0];
	  newop.op = CEPH_OSD_OP_ZERO;
	  newop.offset = p->first;
	  newop.length = p->second;
	  prepare_simple_op(t, reqid, st, soid, old_size, exists, oi, nops, 0, bp, snapc);
	}

	oi.truncate_info.clear();
      }
    }
    break;

  default:
    return -EINVAL;
  }

  if (!exists && oi.snapset.head_exists) {
    st.num_objects++;
    exists = true;
  }

  return 0;
}

void ReplicatedPG::prepare_transaction(ObjectStore::Transaction& t, osd_reqid_t reqid,
				       sobject_t soid,
				       vector<ceph_osd_op>& ops, bufferlist& bl, utime_t mtime,
				       bool& exists, __u64& size, object_info_t& oi,
				       eversion_t at_version, SnapContext& snapc,
				       eversion_t trim_to)
{
  bufferlist log_bl;
  eversion_t log_version = at_version;
  assert(!ops.empty());
  
  eversion_t old_version = oi.version;

  // apply ops
  bool did_snap = false;
  bufferlist::iterator bp = bl.begin();
  for (unsigned i=0; i<ops.size(); i++) {
    // clone?
    if (!did_snap && soid.snap &&
	!ceph_osd_op_type_lock(ops[i].op)) {     // is a (non-lock) modification
      prepare_clone(t, log_bl, reqid, info.stats, soid, size, oi,
		    at_version, snapc);
      did_snap = true;
    }
    prepare_simple_op(t, reqid, info.stats, soid, size, exists, oi,
		      ops, i, bp, snapc);
  }

  // finish.
  oi.version = at_version;
  if (exists) {
    oi.version = at_version;
    oi.prior_version = old_version;
    oi.last_reqid = reqid;
    if (mtime != utime_t()) {
      oi.mtime = mtime;
      dout(10) << " set mtime to " << oi.mtime << dendl;
    } else {
      dout(10) << " mtime unchanged at " << oi.mtime << dendl;
    }

    bufferlist bv(sizeof(oi));
    ::encode(oi, bv);
    t.setattr(info.pgid.to_coll(), soid, OI_ATTR, bv);
  }

  // append to log
  int logopcode = Log::Entry::MODIFY;
  if (!exists)
    logopcode = Log::Entry::DELETE;
  Log::Entry logentry(logopcode, soid, at_version, old_version, reqid, mtime);
  add_log_entry(logentry, log_bl);

  // write pg info, log to disk
  write_info(t);
  append_log(t, log_bl, log_version, trim_to);
}







// ========================================================================
// rep op gather

class C_OSD_ModifyCommit : public Context {
public:
  ReplicatedPG *pg;
  ReplicatedPG::RepGather *repop;

  C_OSD_ModifyCommit(ReplicatedPG *p, ReplicatedPG::RepGather *rg) :
    pg(p), repop(rg) {
    repop->get();
    pg->get();    // we're copying the pointer
  }
  void finish(int r) {
    pg->lock();
    if (!pg->is_deleted()) 
      pg->op_modify_ondisk(repop);
    repop->put();
    pg->unlock();
    pg->put();
  }
};

/** op_modify_commit
 * transaction commit on the acker.
 */
void ReplicatedPG::op_modify_ondisk(RepGather *repop)
{
  if (repop->aborted) {
    dout(10) << "op_modify_ondisk " << *repop << " -- aborted" << dendl;
  } else if (repop->waitfor_disk.count(osd->get_nodeid()) == 0) {
    dout(10) << "op_modify_ondisk " << *repop << " -- already marked ondisk" << dendl;
  } else {
    dout(10) << "op_modify_ondisk " << *repop << dendl;
    repop->waitfor_disk.erase(osd->get_nodeid());
    repop->waitfor_nvram.erase(osd->get_nodeid());
    repop->pg_complete_thru[osd->get_nodeid()] = repop->pg_local_last_complete;
    eval_repop(repop);
  }
}


void ReplicatedPG::apply_repop(RepGather *repop)
{
  dout(10) << "apply_repop  applying update on " << *repop << dendl;
  assert(!repop->applied);

  Context *oncommit = new C_OSD_ModifyCommit(this, repop);
  unsigned r = osd->store->apply_transaction(repop->t, oncommit);
  if (r)
    dout(-10) << "apply_repop  apply transaction return " << r << " on " << *repop << dendl;
  
  // discard my reference to the buffer
  repop->op->get_data().clear();
  
  repop->applied = true;
  
  put_projected_object(repop->pinfo);

  update_stats();

  // any completion stuff to do here?
  sobject_t soid(repop->op->get_oid(), CEPH_NOSNAP);
  ceph_osd_op& first = repop->op->ops[0];

  switch (first.op) { 
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
  
  // disk?
  if (repop->can_send_disk()) {
    if (repop->op->wants_ondisk()) {
      // send commit.
      MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ONDISK);
      dout(10) << " sending commit on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, repop->op->get_orig_source_inst());
      repop->sent_disk = true;
    }
  }

  // nvram?
  else if (repop->can_send_nvram()) {
    if (repop->op->wants_onnvram()) {
      // send commit.
      MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ONNVRAM);
      dout(10) << " sending onnvram on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, repop->op->get_orig_source_inst());
      repop->sent_nvram = true;
    }
  }

  // ack?
  else if (repop->can_send_ack()) {
    // apply
    if (!repop->applied)
      apply_repop(repop);

    if (repop->op->wants_ack()) {
      // send ack
      MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK);
      dout(10) << " sending ack on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, repop->op->get_orig_source_inst());
      repop->sent_ack = true;
    }

    utime_t now = g_clock.now();
    now -= repop->start;
    osd->logger->finc(l_osd_rlsum, now);
    osd->logger->inc(l_osd_rlnum, 1);
  }

  // done.
  if (repop->can_delete()) {
    // adjust peers_complete_thru
    if (!repop->pg_complete_thru.empty()) {
      eversion_t min = info.last_complete;  // hrm....
      for (unsigned i=0; i<acting.size(); i++) {
        if (repop->pg_complete_thru[acting[i]] < min)      // note: if we haven't heard, it'll be zero, which is what we want.
          min = repop->pg_complete_thru[acting[i]];
      }
      
      if (min > peers_complete_thru) {
        dout(10) << " peers_complete_thru " 
		 << peers_complete_thru << " -> " << min
		 << dendl;
        peers_complete_thru = min;
      }
    }

    dout(10) << " removing " << *repop << dendl;
    assert(!repop_queue.empty());
    assert(repop_queue.front() == repop);
    repop_queue.pop_front();
    repop_map.erase(repop->rep_tid);
    repop->put();
  }
}


void ReplicatedPG::issue_repop(RepGather *repop, int dest, utime_t now)
{
  sobject_t soid(repop->op->get_oid(), CEPH_NOSNAP);
  dout(7) << " issue_repop rep_tid " << repop->rep_tid
          << " o " << soid
          << " to osd" << dest
          << dendl;
  
  // forward the write/update/whatever
  int acks_wanted = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;
  MOSDSubOp *wr = new MOSDSubOp(repop->op->get_reqid(), info.pgid, soid,
				repop->op->ops, repop->noop, acks_wanted,
				osd->osdmap->get_epoch(), 
				repop->rep_tid, repop->at_version);
  wr->mtime = repop->mtime;
  wr->old_exists = repop->pinfo->exists;
  wr->old_size = repop->pinfo->size;
  wr->old_version = repop->pinfo->oi.version;
  wr->snapset = repop->pinfo->oi.snapset;
  wr->snapc = repop->snapc;
  wr->get_data() = repop->op->get_data();   // _copy_ bufferlist
  if (osd->osdmap->get_pg_size(info.pgid) == acting.size())
    wr->pg_trim_to = peers_complete_thru;
  wr->peer_stat = osd->get_my_stat_for(now, dest);
  osd->messenger->send_message(wr, osd->osdmap->get_inst(dest));
}

ReplicatedPG::RepGather *ReplicatedPG::new_repop(MOSDOp *op, bool noop,
						 tid_t rep_tid, 
						 ProjectedObjectInfo *pinfo,
						 eversion_t nv,
						 SnapContext& snapc)
{
  dout(10) << "new_repop rep_tid " << rep_tid << " on " << *op << dendl;

  RepGather *repop = new RepGather(op, noop, rep_tid, 
				   pinfo,
				   nv, info.last_complete,
				   snapc);

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
			     int fromosd, eversion_t pg_complete_thru)
{
  MOSDOp *op = repop->op;

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
      repop->pg_complete_thru[fromosd] = pg_complete_thru;
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


ReplicatedPG::ProjectedObjectInfo *ReplicatedPG::get_projected_object(pobject_t poid)
{
  ProjectedObjectInfo *pinfo = &projected_objects[poid];
  pinfo->ref++;

  if (pinfo->ref > 1) {
    dout(10) << "get_projected_object " << poid << " "
	     << (pinfo->ref-1) << " -> " << pinfo->ref << dendl;
    return pinfo;    // already had it
  }

  // pull info off disk
  pinfo->poid = poid;
    
  struct stat st;
  int r = osd->store->stat(info.pgid.to_coll(), poid, &st);
  if (r == 0) {
    pinfo->exists = true;
    pinfo->size = st.st_size;
    
    bufferlist bv;
    r = osd->store->getattr(info.pgid.to_coll(), poid, OI_ATTR, bv);
    assert(r >= 0);
    pinfo->oi.decode(bv);
  } else {
    pinfo->oi.poid = poid;
    pinfo->exists = false;
    pinfo->size = 0;
  }

  dout(10) << "get_projected_object " << poid << " read " << pinfo->oi << dendl;
  return pinfo;
}

void ReplicatedPG::put_projected_object(ProjectedObjectInfo *pinfo)
{
  dout(10) << "put_projected_object " << pinfo->poid << " "
	   << pinfo->ref << " -> " << (pinfo->ref-1) << dendl;

  --pinfo->ref;
  if (pinfo->ref == 0) {
    projected_objects.erase(pinfo->poid);

    if (projected_objects.empty())
      kick();
  }
}




void ReplicatedPG::op_modify(MOSDOp *op)
{
  int whoami = osd->get_nodeid();
  sobject_t soid(op->get_oid(), CEPH_NOSNAP);

  // balance-reads set?
#if 0
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
    return;
  }
#endif

  // get existing object info
  ProjectedObjectInfo *pinfo = get_projected_object(soid);

  // --- locking ---

  // wrlock?
  if (!op->ops.empty() &&  // except noop; we just want to flush
      block_if_wrlocked(op, pinfo->oi)) {
    put_projected_object(pinfo);
    return; // op will be handled later, after the object unlocks
  }

  // dup op?
  bool noop = false;
  const char *opname;
  if (op->ops.empty()) {
    opname = "no-op";
    noop = true;
  } else if (is_dup(op->get_reqid())) {
    dout(3) << "op_modify " << op->ops << " dup op " << op->get_reqid()
             << ", doing WRNOOP" << dendl;
    opname = "no-op";
    noop = true;
  } else
    opname = ceph_osd_op_name(op->ops[0].op);


  // version
  eversion_t at_version = log.top;
  if (!noop) {
    at_version.epoch = osd->osdmap->get_epoch();
    at_version.version++;
    assert(at_version > info.last_update);
    assert(at_version > log.top);
  }

  // snap
  SnapContext snapc;
  snapc.seq = op->get_snap_seq();
  snapc.snaps = op->get_snaps();

  // set version in op, for benefit of client and our eventual reply
  op->set_version(at_version);

  dout(10) << "op_modify " << opname 
           << " " << soid
           << " ov " << pinfo->oi.version << " av " << at_version 
	   << " snapc " << snapc
	   << " snapset " << pinfo->oi.snapset
           << dendl;  

  // verify snap ordering
  if ((op->get_flags() & CEPH_OSD_FLAG_ORDERSNAP) &&
      snapc.seq < pinfo->oi.snapset.seq) {
    dout(10) << " ORDERSNAP flag set and snapc seq " << snapc.seq << " < snapset seq " << pinfo->oi.snapset.seq
	     << " on " << soid << dendl;
    osd->reply_op_error(op, -EOLDSNAPC);
    return;
  }

  // are any peers missing this?
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    if (peer_missing.count(peer) &&
        peer_missing[peer].is_missing(soid)) {
      // push it before this update. 
      // FIXME, this is probably extra much work (eg if we're about to overwrite)
      push_to_replica(soid, peer);
      osd->start_recovery_op(this, 1);
    }
  }

  if (op->get_data().length()) {
    osd->logger->inc(l_osd_c_wr);
    osd->logger->inc(l_osd_c_wrb, op->get_data().length());
  }

  // note my stats
  utime_t now = g_clock.now();

  // issue replica writes
  tid_t rep_tid = osd->get_tid();
  RepGather *repop = new_repop(op, noop, rep_tid, pinfo, at_version, snapc);
  for (unsigned i=1; i<acting.size(); i++)
    issue_repop(repop, acting[i], now);
								
  eversion_t old_last_update = at_version;
	
  // trim log?
  eversion_t trim_to = is_clean() ? peers_complete_thru : eversion_t();
  if (log.top.version - log.bottom.version > info.stats.num_objects)
    trim_to = peers_complete_thru;

  // we are acker.
  if (!noop) {
    // log and update later.
    prepare_transaction(repop->t, op->get_reqid(), soid, op->ops, op->get_data(), repop->mtime,
			pinfo->exists, pinfo->size, pinfo->oi,
			at_version, snapc,
			trim_to);
  }
  
  // keep peer_info up to date
  for (unsigned i=1; i<acting.size(); i++) {
    Info &in = peer_info[acting[i]];
    in.last_update = at_version;
    if (in.last_complete == old_last_update)
      in.last_update = at_version;
  }

  // (logical) local ack.
  // (if alone, this will apply the update.)
  assert(repop->waitfor_ack.count(whoami));
  repop->waitfor_ack.erase(whoami);
  eval_repop(repop);
  repop->put();
}





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
  sobject_t soid = op->poid;

  const char *opname;
  if (op->noop)
    opname = "no-op";
  else
    opname = ceph_osd_op_name(op->ops[0].op);

  dout(10) << "sub_op_modify " << opname 
           << " " << soid 
           << " v " << op->version
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

  // prepare our transaction
  ObjectStore::Transaction t;

  // do op
  int ackerosd = acting[0];
  osd->logger->inc(l_osd_r_wr);
  osd->logger->inc(l_osd_r_wrb, op->get_data().length());
  
  if (!op->noop) {
    object_info_t oi(op->poid);
    oi.version = op->old_version;
    oi.snapset = op->snapset;
    prepare_transaction(t, op->reqid,
			op->poid, op->ops, op->get_data(), op->mtime,
			op->old_exists, op->old_size, oi, op->version,
			op->snapc, op->pg_trim_to);
  }
  
  C_OSD_RepModifyCommit *oncommit = new C_OSD_RepModifyCommit(this, op, ackerosd, info.last_complete);
  
  // apply log update. and possibly update itself.
  unsigned tr = osd->store->apply_transaction(t, oncommit);
  if (tr != 0 &&   // no errors
      tr != 2) {   // or error on collection_add
    derr(0) << "error applying transaction: r = " << tr << dendl;
    assert(tr == 0);
  }
  
  // send ack to acker
  MOSDSubOpReply *ack = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK);
  ack->set_peer_stat(osd->get_my_stat_for(g_clock.now(), ackerosd));
  osd->messenger->send_message(ack, osd->osdmap->get_inst(ackerosd));
  
  // ack myself.
  oncommit->ack(); 
}

void ReplicatedPG::sub_op_modify_ondisk(MOSDSubOp *op, int ackerosd, eversion_t last_complete)
{
  // send commit.
  dout(10) << "rep_modify_commit on op " << *op
           << ", sending commit to osd" << ackerosd
           << dendl;
  if (osd->osdmap->is_up(ackerosd)) {
    MOSDSubOpReply *commit = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ONDISK);
    commit->set_pg_complete_thru(last_complete);
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
	      r->get_pg_complete_thru());
  }

  delete r;
}










// ===========================================================

void ReplicatedPG::calc_head_subsets(SnapSet& snapset, pobject_t head,
				     Missing& missing,
				     interval_set<__u64>& data_subset,
				     map<pobject_t, interval_set<__u64> >& clone_subsets)
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
    pobject_t c = head;
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

void ReplicatedPG::calc_clone_subsets(SnapSet& snapset, sobject_t soid,
				      Missing& missing,
				      interval_set<__u64>& data_subset,
				      map<pobject_t, interval_set<__u64> >& clone_subsets)
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
    pobject_t c = soid;
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
bool ReplicatedPG::pull(sobject_t soid)
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

  map<pobject_t, interval_set<__u64> > clone_subsets;
  interval_set<__u64> data_subset;

  // is this a snapped object?  if so, consult the snapset.. we may not need the entire object!
  if (soid.snap < CEPH_NOSNAP) {
    pobject_t head = soid;
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
    // pulling head.
    // always pull the whole thing.
  }

  // send op
  osd_reqid_t rid;
  tid_t tid = osd->get_tid();
  vector<ceph_osd_op> pull(1);
  pull[0].op = CEPH_OSD_OP_PULL;
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, soid, pull, false, CEPH_OSD_FLAG_ACK,
				   osd->osdmap->get_epoch(), tid, v);
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
void ReplicatedPG::push_to_replica(sobject_t soid, int peer)
{
  dout(10) << "push_to_replica " << soid << " osd" << peer << dendl;

  // get size
  struct stat st;
  int r = osd->store->stat(info.pgid.to_coll(), soid, &st);
  assert(r == 0);
  
  map<pobject_t, interval_set<__u64> > clone_subsets;
  interval_set<__u64> data_subset;

  bufferlist bv;
  r = osd->store->getattr(info.pgid.to_coll(), soid, OI_ATTR, bv);
  assert(r >= 0);
  object_info_t oi(bv);
  
  // are we doing a clone on the replica?
  if (soid.snap < CEPH_NOSNAP) {	
    pobject_t head = soid;
    head.snap = CEPH_NOSNAP;
    if (peer_missing[peer].is_missing(head) &&
	peer_missing[peer].have_old(head) == oi.prior_version) {
      dout(10) << "push_to_replica osd" << peer << " has correct old " << head
	       << " v" << oi.prior_version 
	       << ", pushing " << soid << " attrs as a clone op" << dendl;
      interval_set<__u64> data_subset;
      map<pobject_t, interval_set<__u64> > clone_subsets;
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
    // pushing head.
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
void ReplicatedPG::push(sobject_t soid, int peer)
{
  interval_set<__u64> subset;
  map<pobject_t, interval_set<__u64> > clone_subsets;
  push(soid, peer, subset, clone_subsets);
}

void ReplicatedPG::push(sobject_t soid, int peer, 
			interval_set<__u64> &data_subset,
			map<pobject_t, interval_set<__u64> >& clone_subsets)
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
  vector<ceph_osd_op> push(1);
  push[0].op = CEPH_OSD_OP_PUSH;
  push[0].offset = 0;
  push[0].length = size;
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, soid, push, false, 0,
				   osd->osdmap->get_epoch(), osd->get_tid(), oi.version);
  subop->data_subset.swap(data_subset);
  subop->clone_subsets.swap(clone_subsets);
  subop->set_data(bl);   // note: claims bl, set length above here!
  subop->attrset.swap(attrset);
  osd->messenger->send_message(subop, osd->osdmap->get_inst(peer));
  
  if (is_primary()) {
    peer_missing[peer].got(soid, oi.version);
    pushing[soid].insert(peer);
  }
}

void ReplicatedPG::sub_op_push_reply(MOSDSubOpReply *reply)
{
  dout(10) << "sub_op_push_reply from " << reply->get_source() << " " << *reply << dendl;
  
  int peer = reply->get_source().num();
  sobject_t soid = reply->get_poid();
  
  if (pushing.count(soid) &&
      pushing[soid].count(peer)) {
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
  } else {
    dout(10) << "huh, i wasn't pushing " << soid << dendl;
  }
  delete reply;
}


/** op_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void ReplicatedPG::sub_op_pull(MOSDSubOp *op)
{
  const pobject_t soid = op->poid;
  const eversion_t v = op->version;

  dout(7) << "op_pull " << soid << " v " << op->version
          << " from " << op->get_source()
          << dendl;

  assert(!is_primary());  // we should be a replica or stray.

  push(soid, op->get_source().num(), op->data_subset, op->clone_subsets);
  delete op;
}


/** op_push
 * NOTE: called from opqueue.
 */
void ReplicatedPG::sub_op_push(MOSDSubOp *op)
{
  sobject_t soid = op->poid;
  eversion_t v = op->version;
  ceph_osd_op& push = op->ops[0];

  dout(7) << "op_push " 
          << soid 
          << " v " << v 
	  << " len " << push.length
	  << " data_subset " << op->data_subset
	  << " clone_subsets " << op->clone_subsets
	  << " data len " << op->get_data().length()
          << dendl;

  interval_set<__u64> data_subset;
  map<pobject_t, interval_set<__u64> > clone_subsets;

  // are we missing (this specific version)?
  //  (if version is wrong, it is either old (we don't want it) or 
  //   newer (peering is buggy))
  if (!missing.is_missing(soid, v)) {
    dout(7) << "sub_op_push not missing " << soid << " v" << v << dendl;
    dout(15) << " but i AM missing " << missing.missing << dendl;
    delete op;
    return;
  }

  bufferlist data;
  data.claim(op->get_data());

  // determine data/clone subsets
  data_subset = op->data_subset;
  if (data_subset.empty() && push.length && push.length == data.length())
    data_subset.insert(0, push.length);
  clone_subsets = op->clone_subsets;

  if (is_primary()) {
    if (soid.snap < CEPH_NOSNAP) {
      // clone.  make sure we have enough data.
      pobject_t head = soid;
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
      // head. for now, primary will _only_ pull full copies of the head.
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
  for (map<pobject_t, interval_set<__u64> >::iterator p = clone_subsets.begin();
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
  if (soid.snap != CEPH_NOSNAP &&
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

  // apply to disk!
  write_info(t);
  unsigned r = osd->store->apply_transaction(t);
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

  delete op;

  // kick waiters
  if (waiting_for_missing_object.count(soid)) {
    osd->take_waiters(waiting_for_missing_object[soid]);
    waiting_for_missing_object.erase(soid);
  }
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
      dout(10) << " requeuing " << *repop->op << dendl;
      rq.push_back(repop->op);
      repop->op = 0;
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







void ReplicatedPG::cancel_recovery()
{
  dout(10) << "cancel_recovery" << dendl;

  // forget about where missing items are, or anything we're pulling
  missing_loc.clear();
  osd->num_pulling -= pulling.size();
  pulling.clear();
  pushing.clear();
  log.reset_recovery_pointers();

  osd->finish_recovery_op(this, recovery_ops_active, true);
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

  dout(-10) << "recover_primary pulling " << pulling.size() << " in pg, "
           << osd->num_pulling << "/" << g_conf.osd_max_pull << " total"
           << dendl;
  dout(10) << "recover_primary " << missing << dendl;
  dout(15) << "recover_primary " << missing.missing << dendl;

  // can we slow down on this PG?
  if (osd->num_pulling >= g_conf.osd_max_pull && !pulling.empty()) {
    dout(-10) << "recover_primary already pulling max, waiting" << dendl;
    return 0;
  }

  // look at log!
  Log::Entry *latest = 0;
  int started = 0;
  int skipped = 0;

  map<sobject_t, Missing::item>::iterator p = missing.missing.lower_bound(log.last_requested);
  while (p != missing.missing.end()) {
    assert(log.objects.count(p->first));
    latest = log.objects[p->first];
    assert(latest);

    sobject_t soid(latest->soid);
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
	    vector<snapid_t> snaps;
	    ::decode(snaps, latest->snaps);
	    ObjectStore::Transaction t;
	    _make_clone(t, head, soid, latest->prior_version, latest->version, latest->reqid, latest->mtime, snaps);
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

  if (is_all_uptodate()) 
    finish_recovery();
  else {
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
    vector<pobject_t> ls;
    osd->store->collection_list(info.pgid.to_coll(), ls);
    if (ls.size() != info.stats.num_objects)
      dout(10) << " WARNING: " << ls.size() << " != num_objects " << info.stats.num_objects << dendl;

    set<sobject_t> s;
    
    for (vector<pobject_t>::iterator i = ls.begin();
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


int ReplicatedPG::_scrub(ScrubMap& scrubmap)
{
  int errors = 0;
  dout(10) << "_scrub" << dendl;

  coll_t c = info.pgid.to_coll();

  // traverse in reverse order.
  sobject_t head;
  SnapSet snapset;
  unsigned curclone = 0;

  pg_stat_t stat;

  bufferlist last_data;

  for (vector<ScrubMap::object>::reverse_iterator p = scrubmap.objects.rbegin(); 
       p != scrubmap.objects.rend(); 
       p++) {
    sobject_t soid = p->poid;
    stat.num_objects++;

    // basic checks.
    if (p->attrs.count(OI_ATTR) == 0) {
      dout(0) << "scrub no '" << OI_ATTR << "' attr on " << soid << dendl;
      errors++;
      continue;
    }
    bufferlist bv;
    bv.push_back(p->attrs[OI_ATTR]);
    object_info_t oi(bv);

    dout(20) << "scrub  " << soid << " " << oi << dendl;

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
  
  dout(10) << "scrub got "
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
    ss << info.pgid << " scrub stat mismatch, got "
       << stat.num_objects << "/" << info.stats.num_objects << " objects, "
       << stat.num_object_clones << "/" << info.stats.num_object_clones << " clones, "
       << stat.num_bytes << "/" << info.stats.num_bytes << " bytes, "
       << stat.num_kb << "/" << info.stats.num_kb << " kb.";
    osd->get_logclient()->log(LOG_ERROR, ss);
    errors++;
  }

  dout(10) << "scrub finish" << dendl;
  return errors;
}
