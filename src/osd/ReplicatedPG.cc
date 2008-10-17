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

#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) *_dout << dbeginl << g_clock.now() << " osd" << osd->get_nodeid() << " " << (osd->osdmap ? osd->osdmap->get_epoch():0) << " " << *this << " "
#define  derr(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) *_derr << dbeginl << g_clock.now() << " osd" << osd->get_nodeid() << " " << (osd->osdmap ? osd->osdmap->get_epoch():0) << " " << *this << " "

#include <errno.h>
#include <sys/stat.h>

static const int LOAD_LATENCY    = 1;
static const int LOAD_QUEUE_SIZE = 2;
static const int LOAD_HYBRID     = 3;


// =======================
// pg changes

bool ReplicatedPG::same_for_read_since(epoch_t e)
{
  return (e >= info.history.same_acker_since);
}

bool ReplicatedPG::same_for_modify_since(epoch_t e)
{
  return (e >= info.history.same_primary_since);
}

bool ReplicatedPG::same_for_rep_modify_since(epoch_t e)
{
  // check osd map: same set, or primary+acker?
  return (e >= info.history.same_primary_since &&
	  e >= info.history.same_acker_since);    
}

// ====================
// missing objects

bool ReplicatedPG::is_missing_object(object_t oid)
{
  return missing.missing.count(oid);
}
 

void ReplicatedPG::wait_for_missing_object(object_t oid, Message *m)
{
  assert(is_missing_object(oid));

  pobject_t poid(info.pgid.pool(), 0, oid);

  // we don't have it (yet).
  eversion_t v = missing.missing[oid];
  if (pulling.count(oid)) {
    dout(7) << "missing "
	    << poid 
	    << " v " << v
	    << ", already pulling"
	    << dendl;
  } else {
    dout(7) << "missing " 
	    << poid 
	    << " v " << v
	    << ", pulling"
	    << dendl;
    pull(poid);
  }
  waiting_for_missing_object[oid].push_back(m);
}


// ==========================================================

/** preprocess_op - preprocess an op (before it gets queued).
 * fasttrack read
 */
bool ReplicatedPG::preprocess_op(MOSDOp *op, utime_t now)
{
  // we only care about reads here on out..
  if (!op->is_read()) 
    return false;

  object_t oid = op->get_oid();
  pobject_t poid(info.pgid.pool(), 0, oid);

  // -- load balance reads --
  if (is_primary() &&
      g_conf.osd_rep == OSD_REP_PRIMARY) {
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
      if (stat_object_temp_rd.count(oid))
	temp = stat_object_temp_rd[oid].get(op->get_recv_stamp());
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
	  osd->store->getattr(info.pgid.to_coll(), poid, "balance-reads", &b, 1) >= 0)
	is_balanced = true;
      
      if (!is_balanced && should_balance &&
	  balancing_reads.count(oid) == 0) {
	dout(-10) << "preprocess_op balance-reads on " << oid << dendl;
	balancing_reads.insert(oid);
	ceph_object_layout layout;
	layout.ol_pgid = info.pgid.u.pg64;
	layout.ol_stripe_unit = 0;
	MOSDOp *pop = new MOSDOp(0, osd->get_tid(),
				 oid,
				 layout,
				 osd->osdmap->get_epoch(),
				 CEPH_OSD_OP_BALANCEREADS, 0);
	do_op(pop);
      }
      if (is_balanced && !should_balance &&
	  !unbalancing_reads.count(oid) == 0) {
	dout(-10) << "preprocess_op unbalance-reads on " << oid << dendl;
	unbalancing_reads.insert(oid);
	ceph_object_layout layout;
	layout.ol_pgid = info.pgid.u.pg64;
	layout.ol_stripe_unit = 0;
	MOSDOp *pop = new MOSDOp(0, osd->get_tid(),
				 oid,
				 layout,
				 osd->osdmap->get_epoch(),
				 CEPH_OSD_OP_UNBALANCEREADS, 0);
	do_op(pop);
      }
    }
    
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
	osd->logger->inc("shdout");
	return true;
      }
    }
  } // endif balance reads


  // -- fastpath read?
  // if this is a read and the data is in the cache, do an immediate read.. 
  if ( g_conf.osd_immediate_read_from_cache ) {
    if (osd->store->is_cached(info.pgid.to_coll(), poid,
			      op->get_offset(), 
			      op->get_length()) == 0) {
      if (!is_primary() && !op->get_source().is_osd()) {
	// am i allowed?
	bool v;
	if (osd->store->getattr(info.pgid.to_coll(), poid, "balance-reads", &v, 1) < 0) {
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

  osd->logger->inc("op");

  if (ceph_osd_op_is_read(op->get_op()))
    op_read(op);
  else
    op_modify(op);
}


void ReplicatedPG::do_sub_op(MOSDSubOp *op)
{
  dout(15) << "do_sub_op " << *op << dendl;

  osd->logger->inc("subop");

  switch (op->op) {
    // rep stuff
  case CEPH_OSD_OP_PULL:
    sub_op_pull(op);
    break;
  case CEPH_OSD_OP_PUSH:
    sub_op_push(op);
    break;

  default:
    if (ceph_osd_op_is_modify(op->op) ||
	ceph_osd_op_is_lock(op->op))
      sub_op_modify(op);
    else
      assert(0);
  }

}

void ReplicatedPG::do_sub_op_reply(MOSDSubOpReply *r)
{
  if (r->get_op() == CEPH_OSD_OP_PUSH) {
    // continue peer recovery
    sub_op_push_reply(r);
  } else {
    sub_op_modify_reply(r);
  }
}


bool ReplicatedPG::snap_trimmer()
{
  lock();
  dout(10) << "snap_trimmer start" << dendl;
  
  state_clear(PG_STATE_SNAPTRIMQUEUE);
  state_set(PG_STATE_SNAPTRIMMING);
  update_stats();

  while (info.dead_snaps.size() &&
	 is_active()) {
    snapid_t sn = *info.dead_snaps.begin();
    coll_t c = info.pgid.to_snap_coll(sn);
    vector<pobject_t> ls;
    osd->store->collection_list(c, ls);

    dout(10) << "snap_trimmer collection " << c << " has " << ls.size() << " items" << dendl;

    for (vector<pobject_t>::iterator p = ls.begin(); p != ls.end(); p++) {
      pobject_t coid = *p;

      ObjectStore::Transaction t;

      // load clone snap list
      bufferlist bl;
      osd->store->getattr(info.pgid.to_coll(), coid, "snaps", bl);
      bufferlist::iterator blp = bl.begin();
      vector<snapid_t> snaps;
      ::decode(snaps, blp);

      // load head snapset	
      pobject_t head = coid;
      head.oid.snap = CEPH_NOSNAP;
      bl.clear();
      osd->store->getattr(info.pgid.to_coll(), head, "snapset", bl);
      blp = bl.begin();
      SnapSet snapset;
      ::decode(snapset, blp);
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
	snapid_t last = coid.oid.snap;
	vector<snapid_t>::iterator p;
	for (p = snapset.clones.begin(); p != snapset.clones.end(); p++)
	  if (*p == last)
	    break;
	if (p == snapset.clones.begin()) {
	  // newest clone.
	  snapset.head_overlap.intersection_of(snapset.clone_overlap[last]);
	} else  {
	  // older clone
	  vector<snapid_t>::iterator n = p;
	  n++;
	  if (n != snapset.clones.end())
	    // not oldest clone.
	    snapset.clone_overlap[*n].intersection_of(snapset.clone_overlap[*p]);
	}
	snapset.clones.erase(p);
	snapset.clone_overlap.erase(last);
      } else {
	// save adjusted snaps for this object
	dout(10) << coid << " snaps " << snaps << " -> " << newsnaps << dendl;
	bl.clear();
	::encode(newsnaps, bl);
	t.setattr(info.pgid.to_coll(), coid, "snaps", bl);

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
      } else {
	bl.clear();
	::encode(snapset, bl);
	t.setattr(info.pgid.to_coll(), head, "snapset", bl);
      }

      osd->store->apply_transaction(t);

      // give other threads a chance at this pg
      unlock();
      lock();
    }
    
    info.dead_snaps.erase(sn);
  }  

  // done
  dout(10) << "snap_trimmer done" << dendl;
  state_clear(PG_STATE_SNAPTRIMMING);
  update_stats();

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
bool ReplicatedPG::pick_read_snap(pobject_t& poid)
{
  pobject_t head = poid;
  head.oid.snap = CEPH_NOSNAP;

  SnapSet snapset;
  {
    bufferlist bl;
    int r = osd->store->getattr(info.pgid.to_coll(), head, "snapset", bl);
    if (r < 0)
      return false;  // if head doesn't exist, no snapped version will either.
    bufferlist::iterator p = bl.begin();
    ::decode(snapset, p);
  }

  dout(10) << "pick_read_snap " << poid << " snapset " << snapset << dendl;
  snapid_t want = poid.oid.snap;

  // head?
  if (want > snapset.seq) {
    if (snapset.head_exists) {
      dout(10) << "pick_read_snap  " << head
	       << " want " << want << " > snapset seq " << snapset.seq
	       << " -- HIT" << dendl;
      poid = head;
      return true;
    } else {
      dout(10) << "pick_read_snap  " << head
	       << " want " << want << " > snapset seq " << snapset.seq
	       << " but head_exists = false -- DNE" << dendl;
      return false;
    }
  }

  // which clone would it be?
  unsigned k = 0;
  while (k<snapset.clones.size() && snapset.clones[k] < want)
    k++;
  if (k == snapset.clones.size()) {
    dout(10) << "pick_read_snap  no clones with last >= want " << want << " -- DNE" << dendl;
    return false;
  }
  
  // check clone
  poid.oid.snap = snapset.clones[k];
  vector<snapid_t> snaps;
  {
    bufferlist bl;
    int r = osd->store->getattr(info.pgid.to_coll(), poid, "snaps", bl);
    if (r < 0) {
      dout(20) << "pick_read_snap  " << poid << " dne" << dendl;
      assert(0);
      return false;
    }
    bufferlist::iterator p = bl.begin();
    ::decode(snaps, p);
  }
  dout(20) << "pick_read_snap  " << poid << " snaps " << snaps << dendl;
  snapid_t first = snaps[snaps.size()-1];
  snapid_t last = snaps[0];
  assert(last == poid.oid.snap);
  if (first <= want) {
    dout(20) << "pick_read_snap  " << poid << " [" << first << "," << last << "] contains " << want << " -- HIT" << dendl;
    return true;
  }

  dout(20) << "pick_read_snap  " << poid << " [" << first << "," << last << "] does not contain " << want << " -- DNE" << dendl;
  return false;
} 


void ReplicatedPG::op_read(MOSDOp *op)
{
  object_t oid = op->get_oid();
  pobject_t poid(info.pgid.pool(), 0, oid);

  dout(10) << "op_read " << ceph_osd_op_name(op->get_op())
	   << " " << oid 
           << " " << op->get_offset() << "~" << op->get_length() 
           << dendl;
  
  // wrlocked?
  if (block_if_wrlocked(op)) 
    return;

  // !primary and unbalanced?
  //  (ignore ops forwarded from the primary)
  if (!is_primary()) {
    if (op->get_source().is_osd() &&
	op->get_source().num() == get_primary()) {
      // read was shed to me by the primary
      int from = op->get_source().num();
      osd->take_peer_stat(from, op->get_peer_stat());
      dout(10) << "read shed IN from " << op->get_source() 
		<< " " << op->get_reqid()
		<< ", me = " << osd->my_stat.read_latency_mine
		<< ", them = " << op->get_peer_stat().read_latency
		<< (osd->my_stat.read_latency_mine > op->get_peer_stat().read_latency ? " WTF":"")
		<< dendl;
      osd->logger->inc("shdin");

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
	osd->messenger->send_message(new MOSDPing(osd->osdmap->get_epoch(), osd->my_stat),
				     osd->osdmap->get_inst(from));
      }
    } else {
      // make sure i exist and am balanced, otherwise fw back to acker.
      bool b;
      if (!osd->store->exists(info.pgid.to_coll(), poid) || 
	  osd->store->getattr(info.pgid.to_coll(), poid, "balance-reads", &b, 1) < 0) {
	dout(-10) << "read on replica, object " << poid 
		  << " dne or no balance-reads, fw back to primary" << dendl;
	osd->messenger->forward_message(op, osd->osdmap->get_inst(get_acker()));
	return;
      }
    }
  }
  

  // set up reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), true); 
  long r = 0;

  // do it.
  if (poid.oid.snap && !pick_read_snap(poid)) {
    // we have no revision for this request.
    r = -ENOENT;
    goto done;
  } 
  
  // check inc_lock?
  if (op->get_inc_lock() > 0) {
    __u32 cur = 0;
    osd->store->getattr(info.pgid.to_coll(), poid, "inc_lock", &cur, sizeof(cur));
    if (cur > op->get_inc_lock()) {
      dout(10) << " inc_lock " << cur << " > " << op->get_inc_lock()
	       << " on " << poid << dendl;
      r = -EINCLOCKED;
      goto done;
    }
  }
  
  switch (op->get_op()) {
  case CEPH_OSD_OP_READ:
    {
      // read into a buffer
      bufferlist bl;
      r = osd->store->read(info.pgid.to_coll(), poid, 
			   op->get_offset(), op->get_length(),
			   bl);
      reply->set_data(bl);
      if (r >= 0) 
	reply->set_length(r);
      else
	reply->set_length(0);
      dout(10) << " read got " << r << " / " << op->get_length() << " bytes from obj " << oid << dendl;
    }
    osd->logger->inc("c_rd");
    osd->logger->inc("c_rdb", op->get_length());
    break;
    
  case CEPH_OSD_OP_STAT:
    {
      struct stat st;
      memset(&st, sizeof(st), 0);
      r = osd->store->stat(info.pgid.to_coll(), poid, &st);
      if (r >= 0)
	reply->set_length(st.st_size);
    }
    break;
    
  default:
      assert(0);
  }
  
  
 done:
  if (r >= 0) {
    reply->set_result(0);

    utime_t now = g_clock.now();
    utime_t diff = now;
    diff -= op->get_recv_stamp();
    dout(10) <<  "op_read " << op->get_reqid() << " total op latency " << diff << dendl;
    Mutex::Locker lock(osd->peer_stat_lock);
    osd->stat_rd_ops_in_queue--;
    osd->read_latency_calc.add(diff);

    if (is_primary() &&
	g_conf.osd_balance_reads)
      stat_object_temp_rd[oid].hit(now);  // hit temp.

  } else {
    reply->set_result(r);   // error
  }
  
  // send it
  osd->messenger->send_message(reply, op->get_orig_source_inst());
  
  delete op;
}






// ========================================================================
// MODIFY

void ReplicatedPG::prepare_transaction(ObjectStore::Transaction& t, osd_reqid_t reqid,
				       pobject_t poid, int op, eversion_t at_version,
				       off_t offset, off_t length, bufferlist& bl,
				       SnapSet& snapset, SnapContext& snapc,
				       __u32 inc_lock, eversion_t trim_to)
{
  // WRNOOP does nothing.
  if (op == CEPH_OSD_OP_WRNOOP) 
    return;

  // munge zero into remove?
  if (op == CEPH_OSD_OP_ZERO) {
    struct stat st;
    int r = osd->store->stat(info.pgid.to_coll(), poid, &st);
    if (r >= 0) {
      if (offset == 0 && offset + length >= (loff_t)st.st_size) {
	dout(10) << " munging ZERO " << offset << "~" << length
		 << " -> DELETE (size is " << st.st_size << ")" << dendl;
	op = CEPH_OSD_OP_DELETE;
      }
    }
  }

  // clone?
  if (poid.oid.snap) {
    assert(poid.oid.snap == CEPH_NOSNAP);
    dout(20) << "snapset=" << snapset << "  snapc=" << snapc << dendl;;

    // use newer snapc?
    if (snapset.seq > snapc.seq) {
      snapc.seq = snapset.seq;
      snapc.snaps = snapset.snaps;
      dout(10) << " using newer snapc " << snapc << dendl;
    }

    if (snapset.head_exists &&           // head exists
	snapc.snaps.size() &&            // there are snaps
	snapc.snaps[0] > snapset.seq &&  // existing object is old
	ceph_osd_op_is_modify(op)) {     // is a (non-lock) modification
      // clone
      pobject_t coid = poid;
      coid.oid.snap = snapc.seq;
      
      unsigned l;
      for (l=1; l<snapc.snaps.size() && snapc.snaps[l] > snapset.seq; l++) ;

      vector<snapid_t> snaps(l);
      for (unsigned i=0; i<l; i++)
	snaps[i] = snapc.snaps[i];

      // log clone
      dout(10) << "cloning to " << coid << " v " << at_version << " snaps=" << snaps << dendl;
      Log::Entry cloneentry(PG::Log::Entry::CLONE, coid.oid, at_version, reqid);
      dout(10) << "prepare_transaction " << cloneentry << dendl;
      log.add(cloneentry);
      assert(log.top == at_version);

      // prepare clone
      t.clone(info.pgid.to_coll(), poid, coid);
      bufferlist snapsbl;
      ::encode(snaps, snapsbl);
      t.setattr(info.pgid.to_coll(), coid, "snaps", snapsbl);
      t.setattr(info.pgid.to_coll(), coid, "version", &at_version, sizeof(at_version));
      
      // add to snap bound collections
      coll_t fc = make_snap_collection(t, snaps[0]);
      t.collection_add(fc, info.pgid.to_coll(), coid);
      if (snaps.size() > 1) {
	coll_t lc = make_snap_collection(t, snaps[snaps.size()-1]);
	t.collection_add(lc, info.pgid.to_coll(), coid);
      }

      snapset.clones.push_back(coid.oid.snap);
      snapset.clone_overlap[coid.oid.snap].swap(snapset.head_overlap);
      
      at_version.version++;
    }

    // update snapset with latest snap context
    snapset.seq = snapc.seq;
    snapset.snaps = snapc.snaps;

    // munge delete into a truncate?
    if (op == CEPH_OSD_OP_DELETE &&
	snapset.clones.size()) {
      dout(10) << " munging DELETE -> TRUNCATE(0) bc of clones " << snapset.clones << dendl;
      op = CEPH_OSD_OP_TRUNCATE;
      length = 0;
      snapset.head_exists = false;
    }
  }

  
  // log op
  int opcode = Log::Entry::MODIFY;
  if (op == CEPH_OSD_OP_DELETE) opcode = Log::Entry::DELETE;
  Log::Entry logentry(opcode, poid.oid, at_version, reqid);
  dout(10) << "prepare_transaction " << logentry << dendl;

  assert(at_version > log.top);
  log.add(logentry);
  assert(log.top == at_version);

  // prepare op
  switch (op) {

    // -- locking --

  case CEPH_OSD_OP_WRLOCK:
    { // lock object
      t.setattr(info.pgid.to_coll(), poid, "wrlock", &reqid.name, sizeof(entity_name_t));
    }
    break;  
  case CEPH_OSD_OP_WRUNLOCK:
    { // unlock objects
      t.rmattr(info.pgid.to_coll(), poid, "wrlock");
    }
    break;

  case CEPH_OSD_OP_BALANCEREADS:
    {
      bool bal = true;
      t.setattr(info.pgid.to_coll(), poid, "balance-reads", &bal, sizeof(bal));
    }
    break;
  case CEPH_OSD_OP_UNBALANCEREADS:
    {
      t.rmattr(info.pgid.to_coll(), poid, "balance-reads");
    }
    break;


    // -- modify --

  case CEPH_OSD_OP_WRITE:
    { // write
      assert(bl.length() == length);
      bufferlist nbl;
      nbl.claim(bl);    // give buffers to store; we keep *op in memory for a long time!
      t.write(info.pgid.to_coll(), poid, offset, length, nbl);
      snapset.head_exists = true;
      interval_set<__u64> ch;
      ch.insert(offset, length);
      ch.intersection_of(snapset.head_overlap);
      snapset.head_overlap.subtract(ch);
    }
    break;
    
  case CEPH_OSD_OP_WRITEFULL:
    { // write full object
      assert(bl.length() == length);
      bufferlist nbl;
      nbl.claim(bl);    // give buffers to store; we keep *op in memory for a long time!
      t.truncate(info.pgid.to_coll(), poid, 0);
      t.write(info.pgid.to_coll(), poid, offset, length, nbl);
      snapset.head_exists = true;
      snapset.head_overlap.clear();
    }
    break;
    
  case CEPH_OSD_OP_ZERO:
    { // zero
      t.zero(info.pgid.to_coll(), poid, offset, length);
      interval_set<__u64> ch;
      ch.insert(offset, length);
      ch.intersection_of(snapset.head_overlap);
      snapset.head_overlap.subtract(ch);
    }
    break;

  case CEPH_OSD_OP_TRUNCATE:
    { // truncate
      t.truncate(info.pgid.to_coll(), poid, length);
      interval_set<__u64> keep;
      if (length)
	keep.insert(0, length);
      snapset.head_overlap.intersection_of(keep);
    }
    break;
    
  case CEPH_OSD_OP_DELETE:
    { // delete
      t.remove(info.pgid.to_coll(), poid);
      snapset.head_overlap.clear();  // ok, redundant.
    }
    break;
    
  default:
    assert(0);
  }
  
  // object collection, version
  if (op != CEPH_OSD_OP_DELETE) {
    if (inc_lock && ceph_osd_op_is_modify(op)) 
      t.setattr(info.pgid.to_coll(), poid, "inc_lock", &inc_lock, sizeof(inc_lock));

    t.setattr(info.pgid.to_coll(), poid, "version", &at_version, sizeof(at_version));

    bufferlist snapsetbl;
    ::encode(snapset, snapsetbl);
    t.setattr(info.pgid.to_coll(), poid, "snapset", snapsetbl);
  }

  // update pg info:
  // raise last_complete only if we were previously up to date
  if (info.last_complete == info.last_update)
    info.last_complete = at_version;
  
  // raise last_update.
  assert(at_version > info.last_update);
  info.last_update = at_version;
  
  write_info(t);

  // prepare log append
  append_log(t, logentry, trim_to);
}



// ========================================================================
// rep op gather

class C_OSD_ModifyCommit : public Context {
public:
  ReplicatedPG *pg;
  tid_t rep_tid;
  eversion_t pg_last_complete;
  C_OSD_ModifyCommit(ReplicatedPG *p, tid_t rt, eversion_t lc) : pg(p), rep_tid(rt), pg_last_complete(lc) {
    pg->get();  // we're copying the pointer
  }
  void finish(int r) {
    pg->lock();
    if (!pg->is_deleted()) 
      pg->op_modify_commit(rep_tid, pg_last_complete);
    pg->put_unlock();
  }
};


void ReplicatedPG::get_rep_gather(RepGather *repop)
{
  //repop->lock.Lock();
  dout(10) << "get_repop " << *repop << dendl;
}

void ReplicatedPG::apply_repop(RepGather *repop)
{
  dout(10) << "apply_repop  applying update on " << *repop << dendl;
  assert(!repop->applied);

  Context *oncommit = new C_OSD_ModifyCommit(this, repop->rep_tid, repop->pg_local_last_complete);
  unsigned r = osd->store->apply_transaction(repop->t, oncommit);
  if (r)
    dout(-10) << "apply_repop  apply transaction return " << r << " on " << *repop << dendl;
  
  // discard my reference to the buffer
  repop->op->get_data().clear();
  
  repop->applied = true;


  // any completion stuff to do here?
  object_t oid = repop->op->get_oid();

  switch (repop->op->get_op()) { 
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
    
  case CEPH_OSD_OP_WRUNLOCK:
    dout(-10) << "apply_repop  completed wrunlock on " << oid << dendl;
    if (waiting_for_wr_unlock.count(oid)) {
      osd->take_waiters(waiting_for_wr_unlock[oid]);
      waiting_for_wr_unlock.erase(oid);
    }
    break;
  }   
  
  update_stats();
  
}

void ReplicatedPG::put_rep_gather(RepGather *repop)
{
  dout(10) << "put_repop " << *repop << dendl;
  
  // commit?
  if (repop->can_send_commit()) {
    if (repop->op->wants_commit()) {
      // send commit.
      MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osd->osdmap->get_epoch(), true);
      dout(10) << "put_repop  sending commit on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, repop->op->get_orig_source_inst());
      repop->sent_commit = true;
    }
  }

  // ack?
  else if (repop->can_send_ack()) {
    // apply
    if (!repop->applied)
      apply_repop(repop);

    if (repop->op->wants_ack()) {
      // send ack
      MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osd->osdmap->get_epoch(), false);
      dout(10) << "put_repop  sending ack on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, repop->op->get_orig_source_inst());
      repop->sent_ack = true;
    }

    utime_t now = g_clock.now();
    now -= repop->start;
    osd->logger->finc("rlsum", now);
    osd->logger->inc("rlnum", 1);
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
        dout(10) << "put_repop  peers_complete_thru " 
				 << peers_complete_thru << " -> " << min
				 << dendl;
        peers_complete_thru = min;
      }
    }

    dout(10) << "put_repop  deleting " << *repop << dendl;
	
    assert(rep_gather.count(repop->rep_tid));
    rep_gather.erase(repop->rep_tid);
	
    delete repop->op;
    delete repop;
  }
}


void ReplicatedPG::issue_repop(RepGather *repop, int dest, utime_t now)
{
  pobject_t poid(info.pgid.pool(), 0, repop->op->get_oid());
  dout(7) << " issue_repop rep_tid " << repop->rep_tid
          << " o " << poid
          << " to osd" << dest
          << dendl;
  
  // forward the write/update/whatever
  MOSDSubOp *wr = new MOSDSubOp(repop->op->get_reqid(), info.pgid, poid,
				repop->op->get_op(), 
				repop->op->get_offset(), repop->op->get_length(), 
				osd->osdmap->get_epoch(), 
				repop->rep_tid, repop->op->get_inc_lock(), repop->at_version);
  wr->snapset = repop->snapset;
  wr->snapc = repop->snapc;
  wr->get_data() = repop->op->get_data();   // _copy_ bufferlist
  wr->pg_trim_to = peers_complete_thru;
  wr->peer_stat = osd->get_my_stat_for(now, dest);
  osd->messenger->send_message(wr, osd->osdmap->get_inst(dest));
}

ReplicatedPG::RepGather *ReplicatedPG::new_rep_gather(MOSDOp *op, tid_t rep_tid, eversion_t nv,
						      SnapSet& snapset, SnapContext& snapc)
{
  dout(10) << "new_rep_gather rep_tid " << rep_tid << " on " << *op << dendl;
  RepGather *repop = new RepGather(op, rep_tid, nv, info.last_complete,
				   snapset, snapc);
  
  // osds. commits all come to me.
  for (unsigned i=0; i<acting.size(); i++) {
    int osd = acting[i];
    repop->osds.insert(osd);
    repop->waitfor_commit.insert(osd);
  }

  // primary.  all osds ack to me.
  for (unsigned i=0; i<acting.size(); i++) {
    int osd = acting[i];
    repop->waitfor_ack.insert(osd);
  }

  repop->start = g_clock.now();

  rep_gather[repop->rep_tid] = repop;

  // anyone waiting?  (acks that got here before the op did)
  if (waiting_for_repop.count(repop->rep_tid)) {
    osd->take_waiters(waiting_for_repop[repop->rep_tid]);
    waiting_for_repop.erase(repop->rep_tid);
  }

  return repop;
}
 

void ReplicatedPG::repop_ack(RepGather *repop,
                    int result, bool commit,
                    int fromosd, eversion_t pg_complete_thru)
{
  MOSDOp *op = repop->op;

  dout(7) << "repop_ack rep_tid " << repop->rep_tid << " op " << *op
          << " result " << result << " commit " << commit << " from osd" << fromosd
          << dendl;

  get_rep_gather(repop);
  {
    if (commit) {
      // commit
      assert(repop->waitfor_commit.count(fromosd));      
      repop->waitfor_commit.erase(fromosd);
      repop->waitfor_ack.erase(fromosd);
      repop->pg_complete_thru[fromosd] = pg_complete_thru;
    } else {
      // ack
      repop->waitfor_ack.erase(fromosd);
    }
  }
  put_rep_gather(repop);
}























/** op_modify_commit
 * transaction commit on the acker.
 */
void ReplicatedPG::op_modify_commit(tid_t rep_tid, eversion_t pg_complete_thru)
{
  if (rep_gather.count(rep_tid)) {
    RepGather *repop = rep_gather[rep_tid];
    
    dout(10) << "op_modify_commit " << *repop->op << dendl;
    get_rep_gather(repop);
    {
      assert(repop->waitfor_commit.count(osd->get_nodeid()));
      repop->waitfor_commit.erase(osd->get_nodeid());
      repop->pg_complete_thru[osd->get_nodeid()] = pg_complete_thru;
    }
    put_rep_gather(repop);
    dout(10) << "op_modify_commit done on " << repop << dendl;
  } else {
    dout(10) << "op_modify_commit rep_tid " << rep_tid << " dne" << dendl;
  }
}



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
    pg->sub_op_modify_commit(op, destosd, pg_last_complete);
    pg->put_unlock();
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

void ReplicatedPG::reply_op_error(MOSDOp *op, int err)
{
  MOSDOpReply *reply = new MOSDOpReply(op, err, osd->osdmap->get_epoch(), true);
  osd->messenger->send_message(reply, op->get_orig_source_inst());
  delete op;
}

void ReplicatedPG::op_modify(MOSDOp *op)
{
  int whoami = osd->get_nodeid();
  pobject_t poid(info.pgid.pool(), 0, op->get_oid());

  const char *opname = ceph_osd_op_name(op->get_op());

  // make sure it looks ok
  if ((op->get_op() == CEPH_OSD_OP_WRITE ||
       op->get_op() == CEPH_OSD_OP_WRITEFULL) &&
      op->get_length() != op->get_data().length()) {
    dout(0) << "op_modify got bad write, claimed length " << op->get_length() 
	    << " != payload length " << op->get_data().length()
	    << dendl;
    delete op;
    return;
  }

  // --- locking ---

  // wrlock?
  if (op->get_op() != CEPH_OSD_OP_WRNOOP &&  // except WRNOOP; we just want to flush
      block_if_wrlocked(op)) 
    return; // op will be handled later, after the object unlocks
  
  // check inc_lock?
  if (op->get_inc_lock() > 0) {
    __u32 cur = 0;
    osd->store->getattr(info.pgid.to_coll(), poid, "inc_lock", &cur, sizeof(cur));
    if (cur > op->get_inc_lock()) {
      dout(10) << " inc_lock " << cur << " > " << op->get_inc_lock()
	       << " on " << poid << dendl;
      reply_op_error(op, -EINCLOCKED);
      return;
    }
  }

  // balance-reads set?
  char v;
  if ((op->get_op() != CEPH_OSD_OP_BALANCEREADS && op->get_op() != CEPH_OSD_OP_UNBALANCEREADS) &&
      (osd->store->getattr(info.pgid.to_coll(), poid, "balance-reads", &v, 1) >= 0 ||
       balancing_reads.count(poid.oid))) {
    
    if (!unbalancing_reads.count(poid.oid)) {
      // unbalance
      dout(-10) << "preprocess_op unbalancing-reads on " << poid.oid << dendl;
      unbalancing_reads.insert(poid.oid);
      
      ceph_object_layout layout;
      layout.ol_pgid = info.pgid.u.pg64;
      layout.ol_stripe_unit = 0;
      MOSDOp *pop = new MOSDOp(0, osd->get_tid(),
			       poid.oid,
			       layout,
			       osd->osdmap->get_epoch(),
			       CEPH_OSD_OP_UNBALANCEREADS, 0);
      do_op(pop);
    }

    // add to wait queue
    dout(-10) << "preprocess_op waiting for unbalance-reads on " << poid.oid << dendl;
    waiting_for_unbalanced_reads[poid.oid].push_back(op);
    return;
  }

  // dup op?
  if (is_dup(op->get_reqid())) {
    dout(3) << "op_modify " << opname << " dup op " << op->get_reqid()
             << ", doing WRNOOP" << dendl;
    op->set_op(CEPH_OSD_OP_WRNOOP);
    opname = ceph_osd_op_name(op->get_op());
  }


  // version
  eversion_t av = log.top;
  if (op->get_op() != CEPH_OSD_OP_WRNOOP) {
    av.epoch = osd->osdmap->get_epoch();
    av.version++;
    assert(av > info.last_update);
    assert(av > log.top);
  }

  // snap
  SnapContext snapc;
  snapc.seq = op->get_snap_seq();
  snapc.snaps = op->get_snaps();

  SnapSet snapset;
  if (poid.oid.snap == CEPH_NOSNAP) {
    bufferlist bl;
    int r = osd->store->getattr(info.pgid.to_coll(), poid, "snapset", bl);
    if (r >= 0) {
      bufferlist::iterator p = bl.begin();
      ::decode(snapset, p);
    } else {
      dout(10) << " no \"snapset\" attr, r = " << r << " " << strerror(-r) << dendl;
    }
  } else 
    assert(poid.oid.snap == 0);   // no snapshotting.

  // set version in op, for benefit of client and our eventual reply
  op->set_version(av);

  dout(10) << "op_modify " << opname 
           << " " << poid.oid 
           << " " << op->get_offset() << "~" << op->get_length()
           << " av " << av 
	   << " snapc " << snapc
	   << " snapset " << snapset
           << dendl;  

  // verify snap ordering
  if ((op->get_flags() & CEPH_OSD_OP_ORDERSNAP) &&
      snapc.seq < snapset.seq) {
    dout(10) << " ORDERSNAP flag set and snapc seq " << snapc.seq << " < snapset seq " << snapset.seq
	     << " on " << poid << dendl;
    reply_op_error(op, -EOLDSNAPC);
    return;
  }

  // are any peers missing this?
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    if (peer_missing.count(peer) &&
        peer_missing[peer].is_missing(poid.oid)) {
      // push it before this update. 
      // FIXME, this is probably extra much work (eg if we're about to overwrite)
      push(poid, peer);
    }
  }

  if (op->get_op() == CEPH_OSD_OP_WRITE ||
      op->get_op() == CEPH_OSD_OP_WRITEFULL) {
    osd->logger->inc("c_wr");
    osd->logger->inc("c_wrb", op->get_length());
  }

  // note my stats
  utime_t now = g_clock.now();

  // issue replica writes
  tid_t rep_tid = osd->get_tid();
  RepGather *repop = new_rep_gather(op, rep_tid, av, snapset, snapc);
  for (unsigned i=1; i<acting.size(); i++)
    issue_repop(repop, acting[i], now);

  // we are acker.
  if (op->get_op() != CEPH_OSD_OP_WRNOOP) {
    // log and update later.
    prepare_transaction(repop->t, op->get_reqid(),
			poid, op->get_op(), av,
			op->get_offset(), op->get_length(), op->get_data(),
			snapset, snapc,
			op->get_inc_lock(), peers_complete_thru);
  }
  
  // (logical) local ack.
  // (if alone, this will apply the update.)
  get_rep_gather(repop);
  {
    assert(repop->waitfor_ack.count(whoami));
    repop->waitfor_ack.erase(whoami);
  }
  put_rep_gather(repop);
}



// sub op modify

void ReplicatedPG::sub_op_modify(MOSDSubOp *op)
{
  pobject_t poid = op->poid;
  const char *opname = ceph_osd_op_name(op->op);
  
  dout(10) << "sub_op_modify " << opname 
           << " " << poid 
           << " v " << op->version
	   << " " << op->offset << "~" << op->length
	   << dendl;  

  // sanity checks
  if (op->map_epoch < info.history.same_primary_since) {
    dout(10) << "sub_op_modify discarding old sub_op from "
	     << op->map_epoch << " < " << info.history.same_primary_since << dendl;
    delete op;
    return;
  }
  if (!is_active()) {
    dout(10) << "sub_op_modify not active" << dendl;
    delete op;
    return;
  }
  assert(is_replica());
  
  // note peer's stat
  int fromosd = op->get_source().num();
  osd->take_peer_stat(fromosd, op->peer_stat);

  // we better not be missing this.
  assert(!missing.is_missing(poid.oid));

  // prepare our transaction
  ObjectStore::Transaction t;

  // do op
  int ackerosd = acting[0];
  osd->logger->inc("r_wr");
  osd->logger->inc("r_wrb", op->length);
  
  if (op->op != CEPH_OSD_OP_WRNOOP) {
    prepare_transaction(t, op->reqid,
			op->poid, op->op, op->version,
			op->offset, op->length, op->get_data(), 
			op->snapset, op->snapc,
			op->inc_lock, op->pg_trim_to);
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
  MOSDSubOpReply *ack = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), false);
  ack->set_peer_stat(osd->get_my_stat_for(g_clock.now(), ackerosd));
  osd->messenger->send_message(ack, osd->osdmap->get_inst(ackerosd));
  
  // ack myself.
  oncommit->ack(); 
}

void ReplicatedPG::sub_op_modify_commit(MOSDSubOp *op, int ackerosd, eversion_t last_complete)
{
  // send commit.
  dout(10) << "rep_modify_commit on op " << *op
           << ", sending commit to osd" << ackerosd
           << dendl;
  if (osd->osdmap->is_up(ackerosd)) {
    MOSDSubOpReply *commit = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), true);
    commit->set_pg_complete_thru(last_complete);
    commit->set_peer_stat(osd->get_my_stat_for(g_clock.now(), ackerosd));
    osd->messenger->send_message(commit, osd->osdmap->get_inst(ackerosd));
    delete op;
  }
}

void ReplicatedPG::sub_op_modify_reply(MOSDSubOpReply *r)
{
  // must be replication.
  tid_t rep_tid = r->get_rep_tid();
  int fromosd = r->get_source().num();
  
  osd->take_peer_stat(fromosd, r->get_peer_stat());
  
  if (rep_gather.count(rep_tid)) {
    // oh, good.
    repop_ack(rep_gather[rep_tid], 
	      r->get_result(), r->get_commit(), 
	      fromosd, 
	      r->get_pg_complete_thru());
    delete r;
  } else {
    // early ack.
    waiting_for_repop[rep_tid].push_back(r);
  }
}










// ===========================================================

/** pull - request object from a peer
 */
void ReplicatedPG::pull(pobject_t poid)
{
  assert(missing.loc.count(poid.oid));
  eversion_t v = missing.missing[poid.oid];
  int fromosd = missing.loc[poid.oid];
  
  dout(7) << "pull " << poid
          << " v " << v 
          << " from osd" << fromosd
          << dendl;

  // send op
  osd_reqid_t rid;
  tid_t tid = osd->get_tid();
  
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, poid, CEPH_OSD_OP_PULL,
				   0, 0, 
				   osd->osdmap->get_epoch(), tid, 0, v);
  osd->messenger->send_message(subop, osd->osdmap->get_inst(fromosd));
  
  // take note
  assert(pulling.count(poid.oid) == 0);
  pulling[poid.oid].first = v;
  pulling[poid.oid].second = fromosd;
}


/** push - send object to a peer
 */
void ReplicatedPG::push(pobject_t poid, int peer)
{
  interval_set<__u64> subset;
  push(poid, peer, subset);
}

void ReplicatedPG::push(pobject_t poid, int peer, interval_set<__u64> &subset)
{
  // read data+attrs
  bufferlist bl;
  eversion_t v;
  map<string,bufferptr> attrset;
  __u64 size;

  if (subset.size()) {
    struct stat st;
    int r = osd->store->stat(info.pgid.to_coll(), poid, &st);
    assert(r == 0);
    size = st.st_size;

    for (map<__u64,__u64>::iterator p = subset.m.begin();
	 p != subset.m.end();
	 p++) {
      bufferlist bit;
      osd->store->read(info.pgid.to_coll(), poid, p->first, p->second, bit);
      bl.claim_append(bit);
    }
  } else {
    osd->store->read(info.pgid.to_coll(), poid, 0, 0, bl);
    size = bl.length();
  }
  osd->store->getattr(info.pgid.to_coll(), poid, "version", &v, sizeof(v));
  osd->store->getattrs(info.pgid.to_coll(), poid, attrset);

  // ok
  dout(7) << "push " << poid << " v " << v 
	  << " size " << size
	  << " subset " << subset
          << " data " << bl.length()
          << " to osd" << peer
          << dendl;

  osd->logger->inc("r_push");
  osd->logger->inc("r_pushb", bl.length());
  
  // send
  osd_reqid_t rid;  // useless?
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, poid, CEPH_OSD_OP_PUSH, 0, size,
				   osd->osdmap->get_epoch(), osd->get_tid(), 0, v);
  subop->data_subset = subset;
  subop->set_data(bl);   // note: claims bl, set length above here!
  subop->attrset.swap(attrset);
  osd->messenger->send_message(subop, osd->osdmap->get_inst(peer));
  
  if (is_primary()) {
    peer_missing[peer].got(poid.oid);
    pushing[poid.oid].insert(peer);
  }
}

void ReplicatedPG::sub_op_push_reply(MOSDSubOpReply *reply)
{
  dout(10) << "sub_op_push_reply from " << reply->get_source() << " " << *reply << dendl;
  
  int peer = reply->get_source().num();
  pobject_t poid = reply->get_poid();
  
  if (pushing.count(poid.oid) &&
      pushing[poid.oid].count(peer)) {
    pushing[poid.oid].erase(peer);

    if (peer_missing.count(peer) == 0 ||
        peer_missing[peer].num_missing() == 0) 
      uptodate_set.insert(peer);

    if (pushing[poid.oid].empty()) {
      dout(10) << "pushed " << poid << " to all replicas" << dendl;
      do_peer_recovery();
    } else {
      dout(10) << "pushed " << poid << ", still waiting for push ack from " 
	       << pushing[poid.oid] << dendl;
    }
  } else {
    dout(10) << "huh, i wasn't pushing " << poid << dendl;
  }
  delete reply;
}


/** op_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void ReplicatedPG::sub_op_pull(MOSDSubOp *op)
{
  const pobject_t poid = op->poid;
  const eversion_t v = op->version;

  dout(7) << "op_pull " << poid << " v " << op->version
          << " from " << op->get_source()
          << dendl;

  if (op->map_epoch < info.history.same_primary_since) {
    dout(10) << "sub_op_pull discarding old sub_op from "
	     << op->map_epoch << " < " << info.history.same_primary_since << dendl;
    delete op;
    return;
  }

  assert(!is_primary());  // we should be a replica or stray.

  // push it back!
  push(poid, op->get_source().num(), op->data_subset);
}


/** op_push
 * NOTE: called from opqueue.
 */
void ReplicatedPG::sub_op_push(MOSDSubOp *op)
{
  pobject_t poid = op->poid;
  eversion_t v = op->version;

  dout(7) << "op_push " 
          << poid 
          << " v " << v 
	  << " len " << op->length
	  << " subset " << op->data_subset
	  << " data " << op->get_data().length()
          << dendl;

  interval_set<__u64> data_subset;
  map<pobject_t, interval_set<__u64> > clone_subsets;

  if (is_replica()) {
    // replica should only accept pushes from the current primary.
    if (op->map_epoch < info.history.same_primary_since) {
      dout(10) << "sub_op_push discarding old sub_op from "
	       << op->map_epoch << " < " << info.history.same_primary_since << dendl;
      delete op;
      return;
    }
    // FIXME: actually, no, what i really want here is a personal "same_role_since"
    if (!is_active()) {
      dout(10) << "sub_op_push not active" << dendl;
      delete op;
      return;
    }
    data_subset = op->data_subset;
    if (data_subset.empty() && op->length && op->length == op->get_data().length())
      data_subset.insert(0, op->length);
    clone_subsets = op->clone_subsets;
  } else {
    // primary will accept pushes anytime.

    // *** write me ***
    data_subset = op->data_subset;
    if (data_subset.empty() && op->length && op->length == op->get_data().length())
      data_subset.insert(0, op->length);
  }

  // are we missing (this specific version)?
  //  (if version is wrong, it is either old (we don't want it) or 
  //   newer (peering is buggy))
  if (!missing.is_missing(poid.oid, v)) {
    dout(7) << "sub_op_push not missing " << poid << " v" << v << dendl;
    dout(15) << " but i AM missing " << missing.missing << dendl;
    return;
  }
  
  //assert(op->get_data().length() == op->length);
  
  dout(15) << " data_subset " << data_subset
	   << " clone_subsets " << clone_subsets
	   << dendl;

  // write object and add it to the PG
  ObjectStore::Transaction t;
  t.remove(info.pgid.to_coll(), poid);  // in case old version exists

  __u64 boff = 0;
  for (map<__u64,__u64>::iterator p = data_subset.m.begin();
       p != data_subset.m.end(); 
       p++) {
    bufferlist bit;
    bit.substr_of(op->get_data(), boff, p->second);
    t.write(info.pgid.to_coll(), poid, p->first, p->second, bit);
    boff += p->second;
  }
  for (map<pobject_t, interval_set<__u64> >::iterator p = clone_subsets.begin();
       p != clone_subsets.end();
       p++)
    for (map<__u64,__u64>::iterator q = p->second.m.begin();
	 q != p->second.m.end(); 
	 q++)
      t.clone_range(info.pgid.to_coll(), poid, p->first, q->first, q->second);

  t.setattrs(info.pgid.to_coll(), poid, op->attrset);
  if (poid.oid.snap && poid.oid.snap != CEPH_NOSNAP &&
      op->attrset.count("snaps")) {
    bufferlist bl;
    bl.push_back(op->attrset["snaps"]);
    vector<snapid_t> snaps;
    bufferlist::iterator p = bl.begin();
    ::decode(snaps, p);
    if (snaps.size()) {
      coll_t lc = make_snap_collection(t, snaps[0]);
      t.collection_add(lc, info.pgid.to_coll(), poid);
      if (snaps.size() > 1) {
	coll_t hc = make_snap_collection(t, snaps[snaps.size()-1]);
	t.collection_add(hc, info.pgid.to_coll(), poid);
      }
    }
  }


  // close out pull op?
  if (pulling.count(poid.oid))
    pulling.erase(poid.oid);

  missing.got(poid.oid, v);


  // raise last_complete?
  assert(log.complete_to != log.log.end());
  while (log.complete_to != log.log.end()) {
    if (missing.missing.count(log.complete_to->oid)) break;
    if (info.last_complete < log.complete_to->version)
      info.last_complete = log.complete_to->version;
    log.complete_to++;
  }
  dout(10) << "last_complete now " << info.last_complete << dendl;
  
  
  // apply to disk!
  write_info(t);
  unsigned r = osd->store->apply_transaction(t);
  assert(r == 0);



  if (is_primary()) {
    if (info.is_uptodate())
      uptodate_set.insert(osd->get_nodeid());
    
    if (is_active()) {
      // are others missing this too?  (only if we're active.. skip
      // this part if we're still repeering, it'll just confuse us)
      for (unsigned i=1; i<acting.size(); i++) {
	int peer = acting[i];
	assert(peer_missing.count(peer));
	if (peer_missing[peer].is_missing(poid.oid)) 
	  push(poid, peer);  // ok, push it, and they (will) have it now.
      }

      do_recovery();      // continue recovery
    }

  } else {
    // ack if i'm a replica and being pushed to.
    MOSDSubOpReply *reply = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), false); 
    osd->messenger->send_message(reply, op->get_source_inst());
  }

  delete op;

  // kick waiters
  if (waiting_for_missing_object.count(poid.oid)) {
    osd->take_waiters(waiting_for_missing_object[poid.oid]);
    waiting_for_missing_object.erase(poid.oid);
  }
}



/*
 * pg status change notification
 */

void ReplicatedPG::on_osd_failure(int o)
{
  dout(10) << "on_osd_failure " << o << dendl;

  hash_map<tid_t,RepGather*>::iterator p = rep_gather.begin();
  while (p != rep_gather.end()) {
    RepGather *repop = p->second;
    p++;
    dout(-1) << "checking repop tid " << repop->rep_tid << dendl;
    if (repop->waitfor_ack.count(o) ||
	repop->waitfor_commit.count(o))
      repop_ack(repop, -1, true, o);
  }
  
  // remove from pushing map
  {
    map<object_t, pair<eversion_t,int> >::iterator p = pulling.begin();
    while (p != pulling.end())
      if (p->second.second == o) {
	dout(10) << " forgetting pull of " << p->first << " " << p->second.first
		 << " from osd" << o << dendl;
	pulling.erase(p++);
      } else
	p++;
  }
}

void ReplicatedPG::on_acker_change()
{
  dout(10) << "on_acker_change" << dendl;
}

void ReplicatedPG::on_change()
{
  dout(10) << "on_change" << dendl;

  // apply all local repops
  //  (pg is inactive; we will repeer)
  for (hash_map<tid_t,RepGather*>::iterator p = rep_gather.begin();
       p != rep_gather.end();
       p++) 
    if (!p->second->applied)
      apply_repop(p->second);

  hash_map<tid_t,RepGather*>::iterator p = rep_gather.begin(); 
  while (p != rep_gather.end()) {
    RepGather *repop = p->second;

    if (acting.empty() || acting[0] != osd->get_nodeid()) {
      // no longer primary.  hose repops.
      dout(-1) << "no longer primary, aborting repop tid " << repop->rep_tid << dendl;
      rep_gather.erase(p++);
      delete repop->op;
      delete repop;
    } else {
      // still primary. artificially ack+commit any replicas who dropped out of the pg
      p++;
      dout(-1) << "checking repop tid " << repop->rep_tid << dendl;
      set<int> all;
      set_union(repop->waitfor_commit.begin(), repop->waitfor_commit.end(),
		repop->waitfor_ack.begin(), repop->waitfor_ack.end(),
		inserter(all, all.begin()));
      for (set<int>::iterator q = all.begin(); q != all.end(); q++) {
	bool have = false;
	for (unsigned i=1; i<acting.size(); i++)
	  if (acting[i] == *q) 
	    have = true;
	if (!have)
	  repop_ack(repop, -EIO, true, *q);
      }
    }
  }
  
  // remove strays from pushing map
  {
    map<object_t, set<int> >::iterator p = pushing.begin();
    while (p != pushing.end()) {
      set<int>::iterator q = p->second.begin();
      while (q != p->second.end()) {
	int o = *q++;
	bool have = false;
	for (unsigned i=1; i<acting.size(); i++)
	  if (acting[i] == o) {
	    have = true;
	    break;
	  }
	if (!have) {
	  dout(10) << " forgetting push of " << p->first << " to (now stray) osd" << o << dendl;
	  p->second.erase(o);
	}
      }
      if (p->second.empty())
	pushing.erase(p++);
      else
	p++;	
    }
  }
}

void ReplicatedPG::on_role_change()
{
  dout(10) << "on_role_change" << dendl;

  // take object waiters
  for (hash_map<object_t, list<Message*> >::iterator it = waiting_for_missing_object.begin();
       it != waiting_for_missing_object.end();
       it++)
    osd->take_waiters(it->second);
  waiting_for_missing_object.clear();
}







void ReplicatedPG::cancel_recovery()
{
  // forget about where missing items are, or anything we're pulling
  missing.loc.clear();
  osd->num_pulling -= pulling.size();
  pulling.clear();
  pushing.clear();
}

/**
 * do one recovery op.
 * return true if done, false if nothing left to do.
 */
bool ReplicatedPG::do_recovery()
{
  assert(is_primary());

  dout(-10) << "do_recovery pulling " << pulling.size() << " in pg, "
           << osd->num_pulling << "/" << g_conf.osd_max_pull << " total"
           << dendl;
  dout(10) << "do_recovery " << missing << dendl;
  dout(15) << "do_recovery " << missing.missing << dendl;

  // can we slow down on this PG?
  if (osd->num_pulling >= g_conf.osd_max_pull && !pulling.empty()) {
    dout(-10) << "do_recovery already pulling max, waiting" << dendl;
    return true;
  }

  // look at log!
  Log::Entry *latest = 0;

  while (log.requested_to != log.log.end()) {
    assert(log.objects.count(log.requested_to->oid));
    latest = log.objects[log.requested_to->oid];
    assert(latest);

    dout(10) << "do_recovery "
             << *log.requested_to
	     << (latest->is_update() ? " (update)":"")
             << (pulling.count(latest->oid) ? " (pulling)":"")
	     << (missing.is_missing(latest->oid) ? " (missing)":"")
             << dendl;

    if (latest->is_update() &&
        !pulling.count(latest->oid) &&
        missing.is_missing(latest->oid)) {
      pobject_t poid(info.pgid.pool(), 0, latest->oid);
      pull(poid);
      return true;
    }
    
    log.requested_to++;
  }

  if (!pulling.empty()) {
    dout(7) << "do_recovery requested everything, still waiting" << dendl;
    return false;
  }

  // done?
  assert(missing.num_missing() == 0);
  
  if (info.last_complete != info.last_update) {
    dout(7) << "do_recovery last_complete " << info.last_complete << " -> " << info.last_update << dendl;
    info.last_complete = info.last_update;
  }

  log.complete_to == log.log.end();
  log.requested_to = log.log.end();

  if (is_primary()) {
    // i am primary
    uptodate_set.insert(osd->whoami);
    if (is_all_uptodate()) {
      dout(-7) << "do_recovery complete" << dendl;
      finish_recovery();
    } else {
      dout(-10) << "do_recovery primary now complete, starting peer recovery" << dendl;
      do_peer_recovery();
    }
  } else {
    // tell primary
    dout(7) << "do_recovery complete, telling primary" << dendl;
    vector<PG::Info> ls;
    ls.push_back(info);
    osd->messenger->send_message(new MOSDPGNotify(osd->osdmap->get_epoch(),
                                                  ls),
                                 osd->osdmap->get_inst(get_primary()));
  }

  return false;
}

void ReplicatedPG::do_peer_recovery()
{
  dout(-10) << "do_peer_recovery" << dendl;

  // this is FAR from an optimal recovery order.  pretty lame, really.
  for (unsigned i=0; i<acting.size(); i++) {
    int peer = acting[i];

    if (peer_missing.count(peer) == 0 ||
        peer_missing[peer].num_missing() == 0) 
      continue;
    
    // oldest first!
    object_t oid = peer_missing[peer].rmissing.begin()->second;
    pobject_t poid(info.pgid.pool(), 0, oid);
    eversion_t v = peer_missing[peer].rmissing.begin()->first;

    push(poid, peer);

    // do other peers need it too?
    for (i++; i<acting.size(); i++) {
      int peer = acting[i];
      if (peer_missing.count(peer) &&
          peer_missing[peer].is_missing(oid)) 
	push(poid, peer);
    }

    return;
  }
  
  // nothing to do!
  dout(-10) << "do_peer_recovery - nothing to do!" << dendl;

  if (is_all_uptodate()) 
    finish_recovery();
  else {
    dout(10) << "do_peer_recovery not all uptodate, acting " << acting << ", uptodate " << uptodate_set << dendl;
  }
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
    set<object_t> s;
    
    for (vector<pobject_t>::iterator i = ls.begin();
         i != ls.end();
         i++) 
      s.insert(i->oid); 

    set<object_t> did;
    for (list<Log::Entry>::reverse_iterator p = log.log.rbegin();
         p != log.log.rend();
         p++) {
      if (did.count(p->oid)) continue;
      did.insert(p->oid);
      
      if (p->is_delete()) {
        if (s.count(p->oid)) {
	  pobject_t poid(info.pgid.pool(), 0, p->oid);
          dout(10) << " deleting " << poid
                   << " when " << p->version << dendl;
          t.remove(info.pgid.to_coll(), poid);
        }
        s.erase(p->oid);
      } else {
        // just leave old objects.. they're missing or whatever
        s.erase(p->oid);
      }
    }

    for (set<object_t>::iterator i = s.begin(); 
         i != s.end();
         i++) {
      pobject_t poid(info.pgid.pool(), 0, *i);
      dout(10) << " deleting stray " << poid << dendl;
      t.remove(info.pgid.to_coll(), poid);
    }

  } else {
    // just scan the log.
    set<object_t> did;
    for (list<Log::Entry>::reverse_iterator p = log.log.rbegin();
         p != log.log.rend();
         p++) {
      if (did.count(p->oid)) continue;
      did.insert(p->oid);

      if (p->is_delete()) {
	pobject_t poid(info.pgid.pool(), 0, p->oid);
        dout(10) << " deleting " << poid
                 << " when " << p->version << dendl;
        t.remove(info.pgid.to_coll(), poid);
      } else {
        // keep old(+missing) objects, just for kicks.
      }
    }
  }
}
