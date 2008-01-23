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

  // we don't have it (yet).
  eversion_t v = missing.missing[oid];
  if (objects_pulling.count(oid)) {
    dout(7) << "missing "
	    << oid 
	    << " v " << v
	    << ", already pulling"
	    << dendl;
  } else {
    dout(7) << "missing " 
	    << oid 
	    << " v " << v
	    << ", pulling"
	    << dendl;
    pull(oid);
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

  // -- load balance reads --
  if (is_primary() &&
      g_conf.osd_rep == OSD_REP_PRIMARY) {
    // -- read on primary+acker ---
    
    // test
    if (false) {
      if (acting.size() > 1) {
	int peer = acting[1];
	dout(-10) << "preprocess_op fwd client read op to osd" << peer << " for " << op->get_client() << " " << op->get_client_inst() << dendl;
	osd->messenger->send_message(op, osd->osdmap->get_inst(peer));
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
	  osd->store->getattr(pobject_t(0,0,oid), "balance-reads", &b, 1) >= 0)
	is_balanced = true;
      
      if (!is_balanced && should_balance &&
	  balancing_reads.count(oid) == 0) {
	dout(-10) << "preprocess_op balance-reads on " << oid << dendl;
	balancing_reads.insert(oid);
	ceph_object_layout layout;
	layout.ol_pgid = info.pgid.u;
	layout.ol_stripe_unit = 0;
	MOSDOp *pop = new MOSDOp(osd->messenger->get_myinst(), 0, osd->get_tid(),
				 oid,
				 layout,
				 osd->osdmap->get_epoch(),
				 CEPH_OSD_OP_BALANCEREADS);
	do_op(pop);
      }
      if (is_balanced && !should_balance &&
	  !unbalancing_reads.count(oid) == 0) {
	dout(-10) << "preprocess_op unbalance-reads on " << oid << dendl;
	unbalancing_reads.insert(oid);
	ceph_object_layout layout;
	layout.ol_pgid = info.pgid.u;
	layout.ol_stripe_unit = 0;
	MOSDOp *pop = new MOSDOp(osd->messenger->get_myinst(), 0, osd->get_tid(),
				 oid,
				 layout,
				 osd->osdmap->get_epoch(),
				 CEPH_OSD_OP_UNBALANCEREADS);
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
	osd->messenger->send_message(op, osd->osdmap->get_inst(shedto));
	osd->stat_rd_ops_shed_out++;
	osd->logger->inc("shdout");
	return true;
      }
    }
  } // endif balance reads


  // -- fastpath read?
  // if this is a read and the data is in the cache, do an immediate read.. 
  if ( g_conf.osd_immediate_read_from_cache ) {
    if (osd->store->is_cached( pobject_t(0,0,oid) , 
			       op->get_offset(), 
			       op->get_length() ) == 0) {
      if (!is_primary() && !op->get_source().is_osd()) {
	// am i allowed?
	bool v;
	if (osd->store->getattr(pobject_t(0,0,oid), "balance-reads", &v, 1) < 0) {
	  dout(-10) << "preprocess_op in-cache but no balance-reads on " << oid
		    << ", fwd to primary" << dendl;
	  osd->messenger->send_message(op, osd->osdmap->get_inst(get_primary()));
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

  switch (op->get_op()) {
    
    // reads
  case CEPH_OSD_OP_READ:
  case CEPH_OSD_OP_STAT:
    op_read(op);
    break;
    
    // writes
  case CEPH_OSD_OP_WRNOOP:
  case CEPH_OSD_OP_WRITE:
  case CEPH_OSD_OP_ZERO:
  case CEPH_OSD_OP_DELETE:
  case CEPH_OSD_OP_TRUNCATE:
  case CEPH_OSD_OP_WRLOCK:
  case CEPH_OSD_OP_WRUNLOCK:
  case CEPH_OSD_OP_RDLOCK:
  case CEPH_OSD_OP_RDUNLOCK:
  case CEPH_OSD_OP_UPLOCK:
  case CEPH_OSD_OP_DNLOCK:
  case CEPH_OSD_OP_BALANCEREADS:
  case CEPH_OSD_OP_UNBALANCEREADS:
    op_modify(op);
    break;
    
  default:
    assert(0);
  }
}


void ReplicatedPG::do_sub_op(MOSDSubOp *op)
{
  dout(15) << "do_sub_op " << *op << dendl;

  osd->logger->inc("subop");

  switch (op->get_op()) {
    
    // rep stuff
  case CEPH_OSD_OP_PULL:
    sub_op_pull(op);
    break;
  case CEPH_OSD_OP_PUSH:
    sub_op_push(op);
    break;
    
    // writes
  case CEPH_OSD_OP_WRNOOP:
  case CEPH_OSD_OP_WRITE:
  case CEPH_OSD_OP_ZERO:
  case CEPH_OSD_OP_DELETE:
  case CEPH_OSD_OP_TRUNCATE:
  case CEPH_OSD_OP_WRLOCK:
  case CEPH_OSD_OP_WRUNLOCK:
  case CEPH_OSD_OP_RDLOCK:
  case CEPH_OSD_OP_RDUNLOCK:
  case CEPH_OSD_OP_UPLOCK:
  case CEPH_OSD_OP_DNLOCK:
  case CEPH_OSD_OP_BALANCEREADS:
  case CEPH_OSD_OP_UNBALANCEREADS:
    sub_op_modify(op);
    break;
    
  default:
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



// ========================================================================
// READS

void ReplicatedPG::op_read(MOSDOp *op)
{
  object_t oid = op->get_oid();

  dout(10) << "op_read " << MOSDOp::get_opname(op->get_op())
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
      if (!osd->store->exists(oid) || 
	  osd->store->getattr(oid, "balance-reads", &b, 1) < 0) {
	dout(-10) << "read on replica, object " << oid 
		  << " dne or no balance-reads, fw back to primary" << dendl;
	osd->messenger->send_message(op, osd->osdmap->get_inst(get_acker()));
	return;
      }
    }
  }
  

  // set up reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), true); 
  long r = 0;

  // do it.
  if (oid.rev && !pick_object_rev(oid)) {
    // we have no revision for this request.
    r = -EEXIST;
  } else {
    switch (op->get_op()) {
    case CEPH_OSD_OP_READ:
      {
	// read into a buffer
	bufferlist bl;
	r = osd->store->read(oid, 
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
	r = osd->store->stat(oid, &st);
	if (r >= 0)
	  reply->set_length(st.st_size);
      }
      break;

    default:
      assert(0);
    }
  }
  
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
  osd->messenger->send_message(reply, op->get_client_inst());
  
  delete op;
}






// ========================================================================
// MODIFY

void ReplicatedPG::prepare_log_transaction(ObjectStore::Transaction& t, 
					   osd_reqid_t reqid, pobject_t poid, int op, eversion_t version,
					   objectrev_t crev, objectrev_t rev,
					   eversion_t trim_to)
{
  // clone entry?
  if (crev && rev && rev > crev) {
    eversion_t cv = version;
    cv.version--;
    Log::Entry cloneentry(PG::Log::Entry::CLONE, poid.oid, cv, reqid);
    log.add(cloneentry);

    dout(10) << "prepare_log_transaction " << op
	     << " " << cloneentry
	     << dendl;
  }

  // actual op
  int opcode = Log::Entry::MODIFY;
  if (op == CEPH_OSD_OP_DELETE) opcode = Log::Entry::DELETE;
  Log::Entry logentry(opcode, poid.oid, version, reqid);

  dout(10) << "prepare_log_transaction " << op
           << " " << logentry
           << dendl;

  // append to log
  assert(version > log.top);
  log.add(logentry);
  assert(log.top == version);
  dout(10) << "prepare_log_transaction appended" << dendl;

  // write to pg log on disk
  append_log(t, logentry, trim_to);
}


/** prepare_op_transaction
 * apply an op to the store wrapped in a transaction.
 */
void ReplicatedPG::prepare_op_transaction(ObjectStore::Transaction& t, const osd_reqid_t& reqid,
					  pg_t pgid, int op, pobject_t poid, 
					  off_t offset, off_t length, bufferlist& bl,
					  eversion_t& version, objectrev_t crev, objectrev_t rev)
{
  bool did_clone = false;

  dout(10) << "prepare_op_transaction " << MOSDOp::get_opname( op )
           << " " << poid 
           << " v " << version
	   << " crev " << crev
	   << " rev " << rev
           << dendl;
  
  // WRNOOP does nothing.
  if (op == CEPH_OSD_OP_WRNOOP) 
    return;

  // raise last_complete?
  if (info.last_complete == info.last_update)
    info.last_complete = version;
  
  // raise last_update.
  assert(version > info.last_update);
  info.last_update = version;
  
  // write pg info
  t.collection_setattr(pgid, "info", &info, sizeof(info));

  // clone?
  if (crev && rev && rev > crev) {
    assert(0);
    pobject_t noid = poid;  // FIXME ****
    noid.oid.rev = rev;
    dout(10) << "prepare_op_transaction cloning " << poid << " crev " << crev << " to " << noid << dendl;
    t.clone(poid, noid);
    did_clone = true;
  }  

  // apply the op
  switch (op) {

    // -- locking --

  case CEPH_OSD_OP_WRLOCK:
    { // lock object
      t.setattr(poid, "wrlock", &reqid.name, sizeof(entity_name_t));
    }
    break;  
  case CEPH_OSD_OP_WRUNLOCK:
    { // unlock objects
      t.rmattr(poid, "wrlock");
    }
    break;

  case CEPH_OSD_OP_MININCLOCK:
    {
      uint32_t mininc = length;
      t.setattr(poid, "mininclock", &mininc, sizeof(mininc));
    }
    break;

  case CEPH_OSD_OP_BALANCEREADS:
    {
      bool bal = true;
      t.setattr(poid, "balance-reads", &bal, sizeof(bal));
    }
    break;
  case CEPH_OSD_OP_UNBALANCEREADS:
    {
      t.rmattr(poid, "balance-reads");
    }
    break;


    // -- modify --

  case CEPH_OSD_OP_WRITE:
    { // write
      assert(bl.length() == length);
      bufferlist nbl;
      nbl.claim(bl);    // give buffers to store; we keep *op in memory for a long time!
      t.write(poid, offset, length, nbl);
    }
    break;
    
  case CEPH_OSD_OP_ZERO:
    {
      // zero, remove, or truncate?
      struct stat st;
      int r = osd->store->stat(poid, &st);
      if (r >= 0) {
	if (offset == 0 && offset + length >= (off_t)st.st_size) 
	  t.remove(poid);
	else
	  t.zero(poid, offset, length);
      } else {
	// noop?
	dout(10) << "apply_transaction zero on " << poid << ", but dne?  stat returns " << r << dendl;
      }
    }
    break;

  case CEPH_OSD_OP_TRUNCATE:
    { // truncate
      t.truncate(poid, length);
    }
    break;
    
  case CEPH_OSD_OP_DELETE:
    { // delete
      t.remove(poid);
    }
    break;
    
  default:
    assert(0);
  }
  
  // object collection, version
  if (op == CEPH_OSD_OP_DELETE) {
    // remove object from c
    t.collection_remove(pgid, poid);
  } else {
    // add object to c
    t.collection_add(pgid, poid);
    
    // object version
    t.setattr(poid, "version", &version, sizeof(version));

    // set object crev
    if (crev == 0 ||   // new object
	did_clone)     // we cloned
      t.setattr(poid, "crev", &rev, sizeof(rev));
  }
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
  if (repop->can_send_commit() &&
      repop->op->wants_commit()) {
    // send commit.
    MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osd->osdmap->get_epoch(), true);
    dout(10) << "put_repop  sending commit on " << *repop << " " << reply << dendl;
    osd->messenger->send_message(reply, repop->op->get_client_inst());
    repop->sent_commit = true;
  }

  // ack?
  else if (repop->can_send_ack() &&
           repop->op->wants_ack()) {
    // apply
    if (!repop->applied)
      apply_repop(repop);

    // send ack
    MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osd->osdmap->get_epoch(), false);
    dout(10) << "put_repop  sending ack on " << *repop << " " << reply << dendl;
    osd->messenger->send_message(reply, repop->op->get_client_inst());
    repop->sent_ack = true;

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
  pobject_t poid = repop->op->get_oid();
  dout(7) << " issue_repop rep_tid " << repop->rep_tid
          << " o " << poid
          << " to osd" << dest
          << dendl;
  
  // forward the write/update/whatever
  MOSDSubOp *wr = new MOSDSubOp(repop->op->get_reqid(), info.pgid, poid,
				repop->op->get_op(), 
				repop->op->get_offset(), repop->op->get_length(), 
				osd->osdmap->get_epoch(), 
				repop->rep_tid, repop->new_version);
  wr->get_data() = repop->op->get_data();   // _copy_ bufferlist
  wr->set_pg_trim_to(peers_complete_thru);
  wr->set_peer_stat(osd->get_my_stat_for(now, dest));
  osd->messenger->send_message(wr, osd->osdmap->get_inst(dest));
}

ReplicatedPG::RepGather *ReplicatedPG::new_rep_gather(MOSDOp *op, tid_t rep_tid, eversion_t nv)
{
  dout(10) << "new_rep_gather rep_tid " << rep_tid << " on " << *op << dendl;
  RepGather *repop = new RepGather(op, rep_tid, nv, info.last_complete);
  
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



objectrev_t ReplicatedPG::assign_version(MOSDOp *op)
{
  object_t oid = op->get_oid();

  // check crev
  objectrev_t crev = 0;
  osd->store->getattr(oid, "crev", (char*)&crev, sizeof(crev));

  // assign version
  eversion_t clone_version;
  eversion_t nv = log.top;
  if (op->get_op() != CEPH_OSD_OP_WRNOOP) {
    nv.epoch = osd->osdmap->get_epoch();
    nv.version++;
    assert(nv > info.last_update);
    assert(nv > log.top);

    // will clone?
    if (crev && op->get_oid().rev && op->get_oid().rev > crev) {
      clone_version = nv;
      nv.version++;
    }

    if (op->get_version().version) {
      // replay!
      if (nv.version < op->get_version().version) {
        nv.version = op->get_version().version; 

	// clone?
	if (crev && op->get_oid().rev && op->get_oid().rev > crev) {
	  // backstep clone
	  clone_version = nv;
	  clone_version.version--;
	}
      }
    }
  }

  // set version in op, for benefit of client and our eventual reply
  op->set_version(nv);

  return crev;
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


void ReplicatedPG::op_modify(MOSDOp *op)
{
  int whoami = osd->get_nodeid();
  object_t oid = op->get_oid();
  const char *opname = MOSDOp::get_opname(op->get_op());

  // make sure it looks ok
  if (op->get_op() == CEPH_OSD_OP_WRITE &&
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
  
  // balance-reads set?
  char v;
  if ((op->get_op() != CEPH_OSD_OP_BALANCEREADS && op->get_op() != CEPH_OSD_OP_UNBALANCEREADS) &&
      (osd->store->getattr(op->get_oid(), "balance-reads", &v, 1) >= 0 ||
       balancing_reads.count(op->get_oid()))) {
    
    if (!unbalancing_reads.count(op->get_oid())) {
      // unbalance
      dout(-10) << "preprocess_op unbalancing-reads on " << op->get_oid() << dendl;
      unbalancing_reads.insert(op->get_oid());
      
      ceph_object_layout layout;
      layout.ol_pgid = info.pgid.u;
      layout.ol_stripe_unit = 0;
      MOSDOp *pop = new MOSDOp(osd->messenger->get_myinst(), 0, osd->get_tid(),
			       op->get_oid(),
			       layout,
			       osd->osdmap->get_epoch(),
			       CEPH_OSD_OP_UNBALANCEREADS);
      do_op(pop);
    }

    // add to wait queue
    dout(-10) << "preprocess_op waiting for unbalance-reads on " << op->get_oid() << dendl;
    waiting_for_unbalanced_reads[op->get_oid()].push_back(op);
    return;
  }


  // dup op?
  if (is_dup(op->get_reqid())) {
    dout(3) << "op_modify " << opname << " dup op " << op->get_reqid()
             << ", doing WRNOOP" << dendl;
    op->set_op(CEPH_OSD_OP_WRNOOP);
    opname = MOSDOp::get_opname(op->get_op());
  }

  // assign the op a version
  objectrev_t crev = assign_version(op);
  eversion_t nv = op->get_version();

  // are any peers missing this?
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    if (peer_missing.count(peer) &&
        peer_missing[peer].is_missing(oid)) {
      // push it before this update. 
      // FIXME, this is probably extra much work (eg if we're about to overwrite)
      push(oid, peer);
    }
  }

  dout(10) << "op_modify " << opname 
           << " " << oid 
           << " v " << nv 
    //<< " crev " << crev
	   << " rev " << op->get_oid().rev
           << " " << op->get_offset() << "~" << op->get_length()
           << dendl;  

  if (op->get_op() == CEPH_OSD_OP_WRITE) {
    osd->logger->inc("c_wr");
    osd->logger->inc("c_wrb", op->get_length());
  }

  // note my stats
  utime_t now = g_clock.now();

  // issue replica writes
  tid_t rep_tid = osd->get_tid();
  RepGather *repop = new_rep_gather(op, rep_tid, nv);
  for (unsigned i=1; i<acting.size(); i++)
    issue_repop(repop, acting[i], now);

  // we are acker.
  if (op->get_op() != CEPH_OSD_OP_WRNOOP) {
    // log and update later.
    pobject_t poid = oid;
    prepare_log_transaction(repop->t, op->get_reqid(), poid, op->get_op(), nv,
			    crev, op->get_oid().rev, peers_complete_thru);
    prepare_op_transaction(repop->t, op->get_reqid(),
			   info.pgid, op->get_op(), poid, 
			   op->get_offset(), op->get_length(), op->get_data(),
			   nv, crev, op->get_oid().rev);
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
  pobject_t poid = op->get_poid();
  eversion_t nv = op->get_version();

  const char *opname = MOSDOp::get_opname(op->get_op());

  // check crev
  objectrev_t crev = 0;
  osd->store->getattr(poid, "crev", (char*)&crev, sizeof(crev));

  dout(10) << "sub_op_modify " << opname 
           << " " << poid 
           << " v " << nv 
           << " " << op->get_offset() << "~" << op->get_length()
           << dendl;  
  
  // note peer's stat
  int fromosd = op->get_source().num();
  osd->take_peer_stat(fromosd, op->get_peer_stat());

  // we better not be missing this.
  assert(!missing.is_missing(poid.oid));

  // prepare our transaction
  ObjectStore::Transaction t;

  // do op
  int ackerosd = acting[0];
  osd->logger->inc("r_wr");
  osd->logger->inc("r_wrb", op->get_length());
  
  if (op->get_op() != CEPH_OSD_OP_WRNOOP) {
    prepare_log_transaction(t, op->get_reqid(), op->get_poid(), op->get_op(), op->get_version(),
			    crev, 0, op->get_pg_trim_to());
    prepare_op_transaction(t, op->get_reqid(), 
			   info.pgid, op->get_op(), poid, 
			   op->get_offset(), op->get_length(), op->get_data(), 
			   nv, crev, 0);
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
				   osd->osdmap->get_epoch(), tid, v);
  osd->messenger->send_message(subop, osd->osdmap->get_inst(fromosd));
  
  // take note
  assert(objects_pulling.count(poid.oid) == 0);
  num_pulling++;
  objects_pulling[poid.oid] = v;
}


/** push - send object to a peer
 */
void ReplicatedPG::push(pobject_t poid, int peer)
{
  // read data+attrs
  bufferlist bl;
  eversion_t v;
  int vlen = sizeof(v);
  map<string,bufferptr> attrset;
  
  ObjectStore::Transaction t;
  t.read(poid, 0, 0, &bl);
  t.getattr(poid, "version", &v, &vlen);
  t.getattrs(poid, attrset);
  unsigned tr = osd->store->apply_transaction(t);
  
  assert(tr == 0);  // !!!

  // ok
  dout(7) << "push " << poid << " v " << v 
          << " size " << bl.length()
          << " to osd" << peer
          << dendl;

  osd->logger->inc("r_push");
  osd->logger->inc("r_pushb", bl.length());
  
  // send
  osd_reqid_t rid;  // useless?
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, poid, CEPH_OSD_OP_PUSH, 0, bl.length(),
				osd->osdmap->get_epoch(), osd->get_tid(), v);
  subop->set_data(bl);   // note: claims bl, set length above here!
  subop->set_attrset(attrset);
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
  const pobject_t poid = op->get_poid();
  const eversion_t v = op->get_version();
  int from = op->get_source().num();

  dout(7) << "op_pull " << poid << " v " << op->get_version()
          << " from " << op->get_source()
          << dendl;

  // is a replica asking?  are they missing it?
  if (is_primary()) {
    // primary
    assert(peer_missing.count(from));  // we had better know this, from the peering process.

    if (!peer_missing[from].is_missing(poid.oid)) {
      dout(7) << "op_pull replica isn't actually missing it, we must have already pushed to them" << dendl;
      delete op;
      return;
    }

    // do we have it yet?
    if (is_missing_object(poid.oid)) {
      wait_for_missing_object(poid.oid, op);
      return;
    }
  } else {
    // non-primary
    if (missing.is_missing(poid.oid)) {
      dout(7) << "op_pull not primary, and missing " << poid << ", ignoring" << dendl;
      delete op;
      return;
    }
  }
    
  // push it back!
  push(poid, op->get_source().num());
}


/** op_push
 * NOTE: called from opqueue.
 */
void ReplicatedPG::sub_op_push(MOSDSubOp *op)
{
  pobject_t poid = op->get_poid();
  eversion_t v = op->get_version();

  if (!is_missing_object(poid.oid)) {
    dout(7) << "sub_op_push not missing " << poid << dendl;
    return;
  }
  
  dout(7) << "op_push " 
          << poid 
          << " v " << v 
          << " size " << op->get_length() << " " << op->get_data().length()
          << dendl;

  assert(op->get_data().length() == op->get_length());
  
  // write object and add it to the PG
  ObjectStore::Transaction t;
  t.remove(poid);  // in case old version exists
  t.write(poid, 0, op->get_length(), op->get_data());
  t.setattrs(poid, op->get_attrset());
  t.collection_add(info.pgid, poid);

  // close out pull op?
  num_pulling--;
  if (objects_pulling.count(poid.oid))
    objects_pulling.erase(poid.oid);
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
  t.collection_setattr(info.pgid, "info", &info, sizeof(info));
  unsigned r = osd->store->apply_transaction(t);
  assert(r == 0);



  // am i primary?  are others missing this too?
  if (is_primary()) {
    for (unsigned i=1; i<acting.size(); i++) {
      int peer = acting[i];
      assert(peer_missing.count(peer));
      if (peer_missing[peer].is_missing(poid.oid)) 
	push(poid, peer);  // ok, push it, and they (will) have it now.
    }
  }

  // kick waiters
  if (waiting_for_missing_object.count(poid.oid)) {
    osd->take_waiters(waiting_for_missing_object[poid.oid]);
    waiting_for_missing_object.erase(poid.oid);
  }

  if (is_primary()) {
    // continue recovery
    do_recovery();
  } else {
    // ack if i'm a replica and being pushed to.
    MOSDSubOpReply *reply = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), false); 
    osd->messenger->send_message(reply, op->get_source_inst());
  }

  delete op;
}



/*
 * pg status change notification
 */

void ReplicatedPG::on_osd_failure(int o)
{
  dout(10) << "on_osd_failure " << o << dendl;
  // do async; repop_ack() may modify pg->repop_gather
  list<RepGather*> ls;  
  for (hash_map<tid_t,RepGather*>::iterator p = rep_gather.begin();
       p != rep_gather.end();
       p++) {
    dout(-1) << "checking repop tid " << p->first << dendl;
    if (p->second->waitfor_ack.count(o) ||
	p->second->waitfor_commit.count(o)) 
      ls.push_back(p->second);
  }
  for (list<RepGather*>::iterator p = ls.begin();
       p != ls.end();
       p++)
    repop_ack(*p, -1, true, o);
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
  osd->num_pulling -= objects_pulling.size();
  objects_pulling.clear();
  num_pulling = 0;
  pushing.clear();
}

/**
 * do one recovery op.
 * return true if done, false if nothing left to do.
 */
bool ReplicatedPG::do_recovery()
{
  assert(is_primary());
  /*if (!is_primary()) {
    dout(10) << "do_recovery not primary, doing nothing" << dendl;
    return true;
  }
  */

  if (info.is_uptodate()) {  // am i up to date?
    if (!is_all_uptodate()) {
      dout(-10) << "do_recovery i'm clean but replicas aren't, starting peer recovery" << dendl;
      do_peer_recovery();
    } else {
      dout(-10) << "do_recovery all clean, nothing to do" << dendl;
    }
    return true;
  }

  dout(-10) << "do_recovery pulling " << objects_pulling.size() << " in pg, "
           << osd->num_pulling << "/" << g_conf.osd_max_pull << " total"
           << dendl;
  dout(10) << "do_recovery " << missing << dendl;

  // can we slow down on this PG?
  if (osd->num_pulling >= g_conf.osd_max_pull && !objects_pulling.empty()) {
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
             << (objects_pulling.count(latest->oid) ? " (pulling)":"")
             << dendl;

    if (latest->is_update() &&
        !objects_pulling.count(latest->oid) &&
        missing.is_missing(latest->oid)) {
      pull(latest->oid);
      return true;
    }
    
    log.requested_to++;
  }

  if (!objects_pulling.empty()) {
    dout(7) << "do_recovery requested everything, still waiting" << dendl;
    return false;
  }

  // done?
  assert(missing.num_missing() == 0);
  assert(info.last_complete == info.last_update);
  
  if (is_primary()) {
    // i am primary
    dout(-7) << "do_recovery complete, cleaning strays" << dendl;
    uptodate_set.insert(osd->whoami);
    if (is_all_uptodate())
      finish_recovery();
  } else {
    // tell primary
    dout(7) << "do_recovery complete, telling primary" << dendl;
    list<PG::Info> ls;
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
    eversion_t v = peer_missing[peer].rmissing.begin()->first;

    push(oid, peer);

    // do other peers need it too?
    for (i++; i<acting.size(); i++) {
      int peer = acting[i];
      if (peer_missing.count(peer) &&
          peer_missing[peer].is_missing(oid)) 
	push(oid, peer);
    }

    return;
  }
  
  // nothing to do!
  dout(-10) << "do_peer_recovery - nothing to do!" << dendl;

  if (is_all_uptodate()) 
    finish_recovery();
}

void ReplicatedPG::purge_strays()
{
  dout(10) << "purge_strays " << stray_set << dendl;
  
  for (set<int>::iterator p = stray_set.begin();
       p != stray_set.end();
       p++) {
    dout(10) << "sending PGRemove to osd" << *p << dendl;
    set<pg_t> ls;
    ls.insert(info.pgid);
    MOSDPGRemove *m = new MOSDPGRemove(osd->osdmap->get_epoch(), ls);
    osd->messenger->send_message(m, osd->osdmap->get_inst(*p));
  }

  stray_set.clear();
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
    list<pobject_t> ls;
    osd->store->collection_list(info.pgid, ls);
    set<object_t> s;
    
    for (list<pobject_t>::iterator i = ls.begin();
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
          dout(10) << " deleting " << p->oid
                   << " when " << p->version << dendl;
          t.remove(p->oid);
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
      dout(10) << " deleting stray " << *i << dendl;
      t.remove(*i);
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
        dout(10) << " deleting " << p->oid
                 << " when " << p->version << dendl;
        t.remove(p->oid);
      } else {
        // keep old(+missing) objects, just for kicks.
      }
    }
  }
}
