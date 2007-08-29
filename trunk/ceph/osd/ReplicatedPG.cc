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

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGRemove.h"

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

  if (g_conf.osd_rep == OSD_REP_CHAIN) {
    return e >= info.history.same_since;   // whole pg set same
  } else {
    // primary, splay
    return (e >= info.history.same_primary_since &&
	    e >= info.history.same_acker_since);    
  }
}

// ====================
// missing objects

bool ReplicatedPG::is_missing_object(object_t oid)
{
  return missing.missing.count(oid);
}
 

void ReplicatedPG::wait_for_missing_object(object_t oid, MOSDOp *op)
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
  waiting_for_missing_object[oid].push_back(op);
}




/** preprocess_op - preprocess an op (before it gets queued).
 * fasttrack read
 */
bool ReplicatedPG::preprocess_op(MOSDOp *op)
{
  // we only care about reads here on out..
  if (!op->is_read()) 
    return false;


  // -- load balance reads --
  if (g_conf.osd_balance_reads &&
      is_primary() &&
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
    
    // -- flash crowd?
    if (!op->get_source().is_osd() && 
	is_primary()) {
      // add sample
      osd->iat_averager.add_sample( op->get_oid(), (double)g_clock.now() );
      
      // candidate?
      bool is_flash_crowd_candidate = osd->iat_averager.is_flash_crowd_candidate( op->get_oid() );
      bool is_balanced = false;
      bool b;
      if (osd->store->getattr(op->get_oid(), "balance-reads", &b, 1) >= 0)
	is_balanced = true;
      
      if (!is_balanced && is_flash_crowd_candidate &&
	  balancing_reads.count(op->get_oid()) == 0) {
	dout(-10) << "preprocess_op balance-reads on " << op->get_oid() << dendl;
	balancing_reads.insert(op->get_oid());
	MOSDOp *pop = new MOSDOp(osd->messenger->get_myinst(), 0, osd->get_tid(),
				 op->get_oid(),
				 ObjectLayout(info.pgid),
				 osd->osdmap->get_epoch(),
				 OSD_OP_BALANCEREADS);
	do_op(pop);
      }
      if (is_balanced && !is_flash_crowd_candidate &&
	  !unbalancing_reads.count(op->get_oid()) == 0) {
	dout(-10) << "preprocess_op unbalance-reads on " << op->get_oid() << dendl;
	unbalancing_reads.insert(op->get_oid());
	MOSDOp *pop = new MOSDOp(osd->messenger->get_myinst(), 0, osd->get_tid(),
				 op->get_oid(),
				 ObjectLayout(info.pgid),
				 osd->osdmap->get_epoch(),
				 OSD_OP_UNBALANCEREADS);
	do_op(pop);
      }
    }
    
    
    // check my load. 
    // TODO xxx we must also compare with our own load
    // if i am x percentage higher than replica , 
    // redirect the read 
    
    if ( g_conf.osd_balance_reads == LOAD_LATENCY) {
      double mean_read_time = osd->load_calc.get_average();
      
      if ( mean_read_time != -1 ) {
	
	for (unsigned i=1; 
	     i<acting.size(); 
	     ++i) {
	  int peer = acting[i];
	  
	  dout(10) << "my read time " << mean_read_time 
		   << "peer_readtime" << osd->peer_read_time[peer] 
		   << " of peer" << peer << dendl;
	  
	  if ( osd->peer_read_time.count(peer) &&
	       ( (osd->peer_read_time[peer]*100/mean_read_time) <
		 (100 - g_conf.osd_load_diff_percent))) {
	    dout(10) << " forwarding to peer osd" << peer << dendl;
	    
	    osd->messenger->send_message(op, osd->osdmap->get_inst(peer));
	    return true;
	  }
	}
      }
    }
    else if ( g_conf.osd_balance_reads == LOAD_QUEUE_SIZE ) {
      // am i above my average?
      float my_avg = osd->hb_stat_qlen / osd->hb_stat_ops;
      
      if (osd->pending_ops > my_avg) {
	// is there a peer who is below my average?
	for (unsigned i=1; i<acting.size(); ++i) {
	  int peer = acting[i];
	  if (osd->peer_qlen.count(peer) &&
	      osd->peer_qlen[peer] < my_avg) {
	    // calculate a probability that we should redirect
	    float p = (my_avg - osd->peer_qlen[peer]) / my_avg;             // this is dumb.
	    
	    if (drand48() <= p) {
	      // take the first one
	      dout(10) << "my qlen " << osd->pending_ops << " > my_avg " << my_avg
		       << ", p=" << p 
		       << ", fwd to peer w/ qlen " << osd->peer_qlen[peer]
		       << " osd" << peer
		       << dendl;
	      osd->messenger->send_message(op, osd->osdmap->get_inst(peer));
	      return true;
	    }
	  }
	}
      }
    }
    
    else if ( g_conf.osd_balance_reads == LOAD_HYBRID ) {
      // am i above my average?
      float my_avg = osd->hb_stat_qlen / osd->hb_stat_ops;
      
      if (osd->pending_ops > my_avg) {
	// is there a peer who is below my average?
	for (unsigned i=1; i<acting.size(); ++i) {
	  int peer = acting[i];
	  if (osd->peer_qlen.count(peer) &&
	      osd->peer_qlen[peer] < my_avg) {
	    // calculate a probability that we should redirect
	    //float p = (my_avg - peer_qlen[peer]) / my_avg;             // this is dumb.
	    
	    double mean_read_time = osd->load_calc.get_average();
	    
	    if ( mean_read_time != -1 &&  
		 osd->peer_read_time.count(peer) &&
		 ( (osd->peer_read_time[peer]*100/mean_read_time) <
		   ( 100 - g_conf.osd_load_diff_percent) ) )
	      //if (drand48() <= p) {
	      // take the first one
	      dout(10) << "using hybrid :my qlen " << osd->pending_ops << " > my_avg " << my_avg
		       << "my read time  "<<  mean_read_time
		       << "peer read time " << osd->peer_read_time[peer]  
		       << ", fwd to peer w/ qlen " << osd->peer_qlen[peer]
		       << " osd" << peer
		       << dendl;
	    osd->messenger->send_message(op, osd->osdmap->get_inst(peer));
	    return true;
	  }
	}
      }
    }
  } // endif balance reads


  // -- fastpath read?
  // if this is a read and the data is in the cache, do an immediate read.. 
  if ( g_conf.osd_immediate_read_from_cache ) {
    if (osd->store->is_cached( op->get_oid() , 
			       op->get_offset(), 
			       op->get_length() ) == 0) {
      if (!is_primary()) {
	// am i allowed?
	bool v;
	if (osd->store->getattr(op->get_oid(), "balance-reads", &v, 1) < 0) {
	  dout(10) << "preprocess_op in-cache but no balance-reads on " << op->get_oid()
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
  case OSD_OP_READ:
  case OSD_OP_STAT:
    op_read(op);
    break;
    
    // rep stuff
  case OSD_OP_PULL:
    op_pull(op);
    break;
  case OSD_OP_PUSH:
    op_push(op);
    break;
    
    // writes
  case OSD_OP_WRNOOP:
  case OSD_OP_WRITE:
  case OSD_OP_ZERO:
  case OSD_OP_DELETE:
  case OSD_OP_TRUNCATE:
  case OSD_OP_WRLOCK:
  case OSD_OP_WRUNLOCK:
  case OSD_OP_RDLOCK:
  case OSD_OP_RDUNLOCK:
  case OSD_OP_UPLOCK:
  case OSD_OP_DNLOCK:
  case OSD_OP_BALANCEREADS:
  case OSD_OP_UNBALANCEREADS:
    if (op->get_source().is_osd()) {
      op_rep_modify(op);
    } else {
      // go go gadget pg
      op_modify(op);
    }
    break;
    
  default:
    assert(0);
  }
}

void ReplicatedPG::do_op_reply(MOSDOpReply *r)
{
  if (r->get_op() == OSD_OP_PUSH) {
    // continue peer recovery
    op_push_reply(r);
  } else {
    // must be replication.
    tid_t rep_tid = r->get_rep_tid();
    
    if (rep_gather.count(rep_tid)) {
      // oh, good.
      int fromosd = r->get_source().num();
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
  if (!is_primary() &&
      !(op->get_source().is_osd() &&
	op->get_source().num() == get_primary())) {
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
  

  // set up reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), true); 
  long r = 0;

  // do it.
  if (oid.rev && !pick_object_rev(oid)) {
    // we have no revision for this request.
    r = -EEXIST;
  } else {
    switch (op->get_op()) {
    case OSD_OP_READ:
      {
	// read into a buffer
	bufferlist bl;
	r = osd->store->read(oid, 
			     op->get_offset(), op->get_length(),
			     bl);
	reply->set_data(bl);
	reply->set_length(r);
	dout(15) << " read got " << r << " / " << op->get_length() << " bytes from obj " << oid << dendl;
      }
      osd->logger->inc("c_rd");
      osd->logger->inc("c_rdb", op->get_length());
      break;

    case OSD_OP_STAT:
      {
	struct stat st;
	memset(&st, sizeof(st), 0);
	r = osd->store->stat(oid, &st);
	if (r >= 0)
	  reply->set_object_size(st.st_size);
      }
      break;

    default:
      assert(0);
    }
  }
  
  if (r >= 0) {
    reply->set_result(0);

    dout(10) <<  "READ TIME DIFF"
	     << (double)g_clock.now()-op->get_received_time()
	     << dendl;
    osd->load_calc.add((double)g_clock.now() - op->get_received_time());

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
					   MOSDOp *op, eversion_t& version, 
					   objectrev_t crev, objectrev_t rev,
					   eversion_t trim_to)
{
  const object_t oid = op->get_oid();

  // clone entry?
  if (crev && rev && rev > crev) {
    eversion_t cv = version;
    cv.version--;
    Log::Entry cloneentry(PG::Log::Entry::CLONE, oid, cv, op->get_reqid());
    log.add(cloneentry);

    dout(10) << "prepare_log_transaction " << op->get_op()
	     << " " << cloneentry
	     << dendl;
  }

  // actual op
  int opcode = Log::Entry::MODIFY;
  if (op->get_op() == OSD_OP_DELETE) opcode = Log::Entry::DELETE;
  Log::Entry logentry(opcode, oid, version, op->get_reqid());

  dout(10) << "prepare_log_transaction " << op->get_op()
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
void ReplicatedPG::prepare_op_transaction(ObjectStore::Transaction& t, 
					  MOSDOp *op, eversion_t& version, 
					  objectrev_t crev, objectrev_t rev)
{
  const object_t oid = op->get_oid();
  const pg_t pgid = op->get_pg();

  bool did_clone = false;

  dout(10) << "prepare_op_transaction " << MOSDOp::get_opname( op->get_op() )
           << " " << oid 
           << " v " << version
	   << " crev " << crev
	   << " rev " << rev
           << dendl;
  
  // WRNOOP does nothing.
  if (op->get_op() == OSD_OP_WRNOOP) 
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
    object_t noid = oid;
    noid.rev = rev;
    dout(10) << "prepare_op_transaction cloning " << oid << " crev " << crev << " to " << noid << dendl;
    t.clone(oid, noid);
    did_clone = true;
  }  

  // apply the op
  switch (op->get_op()) {

    // -- locking --

  case OSD_OP_WRLOCK:
    { // lock object
      t.setattr(oid, "wrlock", &op->get_client(), sizeof(entity_name_t));
    }
    break;  
  case OSD_OP_WRUNLOCK:
    { // unlock objects
      t.rmattr(oid, "wrlock");
    }
    break;

  case OSD_OP_MININCLOCK:
    {
      uint32_t mininc = op->get_length();
      t.setattr(oid, "mininclock", &mininc, sizeof(mininc));
    }
    break;

  case OSD_OP_BALANCEREADS:
    {
      bool bal = true;
      t.setattr(oid, "balance-reads", &bal, sizeof(bal));
    }
    break;
  case OSD_OP_UNBALANCEREADS:
    {
      t.rmattr(oid, "balance-reads");
    }
    break;


    // -- modify --

  case OSD_OP_WRITE:
    { // write
      assert(op->get_data().length() == op->get_length());
      bufferlist bl;
      bl.claim( op->get_data() );  // give buffers to store; we keep *op in memory for a long time!
      
      //if (oid < 100000000000000ULL)  // hack hack-- don't write client data
      t.write( oid, op->get_offset(), op->get_length(), bl );
    }
    break;
    
  case OSD_OP_ZERO:
    {
      // zero, remove, or truncate?
      struct stat st;
      int r = osd->store->stat(oid, &st);
      if (r >= 0) {
	if (op->get_length() == 0 ||
	    op->get_offset() + (off_t)op->get_length() >= (off_t)st.st_size) {
	  if (op->get_offset()) 
	    t.truncate(oid, op->get_length() + op->get_offset());
	  else
	    t.remove(oid);
	} else {
	  // zero.  the dumb way.  FIXME.
	  bufferptr bp(op->get_length());
	  bp.zero();
	  bufferlist bl;
	  bl.push_back(bp);
	  t.write(oid, op->get_offset(), op->get_length(), bl);
	}
      } else {
	// noop?
	dout(10) << "apply_transaction zero on " << oid << ", but dne?  stat returns " << r << dendl;
      }
    }
    break;

  case OSD_OP_TRUNCATE:
    { // truncate
      t.truncate(oid, op->get_length() );
    }
    break;
    
  case OSD_OP_DELETE:
    { // delete
      t.remove(oid);
    }
    break;
    
  default:
    assert(0);
  }
  
  // object collection, version
  if (op->get_op() == OSD_OP_DELETE) {
    // remove object from c
    t.collection_remove(pgid, oid);
  } else {
    // add object to c
    t.collection_add(pgid, oid);
    
    // object version
    t.setattr(oid, "version", &version, sizeof(version));

    // set object crev
    if (crev == 0 ||   // new object
	did_clone)     // we cloned
      t.setattr(oid, "crev", &rev, sizeof(rev));
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
  case OSD_OP_UNBALANCEREADS:
    dout(-10) << "apply_repop  completed unbalance-reads on " << oid << dendl;
    unbalancing_reads.erase(oid);
    if (waiting_for_unbalanced_reads.count(oid)) {
      osd->take_waiters(waiting_for_unbalanced_reads[oid]);
      waiting_for_unbalanced_reads.erase(oid);
    }
    break;

  case OSD_OP_BALANCEREADS:
    dout(-10) << "apply_repop  completed balance-reads on " << oid << dendl;
    /*
    if (waiting_for_balanced_reads.count(oid)) {
      osd->take_waiters(waiting_for_balanced_reads[oid]);
      waiting_for_balanced_reads.erase(oid);
    }
    */
    break;
    
  case OSD_OP_WRUNLOCK:
    dout(-10) << "apply_repop  completed wrunlock on " << oid << dendl;
    if (waiting_for_wr_unlock.count(oid)) {
      osd->take_waiters(waiting_for_wr_unlock[oid]);
      waiting_for_wr_unlock.erase(oid);
    }
    break;
  }   
  

}

void ReplicatedPG::put_rep_gather(RepGather *repop)
{
  dout(10) << "put_repop " << *repop << dendl;
  
  // commit?
  if (repop->can_send_commit() &&
      repop->op->wants_commit()) {
    // send commit.
    if (repop->op->wants_reply()) {
      MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osd->osdmap->get_epoch(), true);
      dout(10) << "put_repop  sending commit on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, repop->op->get_client_inst());
    }
    repop->sent_commit = true;
  }

  // ack?
  else if (repop->can_send_ack() &&
           repop->op->wants_ack()) {
    // apply
    apply_repop(repop);

    // send ack
    if (repop->op->wants_reply()) {
      MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osd->osdmap->get_epoch(), false);
      dout(10) << "put_repop  sending ack on " << *repop << " " << reply << dendl;
      osd->messenger->send_message(reply, repop->op->get_client_inst());
    }
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
    //repop->lock.Unlock();  
	
    assert(rep_gather.count(repop->rep_tid));
    rep_gather.erase(repop->rep_tid);
	
    delete repop->op;
    delete repop;

  } else {
    //repop->lock.Unlock();
  }
}


void ReplicatedPG::issue_repop(MOSDOp *op, int dest)
{
  object_t oid = op->get_oid();
  
  dout(7) << " issue_repop rep_tid " << op->get_rep_tid()
          << " o " << oid
          << " to osd" << dest
          << dendl;
  
  // forward the write/update/whatever
  MOSDOp *wr = new MOSDOp(op->get_client_inst(), op->get_client_inc(), op->get_reqid().tid,
                          oid,
                          ObjectLayout(info.pgid),
                          osd->osdmap->get_epoch(),
                          op->get_op());
  wr->get_data() = op->get_data();   // _copy_ bufferlist
  wr->set_length(op->get_length());
  wr->set_offset(op->get_offset());
  wr->set_version(op->get_version());
  
  wr->set_rep_tid(op->get_rep_tid());
  wr->set_pg_trim_to(peers_complete_thru);
  
  osd->messenger->send_message(wr, osd->osdmap->get_inst(dest));
}

ReplicatedPG::RepGather *ReplicatedPG::new_rep_gather(MOSDOp *op)
{
  dout(10) << "new_rep_gather rep_tid " << op->get_rep_tid() << " on " << *op << dendl;
  int whoami = osd->get_nodeid();

  RepGather *repop = new RepGather(op, op->get_rep_tid(), 
                                               op->get_version(), 
                                               info.last_complete);
  
  // osds. commits all come to me.
  for (unsigned i=0; i<acting.size(); i++) {
    int osd = acting[i];
    repop->osds.insert(osd);
    repop->waitfor_commit.insert(osd);
  }

  // acks vary:
  if (g_conf.osd_rep == OSD_REP_CHAIN) {
    // chain rep. 
    // there's my local ack...
    repop->osds.insert(whoami);
    repop->waitfor_ack.insert(whoami);
    repop->waitfor_commit.insert(whoami);

    // also, the previous guy will ack to me
    int myrank = osd->osdmap->calc_pg_rank(whoami, acting);
    if (myrank > 0) {
      int osd = acting[ myrank-1 ];
      repop->osds.insert(osd);
      repop->waitfor_ack.insert(osd);
      repop->waitfor_commit.insert(osd);
    }
  } else {
    // primary, splay.  all osds ack to me.
    for (unsigned i=0; i<acting.size(); i++) {
      int osd = acting[i];
      repop->waitfor_ack.insert(osd);
    }
  }

  repop->start = g_clock.now();

  rep_gather[ repop->rep_tid ] = repop;

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
  if (op->get_op() != OSD_OP_WRNOOP) {
    nv.epoch = osd->osdmap->get_epoch();
    nv.version++;
    assert(nv > info.last_update);
    assert(nv > log.top);

    // will clone?
    if (crev && op->get_rev() && op->get_rev() > crev) {
      clone_version = nv;
      nv.version++;
    }

    if (op->get_version().version) {
      // replay!
      if (nv.version < op->get_version().version) {
        nv.version = op->get_version().version; 

	// clone?
	if (crev && op->get_rev() && op->get_rev() > crev) {
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
  MOSDOp *op;
  int destosd;

  eversion_t pg_last_complete;

  Mutex lock;
  Cond cond;
  bool acked;
  bool waiting;

  C_OSD_RepModifyCommit(ReplicatedPG *p, MOSDOp *oo, int dosd, eversion_t lc) : 
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
    pg->op_rep_modify_commit(op, destosd, pg_last_complete);
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

  // --- locking ---

  // wrlock?
  if (op->get_op() != OSD_OP_WRNOOP &&  // except WRNOOP; we just want to flush
      block_if_wrlocked(op)) 
    return; // op will be handled later, after the object unlocks
  
  // balance-reads set?
  char v;
  if ((op->get_op() != OSD_OP_BALANCEREADS && op->get_op() != OSD_OP_UNBALANCEREADS) &&
      (osd->store->getattr(op->get_oid(), "balance-reads", &v, 1) >= 0 ||
       balancing_reads.count(op->get_oid()))) {
    
    if (!unbalancing_reads.count(op->get_oid())) {
      // unbalance
      dout(-10) << "preprocess_op unbalancing-reads on " << op->get_oid() << dendl;
      unbalancing_reads.insert(op->get_oid());
      
      MOSDOp *pop = new MOSDOp(osd->messenger->get_myinst(), 0, osd->get_tid(),
			       op->get_oid(),
			       ObjectLayout(info.pgid),
			       osd->osdmap->get_epoch(),
			       OSD_OP_UNBALANCEREADS);
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
    op->set_op(OSD_OP_WRNOOP);
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
	   << " rev " << op->get_rev()
           << " " << op->get_offset() << "~" << op->get_length()
           << dendl;  

  if (op->get_op() == OSD_OP_WRITE) {
    osd->logger->inc("c_wr");
    osd->logger->inc("c_wrb", op->get_length());
  }

  // issue replica writes
  RepGather *repop = 0;
  bool alone = (acting.size() == 1);
  tid_t rep_tid = osd->get_tid();
  op->set_rep_tid(rep_tid);

  if (g_conf.osd_rep == OSD_REP_CHAIN && !alone) {
    // chain rep.  send to #2 only.
    int next = acting[1];
    if (acting.size() > 2)
      next = acting[2];
    issue_repop(op, next);
  } 
  else if (g_conf.osd_rep == OSD_REP_SPLAY && !alone) {
    // splay rep.  send to rest.
    for (unsigned i=1; i<acting.size(); ++i)
    //for (unsigned i=acting.size()-1; i>=1; --i)
      issue_repop(op, acting[i]);
  } else {
    // primary rep, or alone.
    repop = new_rep_gather(op);

    // send to rest.
    if (!alone)
      for (unsigned i=1; i<acting.size(); i++)
        issue_repop(op, acting[i]);
  }

  if (repop) {    
    // we are acker.
    if (op->get_op() != OSD_OP_WRNOOP) {
      // log and update later.
      prepare_log_transaction(repop->t, op, nv, crev, op->get_rev(), peers_complete_thru);
      prepare_op_transaction(repop->t, op, nv, crev, op->get_rev());
    }

    // (logical) local ack.
    // (if alone, this will apply the update.)
    get_rep_gather(repop);
    {
      assert(repop->waitfor_ack.count(whoami));
      repop->waitfor_ack.erase(whoami);
    }
    put_rep_gather(repop);

  } else {
    // not acker.  
    // chain or splay.  apply.
    ObjectStore::Transaction t;
    prepare_log_transaction(t, op, nv, crev, op->get_rev(), peers_complete_thru);
    prepare_op_transaction(t, op, nv, crev, op->get_rev());

    C_OSD_RepModifyCommit *oncommit = new C_OSD_RepModifyCommit(this, op, get_acker(), 
                                                                info.last_complete);
    unsigned r = osd->store->apply_transaction(t, oncommit);
    if (r != 0 &&   // no errors
        r != 2) {   // or error on collection_add
      derr(0) << "error applying transaction: r = " << r << dendl;
      assert(r == 0);
    }

    // lets evict the data from our cache to maintain a total large cache size
    if (g_conf.osd_exclusive_caching)
      osd->store->trim_from_cache(op->get_oid(), op->get_offset(), op->get_length());

    oncommit->ack();
  }

}



// replicated 




void ReplicatedPG::op_rep_modify(MOSDOp *op)
{
  object_t oid = op->get_oid();
  eversion_t nv = op->get_version();

  const char *opname = MOSDOp::get_opname(op->get_op());

  // check crev
  objectrev_t crev = 0;
  osd->store->getattr(oid, "crev", (char*)&crev, sizeof(crev));

  dout(10) << "op_rep_modify " << opname 
           << " " << oid 
           << " v " << nv 
           << " " << op->get_offset() << "~" << op->get_length()
           << dendl;  
  
  // we better not be missing this.
  assert(!missing.is_missing(oid));

  // prepare our transaction
  ObjectStore::Transaction t;

  // am i acker?
  RepGather *repop = 0;
  int ackerosd = acting[0];

  if ((g_conf.osd_rep == OSD_REP_CHAIN || g_conf.osd_rep == OSD_REP_SPLAY)) {
    ackerosd = get_acker();
  
    if (is_acker()) {
      // i am tail acker.
      if (rep_gather.count(op->get_rep_tid())) {
        repop = rep_gather[ op->get_rep_tid() ];
      } else {
        repop = new_rep_gather(op);
      }
      
      // infer ack from source
      int fromosd = op->get_source().num();
      get_rep_gather(repop);
      {
        //assert(repop->waitfor_ack.count(fromosd));   // no, we may come thru here twice.
        repop->waitfor_ack.erase(fromosd);
      }
      put_rep_gather(repop);

      // prepare dest socket
      //messenger->prepare_send_message(op->get_client());
    }

    // chain?  forward?
    if (g_conf.osd_rep == OSD_REP_CHAIN && !is_acker()) {
      // chain rep, not at the tail yet.
      int myrank = osd->osdmap->calc_pg_rank(osd->get_nodeid(), acting);
      int next = myrank+1;
      if (next == (int)acting.size())
	next = 1;
      issue_repop(op, acting[next]);	
    }
  }

  // do op?
  C_OSD_RepModifyCommit *oncommit = 0;

  osd->logger->inc("r_wr");
  osd->logger->inc("r_wrb", op->get_length());
  
  if (repop) {
    // acker.  we'll apply later.
    if (op->get_op() != OSD_OP_WRNOOP) {
      prepare_log_transaction(repop->t, op, nv, crev, op->get_rev(), op->get_pg_trim_to());
      prepare_op_transaction(repop->t, op, nv, crev, op->get_rev());
    }
  } else {
    // middle|replica.
    if (op->get_op() != OSD_OP_WRNOOP) {
      prepare_log_transaction(t, op, nv, crev, op->get_rev(), op->get_pg_trim_to());
      prepare_op_transaction(t, op, nv, crev, op->get_rev());
    }

    oncommit = new C_OSD_RepModifyCommit(this, op, ackerosd, info.last_complete);

    // apply log update. and possibly update itself.
    unsigned tr = osd->store->apply_transaction(t, oncommit);
    if (tr != 0 &&   // no errors
        tr != 2) {   // or error on collection_add
      derr(0) << "error applying transaction: r = " << tr << dendl;
      assert(tr == 0);
    }
  }
  
  // ack?
  if (repop) {
    // (logical) local ack.  this may induce the actual update.
    get_rep_gather(repop);
    {
      assert(repop->waitfor_ack.count(osd->get_nodeid()));
      repop->waitfor_ack.erase(osd->get_nodeid());
    }
    put_rep_gather(repop);
  } 
  else {
    // send ack to acker?
    if (g_conf.osd_rep != OSD_REP_CHAIN) {
      MOSDOpReply *ack = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), false);
      osd->messenger->send_message(ack, osd->osdmap->get_inst(ackerosd));
    }

    // ack myself.
    assert(oncommit);
    oncommit->ack(); 
  }

}


void ReplicatedPG::op_rep_modify_commit(MOSDOp *op, int ackerosd, eversion_t last_complete)
{
  // send commit.
  dout(10) << "rep_modify_commit on op " << *op
           << ", sending commit to osd" << ackerosd
           << dendl;
  if (osd->osdmap->is_up(ackerosd)) {
    MOSDOpReply *commit = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), true);
    commit->set_pg_complete_thru(last_complete);
    osd->messenger->send_message(commit, osd->osdmap->get_inst(ackerosd));
    delete op;
  }
}










// ===========================================================

/** pull - request object from a peer
 */
void ReplicatedPG::pull(object_t oid)
{
  assert(missing.loc.count(oid));
  eversion_t v = missing.missing[oid];
  int fromosd = missing.loc[oid];
  
  dout(7) << "pull " << oid
          << " v " << v 
          << " from osd" << fromosd
          << dendl;

  // send op
  tid_t tid = osd->get_tid();
  MOSDOp *op = new MOSDOp(osd->messenger->get_myinst(), 0, tid,
                          oid, info.pgid,
                          osd->osdmap->get_epoch(),
                          OSD_OP_PULL);
  op->set_version(v);
  osd->messenger->send_message(op, osd->osdmap->get_inst(fromosd));
  
  // take note
  assert(objects_pulling.count(oid) == 0);
  num_pulling++;
  objects_pulling[oid] = v;
}


/** push - send object to a peer
 */
void ReplicatedPG::push(object_t oid, int peer)
{
  // read data+attrs
  bufferlist bl;
  eversion_t v;
  int vlen = sizeof(v);
  map<string,bufferptr> attrset;
  
  ObjectStore::Transaction t;
  t.read(oid, 0, 0, &bl);
  t.getattr(oid, "version", &v, &vlen);
  t.getattrs(oid, attrset);
  unsigned tr = osd->store->apply_transaction(t);
  
  assert(tr == 0);  // !!!

  // ok
  dout(7) << "push " << oid << " v " << v 
          << " size " << bl.length()
          << " to osd" << peer
          << dendl;

  osd->logger->inc("r_push");
  osd->logger->inc("r_pushb", bl.length());
  
  // send
  MOSDOp *op = new MOSDOp(osd->messenger->get_myinst(), 0, osd->get_tid(),
                          oid, info.pgid, osd->osdmap->get_epoch(), 
                          OSD_OP_PUSH); 
  op->set_offset(0);
  op->set_length(bl.length());
  op->set_data(bl);   // note: claims bl, set length above here!
  op->set_version(v);
  op->set_attrset(attrset);
  
  osd->messenger->send_message(op, osd->osdmap->get_inst(peer));
  
  if (is_primary()) {
    peer_missing[peer].got(oid);
    pushing[oid].insert(peer);
  }
}



/** op_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void ReplicatedPG::op_pull(MOSDOp *op)
{
  const object_t oid = op->get_oid();
  const eversion_t v = op->get_version();
  int from = op->get_source().num();

  dout(7) << "op_pull " << oid << " v " << op->get_version()
          << " from " << op->get_source()
          << dendl;

  // is a replica asking?  are they missing it?
  if (is_primary()) {
    // primary
    assert(peer_missing.count(from));  // we had better know this, from the peering process.

    if (!peer_missing[from].is_missing(oid)) {
      dout(7) << "op_pull replica isn't actually missing it, we must have already pushed to them" << dendl;
      delete op;
      return;
    }

    // do we have it yet?
    if (is_missing_object(oid)) {
      wait_for_missing_object(oid, op);
      return;
    }
  } else {
    // non-primary
    if (missing.is_missing(oid)) {
      dout(7) << "op_pull not primary, and missing " << oid << ", ignoring" << dendl;
      delete op;
      return;
    }
  }
    
  // push it back!
  push(oid, op->get_source().num());
}


/** op_push
 * NOTE: called from opqueue.
 */
void ReplicatedPG::op_push(MOSDOp *op)
{
  object_t oid = op->get_oid();
  eversion_t v = op->get_version();

  if (!is_missing_object(oid)) {
    dout(7) << "op_push not missing " << oid << dendl;
    return;
  }
  
  dout(7) << "op_push " 
          << oid 
          << " v " << v 
          << " size " << op->get_length() << " " << op->get_data().length()
          << dendl;

  assert(op->get_data().length() == op->get_length());
  
  // write object and add it to the PG
  ObjectStore::Transaction t;
  t.remove(oid);  // in case old version exists
  t.write(oid, 0, op->get_length(), op->get_data());
  t.setattrs(oid, op->get_attrset());
  t.collection_add(info.pgid, oid);

  // close out pull op?
  num_pulling--;
  if (objects_pulling.count(oid))
    objects_pulling.erase(oid);
  missing.got(oid, v);


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
      if (peer_missing[peer].is_missing(oid)) 
	push(oid, peer);  // ok, push it, and they (will) have it now.
    }
  }

  // kick waiters
  if (waiting_for_missing_object.count(oid)) {
    osd->take_waiters(waiting_for_missing_object[oid]);
    waiting_for_missing_object.erase(oid);
  }

  if (is_primary()) {
    // continue recovery
    do_recovery();
  } else {
    // ack if i'm a replica and being pushed to.
    MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), true); 
    osd->messenger->send_message(reply, op->get_source_inst());
  }

  delete op;
}






void ReplicatedPG::note_failed_osd(int o)
{
  dout(10) << "note_failed_osd " << o << dendl;
  // do async; repop_ack() may modify pg->repop_gather
  list<RepGather*> ls;  
  for (map<tid_t,RepGather*>::iterator p = rep_gather.begin();
       p != rep_gather.end();
       p++) {
    //dout(-1) << "checking repop tid " << p->first << dendl;
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

  if (g_conf.osd_rep == OSD_REP_PRIMARY) {
    // we're fine.
    // note that note_failed_osd() above shoudl ahve implicitly acked/committed
    // from the failed guy.
  } else {
    // for splay or chain replication, any change is significant. 
    // apply repops
    for (map<tid_t,RepGather*>::iterator p = rep_gather.begin();
	 p != rep_gather.end();
	 p++) {
      if (!p->second->applied)
	apply_repop(p->second);
      delete p->second->op;
      delete p->second;
    }
    rep_gather.clear();
    
    // and repop waiters
    for (map<tid_t, list<Message*> >::iterator p = waiting_for_repop.begin();
	 p != waiting_for_repop.end();
	 p++)
      for (list<Message*>::iterator pm = p->second.begin();
	   pm != p->second.end();
	   pm++)
	delete *pm;
    waiting_for_repop.clear();
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









/** clean_up_local
 * remove any objects that we're storing but shouldn't.
 * as determined by log.
 */
void ReplicatedPG::clean_up_local(ObjectStore::Transaction& t)
{
  dout(10) << "clean_up_local" << dendl;

  assert(info.last_update >= log.bottom);  // otherwise we need some help!

  if (log.backlog) {
    // be thorough.
    list<object_t> ls;
    osd->store->collection_list(info.pgid, ls);
    set<object_t> s;
    
    for (list<object_t>::iterator i = ls.begin();
         i != ls.end();
         i++) 
      s.insert(*i);

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

void ReplicatedPG::op_push_reply(MOSDOpReply *reply)
{
  dout(10) << "op_push_reply from " << reply->get_source() << " " << *reply << dendl;
  
  int peer = reply->get_source().num();
  object_t oid = reply->get_oid();
  
  if (pushing.count(oid) &&
      pushing[oid].count(peer)) {
    pushing[oid].erase(peer);

    if (peer_missing.count(peer) == 0 ||
        peer_missing[peer].num_missing() == 0) 
      uptodate_set.insert(peer);

    if (pushing[oid].empty()) {
      dout(10) << "pushed " << oid << " to all replicas" << dendl;
      do_peer_recovery();
    } else {
      dout(10) << "pushed " << oid << ", still waiting for push ack from " 
	       << pushing[oid] << dendl;
    }
  } else {
    dout(10) << "huh, i wasn't pushing " << oid << dendl;
  }
  delete reply;
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

