// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "Objecter.h"
#include "osd/OSDMap.h"
#include "osd/PGLS.h"

#include "mon/MonClient.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"

#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"
#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

#include "messages/MOSDFailure.h"

#include <errno.h>

#include "config.h"

#define DOUT_SUBSYS objecter
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << messenger->get_myname() << ".objecter "


// messages ------------------------------

void Objecter::init()
{
  assert(client_lock.is_locked());
  timer.add_event_after(g_conf.objecter_tick_interval, new C_Tick(this));
  maybe_request_map();
}

void Objecter::shutdown() 
{
  assert(client_lock.is_locked());  // otherwise event cancellation is unsafe
  timer.cancel_all_events();
}


void Objecter::dispatch(Message *m)
{
  switch (m->get_type()) {
  case CEPH_MSG_OSD_OPREPLY:
    handle_osd_op_reply((MOSDOpReply*)m);
    break;
    
  case CEPH_MSG_OSD_MAP:
    handle_osd_map((MOSDMap*)m);
    break;

  case MSG_GETPOOLSTATSREPLY:
    handle_get_pool_stats_reply((MGetPoolStatsReply*)m);
    break;

  case CEPH_MSG_STATFS_REPLY:
    handle_fs_stats_reply((MStatfsReply*)m);
    break;

  case CEPH_MSG_POOLOP_REPLY:
    handle_pool_op_reply((MPoolOpReply*)m);
    break;

  default:
    dout(0) << "don't know message type " << m->get_type() << dendl;
    assert(0);
  }
}

void Objecter::handle_osd_map(MOSDMap *m)
{
  assert(osdmap); 

  if (ceph_fsid_compare(&m->fsid, &monc->get_fsid())) {
    dout(0) << "handle_osd_map fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    m->put();
    return;
  }

  if (m->get_last() <= osdmap->get_epoch()) {
    dout(3) << "handle_osd_map ignoring epochs [" 
            << m->get_first() << "," << m->get_last() 
            << "] <= " << osdmap->get_epoch() << dendl;
  } 
  else {
    dout(3) << "handle_osd_map got epochs [" 
            << m->get_first() << "," << m->get_last() 
            << "] > " << osdmap->get_epoch()
            << dendl;

    set<pg_t> changed_pgs;

    if (osdmap->get_epoch()) {
      // we want incrementals
      for (epoch_t e = osdmap->get_epoch() + 1;
	   e <= m->get_last();
	   e++) {

	bool was_pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
	bool was_pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR);
	bool was_full = osdmap->test_flag(CEPH_OSDMAP_FULL);
  
	if (m->incremental_maps.count(e)) {
	  dout(3) << "handle_osd_map decoding incremental epoch " << e << dendl;
	  OSDMap::Incremental inc(m->incremental_maps[e]);
	  osdmap->apply_incremental(inc);
	  
	  // notify messenger
	  for (map<int32_t,uint8_t>::iterator i = inc.new_down.begin();
	       i != inc.new_down.end();
	       i++) 
	    messenger->mark_down(osdmap->get_addr(i->first));
	  
	}
	else if (m->maps.count(e)) {
	  dout(3) << "handle_osd_map decoding full epoch " << e << dendl;
	  osdmap->decode(m->maps[e]);
	}
	else {
	  dout(3) << "handle_osd_map requesting missing epoch " << osdmap->get_epoch()+1 << dendl;
	  maybe_request_map();
	  break;
	}

	if (was_pauserd || was_pausewr || was_full)
	  maybe_request_map();
	
	// scan pgs for changes
	scan_pgs(changed_pgs);

	// kick paused
	if ((was_pauserd && !osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) ||
	    (was_pausewr && !osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) ||
	    (was_full && !osdmap->test_flag(CEPH_OSDMAP_FULL))) {
	  for (hash_map<tid_t,Op*>::iterator p = op_osd.begin();
	       p != op_osd.end();
	       p++) {
	    if (p->second->paused) {
	      p->second->paused = false;
	      op_submit(p->second);
	    }
	  }
	}
        
	assert(e == osdmap->get_epoch());
      }
      
    } else {
      // first map.  we want the full thing.
      if (m->maps.count(m->get_last())) {
	dout(3) << "handle_osd_map decoding full epoch " << m->get_last() << dendl;
	osdmap->decode(m->maps[m->get_last()]);

	scan_pgs(changed_pgs);
      } else {
	dout(3) << "handle_osd_map hmm, i want a full map, requesting" << dendl;
	monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
	monc->renew_subs();
      }
    }

    // kick requests who might be timing out on the wrong osds
    if (!changed_pgs.empty())
      kick_requests(changed_pgs);
  }

  //now check if the map is full -- we want to subscribe if it is!
  if (osdmap->test_flag(CEPH_OSDMAP_FULL) & CEPH_OSDMAP_FULL)
    maybe_request_map();
  
  //finish any Contexts that were waiting on a map update
  map<epoch_t,list< pair< Context*, int > > >::iterator p =
    waiting_for_map.begin();
  while (p != waiting_for_map.end() &&
	 p->first <= osdmap->get_epoch()) {
    //go through the list and call the onfinish methods
    for (list<pair<Context*, int> >::iterator i = p->second.begin();
	 i != p->second.end(); ++i) {
      i->first->finish(i->second);
      delete i->first;
    }
    waiting_for_map.erase(p++);
  }

  m->put();

  monc->sub_got("osdmap", osdmap->get_epoch());
}

void Objecter::wait_for_osd_map()
{
  if (osdmap->get_epoch()) return;
  Mutex lock("");
  Cond cond;
  bool done;
  lock.Lock();
  C_SafeCond *context = new C_SafeCond(&lock, &cond, &done, NULL);
  waiting_for_map[0].push_back(pair<Context*, int>(context, 0));
  while (!done)
    cond.Wait(lock);
  lock.Unlock();
}


void Objecter::maybe_request_map()
{
  int flag = 0;
  if (osdmap->test_flag(CEPH_OSDMAP_FULL)) {
    dout(10) << "maybe_request_map subscribing (continuous) to next osd map (FULL flag is set)" << dendl;
  } else {
    dout(10) << "maybe_request_map subscribing (onetime) to next osd map" << dendl;
    flag = CEPH_SUBSCRIBE_ONETIME;
  }
  if (monc->sub_want("osdmap", osdmap->get_epoch() ? osdmap->get_epoch()+1 : 0, flag))
    monc->renew_subs();
}


Objecter::PG &Objecter::get_pg(pg_t pgid)
{
  if (!pg_map.count(pgid)) {
    osdmap->pg_to_acting_osds(pgid, pg_map[pgid].acting);
    dout(10) << "get_pg " << pgid << " is new, " << pg_map[pgid].acting << dendl;
  } else {
    dout(10) << "get_pg " << pgid << " is old, " << pg_map[pgid].acting << dendl;
  }
  return pg_map[pgid];
}


void Objecter::scan_pgs_for(set<pg_t>& pgs, int osd)
{
  dout(10) << "scan_pgs_for osd" << osd << dendl;

  for (hash_map<pg_t,PG>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    pg_t pgid = i->first;
    PG& pg = i->second;
    if (pg.acting.size() && pg.acting[0] == osd)
      pgs.insert(pgid);
  }
}

void Objecter::scan_pgs(set<pg_t>& changed_pgs)
{
  dout(10) << "scan_pgs" << dendl;

  for (hash_map<pg_t,PG>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    pg_t pgid = i->first;
    PG& pg = i->second;
    
    // calc new.
    vector<int> other;
    osdmap->pg_to_acting_osds(pgid, other);

    if (other == pg.acting) 
      continue; // no change.

    dout(10) << "scan_pgs " << pgid << " " << pg.acting << " -> " << other << dendl;
    
    other.swap(pg.acting);

    if (other.size() && pg.acting.size() &&
	other[0] == pg.acting[0])
      continue;  // same primary.

    // changed significantly.
    dout(10) << "scan_pgs pg " << pgid 
             << " (" << pg.active_tids << ")"
             << " " << other << " -> " << pg.acting
             << dendl;
    changed_pgs.insert(pgid);
  }
}

void Objecter::kick_requests(set<pg_t>& changed_pgs) 
{
  dout(10) << "kick_requests in pgs " << changed_pgs << dendl;

  for (set<pg_t>::iterator i = changed_pgs.begin();
       i != changed_pgs.end();
       i++) {
    pg_t pgid = *i;
    PG& pg = pg_map[pgid];

    // resubmit ops!
    set<tid_t> tids;
    tids.swap( pg.active_tids );
    close_pg( pgid );  // will pbly reopen, unless it's just commits we're missing
    
    dout(10) << "kick_requests pg " << pgid << " tids " << tids << dendl;
    for (set<tid_t>::iterator p = tids.begin();
         p != tids.end();
         p++) {
      tid_t tid = *p;
      
      hash_map<tid_t, Op*>::iterator p = op_osd.find(tid);
      if (p != op_osd.end()) {
	Op *op = p->second;
	put_op_budget(op);
	op_osd.erase(p);

	if (op->onack)
	  num_unacked--;
	if (op->oncommit)
	  num_uncommitted--;
	
        // WRITE
	if (op->onack) {
          dout(3) << "kick_requests missing ack, resub " << tid << dendl;
          op_submit(op);
        } else {
	  assert(op->oncommit);
	  dout(3) << "kick_requests missing commit, resub " << tid << dendl;
	  op_submit(op);
        } 
      }
      else 
        assert(0);
    }         
  }
}


void Objecter::tick()
{
  dout(10) << "tick" << dendl;

  set<int> ping;

  // look for laggy pgs
  utime_t cutoff = g_clock.now();
  cutoff -= g_conf.objecter_timeout;  // timeout
  for (hash_map<pg_t,PG>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    if (!i->second.active_tids.empty() &&
	i->second.last < cutoff) {
      dout(1) << " pg " << i->first << " on " << i->second.acting
	      << " is laggy: " << i->second.active_tids << dendl;
      maybe_request_map();
      //break;

      // send a ping to this osd, to ensure we detect any session resets
      // (osd reply message policy is lossy)
      if (i->second.acting.size())
	ping.insert(i->second.acting[0]);
    }
  }

  for (set<int>::iterator p = ping.begin(); p != ping.end(); p++)
    messenger->send_message(new MPing, osdmap->get_inst(*p));

  // reschedule
  timer.add_event_after(g_conf.objecter_tick_interval, new C_Tick(this));
}

void Objecter::resend_mon_ops()
{
  utime_t cutoff = g_clock.now();
  cutoff -= g_conf.objecter_mon_retry_interval;


  for (map<tid_t,PoolStatOp*>::iterator p = op_poolstat.begin(); p!=op_poolstat.end(); ++p) {
    if (p->second->last_submit < cutoff)
      poolstat_submit(p->second);
  }

  for (map<tid_t,StatfsOp*>::iterator p = op_statfs.begin(); p!=op_statfs.end(); ++p) {
    if (p->second->last_submit < cutoff)
      fs_stats_submit(p->second);
  }

  for (map<tid_t,PoolOp*>::iterator p = op_pool.begin(); p!=op_pool.end(); ++p) {
    if (p->second->last_submit < cutoff)
      pool_op_submit(p->second);
  }
}



// read | write ---------------------------

tid_t Objecter::op_submit(Op *op)
{

  if (op->oid.name.length())
    op->pgid = osdmap->object_locator_to_pg(op->oid, op->oloc);

  // find
  PG &pg = get_pg(op->pgid);
    
  // pick tid
  if (!op->tid)
    op->tid = ++last_tid;
  assert(client_inc >= 0);

  // add to gather set(s)
  int flags = op->flags;
  if (op->onack) {
    flags |= CEPH_OSD_FLAG_ACK;
    ++num_unacked;
  } else {
    dout(20) << " note: not requesting ack" << dendl;
  }
  if (op->oncommit) {
    flags |= CEPH_OSD_FLAG_ONDISK;
    ++num_uncommitted;
  } else {
    dout(20) << " note: not requesting commit" << dendl;
  }
  take_op_budget(op);
  op_osd[op->tid] = op;
  pg.active_tids.insert(op->tid);
  pg.last = g_clock.now();

  // send?
  dout(10) << "op_submit oid " << op->oid
           << " " << op->oloc 
	   << " " << op->ops << " tid " << op->tid
           << " osd" << pg.primary()
           << dendl;

  assert(op->flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE));

  if ((op->flags & CEPH_OSD_FLAG_WRITE) &&
      osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
    dout(10) << " paused modify " << op << " tid " << last_tid << dendl;
    op->paused = true;
    maybe_request_map();
  } else if ((op->flags & CEPH_OSD_FLAG_READ) &&
	     osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
    dout(10) << " paused read " << op << " tid " << last_tid << dendl;
    op->paused = true;
    maybe_request_map();
 } else if ((op->flags & CEPH_OSD_FLAG_WRITE) &&
	    osdmap->test_flag(CEPH_OSDMAP_FULL)) {
    dout(10) << " FULL, paused modify " << op << " tid " << last_tid << dendl;
    op->paused = true;
    maybe_request_map();
  } else if (pg.primary() >= 0) {
    int flags = op->flags;
    if (op->oncommit)
      flags |= CEPH_OSD_FLAG_ONDISK;
    if (op->onack)
      flags |= CEPH_OSD_FLAG_ACK;

    ceph_object_layout ol;
    ol.ol_pgid = op->pgid.v;
    ol.ol_stripe_unit = 0;

    MOSDOp *m = new MOSDOp(client_inc, op->tid,
			   op->oid, ol, osdmap->get_epoch(),
			   flags);

    m->set_snapid(op->snapid);
    m->set_snap_seq(op->snapc.seq);
    m->get_snaps() = op->snapc.snaps;

    m->ops = op->ops;
    m->set_mtime(op->mtime);
    m->set_retry_attempt(op->attempts++);
    
    if (op->version != eversion_t())
      m->set_version(op->version);  // we're replaying this op!

    if (op->priority)
      m->set_priority(op->priority);

    messenger->send_message(m, osdmap->get_inst(pg.primary()));
  } else 
    maybe_request_map();
  
  dout(5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;
  
  return op->tid;
}

int Objecter::calc_op_budget(Op *op)
{
  int op_budget = 0;
  for (vector<OSDOp>::iterator i = op->ops.begin();
       i != op->ops.end();
       ++i) {
    if (i->op.op & CEPH_OSD_OP_MODE_WR) {
      op_budget += i->data.length();
    } else if (i->op.op & CEPH_OSD_OP_MODE_RD) {
      if (i->op.op & CEPH_OSD_OP_TYPE_DATA)
        op_budget += i->op.extent.length;
      else if (i->op.op & CEPH_OSD_OP_TYPE_ATTR)
        op_budget += i->op.xattr.name_len + i->op.xattr.value_len;
    }
  }
  return op_budget;
}

void Objecter::throttle_op(Op *op, int op_budget)
{
  if (!op_budget)
    op_budget = calc_op_budget(op);
  if (!op_throttler.get_or_fail(op_budget)) { //couldn't take right now
    client_lock.Unlock();
    op_throttler.get(op_budget);
    client_lock.Lock();
  }
}

/* This function DOES put the passed message before returning */
void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  dout(10) << "in handle_osd_op_reply" << dendl;
  // get pio
  tid_t tid = m->get_tid();

  if (op_osd.count(tid) == 0) {
    dout(7) << "handle_osd_op_reply " << tid
	    << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
	    << " ... stray" << dendl;
    m->put();
    return;
  }

  dout(7) << "handle_osd_op_reply " << tid
	  << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
	  << " v " << m->get_version() << " in " << m->get_pg()
	  << dendl;
  Op *op = op_osd[ tid ];

  Context *onack = 0;
  Context *oncommit = 0;

  PG &pg = get_pg( m->get_pg() );

  // ignore?
  if (pg.acker() != m->get_source().num()) {
    dout(7) << " ignoring ack|commit from non-acker" << dendl;
    m->put();
    return;
  }
  
  int rc = m->get_result();

  if (rc == -EAGAIN) {
    dout(7) << " got -EAGAIN, resubmitting" << dendl;
    if (op->onack)
      num_unacked--;
    if (op->oncommit)
      num_uncommitted--;
    op_submit(op);
    m->put();
    return;
  }

  // got data?
  if (op->outbl) {
    m->claim_data(*op->outbl);
    op->outbl = 0;
  }

  // ack|commit -> ack
  if (op->onack) {
    dout(15) << "handle_osd_op_reply ack" << dendl;
    op->version = m->get_version();
    onack = op->onack;
    op->onack = 0;  // only do callback once
    num_unacked--;
  }
  if (op->oncommit && m->is_ondisk()) {
    dout(15) << "handle_osd_op_reply safe" << dendl;
    oncommit = op->oncommit;
    op->oncommit = 0;
    num_uncommitted--;
  }

  // done with this tid?
  if (!op->onack && !op->oncommit) {
    assert(pg.active_tids.count(tid));
    pg.active_tids.erase(tid);
    dout(15) << "handle_osd_op_reply completed tid " << tid << ", pg " << m->get_pg()
	     << " still has " << pg.active_tids << dendl;
    if (pg.active_tids.empty()) 
      close_pg( m->get_pg() );
    put_op_budget(op);
    op_osd.erase( tid );
    delete op;
  }
  
  dout(5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;

  // do callbacks
  if (onack) {
    dout(20) << "Calling onack->finish with rc " << rc << dendl;
    onack->finish(rc);
    dout(20) << "Finished onack-finish" << dendl;
    delete onack;
  }
  if (oncommit) {
    oncommit->finish(rc);
    delete oncommit;
  }

  m->put();
}


void Objecter::list_objects(ListContext *list_context, Context *onfinish) {

  dout(10) << "list_objects" << dendl;
  dout(20) << "pool_id " << list_context->pool_id
	   << "\npool_snap_seq " << list_context->pool_snap_seq
	   << "\nmax_entries " << list_context->max_entries
	   << "\nlist_context " << list_context
	   << "\nonfinish " << onfinish
	   << "\nlist_context->current_pg" << list_context->current_pg
	   << "\nlist_context->cookie" << list_context->cookie << dendl;

  if (list_context->at_end) {
    onfinish->finish(0);
    delete onfinish;
    return;
  }

  const pg_pool_t *pool = osdmap->get_pg_pool(list_context->pool_id);
  int pg_num = pool->get_pg_num();

  if (list_context->starting_pg_num == 0) {     // there can't be zero pgs!
    list_context->starting_pg_num = pg_num;
    dout(20) << pg_num << " placement groups" << dendl;
  }
  if (list_context->starting_pg_num != pg_num) {
    // start reading from the beginning; the pgs have changed
    dout(10) << "The placement groups have changed, restarting with " << pg_num << dendl;
    list_context->current_pg = 0;
    list_context->cookie = 0;
    list_context->starting_pg_num = pg_num;
  }
  if (list_context->current_pg == pg_num){ //this context got all the way through
    onfinish->finish(0);
    delete onfinish;
    return;
  }

  ObjectOperation op;
  op.pg_ls(list_context->max_entries, list_context->cookie);

  bufferlist *bl = new bufferlist();
  C_List *onack = new C_List(list_context, onfinish, bl, this);

  object_t oid;
  object_locator_t oloc(list_context->pool_id);

  // 
  Op *o = new Op(oid, oloc, op.ops, CEPH_OSD_FLAG_READ, onack, NULL);
  o->priority = op.priority;
  o->snapid = list_context->pool_snap_seq;
  o->outbl = bl;

  o->pgid = pg_t(list_context->current_pg, list_context->pool_id, -1);

  op_submit(o);
}

void Objecter::_list_reply(ListContext *list_context, bufferlist *bl, Context *final_finish)
{
  dout(10) << "_list_reply" << dendl;

  bufferlist::iterator iter = bl->begin();
  PGLSResponse response;
  ::decode(response, iter);
  list_context->cookie = (uint64_t)response.handle;

  int response_size = response.entries.size();
  dout(20) << "response.entries.size " << response_size
	   << ", response.entries " << response.entries << dendl;
  if (response_size) {
    dout(20) << "got a response with objects, proceeding" << dendl;
    list_context->list.merge(response.entries);
    list_context->max_entries -= response_size;
    dout(20) << "cleaning up and exiting" << dendl;
    if (!list_context->max_entries) {
      final_finish->finish(0);
      delete bl;
      delete final_finish;
      return;
    }
  }
  //if we make this this far, there are no objects left in the current pg, but we want more!
  ++list_context->current_pg;
  dout(20) << "emptied current pg, moving on to next one:" << list_context->current_pg << dendl;
  if(list_context->current_pg < list_context->starting_pg_num){ //we have more pgs to go through
    list_context->cookie = 0;
    delete bl;
    list_objects(list_context, final_finish);
    return;
  }
  
  //if we make it this far, there are no more pgs
  dout(20) << "out of pgs, returning to" << final_finish << dendl;
  list_context->at_end = true;
  delete bl;
  final_finish->finish(0);
  delete final_finish;
  return;
}


//snapshots

int Objecter::create_pool_snap(int pool, string& snapName, Context *onfinish) {
  dout(10) << "create_pool_snap; pool: " << pool << "; snap: " << snapName << dendl;
  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = snapName;
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_CREATE_SNAP;
  op_pool[op->tid] = op;

  pool_op_submit(op);

  return 0;
}

struct C_SelfmanagedSnap : public Context {
  bufferlist bl;
  snapid_t *psnapid;
  Context *fin;
  C_SelfmanagedSnap(snapid_t *ps, Context *f) : psnapid(ps), fin(f) {}
  void finish(int r) {
    if (r == 0) {
      bufferlist::iterator p = bl.begin();
      ::decode(*psnapid, p);
    }
    fin->finish(r);
    delete fin;
  }
};

int Objecter::allocate_selfmanaged_snap(int pool, snapid_t *psnapid,
					Context *onfinish)
{
  dout(10) << "allocate_selfmanaged_snap; pool: " << pool << dendl;
  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  C_SelfmanagedSnap *fin = new C_SelfmanagedSnap(psnapid, onfinish);
  op->onfinish = fin;
  op->blp = &fin->bl;
  op->pool_op = POOL_OP_CREATE_UNMANAGED_SNAP;
  op_pool[op->tid] = op;

  pool_op_submit(op);
  return 0;
}

int Objecter::delete_pool_snap(int pool, string& snapName, Context *onfinish)
{
  dout(10) << "delete_pool_snap; pool: " << pool << "; snap: " << snapName << dendl;
  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = snapName;
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_DELETE_SNAP;
  op_pool[op->tid] = op;
  
  pool_op_submit(op);
  
  return 0;
}

int Objecter::delete_selfmanaged_snap(int pool, snapid_t snap,
				      Context *onfinish) {
  dout(10) << "delete_selfmanaged_snap; pool: " << pool << "; snap: " 
	   << snap << dendl;
  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_DELETE_UNMANAGED_SNAP;
  op->snapid = snap;
  op_pool[op->tid] = op;

  pool_op_submit(op);

  return 0;
}

int Objecter::create_pool(string& name, Context *onfinish, uint64_t auid,
			  int crush_rule)
{
  dout(10) << "create_pool name=" << name << dendl;
  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = 0;
  op->name = name;
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_CREATE;
  op_pool[op->tid] = op;
  op->auid = auid;
  op->crush_rule = crush_rule;

  pool_op_submit(op);

  return 0;
}

int Objecter::delete_pool(int pool, Context *onfinish)
{
  dout(10) << "delete_pool " << pool << dendl;

  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = "delete";
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_DELETE;
  op_pool[op->tid] = op;

  pool_op_submit(op);

  return 0;
}

/**
 * change the auid owner of a pool by contacting the monitor.
 * This requires the current connection to have write permissions
 * on both the pool's current auid and the new (parameter) auid.
 * Uses the standard Context callback when done.
 */
int Objecter::change_pool_auid(int pool, Context *onfinish, uint64_t auid)
{
  dout(10) << "change_pool_auid " << pool << " to " << auid << dendl;
  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = "change_pool_auid";
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_AUID_CHANGE;
  op->auid = auid;
  op_pool[op->tid] = op;

  pool_op_submit(op);

  return 0;
}

void Objecter::pool_op_submit(PoolOp *op) {
  dout(10) << "pool_op_submit " << op->tid << dendl;
  MPoolOp *m = new MPoolOp(monc->get_fsid(), op->tid, op->pool,
			   op->name, op->pool_op,
			   op->auid, last_seen_osdmap_version);
  if (op->snapid) m->snapid = op->snapid;
  if (op->crush_rule) m->crush_rule = op->crush_rule;
  monc->send_mon_message(m);
  op->last_submit = g_clock.now();
}

/**
 * Handle a reply to a PoolOp message. Check that we sent the message
 * and give the caller responsibility for the returned bufferlist.
 * Then either call the finisher or stash the PoolOp, depending on if we
 * have a new enough map.
 * Lastly, clean up the message and PoolOp.
 */
void Objecter::handle_pool_op_reply(MPoolOpReply *m)
{
  dout(10) << "handle_pool_op_reply " << *m << dendl;
  tid_t tid = m->get_tid();
  if (op_pool.count(tid)) {
    PoolOp *op = op_pool[tid];
    dout(10) << "have request " << tid << " at " << op << " Op: " << ceph_pool_op_name(op->pool_op) << dendl;
    if (op->blp) {
      op->blp->claim(*m->response_data);
      m->response_data = NULL;
    }
    if (m->version > last_seen_osdmap_version)
      last_seen_osdmap_version = m->version;
    if (m->replyCode == 0 && osdmap->get_epoch() < m->epoch) {
      dout(20) << "waiting for client to reach epoch " << m->epoch << " before calling back" << dendl;
      wait_for_new_map(op->onfinish, m->epoch, m->replyCode);
    }
    else {
      op->onfinish->finish(m->replyCode);
      delete op->onfinish;
    }
    op->onfinish = NULL;
    delete op;
    op_pool.erase(tid);
  } else {
    dout(10) << "unknown request " << tid << dendl;
  }
  dout(10) << "done" << dendl;
  m->put();
}


// pool stats

void Objecter::get_pool_stats(list<string>& pools, map<string,pool_stat_t> *result,
			      Context *onfinish)
{
  dout(10) << "get_pool_stats " << pools << dendl;

  PoolStatOp *op = new PoolStatOp;
  op->tid = ++last_tid;
  op->pools = pools;
  op->pool_stats = result;
  op->onfinish = onfinish;
  op_poolstat[op->tid] = op;

  poolstat_submit(op);
}

void Objecter::poolstat_submit(PoolStatOp *op)
{
  dout(10) << "poolstat_submit " << op->tid << dendl;
  monc->send_mon_message(new MGetPoolStats(monc->get_fsid(), op->tid, op->pools, last_seen_pgmap_version));
  op->last_submit = g_clock.now();
}

void Objecter::handle_get_pool_stats_reply(MGetPoolStatsReply *m)
{
  dout(10) << "handle_get_pool_stats_reply " << *m << dendl;
  tid_t tid = m->get_tid();

  if (op_poolstat.count(tid)) {
    PoolStatOp *op = op_poolstat[tid];
    dout(10) << "have request " << tid << " at " << op << dendl;
    *op->pool_stats = m->pool_stats;
    if (m->version > last_seen_pgmap_version)
      last_seen_pgmap_version = m->version;
    op->onfinish->finish(0);
    delete op->onfinish;
    op_poolstat.erase(tid);
    delete op;
  } else {
    dout(10) << "unknown request " << tid << dendl;
  } 
  dout(10) << "done" << dendl;
  m->put();
}


void Objecter::get_fs_stats(ceph_statfs& result, Context *onfinish) {
  dout(10) << "get_fs_stats" << dendl;

  StatfsOp *op = new StatfsOp;
  op->tid = ++last_tid;
  op->stats = &result;
  op->onfinish = onfinish;
  op_statfs[op->tid] = op;

  fs_stats_submit(op);
}

void Objecter::fs_stats_submit(StatfsOp *op)
{
  dout(10) << "fs_stats_submit" << op->tid << dendl;
  monc->send_mon_message(new MStatfs(monc->get_fsid(), op->tid, last_seen_pgmap_version));
  op->last_submit = g_clock.now();
}

void Objecter::handle_fs_stats_reply(MStatfsReply *m) {
  dout(10) << "handle_fs_stats_reply " << *m << dendl;
  tid_t tid = m->get_tid();

  if (op_statfs.count(tid)) {
    StatfsOp *op = op_statfs[tid];
    dout(10) << "have request " << tid << " at " << op << dendl;
    *(op->stats) = m->h.st;
    if (m->h.version > last_seen_pgmap_version)
      last_seen_pgmap_version = m->h.version;
    op->onfinish->finish(0);
    delete op->onfinish;
    op_statfs.erase(tid);
    delete op;
  } else {
    dout(10) << "unknown request " << tid << dendl;
  }
  dout(10) << "done" << dendl;
  m->put();
}


// scatter/gather

void Objecter::_sg_read_finish(vector<ObjectExtent>& extents, vector<bufferlist>& resultbl, 
			       bufferlist *bl, Context *onfinish)
{
  // all done
  uint64_t bytes_read = 0;
  
  dout(15) << "_sg_read_finish" << dendl;

  if (extents.size() > 1) {
    /** FIXME This doesn't handle holes efficiently.
     * It allocates zero buffers to fill whole buffer, and
     * then discards trailing ones at the end.
     *
     * Actually, this whole thing is pretty messy with temporary bufferlist*'s all over
     * the heap. 
     */
    
    // map extents back into buffer
    map<uint64_t, bufferlist*> by_off;  // buffer offset -> bufferlist
    
    // for each object extent...
    vector<bufferlist>::iterator bit = resultbl.begin();
    for (vector<ObjectExtent>::iterator eit = extents.begin();
	 eit != extents.end();
	 eit++, bit++) {
      bufferlist& ox_buf = *bit;
      unsigned ox_len = ox_buf.length();
      unsigned ox_off = 0;
      assert(ox_len <= eit->length);           
      
      // for each buffer extent we're mapping into...
      for (map<__u32,__u32>::iterator bit = eit->buffer_extents.begin();
	   bit != eit->buffer_extents.end();
	   bit++) {
	dout(21) << " object " << eit->oid
		 << " extent " << eit->offset << "~" << eit->length
		 << " : ox offset " << ox_off
		 << " -> buffer extent " << bit->first << "~" << bit->second << dendl;
	by_off[bit->first] = new bufferlist;
	
	if (ox_off + bit->second <= ox_len) {
	  // we got the whole bx
	  by_off[bit->first]->substr_of(ox_buf, ox_off, bit->second);
	  if (bytes_read < bit->first + bit->second) 
	    bytes_read = bit->first + bit->second;
	} else if (ox_off + bit->second > ox_len && ox_off < ox_len) {
	  // we got part of this bx
	  by_off[bit->first]->substr_of(ox_buf, ox_off, (ox_len-ox_off));
	  if (bytes_read < bit->first + ox_len-ox_off) 
	    bytes_read = bit->first + ox_len-ox_off;
	  
	  // zero end of bx
	  dout(21) << "  adding some zeros to the end " << ox_off + bit->second-ox_len << dendl;
	  bufferptr z(ox_off + bit->second - ox_len);
	  z.zero();
	  by_off[bit->first]->append( z );
	} else {
	  // we got none of this bx.  zero whole thing.
	  assert(ox_off >= ox_len);
	  dout(21) << "  adding all zeros for this bit " << bit->second << dendl;
	  bufferptr z(bit->second);
	  z.zero();
	  by_off[bit->first]->append( z );
	}
	ox_off += bit->second;
      }
      assert(ox_off == eit->length);
    }
    
    // sort and string bits together
    for (map<uint64_t, bufferlist*>::iterator it = by_off.begin();
	 it != by_off.end();
	 it++) {
      assert(it->second->length());
      if (it->first < (uint64_t)bytes_read) {
	dout(21) << "  concat buffer frag off " << it->first << " len " << it->second->length() << dendl;
	bl->claim_append(*(it->second));
      } else {
	dout(21) << "  NO concat zero buffer frag off " << it->first << " len " << it->second->length() << dendl;          
      }
      delete it->second;
    }
    
    // trim trailing zeros?
    if (bl->length() > bytes_read) {
      dout(10) << " trimming off trailing zeros . bytes_read=" << bytes_read 
	       << " len=" << bl->length() << dendl;
      bl->splice(bytes_read, bl->length() - bytes_read);
      assert(bytes_read == bl->length());
    }
    
  } else {
    dout(15) << "  only one frag" << dendl;
  
    // only one fragment, easy
    bl->claim(resultbl[0]);
    bytes_read = bl->length();
  }
  
  // finish, clean up
  dout(7) << " " << bytes_read << " bytes " 
	  << bl->length()
	  << dendl;
    
  // done
  if (onfinish) {
    onfinish->finish(bytes_read);// > 0 ? bytes_read:m->get_result());
    delete onfinish;
  }
}


void Objecter::ms_handle_connect(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
    resend_mon_ops();
}

void Objecter::ms_handle_reset(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
    //
    int osd = osdmap->identify_osd(con->get_peer_addr());
    if (osd >= 0) {
      dout(1) << "ms_handle_reset on osd" << osd << dendl;
      set<pg_t> changed_pgs;
      scan_pgs_for(changed_pgs, osd);
      kick_requests(changed_pgs);
      maybe_request_map();
    } else {
      dout(10) << "ms_handle_reset on unknown osd addr " << con->get_peer_addr() << dendl;
    }
  }
}

void Objecter::ms_handle_remote_reset(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD)
    maybe_request_map();
}


void Objecter::dump_active()
{
  dout(10) << "dump_active" << dendl;
  for (hash_map<tid_t,Op*>::iterator p = op_osd.begin(); p != op_osd.end(); p++)
    dout(10) << " " << p->first << "\t" << p->second->oid << "\t" << p->second->ops << dendl;
}
