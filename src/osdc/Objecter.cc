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

#include "common/config.h"

#define DOUT_SUBSYS objecter
#undef dout_prefix
#define dout_prefix *_dout << messenger->get_myname() << ".objecter "


// messages ------------------------------

void Objecter::init()
{
  assert(client_lock.is_locked());
  timer.add_event_after(g_conf.objecter_tick_interval, new C_Tick(this));
  maybe_request_map();
}

void Objecter::shutdown() 
{
}

void Objecter::send_linger(LingerOp *info)
{
  if (!info->registering) {
    dout(15) << "send_linger " << info->linger_id << dendl;
    vector<OSDOp> ops = info->ops; // need to pass a copy to ops
    Context *onack = (!info->registered && info->on_reg_ack) ? new C_Linger_Ack(this, info) : NULL;
    Context *oncommit = new C_Linger_Commit(this, info);
    Op *o = new Op(info->oid, info->oloc, ops, info->flags | CEPH_OSD_FLAG_READ,
		   onack, oncommit,
		   info->pobjver);
    o->snapid = info->snap;

    if (info->session)
      recalc_op_target(o);
    op_submit(o, info->session);
    info->registering = true;
  } else {
    dout(15) << "send_linger " << info->linger_id << " already (re)registering" << dendl;
  }
}

void Objecter::_linger_ack(LingerOp *info, int r) 
{
  dout(10) << "_linger_ack " << info->linger_id << dendl;
  if (info->on_reg_ack) {
    info->on_reg_ack->finish(r);
    delete info->on_reg_ack;
    info->on_reg_ack = NULL;
  }
}

void Objecter::_linger_commit(LingerOp *info, int r) 
{
  dout(10) << "_linger_commit " << info->linger_id << dendl;
  if (info->on_reg_commit) {
    info->on_reg_commit->finish(r);
    delete info->on_reg_commit;
    info->on_reg_commit = NULL;
  }

  // only tell the user the first time we do this
  info->registered = true;
  info->registering = false;
  info->pobjver = NULL;
}

void Objecter::unregister_linger(uint64_t linger_id)
{
  map<uint64_t, LingerOp*>::iterator iter = linger_ops.find(linger_id);
  if (iter != linger_ops.end()) {
    LingerOp *info = iter->second;
    info->session_item.remove_myself();
    linger_ops.erase(iter);
    delete info;
  }
}

tid_t Objecter::linger(const object_t& oid, const object_locator_t& oloc, 
		       ObjectOperation& op,
		       snapid_t snap, bufferlist& inbl, bufferlist *poutbl, int flags,
		       Context *onack, Context *onfinish,
		       eversion_t *objver)
{
  LingerOp *info = new LingerOp;
  info->oid = oid;
  info->oloc = oloc;
  info->snap = snap;
  info->flags = flags;
  info->ops = op.ops;
  info->inbl = inbl;
  info->poutbl = poutbl;
  info->pobjver = objver;
  info->on_reg_ack = onack;
  info->on_reg_commit = onfinish;

  info->linger_id = ++max_linger_id;
  linger_ops[info->linger_id] = info;

  send_linger(info);

  return info->linger_id;
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

  bool was_pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool was_pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR);
  bool was_full = osdmap->test_flag(CEPH_OSDMAP_FULL);
  bool kick_paused =
    (was_pauserd && !osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) ||
    (was_pausewr && !osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) ||
    (was_full && !osdmap->test_flag(CEPH_OSDMAP_FULL));

  set<LingerOp*> need_resend_linger;
  set<Op*> need_resend;

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

    if (osdmap->get_epoch()) {
      // we want incrementals
      for (epoch_t e = osdmap->get_epoch() + 1;
	   e <= m->get_last();
	   e++) {
 
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
	
	// check for changed linger mappings (_before_ regular ops)
	for (map<tid_t,LingerOp*>::iterator p = linger_ops.begin();
	     p != linger_ops.end();
	     p++) {
	  LingerOp *op = p->second;
	  if (recalc_linger_op_target(op))
	    need_resend_linger.insert(op);
	}

	// check for changed request mappings
	for (hash_map<tid_t,Op*>::iterator p = ops.begin();
	     p != ops.end();
	     p++) {
	  Op *op = p->second;
	  if (recalc_op_target(op))
	    need_resend.insert(op);
	}

	// osd addr changes?
	for (map<int,OSDSession*>::iterator p = osd_sessions.begin();
	     p != osd_sessions.end(); ) {
	  OSDSession *s = p->second;
	  p++;
	  if (osdmap->is_up(s->osd)) {
	    if (s->con && s->con->get_peer_addr() != osdmap->get_inst(s->osd).addr)
	      close_session(s);
	  } else {
	    close_session(s);
	  }
	}

	assert(e == osdmap->get_epoch());
      }
      
    } else {
      // first map.  we want the full thing.
      if (m->maps.count(m->get_last())) {
	dout(3) << "handle_osd_map decoding full epoch " << m->get_last() << dendl;
	osdmap->decode(m->maps[m->get_last()]);
      } else {
	dout(3) << "handle_osd_map hmm, i want a full map, requesting" << dendl;
	monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
	monc->renew_subs();
      }
    }
  }

  // was paused, or was/is full?
  if (was_pauserd || was_pausewr || was_full ||
      (osdmap->test_flag(CEPH_OSDMAP_FULL) & CEPH_OSDMAP_FULL))
    maybe_request_map();
  
  // unpause paused ops?
  if (kick_paused)
    for (hash_map<tid_t,Op*>::iterator p = ops.begin();
	 p != ops.end();
	 p++) {
      Op *op = p->second;
      if (op->paused)
	need_resend.insert(op);
    }
  
  // resend requests
  for (set<Op*>::iterator p = need_resend.begin(); p != need_resend.end(); p++) {
    Op *op = *p;
    if (op->session)
      send_op(*p);
  }
  for (set<LingerOp*>::iterator p = need_resend_linger.begin(); p != need_resend_linger.end(); p++) {
    LingerOp *op = *p;
    if (op->session)
      send_linger(*p);
  }

  dump_active();
  
  // finish any Contexts that were waiting on a map update
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

Objecter::OSDSession *Objecter::get_session(int osd)
{
  map<int,OSDSession*>::iterator p = osd_sessions.find(osd);
  if (p != osd_sessions.end())
    return p->second;
  OSDSession *s = new OSDSession(osd);
  osd_sessions[osd] = s;
  s->con = messenger->get_connection(osdmap->get_inst(osd));
  return s;
}

void Objecter::reopen_session(OSDSession *s)
{
  entity_inst_t inst = osdmap->get_inst(s->osd);
  dout(10) << "reopen_session osd" << s->osd << " session, addr now " << inst << dendl;
  if (s->con) {
    messenger->mark_down(s->con);
    s->con->put();
  }
  s->con = messenger->get_connection(inst);
  s->incarnation++;
}

void Objecter::close_session(OSDSession *s)
{
  dout(10) << "close_session for osd" << s->osd << dendl;
  if (s->con) {
    messenger->mark_down(s->con);
    s->con->put();
  }
  s->ops.clear();
  s->linger_ops.clear();
  osd_sessions.erase(s->osd);
  delete s;
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


void Objecter::kick_requests(OSDSession *session)
{
  dout(10) << "kick_requests for osd" << session->osd << dendl;

  // resend ops
  for (xlist<Op*>::iterator p = session->ops.begin(); !p.end(); ++p)
    send_op(*p);

  // resend lingers
  for (xlist<LingerOp*>::iterator j = session->linger_ops.begin(); !j.end(); ++j)
    send_linger(*j);
}


void Objecter::tick()
{
  dout(10) << "tick" << dendl;

  set<OSDSession*> toping;

  // look for laggy requests
  utime_t cutoff = g_clock.now();
  cutoff -= g_conf.objecter_timeout;  // timeout

  for (hash_map<tid_t,Op*>::iterator p = ops.begin();
       p != ops.end();
       p++) {
    Op *op = p->second;
    if (op->session && op->stamp < cutoff) {
      dout(2) << " tid " << p->first << " on osd" << op->session->osd << " is laggy" << dendl;
      toping.insert(op->session);
    }
  }

  if (num_homeless_ops || !toping.empty())
    maybe_request_map();

  if (!toping.empty()) {
    // send a ping to these osds, to ensure we detect any session resets
    // (osd reply message policy is lossy)
    for (set<OSDSession*>::iterator i = toping.begin();
	 i != toping.end();
	 i++)
      messenger->send_message(new MPing, osdmap->get_inst((*i)->osd));
  }
    
  // reschedule
  timer.add_event_after(g_conf.objecter_tick_interval, new C_Tick(this));
}

void Objecter::resend_mon_ops()
{
  utime_t cutoff = g_clock.now();
  cutoff -= g_conf.objecter_mon_retry_interval;


  for (map<tid_t,PoolStatOp*>::iterator p = poolstat_ops.begin(); p!=poolstat_ops.end(); ++p) {
    if (p->second->last_submit < cutoff)
      poolstat_submit(p->second);
  }

  for (map<tid_t,StatfsOp*>::iterator p = statfs_ops.begin(); p!=statfs_ops.end(); ++p) {
    if (p->second->last_submit < cutoff)
      fs_stats_submit(p->second);
  }

  for (map<tid_t,PoolOp*>::iterator p = pool_ops.begin(); p!=pool_ops.end(); ++p) {
    if (p->second->last_submit < cutoff)
      pool_op_submit(p->second);
  }
}



// read | write ---------------------------

tid_t Objecter::op_submit(Op *op, OSDSession *s)
{
  // throttle.  before we look at any state, because
  // take_op_budget() may drop our lock while it blocks.
  take_op_budget(op);

  // pick tid
  op->tid = ++last_tid;
  assert(client_inc >= 0);

  // pick target
  if (s) {
    op->session = s;
    s->ops.push_back(&op->session_item);
  } else {
    num_homeless_ops++;  // initially!
    recalc_op_target(op);
  }
    
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
  ops[op->tid] = op;

  // send?
  dout(10) << "op_submit oid " << op->oid
           << " " << op->oloc 
	   << " " << op->ops << " tid " << op->tid
           << " osd" << (op->session ? op->session->osd : -1)
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
  } else if (op->session) {
    send_op(op);
  } else 
    maybe_request_map();
  
  dout(5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;
  
  return op->tid;
}

bool Objecter::is_pg_changed(vector<int>& o, vector<int>& n, bool any_change)
{
  if (o.empty() && n.empty())
    return false;    // both still empty
  if (o.empty() ^ n.empty())
    return true;     // was empty, now not, or vice versa
  if (o[0] != n[0])
    return true;     // primary changed
  if (any_change && o != n)
    return true;
  return false;      // same primary (tho replicas may have changed)
}

bool Objecter::recalc_op_target(Op *op)
{
  vector<int> acting;
  pg_t pgid = op->pgid;
  if (op->oid.name.length())
    pgid = osdmap->object_locator_to_pg(op->oid, op->oloc);
  osdmap->pg_to_acting_osds(pgid, acting);

  if (op->pgid != pgid || is_pg_changed(op->acting, acting, op->used_replica)) {
    op->pgid = pgid;
    op->acting = acting;
    dout(10) << "recalc_op_target tid " << op->tid
	     << " pgid " << pgid << " acting " << acting << dendl;

    OSDSession *s = NULL;
    op->used_replica = false;
    if (acting.size()) {
      int osd;
      bool read = (op->flags & CEPH_OSD_FLAG_READ) && (op->flags & CEPH_OSD_FLAG_WRITE) == 0;
      if (read && (op->flags & CEPH_OSD_FLAG_BALANCE_READS)) {
	int p = rand() % acting.size();
	if (p)
	  op->used_replica = true;
	osd = acting[p];
	dout(10) << " chose random osd" << osd << " of " << acting << dendl;
      } else if (read && (op->flags & CEPH_OSD_FLAG_LOCALIZE_READS)) {
	// look for a local replica
	unsigned i;
	for (i = acting.size()-1; i > 0; i++)
	  if (osdmap->get_addr(i).is_same_host(messenger->get_myaddr())) {
	    op->used_replica = true;
	    dout(10) << " chose local osd" << acting[i] << " of " << acting << dendl;
	    break;
	  }
	osd = acting[i];
      } else
	osd = acting[0];
      s = get_session(osd);
    }

    if (op->session != s) {
      if (!op->session)
	num_homeless_ops--;
      op->session_item.remove_myself();
      op->session = s;
      if (s)
	s->ops.push_back(&op->session_item);
      else
	num_homeless_ops++;
    }
    return true;
  }
  return false;
}

bool Objecter::recalc_linger_op_target(LingerOp *linger_op)
{
  vector<int> acting;
  pg_t pgid = osdmap->object_locator_to_pg(linger_op->oid, linger_op->oloc);
  osdmap->pg_to_acting_osds(pgid, acting);

  if (pgid != linger_op->pgid || is_pg_changed(linger_op->acting, acting)) {
    linger_op->pgid = pgid;
    linger_op->acting = acting;
    dout(10) << "recalc_linger_op_target tid " << linger_op->linger_id
	     << " pgid " << pgid << " acting " << acting << dendl;
    
    OSDSession *s = acting.size() ? get_session(acting[0]) : NULL;
    if (linger_op->session != s) {
      linger_op->session_item.remove_myself();
      linger_op->session = s;
      if (s)
	s->linger_ops.push_back(&linger_op->session_item);
    }
    return true;
  }
  return false;
}

void Objecter::send_op(Op *op)
{
  dout(15) << "send_op " << op->tid << " to osd" << op->session->osd << dendl;

  int flags = op->flags;
  if (op->oncommit)
    flags |= CEPH_OSD_FLAG_ONDISK;
  if (op->onack)
    flags |= CEPH_OSD_FLAG_ACK;

  assert(op->session->con);

  // preallocated rx buffer?
  if (op->con) {
    dout(20) << " revoking rx buffer for " << op->tid << " on " << op->con << dendl;
    op->con->revoke_rx_buffer(op->tid);
    op->con->put();
  }
  if (op->outbl && op->outbl->length()) {
    dout(20) << " posting rx buffer for " << op->tid << " on " << op->session->con << dendl;
    op->con = op->session->con->get();
    op->con->post_rx_buffer(op->tid, *op->outbl);
  }

  op->paused = false;
  op->incarnation = op->session->incarnation;
  op->stamp = g_clock.now();

  MOSDOp *m = new MOSDOp(client_inc, op->tid, 
			 op->oid, op->oloc, op->pgid, osdmap->get_epoch(),
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

  messenger->send_message(m, op->session->con);
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
      if (i->op.op & CEPH_OSD_OP_TYPE_DATA) {
        if ((int64_t)i->op.extent.length > 0)
	  op_budget += (int64_t)i->op.extent.length;
      } else if (i->op.op & CEPH_OSD_OP_TYPE_ATTR) {
        op_budget += i->op.xattr.name_len + i->op.xattr.value_len;
      }
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

  if (ops.count(tid) == 0) {
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
  Op *op = ops[tid];

  if (op->session->con != m->get_connection()) {
    dout(7) << " ignoring reply from " << m->get_source_inst()
	    << ", i last sent to " << op->session->con->get_peer_addr() << dendl;
    m->put();
    return;
  }

  Context *onack = 0;
  Context *oncommit = 0;

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

  if (op->objver)
    *op->objver = m->get_version();

  // got data?
  if (op->outbl) {
    if (op->con)
      op->con->revoke_rx_buffer(op->tid);
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
    op->session_item.remove_myself();
    dout(15) << "handle_osd_op_reply completed tid " << tid << dendl;
    put_op_budget(op);
    ops.erase(tid);
    if (op->con)
      op->con->put();
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
  op.pg_ls(list_context->max_entries, list_context->filter, list_context->cookie);

  bufferlist *bl = new bufferlist();
  C_List *onack = new C_List(list_context, onfinish, bl, this);

  object_t oid;
  object_locator_t oloc(list_context->pool_id);

  // 
  Op *o = new Op(oid, oloc, op.ops, CEPH_OSD_FLAG_READ, onack, NULL, NULL);
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
  bufferlist extra_info;
  ::decode(response, iter);
  if (!iter.end()) {
    ::decode(extra_info, iter);
  }
  list_context->cookie = (uint64_t)response.handle;

  int response_size = response.entries.size();
  dout(20) << "response.entries.size " << response_size
	   << ", response.entries " << response.entries << dendl;
  list_context->extra_info.append(extra_info);
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
  pool_ops[op->tid] = op;

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
  pool_ops[op->tid] = op;

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
  pool_ops[op->tid] = op;
  
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
  pool_ops[op->tid] = op;

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
  pool_ops[op->tid] = op;
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
  pool_ops[op->tid] = op;

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
  pool_ops[op->tid] = op;

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
  if (pool_ops.count(tid)) {
    PoolOp *op = pool_ops[tid];
    dout(10) << "have request " << tid << " at " << op << " Op: " << ceph_pool_op_name(op->pool_op) << dendl;
    if (op->blp)
      op->blp->claim(*m->response_data);
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
    pool_ops.erase(tid);
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
  poolstat_ops[op->tid] = op;

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

  if (poolstat_ops.count(tid)) {
    PoolStatOp *op = poolstat_ops[tid];
    dout(10) << "have request " << tid << " at " << op << dendl;
    *op->pool_stats = m->pool_stats;
    if (m->version > last_seen_pgmap_version)
      last_seen_pgmap_version = m->version;
    op->onfinish->finish(0);
    delete op->onfinish;
    poolstat_ops.erase(tid);
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
  statfs_ops[op->tid] = op;

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

  if (statfs_ops.count(tid)) {
    StatfsOp *op = statfs_ops[tid];
    dout(10) << "have request " << tid << " at " << op << dendl;
    *(op->stats) = m->h.st;
    if (m->h.version > last_seen_pgmap_version)
      last_seen_pgmap_version = m->h.version;
    op->onfinish->finish(0);
    delete op->onfinish;
    statfs_ops.erase(tid);
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
      map<int,OSDSession*>::iterator p = osd_sessions.find(osd);
      if (p != osd_sessions.end()) {
	OSDSession *session = p->second;
	reopen_session(session);
	kick_requests(session);
	maybe_request_map();
      }
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
  dout(20) << "dump_active .. " << num_homeless_ops << " homeless" << dendl;
  for (hash_map<tid_t,Op*>::iterator p = ops.begin(); p != ops.end(); p++) {
    Op *op = p->second;
    dout(20) << op->tid << "\t" << op->pgid << "\tosd" << (op->session ? op->session->osd : -1)
	    << "\t" << op->oid << "\t" << op->ops << dendl;
  }
}

