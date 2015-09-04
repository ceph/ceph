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
#include "Filer.h"

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
#include "messages/MMonCommand.h"

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include <errno.h>

#include "common/config.h"
#include "common/perf_counters.h"
#include "include/str_list.h"
#include "common/errno.h"


#define dout_subsys ceph_subsys_objecter
#undef dout_prefix
#define dout_prefix *_dout << messenger->get_myname() << ".objecter "


enum {
  l_osdc_first = 123200,
  l_osdc_op_active,
  l_osdc_op_laggy,
  l_osdc_op_send,
  l_osdc_op_send_bytes,
  l_osdc_op_resend,
  l_osdc_op_ack,
  l_osdc_op_commit,

  l_osdc_op,
  l_osdc_op_r,
  l_osdc_op_w,
  l_osdc_op_rmw,
  l_osdc_op_pg,

  l_osdc_osdop_stat,
  l_osdc_osdop_create,
  l_osdc_osdop_read,
  l_osdc_osdop_write,
  l_osdc_osdop_writefull,
  l_osdc_osdop_append,
  l_osdc_osdop_zero,
  l_osdc_osdop_truncate,
  l_osdc_osdop_delete,
  l_osdc_osdop_mapext,
  l_osdc_osdop_sparse_read,
  l_osdc_osdop_clonerange,
  l_osdc_osdop_getxattr,
  l_osdc_osdop_setxattr,
  l_osdc_osdop_cmpxattr,
  l_osdc_osdop_rmxattr,
  l_osdc_osdop_resetxattrs,
  l_osdc_osdop_tmap_up,
  l_osdc_osdop_tmap_put,
  l_osdc_osdop_tmap_get,
  l_osdc_osdop_call,
  l_osdc_osdop_watch,
  l_osdc_osdop_notify,
  l_osdc_osdop_src_cmpxattr,
  l_osdc_osdop_pgls,
  l_osdc_osdop_pgls_filter,
  l_osdc_osdop_other,

  l_osdc_linger_active,
  l_osdc_linger_send,
  l_osdc_linger_resend,

  l_osdc_poolop_active,
  l_osdc_poolop_send,
  l_osdc_poolop_resend,

  l_osdc_poolstat_active,
  l_osdc_poolstat_send,
  l_osdc_poolstat_resend,

  l_osdc_statfs_active,
  l_osdc_statfs_send,
  l_osdc_statfs_resend,

  l_osdc_command_active,
  l_osdc_command_send,
  l_osdc_command_resend,

  l_osdc_map_epoch,
  l_osdc_map_full,
  l_osdc_map_inc,

  l_osdc_osd_sessions,
  l_osdc_osd_session_open,
  l_osdc_osd_session_close,
  l_osdc_osd_laggy,
  l_osdc_last,
};


// config obs ----------------------------

static const char *config_keys[] = {
  "crush_location",
  NULL
};

const char** Objecter::get_tracked_conf_keys() const
{
  return config_keys;
}


void Objecter::handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed)
{
  if (changed.count("crush_location")) {
    crush_location.clear();
    vector<string> lvec;
    get_str_vec(cct->_conf->crush_location, ";, \t", lvec);
    int r = CrushWrapper::parse_loc_multimap(lvec, &crush_location);
    if (r < 0) {
      lderr(cct) << "warning: crush_location '" << cct->_conf->crush_location
		 << "' does not parse" << dendl;
    }
  }
}


// messages ------------------------------

void Objecter::init_unlocked()
{
  assert(!initialized);

  if (!logger) {
    PerfCountersBuilder pcb(cct, "objecter", l_osdc_first, l_osdc_last);

    pcb.add_u64(l_osdc_op_active, "op_active");
    pcb.add_u64(l_osdc_op_laggy, "op_laggy");
    pcb.add_u64_counter(l_osdc_op_send, "op_send");
    pcb.add_u64_counter(l_osdc_op_send_bytes, "op_send_bytes");
    pcb.add_u64_counter(l_osdc_op_resend, "op_resend");
    pcb.add_u64_counter(l_osdc_op_ack, "op_ack");
    pcb.add_u64_counter(l_osdc_op_commit, "op_commit");

    pcb.add_u64_counter(l_osdc_op, "op");
    pcb.add_u64_counter(l_osdc_op_r, "op_r");
    pcb.add_u64_counter(l_osdc_op_w, "op_w");
    pcb.add_u64_counter(l_osdc_op_rmw, "op_rmw");
    pcb.add_u64_counter(l_osdc_op_pg, "op_pg");

    pcb.add_u64_counter(l_osdc_osdop_stat, "osdop_stat");
    pcb.add_u64_counter(l_osdc_osdop_create, "osdop_create");
    pcb.add_u64_counter(l_osdc_osdop_read, "osdop_read");
    pcb.add_u64_counter(l_osdc_osdop_write, "osdop_write");
    pcb.add_u64_counter(l_osdc_osdop_writefull, "osdop_writefull");
    pcb.add_u64_counter(l_osdc_osdop_append, "osdop_append");
    pcb.add_u64_counter(l_osdc_osdop_zero, "osdop_zero");
    pcb.add_u64_counter(l_osdc_osdop_truncate, "osdop_truncate");
    pcb.add_u64_counter(l_osdc_osdop_delete, "osdop_delete");
    pcb.add_u64_counter(l_osdc_osdop_mapext, "osdop_mapext");
    pcb.add_u64_counter(l_osdc_osdop_sparse_read, "osdop_sparse_read");
    pcb.add_u64_counter(l_osdc_osdop_clonerange, "osdop_clonerange");
    pcb.add_u64_counter(l_osdc_osdop_getxattr, "osdop_getxattr");
    pcb.add_u64_counter(l_osdc_osdop_setxattr, "osdop_setxattr");
    pcb.add_u64_counter(l_osdc_osdop_cmpxattr, "osdop_cmpxattr");
    pcb.add_u64_counter(l_osdc_osdop_rmxattr, "osdop_rmxattr");
    pcb.add_u64_counter(l_osdc_osdop_resetxattrs, "osdop_resetxattrs");
    pcb.add_u64_counter(l_osdc_osdop_tmap_up, "osdop_tmap_up");
    pcb.add_u64_counter(l_osdc_osdop_tmap_put, "osdop_tmap_put");
    pcb.add_u64_counter(l_osdc_osdop_tmap_get, "osdop_tmap_get");
    pcb.add_u64_counter(l_osdc_osdop_call, "osdop_call");
    pcb.add_u64_counter(l_osdc_osdop_watch, "osdop_watch");
    pcb.add_u64_counter(l_osdc_osdop_notify, "osdop_notify");
    pcb.add_u64_counter(l_osdc_osdop_src_cmpxattr, "osdop_src_cmpxattr");
    pcb.add_u64_counter(l_osdc_osdop_pgls, "osdop_pgls");
    pcb.add_u64_counter(l_osdc_osdop_pgls_filter, "osdop_pgls_filter");
    pcb.add_u64_counter(l_osdc_osdop_other, "osdop_other");

    pcb.add_u64(l_osdc_linger_active, "linger_active");
    pcb.add_u64_counter(l_osdc_linger_send, "linger_send");
    pcb.add_u64_counter(l_osdc_linger_resend, "linger_resend");

    pcb.add_u64(l_osdc_poolop_active, "poolop_active");
    pcb.add_u64_counter(l_osdc_poolop_send, "poolop_send");
    pcb.add_u64_counter(l_osdc_poolop_resend, "poolop_resend");

    pcb.add_u64(l_osdc_poolstat_active, "poolstat_active");
    pcb.add_u64_counter(l_osdc_poolstat_send, "poolstat_send");
    pcb.add_u64_counter(l_osdc_poolstat_resend, "poolstat_resend");

    pcb.add_u64(l_osdc_statfs_active, "statfs_active");
    pcb.add_u64_counter(l_osdc_statfs_send, "statfs_send");
    pcb.add_u64_counter(l_osdc_statfs_resend, "statfs_resend");

    pcb.add_u64(l_osdc_command_active, "command_active");
    pcb.add_u64_counter(l_osdc_command_send, "command_send");
    pcb.add_u64_counter(l_osdc_command_resend, "command_resend");

    pcb.add_u64(l_osdc_map_epoch, "map_epoch");
    pcb.add_u64_counter(l_osdc_map_full, "map_full");
    pcb.add_u64_counter(l_osdc_map_inc, "map_inc");

    pcb.add_u64(l_osdc_osd_sessions, "osd_sessions");  // open sessions
    pcb.add_u64_counter(l_osdc_osd_session_open, "osd_session_open");
    pcb.add_u64_counter(l_osdc_osd_session_close, "osd_session_close");
    pcb.add_u64(l_osdc_osd_laggy, "osd_laggy");

    logger = pcb.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
  }

  m_request_state_hook = new RequestStateHook(this);
  AdminSocket* admin_socket = cct->get_admin_socket();
  int ret = admin_socket->register_command("objecter_requests",
					   "objecter_requests",
					   m_request_state_hook,
					   "show in-progress osd requests");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(ret) << dendl;
  }
}

void Objecter::init_locked()
{
  assert(client_lock.is_locked());
  assert(!initialized);

  schedule_tick();
  if (osdmap->get_epoch() == 0)
    maybe_request_map();

  initialized = true;
}

void Objecter::shutdown_locked() 
{
  assert(client_lock.is_locked());
  assert(initialized);
  initialized = false;

  map<int,OSDSession*>::iterator p;
  while (!osd_sessions.empty()) {
    p = osd_sessions.begin();
    close_session(p->second);
  }

  if (tick_event) {
    timer.cancel_event(tick_event);
    tick_event = NULL;
  }
}

void Objecter::shutdown_unlocked()
{
  if (m_request_state_hook) {
    AdminSocket* admin_socket = cct->get_admin_socket();
    admin_socket->unregister_command("objecter_requests");
    delete m_request_state_hook;
    m_request_state_hook = NULL;
  }

  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = NULL;
  }
}

void Objecter::send_linger(LingerOp *info)
{
  ldout(cct, 15) << "send_linger " << info->linger_id << dendl;
  vector<OSDOp> opv = info->ops; // need to pass a copy to ops
  Context *onack = (!info->registered && info->on_reg_ack) ? new C_Linger_Ack(this, info) : NULL;
  Context *oncommit = new C_Linger_Commit(this, info);
  Op *o = new Op(info->target.base_oid, info->target.base_oloc,
		 opv, info->target.flags | CEPH_OSD_FLAG_READ,
		 onack, oncommit,
		 info->pobjver);
  o->snapid = info->snap;
  o->snapc = info->snapc;
  o->mtime = info->mtime;

  // do not resend this; we will send a new op to reregister
  o->should_resend = false;

  if (info->session) {
    int r = recalc_op_target(o);
    if (r == RECALC_OP_TARGET_POOL_DNE) {
      _send_linger_map_check(info);
    }
  }

  if (info->register_tid) {
    // repeat send.  cancel old registeration op, if any.
    if (ops.count(info->register_tid)) {
      Op *o = ops[info->register_tid];
      op_cancel_map_check(o);
      cancel_linger_op(o);
    }
    info->register_tid = _op_submit(o);
  } else {
    // first send
    // populate info->pgid and info->acting so we
    // don't resend the linger op on the next osdmap update
    recalc_linger_op_target(info);
    info->register_tid = op_submit(o);
  }

  OSDSession *s = o->session;
  if (info->session != s) {
    info->session_item.remove_myself();
    info->session = s;
    if (info->session)
      s->linger_ops.push_back(&info->session_item);
  }

  logger->inc(l_osdc_linger_send);
}

void Objecter::_linger_ack(LingerOp *info, int r) 
{
  ldout(cct, 10) << "_linger_ack " << info->linger_id << dendl;
  if (info->on_reg_ack) {
    info->on_reg_ack->complete(r);
    info->on_reg_ack = NULL;
  }
}

void Objecter::_linger_commit(LingerOp *info, int r) 
{
  ldout(cct, 10) << "_linger_commit " << info->linger_id << dendl;
  if (info->on_reg_commit) {
    info->on_reg_commit->complete(r);
    info->on_reg_commit = NULL;
  }

  // only tell the user the first time we do this
  info->registered = true;
  info->pobjver = NULL;
}

void Objecter::unregister_linger(uint64_t linger_id)
{
  map<uint64_t, LingerOp*>::iterator iter = linger_ops.find(linger_id);
  if (iter != linger_ops.end()) {
    LingerOp *info = iter->second;
    info->session_item.remove_myself();
    linger_ops.erase(iter);
    info->put();
    logger->set(l_osdc_linger_active, linger_ops.size());
  }
}

ceph_tid_t Objecter::linger_mutate(const object_t& oid, const object_locator_t& oloc,
			      ObjectOperation& op,
			      const SnapContext& snapc, utime_t mtime,
			      bufferlist& inbl, int flags,
			      Context *onack, Context *oncommit,
			      version_t *objver)
{
  LingerOp *info = new LingerOp;
  info->target.base_oid = oid;
  info->target.base_oloc = oloc;
  if (info->target.base_oloc.key == oid)
    info->target.base_oloc.key.clear();
  info->snapc = snapc;
  info->mtime = mtime;
  info->target.flags = flags | CEPH_OSD_FLAG_WRITE;
  info->ops = op.ops;
  info->inbl = inbl;
  info->poutbl = NULL;
  info->pobjver = objver;
  info->on_reg_ack = onack;
  info->on_reg_commit = oncommit;

  info->linger_id = ++max_linger_id;
  linger_ops[info->linger_id] = info;

  logger->set(l_osdc_linger_active, linger_ops.size());

  send_linger(info);

  return info->linger_id;
}

ceph_tid_t Objecter::linger_read(const object_t& oid, const object_locator_t& oloc,
			    ObjectOperation& op,
			    snapid_t snap, bufferlist& inbl, bufferlist *poutbl, int flags,
			    Context *onfinish,
			    version_t *objver)
{
  LingerOp *info = new LingerOp;
  info->target.base_oid = oid;
  info->target.base_oloc = oloc;
  if (info->target.base_oloc.key == oid)
    info->target.base_oloc.key.clear();
  info->snap = snap;
  info->target.flags = flags;
  info->ops = op.ops;
  info->inbl = inbl;
  info->poutbl = poutbl;
  info->pobjver = objver;
  info->on_reg_commit = onfinish;

  info->linger_id = ++max_linger_id;
  linger_ops[info->linger_id] = info;

  logger->set(l_osdc_linger_active, linger_ops.size());

  send_linger(info);

  return info->linger_id;
}

void Objecter::dispatch(Message *m)
{
  switch (m->get_type()) {
  case CEPH_MSG_OSD_OPREPLY:
    handle_osd_op_reply(static_cast<MOSDOpReply*>(m));
    break;
    
  case CEPH_MSG_OSD_MAP:
    handle_osd_map(static_cast<MOSDMap*>(m));
    break;

  case MSG_GETPOOLSTATSREPLY:
    handle_get_pool_stats_reply(static_cast<MGetPoolStatsReply*>(m));
    break;

  case CEPH_MSG_STATFS_REPLY:
    handle_fs_stats_reply(static_cast<MStatfsReply*>(m));
    break;

  case CEPH_MSG_POOLOP_REPLY:
    handle_pool_op_reply(static_cast<MPoolOpReply*>(m));
    break;

  case MSG_COMMAND_REPLY:
    handle_command_reply(static_cast<MCommandReply*>(m));
    break;

  default:
    ldout(cct, 0) << "don't know message type " << m->get_type() << dendl;
    assert(0);
  }
}

void Objecter::scan_requests(bool force_resend,
			     bool force_resend_writes,
			     map<ceph_tid_t, Op*>& need_resend,
			     list<LingerOp*>& need_resend_linger,
			     map<ceph_tid_t, CommandOp*>& need_resend_command)
{
  // check for changed linger mappings (_before_ regular ops)
  map<ceph_tid_t,LingerOp*>::iterator lp = linger_ops.begin();
  while (lp != linger_ops.end()) {
    LingerOp *op = lp->second;
    ++lp;   // check_linger_pool_dne() may touch linger_ops; prevent iterator invalidation
    ldout(cct, 10) << " checking linger op " << op->linger_id << dendl;
    int r = recalc_linger_op_target(op);
    switch (r) {
    case RECALC_OP_TARGET_NO_ACTION:
      if (!force_resend && !force_resend_writes)
	break;
      // -- fall-thru --
    case RECALC_OP_TARGET_NEED_RESEND:
      need_resend_linger.push_back(op);
      linger_cancel_map_check(op);
      break;
    case RECALC_OP_TARGET_POOL_DNE:
      check_linger_pool_dne(op);
      break;
    }
  }

  // check for changed request mappings
  map<ceph_tid_t,Op*>::iterator p = ops.begin();
  while (p != ops.end()) {
    Op *op = p->second;
    ++p;   // check_op_pool_dne() may touch ops; prevent iterator invalidation
    ldout(cct, 10) << " checking op " << op->tid << dendl;
    int r = recalc_op_target(op);
    switch (r) {
    case RECALC_OP_TARGET_NO_ACTION:
      if (!force_resend &&
	  (!force_resend_writes || !(op->target.flags & CEPH_OSD_FLAG_WRITE)))
	break;
      // -- fall-thru --
    case RECALC_OP_TARGET_NEED_RESEND:
      need_resend[op->tid] = op;
      op_cancel_map_check(op);
      break;
    case RECALC_OP_TARGET_POOL_DNE:
      check_op_pool_dne(op);
      break;
    }
  }

  // commands
  map<ceph_tid_t,CommandOp*>::iterator cp = command_ops.begin();
  while (cp != command_ops.end()) {
    CommandOp *c = cp->second;
    ++cp;
    ldout(cct, 10) << " checking command " << c->tid << dendl;
    int r = recalc_command_target(c);
    switch (r) {
    case RECALC_OP_TARGET_NO_ACTION:
      // resend if skipped map; otherwise do nothing.
      if (!force_resend && !force_resend_writes)
	break;
      // -- fall-thru --
    case RECALC_OP_TARGET_NEED_RESEND:
      need_resend_command[c->tid] = c;
      command_cancel_map_check(c);
      break;
    case RECALC_OP_TARGET_POOL_DNE:
    case RECALC_OP_TARGET_OSD_DNE:
    case RECALC_OP_TARGET_OSD_DOWN:
      check_command_map_dne(c);
      break;
    }     
  }
}

void Objecter::handle_osd_map(MOSDMap *m)
{
  assert(client_lock.is_locked());
  assert(initialized);
  assert(osdmap); 

  if (m->fsid != monc->get_fsid()) {
    ldout(cct, 0) << "handle_osd_map fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    m->put();
    return;
  }

  bool was_pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool was_full = osdmap_full_flag();
  bool was_pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || was_full;

  list<LingerOp*> need_resend_linger;
  map<ceph_tid_t, Op*> need_resend;
  map<ceph_tid_t, CommandOp*> need_resend_command;

  if (m->get_last() <= osdmap->get_epoch()) {
    ldout(cct, 3) << "handle_osd_map ignoring epochs [" 
            << m->get_first() << "," << m->get_last() 
            << "] <= " << osdmap->get_epoch() << dendl;
  } 
  else {
    ldout(cct, 3) << "handle_osd_map got epochs [" 
            << m->get_first() << "," << m->get_last() 
            << "] > " << osdmap->get_epoch()
            << dendl;

    if (osdmap->get_epoch()) {
      bool skipped_map = false;
      // we want incrementals
      for (epoch_t e = osdmap->get_epoch() + 1;
	   e <= m->get_last();
	   e++) {
 
	if (osdmap->get_epoch() == e-1 &&
	    m->incremental_maps.count(e)) {
	  ldout(cct, 3) << "handle_osd_map decoding incremental epoch " << e << dendl;
	  OSDMap::Incremental inc(m->incremental_maps[e]);
	  osdmap->apply_incremental(inc);
	  logger->inc(l_osdc_map_inc);
	}
	else if (m->maps.count(e)) {
	  ldout(cct, 3) << "handle_osd_map decoding full epoch " << e << dendl;
	  osdmap->decode(m->maps[e]);
	  logger->inc(l_osdc_map_full);
	}
	else {
	  if (e >= m->get_oldest()) {
	    ldout(cct, 3) << "handle_osd_map requesting missing epoch " << osdmap->get_epoch()+1 << dendl;
	    maybe_request_map();
	    break;
	  }
	  ldout(cct, 3) << "handle_osd_map missing epoch " << osdmap->get_epoch()+1
			<< ", jumping to " << m->get_oldest() << dendl;
	  e = m->get_oldest() - 1;
	  skipped_map = true;
	  continue;
	}
	logger->set(l_osdc_map_epoch, osdmap->get_epoch());

	was_full = was_full || osdmap_full_flag();
	scan_requests(skipped_map, was_full, need_resend, need_resend_linger,
		      need_resend_command);

	// osd addr changes?
	for (map<int,OSDSession*>::iterator p = osd_sessions.begin();
	     p != osd_sessions.end(); ) {
	  OSDSession *s = p->second;
	  ++p;
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
	ldout(cct, 3) << "handle_osd_map decoding full epoch " << m->get_last() << dendl;
	osdmap->decode(m->maps[m->get_last()]);

	scan_requests(false, false, need_resend, need_resend_linger,
		      need_resend_command);
      } else {
	ldout(cct, 3) << "handle_osd_map hmm, i want a full map, requesting" << dendl;
	monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
	monc->renew_subs();
      }
    }
  }

  bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || osdmap_full_flag();

  // was/is paused?
  if (was_pauserd || was_pausewr || pauserd || pausewr)
    maybe_request_map();

  // resend requests
  for (map<ceph_tid_t, Op*>::iterator p = need_resend.begin(); p != need_resend.end(); ++p) {
    Op *op = p->second;
    if (op->should_resend) {
      if (op->session && !op->target.paused) {
	logger->inc(l_osdc_op_resend);
	send_op(op);
      }
    } else {
      cancel_linger_op(op);
    }
  }
  for (list<LingerOp*>::iterator p = need_resend_linger.begin(); p != need_resend_linger.end(); ++p) {
    LingerOp *op = *p;
    if (op->session) {
      logger->inc(l_osdc_linger_resend);
      send_linger(op);
    }
  }
  for (map<ceph_tid_t,CommandOp*>::iterator p = need_resend_command.begin(); p != need_resend_command.end(); ++p) {
    CommandOp *c = p->second;
    if (c->session) {
      _send_command(c);
    }
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
      i->first->complete(i->second);
    }
    waiting_for_map.erase(p++);
  }

  m->put();

  monc->sub_got("osdmap", osdmap->get_epoch());

  if (!waiting_for_map.empty())
    maybe_request_map();
}

// op pool check

void Objecter::C_Op_Map_Latest::finish(int r)
{
  if (r == -EAGAIN || r == -ECANCELED)
    return;

  lgeneric_subdout(objecter->cct, objecter, 10) << "op_map_latest r=" << r << " tid=" << tid
						<< " latest " << latest << dendl;

  Mutex::Locker l(objecter->client_lock);

  map<ceph_tid_t, Op*>::iterator iter =
    objecter->check_latest_map_ops.find(tid);
  if (iter == objecter->check_latest_map_ops.end()) {
    lgeneric_subdout(objecter->cct, objecter, 10) << "op_map_latest op " << tid << " not found" << dendl;
    return;
  }

  Op *op = iter->second;
  objecter->check_latest_map_ops.erase(iter);

  lgeneric_subdout(objecter->cct, objecter, 20) << "op_map_latest op " << op << dendl;

  if (op->map_dne_bound == 0)
    op->map_dne_bound = latest;

  objecter->check_op_pool_dne(op);
}

void Objecter::check_op_pool_dne(Op *op)
{
  ldout(cct, 10) << "check_op_pool_dne tid " << op->tid
		 << " current " << osdmap->get_epoch()
		 << " map_dne_bound " << op->map_dne_bound
		 << dendl;
  if (op->map_dne_bound > 0) {
    if (osdmap->get_epoch() >= op->map_dne_bound) {
      // we had a new enough map
      ldout(cct, 10) << "check_op_pool_dne tid " << op->tid
		     << " concluding pool " << op->target.base_pgid.pool() << " dne"
		     << dendl;
      if (op->onack) {
	op->onack->complete(-ENOENT);
      }
      if (op->oncommit) {
	op->oncommit->complete(-ENOENT);
      }
      op->session_item.remove_myself();
      ops.erase(op->tid);
      delete op;
    }
  } else {
    _send_op_map_check(op);
  }
}

void Objecter::_send_op_map_check(Op *op)
{
  assert(client_lock.is_locked());
  // ask the monitor
  if (check_latest_map_ops.count(op->tid) == 0) {
    check_latest_map_ops[op->tid] = op;
    C_Op_Map_Latest *c = new C_Op_Map_Latest(this, op->tid);
    monc->get_version("osdmap", &c->latest, NULL, c);
  }
}

void Objecter::op_cancel_map_check(Op *op)
{
  assert(client_lock.is_locked());
  map<ceph_tid_t, Op*>::iterator iter =
    check_latest_map_ops.find(op->tid);
  if (iter != check_latest_map_ops.end()) {
    check_latest_map_ops.erase(iter);
  }
}

// linger pool check

void Objecter::C_Linger_Map_Latest::finish(int r)
{
  if (r == -EAGAIN || r == -ECANCELED) {
    // ignore callback; we will retry in resend_mon_ops()
    return;
  }

  Mutex::Locker l(objecter->client_lock);

  map<uint64_t, LingerOp*>::iterator iter =
    objecter->check_latest_map_lingers.find(linger_id);
  if (iter == objecter->check_latest_map_lingers.end()) {
    return;
  }

  LingerOp *op = iter->second;
  objecter->check_latest_map_lingers.erase(iter);
  op->put();

  if (op->map_dne_bound == 0)
    op->map_dne_bound = latest;

  objecter->check_linger_pool_dne(op);
}

void Objecter::check_linger_pool_dne(LingerOp *op)
{
  ldout(cct, 10) << "check_linger_pool_dne linger_id " << op->linger_id
		 << " current " << osdmap->get_epoch()
		 << " map_dne_bound " << op->map_dne_bound
		 << dendl;
  if (op->map_dne_bound > 0) {
    if (osdmap->get_epoch() >= op->map_dne_bound) {
      if (op->on_reg_ack) {
	op->on_reg_ack->complete(-ENOENT);
      }
      if (op->on_reg_commit) {
	op->on_reg_commit->complete(-ENOENT);
      }
      unregister_linger(op->linger_id);
    }
  } else {
    _send_linger_map_check(op);
  }
}

void Objecter::_send_linger_map_check(LingerOp *op)
{
  // ask the monitor
  if (check_latest_map_lingers.count(op->linger_id) == 0) {
    op->get();
    check_latest_map_lingers[op->linger_id] = op;
    C_Linger_Map_Latest *c = new C_Linger_Map_Latest(this, op->linger_id);
    monc->get_version("osdmap", &c->latest, NULL, c);
  }
}

void Objecter::linger_cancel_map_check(LingerOp *op)
{
  map<uint64_t, LingerOp*>::iterator iter =
    check_latest_map_lingers.find(op->linger_id);
  if (iter != check_latest_map_lingers.end()) {
    LingerOp *op = iter->second;
    op->put();
    check_latest_map_lingers.erase(iter);
  }
}

// command pool check

void Objecter::C_Command_Map_Latest::finish(int r)
{
  if (r == -EAGAIN || r == -ECANCELED) {
    // ignore callback; we will retry in resend_mon_ops()
    return;
  }

  Mutex::Locker l(objecter->client_lock);

  map<uint64_t, CommandOp*>::iterator iter =
    objecter->check_latest_map_commands.find(tid);
  if (iter == objecter->check_latest_map_commands.end()) {
    return;
  }

  CommandOp *c = iter->second;
  objecter->check_latest_map_commands.erase(iter);
  c->put();

  if (c->map_dne_bound == 0)
    c->map_dne_bound = latest;

  objecter->check_command_map_dne(c);
}

void Objecter::check_command_map_dne(CommandOp *c)
{
  ldout(cct, 10) << "check_command_map_dne tid " << c->tid
		 << " current " << osdmap->get_epoch()
		 << " map_dne_bound " << c->map_dne_bound
		 << dendl;
  if (c->map_dne_bound > 0) {
    if (osdmap->get_epoch() >= c->map_dne_bound) {
      _finish_command(c, c->map_check_error, c->map_check_error_str);
    }
  } else {
    _send_command_map_check(c);
  }
}

void Objecter::_send_command_map_check(CommandOp *c)
{
  // ask the monitor
  if (check_latest_map_commands.count(c->tid) == 0) {
    c->get();
    check_latest_map_commands[c->tid] = c;
    C_Command_Map_Latest *f = new C_Command_Map_Latest(this, c->tid);
    monc->get_version("osdmap", &f->latest, NULL, f);
  }
}

void Objecter::command_cancel_map_check(CommandOp *c)
{
  map<uint64_t, CommandOp*>::iterator iter =
    check_latest_map_commands.find(c->tid);
  if (iter != check_latest_map_commands.end()) {
    CommandOp *c = iter->second;
    c->put();
    check_latest_map_commands.erase(iter);
  }
}



Objecter::OSDSession *Objecter::get_session(int osd)
{
  map<int,OSDSession*>::iterator p = osd_sessions.find(osd);
  if (p != osd_sessions.end())
    return p->second;
  OSDSession *s = new OSDSession(osd);
  osd_sessions[osd] = s;
  s->con = messenger->get_connection(osdmap->get_inst(osd));
  logger->inc(l_osdc_osd_session_open);
  logger->inc(l_osdc_osd_sessions, osd_sessions.size());
  return s;
}

void Objecter::reopen_session(OSDSession *s)
{
  entity_inst_t inst = osdmap->get_inst(s->osd);
  ldout(cct, 10) << "reopen_session osd." << s->osd << " session, addr now " << inst << dendl;
  if (s->con) {
    messenger->mark_down(s->con);
    logger->inc(l_osdc_osd_session_close);
  }
  s->con = messenger->get_connection(inst);
  s->incarnation++;
  logger->inc(l_osdc_osd_session_open);
}

void Objecter::close_session(OSDSession *s)
{
  ldout(cct, 10) << "close_session for osd." << s->osd << dendl;
  if (s->con) {
    messenger->mark_down(s->con);
    logger->inc(l_osdc_osd_session_close);
  }
  s->ops.clear();
  s->linger_ops.clear();
  s->command_ops.clear();
  osd_sessions.erase(s->osd);
  delete s;

  logger->set(l_osdc_osd_sessions, osd_sessions.size());
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

struct C_Objecter_GetVersion : public Context {
  Objecter *objecter;
  uint64_t oldest, newest;
  Context *fin;
  C_Objecter_GetVersion(Objecter *o, Context *c)
    : objecter(o), oldest(0), newest(0), fin(c) {}
  void finish(int r) {
    if (r >= 0)
      objecter->_get_latest_version(oldest, newest, fin);
    else if (r == -EAGAIN) { // try again as instructed
      objecter->wait_for_latest_osdmap(fin);
    } else {
      // it doesn't return any other error codes!
      assert(0);
    }
  }
};

void Objecter::wait_for_latest_osdmap(Context *fin)
{
  ldout(cct, 10) << __func__ << dendl;
  C_Objecter_GetVersion *c = new C_Objecter_GetVersion(this, fin);
  monc->get_version("osdmap", &c->newest, &c->oldest, c);
}

void Objecter::_get_latest_version(epoch_t oldest, epoch_t newest, Context *fin)
{
  if (osdmap->get_epoch() >= newest) {
  ldout(cct, 10) << __func__ << " latest " << newest << ", have it" << dendl;
    if (fin)
      fin->complete(0);
    return;
  }

  ldout(cct, 10) << __func__ << " latest " << newest << ", waiting" << dendl;
  wait_for_new_map(fin, newest, 0);
}

void Objecter::maybe_request_map()
{
  int flag = 0;
  if (osdmap_full_flag()) {
    ldout(cct, 10) << "maybe_request_map subscribing (continuous) to next osd map (FULL flag is set)" << dendl;
  } else {
    ldout(cct, 10) << "maybe_request_map subscribing (onetime) to next osd map" << dendl;
    flag = CEPH_SUBSCRIBE_ONETIME;
  }
  epoch_t epoch = osdmap->get_epoch() ? osdmap->get_epoch()+1 : 0;
  if (monc->sub_want("osdmap", epoch, flag))
    monc->renew_subs();
}

void Objecter::wait_for_new_map(Context *c, epoch_t epoch, int err)
{
  waiting_for_map[epoch].push_back(pair<Context *, int>(c, err));
  maybe_request_map();
}

void Objecter::kick_requests(OSDSession *session)
{
  ldout(cct, 10) << "kick_requests for osd." << session->osd << dendl;

  // resend ops
  map<ceph_tid_t,Op*> resend;  // resend in tid order
  for (xlist<Op*>::iterator p = session->ops.begin(); !p.end();) {
    Op *op = *p;
    ++p;
    logger->inc(l_osdc_op_resend);
    if (op->should_resend) {
      if (!op->target.paused)
	resend[op->tid] = op;
    } else {
      cancel_linger_op(op);
    }
  }
  while (!resend.empty()) {
    send_op(resend.begin()->second);
    resend.erase(resend.begin());
  }

  // resend lingers
  map<uint64_t, LingerOp*> lresend;  // resend in order
  for (xlist<LingerOp*>::iterator j = session->linger_ops.begin(); !j.end(); ++j) {
    logger->inc(l_osdc_linger_resend);
    lresend[(*j)->linger_id] = *j;
  }
  while (!lresend.empty()) {
    send_linger(lresend.begin()->second);
    lresend.erase(lresend.begin());
  }

  // resend commands
  map<uint64_t,CommandOp*> cresend;  // resend in order
  for (xlist<CommandOp*>::iterator k = session->command_ops.begin(); !k.end(); ++k) {
    logger->inc(l_osdc_command_resend);
    cresend[(*k)->tid] = *k;
  }
  while (!cresend.empty()) {
    _send_command(cresend.begin()->second);
    cresend.erase(cresend.begin());
  }
}

void Objecter::schedule_tick()
{
  assert(tick_event == NULL);
  tick_event = new C_Tick(this);
  timer.add_event_after(cct->_conf->objecter_tick_interval, tick_event);
}

void Objecter::tick()
{
  ldout(cct, 10) << "tick" << dendl;
  assert(client_lock.is_locked());
  assert(initialized);

  // we are only called by C_Tick
  assert(tick_event);
  tick_event = NULL;

  set<OSDSession*> toping;

  // look for laggy requests
  utime_t cutoff = ceph_clock_now(cct);
  cutoff -= cct->_conf->objecter_timeout;  // timeout

  unsigned laggy_ops = 0;
  for (map<ceph_tid_t,Op*>::iterator p = ops.begin();
       p != ops.end();
       ++p) {
    Op *op = p->second;
    if (op->session && op->stamp < cutoff) {
      ldout(cct, 2) << " tid " << p->first << " on osd." << op->session->osd << " is laggy" << dendl;
      toping.insert(op->session);
      ++laggy_ops;
    }
  }
  for (map<uint64_t,LingerOp*>::iterator p = linger_ops.begin();
       p != linger_ops.end();
       ++p) {
    LingerOp *op = p->second;
    if (op->session) {
      ldout(cct, 10) << " pinging osd that serves lingering tid " << p->first << " (osd." << op->session->osd << ")" << dendl;
      toping.insert(op->session);
    } else {
      ldout(cct, 10) << " lingering tid " << p->first << " does not have session" << dendl;
    }
  }
  for (map<uint64_t,CommandOp*>::iterator p = command_ops.begin();
       p != command_ops.end();
       ++p) {
    CommandOp *op = p->second;
    if (op->session) {
      ldout(cct, 10) << " pinging osd that serves command tid " << p->first << " (osd." << op->session->osd << ")" << dendl;
      toping.insert(op->session);
    } else {
      ldout(cct, 10) << " command tid " << p->first << " does not have session" << dendl;
    }
  }
  logger->set(l_osdc_op_laggy, laggy_ops);
  logger->set(l_osdc_osd_laggy, toping.size());

  if (num_homeless_ops || !toping.empty())
    maybe_request_map();

  if (!toping.empty()) {
    // send a ping to these osds, to ensure we detect any session resets
    // (osd reply message policy is lossy)
    for (set<OSDSession*>::iterator i = toping.begin();
	 i != toping.end();
	 ++i) {
      messenger->send_message(new MPing, (*i)->con);
    }
  }
    
  // reschedule
  schedule_tick();
}

void Objecter::resend_mon_ops()
{
  assert(client_lock.is_locked());
  ldout(cct, 10) << "resend_mon_ops" << dendl;

  for (map<ceph_tid_t,PoolStatOp*>::iterator p = poolstat_ops.begin(); p!=poolstat_ops.end(); ++p) {
    poolstat_submit(p->second);
    logger->inc(l_osdc_poolstat_resend);
  }

  for (map<ceph_tid_t,StatfsOp*>::iterator p = statfs_ops.begin(); p!=statfs_ops.end(); ++p) {
    fs_stats_submit(p->second);
    logger->inc(l_osdc_statfs_resend);
  }

  for (map<ceph_tid_t,PoolOp*>::iterator p = pool_ops.begin(); p!=pool_ops.end(); ++p) {
    _pool_op_submit(p->second);
    logger->inc(l_osdc_poolop_resend);
  }

  for (map<ceph_tid_t, Op*>::iterator p = check_latest_map_ops.begin();
       p != check_latest_map_ops.end();
       ++p) {
    C_Op_Map_Latest *c = new C_Op_Map_Latest(this, p->second->tid);
    monc->get_version("osdmap", &c->latest, NULL, c);
  }

  for (map<uint64_t, LingerOp*>::iterator p = check_latest_map_lingers.begin();
       p != check_latest_map_lingers.end();
       ++p) {
    C_Linger_Map_Latest *c = new C_Linger_Map_Latest(this, p->second->linger_id);
    monc->get_version("osdmap", &c->latest, NULL, c);
  }

  for (map<uint64_t, CommandOp*>::iterator p = check_latest_map_commands.begin();
       p != check_latest_map_commands.end();
       ++p) {
    C_Command_Map_Latest *c = new C_Command_Map_Latest(this, p->second->tid);
    monc->get_version("osdmap", &c->latest, NULL, c);
  }
}



// read | write ---------------------------

class C_CancelOp : public Context
{
  Objecter::Op *op;
  Objecter *objecter;
public:
  C_CancelOp(Objecter::Op *op, Objecter *objecter) : op(op),
						     objecter(objecter) {}
  void finish(int r) {
    // note that objecter lock == timer lock, and is already held
    objecter->op_cancel(op->tid, -ETIMEDOUT);
  }
};

ceph_tid_t Objecter::op_submit(Op *op, int *ctx_budget)
{
  assert(client_lock.is_locked());
  assert(initialized);

  assert(op->ops.size() == op->out_bl.size());
  assert(op->ops.size() == op->out_rval.size());
  assert(op->ops.size() == op->out_handler.size());

  if (osd_timeout > 0) {
    op->ontimeout = new C_CancelOp(op, this);
    timer.add_event_after(osd_timeout, op->ontimeout);
  }

  // throttle.  before we look at any state, because
  // take_op_budget() may drop our lock while it blocks.
  if (!op->ctx_budgeted || (ctx_budget && (*ctx_budget == -1))) {
    int op_budget = take_op_budget(op);
    // take and pass out the budget for the first OP
    // in the context session
    if (ctx_budget && (*ctx_budget == -1)) {
      *ctx_budget = op_budget;
    }
  }

  return _op_submit(op);
}

ceph_tid_t Objecter::_op_submit(Op *op)
{
  // pick tid if we haven't got one yet
  if (op->tid == ceph_tid_t(0)) {
    ceph_tid_t mytid = ++last_tid;
    op->tid = mytid;
  }
  assert(client_inc >= 0);

  // pick target
  num_homeless_ops++;  // initially; recalc_op_target() will decrement if it finds a target
  int r = recalc_op_target(op);
  bool check_for_latest_map = (r == RECALC_OP_TARGET_POOL_DNE);

  // add to gather set(s)
  if (op->onack) {
    ++num_unacked;
  } else {
    ldout(cct, 20) << " note: not requesting ack" << dendl;
  }
  if (op->oncommit) {
    ++num_uncommitted;
  } else {
    ldout(cct, 20) << " note: not requesting commit" << dendl;
  }
  ops[op->tid] = op;

  logger->set(l_osdc_op_active, ops.size());

  logger->inc(l_osdc_op);
  if ((op->target.flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE)) == (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE))
    logger->inc(l_osdc_op_rmw);
  else if (op->target.flags & CEPH_OSD_FLAG_WRITE)
    logger->inc(l_osdc_op_w);
  else if (op->target.flags & CEPH_OSD_FLAG_READ)
    logger->inc(l_osdc_op_r);

  if (op->target.flags & CEPH_OSD_FLAG_PGOP)
    logger->inc(l_osdc_op_pg);

  for (vector<OSDOp>::iterator p = op->ops.begin(); p != op->ops.end(); ++p) {
    int code = l_osdc_osdop_other;
    switch (p->op.op) {
    case CEPH_OSD_OP_STAT: code = l_osdc_osdop_stat; break;
    case CEPH_OSD_OP_CREATE: code = l_osdc_osdop_create; break;
    case CEPH_OSD_OP_READ: code = l_osdc_osdop_read; break;
    case CEPH_OSD_OP_WRITE: code = l_osdc_osdop_write; break;
    case CEPH_OSD_OP_WRITEFULL: code = l_osdc_osdop_writefull; break;
    case CEPH_OSD_OP_APPEND: code = l_osdc_osdop_append; break;
    case CEPH_OSD_OP_ZERO: code = l_osdc_osdop_zero; break;
    case CEPH_OSD_OP_TRUNCATE: code = l_osdc_osdop_truncate; break;
    case CEPH_OSD_OP_DELETE: code = l_osdc_osdop_delete; break;
    case CEPH_OSD_OP_MAPEXT: code = l_osdc_osdop_mapext; break;
    case CEPH_OSD_OP_SPARSE_READ: code = l_osdc_osdop_sparse_read; break;
    case CEPH_OSD_OP_CLONERANGE: code = l_osdc_osdop_clonerange; break;
    case CEPH_OSD_OP_GETXATTR: code = l_osdc_osdop_getxattr; break;
    case CEPH_OSD_OP_SETXATTR: code = l_osdc_osdop_setxattr; break;
    case CEPH_OSD_OP_CMPXATTR: code = l_osdc_osdop_cmpxattr; break;
    case CEPH_OSD_OP_RMXATTR: code = l_osdc_osdop_rmxattr; break;
    case CEPH_OSD_OP_RESETXATTRS: code = l_osdc_osdop_resetxattrs; break;
    case CEPH_OSD_OP_TMAPUP: code = l_osdc_osdop_tmap_up; break;
    case CEPH_OSD_OP_TMAPPUT: code = l_osdc_osdop_tmap_put; break;
    case CEPH_OSD_OP_TMAPGET: code = l_osdc_osdop_tmap_get; break;
    case CEPH_OSD_OP_CALL: code = l_osdc_osdop_call; break;
    case CEPH_OSD_OP_WATCH: code = l_osdc_osdop_watch; break;
    case CEPH_OSD_OP_NOTIFY: code = l_osdc_osdop_notify; break;
    case CEPH_OSD_OP_SRC_CMPXATTR: code = l_osdc_osdop_src_cmpxattr; break;
    }
    if (code)
      logger->inc(code);
  }

  // send?
  ldout(cct, 10) << "op_submit oid " << op->target.base_oid
           << " " << op->target.base_oloc << " " << op->target.target_oloc
	   << " " << op->ops << " tid " << op->tid
           << " osd." << (op->session ? op->session->osd : -1)
           << dendl;

  assert(op->target.flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE));

  if ((op->target.flags & CEPH_OSD_FLAG_WRITE) &&
      osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
    ldout(cct, 10) << " paused modify " << op << " tid " << last_tid << dendl;
    op->target.paused = true;
    maybe_request_map();
  } else if ((op->target.flags & CEPH_OSD_FLAG_READ) &&
	     osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
    ldout(cct, 10) << " paused read " << op << " tid " << last_tid << dendl;
    op->target.paused = true;
    maybe_request_map();
  } else if ((op->target.flags & CEPH_OSD_FLAG_WRITE) && osdmap_full_flag()) {
    ldout(cct, 0) << " FULL, paused modify " << op << " tid " << last_tid << dendl;
    op->target.paused = true;
    maybe_request_map();
  } else if (op->session) {
    send_op(op);
  } else {
    maybe_request_map();
  }

  if (check_for_latest_map) {
    _send_op_map_check(op);
  }

  ldout(cct, 5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;
  
  return op->tid;
}

int Objecter::op_cancel(ceph_tid_t tid, int r)
{
  assert(client_lock.is_locked());
  assert(initialized);

  map<ceph_tid_t, Op*>::iterator p = ops.find(tid);
  if (p == ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;
  Op *op = p->second;
  if (op->con) {
    ldout(cct, 20) << " revoking rx buffer for " << tid
		   << " on " << op->con << dendl;
    op->con->revoke_rx_buffer(tid);
  }
  if (op->onack) {
    op->onack->complete(r);
    op->onack = NULL;
  }
  if (op->oncommit) {
    op->oncommit->complete(r);
    op->oncommit = NULL;
  }
  op_cancel_map_check(op);
  finish_op(op);
  return 0;
}

bool Objecter::is_pg_changed(
  int oldprimary,
  const vector<int>& oldacting,
  int newprimary,
  const vector<int>& newacting,
  bool any_change)
{
  if (OSDMap::primary_changed(
	oldprimary,
	oldacting,
	newprimary,
	newacting))
    return true;
  if (any_change && oldacting != newacting)
    return true;
  return false;      // same primary (tho replicas may have changed)
}

bool Objecter::target_should_be_paused(op_target_t *t)
{
  bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || osdmap_full_flag();

  return (t->flags & CEPH_OSD_FLAG_READ && pauserd) ||
         (t->flags & CEPH_OSD_FLAG_WRITE && pausewr);
}


/**
 * Wrapper around osdmap->test_flag for special handling of the FULL flag.
 */
bool Objecter::osdmap_full_flag() const
{
  // Ignore the FULL flag if we are working on behalf of an MDS, in order to permit
  // MDS journal writes for file deletions.
  return osdmap->test_flag(CEPH_OSDMAP_FULL) && (messenger->get_myname().type() != entity_name_t::TYPE_MDS);
}


int64_t Objecter::get_object_hash_position(int64_t pool, const string& key,
					   const string& ns)
{
  const pg_pool_t *p = osdmap->get_pg_pool(pool);
  if (!p)
    return -ENOENT;
  return p->hash_key(key, ns);
}

int64_t Objecter::get_object_pg_hash_position(int64_t pool, const string& key,
					      const string& ns)
{
  const pg_pool_t *p = osdmap->get_pg_pool(pool);
  if (!p)
    return -ENOENT;
  return p->raw_hash_to_pg(p->hash_key(key, ns));
}

int Objecter::calc_target(op_target_t *t, epoch_t *last_force_resend, bool any_change)
{
  bool is_read = t->flags & CEPH_OSD_FLAG_READ;
  bool is_write = t->flags & CEPH_OSD_FLAG_WRITE;

  const pg_pool_t *pi = osdmap->get_pg_pool(t->base_oloc.pool);
  bool force_resend = false;
  bool need_check_tiering = false;

  if (pi && osdmap->get_epoch() == pi->last_force_op_resend) {
    if (last_force_resend && *last_force_resend < pi->last_force_op_resend) {
	*last_force_resend = pi->last_force_op_resend;
        force_resend = true;
    } else if (last_force_resend == 0)
      force_resend = true;
  }

  if (t->target_oid.name.empty() || force_resend) {
    t->target_oid = t->base_oid;
    need_check_tiering = true;
  }
  if (t->target_oloc.empty() || force_resend) {
    t->target_oloc = t->base_oloc;
    need_check_tiering = true;
  }
  
  if (need_check_tiering &&
      (t->flags & CEPH_OSD_FLAG_IGNORE_OVERLAY) == 0) {
    if (pi) {
      if (is_read && pi->has_read_tier())
	t->target_oloc.pool = pi->read_tier;
      if (is_write && pi->has_write_tier())
	t->target_oloc.pool = pi->write_tier;
    }
  }

  pg_t pgid;
  if (t->precalc_pgid) {
    assert(t->base_oid.name.empty()); // make sure this is a listing op
    ldout(cct, 10) << __func__ << " have " << t->base_pgid << " pool "
		   << osdmap->have_pg_pool(t->base_pgid.pool()) << dendl;
    if (!osdmap->have_pg_pool(t->base_pgid.pool()))
      return RECALC_OP_TARGET_POOL_DNE;
    pgid = osdmap->raw_pg_to_pg(t->base_pgid);
  } else {
    int ret = osdmap->object_locator_to_pg(t->target_oid, t->target_oloc,
					   pgid);
    if (ret == -ENOENT)
      return RECALC_OP_TARGET_POOL_DNE;
  }

  int min_size = pi->min_size;
  unsigned pg_num = pi->get_pg_num();
  int up_primary, acting_primary;
  vector<int> up, acting;
  osdmap->pg_to_up_acting_osds(pgid, &up, &up_primary,
			       &acting, &acting_primary);
  if (any_change && pg_interval_t::is_new_interval(
          t->acting_primary,
	  acting_primary,
	  t->acting,
	  acting,
	  t->up_primary,
	  up_primary,
	  t->up,
	  up,
	  t->min_size,
	  min_size,
	  t->pg_num,
	  pg_num,
	  pi->raw_pg_to_pg(pgid))) {
    force_resend = true;
  }

  bool need_resend = false;

  bool paused = target_should_be_paused(t);
  if (!paused && paused != t->paused) {
    t->paused = false;
    need_resend = true;
  }

  if (t->pgid != pgid ||
      is_pg_changed(
	t->acting_primary, t->acting, acting_primary, acting,
	t->used_replica || any_change) ||
      force_resend) {
    t->pgid = pgid;
    t->acting = acting;
    t->acting_primary = acting_primary;
    t->up_primary = up_primary;
    t->up = up;
    t->min_size = min_size;
    t->pg_num = pg_num;
    ldout(cct, 10) << __func__ << " "
		   << " pgid " << pgid << " acting " << acting << dendl;
    t->used_replica = false;
    if (acting_primary == -1) {
      t->osd = -1;
    } else {
      int osd;
      bool read = is_read && !is_write;
      if (read && (t->flags & CEPH_OSD_FLAG_BALANCE_READS)) {
	int p = rand() % acting.size();
	if (p)
	  t->used_replica = true;
	osd = acting[p];
	ldout(cct, 10) << " chose random osd." << osd << " of " << acting << dendl;
      } else if (read && (t->flags & CEPH_OSD_FLAG_LOCALIZE_READS) &&
		 acting.size() > 1) {
	// look for a local replica.  prefer the primary if the
	// distance is the same.
	int best = -1;
	int best_locality;
	for (unsigned i = 0; i < acting.size(); ++i) {
	  int locality = osdmap->crush->get_common_ancestor_distance(
		 cct, acting[i], crush_location);
	  ldout(cct, 20) << __func__ << " localize: rank " << i
			 << " osd." << acting[i]
			 << " locality " << locality << dendl;
	  if (i == 0 ||
	      (locality >= 0 && best_locality >= 0 &&
	       locality < best_locality) ||
	      (best_locality < 0 && locality >= 0)) {
	    best = i;
	    best_locality = locality;
	    if (i)
	      t->used_replica = true;
	  }
	}
	assert(best >= 0);
	osd = acting[best];
      } else {
	osd = acting_primary;
      }
      t->osd = osd;
    }
    need_resend = true;
  }
  if (need_resend) {
    return RECALC_OP_TARGET_NEED_RESEND;
  }
  return RECALC_OP_TARGET_NO_ACTION;
}

int Objecter::recalc_op_target(Op *op)
{
  int r = calc_target(&op->target, &op->last_force_resend);
  if (r == RECALC_OP_TARGET_NEED_RESEND) {
    OSDSession *s = NULL;
    if (op->target.osd >= 0)
      s = get_session(op->target.osd);
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
  }
  return r;
}

bool Objecter::recalc_linger_op_target(LingerOp *linger_op)
{
  int r = calc_target(&linger_op->target, &linger_op->last_force_resend, true);
  if (r == RECALC_OP_TARGET_NEED_RESEND) {
    ldout(cct, 10) << "recalc_linger_op_target tid " << linger_op->linger_id
		   << " pgid " << linger_op->target.pgid
		   << " acting " << linger_op->target.acting << dendl;
    
    OSDSession *s = linger_op->target.osd != -1 ?
      get_session(linger_op->target.osd) : NULL;
    if (linger_op->session != s) {
      linger_op->session_item.remove_myself();
      linger_op->session = s;
      if (s)
	s->linger_ops.push_back(&linger_op->session_item);
    }
  }
  return r;
}

void Objecter::cancel_linger_op(Op *op)
{
  ldout(cct, 15) << "cancel_op " << op->tid << dendl;

  assert(!op->should_resend);
  delete op->onack;
  delete op->oncommit;

  finish_op(op);
}

void Objecter::finish_op(Op *op)
{
  ldout(cct, 15) << "finish_op " << op->tid << dendl;

  op->session_item.remove_myself();
  if (!op->ctx_budgeted && op->budgeted)
    put_op_budget(op);

  ops.erase(op->tid);
  logger->set(l_osdc_op_active, ops.size());

  // our reply may have raced with pool deletion resulting in a map
  // check in flight.
  op_cancel_map_check(op);

  if (op->ontimeout)
    timer.cancel_event(op->ontimeout);

  delete op;
}

void Objecter::send_op(Op *op)
{
  ldout(cct, 15) << "send_op " << op->tid << " to osd." << op->session->osd << dendl;

  int flags = op->target.flags;
  if (op->oncommit)
    flags |= CEPH_OSD_FLAG_ONDISK;
  if (op->onack)
    flags |= CEPH_OSD_FLAG_ACK;

  assert(op->session->con);

  // preallocated rx buffer?
  if (op->con) {
    ldout(cct, 20) << " revoking rx buffer for " << op->tid << " on " << op->con << dendl;
    op->con->revoke_rx_buffer(op->tid);
  }
  if (op->outbl &&
      op->ontimeout == NULL &&  // only post rx_buffer if no timeout; see #9582
      op->outbl->length()) {
    ldout(cct, 20) << " posting rx buffer for " << op->tid << " on " << op->session->con << dendl;
    op->con = op->session->con;
    op->con->post_rx_buffer(op->tid, *op->outbl);
  }

  op->target.paused = false;
  op->incarnation = op->session->incarnation;
  op->stamp = ceph_clock_now(cct);

  MOSDOp *m = new MOSDOp(client_inc, op->tid, 
			 op->target.target_oid, op->target.target_oloc,
			 op->target.pgid,
			 osdmap->get_epoch(),
			 flags);

  m->set_snapid(op->snapid);
  m->set_snap_seq(op->snapc.seq);
  m->set_snaps(op->snapc.snaps);

  m->ops = op->ops;
  m->set_mtime(op->mtime);
  m->set_retry_attempt(op->attempts++);

  if (op->replay_version != eversion_t())
    m->set_version(op->replay_version);  // we're replaying this op!

  if (op->priority)
    m->set_priority(op->priority);
  else
    m->set_priority(cct->_conf->osd_client_op_priority);

  logger->inc(l_osdc_op_send);
  logger->inc(l_osdc_op_send_bytes, m->get_data().length());

  messenger->send_message(m, op->session->con);
}

int Objecter::calc_op_budget(Op *op)
{
  int op_budget = 0;
  for (vector<OSDOp>::iterator i = op->ops.begin();
       i != op->ops.end();
       ++i) {
    if (i->op.op & CEPH_OSD_OP_MODE_WR) {
      op_budget += i->indata.length();
    } else if (ceph_osd_op_mode_read(i->op.op)) {
      if (ceph_osd_op_type_data(i->op.op)) {
        if ((int64_t)i->op.extent.length > 0)
	  op_budget += (int64_t)i->op.extent.length;
      } else if (ceph_osd_op_type_attr(i->op.op)) {
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
  if (!op_throttle_bytes.get_or_fail(op_budget)) { //couldn't take right now
    client_lock.Unlock();
    op_throttle_bytes.get(op_budget);
    client_lock.Lock();
  }
  if (!op_throttle_ops.get_or_fail(1)) { //couldn't take right now
    client_lock.Unlock();
    op_throttle_ops.get(1);
    client_lock.Lock();
  }
}

void Objecter::unregister_op(Op *op)
{
  if (op->onack)
    num_unacked--;
  if (op->oncommit)
    num_uncommitted--;
  ops.erase(op->tid);
}

/* This function DOES put the passed message before returning */
void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  assert(client_lock.is_locked());
  assert(initialized);
  ldout(cct, 10) << "in handle_osd_op_reply" << dendl;

  // get pio
  ceph_tid_t tid = m->get_tid();

  if (ops.count(tid) == 0) {
    ldout(cct, 7) << "handle_osd_op_reply " << tid
	    << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
	    << " ... stray" << dendl;
    m->put();
    return;
  }

  ldout(cct, 7) << "handle_osd_op_reply " << tid
		<< (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
		<< " v " << m->get_replay_version() << " uv " << m->get_user_version()
		<< " in " << m->get_pg()
		<< " attempt " << m->get_retry_attempt()
		<< dendl;
  Op *op = ops[tid];

  if (m->get_retry_attempt() >= 0) {
    if (m->get_retry_attempt() != (op->attempts - 1)) {
      ldout(cct, 7) << " ignoring reply from attempt " << m->get_retry_attempt()
		    << " from " << m->get_source_inst()
		    << "; last attempt " << (op->attempts - 1) << " sent to "
		    << op->session->con->get_peer_addr() << dendl;
      m->put();
      return;
    }
  } else {
    // we don't know the request attempt because the server is old, so
    // just accept this one.  we may do ACK callbacks we shouldn't
    // have, but that is better than doing callbacks out of order.
  }

  Context *onack = 0;
  Context *oncommit = 0;

  int rc = m->get_result();

  if (m->is_redirect_reply()) {
    ldout(cct, 5) << " got redirect reply; redirecting" << dendl;
    unregister_op(op);
    m->get_redirect().combine_with_locator(op->target.target_oloc,
					   op->target.target_oid.name);
    _op_submit(op);
    m->put();
    return;
  }

  if (rc == -EAGAIN) {
    ldout(cct, 7) << " got -EAGAIN, resubmitting" << dendl;
    unregister_op(op);
    _op_submit(op);
    m->put();
    return;
  }

  if (op->objver)
    *op->objver = m->get_user_version();
  if (op->reply_epoch)
    *op->reply_epoch = m->get_map_epoch();

  // per-op result demuxing
  vector<OSDOp> out_ops;
  m->claim_ops(out_ops);
  
  if (out_ops.size() != op->ops.size())
    ldout(cct, 0) << "WARNING: tid " << op->tid << " reply ops " << out_ops
		  << " != request ops " << op->ops
		  << " from " << m->get_source_inst() << dendl;

  vector<bufferlist*>::iterator pb = op->out_bl.begin();
  vector<int*>::iterator pr = op->out_rval.begin();
  vector<Context*>::iterator ph = op->out_handler.begin();
  assert(op->out_bl.size() == op->out_rval.size());
  assert(op->out_bl.size() == op->out_handler.size());
  vector<OSDOp>::iterator p = out_ops.begin();
  for (unsigned i = 0;
       p != out_ops.end() && pb != op->out_bl.end();
       ++i, ++p, ++pb, ++pr, ++ph) {
    ldout(cct, 10) << " op " << i << " rval " << p->rval
		   << " len " << p->outdata.length() << dendl;
    if (*pb)
      **pb = p->outdata;
    // set rval before running handlers so that handlers
    // can change it if e.g. decoding fails
    if (*pr)
      **pr = p->rval;
    if (*ph) {
      ldout(cct, 10) << " op " << i << " handler " << *ph << dendl;
      (*ph)->complete(p->rval);
      *ph = NULL;
    }
  }

  // ack|commit -> ack
  if (op->onack) {
    ldout(cct, 15) << "handle_osd_op_reply ack" << dendl;
    op->replay_version = m->get_replay_version();
    onack = op->onack;
    op->onack = 0;  // only do callback once
    num_unacked--;
    logger->inc(l_osdc_op_ack);
  }
  if (op->oncommit && (m->is_ondisk() || rc)) {
    ldout(cct, 15) << "handle_osd_op_reply safe" << dendl;
    oncommit = op->oncommit;
    op->oncommit = 0;
    num_uncommitted--;
    logger->inc(l_osdc_op_commit);
  }

  // got data?
  if (op->outbl) {
    if (op->con)
      op->con->revoke_rx_buffer(op->tid);
    m->claim_data(*op->outbl);
    op->outbl = 0;
  }

  // done with this tid?
  if (!op->onack && !op->oncommit) {
    ldout(cct, 15) << "handle_osd_op_reply completed tid " << tid << dendl;
    finish_op(op);
  }
  
  ldout(cct, 5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;

  // do callbacks
  if (onack) {
    onack->complete(rc);
  }
  if (oncommit) {
    oncommit->complete(rc);
  }

  m->put();
}


uint32_t Objecter::list_objects_seek(ListContext *list_context,
				     uint32_t pos)
{
  assert(client_lock.is_locked());
  pg_t actual = osdmap->raw_pg_to_pg(pg_t(pos, list_context->pool_id));
  ldout(cct, 10) << "list_objects_seek " << list_context
		 << " pos " << pos << " -> " << actual << dendl;
  list_context->current_pg = actual.ps();
  list_context->cookie = collection_list_handle_t();
  list_context->at_end_of_pg = false;
  list_context->at_end_of_pool = false;
  list_context->current_pg_epoch = 0;
  return list_context->current_pg;
}

void Objecter::list_objects(ListContext *list_context, Context *onfinish)
{
  assert(client_lock.is_locked());
  ldout(cct, 10) << "list_objects" << dendl;
  ldout(cct, 20) << " pool_id " << list_context->pool_id
	   << " pool_snap_seq " << list_context->pool_snap_seq
	   << " max_entries " << list_context->max_entries
	   << " list_context " << list_context
	   << " onfinish " << onfinish
	   << " list_context->current_pg " << list_context->current_pg
	   << " list_context->cookie " << list_context->cookie << dendl;

  if (list_context->at_end_of_pg) {
    list_context->at_end_of_pg = false;
    ++list_context->current_pg;
    list_context->current_pg_epoch = 0;
    list_context->cookie = collection_list_handle_t();
    if (list_context->current_pg >= list_context->starting_pg_num) {
      list_context->at_end_of_pool = true;
      ldout(cct, 20) << " no more pgs; reached end of pool" << dendl;
    } else {
      ldout(cct, 20) << " move to next pg " << list_context->current_pg << dendl;
    }
  }
  if (list_context->at_end_of_pool) {
    // release the listing context's budget once all
    // OPs (in the session) are finished
    put_list_context_budget(list_context);

    onfinish->complete(0);
    return;
  }

  const pg_pool_t *pool = osdmap->get_pg_pool(list_context->pool_id);
  int pg_num = pool->get_pg_num();

  if (list_context->starting_pg_num == 0) {     // there can't be zero pgs!
    list_context->starting_pg_num = pg_num;
    ldout(cct, 20) << pg_num << " placement groups" << dendl;
  }
  if (list_context->starting_pg_num != pg_num) {
    // start reading from the beginning; the pgs have changed
    ldout(cct, 10) << " pg_num changed; restarting with " << pg_num << dendl;
    list_context->current_pg = 0;
    list_context->cookie = collection_list_handle_t();
    list_context->current_pg_epoch = 0;
    list_context->starting_pg_num = pg_num;
  }
  assert(list_context->current_pg <= pg_num);

  ObjectOperation op;
  op.pg_ls(list_context->max_entries, list_context->filter, list_context->cookie,
	   list_context->current_pg_epoch);
  list_context->bl.clear();
  C_List *onack = new C_List(list_context, onfinish, this);
  object_locator_t oloc(list_context->pool_id, list_context->nspace);
  pg_read(list_context->current_pg, oloc, op,
	  &list_context->bl, 0, onack, &onack->epoch, &list_context->ctx_budget);
}

void Objecter::_list_reply(ListContext *list_context, int r,
			   Context *final_finish, epoch_t reply_epoch)
{
  ldout(cct, 10) << "_list_reply" << dendl;

  bufferlist::iterator iter = list_context->bl.begin();
  pg_ls_response_t response;
  bufferlist extra_info;
  ::decode(response, iter);
  if (!iter.end()) {
    ::decode(extra_info, iter);
  }
  list_context->cookie = response.handle;
  if (!list_context->current_pg_epoch) {
    // first pgls result, set epoch marker
    ldout(cct, 20) << " first pgls piece, reply_epoch is "
		   << reply_epoch << dendl;
    list_context->current_pg_epoch = reply_epoch;
  }

  int response_size = response.entries.size();
  ldout(cct, 20) << " response.entries.size " << response_size
		 << ", response.entries " << response.entries << dendl;
  list_context->extra_info.append(extra_info);
  if (response_size) {
    list_context->list.merge(response.entries);
  }

  // if the osd returns 1 (newer code), or no entries, it means we
  // hit the end of the pg.
  if (response_size == 0 || r == 1) {
    ldout(cct, 20) << " at end of pg" << dendl;
    list_context->at_end_of_pg = true;
  } else {
    // there is more for this pg; get it?
    if (response_size < list_context->max_entries) {
      list_context->max_entries -= response_size;
      list_objects(list_context, final_finish);
      return;
    }
  }
  if (!list_context->list.empty()) {
    ldout(cct, 20) << " returning results so far" << dendl;
    // release the listing context's budget once all
    // OPs (in the session) are finished
    put_list_context_budget(list_context);
    final_finish->complete(0);
    return;
  }

  // continue!
  list_objects(list_context, final_finish);
}

void Objecter::put_list_context_budget(ListContext *list_context) {
    if (list_context->ctx_budget >= 0) {
      ldout(cct, 10) << " release listing context's budget " << list_context->ctx_budget << dendl;
      put_op_budget_bytes(list_context->ctx_budget);
      list_context->ctx_budget = -1;
    }
  }


//snapshots

int Objecter::create_pool_snap(int64_t pool, string& snap_name, Context *onfinish)
{
  ldout(cct, 10) << "create_pool_snap; pool: " << pool << "; snap: " << snap_name << dendl;

  const pg_pool_t *p = osdmap->get_pg_pool(pool);
  if (!p)
    return -EINVAL;
  if (p->snap_exists(snap_name.c_str()))
    return -EEXIST;

  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = snap_name;
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
    fin->complete(r);
  }
};

int Objecter::allocate_selfmanaged_snap(int64_t pool, snapid_t *psnapid,
					Context *onfinish)
{
  ldout(cct, 10) << "allocate_selfmanaged_snap; pool: " << pool << dendl;
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

int Objecter::delete_pool_snap(int64_t pool, string& snap_name, Context *onfinish)
{
  ldout(cct, 10) << "delete_pool_snap; pool: " << pool << "; snap: " << snap_name << dendl;

  const pg_pool_t *p = osdmap->get_pg_pool(pool);
  if (!p)
    return -EINVAL;
  if (!p->snap_exists(snap_name.c_str()))
    return -ENOENT;

  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = snap_name;
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_DELETE_SNAP;
  pool_ops[op->tid] = op;
  
  pool_op_submit(op);
  
  return 0;
}

int Objecter::delete_selfmanaged_snap(int64_t pool, snapid_t snap,
				      Context *onfinish) {
  ldout(cct, 10) << "delete_selfmanaged_snap; pool: " << pool << "; snap: " 
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
  ldout(cct, 10) << "create_pool name=" << name << dendl;

  if (osdmap->lookup_pg_pool_name(name.c_str()) >= 0)
    return -EEXIST;

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

int Objecter::delete_pool(int64_t pool, Context *onfinish)
{
  ldout(cct, 10) << "delete_pool " << pool << dendl;

  if (!osdmap->have_pg_pool(pool))
    return -ENOENT;

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
int Objecter::change_pool_auid(int64_t pool, Context *onfinish, uint64_t auid)
{
  ldout(cct, 10) << "change_pool_auid " << pool << " to " << auid << dendl;
  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = "change_pool_auid";
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_AUID_CHANGE;
  op->auid = auid;
  pool_ops[op->tid] = op;

  logger->set(l_osdc_poolop_active, pool_ops.size());

  pool_op_submit(op);
  return 0;
}

class C_CancelPoolOp : public Context
{
  ceph_tid_t tid;
  Objecter *objecter;
public:
  C_CancelPoolOp(ceph_tid_t tid, Objecter *objecter) : tid(tid),
						       objecter(objecter) {}
  void finish(int r) {
    // note that objecter lock == timer lock, and is already held
    objecter->pool_op_cancel(tid, -ETIMEDOUT);
  }
};

void Objecter::pool_op_submit(PoolOp *op)
{
  if (mon_timeout > 0) {
    op->ontimeout = new C_CancelPoolOp(op->tid, this);
    timer.add_event_after(mon_timeout, op->ontimeout);
  }
  _pool_op_submit(op);
}

void Objecter::_pool_op_submit(PoolOp *op)
{
  ldout(cct, 10) << "pool_op_submit " << op->tid << dendl;
  MPoolOp *m = new MPoolOp(monc->get_fsid(), op->tid, op->pool,
			   op->name, op->pool_op,
			   op->auid, last_seen_osdmap_version);
  if (op->snapid) m->snapid = op->snapid;
  if (op->crush_rule) m->crush_rule = op->crush_rule;
  monc->send_mon_message(m);
  op->last_submit = ceph_clock_now(cct);

  logger->inc(l_osdc_poolop_send);
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
  assert(client_lock.is_locked());
  assert(initialized);
  ldout(cct, 10) << "handle_pool_op_reply " << *m << dendl;
  ceph_tid_t tid = m->get_tid();
  if (pool_ops.count(tid)) {
    PoolOp *op = pool_ops[tid];
    ldout(cct, 10) << "have request " << tid << " at " << op << " Op: " << ceph_pool_op_name(op->pool_op) << dendl;
    if (op->blp)
      op->blp->claim(m->response_data);
    if (m->version > last_seen_osdmap_version)
      last_seen_osdmap_version = m->version;
    if (osdmap->get_epoch() < m->epoch) {
      ldout(cct, 20) << "waiting for client to reach epoch " << m->epoch << " before calling back" << dendl;
      wait_for_new_map(op->onfinish, m->epoch, m->replyCode);
    }
    else {
      op->onfinish->complete(m->replyCode);
    }
    op->onfinish = NULL;
    finish_pool_op(op);
  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  }
  ldout(cct, 10) << "done" << dendl;
  m->put();
}

int Objecter::pool_op_cancel(ceph_tid_t tid, int r)
{
  assert(client_lock.is_locked());
  assert(initialized);

  map<ceph_tid_t, PoolOp*>::iterator it = pool_ops.find(tid);
  if (it == pool_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  PoolOp *op = it->second;
  if (op->onfinish)
    op->onfinish->complete(r);
  finish_pool_op(op);
  return 0;
}

void Objecter::finish_pool_op(PoolOp *op)
{
  pool_ops.erase(op->tid);
  logger->set(l_osdc_poolop_active, pool_ops.size());

  if (op->ontimeout)
    timer.cancel_event(op->ontimeout);

  delete op;
}

// pool stats

class C_CancelPoolStatOp : public Context
{
  ceph_tid_t tid;
  Objecter *objecter;
public:
  C_CancelPoolStatOp(ceph_tid_t tid, Objecter *objecter) : tid(tid),
						           objecter(objecter) {}
  void finish(int r) {
    // note that objecter lock == timer lock, and is already held
    objecter->pool_stat_op_cancel(tid, -ETIMEDOUT);
  }
};

void Objecter::get_pool_stats(list<string>& pools, map<string,pool_stat_t> *result,
			      Context *onfinish)
{
  ldout(cct, 10) << "get_pool_stats " << pools << dendl;

  PoolStatOp *op = new PoolStatOp;
  op->tid = ++last_tid;
  op->pools = pools;
  op->pool_stats = result;
  op->onfinish = onfinish;
  op->ontimeout = NULL;
  if (mon_timeout > 0) {
    op->ontimeout = new C_CancelPoolStatOp(op->tid, this);
    timer.add_event_after(mon_timeout, op->ontimeout);
  }
  poolstat_ops[op->tid] = op;

  logger->set(l_osdc_poolstat_active, poolstat_ops.size());

  poolstat_submit(op);
}

void Objecter::poolstat_submit(PoolStatOp *op)
{
  ldout(cct, 10) << "poolstat_submit " << op->tid << dendl;
  monc->send_mon_message(new MGetPoolStats(monc->get_fsid(), op->tid, op->pools, last_seen_pgmap_version));
  op->last_submit = ceph_clock_now(cct);

  logger->inc(l_osdc_poolstat_send);
}

void Objecter::handle_get_pool_stats_reply(MGetPoolStatsReply *m)
{
  assert(client_lock.is_locked());
  assert(initialized);
  ldout(cct, 10) << "handle_get_pool_stats_reply " << *m << dendl;
  ceph_tid_t tid = m->get_tid();

  if (poolstat_ops.count(tid)) {
    PoolStatOp *op = poolstat_ops[tid];
    ldout(cct, 10) << "have request " << tid << " at " << op << dendl;
    *op->pool_stats = m->pool_stats;
    if (m->version > last_seen_pgmap_version)
      last_seen_pgmap_version = m->version;
    op->onfinish->complete(0);
    finish_pool_stat_op(op);
  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  } 
  ldout(cct, 10) << "done" << dendl;
  m->put();
}

int Objecter::pool_stat_op_cancel(ceph_tid_t tid, int r)
{
  assert(client_lock.is_locked());
  assert(initialized);

  map<ceph_tid_t, PoolStatOp*>::iterator it = poolstat_ops.find(tid);
  if (it == poolstat_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  PoolStatOp *op = it->second;
  if (op->onfinish)
    op->onfinish->complete(r);
  finish_pool_stat_op(op);
  return 0;
}

void Objecter::finish_pool_stat_op(PoolStatOp *op)
{
  poolstat_ops.erase(op->tid);
  logger->set(l_osdc_poolstat_active, poolstat_ops.size());

  if (op->ontimeout)
    timer.cancel_event(op->ontimeout);

  delete op;
}

class C_CancelStatfsOp : public Context
{
  ceph_tid_t tid;
  Objecter *objecter;
public:
  C_CancelStatfsOp(ceph_tid_t tid, Objecter *objecter) : tid(tid),
							 objecter(objecter) {}
  void finish(int r) {
    // note that objecter lock == timer lock, and is already held
    objecter->statfs_op_cancel(tid, -ETIMEDOUT);
  }
};

void Objecter::get_fs_stats(ceph_statfs& result, Context *onfinish)
{
  ldout(cct, 10) << "get_fs_stats" << dendl;

  StatfsOp *op = new StatfsOp;
  op->tid = ++last_tid;
  op->stats = &result;
  op->onfinish = onfinish;
  op->ontimeout = NULL;
  if (mon_timeout > 0) {
    op->ontimeout = new C_CancelStatfsOp(op->tid, this);
    timer.add_event_after(mon_timeout, op->ontimeout);
  }
  statfs_ops[op->tid] = op;

  logger->set(l_osdc_statfs_active, statfs_ops.size());

  fs_stats_submit(op);
}

void Objecter::fs_stats_submit(StatfsOp *op)
{
  ldout(cct, 10) << "fs_stats_submit" << op->tid << dendl;
  monc->send_mon_message(new MStatfs(monc->get_fsid(), op->tid, last_seen_pgmap_version));
  op->last_submit = ceph_clock_now(cct);

  logger->inc(l_osdc_statfs_send);
}

void Objecter::handle_fs_stats_reply(MStatfsReply *m)
{
  assert(client_lock.is_locked());
  assert(initialized);
  ldout(cct, 10) << "handle_fs_stats_reply " << *m << dendl;
  ceph_tid_t tid = m->get_tid();

  if (statfs_ops.count(tid)) {
    StatfsOp *op = statfs_ops[tid];
    ldout(cct, 10) << "have request " << tid << " at " << op << dendl;
    *(op->stats) = m->h.st;
    if (m->h.version > last_seen_pgmap_version)
      last_seen_pgmap_version = m->h.version;
    op->onfinish->complete(0);
    finish_statfs_op(op);
  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  }
  ldout(cct, 10) << "done" << dendl;
  m->put();
}

int Objecter::statfs_op_cancel(ceph_tid_t tid, int r)
{
  assert(client_lock.is_locked());
  assert(initialized);

  map<ceph_tid_t, StatfsOp*>::iterator it = statfs_ops.find(tid);
  if (it == statfs_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  StatfsOp *op = it->second;
  if (op->onfinish)
    op->onfinish->complete(r);
  finish_statfs_op(op);
  return 0;
}

void Objecter::finish_statfs_op(StatfsOp *op)
{
  statfs_ops.erase(op->tid);
  logger->set(l_osdc_statfs_active, statfs_ops.size());

  if (op->ontimeout)
    timer.cancel_event(op->ontimeout);

  delete op;
}

// scatter/gather

void Objecter::_sg_read_finish(vector<ObjectExtent>& extents, vector<bufferlist>& resultbl, 
			       bufferlist *bl, Context *onfinish)
{
  // all done
  ldout(cct, 15) << "_sg_read_finish" << dendl;

  if (extents.size() > 1) {
    Striper::StripedReadResult r;
    vector<bufferlist>::iterator bit = resultbl.begin();
    for (vector<ObjectExtent>::iterator eit = extents.begin();
	 eit != extents.end();
	 ++eit, ++bit) {
      r.add_partial_result(cct, *bit, eit->buffer_extents);
    }
    bl->clear();
    r.assemble_result(cct, *bl, false);
  } else {
    ldout(cct, 15) << "  only one frag" << dendl;
    bl->claim(resultbl[0]);
  }

  // done
  uint64_t bytes_read = bl->length();
  ldout(cct, 7) << "_sg_read_finish " << bytes_read << " bytes" << dendl;

  if (onfinish) {
    onfinish->complete(bytes_read);// > 0 ? bytes_read:m->get_result());
  }
}


void Objecter::ms_handle_connect(Connection *con)
{
  ldout(cct, 10) << "ms_handle_connect " << con << dendl;
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
    resend_mon_ops();
}

void Objecter::ms_handle_reset(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
    //
    int osd = osdmap->identify_osd(con->get_peer_addr());
    if (osd >= 0) {
      ldout(cct, 1) << "ms_handle_reset on osd." << osd << dendl;
      map<int,OSDSession*>::iterator p = osd_sessions.find(osd);
      if (p != osd_sessions.end()) {
	OSDSession *session = p->second;
	reopen_session(session);
	kick_requests(session);
	maybe_request_map();
      }
    } else {
      ldout(cct, 10) << "ms_handle_reset on unknown osd addr " << con->get_peer_addr() << dendl;
    }
  }
}

void Objecter::ms_handle_remote_reset(Connection *con)
{
  /*
   * treat these the same.
   */
  ms_handle_reset(con);
}


void Objecter::op_target_t::dump(Formatter *f) const
{
  f->dump_stream("pg") << pgid;
  f->dump_int("osd", osd);
  f->dump_stream("object_id") << base_oid;
  f->dump_stream("object_locator") << base_oloc;
  f->dump_stream("target_object_id") << target_oid;
  f->dump_stream("target_object_locator") << target_oloc;
  f->dump_int("paused", (int)paused);
  f->dump_int("used_replica", (int)used_replica);
  f->dump_int("precalc_pgid", (int)precalc_pgid);
}

void Objecter::dump_active()
{
  ldout(cct, 20) << "dump_active .. " << num_homeless_ops << " homeless" << dendl;
  for (map<ceph_tid_t,Op*>::iterator p = ops.begin(); p != ops.end(); ++p) {
    Op *op = p->second;
    ldout(cct, 20) << op->tid << "\t" << op->target.pgid
		   << "\tosd." << (op->session ? op->session->osd : -1)
		   << "\t" << op->target.base_oid
		   << "\t" << op->ops << dendl;
  }
}

void Objecter::dump_requests(Formatter *fmt) const
{
  assert(client_lock.is_locked());

  fmt->open_object_section("requests");
  dump_ops(fmt);
  dump_linger_ops(fmt);
  dump_pool_ops(fmt);
  dump_pool_stat_ops(fmt);
  dump_statfs_ops(fmt);
  dump_command_ops(fmt);
  fmt->close_section(); // requests object
}

void Objecter::dump_ops(Formatter *fmt) const
{
  fmt->open_array_section("ops");
  for (map<ceph_tid_t,Op*>::const_iterator p = ops.begin();
       p != ops.end();
       ++p) {
    Op *op = p->second;
    fmt->open_object_section("op");
    fmt->dump_unsigned("tid", op->tid);
    op->target.dump(fmt);
    fmt->dump_stream("last_sent") << op->stamp;
    fmt->dump_int("attempts", op->attempts);
    fmt->dump_stream("snapid") << op->snapid;
    fmt->dump_stream("snap_context") << op->snapc;
    fmt->dump_stream("mtime") << op->mtime;

    fmt->open_array_section("osd_ops");
    for (vector<OSDOp>::const_iterator it = op->ops.begin();
	 it != op->ops.end();
	 ++it) {
      fmt->dump_stream("osd_op") << *it;
    }
    fmt->close_section(); // osd_ops array

    fmt->close_section(); // op object
  }
  fmt->close_section(); // ops array
}

void Objecter::dump_linger_ops(Formatter *fmt) const
{
  fmt->open_array_section("linger_ops");
  for (map<uint64_t, LingerOp*>::const_iterator p = linger_ops.begin();
       p != linger_ops.end();
       ++p) {
    LingerOp *op = p->second;
    fmt->open_object_section("linger_op");
    fmt->dump_unsigned("linger_id", op->linger_id);
    op->target.dump(fmt);
    fmt->dump_stream("snapid") << op->snap;
    fmt->dump_stream("registered") << op->registered;
    fmt->close_section(); // linger_op object
  }
  fmt->close_section(); // linger_ops array
}

void Objecter::dump_command_ops(Formatter *fmt) const
{
  fmt->open_array_section("command_ops");
  for (map<uint64_t, CommandOp*>::const_iterator p = command_ops.begin();
       p != command_ops.end();
       ++p) {
    CommandOp *op = p->second;
    fmt->open_object_section("command_op");
    fmt->dump_unsigned("command_id", op->tid);
    fmt->dump_int("osd", op->session ? op->session->osd : -1);
    fmt->open_array_section("command");
    for (vector<string>::const_iterator q = op->cmd.begin(); q != op->cmd.end(); ++q)
      fmt->dump_string("word", *q);
    fmt->close_section();
    if (op->target_osd >= 0)
      fmt->dump_int("target_osd", op->target_osd);
    else
      fmt->dump_stream("target_pg") << op->target_pg;
    fmt->close_section(); // command_op object
  }
  fmt->close_section(); // command_ops array
}

void Objecter::dump_pool_ops(Formatter *fmt) const
{
  fmt->open_array_section("pool_ops");
  for (map<ceph_tid_t, PoolOp*>::const_iterator p = pool_ops.begin();
       p != pool_ops.end();
       ++p) {
    PoolOp *op = p->second;
    fmt->open_object_section("pool_op");
    fmt->dump_unsigned("tid", op->tid);
    fmt->dump_int("pool", op->pool);
    fmt->dump_string("name", op->name);
    fmt->dump_int("operation_type", op->pool_op);
    fmt->dump_unsigned("auid", op->auid);
    fmt->dump_unsigned("crush_rule", op->crush_rule);
    fmt->dump_stream("snapid") << op->snapid;
    fmt->dump_stream("last_sent") << op->last_submit;
    fmt->close_section(); // pool_op object
  }
  fmt->close_section(); // pool_ops array
}

void Objecter::dump_pool_stat_ops(Formatter *fmt) const
{
  fmt->open_array_section("pool_stat_ops");
  for (map<ceph_tid_t, PoolStatOp*>::const_iterator p = poolstat_ops.begin();
       p != poolstat_ops.end();
       ++p) {
    PoolStatOp *op = p->second;
    fmt->open_object_section("pool_stat_op");
    fmt->dump_unsigned("tid", op->tid);
    fmt->dump_stream("last_sent") << op->last_submit;

    fmt->open_array_section("pools");
    for (list<string>::const_iterator it = op->pools.begin();
	 it != op->pools.end();
	 ++it) {
      fmt->dump_string("pool", *it);
    }
    fmt->close_section(); // pool_op object

    fmt->close_section(); // pool_stat_op object
  }
  fmt->close_section(); // pool_stat_ops array
}

void Objecter::dump_statfs_ops(Formatter *fmt) const
{
  fmt->open_array_section("statfs_ops");
  for (map<ceph_tid_t, StatfsOp*>::const_iterator p = statfs_ops.begin();
       p != statfs_ops.end();
       ++p) {
    StatfsOp *op = p->second;
    fmt->open_object_section("statfs_op");
    fmt->dump_unsigned("tid", op->tid);
    fmt->dump_stream("last_sent") << op->last_submit;
    fmt->close_section(); // pool_stat_op object
  }
  fmt->close_section(); // pool_stat_ops array
}

Objecter::RequestStateHook::RequestStateHook(Objecter *objecter) :
  m_objecter(objecter)
{
}

bool Objecter::RequestStateHook::call(std::string command, cmdmap_t& cmdmap,
				      std::string format, bufferlist& out)
{
  Formatter *f = new_formatter(format);
  if (!f)
    f = new_formatter("json-pretty");
  m_objecter->client_lock.Lock();
  m_objecter->dump_requests(f);
  m_objecter->client_lock.Unlock();
  f->flush(out);
  delete f;
  return true;
}

void Objecter::blacklist_self(bool set)
{
  ldout(cct, 10) << "blacklist_self " << (set ? "add" : "rm") << dendl;

  vector<string> cmd;
  cmd.push_back("{\"prefix\":\"osd blacklist\", ");
  if (set)
    cmd.push_back("\"blacklistop\":\"add\","); 
  else
    cmd.push_back("\"blacklistop\":\"rm\",");
  stringstream ss;
  ss << messenger->get_myaddr();
  cmd.push_back("\"addr\":\"" + ss.str() + "\"");

  MMonCommand *m = new MMonCommand(monc->get_fsid());
  m->cmd = cmd;

  monc->send_mon_message(m);
}

// commands

void Objecter::handle_command_reply(MCommandReply *m)
{
  map<ceph_tid_t,CommandOp*>::iterator p = command_ops.find(m->get_tid());
  if (p == command_ops.end()) {
    ldout(cct, 10) << "handle_command_reply tid " << m->get_tid() << " not found" << dendl;
    m->put();
    return;
  }

  CommandOp *c = p->second;
  if (!c->session ||
      m->get_connection() != c->session->con) {
    ldout(cct, 10) << "handle_command_reply tid " << m->get_tid() << " got reply from wrong connection "
		   << m->get_connection() << " " << m->get_source_inst() << dendl;
    m->put();
    return;
  }
  if (c->poutbl)
    c->poutbl->claim(m->get_data());
  _finish_command(c, m->r, m->rs);
  m->put();
}

class C_CancelCommandOp : public Context
{
  ceph_tid_t tid;
  Objecter *objecter;
public:
  C_CancelCommandOp(ceph_tid_t tid, Objecter *objecter) : tid(tid),
						          objecter(objecter) {}
  void finish(int r) {
    // note that objecter lock == timer lock, and is already held
    objecter->command_op_cancel(tid, -ETIMEDOUT);
  }
};

int Objecter::_submit_command(CommandOp *c, ceph_tid_t *ptid)
{
  ceph_tid_t tid = ++last_tid;
  ldout(cct, 10) << "_submit_command " << tid << " " << c->cmd << dendl;
  c->tid = tid;
  if (osd_timeout > 0) {
    c->ontimeout = new C_CancelCommandOp(tid, this);
    timer.add_event_after(osd_timeout, c->ontimeout);
  }
  command_ops[tid] = c;
  num_homeless_ops++;
  (void)recalc_command_target(c);

  if (c->session)
    _send_command(c);
  else
    maybe_request_map();
  if (c->map_check_error)
    _send_command_map_check(c);
  *ptid = tid;

  logger->set(l_osdc_command_active, command_ops.size());
  return 0;
}

int Objecter::recalc_command_target(CommandOp *c)
{
  OSDSession *s = NULL;
  c->map_check_error = 0;
  if (c->target_osd >= 0) {
    if (!osdmap->exists(c->target_osd)) {
      c->map_check_error = -ENOENT;
      c->map_check_error_str = "osd dne";
      return RECALC_OP_TARGET_OSD_DNE;
    }
    if (osdmap->is_down(c->target_osd)) {
      c->map_check_error = -ENXIO;
      c->map_check_error_str = "osd down";
      return RECALC_OP_TARGET_OSD_DOWN;
    }
    s = get_session(c->target_osd);
  } else {
    if (!osdmap->have_pg_pool(c->target_pg.pool())) {
      c->map_check_error = -ENOENT;
      c->map_check_error_str = "pool dne";
      return RECALC_OP_TARGET_POOL_DNE;
    }
    int primary;
    vector<int> acting;
    osdmap->pg_to_acting_osds(c->target_pg, &acting, &primary);
    if (primary != -1)
      s = get_session(primary);
  }
  if (c->session != s) {
    ldout(cct, 10) << "recalc_command_target " << c->tid << " now " << c->session << dendl;
    if (s) {
      if (!c->session)
	num_homeless_ops--;
      c->session = s;
      s->command_ops.push_back(&c->session_item);
    } else {
      c->session = NULL;
      num_homeless_ops++;
    }
    return RECALC_OP_TARGET_NEED_RESEND;
  }
  ldout(cct, 20) << "recalc_command_target " << c->tid << " no change, " << c->session << dendl;
  return RECALC_OP_TARGET_NO_ACTION;
}

void Objecter::_send_command(CommandOp *c)
{
  ldout(cct, 10) << "_send_command " << c->tid << dendl;
  assert(c->session);
  assert(c->session->con);
  MCommand *m = new MCommand(monc->monmap.fsid);
  m->cmd = c->cmd;
  m->set_data(c->inbl);
  m->set_tid(c->tid);
  messenger->send_message(m, c->session->con);
  logger->inc(l_osdc_command_send);
}

int Objecter::command_op_cancel(ceph_tid_t tid, int r)
{
  assert(client_lock.is_locked());
  assert(initialized);

  map<ceph_tid_t, CommandOp*>::iterator it = command_ops.find(tid);
  if (it == command_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  CommandOp *op = it->second;
  command_cancel_map_check(op);
  _finish_command(op, -ETIMEDOUT, "");
  return 0;
}

void Objecter::_finish_command(CommandOp *c, int r, string rs)
{
  ldout(cct, 10) << "_finish_command " << c->tid << " = " << r << " " << rs << dendl;
  c->session_item.remove_myself();
  if (c->prs)
    *c->prs = rs;
  if (c->onfinish)
    c->onfinish->complete(r);
  command_ops.erase(c->tid);
  if (c->ontimeout)
    timer.cancel_event(c->ontimeout);
  c->put();

  logger->set(l_osdc_command_active, command_ops.size());
}
