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
  l_osdc_linger_ping,

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

Mutex *Objecter::OSDSession::get_lock(object_t& oid)
{
#define HASH_PRIME 1021
  uint32_t h = ceph_str_hash_linux(oid.name.c_str(), oid.name.size()) % HASH_PRIME;

  return completion_locks[h % num_locks];
}

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

/*
 * initialize only internal data structures, don't initiate cluster interaction
 */
void Objecter::init()
{
  assert(!initialized.read());

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
    pcb.add_u64_counter(l_osdc_linger_ping, "linger_ping");

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

  /* Don't warn on EEXIST, happens if multiple ceph clients
   * are instantiated from one process */
  if (ret < 0 && ret != -EEXIST) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(ret) << dendl;
  }

  timer_lock.Lock();
  timer.init();
  timer_lock.Unlock();

  initialized.set(1);
}

/*
 * ok, cluster interaction can happen
 */
void Objecter::start()
{
  RWLock::RLocker rl(rwlock);

  schedule_tick();
  if (osdmap->get_epoch() == 0) {
    int r = _maybe_request_map();
    assert (r == 0 || osdmap->get_epoch() > 0);
  }
}

void Objecter::shutdown()
{
  assert(initialized.read());

  rwlock.get_write();

  initialized.set(0);

  map<int,OSDSession*>::iterator p;
  while (!osd_sessions.empty()) {
    p = osd_sessions.begin();
    close_session(p->second);
  }

  while(!check_latest_map_lingers.empty()) {
    map<uint64_t, LingerOp*>::iterator i = check_latest_map_lingers.begin();
    i->second->put();
    check_latest_map_lingers.erase(i->first);
  }

  while(!check_latest_map_ops.empty()) {
    map<ceph_tid_t, Op*>::iterator i = check_latest_map_ops.begin();
    i->second->put();
    check_latest_map_ops.erase(i->first);
  }

  while(!check_latest_map_commands.empty()) {
    map<ceph_tid_t, CommandOp*>::iterator i = check_latest_map_commands.begin();
    i->second->put();
    check_latest_map_commands.erase(i->first);
  }

  while(!poolstat_ops.empty()) {
    map<ceph_tid_t,PoolStatOp*>::iterator i = poolstat_ops.begin();
    delete i->second;
    poolstat_ops.erase(i->first);
  }

  while(!statfs_ops.empty()) {
    map<ceph_tid_t, StatfsOp*>::iterator i = statfs_ops.begin();
    delete i->second;
    statfs_ops.erase(i->first);
  }

  while(!pool_ops.empty()) {
    map<ceph_tid_t, PoolOp*>::iterator i = pool_ops.begin();
    delete i->second;
    pool_ops.erase(i->first);
  }

  ldout(cct, 20) << __func__ << " clearing up homeless session..." << dendl;
  while(!homeless_session->linger_ops.empty()) {
    std::map<uint64_t, LingerOp*>::iterator i = homeless_session->linger_ops.begin();
    ldout(cct, 10) << " linger_op " << i->first << dendl;
    LingerOp *lop = i->second;
    {
      RWLock::WLocker wl(homeless_session->lock);
      _session_linger_op_remove(homeless_session, lop);
    }
    linger_ops.erase(lop->linger_id);
    lop->put();
  }

  while(!homeless_session->ops.empty()) {
    std::map<ceph_tid_t, Op*>::iterator i = homeless_session->ops.begin();
    ldout(cct, 10) << " op " << i->first << dendl;
    Op *op = i->second;
    {
      RWLock::WLocker wl(homeless_session->lock);
      _session_op_remove(homeless_session, op);
    }
    op->put();
  }

  while(!homeless_session->command_ops.empty()) {
    std::map<ceph_tid_t, CommandOp*>::iterator i = homeless_session->command_ops.begin();
    ldout(cct, 10) << " command_op " << i->first << dendl;
    CommandOp *cop = i->second;
    {
      RWLock::WLocker wl(homeless_session->lock);
      _session_command_op_remove(homeless_session, cop);
    }
    cop->put();
  }

  if (tick_event) {
    Mutex::Locker l(timer_lock);
    if (timer.cancel_event(tick_event)) {
      ldout(cct, 10) <<  " successfully canceled tick" << dendl;
      tick_event = NULL;
    }
  }

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

  // Let go of Objecter write lock so timer thread can shutdown
  rwlock.unlock();

  {
    Mutex::Locker l(timer_lock);
    timer.shutdown();
  }

  assert(tick_event == NULL);
}

void Objecter::_send_linger(LingerOp *info)
{
  assert(rwlock.is_wlocked());

  RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

  vector<OSDOp> opv;
  Context *onack = NULL;
  if (info->registered) {
    ldout(cct, 15) << "send_linger " << info->linger_id << " reconnect" << dendl;
    onack = new C_Linger_Reconnect(this, info);
    opv.push_back(OSDOp());
    opv.back().op.op = CEPH_OSD_OP_WATCH;
    opv.back().op.watch.cookie = info->cookie;
    opv.back().op.watch.op = CEPH_OSD_WATCH_OP_RECONNECT;
  } else {
    ldout(cct, 15) << "send_linger " << info->linger_id << " register" << dendl;
    onack = new C_Linger_Register(this, info);
    opv = info->ops;
  }
  Context *oncommit = new C_Linger_Commit(this, info);
  Op *o = new Op(info->target.base_oid, info->target.base_oloc,
		 opv, info->target.flags | CEPH_OSD_FLAG_READ,
		 onack, oncommit,
		 info->pobjver);
  o->snapid = info->snap;
  o->snapc = info->snapc;
  o->mtime = info->mtime;

  o->target = info->target;
  o->tid = last_tid.inc();

  // do not resend this; we will send a new op to reregister
  o->should_resend = false;

  if (info->register_tid) {
    // repeat send.  cancel old registeration op, if any.
    info->session->lock.get_write();
    if (info->session->ops.count(info->register_tid)) {
      Op *o = info->session->ops[info->register_tid];
      _op_cancel_map_check(o);
      _cancel_linger_op(o);
    }
    info->session->lock.unlock();

    info->register_tid = _op_submit(o, lc);
  } else {
    // first send
    info->register_tid = _op_submit_with_budget(o, lc);
  }

  logger->inc(l_osdc_linger_send);
}

void Objecter::_linger_register(LingerOp *info, int r)
{
  ldout(cct, 10) << "_linger_register " << info->linger_id << dendl;
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

void Objecter::_linger_reconnect(LingerOp *info, int r)
{
  ldout(cct, 10) << __func__ << " " << info->linger_id << " = " << r
		 << " (last_error " << info->last_error << ")" << dendl;
  if (r < 0) {
    info->watch_lock.Lock();
    info->last_error = r;
    info->watch_cond.Signal();
    if (info->on_error)
      info->on_error->complete(r);
    info->watch_lock.Unlock();
  }
}

void Objecter::_send_linger_ping(LingerOp *info)
{
  assert(rwlock.is_locked());
  assert(info->session->lock.is_locked());

  if (cct->_conf->objecter_inject_no_watch_ping) {
    ldout(cct, 10) << __func__ << " " << info->linger_id << " SKIPPING" << dendl;
    return;
  }
  if (osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
    ldout(cct, 10) << __func__ << " PAUSERD" << dendl;
    return;
  }

  utime_t now = ceph_clock_now(NULL);
  ldout(cct, 10) << __func__ << " " << info->linger_id << " now " << now << dendl;

  RWLock::Context lc(rwlock, RWLock::Context::TakenForRead);

  vector<OSDOp> opv(1);
  opv[0].op.op = CEPH_OSD_OP_WATCH;
  opv[0].op.watch.cookie = info->cookie;
  opv[0].op.watch.op = CEPH_OSD_WATCH_OP_PING;
  C_Linger_Ping *onack = new C_Linger_Ping(this, info);
  Op *o = new Op(info->target.base_oid, info->target.base_oloc,
		 opv, info->target.flags | CEPH_OSD_FLAG_READ,
		 onack, NULL, NULL);
  o->target = info->target;
  o->should_resend = false;
  _send_op_account(o);
  MOSDOp *m = _prepare_osd_op(o);
  o->tid = last_tid.inc();
  _session_op_assign(info->session, o);
  _send_op(o, m);
  info->ping_tid = o->tid;

  onack->sent = now;
  logger->inc(l_osdc_linger_ping);
}

void Objecter::_linger_ping(LingerOp *info, int r, utime_t sent)
{
  ldout(cct, 10) << __func__ << " " << info->linger_id
		 << " sent " << sent << " = " << r
		 << " (last_error " << info->last_error << ")" << dendl;
  info->watch_lock.Lock();
  if (r == 0) {
    info->watch_valid_thru = sent;
  } else if (r < 0) {
    info->last_error = r;
    if (info->on_error)
      info->on_error->complete(r);
  }
  info->watch_cond.SignalAll();
  info->watch_lock.Unlock();
}

int Objecter::linger_check(uint64_t linger_id)
{
  RWLock::WLocker wl(rwlock);
  map<uint64_t, LingerOp*>::iterator iter = linger_ops.find(linger_id);
  if (iter == linger_ops.end()) {
    ldout(cct, 10) << __func__ << " " << linger_id << " dne" << dendl;
    return -EBADF;
  }

  LingerOp *info = iter->second;
  utime_t age = ceph_clock_now(NULL) - info->watch_valid_thru;
  ldout(cct, 10) << __func__ << " " << linger_id
		 << " err " << info->last_error
		 << " age " << age << dendl;
  if (info->last_error)
    return info->last_error;
  return age.to_msec();
}

void Objecter::unregister_linger(uint64_t linger_id)
{
  RWLock::WLocker wl(rwlock);
  _unregister_linger(linger_id);
}

void Objecter::_unregister_linger(uint64_t linger_id)
{
  assert(rwlock.is_wlocked());
  ldout(cct, 20) << __func__ << " linger_id=" << linger_id << dendl;

  map<uint64_t, LingerOp*>::iterator iter = linger_ops.find(linger_id);
  if (iter != linger_ops.end()) {
    LingerOp *info = iter->second;
    OSDSession *s = info->session;
    s->lock.get_write();
    _session_linger_op_remove(s, info);
    s->lock.unlock();

    linger_ops.erase(iter);
    info->canceled = true;
    info->put();

    logger->dec(l_osdc_linger_active);
  }
}

ceph_tid_t Objecter::linger_mutate(const object_t& oid, const object_locator_t& oloc,
				   ObjectOperation& op,
				   const SnapContext& snapc, utime_t mtime,
				   bufferlist& inbl, uint64_t cookie, int flags,
				   Context *onack, Context *oncommit,
				   Context *onerror,
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
  info->cookie = cookie;
  info->inbl = inbl;
  info->poutbl = NULL;
  info->pobjver = objver;
  info->on_reg_ack = onack;
  info->on_reg_commit = oncommit;
  info->on_error = onerror;
  info->watch_valid_thru = ceph_clock_now(NULL);

  RWLock::WLocker wl(rwlock);
  _linger_submit(info);
  logger->inc(l_osdc_linger_active);
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
  info->target.flags = flags | CEPH_OSD_FLAG_READ;
  info->ops = op.ops;
  info->inbl = inbl;
  info->poutbl = poutbl;
  info->pobjver = objver;
  info->on_reg_commit = onfinish;

  RWLock::WLocker wl(rwlock);
  _linger_submit(info);
  logger->inc(l_osdc_linger_active);
  return info->linger_id;
}

void Objecter::_linger_submit(LingerOp *info)
{
  assert(rwlock.is_wlocked());
  RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

  // Acquire linger ID
  info->linger_id = ++max_linger_id;
  ldout(cct, 10) << __func__ << " info " << info
		 << " linger_id " << info->linger_id << dendl;
  linger_ops[info->linger_id] = info;

  // Populate Op::target
  OSDSession *s = NULL;
  _calc_target(&info->target);

  // Create LingerOp<->OSDSession relation
  int r = _get_session(info->target.osd, &s, lc);
  assert(r == 0);
  s->lock.get_write();
  _session_linger_op_assign(s, info);
  s->lock.unlock();
  put_session(s);

  _send_linger(info);
}

bool Objecter::ms_dispatch(Message *m)
{
  ldout(cct, 10) << __func__ << " " << cct << " " << *m << dendl;
  if (!initialized.read())
    return false;

  switch (m->get_type()) {
    // these we exlusively handle
  case CEPH_MSG_OSD_OPREPLY:
    handle_osd_op_reply(static_cast<MOSDOpReply*>(m));
    return true;

  case MSG_COMMAND_REPLY:
    if (m->get_source().type() == CEPH_ENTITY_TYPE_OSD) {
      handle_command_reply(static_cast<MCommandReply*>(m));
      return true;
    } else {
      return false;
    }

  case MSG_GETPOOLSTATSREPLY:
    handle_get_pool_stats_reply(static_cast<MGetPoolStatsReply*>(m));
    return true;

  case CEPH_MSG_POOLOP_REPLY:
    handle_pool_op_reply(static_cast<MPoolOpReply*>(m));
    return true;

  case CEPH_MSG_STATFS_REPLY:
    handle_fs_stats_reply(static_cast<MStatfsReply*>(m));
    return true;

    // these we give others a chance to inspect

    // MDS, OSD
  case CEPH_MSG_OSD_MAP:
    handle_osd_map(static_cast<MOSDMap*>(m));
    return false;
  }
  return false;
}

void Objecter::_scan_requests(OSDSession *s,
                             bool force_resend,
			     bool force_resend_writes,
			     map<ceph_tid_t, Op*>& need_resend,
			     list<LingerOp*>& need_resend_linger,
			     map<ceph_tid_t, CommandOp*>& need_resend_command)
{
  assert(rwlock.is_wlocked());

  list<uint64_t> unregister_lingers;

  RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

  s->lock.get_write();

  // check for changed linger mappings (_before_ regular ops)
  map<ceph_tid_t,LingerOp*>::iterator lp = s->linger_ops.begin();
  while (lp != s->linger_ops.end()) {
    LingerOp *op = lp->second;
    assert(op->session == s);
    ++lp;   // check_linger_pool_dne() may touch linger_ops; prevent iterator invalidation
    ldout(cct, 10) << " checking linger op " << op->linger_id << dendl;
    bool unregister;
    int r = _recalc_linger_op_target(op, lc);
    switch (r) {
    case RECALC_OP_TARGET_NO_ACTION:
      if (!force_resend && !force_resend_writes)
	break;
      // -- fall-thru --
    case RECALC_OP_TARGET_NEED_RESEND:
      need_resend_linger.push_back(op);
      _linger_cancel_map_check(op);
      break;
    case RECALC_OP_TARGET_POOL_DNE:
      _check_linger_pool_dne(op, &unregister);
      if (unregister) {
        ldout(cct, 10) << " need to unregister linger op " << op->linger_id << dendl;
        unregister_lingers.push_back(op->linger_id);
      }
      break;
    }
  }

  // check for changed request mappings
  map<ceph_tid_t,Op*>::iterator p = s->ops.begin();
  while (p != s->ops.end()) {
    Op *op = p->second;
    ++p;   // check_op_pool_dne() may touch ops; prevent iterator invalidation
    ldout(cct, 10) << " checking op " << op->tid << dendl;
    int r = _calc_target(&op->target);
    switch (r) {
    case RECALC_OP_TARGET_NO_ACTION:
      if (!force_resend &&
	  (!force_resend_writes || !(op->target.flags & CEPH_OSD_FLAG_WRITE)))
	break;
      // -- fall-thru --
    case RECALC_OP_TARGET_NEED_RESEND:
      if (op->session) {
	_session_op_remove(op->session, op);
      }
      need_resend[op->tid] = op;
      _op_cancel_map_check(op);
      break;
    case RECALC_OP_TARGET_POOL_DNE:
      _check_op_pool_dne(op, true);
      break;
    }
  }

  // commands
  map<ceph_tid_t,CommandOp*>::iterator cp = s->command_ops.begin();
  while (cp != s->command_ops.end()) {
    CommandOp *c = cp->second;
    ++cp;
    ldout(cct, 10) << " checking command " << c->tid << dendl;
    int r = _calc_command_target(c);
    switch (r) {
    case RECALC_OP_TARGET_NO_ACTION:
      // resend if skipped map; otherwise do nothing.
      if (!force_resend && !force_resend_writes)
	break;
      // -- fall-thru --
    case RECALC_OP_TARGET_NEED_RESEND:
      need_resend_command[c->tid] = c;
      if (c->session) {
        _session_command_op_remove(c->session, c);
      }
      _command_cancel_map_check(c);
      break;
    case RECALC_OP_TARGET_POOL_DNE:
    case RECALC_OP_TARGET_OSD_DNE:
    case RECALC_OP_TARGET_OSD_DOWN:
      _check_command_map_dne(c);
      break;
    }     
  }

  s->lock.unlock();

  for (list<uint64_t>::iterator iter = unregister_lingers.begin(); iter != unregister_lingers.end(); ++iter) {
    _unregister_linger(*iter);
  }
}

void Objecter::handle_osd_map(MOSDMap *m)
{
  RWLock::WLocker wl(rwlock);
  if (!initialized.read())
    return;

  assert(osdmap); 

  if (m->fsid != monc->get_fsid()) {
    ldout(cct, 0) << "handle_osd_map fsid " << m->fsid
		  << " != " << monc->get_fsid() << dendl;
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
  } else {
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
	  ldout(cct, 3) << "handle_osd_map decoding incremental epoch " << e
			<< dendl;
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
	    ldout(cct, 3) << "handle_osd_map requesting missing epoch "
			  << osdmap->get_epoch()+1 << dendl;
	    int r = _maybe_request_map();
            assert(r == 0);
	    break;
	  }
	  ldout(cct, 3) << "handle_osd_map missing epoch "
			<< osdmap->get_epoch()+1
			<< ", jumping to " << m->get_oldest() << dendl;
	  e = m->get_oldest() - 1;
	  skipped_map = true;
	  continue;
	}
	logger->set(l_osdc_map_epoch, osdmap->get_epoch());

	was_full = was_full || osdmap_full_flag();
	_scan_requests(homeless_session, skipped_map, was_full,
		       need_resend, need_resend_linger,
		       need_resend_command);

	// osd addr changes?
	for (map<int,OSDSession*>::iterator p = osd_sessions.begin();
	     p != osd_sessions.end(); ) {
	  OSDSession *s = p->second;
	  _scan_requests(s, skipped_map, was_full,
			 need_resend, need_resend_linger,
			 need_resend_command);
	  ++p;
	  if (!osdmap->is_up(s->osd) ||
	      (s->con &&
	       s->con->get_peer_addr() != osdmap->get_inst(s->osd).addr)) {
	    close_session(s);
	  }
	}

	assert(e == osdmap->get_epoch());
      }
      
    } else {
      // first map.  we want the full thing.
      if (m->maps.count(m->get_last())) {
        for (map<int,OSDSession*>::iterator p = osd_sessions.begin();
	     p != osd_sessions.end(); ++p) {
	  OSDSession *s = p->second;
	  _scan_requests(s, false, false, need_resend, need_resend_linger,
			 need_resend_command);
        }
	ldout(cct, 3) << "handle_osd_map decoding full epoch "
		      << m->get_last() << dendl;
	osdmap->decode(m->maps[m->get_last()]);

	_scan_requests(homeless_session, false, false,
		       need_resend, need_resend_linger,
		       need_resend_command);
      } else {
	ldout(cct, 3) << "handle_osd_map hmm, i want a full map, requesting"
		      << dendl;
	monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
	monc->renew_subs();
      }
    }
  }

  bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || osdmap_full_flag();

  // was/is paused?
  if (was_pauserd || was_pausewr || pauserd || pausewr) {
    int r = _maybe_request_map();
    assert(r == 0);
  }

  RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

  // resend requests
  for (map<ceph_tid_t, Op*>::iterator p = need_resend.begin();
       p != need_resend.end(); ++p) {
    Op *op = p->second;
    OSDSession *s = op->session;
    bool mapped_session = false;
    if (!s) {
      int r = _map_session(&op->target, &s, lc);
      assert(r == 0);
      mapped_session = true;
    } else {
      get_session(s);
    }
    s->lock.get_write();
    if (mapped_session) {
      _session_op_assign(s, op);
    }
    if (op->should_resend) {
      if (!op->session->is_homeless() && !op->target.paused) {
	logger->inc(l_osdc_op_resend);
	_send_op(op);
      }
    } else {
      _cancel_linger_op(op);
    }
    s->lock.unlock();
    put_session(s);
  }
  for (list<LingerOp*>::iterator p = need_resend_linger.begin();
       p != need_resend_linger.end(); ++p) {
    LingerOp *op = *p;
    if (!op->session) {
      _calc_target(&op->target);
      OSDSession *s = NULL;
      int const r = _get_session(op->target.osd, &s, lc);
      assert(r == 0);
      assert(s != NULL);
      op->session = s;
      put_session(s);
    }
    if (!op->session->is_homeless()) {
      logger->inc(l_osdc_linger_resend);
      _send_linger(op);
    }
  }
  for (map<ceph_tid_t,CommandOp*>::iterator p = need_resend_command.begin();
       p != need_resend_command.end(); ++p) {
    CommandOp *c = p->second;
    _assign_command_session(c);
    if (c->session && !c->session->is_homeless()) {
      _send_command(c);
    }
  }

  _dump_active();
  
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

  monc->sub_got("osdmap", osdmap->get_epoch());

  if (!waiting_for_map.empty()) {
    int r = _maybe_request_map();
    assert(r == 0);
  }
}

// op pool check

void Objecter::C_Op_Map_Latest::finish(int r)
{
  if (r == -EAGAIN || r == -ECANCELED)
    return;

  lgeneric_subdout(objecter->cct, objecter, 10) << "op_map_latest r=" << r << " tid=" << tid
						<< " latest " << latest << dendl;

  RWLock::WLocker wl(objecter->rwlock);

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

  objecter->_check_op_pool_dne(op, false);

  op->put();
}

int Objecter::pool_snap_by_name(int64_t poolid, const char *snap_name, snapid_t *snap)
{
  RWLock::RLocker rl(rwlock);

  const map<int64_t, pg_pool_t>& pools = osdmap->get_pools();
  map<int64_t, pg_pool_t>::const_iterator iter = pools.find(poolid);
  if (iter == pools.end()) {
    return -ENOENT;
  }
  const pg_pool_t& pg_pool = iter->second;
  map<snapid_t, pool_snap_info_t>::const_iterator p;
  for (p = pg_pool.snaps.begin();
       p != pg_pool.snaps.end();
       ++p) {
    if (p->second.name == snap_name) {
      *snap = p->first;
      return 0;
    }
  }
  return -ENOENT;
}

int Objecter::pool_snap_get_info(int64_t poolid, snapid_t snap, pool_snap_info_t *info)
{
  RWLock::RLocker rl(rwlock);

  const map<int64_t, pg_pool_t>& pools = osdmap->get_pools();
  map<int64_t, pg_pool_t>::const_iterator iter = pools.find(poolid);
  if (iter == pools.end()) {
    return -ENOENT;
  }
  const pg_pool_t& pg_pool = iter->second;
  map<snapid_t,pool_snap_info_t>::const_iterator p = pg_pool.snaps.find(snap);
  if (p == pg_pool.snaps.end())
    return -ENOENT;
  *info = p->second;

  return 0;
}

int Objecter::pool_snap_list(int64_t poolid, vector<uint64_t> *snaps)
{
  RWLock::RLocker rl(rwlock);

  const pg_pool_t *pi = osdmap->get_pg_pool(poolid);
  for (map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.begin();
       p != pi->snaps.end();
       ++p) {
    snaps->push_back(p->first);
  }
  return 0;
}

void Objecter::_check_op_pool_dne(Op *op, bool session_locked)
{
  assert(rwlock.is_wlocked());

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

      OSDSession *s = op->session;
      assert(s != NULL);

      if (!session_locked) {
        s->lock.get_write();
      }
      _finish_op(op);
      if (!session_locked) {
        s->lock.unlock();
      }
    }
  } else {
    _send_op_map_check(op);
  }
}

void Objecter::_send_op_map_check(Op *op)
{
  assert(rwlock.is_wlocked());
  // ask the monitor
  if (check_latest_map_ops.count(op->tid) == 0) {
    op->get();
    check_latest_map_ops[op->tid] = op;
    C_Op_Map_Latest *c = new C_Op_Map_Latest(this, op->tid);
    monc->get_version("osdmap", &c->latest, NULL, c);
  }
}

void Objecter::_op_cancel_map_check(Op *op)
{
  assert(rwlock.is_wlocked());
  map<ceph_tid_t, Op*>::iterator iter =
    check_latest_map_ops.find(op->tid);
  if (iter != check_latest_map_ops.end()) {
    Op *op = iter->second;
    op->put();
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

  RWLock::WLocker wl(objecter->rwlock);

  map<uint64_t, LingerOp*>::iterator iter =
    objecter->check_latest_map_lingers.find(linger_id);
  if (iter == objecter->check_latest_map_lingers.end()) {
    return;
  }

  LingerOp *op = iter->second;
  objecter->check_latest_map_lingers.erase(iter);

  if (op->map_dne_bound == 0)
    op->map_dne_bound = latest;

  bool unregister;
  objecter->_check_linger_pool_dne(op, &unregister);

  if (unregister) {
    objecter->_unregister_linger(op->linger_id);
  }

  op->put();
}

void Objecter::_check_linger_pool_dne(LingerOp *op, bool *need_unregister)
{
  assert(rwlock.is_wlocked());

  *need_unregister = false;

  ldout(cct, 10) << "_check_linger_pool_dne linger_id " << op->linger_id
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
      *need_unregister = true;
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

void Objecter::_linger_cancel_map_check(LingerOp *op)
{
  assert(rwlock.is_wlocked());

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

  RWLock::WLocker wl(objecter->rwlock);

  map<uint64_t, CommandOp*>::iterator iter =
    objecter->check_latest_map_commands.find(tid);
  if (iter == objecter->check_latest_map_commands.end()) {
    return;
  }

  CommandOp *c = iter->second;
  objecter->check_latest_map_commands.erase(iter);

  if (c->map_dne_bound == 0)
    c->map_dne_bound = latest;

  objecter->_check_command_map_dne(c);

  c->put();
}

void Objecter::_check_command_map_dne(CommandOp *c)
{
  assert(rwlock.is_wlocked());

  ldout(cct, 10) << "_check_command_map_dne tid " << c->tid
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
  assert(rwlock.is_wlocked());

  // ask the monitor
  if (check_latest_map_commands.count(c->tid) == 0) {
    c->get();
    check_latest_map_commands[c->tid] = c;
    C_Command_Map_Latest *f = new C_Command_Map_Latest(this, c->tid);
    monc->get_version("osdmap", &f->latest, NULL, f);
  }
}

void Objecter::_command_cancel_map_check(CommandOp *c)
{
  assert(rwlock.is_wlocked());

  map<uint64_t, CommandOp*>::iterator iter =
    check_latest_map_commands.find(c->tid);
  if (iter != check_latest_map_commands.end()) {
    CommandOp *c = iter->second;
    c->put();
    check_latest_map_commands.erase(iter);
  }
}


/**
 * Look up OSDSession by OSD id.
 *
 * @returns 0 on success, or -EAGAIN if the lock context requires promotion to write.
 */
int Objecter::_get_session(int osd, OSDSession **session, RWLock::Context& lc)
{
  assert(rwlock.is_locked());

  if (osd < 0) {
    *session = homeless_session;
    ldout(cct, 20) << __func__ << " osd=" << osd << " returning homeless" << dendl;
    return 0;
  }

  map<int,OSDSession*>::iterator p = osd_sessions.find(osd);
  if (p != osd_sessions.end()) {
    OSDSession *s = p->second;
    s->get();
    *session = s;
    ldout(cct, 20) << __func__ << " s=" << s << " osd=" << osd << " " << s->get_nref() << dendl;
    return 0;
  }
  if (!lc.is_wlocked()) {
    return -EAGAIN;
  }
  OSDSession *s = new OSDSession(cct, osd);
  osd_sessions[osd] = s;
  s->con = messenger->get_connection(osdmap->get_inst(osd));
  logger->inc(l_osdc_osd_session_open);
  logger->inc(l_osdc_osd_sessions, osd_sessions.size());
  s->get();
  *session = s;
  ldout(cct, 20) << __func__ << " s=" << s << " osd=" << osd << " " << s->get_nref() << dendl;
  return 0;
}

void Objecter::put_session(Objecter::OSDSession *s)
{
  if (s && !s->is_homeless()) {
    ldout(cct, 20) << __func__ << " s=" << s << " osd=" << s->osd << " " << s->get_nref() << dendl;
    s->put();
  }
}

void Objecter::get_session(Objecter::OSDSession *s)
{
  assert(s != NULL);

  if (!s->is_homeless()) {
    ldout(cct, 20) << __func__ << " s=" << s << " osd=" << s->osd << " " << s->get_nref() << dendl;
    s->get();
  }
}

void Objecter::_reopen_session(OSDSession *s)
{
  assert(s->lock.is_locked());

  entity_inst_t inst = osdmap->get_inst(s->osd);
  ldout(cct, 10) << "reopen_session osd." << s->osd << " session, addr now " << inst << dendl;
  if (s->con) {
    s->con->mark_down();
    logger->inc(l_osdc_osd_session_close);
  }
  s->con = messenger->get_connection(inst);
  s->incarnation++;
  logger->inc(l_osdc_osd_session_open);
}

void Objecter::close_session(OSDSession *s)
{
  assert(rwlock.is_wlocked());

  ldout(cct, 10) << "close_session for osd." << s->osd << dendl;
  if (s->con) {
    s->con->mark_down();
    logger->inc(l_osdc_osd_session_close);
  }
  s->lock.get_write();

  std::list<LingerOp*> homeless_lingers;
  std::list<CommandOp*> homeless_commands;
  std::list<Op*> homeless_ops;

  while (!s->linger_ops.empty()) {
    std::map<uint64_t, LingerOp*>::iterator i = s->linger_ops.begin();
    ldout(cct, 10) << " linger_op " << i->first << dendl;
    homeless_lingers.push_back(i->second);
    _session_linger_op_remove(s, i->second);
  }

  while (!s->ops.empty()) {
    std::map<ceph_tid_t, Op*>::iterator i = s->ops.begin();
    ldout(cct, 10) << " op " << i->first << dendl;
    homeless_ops.push_back(i->second);
    _session_op_remove(s, i->second);
  }

  while (!s->command_ops.empty()) {
    std::map<ceph_tid_t, CommandOp*>::iterator i = s->command_ops.begin();
    ldout(cct, 10) << " command_op " << i->first << dendl;
    homeless_commands.push_back(i->second);
    _session_command_op_remove(s, i->second);
  }

  osd_sessions.erase(s->osd);
  s->lock.unlock();
  put_session(s);

  // Assign any leftover ops to the homeless session
  {
    RWLock::WLocker wl(homeless_session->lock);
    for (std::list<LingerOp*>::iterator i = homeless_lingers.begin();
         i != homeless_lingers.end(); ++i) {
      _session_linger_op_assign(homeless_session, *i);
    }
    for (std::list<Op*>::iterator i = homeless_ops.begin();
         i != homeless_ops.end(); ++i) {
      _session_op_assign(homeless_session, *i);
    }
    for (std::list<CommandOp*>::iterator i = homeless_commands.begin();
         i != homeless_commands.end(); ++i) {
      _session_command_op_assign(homeless_session, *i);
    }
  }

  logger->set(l_osdc_osd_sessions, osd_sessions.size());
}

void Objecter::wait_for_osd_map()
{
  rwlock.get_write();
  if (osdmap->get_epoch()) {
    rwlock.put_write();
    return;
  }

  Mutex lock("");
  Cond cond;
  bool done;
  lock.Lock();
  C_SafeCond *context = new C_SafeCond(&lock, &cond, &done, NULL);
  waiting_for_map[0].push_back(pair<Context*, int>(context, 0));
  rwlock.put_write();
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
    if (r >= 0) {
      objecter->get_latest_version(oldest, newest, fin);
    } else if (r == -EAGAIN) { // try again as instructed
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

void Objecter::get_latest_version(epoch_t oldest, epoch_t newest, Context *fin)
{
  RWLock::WLocker wl(rwlock);
  _get_latest_version(oldest, newest, fin);
}

void Objecter::_get_latest_version(epoch_t oldest, epoch_t newest, Context *fin)
{
  assert(rwlock.is_wlocked());
  if (osdmap->get_epoch() >= newest) {
  ldout(cct, 10) << __func__ << " latest " << newest << ", have it" << dendl;
    if (fin)
      fin->complete(0);
    return;
  }

  ldout(cct, 10) << __func__ << " latest " << newest << ", waiting" << dendl;
  _wait_for_new_map(fin, newest, 0);
}

void Objecter::maybe_request_map()
{
  RWLock::RLocker rl(rwlock);
  int r;
  do {
    r = _maybe_request_map();
  } while (r == -EAGAIN);
}

int Objecter::_maybe_request_map()
{
  assert(rwlock.is_locked());
  int flag = 0;
  if (osdmap_full_flag()) {
    ldout(cct, 10) << "_maybe_request_map subscribing (continuous) to next osd map (FULL flag is set)" << dendl;
  } else {
    ldout(cct, 10) << "_maybe_request_map subscribing (onetime) to next osd map" << dendl;
    flag = CEPH_SUBSCRIBE_ONETIME;
  }
  epoch_t epoch = osdmap->get_epoch() ? osdmap->get_epoch()+1 : 0;
  if (monc->sub_want("osdmap", epoch, flag)) {
    monc->renew_subs();
  }
  return 0;
}

void Objecter::_wait_for_new_map(Context *c, epoch_t epoch, int err)
{
  assert(rwlock.is_wlocked());
  waiting_for_map[epoch].push_back(pair<Context *, int>(c, err));
  int r = _maybe_request_map();
  assert(r == 0);
}

bool Objecter::wait_for_map(epoch_t epoch, Context *c, int err)
{
  RWLock::WLocker wl(rwlock);
  if (osdmap->get_epoch() >= epoch) {
    return true;
  }
  _wait_for_new_map(c, epoch, err);
  return false;
}

void Objecter::kick_requests(OSDSession *session)
{
  ldout(cct, 10) << "kick_requests for osd." << session->osd << dendl;

  map<uint64_t, LingerOp *> lresend;
  RWLock::WLocker wl(rwlock);

  session->lock.get_write();
  _kick_requests(session, lresend);
  session->lock.unlock();

  _linger_ops_resend(lresend);
}

void Objecter::_kick_requests(OSDSession *session, map<uint64_t, LingerOp *>& lresend)
{
  assert(rwlock.is_locked());

  // resend ops
  map<ceph_tid_t,Op*> resend;  // resend in tid order
  for (map<ceph_tid_t, Op*>::iterator p = session->ops.begin(); p != session->ops.end();) {
    Op *op = p->second;
    ++p;
    logger->inc(l_osdc_op_resend);
    if (op->should_resend) {
      if (!op->target.paused)
	resend[op->tid] = op;
    } else {
      _cancel_linger_op(op);
    }
  }

  while (!resend.empty()) {
    _send_op(resend.begin()->second);
    resend.erase(resend.begin());
  }

  // resend lingers
  for (map<ceph_tid_t, LingerOp*>::iterator j = session->linger_ops.begin(); j != session->linger_ops.end(); ++j) {
    LingerOp *op = j->second;
    op->get();
    logger->inc(l_osdc_linger_resend);
    assert(lresend.count(j->first) == 0);
    lresend[j->first] = op;
  }

  // resend commands
  map<uint64_t,CommandOp*> cresend;  // resend in order
  for (map<ceph_tid_t, CommandOp*>::iterator k = session->command_ops.begin(); k != session->command_ops.end(); ++k) {
    logger->inc(l_osdc_command_resend);
    cresend[k->first] = k->second;
  }
  while (!cresend.empty()) {
    _send_command(cresend.begin()->second);
    cresend.erase(cresend.begin());
  }
}

void Objecter::_linger_ops_resend(map<uint64_t, LingerOp *>& lresend)
{
  assert(rwlock.is_locked());

  while (!lresend.empty()) {
    LingerOp *op = lresend.begin()->second;
    if (!op->canceled) {
      _send_linger(op);
    }
    op->put();
    lresend.erase(lresend.begin());
  }
}

void Objecter::schedule_tick()
{
  Mutex::Locker l(timer_lock);
  assert(tick_event == NULL);
  tick_event = new C_Tick(this);
  timer.add_event_after(cct->_conf->objecter_tick_interval, tick_event);
}

void Objecter::tick()
{
  RWLock::RLocker rl(rwlock);

  ldout(cct, 10) << "tick" << dendl;

  // we are only called by C_Tick
  assert(tick_event);
  tick_event = NULL;

  if (!initialized.read()) {
    // we raced with shutdown
    ldout(cct, 10) << __func__ << " raced with shutdown" << dendl;
    return;
  }

  set<OSDSession*> toping;

  int r = 0;

  // look for laggy requests
  utime_t cutoff = ceph_clock_now(cct);
  cutoff -= cct->_conf->objecter_timeout;  // timeout

  unsigned laggy_ops;

  do {
    laggy_ops = 0;
    for (map<int,OSDSession*>::iterator siter = osd_sessions.begin(); siter != osd_sessions.end(); ++siter) {
      OSDSession *s = siter->second;
      RWLock::RLocker l(s->lock);
      for (map<ceph_tid_t,Op*>::iterator p = s->ops.begin();
           p != s->ops.end();
           ++p) {
        Op *op = p->second;
        assert(op->session);
        if (op->stamp < cutoff) {
          ldout(cct, 2) << " tid " << p->first << " on osd." << op->session->osd << " is laggy" << dendl;
          toping.insert(op->session);
          ++laggy_ops;
        }
      }
      for (map<uint64_t,LingerOp*>::iterator p = s->linger_ops.begin();
           p != s->linger_ops.end();
           ++p) {
        LingerOp *op = p->second;
        assert(op->session);
        ldout(cct, 10) << " pinging osd that serves lingering tid " << p->first << " (osd." << op->session->osd << ")" << dendl;
        toping.insert(op->session);
	if (op->cookie && !op->last_error)
	  _send_linger_ping(op);
      }
      for (map<uint64_t,CommandOp*>::iterator p = s->command_ops.begin();
           p != s->command_ops.end();
           ++p) {
        CommandOp *op = p->second;
        assert(op->session);
        ldout(cct, 10) << " pinging osd that serves command tid " << p->first << " (osd." << op->session->osd << ")" << dendl;
        toping.insert(op->session);
      }
    }
    if (num_homeless_ops.read() || !toping.empty()) {
      r = _maybe_request_map();
      if (r == -EAGAIN) {
        toping.clear();
      }
    }
  } while (r == -EAGAIN);

  logger->set(l_osdc_op_laggy, laggy_ops);
  logger->set(l_osdc_osd_laggy, toping.size());

  if (!toping.empty()) {
    // send a ping to these osds, to ensure we detect any session resets
    // (osd reply message policy is lossy)
    for (set<OSDSession*>::const_iterator i = toping.begin();
	 i != toping.end();
	 ++i) {
      (*i)->con->send_message(new MPing);
    }
  }

  // reschedule
  schedule_tick();
}

void Objecter::resend_mon_ops()
{
  RWLock::WLocker wl(rwlock);

  ldout(cct, 10) << "resend_mon_ops" << dendl;

  for (map<ceph_tid_t,PoolStatOp*>::iterator p = poolstat_ops.begin(); p!=poolstat_ops.end(); ++p) {
    _poolstat_submit(p->second);
    logger->inc(l_osdc_poolstat_resend);
  }

  for (map<ceph_tid_t,StatfsOp*>::iterator p = statfs_ops.begin(); p!=statfs_ops.end(); ++p) {
    _fs_stats_submit(p->second);
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
  ceph_tid_t tid;
  Objecter *objecter;
public:
  C_CancelOp(ceph_tid_t t, Objecter *objecter) : tid(t), objecter(objecter) {}
  void finish(int r) {
    objecter->op_cancel(tid, -ETIMEDOUT);
  }
};

ceph_tid_t Objecter::op_submit(Op *op, int *ctx_budget)
{
  RWLock::RLocker rl(rwlock);
  RWLock::Context lc(rwlock, RWLock::Context::TakenForRead);
  return _op_submit_with_budget(op, lc, ctx_budget);
}

ceph_tid_t Objecter::_op_submit_with_budget(Op *op, RWLock::Context& lc, int *ctx_budget)
{
  assert(initialized.read());

  assert(op->ops.size() == op->out_bl.size());
  assert(op->ops.size() == op->out_rval.size());
  assert(op->ops.size() == op->out_handler.size());

  // throttle.  before we look at any state, because
  // take_op_budget() may drop our lock while it blocks.
  if (!op->ctx_budgeted || (ctx_budget && (*ctx_budget == -1))) {
    int op_budget = _take_op_budget(op);
    // take and pass out the budget for the first OP
    // in the context session
    if (ctx_budget && (*ctx_budget == -1)) {
      *ctx_budget = op_budget;
    }
  }

  ceph_tid_t tid = _op_submit(op, lc);

  if (osd_timeout > 0) {
    Mutex::Locker l(timer_lock);
    op->ontimeout = new C_CancelOp(tid, this);
    timer.add_event_after(osd_timeout, op->ontimeout);
  }

  return tid;
}

void Objecter::_send_op_account(Op *op)
{
  inflight_ops.inc();

  // add to gather set(s)
  if (op->onack) {
    num_unacked.inc();
  } else {
    ldout(cct, 20) << " note: not requesting ack" << dendl;
  }
  if (op->oncommit) {
    num_uncommitted.inc();
  } else {
    ldout(cct, 20) << " note: not requesting commit" << dendl;
  }

  logger->inc(l_osdc_op_active);
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
}

ceph_tid_t Objecter::_op_submit(Op *op, RWLock::Context& lc)
{
  assert(rwlock.is_locked());

  ldout(cct, 10) << __func__ << " op " << op << dendl;

  // pick target
  assert(op->session == NULL);
  OSDSession *s = NULL;

  bool const check_for_latest_map = _calc_target(&op->target) == RECALC_OP_TARGET_POOL_DNE;

  // Try to get a session, including a retry if we need to take write lock
  int r = _get_session(op->target.osd, &s, lc);
  if (r == -EAGAIN) {
    assert(s == NULL);
    lc.promote();
    r = _get_session(op->target.osd, &s, lc);
  }
  assert(r == 0);
  assert(s);  // may be homeless

  // We may need to take wlock if we will need to _set_op_map_check later.
  if (check_for_latest_map && !lc.is_wlocked()) {
    lc.promote();
  }

  _send_op_account(op);

  // send?
  ldout(cct, 10) << "_op_submit oid " << op->target.base_oid
           << " " << op->target.base_oloc << " " << op->target.target_oloc
	   << " " << op->ops << " tid " << op->tid
           << " osd." << (!s->is_homeless() ? s->osd : -1)
           << dendl;

  assert(op->target.flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE));

  bool need_send = false;

  if ((op->target.flags & CEPH_OSD_FLAG_WRITE) &&
      osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
    ldout(cct, 10) << " paused modify " << op << " tid " << last_tid.read() << dendl;
    op->target.paused = true;
    _maybe_request_map();
  } else if ((op->target.flags & CEPH_OSD_FLAG_READ) &&
	     osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
    ldout(cct, 10) << " paused read " << op << " tid " << last_tid.read() << dendl;
    op->target.paused = true;
    _maybe_request_map();
  } else if ((op->target.flags & CEPH_OSD_FLAG_WRITE) && osdmap_full_flag()) {
    ldout(cct, 0) << " FULL, paused modify " << op << " tid " << last_tid.read() << dendl;
    op->target.paused = true;
    _maybe_request_map();
  } else if (!s->is_homeless()) {
    need_send = true;
  } else {
    _maybe_request_map();
  }

  MOSDOp *m = NULL;
  if (need_send) {
    m = _prepare_osd_op(op);
  }

  s->lock.get_write();
  if (op->tid == 0)
    op->tid = last_tid.inc();
  _session_op_assign(s, op);

  if (need_send) {
    _send_op(op, m);
  }

  // Last chance to touch Op here, after giving up session lock it can be
  // freed at any time by response handler.
  ceph_tid_t tid = op->tid;
  if (check_for_latest_map) {
    _send_op_map_check(op);
  }
  op = NULL;

  s->lock.unlock();
  put_session(s);

  ldout(cct, 5) << num_unacked.read() << " unacked, " << num_uncommitted.read() << " uncommitted" << dendl;

  return tid;
}

int Objecter::op_cancel(OSDSession *s, ceph_tid_t tid, int r)
{
  assert(initialized.read());

  s->lock.get_write();

  map<ceph_tid_t, Op*>::iterator p = s->ops.find(tid);
  if (p == s->ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  if (s->con) {
    ldout(cct, 20) << " revoking rx buffer for " << tid
		   << " on " << s->con << dendl;
    s->con->revoke_rx_buffer(tid);
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;
  Op *op = p->second;
  if (op->onack) {
    op->onack->complete(r);
    op->onack = NULL;
  }
  if (op->oncommit) {
    op->oncommit->complete(r);
    op->oncommit = NULL;
  }
  _op_cancel_map_check(op);
  _finish_op(op);
  s->lock.unlock();

  return 0;
}

int Objecter::op_cancel(ceph_tid_t tid, int r)
{
  int ret = 0;

  rwlock.get_write();

start:

  for (map<int, OSDSession *>::iterator siter = osd_sessions.begin(); siter != osd_sessions.end(); ++siter) {
    OSDSession *s = siter->second;
    s->lock.get_read();
    if (s->ops.find(tid) != s->ops.end()) {
      s->lock.unlock();
      ret = op_cancel(s, tid, r);
      if (ret == -ENOENT) {
        /* oh no! raced, maybe tid moved to another session, restarting */
        goto start;
      }
      rwlock.unlock();
      return ret;
    }
    s->lock.unlock();
  }

  // Handle case where the op is in homeless session
  homeless_session->lock.get_read();
  if (homeless_session->ops.find(tid) != homeless_session->ops.end()) {
    homeless_session->lock.unlock();
    ret = op_cancel(homeless_session, tid, r);
    if (ret == -ENOENT) {
      /* oh no! raced, maybe tid moved to another session, restarting */
      goto start;
    } else {
      rwlock.unlock();
      return ret;
    }
  } else {
    homeless_session->lock.unlock();
  }

  rwlock.unlock();

  return ret;
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

int Objecter::_calc_target(op_target_t *t, bool any_change)
{
  assert(rwlock.is_locked());

  bool is_read = t->flags & CEPH_OSD_FLAG_READ;
  bool is_write = t->flags & CEPH_OSD_FLAG_WRITE;

  const pg_pool_t *pi = osdmap->get_pg_pool(t->base_oloc.pool);
  if (!pi) {
    t->osd = -1;
    return RECALC_OP_TARGET_POOL_DNE;
  }

  bool force_resend = false;
  bool need_check_tiering = false;
  if (osdmap->get_epoch() == pi->last_force_op_resend) {
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
    if (is_read && pi->has_read_tier())
      t->target_oloc.pool = pi->read_tier;
    if (is_write && pi->has_write_tier())
      t->target_oloc.pool = pi->write_tier;
  }

  pg_t pgid;
  if (t->precalc_pgid) {
    assert(t->base_oid.name.empty()); // make sure this is a listing op
    ldout(cct, 10) << __func__ << " have " << t->base_pgid << " pool "
		   << osdmap->have_pg_pool(t->base_pgid.pool()) << dendl;
    if (!osdmap->have_pg_pool(t->base_pgid.pool())) {
      t->osd = -1;
      return RECALC_OP_TARGET_POOL_DNE;
    }
    pgid = osdmap->raw_pg_to_pg(t->base_pgid);
  } else {
    int ret = osdmap->object_locator_to_pg(t->target_oid, t->target_oloc,
					   pgid);
    if (ret == -ENOENT) {
      t->osd = -1;
      return RECALC_OP_TARGET_POOL_DNE;
    }
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
	int best_locality = 0;
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

int Objecter::_map_session(op_target_t *target, OSDSession **s,
			   RWLock::Context& lc)
{
  int r = _calc_target(target);
  if (r < 0) {
    return r;
  }
  return _get_session(target->osd, s, lc);
}

void Objecter::_session_op_assign(OSDSession *to, Op *op)
{
  assert(to->lock.is_locked());
  assert(op->session == NULL);
  assert(op->tid);

  get_session(to);
  op->session = to;
  to->ops[op->tid] = op;

  if (to->is_homeless()) {
    num_homeless_ops.inc();
  }

  ldout(cct, 15) << __func__ << " " << to->osd << " " << op->tid << dendl;
}

void Objecter::_session_op_remove(OSDSession *from, Op *op)
{
  assert(op->session == from);
  assert(from->lock.is_locked());

  if (from->is_homeless()) {
    num_homeless_ops.dec();
  }

  from->ops.erase(op->tid);
  put_session(from);
  op->session = NULL;

  ldout(cct, 15) << __func__ << " " << from->osd << " " << op->tid << dendl;
}

void Objecter::_session_linger_op_assign(OSDSession *to, LingerOp *op)
{
  assert(to->lock.is_wlocked());
  assert(op->session == NULL);

  if (to->is_homeless()) {
    num_homeless_ops.inc();
  }

  get_session(to);
  op->session = to;
  to->linger_ops[op->linger_id] = op;

  ldout(cct, 15) << __func__ << " " << to->osd << " " << op->linger_id << dendl;
}

void Objecter::_session_linger_op_remove(OSDSession *from, LingerOp *op)
{
  assert(from == op->session);
  assert(from->lock.is_locked());

  if (from->is_homeless()) {
    num_homeless_ops.dec();
  }

  from->linger_ops.erase(op->linger_id);
  put_session(from);
  op->session = NULL;

  ldout(cct, 15) << __func__ << " " << from->osd << " " << op->linger_id << dendl;
}

void Objecter::_session_command_op_remove(OSDSession *from, CommandOp *op)
{
  assert(from == op->session);
  assert(from->lock.is_locked());

  if (from->is_homeless()) {
    num_homeless_ops.dec();
  }

  from->command_ops.erase(op->tid);
  put_session(from);
  op->session = NULL;

  ldout(cct, 15) << __func__ << " " << from->osd << " " << op->tid << dendl;
}

void Objecter::_session_command_op_assign(OSDSession *to, CommandOp *op)
{
  assert(to->lock.is_locked());
  assert(op->session == NULL);
  assert(op->tid);

  if (to->is_homeless()) {
    num_homeless_ops.inc();
  }

  get_session(to);
  op->session = to;
  to->command_ops[op->tid] = op;

  ldout(cct, 15) << __func__ << " " << to->osd << " " << op->tid << dendl;
}

int Objecter::_get_osd_session(int osd, RWLock::Context& lc, OSDSession **psession)
{
  int r;
  do {
    r = _get_session(osd, psession, lc);
    if (r == -EAGAIN) {
      assert(!lc.is_wlocked());

      if (!_promote_lock_check_race(lc)) {
        return r;
      }
    }
  } while (r == -EAGAIN);
  assert(r == 0);

  return 0;
}

int Objecter::_get_op_target_session(Op *op, RWLock::Context& lc, OSDSession **psession)
{
  return _get_osd_session(op->target.osd, lc, psession);
}

bool Objecter::_promote_lock_check_race(RWLock::Context& lc)
{
  epoch_t epoch = osdmap->get_epoch();
  lc.promote();
  return (epoch == osdmap->get_epoch());
}

int Objecter::_recalc_linger_op_target(LingerOp *linger_op, RWLock::Context& lc)
{
  assert(rwlock.is_wlocked());

  int r = _calc_target(&linger_op->target, true);
  if (r == RECALC_OP_TARGET_NEED_RESEND) {
    ldout(cct, 10) << "recalc_linger_op_target tid " << linger_op->linger_id
		   << " pgid " << linger_op->target.pgid
		   << " acting " << linger_op->target.acting << dendl;
    
    OSDSession *s;
    r = _get_osd_session(linger_op->target.osd, lc, &s);
    if (r < 0) {
      // We have no session for the new destination for the op, so leave it
      // in this session to be handled again next time we scan requests
      return r;
    }

    if (linger_op->session != s) {
      // NB locking two sessions (s and linger_op->session) at the same time here
      // is only safe because we are the only one that takes two, and we are
      // holding rwlock for write.  Disable lockdep because it doesn't know that.
      s->lock.get_write(false);
      _session_linger_op_remove(linger_op->session, linger_op);
      _session_linger_op_assign(s, linger_op);
      s->lock.unlock(false);
    }

    put_session(s);
    return RECALC_OP_TARGET_NEED_RESEND;
  }
  return r;
}

void Objecter::_cancel_linger_op(Op *op)
{
  ldout(cct, 15) << "cancel_op " << op->tid << dendl;

  assert(!op->should_resend);
  delete op->onack;
  delete op->oncommit;

  _finish_op(op);
}

void Objecter::_finish_op(Op *op)
{
  ldout(cct, 15) << "finish_op " << op->tid << dendl;

  assert(op->session->lock.is_wlocked());

  if (!op->ctx_budgeted && op->budgeted)
    put_op_budget(op);

  if (op->ontimeout) {
    Mutex::Locker l(timer_lock);
    timer.cancel_event(op->ontimeout);
  }

  _session_op_remove(op->session, op);

  logger->dec(l_osdc_op_active);

  assert(check_latest_map_ops.find(op->tid) == check_latest_map_ops.end());

  inflight_ops.dec();

  op->put();
}

void Objecter::finish_op(OSDSession *session, ceph_tid_t tid)
{
  ldout(cct, 15) << "finish_op " << tid << dendl;
  RWLock::RLocker rl(rwlock);
  
  RWLock::WLocker wl(session->lock);

  map<ceph_tid_t, Op *>::iterator iter = session->ops.find(tid);
  if (iter == session->ops.end())
    return;

  Op *op = iter->second;

  _finish_op(op);
}

MOSDOp *Objecter::_prepare_osd_op(Op *op)
{
  assert(rwlock.is_locked());

  int flags = op->target.flags;
  flags |= CEPH_OSD_FLAG_KNOWN_REDIR;
  if (op->oncommit)
    flags |= CEPH_OSD_FLAG_ONDISK;
  if (op->onack)
    flags |= CEPH_OSD_FLAG_ACK;

  op->target.paused = false;
  op->stamp = ceph_clock_now(cct);

  MOSDOp *m = new MOSDOp(client_inc.read(), op->tid, 
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

  return m;
}

void Objecter::_send_op(Op *op, MOSDOp *m)
{
  assert(rwlock.is_locked());
  assert(op->session->lock.is_locked());

  if (!m) {
    assert(op->tid > 0);
    m = _prepare_osd_op(op);
  }

  ldout(cct, 15) << "_send_op " << op->tid << " to osd." << op->session->osd << dendl;

  ConnectionRef con = op->session->con;
  assert(con);

  // preallocated rx buffer?
  if (op->con) {
    ldout(cct, 20) << " revoking rx buffer for " << op->tid << " on " << op->con << dendl;
    op->con->revoke_rx_buffer(op->tid);
  }
  if (op->outbl &&
      op->ontimeout == NULL &&  // only post rx_buffer if no timeout; see #9582
      op->outbl->length()) {
    ldout(cct, 20) << " posting rx buffer for " << op->tid << " on " << con << dendl;
    op->con = con;
    op->con->post_rx_buffer(op->tid, *op->outbl);
  }

  op->incarnation = op->session->incarnation;

  m->set_tid(op->tid);

  op->session->con->send_message(m);
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

void Objecter::_throttle_op(Op *op, int op_budget)
{
  assert(rwlock.is_locked());

  bool locked_for_write = rwlock.is_wlocked();

  if (!op_budget)
    op_budget = calc_op_budget(op);
  if (!op_throttle_bytes.get_or_fail(op_budget)) { //couldn't take right now
    rwlock.unlock();
    op_throttle_bytes.get(op_budget);
    rwlock.get(locked_for_write);
  }
  if (!op_throttle_ops.get_or_fail(1)) { //couldn't take right now
    rwlock.unlock();
    op_throttle_ops.get(1);
    rwlock.get(locked_for_write);
  }
}

void Objecter::unregister_op(Op *op)
{
  op->session->lock.get_write();
  op->session->ops.erase(op->tid);
  op->session->lock.unlock();
  put_session(op->session);
  op->session = NULL;

  inflight_ops.dec();
}

/* This function DOES put the passed message before returning */
void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  ldout(cct, 10) << "in handle_osd_op_reply" << dendl;

  // get pio
  ceph_tid_t tid = m->get_tid();

  int osd_num = (int)m->get_source().num();

  RWLock::RLocker l(rwlock);
  if (!initialized.read()) {
    m->put();
    return;
  }

  RWLock::Context lc(rwlock, RWLock::Context::TakenForRead);

  map<int, OSDSession *>::iterator siter = osd_sessions.find(osd_num);
  if (siter == osd_sessions.end()) {
    ldout(cct, 7) << "handle_osd_op_reply " << tid
		  << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ?
						  " onnvram":" ack"))
		  << " ... unknown osd" << dendl;
    m->put();
    return;
  }

  OSDSession *s = siter->second;
  get_session(s);

  s->lock.get_write();

  map<ceph_tid_t, Op *>::iterator iter = s->ops.find(tid);
  if (iter == s->ops.end()) {
    ldout(cct, 7) << "handle_osd_op_reply " << tid
	    << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
	    << " ... stray" << dendl;
    s->lock.unlock();
    put_session(s);
    m->put();
    return;
  }

  ldout(cct, 7) << "handle_osd_op_reply " << tid
		<< (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
		<< " v " << m->get_replay_version() << " uv " << m->get_user_version()
		<< " in " << m->get_pg()
		<< " attempt " << m->get_retry_attempt()
		<< dendl;
  Op *op = iter->second;

  if (m->get_retry_attempt() >= 0) {
    if (m->get_retry_attempt() != (op->attempts - 1)) {
      ldout(cct, 7) << " ignoring reply from attempt " << m->get_retry_attempt()
		    << " from " << m->get_source_inst()
		    << "; last attempt " << (op->attempts - 1) << " sent to "
		    << op->session->con->get_peer_addr() << dendl;
      m->put();
      s->lock.unlock();
      put_session(s);
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
    if (op->onack)
      num_unacked.dec();
    if (op->oncommit)
      num_uncommitted.dec();
    _session_op_remove(s, op);
    s->lock.unlock();
    put_session(s);

    // FIXME: two redirects could race and reorder

    op->tid = 0;
    m->get_redirect().combine_with_locator(op->target.target_oloc,
					   op->target.target_oid.name);
    op->target.flags |= CEPH_OSD_FLAG_REDIRECTED;
    _op_submit(op, lc);
    m->put();
    return;
  }

  if (rc == -EAGAIN) {
    ldout(cct, 7) << " got -EAGAIN, resubmitting" << dendl;

    // new tid
    s->ops.erase(op->tid);
    op->tid = last_tid.inc();

    _send_op(op);
    s->lock.unlock();
    put_session(s);
    m->put();
    return;
  }

  l.unlock();
  lc.set_state(RWLock::Context::Untaken);

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
    num_unacked.dec();
    logger->inc(l_osdc_op_ack);
  }
  if (op->oncommit && (m->is_ondisk() || rc)) {
    ldout(cct, 15) << "handle_osd_op_reply safe" << dendl;
    oncommit = op->oncommit;
    op->oncommit = 0;
    num_uncommitted.dec();
    logger->inc(l_osdc_op_commit);
  }

  // got data?
  if (op->outbl) {
    if (op->con)
      op->con->revoke_rx_buffer(op->tid);
    m->claim_data(*op->outbl);
    op->outbl = 0;
  }

  /* get it before we call _finish_op() */
  Mutex *completion_lock = (op->target.base_oid.name.size() ? s->get_lock(op->target.base_oid) : NULL);

  // done with this tid?
  if (!op->onack && !op->oncommit) {
    ldout(cct, 15) << "handle_osd_op_reply completed tid " << tid << dendl;
    _finish_op(op);
  }

  ldout(cct, 5) << num_unacked.read() << " unacked, " << num_uncommitted.read() << " uncommitted" << dendl;

  // serialize completions
  if (completion_lock) {
    completion_lock->Lock();
  }
  s->lock.unlock();

  // do callbacks
  if (onack) {
    onack->complete(rc);
  }
  if (oncommit) {
    oncommit->complete(rc);
  }
  if (completion_lock) {
    completion_lock->Unlock();
  }

  m->put();
  put_session(s);
}


uint32_t Objecter::list_nobjects_seek(NListContext *list_context,
				     uint32_t pos)
{
  RWLock::RLocker rl(rwlock);
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

void Objecter::list_nobjects(NListContext *list_context, Context *onfinish)
{
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
    put_nlist_context_budget(list_context);

    onfinish->complete(0);
    return;
  }

  rwlock.get_read();
  const pg_pool_t *pool = osdmap->get_pg_pool(list_context->pool_id);
  int pg_num = pool->get_pg_num();
  rwlock.unlock();

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
  op.pg_nls(list_context->max_entries, list_context->filter, list_context->cookie,
	     list_context->current_pg_epoch);
  list_context->bl.clear();
  C_NList *onack = new C_NList(list_context, onfinish, this);
  object_locator_t oloc(list_context->pool_id, list_context->nspace);

  pg_read(list_context->current_pg, oloc, op,
	  &list_context->bl, 0, onack, &onack->epoch, &list_context->ctx_budget);
}

void Objecter::_nlist_reply(NListContext *list_context, int r,
			   Context *final_finish, epoch_t reply_epoch)
{
  ldout(cct, 10) << "_list_reply" << dendl;

  bufferlist::iterator iter = list_context->bl.begin();
  pg_nls_response_t response;
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
      list_nobjects(list_context, final_finish);
      return;
    }
  }
  if (!list_context->list.empty()) {
    ldout(cct, 20) << " returning results so far" << dendl;
    // release the listing context's budget once all
    // OPs (in the session) are finished
    put_nlist_context_budget(list_context);
    final_finish->complete(0);
    return;
  }

  // continue!
  list_nobjects(list_context, final_finish);
}

void Objecter::put_nlist_context_budget(NListContext *list_context) {
    if (list_context->ctx_budget >= 0) {
      ldout(cct, 10) << " release listing context's budget " << list_context->ctx_budget << dendl;
      put_op_budget_bytes(list_context->ctx_budget);
      list_context->ctx_budget = -1;
    }
  }

uint32_t Objecter::list_objects_seek(ListContext *list_context,
				     uint32_t pos)
{
  RWLock::RLocker rl(rwlock);
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

  rwlock.get_read();
  const pg_pool_t *pool = osdmap->get_pg_pool(list_context->pool_id);
  int pg_num = pool->get_pg_num();
  rwlock.unlock();

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
  RWLock::WLocker wl(rwlock);
  ldout(cct, 10) << "create_pool_snap; pool: " << pool << "; snap: " << snap_name << dendl;

  const pg_pool_t *p = osdmap->get_pg_pool(pool);
  if (!p)
    return -EINVAL;
  if (p->snap_exists(snap_name.c_str()))
    return -EEXIST;

  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = last_tid.inc();
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
  RWLock::WLocker wl(rwlock);
  ldout(cct, 10) << "allocate_selfmanaged_snap; pool: " << pool << dendl;
  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = last_tid.inc();
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
  RWLock::WLocker wl(rwlock);
  ldout(cct, 10) << "delete_pool_snap; pool: " << pool << "; snap: " << snap_name << dendl;

  const pg_pool_t *p = osdmap->get_pg_pool(pool);
  if (!p)
    return -EINVAL;
  if (!p->snap_exists(snap_name.c_str()))
    return -ENOENT;

  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = last_tid.inc();
  op->pool = pool;
  op->name = snap_name;
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_DELETE_SNAP;
  pool_ops[op->tid] = op;
  
  pool_op_submit(op);
  
  return 0;
}

int Objecter::delete_selfmanaged_snap(int64_t pool, snapid_t snap,
				      Context *onfinish)
{
  RWLock::WLocker wl(rwlock);
  ldout(cct, 10) << "delete_selfmanaged_snap; pool: " << pool << "; snap: " 
	   << snap << dendl;
  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = last_tid.inc();
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
  RWLock::WLocker wl(rwlock);
  ldout(cct, 10) << "create_pool name=" << name << dendl;

  if (osdmap->lookup_pg_pool_name(name.c_str()) >= 0)
    return -EEXIST;

  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = last_tid.inc();
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
  RWLock::WLocker wl(rwlock);
  ldout(cct, 10) << "delete_pool " << pool << dendl;

  if (!osdmap->have_pg_pool(pool))
    return -ENOENT;

  _do_delete_pool(pool, onfinish);
  return 0;
}

int Objecter::delete_pool(const string &pool_name, Context *onfinish)
{
  RWLock::WLocker wl(rwlock);
  ldout(cct, 10) << "delete_pool " << pool_name << dendl;

  int64_t pool = osdmap->lookup_pg_pool_name(pool_name);
  if (pool < 0)
    return pool;

  _do_delete_pool(pool, onfinish);
  return 0;
}

void Objecter::_do_delete_pool(int64_t pool, Context *onfinish)
{
  PoolOp *op = new PoolOp;
  op->tid = last_tid.inc();
  op->pool = pool;
  op->name = "delete";
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_DELETE;
  pool_ops[op->tid] = op;
  pool_op_submit(op);
}

/**
 * change the auid owner of a pool by contacting the monitor.
 * This requires the current connection to have write permissions
 * on both the pool's current auid and the new (parameter) auid.
 * Uses the standard Context callback when done.
 */
int Objecter::change_pool_auid(int64_t pool, Context *onfinish, uint64_t auid)
{
  RWLock::WLocker wl(rwlock);
  ldout(cct, 10) << "change_pool_auid " << pool << " to " << auid << dendl;
  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = last_tid.inc();
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
    objecter->pool_op_cancel(tid, -ETIMEDOUT);
  }
};

void Objecter::pool_op_submit(PoolOp *op)
{
  assert(rwlock.is_locked());
  if (mon_timeout > 0) {
    Mutex::Locker l(timer_lock);
    op->ontimeout = new C_CancelPoolOp(op->tid, this);
    timer.add_event_after(mon_timeout, op->ontimeout);
  }
  _pool_op_submit(op);
}

void Objecter::_pool_op_submit(PoolOp *op)
{
  assert(rwlock.is_wlocked());

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
  rwlock.get_read();
  if (!initialized.read()) {
    rwlock.put_read();
    m->put();
    return;
  }

  ldout(cct, 10) << "handle_pool_op_reply " << *m << dendl;
  ceph_tid_t tid = m->get_tid();
  map<ceph_tid_t, PoolOp *>::iterator iter = pool_ops.find(tid);
  if (iter != pool_ops.end()) {
    PoolOp *op = iter->second;
    ldout(cct, 10) << "have request " << tid << " at " << op << " Op: " << ceph_pool_op_name(op->pool_op) << dendl;
    if (op->blp)
      op->blp->claim(m->response_data);
    if (m->version > last_seen_osdmap_version)
      last_seen_osdmap_version = m->version;
    if (osdmap->get_epoch() < m->epoch) {
      rwlock.unlock();
      rwlock.get_write();
      if (osdmap->get_epoch() < m->epoch) {
        ldout(cct, 20) << "waiting for client to reach epoch " << m->epoch << " before calling back" << dendl;
        _wait_for_new_map(op->onfinish, m->epoch, m->replyCode);
      }
    }
    else {
      op->onfinish->complete(m->replyCode);
    }
    op->onfinish = NULL;
    if (!rwlock.is_wlocked()) {
      rwlock.unlock();
      rwlock.get_write();
    }
    iter = pool_ops.find(tid);
    if (iter != pool_ops.end()) {
      _finish_pool_op(op);
    }
  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  }
  rwlock.unlock();

  ldout(cct, 10) << "done" << dendl;
  m->put();
}

int Objecter::pool_op_cancel(ceph_tid_t tid, int r)
{
  assert(initialized.read());

  RWLock::WLocker wl(rwlock);

  map<ceph_tid_t, PoolOp*>::iterator it = pool_ops.find(tid);
  if (it == pool_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  PoolOp *op = it->second;
  if (op->onfinish)
    op->onfinish->complete(r);

  _finish_pool_op(op);
  return 0;
}

void Objecter::_finish_pool_op(PoolOp *op)
{
  assert(rwlock.is_wlocked());
  pool_ops.erase(op->tid);
  logger->set(l_osdc_poolop_active, pool_ops.size());

  if (op->ontimeout) {
    Mutex::Locker l(timer_lock);
    timer.cancel_event(op->ontimeout);
  }

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
  op->tid = last_tid.inc();
  op->pools = pools;
  op->pool_stats = result;
  op->onfinish = onfinish;
  op->ontimeout = NULL;
  if (mon_timeout > 0) {
    Mutex::Locker l(timer_lock);
    op->ontimeout = new C_CancelPoolStatOp(op->tid, this);
    timer.add_event_after(mon_timeout, op->ontimeout);
  }

  RWLock::WLocker wl(rwlock);

  poolstat_ops[op->tid] = op;

  logger->set(l_osdc_poolstat_active, poolstat_ops.size());

  _poolstat_submit(op);
}

void Objecter::_poolstat_submit(PoolStatOp *op)
{
  ldout(cct, 10) << "_poolstat_submit " << op->tid << dendl;
  monc->send_mon_message(new MGetPoolStats(monc->get_fsid(), op->tid, op->pools, last_seen_pgmap_version));
  op->last_submit = ceph_clock_now(cct);

  logger->inc(l_osdc_poolstat_send);
}

void Objecter::handle_get_pool_stats_reply(MGetPoolStatsReply *m)
{
  ldout(cct, 10) << "handle_get_pool_stats_reply " << *m << dendl;
  ceph_tid_t tid = m->get_tid();

  RWLock::WLocker wl(rwlock);
  if (!initialized.read()) {
    m->put();
    return;
  }

  map<ceph_tid_t, PoolStatOp *>::iterator iter = poolstat_ops.find(tid);
  if (iter != poolstat_ops.end()) {
    PoolStatOp *op = poolstat_ops[tid];
    ldout(cct, 10) << "have request " << tid << " at " << op << dendl;
    *op->pool_stats = m->pool_stats;
    if (m->version > last_seen_pgmap_version) {
      last_seen_pgmap_version = m->version;
    }
    op->onfinish->complete(0);
    _finish_pool_stat_op(op);
  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  } 
  ldout(cct, 10) << "done" << dendl;
  m->put();
}

int Objecter::pool_stat_op_cancel(ceph_tid_t tid, int r)
{
  assert(initialized.read());

  RWLock::WLocker wl(rwlock);

  map<ceph_tid_t, PoolStatOp*>::iterator it = poolstat_ops.find(tid);
  if (it == poolstat_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  PoolStatOp *op = it->second;
  if (op->onfinish)
    op->onfinish->complete(r);
  _finish_pool_stat_op(op);
  return 0;
}

void Objecter::_finish_pool_stat_op(PoolStatOp *op)
{
  assert(rwlock.is_wlocked());

  poolstat_ops.erase(op->tid);
  logger->set(l_osdc_poolstat_active, poolstat_ops.size());

  if (op->ontimeout) {
    Mutex::Locker l(timer_lock);
    timer.cancel_event(op->ontimeout);
  }

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
    objecter->statfs_op_cancel(tid, -ETIMEDOUT);
  }
};

void Objecter::get_fs_stats(ceph_statfs& result, Context *onfinish)
{
  ldout(cct, 10) << "get_fs_stats" << dendl;
  RWLock::WLocker l(rwlock);

  StatfsOp *op = new StatfsOp;
  op->tid = last_tid.inc();
  op->stats = &result;
  op->onfinish = onfinish;
  op->ontimeout = NULL;
  if (mon_timeout > 0) {
    Mutex::Locker l(timer_lock);
    op->ontimeout = new C_CancelStatfsOp(op->tid, this);
    timer.add_event_after(mon_timeout, op->ontimeout);
  }
  statfs_ops[op->tid] = op;

  logger->set(l_osdc_statfs_active, statfs_ops.size());

  _fs_stats_submit(op);
}

void Objecter::_fs_stats_submit(StatfsOp *op)
{
  assert(rwlock.is_wlocked());

  ldout(cct, 10) << "fs_stats_submit" << op->tid << dendl;
  monc->send_mon_message(new MStatfs(monc->get_fsid(), op->tid, last_seen_pgmap_version));
  op->last_submit = ceph_clock_now(cct);

  logger->inc(l_osdc_statfs_send);
}

void Objecter::handle_fs_stats_reply(MStatfsReply *m)
{
  RWLock::WLocker wl(rwlock);
  if (!initialized.read()) {
    m->put();
    return;
  }

  ldout(cct, 10) << "handle_fs_stats_reply " << *m << dendl;
  ceph_tid_t tid = m->get_tid();

  if (statfs_ops.count(tid)) {
    StatfsOp *op = statfs_ops[tid];
    ldout(cct, 10) << "have request " << tid << " at " << op << dendl;
    *(op->stats) = m->h.st;
    if (m->h.version > last_seen_pgmap_version)
      last_seen_pgmap_version = m->h.version;
    op->onfinish->complete(0);
    _finish_statfs_op(op);
  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  }
  m->put();
  ldout(cct, 10) << "done" << dendl;
}

int Objecter::statfs_op_cancel(ceph_tid_t tid, int r)
{
  assert(initialized.read());

  RWLock::WLocker wl(rwlock);

  map<ceph_tid_t, StatfsOp*>::iterator it = statfs_ops.find(tid);
  if (it == statfs_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  StatfsOp *op = it->second;
  if (op->onfinish)
    op->onfinish->complete(r);
  _finish_statfs_op(op);
  return 0;
}

void Objecter::_finish_statfs_op(StatfsOp *op)
{
  assert(rwlock.is_wlocked());

  statfs_ops.erase(op->tid);
  logger->set(l_osdc_statfs_active, statfs_ops.size());

  if (op->ontimeout) {
    Mutex::Locker l(timer_lock);
    timer.cancel_event(op->ontimeout);
  }

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
  if (!initialized.read())
    return;

  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
    resend_mon_ops();
}

bool Objecter::ms_handle_reset(Connection *con)
{
  if (!initialized.read())
    return false;
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
    //
    int osd = osdmap->identify_osd(con->get_peer_addr());
    if (osd >= 0) {
      ldout(cct, 1) << "ms_handle_reset on osd." << osd << dendl;
      rwlock.get_write();
      if (!initialized.read()) {
	rwlock.put_write();
	return false;
      }
      map<int,OSDSession*>::iterator p = osd_sessions.find(osd);
      if (p != osd_sessions.end()) {
	OSDSession *session = p->second;
        map<uint64_t, LingerOp *> lresend;
        session->lock.get_write();
	_reopen_session(session);
	_kick_requests(session, lresend);
        session->lock.unlock();
        _linger_ops_resend(lresend);
        rwlock.unlock();
	maybe_request_map();
      } else {
        rwlock.unlock();
      }
    } else {
      ldout(cct, 10) << "ms_handle_reset on unknown osd addr " << con->get_peer_addr() << dendl;
    }
    return true;
  }
  return false;
}

void Objecter::ms_handle_remote_reset(Connection *con)
{
  /*
   * treat these the same.
   */
  ms_handle_reset(con);
}

bool Objecter::ms_get_authorizer(int dest_type,
				 AuthAuthorizer **authorizer,
				 bool force_new)
{
  if (!initialized.read())
    return false;
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;
  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
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

void Objecter::_dump_active(OSDSession *s)
{
  for (map<ceph_tid_t,Op*>::iterator p = s->ops.begin(); p != s->ops.end(); ++p) {
    Op *op = p->second;
    ldout(cct, 20) << op->tid << "\t" << op->target.pgid
		   << "\tosd." << (op->session ? op->session->osd : -1)
		   << "\t" << op->target.base_oid
		   << "\t" << op->ops << dendl;
  }
}

void Objecter::_dump_active()
{
  ldout(cct, 20) << "dump_active .. " << num_homeless_ops.read() << " homeless" << dendl;
  for (map<int, OSDSession *>::iterator siter = osd_sessions.begin(); siter != osd_sessions.end(); ++siter) {
    OSDSession *s = siter->second;
    s->lock.get_read();
    _dump_active(s);
    s->lock.unlock();
  }
  _dump_active(homeless_session);
}

void Objecter::dump_active()
{
  rwlock.get_read();
  _dump_active();
  rwlock.unlock();
}

void Objecter::dump_requests(Formatter *fmt)
{
  fmt->open_object_section("requests");
  dump_ops(fmt);
  dump_linger_ops(fmt);
  dump_pool_ops(fmt);
  dump_pool_stat_ops(fmt);
  dump_statfs_ops(fmt);
  dump_command_ops(fmt);
  fmt->close_section(); // requests object
}

void Objecter::_dump_ops(const OSDSession *s, Formatter *fmt)
{
  for (map<ceph_tid_t,Op*>::const_iterator p = s->ops.begin();
       p != s->ops.end();
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
}

void Objecter::dump_ops(Formatter *fmt)
{
  fmt->open_array_section("ops");
  rwlock.get_read();
  for (map<int, OSDSession *>::const_iterator siter = osd_sessions.begin(); siter != osd_sessions.end(); ++siter) {
    OSDSession *s = siter->second;
    s->lock.get_read();
    _dump_ops(s, fmt);
    s->lock.unlock();
  }
  rwlock.unlock();
  _dump_ops(homeless_session, fmt);
  fmt->close_section(); // ops array
}

void Objecter::_dump_linger_ops(const OSDSession *s, Formatter *fmt)
{
  for (map<uint64_t, LingerOp*>::const_iterator p = s->linger_ops.begin();
       p != s->linger_ops.end();
       ++p) {
    LingerOp *op = p->second;
    fmt->open_object_section("linger_op");
    fmt->dump_unsigned("linger_id", op->linger_id);
    op->target.dump(fmt);
    fmt->dump_stream("snapid") << op->snap;
    fmt->dump_stream("registered") << op->registered;
    fmt->close_section(); // linger_op object
  }
}

void Objecter::dump_linger_ops(Formatter *fmt)
{
  fmt->open_array_section("linger_ops");
  rwlock.get_read();
  for (map<int, OSDSession *>::const_iterator siter = osd_sessions.begin(); siter != osd_sessions.end(); ++siter) {
    OSDSession *s = siter->second;
    s->lock.get_read();
    _dump_linger_ops(s, fmt);
    s->lock.unlock();
  }
  rwlock.unlock();
  _dump_linger_ops(homeless_session, fmt);
  fmt->close_section(); // linger_ops array
}

void Objecter::_dump_command_ops(const OSDSession *s, Formatter *fmt)
{
  for (map<uint64_t, CommandOp*>::const_iterator p = s->command_ops.begin();
       p != s->command_ops.end();
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
}

void Objecter::dump_command_ops(Formatter *fmt)
{
  fmt->open_array_section("command_ops");
  rwlock.get_read();
  for (map<int, OSDSession *>::const_iterator siter = osd_sessions.begin(); siter != osd_sessions.end(); ++siter) {
    OSDSession *s = siter->second;
    s->lock.get_read();
    _dump_command_ops(s, fmt);
    s->lock.unlock();
  }
  rwlock.unlock();
  _dump_command_ops(homeless_session, fmt);
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
  RWLock::RLocker rl(m_objecter->rwlock);
  m_objecter->dump_requests(f);
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
  int osd_num = (int)m->get_source().num();

  RWLock::WLocker wl(rwlock);
  if (!initialized.read()) {
    m->put();
    return;
  }

  map<int, OSDSession *>::iterator siter = osd_sessions.find(osd_num);
  if (siter == osd_sessions.end()) {
    ldout(cct, 10) << "handle_command_reply tid " << m->get_tid() << " osd not found" << dendl;
    m->put();
    return;
  }

  OSDSession *s = siter->second;

  s->lock.get_read();
  map<ceph_tid_t,CommandOp*>::iterator p = s->command_ops.find(m->get_tid());
  if (p == s->command_ops.end()) {
    ldout(cct, 10) << "handle_command_reply tid " << m->get_tid() << " not found" << dendl;
    m->put();
    s->lock.unlock();
    return;
  }

  CommandOp *c = p->second;
  if (!c->session ||
      m->get_connection() != c->session->con) {
    ldout(cct, 10) << "handle_command_reply tid " << m->get_tid() << " got reply from wrong connection "
		   << m->get_connection() << " " << m->get_source_inst() << dendl;
    m->put();
    s->lock.unlock();
    return;
  }
  if (c->poutbl)
    c->poutbl->claim(m->get_data());

  s->lock.unlock();


  _finish_command(c, m->r, m->rs);
  m->put();
}

class C_CancelCommandOp : public Context
{
  Objecter::OSDSession *s;
  ceph_tid_t tid;
  Objecter *objecter;
public:
  C_CancelCommandOp(Objecter::OSDSession *s, ceph_tid_t tid, Objecter *objecter) : s(s), tid(tid),
						     objecter(objecter) {}
  void finish(int r) {
    objecter->command_op_cancel(s, tid, -ETIMEDOUT);
  }
};

int Objecter::submit_command(CommandOp *c, ceph_tid_t *ptid)
{
  RWLock::WLocker wl(rwlock);

  RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

  ceph_tid_t tid = last_tid.inc();
  ldout(cct, 10) << "_submit_command " << tid << " " << c->cmd << dendl;
  c->tid = tid;

  {
   RWLock::WLocker hs_wl(homeless_session->lock);
  _session_command_op_assign(homeless_session, c);
  }

  (void)_calc_command_target(c);
  _assign_command_session(c);
  if (osd_timeout > 0) {
    Mutex::Locker l(timer_lock);
    c->ontimeout = new C_CancelCommandOp(c->session, tid, this);
    timer.add_event_after(osd_timeout, c->ontimeout);
  }

  if (!c->session->is_homeless()) {
    _send_command(c);
  } else {
    int r = _maybe_request_map();
    assert(r != -EAGAIN); /* because rwlock is already write-locked */
  }
  if (c->map_check_error)
    _send_command_map_check(c);
  *ptid = tid;

  logger->inc(l_osdc_command_active);

  return 0;
}

int Objecter::_calc_command_target(CommandOp *c)
{
  assert(rwlock.is_wlocked());

  RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

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
    c->osd = c->target_osd;
  } else {
    if (!osdmap->have_pg_pool(c->target_pg.pool())) {
      c->map_check_error = -ENOENT;
      c->map_check_error_str = "pool dne";
      return RECALC_OP_TARGET_POOL_DNE;
    }
    vector<int> acting;
    osdmap->pg_to_acting_osds(c->target_pg, &acting, &c->osd);
  }

  OSDSession *s;
  int r = _get_session(c->osd, &s, lc);
  assert(r != -EAGAIN); /* shouldn't happen as we're holding the write lock */

  if (c->session != s) {
    put_session(s);
    return RECALC_OP_TARGET_NEED_RESEND;
  }

  put_session(s);

  ldout(cct, 20) << "_recalc_command_target " << c->tid << " no change, " << c->session << dendl;

  return RECALC_OP_TARGET_NO_ACTION;
}

void Objecter::_assign_command_session(CommandOp *c)
{
  assert(rwlock.is_wlocked());

  RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

  OSDSession *s;
  int r = _get_session(c->osd, &s, lc);
  assert(r != -EAGAIN); /* shouldn't happen as we're holding the write lock */

  if (c->session != s) {
    if (c->session) {
      OSDSession *cs = c->session;
      cs->lock.get_write();
      _session_command_op_remove(c->session, c);
      cs->lock.unlock();
    }
    s->lock.get_write();
    _session_command_op_assign(s, c);
    s->lock.unlock();
  }

  put_session(s);
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
  c->session->con->send_message(m);
  logger->inc(l_osdc_command_send);
}

int Objecter::command_op_cancel(OSDSession *s, ceph_tid_t tid, int r)
{
  assert(initialized.read());

  RWLock::WLocker wl(rwlock);

  map<ceph_tid_t, CommandOp*>::iterator it = s->command_ops.find(tid);
  if (it == s->command_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  CommandOp *op = it->second;
  _command_cancel_map_check(op);
  _finish_command(op, -ETIMEDOUT, "");
  return 0;
}

void Objecter::_finish_command(CommandOp *c, int r, string rs)
{
  assert(rwlock.is_wlocked());

  ldout(cct, 10) << "_finish_command " << c->tid << " = " << r << " " << rs << dendl;
  if (c->prs)
    *c->prs = rs;
  if (c->onfinish)
    c->onfinish->complete(r);

  if (c->ontimeout) {
    Mutex::Locker l(timer_lock);
    timer.cancel_event(c->ontimeout);
  }

  OSDSession *s = c->session;
  s->lock.get_write();
  _session_command_op_remove(c->session, c);
  s->lock.unlock();

  c->put();

  logger->dec(l_osdc_command_active);
}

Objecter::OSDSession::~OSDSession()
{
  // Caller is responsible for re-assigning or
  // destroying any ops that were assigned to us
  assert(ops.empty());
  assert(linger_ops.empty());
  assert(command_ops.empty());

  for (int i = 0; i < num_locks; i++) {
    delete completion_locks[i];
  }
  delete[] completion_locks;
}

Objecter::~Objecter()
{
  delete osdmap;

  assert(homeless_session->get_nref() == 1);
  assert(num_homeless_ops.read() == 0);
  homeless_session->put();

  assert(osd_sessions.empty());
  assert(poolstat_ops.empty());
  assert(statfs_ops.empty());
  assert(pool_ops.empty());
  assert(waiting_for_map.empty());
  assert(linger_ops.empty());
  assert(check_latest_map_lingers.empty());
  assert(check_latest_map_ops.empty());
  assert(check_latest_map_commands.empty());

  assert(!tick_event);
  assert(!m_request_state_hook);
  assert(!logger);
}

