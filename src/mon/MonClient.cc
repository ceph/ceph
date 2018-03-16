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

#include <random>

#include "include/scope_guard.h"
#include "include/stringify.h"

#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MMonMap.h"
#include "messages/MConfig.h"
#include "messages/MGetConfig.h"
#include "messages/MAuth.h"
#include "messages/MLogAck.h"
#include "messages/MAuthReply.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MPing.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"
#include "common/errno.h"
#include "common/LogClient.h"

#include "MonClient.h"
#include "MonMap.h"

#include "auth/Auth.h"
#include "auth/KeyRing.h"
#include "auth/AuthClientHandler.h"
#include "auth/AuthMethodList.h"
#include "auth/RotatingKeyRing.h"

#define dout_subsys ceph_subsys_monc
#undef dout_prefix
#define dout_prefix *_dout << "monclient" << (_hunting() ? "(hunting)":"") << ": "

MonClient::MonClient(CephContext *cct_) :
  Dispatcher(cct_),
  messenger(NULL),
  monc_lock("MonClient::monc_lock"),
  timer(cct_, monc_lock),
  finisher(cct_),
  initialized(false),
  no_keyring_disabled_cephx(false),
  log_client(NULL),
  more_log_pending(false),
  want_monmap(true),
  had_a_connection(false),
  reopen_interval_multiplier(
    cct_->_conf->get_val<double>("mon_client_hunt_interval_min_multiple")),
  last_mon_command_tid(0),
  version_req_id(0)
{
}

MonClient::~MonClient()
{
}

int MonClient::build_initial_monmap()
{
  ldout(cct, 10) << __func__ << dendl;
  return monmap.build_initial(cct, cerr);
}

int MonClient::get_monmap()
{
  ldout(cct, 10) << __func__ << dendl;
  Mutex::Locker l(monc_lock);
  
  _sub_want("monmap", 0, 0);
  if (!_opened())
    _reopen_session();

  while (want_monmap)
    map_cond.Wait(monc_lock);

  ldout(cct, 10) << __func__ << " done" << dendl;
  return 0;
}

int MonClient::get_monmap_and_config()
{
  ldout(cct, 10) << __func__ << dendl;
  assert(!messenger);

  int tries = 10;

  utime_t interval;
  interval.set_from_double(cct->_conf->mon_client_hunt_interval);

  cct->init_crypto();

  int r = build_initial_monmap();
  if (r < 0) {
    lderr(cct) << __func__ << " cannot identify monitors to contact" << dendl;
    goto out;
  }

  messenger = Messenger::create_client_messenger(
    cct, "temp_mon_client");
  assert(messenger);
  messenger->add_dispatcher_head(this);
  messenger->start();

  while (tries-- > 0) {
    r = init();
    if (r < 0) {
      goto out_msgr;
    }
    r = authenticate(cct->_conf->client_mount_timeout);
    if (r == -ETIMEDOUT) {
      shutdown();
      continue;
    }
    if (r < 0) {
      goto out_shutdown;
    }
    if (!monmap.persistent_features.contains_all(
	  ceph::features::mon::FEATURE_MIMIC)) {
      ldout(cct,10) << __func__ << " pre-mimic monitor, no config to fetch"
		    << dendl;
      r = 0;
      break;
    }
    {
      Mutex::Locker l(monc_lock);
      while (!got_config && r == 0) {
	ldout(cct,20) << __func__ << " waiting for config" << dendl;
	r = map_cond.WaitInterval(monc_lock, interval);
      }
      if (got_config) {
	ldout(cct,10) << __func__ << " success" << dendl;
	r = 0;
	break;
      }
    }
    lderr(cct) << __func__ << " failed to get config" << dendl;
    shutdown();
    continue;
  }

out_shutdown:
  shutdown();

out_msgr:
  messenger->shutdown();
  messenger->wait();
  delete messenger;
  messenger = nullptr;

  if (!monmap.fsid.is_zero()) {
    cct->_conf->set_val("fsid", stringify(monmap.fsid));
  }

out:
  cct->shutdown_crypto();
  return r;
}


/**
 * Ping the monitor with id @p mon_id and set the resulting reply in
 * the provided @p result_reply, if this last parameter is not NULL.
 *
 * So that we don't rely on the MonClient's default messenger, set up
 * during connect(), we create our own messenger to comunicate with the
 * specified monitor.  This is advantageous in the following ways:
 *
 * - Isolate the ping procedure from the rest of the MonClient's operations,
 *   allowing us to not acquire or manage the big monc_lock, thus not
 *   having to block waiting for some other operation to finish before we
 *   can proceed.
 *   * for instance, we can ping mon.FOO even if we are currently hunting
 *     or blocked waiting for auth to complete with mon.BAR.
 *
 * - Ping a monitor prior to establishing a connection (using connect())
 *   and properly establish the MonClient's messenger.  This frees us
 *   from dealing with the complex foo that happens in connect().
 *
 * We also don't rely on MonClient as a dispatcher for this messenger,
 * unlike what happens with the MonClient's default messenger.  This allows
 * us to sandbox the whole ping, having it much as a separate entity in
 * the MonClient class, considerably simplifying the handling and dispatching
 * of messages without needing to consider monc_lock.
 *
 * Current drawback is that we will establish a messenger for each ping
 * we want to issue, instead of keeping a single messenger instance that
 * would be used for all pings.
 */
int MonClient::ping_monitor(const string &mon_id, string *result_reply)
{
  ldout(cct, 10) << __func__ << dendl;

  string new_mon_id;
  if (monmap.contains("noname-"+mon_id)) {
    new_mon_id = "noname-"+mon_id;
  } else {
    new_mon_id = mon_id;
  }

  if (new_mon_id.empty()) {
    ldout(cct, 10) << __func__ << " specified mon id is empty!" << dendl;
    return -EINVAL;
  } else if (!monmap.contains(new_mon_id)) {
    ldout(cct, 10) << __func__ << " no such monitor 'mon." << new_mon_id << "'"
                   << dendl;
    return -ENOENT;
  }

  MonClientPinger *pinger = new MonClientPinger(cct, result_reply);

  Messenger *smsgr = Messenger::create_client_messenger(cct, "temp_ping_client");
  smsgr->add_dispatcher_head(pinger);
  smsgr->start();

  ConnectionRef con = smsgr->get_connection(monmap.get_inst(new_mon_id));
  ldout(cct, 10) << __func__ << " ping mon." << new_mon_id
                 << " " << con->get_peer_addr() << dendl;
  con->send_message(new MPing);

  pinger->lock.Lock();
  int ret = pinger->wait_for_reply(cct->_conf->client_mount_timeout);
  if (ret == 0) {
    ldout(cct,10) << __func__ << " got ping reply" << dendl;
  } else {
    ret = -ret;
  }
  pinger->lock.Unlock();

  con->mark_down();
  smsgr->shutdown();
  smsgr->wait();
  delete smsgr;
  delete pinger;
  return ret;
}

bool MonClient::ms_dispatch(Message *m)
{
  if (my_addr == entity_addr_t())
    my_addr = messenger->get_myaddr();

  // we only care about these message types
  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
  case CEPH_MSG_AUTH_REPLY:
  case CEPH_MSG_MON_SUBSCRIBE_ACK:
  case CEPH_MSG_MON_GET_VERSION_REPLY:
  case MSG_MON_COMMAND_ACK:
  case MSG_LOGACK:
  case MSG_CONFIG:
    break;
  default:
    return false;
  }

  Mutex::Locker lock(monc_lock);

  if (_hunting()) {
    auto pending_con = pending_cons.find(m->get_source_addr());
    if (pending_con == pending_cons.end() ||
	pending_con->second.get_con() != m->get_connection()) {
      // ignore any messages outside hunting sessions
      ldout(cct, 10) << "discarding stray monitor message " << *m << dendl;
      m->put();
      return true;
    }
  } else if (!active_con || active_con->get_con() != m->get_connection()) {
    // ignore any messages outside our session(s)
    ldout(cct, 10) << "discarding stray monitor message " << *m << dendl;
    m->put();
    return true;
  }

  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    handle_monmap(static_cast<MMonMap*>(m));
    if (passthrough_monmap) {
      return false;
    } else {
      m->put();
    }
    break;
  case CEPH_MSG_AUTH_REPLY:
    handle_auth(static_cast<MAuthReply*>(m));
    break;
  case CEPH_MSG_MON_SUBSCRIBE_ACK:
    handle_subscribe_ack(static_cast<MMonSubscribeAck*>(m));
    break;
  case CEPH_MSG_MON_GET_VERSION_REPLY:
    handle_get_version_reply(static_cast<MMonGetVersionReply*>(m));
    break;
  case MSG_MON_COMMAND_ACK:
    handle_mon_command_ack(static_cast<MMonCommandAck*>(m));
    break;
  case MSG_LOGACK:
    if (log_client) {
      log_client->handle_log_ack(static_cast<MLogAck*>(m));
      m->put();
      if (more_log_pending) {
	send_log();
      }
    } else {
      m->put();
    }
    break;
  case MSG_CONFIG:
    handle_config(static_cast<MConfig*>(m));
    break;
  }
  return true;
}

void MonClient::send_log(bool flush)
{
  if (log_client) {
    Message *lm = log_client->get_mon_log_message(flush);
    if (lm)
      _send_mon_message(lm);
    more_log_pending = log_client->are_pending();
  }
}

void MonClient::flush_log()
{
  Mutex::Locker l(monc_lock);
  send_log();
}

/* Unlike all the other message-handling functions, we don't put away a reference
* because we want to support MMonMap passthrough to other Dispatchers. */
void MonClient::handle_monmap(MMonMap *m)
{
  ldout(cct, 10) << __func__ << " " << *m << dendl;
  auto peer = m->get_source_addr();
  string cur_mon = monmap.get_name(peer);

  bufferlist::iterator p = m->monmapbl.begin();
  decode(monmap, p);

  ldout(cct, 10) << " got monmap " << monmap.epoch
		 << ", mon." << cur_mon << " is now rank " << monmap.get_rank(cur_mon)
		 << dendl;
  ldout(cct, 10) << "dump:\n";
  monmap.print(*_dout);
  *_dout << dendl;

  _sub_got("monmap", monmap.get_epoch());

  if (!monmap.get_addr_name(peer, cur_mon)) {
    ldout(cct, 10) << "mon." << cur_mon << " went away" << dendl;
    // can't find the mon we were talking to (above)
    _reopen_session();
  }

  map_cond.Signal();
  want_monmap = false;
}

void MonClient::handle_config(MConfig *m)
{
  ldout(cct,10) << __func__ << " " << *m << dendl;
  cct->_conf->set_mon_vals(cct, m->config);
  m->put();
  got_config = true;
  map_cond.Signal();
}

// ----------------------

int MonClient::init()
{
  ldout(cct, 10) << __func__ << dendl;

  messenger->add_dispatcher_head(this);

  entity_name = cct->_conf->name;

  Mutex::Locker l(monc_lock);

  string method;
  if (!cct->_conf->auth_supported.empty())
    method = cct->_conf->auth_supported;
  else if (entity_name.get_type() == CEPH_ENTITY_TYPE_OSD ||
	   entity_name.get_type() == CEPH_ENTITY_TYPE_MDS ||
	   entity_name.get_type() == CEPH_ENTITY_TYPE_MON ||
	   entity_name.get_type() == CEPH_ENTITY_TYPE_MGR)
    method = cct->_conf->auth_cluster_required;
  else
    method = cct->_conf->auth_client_required;
  auth_supported.reset(new AuthMethodList(cct, method));
  ldout(cct, 10) << "auth_supported " << auth_supported->get_supported_set() << " method " << method << dendl;

  int r = 0;
  keyring.reset(new KeyRing); // initializing keyring anyway

  if (auth_supported->is_supported_auth(CEPH_AUTH_CEPHX)) {
    r = keyring->from_ceph_context(cct);
    if (r == -ENOENT) {
      auth_supported->remove_supported_auth(CEPH_AUTH_CEPHX);
      if (!auth_supported->get_supported_set().empty()) {
	r = 0;
	no_keyring_disabled_cephx = true;
      } else {
	lderr(cct) << "ERROR: missing keyring, cannot use cephx for authentication" << dendl;
      }
    }
  }

  if (r < 0) {
    return r;
  }

  rotating_secrets.reset(
    new RotatingKeyRing(cct, cct->get_module_type(), keyring.get()));

  initialized = true;

  timer.init();
  finisher.start();
  schedule_tick();

  return 0;
}

void MonClient::shutdown()
{
  ldout(cct, 10) << __func__ << dendl;
  monc_lock.Lock();
  while (!version_requests.empty()) {
    version_requests.begin()->second->context->complete(-ECANCELED);
    ldout(cct, 20) << __func__ << " canceling and discarding version request "
		   << version_requests.begin()->second << dendl;
    delete version_requests.begin()->second;
    version_requests.erase(version_requests.begin());
  }
  while (!mon_commands.empty()) {
    auto tid = mon_commands.begin()->first;
    _cancel_mon_command(tid);
  }
  while (!waiting_for_session.empty()) {
    ldout(cct, 20) << __func__ << " discarding pending message " << *waiting_for_session.front() << dendl;
    waiting_for_session.front()->put();
    waiting_for_session.pop_front();
  }

  active_con.reset();
  pending_cons.clear();
  auth.reset();

  monc_lock.Unlock();

  if (initialized) {
    finisher.wait_for_empty();
    finisher.stop();
    initialized = false;
  }
  monc_lock.Lock();
  timer.shutdown();

  monc_lock.Unlock();
}

int MonClient::authenticate(double timeout)
{
  Mutex::Locker lock(monc_lock);

  if (active_con) {
    ldout(cct, 5) << "already authenticated" << dendl;
    return 0;
  }

  _sub_want("monmap", monmap.get_epoch() ? monmap.get_epoch() + 1 : 0, 0);
  _sub_want("config", 0, 0);
  if (!_opened())
    _reopen_session();

  utime_t until = ceph_clock_now();
  until += timeout;
  if (timeout > 0.0)
    ldout(cct, 10) << "authenticate will time out at " << until << dendl;
  authenticate_err = 0;
  while (!active_con && !authenticate_err) {
    if (timeout > 0.0) {
      int r = auth_cond.WaitUntil(monc_lock, until);
      if (r == ETIMEDOUT && !active_con) {
	ldout(cct, 0) << "authenticate timed out after " << timeout << dendl;
	authenticate_err = -r;
      }
    } else {
      auth_cond.Wait(monc_lock);
    }
  }

  if (active_con) {
    ldout(cct, 5) << __func__ << " success, global_id "
		  << active_con->get_global_id() << dendl;
    // active_con should not have been set if there was an error
    assert(authenticate_err == 0);
    authenticated = true;
  }

  if (authenticate_err < 0 && no_keyring_disabled_cephx) {
    lderr(cct) << __func__ << " NOTE: no keyring found; disabled cephx authentication" << dendl;
  }

  return authenticate_err;
}

void MonClient::handle_auth(MAuthReply *m)
{
  assert(monc_lock.is_locked());
  if (!_hunting()) {
    std::swap(active_con->get_auth(), auth);
    int ret = active_con->authenticate(m);
    m->put();
    std::swap(auth, active_con->get_auth());
    if (global_id != active_con->get_global_id()) {
      lderr(cct) << __func__ << " peer assigned me a different global_id: "
		 << active_con->get_global_id() << dendl;
    }
    if (ret != -EAGAIN) {
      _finish_auth(ret);
    }
    return;
  }

  // hunting
  auto found = pending_cons.find(m->get_source_addr());
  assert(found != pending_cons.end());
  int auth_err = found->second.handle_auth(m, entity_name, want_keys,
					   rotating_secrets.get());
  m->put();
  if (auth_err == -EAGAIN) {
    return;
  }
  if (auth_err) {
    pending_cons.erase(found);
    if (!pending_cons.empty()) {
      // keep trying with pending connections
      return;
    }
    // the last try just failed, give up.
  } else {
    auto& mc = found->second;
    assert(mc.have_session());
    active_con.reset(new MonConnection(std::move(mc)));
    pending_cons.clear();
  }

  _finish_hunting();

  if (!auth_err) {
    last_rotating_renew_sent = utime_t();
    while (!waiting_for_session.empty()) {
      _send_mon_message(waiting_for_session.front());
      waiting_for_session.pop_front();
    }
    _resend_mon_commands();
    send_log(true);
    if (active_con) {
      std::swap(auth, active_con->get_auth());
      global_id = active_con->get_global_id();
    }
  }
  _finish_auth(auth_err);
  if (!auth_err) {
    Context *cb = nullptr;
    if (session_established_context) {
      cb = session_established_context.release();
    }
    if (cb) {
      monc_lock.Unlock();
      cb->complete(0);
      monc_lock.Lock();
    }
  }
}

void MonClient::_finish_auth(int auth_err)
{
  authenticate_err = auth_err;
  // _resend_mon_commands() could _reopen_session() if the connected mon is not
  // the one the MonCommand is targeting.
  if (!auth_err && active_con) {
    assert(auth);
    _check_auth_tickets();
  }
  auth_cond.SignalAll();
}

// ---------

void MonClient::_send_mon_message(Message *m)
{
  assert(monc_lock.is_locked());
  if (active_con) {
    auto cur_con = active_con->get_con();
    ldout(cct, 10) << "_send_mon_message to mon."
		   << monmap.get_name(cur_con->get_peer_addr())
		   << " at " << cur_con->get_peer_addr() << dendl;
    cur_con->send_message(m);
  } else {
    waiting_for_session.push_back(m);
  }
}

void MonClient::_reopen_session(int rank)
{
  assert(monc_lock.is_locked());
  ldout(cct, 10) << __func__ << " rank " << rank << dendl;

  active_con.reset();
  pending_cons.clear();

  _start_hunting();

  if (rank >= 0) {
    _add_conn(rank, global_id);
  } else {
    _add_conns(global_id);
  }

  // throw out old queued messages
  while (!waiting_for_session.empty()) {
    waiting_for_session.front()->put();
    waiting_for_session.pop_front();
  }

  // throw out version check requests
  while (!version_requests.empty()) {
    finisher.queue(version_requests.begin()->second->context, -EAGAIN);
    delete version_requests.begin()->second;
    version_requests.erase(version_requests.begin());
  }

  for (auto& c : pending_cons) {
    c.second.start(monmap.get_epoch(), entity_name, *auth_supported);
  }

  for (map<string,ceph_mon_subscribe_item>::iterator p = sub_sent.begin();
       p != sub_sent.end();
       ++p) {
    if (sub_new.count(p->first) == 0)
      sub_new[p->first] = p->second;
  }
  if (!sub_new.empty())
    _renew_subs();
}

MonConnection& MonClient::_add_conn(unsigned rank, uint64_t global_id)
{
  auto peer = monmap.get_addr(rank);
  auto conn = messenger->get_connection(monmap.get_inst(rank));
  MonConnection mc(cct, conn, global_id);
  auto inserted = pending_cons.insert(make_pair(peer, move(mc)));
  ldout(cct, 10) << "picked mon." << monmap.get_name(rank)
                 << " con " << conn
                 << " addr " << conn->get_peer_addr()
                 << dendl;
  return inserted.first->second;
}

void MonClient::_add_conns(uint64_t global_id)
{
  uint16_t min_priority = std::numeric_limits<uint16_t>::max();
  for (const auto& m : monmap.mon_info) {
    if (m.second.priority < min_priority) {
      min_priority = m.second.priority;
    }
  }
  vector<unsigned> ranks;
  for (const auto& m : monmap.mon_info) {
    if (m.second.priority == min_priority) {
      ranks.push_back(monmap.get_rank(m.first));
    }
  }
  std::random_device rd;
  std::mt19937 rng(rd());
  std::shuffle(ranks.begin(), ranks.end(), rng);
  unsigned n = cct->_conf->mon_client_hunt_parallel;
  if (n == 0 || n > ranks.size()) {
    n = ranks.size();
  }
  for (unsigned i = 0; i < n; i++) {
    _add_conn(ranks[i], global_id);
  }
}

bool MonClient::ms_handle_reset(Connection *con)
{
  Mutex::Locker lock(monc_lock);

  if (con->get_peer_type() != CEPH_ENTITY_TYPE_MON)
    return false;

  if (_hunting()) {
    if (pending_cons.count(con->get_peer_addr())) {
      ldout(cct, 10) << __func__ << " hunted mon " << con->get_peer_addr() << dendl;
    } else {
      ldout(cct, 10) << __func__ << " stray mon " << con->get_peer_addr() << dendl;
    }
    return true;
  } else {
    if (active_con && con == active_con->get_con()) {
      ldout(cct, 10) << __func__ << " current mon " << con->get_peer_addr() << dendl;
      _reopen_session();
      return false;
    } else {
      ldout(cct, 10) << "ms_handle_reset stray mon " << con->get_peer_addr() << dendl;
      return true;
    }
  }
}

bool MonClient::_opened() const
{
  assert(monc_lock.is_locked());
  return active_con || _hunting();
}

bool MonClient::_hunting() const
{
  return !pending_cons.empty();
}

void MonClient::_start_hunting()
{
  assert(!_hunting());
  // adjust timeouts if necessary
  if (!had_a_connection)
    return;
  reopen_interval_multiplier *= cct->_conf->mon_client_hunt_interval_backoff;
  if (reopen_interval_multiplier >
      cct->_conf->mon_client_hunt_interval_max_multiple) {
    reopen_interval_multiplier =
      cct->_conf->mon_client_hunt_interval_max_multiple;
  }
}

void MonClient::_finish_hunting()
{
  assert(monc_lock.is_locked());
  // the pending conns have been cleaned.
  assert(!_hunting());
  if (active_con) {
    auto con = active_con->get_con();
    ldout(cct, 1) << "found mon."
		  << monmap.get_name(con->get_peer_addr())
		  << dendl;
  } else {
    ldout(cct, 1) << "no mon sessions established" << dendl;
  }

  had_a_connection = true;
  _un_backoff();
}

void MonClient::tick()
{
  ldout(cct, 10) << __func__ << dendl;

  auto reschedule_tick = make_scope_guard([this] {
      schedule_tick();
    });

  _check_auth_tickets();
  
  if (_hunting()) {
    ldout(cct, 1) << "continuing hunt" << dendl;
    return _reopen_session();
  } else if (active_con) {
    // just renew as needed
    utime_t now = ceph_clock_now();
    auto cur_con = active_con->get_con();
    if (!cur_con->has_feature(CEPH_FEATURE_MON_STATEFUL_SUB)) {
      ldout(cct, 10) << "renew subs? (now: " << now
		     << "; renew after: " << sub_renew_after << ") -- "
		     << (now > sub_renew_after ? "yes" : "no")
		     << dendl;
      if (now > sub_renew_after)
	_renew_subs();
    }

    cur_con->send_keepalive();

    if (cct->_conf->mon_client_ping_timeout > 0 &&
	cur_con->has_feature(CEPH_FEATURE_MSGR_KEEPALIVE2)) {
      utime_t lk = cur_con->get_last_keepalive_ack();
      utime_t interval = now - lk;
      if (interval > cct->_conf->mon_client_ping_timeout) {
	ldout(cct, 1) << "no keepalive since " << lk << " (" << interval
		      << " seconds), reconnecting" << dendl;
	return _reopen_session();
      }
      send_log();
    }

    _un_backoff();
  }
}

void MonClient::_un_backoff()
{
  // un-backoff our reconnect interval
  reopen_interval_multiplier = std::max(
    cct->_conf->get_val<double>("mon_client_hunt_interval_min_multiple"),
    reopen_interval_multiplier /
    cct->_conf->get_val<double>("mon_client_hunt_interval_backoff"));
  ldout(cct, 20) << __func__ << " reopen_interval_multipler now "
		 << reopen_interval_multiplier << dendl;
}

void MonClient::schedule_tick()
{
  auto do_tick = make_lambda_context([this]() { tick(); });
  if (_hunting()) {
    const auto hunt_interval = (cct->_conf->mon_client_hunt_interval *
				reopen_interval_multiplier);
    timer.add_event_after(hunt_interval, do_tick);
  } else {
    timer.add_event_after(cct->_conf->mon_client_ping_interval, do_tick);
  }
}

// ---------

void MonClient::_renew_subs()
{
  assert(monc_lock.is_locked());
  if (sub_new.empty()) {
    ldout(cct, 10) << __func__ << " - empty" << dendl;
    return;
  }

  ldout(cct, 10) << __func__ << dendl;
  if (!_opened())
    _reopen_session();
  else {
    if (sub_renew_sent == utime_t())
      sub_renew_sent = ceph_clock_now();

    MMonSubscribe *m = new MMonSubscribe;
    m->what = sub_new;
    _send_mon_message(m);

    // update sub_sent with sub_new
    sub_new.insert(sub_sent.begin(), sub_sent.end());
    std::swap(sub_new, sub_sent);
    sub_new.clear();
  }
}

void MonClient::handle_subscribe_ack(MMonSubscribeAck *m)
{
  if (sub_renew_sent != utime_t()) {
    // NOTE: this is only needed for legacy (infernalis or older)
    // mons; see tick().
    sub_renew_after = sub_renew_sent;
    sub_renew_after += m->interval / 2.0;
    ldout(cct, 10) << __func__ << " sent " << sub_renew_sent << " renew after " << sub_renew_after << dendl;
    sub_renew_sent = utime_t();
  } else {
    ldout(cct, 10) << __func__ << " sent " << sub_renew_sent << ", ignoring" << dendl;
  }

  m->put();
}

int MonClient::_check_auth_tickets()
{
  assert(monc_lock.is_locked());
  if (active_con && auth) {
    if (auth->need_tickets()) {
      ldout(cct, 10) << __func__ << " getting new tickets!" << dendl;
      MAuth *m = new MAuth;
      m->protocol = auth->get_protocol();
      auth->prepare_build_request();
      auth->build_request(m->auth_payload);
      _send_mon_message(m);
    }

    _check_auth_rotating();
  }
  return 0;
}

int MonClient::_check_auth_rotating()
{
  assert(monc_lock.is_locked());
  if (!rotating_secrets ||
      !auth_principal_needs_rotating_keys(entity_name)) {
    ldout(cct, 20) << "_check_auth_rotating not needed by " << entity_name << dendl;
    return 0;
  }

  if (!active_con || !auth) {
    ldout(cct, 10) << "_check_auth_rotating waiting for auth session" << dendl;
    return 0;
  }

  utime_t now = ceph_clock_now();
  utime_t cutoff = now;
  cutoff -= std::min(30.0, cct->_conf->auth_service_ticket_ttl / 4.0);
  utime_t issued_at_lower_bound = now;
  issued_at_lower_bound -= cct->_conf->auth_service_ticket_ttl;
  if (!rotating_secrets->need_new_secrets(cutoff)) {
    ldout(cct, 10) << "_check_auth_rotating have uptodate secrets (they expire after " << cutoff << ")" << dendl;
    rotating_secrets->dump_rotating();
    return 0;
  }

  ldout(cct, 10) << "_check_auth_rotating renewing rotating keys (they expired before " << cutoff << ")" << dendl;
  if (!rotating_secrets->need_new_secrets() &&
      rotating_secrets->need_new_secrets(issued_at_lower_bound)) {
    // the key has expired before it has been issued?
    lderr(cct) << __func__ << " possible clock skew, rotating keys expired way too early"
               << " (before " << issued_at_lower_bound << ")" << dendl;
  }
  if ((now > last_rotating_renew_sent) &&
      double(now - last_rotating_renew_sent) < 1) {
    ldout(cct, 10) << __func__ << " called too often (last: "
                   << last_rotating_renew_sent << "), skipping refresh" << dendl;
    return 0;
  }
  MAuth *m = new MAuth;
  m->protocol = auth->get_protocol();
  if (auth->build_rotating_request(m->auth_payload)) {
    last_rotating_renew_sent = now;
    _send_mon_message(m);
  } else {
    m->put();
  }
  return 0;
}

int MonClient::wait_auth_rotating(double timeout)
{
  Mutex::Locker l(monc_lock);
  utime_t now = ceph_clock_now();
  utime_t until = now;
  until += timeout;

  // Must be initialized
  assert(auth != nullptr);

  if (auth->get_protocol() == CEPH_AUTH_NONE)
    return 0;
  
  if (!rotating_secrets)
    return 0;

  while (auth_principal_needs_rotating_keys(entity_name) &&
	 rotating_secrets->need_new_secrets(now)) {
    if (now >= until) {
      ldout(cct, 0) << __func__ << " timed out after " << timeout << dendl;
      return -ETIMEDOUT;
    }
    ldout(cct, 10) << __func__ << " waiting (until " << until << ")" << dendl;
    auth_cond.WaitUntil(monc_lock, until);
    now = ceph_clock_now();
  }
  ldout(cct, 10) << __func__ << " done" << dendl;
  return 0;
}

// ---------

void MonClient::_send_command(MonCommand *r)
{
  entity_addr_t peer;
  if (active_con) {
    peer = active_con->get_con()->get_peer_addr();
  }

  if (r->target_rank >= 0 &&
      r->target_rank != monmap.get_rank(peer)) {
    ldout(cct, 10) << __func__ << " " << r->tid << " " << r->cmd
		   << " wants rank " << r->target_rank
		   << ", reopening session"
		   << dendl;
    if (r->target_rank >= (int)monmap.size()) {
      ldout(cct, 10) << " target " << r->target_rank << " >= max mon " << monmap.size() << dendl;
      _finish_command(r, -ENOENT, "mon rank dne");
      return;
    }
    _reopen_session(r->target_rank);
    return;
  }

  if (r->target_name.length() &&
      r->target_name != monmap.get_name(peer)) {
    ldout(cct, 10) << __func__ << " " << r->tid << " " << r->cmd
		   << " wants mon " << r->target_name
		   << ", reopening session"
		   << dendl;
    if (!monmap.contains(r->target_name)) {
      ldout(cct, 10) << " target " << r->target_name << " not present in monmap" << dendl;
      _finish_command(r, -ENOENT, "mon dne");
      return;
    }
    _reopen_session(monmap.get_rank(r->target_name));
    return;
  }

  ldout(cct, 10) << __func__ << " " << r->tid << " " << r->cmd << dendl;
  MMonCommand *m = new MMonCommand(monmap.fsid);
  m->set_tid(r->tid);
  m->cmd = r->cmd;
  m->set_data(r->inbl);
  _send_mon_message(m);
  return;
}

void MonClient::_resend_mon_commands()
{
  // resend any requests
  for (map<uint64_t,MonCommand*>::iterator p = mon_commands.begin();
       p != mon_commands.end();
       ++p) {
    _send_command(p->second);
  }
}

void MonClient::handle_mon_command_ack(MMonCommandAck *ack)
{
  MonCommand *r = NULL;
  uint64_t tid = ack->get_tid();

  if (tid == 0 && !mon_commands.empty()) {
    r = mon_commands.begin()->second;
    ldout(cct, 10) << __func__ << " has tid 0, assuming it is " << r->tid << dendl;
  } else {
    map<uint64_t,MonCommand*>::iterator p = mon_commands.find(tid);
    if (p == mon_commands.end()) {
      ldout(cct, 10) << __func__ << " " << ack->get_tid() << " not found" << dendl;
      ack->put();
      return;
    }
    r = p->second;
  }

  ldout(cct, 10) << __func__ << " " << r->tid << " " << r->cmd << dendl;
  if (r->poutbl)
    r->poutbl->claim(ack->get_data());
  _finish_command(r, ack->r, ack->rs);
  ack->put();
}

int MonClient::_cancel_mon_command(uint64_t tid)
{
  assert(monc_lock.is_locked());

  map<ceph_tid_t, MonCommand*>::iterator it = mon_commands.find(tid);
  if (it == mon_commands.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  MonCommand *cmd = it->second;
  _finish_command(cmd, -ETIMEDOUT, "");
  return 0;
}

void MonClient::_finish_command(MonCommand *r, int ret, string rs)
{
  ldout(cct, 10) << __func__ << " " << r->tid << " = " << ret << " " << rs << dendl;
  if (r->prval)
    *(r->prval) = ret;
  if (r->prs)
    *(r->prs) = rs;
  if (r->onfinish)
    finisher.queue(r->onfinish, ret);
  mon_commands.erase(r->tid);
  delete r;
}

void MonClient::start_mon_command(const vector<string>& cmd,
				 const bufferlist& inbl,
				 bufferlist *outbl, string *outs,
				 Context *onfinish)
{
  Mutex::Locker l(monc_lock);
  MonCommand *r = new MonCommand(++last_mon_command_tid);
  r->cmd = cmd;
  r->inbl = inbl;
  r->poutbl = outbl;
  r->prs = outs;
  r->onfinish = onfinish;
  if (cct->_conf->rados_mon_op_timeout > 0) {
    class C_CancelMonCommand : public Context
    {
      uint64_t tid;
      MonClient *monc;
      public:
      C_CancelMonCommand(uint64_t tid, MonClient *monc) : tid(tid), monc(monc) {}
      void finish(int r) override {
	monc->_cancel_mon_command(tid);
      }
    };
    r->ontimeout = new C_CancelMonCommand(r->tid, this);
    timer.add_event_after(cct->_conf->rados_mon_op_timeout, r->ontimeout);
  }
  mon_commands[r->tid] = r;
  _send_command(r);
}

void MonClient::start_mon_command(const string &mon_name,
				 const vector<string>& cmd,
				 const bufferlist& inbl,
				 bufferlist *outbl, string *outs,
				 Context *onfinish)
{
  Mutex::Locker l(monc_lock);
  MonCommand *r = new MonCommand(++last_mon_command_tid);
  r->target_name = mon_name;
  r->cmd = cmd;
  r->inbl = inbl;
  r->poutbl = outbl;
  r->prs = outs;
  r->onfinish = onfinish;
  mon_commands[r->tid] = r;
  _send_command(r);
}

void MonClient::start_mon_command(int rank,
				 const vector<string>& cmd,
				 const bufferlist& inbl,
				 bufferlist *outbl, string *outs,
				 Context *onfinish)
{
  Mutex::Locker l(monc_lock);
  MonCommand *r = new MonCommand(++last_mon_command_tid);
  r->target_rank = rank;
  r->cmd = cmd;
  r->inbl = inbl;
  r->poutbl = outbl;
  r->prs = outs;
  r->onfinish = onfinish;
  mon_commands[r->tid] = r;
  _send_command(r);
}

// ---------

void MonClient::get_version(string map, version_t *newest, version_t *oldest, Context *onfinish)
{
  version_req_d *req = new version_req_d(onfinish, newest, oldest);
  ldout(cct, 10) << "get_version " << map << " req " << req << dendl;
  Mutex::Locker l(monc_lock);
  MMonGetVersion *m = new MMonGetVersion();
  m->what = map;
  m->handle = ++version_req_id;
  version_requests[m->handle] = req;
  _send_mon_message(m);
}

void MonClient::handle_get_version_reply(MMonGetVersionReply* m)
{
  assert(monc_lock.is_locked());
  map<ceph_tid_t, version_req_d*>::iterator iter = version_requests.find(m->handle);
  if (iter == version_requests.end()) {
    ldout(cct, 0) << __func__ << " version request with handle " << m->handle
		  << " not found" << dendl;
  } else {
    version_req_d *req = iter->second;
    ldout(cct, 10) << __func__ << " finishing " << req << " version " << m->version << dendl;
    version_requests.erase(iter);
    if (req->newest)
      *req->newest = m->version;
    if (req->oldest)
      *req->oldest = m->oldest_version;
    finisher.queue(req->context, 0);
    delete req;
  }
  m->put();
}

AuthAuthorizer* MonClient::build_authorizer(int service_id) const {
  Mutex::Locker l(monc_lock);
  if (auth) {
    return auth->build_authorizer(service_id);
  } else {
    ldout(cct, 0) << __func__ << " for " << ceph_entity_type_name(service_id)
		  << ", but no auth is available now" << dendl;
    return nullptr;
  }
}

#define dout_subsys ceph_subsys_monc
#undef dout_prefix
#define dout_prefix *_dout << "monclient" << (have_session() ? ": " : "(hunting): ")

MonConnection::MonConnection(CephContext *cct, ConnectionRef con, uint64_t global_id)
  : cct(cct), con(con), global_id(global_id)
{}

MonConnection::~MonConnection()
{
  if (con) {
    con->mark_down();
    con.reset();
  }
}

bool MonConnection::have_session() const
{
  return state == State::HAVE_SESSION;
}

void MonConnection::start(epoch_t epoch,
                         const EntityName& entity_name,
                         const AuthMethodList& auth_supported)
{
  // restart authentication handshake
  state = State::NEGOTIATING;

  // send an initial keepalive to ensure our timestamp is valid by the
  // time we are in an OPENED state (by sequencing this before
  // authentication).
  con->send_keepalive();

  auto m = new MAuth;
  m->protocol = 0;
  m->monmap_epoch = epoch;
  __u8 struct_v = 1;
  encode(struct_v, m->auth_payload);
  encode(auth_supported.get_supported_set(), m->auth_payload);
  encode(entity_name, m->auth_payload);
  encode(global_id, m->auth_payload);
  con->send_message(m);
}

int MonConnection::handle_auth(MAuthReply* m,
			       const EntityName& entity_name,
			       uint32_t want_keys,
			       RotatingKeyRing* keyring)
{
  if (state == State::NEGOTIATING) {
    int r = _negotiate(m, entity_name, want_keys, keyring);
    if (r) {
      return r;
    }
    state = State::AUTHENTICATING;
  }
  int r = authenticate(m);
  if (!r) {
    state = State::HAVE_SESSION;
  }
  return r;
}

int MonConnection::_negotiate(MAuthReply *m,
			      const EntityName& entity_name,
			      uint32_t want_keys,
			      RotatingKeyRing* keyring)
{
  if (auth && (int)m->protocol == auth->get_protocol()) {
    // good, negotiation completed
    auth->reset();
    return 0;
  }

  auth.reset(get_auth_client_handler(cct, m->protocol, keyring));
  if (!auth) {
    ldout(cct, 10) << "no handler for protocol " << m->protocol << dendl;
    if (m->result == -ENOTSUP) {
      ldout(cct, 10) << "none of our auth protocols are supported by the server"
		     << dendl;
    }
    return m->result;
  }

  // do not request MGR key unless the mon has the SERVER_KRAKEN
  // feature.  otherwise it will give us an auth error.  note that
  // we have to use the FEATUREMASK because pre-jewel the kraken
  // feature bit was used for something else.
  if ((want_keys & CEPH_ENTITY_TYPE_MGR) &&
      !(m->get_connection()->has_features(CEPH_FEATUREMASK_SERVER_KRAKEN))) {
    ldout(cct, 1) << __func__
		  << " not requesting MGR keys from pre-kraken monitor"
		  << dendl;
    want_keys &= ~CEPH_ENTITY_TYPE_MGR;
  }
  auth->set_want_keys(want_keys);
  auth->init(entity_name);
  auth->set_global_id(global_id);
  return 0;
}

int MonConnection::authenticate(MAuthReply *m)
{
  assert(auth);
  if (!m->global_id) {
    ldout(cct, 1) << "peer sent an invalid global_id" << dendl;
  }
  if (m->global_id != global_id) {
    // it's a new session
    auth->reset();
    global_id = m->global_id;
    auth->set_global_id(global_id);
    ldout(cct, 10) << "my global_id is " << m->global_id << dendl;
  }
  auto p = m->result_bl.begin();
  int ret = auth->handle_response(m->result, p);
  if (ret == -EAGAIN) {
    auto ma = new MAuth;
    ma->protocol = auth->get_protocol();
    auth->prepare_build_request();
    auth->build_request(ma->auth_payload);
    con->send_message(ma);
  }
  return ret;
}
