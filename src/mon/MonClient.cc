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

#include <algorithm>
#include <iterator>
#include <random>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/copy_n.hpp>
#include "common/weighted_shuffle.h"

#include "include/random.h"
#include "include/scope_guard.h"
#include "include/stringify.h"

#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MMonMap.h"
#include "messages/MConfig.h"
#include "messages/MAuth.h"
#include "messages/MLogAck.h"
#include "messages/MAuthReply.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MPing.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"
#include "common/errno.h"
#include "common/hostname.h"
#include "common/LogClient.h"

#include "MonClient.h"
#include "error_code.h"
#include "MonMap.h"

#include "auth/Auth.h"
#include "auth/KeyRing.h"
#include "auth/AuthClientHandler.h"
#include "auth/AuthRegistry.h"
#include "auth/RotatingKeyRing.h"

#define dout_subsys ceph_subsys_monc
#undef dout_prefix
#define dout_prefix *_dout << "monclient" << (_hunting() ? "(hunting)":"") << ": "

namespace bs = boost::system;
using std::string;
using namespace std::literals;

MonClient::MonClient(CephContext *cct_, boost::asio::io_context& service) :
  Dispatcher(cct_),
  AuthServer(cct_),
  messenger(NULL),
  timer(cct_, monc_lock),
  service(service),
  initialized(false),
  log_client(NULL),
  more_log_pending(false),
  want_monmap(true),
  had_a_connection(false),
  reopen_interval_multiplier(
    cct_->_conf.get_val<double>("mon_client_hunt_interval_min_multiple")),
  last_mon_command_tid(0),
  version_req_id(0)
{}

MonClient::~MonClient()
{
}

int MonClient::build_initial_monmap()
{
  ldout(cct, 10) << __func__ << dendl;
  int r = monmap.build_initial(cct, false, std::cerr);
  ldout(cct,10) << "monmap:\n";
  monmap.print(*_dout);
  *_dout << dendl;
  return r;
}

int MonClient::get_monmap()
{
  ldout(cct, 10) << __func__ << dendl;
  std::unique_lock l(monc_lock);

  sub.want("monmap", 0, 0);
  if (!_opened())
    _reopen_session();
  map_cond.wait(l, [this] { return !want_monmap; });
  ldout(cct, 10) << __func__ << " done" << dendl;
  return 0;
}

int MonClient::get_monmap_and_config()
{
  ldout(cct, 10) << __func__ << dendl;
  ceph_assert(!messenger);

  int tries = 10;

  cct->init_crypto();
  auto shutdown_crypto = make_scope_guard([this] {
    cct->shutdown_crypto();
  });

  int r = build_initial_monmap();
  if (r < 0) {
    lderr(cct) << __func__ << " cannot identify monitors to contact" << dendl;
    return r;
  }

  messenger = Messenger::create_client_messenger(
    cct, "temp_mon_client");
  ceph_assert(messenger);
  messenger->add_dispatcher_head(this);
  messenger->start();
  auto shutdown_msgr = make_scope_guard([this] {
    messenger->shutdown();
    messenger->wait();
    delete messenger;
    messenger = nullptr;
    if (!monmap.fsid.is_zero()) {
      cct->_conf.set_val("fsid", stringify(monmap.fsid));
    }
  });

  want_bootstrap_config = true;
  auto shutdown_config = make_scope_guard([this] {
    std::unique_lock l(monc_lock);
    want_bootstrap_config = false;
    bootstrap_config.reset();
  });

  ceph::ref_t<MConfig> config;
  while (tries-- > 0) {
    r = init();
    if (r < 0) {
      return r;
    }
    r = authenticate(
      cct->_conf.get_val<std::chrono::seconds>("client_mount_timeout").count());
    if (r < 0) {
      break;
    }
    {
      std::unique_lock l(monc_lock);
      if (monmap.get_epoch() &&
	  !monmap.persistent_features.contains_all(
	    ceph::features::mon::FEATURE_MIMIC)) {
	ldout(cct,10) << __func__ << " pre-mimic monitor, no config to fetch"
		      << dendl;
	r = 0;
	break;
      }
      while ((!bootstrap_config || monmap.get_epoch() == 0) && r == 0) {
	ldout(cct,20) << __func__ << " waiting for monmap|config" << dendl;
	auto status = map_cond.wait_for(l, ceph::make_timespan(
	    cct->_conf->mon_client_hunt_interval));
	if (status == std::cv_status::timeout) {
	  r = -ETIMEDOUT;
	}
      }

      if (bootstrap_config) {
	ldout(cct,10) << __func__ << " success" << dendl;
	config = std::move(bootstrap_config);
	r = 0;
	break;
      }
    }
    lderr(cct) << __func__ << " failed to get config" << dendl;
    shutdown();
    continue;
  }

  if (config) {
    // apply the bootstrap config to ensure its applied prior to completing
    // the bootstrap
    cct->_conf.set_mon_vals(cct, config->config, config_cb);
  }

  shutdown();
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

  // N.B. monc isn't initialized

  auth_registry.refresh_config();

  KeyRing keyring;
  keyring.from_ceph_context(cct);
  RotatingKeyRing rkeyring(cct, cct->get_module_type(), &keyring);

  MonClientPinger *pinger = new MonClientPinger(cct,
						&rkeyring,
						result_reply);

  Messenger *smsgr = Messenger::create_client_messenger(cct, "temp_ping_client");
  smsgr->add_dispatcher_head(pinger);
  smsgr->set_auth_client(pinger);
  smsgr->start();

  ConnectionRef con = smsgr->connect_to_mon(monmap.get_addrs(new_mon_id));
  ldout(cct, 10) << __func__ << " ping mon." << new_mon_id
                 << " " << con->get_peer_addr() << dendl;

  pinger->mc.reset(new MonConnection(cct, con, 0, &auth_registry));
  pinger->mc->start(monmap.get_epoch(), entity_name);
  con->send_message(new MPing);

  int ret = pinger->wait_for_reply(cct->_conf->mon_client_ping_timeout);
  if (ret == 0) {
    ldout(cct,10) << __func__ << " got ping reply" << dendl;
  } else {
    ret = -ret;
  }

  con->mark_down();
  pinger->mc.reset();
  smsgr->shutdown();
  smsgr->wait();
  delete smsgr;
  delete pinger;
  return ret;
}

bool MonClient::ms_dispatch(Message *m)
{
  // we only care about these message types
  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
  case CEPH_MSG_AUTH_REPLY:
  case CEPH_MSG_MON_SUBSCRIBE_ACK:
  case CEPH_MSG_MON_GET_VERSION_REPLY:
  case MSG_MON_COMMAND_ACK:
  case MSG_COMMAND_REPLY:
  case MSG_LOGACK:
  case MSG_CONFIG:
    break;
  case CEPH_MSG_PING:
    m->put();
    return true;
  default:
    return false;
  }

  std::lock_guard lock(monc_lock);

  if (!m->get_connection()->is_anon() &&
      m->get_source().type() == CEPH_ENTITY_TYPE_MON) {
    if (_hunting()) {
      auto p = _find_pending_con(m->get_connection());
      if (p == pending_cons.end()) {
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
  case MSG_COMMAND_REPLY:
    if (m->get_connection()->is_anon() &&
        m->get_source().type() == CEPH_ENTITY_TYPE_MON) {
      // this connection is from 'tell'... ignore everything except our command
      // reply.  (we'll get misc other message because we authenticated, but we
      // don't need them.)
      handle_command_reply(static_cast<MCommandReply*>(m));
      return true;
    }
    // leave the message for another dispatch handler (e.g., Objecter)
    return false;
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
    auto lm = log_client->get_mon_log_message(flush);
    if (lm)
      _send_mon_message(std::move(lm));
    more_log_pending = log_client->are_pending();
  }
}

void MonClient::flush_log()
{
  std::lock_guard l(monc_lock);
  send_log();
}

/* Unlike all the other message-handling functions, we don't put away a reference
* because we want to support MMonMap passthrough to other Dispatchers. */
void MonClient::handle_monmap(MMonMap *m)
{
  ldout(cct, 10) << __func__ << " " << *m << dendl;
  auto con_addrs = m->get_source_addrs();
  string old_name = monmap.get_name(con_addrs);
  const auto old_epoch = monmap.get_epoch();

  auto p = m->monmapbl.cbegin();
  decode(monmap, p);

  ldout(cct, 10) << " got monmap " << monmap.epoch
		 << " from mon." << old_name
		 << " (according to old e" << monmap.get_epoch() << ")"
 		 << dendl;
  ldout(cct, 10) << "dump:\n";
  monmap.print(*_dout);
  *_dout << dendl;

  if (old_epoch != monmap.get_epoch()) {
    tried.clear();
  }
  if (old_name.size() == 0) {
    ldout(cct,10) << " can't identify which mon we were connected to" << dendl;
    _reopen_session();
  } else {
    auto new_name = monmap.get_name(con_addrs);
    if (new_name.empty()) {
      ldout(cct, 10) << "mon." << old_name << " at " << con_addrs
		     << " went away" << dendl;
      // can't find the mon we were talking to (above)
      _reopen_session();
    } else if (messenger->should_use_msgr2() &&
	       monmap.get_addrs(new_name).has_msgr2() &&
	       !con_addrs.has_msgr2()) {
      ldout(cct,1) << " mon." << new_name << " has (v2) addrs "
		   << monmap.get_addrs(new_name) << " but i'm connected to "
		   << con_addrs << ", reconnecting" << dendl;
      _reopen_session();
    }
  }

  cct->set_mon_addrs(monmap);

  sub.got("monmap", monmap.get_epoch());
  map_cond.notify_all();
  want_monmap = false;

  if (authenticate_err == 1) {
    _finish_auth(0);
  }
}

void MonClient::handle_config(MConfig *m)
{
  ldout(cct,10) << __func__ << " " << *m << dendl;

  if (want_bootstrap_config) {
    // get_monmap_and_config is waiting for config which it will apply
    // synchronously
    bootstrap_config = ceph::ref_t<MConfig>(m, false);
    map_cond.notify_all();
    return;
  }

  // Take the sledgehammer approach to ensuring we don't depend on
  // anything in MonClient.
  boost::asio::post(finish_strand,
		    [m, cct = boost::intrusive_ptr<CephContext>(cct),
		     config_notify_cb = config_notify_cb,
		     config_cb = config_cb]() {
		      cct->_conf.set_mon_vals(cct.get(), m->config, config_cb);
		      if (config_notify_cb) {
			config_notify_cb();
		      }
		      m->put();
		    });
}

// ----------------------

int MonClient::init()
{
  ldout(cct, 10) << __func__ << dendl;

  entity_name = cct->_conf->name;

  auth_registry.refresh_config();

  std::lock_guard l(monc_lock);
  keyring.reset(new KeyRing);
  if (auth_registry.is_supported_method(messenger->get_mytype(),
					CEPH_AUTH_CEPHX)) {
    // this should succeed, because auth_registry just checked!
    int r = keyring->from_ceph_context(cct);
    if (r != 0) {
      // but be somewhat graceful in case there was a race condition
      lderr(cct) << "keyring not found" << dendl;
      return r;
    }
  }
  if (!auth_registry.any_supported_methods(messenger->get_mytype())) {
    return -ENOENT;
  }

  rotating_secrets.reset(
    new RotatingKeyRing(cct, cct->get_module_type(), keyring.get()));

  initialized = true;

  messenger->set_auth_client(this);
  messenger->add_dispatcher_head(this);

  timer.init();
  schedule_tick();

  cct->get_admin_socket()->register_command(
    "rotate-key",
    this,
    "rotate live authentication key");

  return 0;
}

void MonClient::shutdown()
{
  ldout(cct, 10) << __func__ << dendl;

  cct->get_admin_socket()->unregister_commands(this);
  
  monc_lock.lock();
  stopping = true;
  while (!version_requests.empty()) {
    ceph::async::post(std::move(version_requests.begin()->second),
		      monc_errc::shutting_down, 0, 0);
    ldout(cct, 20) << __func__ << " canceling and discarding version request "
		   << version_requests.begin()->first << dendl;
    version_requests.erase(version_requests.begin());
  }
  while (!mon_commands.empty()) {
    auto tid = mon_commands.begin()->first;
    _cancel_mon_command(tid);
  }
  ldout(cct, 20) << __func__ << " discarding " << waiting_for_session.size()
		 << " pending message(s)" << dendl;
  waiting_for_session.clear();

  active_con.reset();
  pending_cons.clear();

  auth.reset();
  global_id = 0;
  authenticate_err = 0;
  authenticated = false;

  monc_lock.unlock();

  if (initialized) {
    initialized = false;
  }
  monc_lock.lock();
  timer.shutdown();
  stopping = false;
  monc_lock.unlock();
}

int MonClient::authenticate(double timeout)
{
  std::unique_lock lock{monc_lock};

  if (active_con) {
    ldout(cct, 5) << "already authenticated" << dendl;
    return 0;
  }
  sub.want("monmap", monmap.get_epoch() ? monmap.get_epoch() + 1 : 0, 0);
  sub.want("config", 0, 0);
  if (!_opened())
    _reopen_session();

  auto until = ceph::mono_clock::now();
  until += ceph::make_timespan(timeout);
  if (timeout > 0.0)
    ldout(cct, 10) << "authenticate will time out at " << until << dendl;
  while (!active_con && authenticate_err >= 0) {
    if (timeout > 0.0) {
      auto r = auth_cond.wait_until(lock, until);
      if (r == std::cv_status::timeout && !active_con) {
	ldout(cct, 0) << "authenticate timed out after " << timeout << dendl;
	authenticate_err = -ETIMEDOUT;
      }
    } else {
      auth_cond.wait(lock);
    }
  }

  if (active_con) {
    ldout(cct, 5) << __func__ << " success, global_id "
		  << active_con->get_global_id() << dendl;
    // active_con should not have been set if there was an error
    ceph_assert(authenticate_err >= 0);
    authenticated = true;
  }

  if (authenticate_err < 0 && auth_registry.no_keyring_disabled_cephx()) {
    lderr(cct) << __func__ << " NOTE: no keyring found; disabled cephx authentication" << dendl;
  }

  return authenticate_err;
}

int MonClient::call(
    std::string_view command,
    const cmdmap_t& cmdmap,
    const ceph::buffer::list &inbl,
    ceph::Formatter *f,
    std::ostream& errss,
    ceph::buffer::list& out)
{
  if (command == "rotate-key") {
    CryptoKey key;
    try {
      key.decode_base64(inbl.to_str());
    } catch (buffer::error& e) {
      errss << "error decoding key: " << e.what();
      return -EINVAL;
    }
    if (keyring) {
      ldout(cct, 1) << "rotate live key for " << entity_name << dendl;
      keyring->add(entity_name, key);
    } else {
      errss << "cephx not enabled; no key to rotate";
      return -EINVAL;
    }
  }
  return 0;
}

void MonClient::handle_auth(MAuthReply *m)
{
  ceph_assert(ceph_mutex_is_locked(monc_lock));

  if (m->get_connection()->is_anon()) {
    // anon connection, used for mon tell commands
    for (auto& p : mon_commands) {
      if (p.second->target_con == m->get_connection()) {
	auto& mc = p.second->target_session;
	int ret = mc->handle_auth(m, entity_name,
				  CEPH_ENTITY_TYPE_MON,
				  rotating_secrets.get());
	(void)ret; // we don't care
	break;
      }
    }
    m->put();
    return;
  }

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
  auto found = _find_pending_con(m->get_connection());
  ceph_assert(found != pending_cons.end());
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
    ceph_assert(mc.have_session());
    active_con.reset(new MonConnection(std::move(mc)));
    pending_cons.clear();
  }

  _finish_hunting(auth_err);
  _finish_auth(auth_err);
}

void MonClient::_finish_auth(int auth_err)
{
  ldout(cct,10) << __func__ << " " << auth_err << dendl;
  authenticate_err = auth_err;
  // _resend_mon_commands() could _reopen_session() if the connected mon is not
  // the one the MonCommand is targeting.
  if (!auth_err && active_con) {
    ceph_assert(auth);
    _check_auth_tickets();
  } else if (auth_err == -EAGAIN && !active_con) {
    ldout(cct,10) << __func__ 
                  << " auth returned EAGAIN, reopening the session to try again"
                  << dendl;
    _reopen_session();
  }
  auth_cond.notify_all();
}

// ---------

void MonClient::send_mon_message(MessageRef m)
{
  std::lock_guard l{monc_lock};
  _send_mon_message(std::move(m));
}

void MonClient::_send_mon_message(MessageRef m)
{
  ceph_assert(ceph_mutex_is_locked(monc_lock));
  if (active_con) {
    auto cur_con = active_con->get_con();
    ldout(cct, 10) << "_send_mon_message to mon."
		   << monmap.get_name(cur_con->get_peer_addr())
		   << " at " << cur_con->get_peer_addr() << dendl;
    cur_con->send_message2(std::move(m));
  } else {
    waiting_for_session.push_back(std::move(m));
  }
}

void MonClient::_reopen_session(int rank)
{
  ceph_assert(ceph_mutex_is_locked(monc_lock));
  ldout(cct, 10) << __func__ << " rank " << rank << dendl;

  active_con.reset();
  pending_cons.clear();

  authenticate_err = 1;  // == in progress

  _start_hunting();

  if (rank == -1) {
    rank = cct->_conf.get_val<int64_t>("mon_client_target_rank");
  }

  if (rank >= 0) {
    _add_conn(rank);
  } else {
    _add_conns();
  }

  // throw out old queued messages
  waiting_for_session.clear();

  // throw out version check requests
  while (!version_requests.empty()) {
    ceph::async::post(std::move(version_requests.begin()->second),
		      monc_errc::session_reset, 0, 0);
    version_requests.erase(version_requests.begin());
  }

  for (auto& c : pending_cons) {
    c.second.start(monmap.get_epoch(), entity_name);
  }

  if (sub.reload()) {
    _renew_subs();
  }
}

void MonClient::_add_conn(unsigned rank)
{
  auto peer = monmap.get_addrs(rank);
  auto conn = messenger->connect_to_mon(peer);
  MonConnection mc(cct, conn, global_id, &auth_registry);
  if (auth) {
    mc.get_auth().reset(auth->clone());
  }
  pending_cons.insert(std::make_pair(peer, std::move(mc)));
  ldout(cct, 10) << "picked mon." << monmap.get_name(rank)
                 << " con " << conn
                 << " addr " << peer
                 << dendl;
}

void MonClient::_add_conns()
{
  // collect the next batch of candidates who are listed right next to the ones
  // already tried
  auto get_next_batch = [this]() -> std::vector<unsigned> {
    std::multimap<uint16_t, unsigned> ranks_by_priority;
    boost::copy(
      monmap.mon_info | boost::adaptors::filtered(
        [this](auto& info) {
          auto rank = monmap.get_rank(info.first);
          return tried.count(rank) == 0;
        }) | boost::adaptors::transformed(
          [this](auto& info) {
            auto rank = monmap.get_rank(info.first);
            return std::make_pair(info.second.priority, rank);
          }), std::inserter(ranks_by_priority, end(ranks_by_priority)));
    if (ranks_by_priority.empty()) {
      return {};
    }
    // only choose the monitors with lowest priority
    auto cands = boost::make_iterator_range(
      ranks_by_priority.equal_range(ranks_by_priority.begin()->first));
    std::vector<unsigned> ranks;
    boost::range::copy(cands | boost::adaptors::map_values,
		       std::back_inserter(ranks));
    return ranks;
  };
  auto ranks = get_next_batch();
  if (ranks.empty()) {
    tried.clear();  // start over
    ranks = get_next_batch();
  }
  ceph_assert(!ranks.empty());
  if (ranks.size() > 1) {
    std::vector<uint16_t> weights;
    for (auto i : ranks) {
      auto rank_name = monmap.get_name(i);
      weights.push_back(monmap.get_weight(rank_name));
    }
    random_device_t rd;
    if (std::accumulate(begin(weights), end(weights), 0u) == 0) {
      std::shuffle(begin(ranks), end(ranks), std::mt19937{rd()});
    } else {
      weighted_shuffle(begin(ranks), end(ranks), begin(weights), end(weights),
		       std::mt19937{rd()});
    }
  }
  ldout(cct, 10) << __func__ << " ranks=" << ranks << dendl;
  unsigned n = cct->_conf->mon_client_hunt_parallel;
  if (n == 0 || n > ranks.size()) {
    n = ranks.size();
  }
  for (unsigned i = 0; i < n; i++) {
    _add_conn(ranks[i]);
    tried.insert(ranks[i]);
  }
}

bool MonClient::ms_handle_reset(Connection *con)
{
  std::lock_guard lock(monc_lock);

  if (con->get_peer_type() != CEPH_ENTITY_TYPE_MON)
    return false;

  if (con->is_anon()) {
    auto p = mon_commands.begin();
    while (p != mon_commands.end()) {
      auto cmd = p->second;
      ++p;
      if (cmd->target_con == con) {
	_send_command(cmd); // may retry or fail
	break;
      }
    }
    return true;
  }

  if (_hunting()) {
    if (pending_cons.count(con->get_peer_addrs())) {
      ldout(cct, 10) << __func__ << " hunted mon " << con->get_peer_addrs()
		     << dendl;
    } else {
      ldout(cct, 10) << __func__ << " stray mon " << con->get_peer_addrs()
		     << dendl;
    }
    return true;
  } else {
    if (active_con && con == active_con->get_con()) {
      ldout(cct, 10) << __func__ << " current mon " << con->get_peer_addrs()
		     << dendl;
      _reopen_session();
      return false;
    } else {
      ldout(cct, 10) << "ms_handle_reset stray mon " << con->get_peer_addrs()
		     << dendl;
      return true;
    }
  }
}

bool MonClient::_opened() const
{
  ceph_assert(ceph_mutex_is_locked(monc_lock));
  return active_con || _hunting();
}

bool MonClient::_hunting() const
{
  return !pending_cons.empty();
}

void MonClient::_start_hunting()
{
  ceph_assert(!_hunting());
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

void MonClient::_finish_hunting(int auth_err)
{
  ldout(cct,10) << __func__ << " " << auth_err << dendl;
  ceph_assert(ceph_mutex_is_locked(monc_lock));
  // the pending conns have been cleaned.
  ceph_assert(!_hunting());
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

  if (!auth_err) {
    last_rotating_renew_sent = utime_t();
    while (!waiting_for_session.empty()) {
      _send_mon_message(std::move(waiting_for_session.front()));
      waiting_for_session.pop_front();
    }
    _resend_mon_commands();
    send_log(true);
    if (active_con) {
      auth = std::move(active_con->get_auth());
      if (global_id && global_id != active_con->get_global_id()) {
	lderr(cct) << __func__ << " global_id changed from " << global_id
		   << " to " << active_con->get_global_id() << dendl;
      }
      global_id = active_con->get_global_id();
    }
  }
}

void MonClient::tick()
{
  ldout(cct, 10) << __func__ << dendl;

  utime_t now = ceph_clock_now();

  auto reschedule_tick = make_scope_guard([this] {
      schedule_tick();
    });

  _check_auth_tickets();
  _check_tell_commands();
  
  if (_hunting()) {
    ldout(cct, 1) << "continuing hunt" << dendl;
    return _reopen_session();
  } else if (active_con) {
    // just renew as needed
    auto cur_con = active_con->get_con();
    if (!cur_con->has_feature(CEPH_FEATURE_MON_STATEFUL_SUB)) {
      const bool maybe_renew = sub.need_renew();
      ldout(cct, 10) << "renew subs? -- " << (maybe_renew ? "yes" : "no")
		     << dendl;
      if (maybe_renew) {
	_renew_subs();
      }
    }

    if (now > last_keepalive + cct->_conf->mon_client_ping_interval) {
      cur_con->send_keepalive();
      last_keepalive = now;

      if (cct->_conf->mon_client_ping_timeout > 0 &&
	  cur_con->has_feature(CEPH_FEATURE_MSGR_KEEPALIVE2)) {
	utime_t lk = cur_con->get_last_keepalive_ack();
	utime_t interval = now - lk;
	if (interval > cct->_conf->mon_client_ping_timeout) {
	  ldout(cct, 1) << "no keepalive since " << lk << " (" << interval
			<< " seconds), reconnecting" << dendl;
	  return _reopen_session();
	}
      }

      _un_backoff();
    }

    if (now > last_send_log + cct->_conf->mon_client_log_interval) {
      send_log();
      last_send_log = now;
    }
  }
}

void MonClient::_un_backoff()
{
  // un-backoff our reconnect interval
  reopen_interval_multiplier = std::max(
    cct->_conf.get_val<double>("mon_client_hunt_interval_min_multiple"),
    reopen_interval_multiplier /
    cct->_conf.get_val<double>("mon_client_hunt_interval_backoff"));
  ldout(cct, 20) << __func__ << " reopen_interval_multipler now "
		 << reopen_interval_multiplier << dendl;
}

void MonClient::schedule_tick()
{
  auto do_tick = make_lambda_context([this](int) { tick(); });
  if (!is_connected()) {
    // start another round of hunting
    const auto hunt_interval = (cct->_conf->mon_client_hunt_interval *
				reopen_interval_multiplier);
    timer.add_event_after(hunt_interval, do_tick);
  } else {
    // keep in touch
    timer.add_event_after(std::min(cct->_conf->mon_client_ping_interval,
				   cct->_conf->mon_client_log_interval),
			  do_tick);
  }
}

// ---------

void MonClient::_renew_subs()
{
  ceph_assert(ceph_mutex_is_locked(monc_lock));
  if (!sub.have_new()) {
    ldout(cct, 10) << __func__ << " - empty" << dendl;
    return;
  }

  ldout(cct, 10) << __func__ << dendl;
  if (!_opened())
    _reopen_session();
  else {
    auto m = ceph::make_message<MMonSubscribe>();
    m->what = sub.get_subs();
    m->hostname = ceph_get_short_hostname();
    _send_mon_message(std::move(m));
    sub.renewed();
  }
}

void MonClient::handle_subscribe_ack(MMonSubscribeAck *m)
{
  sub.acked(m->interval);
  m->put();
}

int MonClient::_check_auth_tickets()
{
  ldout(cct, 10) << __func__ << dendl;
  ceph_assert(ceph_mutex_is_locked(monc_lock));
  if (active_con && auth) {
    if (auth->need_tickets()) {
      ldout(cct, 10) << __func__ << " getting new tickets!" << dendl;
      auto m = ceph::make_message<MAuth>();
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
  ceph_assert(ceph_mutex_is_locked(monc_lock));
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
  auto m = ceph::make_message<MAuth>();
  m->protocol = auth->get_protocol();
  if (auth->build_rotating_request(m->auth_payload)) {
    last_rotating_renew_sent = now;
    _send_mon_message(std::move(m));
  }
  return 0;
}

int MonClient::wait_auth_rotating(double timeout)
{
  std::unique_lock l(monc_lock);

  // Must be initialized
  ceph_assert(auth != nullptr);

  if (auth->get_protocol() == CEPH_AUTH_NONE)
    return 0;
  
  if (!rotating_secrets)
    return 0;

  ldout(cct, 10) << __func__ << " waiting for " << timeout << dendl;
  utime_t cutoff = ceph_clock_now();
  cutoff -= std::min(30.0, cct->_conf->auth_service_ticket_ttl / 4.0);
  if (auth_cond.wait_for(l, ceph::make_timespan(timeout), [this, cutoff] {
    return (!auth_principal_needs_rotating_keys(entity_name) ||
	    !rotating_secrets->need_new_secrets(cutoff));
  })) {
    ldout(cct, 10) << __func__ << " done" << dendl;
    return 0;
  } else {
    ldout(cct, 0) << __func__ << " timed out after " << timeout << dendl;
    return -ETIMEDOUT;
  }
}

// ---------

void MonClient::_send_command(MonCommand *r)
{
  if (r->is_tell()) {
    ++r->send_attempts;
    if (r->send_attempts > cct->_conf->mon_client_directed_command_retry) {
      _finish_command(r, monc_errc::mon_unavailable, "mon unavailable", {});
      return;
    }
    // tell-style command
    if (monmap.min_mon_release >= ceph_release_t::octopus) {
      if (r->target_con) {
	r->target_con->mark_down();
      }
      if (r->target_rank >= 0) {
	if (r->target_rank >= (int)monmap.size()) {
	  ldout(cct, 10) << " target " << r->target_rank
			 << " >= max mon " << monmap.size() << dendl;
	  _finish_command(r, monc_errc::rank_dne, "mon rank dne"sv, {});
	  return;
	}
	r->target_con = messenger->connect_to_mon(
	  monmap.get_addrs(r->target_rank), true /* anon */);
      } else {
	if (!monmap.contains(r->target_name)) {
	  ldout(cct, 10) << " target " << r->target_name
			 << " not present in monmap" << dendl;
	  _finish_command(r, monc_errc::mon_dne, "mon dne"sv, {});
	  return;
	}
	r->target_con = messenger->connect_to_mon(
	  monmap.get_addrs(r->target_name), true /* anon */);
      }

      r->target_session.reset(new MonConnection(cct, r->target_con, 0,
						&auth_registry));
      r->target_session->start(monmap.get_epoch(), entity_name);
      r->last_send_attempt = ceph_clock_now();

      MCommand *m = new MCommand(monmap.fsid);
      m->set_tid(r->tid);
      m->cmd = r->cmd;
      m->set_data(r->inbl);
      r->target_session->queue_command(m);
      return;
    }

    // ugly legacy handling of pre-octopus mons
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
	ldout(cct, 10) << " target " << r->target_rank
		       << " >= max mon " << monmap.size() << dendl;
	_finish_command(r, monc_errc::rank_dne, "mon rank dne"sv, {});
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
	ldout(cct, 10) << " target " << r->target_name
		       << " not present in monmap" << dendl;
	_finish_command(r, monc_errc::mon_dne, "mon dne"sv, {});
	return;
      }
      _reopen_session(monmap.get_rank(r->target_name));
      return;
    }
    // fall-thru to send 'normal' CLI command
  }

  // normal CLI command
  ldout(cct, 10) << __func__ << " " << r->tid << " " << r->cmd << dendl;
  auto m = ceph::make_message<MMonCommand>(monmap.fsid);
  m->set_tid(r->tid);
  m->cmd = r->cmd;
  m->set_data(r->inbl);
  _send_mon_message(std::move(m));
  return;
}

void MonClient::_check_tell_commands()
{
  // resend any requests
  auto now = ceph_clock_now();
  auto p = mon_commands.begin();
  while (p != mon_commands.end()) {
    auto cmd = p->second;
    ++p;
    if (cmd->is_tell() &&
	cmd->last_send_attempt != utime_t() &&
	now - cmd->last_send_attempt > cct->_conf->mon_client_hunt_interval) {
      ldout(cct,5) << __func__ << " timeout tell command " << cmd->tid << dendl;
      _send_command(cmd); // might remove cmd from mon_commands
    }
  }
}

void MonClient::_resend_mon_commands()
{
  // resend any requests
  auto p = mon_commands.begin();
  while (p != mon_commands.end()) {
    auto cmd = p->second;
    ++p;
    if (cmd->is_tell() && monmap.min_mon_release >= ceph_release_t::octopus) {
      // starting with octopus, tell commands use their own connetion and need no
      // special resend when we finish hunting.
    } else {
      _send_command(cmd); // might remove cmd from mon_commands
    }
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
    auto p = mon_commands.find(tid);
    if (p == mon_commands.end()) {
      ldout(cct, 10) << __func__ << " " << ack->get_tid() << " not found" << dendl;
      ack->put();
      return;
    }
    r = p->second;
  }

  ldout(cct, 10) << __func__ << " " << r->tid << " " << r->cmd << dendl;
  auto ec = ack->r < 0 ? bs::error_code(-ack->r, mon_category())
    : bs::error_code();
  _finish_command(r, ec, ack->rs,
		  std::move(ack->get_data()));
  ack->put();
}

void MonClient::handle_command_reply(MCommandReply *reply)
{
  MonCommand *r = NULL;
  uint64_t tid = reply->get_tid();

  if (tid == 0 && !mon_commands.empty()) {
    r = mon_commands.begin()->second;
    ldout(cct, 10) << __func__ << " has tid 0, assuming it is " << r->tid
		   << dendl;
  } else {
    auto p = mon_commands.find(tid);
    if (p == mon_commands.end()) {
      ldout(cct, 10) << __func__ << " " << reply->get_tid() << " not found"
		     << dendl;
      reply->put();
      return;
    }
    r = p->second;
  }

  ldout(cct, 10) << __func__ << " " << r->tid << " " << r->cmd << dendl;
  auto ec = reply->r < 0 ? bs::error_code(-reply->r, mon_category())
    : bs::error_code();
  _finish_command(r, ec, reply->rs, std::move(reply->get_data()));
  reply->put();
}

int MonClient::_cancel_mon_command(uint64_t tid)
{
  ceph_assert(ceph_mutex_is_locked(monc_lock));

  auto it = mon_commands.find(tid);
  if (it == mon_commands.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  MonCommand *cmd = it->second;
  _finish_command(cmd, monc_errc::timed_out, "timed out"sv, {});
  return 0;
}

void MonClient::_finish_command(MonCommand *r, bs::error_code ret,
				std::string_view rs, ceph::buffer::list&& bl)
{
  ldout(cct, 10) << __func__ << " " << r->tid << " = " << ret << " " << rs
		 << dendl;
  ceph::async::post(std::move(r->onfinish), ret, std::string(rs),
		    std::move(bl));
  if (r->target_con) {
    r->target_con->mark_down();
  }
  mon_commands.erase(r->tid);
  delete r;
}

// ---------

void MonClient::handle_get_version_reply(MMonGetVersionReply* m)
{
  ceph_assert(ceph_mutex_is_locked(monc_lock));
  auto iter = version_requests.find(m->handle);
  if (iter == version_requests.end()) {
    ldout(cct, 0) << __func__ << " version request with handle " << m->handle
		  << " not found" << dendl;
  } else {
    auto req = std::move(iter->second);
    ldout(cct, 10) << __func__ << " finishing " << iter->first << " version "
		   << m->version << dendl;
    version_requests.erase(iter);
    ceph::async::post(std::move(req), bs::error_code(),
		      m->version, m->oldest_version);
  }
  m->put();
}

int MonClient::get_auth_request(
  Connection *con,
  AuthConnectionMeta *auth_meta,
  uint32_t *auth_method,
  std::vector<uint32_t> *preferred_modes,
  ceph::buffer::list *bl)
{
  std::lock_guard l(monc_lock);
  ldout(cct,10) << __func__ << " con " << con << " auth_method " << *auth_method
		<< dendl;

  // connection to mon?
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    ceph_assert(!auth_meta->authorizer);
    if (con->is_anon()) {
      for (auto& i : mon_commands) {
	if (i.second->target_con == con) {
	  return i.second->target_session->get_auth_request(
	    auth_method, preferred_modes, bl,
	    entity_name, want_keys, rotating_secrets.get());
	}
      }
    }
    for (auto& i : pending_cons) {
      if (i.second.is_con(con)) {
	return i.second.get_auth_request(
	  auth_method, preferred_modes, bl,
	  entity_name, want_keys, rotating_secrets.get());
      }
    }
    return -ENOENT;
  }

  // generate authorizer
  if (!auth) {
    lderr(cct) << __func__ << " but no auth handler is set up" << dendl;
    return -EACCES;
  }
  auth_meta->authorizer.reset(auth->build_authorizer(con->get_peer_type()));
  if (!auth_meta->authorizer) {
    lderr(cct) << __func__ << " failed to build_authorizer for type "
	       << ceph_entity_type_name(con->get_peer_type()) << dendl;
    return -EACCES;
  }
  auth_meta->auth_method = auth_meta->authorizer->protocol;
  auth_registry.get_supported_modes(con->get_peer_type(),
				    auth_meta->auth_method,
				    preferred_modes);
  *bl = auth_meta->authorizer->bl;
  return 0;
}

int MonClient::handle_auth_reply_more(
  Connection *con,
  AuthConnectionMeta *auth_meta,
  const ceph::buffer::list& bl,
  ceph::buffer::list *reply)
{
  std::lock_guard l(monc_lock);

  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    if (con->is_anon()) {
      for (auto& i : mon_commands) {
	if (i.second->target_con == con) {
	  return i.second->target_session->handle_auth_reply_more(
	    auth_meta, bl, reply);
	}
      }
    }
    for (auto& i : pending_cons) {
      if (i.second.is_con(con)) {
	return i.second.handle_auth_reply_more(auth_meta, bl, reply);
      }
    }
    return -ENOENT;
  }

  // authorizer challenges
  if (!auth || !auth_meta->authorizer) {
    lderr(cct) << __func__ << " no authorizer?" << dendl;
    return -1;
  }
  auth_meta->authorizer->add_challenge(cct, bl);
  *reply = auth_meta->authorizer->bl;
  return 0;
}

int MonClient::handle_auth_done(
  Connection *con,
  AuthConnectionMeta *auth_meta,
  uint64_t global_id,
  uint32_t con_mode,
  const ceph::buffer::list& bl,
  CryptoKey *session_key,
  std::string *connection_secret)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    std::lock_guard l(monc_lock);
    if (con->is_anon()) {
      for (auto& i : mon_commands) {
	if (i.second->target_con == con) {
	  return i.second->target_session->handle_auth_done(
	    auth_meta, global_id, bl,
	    session_key, connection_secret);
	}
      }
    }
    for (auto& i : pending_cons) {
      if (i.second.is_con(con)) {
	int r = i.second.handle_auth_done(
	  auth_meta, global_id, bl,
	  session_key, connection_secret);
	if (r) {
	  pending_cons.erase(i.first);
	  if (!pending_cons.empty()) {
	    return r;
	  }
	} else {
	  active_con.reset(new MonConnection(std::move(i.second)));
	  pending_cons.clear();
	  ceph_assert(active_con->have_session());
	}

	_finish_hunting(r);
	if (r || monmap.get_epoch() > 0) {
	  _finish_auth(r);
	}
	return r;
      }
    }
    return -ENOENT;
  } else {
    // verify authorizer reply
    auto p = bl.begin();
    if (!auth_meta->authorizer->verify_reply(p, &auth_meta->connection_secret)) {
      ldout(cct, 0) << __func__ << " failed verifying authorizer reply"
		    << dendl;
      return -EACCES;
    }
    auth_meta->session_key = auth_meta->authorizer->session_key;
    return 0;
  }
}

int MonClient::handle_auth_bad_method(
  Connection *con,
  AuthConnectionMeta *auth_meta,
  uint32_t old_auth_method,
  int result,
  const std::vector<uint32_t>& allowed_methods,
  const std::vector<uint32_t>& allowed_modes)
{
  auth_meta->allowed_methods = allowed_methods;

  std::lock_guard l(monc_lock);
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    if (con->is_anon()) {
      for (auto& i : mon_commands) {
	if (i.second->target_con == con) {
	  int r = i.second->target_session->handle_auth_bad_method(
	    old_auth_method,
	    result,
	    allowed_methods,
	    allowed_modes);
	  if (r < 0) {
	    auto ec = bs::error_code(-r, mon_category());
	    _finish_command(i.second, ec, "auth failed"sv, {});
	  }
	  return r;
	}
      }
    }
    for (auto& i : pending_cons) {
      if (i.second.is_con(con)) {
	int r = i.second.handle_auth_bad_method(old_auth_method,
						result,
						allowed_methods,
						allowed_modes);
	if (r == 0) {
	  return r; // try another method on this con
	}
	pending_cons.erase(i.first);
	if (!pending_cons.empty()) {
	  return r;  // fail this con, maybe another con will succeed
	}
	// fail hunt
	_finish_hunting(r);
	_finish_auth(r);
	return r;
      }
    }
    return -ENOENT;
  } else {
    // huh...
    ldout(cct,10) << __func__ << " hmm, they didn't like " << old_auth_method
		  << " result " << cpp_strerror(result)
		  << " and auth is " << (auth ? auth->get_protocol() : 0)
		  << dendl;
    return -EACCES;
  }
}

int MonClient::handle_auth_request(
  Connection *con,
  AuthConnectionMeta *auth_meta,
  bool more,
  uint32_t auth_method,
  const ceph::buffer::list& payload,
  ceph::buffer::list *reply)
{
  if (payload.length() == 0) {
    // for some channels prior to nautilus (osd heartbeat), we
    // tolerate the lack of an authorizer.
    if (!con->get_messenger()->require_authorizer) {
      handle_authentication_dispatcher->ms_handle_fast_authentication(con);
      return 1;
    }
    return -EACCES;
  }
  auth_meta->auth_mode = payload[0];
  if (auth_meta->auth_mode < AUTH_MODE_AUTHORIZER ||
      auth_meta->auth_mode > AUTH_MODE_AUTHORIZER_MAX) {
    return -EACCES;
  }
  AuthAuthorizeHandler *ah = get_auth_authorize_handler(con->get_peer_type(),
							auth_method);
  if (!ah) {
    lderr(cct) << __func__ << " no AuthAuthorizeHandler found for auth method "
	       << auth_method << dendl;
    return -EOPNOTSUPP;
  }

  auto ac = &auth_meta->authorizer_challenge;
  if (auth_meta->skip_authorizer_challenge) {
    ldout(cct, 10) << __func__ << " skipping challenge on " << con << dendl;
    ac = nullptr;
  }

  bool was_challenge = (bool)auth_meta->authorizer_challenge;
  bool isvalid = ah->verify_authorizer(
    cct,
    *rotating_secrets,
    payload,
    auth_meta->get_connection_secret_length(),
    reply,
    &con->peer_name,
    &con->peer_global_id,
    &con->peer_caps_info,
    &auth_meta->session_key,
    &auth_meta->connection_secret,
    ac);
  if (isvalid) {
    handle_authentication_dispatcher->ms_handle_fast_authentication(con);
    return 1;
  }
  if (!more && !was_challenge && auth_meta->authorizer_challenge) {
    ldout(cct,10) << __func__ << " added challenge on " << con << dendl;
    return 0;
  }
  ldout(cct,10) << __func__ << " bad authorizer on " << con << dendl;
  // discard old challenge
  auth_meta->authorizer_challenge.reset();
  return -EACCES;
}

AuthAuthorizer* MonClient::build_authorizer(int service_id) const {
  std::lock_guard l(monc_lock);
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

MonConnection::MonConnection(
  CephContext *cct, ConnectionRef con, uint64_t global_id,
  AuthRegistry *ar)
  : cct(cct), con(con), global_id(global_id), auth_registry(ar)
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
			  const EntityName& entity_name)
{
  using ceph::encode;
  auth_start = ceph_clock_now();

  if (con->get_peer_addr().is_msgr2()) {
    ldout(cct, 10) << __func__ << " opening mon connection" << dendl;
    state = State::AUTHENTICATING;
    con->send_message(new MMonGetMap());
    return;
  }

  // restart authentication handshake
  state = State::NEGOTIATING;

  // send an initial keepalive to ensure our timestamp is valid by the
  // time we are in an OPENED state (by sequencing this before
  // authentication).
  con->send_keepalive();

  auto m = new MAuth;
  m->protocol = CEPH_AUTH_UNKNOWN;
  m->monmap_epoch = epoch;
  __u8 struct_v = 1;
  encode(struct_v, m->auth_payload);
  std::vector<uint32_t> auth_supported;
  auth_registry->get_supported_methods(con->get_peer_type(), &auth_supported);
  encode(auth_supported, m->auth_payload);
  encode(entity_name, m->auth_payload);
  encode(global_id, m->auth_payload);
  con->send_message(m);
}

int MonConnection::get_auth_request(
  uint32_t *method,
  std::vector<uint32_t> *preferred_modes,
  ceph::buffer::list *bl,
  const EntityName& entity_name,
  uint32_t want_keys,
  RotatingKeyRing* keyring)
{
  using ceph::encode;
  // choose method
  if (auth_method < 0) {
    std::vector<uint32_t> as;
    auth_registry->get_supported_methods(con->get_peer_type(), &as);
    if (as.empty()) {
      return -EACCES;
    }
    auth_method = as.front();
  }
  *method = auth_method;
  auth_registry->get_supported_modes(con->get_peer_type(), auth_method,
				     preferred_modes);
  ldout(cct,10) << __func__ << " method " << *method
		<< " preferred_modes " << *preferred_modes << dendl;
  if (preferred_modes->empty()) {
    return -EACCES;
  }

  int r = _init_auth(*method, entity_name, want_keys, keyring, true);
  ceph_assert(r == 0);

  // initial requset includes some boilerplate...
  encode((char)AUTH_MODE_MON, *bl);
  encode(entity_name, *bl);
  encode(global_id, *bl);

  // and (maybe) some method-specific initial payload
  auth->build_initial_request(bl);

  return 0;
}

int MonConnection::handle_auth_reply_more(
  AuthConnectionMeta *auth_meta,
  const ceph::buffer::list& bl,
  ceph::buffer::list *reply)
{
  ldout(cct, 10) << __func__ << " payload " << bl.length() << dendl;
  ldout(cct, 30) << __func__ << " got\n";
  bl.hexdump(*_dout);
  *_dout << dendl;

  auto p = bl.cbegin();
  ldout(cct, 10) << __func__ << " payload_len " << bl.length() << dendl;
  int r = auth->handle_response(0, p, &auth_meta->session_key,
				&auth_meta->connection_secret);
  if (r == -EAGAIN) {
    auth->prepare_build_request();
    auth->build_request(*reply);
    ldout(cct, 10) << __func__ << " responding with " << reply->length()
		   << " bytes" << dendl;
    r = 0;
  } else if (r < 0) {
    lderr(cct) << __func__ << " handle_response returned " << r << dendl;
  } else {
    ldout(cct, 10) << __func__ << " authenticated!" << dendl;
    // FIXME
    ceph_abort(cct, "write me");
  }
  return r;
}

int MonConnection::handle_auth_done(
  AuthConnectionMeta *auth_meta,
  uint64_t new_global_id,
  const ceph::buffer::list& bl,
  CryptoKey *session_key,
  std::string *connection_secret)
{
  ldout(cct,10) << __func__ << " global_id " << new_global_id
		<< " payload " << bl.length()
		<< dendl;
  global_id = new_global_id;
  auth->set_global_id(global_id);
  auto p = bl.begin();
  int auth_err = auth->handle_response(0, p, &auth_meta->session_key,
				       &auth_meta->connection_secret);
  if (auth_err >= 0) {
    state = State::HAVE_SESSION;
  }
  con->set_last_keepalive_ack(auth_start);

  if (pending_tell_command) {
    con->send_message2(std::move(pending_tell_command));
  }
  return auth_err;
}

int MonConnection::handle_auth_bad_method(
  uint32_t old_auth_method,
  int result,
  const std::vector<uint32_t>& allowed_methods,
  const std::vector<uint32_t>& allowed_modes)
{
  ldout(cct,10) << __func__ << " old_auth_method " << old_auth_method
		<< " result " << cpp_strerror(result)
		<< " allowed_methods " << allowed_methods << dendl;
  std::vector<uint32_t> auth_supported;
  auth_registry->get_supported_methods(con->get_peer_type(), &auth_supported);
  auto p = std::find(auth_supported.begin(), auth_supported.end(),
		     old_auth_method);
  assert(p != auth_supported.end());
  p = std::find_first_of(std::next(p), auth_supported.end(),
			 allowed_methods.begin(), allowed_methods.end());
  if (p == auth_supported.end()) {
    lderr(cct) << __func__ << " server allowed_methods " << allowed_methods
	       << " but i only support " << auth_supported << dendl;
    return -EACCES;
  }
  auth_method = *p;
  ldout(cct,10) << __func__ << " will try " << auth_method << " next" << dendl;
  return 0;
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
  ldout(cct, 10) << __func__ << dendl;
  int r = _init_auth(m->protocol, entity_name, want_keys, keyring, false);
  if (r == -ENOTSUP) {
    if (m->result == -ENOTSUP) {
      ldout(cct, 10) << "none of our auth protocols are supported by the server"
		     << dendl;
    }
    return m->result;
  }
  return r;
}

int MonConnection::_init_auth(
  uint32_t method,
  const EntityName& entity_name,
  uint32_t want_keys,
  RotatingKeyRing* keyring,
  bool msgr2)
{
  ldout(cct, 10) << __func__ << " method " << method << dendl;
  if (auth && auth->get_protocol() == (int)method) {
    ldout(cct, 10) << __func__ << " already have auth, reseting" << dendl;
    auth->reset();
    return 0;
  }

  ldout(cct, 10) << __func__ << " creating new auth" << dendl;
  auth.reset(AuthClientHandler::create(cct, method, keyring));
  if (!auth) {
    ldout(cct, 10) << " no handler for protocol " << method << dendl;
    return -ENOTSUP;
  }

  // do not request MGR key unless the mon has the SERVER_KRAKEN
  // feature.  otherwise it will give us an auth error.  note that
  // we have to use the FEATUREMASK because pre-jewel the kraken
  // feature bit was used for something else.
  if (!msgr2 &&
      (want_keys & CEPH_ENTITY_TYPE_MGR) &&
      !(con->has_features(CEPH_FEATUREMASK_SERVER_KRAKEN))) {
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
  ceph_assert(auth);
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
  auto p = m->result_bl.cbegin();
  int ret = auth->handle_response(m->result, p, nullptr, nullptr);
  if (ret == -EAGAIN) {
    auto ma = new MAuth;
    ma->protocol = auth->get_protocol();
    auth->prepare_build_request();
    auth->build_request(ma->auth_payload);
    con->send_message(ma);
  }
  if (ret == 0 && pending_tell_command) {
    con->send_message2(std::move(pending_tell_command));
  }

  return ret;
}

void MonClient::register_config_callback(md_config_t::config_callback fn) {
  ceph_assert(!config_cb);
  config_cb = fn;
}

md_config_t::config_callback MonClient::get_config_callback() {
  return config_cb;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnon-virtual-dtor"
class monc_error_category : public ceph::converting_category {
public:
  monc_error_category(){}
  const char* name() const noexcept override;
  const char* message(int ev, char*, std::size_t) const noexcept override;
  std::string message(int ev) const override;
  bs::error_condition default_error_condition(int ev) const noexcept
    override;
  bool equivalent(int ev, const bs::error_condition& c) const
    noexcept override;
  using ceph::converting_category::equivalent;
  int from_code(int ev) const noexcept override;
};
#pragma GCC diagnostic pop
#pragma clang diagnostic pop

const char* monc_error_category::name() const noexcept {
  return "monc";
}

const char* monc_error_category::message(int ev, char*, std::size_t) const noexcept {
  if (ev == 0)
    return "No error";

  switch (static_cast<monc_errc>(ev)) {
  case monc_errc::shutting_down: // Command failed due to MonClient shutting down
    return "Command failed due to MonClient shutting down";
  case monc_errc::session_reset:
    return "Monitor session was reset";
  case monc_errc::rank_dne:
    return "Requested monitor rank does not exist";
  case monc_errc::mon_dne:
    return "Requested monitor does not exist";
  case monc_errc::timed_out:
    return "Monitor operation timed out";
  case monc_errc::mon_unavailable:
    return "Monitor unavailable";
  }

  return "Unknown error";
}

std::string monc_error_category::message(int ev) const {
  return message(ev, nullptr, 0);
}

bs::error_condition monc_error_category::default_error_condition(int ev) const noexcept {
  switch (static_cast<monc_errc>(ev)) {
  case monc_errc::shutting_down:
    return bs::errc::operation_canceled;
  case monc_errc::session_reset:
    return bs::errc::resource_unavailable_try_again;
  case monc_errc::rank_dne:
    [[fallthrough]];
  case monc_errc::mon_dne:
    return ceph::errc::not_in_map;
  case monc_errc::timed_out:
    return bs::errc::timed_out;
  case monc_errc::mon_unavailable:
    return bs::errc::no_such_device;
  }
  return { ev, *this };
}

bool monc_error_category::equivalent(int ev, const bs::error_condition& c) const noexcept {
  switch (static_cast<monc_errc>(ev)) {
  case monc_errc::rank_dne:
    [[fallthrough]];
  case monc_errc::mon_dne:
      return c == bs::errc::no_such_file_or_directory;
  default:
    return default_error_condition(ev) == c;
  }
}

int monc_error_category::from_code(int ev) const noexcept {
  if (ev == 0)
    return 0;

  switch (static_cast<monc_errc>(ev)) {
  case monc_errc::shutting_down:
    return -ECANCELED;
  case monc_errc::session_reset:
    return -EAGAIN;
  case monc_errc::rank_dne:
    [[fallthrough]];
  case monc_errc::mon_dne:
    return -ENOENT;
  case monc_errc::timed_out:
    return -ETIMEDOUT;
  case monc_errc::mon_unavailable:
    return -ENXIO;
  }
  return -EDOM;
}

const bs::error_category& monc_category() noexcept {
  static const monc_error_category c;
  return c;
}
