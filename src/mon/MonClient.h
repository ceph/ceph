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

#ifndef CEPH_MONCLIENT_H
#define CEPH_MONCLIENT_H

#include <condition_variable>
#include <memory>
#include <mutex>
#include <random>

#include <boost/intrusive/set.hpp>

#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "MonMap.h"

#include "common/ceph_timer.h"
#include "common/Finisher.h"

#include "auth/AuthClientHandler.h"
#include "auth/RotatingKeyRing.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonGetVersion.h"

#include "osd/osd_types.h"


class MonMap;
class MMonMap;
class MMonGetVersionReply;
struct MMonSubscribeAck;
class MMonCommandAck;
class MCommandReply;
struct MAuthReply;
class MAuthRotating;
class MPing;
class LogClient;
class AuthSupported;
class AuthAuthorizeHandlerRegistry;
class AuthMethodList;

enum MonClientState {
  MC_STATE_NONE,
  MC_STATE_NEGOTIATING,
  MC_STATE_AUTHENTICATING,
  MC_STATE_HAVE_SESSION,
};

struct MonClientPinger : public Dispatcher {
  std::mutex lock;
  using lock_guard = std::lock_guard<decltype(lock)>;
  using unique_lock = std::unique_lock<decltype(lock)>;
  std::condition_variable ping_recvd_cond;
  string *result;
  bool done;

  MonClientPinger(CephContext *cct_, string *res_)
    : Dispatcher(cct_), result(res_), done(false) { }

  int wait_for_reply(unique_lock& l, double timeout = 0.0) {
    auto dur = ceph::make_timespan(timeout == 0.0 ?
				   cct->_conf->client_mount_timeout :
				   timeout);
    done = false;

    ping_recvd_cond.wait_for(l, dur, [this] { return done; });
    if (!done)
      return -ETIMEDOUT;

    return 0;
  }

  bool ms_dispatch(Message *m) {
    lock_guard l(lock);
    if (m->get_type() != CEPH_MSG_PING)
      return false;

    bufferlist &payload = m->get_payload();
    if (result && payload.length() > 0) {
      auto p = payload.begin();
      ::decode(*result, p);
    }
    done = true;
    ping_recvd_cond.notify_all();
    m->put();
    return true;
  }
  bool ms_handle_reset(Connection *con) {
    lock_guard l(lock);
    done = true;
    ping_recvd_cond.notify_all();
    return true;
  }
  void ms_handle_remote_reset(Connection *con) {}
};

class MonClient : public Dispatcher {
public:
  MonMap monmap;
private:
  MonClientState state = MC_STATE_NONE;

  Messenger *messenger = nullptr;

  string cur_mon;
  ConnectionRef cur_con = nullptr;

  std::minstd_rand rng;

  EntityName entity_name;

  entity_addr_t my_addr;

  std::mutex monc_lock;
  using lock_guard = std::lock_guard<decltype(monc_lock)>;
  using unique_lock = std::unique_lock<decltype(monc_lock)>;
  ceph::timer<ceph::mono_clock> timer;
  Finisher finisher;

  // Added to support session signatures.  PLR

  AuthAuthorizeHandlerRegistry *authorize_handler_registry = nullptr;

  bool initialized = false;
  bool no_keyring_disabled_cephx = false;

  LogClient *log_client = nullptr;
  bool more_log_pending = false;

  void send_log();

  std::unique_ptr<AuthMethodList> auth_supported;

  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

  void handle_monmap(MMonMap *m);

  void handle_auth(unique_lock& l, MAuthReply *m);

  // monitor session
  bool hunting = true;

  void tick();
  ceph::timespan tick_time();

  std::condition_variable auth_cond;

  void handle_auth_rotating_response(MAuthRotating *m);
  // monclient
  bool want_monmap = true;

  uint32_t want_keys = 0;

  uint64_t global_id = 0;

  // authenticate
private:
  std::condition_variable map_cond;
  int authenticate_err = 0;

  std::vector<MessageRef> waiting_for_session;
  using thunk = cxx_function::unique_function<void()&& noexcept>;
  std::unique_ptr<thunk> session_established;
  bool had_a_connection = false;
  double reopen_interval_multiplier = 1.0;

  string _pick_random_mon();
  void _finish_hunting();
  void _reopen_session(int rank, string name);
  void _reopen_session() {
    _reopen_session(-1, string());
  }
  void _send_mon_message(MessageRef&& m, bool force=false);

public:
  void set_entity_name(EntityName name) { entity_name = name; }

  int _check_auth_tickets();
  int _check_auth_rotating();
  int wait_auth_rotating(double timeout);

  int authenticate(double timeout=0.0);

  /**
   * Try to flush as many log messages as we can in a single
   * message.  Use this before shutting down to transmit your
   * last message.
   */
  void flush_log();

  // mon subscriptions
private:
  map<string,ceph_mon_subscribe_item> sub_sent; // my subs, and
						// current versions
  map<string,ceph_mon_subscribe_item> sub_new;  // unsent new subs
  ceph::real_time sub_renew_sent = ceph::real_time::min();
  ceph::real_time sub_renew_after = ceph::real_time::min();

  void _renew_subs();
  void handle_subscribe_ack(MMonSubscribeAck* m);

  bool _sub_want(string what, version_t start, unsigned flags) {
    if ((sub_new.count(what) == 0 &&
	 sub_sent.count(what) &&
	 sub_sent[what].start == start &&
	 sub_sent[what].flags == flags) ||
	(sub_new.count(what) &&
	 sub_new[what].start == start &&
	 sub_new[what].flags == flags))
      return false;
    sub_new[what].start = start;
    sub_new[what].flags = flags;
    return true;
  }
  void _sub_got(string what, version_t got) {
    if (sub_new.count(what)) {
      if (sub_new[what].start <= got) {
	if (sub_new[what].flags & CEPH_SUBSCRIBE_ONETIME)
	  sub_new.erase(what);
	else
	  sub_new[what].start = got + 1;
      }
    } else if (sub_sent.count(what)) {
      if (sub_sent[what].start <= got) {
	if (sub_sent[what].flags & CEPH_SUBSCRIBE_ONETIME)
	  sub_sent.erase(what);
	else
	  sub_sent[what].start = got + 1;
      }
    }
  }
  void _sub_unwant(string what) {
    sub_sent.erase(what);
    sub_new.erase(what);
  }

  // auth tickets
public:
  std::unique_ptr<AuthClientHandler> auth;
public:
  void renew_subs() {
    lock_guard l(monc_lock);
    _renew_subs();
  }
  bool sub_want(string what, version_t start, unsigned flags) {
    lock_guard l(monc_lock);
    return _sub_want(what, start, flags);
  }
  void sub_got(string what, version_t have) {
    lock_guard l(monc_lock);
    _sub_got(what, have);
  }
  void sub_unwant(string what) {
    lock_guard l(monc_lock);
    _sub_unwant(what);
  }
  /**
   * Increase the requested subscription start point. If you do increase
   * the value, apply the passed-in flags as well; otherwise do nothing.
   */
  bool sub_want_increment(string what, version_t start, unsigned flags) {
    lock_guard l(monc_lock);
    auto i = sub_new.find(what);
    if (i != sub_new.end()) {
      if (i->second.start >= start)
	return false;
      i->second.start = start;
      i->second.flags = flags;
      return true;
    }

    i = sub_sent.find(what);
    if (i == sub_sent.end() || i->second.start < start) {
      ceph_mon_subscribe_item& item = sub_new[what];
      item.start = start;
      item.flags = flags;
      return true;
    }
    return false;
  }

  std::unique_ptr<KeyRing> keyring;
  std::unique_ptr<RotatingKeyRing> rotating_secrets;

 public:
  explicit MonClient(CephContext *cct_);
  ~MonClient();

  int init();
  void shutdown();

  void set_log_client(LogClient *clog) {
    log_client = clog;
  }

  int build_initial_monmap();
  int get_monmap();
  int get_monmap_privately();
  /**
   * Ping monitor with ID @p mon_id and record the resulting
   * reply in @p result_reply.
   *
   * @param[in]  mon_id Target monitor's ID
   * @param[out] result_reply reply from mon.ID, if param != NULL
   * @returns    0 in case of success; < 0 in case of error,
   *             -ETIMEDOUT if monitor didn't reply before timeout
   *             expired (default: conf->client_mount_timeout).
   */
  int ping_monitor(const string &mon_id, string *result_reply);

  void send_mon_message(MessageRef&& m) {
    lock_guard l(monc_lock);
    _send_mon_message(std::move(m));
  }
  /**
   * If you specify a callback, you should not call
   * reopen_session() again until it has been triggered. The MonClient
   * will behave, but the first callback could be triggered after
   * the session has been killed and the MonClient has started trying
   * to reconnect to another monitor.
   */
  void reopen_session() {
    lock_guard l(monc_lock);
    session_established.reset();
    _reopen_session();
  }

  template<typename... Args>
  void reopen_session(Args&&... args) {
    lock_guard l(monc_lock);
    session_established = std::unique_ptr<thunk>(
      new thunk(std::forward<Args>(args)...));
    _reopen_session();
  }

  entity_addr_t get_my_addr() const {
    return my_addr;
  }

  const uuid_d& get_fsid() {
    return monmap.fsid;
  }

  entity_addr_t get_mon_addr(unsigned i) {
    lock_guard l(monc_lock);
    if (i < monmap.size())
      return monmap.get_addr(i);
    return entity_addr_t();
  }
  entity_inst_t get_mon_inst(unsigned i) {
    lock_guard l(monc_lock);
    if (i < monmap.size())
      return monmap.get_inst(i);
    return entity_inst_t();
  }
  int get_num_mon() {
    lock_guard l(monc_lock);
    return monmap.size();
  }

  uint64_t get_global_id() const {
    return global_id;
  }

  void set_messenger(Messenger *m) { messenger = m; }

  void send_auth_message(MessageRef&& m) {
    _send_mon_message(std::move(m), true);
  }

  void set_want_keys(uint32_t want) {
    want_keys = want;
    if (auth)
      auth->set_want_keys(want | CEPH_ENTITY_TYPE_MON);
  }

  void add_want_keys(uint32_t want) {
    want_keys |= want;
    if (auth)
      auth->add_want_keys(want);
  }

  using MonCommand_cb = cxx_function::unique_function<
    void(int&, string&, bufferlist&)&& noexcept>;

  // admin commands
private:
  uint64_t last_mon_command_tid = 0;

  struct MonCommand : public boost::intrusive::set_base_hook<> {
    struct compare {
      bool operator()(const MonCommand& l, const MonCommand& r) const {
	return l.tid < r.tid;
      }

      bool operator()(const MonCommand& l, const uint64_t& r) const {
	return l.tid < r;
      }

      bool operator()(const uint64_t& l, const MonCommand& r) const {
	return l < r.tid;
      }
    };
    std::string target_name;
    int target_rank = -1;
    uint64_t tid;
    std::vector<std::string> cmd;
    bufferlist inbl;
    MonCommand_cb onfinish;
    uint64_t ontimeout = 0;

    explicit MonCommand(uint64_t t) : tid(t) { }
  };
  // For our purposes, we treat this as owning its contents
  boost::intrusive::set<
    MonCommand,
    boost::intrusive::compare<MonCommand::compare> > mon_commands;

  std::unique_ptr<MonCommand> _reclaim_mon_command(
    decltype(mon_commands)::iterator i) {
    std::unique_ptr<MonCommand> m(&(*i));
    mon_commands.erase(i);
    return m;
  }
  void _send_and_record(std::unique_ptr<MonCommand> m) {
    MonCommand& q = *m; // Non-owning reference we pass to send
    mon_commands.insert(*(m.release()));
    _send_command(q);
  }
  void _send_command(MonCommand& r);
  void _resend_mon_commands();
  int _cancel_mon_command(uint64_t tid, int r);
  void cancel_mon_commands();
  void _finish_command(std::unique_ptr<MonCommand> r, int ret, string&& rs,
		       bufferlist&& rbl);
  void handle_mon_command_ack(MMonCommandAck *ack);

public:
  int start_mon_command(const vector<string>& cmd, const bufferlist& inbl,
			MonCommand_cb&& onfinish);
  int start_mon_command(int mon_rank,
			const vector<string>& cmd, const bufferlist& inbl,
			MonCommand_cb&& onfinish);
  int start_mon_command(const string &mon_name, ///< mon name, with mon. prefix
			const vector<string>& cmd, const bufferlist& inbl,
			MonCommand_cb&& onfinish);

  // version requests
public:

  // Response code, newest, oldest
  using Version_cb = cxx_function::unique_function<
    void(int, version_t, version_t) &&>;

  /**
   * get latest known version(s) of cluster map
   *
   * @param map string name of map (e.g., 'osdmap')
   * @param onfinish Callback to be triggered on success
   *
   * \note Versions are invalid unless response code is 0
   *       -EAGAIN means we need to resubmit.
   */
  template<typename... Args>
  void get_version(string map, Version_cb&& onfinish) {
    lock_guard l(monc_lock);
    boost::intrusive_ptr<MMonGetVersion> m(new MMonGetVersion, false);
    m->what = map;
    m->handle = ++version_req_id;
    version_requests.emplace(m->handle,
			     std::move(onfinish));
    _send_mon_message(std::move(m));
  }

private:

  std::map<ceph_tid_t, Version_cb> version_requests;
  ceph_tid_t version_req_id = 0;
  void handle_get_version_reply(MMonGetVersionReply* m);


  MonClient(const MonClient &rhs);
  MonClient& operator=(const MonClient &rhs);
};

#endif
