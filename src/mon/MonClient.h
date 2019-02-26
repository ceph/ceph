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

#include <memory>

#include "msg/Messenger.h"

#include "MonMap.h"
#include "MonSub.h"

#include "common/Timer.h"
#include "common/Finisher.h"
#include "common/config.h"

#include "auth/AuthClient.h"
#include "auth/AuthServer.h"

class MMonMap;
class MConfig;
class MMonGetVersionReply;
struct MMonSubscribeAck;
class MMonCommandAck;
struct MAuthReply;
class MAuthRotating;
class LogClient;
class AuthAuthorizer;
class AuthClientHandler;
class AuthMethodList;
class AuthRegistry;
class KeyRing;
class RotatingKeyRing;

class MonConnection {
public:
  MonConnection(CephContext *cct,
		ConnectionRef conn,
		uint64_t global_id,
		AuthRegistry *auth_registry);
  ~MonConnection();
  MonConnection(MonConnection&& rhs) = default;
  MonConnection& operator=(MonConnection&&) = default;
  MonConnection(const MonConnection& rhs) = delete;
  MonConnection& operator=(const MonConnection&) = delete;
  int handle_auth(MAuthReply *m,
		  const EntityName& entity_name,
		  uint32_t want_keys,
		  RotatingKeyRing* keyring);
  int authenticate(MAuthReply *m);
  void start(epoch_t epoch,
             const EntityName& entity_name);
  bool have_session() const;
  uint64_t get_global_id() const {
    return global_id;
  }
  ConnectionRef get_con() {
    return con;
  }
  std::unique_ptr<AuthClientHandler>& get_auth() {
    return auth;
  }

  int get_auth_request(
    uint32_t *method,
    std::vector<uint32_t> *preferred_modes,
    bufferlist *out,
    const EntityName& entity_name,
    uint32_t want_keys,
    RotatingKeyRing* keyring);
  int handle_auth_reply_more(
    AuthConnectionMeta *auth_meta,
    const bufferlist& bl,
    bufferlist *reply);
  int handle_auth_done(
    AuthConnectionMeta *auth_meta,
    uint64_t global_id,
    const bufferlist& bl,
    CryptoKey *session_key,
    std::string *connection_secret);
  int handle_auth_bad_method(
    uint32_t old_auth_method,
    int result,
    const std::vector<uint32_t>& allowed_methods,
    const std::vector<uint32_t>& allowed_modes);

  bool is_con(Connection *c) const {
    return con.get() == c;
  }

private:
  int _negotiate(MAuthReply *m,
		 const EntityName& entity_name,
		 uint32_t want_keys,
		 RotatingKeyRing* keyring);
  int _init_auth(uint32_t method,
		 const EntityName& entity_name,
		 uint32_t want_keys,
		 RotatingKeyRing* keyring,
		 bool msgr2);

private:
  CephContext *cct;
  enum class State {
    NONE,
    NEGOTIATING,       // v1 only
    AUTHENTICATING,    // v1 and v2
    HAVE_SESSION,
  };
  State state = State::NONE;
  ConnectionRef con;
  int auth_method = -1;
  utime_t auth_start;

  std::unique_ptr<AuthClientHandler> auth;
  uint64_t global_id;

  AuthRegistry *auth_registry;
};


struct MonClientPinger : public Dispatcher,
			 public AuthClient {

  Mutex lock;
  Cond ping_recvd_cond;
  string *result;
  bool done;
  RotatingKeyRing *keyring;
  std::unique_ptr<MonConnection> mc;

  MonClientPinger(CephContext *cct_,
		  RotatingKeyRing *keyring,
		  string *res_) :
    Dispatcher(cct_),
    lock("MonClientPinger::lock"),
    result(res_),
    done(false),
    keyring(keyring)
  { }

  int wait_for_reply(double timeout = 0.0) {
    utime_t until = ceph_clock_now();
    until += (timeout > 0 ? timeout : cct->_conf->client_mount_timeout);
    done = false;

    int ret = 0;
    while (!done) {
      ret = ping_recvd_cond.WaitUntil(lock, until);
      if (ret == ETIMEDOUT)
        break;
    }
    return ret;
  }

  bool ms_dispatch(Message *m) override {
    std::lock_guard l(lock);
    if (m->get_type() != CEPH_MSG_PING)
      return false;

    bufferlist &payload = m->get_payload();
    if (result && payload.length() > 0) {
      auto p = std::cbegin(payload);
      decode(*result, p);
    }
    done = true;
    ping_recvd_cond.SignalAll();
    m->put();
    return true;
  }
  bool ms_handle_reset(Connection *con) override {
    std::lock_guard l(lock);
    done = true;
    ping_recvd_cond.SignalAll();
    return true;
  }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override {
    return false;
  }

  // AuthClient
  int get_auth_request(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint32_t *auth_method,
    std::vector<uint32_t> *preferred_modes,
    bufferlist *bl) override {
    return mc->get_auth_request(auth_method, preferred_modes, bl,
				cct->_conf->name, 0, keyring);
  }
  int handle_auth_reply_more(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    const bufferlist& bl,
    bufferlist *reply) override {
    return mc->handle_auth_reply_more(auth_meta, bl, reply);
  }
  int handle_auth_done(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint64_t global_id,
    uint32_t con_mode,
    const bufferlist& bl,
    CryptoKey *session_key,
    std::string *connection_secret) override {
    return mc->handle_auth_done(auth_meta, global_id, bl,
				session_key, connection_secret);
  }
  int handle_auth_bad_method(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint32_t old_auth_method,
    int result,
    const std::vector<uint32_t>& allowed_methods,
    const std::vector<uint32_t>& allowed_modes) override {
    return mc->handle_auth_bad_method(old_auth_method, result,
				      allowed_methods, allowed_modes);
  }
};


class MonClient : public Dispatcher,
		  public AuthClient,
		  public AuthServer /* for mgr, osd, mds */ {
public:
  MonMap monmap;
  map<string,string> config_mgr;

private:
  Messenger *messenger;

  std::unique_ptr<MonConnection> active_con;
  std::map<entity_addrvec_t, MonConnection> pending_cons;

  EntityName entity_name;

  mutable Mutex monc_lock;
  SafeTimer timer;
  Finisher finisher;

  bool initialized;
  bool stopping = false;

  LogClient *log_client;
  bool more_log_pending;

  void send_log(bool flush = false);

  bool ms_dispatch(Message *m) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; }

  void handle_monmap(MMonMap *m);
  void handle_config(MConfig *m);

  void handle_auth(MAuthReply *m);

  // monitor session
  void tick();
  void schedule_tick();

  // monclient
  bool want_monmap;
  Cond map_cond;
  bool passthrough_monmap = false;
  bool got_config = false;

  // authenticate
  std::unique_ptr<AuthClientHandler> auth;
  uint32_t want_keys = 0;
  uint64_t global_id = 0;
  Cond auth_cond;
  int authenticate_err = 0;
  bool authenticated = false;

  list<Message*> waiting_for_session;
  utime_t last_rotating_renew_sent;
  std::unique_ptr<Context> session_established_context;
  bool had_a_connection;
  double reopen_interval_multiplier;

  Dispatcher *handle_authentication_dispatcher = nullptr;
  
  bool _opened() const;
  bool _hunting() const;
  void _start_hunting();
  void _finish_hunting(int auth_err);
  void _finish_auth(int auth_err);
  void _reopen_session(int rank = -1);
  MonConnection& _add_conn(unsigned rank, uint64_t global_id);
  void _un_backoff();
  void _add_conns(uint64_t global_id);
  void _send_mon_message(Message *m);

  std::map<entity_addrvec_t, MonConnection>::iterator _find_pending_con(
    const ConnectionRef& con) {
    for (auto i = pending_cons.begin(); i != pending_cons.end(); ++i) {
      if (i->second.get_con() == con) {
	return i;
      }
    }
    return pending_cons.end();
  }

public:
  // AuthClient
  int get_auth_request(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint32_t *method,
    std::vector<uint32_t> *preferred_modes,
    bufferlist *bl) override;
  int handle_auth_reply_more(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    const bufferlist& bl,
    bufferlist *reply) override;
  int handle_auth_done(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint64_t global_id,
    uint32_t con_mode,
    const bufferlist& bl,
    CryptoKey *session_key,
    std::string *connection_secret) override;
  int handle_auth_bad_method(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint32_t old_auth_method,
    int result,
    const std::vector<uint32_t>& allowed_methods,
    const std::vector<uint32_t>& allowed_modes) override;
  // AuthServer
  int handle_auth_request(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    bool more,
    uint32_t auth_method,
    const bufferlist& bl,
    bufferlist *reply);

  void set_entity_name(EntityName name) { entity_name = name; }
  void set_handle_authentication_dispatcher(Dispatcher *d) {
    handle_authentication_dispatcher = d;
  }
  int _check_auth_tickets();
  int _check_auth_rotating();
  int wait_auth_rotating(double timeout);

  int authenticate(double timeout=0.0);
  bool is_authenticated() const {return authenticated;}

  bool is_connected() const { return active_con != nullptr; }

  /**
   * Try to flush as many log messages as we can in a single
   * message.  Use this before shutting down to transmit your
   * last message.
   */
  void flush_log();

private:
  // mon subscriptions
  MonSub sub;
  void _renew_subs();
  void handle_subscribe_ack(MMonSubscribeAck* m);

public:
  void renew_subs() {
    std::lock_guard l(monc_lock);
    _renew_subs();
  }
  bool sub_want(string what, version_t start, unsigned flags) {
    std::lock_guard l(monc_lock);
    return sub.want(what, start, flags);
  }
  void sub_got(string what, version_t have) {
    std::lock_guard l(monc_lock);
    sub.got(what, have);
  }
  void sub_unwant(string what) {
    std::lock_guard l(monc_lock);
    sub.unwant(what);
  }
  bool sub_want_increment(string what, version_t start, unsigned flags) {
    std::lock_guard l(monc_lock);
    return sub.inc_want(what, start, flags);
  }
  
  std::unique_ptr<KeyRing> keyring;
  std::unique_ptr<RotatingKeyRing> rotating_secrets;

 public:
  explicit MonClient(CephContext *cct_);
  MonClient(const MonClient &) = delete;
  MonClient& operator=(const MonClient &) = delete;
  ~MonClient() override;

  int init();
  void shutdown();

  void set_log_client(LogClient *clog) {
    log_client = clog;
  }

  int build_initial_monmap();
  int get_monmap();
  int get_monmap_and_config();
  /**
   * If you want to see MonMap messages, set this and
   * the MonClient will tell the Messenger it hasn't
   * dealt with it.
   * Note that if you do this, *you* are of course responsible for
   * putting the message reference!
   */
  void set_passthrough_monmap() {
    std::lock_guard l(monc_lock);
    passthrough_monmap = true;
  }
  void unset_passthrough_monmap() {
    std::lock_guard l(monc_lock);
    passthrough_monmap = false;
  }
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

  void send_mon_message(Message *m) {
    std::lock_guard l(monc_lock);
    _send_mon_message(m);
  }
  /**
   * If you specify a callback, you should not call
   * reopen_session() again until it has been triggered. The MonClient
   * will behave, but the first callback could be triggered after
   * the session has been killed and the MonClient has started trying
   * to reconnect to another monitor.
   */
  void reopen_session(Context *cb=NULL) {
    std::lock_guard l(monc_lock);
    if (cb) {
      session_established_context.reset(cb);
    }
    _reopen_session();
  }

  const uuid_d& get_fsid() const {
    return monmap.fsid;
  }

  entity_addrvec_t get_mon_addrs(unsigned i) const {
    std::lock_guard l(monc_lock);
    if (i < monmap.size())
      return monmap.get_addrs(i);
    return entity_addrvec_t();
  }
  int get_num_mon() const {
    std::lock_guard l(monc_lock);
    return monmap.size();
  }

  uint64_t get_global_id() const {
    std::lock_guard l(monc_lock);
    return global_id;
  }

  void set_messenger(Messenger *m) { messenger = m; }
  entity_addrvec_t get_myaddrs() const { return messenger->get_myaddrs(); }
  AuthAuthorizer* build_authorizer(int service_id) const;

  void set_want_keys(uint32_t want) {
    want_keys = want;
  }

  // admin commands
private:
  uint64_t last_mon_command_tid;

  struct MonCommand {
    string target_name;
    int target_rank;
    uint64_t tid;
    vector<string> cmd;
    bufferlist inbl;
    bufferlist *poutbl;
    string *prs;
    int *prval;
    Context *onfinish, *ontimeout;

    explicit MonCommand(uint64_t t)
      : target_rank(-1),
	tid(t),
	poutbl(NULL), prs(NULL), prval(NULL), onfinish(NULL), ontimeout(NULL)
    {}
  };
  map<uint64_t,MonCommand*> mon_commands;

  void _send_command(MonCommand *r);
  void _resend_mon_commands();
  int _cancel_mon_command(uint64_t tid);
  void _finish_command(MonCommand *r, int ret, string rs);
  void _finish_auth();
  void handle_mon_command_ack(MMonCommandAck *ack);

public:
  void start_mon_command(const vector<string>& cmd, const bufferlist& inbl,
			bufferlist *outbl, string *outs,
			Context *onfinish);
  void start_mon_command(int mon_rank,
			const vector<string>& cmd, const bufferlist& inbl,
			bufferlist *outbl, string *outs,
			Context *onfinish);
  void start_mon_command(const string &mon_name,  ///< mon name, with mon. prefix
			const vector<string>& cmd, const bufferlist& inbl,
			bufferlist *outbl, string *outs,
			Context *onfinish);

  // version requests
public:
  /**
   * get latest known version(s) of cluster map
   *
   * @param map string name of map (e.g., 'osdmap')
   * @param newest pointer where newest map version will be stored
   * @param oldest pointer where oldest map version will be stored
   * @param onfinish context that will be triggered on completion
   * @return (via context) 0 on success, -EAGAIN if we need to resubmit our request
   */
  void get_version(string map, version_t *newest, version_t *oldest, Context *onfinish);
  /**
   * Run a callback within our lock, with a reference
   * to the MonMap
   */
  template<typename Callback, typename...Args>
  auto with_monmap(Callback&& cb, Args&&...args) const ->
    decltype(cb(monmap, std::forward<Args>(args)...)) {
    std::lock_guard l(monc_lock);
    return std::forward<Callback>(cb)(monmap, std::forward<Args>(args)...);
  }

  void register_config_callback(md_config_t::config_callback fn);
  void register_config_notify_callback(std::function<void(void)> f) {
    config_notify_cb = f;
  }
  md_config_t::config_callback get_config_callback();

private:
  struct version_req_d {
    Context *context;
    version_t *newest, *oldest;
    version_req_d(Context *con, version_t *n, version_t *o) : context(con),newest(n), oldest(o) {}
  };

  map<ceph_tid_t, version_req_d*> version_requests;
  ceph_tid_t version_req_id;
  void handle_get_version_reply(MMonGetVersionReply* m);

  md_config_t::config_callback config_cb;
  std::function<void(void)> config_notify_cb;
};

#endif
