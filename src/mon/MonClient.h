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

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include "msg/Messenger.h"

#include "MonMap.h"
#include "MonSub.h"

#include "common/admin_socket.h"
#include "common/async/completion.h"
#include "common/Timer.h"
#include "common/config.h"
#include "messages/MMonGetVersion.h"

#include "auth/AuthClient.h"
#include "auth/AuthServer.h"

class MMonMap;
class MConfig;
class MMonGetVersionReply;
class MMonCommandAck;
class LogClient;
class AuthClientHandler;
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
    ceph::buffer::list *out,
    const EntityName& entity_name,
    uint32_t want_keys,
    RotatingKeyRing* keyring);
  int handle_auth_reply_more(
    AuthConnectionMeta *auth_meta,
    const ceph::buffer::list& bl,
    ceph::buffer::list *reply);
  int handle_auth_done(
    AuthConnectionMeta *auth_meta,
    uint64_t global_id,
    const ceph::buffer::list& bl,
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
  void queue_command(Message *m) {
    pending_tell_command = m;
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

  MessageRef pending_tell_command;

  AuthRegistry *auth_registry;
};


struct MonClientPinger : public Dispatcher,
			 public AuthClient {
  ceph::mutex lock = ceph::make_mutex("MonClientPinger::lock");
  ceph::condition_variable ping_recvd_cond;
  std::string *result;
  bool done;
  RotatingKeyRing *keyring;
  std::unique_ptr<MonConnection> mc;

  MonClientPinger(CephContext *cct_,
		  RotatingKeyRing *keyring,
		  std::string *res_) :
    Dispatcher(cct_),
    result(res_),
    done(false),
    keyring(keyring)
  { }

  int wait_for_reply(double timeout = 0.0) {
    std::unique_lock locker{lock};
    if (timeout <= 0) {
      timeout = std::chrono::duration<double>(cct->_conf.get_val<std::chrono::seconds>("client_mount_timeout")).count();
    }
    done = false;
    if (ping_recvd_cond.wait_for(locker,
				 ceph::make_timespan(timeout),
				 [this] { return done; })) {
      return 0;
    } else {
      return ETIMEDOUT;
    }
  }

  bool ms_dispatch(Message *m) override {
    using ceph::decode;
    std::lock_guard l(lock);
    if (m->get_type() != CEPH_MSG_PING)
      return false;

    ceph::buffer::list &payload = m->get_payload();
    if (result && payload.length() > 0) {
      auto p = std::cbegin(payload);
      decode(*result, p);
    }
    done = true;
    ping_recvd_cond.notify_all();
    m->put();
    return true;
  }
  bool ms_handle_reset(Connection *con) override {
    std::lock_guard l(lock);
    done = true;
    ping_recvd_cond.notify_all();
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
    ceph::buffer::list *bl) override {
    return mc->get_auth_request(auth_method, preferred_modes, bl,
				cct->_conf->name, 0, keyring);
  }
  int handle_auth_reply_more(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    const ceph::buffer::list& bl,
    ceph::buffer::list *reply) override {
    return mc->handle_auth_reply_more(auth_meta, bl, reply);
  }
  int handle_auth_done(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint64_t global_id,
    uint32_t con_mode,
    const ceph::buffer::list& bl,
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

const boost::system::error_category& monc_category() noexcept;

enum class monc_errc {
  shutting_down = 1, // Command failed due to MonClient shutting down
  session_reset, // Monitor session was reset
  rank_dne, // Requested monitor rank does not exist
  mon_dne, // Requested monitor does not exist
  timed_out, // Monitor operation timed out
  mon_unavailable // Monitor unavailable
};

namespace boost::system {
template<>
struct is_error_code_enum<::monc_errc> {
  static const bool value = true;
};
}

//  implicit conversion:
inline boost::system::error_code make_error_code(monc_errc e) noexcept {
  return { static_cast<int>(e), monc_category() };
}

// explicit conversion:
inline boost::system::error_condition make_error_condition(monc_errc e) noexcept {
  return { static_cast<int>(e), monc_category() };
}

const boost::system::error_category& monc_category() noexcept;

class MonClient : public Dispatcher,
		  public AuthClient,
		  public AuthServer, /* for mgr, osd, mds */
		  public AdminSocketHook {
  static constexpr auto dout_subsys = ceph_subsys_monc;
public:
  // Error, Newest, Oldest
  using VersionSig = void(boost::system::error_code, version_t, version_t);
  using VersionCompletion = ceph::async::Completion<VersionSig>;

  using CommandSig = void(boost::system::error_code, std::string,
			  ceph::buffer::list);
  using CommandCompletion = ceph::async::Completion<CommandSig>;

  MonMap monmap;
  std::map<std::string,std::string> config_mgr;

private:
  Messenger *messenger;

  std::unique_ptr<MonConnection> active_con;
  std::map<entity_addrvec_t, MonConnection> pending_cons;
  std::set<unsigned> tried;

  EntityName entity_name;

  mutable ceph::mutex monc_lock = ceph::make_mutex("MonClient::monc_lock");
  SafeTimer timer;
  boost::asio::io_context& service;
  boost::asio::strand<boost::asio::io_context::executor_type>
      finish_strand{service.get_executor()};

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

  int call(
    std::string_view command,
    const cmdmap_t& cmdmap,
    const ceph::buffer::list &inbl,
    ceph::Formatter *f,
    std::ostream& errss,
    ceph::buffer::list& out) override;
  
  // monitor session
  utime_t last_keepalive;
  utime_t last_send_log;

  void tick();
  void schedule_tick();

  // monclient
  bool want_monmap;
  ceph::condition_variable map_cond;
  bool passthrough_monmap = false;

  bool want_bootstrap_config = false;
  ceph::ref_t<MConfig> bootstrap_config;

  // authenticate
  std::unique_ptr<AuthClientHandler> auth;
  uint32_t want_keys = 0;
  uint64_t global_id = 0;
  ceph::condition_variable auth_cond;
  int authenticate_err = 0;
  bool authenticated = false;

  std::list<MessageRef> waiting_for_session;
  utime_t last_rotating_renew_sent;
  bool had_a_connection;
  double reopen_interval_multiplier;

  Dispatcher *handle_authentication_dispatcher = nullptr;
  bool _opened() const;
  bool _hunting() const;
  void _start_hunting();
  void _finish_hunting(int auth_err);
  void _finish_auth(int auth_err);
  void _reopen_session(int rank = -1);
  void _add_conn(unsigned rank);
  void _add_conns();
  void _un_backoff();
  void _send_mon_message(MessageRef m);

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
    ceph::buffer::list *bl) override;
  int handle_auth_reply_more(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    const ceph::buffer::list& bl,
    ceph::buffer::list *reply) override;
  int handle_auth_done(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint64_t global_id,
    uint32_t con_mode,
    const ceph::buffer::list& bl,
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
    const ceph::buffer::list& bl,
    ceph::buffer::list *reply) override;

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
  bool sub_want(std::string what, version_t start, unsigned flags) {
    std::lock_guard l(monc_lock);
    return sub.want(what, start, flags);
  }
  void sub_got(std::string what, version_t have) {
    std::lock_guard l(monc_lock);
    sub.got(what, have);
  }
  void sub_unwant(std::string what) {
    std::lock_guard l(monc_lock);
    sub.unwant(what);
  }
  bool sub_want_increment(std::string what, version_t start, unsigned flags) {
    std::lock_guard l(monc_lock);
    return sub.inc_want(what, start, flags);
  }

  std::unique_ptr<KeyRing> keyring;
  std::unique_ptr<RotatingKeyRing> rotating_secrets;

 public:
  MonClient(CephContext *cct_, boost::asio::io_context& service);
  MonClient(const MonClient &) = delete;
  MonClient& operator=(const MonClient &) = delete;
  ~MonClient() override;

  int init();
  void shutdown();

  void set_log_client(LogClient *clog) {
    log_client = clog;
  }
  LogClient *get_log_client() {
    return log_client;
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
  int ping_monitor(const std::string &mon_id, std::string *result_reply);

  void send_mon_message(Message *m) {
    send_mon_message(MessageRef{m, false});
  }
  void send_mon_message(MessageRef m);

  void reopen_session() {
    std::lock_guard l(monc_lock);
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
    // for tell only
    std::string target_name;
    int target_rank = -1;
    ConnectionRef target_con;
    std::unique_ptr<MonConnection> target_session;
    unsigned send_attempts = 0;  ///< attempt count for legacy mons
    utime_t last_send_attempt;
    uint64_t tid;
    std::vector<std::string> cmd;
    ceph::buffer::list inbl;
    std::unique_ptr<CommandCompletion> onfinish;
    std::optional<boost::asio::steady_timer> cancel_timer;

    MonCommand(MonClient& monc, uint64_t t, std::unique_ptr<CommandCompletion> onfinish)
      : tid(t), onfinish(std::move(onfinish)) {
      auto timeout =
          monc.cct->_conf.get_val<std::chrono::seconds>("rados_mon_op_timeout");
      if (timeout.count() > 0) {
	cancel_timer.emplace(monc.service, timeout);
	cancel_timer->async_wait(
          [this, &monc](boost::system::error_code ec) {
	    if (ec)
	      return;
	    std::scoped_lock l(monc.monc_lock);
	    monc._cancel_mon_command(tid);
	  });
      }
    }

    bool is_tell() const {
      return target_name.size() || target_rank >= 0;
    }
  };
  friend MonCommand;
  std::map<uint64_t,MonCommand*> mon_commands;

  void _send_command(MonCommand *r);
  void _check_tell_commands();
  void _resend_mon_commands();
  int _cancel_mon_command(uint64_t tid);
  void _finish_command(MonCommand *r, boost::system::error_code ret, std::string_view rs,
		       bufferlist&& bl);
  void _finish_auth();
  void handle_mon_command_ack(MMonCommandAck *ack);
  void handle_command_reply(MCommandReply *reply);

public:
  template<typename CompletionToken>
  auto start_mon_command(const std::vector<std::string>& cmd,
                         const ceph::buffer::list& inbl,
			 CompletionToken&& token) {
    ldout(cct,10) << __func__ << " cmd=" << cmd << dendl;
    boost::asio::async_completion<CompletionToken, CommandSig> init(token);
    {
      std::scoped_lock l(monc_lock);
      auto h = CommandCompletion::create(service.get_executor(),
					 std::move(init.completion_handler));
      if (!initialized || stopping) {
	ceph::async::post(std::move(h), monc_errc::shutting_down, std::string{},
			  bufferlist{});
      } else {
	auto r = new MonCommand(*this, ++last_mon_command_tid, std::move(h));
	r->cmd = cmd;
	r->inbl = inbl;
	mon_commands.emplace(r->tid, r);
	_send_command(r);
      }
    }
    return init.result.get();
  }

  template<typename CompletionToken>
  auto start_mon_command(int mon_rank, const std::vector<std::string>& cmd,
			 const ceph::buffer::list& inbl, CompletionToken&& token) {
    ldout(cct,10) << __func__ << " cmd=" << cmd << dendl;
    boost::asio::async_completion<CompletionToken, CommandSig> init(token);
    {
      std::scoped_lock l(monc_lock);
      auto h = CommandCompletion::create(service.get_executor(),
					 std::move(init.completion_handler));
      if (!initialized || stopping) {
	ceph::async::post(std::move(h), monc_errc::shutting_down, std::string{},
			  bufferlist{});
      } else {
	auto r = new MonCommand(*this, ++last_mon_command_tid, std::move(h));
	r->target_rank = mon_rank;
	r->cmd = cmd;
	r->inbl = inbl;
	mon_commands.emplace(r->tid, r);
	_send_command(r);
      }
    }
    return init.result.get();
  }

  template<typename CompletionToken>
  auto start_mon_command(const std::string& mon_name,
                         const std::vector<std::string>& cmd,
			 const ceph::buffer::list& inbl,
			 CompletionToken&& token) {
    ldout(cct,10) << __func__ << " cmd=" << cmd << dendl;
    boost::asio::async_completion<CompletionToken, CommandSig> init(token);
    {
      std::scoped_lock l(monc_lock);
      auto h = CommandCompletion::create(service.get_executor(),
					 std::move(init.completion_handler));
      if (!initialized || stopping) {
	ceph::async::post(std::move(h), monc_errc::shutting_down, std::string{},
			  bufferlist{});
      } else {
	auto r = new MonCommand(*this, ++last_mon_command_tid, std::move(h));
	// detect/tolerate mon *rank* passed as a string
	std::string err;
	int rank = strict_strtoll(mon_name.c_str(), 10, &err);
	if (err.size() == 0 && rank >= 0) {
	  ldout(cct,10) << __func__ << " interpreting name '" << mon_name
			<< "' as rank " << rank << dendl;
	  r->target_rank = rank;
	} else {
	  r->target_name = mon_name;
	}
	r->cmd = cmd;
	r->inbl = inbl;
	mon_commands.emplace(r->tid, r);
	_send_command(r);
      }
    }
    return init.result.get();
  }

  class ContextVerter {
    std::string* outs;
    ceph::bufferlist* outbl;
    Context* onfinish;

  public:
    ContextVerter(std::string* outs, ceph::bufferlist* outbl, Context* onfinish)
      : outs(outs), outbl(outbl), onfinish(onfinish) {}
    ~ContextVerter() = default;
    ContextVerter(const ContextVerter&) = default;
    ContextVerter& operator =(const ContextVerter&) = default;
    ContextVerter(ContextVerter&&) = default;
    ContextVerter& operator =(ContextVerter&&) = default;

    void operator()(boost::system::error_code e,
		    std::string s,
		    ceph::bufferlist bl) {
      if (outs)
	*outs = std::move(s);
      if (outbl)
	*outbl = std::move(bl);
      if (onfinish)
	onfinish->complete(ceph::from_error_code(e));
    }
  };

  void start_mon_command(const std::vector<std::string>& cmd, const bufferlist& inbl,
			 bufferlist *outbl, std::string *outs,
			 Context *onfinish) {
    start_mon_command(cmd, inbl, ContextVerter(outs, outbl, onfinish));
  }
  void start_mon_command(int mon_rank,
			 const std::vector<std::string>& cmd, const bufferlist& inbl,
			 bufferlist *outbl, std::string *outs,
			 Context *onfinish) {
    start_mon_command(mon_rank, cmd, inbl, ContextVerter(outs, outbl, onfinish));
  }
  void start_mon_command(const std::string &mon_name,  ///< mon name, with mon. prefix
			 const std::vector<std::string>& cmd, const bufferlist& inbl,
			 bufferlist *outbl, std::string *outs,
			 Context *onfinish) {
    start_mon_command(mon_name, cmd, inbl, ContextVerter(outs, outbl, onfinish));
  }


  // version requests
public:
  /**
   * get latest known version(s) of cluster map
   *
   * @param map string name of map (e.g., 'osdmap')
   * @param token context that will be triggered on completion
   * @return (via Completion) {} on success,
   *         boost::system::errc::resource_unavailable_try_again if we need to
   *         resubmit our request
   */
  template<typename CompletionToken>
  auto get_version(std::string&& map, CompletionToken&& token) {
    boost::asio::async_completion<CompletionToken, VersionSig> init(token);
    {
      std::scoped_lock l(monc_lock);
      auto m = ceph::make_message<MMonGetVersion>();
      m->what = std::move(map);
      m->handle = ++version_req_id;
      version_requests.emplace(m->handle,
			       VersionCompletion::create(
				 service.get_executor(),
				 std::move(init.completion_handler)));
      _send_mon_message(m);
    }
    return init.result.get();
  }

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

  std::map<ceph_tid_t, std::unique_ptr<VersionCompletion>> version_requests;
  ceph_tid_t version_req_id;
  void handle_get_version_reply(MMonGetVersionReply* m);
  md_config_t::config_callback config_cb;
  std::function<void(void)> config_notify_cb;
};

#endif
