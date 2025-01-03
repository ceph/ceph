// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>

#include "auth/AuthRegistry.h"
#include "auth/KeyRing.h"
#include "common/ceph_context.h"

#include "crimson/auth/AuthClient.h"
#include "crimson/auth/AuthServer.h"
#include "crimson/common/auth_handler.h"
#include "crimson/common/gated.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Fwd.h"

#include "mon/MonMap.h"

#include "mon/MonSub.h"

template<typename Message> using Ref = boost::intrusive_ptr<Message>;
namespace crimson::net {
  class Messenger;
}

class LogClient;

struct AuthAuthorizeHandler;
class MAuthReply;
struct MMonMap;
struct MMonSubscribeAck;
struct MMonGetVersionReply;
struct MMonCommand;
struct MMonCommandAck;
struct MLogAck;
struct MConfig;

enum class log_flushing_t;

namespace crimson::mon {

class Connection;

class Client : public crimson::net::Dispatcher,
	       public crimson::auth::AuthClient,
	       public crimson::auth::AuthServer
{
  EntityName entity_name;
  KeyRing keyring;
  const uint32_t want_keys;

  MonMap monmap;
  bool ready_to_send = false;
  seastar::shared_ptr<Connection> active_con;
  std::vector<seastar::shared_ptr<Connection>> pending_conns;
  seastar::timer<seastar::lowres_clock> timer;

  crimson::net::Messenger& msgr;

  LogClient *log_client;
  bool more_log_pending = false;
  utime_t last_send_log;

  seastar::future<> send_log(log_flushing_t flush_flag);
  seastar::future<> wait_for_send_log();

  // commands
  using get_version_t = seastar::future<std::tuple<version_t, version_t>>;

  ceph_tid_t last_version_req_id = 0;
  std::map<ceph_tid_t, typename get_version_t::promise_type> version_reqs;

  ceph_tid_t last_mon_command_id = 0;
  using command_result_t =
    seastar::future<std::tuple<std::int32_t, std::string, ceph::bufferlist>>;
  struct mon_command_t {
    MURef<MMonCommand> req;
    typename command_result_t::promise_type result;
    mon_command_t(MURef<MMonCommand> req);
  };
  std::vector<mon_command_t> mon_commands;

  MonSub sub;

public:
  Client(crimson::net::Messenger&, crimson::common::AuthHandler&);
  Client(Client&&);
  ~Client();
  seastar::future<> start();
  seastar::future<> stop();

  void set_log_client(LogClient *clog) {
    log_client = clog;
  }

  const uuid_d& get_fsid() const {
    return monmap.fsid;
  }
  get_version_t get_version(const std::string& map);
  command_result_t run_command(std::string&& cmd,
                               bufferlist&& bl);
  seastar::future<> send_message(MessageURef);
  bool sub_want(const std::string& what, version_t start, unsigned flags);
  void sub_got(const std::string& what, version_t have);
  void sub_unwant(const std::string& what);
  bool sub_want_increment(const std::string& what, version_t start, unsigned flags);
  seastar::future<> renew_subs();
  seastar::future<> wait_for_config();

  void print(std::ostream&) const;
private:
  // AuthServer methods
  std::pair<std::vector<uint32_t>, std::vector<uint32_t>>
  get_supported_auth_methods(int peer_type) final;
  uint32_t pick_con_mode(int peer_type,
			 uint32_t auth_method,
			 const std::vector<uint32_t>& preferred_modes) final;
  AuthAuthorizeHandler* get_auth_authorize_handler(int peer_type,
						   int auth_method) final;
  int handle_auth_request(crimson::net::Connection &conn,
			  AuthConnectionMeta &auth_meta,
			  bool more,
			  uint32_t auth_method,
			  const ceph::bufferlist& payload,
			  uint64_t *p_peer_global_id,
			  ceph::bufferlist *reply) final;

  crimson::common::CephContext cct; // for auth_registry
  AuthRegistry auth_registry;
  crimson::common::AuthHandler& auth_handler;

  // AuthClient methods
  crimson::auth::AuthClient::auth_request_t
  get_auth_request(crimson::net::Connection &conn,
		   AuthConnectionMeta &auth_meta) final;

   // Handle server's request to continue the handshake
  ceph::bufferlist handle_auth_reply_more(crimson::net::Connection &conn,
					  AuthConnectionMeta &auth_meta,
					  const bufferlist& bl) final;

   // Handle server's indication that authentication succeeded
  int handle_auth_done(crimson::net::Connection &conn,
		       AuthConnectionMeta &auth_meta,
		       uint64_t global_id,
		       uint32_t con_mode,
		       const bufferlist& bl) final;

   // Handle server's indication that the previous auth attempt failed
  int handle_auth_bad_method(crimson::net::Connection &conn,
			     AuthConnectionMeta &auth_meta,
			     uint32_t old_auth_method,
			     int result,
			     const std::vector<uint32_t>& allowed_methods,
			     const std::vector<uint32_t>& allowed_modes) final;

private:
  void tick();

  std::optional<seastar::future<>> ms_dispatch(crimson::net::ConnectionRef conn,
                                               MessageRef m) override;
  void ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace) override;

  seastar::future<> handle_monmap(crimson::net::Connection &conn,
				  Ref<MMonMap> m);
  seastar::future<> handle_auth_reply(crimson::net::Connection &conn,
				      Ref<MAuthReply> m);
  seastar::future<> handle_subscribe_ack(Ref<MMonSubscribeAck> m);
  seastar::future<> handle_get_version_reply(Ref<MMonGetVersionReply> m);
  seastar::future<> handle_mon_command_ack(Ref<MMonCommandAck> m);
  seastar::future<> handle_log_ack(Ref<MLogAck> m);
  seastar::future<> handle_config(Ref<MConfig> m);

  seastar::future<> on_session_opened();
private:
  seastar::future<> load_keyring();
  seastar::future<> authenticate();

  bool is_hunting() const;
  // @param rank, rank of the monitor to be connected, if it is less than 0,
  //              try to connect to all monitors in monmap, until one of them
  //              is connected.
  // @return true if a connection to monitor is established
  seastar::future<bool> reopen_session(int rank);
  std::vector<unsigned> get_random_mons(unsigned n) const;
  seastar::future<> _add_conn(unsigned rank, uint64_t global_id);
  void _finish_auth(const entity_addr_t& peer);
  crimson::common::gate_per_shard gates;

  // messages that are waiting for the active_con to be available
  struct pending_msg_t {
    pending_msg_t(MessageURef m) : msg(std::move(m)) {}
    MessageURef msg;
    seastar::promise<> pr;
  };
  std::deque<pending_msg_t> pending_messages;
  std::optional<seastar::promise<>> config_updated;
};

inline std::ostream& operator<<(std::ostream& out, const Client& client) {
  client.print(out);
  return out;
}

} // namespace crimson::mon

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::mon::Client> : fmt::ostream_formatter {};
#endif
