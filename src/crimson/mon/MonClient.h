// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <memory>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include "auth/KeyRing.h"

#include "crimson/common/auth_service.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Fwd.h"

#include "mon/MonMap.h"

#include "mon/MonSub.h"

template<typename Message> using Ref = boost::intrusive_ptr<Message>;
namespace ceph::net {
  class Messenger;
}

class AuthMethodList;
class MAuthReply;
struct MMonMap;
struct MMonSubscribeAck;
struct MMonGetVersionReply;
struct MMonCommandAck;
struct MLogAck;
struct MConfig;

namespace ceph::mon {

class Connection;

class Client : public ceph::net::Dispatcher,
	       public ceph::common::AuthService
{
  EntityName entity_name;
  KeyRing keyring;
  std::unique_ptr<AuthMethodList> auth_methods;
  const uint32_t want_keys;

  MonMap monmap;
  seastar::promise<MessageRef> reply;
  std::unique_ptr<Connection> active_con;
  std::vector<Connection> pending_conns;
  seastar::timer<seastar::lowres_clock> timer;
  seastar::gate tick_gate;

  ceph::net::Messenger& msgr;

  // commands
  using get_version_t = seastar::future<version_t, version_t>;

  ceph_tid_t last_version_req_id = 0;
  std::map<ceph_tid_t, typename get_version_t::promise_type> version_reqs;

  ceph_tid_t last_mon_command_id = 0;
  using command_result_t =
    seastar::future<std::int32_t, string, ceph::bufferlist>;
  std::map<ceph_tid_t, typename command_result_t::promise_type> mon_commands;

  MonSub sub;

public:
  Client(ceph::net::Messenger& messenger);
  Client(Client&&);
  ~Client();
  seastar::future<> start();
  seastar::future<> stop();

  const uuid_d& get_fsid() const {
    return monmap.fsid;
  }
  get_version_t get_version(const std::string& map);
  command_result_t run_command(const std::vector<std::string>& cmd,
			       const bufferlist& bl);
  seastar::future<> send_message(MessageRef);
  bool sub_want(const std::string& what, version_t start, unsigned flags);
  void sub_got(const std::string& what, version_t have);
  void sub_unwant(const std::string& what);
  bool sub_want_increment(const std::string& what, version_t start, unsigned flags);
  seastar::future<> renew_subs();
  // AuthService methods
  AuthAuthorizer* get_authorizer(peer_type_t peer) const override;

private:
  void tick();

  seastar::future<> ms_dispatch(ceph::net::ConnectionRef conn,
				MessageRef m) override;
  seastar::future<> ms_handle_reset(ceph::net::ConnectionRef conn) override;
  AuthAuthorizer* ms_get_authorizer(peer_type_t peer) const override;

  seastar::future<> handle_monmap(ceph::net::ConnectionRef conn,
				  Ref<MMonMap> m);
  seastar::future<> handle_auth_reply(ceph::net::ConnectionRef conn,
				      Ref<MAuthReply> m);
  seastar::future<> handle_subscribe_ack(Ref<MMonSubscribeAck> m);
  seastar::future<> handle_get_version_reply(Ref<MMonGetVersionReply> m);
  seastar::future<> handle_mon_command_ack(Ref<MMonCommandAck> m);
  seastar::future<> handle_log_ack(Ref<MLogAck> m);
  seastar::future<> handle_config(Ref<MConfig> m);

private:
  seastar::future<> load_keyring();
  seastar::future<> authenticate();

  bool is_hunting() const;
  seastar::future<> reopen_session(int rank);
  std::vector<unsigned> get_random_mons(unsigned n) const;
  seastar::future<> _add_conn(unsigned rank, uint64_t global_id);
};

} // namespace ceph::mon
