// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include <boost/pointer_cast.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include "auth/DummyAuth.h"
#include "common/dout.h"
#include "global/global_init.h"
#include "messages/MPing.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MOSDOp.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "test_messenger.h"

namespace {

#define dout_subsys ceph_subsys_test

using namespace ceph::net::test;
using SocketPolicy = Messenger::Policy;

constexpr int CEPH_OSD_PROTOCOL = 10;

class FailoverSuitePeer : public Dispatcher {
  using cb_t = std::function<void()>;
  DummyAuthClientServer dummy_auth;
  std::unique_ptr<Messenger> peer_msgr;
  cb_t op_callback;

  Connection* tracked_conn = nullptr;
  unsigned pending_send = 0;

  bool ms_can_fast_dispatch_any() const override { return true; }
  bool ms_can_fast_dispatch(const Message* m) const override { return true; }
  void ms_fast_dispatch(Message* m) override {
    auto conn = m->get_connection().get();
    if (tracked_conn == nullptr) {
      ldout(cct, 0) << "[!TestPeer] got op from Test(conn "
                    << conn << "not tracked yet)" << dendl;
      tracked_conn = conn;
    } else if (tracked_conn != conn) {
      lderr(cct) << "[TestPeer] got op from Test: conn(" << conn
                 << ") != tracked_conn(" << tracked_conn
                 << ")" << dendl;
      ceph_abort();
    } else {
      ldout(cct, 0) << "[TestPeer] got op from Test" << dendl;
    }
    op_callback();
  }
  bool ms_dispatch(Message* m) override { ceph_abort(); }
  void ms_handle_fast_connect(Connection* conn) override {
    if (tracked_conn == conn) {
      ldout(cct, 0) << "[TestPeer] connected: " << conn << dendl;
    } else {
      lderr(cct) << "[TestPeer] connected: conn(" << conn
                 << ") != tracked_conn(" << tracked_conn
                 << ")" << dendl;
      ceph_abort();
    }
  }
  void ms_handle_fast_accept(Connection* conn) override {
    if (tracked_conn == nullptr) {
      ldout(cct, 0) << "[TestPeer] accepted: " << conn << dendl;
      tracked_conn = conn;
    } else if (tracked_conn != conn) {
      lderr(cct) << "[TestPeer] accepted: conn(" << conn
                 << ") != tracked_conn(" << tracked_conn
                 << ")" << dendl;
      ceph_abort();
    } else {
      ldout(cct, 0) << "[!TestPeer] accepted(stale event): " << conn << dendl;
    }
    flush_pending_send();
  }
  bool ms_handle_reset(Connection* conn) override {
    if (tracked_conn == conn) {
      ldout(cct, 0) << "[TestPeer] reset: " << conn << dendl;
      tracked_conn = nullptr;
    } else {
      ldout(cct, 0) << "[!TestPeer] reset(invalid event): conn(" << conn
                    << ") != tracked_conn(" << tracked_conn
                    << ")" << dendl;
    }
    return true;
  }
  void ms_handle_remote_reset(Connection* conn) override {
    if (tracked_conn == conn) {
      ldout(cct, 0) << "[TestPeer] remote reset: " << conn << dendl;
    } else {
      ldout(cct, 0) << "[!TestPeer] reset(invalid event): conn(" << conn
                    << ") != tracked_conn(" << tracked_conn
                    << ")" << dendl;
    }
  }
  bool ms_handle_refused(Connection* conn) override {
    ldout(cct, 0) << "[!TestPeer] refused: " << conn << dendl;
    return true;
  }

 private:
  void init(entity_addr_t test_peer_addr, SocketPolicy policy) {
    peer_msgr.reset(Messenger::create(
      cct, "async",
      entity_name_t::OSD(TEST_PEER_OSD),
      "TestPeer",
      TEST_PEER_NONCE));
    dummy_auth.auth_registry.refresh_config();
    peer_msgr->set_cluster_protocol(CEPH_OSD_PROTOCOL);
    peer_msgr->set_default_policy(policy);
    peer_msgr->set_auth_client(&dummy_auth);
    peer_msgr->set_auth_server(&dummy_auth);
    peer_msgr->bind(test_peer_addr);
    peer_msgr->add_dispatcher_head(this);
    peer_msgr->start();
  }

  void send_op() {
    ceph_assert(tracked_conn);
    pg_t pgid;
    object_locator_t oloc;
    hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                   pgid.pool(), oloc.nspace);
    spg_t spgid(pgid);
    tracked_conn->send_message2(make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0));
  }

  void flush_pending_send() {
    if (pending_send != 0) {
      ldout(cct, 0) << "[TestPeer] flush sending "
                    << pending_send << " ops" << dendl;
    }
    ceph_assert(tracked_conn);
    while (pending_send) {
      send_op();
      --pending_send;
    }
  }

 public:
  FailoverSuitePeer(CephContext* cct, cb_t op_callback)
    : Dispatcher(cct), dummy_auth(cct), op_callback(op_callback) { }

  void shutdown() {
    peer_msgr->shutdown();
    peer_msgr->wait();
  }

  void connect_peer(entity_addr_t test_addr) {
    ldout(cct, 0) << "[TestPeer] connect_peer(" << test_addr << ")" << dendl;
    auto conn = peer_msgr->connect_to_osd(entity_addrvec_t{test_addr});
    if (tracked_conn) {
      if (tracked_conn == conn.get()) {
        ldout(cct, 0) << "[TestPeer] this is not a new session " << conn.get() << dendl;
      } else {
        ldout(cct, 0) << "[TestPeer] this is a new session " << conn.get()
                      << ", replacing old one " << tracked_conn << dendl;
      }
    } else {
      ldout(cct, 0) << "[TestPeer] this is a new session " << conn.get() << dendl;
    }
    tracked_conn = conn.get();
    flush_pending_send();
  }

  void send_peer() {
    if (tracked_conn) {
      ldout(cct, 0) << "[TestPeer] send_peer()" << dendl;
      send_op();
    } else {
      ++pending_send;
      ldout(cct, 0) << "[TestPeer] send_peer() (pending " << pending_send << ")" << dendl;
    }
  }

  void keepalive_peer() {
    ldout(cct, 0) << "[TestPeer] keepalive_peer()" << dendl;
    ceph_assert(tracked_conn);
    tracked_conn->send_keepalive();
  }

  void markdown() {
    ldout(cct, 0) << "[TestPeer] markdown()" << dendl;
    ceph_assert(tracked_conn);
    tracked_conn->mark_down();
    tracked_conn = nullptr;
  }

  static std::unique_ptr<FailoverSuitePeer>
  create(CephContext* cct, entity_addr_t test_peer_addr,
         SocketPolicy policy, cb_t op_callback) {
    auto suite = std::make_unique<FailoverSuitePeer>(cct, op_callback);
    suite->init(test_peer_addr, policy);
    return suite;
  }
};

SocketPolicy to_socket_policy(CephContext* cct, policy_t policy) {
  switch (policy) {
   case policy_t::stateful_server:
    return SocketPolicy::stateful_server(0);
   case policy_t::stateless_server:
    return SocketPolicy::stateless_server(0);
   case policy_t::lossless_peer:
    return SocketPolicy::lossless_peer(0);
   case policy_t::lossless_peer_reuse:
    return SocketPolicy::lossless_peer_reuse(0);
   case policy_t::lossy_client:
    return SocketPolicy::lossy_client(0);
   case policy_t::lossless_client:
    return SocketPolicy::lossless_client(0);
   default:
    lderr(cct) << "[CmdSrv] unexpected policy type" << dendl;
    ceph_abort();
  }
}

class FailoverTestPeer : public Dispatcher {
  DummyAuthClientServer dummy_auth;
  std::unique_ptr<Messenger> cmd_msgr;
  Connection *cmd_conn = nullptr;
  const entity_addr_t test_peer_addr;
  std::unique_ptr<FailoverSuitePeer> test_suite;
  const bool nonstop;

  bool ms_can_fast_dispatch_any() const override { return false; }
  bool ms_can_fast_dispatch(const Message* m) const override { return false; }
  void ms_fast_dispatch(Message* m) override { ceph_abort(); }
  bool ms_dispatch(Message* m) override {
    auto conn = m->get_connection().get();
    if (cmd_conn == nullptr) {
      ldout(cct, 0) << "[!CmdSrv] got msg from CmdCli(conn "
                    << conn << "not tracked yet)" << dendl;
      cmd_conn = conn;
    } else if (cmd_conn != conn) {
      lderr(cct) << "[CmdSrv] got msg from CmdCli: conn(" << conn
                 << ") != cmd_conn(" << cmd_conn
                 << ")" << dendl;
      ceph_abort();
    } else {
      // good!
    }
    switch (m->get_type()) {
     case CEPH_MSG_PING: {
       ldout(cct, 0) << "[CmdSrv] got PING, sending PONG ..." << dendl;
       cmd_conn->send_message2(make_message<MPing>());
       break;
     }
     case MSG_COMMAND: {
      auto m_cmd = boost::static_pointer_cast<MCommand>(m);
      auto cmd = static_cast<cmd_t>(m_cmd->cmd[0][0]);
      if (cmd == cmd_t::shutdown) {
        ldout(cct, 0) << "All tests succeeded" << dendl;
        if (!nonstop) {
          ldout(cct, 0) << "[CmdSrv] shutdown ..." << dendl;
          cmd_msgr->shutdown();
        } else {
          ldout(cct, 0) << "[CmdSrv] nonstop set ..." << dendl;
        }
      } else {
        ldout(cct, 0) << "[CmdSrv] got cmd " << cmd << dendl;
        handle_cmd(cmd, m_cmd);
        ldout(cct, 0) << "[CmdSrv] done, send cmd reply ..." << dendl;
        cmd_conn->send_message2(make_message<MCommandReply>());
      }
      break;
     }
     default:
      lderr(cct) << "[CmdSrv] " << __func__ << " " << cmd_conn
                 << " got unexpected msg from CmdCli: "
                 << m << dendl;
      ceph_abort();
    }
    m->put();
    return true;
  }
  void ms_handle_fast_connect(Connection*) override { ceph_abort(); }
  void ms_handle_fast_accept(Connection *conn) override {
    if (cmd_conn == nullptr) {
      ldout(cct, 0) << "[CmdSrv] accepted: " << conn << dendl;
      cmd_conn = conn;
    } else if (cmd_conn != conn) {
      lderr(cct) << "[CmdSrv] accepted: conn(" << conn
                 << ") != cmd_conn(" << cmd_conn
                 << ")" << dendl;
      ceph_abort();
    } else {
      ldout(cct, 0) << "[!CmdSrv] accepted(stale event): " << conn << dendl;
    }
  }
  bool ms_handle_reset(Connection* conn) override {
    if (cmd_conn == conn) {
      ldout(cct, 0) << "[CmdSrv] reset: " << conn << dendl;
      cmd_conn = nullptr;
    } else {
      ldout(cct, 0) << "[!CmdSrv] reset(invalid event): conn(" << conn
                    << ") != cmd_conn(" << cmd_conn
                    << ")" << dendl;
    }
    return true;
  }
  void ms_handle_remote_reset(Connection*) override { ceph_abort(); }
  bool ms_handle_refused(Connection*) override { ceph_abort(); }

 private:
  void notify_recv_op() {
    ceph_assert(cmd_conn);
    auto m = make_message<MCommand>();
    m->cmd.emplace_back(1, static_cast<char>(cmd_t::suite_recv_op));
    cmd_conn->send_message2(m);
  }

  void handle_cmd(cmd_t cmd, MRef<MCommand> m_cmd) {
    switch (cmd) {
     case cmd_t::suite_start: {
      if (test_suite) {
        test_suite->shutdown();
        test_suite.reset();
        ldout(cct, 0) << "--------  suite stopped (force)  --------\n\n" << dendl;
      }
      auto p = static_cast<policy_t>(m_cmd->cmd[1][0]);
      ldout(cct, 0) << "[CmdSrv] suite starting (" << p
                    <<", " << test_peer_addr << ") ..." << dendl;
      auto policy = to_socket_policy(cct, p);
      auto suite = FailoverSuitePeer::create(cct, test_peer_addr, policy,
                                             [this] { notify_recv_op(); });
      test_suite.swap(suite);
      return;
     }
     case cmd_t::suite_stop:
      ceph_assert(test_suite);
      test_suite->shutdown();
      test_suite.reset();
      ldout(cct, 0) << "--------  suite stopped  --------\n\n" << dendl;
      return;
     case cmd_t::suite_connect_me: {
      ceph_assert(test_suite);
      entity_addr_t test_addr = entity_addr_t();
      test_addr.parse(m_cmd->cmd[1].c_str(), nullptr);
      test_suite->connect_peer(test_addr);
      return;
     }
     case cmd_t::suite_send_me:
      ceph_assert(test_suite);
      test_suite->send_peer();
      return;
     case cmd_t::suite_keepalive_me:
      ceph_assert(test_suite);
      test_suite->keepalive_peer();
      return;
     case cmd_t::suite_markdown:
      ceph_assert(test_suite);
      test_suite->markdown();
      return;
     default:
      lderr(cct) << "[CmdSrv] got unexpected command " << m_cmd
                 << " from CmdCli" << dendl;
      ceph_abort();
    }
  }

  void init(entity_addr_t cmd_peer_addr) {
    cmd_msgr.reset(Messenger::create(
      cct, "async",
      entity_name_t::OSD(CMD_SRV_OSD),
      "CmdSrv",
      CMD_SRV_NONCE));
    dummy_auth.auth_registry.refresh_config();
    cmd_msgr->set_cluster_protocol(CEPH_OSD_PROTOCOL);
    cmd_msgr->set_default_policy(Messenger::Policy::stateless_server(0));
    cmd_msgr->set_auth_client(&dummy_auth);
    cmd_msgr->set_auth_server(&dummy_auth);
    cmd_msgr->bind(cmd_peer_addr);
    cmd_msgr->add_dispatcher_head(this);
    cmd_msgr->start();
  }

 public:
  FailoverTestPeer(CephContext* cct,
                   entity_addr_t test_peer_addr,
                   bool nonstop)
    : Dispatcher(cct),
      dummy_auth(cct),
      test_peer_addr(test_peer_addr),
      nonstop(nonstop) { }

  void wait() { cmd_msgr->wait(); }

  static std::unique_ptr<FailoverTestPeer>
  create(CephContext* cct,
         entity_addr_t cmd_peer_addr,
         entity_addr_t test_peer_addr,
         bool nonstop) {
    auto test_peer = std::make_unique<FailoverTestPeer>(
        cct, test_peer_addr, nonstop);
    test_peer->init(cmd_peer_addr);
    ldout(cct, 0) << "[CmdSrv] ready" << dendl;
    return test_peer;
  }
};

}

int main(int argc, char** argv)
{
  namespace po = boost::program_options;
  po::options_description desc{"Allowed options"};
  desc.add_options()
    ("help,h", "show help message")
    ("addr", po::value<std::string>()->default_value("v2:127.0.0.1:9012"),
     "This is CmdSrv address, and TestPeer address is at port+=1")
    ("nonstop", po::value<bool>()->default_value(false),
     "Do not shutdown TestPeer when all tests are successful");
  po::variables_map vm;
  std::vector<std::string> unrecognized_options;
  try {
    auto parsed = po::command_line_parser(argc, argv)
      .options(desc)
      .allow_unregistered()
      .run();
    po::store(parsed, vm);
    if (vm.count("help")) {
      std::cout << desc << std::endl;
      return 0;
    }
    po::notify(vm);
    unrecognized_options = po::collect_unrecognized(parsed.options, po::include_positional);
  } catch(const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(nullptr, args,
                         CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(cct.get());

  auto addr = vm["addr"].as<std::string>();
  entity_addr_t cmd_peer_addr;
  cmd_peer_addr.parse(addr.c_str(), nullptr);
  cmd_peer_addr.set_nonce(CMD_SRV_NONCE);
  ceph_assert_always(cmd_peer_addr.is_msgr2());
  auto test_peer_addr = get_test_peer_addr(cmd_peer_addr);
  auto nonstop = vm["nonstop"].as<bool>();
  ldout(cct, 0) << "test configuration: cmd_peer_addr=" << cmd_peer_addr
                << ", test_peer_addr=" << test_peer_addr
                << ", nonstop=" << nonstop
                << dendl;

  auto test_peer = FailoverTestPeer::create(
      cct.get(),
      cmd_peer_addr,
      test_peer_addr,
      nonstop);
  test_peer->wait();
}
