// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include "auth/Auth.h"
#include "global/global_init.h"
#include "messages/MPing.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "auth/DummyAuth.h"

enum class echo_role {
  as_server,
  as_client,
};

namespace native_pingpong {

constexpr int CEPH_OSD_PROTOCOL = 10;

struct Server {
  Server(CephContext* cct, const entity_inst_t& entity)
    : dummy_auth(cct), dispatcher(cct)
  {
    msgr.reset(Messenger::create(cct, "async",
                                 entity.name, "pong", entity.addr.get_nonce(), 0));
    dummy_auth.auth_registry.refresh_config();
    msgr->set_cluster_protocol(CEPH_OSD_PROTOCOL);
    msgr->set_default_policy(Messenger::Policy::stateless_server(0));
    msgr->set_auth_client(&dummy_auth);
    msgr->set_auth_server(&dummy_auth);
    dispatcher.ms_set_require_authorizer(false);
  }
  DummyAuthClientServer dummy_auth;
  unique_ptr<Messenger> msgr;
  struct ServerDispatcher : Dispatcher {
    std::mutex mutex;
    std::condition_variable on_reply;
    bool replied = false;
    ServerDispatcher(CephContext* cct)
      : Dispatcher(cct)
    {}
    bool ms_can_fast_dispatch_any() const override {
      return true;
    }
    bool ms_can_fast_dispatch(const Message* m) const override {
      return m->get_type() == CEPH_MSG_PING;
    }
    void ms_fast_dispatch(Message* m) override {
      m->get_connection()->send_message(new MPing);
      m->put();
      {
        std::lock_guard lock{mutex};
        replied = true;
      }
      on_reply.notify_one();
    }
    bool ms_dispatch(Message*) override {
      ceph_abort();
    }
    bool ms_handle_reset(Connection*) override {
      return true;
    }
    void ms_handle_remote_reset(Connection*) override {
    }
    bool ms_handle_refused(Connection*) override {
      return true;
    }
    void echo() {
      replied = false;
      std::unique_lock lock{mutex};
      return on_reply.wait(lock, [this] { return replied; });
    }
  } dispatcher;
  void echo() {
    dispatcher.echo();
  }
};

struct Client {
  unique_ptr<Messenger> msgr;
  Client(CephContext *cct)
    : dummy_auth(cct), dispatcher(cct)
  {
    msgr.reset(Messenger::create(cct, "async",
                                 entity_name_t::CLIENT(-1), "ping",
                                 getpid(), 0));
    dummy_auth.auth_registry.refresh_config();
    msgr->set_cluster_protocol(CEPH_OSD_PROTOCOL);
    msgr->set_default_policy(Messenger::Policy::lossy_client(0));
    msgr->set_auth_client(&dummy_auth);
    msgr->set_auth_server(&dummy_auth);
    dispatcher.ms_set_require_authorizer(false);
  }
  DummyAuthClientServer dummy_auth;
  struct ClientDispatcher : Dispatcher {
    std::mutex mutex;
    std::condition_variable on_reply;
    bool replied = false;

    ClientDispatcher(CephContext* cct)
      : Dispatcher(cct)
    {}
    bool ms_can_fast_dispatch_any() const override {
      return true;
    }
    bool ms_can_fast_dispatch(const Message* m) const override {
      return m->get_type() == CEPH_MSG_PING;
    }
    void ms_fast_dispatch(Message* m) override {
      m->put();
      {
        std::lock_guard lock{mutex};
        replied = true;
      }
      on_reply.notify_one();
    }
    bool ms_dispatch(Message*) override {
      ceph_abort();
    }
    bool ms_handle_reset(Connection *) override {
      return true;
    }
    void ms_handle_remote_reset(Connection*) override {
    }
    bool ms_handle_refused(Connection*) override {
      return true;
    }
    bool ping(Messenger* msgr, const entity_inst_t& peer) {
      auto conn = msgr->connect_to(peer.name.type(),
                                   entity_addrvec_t{peer.addr});
      replied = false;
      conn->send_message(new MPing);
      std::unique_lock lock{mutex};
      return on_reply.wait_for(lock, 500ms, [&] {
        return replied;
      });
    }
  } dispatcher;
  void ping(const entity_inst_t& peer) {
    dispatcher.ping(msgr.get(), peer);
  }
};
} // namespace native_pingpong

static void ceph_echo(CephContext* cct,
                      entity_addr_t addr, echo_role role, unsigned count)
{
  std::cout << "ceph/";
  entity_inst_t entity{entity_name_t::OSD(0), addr};
  if (role == echo_role::as_server) {
    std::cout << "server listening at " << addr << std::endl;
    native_pingpong::Server server{cct, entity};
    server.msgr->bind(addr);
    server.msgr->add_dispatcher_head(&server.dispatcher);
    server.msgr->start();
    for (unsigned i = 0; i < count; i++) {
      server.echo();
    }
    server.msgr->shutdown();
    server.msgr->wait();
  } else {
    std::cout << "client sending to " << addr << std::endl;
    native_pingpong::Client client{cct};
    client.msgr->add_dispatcher_head(&client.dispatcher);
    client.msgr->start();
    auto conn = client.msgr->connect_to(entity.name.type(),
                                        entity_addrvec_t{entity.addr});
    for (unsigned i = 0; i < count; i++) {
      std::cout << "seq=" << i << std::endl;
      client.ping(entity);
    }
    client.msgr->shutdown();
    client.msgr->wait();
  }
}

int main(int argc, char** argv)
{
  namespace po = boost::program_options;
  po::options_description desc{"Allowed options"};
  desc.add_options()
    ("help,h", "show help message")
    ("role", po::value<std::string>()->default_value("pong"),
     "role to play (ping | pong)")
    ("port", po::value<uint16_t>()->default_value(9010),
     "port #")
    ("nonce", po::value<uint32_t>()->default_value(42),
     "a unique number to identify the pong server")
    ("count", po::value<unsigned>()->default_value(10),
     "stop after sending/echoing <count> MPing messages")
    ("v2", po::value<bool>()->default_value(false),
     "using msgr v2 protocol");
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

  entity_addr_t addr;
  if (vm["v2"].as<bool>()) {
    addr.set_type(entity_addr_t::TYPE_MSGR2);
  } else {
    addr.set_type(entity_addr_t::TYPE_LEGACY);
  }
  addr.set_family(AF_INET);
  addr.set_port(vm["port"].as<std::uint16_t>());
  addr.set_nonce(vm["nonce"].as<std::uint32_t>());

  echo_role role = echo_role::as_server;
  if (vm["role"].as<std::string>() == "ping") {
    role = echo_role::as_client;
  }

  auto count = vm["count"].as<unsigned>();
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(nullptr, args,
                         CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(cct.get());
  ceph_echo(cct.get(), addr, role, count);
}
