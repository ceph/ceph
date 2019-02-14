// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include "auth/Auth.h"
#include "global/global_init.h"
#include "messages/MPing.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Messenger.h"
#include "crimson/net/Config.h"
#include "crimson/thread/Condition.h"
#include "crimson/thread/Throttle.h"

#include <seastar/core/alien.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>


enum class echo_role {
  as_server,
  as_client,
};

namespace seastar_pingpong {
struct DummyAuthAuthorizer : public AuthAuthorizer {
  DummyAuthAuthorizer()
    : AuthAuthorizer(CEPH_AUTH_CEPHX)
  {}
  bool verify_reply(bufferlist::const_iterator&,
                    std::string *connection_secret) override {
    return true;
  }
  bool add_challenge(CephContext*, const bufferlist&) override {
    return true;
  }
};

struct Server {
  ceph::thread::Throttle byte_throttler;
  ceph::net::Messenger& msgr;
  struct ServerDispatcher : ceph::net::Dispatcher {
    unsigned count = 0;
    seastar::condition_variable on_reply;
    seastar::future<> ms_dispatch(ceph::net::ConnectionRef c,
                                  MessageRef m) override {
      std::cout << "server got ping " << *m << std::endl;
      // reply with a pong
      return c->send(MessageRef{new MPing(), false}).then([this] {
        ++count;
        on_reply.signal();
      });
    }
    seastar::future<ceph::net::msgr_tag_t, bufferlist>
    ms_verify_authorizer(peer_type_t peer_type,
                         auth_proto_t protocol,
                         bufferlist& auth) override {
      return seastar::make_ready_future<ceph::net::msgr_tag_t, bufferlist>(
          0, bufferlist{});
    }
    seastar::future<std::unique_ptr<AuthAuthorizer>>
    ms_get_authorizer(peer_type_t) override {
      return seastar::make_ready_future<std::unique_ptr<AuthAuthorizer>>(
          new DummyAuthAuthorizer{});
    }
  } dispatcher;
  Server(ceph::net::Messenger& msgr)
    : byte_throttler(ceph::net::conf.osd_client_message_size_cap),
      msgr{msgr}
  {
    msgr.set_crc_header();
    msgr.set_crc_data();
  }
};

struct Client {
  ceph::thread::Throttle byte_throttler;
  ceph::net::Messenger& msgr;
  struct ClientDispatcher : ceph::net::Dispatcher {
    unsigned count = 0;
    seastar::condition_variable on_reply;
    seastar::future<> ms_dispatch(ceph::net::ConnectionRef c,
                                  MessageRef m) override {
      std::cout << "client got pong " << *m << std::endl;
      ++count;
      on_reply.signal();
      return seastar::now();
    }
  } dispatcher;
  Client(ceph::net::Messenger& msgr)
    : byte_throttler(ceph::net::conf.osd_client_message_size_cap),
      msgr{msgr}
  {
    msgr.set_crc_header();
    msgr.set_crc_data();
  }
};
} // namespace seastar_pingpong

namespace native_pingpong {

constexpr int CEPH_OSD_PROTOCOL = 10;

struct Server {
  Server(CephContext* cct, const entity_inst_t& entity)
    : dispatcher(cct)
  {
    msgr.reset(Messenger::create(cct, "async",
                                 entity.name, "pong", entity.addr.get_nonce(), 0));
    msgr->set_cluster_protocol(CEPH_OSD_PROTOCOL);
    msgr->set_default_policy(Messenger::Policy::stateless_server(0));
  }
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
    : dispatcher(cct)
  {
    msgr.reset(Messenger::create(cct, "async",
                                 entity_name_t::CLIENT(-1), "ping",
                                 getpid(), 0));
    msgr->set_cluster_protocol(CEPH_OSD_PROTOCOL);
    msgr->set_default_policy(Messenger::Policy::lossy_client(0));
  }
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

class SeastarContext {
  seastar::file_desc begin_fd;
  ceph::thread::Condition on_end;

public:
  SeastarContext()
    : begin_fd{seastar::file_desc::eventfd(0, 0)}
  {}

  template<class Func>
  std::thread with_seastar(Func&& func) {
    return std::thread{[this, func = std::forward<Func>(func)] {
        // alien: are you ready?
        wait_for_seastar();
        // alien: could you help me apply(func)?
        func();
        // alien: i've sent my request. have you replied it?
        // wait_for_seastar();
        // alien: you are free to go!
        on_end.notify();
      }};
  }

  void run(seastar::app_template& app, int argc, char** argv) {
    app.run(argc, argv, [this] {
      return seastar::now().then([this] {
        return set_seastar_ready();
      }).then([this] {
        // seastar: let me know once i am free to leave.
        return on_end.wait();
      }).handle_exception([](auto ep) {
        std::cerr << "Error: " << ep << std::endl;
      }).finally([] {
        seastar::engine().exit(0);
      });
    });
  }

  seastar::future<> set_seastar_ready() {
    // seastar: i am ready to serve!
    ::eventfd_write(begin_fd.get(), 1);
    return seastar::now();
  }

private:
  void wait_for_seastar() {
    eventfd_t result = 0;
    if (int r = ::eventfd_read(begin_fd.get(), &result); r < 0) {
      std::cerr << "unable to eventfd_read():" << errno << std::endl;
    }
  }
};

static seastar::future<>
seastar_echo(const entity_addr_t addr, echo_role role, unsigned count)
{
  std::cout << "seastar/";
  if (role == echo_role::as_server) {
    return ceph::net::Messenger::create(entity_name_t::OSD(0), "server",
                                        addr.get_nonce(), 0)
      .then([addr, count] (auto msgr) {
        return seastar::do_with(seastar_pingpong::Server{*msgr},
          [addr, count](auto& server) mutable {
            std::cout << "server listening at " << addr << std::endl;
            // bind the server
            server.msgr.set_policy_throttler(entity_name_t::TYPE_OSD,
                                             &server.byte_throttler);
            return server.msgr.bind(entity_addrvec_t{addr})
              .then([&server] {
                return server.msgr.start(&server.dispatcher);
              }).then([&dispatcher=server.dispatcher, count] {
                return dispatcher.on_reply.wait([&dispatcher, count] {
                  return dispatcher.count >= count;
                });
              }).finally([&server] {
                std::cout << "server shutting down" << std::endl;
                return server.msgr.shutdown();
              });
          });
      });
  } else {
    return ceph::net::Messenger::create(entity_name_t::OSD(1), "client",
                                        addr.get_nonce(), 0)
      .then([addr, count] (auto msgr) {
        return seastar::do_with(seastar_pingpong::Client{*msgr},
          [addr, count](auto& client) {
            std::cout << "client sending to " << addr << std::endl;
            client.msgr.set_policy_throttler(entity_name_t::TYPE_OSD,
                                             &client.byte_throttler);
            return client.msgr.start(&client.dispatcher)
              .then([addr, &client] {
                return client.msgr.connect(addr, entity_name_t::TYPE_OSD);
              }).then([&disp=client.dispatcher, count](ceph::net::ConnectionXRef conn) {
                return seastar::do_until(
                  [&disp,count] { return disp.count >= count; },
                  [&disp,conn] { return (*conn)->send(MessageRef{new MPing(), false})
                                   .then([&] { return disp.on_reply.wait(); });
                });
              }).finally([&client] {
                std::cout << "client shutting down" << std::endl;
                return client.msgr.shutdown();
              });
          });
      });
  }
}

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
    ("test", po::value<std::string>()->default_value("seastar"),
     "messenger to use (seastar | ceph)")
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
  if (vm["test"].as<std::string>() == "seastar") {
    seastar::app_template app;
    SeastarContext sc;
    auto job = sc.with_seastar([&] {
      auto fut = seastar::alien::submit_to(0, [addr, role, count] {
        return seastar_echo(addr, role, count);
      });
      fut.wait();
    });
    std::vector<char*> av{argv[0]};
    std::transform(begin(unrecognized_options),
                   end(unrecognized_options),
                   std::back_inserter(av),
                   [](auto& s) {
                     return const_cast<char*>(s.c_str());
                   });
    sc.run(app, av.size(), av.data());
    job.join();
  } else {
    std::vector<const char*> args(argv, argv + argc);
    auto cct = global_init(nullptr, args,
                           CEPH_ENTITY_TYPE_CLIENT,
                           CODE_ENVIRONMENT_UTILITY,
                           CINIT_FLAG_NO_MON_CONFIG);
    common_init_finish(cct.get());
    ceph_echo(cct.get(), addr, role, count);
  }
}

/*
 * Local Variables:
 * compile-command: "make -j4 \
 * -C ../../../build \
 * unittest_seastar_echo"
 * End:
 */
