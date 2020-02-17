// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include "auth/Auth.h"
#include "messages/MPing.h"
#include "crimson/auth/DummyAuth.h"
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
  crimson::thread::Throttle byte_throttler;
  crimson::net::Messenger& msgr;
  crimson::auth::DummyAuthClientServer dummy_auth;
  struct ServerDispatcher : crimson::net::Dispatcher {
    unsigned count = 0;
    seastar::condition_variable on_reply;
    seastar::future<> ms_dispatch(crimson::net::Connection* c,
                                  MessageRef m) override {
      std::cout << "server got ping " << *m << std::endl;
      // reply with a pong
      return c->send(make_message<MPing>()).then([this] {
        ++count;
        on_reply.signal();
      });
    }
    seastar::future<crimson::net::msgr_tag_t, bufferlist>
    ms_verify_authorizer(entity_type_t peer_type,
                         auth_proto_t protocol,
                         bufferlist& auth) override {
      return seastar::make_ready_future<crimson::net::msgr_tag_t, bufferlist>(
          0, bufferlist{});
    }
  } dispatcher;
  Server(crimson::net::Messenger& msgr)
    : byte_throttler(crimson::net::conf.osd_client_message_size_cap),
      msgr{msgr}
  {
    msgr.set_crc_header();
    msgr.set_crc_data();
  }
};

struct Client {
  crimson::thread::Throttle byte_throttler;
  crimson::net::Messenger& msgr;
  crimson::auth::DummyAuthClientServer dummy_auth;
  struct ClientDispatcher : crimson::net::Dispatcher {
    unsigned count = 0;
    seastar::condition_variable on_reply;
    seastar::future<> ms_dispatch(crimson::net::Connection* c,
                                  MessageRef m) override {
      std::cout << "client got pong " << *m << std::endl;
      ++count;
      on_reply.signal();
      return seastar::now();
    }
  } dispatcher;
  Client(crimson::net::Messenger& msgr)
    : byte_throttler(crimson::net::conf.osd_client_message_size_cap),
      msgr{msgr}
  {
    msgr.set_crc_header();
    msgr.set_crc_data();
  }
};
} // namespace seastar_pingpong

class SeastarContext {
  seastar::file_desc begin_fd;
  crimson::thread::Condition on_end;

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
    return crimson::net::Messenger::create(entity_name_t::OSD(0), "server",
                                        addr.get_nonce(), 0)
      .then([addr, count] (auto msgr) {
        return seastar::do_with(seastar_pingpong::Server{*msgr},
          [addr, count](auto& server) mutable {
            std::cout << "server listening at " << addr << std::endl;
            // bind the server
            server.msgr.set_default_policy(crimson::net::SocketPolicy::stateless_server(0));
            server.msgr.set_policy_throttler(entity_name_t::TYPE_OSD,
                                             &server.byte_throttler);
            server.msgr.set_require_authorizer(false);
            server.msgr.set_auth_client(&server.dummy_auth);
            server.msgr.set_auth_server(&server.dummy_auth);
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
    return crimson::net::Messenger::create(entity_name_t::OSD(1), "client",
                                        addr.get_nonce(), 0)
      .then([addr, count] (auto msgr) {
        return seastar::do_with(seastar_pingpong::Client{*msgr},
          [addr, count](auto& client) {
            std::cout << "client sending to " << addr << std::endl;
            client.msgr.set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
            client.msgr.set_policy_throttler(entity_name_t::TYPE_OSD,
                                             &client.byte_throttler);
            client.msgr.set_require_authorizer(false);
            client.msgr.set_auth_client(&client.dummy_auth);
            client.msgr.set_auth_server(&client.dummy_auth);
            return client.msgr.start(&client.dispatcher)
              .then([addr, &client] {
                return client.msgr.connect(addr, entity_name_t::TYPE_OSD);
              }).then([&disp=client.dispatcher, count](crimson::net::ConnectionXRef conn) {
                return seastar::do_until(
                  [&disp,count] { return disp.count >= count; },
                  [&disp,conn] { return (*conn)->send(make_message<MPing>())
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
}

/*
 * Local Variables:
 * compile-command: "make -j4 \
 * -C ../../../build \
 * unittest_seastar_echo"
 * End:
 */
