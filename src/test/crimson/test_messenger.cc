#include "messages/MPing.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/SocketMessenger.h"

#include <random>
#include <boost/program_options.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>

namespace bpo = boost::program_options;

static std::random_device rd;
static std::default_random_engine rng{rd()};
static bool verbose = false;

static seastar::future<> test_echo(unsigned rounds,
                                   double keepalive_ratio)
{
  struct test_state {
    entity_addr_t addr;

    struct {
      ceph::net::SocketMessenger messenger{entity_name_t::OSD(1)};
      struct ServerDispatcher : ceph::net::Dispatcher {
        seastar::future<> ms_dispatch(ceph::net::ConnectionRef c,
                                      MessageRef m) override {
          if (verbose) {
            std::cout << "server got " << *m << std::endl;
          }
          // reply with a pong
          return c->send(MessageRef{new MPing(), false});
        }
      } dispatcher;
    } server;

    struct {
      unsigned rounds;
      std::bernoulli_distribution keepalive_dist{};
      ceph::net::SocketMessenger messenger{entity_name_t::OSD(0)};
      struct ClientDispatcher : ceph::net::Dispatcher {
        seastar::promise<MessageRef> reply;
        unsigned count = 0u;
        seastar::future<> ms_dispatch(ceph::net::ConnectionRef c,
                                      MessageRef m) override {
          ++count;
          if (verbose) {
            std::cout << "client ms_dispatch " << count << std::endl;
          }
          reply.set_value(std::move(m));
          return seastar::now();
        }
      } dispatcher;
      seastar::future<> pingpong(ceph::net::ConnectionRef c) {
        return seastar::repeat([conn=std::move(c), this] {
          if (keepalive_dist(rng)) {
            return conn->keepalive().then([] {
              return seastar::make_ready_future<seastar::stop_iteration>(
                seastar::stop_iteration::no);
              });
          } else {
            return conn->send(MessageRef{new MPing(), false}).then([&] {
              return dispatcher.reply.get_future();
            }).then([&] (MessageRef msg) {
              dispatcher.reply = seastar::promise<MessageRef>{};
              if (verbose) {
                std::cout << "client got reply " << *msg << std::endl;
              }
              return seastar::make_ready_future<seastar::stop_iteration>(
                  seastar::stop_iteration::yes);
            });
          };
        });
      }
      bool done() const {
        return dispatcher.count >= rounds;
      }
    } client;
  };
  return seastar::do_with(test_state{},
    [rounds, keepalive_ratio] (test_state& t) {
      // bind the server
      t.addr.set_family(AF_INET);
      t.addr.set_port(9010);
      t.server.messenger.bind(t.addr);

      t.client.rounds = rounds;
      t.client.keepalive_dist = std::bernoulli_distribution{keepalive_ratio};

      return t.server.messenger.start(&t.server.dispatcher)
        .then([&] {
          return t.client.messenger.start(&t.client.dispatcher)
            .then([&] {
              return t.client.messenger.connect(t.addr,
                                                entity_name_t::TYPE_OSD);
            }).then([&client=t.client] (ceph::net::ConnectionRef conn) {
              if (verbose) {
                std::cout << "client connected" << std::endl;
              }
              return seastar::repeat([&client,conn=std::move(conn)] {
                return client.pingpong(conn).then([&client] {
                  return seastar::make_ready_future<seastar::stop_iteration>(
                    client.done() ?
                    seastar::stop_iteration::yes :
                    seastar::stop_iteration::no);
                });
              });
            }).finally([&] {
              if (verbose) {
                std::cout << "client shutting down" << std::endl;
              }
              return t.client.messenger.shutdown();
            });
        }).finally([&] {
          if (verbose) {
            std::cout << "server shutting down" << std::endl;
          }
          return t.server.messenger.shutdown();
        });
    });
}

static seastar::future<> test_concurrent_dispatch()
{
  struct test_state {
    entity_addr_t addr;

    struct {
      ceph::net::SocketMessenger messenger{entity_name_t::OSD(1)};
      class ServerDispatcher : public ceph::net::Dispatcher {
        int count = 0;
        seastar::promise<> on_second; // satisfied on second dispatch
        seastar::promise<> on_done; // satisfied when first dispatch unblocks
       public:
        seastar::future<> ms_dispatch(ceph::net::ConnectionRef c,
                                      MessageRef m) override {
          switch (++count) {
          case 1:
            // block on the first request until we reenter with the second
            return on_second.get_future().then([=] { on_done.set_value(); });
          case 2:
            on_second.set_value();
            return seastar::now();
          default:
            throw std::runtime_error("unexpected count");
          }
        }
        seastar::future<> wait() { return on_done.get_future(); }
      } dispatcher;
    } server;

    struct {
      ceph::net::SocketMessenger messenger{entity_name_t::OSD(0)};
      ceph::net::Dispatcher dispatcher;
    } client;
  };
  return seastar::do_with(test_state{},
    [] (test_state& t) {
      // bind the server
      t.addr.set_family(AF_INET);
      t.addr.set_port(9010);
      t.server.messenger.bind(t.addr);

      return t.server.messenger.start(&t.server.dispatcher)
        .then([&] {
          return t.client.messenger.start(&t.client.dispatcher)
            .then([&] {
              return t.client.messenger.connect(t.addr,
                                                entity_name_t::TYPE_OSD);
            }).then([] (ceph::net::ConnectionRef conn) {
              // send two messages
              conn->send(MessageRef{new MPing, false});
              conn->send(MessageRef{new MPing, false});
            }).then([&] {
              // wait for the server to get both
              return t.server.dispatcher.wait();
            }).finally([&] {
              return t.client.messenger.shutdown();
            });
        }).finally([&] {
          return t.server.messenger.shutdown();
        });
    });
}

int main(int argc, char** argv)
{
  seastar::app_template app;
  app.add_options()
    ("verbose,v", bpo::value<bool>()->default_value(false),
     "chatty if true")
    ("rounds", bpo::value<unsigned>()->default_value(512),
     "number of pingpong rounds")
    ("keepalive-ratio", bpo::value<double>()->default_value(0.1),
     "ratio of keepalive in ping messages");
  return app.run(argc, argv, [&] {
    auto&& config = app.configuration();
    verbose = config["verbose"].as<bool>();
    auto rounds = config["rounds"].as<unsigned>();
    auto keepalive_ratio = config["keepalive-ratio"].as<double>();
    return test_echo(rounds, keepalive_ratio)
    .then([] {
      return test_concurrent_dispatch();
    }).then([] {
      std::cout << "All tests succeeded" << std::endl;
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });
}
