// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "common/ceph_time.h"
#include "messages/MPing.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "crimson/auth/DummyAuth.h"
#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Messenger.h"
#include "crimson/net/Interceptor.h"

#include <map>
#include <random>
#include <boost/program_options.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>

#include "test_messenger.h"

using namespace std::chrono_literals;
namespace bpo = boost::program_options;
using crimson::common::local_conf;

namespace {

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_test);
}

static std::random_device rd;
static std::default_random_engine rng{rd()};
static bool verbose = false;

static entity_addr_t get_server_addr() {
  static int port = 9030;
  ++port;
  entity_addr_t saddr;
  saddr.parse("127.0.0.1", nullptr);
  saddr.set_port(port);
  return saddr;
}

template <typename T, typename... Args>
seastar::future<T*> create_sharded(Args... args) {
  // we should only construct/stop shards on #0
  return seastar::smp::submit_to(0, [=] {
    auto sharded_obj = seastar::make_lw_shared<seastar::sharded<T>>();
    return sharded_obj->start(args...
    ).then([sharded_obj] {
      seastar::engine().at_exit([sharded_obj] {
        return sharded_obj->stop().then([sharded_obj] {});
      });
      return sharded_obj.get();
    });
  }).then([](seastar::sharded<T> *ptr_shard) {
    return &ptr_shard->local();
  });
}

class ShardedGates
  : public seastar::peering_sharded_service<ShardedGates> {
public:
  ShardedGates() = default;
  ~ShardedGates() {
    assert(gate.is_closed());
  }

  template <typename Func>
  void dispatch_in_background(const char *what, Func &&f) {
    std::ignore = seastar::with_gate(
      container().local().gate, std::forward<Func>(f)
    ).handle_exception([what](std::exception_ptr eptr) {
      try {
        std::rethrow_exception(eptr);
      } catch (std::exception &e) {
        logger().error("ShardedGates::dispatch_in_background: "
                       "{} got exxception {}", what, e.what());
      }
    });
  }

  seastar::future<> close() {
    return container().invoke_on_all([](auto &local) {
      return local.gate.close();
    });
  }

  static seastar::future<ShardedGates*> create() {
    return create_sharded<ShardedGates>();
  }

  // seastar::future<> stop() is intentially not implemented

private:
  seastar::gate gate;
};

static seastar::future<> test_echo(unsigned rounds,
                                   double keepalive_ratio)
{
  struct test_state {
    struct Server final
        : public crimson::net::Dispatcher {
      ShardedGates &gates;
      crimson::net::MessengerRef msgr;
      crimson::auth::DummyAuthClientServer dummy_auth;

      Server(ShardedGates &gates) : gates{gates} {}

      void ms_handle_accept(
          crimson::net::ConnectionRef conn,
          seastar::shard_id prv_shard,
          bool is_replace) override {
        logger().info("server accepted {}", *conn);
        ceph_assert(prv_shard == seastar::this_shard_id());
        ceph_assert(!is_replace);
      }

      std::optional<seastar::future<>> ms_dispatch(
          crimson::net::ConnectionRef c, MessageRef m) override {
        if (verbose) {
          logger().info("server got {}", *m);
        }
        // reply with a pong
        gates.dispatch_in_background("echo_send_pong", [c] {
          return c->send(crimson::make_message<MPing>());
        });
        return {seastar::now()};
      }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        msgr = crimson::net::Messenger::create(
            name, lname, nonce, false);
        msgr->set_default_policy(crimson::net::SocketPolicy::stateless_server(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->set_auth_server(&dummy_auth);
        return msgr->bind(entity_addrvec_t{addr}).safe_then([this] {
          return msgr->start({this});
        }, crimson::net::Messenger::bind_ertr::all_same_way(
            [addr] (const std::error_code& e) {
          logger().error("test_echo(): "
                         "there is another instance running at {}", addr);
          ceph_abort();
        }));
      }
      seastar::future<> shutdown() {
        ceph_assert(msgr);
	msgr->stop();
        return msgr->shutdown();
      }
    };

    class Client final
        : public crimson::net::Dispatcher,
          public seastar::peering_sharded_service<Client> {
    public:
      Client(seastar::shard_id primary_sid,
             unsigned rounds,
             double keepalive_ratio,
             ShardedGates *gates)
        : primary_sid{primary_sid},
          keepalive_dist(std::bernoulli_distribution{keepalive_ratio}),
          rounds(rounds),
          gates{*gates} {}

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        assert(seastar::this_shard_id() == primary_sid);
        msgr = crimson::net::Messenger::create(
            name, lname, nonce, false);
        msgr->set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->set_auth_server(&dummy_auth);
        return msgr->start({this});
      }

      seastar::future<> shutdown() {
        assert(seastar::this_shard_id() == primary_sid);
        ceph_assert(msgr);
	msgr->stop();
        return msgr->shutdown();
      }

      seastar::future<> dispatch_pingpong(const entity_addr_t& peer_addr) {
        assert(seastar::this_shard_id() == primary_sid);
        mono_time start_time = mono_clock::now();
        auto conn = msgr->connect(peer_addr, entity_name_t::TYPE_OSD);
        return seastar::futurize_invoke([this, conn] {
          return do_dispatch_pingpong(conn);
        }).then([] {
          // 500ms should be enough to establish the connection
          return seastar::sleep(500ms);
        }).then([this, conn, start_time] {
          return container().invoke_on(
              conn->get_shard_id(),
              [pconn=&*conn, start_time](auto &local) {
            assert(pconn->is_connected());
            auto session = local.find_session(pconn);
            std::chrono::duration<double> dur_handshake = session->connected_time - start_time;
            std::chrono::duration<double> dur_pingpong = session->finish_time - session->connected_time;
            logger().info("{}: handshake {}, pingpong {}",
                          *pconn, dur_handshake.count(), dur_pingpong.count());
          }).then([conn] {});
        });
      }

      static seastar::future<Client*> create(
          unsigned rounds,
          double keepalive_ratio,
          ShardedGates *gates) {
        return create_sharded<Client>(
          seastar::this_shard_id(),
          rounds,
          keepalive_ratio,
          gates);
      }

     private:
      struct PingSession : public seastar::enable_shared_from_this<PingSession> {
        unsigned count = 0u;
        mono_time connected_time;
        mono_time finish_time;
      };
      using PingSessionRef = seastar::shared_ptr<PingSession>;

      void ms_handle_connect(
          crimson::net::ConnectionRef conn,
          seastar::shard_id prv_shard) override {
        auto &local = container().local();
        assert(prv_shard == seastar::this_shard_id());
        auto session = seastar::make_shared<PingSession>();
        auto [i, added] = local.sessions.emplace(&*conn, session);
        std::ignore = i;
        ceph_assert(added);
        session->connected_time = mono_clock::now();
      }

      std::optional<seastar::future<>> ms_dispatch(
          crimson::net::ConnectionRef c, MessageRef m) override {
        auto &local = container().local();
        auto session = local.find_session(&*c);
        ++(session->count);
        if (verbose) {
          logger().info("client ms_dispatch {}", session->count);
        }

        if (session->count > rounds) {
          logger().error("{}: got {} pongs, more than expected {}", *c, session->count, rounds);
          ceph_abort();
        } else if (session->count == rounds) {
          logger().info("{}: finished receiving {} pongs", *c, session->count);
          session->finish_time = mono_clock::now();
          gates.dispatch_in_background("echo_notify_done", [c, this] {
            return container().invoke_on(primary_sid, [pconn=&*c](auto &local) {
              auto found = local.pending_conns.find(pconn);
              ceph_assert(found != local.pending_conns.end());
              found->second.set_value();
            }).then([c] {});
          });
        }
        return {seastar::now()};
      }

      PingSessionRef find_session(crimson::net::Connection *c) {
        auto found = sessions.find(c);
        if (found == sessions.end()) {
          ceph_assert(false);
        }
        return found->second;
      }

      seastar::future<> do_dispatch_pingpong(crimson::net::ConnectionRef conn) {
        auto [i, added] = pending_conns.emplace(&*conn, seastar::promise<>());
        std::ignore = i;
        ceph_assert(added);
        return seastar::do_with(0u, 0u,
            [this, conn](auto &count_ping, auto &count_keepalive) {
          return seastar::do_until(
            [this, conn, &count_ping, &count_keepalive] {
              bool stop = (count_ping == rounds);
              if (stop) {
                logger().info("{}: finished sending {} pings with {} keepalives",
                              *conn, count_ping, count_keepalive);
              }
              return stop;
            },
            [this, conn, &count_ping, &count_keepalive] {
              return seastar::repeat([this, conn, &count_ping, &count_keepalive] {
                if (keepalive_dist(rng)) {
                  return conn->send_keepalive(
                  ).then([&count_keepalive] {
                    count_keepalive += 1;
                    return seastar::make_ready_future<seastar::stop_iteration>(
                      seastar::stop_iteration::no);
                  });
                } else {
                  return conn->send(crimson::make_message<MPing>()
                  ).then([&count_ping] {
                    count_ping += 1;
                    return seastar::make_ready_future<seastar::stop_iteration>(
                      seastar::stop_iteration::yes);
                  });
                }
              });
            }).then([this, conn] {
              auto found = pending_conns.find(&*conn);
              assert(found != pending_conns.end());
              return found->second.get_future();
            }
          );
        });
      }

    private:
      // primary shard only
      const seastar::shard_id primary_sid;
      std::bernoulli_distribution keepalive_dist;
      crimson::net::MessengerRef msgr;
      std::map<crimson::net::Connection*, seastar::promise<>> pending_conns;
      crimson::auth::DummyAuthClientServer dummy_auth;

      // per shard
      const unsigned rounds;
      std::map<crimson::net::Connection*, PingSessionRef> sessions;
      ShardedGates &gates;
    };
  };

  logger().info("test_echo(rounds={}, keepalive_ratio={}):",
                rounds, keepalive_ratio);
  return ShardedGates::create(
  ).then([rounds, keepalive_ratio](auto *gates) {
    return seastar::when_all_succeed(
      test_state::Client::create(rounds, keepalive_ratio, gates),
      test_state::Client::create(rounds, keepalive_ratio, gates),
      seastar::make_ready_future<ShardedGates*>(gates));
  }).then_unpack([](auto *client1, auto *client2, auto *gates) {
    auto server1 = seastar::make_shared<test_state::Server>(*gates);
    auto server2 = seastar::make_shared<test_state::Server>(*gates);
    // start servers and clients
    auto addr1 = get_server_addr();
    auto addr2 = get_server_addr();
    addr1.set_type(entity_addr_t::TYPE_MSGR2);
    addr2.set_type(entity_addr_t::TYPE_MSGR2);
    return seastar::when_all_succeed(
        server1->init(entity_name_t::OSD(0), "server1", 1, addr1),
        server2->init(entity_name_t::OSD(1), "server2", 2, addr2),
        client1->init(entity_name_t::OSD(2), "client1", 3),
        client2->init(entity_name_t::OSD(3), "client2", 4)
    // dispatch pingpoing
    ).then_unpack([client1, client2, server1, server2] {
      return seastar::when_all_succeed(
        // test connecting in parallel, accepting in parallel
        client1->dispatch_pingpong(server1->msgr->get_myaddr()),
        client1->dispatch_pingpong(server2->msgr->get_myaddr()),
        client2->dispatch_pingpong(server1->msgr->get_myaddr()),
        client2->dispatch_pingpong(server2->msgr->get_myaddr()));
    // shutdown
    }).then_unpack([client1] {
      logger().info("client1 shutdown...");
      return client1->shutdown();
    }).then([client2] {
      logger().info("client2 shutdown...");
      return client2->shutdown();
    }).then([server1] {
      logger().info("server1 shutdown...");
      return server1->shutdown();
    }).then([server2] {
      logger().info("server2 shutdown...");
      return server2->shutdown();
    }).then([] {
      logger().info("test_echo() done!\n");
    }).handle_exception([](auto eptr) {
      logger().error("test_echo() failed: got exception {}", eptr);
      throw;
    }).finally([gates, server1, server2] {
      return gates->close();
    });
  });
}

seastar::future<> test_preemptive_shutdown() {
  struct test_state {
    class Server final
      : public crimson::net::Dispatcher {
      crimson::net::MessengerRef msgr;
      crimson::auth::DummyAuthClientServer dummy_auth;

      std::optional<seastar::future<>> ms_dispatch(
          crimson::net::ConnectionRef c, MessageRef m) override {
        std::ignore = c->send(crimson::make_message<MPing>());
        return {seastar::now()};
      }

     public:
      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        msgr = crimson::net::Messenger::create(
            name, lname, nonce, true);
        msgr->set_default_policy(crimson::net::SocketPolicy::stateless_server(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->set_auth_server(&dummy_auth);
        return msgr->bind(entity_addrvec_t{addr}).safe_then([this] {
          return msgr->start({this});
        }, crimson::net::Messenger::bind_ertr::all_same_way(
            [addr] (const std::error_code& e) {
          logger().error("test_preemptive_shutdown(): "
                         "there is another instance running at {}", addr);
          ceph_abort();
        }));
      }
      entity_addr_t get_addr() const {
        return msgr->get_myaddr();
      }
      seastar::future<> shutdown() {
	msgr->stop();
        return msgr->shutdown();
      }
    };

    class Client final
      : public crimson::net::Dispatcher {
      crimson::net::MessengerRef msgr;
      crimson::auth::DummyAuthClientServer dummy_auth;

      bool stop_send = false;
      seastar::promise<> stopped_send_promise;

      std::optional<seastar::future<>> ms_dispatch(
          crimson::net::ConnectionRef, MessageRef m) override {
        return {seastar::now()};
      }

     public:
      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        msgr = crimson::net::Messenger::create(
            name, lname, nonce, true);
        msgr->set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->set_auth_server(&dummy_auth);
        return msgr->start({this});
      }
      void send_pings(const entity_addr_t& addr) {
        auto conn = msgr->connect(addr, entity_name_t::TYPE_OSD);
        // forwarded to stopped_send_promise
        (void) seastar::do_until(
          [this] { return stop_send; },
          [conn] {
            return conn->send(crimson::make_message<MPing>()).then([] {
              return seastar::sleep(0ms);
            });
          }
        ).then_wrapped([this, conn] (auto fut) {
          fut.forward_to(std::move(stopped_send_promise));
        });
      }
      seastar::future<> shutdown() {
	msgr->stop();
        return msgr->shutdown().then([this] {
          stop_send = true;
          return stopped_send_promise.get_future();
        });
      }
    };
  };

  logger().info("test_preemptive_shutdown():");
  auto server = seastar::make_shared<test_state::Server>();
  auto client = seastar::make_shared<test_state::Client>();
  auto addr = get_server_addr();
  addr.set_type(entity_addr_t::TYPE_MSGR2);
  addr.set_family(AF_INET);
  return seastar::when_all_succeed(
    server->init(entity_name_t::OSD(6), "server4", 7, addr),
    client->init(entity_name_t::OSD(7), "client4", 8)
  ).then_unpack([server, client] {
    client->send_pings(server->get_addr());
    return seastar::sleep(100ms);
  }).then([client] {
    logger().info("client shutdown...");
    return client->shutdown();
  }).then([server] {
    logger().info("server shutdown...");
    return server->shutdown();
  }).then([] {
    logger().info("test_preemptive_shutdown() done!\n");
  }).handle_exception([server, client] (auto eptr) {
    logger().error("test_preemptive_shutdown() failed: got exception {}", eptr);
    throw;
  });
}

using ceph::msgr::v2::Tag;
using crimson::net::bp_action_t;
using crimson::net::bp_type_t;
using crimson::net::Breakpoint;
using crimson::net::Connection;
using crimson::net::ConnectionRef;
using crimson::net::custom_bp_t;
using crimson::net::Dispatcher;
using crimson::net::Interceptor;
using crimson::net::Messenger;
using crimson::net::MessengerRef;
using crimson::net::SocketPolicy;
using crimson::net::tag_bp_t;
using namespace ceph::net::test;

struct counter_t { unsigned counter = 0; };

enum class conn_state_t {
  unknown = 0,
  established,
  closed,
  replaced,
};

std::ostream& operator<<(std::ostream& out, const conn_state_t& state) {
  switch(state) {
   case conn_state_t::unknown:
    return out << "unknown";
   case conn_state_t::established:
    return out << "established";
   case conn_state_t::closed:
    return out << "closed";
   case conn_state_t::replaced:
    return out << "replaced";
   default:
    ceph_abort();
  }
}

} // anonymous namespace

#if FMT_VERSION >= 90000
template<>
struct fmt::formatter<conn_state_t> : fmt::ostream_formatter {};
#endif

namespace {

struct ConnResult {
  ConnectionRef conn;
  unsigned index;
  conn_state_t state = conn_state_t::unknown;

  unsigned connect_attempts = 0;
  unsigned client_connect_attempts = 0;
  unsigned client_reconnect_attempts = 0;
  unsigned cnt_connect_dispatched = 0;

  unsigned accept_attempts = 0;
  unsigned server_connect_attempts = 0;
  unsigned server_reconnect_attempts = 0;
  unsigned cnt_accept_dispatched = 0;

  unsigned cnt_reset_dispatched = 0;
  unsigned cnt_remote_reset_dispatched = 0;

  ConnResult(ConnectionRef conn, unsigned index)
    : conn(conn), index(index) {}

  template <typename T>
  void _assert_eq(const char* expr_actual, T actual,
                  const char* expr_expected, T expected) const {
    if (actual != expected) {
      throw std::runtime_error(fmt::format(
            "[{}] {} '{}' is actually {}, not the expected '{}' {}",
            index, *conn, expr_actual, actual, expr_expected, expected));
    }
  }

#define ASSERT_EQUAL(actual, expected) \
  _assert_eq(#actual, actual, #expected, expected)

  void assert_state_at(conn_state_t expected) const {
    ASSERT_EQUAL(state, expected);
  }

  void assert_connect(unsigned attempts,
                      unsigned connects,
                      unsigned reconnects,
                      unsigned dispatched) const {
    ASSERT_EQUAL(connect_attempts, attempts);
    ASSERT_EQUAL(client_connect_attempts, connects);
    ASSERT_EQUAL(client_reconnect_attempts, reconnects);
    ASSERT_EQUAL(cnt_connect_dispatched, dispatched);
  }

  void assert_connect(unsigned attempts,
                      unsigned dispatched) const {
    ASSERT_EQUAL(connect_attempts, attempts);
    ASSERT_EQUAL(cnt_connect_dispatched, dispatched);
  }

  void assert_accept(unsigned attempts,
                     unsigned accepts,
                     unsigned reaccepts,
                     unsigned dispatched) const {
    ASSERT_EQUAL(accept_attempts, attempts);
    ASSERT_EQUAL(server_connect_attempts, accepts);
    ASSERT_EQUAL(server_reconnect_attempts, reaccepts);
    ASSERT_EQUAL(cnt_accept_dispatched, dispatched);
  }

  void assert_accept(unsigned attempts,
                     unsigned dispatched) const {
    ASSERT_EQUAL(accept_attempts, attempts);
    ASSERT_EQUAL(cnt_accept_dispatched, dispatched);
  }

  void assert_reset(unsigned local, unsigned remote) const {
    ASSERT_EQUAL(cnt_reset_dispatched, local);
    ASSERT_EQUAL(cnt_remote_reset_dispatched, remote);
  }

  void dump() const {
    logger().info("\nResult({}):\n"
                  "  conn: [{}] {}:\n"
                  "  state: {}\n"
                  "  connect_attempts: {}\n"
                  "  client_connect_attempts: {}\n"
                  "  client_reconnect_attempts: {}\n"
                  "  cnt_connect_dispatched: {}\n"
                  "  accept_attempts: {}\n"
                  "  server_connect_attempts: {}\n"
                  "  server_reconnect_attempts: {}\n"
                  "  cnt_accept_dispatched: {}\n"
                  "  cnt_reset_dispatched: {}\n"
                  "  cnt_remote_reset_dispatched: {}\n",
                  static_cast<const void*>(this),
                  index, *conn,
                  state,
                  connect_attempts,
                  client_connect_attempts,
                  client_reconnect_attempts,
                  cnt_connect_dispatched,
                  accept_attempts,
                  server_connect_attempts,
                  server_reconnect_attempts,
                  cnt_accept_dispatched,
                  cnt_reset_dispatched,
                  cnt_remote_reset_dispatched);
  }
};
using ConnResults = std::vector<ConnResult>;

struct TestInterceptor : public Interceptor {
  std::map<Breakpoint, std::map<unsigned, bp_action_t>> breakpoints;
  std::map<Breakpoint, counter_t> breakpoints_counter;
  std::map<Connection*, unsigned> conns;
  ConnResults results;
  std::optional<seastar::abort_source> signal;
  const seastar::shard_id primary_sid;

  TestInterceptor() : primary_sid{seastar::this_shard_id()} {}

  // only used for copy breakpoint configurations
  TestInterceptor(const TestInterceptor& other) : primary_sid{other.primary_sid} {
    assert(other.breakpoints_counter.empty());
    assert(other.conns.empty());
    assert(other.results.empty());
    breakpoints = other.breakpoints;
    assert(!other.signal);
    assert(seastar::this_shard_id() == primary_sid);
  }

  void make_fault(Breakpoint bp, unsigned round = 1) {
    assert(round >= 1);
    breakpoints[bp][round] = bp_action_t::FAULT;
  }

  void make_block(Breakpoint bp, unsigned round = 1) {
    assert(round >= 1);
    breakpoints[bp][round] = bp_action_t::BLOCK;
  }

  void make_stall(Breakpoint bp, unsigned round = 1) {
    assert(round >= 1);
    breakpoints[bp][round] = bp_action_t::STALL;
  }

  ConnResult* find_result(Connection *conn) {
    assert(seastar::this_shard_id() == primary_sid);
    auto it = conns.find(conn);
    if (it == conns.end()) {
      return nullptr;
    } else {
      return &results[it->second];
    }
  }

  seastar::future<> wait() {
    assert(seastar::this_shard_id() == primary_sid);
    assert(!signal);
    signal = seastar::abort_source();
    return seastar::sleep_abortable(10s, *signal).then([] {
      throw std::runtime_error("Timeout (10s) in TestInterceptor::wait()");
    }).handle_exception_type([] (const seastar::sleep_aborted& e) {
      // wait done!
    });
  }

  void notify() {
    assert(seastar::this_shard_id() == primary_sid);
    if (signal) {
      signal->request_abort();
      signal = std::nullopt;
    }
  }

 private:
  void register_conn(ConnectionRef conn) override {
    auto result = find_result(&*conn);
    if (result != nullptr) {
      logger().error("The connection [{}] {} already exists when register {}",
                     result->index, *result->conn, *conn);
      ceph_abort();
    }
    unsigned index = results.size();
    results.emplace_back(conn, index);
    conns[&*conn] = index;
    notify();
    logger().info("[{}] {} new connection registered", index, *conn);
  }

  void register_conn_closed(ConnectionRef conn) override {
    auto result = find_result(&*conn);
    if (result == nullptr) {
      logger().error("Untracked closed connection: {}", *conn);
      ceph_abort();
    }

    if (result->state != conn_state_t::replaced) {
      result->state = conn_state_t::closed;
    }
    notify();
    logger().info("[{}] {} closed({})", result->index, *conn, result->state);
  }

  void register_conn_ready(ConnectionRef conn) override {
    auto result = find_result(&*conn);
    if (result == nullptr) {
      logger().error("Untracked ready connection: {}", *conn);
      ceph_abort();
    }

    ceph_assert(conn->is_protocol_ready());
    notify();
    logger().info("[{}] {} ready", result->index, *conn);
  }

  void register_conn_replaced(ConnectionRef conn) override {
    auto result = find_result(&*conn);
    if (result == nullptr) {
      logger().error("Untracked replaced connection: {}", *conn);
      ceph_abort();
    }

    result->state = conn_state_t::replaced;
    logger().info("[{}] {} {}", result->index, *conn, result->state);
  }

  seastar::future<bp_action_t>
  intercept(Connection &_conn, std::vector<Breakpoint> bps) override {
    assert(bps.size() >= 1);
    Connection *conn = &_conn;

    return seastar::smp::submit_to(primary_sid, [conn, bps, this] {
      std::vector<bp_action_t> actions;
      for (const Breakpoint &bp : bps) {
        ++breakpoints_counter[bp].counter;

        auto result = find_result(&*conn);
        if (result == nullptr) {
          logger().error("Untracked intercepted connection: {}, at breakpoint {}({})",
                         *conn, bp, breakpoints_counter[bp].counter);
          ceph_abort();
        }

        if (bp == custom_bp_t::SOCKET_CONNECTING) {
          ++result->connect_attempts;
          logger().info("[Test] connect_attempts={}", result->connect_attempts);
        } else if (bp == tag_bp_t{Tag::CLIENT_IDENT, bp_type_t::WRITE}) {
          ++result->client_connect_attempts;
          logger().info("[Test] client_connect_attempts={}", result->client_connect_attempts);
        } else if (bp == tag_bp_t{Tag::SESSION_RECONNECT, bp_type_t::WRITE}) {
          ++result->client_reconnect_attempts;
          logger().info("[Test] client_reconnect_attempts={}", result->client_reconnect_attempts);
        } else if (bp == custom_bp_t::SOCKET_ACCEPTED) {
          ++result->accept_attempts;
          logger().info("[Test] accept_attempts={}", result->accept_attempts);
        } else if (bp == tag_bp_t{Tag::CLIENT_IDENT, bp_type_t::READ}) {
          ++result->server_connect_attempts;
          logger().info("[Test] server_connect_attemps={}", result->server_connect_attempts);
        } else if (bp == tag_bp_t{Tag::SESSION_RECONNECT, bp_type_t::READ}) {
          ++result->server_reconnect_attempts;
          logger().info("[Test] server_reconnect_attempts={}", result->server_reconnect_attempts);
        }

        auto it_bp = breakpoints.find(bp);
        if (it_bp != breakpoints.end()) {
          auto it_cnt = it_bp->second.find(breakpoints_counter[bp].counter);
          if (it_cnt != it_bp->second.end()) {
            logger().info("[{}] {} intercepted {}({}) => {}",
                          result->index, *conn, bp,
                          breakpoints_counter[bp].counter, it_cnt->second);
            actions.emplace_back(it_cnt->second);
            continue;
          }
        }
        logger().info("[{}] {} intercepted {}({})",
                      result->index, *conn, bp, breakpoints_counter[bp].counter);
        actions.emplace_back(bp_action_t::CONTINUE);
      }

      bp_action_t action = bp_action_t::CONTINUE;
      for (bp_action_t &a : actions) {
        if (a != bp_action_t::CONTINUE) {
          if (action == bp_action_t::CONTINUE) {
            action = a;
          } else {
            ceph_abort("got multiple incompatible actions");
          }
        }
      }
      return seastar::make_ready_future<bp_action_t>(action);
    });
  }
};

SocketPolicy to_socket_policy(policy_t policy) {
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
    logger().error("unexpected policy type");
    ceph_abort();
  }
}

class FailoverSuite : public Dispatcher {
  crimson::auth::DummyAuthClientServer dummy_auth;
  MessengerRef test_msgr;
  const entity_addr_t test_peer_addr;
  TestInterceptor interceptor;

  unsigned tracked_index = 0;
  Connection *tracked_conn = nullptr;
  unsigned pending_send = 0;
  unsigned pending_peer_receive = 0;
  unsigned pending_receive = 0;

  ShardedGates &gates;
  const seastar::shard_id primary_sid;

  std::optional<seastar::future<>> ms_dispatch(
      ConnectionRef conn_ref, MessageRef m) override {
    ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
    Connection *conn = &*conn_ref;
    gates.dispatch_in_background("TestSuite_ms_dispatch",
        [this, conn, conn_ref] {
      return seastar::smp::submit_to(primary_sid, [this, conn] {
        auto result = interceptor.find_result(&*conn);
        if (result == nullptr) {
          logger().error("Untracked ms dispatched connection: {}", *conn);
          ceph_abort();
        }

        if (tracked_conn != &*conn) {
          logger().warn("[{}] {} got op, but doesn't match tracked_conn [{}] {}",
                        result->index, *conn, tracked_index, *tracked_conn);
        } else {
          ceph_assert(result->index == tracked_index);
        }

        ceph_assert(pending_receive > 0);
        --pending_receive;
        if (pending_receive == 0) {
          interceptor.notify();
        }
        logger().info("[Test] got op, left {} ops -- [{}] {}",
                      pending_receive, result->index, *conn);
      }).then([conn_ref] {});
    });
    return {seastar::now()};
  }

  void ms_handle_accept(
      ConnectionRef conn_ref,
      seastar::shard_id prv_shard,
      bool is_replace) override {
    Connection *conn = &*conn_ref;
    gates.dispatch_in_background("TestSuite_ms_dispatch",
        [this, conn, conn_ref] {
      return seastar::smp::submit_to(primary_sid, [this, conn] {
        auto result = interceptor.find_result(&*conn);
        if (result == nullptr) {
          logger().error("Untracked accepted connection: {}", *conn);
          ceph_abort();
        }

        if (tracked_conn &&
            !tracked_conn->is_protocol_closed() &&
            tracked_conn != &*conn) {
          logger().error("[{}] {} got accepted, but there's already a valid traced_conn [{}] {}",
                         result->index, *conn, tracked_index, *tracked_conn);
          ceph_abort();
        }

        tracked_index = result->index;
        tracked_conn = &*conn;
        ++result->cnt_accept_dispatched;
        logger().info("[Test] got accept (cnt_accept_dispatched={}), track [{}] {}",
                      result->cnt_accept_dispatched, result->index, *conn);
        return flush_pending_send();
      }).then([conn_ref] {});
    });
  }

  void ms_handle_connect(
      ConnectionRef conn_ref,
      seastar::shard_id prv_shard) override {
    Connection *conn = &*conn_ref;
    gates.dispatch_in_background("TestSuite_ms_dispatch",
        [this, conn, conn_ref] {
      return seastar::smp::submit_to(primary_sid, [this, conn] {
        auto result = interceptor.find_result(&*conn);
        if (result == nullptr) {
          logger().error("Untracked connected connection: {}", *conn);
          ceph_abort();
        }

        if (tracked_conn &&
            !tracked_conn->is_protocol_closed() &&
            tracked_conn != &*conn) {
          logger().error("[{}] {} got connected, but there's already a avlid tracked_conn [{}] {}",
                         result->index, *conn, tracked_index, *tracked_conn);
          ceph_abort();
        }

        if (tracked_conn == &*conn) {
          ceph_assert(result->index == tracked_index);
        }

        ++result->cnt_connect_dispatched;
        logger().info("[Test] got connected (cnt_connect_dispatched={}) -- [{}] {}",
                      result->cnt_connect_dispatched, result->index, *conn);
      }).then([conn_ref] {});
    });
  }

  void ms_handle_reset(
      ConnectionRef conn_ref,
      bool is_replace) override {
    Connection *conn = &*conn_ref;
    gates.dispatch_in_background("TestSuite_ms_dispatch",
        [this, conn, conn_ref] {
      return seastar::smp::submit_to(primary_sid, [this, conn] {
        auto result = interceptor.find_result(&*conn);
        if (result == nullptr) {
          logger().error("Untracked reset connection: {}", *conn);
          ceph_abort();
        }

        if (tracked_conn != &*conn) {
          logger().warn("[{}] {} got reset, but doesn't match tracked_conn [{}] {}",
                        result->index, *conn, tracked_index, *tracked_conn);
        } else {
          ceph_assert(result->index == tracked_index);
          tracked_index = 0;
          tracked_conn = nullptr;
        }

        ++result->cnt_reset_dispatched;
        logger().info("[Test] got reset (cnt_reset_dispatched={}), untrack [{}] {}",
                      result->cnt_reset_dispatched, result->index, *conn);
      }).then([conn_ref] {});
    });
  }

  void ms_handle_remote_reset(
      ConnectionRef conn_ref) override {
    Connection *conn = &*conn_ref;
    gates.dispatch_in_background("TestSuite_ms_dispatch",
        [this, conn, conn_ref] {
      return seastar::smp::submit_to(primary_sid, [this, conn] {
        auto result = interceptor.find_result(&*conn);
        if (result == nullptr) {
          logger().error("Untracked remotely reset connection: {}", *conn);
          ceph_abort();
        }

        if (tracked_conn != &*conn) {
          logger().warn("[{}] {} got remotely reset, but doesn't match tracked_conn [{}] {}",
                        result->index, *conn, tracked_index, *tracked_conn);
        } else {
          ceph_assert(result->index == tracked_index);
        }

        ++result->cnt_remote_reset_dispatched;
        logger().info("[Test] got remote reset (cnt_remote_reset_dispatched={}) -- [{}] {}",
                      result->cnt_remote_reset_dispatched, result->index, *conn);
      }).then([conn_ref] {});
    });
  }

 private:
  seastar::future<> init(entity_addr_t test_addr, SocketPolicy policy) {
    test_msgr->set_default_policy(policy);
    test_msgr->set_auth_client(&dummy_auth);
    test_msgr->set_auth_server(&dummy_auth);
    test_msgr->set_interceptor(&interceptor);
    return test_msgr->bind(entity_addrvec_t{test_addr}).safe_then([this] {
      return test_msgr->start({this});
    }, Messenger::bind_ertr::all_same_way([test_addr] (const std::error_code& e) {
      logger().error("FailoverSuite: "
                     "there is another instance running at {}", test_addr);
      ceph_abort();
    }));
  }

  seastar::future<> send_op(bool expect_reply=true) {
    ceph_assert(tracked_conn);
    ceph_assert(!tracked_conn->is_protocol_closed());
    if (expect_reply) {
      ++pending_peer_receive;
    }
    pg_t pgid;
    object_locator_t oloc;
    hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                   pgid.pool(), oloc.nspace);
    spg_t spgid(pgid);
    return tracked_conn->send(crimson::make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0));
  }

  seastar::future<> flush_pending_send() {
    if (pending_send != 0) {
      logger().info("[Test] flush sending {} ops", pending_send);
    }
    ceph_assert(tracked_conn);
    ceph_assert(!tracked_conn->is_protocol_closed());
    return seastar::do_until(
        [this] { return pending_send == 0; },
        [this] {
      --pending_send;
      return send_op();
    });
  }

  seastar::future<> wait_ready(unsigned num_ready_conns,
                               unsigned num_replaced,
                               bool wait_received) {
    assert(seastar::this_shard_id() == primary_sid);
    unsigned pending_conns = 0;
    unsigned pending_establish = 0;
    unsigned replaced_conns = 0;
    for (auto& result : interceptor.results) {
      if (result.conn->is_protocol_closed_clean()) {
        if (result.state == conn_state_t::replaced) {
          ++replaced_conns;
        }
      } else if (result.conn->is_protocol_ready()) {
        if (pending_send == 0 && pending_peer_receive == 0 && pending_receive == 0) {
          result.state = conn_state_t::established;
        } else {
          ++pending_establish;
        }
      } else {
        ++pending_conns;
      }
    }

    bool do_wait = false;
    if (num_ready_conns > 0) {
      if (interceptor.results.size() > num_ready_conns) {
        throw std::runtime_error(fmt::format(
              "{} connections, more than expected: {}",
              interceptor.results.size(), num_ready_conns));
      } else if (interceptor.results.size() < num_ready_conns || pending_conns > 0) {
        logger().info("[Test] wait_ready(): wait for connections,"
                      " currently {} out of {}, pending {} ready ...",
                      interceptor.results.size(), num_ready_conns, pending_conns);
        do_wait = true;
      }
    }
    if (wait_received) {
      if (pending_send || pending_peer_receive || pending_receive) {
        if (pending_conns || pending_establish) {
          logger().info("[Test] wait_ready(): wait for pending_send={},"
                        " pending_peer_receive={}, pending_receive={},"
                        " pending {}/{} ready/establish connections ...",
                        pending_send, pending_peer_receive, pending_receive,
                        pending_conns, pending_establish);
          do_wait = true;
        } else {
          // If there are pending messages, stop waiting if there are
          // no longer pending connections.
        }
      } else {
         // Stop waiting if there are no pending messages. Pending connections
         // should not be important.
      }
    }
    if (num_replaced > 0) {
      if (replaced_conns > num_replaced) {
        throw std::runtime_error(fmt::format(
            "{} replaced connections, more than expected: {}",
            replaced_conns, num_replaced));
      }
      if (replaced_conns < num_replaced) {
        logger().info("[Test] wait_ready(): wait for {} replaced connections,"
                      " currently {} ...",
                      num_replaced, replaced_conns);
        do_wait = true;
      }
    }

    if (do_wait) {
      return interceptor.wait(
      ).then([this, num_ready_conns, num_replaced, wait_received] {
        return wait_ready(num_ready_conns, num_replaced, wait_received);
      });
    } else {
      logger().info("[Test] wait_ready(): wait done!");
      return seastar::now();
    }
  }

 // called by FailoverTest
 public:
  FailoverSuite(MessengerRef test_msgr,
                entity_addr_t test_peer_addr,
                const TestInterceptor& interceptor,
                ShardedGates &gates)
    : test_msgr(test_msgr),
      test_peer_addr(test_peer_addr),
      interceptor(interceptor),
      gates{gates},
      primary_sid{seastar::this_shard_id()} { }

  entity_addr_t get_addr() const {
    return test_msgr->get_myaddr();
  }

  seastar::future<> shutdown() {
    test_msgr->stop();
    return test_msgr->shutdown();
  }

  void needs_receive() {
    ++pending_receive;
  }

  void notify_peer_reply() {
    ceph_assert(pending_peer_receive > 0);
    --pending_peer_receive;
    logger().info("[Test] TestPeer said got op, left {} ops",
                  pending_peer_receive);
    if (pending_peer_receive == 0) {
      interceptor.notify();
    }
  }

  void post_check() const {
    // make sure all breakpoints were hit
    for (auto& kv : interceptor.breakpoints) {
      auto it = interceptor.breakpoints_counter.find(kv.first);
      if (it == interceptor.breakpoints_counter.end()) {
        throw std::runtime_error(fmt::format("{} was missed", kv.first));
      }
      auto expected = kv.second.rbegin()->first;
      if (expected > it->second.counter) {
        throw std::runtime_error(fmt::format(
              "{} only triggered {} times, not the expected {}",
              kv.first, it->second.counter, expected));
      }
    }
  }

  void dump_results() const {
    for (auto& result : interceptor.results) {
      result.dump();
    }
  }

  static seastar::future<std::unique_ptr<FailoverSuite>>
  create(entity_addr_t test_addr,
         SocketPolicy test_policy,
         entity_addr_t test_peer_addr,
         const TestInterceptor& interceptor,
         ShardedGates &gates) {
    auto suite = std::make_unique<FailoverSuite>(
        Messenger::create(
          entity_name_t::OSD(TEST_OSD),
          "Test",
          TEST_NONCE,
          false),
        test_peer_addr,
        interceptor,
        gates);
    return suite->init(test_addr, test_policy
    ).then([suite = std::move(suite)] () mutable {
      return std::move(suite);
    });
  }

 // called by tests
 public:
  seastar::future<> connect_peer() {
    logger().info("[Test] connect_peer({})", test_peer_addr);
    assert(seastar::this_shard_id() == primary_sid);
    auto conn = test_msgr->connect(test_peer_addr, entity_name_t::TYPE_OSD);
    auto result = interceptor.find_result(&*conn);
    ceph_assert(result != nullptr);

    if (tracked_conn) {
      if (tracked_conn->is_protocol_closed()) {
        logger().info("[Test] this is a new session"
                      " replacing an closed one");
        ceph_assert(tracked_conn != &*conn);
      } else {
        logger().info("[Test] this is not a new session");
        ceph_assert(tracked_index == result->index);
        ceph_assert(tracked_conn == &*conn);
      }
    } else {
      logger().info("[Test] this is a new session");
    }
    tracked_index = result->index;
    tracked_conn = &*conn;

    return flush_pending_send();
  }

  seastar::future<> send_peer() {
    assert(seastar::this_shard_id() == primary_sid);
    if (tracked_conn) {
      logger().info("[Test] send_peer()");
      ceph_assert(!tracked_conn->is_protocol_closed());
      ceph_assert(!pending_send);
      return send_op();
    } else {
      ++pending_send;
      logger().info("[Test] send_peer() (pending {})", pending_send);
      return seastar::now();
    }
  }

  seastar::future<> keepalive_peer() {
    logger().info("[Test] keepalive_peer()");
    assert(seastar::this_shard_id() == primary_sid);
    ceph_assert(tracked_conn);
    ceph_assert(!tracked_conn->is_protocol_closed());
    return tracked_conn->send_keepalive();
  }

  seastar::future<> try_send_peer() {
    logger().info("[Test] try_send_peer()");
    assert(seastar::this_shard_id() == primary_sid);
    ceph_assert(tracked_conn);
    ceph_assert(!tracked_conn->is_protocol_closed());
    return send_op(false);
  }

  seastar::future<> markdown() {
    logger().info("[Test] markdown() in 100ms ...");
    assert(seastar::this_shard_id() == primary_sid);
    ceph_assert(tracked_conn);
    // sleep to propagate potential remaining acks
    return seastar::sleep(50ms
    ).then([this] {
      return seastar::smp::submit_to(
          tracked_conn->get_shard_id(), [tracked_conn=tracked_conn] {
        assert(tracked_conn->get_shard_id() == seastar::this_shard_id());
        tracked_conn->mark_down();
      });
    }).then([] {
      // sleep to wait for markdown propagate to the primary sid
      return seastar::sleep(100ms);
    });
  }

  seastar::future<> wait_blocked() {
    logger().info("[Test] wait_blocked() ...");
    assert(seastar::this_shard_id() == primary_sid);
    return interceptor.blocker.wait_blocked();
  }

  void unblock() {
    logger().info("[Test] unblock()");
    assert(seastar::this_shard_id() == primary_sid);
    return interceptor.blocker.unblock();
  }

  seastar::future<> wait_replaced(unsigned count) {
    logger().info("[Test] wait_replaced({}) ...", count);
    return wait_ready(0, count, false);
  }

  seastar::future<> wait_established() {
    logger().info("[Test] wait_established() ...");
    return wait_ready(0, 0, true);
  }

  seastar::future<std::reference_wrapper<ConnResults>>
  wait_results(unsigned count) {
    logger().info("[Test] wait_result({}) ...", count);
    return wait_ready(count, 0, true).then([this] {
      return std::reference_wrapper<ConnResults>(interceptor.results);
    });
  }

  bool is_standby() {
    assert(seastar::this_shard_id() == primary_sid);
    ceph_assert(tracked_conn);
    return tracked_conn->is_protocol_standby();
  }
};

class FailoverTest : public Dispatcher {
  crimson::auth::DummyAuthClientServer dummy_auth;
  MessengerRef cmd_msgr;
  ConnectionRef cmd_conn;
  const entity_addr_t test_addr;
  const entity_addr_t test_peer_addr;

  std::optional<seastar::promise<>> recv_pong;
  std::optional<seastar::promise<>> recv_cmdreply;

  std::unique_ptr<FailoverSuite> test_suite;

  std::optional<seastar::future<>> ms_dispatch(ConnectionRef c, MessageRef m) override {
    switch (m->get_type()) {
     case CEPH_MSG_PING:
      ceph_assert(recv_pong);
      recv_pong->set_value();
      recv_pong = std::nullopt;
      break;
     case MSG_COMMAND_REPLY:
      ceph_assert(recv_cmdreply);
      recv_cmdreply->set_value();
      recv_cmdreply = std::nullopt;
      break;
     case MSG_COMMAND: {
      auto m_cmd = boost::static_pointer_cast<MCommand>(m);
      ceph_assert(static_cast<cmd_t>(m_cmd->cmd[0][0]) == cmd_t::suite_recv_op);
      ceph_assert(test_suite);
      test_suite->notify_peer_reply();
      break;
     }
     default:
      logger().error("{} got unexpected msg from cmd server: {}", *c, *m);
      ceph_abort();
    }
    return {seastar::now()};
  }

 private:
  seastar::future<> prepare_cmd(
      cmd_t cmd,
      std::function<void(MCommand&)>
        f_prepare = [] (auto& m) { return; }) {
    assert(!recv_cmdreply);
    recv_cmdreply  = seastar::promise<>();
    auto fut = recv_cmdreply->get_future();
    auto m = crimson::make_message<MCommand>();
    m->cmd.emplace_back(1, static_cast<char>(cmd));
    f_prepare(*m);
    return cmd_conn->send(std::move(m)).then([fut = std::move(fut)] () mutable {
      return std::move(fut);
    });
  }

  seastar::future<> start_peer(policy_t peer_policy) {
    return prepare_cmd(cmd_t::suite_start,
        [peer_policy] (auto& m) {
      m.cmd.emplace_back(1, static_cast<char>(peer_policy));
    });
  }

  seastar::future<> stop_peer() {
    return prepare_cmd(cmd_t::suite_stop);
  }

  seastar::future<> pingpong() {
    assert(!recv_pong);
    recv_pong = seastar::promise<>();
    auto fut = recv_pong->get_future();
    return cmd_conn->send(crimson::make_message<MPing>()
    ).then([fut = std::move(fut)] () mutable {
      return std::move(fut);
    });
  }

  seastar::future<> init(entity_addr_t cmd_peer_addr) {
    cmd_msgr->set_default_policy(SocketPolicy::lossy_client(0));
    cmd_msgr->set_auth_client(&dummy_auth);
    cmd_msgr->set_auth_server(&dummy_auth);
    return cmd_msgr->start({this}).then([this, cmd_peer_addr] {
      logger().info("CmdCli connect to CmdSrv({}) ...", cmd_peer_addr);
      cmd_conn = cmd_msgr->connect(cmd_peer_addr, entity_name_t::TYPE_OSD);
      return pingpong();
    });
  }

 public:
  FailoverTest(MessengerRef cmd_msgr,
               entity_addr_t test_addr,
               entity_addr_t test_peer_addr)
    : cmd_msgr(cmd_msgr),
      test_addr(test_addr),
      test_peer_addr(test_peer_addr) { }

  seastar::future<> shutdown() {
    logger().info("CmdCli shutdown...");
    assert(!recv_cmdreply);
    auto m = crimson::make_message<MCommand>();
    m->cmd.emplace_back(1, static_cast<char>(cmd_t::shutdown));
    return cmd_conn->send(std::move(m)).then([] {
      return seastar::sleep(200ms);
    }).then([this] {
      cmd_msgr->stop();
      return cmd_msgr->shutdown();
    });
  }

  static seastar::future<seastar::lw_shared_ptr<FailoverTest>>
  create(entity_addr_t test_addr,
         entity_addr_t cmd_peer_addr,
         entity_addr_t test_peer_addr) {
    auto test = seastar::make_lw_shared<FailoverTest>(
        Messenger::create(
          entity_name_t::OSD(CMD_CLI_OSD),
          "CmdCli",
          CMD_CLI_NONCE,
          true),
        test_addr, test_peer_addr);
    return test->init(cmd_peer_addr).then([test] {
      logger().info("CmdCli ready");
      return test;
    });
  }

 // called by tests
 public:
  seastar::future<> run_suite(
      std::string name,
      const TestInterceptor& interceptor,
      policy_t test_policy,
      policy_t peer_policy,
      std::function<seastar::future<>(FailoverSuite&)>&& f) {
    logger().info("\n\n[{}]", name);
    ceph_assert(!test_suite);
    SocketPolicy test_policy_ = to_socket_policy(test_policy);
    return ShardedGates::create(
    ).then([this, test_policy_, peer_policy, interceptor,
            f=std::move(f)](auto *gates) mutable {
      return FailoverSuite::create(
        test_addr, test_policy_, test_peer_addr, interceptor, *gates
      ).then([this, peer_policy, f = std::move(f)](auto suite) mutable {
        ceph_assert(suite->get_addr() == test_addr);
        test_suite.swap(suite);
        return start_peer(peer_policy
        ).then([this, f = std::move(f)] {
          return f(*test_suite);
        }).then([this] {
          test_suite->post_check();
          logger().info("\n[SUCCESS]");
        }).handle_exception([this](auto eptr) {
          logger().info("\n[FAIL: {}]", eptr);
          test_suite->dump_results();
          throw;
        }).then([this] {
          return stop_peer();
        }).then([this] {
          return test_suite->shutdown(
          ).then([this] {
            test_suite.reset();
          });
        });
      }).then([gates] {
        return gates->close();
      });
    });
  }

  seastar::future<> peer_connect_me() {
    logger().info("[Test] peer_connect_me({})", test_addr);
    return prepare_cmd(cmd_t::suite_connect_me,
        [this] (auto& m) {
      m.cmd.emplace_back(fmt::format("{}", test_addr));
    });
  }

  seastar::future<> peer_send_me() {
    logger().info("[Test] peer_send_me()");
    ceph_assert(test_suite);
    test_suite->needs_receive();
    return prepare_cmd(cmd_t::suite_send_me);
  }

  seastar::future<> try_peer_send_me() {
    logger().info("[Test] try_peer_send_me()");
    ceph_assert(test_suite);
    return prepare_cmd(cmd_t::suite_send_me);
  }

  seastar::future<> send_bidirectional() {
    ceph_assert(test_suite);
    return test_suite->send_peer().then([this] {
      return peer_send_me();
    });
  }

  seastar::future<> peer_keepalive_me() {
    logger().info("[Test] peer_keepalive_me()");
    ceph_assert(test_suite);
    return prepare_cmd(cmd_t::suite_keepalive_me);
  }

  seastar::future<> markdown_peer() {
    logger().info("[Test] markdown_peer() in 150ms ...");
    // sleep to propagate potential remaining acks
    return seastar::sleep(50ms
    ).then([this] {
      return prepare_cmd(cmd_t::suite_markdown);
    }).then([] {
      // sleep awhile for peer markdown propagated
      return seastar::sleep(100ms);
    });
  }
};

class FailoverSuitePeer : public Dispatcher {
  using cb_t = std::function<seastar::future<>()>;
  crimson::auth::DummyAuthClientServer dummy_auth;
  MessengerRef peer_msgr;
  cb_t op_callback;

  ConnectionRef tracked_conn;
  unsigned pending_send = 0;

  std::optional<seastar::future<>> ms_dispatch(ConnectionRef conn, MessageRef m) override {
    logger().info("[TestPeer] got op from Test");
    ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
    std::ignore = op_callback();
    return {seastar::now()};
  }

  void ms_handle_accept(
      ConnectionRef conn,
      seastar::shard_id prv_shard,
      bool is_replace) override {
    assert(prv_shard == seastar::this_shard_id());
    logger().info("[TestPeer] got accept from Test");

    if (tracked_conn &&
        !tracked_conn->is_protocol_closed() &&
        tracked_conn != conn) {
      logger().error("[TestPeer] {} got accepted, but there's already a valid traced_conn {}",
                     *conn, *tracked_conn);
    }
    tracked_conn = conn;
    std::ignore = flush_pending_send();
  }

  void ms_handle_reset(ConnectionRef conn, bool is_replace) override {
    logger().info("[TestPeer] got reset from Test");
  }

 private:
  seastar::future<> init(entity_addr_t test_peer_addr, SocketPolicy policy) {
    peer_msgr->set_default_policy(policy);
    peer_msgr->set_auth_client(&dummy_auth);
    peer_msgr->set_auth_server(&dummy_auth);
    return peer_msgr->bind(entity_addrvec_t{test_peer_addr}).safe_then([this] {
      return peer_msgr->start({this});
    }, Messenger::bind_ertr::all_same_way([test_peer_addr] (const std::error_code& e) {
      logger().error("FailoverSuitePeer: "
                     "there is another instance running at {}", test_peer_addr);
      ceph_abort();
    }));
  }

  seastar::future<> send_op() {
    ceph_assert(tracked_conn);
    if (tracked_conn->is_protocol_closed()) {
      logger().error("[TestPeer] send op but the connection is closed -- {}",
                     *tracked_conn);
    }

    pg_t pgid;
    object_locator_t oloc;
    hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                   pgid.pool(), oloc.nspace);
    spg_t spgid(pgid);
    return tracked_conn->send(crimson::make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0));
  }

  seastar::future<> flush_pending_send() {
    if (pending_send != 0) {
      logger().info("[TestPeer] flush sending {} ops", pending_send);
    }
    ceph_assert(tracked_conn);
    return seastar::do_until(
        [this] { return pending_send == 0; },
        [this] {
      --pending_send;
      return send_op();
    });
  }

 public:
  FailoverSuitePeer(MessengerRef peer_msgr, cb_t op_callback)
    : peer_msgr(peer_msgr),
      op_callback(op_callback) { }

  seastar::future<> shutdown() {
    peer_msgr->stop();
    return peer_msgr->shutdown();
  }

  seastar::future<> connect_peer(entity_addr_t test_addr_decoded) {
    logger().info("[TestPeer] connect_peer({})", test_addr_decoded);
    auto conn = peer_msgr->connect(test_addr_decoded, entity_name_t::TYPE_OSD);

    if (tracked_conn) {
      if (tracked_conn->is_protocol_closed()) {
        logger().info("[TestPeer] this is a new session"
                      " replacing an closed one");
        ceph_assert(tracked_conn != conn);
      } else {
        logger().info("[TestPeer] this is not a new session");
        ceph_assert(tracked_conn == conn);
      }
    } else {
      logger().info("[TestPeer] this is a new session");
    }
    tracked_conn = conn;

    return flush_pending_send();
  }

  seastar::future<> send_peer() {
    if (tracked_conn) {
      logger().info("[TestPeer] send_peer()");
      ceph_assert(!pending_send);
      return send_op();
    } else {
      ++pending_send;
      logger().info("[TestPeer] send_peer() (pending {})", pending_send);
      return seastar::now();
    }
  }

  seastar::future<> keepalive_peer() {
    logger().info("[TestPeer] keepalive_peer()");
    ceph_assert(tracked_conn);
    return tracked_conn->send_keepalive();
  }

  seastar::future<> markdown() {
    logger().info("[TestPeer] markdown()");
    ceph_assert(tracked_conn);
    tracked_conn->mark_down();
    return seastar::now();
  }

  static seastar::future<std::unique_ptr<FailoverSuitePeer>>
  create(entity_addr_t test_peer_addr, const SocketPolicy& policy, cb_t op_callback) {
    auto suite = std::make_unique<FailoverSuitePeer>(
      Messenger::create(
        entity_name_t::OSD(TEST_PEER_OSD),
        "TestPeer",
        TEST_PEER_NONCE,
        true),
      op_callback
    );
    return suite->init(test_peer_addr, policy
    ).then([suite = std::move(suite)] () mutable {
      return std::move(suite);
    });
  }
};

class FailoverTestPeer : public Dispatcher {
  crimson::auth::DummyAuthClientServer dummy_auth;
  MessengerRef cmd_msgr;
  ConnectionRef cmd_conn;
  const entity_addr_t test_peer_addr;
  std::unique_ptr<FailoverSuitePeer> test_suite;

  std::optional<seastar::future<>> ms_dispatch(ConnectionRef c, MessageRef m) override {
    ceph_assert(cmd_conn == c);
    switch (m->get_type()) {
     case CEPH_MSG_PING:
      std::ignore = c->send(crimson::make_message<MPing>());
      break;
     case MSG_COMMAND: {
      auto m_cmd = boost::static_pointer_cast<MCommand>(m);
      auto cmd = static_cast<cmd_t>(m_cmd->cmd[0][0]);
      if (cmd == cmd_t::shutdown) {
        logger().info("CmdSrv shutdown...");
        // forwarded to FailoverTestPeer::wait()
        cmd_msgr->stop();
        std::ignore = cmd_msgr->shutdown();
      } else {
        std::ignore = handle_cmd(cmd, m_cmd).then([c] {
          return c->send(crimson::make_message<MCommandReply>());
        });
      }
      break;
     }
     default:
      logger().error("{} got unexpected msg from cmd client: {}", *c, *m);
      ceph_abort();
    }
    return {seastar::now()};
  }

  void ms_handle_accept(
      ConnectionRef conn,
      seastar::shard_id prv_shard,
      bool is_replace) override {
    assert(prv_shard == seastar::this_shard_id());
    cmd_conn = conn;
  }

 private:
  seastar::future<> notify_recv_op() {
    ceph_assert(cmd_conn);
    auto m = crimson::make_message<MCommand>();
    m->cmd.emplace_back(1, static_cast<char>(cmd_t::suite_recv_op));
    return cmd_conn->send(std::move(m));
  }

  seastar::future<> handle_cmd(cmd_t cmd, MRef<MCommand> m_cmd) {
    switch (cmd) {
     case cmd_t::suite_start: {
      ceph_assert(!test_suite);
      auto policy = to_socket_policy(static_cast<policy_t>(m_cmd->cmd[1][0]));
      return FailoverSuitePeer::create(
        test_peer_addr, policy, [this] { return notify_recv_op(); }
      ).then([this] (auto suite) {
        test_suite.swap(suite);
      });
     }
     case cmd_t::suite_stop:
      ceph_assert(test_suite);
      return test_suite->shutdown().then([this] {
        test_suite.reset();
      });
     case cmd_t::suite_connect_me: {
      ceph_assert(test_suite);
      entity_addr_t test_addr_decoded = entity_addr_t();
      test_addr_decoded.parse(m_cmd->cmd[1].c_str(), nullptr);
      return test_suite->connect_peer(test_addr_decoded);
     }
     case cmd_t::suite_send_me:
      ceph_assert(test_suite);
      return test_suite->send_peer();
     case cmd_t::suite_keepalive_me:
      ceph_assert(test_suite);
      return test_suite->keepalive_peer();
     case cmd_t::suite_markdown:
      ceph_assert(test_suite);
      return test_suite->markdown();
     default:
      logger().error("TestPeer got unexpected command {} from Test",
		     fmt::ptr(m_cmd.get()));
      ceph_abort();
      return seastar::now();
    }
  }

  seastar::future<> init(entity_addr_t cmd_peer_addr) {
    cmd_msgr->set_default_policy(SocketPolicy::stateless_server(0));
    cmd_msgr->set_auth_client(&dummy_auth);
    cmd_msgr->set_auth_server(&dummy_auth);
    return cmd_msgr->bind(entity_addrvec_t{cmd_peer_addr}).safe_then([this] {
      return cmd_msgr->start({this});
    }, Messenger::bind_ertr::all_same_way([cmd_peer_addr] (const std::error_code& e) {
      logger().error("FailoverTestPeer: "
                     "there is another instance running at {}", cmd_peer_addr);
      ceph_abort();
    }));
  }

 public:
  FailoverTestPeer(MessengerRef cmd_msgr,
                   entity_addr_t test_peer_addr)
    : cmd_msgr(cmd_msgr),
      test_peer_addr(test_peer_addr) { }

  seastar::future<> wait() {
    return cmd_msgr->wait();
  }

  static seastar::future<std::unique_ptr<FailoverTestPeer>>
  create(entity_addr_t cmd_peer_addr, entity_addr_t test_peer_addr) {
    auto test_peer = std::make_unique<FailoverTestPeer>(
        Messenger::create(
          entity_name_t::OSD(CMD_SRV_OSD),
          "CmdSrv",
          CMD_SRV_NONCE,
          true),
        test_peer_addr);
    return test_peer->init(cmd_peer_addr
    ).then([test_peer = std::move(test_peer)] () mutable {
      logger().info("CmdSrv ready");
      return std::move(test_peer);
    });
  }
};

seastar::future<>
test_v2_lossy_early_connect_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {custom_bp_t::SOCKET_CONNECTING},
      {custom_bp_t::BANNER_WRITE},
      {custom_bp_t::BANNER_READ},
      {custom_bp_t::BANNER_PAYLOAD_READ},
      {Tag::HELLO, bp_type_t::WRITE},
      {Tag::HELLO, bp_type_t::READ},
      {Tag::AUTH_REQUEST, bp_type_t::WRITE},
      {Tag::AUTH_DONE, bp_type_t::READ},
      {Tag::AUTH_SIGNATURE, bp_type_t::WRITE},
      {Tag::AUTH_SIGNATURE, bp_type_t::READ},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_early_connect_fault -- {}", bp),
          interceptor,
          policy_t::lossy_client,
          policy_t::stateless_server,
          [] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 1, 0, 1);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossy_connect_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::CLIENT_IDENT, bp_type_t::WRITE},
      {Tag::SERVER_IDENT, bp_type_t::READ},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_connect_fault -- {}", bp),
          interceptor,
          policy_t::lossy_client,
          policy_t::stateless_server,
          [] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 2, 0, 1);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossy_connected_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::MESSAGE, bp_type_t::WRITE},
      {Tag::MESSAGE, bp_type_t::READ},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_connected_fault -- {}", bp),
          interceptor,
          policy_t::lossy_client,
          policy_t::stateless_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&test] {
          return test.send_bidirectional();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::closed);
          results[0].assert_connect(1, 1, 0, 1);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(1, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossy_early_accept_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {custom_bp_t::BANNER_WRITE},
      {custom_bp_t::BANNER_READ},
      {custom_bp_t::BANNER_PAYLOAD_READ},
      {Tag::HELLO, bp_type_t::WRITE},
      {Tag::HELLO, bp_type_t::READ},
      {Tag::AUTH_REQUEST, bp_type_t::READ},
      {Tag::AUTH_DONE, bp_type_t::WRITE},
      {Tag::AUTH_SIGNATURE, bp_type_t::WRITE},
      {Tag::AUTH_SIGNATURE, bp_type_t::READ},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_early_accept_fault -- {}", bp),
          interceptor,
          policy_t::stateless_server,
          policy_t::lossy_client,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&test] {
          return test.peer_send_me();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::closed);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 0, 0, 0);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::established);
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 1, 0, 1);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossy_accept_fault(FailoverTest& test) {
  auto bp = Breakpoint{Tag::CLIENT_IDENT, bp_type_t::READ};
  TestInterceptor interceptor;
  interceptor.make_fault(bp);
  return test.run_suite(
      fmt::format("test_v2_lossy_accept_fault -- {}", bp),
      interceptor,
      policy_t::stateless_server,
      policy_t::lossy_client,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.peer_send_me();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 1);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_lossy_establishing_fault(FailoverTest& test) {
  auto bp = Breakpoint{Tag::SERVER_IDENT, bp_type_t::WRITE};
  TestInterceptor interceptor;
  interceptor.make_fault(bp);
  return test.run_suite(
      fmt::format("test_v2_lossy_establishing_fault -- {}", bp),
      interceptor,
      policy_t::stateless_server,
      policy_t::lossy_client,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.peer_send_me();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 1);
      results[0].assert_reset(1, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 1);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_lossy_accepted_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::MESSAGE, bp_type_t::WRITE},
      {Tag::MESSAGE, bp_type_t::READ},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_accepted_fault -- {}", bp),
          interceptor,
          policy_t::stateless_server,
          policy_t::lossy_client,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&test] {
          return test.send_bidirectional();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::closed);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 1, 0, 1);
          results[0].assert_reset(1, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_connect_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::CLIENT_IDENT, bp_type_t::WRITE},
      {Tag::SERVER_IDENT, bp_type_t::READ},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossless_connect_fault -- {}", bp),
          interceptor,
          policy_t::lossless_client,
          policy_t::stateful_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&test] {
          return test.send_bidirectional();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 2, 0, 1);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_connected_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::MESSAGE, bp_type_t::WRITE},
      {Tag::MESSAGE, bp_type_t::READ},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossless_connected_fault -- {}", bp),
          interceptor,
          policy_t::lossless_client,
          policy_t::stateful_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&test] {
          return test.send_bidirectional();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 1, 1, 2);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_connected_fault2(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::ACK, bp_type_t::READ},
      {Tag::ACK, bp_type_t::WRITE},
      {Tag::KEEPALIVE2, bp_type_t::READ},
      {Tag::KEEPALIVE2, bp_type_t::WRITE},
      {Tag::KEEPALIVE2_ACK, bp_type_t::READ},
      {Tag::KEEPALIVE2_ACK, bp_type_t::WRITE},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossless_connected_fault2 -- {}", bp),
          interceptor,
          policy_t::lossless_client,
          policy_t::stateful_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_established();
        }).then([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.keepalive_peer();
        }).then([&suite] {
          return suite.wait_established();
        }).then([&test] {
          return test.peer_send_me();
        }).then([&test] {
          return test.peer_keepalive_me();
        }).then([&suite] {
          return suite.wait_established();
        }).then([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.wait_established();
        }).then([&test] {
          return test.peer_send_me();
        }).then([&suite] {
          return suite.wait_established();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 1, 1, 2);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_reconnect_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<std::pair<Breakpoint, Breakpoint>>{
      {{Tag::MESSAGE, bp_type_t::WRITE},
       {Tag::SESSION_RECONNECT, bp_type_t::WRITE}},
      {{Tag::MESSAGE, bp_type_t::WRITE},
       {Tag::SESSION_RECONNECT_OK, bp_type_t::READ}},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp_pair) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp_pair.first);
      interceptor.make_fault(bp_pair.second);
      return test.run_suite(
          fmt::format("test_v2_lossless_reconnect_fault -- {}, {}",
                      bp_pair.first, bp_pair.second),
          interceptor,
          policy_t::lossless_client,
          policy_t::stateful_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&test] {
          return test.send_bidirectional();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(3, 1, 2, 2);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_accept_fault(FailoverTest& test) {
  auto bp = Breakpoint{Tag::CLIENT_IDENT, bp_type_t::READ};
  TestInterceptor interceptor;
  interceptor.make_fault(bp);
  return test.run_suite(
      fmt::format("test_v2_lossless_accept_fault -- {}", bp),
      interceptor,
      policy_t::stateful_server,
      policy_t::lossless_client,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.send_bidirectional();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 1);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_lossless_establishing_fault(FailoverTest& test) {
  auto bp = Breakpoint{Tag::SERVER_IDENT, bp_type_t::WRITE};
  TestInterceptor interceptor;
  interceptor.make_fault(bp);
  return test.run_suite(
      fmt::format("test_v2_lossless_establishing_fault -- {}", bp),
      interceptor,
      policy_t::stateful_server,
      policy_t::lossless_client,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.send_bidirectional();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_lossless_accepted_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::MESSAGE, bp_type_t::WRITE},
      {Tag::MESSAGE, bp_type_t::READ},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossless_accepted_fault -- {}", bp),
          interceptor,
          policy_t::stateful_server,
          policy_t::lossless_client,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&test] {
          return test.send_bidirectional();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 1, 0, 2);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::replaced);
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 0);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_reaccept_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<std::pair<Breakpoint, Breakpoint>>{
      {{Tag::MESSAGE, bp_type_t::READ},
       {Tag::SESSION_RECONNECT, bp_type_t::READ}},
      {{Tag::MESSAGE, bp_type_t::READ},
       {Tag::SESSION_RECONNECT_OK, bp_type_t::WRITE}},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp_pair) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp_pair.first);
      interceptor.make_fault(bp_pair.second);
      return test.run_suite(
          fmt::format("test_v2_lossless_reaccept_fault -- {}, {}",
                      bp_pair.first, bp_pair.second),
          interceptor,
          policy_t::stateful_server,
          policy_t::lossless_client,
          [&test, bp = bp_pair.second] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&test] {
          return test.send_bidirectional();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(3);
        }).then([bp] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(0, 0, 0, 0);
          if (bp == Breakpoint{Tag::SESSION_RECONNECT, bp_type_t::READ}) {
            results[0].assert_accept(1, 1, 0, 2);
          } else {
            results[0].assert_accept(1, 1, 0, 3);
          }
          results[0].assert_reset(0, 0);
          if (bp == Breakpoint{Tag::SESSION_RECONNECT, bp_type_t::READ}) {
            results[1].assert_state_at(conn_state_t::closed);
          } else {
            results[1].assert_state_at(conn_state_t::replaced);
          }
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 0, 1, 0);
          results[1].assert_reset(0, 0);
          results[2].assert_state_at(conn_state_t::replaced);
          results[2].assert_connect(0, 0, 0, 0);
          results[2].assert_accept(1, 0, 1, 0);
          results[2].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_peer_connect_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::CLIENT_IDENT, bp_type_t::WRITE},
      {Tag::SERVER_IDENT, bp_type_t::READ},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_peer_connect_fault -- {}", bp),
          interceptor,
          policy_t::lossless_peer,
          policy_t::lossless_peer,
          [] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 2, 0, 1);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_peer_accept_fault(FailoverTest& test) {
  auto bp = Breakpoint{Tag::CLIENT_IDENT, bp_type_t::READ};
  TestInterceptor interceptor;
  interceptor.make_fault(bp);
  return test.run_suite(
      fmt::format("test_v2_peer_accept_fault -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.peer_send_me();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 1);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_peer_establishing_fault(FailoverTest& test) {
  auto bp = Breakpoint{Tag::SERVER_IDENT, bp_type_t::WRITE};
  TestInterceptor interceptor;
  interceptor.make_fault(bp);
  return test.run_suite(
      fmt::format("test_v2_peer_establishing_fault -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.peer_send_me();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_peer_connected_fault_reconnect(FailoverTest& test) {
  auto bp = Breakpoint{Tag::MESSAGE, bp_type_t::WRITE};
  TestInterceptor interceptor;
  interceptor.make_fault(bp);
  return test.run_suite(
      fmt::format("test_v2_peer_connected_fault_reconnect -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.wait_results(1);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(2, 1, 1, 2);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_peer_connected_fault_reaccept(FailoverTest& test) {
  auto bp = Breakpoint{Tag::MESSAGE, bp_type_t::READ};
  TestInterceptor interceptor;
  interceptor.make_fault(bp);
  return test.run_suite(
      fmt::format("test_v2_peer_connected_fault_reaccept -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 1);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 0, 1, 0);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<bool>
check_peer_wins(FailoverTest& test) {
  return seastar::do_with(bool(), [&test] (auto& ret) {
    return test.run_suite("check_peer_wins",
                          TestInterceptor(),
                          policy_t::lossy_client,
                          policy_t::stateless_server,
                          [&ret] (FailoverSuite& suite) {
      return suite.connect_peer().then([&suite] {
        return suite.wait_results(1);
      }).then([&ret] (ConnResults& results) {
        results[0].assert_state_at(conn_state_t::established);
        ret = results[0].conn->peer_wins();
        logger().info("check_peer_wins: {}", ret);
      });
    }).then([&ret] {
      return ret;
    });
  });
}

seastar::future<>
test_v2_racing_reconnect_acceptor_lose(FailoverTest& test) {
  return seastar::do_with(std::vector<std::pair<unsigned, Breakpoint>>{
      {1, {Tag::SESSION_RECONNECT, bp_type_t::READ}},
      {2, {custom_bp_t::BANNER_WRITE}},
      {2, {custom_bp_t::BANNER_READ}},
      {2, {custom_bp_t::BANNER_PAYLOAD_READ}},
      {2, {Tag::HELLO, bp_type_t::WRITE}},
      {2, {Tag::HELLO, bp_type_t::READ}},
      {2, {Tag::AUTH_REQUEST, bp_type_t::READ}},
      {2, {Tag::AUTH_DONE, bp_type_t::WRITE}},
      {2, {Tag::AUTH_SIGNATURE, bp_type_t::WRITE}},
      {2, {Tag::AUTH_SIGNATURE, bp_type_t::READ}},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      // fault acceptor
      interceptor.make_fault({Tag::MESSAGE, bp_type_t::READ});
      // block acceptor
      interceptor.make_block(bp.second, bp.first);
      return test.run_suite(
          fmt::format("test_v2_racing_reconnect_acceptor_lose -- {}({})",
                      bp.second, bp.first),
          interceptor,
          policy_t::lossless_peer,
          policy_t::lossless_peer,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&test] {
          return test.peer_send_me();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_blocked();
        }).then([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.wait_established();
        }).then([&suite] {
          suite.unblock();
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(1, 0, 1, 1);
          results[0].assert_accept(1, 1, 0, 1);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::closed);
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 0);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_racing_reconnect_acceptor_win(FailoverTest& test) {
  return seastar::do_with(std::vector<std::pair<unsigned, Breakpoint>>{
      {1, {Tag::SESSION_RECONNECT, bp_type_t::WRITE}},
      {2, {custom_bp_t::SOCKET_CONNECTING}},
      {2, {custom_bp_t::BANNER_WRITE}},
      {2, {custom_bp_t::BANNER_READ}},
      {2, {custom_bp_t::BANNER_PAYLOAD_READ}},
      {2, {Tag::HELLO, bp_type_t::WRITE}},
      {2, {Tag::HELLO, bp_type_t::READ}},
      {2, {Tag::AUTH_REQUEST, bp_type_t::WRITE}},
      {2, {Tag::AUTH_DONE, bp_type_t::READ}},
      {2, {Tag::AUTH_SIGNATURE, bp_type_t::WRITE}},
      {2, {Tag::AUTH_SIGNATURE, bp_type_t::READ}},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      // fault connector
      interceptor.make_fault({Tag::MESSAGE, bp_type_t::WRITE});
      // block connector
      interceptor.make_block(bp.second, bp.first);
      return test.run_suite(
          fmt::format("test_v2_racing_reconnect_acceptor_win -- {}({})",
                      bp.second, bp.first),
          interceptor,
          policy_t::lossless_peer,
          policy_t::lossless_peer,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_blocked();
        }).then([&test] {
          return test.peer_send_me();
        }).then([&suite] {
          return suite.wait_replaced(1);
        }).then([&suite] {
          suite.unblock();
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 1);
          results[0].assert_accept(0, 0, 0, 1);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::replaced);
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 0, 1, 0);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_racing_connect_acceptor_lose(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {custom_bp_t::BANNER_WRITE},
      {custom_bp_t::BANNER_READ},
      {custom_bp_t::BANNER_PAYLOAD_READ},
      {Tag::HELLO, bp_type_t::WRITE},
      {Tag::HELLO, bp_type_t::READ},
      {Tag::AUTH_REQUEST, bp_type_t::READ},
      {Tag::AUTH_DONE, bp_type_t::WRITE},
      {Tag::AUTH_SIGNATURE, bp_type_t::WRITE},
      {Tag::AUTH_SIGNATURE, bp_type_t::READ},
      {Tag::CLIENT_IDENT, bp_type_t::READ},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      // block acceptor
      interceptor.make_block(bp);
      return test.run_suite(
          fmt::format("test_v2_racing_connect_acceptor_lose -- {}", bp),
          interceptor,
          policy_t::lossless_peer,
          policy_t::lossless_peer,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&test] {
          return test.peer_send_me();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_blocked();
        }).then([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_established();
        }).then([&suite] {
          suite.unblock();
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::closed);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 0);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::established);
          results[1].assert_connect(1, 1, 0, 1);
          results[1].assert_accept(0, 0, 0, 0);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_racing_connect_acceptor_win(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {custom_bp_t::SOCKET_CONNECTING},
      {custom_bp_t::BANNER_WRITE},
      {custom_bp_t::BANNER_READ},
      {custom_bp_t::BANNER_PAYLOAD_READ},
      {Tag::HELLO, bp_type_t::WRITE},
      {Tag::HELLO, bp_type_t::READ},
      {Tag::AUTH_REQUEST, bp_type_t::WRITE},
      {Tag::AUTH_DONE, bp_type_t::READ},
      {Tag::AUTH_SIGNATURE, bp_type_t::WRITE},
      {Tag::AUTH_SIGNATURE, bp_type_t::READ},
      {Tag::CLIENT_IDENT, bp_type_t::WRITE},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      // block connector
      interceptor.make_block(bp);
      return test.run_suite(
          fmt::format("test_v2_racing_connect_acceptor_win -- {}", bp),
          interceptor,
          policy_t::lossless_peer,
          policy_t::lossless_peer,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_invoke([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_blocked();
        }).then([&test] {
          return test.peer_send_me();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_replaced(1);
        }).then([&suite] {
          suite.unblock();
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(1, 0);
          results[0].assert_accept(0, 0, 0, 1);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::replaced);
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 1, 0, 0);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_racing_connect_reconnect_lose(FailoverTest& test) {
  TestInterceptor interceptor;
  interceptor.make_fault({Tag::SERVER_IDENT, bp_type_t::READ});
  interceptor.make_block({Tag::CLIENT_IDENT, bp_type_t::WRITE}, 2);
  return test.run_suite("test_v2_racing_connect_reconnect_lose",
                        interceptor,
                        policy_t::lossless_peer,
                        policy_t::lossless_peer,
                        [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.wait_blocked();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_replaced(1);
    }).then([&suite] {
      suite.unblock();
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(2, 2, 0, 0);
      results[0].assert_accept(0, 0, 0, 1);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 1, 0);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_racing_connect_reconnect_win(FailoverTest& test) {
  TestInterceptor interceptor;
  interceptor.make_fault({Tag::SERVER_IDENT, bp_type_t::READ});
  interceptor.make_block({Tag::SESSION_RECONNECT, bp_type_t::READ});
  return test.run_suite("test_v2_racing_connect_reconnect_win",
                        interceptor,
                        policy_t::lossless_peer,
                        policy_t::lossless_peer,
                        [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.wait_blocked();
    }).then([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.wait_established();
    }).then([&suite] {
      suite.unblock();
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(2, 2, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::closed);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 0, 1, 0);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_stale_connect(FailoverTest& test) {
  auto bp = Breakpoint{Tag::SERVER_IDENT, bp_type_t::READ};
  TestInterceptor interceptor;
  interceptor.make_stall(bp);
  return test.run_suite(
      fmt::format("test_v2_stale_connect -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.wait_blocked();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_replaced(1);
    }).then([&suite] {
      suite.unblock();
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(1, 1, 0, 0);
      results[0].assert_accept(0, 0, 0, 1);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 1, 0);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_stale_reconnect(FailoverTest& test) {
  auto bp = Breakpoint{Tag::SESSION_RECONNECT_OK, bp_type_t::READ};
  TestInterceptor interceptor;
  interceptor.make_fault({Tag::MESSAGE, bp_type_t::WRITE});
  interceptor.make_stall(bp);
  return test.run_suite(
      fmt::format("test_v2_stale_reconnect -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.wait_blocked();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_replaced(1);
    }).then([&suite] {
      suite.unblock();
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(2, 1, 1, 1);
      results[0].assert_accept(0, 0, 0, 1);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 0, 1, 0);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_stale_accept(FailoverTest& test) {
  auto bp = Breakpoint{Tag::CLIENT_IDENT, bp_type_t::READ};
  TestInterceptor interceptor;
  interceptor.make_stall(bp);
  return test.run_suite(
      fmt::format("test_v2_stale_accept -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.peer_connect_me();
    }).then([&suite] {
      return suite.wait_blocked();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_established();
    }).then([&suite] {
      suite.unblock();
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 1);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_stale_establishing(FailoverTest& test) {
  auto bp = Breakpoint{Tag::SERVER_IDENT, bp_type_t::WRITE};
  TestInterceptor interceptor;
  interceptor.make_stall(bp);
  return test.run_suite(
      fmt::format("test_v2_stale_establishing -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.peer_connect_me();
    }).then([&suite] {
      return suite.wait_blocked();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_replaced(1);
    }).then([&suite] {
      suite.unblock();
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 0);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_stale_reaccept(FailoverTest& test) {
  auto bp = Breakpoint{Tag::SESSION_RECONNECT_OK, bp_type_t::WRITE};
  TestInterceptor interceptor;
  interceptor.make_fault({Tag::MESSAGE, bp_type_t::READ});
  interceptor.make_stall(bp);
  return test.run_suite(
      fmt::format("test_v2_stale_reaccept -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      return test.peer_send_me();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&suite] {
      return suite.wait_blocked();
    }).then([] {
      logger().info("[Test] block the broken REPLACING for 210ms...");
      return seastar::sleep(210ms);
    }).then([&suite] {
      suite.unblock();
      return suite.wait_results(3);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 3);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 0, 1, 0);
      results[1].assert_reset(0, 0);
      results[2].assert_state_at(conn_state_t::replaced);
      results[2].assert_connect(0, 0, 0, 0);
      results[2].assert_accept(1, 0);
      results[2].assert_reset(0, 0);
      ceph_assert(results[2].server_reconnect_attempts >= 1);
    });
  });
}

seastar::future<>
test_v2_lossy_client(FailoverTest& test) {
  return test.run_suite(
      "test_v2_lossy_client",
      TestInterceptor(),
      policy_t::lossy_client,
      policy_t::stateless_server,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&suite] {
      logger().info("-- 0 --");
      logger().info("[Test] setup connection...");
      return suite.connect_peer();
    }).then([&test] {
      return test.send_bidirectional();
    }).then([&suite] {
      return suite.wait_results(1);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
    }).then([&suite] {
      logger().info("-- 1 --");
      logger().info("[Test] client markdown...");
      return suite.markdown();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(1, 1, 0, 1);
      results[1].assert_accept(0, 0, 0, 0);
      results[1].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 2 --");
      logger().info("[Test] server markdown...");
      return test.markdown_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::closed);
      results[1].assert_connect(1, 1, 0, 1);
      results[1].assert_accept(0, 0, 0, 0);
      results[1].assert_reset(1, 0);
    }).then([&suite] {
      logger().info("-- 3 --");
      logger().info("[Test] client reconnect...");
      return suite.connect_peer();
    }).then([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.wait_results(3);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::closed);
      results[1].assert_connect(1, 1, 0, 1);
      results[1].assert_accept(0, 0, 0, 0);
      results[1].assert_reset(1, 0);
      results[2].assert_state_at(conn_state_t::established);
      results[2].assert_connect(1, 1, 0, 1);
      results[2].assert_accept(0, 0, 0, 0);
      results[2].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_stateless_server(FailoverTest& test) {
  return test.run_suite(
      "test_v2_stateless_server",
      TestInterceptor(),
      policy_t::stateless_server,
      policy_t::lossy_client,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      logger().info("-- 0 --");
      logger().info("[Test] setup connection...");
      return test.peer_connect_me();
    }).then([&test] {
      return test.send_bidirectional();
    }).then([&suite] {
      return suite.wait_results(1);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 1);
      results[0].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 1 --");
      logger().info("[Test] client markdown...");
      return test.markdown_peer();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 1);
      results[0].assert_reset(1, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 1);
      results[1].assert_reset(0, 0);
    }).then([&suite] {
      logger().info("-- 2 --");
      logger().info("[Test] server markdown...");
      return suite.markdown();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 1);
      results[0].assert_reset(1, 0);
      results[1].assert_state_at(conn_state_t::closed);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 1);
      results[1].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 3 --");
      logger().info("[Test] client reconnect...");
      return test.peer_connect_me();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_results(3);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 1);
      results[0].assert_reset(1, 0);
      results[1].assert_state_at(conn_state_t::closed);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 1);
      results[1].assert_reset(0, 0);
      results[2].assert_state_at(conn_state_t::established);
      results[2].assert_connect(0, 0, 0, 0);
      results[2].assert_accept(1, 1, 0, 1);
      results[2].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_lossless_client(FailoverTest& test) {
  return test.run_suite(
      "test_v2_lossless_client",
      TestInterceptor(),
      policy_t::lossless_client,
      policy_t::stateful_server,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&suite] {
      logger().info("-- 0 --");
      logger().info("[Test] setup connection...");
      return suite.connect_peer();
    }).then([&test] {
      return test.send_bidirectional();
    }).then([&suite] {
      return suite.wait_results(1);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
    }).then([&suite] {
      logger().info("-- 1 --");
      logger().info("[Test] client markdown...");
      return suite.markdown();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(1, 1, 0, 1);
      results[1].assert_accept(0, 0, 0, 0);
      results[1].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 2 --");
      logger().info("[Test] server markdown...");
      return test.markdown_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(2, 2, 1, 2);
      results[1].assert_accept(0, 0, 0, 0);
      results[1].assert_reset(0, 1);
    }).then([&suite] {
      logger().info("-- 3 --");
      logger().info("[Test] client reconnect...");
      return suite.connect_peer();
    }).then([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(2, 2, 1, 2);
      results[1].assert_accept(0, 0, 0, 0);
      results[1].assert_reset(0, 1);
    });
  });
}

seastar::future<>
test_v2_stateful_server(FailoverTest& test) {
  return test.run_suite(
      "test_v2_stateful_server",
      TestInterceptor(),
      policy_t::stateful_server,
      policy_t::lossless_client,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      logger().info("-- 0 --");
      logger().info("[Test] setup connection...");
      return test.peer_connect_me();
    }).then([&test] {
      return test.send_bidirectional();
    }).then([&suite] {
      return suite.wait_results(1);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 1);
      results[0].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 1 --");
      logger().info("[Test] client markdown...");
      return test.markdown_peer();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 1);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
    }).then([&suite] {
      logger().info("-- 2 --");
      logger().info("[Test] server markdown...");
      return suite.markdown();
    }).then([&suite] {
      return suite.wait_results(3);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 1);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
      results[2].assert_state_at(conn_state_t::established);
      results[2].assert_connect(0, 0, 0, 0);
      results[2].assert_accept(1, 1, 1, 1);
      results[2].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 3 --");
      logger().info("[Test] client reconnect...");
      return test.peer_connect_me();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_results(3);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 1);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
      results[2].assert_state_at(conn_state_t::established);
      results[2].assert_connect(0, 0, 0, 0);
      results[2].assert_accept(1, 1, 1, 1);
      results[2].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_peer_reuse_connector(FailoverTest& test) {
  return test.run_suite(
      "test_v2_peer_reuse_connector",
      TestInterceptor(),
      policy_t::lossless_peer_reuse,
      policy_t::lossless_peer_reuse,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&suite] {
      logger().info("-- 0 --");
      logger().info("[Test] setup connection...");
      return suite.connect_peer();
    }).then([&test] {
      return test.send_bidirectional();
    }).then([&suite] {
      return suite.wait_results(1);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
    }).then([&suite] {
      logger().info("-- 1 --");
      logger().info("[Test] connector markdown...");
      return suite.markdown();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(1, 1, 0, 1);
      results[1].assert_accept(0, 0, 0, 0);
      results[1].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 2 --");
      logger().info("[Test] acceptor markdown...");
      return test.markdown_peer();
    }).then([&suite] {
      ceph_assert(suite.is_standby());
      logger().info("-- 3 --");
      logger().info("[Test] connector reconnect...");
      return suite.connect_peer();
    }).then([&suite] {
      return suite.try_send_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(2, 2, 1, 2);
      results[1].assert_accept(0, 0, 0, 0);
      results[1].assert_reset(0, 1);
    });
  });
}

seastar::future<>
test_v2_peer_reuse_acceptor(FailoverTest& test) {
  return test.run_suite(
      "test_v2_peer_reuse_acceptor",
      TestInterceptor(),
      policy_t::lossless_peer_reuse,
      policy_t::lossless_peer_reuse,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      logger().info("-- 0 --");
      logger().info("[Test] setup connection...");
      return test.peer_connect_me();
    }).then([&test] {
      return test.send_bidirectional();
    }).then([&suite] {
      return suite.wait_results(1);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 1);
      results[0].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 1 --");
      logger().info("[Test] connector markdown...");
      return test.markdown_peer();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 1);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
    }).then([&suite] {
      logger().info("-- 2 --");
      logger().info("[Test] acceptor markdown...");
      return suite.markdown();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 1);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 3 --");
      logger().info("[Test] connector reconnect...");
      return test.peer_connect_me();
    }).then([&test] {
      return test.try_peer_send_me();
    }).then([&suite] {
      return suite.wait_results(3);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 1);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
      results[2].assert_state_at(conn_state_t::established);
      results[2].assert_connect(0, 0, 0, 0);
      results[2].assert_accept(1, 1, 1, 1);
      results[2].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_lossless_peer_connector(FailoverTest& test) {
  return test.run_suite(
      "test_v2_lossless_peer_connector",
      TestInterceptor(),
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&suite] {
      logger().info("-- 0 --");
      logger().info("[Test] setup connection...");
      return suite.connect_peer();
    }).then([&test] {
      return test.send_bidirectional();
    }).then([&suite] {
      return suite.wait_results(1);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
    }).then([&suite] {
      logger().info("-- 1 --");
      logger().info("[Test] connector markdown...");
      return suite.markdown();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(1, 1, 0, 1);
      results[1].assert_accept(0, 0, 0, 0);
      results[1].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 2 --");
      logger().info("[Test] acceptor markdown...");
      return test.markdown_peer();
    }).then([&suite] {
      ceph_assert(suite.is_standby());
      logger().info("-- 3 --");
      logger().info("[Test] connector reconnect...");
      return suite.connect_peer();
    }).then([&suite] {
      return suite.try_send_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::established);
      results[1].assert_connect(2, 2, 1, 2);
      results[1].assert_accept(0, 0, 0, 0);
      results[1].assert_reset(0, 1);
    });
  });
}

seastar::future<>
test_v2_lossless_peer_acceptor(FailoverTest& test) {
  return test.run_suite(
      "test_v2_lossless_peer_acceptor",
      TestInterceptor(),
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_invoke([&test] {
      logger().info("-- 0 --");
      logger().info("[Test] setup connection...");
      return test.peer_connect_me();
    }).then([&test] {
      return test.send_bidirectional();
    }).then([&suite] {
      return suite.wait_results(1);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 1);
      results[0].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 1 --");
      logger().info("[Test] connector markdown...");
      return test.markdown_peer();
    }).then([&test] {
      return test.peer_connect_me();
    }).then([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
    }).then([&suite] {
      logger().info("-- 2 --");
      logger().info("[Test] acceptor markdown...");
      return suite.markdown();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
    }).then([&test] {
      logger().info("-- 3 --");
      logger().info("[Test] connector reconnect...");
      return test.peer_connect_me();
    }).then([&test] {
      return test.try_peer_send_me();
    }).then([&suite] {
      return suite.wait_results(3);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::closed);
      results[0].assert_connect(0, 0, 0, 0);
      results[0].assert_accept(1, 1, 0, 2);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 1, 0, 0);
      results[1].assert_reset(0, 0);
      results[2].assert_state_at(conn_state_t::established);
      results[2].assert_connect(0, 0, 0, 0);
      results[2].assert_accept(1, 1, 1, 1);
      results[2].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_protocol(entity_addr_t test_addr,
                 entity_addr_t cmd_peer_addr,
                 entity_addr_t test_peer_addr,
                 bool test_peer_islocal,
                 bool peer_wins) {
  ceph_assert_always(test_addr.is_msgr2());
  ceph_assert_always(cmd_peer_addr.is_msgr2());
  ceph_assert_always(test_peer_addr.is_msgr2());

  if (test_peer_islocal) {
    // initiate crimson test peer locally
    logger().info("test_v2_protocol: start local TestPeer at {}...", cmd_peer_addr);
    return FailoverTestPeer::create(cmd_peer_addr, test_peer_addr
    ).then([test_addr, cmd_peer_addr, test_peer_addr, peer_wins](auto peer) {
      return test_v2_protocol(
        test_addr,
        cmd_peer_addr,
        test_peer_addr,
        false,
        peer_wins
      ).then([peer = std::move(peer)] () mutable {
        return peer->wait().then([peer = std::move(peer)] {});
      });
    }).handle_exception([] (auto eptr) {
      logger().error("FailoverTestPeer failed: got exception {}", eptr);
      throw;
    });
  }

  return FailoverTest::create(test_addr, cmd_peer_addr, test_peer_addr
  ).then([peer_wins](auto test) {
    return seastar::futurize_invoke([test] {
      return test_v2_lossy_early_connect_fault(*test);
    }).then([test] {
      return test_v2_lossy_connect_fault(*test);
    }).then([test] {
      return test_v2_lossy_connected_fault(*test);
    }).then([test] {
      return test_v2_lossy_early_accept_fault(*test);
    }).then([test] {
      return test_v2_lossy_accept_fault(*test);
    }).then([test] {
      return test_v2_lossy_establishing_fault(*test);
    }).then([test] {
      return test_v2_lossy_accepted_fault(*test);
    }).then([test] {
      return test_v2_lossless_connect_fault(*test);
    }).then([test] {
      return test_v2_lossless_connected_fault(*test);
    }).then([test] {
      return test_v2_lossless_connected_fault2(*test);
    }).then([test] {
      return test_v2_lossless_reconnect_fault(*test);
    }).then([test] {
      return test_v2_lossless_accept_fault(*test);
    }).then([test] {
      return test_v2_lossless_establishing_fault(*test);
    }).then([test] {
      return test_v2_lossless_accepted_fault(*test);
    }).then([test] {
      return test_v2_lossless_reaccept_fault(*test);
    }).then([test] {
      return test_v2_peer_connect_fault(*test);
    }).then([test] {
      return test_v2_peer_accept_fault(*test);
    }).then([test] {
      return test_v2_peer_establishing_fault(*test);
    }).then([test] {
      return test_v2_peer_connected_fault_reconnect(*test);
    }).then([test] {
      return test_v2_peer_connected_fault_reaccept(*test);
    }).then([test] {
      return check_peer_wins(*test);
    }).then([test, peer_wins](bool ret_peer_wins) {
      ceph_assert(peer_wins == ret_peer_wins);
      if (ret_peer_wins) {
        return seastar::futurize_invoke([test] {
          return test_v2_racing_connect_acceptor_win(*test);
        }).then([test] {
          return test_v2_racing_reconnect_acceptor_win(*test);
        });
      } else {
        return seastar::futurize_invoke([test] {
          return test_v2_racing_connect_acceptor_lose(*test);
        }).then([test] {
          return test_v2_racing_reconnect_acceptor_lose(*test);
        });
      }
    }).then([test] {
      return test_v2_racing_connect_reconnect_win(*test);
    }).then([test] {
      return test_v2_racing_connect_reconnect_lose(*test);
    }).then([test] {
      return test_v2_stale_connect(*test);
    }).then([test] {
      return test_v2_stale_reconnect(*test);
    }).then([test] {
      return test_v2_stale_accept(*test);
    }).then([test] {
      return test_v2_stale_establishing(*test);
    }).then([test] {
      return test_v2_stale_reaccept(*test);
    }).then([test] {
      return test_v2_lossy_client(*test);
    }).then([test] {
      return test_v2_stateless_server(*test);
    }).then([test] {
      return test_v2_lossless_client(*test);
    }).then([test] {
      return test_v2_stateful_server(*test);
    }).then([test] {
      return test_v2_peer_reuse_connector(*test);
    }).then([test] {
      return test_v2_peer_reuse_acceptor(*test);
    }).then([test] {
      return test_v2_lossless_peer_connector(*test);
    }).then([test] {
      return test_v2_lossless_peer_acceptor(*test);
    }).then([test] {
      return test->shutdown().then([test] {});
    });
  }).handle_exception([] (auto eptr) {
    logger().error("FailoverTest failed: got exception {}", eptr);
    throw;
  });
}

}

seastar::future<int> do_test(seastar::app_template& app)
{
  std::vector<const char*> args;
  std::string cluster;
  std::string conf_file_list;
  auto init_params = ceph_argparse_early_args(args,
                                              CEPH_ENTITY_TYPE_CLIENT,
                                              &cluster,
                                              &conf_file_list);
  return crimson::common::sharded_conf().start(
    init_params.name, cluster
  ).then([] {
    return local_conf().start();
  }).then([conf_file_list] {
    return local_conf().parse_config_files(conf_file_list);
  }).then([&app] {
    auto&& config = app.configuration();
    verbose = config["verbose"].as<bool>();
    auto rounds = config["rounds"].as<unsigned>();
    auto keepalive_ratio = config["keepalive-ratio"].as<double>();
    auto testpeer_islocal = config["testpeer-islocal"].as<bool>();

    entity_addr_t test_addr;
    ceph_assert(test_addr.parse(
        config["test-addr"].as<std::string>().c_str(), nullptr));
    test_addr.set_nonce(TEST_NONCE);

    entity_addr_t cmd_peer_addr;
    ceph_assert(cmd_peer_addr.parse(
        config["testpeer-addr"].as<std::string>().c_str(), nullptr));
    cmd_peer_addr.set_nonce(CMD_SRV_NONCE);

    entity_addr_t test_peer_addr = get_test_peer_addr(cmd_peer_addr);
    bool peer_wins = (test_addr > test_peer_addr);

    logger().info("test configuration: verbose={}, rounds={}, keepalive_ratio={}, "
                  "test_addr={}, cmd_peer_addr={}, test_peer_addr={}, "
                  "testpeer_islocal={}, peer_wins={}, smp={}",
                  verbose, rounds, keepalive_ratio,
                  test_addr, cmd_peer_addr, test_peer_addr,
                  testpeer_islocal, peer_wins,
                  seastar::smp::count);
    return test_echo(rounds, keepalive_ratio
    ).then([] {
      return test_preemptive_shutdown();
    }).then([test_addr, cmd_peer_addr, test_peer_addr, testpeer_islocal, peer_wins] {
      return test_v2_protocol(
          test_addr,
          cmd_peer_addr,
          test_peer_addr,
          testpeer_islocal,
          peer_wins);
    }).then([] {
      logger().info("All tests succeeded");
      // Seastar has bugs to have events undispatched during shutdown,
      // which will result in memory leak and thus fail LeakSanitizer.
      return seastar::sleep(100ms);
    });
  }).then([] {
    return crimson::common::sharded_conf().stop();
  }).then([] {
    return 0;
  }).handle_exception([] (auto eptr) {
    logger().error("Test failed: got exception {}", eptr);
    return 1;
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
     "ratio of keepalive in ping messages")
    ("test-addr", bpo::value<std::string>()->default_value("v2:127.0.0.1:9014"),
     "address of v2 failover tests")
    ("testpeer-addr", bpo::value<std::string>()->default_value("v2:127.0.0.1:9012"),
     "addresses of v2 failover testpeer"
     " (This is CmdSrv address, and TestPeer address is at port+=1)")
    ("testpeer-islocal", bpo::value<bool>()->default_value(true),
     "create a local crimson testpeer, or connect to a remote testpeer");
  return app.run(argc, argv, [&app] {
    // This test normally succeeds within 60 seconds, so kill it after 300
    // seconds in case it is blocked forever due to unaddressed bugs.
    return seastar::with_timeout(seastar::lowres_clock::now() + 300s, do_test(app))
      .handle_exception_type([](seastar::timed_out_error&) {
        logger().error("test_messenger timeout after 300s, abort! "
                       "Consider to extend the period if the test is still running.");
        // use the retcode of timeout(1)
        return 124;
      });
  });
}
