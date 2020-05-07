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
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

#include "test_cmds.h"

namespace bpo = boost::program_options;
using crimson::common::local_conf;

namespace {

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_ms);
}

static std::random_device rd;
static std::default_random_engine rng{rd()};
static bool verbose = false;

static seastar::future<> test_echo(unsigned rounds,
                                   double keepalive_ratio,
                                   bool v2)
{
  struct test_state {
    struct Server final
        : public crimson::net::Dispatcher {
      crimson::net::MessengerRef msgr;
      crimson::auth::DummyAuthClientServer dummy_auth;

      seastar::future<> ms_dispatch(crimson::net::Connection* c,
                                    MessageRef m) override {
        if (verbose) {
          logger().info("server got {}", *m);
        }
        // reply with a pong
        return c->send(make_message<MPing>());
      }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        msgr = crimson::net::Messenger::create(name, lname, nonce);
        msgr->set_default_policy(crimson::net::SocketPolicy::stateless_server(0));
        msgr->set_require_authorizer(false);
        msgr->set_auth_client(&dummy_auth);
        msgr->set_auth_server(&dummy_auth);
        return msgr->bind(entity_addrvec_t{addr}).then([this] {
          return msgr->start(this);
        });
      }
      seastar::future<> shutdown() {
        ceph_assert(msgr);
        return msgr->shutdown();
      }
    };

    struct Client final
        : public crimson::net::Dispatcher {
      struct PingSession : public seastar::enable_shared_from_this<PingSession> {
        unsigned count = 0u;
        mono_time connected_time;
        mono_time finish_time;
      };
      using PingSessionRef = seastar::shared_ptr<PingSession>;

      unsigned rounds;
      std::bernoulli_distribution keepalive_dist;
      crimson::net::MessengerRef msgr;
      std::map<crimson::net::Connection*, seastar::promise<>> pending_conns;
      std::map<crimson::net::Connection*, PingSessionRef> sessions;
      crimson::auth::DummyAuthClientServer dummy_auth;

      Client(unsigned rounds, double keepalive_ratio)
        : rounds(rounds),
          keepalive_dist(std::bernoulli_distribution{keepalive_ratio}) {}

      PingSessionRef find_session(crimson::net::Connection* c) {
        auto found = sessions.find(c);
        if (found == sessions.end()) {
          ceph_assert(false);
        }
        return found->second;
      }

      seastar::future<> ms_handle_connect(crimson::net::ConnectionRef conn) override {
        auto session = seastar::make_shared<PingSession>();
        auto [i, added] = sessions.emplace(conn.get(), session);
        std::ignore = i;
        ceph_assert(added);
        session->connected_time = mono_clock::now();
        return seastar::now();
      }
      seastar::future<> ms_dispatch(crimson::net::Connection* c,
                                    MessageRef m) override {
        auto session = find_session(c);
        ++(session->count);
        if (verbose) {
          logger().info("client ms_dispatch {}", session->count);
        }

        if (session->count == rounds) {
          logger().info("{}: finished receiving {} pongs", *c, session->count);
          session->finish_time = mono_clock::now();
          auto found = pending_conns.find(c);
          ceph_assert(found != pending_conns.end());
          found->second.set_value();
        }
        return seastar::now();
      }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        msgr = crimson::net::Messenger::create(name, lname, nonce);
        msgr->set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->set_auth_server(&dummy_auth);
        return msgr->start(this);
      }

      seastar::future<> shutdown() {
        ceph_assert(msgr);
        return msgr->shutdown();
      }

      seastar::future<> dispatch_pingpong(const entity_addr_t& peer_addr) {
        mono_time start_time = mono_clock::now();
        auto conn = msgr->connect(peer_addr, entity_name_t::TYPE_OSD);
        return seastar::futurize_invoke([this, conn] {
          return do_dispatch_pingpong(conn.get());
        }).finally([this, conn, start_time] {
          auto session = find_session(conn.get());
          std::chrono::duration<double> dur_handshake = session->connected_time - start_time;
          std::chrono::duration<double> dur_pingpong = session->finish_time - session->connected_time;
          logger().info("{}: handshake {}, pingpong {}",
                        *conn, dur_handshake.count(), dur_pingpong.count());
        });
      }

     private:
      seastar::future<> do_dispatch_pingpong(crimson::net::Connection* conn) {
        auto [i, added] = pending_conns.emplace(conn, seastar::promise<>());
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
                    return conn->keepalive()
                      .then([&count_keepalive] {
                        count_keepalive += 1;
                        return seastar::make_ready_future<seastar::stop_iteration>(
                          seastar::stop_iteration::no);
                      });
                  } else {
                    return conn->send(make_message<MPing>())
                      .then([&count_ping] {
                        count_ping += 1;
                        return seastar::make_ready_future<seastar::stop_iteration>(
                          seastar::stop_iteration::yes);
                      });
                  }
                });
            }).then([this, conn] {
              auto found = pending_conns.find(conn);
              return found->second.get_future();
            }
          );
        });
      }
    };
  };

  logger().info("test_echo(rounds={}, keepalive_ratio={}, v2={}):",
                rounds, keepalive_ratio, v2);
  auto server1 = seastar::make_shared<test_state::Server>();
  auto server2 = seastar::make_shared<test_state::Server>();
  auto client1 = seastar::make_shared<test_state::Client>(rounds, keepalive_ratio);
  auto client2 = seastar::make_shared<test_state::Client>(rounds, keepalive_ratio);
  // start servers and clients
  entity_addr_t addr1;
  addr1.parse("127.0.0.1:9010", nullptr);
  entity_addr_t addr2;
  addr2.parse("127.0.0.1:9011", nullptr);
  if (v2) {
    addr1.set_type(entity_addr_t::TYPE_MSGR2);
    addr2.set_type(entity_addr_t::TYPE_MSGR2);
  } else {
    addr1.set_type(entity_addr_t::TYPE_LEGACY);
    addr2.set_type(entity_addr_t::TYPE_LEGACY);
  }
  return seastar::when_all_succeed(
      server1->init(entity_name_t::OSD(0), "server1", 1, addr1),
      server2->init(entity_name_t::OSD(1), "server2", 2, addr2),
      client1->init(entity_name_t::OSD(2), "client1", 3),
      client2->init(entity_name_t::OSD(3), "client2", 4)
  // dispatch pingpoing
  ).then([client1, client2, server1, server2] {
    return seastar::when_all_succeed(
        // test connecting in parallel, accepting in parallel
        client1->dispatch_pingpong(server2->msgr->get_myaddr()),
        client2->dispatch_pingpong(server1->msgr->get_myaddr()));
  // shutdown
  }).finally([client1] {
    logger().info("client1 shutdown...");
    return client1->shutdown();
  }).finally([client2] {
    logger().info("client2 shutdown...");
    return client2->shutdown();
  }).finally([server1] {
    logger().info("server1 shutdown...");
    return server1->shutdown();
  }).finally([server2] {
    logger().info("server2 shutdown...");
    return server2->shutdown();
  }).finally([server1, server2, client1, client2] {
    logger().info("test_echo() done!\n");
  });
}

static seastar::future<> test_concurrent_dispatch(bool v2)
{
  struct test_state {
    struct Server final
      : public crimson::net::Dispatcher {
      crimson::net::MessengerRef msgr;
      int count = 0;
      seastar::promise<> on_second; // satisfied on second dispatch
      seastar::promise<> on_done; // satisfied when first dispatch unblocks
      crimson::auth::DummyAuthClientServer dummy_auth;

      seastar::future<> ms_dispatch(crimson::net::Connection* c,
                                    MessageRef m) override {
        switch (++count) {
        case 1:
          // block on the first request until we reenter with the second
          return on_second.get_future().then([this] {
            on_done.set_value();
          });
        case 2:
          on_second.set_value();
          return seastar::now();
        default:
          throw std::runtime_error("unexpected count");
        }
      }

      seastar::future<> wait() { return on_done.get_future(); }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        msgr = crimson::net::Messenger::create(name, lname, nonce);
        msgr->set_default_policy(crimson::net::SocketPolicy::stateless_server(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->set_auth_server(&dummy_auth);
        return msgr->bind(entity_addrvec_t{addr}).then([this] {
          return msgr->start(this);
        });
      }
    };

    struct Client final
      : public crimson::net::Dispatcher {
      crimson::net::MessengerRef msgr;
      crimson::auth::DummyAuthClientServer dummy_auth;

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        msgr = crimson::net::Messenger::create(name, lname, nonce);
        msgr->set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->set_auth_server(&dummy_auth);
        return msgr->start(this);
      }
    };
  };

  logger().info("test_concurrent_dispatch(v2={}):", v2);
  auto server = seastar::make_shared<test_state::Server>();
  auto client = seastar::make_shared<test_state::Client>();
  entity_addr_t addr;
  addr.parse("127.0.0.1:9010", nullptr);
  if (v2) {
    addr.set_type(entity_addr_t::TYPE_MSGR2);
  } else {
    addr.set_type(entity_addr_t::TYPE_LEGACY);
  }
  addr.set_family(AF_INET);
  return seastar::when_all_succeed(
      server->init(entity_name_t::OSD(4), "server3", 5, addr),
      client->init(entity_name_t::OSD(5), "client3", 6)
  ).then([server, client] {
    auto conn = client->msgr->connect(server->msgr->get_myaddr(),
                                      entity_name_t::TYPE_OSD);
    // send two messages
    return conn->send(make_message<MPing>()).then([conn] {
      return conn->send(make_message<MPing>());
    });
  }).then([server] {
    return server->wait();
  }).finally([client] {
    logger().info("client shutdown...");
    return client->msgr->shutdown();
  }).finally([server] {
    logger().info("server shutdown...");
    return server->msgr->shutdown();
  }).finally([server, client] {
    logger().info("test_concurrent_dispatch() done!\n");
  });
}

seastar::future<> test_preemptive_shutdown(bool v2) {
  struct test_state {
    class Server final
      : public crimson::net::Dispatcher {
      crimson::net::MessengerRef msgr;
      crimson::auth::DummyAuthClientServer dummy_auth;

      seastar::future<> ms_dispatch(crimson::net::Connection* c,
                                    MessageRef m) override {
        return c->send(make_message<MPing>());
      }

     public:
      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        msgr = crimson::net::Messenger::create(name, lname, nonce);
        msgr->set_default_policy(crimson::net::SocketPolicy::stateless_server(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->set_auth_server(&dummy_auth);
        return msgr->bind(entity_addrvec_t{addr}).then([this] {
          return msgr->start(this);
        });
      }
      entity_addr_t get_addr() const {
        return msgr->get_myaddr();
      }
      seastar::future<> shutdown() {
        return msgr->shutdown();
      }
    };

    class Client final
      : public crimson::net::Dispatcher {
      crimson::net::MessengerRef msgr;
      crimson::auth::DummyAuthClientServer dummy_auth;

      bool stop_send = false;
      seastar::promise<> stopped_send_promise;

      seastar::future<> ms_dispatch(crimson::net::Connection* c,
                                    MessageRef m) override {
        return seastar::now();
      }

     public:
      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        msgr = crimson::net::Messenger::create(name, lname, nonce);
        msgr->set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->set_auth_server(&dummy_auth);
        return msgr->start(this);
      }
      void send_pings(const entity_addr_t& addr) {
        auto conn = msgr->connect(addr, entity_name_t::TYPE_OSD);
        // forwarded to stopped_send_promise
        (void) seastar::do_until(
          [this] { return stop_send; },
          [this, conn] {
            return conn->send(make_message<MPing>()).then([] {
              return seastar::sleep(0ms);
            });
          }
        ).then_wrapped([this, conn] (auto fut) {
          fut.forward_to(std::move(stopped_send_promise));
        });
      }
      seastar::future<> shutdown() {
        return msgr->shutdown().then([this] {
          stop_send = true;
          return stopped_send_promise.get_future();
        });
      }
    };
  };

  logger().info("test_preemptive_shutdown(v2={}):", v2);
  auto server = seastar::make_shared<test_state::Server>();
  auto client = seastar::make_shared<test_state::Client>();
  entity_addr_t addr;
  addr.parse("127.0.0.1:9010", nullptr);
  if (v2) {
    addr.set_type(entity_addr_t::TYPE_MSGR2);
  } else {
    addr.set_type(entity_addr_t::TYPE_LEGACY);
  }
  addr.set_family(AF_INET);
  return seastar::when_all_succeed(
    server->init(entity_name_t::OSD(6), "server4", 7, addr),
    client->init(entity_name_t::OSD(7), "client4", 8)
  ).then([server, client] {
    client->send_pings(server->get_addr());
    return seastar::sleep(100ms);
  }).then([client] {
    logger().info("client shutdown...");
    return client->shutdown();
  }).finally([server] {
    logger().info("server shutdown...");
    return server->shutdown();
  }).finally([server, client] {
    logger().info("test_preemptive_shutdown() done!\n");
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
using ceph::net::test::cmd_t;
using ceph::net::test::policy_t;

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

  ConnResult(Connection& conn, unsigned index)
    : conn(conn.shared_from_this()), index(index) {}

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
                  this,
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
  std::map<ConnectionRef, unsigned> conns;
  ConnResults results;
  std::optional<seastar::abort_source> signal;

  TestInterceptor() = default;
  // only used for copy breakpoint configurations
  TestInterceptor(const TestInterceptor& other) {
    assert(other.breakpoints_counter.empty());
    assert(other.conns.empty());
    assert(other.results.empty());
    breakpoints = other.breakpoints;
    assert(!other.signal);
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

  ConnResult* find_result(ConnectionRef conn) {
    auto it = conns.find(conn);
    if (it == conns.end()) {
      return nullptr;
    } else {
      return &results[it->second];
    }
  }

  seastar::future<> wait() {
    assert(!signal);
    signal = seastar::abort_source();
    return seastar::sleep_abortable(10s, *signal).then([this] {
      throw std::runtime_error("Timeout (10s) in TestInterceptor::wait()");
    }).handle_exception_type([] (const seastar::sleep_aborted& e) {
      // wait done!
    });
  }

  void notify() {
    if (signal) {
      signal->request_abort();
      signal = std::nullopt;
    }
  }

 private:
  void register_conn(Connection& conn) override {
    unsigned index = results.size();
    results.emplace_back(conn, index);
    conns[conn.shared_from_this()] = index;
    notify();
    logger().info("[{}] {} new connection registered", index, conn);
  }

  void register_conn_closed(Connection& conn) override {
    auto result = find_result(conn.shared_from_this());
    if (result == nullptr) {
      logger().error("Untracked closed connection: {}", conn);
      ceph_abort();
    }

    if (result->state != conn_state_t::replaced) {
      result->state = conn_state_t::closed;
    }
    notify();
    logger().info("[{}] {} closed({})", result->index, conn, result->state);
  }

  void register_conn_ready(Connection& conn) override {
    auto result = find_result(conn.shared_from_this());
    if (result == nullptr) {
      logger().error("Untracked ready connection: {}", conn);
      ceph_abort();
    }

    ceph_assert(conn.is_connected());
    notify();
    logger().info("[{}] {} ready", result->index, conn);
  }

  void register_conn_replaced(Connection& conn) override {
    auto result = find_result(conn.shared_from_this());
    if (result == nullptr) {
      logger().error("Untracked replaced connection: {}", conn);
      ceph_abort();
    }

    result->state = conn_state_t::replaced;
    logger().info("[{}] {} {}", result->index, conn, result->state);
  }

  bp_action_t intercept(Connection& conn, Breakpoint bp) override {
    ++breakpoints_counter[bp].counter;

    auto result = find_result(conn.shared_from_this());
    if (result == nullptr) {
      logger().error("Untracked intercepted connection: {}, at breakpoint {}({})",
                     conn, bp, breakpoints_counter[bp].counter);
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
                      result->index, conn, bp,
                      breakpoints_counter[bp].counter, it_cnt->second);
        return it_cnt->second;
      }
    }
    logger().info("[{}] {} intercepted {}({})",
                  result->index, conn, bp, breakpoints_counter[bp].counter);
    return bp_action_t::CONTINUE;
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
  ConnectionRef tracked_conn;
  unsigned pending_send = 0;
  unsigned pending_peer_receive = 0;
  unsigned pending_receive = 0;

  seastar::future<> ms_dispatch(Connection* c, MessageRef m) override {
    auto result = interceptor.find_result(c->shared_from_this());
    if (result == nullptr) {
      logger().error("Untracked ms dispatched connection: {}", *c);
      ceph_abort();
    }

    if (tracked_conn != c->shared_from_this()) {
      logger().error("[{}] {} got op, but doesn't match tracked_conn [{}] {}",
                     result->index, *c, tracked_index, *tracked_conn);
      ceph_abort();
    }
    ceph_assert(result->index == tracked_index);

    ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
    ceph_assert(pending_receive > 0);
    --pending_receive;
    if (pending_receive == 0) {
      interceptor.notify();
    }
    logger().info("[Test] got op, left {} ops -- [{}] {}",
                  pending_receive, result->index, *c);
    return seastar::now();
  }

  seastar::future<> ms_handle_accept(ConnectionRef conn) override {
    auto result = interceptor.find_result(conn);
    if (result == nullptr) {
      logger().error("Untracked accepted connection: {}", *conn);
      ceph_abort();
    }

    if (tracked_conn &&
        !tracked_conn->is_closed() &&
        tracked_conn != conn) {
      logger().error("[{}] {} got accepted, but there's already traced_conn [{}] {}",
                     result->index, *conn, tracked_index, *tracked_conn);
      ceph_abort();
    }

    tracked_index = result->index;
    tracked_conn = conn;
    ++result->cnt_accept_dispatched;
    logger().info("[Test] got accept (cnt_accept_dispatched={}), track [{}] {}",
                  result->cnt_accept_dispatched, result->index, *conn);
    return flush_pending_send();
  }

  seastar::future<> ms_handle_connect(ConnectionRef conn) override {
    auto result = interceptor.find_result(conn);
    if (result == nullptr) {
      logger().error("Untracked connected connection: {}", *conn);
      ceph_abort();
    }

    if (tracked_conn != conn) {
      logger().error("[{}] {} got connected, but doesn't match tracked_conn [{}] {}",
                     result->index, *conn, tracked_index, *tracked_conn);
      ceph_abort();
    }
    ceph_assert(result->index == tracked_index);

    ++result->cnt_connect_dispatched;
    logger().info("[Test] got connected (cnt_connect_dispatched={}) -- [{}] {}",
                  result->cnt_connect_dispatched, result->index, *conn);
    return seastar::now();
  }

  seastar::future<> ms_handle_reset(ConnectionRef conn, bool is_replace) override {
    auto result = interceptor.find_result(conn);
    if (result == nullptr) {
      logger().error("Untracked reset connection: {}", *conn);
      ceph_abort();
    }

    if (tracked_conn != conn) {
      logger().error("[{}] {} got reset, but doesn't match tracked_conn [{}] {}",
                     result->index, *conn, tracked_index, *tracked_conn);
      ceph_abort();
    }
    ceph_assert(result->index == tracked_index);

    tracked_index = 0;
    tracked_conn = nullptr;
    ++result->cnt_reset_dispatched;
    logger().info("[Test] got reset (cnt_reset_dispatched={}), untrack [{}] {}",
                  result->cnt_reset_dispatched, result->index, *conn);
    return seastar::now();
  }

  seastar::future<> ms_handle_remote_reset(ConnectionRef conn) override {
    auto result = interceptor.find_result(conn);
    if (result == nullptr) {
      logger().error("Untracked remotely reset connection: {}", *conn);
      ceph_abort();
    }

    if (tracked_conn != conn) {
      logger().error("[{}] {} got remotely reset, but doesn't match tracked_conn [{}] {}",
                     result->index, *conn, tracked_index, *tracked_conn);
      ceph_abort();
    }
    ceph_assert(result->index == tracked_index);

    ++result->cnt_remote_reset_dispatched;
    logger().info("[Test] got remote reset (cnt_remote_reset_dispatched={}) -- [{}] {}",
                  result->cnt_remote_reset_dispatched, result->index, *conn);
    return seastar::now();
  }

 private:
  seastar::future<> init(entity_addr_t addr, SocketPolicy policy) {
    test_msgr->set_default_policy(policy);
    test_msgr->set_auth_client(&dummy_auth);
    test_msgr->set_auth_server(&dummy_auth);
    test_msgr->interceptor = &interceptor;
    return test_msgr->bind(entity_addrvec_t{addr}).then([this] {
      return test_msgr->start(this);
    });
  }

  seastar::future<> send_op(bool expect_reply=true) {
    ceph_assert(tracked_conn);
    if (expect_reply) {
      ++pending_peer_receive;
    }
    pg_t pgid;
    object_locator_t oloc;
    hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                   pgid.pool(), oloc.nspace);
    spg_t spgid(pgid);
    return tracked_conn->send(make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0));
  }

  seastar::future<> flush_pending_send() {
    if (pending_send != 0) {
      logger().info("[Test] flush sending {} ops", pending_send);
    }
    ceph_assert(tracked_conn);
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
    unsigned pending_conns = 0;
    unsigned pending_establish = 0;
    unsigned replaced_conns = 0;
    for (auto& result : interceptor.results) {
      if (result.conn->is_closed_clean()) {
        if (result.state == conn_state_t::replaced) {
          ++replaced_conns;
        }
      } else if (result.conn->is_connected()) {
        if (tracked_conn != result.conn || tracked_index != result.index) {
          throw std::runtime_error(fmt::format(
                "The connected connection [{}] {} doesn't"
                " match the tracked connection [{}] {}",
                result.index, *result.conn, tracked_index, tracked_conn));
        }
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
    if (wait_received &&
        (pending_send || pending_peer_receive || pending_receive)) {
      if (pending_conns || pending_establish) {
        logger().info("[Test] wait_ready(): wait for pending_send={},"
                      " pending_peer_receive={}, pending_receive={},"
                      " pending {}/{} ready/establish connections ...",
                      pending_send, pending_peer_receive, pending_receive,
                      pending_conns, pending_establish);
        do_wait = true;
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
                const TestInterceptor& interceptor)
    : test_msgr(test_msgr),
      test_peer_addr(test_peer_addr),
      interceptor(interceptor) { }

  entity_addr_t get_addr() const {
    return test_msgr->get_myaddr();
  }

  seastar::future<> shutdown() {
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
         const TestInterceptor& interceptor) {
    auto suite = std::make_unique<FailoverSuite>(
        Messenger::create(entity_name_t::OSD(2), "Test", 2),
        test_peer_addr, interceptor);
    return suite->init(test_addr, test_policy
    ).then([suite = std::move(suite)] () mutable {
      return std::move(suite);
    });
  }

 // called by tests
 public:
  seastar::future<> connect_peer() {
    logger().info("[Test] connect_peer({})", test_peer_addr);
    auto conn = test_msgr->connect(test_peer_addr, entity_name_t::TYPE_OSD);
    auto result = interceptor.find_result(conn);
    ceph_assert(result != nullptr);

    if (tracked_conn) {
      if (tracked_conn->is_closed()) {
        ceph_assert(tracked_conn != conn);
        logger().info("[Test] this is a new session replacing an closed one");
      } else {
        ceph_assert(tracked_index == result->index);
        ceph_assert(tracked_conn == conn);
        logger().info("[Test] this is not a new session");
      }
    } else {
      logger().info("[Test] this is a new session");
    }
    tracked_index = result->index;
    tracked_conn = conn;

    return flush_pending_send();
  }

  seastar::future<> send_peer() {
    if (tracked_conn) {
      logger().info("[Test] send_peer()");
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
    ceph_assert(tracked_conn);
    return tracked_conn->keepalive();
  }

  seastar::future<> try_send_peer() {
    logger().info("[Test] try_send_peer()");
    ceph_assert(tracked_conn);
    return send_op(false);
  }

  seastar::future<> markdown() {
    logger().info("[Test] markdown()");
    ceph_assert(tracked_conn);
    tracked_conn->mark_down();
    return seastar::now();
  }

  seastar::future<> wait_blocked() {
    logger().info("[Test] wait_blocked() ...");
    return interceptor.blocker.wait_blocked();
  }

  void unblock() {
    logger().info("[Test] unblock()");
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
    ceph_assert(tracked_conn);
    return !(tracked_conn->is_connected() || tracked_conn->is_closed());
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

  seastar::future<> ms_dispatch(Connection* c, MessageRef m) override {
    switch (m->get_type()) {
     case CEPH_MSG_PING:
      ceph_assert(recv_pong);
      recv_pong->set_value();
      recv_pong = std::nullopt;
      return seastar::now();
     case MSG_COMMAND_REPLY:
      ceph_assert(recv_cmdreply);
      recv_cmdreply->set_value();
      recv_cmdreply = std::nullopt;
      return seastar::now();
     case MSG_COMMAND: {
      auto m_cmd = boost::static_pointer_cast<MCommand>(m);
      ceph_assert(static_cast<cmd_t>(m_cmd->cmd[0][0]) == cmd_t::suite_recv_op);
      ceph_assert(test_suite);
      test_suite->notify_peer_reply();
      return seastar::now();
     }
     default:
      logger().error("{} got unexpected msg from cmd server: {}", *c, *m);
      ceph_abort();
    }
  }

 private:
  seastar::future<> prepare_cmd(
      cmd_t cmd,
      std::function<void(ceph::ref_t<MCommand>)>
        f_prepare = [] (auto m) { return; }) {
    assert(!recv_cmdreply);
    recv_cmdreply  = seastar::promise<>();
    auto fut = recv_cmdreply->get_future();
    auto m = make_message<MCommand>();
    m->cmd.emplace_back(1, static_cast<char>(cmd));
    f_prepare(m);
    return cmd_conn->send(m).then([fut = std::move(fut)] () mutable {
      return std::move(fut);
    });
  }

  seastar::future<> start_peer(policy_t peer_policy) {
    return prepare_cmd(cmd_t::suite_start,
        [peer_policy] (auto m) {
      m->cmd.emplace_back(1, static_cast<char>(peer_policy));
    });
  }

  seastar::future<> stop_peer() {
    return prepare_cmd(cmd_t::suite_stop);
  }

  seastar::future<> pingpong() {
    assert(!recv_pong);
    recv_pong = seastar::promise<>();
    auto fut = recv_pong->get_future();
    return cmd_conn->send(make_message<MPing>()
    ).then([this, fut = std::move(fut)] () mutable {
      return std::move(fut);
    });
  }

  seastar::future<> init(entity_addr_t cmd_peer_addr) {
    cmd_msgr->set_default_policy(SocketPolicy::lossy_client(0));
    cmd_msgr->set_auth_client(&dummy_auth);
    cmd_msgr->set_auth_server(&dummy_auth);
    return cmd_msgr->start(this).then([this, cmd_peer_addr] {
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
    auto m = make_message<MCommand>();
    m->cmd.emplace_back(1, static_cast<char>(cmd_t::shutdown));
    return cmd_conn->send(m).then([this] {
      return seastar::sleep(200ms);
    }).finally([this] {
      return cmd_msgr->shutdown();
    });
  }

  static seastar::future<seastar::lw_shared_ptr<FailoverTest>>
  create(entity_addr_t cmd_peer_addr, entity_addr_t test_addr) {
    test_addr.set_nonce(2);
    cmd_peer_addr.set_nonce(3);
    entity_addr_t test_peer_addr = cmd_peer_addr;
    test_peer_addr.set_port(cmd_peer_addr.get_port() + 1);
    test_peer_addr.set_nonce(4);
    auto test = seastar::make_lw_shared<FailoverTest>(
        Messenger::create(entity_name_t::OSD(1), "CmdCli", 1),
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
    return FailoverSuite::create(
        test_addr, test_policy_, test_peer_addr, interceptor
    ).then([this, peer_policy, f = std::move(f)] (auto suite) mutable {
      ceph_assert(suite->get_addr() == test_addr);
      test_suite.swap(suite);
      return start_peer(peer_policy).then([this, f = std::move(f)] {
        return f(*test_suite);
      }).then([this] {
        test_suite->post_check();
        logger().info("\n[SUCCESS]");
      }).handle_exception([this] (auto eptr) {
        logger().info("\n[FAIL: {}]", eptr);
        test_suite->dump_results();
        throw;
      }).finally([this] {
        return stop_peer();
      }).finally([this] {
        return test_suite->shutdown().then([this] {
          test_suite.reset();
        });
      });
    });
  }

  seastar::future<> peer_connect_me() {
    logger().info("[Test] peer_connect_me({})", test_addr);
    return prepare_cmd(cmd_t::suite_connect_me,
        [this] (auto m) {
      m->cmd.emplace_back(fmt::format("{}", test_addr));
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
    logger().info("[Test] markdown_peer()");
    return prepare_cmd(cmd_t::suite_markdown).then([] {
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

  seastar::future<> ms_dispatch(Connection* c, MessageRef m) override {
    logger().info("[TestPeer] got op from Test");
    ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
    ceph_assert(tracked_conn == c->shared_from_this());
    return op_callback();
  }

  seastar::future<> ms_handle_accept(ConnectionRef conn) override {
    logger().info("[TestPeer] got accept from Test");
    ceph_assert(!tracked_conn ||
                tracked_conn->is_closed() ||
                tracked_conn == conn);
    tracked_conn = conn;
    return flush_pending_send();
  }

  seastar::future<> ms_handle_reset(ConnectionRef conn, bool is_replace) override {
    logger().info("[TestPeer] got reset from Test");
    ceph_assert(tracked_conn == conn);
    tracked_conn = nullptr;
    return seastar::now();
  }

 private:
  seastar::future<> init(entity_addr_t addr, SocketPolicy policy) {
    peer_msgr->set_default_policy(policy);
    peer_msgr->set_auth_client(&dummy_auth);
    peer_msgr->set_auth_server(&dummy_auth);
    return peer_msgr->bind(entity_addrvec_t{addr}).then([this] {
      return peer_msgr->start(this);
    });
  }

  seastar::future<> send_op() {
    ceph_assert(tracked_conn);
    pg_t pgid;
    object_locator_t oloc;
    hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                   pgid.pool(), oloc.nspace);
    spg_t spgid(pgid);
    return tracked_conn->send(make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0));
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
    : peer_msgr(peer_msgr), op_callback(op_callback) { }

  seastar::future<> shutdown() {
    return peer_msgr->shutdown();
  }

  seastar::future<> connect_peer(entity_addr_t addr) {
    logger().info("[TestPeer] connect_peer({})", addr);
    auto new_tracked_conn = peer_msgr->connect(addr, entity_name_t::TYPE_OSD);
    if (tracked_conn) {
      if (tracked_conn->is_closed()) {
        ceph_assert(tracked_conn != new_tracked_conn);
        logger().info("[TestPeer] this is a new session"
                      " replacing an closed one");
      } else {
        ceph_assert(tracked_conn == new_tracked_conn);
        logger().info("[TestPeer] this is not a new session");
      }
    } else {
      logger().info("[TestPeer] this is a new session");
    }
    tracked_conn = new_tracked_conn;
    return flush_pending_send();
  }

  seastar::future<> send_peer() {
    if (tracked_conn) {
      logger().info("[TestPeer] send_peer()");
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
    return tracked_conn->keepalive();
  }

  seastar::future<> markdown() {
    logger().info("[TestPeer] markdown()");
    ceph_assert(tracked_conn);
    tracked_conn->mark_down();
    return seastar::now();
  }

  static seastar::future<std::unique_ptr<FailoverSuitePeer>>
  create(entity_addr_t addr, const SocketPolicy& policy, cb_t op_callback) {
    auto suite = std::make_unique<FailoverSuitePeer>(
        Messenger::create(entity_name_t::OSD(4), "TestPeer", 4), op_callback);
    return suite->init(addr, policy
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

  seastar::future<> ms_dispatch(Connection* c, MessageRef m) override {
    ceph_assert(cmd_conn == c->shared_from_this());
    switch (m->get_type()) {
     case CEPH_MSG_PING:
      return c->send(make_message<MPing>());
     case MSG_COMMAND: {
      auto m_cmd = boost::static_pointer_cast<MCommand>(m);
      auto cmd = static_cast<cmd_t>(m_cmd->cmd[0][0]);
      if (cmd == cmd_t::shutdown) {
        logger().info("CmdSrv shutdown...");
        // forwarded to FailoverTestPeer::wait()
        (void) cmd_msgr->shutdown();
        return seastar::now();
      }
      return handle_cmd(cmd, m_cmd).then([c] {
        return c->send(make_message<MCommandReply>());
      });
     }
     default:
      logger().error("{} got unexpected msg from cmd client: {}", *c, m);
      ceph_abort();
    }
  }

  seastar::future<> ms_handle_accept(ConnectionRef conn) override {
    cmd_conn = conn;
    return seastar::now();
  }

 private:
  seastar::future<> notify_recv_op() {
    ceph_assert(cmd_conn);
    auto m = make_message<MCommand>();
    m->cmd.emplace_back(1, static_cast<char>(cmd_t::suite_recv_op));
    return cmd_conn->send(m);
  }

  seastar::future<> handle_cmd(cmd_t cmd, MRef<MCommand> m_cmd) {
    switch (cmd) {
     case cmd_t::suite_start: {
      ceph_assert(!test_suite);
      auto policy = to_socket_policy(static_cast<policy_t>(m_cmd->cmd[1][0]));
      return FailoverSuitePeer::create(test_peer_addr, policy,
                                       [this] { return notify_recv_op(); }
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
      entity_addr_t test_addr = entity_addr_t();
      test_addr.parse(m_cmd->cmd[1].c_str(), nullptr);
      return test_suite->connect_peer(test_addr);
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
      logger().error("TestPeer got unexpected command {} from Test", m_cmd);
      ceph_abort();
      return seastar::now();
    }
  }

  seastar::future<> init(entity_addr_t cmd_peer_addr) {
    cmd_msgr->set_default_policy(SocketPolicy::stateless_server(0));
    cmd_msgr->set_auth_client(&dummy_auth);
    cmd_msgr->set_auth_server(&dummy_auth);
    return cmd_msgr->bind(entity_addrvec_t{cmd_peer_addr}).then([this] {
      return cmd_msgr->start(this);
    });
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
  create(entity_addr_t cmd_peer_addr) {
    // suite bind to cmd_peer_addr, with port + 1
    entity_addr_t test_peer_addr = cmd_peer_addr;
    test_peer_addr.set_port(cmd_peer_addr.get_port() + 1);
    auto test_peer = std::make_unique<FailoverTestPeer>(
        Messenger::create(entity_name_t::OSD(3), "CmdSrv", 3), test_peer_addr);
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
      {custom_bp_t::BANNER_WRITE},
      {custom_bp_t::BANNER_READ},
      {custom_bp_t::BANNER_PAYLOAD_READ},
      {custom_bp_t::SOCKET_CONNECTING},
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
          [&test] (FailoverSuite& suite) {
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
          [&test] (FailoverSuite& suite) {
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
          [&test] (FailoverSuite& suite) {
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
      [&test] (FailoverSuite& suite) {
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
peer_wins(FailoverTest& test) {
  return seastar::do_with(bool(), [&test] (auto& ret) {
    return test.run_suite("peer_wins",
                          TestInterceptor(),
                          policy_t::lossy_client,
                          policy_t::stateless_server,
                          [&test, &ret] (FailoverSuite& suite) {
      return suite.connect_peer().then([&suite] {
        return suite.wait_results(1);
      }).then([&ret] (ConnResults& results) {
        results[0].assert_state_at(conn_state_t::established);
        ret = results[0].conn->peer_wins();
        logger().info("peer_wins: {}", ret);
      });
    }).then([&ret] {
      return ret;
    });
  });
}

seastar::future<>
test_v2_racing_reconnect_win(FailoverTest& test) {
  return seastar::do_with(std::vector<std::pair<unsigned, Breakpoint>>{
      {2, {custom_bp_t::BANNER_WRITE}},
      {2, {custom_bp_t::BANNER_READ}},
      {2, {custom_bp_t::BANNER_PAYLOAD_READ}},
      {2, {Tag::HELLO, bp_type_t::WRITE}},
      {2, {Tag::HELLO, bp_type_t::READ}},
      {2, {Tag::AUTH_REQUEST, bp_type_t::READ}},
      {2, {Tag::AUTH_DONE, bp_type_t::WRITE}},
      {2, {Tag::AUTH_SIGNATURE, bp_type_t::WRITE}},
      {2, {Tag::AUTH_SIGNATURE, bp_type_t::READ}},
      {1, {Tag::SESSION_RECONNECT, bp_type_t::READ}},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault({Tag::MESSAGE, bp_type_t::READ});
      interceptor.make_block(bp.second, bp.first);
      return test.run_suite(
          fmt::format("test_v2_racing_reconnect_win -- {}({})",
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
test_v2_racing_reconnect_lose(FailoverTest& test) {
  return seastar::do_with(std::vector<std::pair<unsigned, Breakpoint>>{
      {2, {custom_bp_t::BANNER_WRITE}},
      {2, {custom_bp_t::BANNER_READ}},
      {2, {custom_bp_t::BANNER_PAYLOAD_READ}},
      {2, {Tag::HELLO, bp_type_t::WRITE}},
      {2, {Tag::HELLO, bp_type_t::READ}},
      {2, {Tag::AUTH_REQUEST, bp_type_t::WRITE}},
      {2, {Tag::AUTH_DONE, bp_type_t::READ}},
      {2, {Tag::AUTH_SIGNATURE, bp_type_t::WRITE}},
      {2, {Tag::AUTH_SIGNATURE, bp_type_t::READ}},
      {1, {Tag::SESSION_RECONNECT, bp_type_t::WRITE}},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault({Tag::MESSAGE, bp_type_t::WRITE});
      interceptor.make_block(bp.second, bp.first);
      return test.run_suite(
          fmt::format("test_v2_racing_reconnect_lose -- {}({})",
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
test_v2_racing_connect_win(FailoverTest& test) {
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
      interceptor.make_block(bp);
      return test.run_suite(
          fmt::format("test_v2_racing_connect_win -- {}", bp),
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
test_v2_racing_connect_lose(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
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
      interceptor.make_block(bp);
      return test.run_suite(
          fmt::format("test_v2_racing_connect_lose -- {}", bp),
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
    }).then([&suite] {
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
    }).then([] {
      return seastar::sleep(100ms);
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
    }).then([] {
      logger().info("-- 2 --");
      logger().info("[Test] acceptor markdown...");
      return seastar::sleep(100ms);
    }).then([&suite] {
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
    }).then([] {
      return seastar::sleep(100ms);
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
    }).then([] {
      logger().info("-- 2 --");
      logger().info("[Test] acceptor markdown...");
      return seastar::sleep(100ms);
    }).then([&suite] {
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
                 entity_addr_t test_peer_addr,
                 bool test_peer_islocal) {
  ceph_assert(test_addr.is_msgr2());
  ceph_assert(test_peer_addr.is_msgr2());

  if (test_peer_islocal) {
    // initiate crimson test peer locally
    logger().info("test_v2_protocol: start local TestPeer at {}...", test_peer_addr);
    return FailoverTestPeer::create(test_peer_addr
    ).then([test_addr, test_peer_addr] (auto peer) {
      return test_v2_protocol(test_addr, test_peer_addr, false
      ).finally([peer = std::move(peer)] () mutable {
        return peer->wait().then([peer = std::move(peer)] {});
      });
    }).handle_exception([] (auto eptr) {
      logger().error("FailoverTestPeer: got exception {}", eptr);
      throw;
    });
  }

  return FailoverTest::create(test_peer_addr, test_addr).then([] (auto test) {
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
      return peer_wins(*test);
    }).then([test] (bool peer_wins) {
      if (peer_wins) {
        return seastar::futurize_invoke([test] {
          return test_v2_racing_connect_lose(*test);
        }).then([test] {
          return test_v2_racing_reconnect_lose(*test);
        });
      } else {
        return seastar::futurize_invoke([test] {
          return test_v2_racing_connect_win(*test);
        }).then([test] {
          return test_v2_racing_reconnect_win(*test);
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
    }).finally([test] {
      return test->shutdown().then([test] {});
    });
  }).handle_exception([] (auto eptr) {
    logger().error("FailoverTest: got exception {}", eptr);
    throw;
  });
}

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
    ("v2-test-addr", bpo::value<std::string>()->default_value("v2:127.0.0.1:9012"),
     "address of v2 failover tests")
    ("v2-testpeer-addr", bpo::value<std::string>()->default_value("v2:127.0.0.1:9013"),
     "addresses of v2 failover testpeer"
     " (CmdSrv address and TestPeer address with port+=1)")
    ("v2-testpeer-islocal", bpo::value<bool>()->default_value(true),
     "create a local crimson testpeer, or connect to a remote testpeer");
  return app.run(argc, argv, [&app] {
    std::vector<const char*> args;
    std::string cluster;
    std::string conf_file_list;
    auto init_params = ceph_argparse_early_args(args,
                                                CEPH_ENTITY_TYPE_CLIENT,
                                                &cluster,
                                                &conf_file_list);
    return crimson::common::sharded_conf().start(init_params.name, cluster)
    .then([conf_file_list] {
      return local_conf().parse_config_files(conf_file_list);
    }).then([&app] {
      auto&& config = app.configuration();
      verbose = config["verbose"].as<bool>();
      auto rounds = config["rounds"].as<unsigned>();
      auto keepalive_ratio = config["keepalive-ratio"].as<double>();
      entity_addr_t v2_test_addr;
      ceph_assert(v2_test_addr.parse(
          config["v2-test-addr"].as<std::string>().c_str(), nullptr));
      entity_addr_t v2_testpeer_addr;
      ceph_assert(v2_testpeer_addr.parse(
          config["v2-testpeer-addr"].as<std::string>().c_str(), nullptr));
      auto v2_testpeer_islocal = config["v2-testpeer-islocal"].as<bool>();
      return test_echo(rounds, keepalive_ratio, false)
      .then([rounds, keepalive_ratio] {
        return test_echo(rounds, keepalive_ratio, true);
      }).then([] {
        return test_concurrent_dispatch(false);
      }).then([] {
        return test_concurrent_dispatch(true);
      }).then([] {
        return test_preemptive_shutdown(false);
      }).then([] {
        return test_preemptive_shutdown(true);
      }).then([v2_test_addr, v2_testpeer_addr, v2_testpeer_islocal] {
        return test_v2_protocol(v2_test_addr, v2_testpeer_addr, v2_testpeer_islocal);
      }).then([] {
        std::cout << "All tests succeeded" << std::endl;
        // Seastar has bugs to have events undispatched during shutdown,
        // which will result in memory leak and thus fail LeakSanitizer.
        return seastar::sleep(100ms);
      });
    }).then([] {
      return crimson::common::sharded_conf().stop();
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });
}
