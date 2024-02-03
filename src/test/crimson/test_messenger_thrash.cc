// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <map>
#include <random>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>

#include "common/ceph_argparse.h"
#include "messages/MPing.h"
#include "messages/MCommand.h"
#include "crimson/auth/DummyAuth.h"
#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Messenger.h"

using namespace std::chrono_literals;
namespace bpo = boost::program_options;
using crimson::common::local_conf;
using payload_seq_t = uint64_t;

struct Payload {
  enum Who : uint8_t {
    PING = 0,
    PONG = 1,
  };
  uint8_t who = 0;
  payload_seq_t seq = 0;
  bufferlist data;

  Payload(Who who, uint64_t seq, const bufferlist& data)
    : who(who), seq(seq), data(data)
  {}
  Payload() = default;
  DENC(Payload, v, p) {
    DENC_START(1, 1, p);
    denc(v.who, p);
    denc(v.seq, p);
    denc(v.data, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(Payload)

template<>
struct fmt::formatter<Payload> : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const Payload& pl, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "reply={} i={}", pl.who, pl.seq);
  }
};

namespace {

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_test);
}

std::random_device rd;
std::default_random_engine rng{rd()};
std::uniform_int_distribution<> prob(0,99);
bool verbose = false;

entity_addr_t get_server_addr() {
  static int port = 16800;
  ++port;
  entity_addr_t saddr;
  saddr.parse("127.0.0.1", nullptr);
  saddr.set_port(port);
  return saddr;
}

uint64_t get_nonce() {
  static uint64_t nonce = 1;
  ++nonce;
  return nonce;
}

struct thrash_params_t {
  std::size_t servers;
  std::size_t clients;
  std::size_t connections;
  std::size_t random_op;
};

class SyntheticWorkload;

class SyntheticDispatcher final
    : public crimson::net::Dispatcher {
  public:
  std::map<crimson::net::Connection*, std::deque<payload_seq_t> > conn_sent;
  std::map<payload_seq_t, bufferlist> sent;
  unsigned index;
  SyntheticWorkload *workload;

  SyntheticDispatcher(bool s, SyntheticWorkload *wl):
    index(0), workload(wl) {
  }

  std::optional<seastar::future<>> ms_dispatch(crimson::net::ConnectionRef con,
                                               MessageRef m) final {
    if (verbose) {
      logger().warn("{}: con = {}", __func__, *con);
    }
    // MSG_COMMAND is used to disorganize regular message flow
    if (m->get_type() == MSG_COMMAND) {
      return seastar::now();
    }

    Payload pl;
    auto p = m->get_data().cbegin();
    decode(pl, p);
    if (pl.who == Payload::PING) {
      logger().info(" {} conn= {} {}", __func__, *con, pl);
      return reply_message(m, con, pl);
    } else {
      ceph_assert(pl.who == Payload::PONG);
      if (sent.count(pl.seq)) {
        logger().info(" {} conn= {} {}", __func__, *con, pl);
        ceph_assert(conn_sent[&*con].front() == pl.seq);
        ceph_assert(pl.data.contents_equal(sent[pl.seq]));
        conn_sent[&*con].pop_front();
        sent.erase(pl.seq);
      }

      return seastar::now();
    }
  }

  void ms_handle_accept(
      crimson::net::ConnectionRef conn,
      seastar::shard_id prv_shard,
      bool is_replace) final {
    logger().info("{} - Connection:{}", __func__, *conn);
    assert(prv_shard == seastar::this_shard_id());
  }

  void ms_handle_connect(
      crimson::net::ConnectionRef conn,
      seastar::shard_id prv_shard) final {
    logger().info("{} - Connection:{}", __func__, *conn);
    assert(prv_shard == seastar::this_shard_id());
  }

  void ms_handle_reset(crimson::net::ConnectionRef con, bool is_replace) final;

  void ms_handle_remote_reset(crimson::net::ConnectionRef con) final {
    clear_pending(con);
  }

  std::optional<seastar::future<>> reply_message(
      const MessageRef m,
      crimson::net::ConnectionRef con,
      Payload& pl) {
    pl.who = Payload::PONG;
    bufferlist bl;
    encode(pl, bl);
    auto rm = crimson::make_message<MPing>();
    rm->set_data(bl);
    if (verbose) {
      logger().info("{} conn= {} reply i= {}",
        __func__, *con, pl.seq);
    }
    return con->send(std::move(rm));
  }

  seastar::future<> send_message_wrap(crimson::net::ConnectionRef con,
                                      const bufferlist& data) {
    auto m = crimson::make_message<MPing>();
    Payload pl{Payload::PING, index++, data};
    bufferlist bl;
    encode(pl, bl);
    m->set_data(bl);
    sent[pl.seq] = pl.data;
    conn_sent[&*con].push_back(pl.seq);
    logger().info("{} conn= {} send i= {}",
      __func__, *con, pl.seq);

    return con->send(std::move(m));
  }

  uint64_t get_num_pending_msgs() {
    return sent.size();
  }

  void clear_pending(crimson::net::ConnectionRef con) {
    for (std::deque<uint64_t>::iterator it = conn_sent[&*con].begin();
         it != conn_sent[&*con].end(); ++it)
      sent.erase(*it);
    conn_sent.erase(&*con);
  }

  void print() {
    for (auto && [connptr, list] : conn_sent) {
      if (!list.empty()) {
        logger().info("{} {} wait {}", __func__,
                      (void*)connptr, list.size());
      }
    }
  }
};

class SyntheticWorkload {
  // messengers must be freed after its connections
  std::set<crimson::net::MessengerRef> available_servers;
  std::set<crimson::net::MessengerRef> available_clients;

  crimson::net::SocketPolicy server_policy;
  crimson::net::SocketPolicy client_policy;
  std::map<crimson::net::ConnectionRef,
    std::pair<crimson::net::MessengerRef,
    crimson::net::MessengerRef>> available_connections;
  SyntheticDispatcher dispatcher;
  std::vector<bufferlist> rand_data;
  crimson::auth::DummyAuthClientServer dummy_auth;

  seastar::future<crimson::net::ConnectionRef> get_random_connection() {
    return seastar::do_until(
      [this] { return dispatcher.get_num_pending_msgs() <= max_in_flight; },
      [] { return seastar::sleep(100ms); }
    ).then([this] {
      boost::uniform_int<> choose(0, available_connections.size() - 1);
      int index = choose(rng);
      std::map<crimson::net::ConnectionRef,
        std::pair<crimson::net::MessengerRef, crimson::net::MessengerRef>>::iterator i
        = available_connections.begin();
      for (; index > 0; --index, ++i) ;
      return seastar::make_ready_future<crimson::net::ConnectionRef>(i->first);
   });
  }

 public:
   const unsigned min_connections = 10;
   const unsigned max_in_flight = 64;
   const unsigned max_connections = 128;
   const unsigned max_message_len = 1024 * 1024 * 4;
   const uint64_t  servers, clients;

   SyntheticWorkload(int servers, int clients, int random_num,
                     crimson::net::SocketPolicy srv_policy,
                     crimson::net::SocketPolicy cli_policy)
     : server_policy(srv_policy),
       client_policy(cli_policy),
       dispatcher(false, this),
       servers(servers),
       clients(clients) {

     for (int i = 0; i < random_num; i++) {
       bufferlist bl;
       boost::uniform_int<> u(32, max_message_len);
       uint64_t value_len = u(rng);
       bufferptr bp(value_len);
       bp.zero();
       for (uint64_t j = 0; j < value_len-sizeof(i); ) {
         memcpy(bp.c_str()+j, &i, sizeof(i));
         j += 4096;
       }

       bl.append(bp);
       rand_data.push_back(bl);
     }
   }


   bool can_create_connection() {
     return available_connections.size() < max_connections;
   }

   seastar::future<> maybe_generate_connection() {
     if (!can_create_connection()) {
       return seastar::now();
     }
     crimson::net::MessengerRef server, client;
     {
       boost::uniform_int<> choose(0, available_servers.size() - 1);
       int index = choose(rng);
       std::set<crimson::net::MessengerRef>::iterator i
         = available_servers.begin();
       for (; index > 0; --index, ++i) ;
       server = *i;
     }
     {
       boost::uniform_int<> choose(0, available_clients.size() - 1);
       int index = choose(rng);
       std::set<crimson::net::MessengerRef>::iterator i
         = available_clients.begin();
       for (; index > 0; --index, ++i) ;
       client = *i;
     }


     std::pair<crimson::net::MessengerRef, crimson::net::MessengerRef>
       connected_pair;
     {
       crimson::net::ConnectionRef conn = client->connect(
                                            server->get_myaddr(),
                                            entity_name_t::TYPE_OSD);
       connected_pair = std::make_pair(client, server);
       available_connections[conn] = connected_pair;
     }
     return seastar::now();
   }

   seastar::future<> random_op (const uint64_t& iter) {
     return seastar::do_with(iter, [this] (uint64_t& iter) {
       return seastar::do_until(
         [&] { return iter == 0; },
         [&, this]
       {
         if (!(iter % 10)) {
           logger().info("{} Op {} : ", __func__ ,iter);
           print_internal_state();
         }
         --iter;
         int val = prob(rng);
         if(val > 90) {
           return maybe_generate_connection();
         } else if (val > 80) {
           return drop_connection();
         } else if (val > 10) {
           return send_message();
         } else {
           return seastar::sleep(
             std::chrono::milliseconds(rand() % 1000 + 500));
         }
       });
     });
   }

   seastar::future<> generate_connections (const uint64_t& iter) {
     return seastar::do_with(iter, [this] (uint64_t& iter) {
       return seastar::do_until(
         [&] { return iter == 0; },
         [&, this]
       {
         --iter;
         if (!(connections_count() % 10)) {
          logger().info("seeding connection {}",
                        connections_count());
         }
         return maybe_generate_connection();
       });
     });
   }

   seastar::future<> init_server(const entity_name_t& name,
                          const std::string& lname,
                          const uint64_t nonce,
                          const entity_addr_t& addr) {
     crimson::net::MessengerRef msgr =
       crimson::net::Messenger::create(
           name, lname, nonce, true);
     msgr->set_default_policy(server_policy);
     msgr->set_auth_client(&dummy_auth);
     msgr->set_auth_server(&dummy_auth);
     available_servers.insert(msgr);
     return msgr->bind(entity_addrvec_t{addr}).safe_then(
         [this, msgr] {
       return msgr->start({&dispatcher});
     }, crimson::net::Messenger::bind_ertr::all_same_way(
         [addr] (const std::error_code& e) {
       logger().error("{} test_messenger_thrash(): "
                      "there is another instance running at {}",
                       __func__, addr);
       ceph_abort();
     }));
   }

   seastar::future<> init_client(const entity_name_t& name,
                          const std::string& lname,
                          const uint64_t nonce) {
     crimson::net::MessengerRef msgr =
       crimson::net::Messenger::create(
           name, lname, nonce, true);
     msgr->set_default_policy(client_policy);
     msgr->set_auth_client(&dummy_auth);
     msgr->set_auth_server(&dummy_auth);
     available_clients.insert(msgr);
     return msgr->start({&dispatcher});
   }

   seastar::future<> send_message() {
     return get_random_connection()
     .then([this] (crimson::net::ConnectionRef conn) {
       boost::uniform_int<> true_false(0, 99);
       int val = true_false(rng);
       if (val >= 95) {
         uuid_d uuid;
         uuid.generate_random();
         auto m = crimson::make_message<MCommand>(uuid);
         std::vector<std::string> cmds;
         cmds.push_back("command");
         m->cmd = cmds;
         m->set_priority(200);
         return conn->send(std::move(m));
       } else {
         boost::uniform_int<> u(0, rand_data.size()-1);
         return dispatcher.send_message_wrap(conn, rand_data[u(rng)]);
       }
     });
   }

   seastar::future<> drop_connection() {
     if (available_connections.size() < min_connections) {
       return seastar::now();
     }

     return get_random_connection()
     .then([this] (crimson::net::ConnectionRef conn) {
       dispatcher.clear_pending(conn);
       conn->mark_down();
       if (!client_policy.server &&
           client_policy.standby) {
         // it's a lossless policy, so we need to mark down each side
         std::pair<crimson::net::MessengerRef, crimson::net::MessengerRef> &p =
           available_connections[conn];
         if (!p.first->get_default_policy().server &&
             !p.second->get_default_policy().server) {
             //verify that equal-to operator applies here
           ceph_assert(p.first->owns_connection(*conn));
           crimson::net::ConnectionRef peer = p.second->connect(
             p.first->get_myaddr(), p.first->get_mytype());
           peer->mark_down();
           dispatcher.clear_pending(peer);
           available_connections.erase(peer);
         }
       }
       ceph_assert(available_connections.erase(conn) == 1U);
       return seastar::now();
     });
   }

   void print_internal_state(bool detail=false) {
     logger().info("available_connections: {} inflight messages: {}",
       available_connections.size(),
       dispatcher.get_num_pending_msgs());
     if (detail && !available_connections.empty()) {
       dispatcher.print();
     }
   }

   seastar::future<> wait_for_done() {
     int i = 0;
     return seastar::do_until(
       [this] { return !dispatcher.get_num_pending_msgs(); },
       [this, &i]
     {
       if (i++ % 50 == 0){
         print_internal_state(true);
       }
       return seastar::sleep(100ms);
     }).then([this] {
       return seastar::do_for_each(available_servers, [] (auto server) {
	 if (verbose) {
           logger().info("server {} shutdown" , server->get_myaddrs());
	 }
         server->stop();
         return server->shutdown();
       });
     }).then([this] {
       return seastar::do_for_each(available_clients, [] (auto client) {
	 if (verbose) {
           logger().info("client {} shutdown" , client->get_myaddrs());
	 }
         client->stop();
         return client->shutdown();
       });
     });
   }

   void handle_reset(crimson::net::ConnectionRef con) {
     available_connections.erase(con);
   }

   uint64_t servers_count() {
     return available_servers.size();
   }

   uint64_t clients_count() {
     return available_clients.size();
   }

   uint64_t connections_count() {
     return available_connections.size();
   }
};

void SyntheticDispatcher::ms_handle_reset(crimson::net::ConnectionRef con,
                                          bool is_replace) {
  workload->handle_reset(con);
  clear_pending(con);
}

seastar::future<> reset_conf() {
  return seastar::when_all_succeed(
    local_conf().set_val("ms_inject_socket_failures", "0"),
    local_conf().set_val("ms_inject_internal_delays", "0"),
    local_conf().set_val("ms_inject_delay_probability", "0"),
    local_conf().set_val("ms_inject_delay_max", "0")
  ).then_unpack([] {
    return seastar::now();
  });
}

// Testing Crimson messenger (with msgr-v2 protocol) robustness against
// network delays and failures. The test includes stress tests and
// socket level delays/failures injection tests, letting time
// and randomness achieve the best test coverage.

// Test Parameters:
// Clients: 8             (stateful)
// Servers: 32            (lossless)
// Connections: 100       (Generated between random clients/server)
// Random Operations: 120 (Generate/Drop Connection, Send Message, Sleep)
seastar::future<> test_stress(thrash_params_t tp)
{

  logger().info("test_stress():");

  SyntheticWorkload test_msg(tp.servers, tp.clients, 100,
                             crimson::net::SocketPolicy::stateful_server(0),
                             crimson::net::SocketPolicy::lossless_client(0));

  return seastar::do_with(test_msg, [tp]
    (SyntheticWorkload& test_msg) {
      return seastar::do_until([&test_msg] {
        return test_msg.servers_count() == test_msg.servers; },
      [&test_msg] {
        entity_addr_t bind_addr = get_server_addr();
        bind_addr.set_type(entity_addr_t::TYPE_MSGR2);
	uint64_t server_num = get_nonce();
        return test_msg.init_server(entity_name_t::OSD(server_num),
                             "server", server_num , bind_addr);
      }).then([&test_msg] {
      return seastar::do_until([&test_msg] {
        return test_msg.clients_count() == test_msg.clients; },
      [&test_msg] {
        return test_msg.init_client(entity_name_t::CLIENT(-1),
                             "client", get_nonce());
	});
      }).then([&test_msg, tp] {
        return test_msg.generate_connections(tp.connections);
      }).then([&test_msg, tp] {
        return test_msg.random_op(tp.random_op);
      }).then([&test_msg] {
	return test_msg.wait_for_done();
      }).then([] {
        logger().info("test_stress() DONE");
      }).handle_exception([] (auto eptr) {
        logger().error(
          "test_stress() failed: got exception {}",
          eptr);
        throw;
      });
    });
}

// Test Parameters:
// Clients: 8              (statefull)
// Servers: 32             (loseless)
// Connections: 100        (Generated between random clients/server)
// Random Operations: 120 (Generate/Drop Connection, Send Message, Sleep)
seastar::future<> test_injection(thrash_params_t tp)
{

  logger().info("test_injection():");

  SyntheticWorkload test_msg(tp.servers, tp.clients, 100,
                             crimson::net::SocketPolicy::stateful_server(0),
                             crimson::net::SocketPolicy::lossless_client(0));

  return seastar::do_with(test_msg, [tp]
    (SyntheticWorkload& test_msg) {
      return seastar::do_until([&test_msg] {
        return test_msg.servers_count() == test_msg.servers; },
      [&test_msg] {
        entity_addr_t bind_addr = get_server_addr();
        bind_addr.set_type(entity_addr_t::TYPE_MSGR2);
	uint64_t server_num = get_nonce();
        return test_msg.init_server(entity_name_t::OSD(server_num),
                             "server", server_num , bind_addr);
      }).then([&test_msg] {
      return seastar::do_until([&test_msg] {
        return test_msg.clients_count() == test_msg.clients; },
      [&test_msg] {
        return test_msg.init_client(entity_name_t::CLIENT(-1),
                             "client", get_nonce());
	});
      }).then([] {
        return seastar::when_all_succeed(
          local_conf().set_val("ms_inject_socket_failures", "30"),
          local_conf().set_val("ms_inject_internal_delays", "0.1"),
          local_conf().set_val("ms_inject_delay_probability", "1"),
	  local_conf().set_val("ms_inject_delay_max", "5"));
      }).then_unpack([] {
	return seastar::now();
      }).then([&test_msg, tp] {
        return test_msg.generate_connections(tp.connections);
      }).then([&test_msg, tp] {
        return test_msg.random_op(tp.random_op);
      }).then([&test_msg] {
	  return test_msg.wait_for_done();
      }).then([] {
        logger().info("test_inejction() DONE");
	return seastar::now();
      }).then([] {
        return reset_conf();
      }).handle_exception([] (auto eptr) {
        logger().error(
          "test_injection() failed: got exception {}",
          eptr);
        throw;
      });
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
      return test_stress(thrash_params_t{8, 32, 50, 120})
    .then([] {
      return test_injection(thrash_params_t{16, 32, 50, 120});
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
     "chatty if true");
  return app.run(argc, argv, [&app] {
    return do_test(app);
  });
}
