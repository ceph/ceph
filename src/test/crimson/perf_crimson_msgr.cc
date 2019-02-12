// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <map>
#include <random>
#include <boost/program_options.hpp>

#include <seastar/core/app-template.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/semaphore.hh>

#include "common/ceph_time.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Messenger.h"

namespace bpo = boost::program_options;

namespace {

template<typename Message>
using Ref = boost::intrusive_ptr<Message>;

seastar::logger& logger() {
  return ceph::get_logger(ceph_subsys_ms);
}


enum class perf_mode_t {
  both,
  client,
  server
};

static std::random_device rd;
static std::default_random_engine rng{rd()};

static seastar::future<> run(unsigned rounds,
                             double keepalive_ratio,
                             int bs,
                             int depth,
                             std::string addr,
                             perf_mode_t mode)
{
  struct test_state {
    struct Server final
        : public ceph::net::Dispatcher,
          public seastar::peering_sharded_service<Server> {
      ceph::net::Messenger *msgr = nullptr;

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::make_ready_future<>();
      }
      seastar::future<> ms_dispatch(ceph::net::ConnectionRef c,
                                    MessageRef m) override {
        ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
        // reply
        Ref<MOSDOp> req = boost::static_pointer_cast<MOSDOp>(m);
        req->finish_decode();
        return c->send(MessageRef{ new MOSDOpReply(req.get(), 0, 0, 0, false), false });
      }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        auto&& fut = ceph::net::Messenger::create(name, lname, nonce, 1);
        return fut.then([this, addr](ceph::net::Messenger *messenger) {
            return container().invoke_on_all([messenger](auto& server) {
                server.msgr = messenger->get_local_shard();
                server.msgr->set_crc_header();
              }).then([messenger, addr] {
                return messenger->bind(entity_addrvec_t{addr});
              }).then([this, messenger] {
                return messenger->start(this);
              });
          });
      }
      seastar::future<> shutdown() {
        ceph_assert(msgr);
        return msgr->shutdown();
      }
    };

    struct Client final
        : public ceph::net::Dispatcher,
          public seastar::peering_sharded_service<Client> {

      struct PingSession : public seastar::enable_shared_from_this<PingSession> {
        unsigned count = 0u;
        mono_time connected_time;
        mono_time finish_time;
      };
      using PingSessionRef = seastar::shared_ptr<PingSession>;

      unsigned rounds;
      std::bernoulli_distribution keepalive_dist;
      ceph::net::Messenger *msgr = nullptr;
      std::map<ceph::net::Connection*, seastar::promise<>> pending_conns;
      std::map<ceph::net::ConnectionRef, PingSessionRef> sessions;
      int msg_len;
      bufferlist msg_data;
      seastar::semaphore depth;

      Client(unsigned rounds, double keepalive_ratio, int msg_len, int depth)
        : rounds(rounds),
          keepalive_dist(std::bernoulli_distribution{keepalive_ratio}),
          depth(depth) {
        bufferptr ptr(msg_len);
        memset(ptr.c_str(), 0, msg_len);
        msg_data.append(ptr);
      }

      PingSessionRef find_session(ceph::net::ConnectionRef c) {
        auto found = sessions.find(c);
        if (found == sessions.end()) {
          ceph_assert(false);
        }
        return found->second;
      }

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::now();
      }
      seastar::future<> ms_handle_connect(ceph::net::ConnectionRef conn) override {
        logger().info("{}: connected to {}", *conn, conn->get_peer_addr());
        auto session = seastar::make_shared<PingSession>();
        auto [i, added] = sessions.emplace(conn, session);
        std::ignore = i;
        ceph_assert(added);
        session->connected_time = mono_clock::now();
        return seastar::now();
      }
      seastar::future<> ms_dispatch(ceph::net::ConnectionRef c,
                                    MessageRef m) override {
        ceph_assert(m->get_type() == CEPH_MSG_OSD_OPREPLY);
        depth.signal(1);
        auto session = find_session(c);
        ++(session->count);

        if (session->count == rounds) {
          logger().info("{}: finished receiving {} OPREPLYs", *c.get(), session->count);
          session->finish_time = mono_clock::now();
          return container().invoke_on_all([conn = c.get()](auto &client) {
              auto found = client.pending_conns.find(conn);
              ceph_assert(found != client.pending_conns.end());
              found->second.set_value();
            });
        } else {
          return seastar::now();
        }
      }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        return ceph::net::Messenger::create(name, lname, nonce, 2)
          .then([this](ceph::net::Messenger *messenger) {
            return container().invoke_on_all([messenger](auto& client) {
                client.msgr = messenger->get_local_shard();
                client.msgr->set_crc_header();
              }).then([this, messenger] {
                return messenger->start(this);
              });
          });
      }

      seastar::future<> shutdown() {
        ceph_assert(msgr);
        return msgr->shutdown();
      }

      seastar::future<> dispatch_messages(const entity_addr_t& peer_addr, bool foreign_dispatch=true) {
        mono_time start_time = mono_clock::now();
        return msgr->connect(peer_addr, entity_name_t::TYPE_OSD)
          .then([this, foreign_dispatch, start_time](auto conn) {
            return seastar::futurize_apply([this, conn, foreign_dispatch] {
                if (foreign_dispatch) {
                  return do_dispatch_messages(&**conn);
                } else {
                  // NOTE: this could be faster if we don't switch cores in do_dispatch_messages().
                  return container().invoke_on(conn->get()->shard_id(), [conn = &**conn](auto &client) {
                      return client.do_dispatch_messages(conn);
                    });
                }
              }).finally([this, conn, start_time] {
                return container().invoke_on(conn->get()->shard_id(), [conn, start_time](auto &client) {
                    auto session = client.find_session((*conn)->shared_from_this());
                    std::chrono::duration<double> dur_handshake = session->connected_time - start_time;
                    std::chrono::duration<double> dur_messaging = session->finish_time - session->connected_time;
                    logger().info("{}: handshake {}, messaging {}",
                                  **conn, dur_handshake.count(), dur_messaging.count());
                  });
              });
          });
      }

     private:
      seastar::future<> send_msg(ceph::net::Connection* conn) {
        return depth.wait(1).then([this, conn] {
          const static pg_t pgid;
          const static object_locator_t oloc;
          const static hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                                      pgid.pool(), oloc.nspace);
          static spg_t spgid(pgid);
          MOSDOp *m = new MOSDOp(0, 0, hobj, spgid, 0, 0, 0);
          bufferlist data(msg_data);
          m->write(0, msg_len, data);
          MessageRef msg = {m, false};
          return conn->send(msg);
        });
      }

      seastar::future<> do_dispatch_messages(ceph::net::Connection* conn) {
        return container().invoke_on_all([conn](auto& client) {
            auto [i, added] = client.pending_conns.emplace(conn, seastar::promise<>());
            std::ignore = i;
            ceph_assert(added);
          }).then([this, conn] {
            return seastar::do_with(0u, 0u,
                                    [this, conn](auto &count_ping, auto &count_keepalive) {
                return seastar::do_until(
                  [this, conn, &count_ping, &count_keepalive] {
                    bool stop = (count_ping == rounds);
                    if (stop) {
                      logger().info("{}: finished sending {} OSDOPs with {} keepalives",
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
                          return send_msg(conn)
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
                  });
              });
          });
      }
    };
  };

  return seastar::when_all_succeed(
      ceph::net::create_sharded<test_state::Server>(),
      ceph::net::create_sharded<test_state::Client>(rounds, keepalive_ratio, bs, depth))
    .then([rounds, keepalive_ratio, addr, mode](test_state::Server *server,
                                                test_state::Client *client) {
      entity_addr_t target_addr;
      target_addr.parse(addr.c_str(), nullptr);
      target_addr.set_type(entity_addr_t::TYPE_LEGACY);
      if (mode == perf_mode_t::both) {
          return seastar::when_all_succeed(
              server->init(entity_name_t::OSD(0), "server", 0, target_addr),
              client->init(entity_name_t::OSD(1), "client", 0))
          // dispatch pingpoing
            .then([client, target_addr] {
              return client->dispatch_messages(target_addr, false);
          // shutdown
            }).finally([client] {
              logger().info("client shutdown...");
              return client->shutdown();
            }).finally([server] {
              logger().info("server shutdown...");
              return server->shutdown();
            });
      } else if (mode == perf_mode_t::client) {
          return client->init(entity_name_t::OSD(1), "client", 0)
          // dispatch pingpoing
            .then([client, target_addr] {
              return client->dispatch_messages(target_addr, false);
          // shutdown
            }).finally([client] {
              logger().info("client shutdown...");
              return client->shutdown();
            });
      } else { // mode == perf_mode_t::server
          return server->init(entity_name_t::OSD(0), "server", 0, target_addr)
          // dispatch pingpoing
            .then([server] {
              return server->msgr->wait();
          // shutdown
            }).finally([server] {
              logger().info("server shutdown...");
              return server->shutdown();
            });
      }
    });
}

}

int main(int argc, char** argv)
{
  seastar::app_template app;
  app.add_options()
    ("addr", bpo::value<std::string>()->default_value("0.0.0.0:9010"),
     "start server")
    ("mode", bpo::value<int>()->default_value(0),
     "0: both, 1:client, 2:server")
    ("rounds", bpo::value<unsigned>()->default_value(65536),
     "number of messaging rounds")
    ("keepalive-ratio", bpo::value<double>()->default_value(0),
     "ratio of keepalive in ping messages")
    ("bs", bpo::value<int>()->default_value(4096),
     "block size")
    ("depth", bpo::value<int>()->default_value(512),
     "io depth");
  return app.run(argc, argv, [&app] {
      auto&& config = app.configuration();
      auto rounds = config["rounds"].as<unsigned>();
      auto keepalive_ratio = config["keepalive-ratio"].as<double>();
      auto bs = config["bs"].as<int>();
      auto depth = config["depth"].as<int>();
      auto addr = config["addr"].as<std::string>();
      auto mode = config["mode"].as<int>();
      logger().info("\nsettings:\n  addr={}\n  mode={}\n  rounds={}\n  keepalive-ratio={}\n  bs={}\n  depth={}",
                    addr, mode, rounds, keepalive_ratio, bs, depth);
      ceph_assert(mode >= 0 && mode <= 2);
      auto _mode = static_cast<perf_mode_t>(mode);
      return run(rounds, keepalive_ratio, bs, depth, addr, _mode)
        .then([] {
          std::cout << "successful" << std::endl;
        }).handle_exception([] (auto eptr) {
          std::cout << "failed" << std::endl;
          return seastar::make_exception_future<>(eptr);
        });
    });
}
