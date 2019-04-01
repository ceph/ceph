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

#include "crimson/auth/DummyAuth.h"
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

static seastar::future<> run(unsigned rounds,
                             unsigned jobs,
                             unsigned bs,
                             unsigned depth,
                             std::string addr,
                             perf_mode_t mode,
                             unsigned core)
{
  struct test_state {
    struct Server final
        : public ceph::net::Dispatcher,
          public seastar::peering_sharded_service<Server> {
      ceph::net::Messenger *msgr = nullptr;
      ceph::auth::DummyAuthClientServer dummy_auth;
      const seastar::shard_id sid;
      const seastar::shard_id msgr_sid;
      std::string lname;

      Server(unsigned msgr_core)
        : sid{seastar::engine().cpu_id()},
          msgr_sid{msgr_core} {
        lname = "server#";
        lname += std::to_string(sid);
      }

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
        return c->send(MessageRef{ new MOSDOpReply(req.get(), 0, 0, 0, false), false });
      }

      seastar::future<> init(const entity_addr_t& addr) {
        return container().invoke_on(msgr_sid, [addr] (auto& server) {
          // server msgr is always with nonce 0
          auto&& fut = ceph::net::Messenger::create(entity_name_t::OSD(server.sid), server.lname, 0, server.sid);
          return fut.then([&server, addr](ceph::net::Messenger *messenger) {
              return server.container().invoke_on_all([messenger](auto& server) {
                  server.msgr = messenger->get_local_shard();
                  server.msgr->set_default_policy(ceph::net::SocketPolicy::stateless_server(0));
                  server.msgr->set_auth_client(&server.dummy_auth);
                  server.msgr->set_auth_server(&server.dummy_auth);
                }).then([messenger, addr] {
                  return messenger->bind(entity_addrvec_t{addr});
                }).then([&server, messenger] {
                  return messenger->start(&server);
                });
            });
        });
      }
      seastar::future<> shutdown() {
        logger().info("\n{} shutdown...", lname);
        return container().invoke_on(msgr_sid, [] (auto& server) {
          ceph_assert(server.msgr);
          return server.msgr->shutdown();
        });
      }
    };

    struct Client final
        : public ceph::net::Dispatcher,
          public seastar::peering_sharded_service<Client> {

      struct PingSession : public seastar::enable_shared_from_this<PingSession> {
        unsigned received_count = 0u;
        mono_time connecting_time;
        mono_time connected_time;
        mono_time start_time;
        mono_time finish_time;
        seastar::promise<> done;
      };
      using PingSessionRef = seastar::shared_ptr<PingSession>;

      const seastar::shard_id sid;
      std::string lname;

      const unsigned jobs;
      const unsigned rounds;
      ceph::net::Messenger *msgr = nullptr;
      const unsigned msg_len;
      bufferlist msg_data;
      seastar::semaphore depth;
      ceph::auth::DummyAuthClientServer dummy_auth;

      unsigned sent_count = 0u;
      ceph::net::ConnectionRef active_conn = nullptr;
      PingSessionRef active_session = nullptr;

      Client(unsigned jobs, unsigned rounds, unsigned msg_len, unsigned depth)
        : sid{seastar::engine().cpu_id()},
          jobs{jobs},
          rounds{rounds/jobs},
          msg_len{msg_len},
          depth{depth} {
        lname = "client#";
        lname += std::to_string(sid);
        bufferptr ptr(msg_len);
        memset(ptr.c_str(), 0, msg_len);
        msg_data.append(ptr);
      }

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::now();
      }
      seastar::future<> ms_handle_connect(ceph::net::ConnectionRef conn) override {
        logger().info("{}: connected", *conn);
        active_session = seastar::make_shared<PingSession>();
        active_session->connected_time = mono_clock::now();
        return seastar::now();
      }
      seastar::future<> ms_dispatch(ceph::net::ConnectionRef c,
                                    MessageRef m) override {
        ceph_assert(m->get_type() == CEPH_MSG_OSD_OPREPLY);
        depth.signal(1);
        ceph_assert(active_session);
        ++(active_session->received_count);

        if (active_session->received_count == rounds) {
          logger().info("{}: finished receiving {} OPREPLYs", *c, active_session->received_count);
          active_session->finish_time = mono_clock::now();
          active_session->done.set_value();
        }
        return seastar::now();
      }

      // should start messenger at this shard?
      bool is_active() {
        ceph_assert(seastar::engine().cpu_id() == sid);
        return sid != 0 && sid <= jobs;
      }

      seastar::future<> init() {
        return container().invoke_on_all([] (auto& client) {
          if (client.is_active()) {
            return ceph::net::Messenger::create(entity_name_t::OSD(client.sid), client.lname, client.sid, client.sid)
            .then([&client] (ceph::net::Messenger *messenger) {
              client.msgr = messenger;
              client.msgr->set_default_policy(ceph::net::SocketPolicy::lossy_client(0));
              client.msgr->set_auth_client(&client.dummy_auth);
              client.msgr->set_auth_server(&client.dummy_auth);
              return client.msgr->start(&client);
            });
          }
          return seastar::now();
        });
      }

      seastar::future<> shutdown() {
        return container().invoke_on_all([] (auto& client) {
          if (client.is_active()) {
            logger().info("\n{} shutdown...", client.lname);
            ceph_assert(client.msgr);
            return client.msgr->shutdown();
          }
          return seastar::now();
        });
      }

      seastar::future<> dispatch_messages(const entity_addr_t& peer_addr) {
        return container().invoke_on_all([peer_addr] (auto& client) {
          // start clients in active cores (#1 ~ #jobs)
          if (client.is_active()) {
            mono_time start_time = mono_clock::now();
            return client.msgr->connect(peer_addr, entity_name_t::TYPE_OSD)
            .then([&client] (auto conn) {
              client.active_conn = conn->release();
              // make sure handshake won't heart the performance
              return seastar::sleep(1s);
            }).then([&client, start_time] {
              if (!client.active_session) {
                logger().error("\n{} not connected after 1s!\n", client.lname);
                ceph_assert(false);
              }
              client.active_session->connecting_time = start_time;
            });
          }
          return seastar::now();
        }).then([this] {
          logger().info("\nstart sending {} MOSDOps from {} clients",
                        jobs * rounds, jobs);
          mono_time start_time = mono_clock::now();
          return container().invoke_on_all([] (auto& client) {
            if (client.is_active()) {
              return client.do_dispatch_messages(client.active_conn.get());
            }
            return seastar::now();
          }).then([this, start_time] {
            std::chrono::duration<double> dur_messaging = mono_clock::now() - start_time;
            logger().info("\nSummary:\n  clients: {}\n  MOSDOps: {}\n  total time: {}s\n",
                          jobs, jobs * rounds, dur_messaging.count());
          });
        });
      }

     private:
      seastar::future<> send_msg(ceph::net::Connection* conn) {
        ceph_assert(seastar::engine().cpu_id() == sid);
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
        ceph_assert(seastar::engine().cpu_id() == sid);
        ceph_assert(sent_count == 0);
        active_session->start_time = mono_clock::now();
        return seastar::do_until(
          [this, conn] {
            bool stop = (sent_count == rounds);
            if (stop) {
              logger().info("{}: finished sending {} OSDOPs",
                            *conn, sent_count);
            }
            return stop;
          },
          [this, conn] {
            sent_count += 1;
            return send_msg(conn);
          }
        ).then([this] {
          return active_session->done.get_future();
        }).then([this] {
          std::chrono::duration<double> dur_conn = active_session->connected_time - active_session->connecting_time;
          std::chrono::duration<double> dur_msg = mono_clock::now() - active_session->start_time;
          logger().info("\n{}:\n  messages: {}\n  connect time: {}s\n  messaging time: {}s\n",
                        lname, active_session->received_count, dur_conn.count(), dur_msg.count());
        });
      }
    };
  };

  return seastar::when_all_succeed(
      ceph::net::create_sharded<test_state::Server>(core),
      ceph::net::create_sharded<test_state::Client>(jobs, rounds, bs, depth))
    .then([=](test_state::Server *server,
              test_state::Client *client) {
      entity_addr_t target_addr;
      target_addr.parse(addr.c_str(), nullptr);
      if (mode == perf_mode_t::both) {
          logger().info("\nperf settings:\n  mode=server+client\n  server addr={}\n  server core={}\n  rounds={}\n  client jobs={}\n  bs={}\n  depth={}\n",
                        addr, core, rounds, jobs, bs, depth);
          ceph_assert(seastar::smp::count >= std::max(1+jobs, 1+core));
          ceph_assert(core == 0 || core > jobs);
          ceph_assert(jobs > 0);
          return seastar::when_all_succeed(
              server->init(target_addr),
              client->init())
          // dispatch ops
            .then([client, target_addr] {
              return client->dispatch_messages(target_addr);
          // shutdown
            }).finally([client] {
              return client->shutdown();
            }).finally([server] {
              return server->shutdown();
            });
      } else if (mode == perf_mode_t::client) {
          logger().info("\nperf settings:\n  mode=client\n  server addr={}\n  rounds={}\n  client jobs={}\n  bs={}\n  depth={}\n",
                        addr, rounds, jobs, bs, depth);
          ceph_assert(seastar::smp::count >= 1+jobs);
          ceph_assert(jobs > 0);
          return client->init()
          // dispatch ops
            .then([client, target_addr] {
              return client->dispatch_messages(target_addr);
          // shutdown
            }).finally([client] {
              return client->shutdown();
            });
      } else { // mode == perf_mode_t::server
          ceph_assert(seastar::smp::count >= 1+core);
          logger().info("\nperf settings:\n  mode=server\n  server addr={}\n  server core={}\n",
                        addr, core);
          return server->init(target_addr)
          // dispatch ops
            .then([server] {
              return server->msgr->wait();
          // shutdown
            }).finally([server] {
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
    ("addr", bpo::value<std::string>()->default_value("v1:0.0.0.0:9010"),
     "server address")
    ("core", bpo::value<unsigned>()->default_value(0),
     "server running core")
    ("mode", bpo::value<unsigned>()->default_value(0),
     "0: both, 1:client, 2:server")
    ("rounds", bpo::value<unsigned>()->default_value(65536),
     "number of messaging rounds")
    ("jobs", bpo::value<unsigned>()->default_value(1),
     "number of jobs (client messengers)")
    ("bs", bpo::value<unsigned>()->default_value(4096),
     "block size")
    ("depth", bpo::value<unsigned>()->default_value(512),
     "io depth");
  return app.run(argc, argv, [&app] {
      auto&& config = app.configuration();
      auto rounds = config["rounds"].as<unsigned>();
      auto jobs = config["jobs"].as<unsigned>();
      auto bs = config["bs"].as<unsigned>();
      auto depth = config["depth"].as<unsigned>();
      auto addr = config["addr"].as<std::string>();
      auto core = config["core"].as<unsigned>();
      auto mode = config["mode"].as<unsigned>();
      ceph_assert(mode <= 2);
      auto _mode = static_cast<perf_mode_t>(mode);
      return run(rounds, jobs, bs, depth, addr, _mode, core)
        .then([] {
          logger().info("\nsuccessful!\n");
        }).handle_exception([] (auto eptr) {
          logger().info("\nfailed!\n");
          return seastar::make_exception_future<>(eptr);
        });
    });
}
