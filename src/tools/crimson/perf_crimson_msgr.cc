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
#include <seastar/core/smp.hh>

#include "common/ceph_time.h"
#include "messages/MOSDOp.h"

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
  return crimson::get_logger(ceph_subsys_ms);
}

template <typename T, typename... Args>
seastar::future<T*> create_sharded(Args... args) {
  // seems we should only construct/stop shards on #0
  return seastar::smp::submit_to(0, [=] {
    auto sharded_obj = seastar::make_lw_shared<seastar::sharded<T>>();
    return sharded_obj->start(args...).then([sharded_obj]() {
      seastar::engine().at_exit([sharded_obj]() {
          return sharded_obj->stop().finally([sharded_obj] {});
        });
      return sharded_obj.get();
    });
  }).then([] (seastar::sharded<T> *ptr_shard) {
    // return the pointer valid for the caller CPU
    return &ptr_shard->local();
  });
}

enum class perf_mode_t {
  both,
  client,
  server
};

struct client_config {
  entity_addr_t server_addr;
  unsigned block_size;
  unsigned ramptime;
  unsigned msgtime;
  unsigned jobs;
  unsigned depth;
  bool v1_crc_enabled;

  std::string str() const {
    std::ostringstream out;
    out << "client[>> " << server_addr
        << "](bs=" << block_size
        << ", ramptime=" << ramptime
        << ", msgtime=" << msgtime
        << ", jobs=" << jobs
        << ", depth=" << depth
        << ", v1-crc-enabled=" << v1_crc_enabled
        << ")";
    return out.str();
  }

  static client_config load(bpo::variables_map& options) {
    client_config conf;
    entity_addr_t addr;
    ceph_assert(addr.parse(options["addr"].as<std::string>().c_str(), nullptr));

    conf.server_addr = addr;
    conf.block_size = options["cbs"].as<unsigned>();
    conf.ramptime = options["ramptime"].as<unsigned>();
    conf.msgtime = options["msgtime"].as<unsigned>();
    conf.jobs = options["jobs"].as<unsigned>();
    conf.depth = options["depth"].as<unsigned>();
    ceph_assert(conf.depth % conf.jobs == 0);
    conf.v1_crc_enabled = options["v1-crc-enabled"].as<bool>();
    return conf;
  }
};

struct server_config {
  entity_addr_t addr;
  unsigned block_size;
  unsigned core;
  bool v1_crc_enabled;

  std::string str() const {
    std::ostringstream out;
    out << "server[" << addr
        << "](bs=" << block_size
        << ", core=" << core
        << ", v1-crc-enabled=" << v1_crc_enabled
        << ")";
    return out.str();
  }

  static server_config load(bpo::variables_map& options) {
    server_config conf;
    entity_addr_t addr;
    ceph_assert(addr.parse(options["addr"].as<std::string>().c_str(), nullptr));

    conf.addr = addr;
    conf.block_size = options["sbs"].as<unsigned>();
    conf.core = options["core"].as<unsigned>();
    conf.v1_crc_enabled = options["v1-crc-enabled"].as<bool>();
    return conf;
  }
};

const unsigned SAMPLE_RATE = 7;

static seastar::future<> run(
    perf_mode_t mode,
    const client_config& client_conf,
    const server_config& server_conf)
{
  struct test_state {
    struct Server;
    using ServerFRef = seastar::foreign_ptr<std::unique_ptr<Server>>;

    struct Server final
        : public crimson::net::Dispatcher {
      crimson::net::MessengerRef msgr;
      crimson::auth::DummyAuthClientServer dummy_auth;
      const seastar::shard_id msgr_sid;
      std::string lname;
      unsigned msg_len;
      bufferlist msg_data;

      Server(unsigned msg_len)
        : msgr_sid{seastar::this_shard_id()},
          msg_len{msg_len} {
        lname = "server#";
        lname += std::to_string(msgr_sid);
        msg_data.append_zero(msg_len);
      }

      seastar::future<> ms_dispatch(crimson::net::Connection* c,
                                    MessageRef m) override {
        ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);

        // server replies with MOSDOp to generate server-side write workload
        const static pg_t pgid;
        const static object_locator_t oloc;
        const static hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                                    pgid.pool(), oloc.nspace);
        static spg_t spgid(pgid);
        auto rep = make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0);
        bufferlist data(msg_data);
        rep->write(0, msg_len, data);
        rep->set_tid(m->get_tid());
        return c->send(std::move(rep));
      }

      seastar::future<> init(bool v1_crc_enabled, const entity_addr_t& addr) {
        return seastar::smp::submit_to(msgr_sid, [v1_crc_enabled, addr, this] {
          // server msgr is always with nonce 0
          msgr = crimson::net::Messenger::create(entity_name_t::OSD(msgr_sid), lname, 0);
          msgr->set_default_policy(crimson::net::SocketPolicy::stateless_server(0));
          msgr->set_auth_client(&dummy_auth);
          msgr->set_auth_server(&dummy_auth);
          if (v1_crc_enabled) {
            msgr->set_crc_header();
            msgr->set_crc_data();
          }
          return msgr->bind(entity_addrvec_t{addr}).then([this] {
	    auto chained_dispatchers = seastar::make_lw_shared<ChainedDispatchers>();
	    chained_dispatchers->push_back(*this);
            return msgr->start(chained_dispatchers);
          });
        });
      }
      seastar::future<> shutdown() {
        logger().info("{} shutdown...", lname);
        return seastar::smp::submit_to(msgr_sid, [this] {
          ceph_assert(msgr);
          return msgr->shutdown();
        });
      }
      seastar::future<> wait() {
        return seastar::smp::submit_to(msgr_sid, [this] {
          ceph_assert(msgr);
          return msgr->wait();
        });
      }

      static seastar::future<ServerFRef> create(seastar::shard_id msgr_sid, unsigned msg_len) {
        return seastar::smp::submit_to(msgr_sid, [msg_len] {
          return seastar::make_foreign(std::make_unique<Server>(msg_len));
        });
      }
    };

    struct Client final
        : public crimson::net::Dispatcher,
          public seastar::peering_sharded_service<Client> {

      struct ConnStats {
        mono_time connecting_time = mono_clock::zero();
        mono_time connected_time = mono_clock::zero();
        unsigned received_count = 0u;

        mono_time start_time = mono_clock::zero();
        unsigned start_count = 0u;

        unsigned sampled_count = 0u;
        double total_lat_s = 0.0;

        // for reporting only
        mono_time finish_time = mono_clock::zero();

        void start() {
          start_time = mono_clock::now();
          start_count = received_count;
          sampled_count = 0u;
          total_lat_s = 0.0;
          finish_time = mono_clock::zero();
        }
      };
      ConnStats conn_stats;

      struct PeriodStats {
        mono_time start_time = mono_clock::zero();
        unsigned start_count = 0u;
        unsigned sampled_count = 0u;
        double total_lat_s = 0.0;

        // for reporting only
        mono_time finish_time = mono_clock::zero();
        unsigned finish_count = 0u;
        unsigned depth = 0u;

        void reset(unsigned received_count, PeriodStats* snap = nullptr) {
          if (snap) {
            snap->start_time = start_time;
            snap->start_count = start_count;
            snap->sampled_count = sampled_count;
            snap->total_lat_s = total_lat_s;
            snap->finish_time = mono_clock::now();
            snap->finish_count = received_count;
          }
          start_time = mono_clock::now();
          start_count = received_count;
          sampled_count = 0u;
          total_lat_s = 0.0;
        }
      };
      PeriodStats period_stats;

      const seastar::shard_id sid;
      std::string lname;

      const unsigned jobs;
      crimson::net::MessengerRef msgr;
      const unsigned msg_len;
      bufferlist msg_data;
      const unsigned nr_depth;
      seastar::semaphore depth;
      std::vector<mono_time> time_msgs_sent;
      crimson::auth::DummyAuthClientServer dummy_auth;

      unsigned sent_count = 0u;
      crimson::net::ConnectionRef active_conn = nullptr;

      bool stop_send = false;
      seastar::promise<> stopped_send_promise;

      Client(unsigned jobs, unsigned msg_len, unsigned depth)
        : sid{seastar::this_shard_id()},
          jobs{jobs},
          msg_len{msg_len},
          nr_depth{depth/jobs},
          depth{nr_depth},
          time_msgs_sent{depth/jobs, mono_clock::zero()} {
        lname = "client#";
        lname += std::to_string(sid);
        msg_data.append_zero(msg_len);
      }

      unsigned get_current_depth() const {
        ceph_assert(depth.available_units() >= 0);
        return nr_depth - depth.current();
      }

      void ms_handle_connect(crimson::net::ConnectionRef conn) override {
        conn_stats.connected_time = mono_clock::now();
      }
      seastar::future<> ms_dispatch(crimson::net::Connection* c,
                                    MessageRef m) override {
        // server replies with MOSDOp to generate server-side write workload
        ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);

        auto msg_id = m->get_tid();
        if (msg_id % SAMPLE_RATE == 0) {
          auto index = msg_id % time_msgs_sent.size();
          ceph_assert(time_msgs_sent[index] != mono_clock::zero());
          std::chrono::duration<double> cur_latency = mono_clock::now() - time_msgs_sent[index];
          conn_stats.total_lat_s += cur_latency.count();
          ++(conn_stats.sampled_count);
          period_stats.total_lat_s += cur_latency.count();
          ++(period_stats.sampled_count);
          time_msgs_sent[index] = mono_clock::zero();
        }

        ++(conn_stats.received_count);
        depth.signal(1);

        return seastar::now();
      }

      // should start messenger at this shard?
      bool is_active() {
        ceph_assert(seastar::this_shard_id() == sid);
        return sid != 0 && sid <= jobs;
      }

      seastar::future<> init(bool v1_crc_enabled) {
	auto chained_dispatchers = seastar::make_lw_shared<ChainedDispatchers>();
	chained_dispatchers->push_back(*this);
        return container().invoke_on_all([v1_crc_enabled, chained_dispatchers] (auto& client) mutable {
          if (client.is_active()) {
            client.msgr = crimson::net::Messenger::create(entity_name_t::OSD(client.sid), client.lname, client.sid);
            client.msgr->set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
            client.msgr->set_require_authorizer(false);
            client.msgr->set_auth_client(&client.dummy_auth);
            client.msgr->set_auth_server(&client.dummy_auth);
            if (v1_crc_enabled) {
              client.msgr->set_crc_header();
              client.msgr->set_crc_data();
            }
            return client.msgr->start(chained_dispatchers);
          }
          return seastar::now();
        });
      }

      seastar::future<> shutdown() {
        return container().invoke_on_all([] (auto& client) {
          if (client.is_active()) {
            logger().info("{} shutdown...", client.lname);
            ceph_assert(client.msgr);
            return client.msgr->shutdown().then([&client] {
              return client.stop_dispatch_messages();
            });
          }
          return seastar::now();
        });
      }

      seastar::future<> connect_wait_verify(const entity_addr_t& peer_addr) {
        return container().invoke_on_all([peer_addr] (auto& client) {
          // start clients in active cores (#1 ~ #jobs)
          if (client.is_active()) {
            mono_time start_time = mono_clock::now();
            client.active_conn = client.msgr->connect(peer_addr, entity_name_t::TYPE_OSD);
            // make sure handshake won't hurt the performance
            return seastar::sleep(1s).then([&client, start_time] {
              if (client.conn_stats.connected_time == mono_clock::zero()) {
                logger().error("\n{} not connected after 1s!\n", client.lname);
                ceph_assert(false);
              }
              client.conn_stats.connecting_time = start_time;
            });
          }
          return seastar::now();
        });
      }

     private:
      class TimerReport {
       private:
        const unsigned jobs;
        const unsigned msgtime;
        const unsigned bytes_of_block;

        unsigned elapsed = 0u;
        std::vector<mono_time> start_times;
        std::vector<PeriodStats> snaps;
        std::vector<ConnStats> summaries;

       public:
        TimerReport(unsigned jobs, unsigned msgtime, unsigned bs)
          : jobs{jobs},
            msgtime{msgtime},
            bytes_of_block{bs},
            start_times{jobs, mono_clock::zero()},
            snaps{jobs},
            summaries{jobs} {}

        unsigned get_elapsed() const { return elapsed; }

        PeriodStats& get_snap_by_job(seastar::shard_id sid) {
          ceph_assert(sid >= 1 && sid <= jobs);
          return snaps[sid - 1];
        }

        ConnStats& get_summary_by_job(seastar::shard_id sid) {
          ceph_assert(sid >= 1 && sid <= jobs);
          return summaries[sid - 1];
        }

        bool should_stop() const {
          return elapsed >= msgtime;
        }

        seastar::future<> ticktock() {
          return seastar::sleep(1s).then([this] {
            ++elapsed;
          });
        }

        void report_header() {
          std::ostringstream sout;
          sout << std::setfill(' ')
               << std::setw(7) << "sec"
               << std::setw(6) << "depth"
               << std::setw(8) << "IOPS"
               << std::setw(8) << "MB/s"
               << std::setw(8) << "lat(ms)";
          std::cout << sout.str() << std::endl;
        }

        void report_period() {
          if (elapsed == 1) {
            // init this->start_times at the first period
            for (unsigned i=0; i<jobs; ++i) {
              start_times[i] = snaps[i].start_time;
            }
          }
          std::chrono::duration<double> elapsed_d = 0s;
          unsigned depth = 0u;
          unsigned ops = 0u;
          unsigned sampled_count = 0u;
          double total_lat_s = 0.0;
          for (const auto& snap: snaps) {
            elapsed_d += (snap.finish_time - snap.start_time);
            depth += snap.depth;
            ops += (snap.finish_count - snap.start_count);
            sampled_count += snap.sampled_count;
            total_lat_s += snap.total_lat_s;
          }
          double elapsed_s = elapsed_d.count() / jobs;
          double iops = ops/elapsed_s;
          std::ostringstream sout;
          sout << setfill(' ')
               << std::setw(7) << elapsed_s
               << std::setw(6) << depth
               << std::setw(8) << iops
               << std::setw(8) << iops * bytes_of_block / 1048576
               << std::setw(8) << (total_lat_s / sampled_count * 1000);
          std::cout << sout.str() << std::endl;
        }

        void report_summary() const {
          std::chrono::duration<double> elapsed_d = 0s;
          unsigned ops = 0u;
          unsigned sampled_count = 0u;
          double total_lat_s = 0.0;
          for (const auto& summary: summaries) {
            elapsed_d += (summary.finish_time - summary.start_time);
            ops += (summary.received_count - summary.start_count);
            sampled_count += summary.sampled_count;
            total_lat_s += summary.total_lat_s;
          }
          double elapsed_s = elapsed_d.count() / jobs;
          double iops = ops / elapsed_s;
          std::ostringstream sout;
          sout << "--------------"
               << " summary "
               << "--------------\n"
               << setfill(' ')
               << std::setw(7) << elapsed_s
               << std::setw(6) << "-"
               << std::setw(8) << iops
               << std::setw(8) << iops * bytes_of_block / 1048576
               << std::setw(8) << (total_lat_s / sampled_count * 1000)
               << "\n";
          std::cout << sout.str() << std::endl;
        }
      };

      seastar::future<> report_period(TimerReport& report) {
        return container().invoke_on_all([&report] (auto& client) {
          if (client.is_active()) {
            PeriodStats& snap = report.get_snap_by_job(client.sid);
            client.period_stats.reset(client.conn_stats.received_count,
                                      &snap);
            snap.depth = client.get_current_depth();
          }
        }).then([&report] {
          report.report_period();
        });
      }

      seastar::future<> report_summary(TimerReport& report) {
        return container().invoke_on_all([&report] (auto& client) {
          if (client.is_active()) {
            ConnStats& summary = report.get_summary_by_job(client.sid);
            summary = client.conn_stats;
            summary.finish_time = mono_clock::now();
          }
        }).then([&report] {
          report.report_summary();
        });
      }

     public:
      seastar::future<> dispatch_with_timer(unsigned ramptime, unsigned msgtime) {
        logger().info("[all clients]: start sending MOSDOps from {} clients", jobs);
        return container().invoke_on_all([] (auto& client) {
          if (client.is_active()) {
            client.do_dispatch_messages(client.active_conn.get());
          }
        }).then([this, ramptime] {
          logger().info("[all clients]: ramping up {} seconds...", ramptime);
          return seastar::sleep(std::chrono::seconds(ramptime));
        }).then([this] {
          return container().invoke_on_all([] (auto& client) {
            if (client.is_active()) {
              client.conn_stats.start();
              client.period_stats.reset(client.conn_stats.received_count);
            }
          });
        }).then([this, msgtime] {
          logger().info("[all clients]: reporting {} seconds...\n", msgtime);
          return seastar::do_with(
              TimerReport(jobs, msgtime, msg_len), [this] (auto& report) {
            report.report_header();
            return seastar::do_until(
              [&report] { return report.should_stop(); },
              [&report, this] {
                return report.ticktock().then([&report, this] {
                  // report period every 1s
                  return report_period(report);
                }).then([&report, this] {
                  // report summary every 10s
                  if (report.get_elapsed() % 10 == 0) {
                    return report_summary(report);
                  } else {
                    return seastar::now();
                  }
                });
              }
            ).then([&report, this] {
              // report the final summary
              if (report.get_elapsed() % 10 != 0) {
                return report_summary(report);
              } else {
                return seastar::now();
              }
            });
          });
        });
      }

     private:
      seastar::future<> send_msg(crimson::net::Connection* conn) {
        ceph_assert(seastar::this_shard_id() == sid);
        return depth.wait(1).then([this, conn] {
          const static pg_t pgid;
          const static object_locator_t oloc;
          const static hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                                      pgid.pool(), oloc.nspace);
          static spg_t spgid(pgid);
          auto m = make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0);
          bufferlist data(msg_data);
          m->write(0, msg_len, data);
          // use tid as the identity of each round
          m->set_tid(sent_count);

          // sample message latency
          if (sent_count % SAMPLE_RATE == 0) {
            auto index = sent_count % time_msgs_sent.size();
            ceph_assert(time_msgs_sent[index] == mono_clock::zero());
            time_msgs_sent[index] = mono_clock::now();
          }

          return conn->send(std::move(m));
        });
      }

      class DepthBroken: public std::exception {};

      seastar::future<> stop_dispatch_messages() {
        stop_send = true;
        depth.broken(DepthBroken());
        return stopped_send_promise.get_future();
      }

      void do_dispatch_messages(crimson::net::Connection* conn) {
        ceph_assert(seastar::this_shard_id() == sid);
        ceph_assert(sent_count == 0);
        conn_stats.start_time = mono_clock::now();
        // forwarded to stopped_send_promise
        (void) seastar::do_until(
          [this] { return stop_send; },
          [this, conn] {
            sent_count += 1;
            return send_msg(conn);
          }
        ).handle_exception_type([] (const DepthBroken& e) {
          // ok, stopped by stop_dispatch_messages()
        }).finally([this, conn] {
          std::chrono::duration<double> dur_conn = conn_stats.connected_time - conn_stats.connecting_time;
          std::chrono::duration<double> dur_msg = mono_clock::now() - conn_stats.start_time;
          unsigned ops = conn_stats.received_count - conn_stats.start_count;
          logger().info("{}: stopped sending OSDOPs.\n"
                        "{}(depth={}):\n"
                        "  connect time: {}s\n"
                        "  messages received: {}\n"
                        "  messaging time: {}s\n"
                        "  latency: {}ms\n"
                        "  IOPS: {}\n"
                        "  throughput: {}MB/s\n",
                        *conn,
                        lname,
                        nr_depth,
                        dur_conn.count(),
                        ops,
                        dur_msg.count(),
                        conn_stats.total_lat_s / conn_stats.sampled_count * 1000,
                        ops / dur_msg.count(),
                        ops / dur_msg.count() * msg_len / 1048576);
          stopped_send_promise.set_value();
        });
      }
    };
  };

  return seastar::when_all(
      test_state::Server::create(server_conf.core, server_conf.block_size),
      create_sharded<test_state::Client>(client_conf.jobs, client_conf.block_size, client_conf.depth)
  ).then([=](auto&& ret) {
    auto fp_server = std::move(std::get<0>(ret).get0());
    auto client = std::move(std::get<1>(ret).get0());
    test_state::Server* server = fp_server.get();
    if (mode == perf_mode_t::both) {
      logger().info("\nperf settings:\n  {}\n  {}\n",
                    client_conf.str(), server_conf.str());
      ceph_assert(seastar::smp::count >= 1+client_conf.jobs);
      ceph_assert(client_conf.jobs > 0);
      ceph_assert(seastar::smp::count >= 1+server_conf.core);
      ceph_assert(server_conf.core == 0 || server_conf.core > client_conf.jobs);
      return seastar::when_all_succeed(
        server->init(server_conf.v1_crc_enabled, server_conf.addr),
        client->init(client_conf.v1_crc_enabled)
      ).then([client, addr = client_conf.server_addr] {
        return client->connect_wait_verify(addr);
      }).then([client, ramptime = client_conf.ramptime,
               msgtime = client_conf.msgtime] {
        return client->dispatch_with_timer(ramptime, msgtime);
      }).finally([client] {
        return client->shutdown();
      }).finally([server, fp_server = std::move(fp_server)] () mutable {
        return server->shutdown().then([cleanup = std::move(fp_server)] {});
      });
    } else if (mode == perf_mode_t::client) {
      logger().info("\nperf settings:\n  {}\n", client_conf.str());
      ceph_assert(seastar::smp::count >= 1+client_conf.jobs);
      ceph_assert(client_conf.jobs > 0);
      return client->init(client_conf.v1_crc_enabled
      ).then([client, addr = client_conf.server_addr] {
        return client->connect_wait_verify(addr);
      }).then([client, ramptime = client_conf.ramptime,
               msgtime = client_conf.msgtime] {
        return client->dispatch_with_timer(ramptime, msgtime);
      }).finally([client] {
        return client->shutdown();
      });
    } else { // mode == perf_mode_t::server
      ceph_assert(seastar::smp::count >= 1+server_conf.core);
      logger().info("\nperf settings:\n  {}\n", server_conf.str());
      return server->init(server_conf.v1_crc_enabled, server_conf.addr
      // dispatch ops
      ).then([server] {
        return server->wait();
      // shutdown
      }).finally([server, fp_server = std::move(fp_server)] () mutable {
        return server->shutdown().then([cleanup = std::move(fp_server)] {});
      });
    }
  });
}

}

int main(int argc, char** argv)
{
  seastar::app_template app;
  app.add_options()
    ("mode", bpo::value<unsigned>()->default_value(0),
     "0: both, 1:client, 2:server")
    ("addr", bpo::value<std::string>()->default_value("v1:127.0.0.1:9010"),
     "server address")
    ("ramptime", bpo::value<unsigned>()->default_value(5),
     "seconds of client ramp-up time")
    ("msgtime", bpo::value<unsigned>()->default_value(15),
     "seconds of client messaging time")
    ("jobs", bpo::value<unsigned>()->default_value(1),
     "number of client jobs (messengers)")
    ("cbs", bpo::value<unsigned>()->default_value(4096),
     "client block size")
    ("depth", bpo::value<unsigned>()->default_value(512),
     "client io depth")
    ("core", bpo::value<unsigned>()->default_value(0),
     "server running core")
    ("sbs", bpo::value<unsigned>()->default_value(0),
     "server block size")
    ("v1-crc-enabled", bpo::value<bool>()->default_value(false),
     "enable v1 CRC checks");
  return app.run(argc, argv, [&app] {
      auto&& config = app.configuration();
      auto mode = config["mode"].as<unsigned>();
      ceph_assert(mode <= 2);
      auto _mode = static_cast<perf_mode_t>(mode);
      auto server_conf = server_config::load(config);
      auto client_conf = client_config::load(config);
      return run(_mode, client_conf, server_conf).then([] {
          logger().info("\nsuccessful!\n");
        }).handle_exception([] (auto eptr) {
          logger().info("\nfailed!\n");
          return seastar::make_exception_future<>(eptr);
        });
    });
}
