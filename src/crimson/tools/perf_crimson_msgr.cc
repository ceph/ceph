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
#include "crimson/common/config_proxy.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Messenger.h"

using namespace std;
using namespace std::chrono_literals;

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
          return sharded_obj->stop().then([sharded_obj] {});
        });
      return sharded_obj.get();
    });
  }).then([] (seastar::sharded<T> *ptr_shard) {
    // return the pointer valid for the caller CPU
    return &ptr_shard->local();
  });
}

double get_reactor_utilization() {
  auto &value_map = seastar::metrics::impl::get_value_map();
  auto found = value_map.find("reactor_utilization");
  assert(found != value_map.end());
  auto &[full_name, metric_family] = *found;
  std::ignore = full_name;
  assert(metric_family.size() == 1);
  const auto& [labels, metric] = *metric_family.begin();
  std::ignore = labels;
  auto value = (*metric)();
  return value.ui();
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

  std::string str() const {
    std::ostringstream out;
    out << "client[>> " << server_addr
        << "](bs=" << block_size
        << ", ramptime=" << ramptime
        << ", msgtime=" << msgtime
        << ", jobs=" << jobs
        << ", depth=" << depth
        << ")";
    return out.str();
  }

  static client_config load(bpo::variables_map& options) {
    client_config conf;
    entity_addr_t addr;
    ceph_assert(addr.parse(options["server-addr"].as<std::string>().c_str(), nullptr));
    ceph_assert_always(addr.is_msgr2());

    conf.server_addr = addr;
    conf.block_size = options["client-bs"].as<unsigned>();
    conf.ramptime = options["ramptime"].as<unsigned>();
    conf.msgtime = options["msgtime"].as<unsigned>();
    conf.jobs = options["client-jobs"].as<unsigned>();
    conf.depth = options["depth"].as<unsigned>();
    ceph_assert(conf.depth % conf.jobs == 0);
    return conf;
  }
};

struct server_config {
  entity_addr_t addr;
  unsigned block_size;
  unsigned core;

  std::string str() const {
    std::ostringstream out;
    out << "server[" << addr
        << "](bs=" << block_size
        << ", core=" << core
        << ")";
    return out.str();
  }

  static server_config load(bpo::variables_map& options) {
    server_config conf;
    entity_addr_t addr;
    ceph_assert(addr.parse(options["server-addr"].as<std::string>().c_str(), nullptr));
    ceph_assert_always(addr.is_msgr2());

    conf.addr = addr;
    conf.block_size = options["server-bs"].as<unsigned>();
    conf.core = options["server-core"].as<unsigned>();
    return conf;
  }
};

const unsigned SAMPLE_RATE = 7;

static seastar::future<> run(
    perf_mode_t mode,
    const client_config& client_conf,
    const server_config& server_conf,
    bool crc_enabled)
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

      std::optional<seastar::future<>> ms_dispatch(
          crimson::net::ConnectionRef c, MessageRef m) override {
        ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);

        // server replies with MOSDOp to generate server-side write workload
        const static pg_t pgid;
        const static object_locator_t oloc;
        const static hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                                    pgid.pool(), oloc.nspace);
        static spg_t spgid(pgid);
        auto rep = crimson::make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0);
        bufferlist data(msg_data);
        rep->write(0, msg_len, data);
        rep->set_tid(m->get_tid());
        std::ignore = c->send(std::move(rep));
        return {seastar::now()};
      }

      seastar::future<> init(const entity_addr_t& addr) {
        return seastar::smp::submit_to(msgr_sid, [addr, this] {
          // server msgr is always with nonce 0
          msgr = crimson::net::Messenger::create(
              entity_name_t::OSD(msgr_sid),
              lname, 0, true);
          msgr->set_default_policy(crimson::net::SocketPolicy::stateless_server(0));
          msgr->set_auth_client(&dummy_auth);
          msgr->set_auth_server(&dummy_auth);
          return msgr->bind(entity_addrvec_t{addr}).safe_then([this] {
            return msgr->start({this});
          }, crimson::net::Messenger::bind_ertr::all_same_way(
              [addr] (const std::error_code& e) {
            logger().error("Server: "
                           "there is another instance running at {}", addr);
            ceph_abort();
          }));
        });
      }
      seastar::future<> shutdown() {
        logger().info("{} shutdown...", lname);
        return seastar::smp::submit_to(msgr_sid, [this] {
          ceph_assert(msgr);
          msgr->stop();
          return msgr->shutdown();
        });
      }
      seastar::future<> wait() {
        return seastar::smp::submit_to(msgr_sid, [this] {
          ceph_assert(msgr);
          return msgr->wait();
        });
      }

      static seastar::future<ServerFRef> create(
          seastar::shard_id msgr_sid,
          unsigned msg_len) {
        return seastar::smp::submit_to(
            msgr_sid, [msg_len] {
          return seastar::make_foreign(
              std::make_unique<Server>(msg_len));
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
        double sampled_total_lat_s = 0.0;

        // for reporting only
        mono_time finish_time = mono_clock::zero();

        void start_connecting() {
          connecting_time = mono_clock::now();
        }

        void finish_connecting() {
          connected_time = mono_clock::now();
        }

        void start_collect() {
          start_time = mono_clock::now();
          start_count = received_count;
          sampled_count = 0u;
          sampled_total_lat_s = 0.0;
          finish_time = mono_clock::zero();
        }

        void prepare_summary(const ConnStats &current) {
          *this = current;
          finish_time = mono_clock::now();
        }
      };
      ConnStats conn_stats;

      struct PeriodStats {
        mono_time start_time = mono_clock::zero();
        unsigned start_count = 0u;
        unsigned sampled_count = 0u;
        double sampled_total_lat_s = 0.0;

        // for reporting only
        mono_time finish_time = mono_clock::zero();
        unsigned finish_count = 0u;
        unsigned depth = 0u;
        double reactor_utilization = 0;

        void start_collect(unsigned received_count) {
          start_time = mono_clock::now();
          start_count = received_count;
          sampled_count = 0u;
          sampled_total_lat_s = 0.0;
        }

        void reset_period(
            unsigned received_count, unsigned _depth, PeriodStats &snapshot) {
          snapshot.start_time = start_time;
          snapshot.start_count = start_count;
          snapshot.sampled_count = sampled_count;
          snapshot.sampled_total_lat_s = sampled_total_lat_s;
          snapshot.finish_time = mono_clock::now();
          snapshot.finish_count = received_count;
          snapshot.depth = _depth;
          snapshot.reactor_utilization = get_reactor_utilization();

          start_collect(received_count);
        }
      };
      PeriodStats period_stats;

      const seastar::shard_id sid;
      const unsigned id;
      std::string lname;
      const std::optional<unsigned> server_sid;

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

      struct ClientStats {
        std::string name;
        unsigned depth = 0;
        double connect_time_s = 0;
        unsigned total_msgs = 0;
        double messaging_time_s = 0;
        double latency_ms = 0;
        double iops = 0;
        double throughput_mbps = 0;

        void report() const {
          auto str = fmt::format(
            "{}(depth={}):\n"
            "  connect time: {:08f}s\n"
            "  messages received: {}\n"
            "  messaging time: {:08f}s\n"
            "  latency: {:08f}ms\n"
            "  IOPS: {:08f}\n"
            "  out throughput: {:08f}MB/s",
            name, depth, connect_time_s,
            total_msgs, messaging_time_s,
            latency_ms, iops,
            throughput_mbps);
          std::cout << str << std::endl;
        }
      };

      bool stop_send = false;
      seastar::promise<ClientStats> stopped_send_promise;

      Client(unsigned jobs, unsigned msg_len, unsigned depth, std::optional<unsigned> server_sid)
        : sid{seastar::this_shard_id()},
          id{sid + jobs - seastar::smp::count},
          server_sid{server_sid},
          jobs{jobs},
          msg_len{msg_len},
          nr_depth{depth/jobs},
          depth{nr_depth},
          time_msgs_sent{depth/jobs, mono_clock::zero()} {
        if (is_active()) {
          assert(sid > 0);
          lname = "client";
          lname += std::to_string(id);
          lname += "@";
          lname += std::to_string(sid);
        } else {
          lname = "invalid_client";
        }
        msg_data.append_zero(msg_len);
      }

      unsigned get_current_depth() const {
        ceph_assert(depth.available_units() >= 0);
        return nr_depth - depth.current();
      }

      void ms_handle_connect(
          crimson::net::ConnectionRef conn,
          seastar::shard_id new_shard) override {
        ceph_assert_always(new_shard == seastar::this_shard_id());
        conn_stats.finish_connecting();
      }

      std::optional<seastar::future<>> ms_dispatch(
          crimson::net::ConnectionRef, MessageRef m) override {
        assert(is_active());
        // server replies with MOSDOp to generate server-side write workload
        ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);

        auto msg_id = m->get_tid();
        if (msg_id % SAMPLE_RATE == 0) {
          auto index = msg_id % time_msgs_sent.size();
          ceph_assert(time_msgs_sent[index] != mono_clock::zero());
          std::chrono::duration<double> cur_latency = mono_clock::now() - time_msgs_sent[index];
          conn_stats.sampled_total_lat_s += cur_latency.count();
          ++(conn_stats.sampled_count);
          period_stats.sampled_total_lat_s += cur_latency.count();
          ++(period_stats.sampled_count);
          time_msgs_sent[index] = mono_clock::zero();
        }

        ++(conn_stats.received_count);
        depth.signal(1);

        return {seastar::now()};
      }

      // should start messenger at this shard?
      bool is_active() {
        ceph_assert(seastar::this_shard_id() == sid);
        ceph_assert(seastar::smp::count > jobs);
        return sid + jobs >= seastar::smp::count;
      }

      seastar::future<> init() {
        return container().invoke_on_all([] (auto& client) {
          if (client.is_active()) {
            client.msgr = crimson::net::Messenger::create(
                entity_name_t::OSD(client.sid),
                client.lname, client.sid, true);
            client.msgr->set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
            client.msgr->set_auth_client(&client.dummy_auth);
            client.msgr->set_auth_server(&client.dummy_auth);
            return client.msgr->start({&client});
          }
          return seastar::now();
        });
      }

      seastar::future<> shutdown() {
        return seastar::do_with(
            std::vector<ClientStats>(jobs),
            [this](auto &all_stats) {
          return container().invoke_on_all([&all_stats](auto& client) {
            if (!client.is_active()) {
              return seastar::now();
            }

            logger().info("{} shutdown...", client.lname);
            ceph_assert(client.msgr);
            client.msgr->stop();
            return seastar::when_all(
              client.stop_dispatch_messages(
              ).then([&all_stats, &client](auto stats) {
                all_stats[client.id] = stats;
              }),
              client.msgr->shutdown()
            ).discard_result();
          }).then([&all_stats] {
            auto nr_clients = all_stats.size();
            ClientStats summary;
            summary.name = "AllClients" + std::to_string(nr_clients);
            for (const auto &stats : all_stats) {
              stats.report();
              summary.depth += stats.depth;
              summary.connect_time_s += stats.connect_time_s;
              summary.total_msgs += stats.total_msgs;
              summary.messaging_time_s += stats.messaging_time_s;
              summary.latency_ms += stats.latency_ms;
              summary.iops += stats.iops;
              summary.throughput_mbps += stats.throughput_mbps;
            }
            summary.connect_time_s /= nr_clients;
            summary.messaging_time_s /= nr_clients;
            summary.latency_ms /= nr_clients;
            summary.report();
          });
        });
      }

      seastar::future<> connect_wait_verify(const entity_addr_t& peer_addr) {
        return container().invoke_on_all([peer_addr] (auto& client) {
          // start clients in active cores
          if (client.is_active()) {
            client.conn_stats.start_connecting();
            client.active_conn = client.msgr->connect(peer_addr, entity_name_t::TYPE_OSD);
            // make sure handshake won't hurt the performance
            return seastar::sleep(1s).then([&client] {
              if (client.conn_stats.connected_time == mono_clock::zero()) {
                logger().error("\n{} not connected after 1s!\n", client.lname);
                ceph_assert(false);
              }
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
        std::vector<PeriodStats> snaps;
        std::vector<ConnStats> summaries;
        std::optional<double> server_reactor_utilization;

       public:
        TimerReport(unsigned jobs, unsigned msgtime, unsigned bs)
          : jobs{jobs},
            msgtime{msgtime},
            bytes_of_block{bs},
            snaps{jobs},
            summaries{jobs} {}

        unsigned get_elapsed() const { return elapsed; }

        PeriodStats& get_snap_by_job(unsigned client_id) {
          return snaps[client_id];
        }

        ConnStats& get_summary_by_job(unsigned client_id) {
          return summaries[client_id];
        }

        void set_server_reactor_utilization(double ru) {
          server_reactor_utilization = ru;
        }

        bool should_stop() const {
          return elapsed >= msgtime;
        }

        seastar::future<> ticktock() {
          return seastar::sleep(1s).then([this] {
            ++elapsed;
          });
        }

        void report_header() const {
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
          std::chrono::duration<double> elapsed_d = 0s;
          unsigned depth = 0u;
          unsigned ops = 0u;
          unsigned sampled_count = 0u;
          double sampled_total_lat_s = 0.0;
          for (const auto& snap: snaps) {
            elapsed_d += (snap.finish_time - snap.start_time);
            depth += snap.depth;
            ops += (snap.finish_count - snap.start_count);
            sampled_count += snap.sampled_count;
            sampled_total_lat_s += snap.sampled_total_lat_s;
          }
          double elapsed_s = elapsed_d.count() / jobs;
          double iops = ops/elapsed_s;
          std::ostringstream sout;
          sout << setfill(' ')
               << std::setw(7) << elapsed_s
               << std::setw(6) << depth
               << std::setw(8) << iops
               << std::setw(8) << iops * bytes_of_block / 1048576
               << std::setw(8) << (sampled_total_lat_s / sampled_count * 1000)
               << " -- ";
          if (server_reactor_utilization.has_value()) {
            sout << *server_reactor_utilization << " -- ";
          }
          for (const auto& snap : snaps) {
            sout << snap.reactor_utilization << ",";
          }
          std::cout << sout.str() << std::endl;
        }

        void report_summary() const {
          std::chrono::duration<double> elapsed_d = 0s;
          unsigned ops = 0u;
          unsigned sampled_count = 0u;
          double sampled_total_lat_s = 0.0;
          for (const auto& summary: summaries) {
            elapsed_d += (summary.finish_time - summary.start_time);
            ops += (summary.received_count - summary.start_count);
            sampled_count += summary.sampled_count;
            sampled_total_lat_s += summary.sampled_total_lat_s;
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
               << std::setw(8) << (sampled_total_lat_s / sampled_count * 1000)
               << "\n";
          std::cout << sout.str() << std::endl;
        }
      };

      seastar::future<> report_period(TimerReport& report) {
        return container().invoke_on_all([&report] (auto& client) {
          if (client.is_active()) {
            PeriodStats& snap = report.get_snap_by_job(client.id);
            client.period_stats.reset_period(
                client.conn_stats.received_count,
                client.get_current_depth(),
                snap);
          }
          if (client.server_sid.has_value() &&
              seastar::this_shard_id() == *client.server_sid) {
            assert(!client.is_active());
            report.set_server_reactor_utilization(get_reactor_utilization());
          }
        }).then([&report] {
          report.report_period();
        });
      }

      seastar::future<> report_summary(TimerReport& report) {
        return container().invoke_on_all([&report] (auto& client) {
          if (client.is_active()) {
            ConnStats& summary = report.get_summary_by_job(client.id);
            summary.prepare_summary(client.conn_stats);
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
        }).then([ramptime] {
          logger().info("[all clients]: ramping up {} seconds...", ramptime);
          return seastar::sleep(std::chrono::seconds(ramptime));
        }).then([this] {
          return container().invoke_on_all([] (auto& client) {
            if (client.is_active()) {
              client.conn_stats.start_collect();
              client.period_stats.start_collect(client.conn_stats.received_count);
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
          auto m = crimson::make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0);
          bufferlist data(msg_data);
          m->write(0, msg_len, data);
          // use tid as the identity of each round
          m->set_tid(sent_count);

          // sample message latency
          if (unlikely(sent_count % SAMPLE_RATE == 0)) {
            auto index = sent_count % time_msgs_sent.size();
            ceph_assert(time_msgs_sent[index] == mono_clock::zero());
            time_msgs_sent[index] = mono_clock::now();
          }

          return conn->send(std::move(m));
        });
      }

      class DepthBroken: public std::exception {};

      seastar::future<ClientStats> stop_dispatch_messages() {
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
        }).then([this, conn] {
          logger().info("{}: stopped sending OSDOPs", *conn);

          std::chrono::duration<double> dur_conn = conn_stats.connected_time - conn_stats.connecting_time;
          std::chrono::duration<double> dur_msg = mono_clock::now() - conn_stats.start_time;
          unsigned ops = conn_stats.received_count - conn_stats.start_count;

          ClientStats stats;
          stats.name = lname;
          stats.depth = nr_depth;
          stats.connect_time_s = dur_conn.count();
          stats.total_msgs = ops;
          stats.messaging_time_s = dur_msg.count();
          stats.latency_ms =
            conn_stats.sampled_total_lat_s / conn_stats.sampled_count * 1000;
          stats.iops = ops / dur_msg.count();
          stats.throughput_mbps = ops / dur_msg.count() * msg_len / 1048576;

          stopped_send_promise.set_value(stats);
        });
      }
    };
  };

  std::optional<unsigned> server_sid;
  if (mode == perf_mode_t::both) {
    server_sid = server_conf.core;
  }
  return seastar::when_all(
      test_state::Server::create(
        server_conf.core,
        server_conf.block_size),
      create_sharded<test_state::Client>(
        client_conf.jobs,
        client_conf.block_size,
        client_conf.depth,
        server_sid),
      crimson::common::sharded_conf().start(
        EntityName{}, std::string_view{"ceph"}
      ).then([] {
        return crimson::common::local_conf().start();
      }).then([crc_enabled] {
        return crimson::common::local_conf().set_val(
            "ms_crc_data", crc_enabled ? "true" : "false");
      })
  ).then([=](auto&& ret) {
    auto fp_server = std::move(std::get<0>(ret).get0());
    auto client = std::move(std::get<1>(ret).get0());
    test_state::Server* server = fp_server.get();
    // reserve core 0 for potentially better performance
    if (mode == perf_mode_t::both) {
      logger().info("\nperf settings:\n  smp={}\n  {}\n  {}\n",
                    seastar::smp::count, client_conf.str(), server_conf.str());
      ceph_assert(seastar::smp::count > client_conf.jobs);
      ceph_assert(client_conf.jobs > 0);
      ceph_assert(seastar::smp::count > server_conf.core + client_conf.jobs);
      return seastar::when_all_succeed(
        server->init(server_conf.addr),
        client->init()
      ).then_unpack([client, addr = client_conf.server_addr] {
        return client->connect_wait_verify(addr);
      }).then([client, ramptime = client_conf.ramptime,
               msgtime = client_conf.msgtime] {
        return client->dispatch_with_timer(ramptime, msgtime);
      }).then([client] {
        return client->shutdown();
      }).then([server, fp_server = std::move(fp_server)] () mutable {
        return server->shutdown().then([cleanup = std::move(fp_server)] {});
      });
    } else if (mode == perf_mode_t::client) {
      logger().info("\nperf settings:\n  smp={}\n  {}\n",
                    seastar::smp::count, client_conf.str());
      ceph_assert(seastar::smp::count > client_conf.jobs);
      ceph_assert(client_conf.jobs > 0);
      return client->init(
      ).then([client, addr = client_conf.server_addr] {
        return client->connect_wait_verify(addr);
      }).then([client, ramptime = client_conf.ramptime,
               msgtime = client_conf.msgtime] {
        return client->dispatch_with_timer(ramptime, msgtime);
      }).then([client] {
        return client->shutdown();
      });
    } else { // mode == perf_mode_t::server
      ceph_assert(seastar::smp::count > server_conf.core);
      logger().info("\nperf settings:\n  smp={}\n  {}\n",
                    seastar::smp::count, server_conf.str());
      return server->init(server_conf.addr
      // dispatch ops
      ).then([server] {
        return server->wait();
      // shutdown
      }).then([server, fp_server = std::move(fp_server)] () mutable {
        return server->shutdown().then([cleanup = std::move(fp_server)] {});
      });
    }
  }).finally([] {
    return crimson::common::sharded_conf().stop();
  });
}

}

int main(int argc, char** argv)
{
  seastar::app_template app;
  app.add_options()
    ("mode", bpo::value<unsigned>()->default_value(0),
     "0: both, 1:client, 2:server")
    ("server-addr", bpo::value<std::string>()->default_value("v2:127.0.0.1:9010"),
     "server address(only support msgr v2 protocol)")
    ("ramptime", bpo::value<unsigned>()->default_value(5),
     "seconds of client ramp-up time")
    ("msgtime", bpo::value<unsigned>()->default_value(15),
     "seconds of client messaging time")
    ("client-jobs", bpo::value<unsigned>()->default_value(1),
     "number of client jobs (messengers)")
    ("client-bs", bpo::value<unsigned>()->default_value(4096),
     "client block size")
    ("depth", bpo::value<unsigned>()->default_value(512),
     "client io depth")
    ("server-core", bpo::value<unsigned>()->default_value(1),
     "server running core")
    ("server-bs", bpo::value<unsigned>()->default_value(0),
     "server block size")
    ("crc-enabled", bpo::value<bool>()->default_value(false),
     "enable CRC checks");
  return app.run(argc, argv, [&app] {
      auto&& config = app.configuration();
      auto mode = config["mode"].as<unsigned>();
      ceph_assert(mode <= 2);
      auto _mode = static_cast<perf_mode_t>(mode);
      bool crc_enabled = config["crc-enabled"].as<bool>();
      auto server_conf = server_config::load(config);
      auto client_conf = client_config::load(config);
      return run(_mode, client_conf, server_conf, crc_enabled
      ).then([] {
          logger().info("\nsuccessful!\n");
        }).handle_exception([] (auto eptr) {
          logger().info("\nfailed!\n");
          return seastar::make_exception_future<>(eptr);
        });
    });
}
