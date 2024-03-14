// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <map>
#include <boost/program_options.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include <seastar/core/app-template.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>

#include "common/ceph_time.h"
#include "messages/MOSDOp.h"
#include "include/random.h"

#include "crimson/auth/DummyAuth.h"
#include "crimson/common/log.h"
#include "crimson/common/config_proxy.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Messenger.h"
#include "crimson/osd/stop_signal.h"

using namespace std;
using namespace std::chrono_literals;

using lowres_clock_t = seastar::lowres_system_clock;

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
  unsigned num_clients;
  unsigned num_conns;
  unsigned depth;
  bool skip_core_0;

  std::string str() const {
    std::ostringstream out;
    out << "client[>> " << server_addr
        << "](bs=" << block_size
        << ", ramptime=" << ramptime
        << ", msgtime=" << msgtime
        << ", num_clients=" << num_clients
        << ", num_conns=" << num_conns
        << ", depth=" << depth
        << ", skip_core_0=" << skip_core_0
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
    conf.num_clients = options["clients"].as<unsigned>();
    ceph_assert_always(conf.num_clients > 0);
    conf.num_conns = options["conns-per-client"].as<unsigned>();
    ceph_assert_always(conf.num_conns > 0);
    conf.depth = options["depth"].as<unsigned>();
    conf.skip_core_0 = options["client-skip-core-0"].as<bool>();
    return conf;
  }
};

struct server_config {
  entity_addr_t addr;
  unsigned block_size;
  bool is_fixed_cpu;
  unsigned core;

  std::string str() const {
    std::ostringstream out;
    out << "server[" << addr
        << "](bs=" << block_size
        << ", is_fixed_cpu=" << is_fixed_cpu
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
    conf.is_fixed_cpu = options["server-fixed-cpu"].as<bool>();
    conf.core = options["server-core"].as<unsigned>();
    return conf;
  }
};

const unsigned SAMPLE_RATE = 256;

static seastar::future<> run(
    perf_mode_t mode,
    const client_config& client_conf,
    const server_config& server_conf,
    bool crc_enabled)
{
  struct test_state {
    struct Server final
        : public crimson::net::Dispatcher,
          public seastar::peering_sharded_service<Server> {
      // available only in msgr_sid
      crimson::net::MessengerRef msgr;
      crimson::auth::DummyAuthClientServer dummy_auth;
      const seastar::shard_id msgr_sid;
      std::string lname;

      bool is_fixed_cpu = true;
      bool is_stopped = false;
      std::optional<seastar::future<>> fut_report;

      unsigned conn_count = 0;
      unsigned msg_count = 0;
      MessageRef last_msg;

      // available in all shards
      unsigned msg_len;
      bufferlist msg_data;

      Server(seastar::shard_id msgr_sid, unsigned msg_len, bool needs_report)
        : msgr_sid{msgr_sid},
          msg_len{msg_len} {
        lname = fmt::format("server@{}", msgr_sid);
        msg_data.append_zero(msg_len);

        if (seastar::this_shard_id() == msgr_sid &&
            needs_report) {
          start_report();
        }
      }

      void ms_handle_connect(
          crimson::net::ConnectionRef,
          seastar::shard_id) override {
        ceph_abort("impossible, server won't connect");
      }

      void ms_handle_accept(
          crimson::net::ConnectionRef,
          seastar::shard_id new_shard,
          bool is_replace) override {
        ceph_assert_always(new_shard == seastar::this_shard_id());
        auto &server = container().local();
        ++server.conn_count;
      }

      void ms_handle_reset(
          crimson::net::ConnectionRef,
          bool) override {
        auto &server = container().local();
        --server.conn_count;
      }

      std::optional<seastar::future<>> ms_dispatch(
          crimson::net::ConnectionRef c, MessageRef m) override {
        assert(c->get_shard_id() == seastar::this_shard_id());
        ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);

        auto &server = container().local();

        // server replies with MOSDOp to generate server-side write workload
        const static pg_t pgid;
        const static object_locator_t oloc;
        const static hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                                    pgid.pool(), oloc.nspace);
        static spg_t spgid(pgid);
        auto rep = crimson::make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0);
        bufferlist data(server.msg_data);
        rep->write(0, server.msg_len, data);
        rep->set_tid(m->get_tid());
        ++server.msg_count;
        std::ignore = c->send(std::move(rep));

        if (server.msg_count % 16 == 0) {
          server.last_msg = std::move(m);
        }
        return {seastar::now()};
      }

      seastar::future<> init(const entity_addr_t& addr, bool is_fixed_cpu) {
        return container().invoke_on(
            msgr_sid, [addr, is_fixed_cpu](auto &server) {
          // server msgr is always with nonce 0
          server.msgr = crimson::net::Messenger::create(
              entity_name_t::OSD(server.msgr_sid),
              server.lname, 0, is_fixed_cpu);
          server.msgr->set_default_policy(crimson::net::SocketPolicy::stateless_server(0));
          server.msgr->set_auth_client(&server.dummy_auth);
          server.msgr->set_auth_server(&server.dummy_auth);
          server.is_fixed_cpu = is_fixed_cpu;
          return server.msgr->bind(entity_addrvec_t{addr}
          ).safe_then([&server] {
            return server.msgr->start({&server});
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
        return container().invoke_on(
            msgr_sid, [](auto &server) {
          server.is_stopped = true;
          ceph_assert(server.msgr);
          server.msgr->stop();
          return server.msgr->shutdown(
          ).then([&server] {
            if (server.fut_report.has_value()) {
              return std::move(server.fut_report.value());
            } else {
              return seastar::now();
            }
          });
        });
      }

    private:
      struct ShardReport {
        unsigned msg_count = 0;

        // per-interval metrics
        double reactor_utilization;
        unsigned conn_count = 0;
        int msg_size = 0;
        unsigned msg_count_interval = 0;
      };

      // should not be called frequently to impact performance
      void get_report(ShardReport& last) {
        unsigned last_msg_count = last.msg_count;
        int msg_size = -1;
        if (last_msg) {
          auto msg = boost::static_pointer_cast<MOSDOp>(last_msg);
          msg->finish_decode();
          ceph_assert_always(msg->ops.size() == 1);
          msg_size = msg->ops[0].op.extent.length;
          last_msg.reset();
        }

        last.msg_count = msg_count;
        last.reactor_utilization = get_reactor_utilization();
        last.conn_count = conn_count;
        last.msg_size = msg_size;
        last.msg_count_interval = msg_count - last_msg_count;
      }

      struct TimerReport {
        unsigned elapsed = 0u;
        mono_time start_time = mono_clock::zero();
        std::vector<ShardReport> reports;

        TimerReport(unsigned shards) : reports(shards) {}
      };

      void start_report() {
        seastar::promise<> pr_report;
        fut_report = pr_report.get_future();
        seastar::do_with(
            TimerReport(seastar::smp::count),
            [this](auto &report) {
          return seastar::do_until(
            [this] { return is_stopped; },
            [&report, this] {
              return seastar::sleep(2s
              ).then([&report, this] {
                report.elapsed += 2;
                if (is_fixed_cpu) {
                  return seastar::smp::submit_to(msgr_sid,
                      [&report, this] {
                    auto &server = container().local();
                    server.get_report(report.reports[seastar::this_shard_id()]);
                  }).then([&report, this] {
                    auto now = mono_clock::now();
                    auto prv = report.start_time;
                    report.start_time = now;
                    if (prv == mono_clock::zero()) {
                      // cannot compute duration
                      return;
                    }
                    std::chrono::duration<double> duration_d = now - prv;
                    double duration = duration_d.count();
                    auto &ireport = report.reports[msgr_sid];
                    double iops = ireport.msg_count_interval / duration;
                    double throughput_MB = -1;
                    if (ireport.msg_size >= 0) {
                      throughput_MB = iops * ireport.msg_size / 1048576;
                    }
                    std::ostringstream sout;
                    sout << setfill(' ')
                         << report.elapsed
                         << "(" << std::setw(5) << duration << ") "
                         << std::setw(9) << iops << "IOPS "
                         << std::setw(8) << throughput_MB << "MiB/s "
                         << ireport.reactor_utilization
                         << "(" << ireport.conn_count << ")";
                    std::cout << sout.str() << std::endl;
                  });
                } else {
                  return seastar::smp::invoke_on_all([&report, this] {
                    auto &server = container().local();
                    server.get_report(report.reports[seastar::this_shard_id()]);
                  }).then([&report, this] {
                    auto now = mono_clock::now();
                    auto prv = report.start_time;
                    report.start_time = now;
                    if (prv == mono_clock::zero()) {
                      // cannot compute duration
                      return;
                    }
                    std::chrono::duration<double> duration_d = now - prv;
                    double duration = duration_d.count();
                    unsigned num_msgs = 0;
                    // -1 means unavailable, -2 means mismatch
                    int msg_size = -1;
                    for (auto &i : report.reports) {
                      if (i.msg_size >= 0) {
                        if (msg_size == -2) {
                          // pass
                        } else if (msg_size == -1) {
                          msg_size = i.msg_size;
                        } else {
                          if (msg_size != i.msg_size) {
                            msg_size = -2;
                          }
                        }
                      }
                      num_msgs += i.msg_count_interval;
                    }
                    double iops = num_msgs / duration;
                    double throughput_MB = msg_size;
                    if (msg_size >= 0) {
                      throughput_MB = iops * msg_size / 1048576;
                    }
                    std::ostringstream sout;
                    sout << setfill(' ')
                         << report.elapsed
                         << "(" << std::setw(5) << duration << ") "
                         << std::setw(9) << iops << "IOPS "
                         << std::setw(8) << throughput_MB << "MiB/s ";
                    for (auto &i : report.reports) {
                      sout << i.reactor_utilization
                           << "(" << i.conn_count << ") ";
                    }
                    std::cout << sout.str() << std::endl;
                  });
                }
              });
            }
          );
        }).then([this] {
          logger().info("report is stopped!");
        }).forward_to(std::move(pr_report));
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
          ceph_assert_always(connected_time == mono_clock::zero());
          connected_time = mono_clock::now();
        }

        void start_collect() {
          ceph_assert_always(connected_time != mono_clock::zero());
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

      struct PeriodStats {
        mono_time start_time = mono_clock::zero();
        unsigned start_count = 0u;
        unsigned sampled_count = 0u;
        double sampled_total_lat_s = 0.0;

        // for reporting only
        mono_time finish_time = mono_clock::zero();
        unsigned finish_count = 0u;
        unsigned depth = 0u;

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

          start_collect(received_count);
        }
      };

      struct JobReport {
        std::string name;
        unsigned depth = 0;
        double connect_time_s = 0;
        unsigned total_msgs = 0;
        double messaging_time_s = 0;
        double latency_ms = 0;
        double iops = 0;
        double throughput_mbps = 0;

        void account(const JobReport &stats) {
          depth += stats.depth;
          connect_time_s += stats.connect_time_s;
          total_msgs += stats.total_msgs;
          messaging_time_s += stats.messaging_time_s;
          latency_ms += stats.latency_ms;
          iops += stats.iops;
          throughput_mbps += stats.throughput_mbps;
        }

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

      struct ConnectionPriv : public crimson::net::Connection::user_private_t {
        unsigned index;
        ConnectionPriv(unsigned i) : index{i} {}
      };

      struct ConnState {
        crimson::net::MessengerRef msgr;
        ConnStats conn_stats;
        PeriodStats period_stats;
        seastar::semaphore depth;
        std::vector<lowres_clock_t::time_point> time_msgs_sent;
        unsigned sent_count = 0u;
        crimson::net::ConnectionRef active_conn;
        bool stop_send = false;
        seastar::promise<JobReport> stopped_send_promise;

        ConnState(std::size_t _depth)
          : depth{_depth},
            time_msgs_sent{_depth, lowres_clock_t::time_point::min()} {}

        unsigned get_current_units() const {
          ceph_assert(depth.available_units() >= 0);
          return depth.current();
        }

        seastar::future<JobReport> stop_dispatch_messages() {
          stop_send = true;
          depth.broken(DepthBroken());
          return stopped_send_promise.get_future();
        }
      };

      const seastar::shard_id sid;
      const unsigned id;
      const std::optional<unsigned> server_sid;

      const unsigned num_clients;
      const unsigned num_conns;
      const unsigned msg_len;
      bufferlist msg_data;
      const unsigned nr_depth;
      const unsigned nonce_base;
      crimson::auth::DummyAuthClientServer dummy_auth;

      std::vector<ConnState> conn_states;

      Client(unsigned num_clients,
             unsigned num_conns,
             unsigned msg_len,
             unsigned _depth,
             unsigned nonce_base,
             std::optional<unsigned> server_sid)
        : sid{seastar::this_shard_id()},
          id{sid + num_clients - seastar::smp::count},
          server_sid{server_sid},
          num_clients{num_clients},
          num_conns{num_conns},
          msg_len{msg_len},
          nr_depth{_depth},
          nonce_base{nonce_base} {
        if (is_active()) {
          for (unsigned i = 0; i < num_conns; ++i) {
            conn_states.emplace_back(nr_depth);
          }
        }
        msg_data.append_zero(msg_len);
      }

      std::string get_name(unsigned i) {
        return fmt::format("client{}Conn{}@{}", id, i, sid);
      }

      void ms_handle_connect(
          crimson::net::ConnectionRef conn,
          seastar::shard_id prv_shard) override {
        ceph_assert_always(prv_shard == seastar::this_shard_id());
        assert(is_active());
        unsigned index = static_cast<ConnectionPriv&>(conn->get_user_private()).index;
        auto &conn_state = conn_states[index];
        conn_state.conn_stats.finish_connecting();
      }

      std::optional<seastar::future<>> ms_dispatch(
          crimson::net::ConnectionRef conn, MessageRef m) override {
        assert(is_active());
        // server replies with MOSDOp to generate server-side write workload
        ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);

        unsigned index = static_cast<ConnectionPriv&>(conn->get_user_private()).index;
        assert(index < num_conns);
        auto &conn_state = conn_states[index];

        auto msg_id = m->get_tid();
        if (msg_id % SAMPLE_RATE == 0) {
          auto msg_index = msg_id % conn_state.time_msgs_sent.size();
          ceph_assert(conn_state.time_msgs_sent[msg_index] !=
              lowres_clock_t::time_point::min());
          std::chrono::duration<double> cur_latency =
              lowres_clock_t::now() - conn_state.time_msgs_sent[msg_index];
          conn_state.conn_stats.sampled_total_lat_s += cur_latency.count();
          ++(conn_state.conn_stats.sampled_count);
          conn_state.period_stats.sampled_total_lat_s += cur_latency.count();
          ++(conn_state.period_stats.sampled_count);
          conn_state.time_msgs_sent[msg_index] = lowres_clock_t::time_point::min();
        }

        ++(conn_state.conn_stats.received_count);
        conn_state.depth.signal(1);

        return {seastar::now()};
      }

      // should start messenger at this shard?
      bool is_active() {
        ceph_assert(seastar::this_shard_id() == sid);
        return sid + num_clients >= seastar::smp::count;
      }

      seastar::future<> init() {
        return container().invoke_on_all([](auto& client) {
          if (client.is_active()) {
            return seastar::do_for_each(
                boost::make_counting_iterator(0u),
                boost::make_counting_iterator(client.num_conns),
                [&client](auto i) {
              auto &conn_state = client.conn_states[i];
              std::string name = client.get_name(i);
              conn_state.msgr = crimson::net::Messenger::create(
                  entity_name_t::OSD(client.id * client.num_conns + i),
                  name, client.nonce_base + client.id * client.num_conns + i, true);
              conn_state.msgr->set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
              conn_state.msgr->set_auth_client(&client.dummy_auth);
              conn_state.msgr->set_auth_server(&client.dummy_auth);
              return conn_state.msgr->start({&client});
            });
          }
          return seastar::now();
        });
      }

      seastar::future<> shutdown() {
        return seastar::do_with(
            std::vector<JobReport>(num_clients * num_conns),
            [this](auto &all_stats) {
          return container().invoke_on_all([&all_stats](auto& client) {
            if (!client.is_active()) {
              return seastar::now();
            }

            return seastar::parallel_for_each(
                boost::make_counting_iterator(0u),
                boost::make_counting_iterator(client.num_conns),
                [&all_stats, &client](auto i) {
              logger().info("{} shutdown...", client.get_name(i));
              auto &conn_state = client.conn_states[i];
              return conn_state.stop_dispatch_messages(
              ).then([&all_stats, &client, i](auto stats) {
                all_stats[client.id * client.num_conns + i] = stats;
              });
            }).then([&client] {
              return seastar::do_for_each(
                  boost::make_counting_iterator(0u),
                  boost::make_counting_iterator(client.num_conns),
                  [&client](auto i) {
                auto &conn_state = client.conn_states[i];
                ceph_assert(conn_state.msgr);
                conn_state.msgr->stop();
                return conn_state.msgr->shutdown();
              });
            });
          }).then([&all_stats, this] {
            auto nr_jobs = all_stats.size();
            JobReport summary;
            std::vector<JobReport> clients(num_clients);

            for (unsigned i = 0; i < nr_jobs; ++i) {
              auto &stats = all_stats[i];
              stats.report();
              clients[i / num_conns].account(stats);
              summary.account(stats);
            }

            std::cout << std::endl;
            std::cout << "per client:" << std::endl;
            for (unsigned i = 0; i < num_clients; ++i) {
              auto &stats = clients[i];
              stats.name = fmt::format("client{}", i);
              stats.connect_time_s /= num_conns;
              stats.messaging_time_s /= num_conns;
              stats.latency_ms /= num_conns;
              stats.report();
            }

            std::cout << std::endl;
            summary.name = fmt::format("all", nr_jobs);
            summary.connect_time_s /= nr_jobs;
            summary.messaging_time_s /= nr_jobs;
            summary.latency_ms /= nr_jobs;
            summary.report();
          });
        });
      }

      seastar::future<> connect_wait_verify(const entity_addr_t& peer_addr) {
        return container().invoke_on_all([peer_addr](auto& client) {
          // start clients in active cores
          if (client.is_active()) {
            for (unsigned i = 0; i < client.num_conns; ++i) {
              auto &conn_state = client.conn_states[i];
              conn_state.conn_stats.start_connecting();
              conn_state.active_conn = conn_state.msgr->connect(peer_addr, entity_name_t::TYPE_OSD);
              conn_state.active_conn->set_user_private(
                  std::make_unique<ConnectionPriv>(i));
            }
            // make sure handshake won't hurt the performance
            return seastar::sleep(1s).then([&client] {
              for (unsigned i = 0; i < client.num_conns; ++i) {
                auto &conn_state = client.conn_states[i];
                if (conn_state.conn_stats.connected_time == mono_clock::zero()) {
                  logger().error("\n{} not connected after 1s!\n",
                                 client.get_name(i));
                  ceph_assert(false);
                }
              }
            });
          }
          return seastar::now();
        });
      }

     private:
      class TimerReport {
       private:
        const unsigned num_clients;
        const unsigned num_conns;
        const unsigned msgtime;
        const unsigned bytes_of_block;

        unsigned elapsed = 0u;
        std::vector<PeriodStats> snaps;
        std::vector<ConnStats> summaries;
        std::vector<double> client_reactor_utilizations;
        std::optional<double> server_reactor_utilization;

       public:
        TimerReport(unsigned num_clients, unsigned num_conns, unsigned msgtime, unsigned bs)
          : num_clients{num_clients},
            num_conns{num_conns},
            msgtime{msgtime},
            bytes_of_block{bs},
            snaps{num_clients * num_conns},
            summaries{num_clients * num_conns},
            client_reactor_utilizations(num_clients) {}

        unsigned get_elapsed() const { return elapsed; }

        PeriodStats& get_snap(unsigned client_id, unsigned i) {
          return snaps[client_id * num_conns + i];
        }

        ConnStats& get_summary(unsigned client_id, unsigned i) {
          return summaries[client_id * num_conns + i];
        }

        void set_client_reactor_utilization(unsigned client_id, double ru) {
          client_reactor_utilizations[client_id] = ru;
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
               << std::setw(6) << "sec"
               << std::setw(7) << "depth"
               << std::setw(10) << "IOPS"
               << std::setw(9) << "MB/s"
               << std::setw(9) << "lat(ms)";
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
          double elapsed_s = elapsed_d.count() / (num_clients * num_conns);
          double iops = ops/elapsed_s;
          std::ostringstream sout;
          sout << setfill(' ')
               << std::setw(5) << elapsed_s
               << " "
               << std::setw(6) << depth
               << " "
               << std::setw(9) << iops
               << " "
               << std::setw(8) << iops * bytes_of_block / 1048576
               << " "
               << std::setw(8) << (sampled_total_lat_s / sampled_count * 1000)
               << " -- ";
          if (server_reactor_utilization.has_value()) {
            sout << *server_reactor_utilization << " -- ";
          }
          for (double cru : client_reactor_utilizations) {
            sout << cru << ",";
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
          double elapsed_s = elapsed_d.count() / (num_clients * num_conns);
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
            for (unsigned i = 0; i < client.num_conns; ++i) {
              auto &conn_state = client.conn_states[i];
              PeriodStats& snap = report.get_snap(client.id, i);
              conn_state.period_stats.reset_period(
                  conn_state.conn_stats.received_count,
                  client.nr_depth - conn_state.get_current_units(),
                  snap);
            }
            report.set_client_reactor_utilization(client.id, get_reactor_utilization());
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
            for (unsigned i = 0; i < client.num_conns; ++i) {
              auto &conn_state = client.conn_states[i];
              ConnStats& summary = report.get_summary(client.id, i);
              summary.prepare_summary(conn_state.conn_stats);
            }
          }
        }).then([&report] {
          report.report_summary();
        });
      }

     public:
      seastar::future<> dispatch_with_timer(unsigned ramptime, unsigned msgtime) {
        logger().info("[all clients]: start sending MOSDOps from {} clients * {} conns",
                      num_clients, num_conns);
        return container().invoke_on_all([] (auto& client) {
          if (client.is_active()) {
            for (unsigned i = 0; i < client.num_conns; ++i) {
              client.do_dispatch_messages(i);
            }
          }
        }).then([ramptime] {
          logger().info("[all clients]: ramping up {} seconds...", ramptime);
          return seastar::sleep(std::chrono::seconds(ramptime));
        }).then([this] {
          return container().invoke_on_all([] (auto& client) {
            if (client.is_active()) {
              for (unsigned i = 0; i < client.num_conns; ++i) {
                auto &conn_state = client.conn_states[i];
                conn_state.conn_stats.start_collect();
                conn_state.period_stats.start_collect(conn_state.conn_stats.received_count);
              }
            }
          });
        }).then([this, msgtime] {
          logger().info("[all clients]: reporting {} seconds...\n", msgtime);
          return seastar::do_with(
              TimerReport(num_clients, num_conns, msgtime, msg_len),
              [this](auto& report) {
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
      seastar::future<> send_msg(ConnState &conn_state) {
        ceph_assert(seastar::this_shard_id() == sid);
        conn_state.sent_count += 1;
        return conn_state.depth.wait(1
        ).then([this, &conn_state] {
          const static pg_t pgid;
          const static object_locator_t oloc;
          const static hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                                      pgid.pool(), oloc.nspace);
          static spg_t spgid(pgid);
          auto m = crimson::make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0);
          bufferlist data(msg_data);
          m->write(0, msg_len, data);
          // use tid as the identity of each round
          m->set_tid(conn_state.sent_count);

          // sample message latency
          if (unlikely(conn_state.sent_count % SAMPLE_RATE == 0)) {
            auto index = conn_state.sent_count % conn_state.time_msgs_sent.size();
            ceph_assert(conn_state.time_msgs_sent[index] ==
                        lowres_clock_t::time_point::min());
            conn_state.time_msgs_sent[index] = lowres_clock_t::now();
          }

          return conn_state.active_conn->send(std::move(m));
        });
      }

      class DepthBroken: public std::exception {};

      seastar::future<JobReport> stop_dispatch_messages(unsigned i) {
        auto &conn_state = conn_states[i];
        conn_state.stop_send = true;
        conn_state.depth.broken(DepthBroken());
        return conn_state.stopped_send_promise.get_future();
      }

      void do_dispatch_messages(unsigned i) {
        ceph_assert(seastar::this_shard_id() == sid);
        auto &conn_state = conn_states[i];
        ceph_assert(conn_state.sent_count == 0);
        conn_state.conn_stats.start_time = mono_clock::now();
        // forwarded to stopped_send_promise
        (void) seastar::do_until(
          [&conn_state] { return conn_state.stop_send; },
          [this, &conn_state] { return send_msg(conn_state); }
        ).handle_exception_type([] (const DepthBroken& e) {
          // ok, stopped by stop_dispatch_messages()
        }).then([this, &conn_state, i] {
          std::string name = get_name(i);
          logger().info("{} {}: stopped sending OSDOPs",
                        name, *conn_state.active_conn);

          std::chrono::duration<double> dur_conn =
              conn_state.conn_stats.connected_time -
              conn_state.conn_stats.connecting_time;
          std::chrono::duration<double> dur_msg =
              mono_clock::now() - conn_state.conn_stats.start_time;
          unsigned ops =
              conn_state.conn_stats.received_count -
              conn_state.conn_stats.start_count;

          JobReport stats;
          stats.name = name;
          stats.depth = nr_depth;
          stats.connect_time_s = dur_conn.count();
          stats.total_msgs = ops;
          stats.messaging_time_s = dur_msg.count();
          stats.latency_ms =
              conn_state.conn_stats.sampled_total_lat_s /
              conn_state.conn_stats.sampled_count * 1000;
          stats.iops = ops / dur_msg.count();
          stats.throughput_mbps = ops / dur_msg.count() * msg_len / 1048576;

          conn_state.stopped_send_promise.set_value(stats);
        });
      }
    };
  };

  std::optional<unsigned> server_sid;
  bool server_needs_report = false;
  if (mode == perf_mode_t::both) {
    ceph_assert(server_conf.is_fixed_cpu == true);
    server_sid = server_conf.core;
  } else if (mode == perf_mode_t::server) {
    server_needs_report = true;
  }
  return seastar::when_all(
    seastar::futurize_invoke([mode, server_conf, server_needs_report] {
      if (mode == perf_mode_t::client) {
        return seastar::make_ready_future<test_state::Server*>(nullptr);
      } else {
        return create_sharded<test_state::Server>(
          server_conf.core,
          server_conf.block_size,
          server_needs_report);
      }
    }),
    seastar::futurize_invoke([mode, client_conf, server_sid] {
      if (mode == perf_mode_t::server) {
        return seastar::make_ready_future<test_state::Client*>(nullptr);
      } else {
        unsigned nonce_base = ceph::util::generate_random_number<unsigned>();
        logger().info("client nonce_base={}", nonce_base);
        return create_sharded<test_state::Client>(
          client_conf.num_clients,
          client_conf.num_conns,
          client_conf.block_size,
          client_conf.depth,
          nonce_base,
          server_sid);
      }
    }),
    crimson::common::sharded_conf().start(
      EntityName{}, std::string_view{"ceph"}
    ).then([] {
      return crimson::common::local_conf().start();
    }).then([crc_enabled] {
      return crimson::common::local_conf().set_val(
          "ms_crc_data", crc_enabled ? "true" : "false");
    })
  ).then([=](auto&& ret) {
    auto server = std::move(std::get<0>(ret).get0());
    auto client = std::move(std::get<1>(ret).get0());
    // reserve core 0 for potentially better performance
    if (mode == perf_mode_t::both) {
      logger().info("\nperf settings:\n  smp={}\n  {}\n  {}\n",
                    seastar::smp::count, client_conf.str(), server_conf.str());
      if (client_conf.skip_core_0) {
        ceph_assert(seastar::smp::count > client_conf.num_clients);
      } else {
        ceph_assert(seastar::smp::count >= client_conf.num_clients);
      }
      ceph_assert(client_conf.num_clients > 0);
      ceph_assert(seastar::smp::count > server_conf.core + client_conf.num_clients);
      return seastar::when_all_succeed(
        // it is not reasonable to allow server/client to shared cores for
        // performance benchmarking purposes.
        server->init(server_conf.addr, server_conf.is_fixed_cpu),
        client->init()
      ).then_unpack([client, addr = client_conf.server_addr] {
        return client->connect_wait_verify(addr);
      }).then([client, ramptime = client_conf.ramptime,
               msgtime = client_conf.msgtime] {
        return client->dispatch_with_timer(ramptime, msgtime);
      }).then([client] {
        return client->shutdown();
      }).then([server] {
        return server->shutdown();
      });
    } else if (mode == perf_mode_t::client) {
      logger().info("\nperf settings:\n  smp={}\n  {}\n",
                    seastar::smp::count, client_conf.str());
      if (client_conf.skip_core_0) {
        ceph_assert(seastar::smp::count > client_conf.num_clients);
      } else {
        ceph_assert(seastar::smp::count >= client_conf.num_clients);
      }
      ceph_assert(client_conf.num_clients > 0);
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
      return seastar::async([server, server_conf] {
        // FIXME: SIGINT is not received by stop_signal
        seastar_apps_lib::stop_signal should_stop;
        server->init(server_conf.addr, server_conf.is_fixed_cpu).get();
        should_stop.wait().get();
        server->shutdown().get();
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
    ("clients", bpo::value<unsigned>()->default_value(1),
     "number of client messengers")
    ("conns-per-client", bpo::value<unsigned>()->default_value(1),
     "number of connections per client")
    ("client-bs", bpo::value<unsigned>()->default_value(4096),
     "client block size")
    ("depth", bpo::value<unsigned>()->default_value(512),
     "client io depth per job")
    ("client-skip-core-0", bpo::value<bool>()->default_value(true),
     "client skip core 0")
    ("server-fixed-cpu", bpo::value<bool>()->default_value(true),
     "server is in the fixed cpu mode, non-fixed doesn't support the mode both")
    ("server-core", bpo::value<unsigned>()->default_value(1),
     "server messenger running core")
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
