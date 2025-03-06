// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/timer.hh>
#include <seastar/core/shared_mutex.hh>

#include "crimson/common/gated.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Fwd.h"
#include "mgr/DaemonHealthMetric.h"
#include "mon/MgrMap.h"
#include "mgr/MetricTypes.h"

template<typename Message> using Ref = boost::intrusive_ptr<Message>;
namespace crimson::net {
  class Messenger;
}

class MMgrMap;
class MMgrConfigure;

namespace crimson::mgr
{

// implement WithStats if you want to report stats to mgr periodically
class WithStats {
public:
  virtual seastar::future<MessageURef> get_stats() = 0;
  virtual ~WithStats() {}
};

class Client : public crimson::net::Dispatcher {
  using get_perf_report_cb_t = std::function<seastar::future<MetricPayload> ()>;
  using set_perf_queries_cb_t =
    std::function<seastar::future<> (const ConfigPayload &)>;
public:
  Client(crimson::net::Messenger& msgr,
	 WithStats& with_stats,
	 set_perf_queries_cb_t cb_set,
	 get_perf_report_cb_t cb_get);
  seastar::future<> start();
  seastar::future<> stop();
  seastar::future<> send(MessageURef msg);
  void report();
  void update_daemon_health(std::vector<DaemonHealthMetric>&& metrics);

private:
  std::optional<seastar::future<>> ms_dispatch(
      crimson::net::ConnectionRef conn, Ref<Message> m) override;
  void ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace) final;
  void ms_handle_connect(crimson::net::ConnectionRef conn, seastar::shard_id) final;
  seastar::future<> handle_mgr_map(crimson::net::ConnectionRef conn,
				   Ref<MMgrMap> m);
  seastar::future<> handle_mgr_conf(crimson::net::ConnectionRef conn,
				    Ref<MMgrConfigure> m);
  seastar::future<> reconnect();
  seastar::future<> retry_interval();

  void print(std::ostream&) const;
  friend std::ostream& operator<<(std::ostream& out, const Client& client);
private:
  MgrMap mgrmap;
  crimson::net::Messenger& msgr;
  WithStats& with_stats;
  crimson::net::ConnectionRef conn;
  seastar::shared_mutex conn_lock;
  seastar::timer<seastar::lowres_clock> report_timer;
  crimson::common::gate_per_shard gates;
  uint64_t last_config_bl_version = 0;
  std::string service_name, daemon_name;
  set_perf_queries_cb_t set_perf_queries_cb;
  get_perf_report_cb_t get_perf_report_cb;

  std::vector<DaemonHealthMetric> daemon_health_metrics;

  void _send_report();
};

inline std::ostream& operator<<(std::ostream& out, const Client& client) {
  client.print(out);
  return out;
}

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::mgr::Client> : fmt::ostream_formatter {};
#endif
