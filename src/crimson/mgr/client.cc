// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "client.h"

#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>

#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/common/coroutine.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrOpen.h"
#include "messages/MMgrReport.h"

SET_SUBSYS(mgrc);

using crimson::common::local_conf;

namespace crimson::mgr
{

Client::Client(crimson::net::Messenger& msgr,
	       WithStats& with_stats,
	       set_perf_queries_cb_t cb_set,
	       get_perf_report_cb_t cb_get)
  : msgr{msgr},
    with_stats{with_stats},
    report_timer{[this] {report();}},
    set_perf_queries_cb(cb_set),
    get_perf_report_cb(cb_get)
{}

seastar::future<> Client::start()
{
  LOG_PREFIX(Client::start);
  DEBUGDPP("", *this);
  co_return;
}

seastar::future<> Client::stop()
{
  LOG_PREFIX(Client::stop);
  DEBUGDPP("", *this);
  report_timer.cancel();
  if (conn) {
    DEBUGDPP("marking down", *this);
    conn->mark_down();
  }
  co_await gates.close_all();
}

seastar::future<> Client::send(MessageURef msg)
{
  LOG_PREFIX(Client::send);
  DEBUGDPP("{}", *this, *msg);
  if (!conn_lock.try_lock_shared()) {
    WARNDPP("ongoing reconnect, report skipped", *this, *msg);
    co_return;
  }
  auto unlocker = seastar::defer([this] {
    conn_lock.unlock_shared();
  });
  if (!conn) {
    WARNDPP("no conn available, report skipped", *this, *msg);
    co_return;
  }
  DEBUGDPP("sending {}", *this, *msg);
  co_await conn->send(std::move(msg));
}

std::optional<seastar::future<>>
Client::ms_dispatch(crimson::net::ConnectionRef conn, MessageRef m)
{
  LOG_PREFIX(Client::ms_dispatch);
  DEBUGDPP("{}", *this, *m);
  bool dispatched = true;
  gates.dispatch_in_background(__func__, *this,
  [this, conn, &m, &dispatched, FNAME] {
    DEBUGDPP("dispatching in background {}", *this, *m);
    switch(m->get_type()) {
    case MSG_MGR_MAP:
      return handle_mgr_map(conn, boost::static_pointer_cast<MMgrMap>(m));
    case MSG_MGR_CONFIGURE:
      return handle_mgr_conf(conn, boost::static_pointer_cast<MMgrConfigure>(m));
    default:
      dispatched = false;
      return seastar::now();
    }
  });
  return (dispatched ? std::make_optional(seastar::now()) : std::nullopt);
}

void Client::ms_handle_connect(
    crimson::net::ConnectionRef c,
    seastar::shard_id prv_shard)
{
  LOG_PREFIX(Client::ms_handle_connect);
  DEBUGDPP("prev_shard: {}", *this, prv_shard);
  ceph_assert_always(prv_shard == seastar::this_shard_id());
  gates.dispatch_in_background(__func__, *this,
  [this, c, FNAME] {
    if (conn == c) {
      DEBUGDPP("dispatching in background", *this);
      // ask for the mgrconfigure message
      auto m = crimson::make_message<MMgrOpen>();
      m->daemon_name = local_conf()->name.get_id();
      local_conf().get_config_bl(0, &m->config_bl, &last_config_bl_version);
      local_conf().get_defaults_bl(&m->config_defaults_bl);
      return send(std::move(m));
    } else {
      DEBUGDPP("connection changed", *this);
      return seastar::now();
    }
  });
}

void Client::ms_handle_reset(crimson::net::ConnectionRef c, bool /* is_replace */)
{
  LOG_PREFIX(Client::ms_handle_reset);
  DEBUGDPP("", *this);
  gates.dispatch_in_background(__func__, *this,
  [this, c, FNAME] {
    DEBUGDPP("dispatching in background", *this);
    if (conn == c) {
      report_timer.cancel();
      return reconnect();
    } else {
      return seastar::now();
    }
  });
}

seastar::future<> Client::retry_interval()
{
  LOG_PREFIX(Client::retry_interval);
  auto retry_interval = std::chrono::duration<double>(
    local_conf().get_val<double>("mgr_connect_retry_interval"));
  auto a_while = std::chrono::duration_cast<seastar::steady_clock_type::duration>(
    retry_interval);
  DEBUGDPP("reconnecting in {} seconds", *this, retry_interval);
  co_await seastar::sleep(a_while);
}

seastar::future<> Client::reconnect()
{
  LOG_PREFIX(Client::reconnect);
  DEBUGDPP("", *this);
  co_await conn_lock.lock();
  auto unlocker = seastar::defer([this] {
    conn_lock.unlock();
  });
  if (conn) {
    DEBUGDPP("marking down", *this);
    conn->mark_down();
    conn = {};
  }
  if (!mgrmap.get_available()) {
    WARNDPP("No active mgr available yet", *this);
    co_return;
  }
  co_await retry_interval();

  auto peer = mgrmap.get_active_addrs().pick_addr(msgr.get_myaddr().get_type());
  if (peer == entity_addr_t{}) {
    // crimson msgr only uses the first bound addr
    ERRORDPP("mgr.{} does not have an addr compatible with me",
             *this, mgrmap.get_active_name());
    co_return;
  }
  conn = msgr.connect(peer, CEPH_ENTITY_TYPE_MGR);
  DEBUGDPP("reconnected successfully", *this);
}

seastar::future<> Client::handle_mgr_map(crimson::net::ConnectionRef,
                                         Ref<MMgrMap> m)
{
  LOG_PREFIX(Client::handle_mgr_map);
  DEBUGDPP("", *this);
  mgrmap = m->get_map();
  if (!conn || conn->get_peer_addr() !=
               mgrmap.get_active_addrs().legacy_addr()) {
    co_await reconnect();
  }
}

seastar::future<> Client::handle_mgr_conf(crimson::net::ConnectionRef,
                                          Ref<MMgrConfigure> m)
{
  LOG_PREFIX(Client::handle_mgr_conf);
  DEBUGDPP("{}", *this, *m);

  auto report_period = std::chrono::seconds{m->stats_period};
  if (report_period.count()) {
    if (report_timer.armed()) {
      report_timer.rearm(report_timer.get_timeout(), report_period);
    } else {
      report_timer.arm_periodic(report_period);
    }
  } else {
    report_timer.cancel();
  }
  if (!m->osd_perf_metric_queries.empty()) {
    ceph_assert(set_perf_queries_cb);
    co_await set_perf_queries_cb(m->osd_perf_metric_queries);
  }
}

void Client::report()
{
  LOG_PREFIX(Client::report);
  DEBUGDPP("", *this);
  _send_report();
  gates.dispatch_in_background(__func__, *this, [this, FNAME] {
    DEBUGDPP("dispatching in background", *this);
    return with_stats.get_stats(
    ).then([this](auto &&pg_stats) {
      return send(std::move(pg_stats));
    });
  });
}

void Client::update_daemon_health(std::vector<DaemonHealthMetric>&& metrics)
{
  daemon_health_metrics = std::move(metrics);
}

void Client::_send_report()
{
  LOG_PREFIX(Client::_send_report);
  DEBUGDPP("", *this);
  gates.dispatch_in_background(__func__, *this, [this, FNAME] {
    DEBUGDPP("dispatching in background", *this);
    auto report = make_message<MMgrReport>();
    // Adding empty information since we don't support perfcounters yet
    report->undeclare_types.emplace_back();
    ENCODE_START(1, 1, report->packed);
    report->declare_types.emplace_back();
    ENCODE_FINISH(report->packed);

    if (daemon_name.size()) {
      report->daemon_name = daemon_name;
    } else {
      report->daemon_name = local_conf()->name.get_id();
    }
    report->service_name = service_name;
    report->daemon_health_metrics = std::move(daemon_health_metrics);
    local_conf().get_config_bl(last_config_bl_version, &report->config_bl,
	                      &last_config_bl_version);
    if (get_perf_report_cb) {
      return get_perf_report_cb(
      ).then([report=std::move(report), this](auto payload) mutable {
	report->metric_report_message = MetricReportMessage(std::move(payload));
	return send(std::move(report));
      });
    }
    return send(std::move(report));
  });
}

void Client::print(std::ostream& out) const
{
  out << "mgrc ";
}

}
