// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "client.h"

#include <seastar/core/sleep.hh>

#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrOpen.h"
#include "messages/MMgrReport.h"

namespace {
  seastar::logger& logger()
  {
    return crimson::get_logger(ceph_subsys_mgrc);
  }
}

using crimson::common::local_conf;

namespace crimson::mgr
{

Client::Client(crimson::net::Messenger& msgr,
                 WithStats& with_stats)
  : msgr{msgr},
    with_stats{with_stats},
    report_timer{[this] {report();}}
{}

seastar::future<> Client::start()
{
  return seastar::now();
}

seastar::future<> Client::stop()
{
  logger().info("{}", __func__);
  report_timer.cancel();
  auto fut = gate.close();
  if (conn) {
    conn->mark_down();
  }
  return fut;
}

std::optional<seastar::future<>>
Client::ms_dispatch(crimson::net::ConnectionRef conn, MessageRef m)
{
  bool dispatched = true;
  gate.dispatch_in_background(__func__, *this, [this, conn, &m, &dispatched] {
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
  ceph_assert_always(prv_shard == seastar::this_shard_id());
  gate.dispatch_in_background(__func__, *this, [this, c] {
    if (conn == c) {
      // ask for the mgrconfigure message
      auto m = crimson::make_message<MMgrOpen>();
      m->daemon_name = local_conf()->name.get_id();
      local_conf().get_config_bl(0, &m->config_bl, &last_config_bl_version);
      local_conf().get_defaults_bl(&m->config_defaults_bl);
      return conn->send(std::move(m));
    } else {
      return seastar::now();
    }
  });
}

void Client::ms_handle_reset(crimson::net::ConnectionRef c, bool /* is_replace */)
{
  gate.dispatch_in_background(__func__, *this, [this, c] {
    if (conn == c) {
      report_timer.cancel();
      return reconnect();
    } else {
      return seastar::now();
    }
  });
}

seastar::future<> Client::reconnect()
{
  if (conn) {
    conn->mark_down();
    conn = {};
  }
  if (!mgrmap.get_available()) {
    logger().warn("No active mgr available yet");
    return seastar::now();
  }
  auto retry_interval = std::chrono::duration<double>(
    local_conf().get_val<double>("mgr_connect_retry_interval"));
  auto a_while = std::chrono::duration_cast<seastar::steady_clock_type::duration>(
    retry_interval);
  return seastar::sleep(a_while).then([this] {
    auto peer = mgrmap.get_active_addrs().pick_addr(msgr.get_myaddr().get_type());
    if (peer == entity_addr_t{}) {
      // crimson msgr only uses the first bound addr
      logger().error("mgr.{} does not have an addr compatible with me",
                     mgrmap.get_active_name());
      return;
    }
    conn = msgr.connect(peer, CEPH_ENTITY_TYPE_MGR);
  });
}

seastar::future<> Client::handle_mgr_map(crimson::net::ConnectionRef,
                                         Ref<MMgrMap> m)
{
  mgrmap = m->get_map();
  if (!conn) {
    return reconnect();
  } else if (conn->get_peer_addr() !=
             mgrmap.get_active_addrs().legacy_addr()) {
    return reconnect();
  } else {
    return seastar::now();
  }
}

seastar::future<> Client::handle_mgr_conf(crimson::net::ConnectionRef,
                                          Ref<MMgrConfigure> m)
{
  logger().info("{} {}", __func__, *m);

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
  return seastar::now();
}

void Client::report()
{
  _send_report();
  gate.dispatch_in_background(__func__, *this, [this] {
    if (!conn) {
      logger().warn("report: no conn available; report skipped");
      return seastar::now();
    }
    return with_stats.get_stats(
    ).then([this](auto &&pg_stats) {
      return conn->send(std::move(pg_stats));
    });
  });
}

void Client::_send_report()
{
  // TODO: implement daemon_health_metrics support
  // https://tracker.ceph.com/issues/63766
  gate.dispatch_in_background(__func__, *this, [this] {
    if (!conn) {
      logger().warn("cannot send report; no conn available");
      return seastar::now();
    }
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
    local_conf().get_config_bl(last_config_bl_version, &report->config_bl,
	                      &last_config_bl_version);
    return conn->send(std::move(report));
  });
}

void Client::print(std::ostream& out) const
{
  out << "mgrc ";
}

}
