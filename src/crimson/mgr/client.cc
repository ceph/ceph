#include "client.h"

#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrOpen.h"

namespace {
  seastar::logger& logger()
  {
    return ceph::get_logger(ceph_subsys_mgrc);
  }
  template<typename Message, typename... Args>
  Ref<Message> make_message(Args&&... args)
  {
    // Message inherits from RefCountedObject, whose nref is 1 when it is
    // constructed, so we pass "add_ref = false" to intrusive_ptr's ctor
    return {new Message{std::forward<Args>(args)...}, false};
  }
}

using ceph::common::local_conf;

namespace ceph::mgr
{

Client::Client(ceph::net::Messenger& msgr,
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
  return gate.close().then([this] {
    if (conn) {
      return conn->close();
    } else {
      return seastar::now();
    }
  });
}

seastar::future<> Client::ms_dispatch(ceph::net::ConnectionRef conn,
                                      MessageRef m)
{
  switch(m->get_type()) {
  case MSG_MGR_MAP:
    return handle_mgr_map(conn, boost::static_pointer_cast<MMgrMap>(m));
  case MSG_MGR_CONFIGURE:
    return handle_mgr_conf(conn, boost::static_pointer_cast<MMgrConfigure>(m));
  default:
    return seastar::now();
  }
}

seastar::future<> Client::ms_handle_reset(ceph::net::ConnectionRef c)
{
  if (conn == c) {
    return reconnect();
  } else {
    return seastar::now();
  }
}

seastar::future<> Client::reconnect()
{
  return (conn ? conn->close() : seastar::now()).then([this] {
    if (!mgrmap.get_available()) {
      logger().warn("No active mgr available yet");
      return seastar::now();
    }
    auto peer = mgrmap.get_active_addrs().legacy_addr();
    return msgr.connect(peer, CEPH_ENTITY_TYPE_MGR).then(
      [this](auto xconn) {
        conn = xconn->release();
        // ask for the mgrconfigure message
        auto m = make_message<MMgrOpen>();
        m->daemon_name = local_conf()->name.get_id();
        return conn->send(std::move(m));
      });
  });
}

seastar::future<> Client::handle_mgr_map(ceph::net::ConnectionRef,
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

seastar::future<> Client::handle_mgr_conf(ceph::net::ConnectionRef conn,
                                          Ref<MMgrConfigure> m)
{
  logger().info("{} {}", __func__, *m);
  report_period = std::chrono::seconds{m->stats_period};
  if (report_period.count() && !report_timer.armed() ) {
    report();
  }
  return seastar::now();
}

void Client::report()
{
  seastar::with_gate(gate, [this] {
    auto pg_stats = with_stats.get_stats();
    return conn->send(std::move(pg_stats)).finally([this] {
      if (report_period.count()) {
        report_timer.arm(report_period);
      }
    });
  });
}

}
