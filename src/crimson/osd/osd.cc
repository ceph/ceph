#include "osd.h"

#include "crimson/net/Connection.h"
#include "crimson/net/SocketMessenger.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

using ceph::common::local_conf;

OSD::OSD(int id, uint32_t nonce)
  : whoami{id},
    cluster_msgr{new ceph::net::SocketMessenger{entity_name_t::OSD(whoami),
                                                "cluster", nonce}},
    client_msgr{new ceph::net::SocketMessenger{entity_name_t::OSD(whoami),
                                               "client", nonce}},
    monc{*client_msgr}
{
  for (auto msgr : {cluster_msgr.get(), client_msgr.get()}) {
    if (local_conf()->ms_crc_data) {
      msgr->set_crc_data();
    }
    if (local_conf()->ms_crc_header) {
      msgr->set_crc_header();
    }
  }
  dispatchers.push_front(this);
  dispatchers.push_front(&monc);
}

OSD::~OSD() = default;

seastar::future<> OSD::start()
{
  logger().info("start");
  return client_msgr.start(&dispatchers).then([this] {
    return monc.start();
  }).then([this] {
    monc.sub_want("mgrmap", 0, 0);
    monc.sub_want("osdmap", 0, 0);
    return monc.renew_subs();
  });
}

seastar::future<> OSD::stop()
{
  return gate.close().then([this] {
    return monc.stop();
  }).then([this] {
    return client_msgr->shutdown();
  });
}

seastar::future<> OSD::ms_dispatch(ceph::net::ConnectionRef conn, MessageRef m)
{
  logger().info("ms_dispatch {}", *m);
  return seastar::now();
}

seastar::future<> OSD::ms_handle_connect(ceph::net::ConnectionRef conn)
{
  if (conn->get_peer_type() != CEPH_ENTITY_TYPE_MON) {
    return seastar::now();
  } else {
    return seastar::now();
  }
}

seastar::future<> OSD::ms_handle_reset(ceph::net::ConnectionRef conn)
{
  // TODO: cleanup the session attached to this connection
  logger().warn("ms_handle_reset");
  return seastar::now();
}

seastar::future<> OSD::ms_handle_remote_reset(ceph::net::ConnectionRef conn)
{
  logger().warn("ms_handle_remote_reset");
  return seastar::now();
}
