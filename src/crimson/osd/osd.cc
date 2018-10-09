#include "osd.h"

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
}

OSD::~OSD() = default;

seastar::future<> OSD::start()
{
  logger().info("start");
  return seastar::now();
}

seastar::future<> OSD::stop()
{
  return gate.close();
}
