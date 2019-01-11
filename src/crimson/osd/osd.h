#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include "crimson/mon/MonClient.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/osd/chained_dispatchers.h"

namespace ceph::net {
  class Messenger;
}

class OSD : public ceph::net::Dispatcher {
  seastar::gate gate;
  const int whoami;
  // talk with osd
  std::unique_ptr<ceph::net::Messenger> cluster_msgr;
  // talk with mon/mgr
  std::unique_ptr<ceph::net::Messenger> client_msgr;
  ChainedDispatchers dispatchers;
  ceph::mon::Client monc;

  // Dispatcher methods
  seastar::future<> ms_dispatch(ceph::net::ConnectionRef conn, MessageRef m) override;
  seastar::future<> ms_handle_connect(ceph::net::ConnectionRef conn) override;
  seastar::future<> ms_handle_reset(ceph::net::ConnectionRef conn) override;
  seastar::future<> ms_handle_remote_reset(ceph::net::ConnectionRef conn) override;

public:
  OSD(int id, uint32_t nonce);
  ~OSD();

  seastar::future<> start();
  seastar::future<> stop();
};
