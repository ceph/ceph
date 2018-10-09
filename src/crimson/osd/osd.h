#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include "crimson/net/Dispatcher.h"

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

public:
  OSD(int id, uint32_t nonce);
  ~OSD();

  seastar::future<> start();
  seastar::future<> stop();
};
