// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>

#include "crimson/net/Dispatcher.h"
#include "crimson/net/Fwd.h"
#include "mon/MgrMap.h"

template<typename Message> using Ref = boost::intrusive_ptr<Message>;
namespace ceph::net {
  class Messenger;
}

class MMgrMap;
class MMgrConfigure;

namespace ceph::mgr
{

// implement WithStats if you want to report stats to mgr periodically
class WithStats {
public:
  // the method is not const, because the class sending stats might need to
  // update a seq number every time it collects the stats
  virtual MessageRef get_stats() = 0;
  virtual ~WithStats() {}
};

class Client : public ceph::net::Dispatcher {
public:
  Client(ceph::net::Messenger& msgr,
	 WithStats& with_stats);
  seastar::future<> start();
  seastar::future<> stop();
private:
  seastar::future<> ms_dispatch(ceph::net::ConnectionRef conn,
				Ref<Message> m) override;
  seastar::future<> ms_handle_reset(ceph::net::ConnectionRef conn) override;
  seastar::future<> handle_mgr_map(ceph::net::ConnectionRef conn,
				   Ref<MMgrMap> m);
  seastar::future<> handle_mgr_conf(ceph::net::ConnectionRef conn,
				    Ref<MMgrConfigure> m);
  seastar::future<> reconnect();
  void report();

private:
  MgrMap mgrmap;
  ceph::net::Messenger& msgr;
  WithStats& with_stats;
  ceph::net::ConnectionRef conn;
  std::chrono::seconds report_period{0};
  seastar::timer<seastar::lowres_clock> report_timer;
  seastar::gate gate;
};

}
