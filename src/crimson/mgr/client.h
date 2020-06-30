// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>

#include "crimson/net/Dispatcher.h"
#include "crimson/net/Fwd.h"
#include "mon/MgrMap.h"

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
  // the method is not const, because the class sending stats might need to
  // update a seq number every time it collects the stats
  virtual MessageRef get_stats() = 0;
  virtual ~WithStats() {}
};

class Client : public crimson::net::Dispatcher {
public:
  Client(crimson::net::Messenger& msgr,
	 WithStats& with_stats);
  seastar::future<> start();
  seastar::future<> stop();
private:
  seastar::future<> ms_dispatch(crimson::net::Connection* conn,
				Ref<Message> m) override;
  void ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace) final;
  void ms_handle_connect(crimson::net::ConnectionRef conn) final;
  seastar::future<> handle_mgr_map(crimson::net::Connection* conn,
				   Ref<MMgrMap> m);
  seastar::future<> handle_mgr_conf(crimson::net::Connection* conn,
				    Ref<MMgrConfigure> m);
  seastar::future<> reconnect();
  void report();

  void print(std::ostream&) const;
  friend std::ostream& operator<<(std::ostream& out, const Client& client);
private:
  MgrMap mgrmap;
  crimson::net::Messenger& msgr;
  WithStats& with_stats;
  crimson::net::ConnectionRef conn;
  seastar::timer<seastar::lowres_clock> report_timer;
  crimson::common::Gated gate;
};

inline std::ostream& operator<<(std::ostream& out, const Client& client) {
  client.print(out);
  return out;
}

}
