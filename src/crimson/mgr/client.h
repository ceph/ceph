// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/timer.hh>

#include "crimson/common/gated.h"
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
  virtual seastar::future<MessageURef> get_stats() = 0;
  virtual ~WithStats() {}
};

class Client : public crimson::net::Dispatcher {
public:
  Client(crimson::net::Messenger& msgr,
	 WithStats& with_stats);
  seastar::future<> start();
  seastar::future<> stop();
  void report();

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

  void print(std::ostream&) const;
  friend std::ostream& operator<<(std::ostream& out, const Client& client);
private:
  MgrMap mgrmap;
  crimson::net::Messenger& msgr;
  WithStats& with_stats;
  crimson::net::ConnectionRef conn;
  seastar::timer<seastar::lowres_clock> report_timer;
  crimson::common::Gated gate;
  uint64_t last_config_bl_version = 0;
  std::string service_name, daemon_name;

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
