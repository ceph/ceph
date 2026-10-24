// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>
#include <string>

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_types.hh>

namespace crimson::net {

// Per reactor counters of one messenger, owned by it through a
// seastar::sharded. Exported as "msgr_*" with a "msgr" label (e.g. client,
// cluster, hb_front, hb_back) when the messenger is created with
// export_metrics.
struct msgr_stats_t {
  msgr_stats_t(const std::string& msgr_name, bool export_metrics);

  uint64_t msgs_sent = 0;
  uint64_t bytes_sent = 0;
  uint64_t msgs_received = 0;
  uint64_t bytes_received = 0;
  // send() called on a reactor other than the connection's IO reactor;
  // counted on the calling reactor.
  uint64_t send_cross_core = 0;
  uint64_t out_writes = 0;
  uint64_t out_flushes = 0;
  int64_t connections_open = 0;
  // from send() to the message being encoded for the socket (lowres)
  seastar::metrics::histogram send_queue_wait;

private:
  seastar::metrics::metric_groups metrics;
};

} // namespace crimson::net
