// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "msgr_stats.h"

#include "crimson/common/lat_histogram.h"

namespace crimson::net {

msgr_stats_t::msgr_stats_t(const std::string& msgr_name, bool export_metrics)
{
  crimson::common::init_lat_histogram(send_queue_wait);
  if (!export_metrics) {
    return;
  }
  namespace sm = seastar::metrics;
  auto label = sm::label("msgr")(msgr_name);
  metrics.add_group("msgr", {
    sm::make_counter("msgs_sent", msgs_sent,
      sm::description("messages encoded and written to sockets"), {label}),
    sm::make_counter("bytes_sent", bytes_sent,
      sm::description("bytes written to sockets, including frame overhead"),
      {label}),
    sm::make_counter("msgs_received", msgs_received,
      sm::description("messages read and decoded"), {label}),
    sm::make_counter("bytes_received", bytes_received,
      sm::description("message payload bytes read"), {label}),
    sm::make_counter("send_cross_core", send_cross_core,
      sm::description(
        "sends issued on a reactor other than the connection's IO reactor"),
      {label}),
    sm::make_counter("out_writes", out_writes,
      sm::description("batches of messages handed to the socket"), {label}),
    sm::make_counter("out_flushes", out_flushes,
      sm::description("socket flushes; msgs_sent/out_flushes is the batch size"),
      {label}),
    sm::make_gauge("connections_open", connections_open,
      sm::description("connections whose IO is open on this reactor"),
      {label}),
    sm::make_histogram("send_queue_wait_ms",
      [this]() -> sm::histogram& { return send_queue_wait; },
      sm::description("time from send() until the message is encoded"),
      {label}),
  });
}

} // namespace crimson::net
