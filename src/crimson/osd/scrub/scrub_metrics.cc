// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "scrub_metrics.h"

namespace crimson::osd::scrub {

void ScrubMetrics::register_metrics(
  const std::string& pool_type,
  const std::string& scrub_level,
  const std::string& pg_id)
{
  // Guard against double registration
  if (metrics_registered) {
    return;
  }

  namespace sm = seastar::metrics;

  std::vector<sm::label_instance> labels = {
    sm::label_instance("pool_type", pool_type),
    sm::label_instance("scrub_level", scrub_level),
    sm::label_instance("pg_id", pg_id)
  };

  metrics.add_group("scrub", {
    // Scrub I/O metrics (common)
    sm::make_counter("omapgetheader_cnt", omapgetheader_cnt,
      sm::description("omap get header calls count"),
      labels),
    sm::make_counter("omapgetheader_bytes", omapgetheader_bytes,
      sm::description("bytes read by omap get header"),
      labels),
    sm::make_counter("omapget_cnt", omapget_cnt,
      sm::description("omap get calls count"),
      labels),
    sm::make_counter("omapget_bytes", omapget_bytes,
      sm::description("total bytes read by omap get"),
      labels),

    // Pool-specific I/O metrics
    sm::make_counter("getattr_cnt", getattr_cnt,
      sm::description("get_attr calls count"),
      labels),
    sm::make_counter("stats_cnt", stats_cnt,
      sm::description("stats calls count"),
      labels),
    sm::make_counter("read_cnt", read_cnt,
      sm::description("read calls count"),
      labels),
    sm::make_counter("read_bytes", read_bytes,
      sm::description("total bytes read"),
      labels),

    // Scrub session metrics
    sm::make_counter("started_cnt", started_cnt,
      sm::description("scrubs that got started"),
      labels),
    sm::make_counter("active_started_cnt", active_started_cnt,
      sm::description("scrubs that got past replicas reservation"),
      labels),
    sm::make_counter("successful_cnt", successful_cnt,
      sm::description("successful scrubs count"),
      labels),
    sm::make_counter("successful_elapsed", successful_elapsed,
      sm::description("time to complete a successful scrub"),
      labels),
    sm::make_counter("failed_cnt", failed_cnt,
      sm::description("failed scrubs count"),
      labels),
    sm::make_counter("failed_elapsed", failed_elapsed,
      sm::description("time from start to failure"),
      labels),
    sm::make_counter("write_intersects", write_intersects,
      sm::description("client write op intersects chunk range"),
      labels),
    sm::make_counter("write_blocked", write_blocked,
      sm::description("write op did not preempt the scrub"),
      labels),

    // Reservation process metrics
    sm::make_counter("rsv_successful_cnt", rsv_successful_cnt,
      sm::description("successful replicas reservation count"),
      labels),
    sm::make_counter("rsv_successful_elapsed", rsv_successful_elapsed,
      sm::description("time to complete a successful replicas reservation"),
      labels),
    sm::make_counter("rsv_aborted_cnt", rsv_aborted_cnt,
      sm::description("failed attempt to reserve replicas due to an abort"),
      labels),
    sm::make_counter("rsv_rejected_cnt", rsv_rejected_cnt,
      sm::description("reservation failed due to a 'rejected' response"),
      labels),
    sm::make_counter("rsv_skipped_cnt", rsv_skipped_cnt,
      sm::description("reservation skipped for high-priority scrubs"),
      labels),
    sm::make_counter("rsv_failed_elapsed", rsv_failed_elapsed,
      sm::description("time for a replicas reservation process to fail"),
      labels),
    sm::make_gauge("rsv_secondaries_num", rsv_secondaries_num,
      sm::description("number of replicas"),
      labels)
  });

  metrics_registered = true;
}

void ScrubMetrics::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrub_metrics");

  f->open_object_section("io_metrics");
  f->dump_unsigned("omapgetheader_cnt", omapgetheader_cnt);
  f->dump_unsigned("omapgetheader_bytes", omapgetheader_bytes);
  f->dump_unsigned("omapget_cnt", omapget_cnt);
  f->dump_unsigned("omapget_bytes", omapget_bytes);
  f->dump_unsigned("getattr_cnt", getattr_cnt);
  f->dump_unsigned("stats_cnt", stats_cnt);
  f->dump_unsigned("read_cnt", read_cnt);
  f->dump_unsigned("read_bytes", read_bytes);
  f->close_section();

  f->open_object_section("session_metrics");
  f->dump_unsigned("started_cnt", started_cnt);
  f->dump_unsigned("active_started_cnt", active_started_cnt);
  f->dump_unsigned("successful_cnt", successful_cnt);
  f->dump_unsigned("successful_elapsed", successful_elapsed);
  f->dump_unsigned("failed_cnt", failed_cnt);
  f->dump_unsigned("failed_elapsed", failed_elapsed);
  f->dump_unsigned("write_intersects", write_intersects);
  f->dump_unsigned("write_blocked", write_blocked);
  f->close_section();

  f->open_object_section("reservation_metrics");
  f->dump_unsigned("rsv_successful_cnt", rsv_successful_cnt);
  f->dump_unsigned("rsv_successful_elapsed", rsv_successful_elapsed);
  f->dump_unsigned("rsv_aborted_cnt", rsv_aborted_cnt);
  f->dump_unsigned("rsv_rejected_cnt", rsv_rejected_cnt);
  f->dump_unsigned("rsv_skipped_cnt", rsv_skipped_cnt);
  f->dump_unsigned("rsv_failed_elapsed", rsv_failed_elapsed);
  f->dump_unsigned("rsv_secondaries_num", rsv_secondaries_num);
  f->close_section();

  f->close_section();
}

} // namespace crimson::osd::scrub
