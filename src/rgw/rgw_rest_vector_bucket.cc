// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_vector_bucket.h"
#include "rgw_op.h"
#include "rgw_s3vector_background.h"
#include "common/ceph_time.h"
#include <chrono>
#include <cstdlib>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

// ============================================================================
// Ceph admin REST API: /admin/vectorbucket
// ============================================================================

namespace {

// Format a coarse_real_time as an ISO-8601 UTC string via the shared
// ceph::to_iso_8601 helper (nanosecond precision, e.g.
// "2026-08-01T12:00:12.500000000Z"). coarse_real_clock and real_clock share
// the same timespan duration and both count from the Unix epoch, so the
// time_since_epoch() duration transfers directly.
std::string s3v_iso8601(ceph::coarse_real_time t) {
  return ceph::to_iso_8601(ceph::real_time{t.time_since_epoch()});
}

// GET /admin/vectorbucket/rebuild[?vectorbucket=<name>][&tenant=<t>][&since=<epoch>]
// Returns this RGW instance's background rebuild status + event log.
class RGWOp_VectorBucket_Rebuild_Status : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }
  void execute(optional_yield y) override;
  const char* name() const override { return "vectorbucket_rebuild_status"; }
};

void RGWOp_VectorBucket_Rebuild_Status::execute(optional_yield y) {
  const std::string bucket_filter = s->info.args.get("vectorbucket");
  const std::string tenant_filter = s->info.args.get("tenant");
  uint64_t since = 0;
  {
    bool exists = false;
    const std::string since_str = s->info.args.get("since", &exists);
    if (exists && !since_str.empty()) {
      since = strtoull(since_str.c_str(), nullptr, 10);
    }
  }

  const auto status = rgw::s3vector::get_background_status();
  const auto evresult = rgw::s3vector::get_rebuild_events(since, tenant_filter, bucket_filter);

  Formatter* f = flusher.get_formatter();
  flusher.start(0);

  f->open_object_section("rebuild_status");

  // instance identity first — makes the per-instance scope explicit
  f->open_object_section("instance");
  f->dump_string("instance_id", status.instance_id);
  f->dump_string("host_id", status.host_id);
  f->close_section();

  f->open_object_section("status");
  f->dump_int("active_rebuilds", status.active_rebuilds);
  f->dump_int("max_concurrent_rebuilds", status.max_concurrent_rebuilds);
  f->dump_int("num_workers", status.num_workers);
  f->dump_int("tables_tracked", status.tables_tracked);
  f->close_section();

  // Aggregate "since boot" counters are exposed via Ceph PerfCounters
  // ("rgw_s3vector_background", scraped by the prometheus stack), not here.

  const auto now = ceph::coarse_real_clock::now();
  f->open_array_section("active_builds");
  for (const auto& b : status.active_builds_list) {
    if (!bucket_filter.empty() && b.bucket != bucket_filter) {
      continue;
    }
    if (!tenant_filter.empty() && b.tenant != tenant_filter) {
      continue;
    }
    f->open_object_section("active_build");
    if (!b.tenant.empty()) {
      f->dump_string("tenant", b.tenant);
    }
    f->dump_string("bucket", b.bucket);
    f->dump_string("index", b.index);
    f->dump_string("start_time", s3v_iso8601(b.start_time));
    f->dump_int("duration_so_far_ms",
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - b.start_time).count());
    f->dump_int("lock_refreshes", b.lock_refreshes);
    f->close_section();
  }
  f->close_section();

  // Emit truncation metadata before the array so consumers can detect a cropped
  // response without scanning to the end.
  f->dump_unsigned("total_matched_events", evresult.total_matched);
  if (evresult.truncated) {
    f->dump_bool("truncated", true);
    f->dump_string("truncated_note",
        "response capped at rgw_s3vector_event_log_size; use ?since= to narrow");
  }

  f->open_array_section("rebuild_events");
  for (const auto& e : evresult.events) {
    f->open_object_section("event");
    f->dump_string("type", e.type);
    f->dump_string("timestamp", s3v_iso8601(e.timestamp));
    if (!e.tenant.empty()) {
      f->dump_string("tenant", e.tenant);
    }
    if (!e.bucket.empty()) {
      f->dump_string("bucket", e.bucket);
    }
    if (!e.index.empty()) {
      f->dump_string("index", e.index);
    }
    f->dump_int("active_rebuilds", e.active_rebuilds);
    f->dump_int("max_concurrent", e.max_concurrent);
    if (e.duration_ms > 0) {
      f->dump_int("duration_ms", e.duration_ms);
    }
    if (!e.result.empty()) {
      f->dump_string("result", e.result);
    }
    if (e.repeat_count > 1) {
      f->dump_unsigned("repeat_count", e.repeat_count);
    }
    f->close_section();
  }
  f->close_section();

  f->close_section(); // rebuild_status

  flusher.flush();
}

} // anonymous namespace

RGWRESTMgr_VectorBucket::RGWRESTMgr_VectorBucket() {
  register_resource("rebuild", new RGWRESTMgr_VectorBucketRebuild);
}

RGWOp* RGWHandler_VectorBucketRebuild::op_get() {
  return new RGWOp_VectorBucket_Rebuild_Status;
}
