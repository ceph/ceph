// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_tracer.h"

#include <algorithm>
#include <mutex>

#include "common/config_obs.h"
#include "rgw_op.h"
#include "rgw_perf_counters.h"
#include "rgw_sal.h"

namespace tracing {
namespace rgw {

tracing::Tracer tracer;

bool trace_slow_requests(CephContext* cct)
{
  return cct->_conf.get_val<double>("rgw_trace_slow_threshold") > 0;
}

namespace {

// at most rgw_trace_max_per_sec traces a second, holding one second's worth
bool take_token(CephContext* cct)
{
  static std::mutex lock;
  static double tokens = 0;
  static utime_t stamp;
  const double rate = cct->_conf.get_val<uint64_t>("rgw_trace_max_per_sec");
  const utime_t now = ceph_clock_now();
  std::lock_guard l(lock);
  tokens = std::min(rate, tokens + (now - stamp) * rate);
  stamp = now;
  if (tokens < 1) {
    return false;
  }
  tokens -= 1;
  return true;
}

// re-creates the exporter when its destination changes, as the OSDs do
class ExporterObserver : public md_config_obs_t {
 public:
  std::vector<std::string> get_tracked_keys() const noexcept override {
    return {"trace_exporter", "trace_otlp_endpoint",
            "jaeger_agent_host", "jaeger_agent_port"};
  }
  void handle_conf_change(const ConfigProxy&,
                          const std::set<std::string>&) override {
    tracer.reconfigure();
  }
};
ExporterObserver exporter_observer;
bool observing = false;

} // anonymous namespace

void init(CephContext* cct)
{
  tracer.init(cct, "rgw");
  if (!observing) {
    cct->_conf.add_observer(&exporter_observer);
    observing = true;
  }
}

void shutdown(CephContext* cct)
{
  if (observing) {
    cct->_conf.remove_observer(&exporter_observer);
    observing = false;
  }
}

void trace_slow_request(const req_state* s, const RGWOp* op, ::rgw::sal::Driver* driver)
{
  CephContext* cct = s->cct;
  const double threshold = cct->_conf.get_val<double>("rgw_trace_slow_threshold");
  if (threshold <= 0 || !s->trace) {
    return;
  }
  OpTimeline t;
  t.start.set_from_double(req_state::Clock::to_double(s->time));
  t.end = ceph_clock_now();
  if (t.end - t.start < threshold) {
    return;
  }
  if (!take_token(cct)) {
    if (perfcounter) {
      perfcounter->inc(l_rgw_slow_request_traces_dropped);
    }
    return;
  }
  trace_id_t trace_id;
  span_id_t span_id;
  if (!context_ids(s->trace->GetContext(), &trace_id, &span_id)) {
    return;
  }
  // the ids the OSDs already placed their slow ops under
  t.trace_id = trace_id;
  t.span_id = span_id;
  t.name = op->name();
  t.attributes = {
    {TRANS_ID, s->trans_id},
    {HOST_ID, driver->get_host_id()},
  };
  t.int_attributes = {
    {OP_RESULT, op->get_ret()},
    {"http_status", s->err.http_ret},
  };
  if (!::rgw::sal::User::empty(s->user)) {
    t.attributes.emplace_back(USER_ID, s->user->get_id().id);
  }
  if (!::rgw::sal::Bucket::empty(s->bucket)) {
    t.attributes.emplace_back(BUCKET_NAME, s->bucket->get_name());
  }
  if (!::rgw::sal::Object::empty(s->object)) {
    t.attributes.emplace_back(OBJECT_NAME, s->object->get_name());
  }
  tracer.record_op(t);
  if (perfcounter) {
    perfcounter->inc(l_rgw_slow_request_traces);
  }
}

} // namespace rgw
} // namespace tracing
