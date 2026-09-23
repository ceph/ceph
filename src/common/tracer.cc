// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/ceph_context.h"
#include "tracer.h"
#include "common/debug.h"

#ifdef HAVE_JAEGER
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/exporters/jaeger/jaeger_exporter.h"

#define dout_subsys ceph_subsys_trace
#undef dout_prefix
#define dout_prefix (*_dout << "otel_tracing: ")

namespace tracing {

const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> Tracer::noop_tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("no-op", OPENTELEMETRY_SDK_VERSION);
const jspan_ptr Tracer::noop_span = noop_tracer->StartSpan("noop");

using bufferlist = ceph::buffer::list;

void Tracer::init(CephContext* _cct, opentelemetry::nostd::string_view service_name) {
  ceph_assert(_cct);
  cct = _cct;
  if (!tracer) {
    ldout(cct, 3) << "tracer was not loaded, initializing tracing" << dendl;
    opentelemetry::exporter::jaeger::JaegerExporterOptions exporter_options;
    exporter_options.server_port = cct->_conf.get_val<int64_t>("jaeger_agent_port");
    const opentelemetry::sdk::trace::BatchSpanProcessorOptions processor_options;
    const auto jaeger_resource = opentelemetry::sdk::resource::Resource::Create(std::move(opentelemetry::sdk::resource::ResourceAttributes{{"service.name", service_name}}));
    auto jaeger_exporter = std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>(new opentelemetry::exporter::jaeger::JaegerExporter(exporter_options));
    auto processor = std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>(new opentelemetry::sdk::trace::BatchSpanProcessor(std::move(jaeger_exporter), processor_options));
    const auto provider = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(new opentelemetry::sdk::trace::TracerProvider(std::move(processor), jaeger_resource));
    opentelemetry::trace::Provider::SetTracerProvider(provider);
    tracer = provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);
  }
}

jspan_ptr Tracer::start_trace(opentelemetry::nostd::string_view trace_name) {
  ceph_assert(cct);
  if (is_enabled()) {
    ceph_assert(tracer);
    ldout(cct, 20) << "start trace for " << trace_name << " " << dendl;
    return tracer->StartSpan(trace_name);
  }
  return noop_span;
}

jspan_ptr Tracer::start_trace(opentelemetry::nostd::string_view trace_name, bool trace_is_enabled) {
  ceph_assert(cct);
  ldout(cct, 20) << "start trace enabled " << trace_is_enabled << " " << dendl;
  if (trace_is_enabled) {
    ceph_assert(tracer);
    ldout(cct, 20) << "start trace for " << trace_name << " " << dendl;
    return tracer->StartSpan(trace_name);
  }
  return noop_tracer->StartSpan(trace_name);
}

jspan_ptr Tracer::add_span(opentelemetry::nostd::string_view span_name, const jspan_ptr& parent_span) {
  if (is_enabled() && parent_span && parent_span->IsRecording()) {
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_span->GetContext();
    ldout(cct, 20) << "adding span " << span_name << " " << dendl;
    return tracer->StartSpan(span_name, span_opts);
  }
  return noop_span;
}

jspan_ptr Tracer::add_span(opentelemetry::nostd::string_view span_name, const jspan_context& parent_ctx) {
  if (parent_ctx.IsValid()) {
    ceph_assert(tracer);
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_ctx;
    ldout(cct, 20) << "adding span " << span_name << " " << dendl;
    return tracer->StartSpan(span_name, span_opts);
  }
  return noop_span;
}

bool Tracer::is_enabled() const {
  return cct->_conf->jaeger_tracing_enable;
}

// phases shorter than this share of the op are kept as span events only
static constexpr double min_phase_share = 0.01;

std::string Tracer::record_op(const OpTimeline& t, const jspan_context& parent) {
  if (!tracer) {
    return {};
  }
  namespace otel_common = opentelemetry::common;
  namespace otel_trace = opentelemetry::trace;
  // the timeline is in wall-clock time; the SDK measures durations with the
  // steady clock, so map each stamp onto it relative to now
  const auto sys_now = std::chrono::system_clock::now();
  const auto steady_now = std::chrono::steady_clock::now();
  auto sys_time = [](utime_t u) {
    return std::chrono::system_clock::time_point(
      std::chrono::duration_cast<std::chrono::system_clock::duration>(
        std::chrono::nanoseconds(u.to_nsec())));
  };
  auto steady_time = [&](utime_t u) {
    return otel_common::SteadyTimestamp(steady_now - (sys_now - sys_time(u)));
  };
  auto start_opts = [&](utime_t u) {
    otel_trace::StartSpanOptions opts;
    opts.start_system_time = otel_common::SystemTimestamp(sys_time(u));
    opts.start_steady_time = steady_time(u);
    return opts;
  };
  auto end_opts = [&](utime_t u) {
    otel_trace::EndSpanOptions opts;
    opts.end_steady_time = steady_time(u);
    return opts;
  };

  auto opts = start_opts(t.start);
  if (parent.IsValid()) {
    opts.parent = parent;
  }
  auto span = tracer->StartSpan(t.name, opts);
  for (const auto& [key, value] : t.attributes) {
    span->SetAttribute(key, opentelemetry::nostd::string_view(value));
  }
  const double total = t.end - t.start;
  span->SetAttribute("duration_s", total);
  if (!t.complete) {
    span->SetAttribute("in_flight", true);
  }
  // the time between two consecutive events is a phase, named after both, as
  // "waiting for subops from 1,2 -> sub_op_commit_rec from osd.1". Events
  // stamped outside the op's lifetime are skipped: some stamps can be unset.
  const std::pair<utime_t, std::string>* prev = nullptr;
  for (const auto& event : t.events) {
    const auto& [stamp, name] = event;
    if (stamp < t.start || stamp > t.end) {
      continue;
    }
    span->AddEvent(name, otel_common::SystemTimestamp(sys_time(stamp)));
    if (prev) {
      const double length = stamp - prev->first;
      if (length > 0 && length >= total * min_phase_share) {
        auto phase_opts = start_opts(prev->first);
        phase_opts.parent = span->GetContext();
        tracer->StartSpan(prev->second + " -> " + name, phase_opts)->End(end_opts(stamp));
      }
    }
    prev = &event;
  }
  if (prev && !t.complete && t.end - prev->first >= total * min_phase_share) {
    // an op still in flight is stuck in the phase after its last event
    auto phase_opts = start_opts(prev->first);
    phase_opts.parent = span->GetContext();
    tracer->StartSpan(prev->second + " -> (in flight)", phase_opts)->End(end_opts(t.end));
  }
  span->End(end_opts(t.end));

  char trace_id[2 * TraceIdkSize];
  span->GetContext().trace_id().ToLowerBase16(trace_id);
  ldout(cct, 10) << "recorded op trace " << std::string_view(trace_id, sizeof(trace_id))
                 << " for " << t.name << " lasting " << total << "s" << dendl;
  return std::string(trace_id, sizeof(trace_id));
}

} // namespace tracing

#endif // HAVE_JAEGER

