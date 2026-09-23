// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/ceph_context.h"
#include "tracer.h"
#include "common/debug.h"

namespace tracing {

std::vector<OpPhase> op_phases(const OpTimeline& t, double min_share) {
  std::vector<OpPhase> phases;
  const double total = t.end - t.start;
  auto add = [&](utime_t start, utime_t end, std::string name) {
    const double length = end - start;
    if (length > 0 && length >= total * min_share) {
      phases.push_back({std::move(name), start, end});
    }
  };
  const std::pair<utime_t, std::string>* prev = nullptr;
  for (const auto& event : t.events) {
    if (!t.in_lifetime(event.first)) {
      continue;
    }
    if (prev) {
      add(prev->first, event.first, prev->second + " -> " + event.second);
    }
    prev = &event;
  }
  if (prev && !t.complete) {
    add(prev->first, t.end, prev->second + " -> (in flight)");
  }
  return phases;
}

} // namespace tracing

#ifdef HAVE_JAEGER
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/exporters/jaeger/jaeger_exporter.h"
#ifdef HAVE_OTLP
#include "opentelemetry/exporters/otlp/otlp_http_exporter.h"
#endif

#define dout_subsys ceph_subsys_trace
#undef dout_prefix
#define dout_prefix (*_dout << "otel_tracing: ")

namespace tracing {

const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> Tracer::noop_tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("no-op", OPENTELEMETRY_SDK_VERSION);
const jspan_ptr Tracer::noop_span = noop_tracer->StartSpan("noop");

using bufferlist = ceph::buffer::list;

// the exporter selected by trace_exporter. The exporters are created with new
// and owned through SpanExporter: make_unique<JaegerExporter> would instantiate
// JaegerExporter's implicit destructor here, where the ThriftSender it owns is
// an incomplete type
static std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> make_exporter(CephContext* cct) {
  using exporter_ptr = std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>;
  if (cct->_conf.get_val<std::string>("trace_exporter") == "otlp") {
#ifdef HAVE_OTLP
    opentelemetry::exporter::otlp::OtlpHttpExporterOptions options;
    options.url = cct->_conf.get_val<std::string>("trace_otlp_endpoint");
    options.content_type = opentelemetry::exporter::otlp::HttpRequestContentType::kBinary;
    ldout(cct, 1) << "exporting spans over OTLP/HTTP to " << options.url << dendl;
    return exporter_ptr(new opentelemetry::exporter::otlp::OtlpHttpExporter(options));
#else
    lderr(cct) << "trace_exporter is otlp, but this build has no OTLP support;"
               << " exporting over Jaeger UDP instead" << dendl;
#endif
  }
  opentelemetry::exporter::jaeger::JaegerExporterOptions options;
  options.endpoint = cct->_conf.get_val<std::string>("jaeger_agent_host");
  options.server_port = cct->_conf.get_val<int64_t>("jaeger_agent_port");
  ldout(cct, 1) << "exporting spans to " << options.endpoint << ":"
                << options.server_port << dendl;
  return exporter_ptr(new opentelemetry::exporter::jaeger::JaegerExporter(options));
}

Tracer::tracer_ptr Tracer::make_tracer() {
  const opentelemetry::sdk::trace::BatchSpanProcessorOptions processor_options;
  const auto jaeger_resource = opentelemetry::sdk::resource::Resource::Create(std::move(opentelemetry::sdk::resource::ResourceAttributes{{"service.name", service_name}}));
  auto processor = std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>(new opentelemetry::sdk::trace::BatchSpanProcessor(make_exporter(cct), processor_options));
  const auto provider = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(new opentelemetry::sdk::trace::TracerProvider(std::move(processor), jaeger_resource));
  opentelemetry::trace::Provider::SetTracerProvider(provider);
  return provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);
}

void Tracer::init(CephContext* _cct, opentelemetry::nostd::string_view _service_name) {
  ceph_assert(_cct);
  cct = _cct;
  std::unique_lock l(tracer_lock);
  if (!tracer) {
    ldout(cct, 3) << "tracer was not loaded, initializing tracing" << dendl;
    service_name = std::string(_service_name);
    tracer = make_tracer();
  }
}

void Tracer::reconfigure() {
  if (!cct || !get_tracer()) {
    return;  // not initialized; init() will read the new settings
  }
  // the previous provider flushes its pending spans when its last span ends
  auto t = make_tracer();
  std::unique_lock l(tracer_lock);
  tracer = std::move(t);
}

jspan_ptr Tracer::start_trace(opentelemetry::nostd::string_view trace_name) {
  ceph_assert(cct);
  if (is_enabled()) {
    auto t = get_tracer();
    ceph_assert(t);
    ldout(cct, 20) << "start trace for " << trace_name << " " << dendl;
    return t->StartSpan(trace_name);
  }
  return noop_span;
}

jspan_ptr Tracer::start_trace(opentelemetry::nostd::string_view trace_name, bool trace_is_enabled) {
  ceph_assert(cct);
  ldout(cct, 20) << "start trace enabled " << trace_is_enabled << " " << dendl;
  if (trace_is_enabled) {
    auto t = get_tracer();
    ceph_assert(t);
    ldout(cct, 20) << "start trace for " << trace_name << " " << dendl;
    return t->StartSpan(trace_name);
  }
  return noop_tracer->StartSpan(trace_name);
}

jspan_ptr Tracer::add_span(opentelemetry::nostd::string_view span_name, const jspan_ptr& parent_span) {
  if (is_enabled() && parent_span && parent_span->IsRecording()) {
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_span->GetContext();
    ldout(cct, 20) << "adding span " << span_name << " " << dendl;
    return get_tracer()->StartSpan(span_name, span_opts);
  }
  return noop_span;
}

jspan_ptr Tracer::add_span(opentelemetry::nostd::string_view span_name, const jspan_context& parent_ctx) {
  if (parent_ctx.IsValid()) {
    auto t = get_tracer();
    ceph_assert(t);
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_ctx;
    ldout(cct, 20) << "adding span " << span_name << " " << dendl;
    return t->StartSpan(span_name, span_opts);
  }
  return noop_span;
}

bool Tracer::is_enabled() const {
  return cct->_conf->jaeger_tracing_enable;
}

// phases shorter than this share of the op are kept as span events only
static constexpr double min_phase_share = 0.01;

std::string Tracer::record_op(const OpTimeline& t, const jspan_context& parent) {
  auto tracer = get_tracer();
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
  for (const auto& [stamp, name] : t.events) {
    if (t.in_lifetime(stamp)) {
      span->AddEvent(name, otel_common::SystemTimestamp(sys_time(stamp)));
    }
  }
  // e.g. "waiting for subops from 1,2 -> sub_op_commit_rec from osd.1"
  for (const auto& phase : op_phases(t, min_phase_share)) {
    auto phase_opts = start_opts(phase.start);
    phase_opts.parent = span->GetContext();
    tracer->StartSpan(phase.name, phase_opts)->End(end_opts(phase.end));
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

