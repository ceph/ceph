// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "tracer.h"
#include "common/debug.h"
#include "include/encoding.h"

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

void encode(const jspan_context& span_ctx, bufferlist& bl, uint64_t f) {
  ENCODE_START(1, 1, bl);
  using namespace opentelemetry;
  using namespace trace;
  auto is_valid = span_ctx.IsValid();
  encode(is_valid, bl);
  if (is_valid) {
    encode_nohead(std::string_view(reinterpret_cast<const char*>(span_ctx.trace_id().Id().data()), TraceIdkSize), bl);
    encode_nohead(std::string_view(reinterpret_cast<const char*>(span_ctx.span_id().Id().data()), SpanIdkSize), bl);
    encode(span_ctx.trace_flags().flags(), bl);
  }
  ENCODE_FINISH(bl);
}

void decode(jspan_context& span_ctx, bufferlist::const_iterator& bl) {
  using namespace opentelemetry;
  using namespace trace;
  DECODE_START(1, bl);
  bool is_valid;
  decode(is_valid, bl);
  if (is_valid) {
    std::array<uint8_t, TraceIdkSize> trace_id;
    std::array<uint8_t, SpanIdkSize> span_id;
    uint8_t flags;
    decode(trace_id, bl);
    decode(span_id, bl);
    decode(flags, bl);
    span_ctx = SpanContext(
      TraceId(nostd::span<uint8_t, TraceIdkSize>(trace_id)),
      SpanId(nostd::span<uint8_t, SpanIdkSize>(span_id)),
      TraceFlags(flags),
      true);
  }
  DECODE_FINISH(bl);
}

} // namespace tracing

#else  // !HAVE_JAEGER

namespace tracing {

void encode(const jspan_context& span_ctx, bufferlist& bl, uint64_t f) {
  ENCODE_START(1, 1, bl);
  // jaeger is missing, set "is_valid" to false.
  bool is_valid = false;
  encode(is_valid, bl);
  ENCODE_FINISH(bl);
}

void decode(jspan_context& span_ctx, bufferlist::const_iterator& bl) {
  DECODE_START(254, bl);
  // jaeger is missing, consume the buffer but do not decode it.
  DECODE_FINISH(bl);
}

} // namespace tracing

#endif // !HAVE_JAEGER
