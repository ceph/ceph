// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "tracer.h"

#ifdef HAVE_JAEGER

namespace tracing {

const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> Tracer::noop_tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("no-op", OPENTELEMETRY_SDK_VERSION);

Tracer::Tracer(opentelemetry::nostd::string_view service_name) {
  init(service_name);
}

void Tracer::init(opentelemetry::nostd::string_view service_name) {
  if (!tracer) {
    const opentelemetry::exporter::jaeger::JaegerExporterOptions opts;
    auto jaeger_exporter  = std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>(new opentelemetry::exporter::jaeger::JaegerExporter(opts));
    auto processor = std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>(new opentelemetry::sdk::trace::SimpleSpanProcessor(std::move(jaeger_exporter)));
    const auto jaeger_resource = opentelemetry::sdk::resource::Resource::Create(std::move(opentelemetry::sdk::resource::ResourceAttributes{{"service.name", service_name}}));
    const auto provider = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(new opentelemetry::sdk::trace::TracerProvider(std::move(processor), jaeger_resource));
    tracer = provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);
  }
}

void Tracer::shutdown() {
  if (tracer) {
    tracer->CloseWithMicroseconds(1);
  }
}

jspan Tracer::start_trace(opentelemetry::nostd::string_view trace_name) {
  if (is_enabled()) {
    return tracer->StartSpan(trace_name);
  }
  return noop_tracer->StartSpan(trace_name);
}

jspan Tracer::add_span(opentelemetry::nostd::string_view span_name, jspan& parent_span) {
  if (is_enabled() && parent_span) {
    const auto parent_ctx = parent_span->GetContext();
    if (parent_ctx.IsValid()) {
      opentelemetry::trace::StartSpanOptions span_opts;
      span_opts.parent = parent_ctx;
      return tracer->StartSpan(span_name, span_opts);
    }
  }
  return noop_tracer->StartSpan(span_name);
}

bool Tracer::is_enabled() const {
  return g_ceph_context->_conf->jaeger_tracing_enable;
}

} // namespace tracing

#endif // HAVE_JAEGER
