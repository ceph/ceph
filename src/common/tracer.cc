// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "tracer.h"

#ifdef HAVE_JAEGER
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/exporters/jaeger/jaeger_exporter.h"

namespace tracing {

const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> Tracer::noop_tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("no-op", OPENTELEMETRY_SDK_VERSION);
const jspan Tracer::noop_span = noop_tracer->StartSpan("noop");

using bufferlist = ceph::buffer::list;

Tracer::Tracer(opentelemetry::nostd::string_view service_name) {
  init(service_name);
}

void Tracer::init(opentelemetry::nostd::string_view service_name) {
  if (!tracer) {
    opentelemetry::exporter::jaeger::JaegerExporterOptions exporter_options;
    exporter_options.server_port = 6799;
    const opentelemetry::sdk::trace::BatchSpanProcessorOptions processor_options;
    const auto jaeger_resource = opentelemetry::sdk::resource::Resource::Create(std::move(opentelemetry::sdk::resource::ResourceAttributes{{"service.name", service_name}}));
    auto jaeger_exporter = std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>(new opentelemetry::exporter::jaeger::JaegerExporter(exporter_options));
    auto processor = std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>(new opentelemetry::sdk::trace::BatchSpanProcessor(std::move(jaeger_exporter), processor_options));
    const auto provider = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(new opentelemetry::sdk::trace::TracerProvider(std::move(processor), jaeger_resource));
    opentelemetry::trace::Provider::SetTracerProvider(provider);
    tracer = provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);
  }
}

jspan Tracer::start_trace(opentelemetry::nostd::string_view trace_name) {
  if (is_enabled()) {
    return tracer->StartSpan(trace_name);
  }
  return noop_span;
}

jspan Tracer::start_trace(opentelemetry::nostd::string_view trace_name, bool trace_is_enabled) {
  if (trace_is_enabled) {
    return tracer->StartSpan(trace_name);
  }
  return noop_tracer->StartSpan(trace_name);
}

jspan Tracer::add_span(opentelemetry::nostd::string_view span_name, const jspan& parent_span) {
  if (is_enabled() && parent_span && parent_span->IsRecording()) {
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_span->GetContext();
    return tracer->StartSpan(span_name, span_opts);
  }
  return noop_span;
}

jspan Tracer::add_span(opentelemetry::nostd::string_view span_name, const jspan_context& parent_ctx) {
  if (is_enabled() && parent_ctx.IsValid()) {
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_ctx;
    return tracer->StartSpan(span_name, span_opts);
  }
  return noop_span;
}

bool Tracer::is_enabled() const {
  return g_ceph_context->_conf->jaeger_tracing_enable;
}

} // namespace tracing

#endif // HAVE_JAEGER
