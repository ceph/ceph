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

Tracer::Tracer(CephContext* _cct,
	       opentelemetry::nostd::string_view service_name) {
  init(_cct, service_name);
}

void Tracer::init(CephContext* _cct, opentelemetry::nostd::string_view service_name) {
  ceph_assert(_cct);
  cct = _cct;
  if (!tracer) {
    // Check if a real provider is already configured
    auto existing_provider = opentelemetry::trace::Provider::GetTracerProvider();
    
    // Only set up a new provider if we're still using the default no-op provider
    // This allows daemons (OSD, RGW, etc.) to initialize tracing while preventing
    // libraries (neorados, librbd when used as a library) from overwriting it
    if (dynamic_cast<opentelemetry::trace::NoopTracerProvider*>(existing_provider.get())) {
      // No real provider set yet - we can initialize one (daemon use case)
      opentelemetry::exporter::jaeger::JaegerExporterOptions exporter_options;
      exporter_options.server_port = cct->_conf.get_val<int64_t>("jaeger_agent_port");
      ldout(cct, 3) << "tracer was not loaded, initializing tracing for service: " << service_name
                    << " on port: " << exporter_options.server_port << dendl;
      const opentelemetry::sdk::trace::BatchSpanProcessorOptions processor_options;
      const auto jaeger_resource = opentelemetry::sdk::resource::Resource::Create(std::move(opentelemetry::sdk::resource::ResourceAttributes{{"service.name", service_name}}));
      auto jaeger_exporter = std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>(new opentelemetry::exporter::jaeger::JaegerExporter(exporter_options));
      auto processor = std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>(new opentelemetry::sdk::trace::BatchSpanProcessor(std::move(jaeger_exporter), processor_options));
      const auto provider = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(new opentelemetry::sdk::trace::TracerProvider(std::move(processor), jaeger_resource));
      opentelemetry::trace::Provider::SetTracerProvider(provider);
      tracer = provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);
    } else {
      // A real provider is already set - use it (library use case)
      ldout(cct, 0) << "Using existing tracer provider for service: " << service_name << dendl;
      tracer = existing_provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);
    }
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

// WARNING: When working with Messages, do NOT use Message::trace as parent_span!
// Message::trace is NOT serialized over the network and will be noop_span on the
// receiving side (IsRecording() will return false). Instead, use the overload that
// takes jspan_context and pass Message::otel_trace, which contains the serialized
// span context (trace_id, span_id) needed to create proper child spans.
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
  if (is_enabled() && parent_ctx.IsValid()) {
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

} // namespace tracing

#endif // HAVE_JAEGER
