// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/common_fwd.h"
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

using bufferlist = ceph::buffer::list;

Tracer::Tracer(CephContext* _cct,
	       opentelemetry::nostd::string_view service_name) {
  init(_cct, service_name);
}

void Tracer::init(CephContext* _cct, opentelemetry::nostd::string_view library_name) {
  ceph_assert(_cct);
  cct = _cct;
  if (!tracer) {
    // Check if root service name has been set, otherwise use default "io.ceph"
    std::string provider_service_name;
    {
      std::lock_guard<std::mutex> lock(provider_config_mutex);
      if (root_service_name_set) {
        provider_service_name = root_service_name;
      } else {
        provider_service_name = "io.ceph";
      }
    }

    ldout(_cct, 3) << "Initializing tracer for component: " << library_name << dendl;
    
    // Only set up a provider if one hasn't been initialized yet
    if (!provider_initialized) {
      opentelemetry::exporter::jaeger::JaegerExporterOptions exporter_options;
      exporter_options.server_port = cct->_conf.get_val<int64_t>("jaeger_agent_port");
      ldout(_cct, 3) << "Initializing tracer provider for service: " << provider_service_name
                     << " on port: " << exporter_options.server_port << dendl;
      const opentelemetry::sdk::trace::BatchSpanProcessorOptions processor_options;
      opentelemetry::sdk::resource::ResourceAttributes attrs{
        {"service.name", provider_service_name}
      };
      {
        std::lock_guard<std::mutex> lock(provider_config_mutex);
        for (const auto& [key, value] : resource_attributes) {
          ldout(_cct, 3) << "Adding tracer resource attribute: "
                         << key << "=" << value << dendl;
          attrs[key] = value;
        }
      }
      const auto jaeger_resource =
        opentelemetry::sdk::resource::Resource::Create(std::move(attrs));
      auto jaeger_exporter = std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>(
        new opentelemetry::exporter::jaeger::JaegerExporter(exporter_options));
      auto processor = std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>(
        new opentelemetry::sdk::trace::BatchSpanProcessor(std::move(jaeger_exporter), processor_options));
      const auto provider = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(
        new opentelemetry::sdk::trace::TracerProvider(std::move(processor), jaeger_resource));
      opentelemetry::trace::Provider::SetTracerProvider(provider);
      provider_initialized = true;
    }

    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    ldout(_cct, 3) << "Using tracer provider " << provider.get() << " (service: " << provider_service_name << ") for library: " << library_name << dendl;
    tracer = provider->GetTracer(library_name, OPENTELEMETRY_SDK_VERSION);
  }
}

void Tracer::set_root_service_name(std::string_view name) {
  std::lock_guard<std::mutex> lock(provider_config_mutex);
  
  if (root_service_name_set) {
    // Already set - this is an error
    throw std::runtime_error(
      std::string("Root service name already set to '") + root_service_name +
      "', cannot set to '" + std::string(name) + "'");
  }
  
  root_service_name = std::string(name);
  root_service_name_set = true;
}

std::string Tracer::get_root_service_name() {
  std::lock_guard<std::mutex> lock(provider_config_mutex);
  return root_service_name;
}

bool Tracer::has_root_service_name() {
  std::lock_guard<std::mutex> lock(provider_config_mutex);
  return root_service_name_set;
}

void Tracer::set_resource_attribute(std::string_view key, std::string_view value) {
  std::lock_guard<std::mutex> lock(provider_config_mutex);
  resource_attributes[std::string(key)] = std::string(value);
}

#ifndef WITH_CRIMSON
otel_span_ref Tracer::start_trace(opentelemetry::nostd::string_view trace_name) {
  ceph_assert(cct);
  if (is_enabled()) {
    ceph_assert(tracer);
    ldout(cct, 20) << "start trace for " << trace_name << " " << dendl;
    return tracer->StartSpan(trace_name);
  }
  return noop_span;
}

otel_span_ref Tracer::start_trace(opentelemetry::nostd::string_view trace_name, bool trace_is_enabled) {
  ceph_assert(cct);
  ldout(cct, 20) << "start trace enabled " << trace_is_enabled << " " << dendl;
  if (trace_is_enabled) {
    ceph_assert(tracer);
    ldout(cct, 20) << "start trace for " << trace_name << " " << dendl;
    return tracer->StartSpan(trace_name);
  }
  return noop_span;
}

// WARNING: When working with Messages, do NOT use Message::trace as parent_span!
// Message::trace is NOT serialized over the network and will be noop_span on the
// receiving side (IsRecording() will return false). Instead, use the overload that
// takes otel_span_context_t and pass Message::otel_trace, which contains the serialized
// span context (trace_id, span_id) needed to create proper child spans.
otel_span_ref Tracer::add_span(opentelemetry::nostd::string_view span_name, const otel_span_ref& parent_span) {
  if (is_enabled() && parent_span && parent_span->IsRecording()) {
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_span->GetContext();
    ldout(cct, 20) << "adding span " << span_name << " " << dendl;
    return tracer->StartSpan(span_name, span_opts);
  }
  return noop_span;
}

otel_span_ref Tracer::add_span(opentelemetry::nostd::string_view span_name, const otel_span_context_t& parent_ctx) {
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
  if (!cct) [[unlikely]] {
    return false;
  }
  return cct->_conf->jaeger_tracing_enable;
}
#endif // WITH_CRIMSON

} // namespace tracing

#endif // HAVE_JAEGER
