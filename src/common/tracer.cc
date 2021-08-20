// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "tracer.h"

#ifdef HAVE_JAEGER

namespace tracing {

const std::shared_ptr<opentracing::Tracer> Tracer::noop_tracer = opentracing::MakeNoopTracer();

Tracer::Tracer(opentracing::string_view service_name) {
  init(service_name);
}

void Tracer::init(opentracing::string_view service_name) {
  if (!open_tracer) {
    using namespace jaeger_configuration;
    const jaegertracing::Config conf(false, const_sampler, reporter_default_config, headers_config, baggage_config, service_name, std::vector<jaegertracing::Tag>());
    open_tracer = jaegertracing::Tracer::make(conf);
  }
}

void Tracer::shutdown() {
  open_tracer.reset();
}

jspan Tracer::start_trace(opentracing::string_view trace_name) {
  if (is_enabled()) {
    return open_tracer->StartSpan(trace_name);
  }
  return noop_tracer->StartSpan(trace_name);
}

jspan Tracer::add_span(opentracing::string_view span_name, jspan& parent_span) {
  if (is_enabled()) {
    if (parent_span) {
      return open_tracer->StartSpan(span_name, { opentracing::ChildOf(&parent_span->context()) });
    }
    return open_tracer->StartSpan(span_name);
  }
  return noop_tracer->StartSpan(span_name);
}

bool Tracer::is_enabled() const {
  return g_ceph_context->_conf->jaeger_tracing_enable;
}
} // namespace tracing

#endif // HAVE_JAEGER
