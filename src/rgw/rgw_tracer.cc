// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "rgw_tracer.h"

#ifdef HAVE_JAEGER

thread_local tracing::Tracer rgw_tracer(jaeger_configuration::jaeger_default_config);

namespace tracing {

const std::shared_ptr<opentracing::Tracer> Tracer::noop_tracer = opentracing::MakeNoopTracer();

Tracer::Tracer(jaegertracing::Config& conf):open_tracer(jaegertracing::Tracer::make(conf)) {}

std::unique_ptr<opentracing::Span> Tracer::start_trace(opentracing::string_view trace_name) {
  if(is_enabled()) {
    return open_tracer->StartSpan(trace_name);
  }
  return noop_tracer->StartSpan(trace_name);
}

std::unique_ptr<opentracing::Span> Tracer::start_span(opentracing::string_view span_name, std::unique_ptr<opentracing::Span>& parent_span) {
  if(is_enabled()) {
    return open_tracer->StartSpan(span_name, { opentracing::ChildOf(&parent_span->context()) });
  }
  return noop_tracer->StartSpan(span_name);
}

bool Tracer::is_enabled() const {
  return g_ceph_context->_conf->rgw_jaeger_enable;
}
} // namespace tracing

namespace jaeger_configuration {

jaegertracing::samplers::Config const_sampler("const", 1, "", 0, jaegertracing::samplers::Config::defaultSamplingRefreshInterval());

jaegertracing::reporters::Config reporter_default_config(jaegertracing::reporters::Config::kDefaultQueueSize, jaegertracing::reporters::Config::defaultBufferFlushInterval(), true, jaegertracing::reporters::Config::kDefaultLocalAgentHostPort, "");

jaegertracing::propagation::HeadersConfig headers_config("","","","");

jaegertracing::baggage::RestrictionsConfig baggage_config(false, "", std::chrono::steady_clock::duration());

jaegertracing::Config jaeger_default_config(false, const_sampler, reporter_default_config, headers_config, baggage_config, "rgw", std::vector<jaegertracing::Tag>());

}

#else // !HAVE_JAEGER

tracing::Tracer rgw_tracer;

#endif
 