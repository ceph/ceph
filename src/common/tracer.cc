// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "tracer.h"

#ifdef HAVE_JAEGER

namespace tracing {

using bufferlist = ceph::buffer::list;

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

jspan Tracer::add_span(opentracing::string_view span_name, const jspan_context& parent_ctx) {
  if (is_enabled() && parent_ctx != jaegertracing::SpanContext()) {
    return open_tracer->StartSpan(span_name, { opentracing::ChildOf(&parent_ctx) });
  }
  return noop_tracer->StartSpan(span_name);
}

bool Tracer::is_enabled() const {
  return g_ceph_context->_conf->jaeger_tracing_enable;
}


void encode(jspan& span, bufferlist& bl) {
  if (span) {
    const auto* jaegerCtx = static_cast<const jaegertracing::SpanContext*>(&span->context());
    ENCODE_START(1, 1, bl);
    encode(jaegerCtx->traceID().high(), bl);
    encode(jaegerCtx->traceID().low(), bl);
    encode(jaegerCtx->spanID(), bl);
    encode(jaegerCtx->parentID(), bl);
    encode(jaegerCtx->flags(), bl);
    encode(static_cast<uint32_t>(jaegerCtx->baggage().size()), bl);
    for (auto&& pair : jaegerCtx->baggage()) {
      auto&& key = pair.first;
      encode(static_cast<uint32_t>(key.size()), bl);
      encode(key.c_str(), bl);
      auto&& value = pair.second;
      encode(static_cast<uint32_t>(value.size()), bl);
      encode(value.c_str(), bl);
    }
    ENCODE_FINISH(bl);

  }
}

void decode(jspan_context& span_ctx, bufferlist::const_iterator& bl) {
  using namespace jaegertracing;
  if(bl.get_remaining() > 0){
    DECODE_START(1, bl);
    uint64_t trace_id_high;
    uint64_t trace_id_low;
    uint64_t span_id;
    uint64_t parent_id;
    char flags;
    uint32_t num_baggage_items;
    SpanContext::StrMap baggage;

    decode(trace_id_high, bl);
    decode(trace_id_low, bl);
    decode(span_id, bl);
    decode(parent_id, bl);
    decode(flags, bl);
    decode(num_baggage_items, bl);

    baggage.reserve(num_baggage_items);
    for (uint32_t i = 0; i < num_baggage_items; ++i) {
      uint32_t key_length;
      decode(key_length, bl);
      std::string key(key_length, '\0');
      decode(key, bl);

      uint32_t value_length;
      decode(value_length, bl);
      std::string value(value_length, '\0');
      decode(value, bl);

      baggage[key] = value;
    }
    TraceID trace_id(trace_id_high, trace_id_low);
    SpanContext ctx(trace_id, span_id, parent_id, flags, baggage);
    ctx.swap(span_ctx);
    DECODE_FINISH(bl);
  }
}

} // namespace tracing

#endif // HAVE_JAEGER
