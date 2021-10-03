// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef TRACER_H
#define TRACER_H

#include "acconfig.h"
#include "include/buffer.h"

#ifdef HAVE_JAEGER

#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <jaegertracing/Tracer.h>

typedef std::unique_ptr<opentracing::Span> jspan;
typedef jaegertracing::SpanContext jspan_context;

namespace tracing {

class Tracer {
 private:
  const static std::shared_ptr<opentracing::Tracer> noop_tracer;
  std::shared_ptr<opentracing::Tracer> open_tracer;

 public:
  Tracer() = default;
  Tracer(opentracing::string_view service_name);

  void init(opentracing::string_view service_name);
  void shutdown();

  bool is_enabled() const;
  // creates and returns a new span with `trace_name`
  // this span represents a trace, since it has no parent.
  jspan start_trace(opentracing::string_view trace_name);
  // creates and returns a new span with `span_name` which parent span is `parent_span'
  jspan add_span(opentracing::string_view span_name, jspan& parent_span);
  jspan add_span(opentracing::string_view span_name, const jspan_context& parent_ctx);

};

void encode(jspan& span, ceph::buffer::list& bl);
void decode(ceph::buffer::list::const_iterator& bl, jspan_context& span_ctx);

} // namespace tracing

namespace jaeger_configuration {

inline const jaegertracing::samplers::Config const_sampler("const", 1, "", 0, jaegertracing::samplers::Config::defaultSamplingRefreshInterval());

inline const jaegertracing::reporters::Config reporter_default_config(jaegertracing::reporters::Config::kDefaultQueueSize, jaegertracing::reporters::Config::defaultBufferFlushInterval(), true, jaegertracing::reporters::Config::kDefaultLocalAgentHostPort, "");

inline const jaegertracing::propagation::HeadersConfig headers_config("", "", "", "");

inline const jaegertracing::baggage::RestrictionsConfig baggage_config(false, "", std::chrono::steady_clock::duration());

}

#else  // !HAVE_JAEGER

#include <string_view>

class Value {
 public:
  template <typename T> Value(T val) {}
};

struct span_stub {
  template <typename T>
  void SetTag(std::string_view key, const T& value) const noexcept {}
  void Log(std::initializer_list<std::pair<std::string_view, Value>> fields) {}
};

class jspan {
  span_stub span;
 public:
  span_stub& operator*() { return span; }
  const span_stub& operator*() const { return span; }

  span_stub* operator->() { return &span; }
  const span_stub* operator->() const { return &span; }

  operator bool() const { return false; }
};

struct jspan_context {};

namespace tracing {

struct Tracer {
  bool is_enabled() const { return false; }
  jspan start_trace(std::string_view) { return {}; }
  jspan add_span(std::string_view, const jspan&) { return {}; }
  jspan add_span(std::string_view span_name, const jspan_context& parent_ctx) { return {}; }
  void init(std::string_view service_name) {}
  void shutdown() {}
};
  inline void encode(jspan& span, ceph::buffer::list& bl) {}
  inline void decode(ceph::buffer::list::const_iterator& bl, jspan_context& span_ctx) {}
}

#endif // !HAVE_JAEGER

#endif // TRACER_H
