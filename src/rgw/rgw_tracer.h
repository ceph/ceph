// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_TRACER_H
#define RGW_TRACER_H

#ifdef HAVE_JAEGER

#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <jaegertracing/Tracer.h>

typedef std::unique_ptr<opentracing::Span> jspan;

#else  // !HAVE_JAEGER

struct span_stub {
  template <typename T>
  void SetTag(std::string_view key, const T& value) const noexcept {}
  void Log(std::initializer_list<std::pair<std::string_view, std::string_view>> fields) {}
};

class jspan {
  span_stub span;
 public:
  span_stub& operator*() { return span; }
  const span_stub& operator*() const { return span; }

  span_stub* operator->() { return &span; }
  const span_stub* operator->() const { return &span; }
};

#endif // !HAVE_JAEGER

namespace tracing {

const auto OP = "op";
const auto BUCKET_NAME = "bucket_name";
const auto USER_ID = "user_id";
const auto OBJECT_NAME = "object_name";
const auto RETURN = "return";
const auto UPLOAD_ID = "upload_id";
const auto TYPE = "type";
const auto REQUEST = "request";

#ifdef HAVE_JAEGER

class Tracer {
private:
  const static std::shared_ptr<opentracing::Tracer> noop_tracer;
  std::shared_ptr<opentracing::Tracer> open_tracer;

public:
  Tracer(jaegertracing::Config& conf);
  bool is_enabled() const;
  // creates and returns a new span with `trace_name`
  // this span represents a trace, since it has no parent.
  jspan start_trace(opentracing::string_view trace_name);
  // creates and returns a new span with `trace_name` which parent span is `parent_span'
  jspan start_span(opentracing::string_view span_name, jspan& parent_span);
};

#else // !HAVE_JAEGER

struct Tracer {
  bool is_enabled() const { return false; }
  jspan start_trace(std::string_view) { return {}; }
  jspan start_span(std::string_view, const jspan&) { return {}; }
};

#endif

} // namespace tracing


#ifdef HAVE_JAEGER

extern thread_local tracing::Tracer rgw_tracer;

namespace jaeger_configuration {

extern jaegertracing::Config jaeger_default_config;

}

#else // !HAVE_JAEGER

extern tracing::Tracer rgw_tracer;

#endif

#endif // RGW_TRACER_H
 