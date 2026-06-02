// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "include/common_fwd.h"
#include "include/encoding.h"
#include <map>
#include <mutex>
#include <string>

#ifdef HAVE_JAEGER
#include "opentelemetry/trace/provider.h"

using otel_span_t = opentelemetry::trace::Span;
using otel_span_ref = opentelemetry::nostd::shared_ptr<otel_span_t>;
using otel_span_context_t = opentelemetry::trace::SpanContext;
using otel_attribute_t = opentelemetry::common::AttributeValue;

namespace tracing {

static constexpr int TraceIdkSize = 16;
static constexpr int SpanIdkSize = 8;
static_assert(TraceIdkSize == opentelemetry::trace::TraceId::kSize);
static_assert(SpanIdkSize == opentelemetry::trace::SpanId::kSize);

const inline otel_span_context_t noop_span_ctx{false, false};

class Tracer {
 private:
  CephContext* cct = nullptr;
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer;
  
  // Static flag to track if a real (non-noop) tracer provider has been initialized
  // This allows daemons to initialize tracing while preventing libraries from overwriting it
  inline static bool provider_initialized = false;

  // Root service name for hierarchical tracer naming
  // Set once by daemon main(), used by all tracers in the process
  inline static std::string root_service_name = "";
  inline static bool root_service_name_set = false;
  inline static std::mutex provider_config_mutex;

  // Additional provider resource attributes, set once before provider init
  inline static std::map<std::string, std::string, std::less<>> resource_attributes;

 public:

  Tracer() = default;
  Tracer(CephContext* cct, opentelemetry::nostd::string_view service_name);

  void init(CephContext* _cct, opentelemetry::nostd::string_view service_name);

  bool is_enabled() const;
  
  // Root service name management for hierarchical naming
  // Should be called once by daemon main() before any tracer initialization
  static void set_root_service_name(std::string_view name);
  static std::string get_root_service_name();
  static bool has_root_service_name();
  static void set_resource_attribute(std::string_view key, std::string_view value);
  
  // creates and returns a new span with `trace_name`
  // this span represents a trace, since it has no parent.
  otel_span_ref start_trace(opentelemetry::nostd::string_view trace_name);

  // creates and returns a new span with `trace_name`
  // if false is given to `trace_is_enabled` param, noop span will be returned
  otel_span_ref start_trace(opentelemetry::nostd::string_view trace_name, bool trace_is_enabled);

  // creates and returns a new span with `span_name` which parent span is `parent_span'
  otel_span_ref add_span(opentelemetry::nostd::string_view span_name, const otel_span_ref& parent_span);
  // creates and return a new span with `span_name`
  // the span is added to the trace which it's context is `parent_ctx`.
  // parent_ctx contains the required information of the trace.
  otel_span_ref add_span(opentelemetry::nostd::string_view span_name, const otel_span_context_t& parent_ctx);

  // A No-op span that can be safely passed to anything expecting a span
  inline static const otel_span_ref noop_span = [] {
  auto provider = opentelemetry::trace::Provider::GetTracerProvider();
  auto tracer = provider->GetTracer("no-op", "");
  auto span = tracer->StartSpan("noop");
  
  // Ensure we never return a null pointer
  if (!span || !span.get()) {
    // Create a minimal valid no-op span if the provider fails
    // This should never happen with a proper OpenTelemetry setup
    ceph_abort("OpenTelemetry noop_span initialization failed - returned null pointer");
  }
  
  return span;
  }();
};

inline void encode(const otel_span_context_t& span_ctx, bufferlist& bl, uint64_t f = 0) {
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

inline void decode(otel_span_context_t& span_ctx, bufferlist::const_iterator& bl) {
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

#include <string_view>

class Value {
 public:
  template <typename T> Value(T val) {}
};

using otel_attribute_t = Value;

namespace opentelemetry {
inline namespace v1 {
namespace trace {
class SpanContext {
public:
  SpanContext() = default;
  SpanContext(bool sampled_flag, bool is_remote) {}
  bool IsValid() const { return false;}
};
} // namespace trace
} // namespace v1
} // namespace opentelemetry

using otel_span_context_t = opentelemetry::v1::trace::SpanContext;

class otel_span_t {
  otel_span_context_t _ctx;
public:
  template <typename T>
  void SetAttribute(std::string_view key, const T& value) const noexcept {}
  void AddEvent(std::string_view) const {}
  void AddEvent(std::string_view, std::initializer_list<std::pair<std::string_view, otel_attribute_t>> fields) const {}
  template <typename T> void AddEvent(std::string_view name, const T& fields = {}) const {}
  otel_span_context_t GetContext() const { return _ctx; }
  void UpdateName(std::string_view) const {}
  void End() const {}
  bool IsRecording() const { return false; }
};

class otel_span_ref {
  otel_span_t span;
public:
  otel_span_t& operator*() { return span; }
  const otel_span_t& operator*() const { return span; }
  otel_span_t* operator->() { return &span; }
  const otel_span_t* operator->() const { return &span; }
  operator bool() const { return false; }
  otel_span_t* get() { return &span; }
  const otel_span_t* get() const { return &span; }
};

namespace tracing {
const static otel_span_context_t noop_span_ctx{};

struct Tracer {
  Tracer() = default;
  Tracer(CephContext*, std::string_view) {}

  void init(CephContext* _cct, std::string_view service_name) {}
  bool is_enabled() const { return false; }
  otel_span_ref start_trace(std::string_view, bool enabled = true) { return {}; }
  otel_span_ref add_span(std::string_view, const otel_span_ref&) { return {}; }
  otel_span_ref add_span(std::string_view span_name, const otel_span_context_t& parent_ctx) { return {}; }

  static inline const otel_span_ref noop_span{};
};

inline void encode(const otel_span_context_t& span_ctx, bufferlist& bl, uint64_t f = 0) {
  ENCODE_START(1, 1, bl);
  // jaeger is missing, set "is_valid" to false.
  bool is_valid = false;
  encode(is_valid, bl);
  ENCODE_FINISH(bl);
}

inline void decode(otel_span_context_t& span_ctx, bufferlist::const_iterator& bl) {
  DECODE_START(254, bl);
  // jaeger is missing, consume the buffer but do not decode it.
  DECODE_FINISH(bl);
}

} // namespace tracing

#endif // !HAVE_JAEGER
