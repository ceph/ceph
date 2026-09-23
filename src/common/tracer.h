// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "acconfig.h"
#include "include/encoding.h"
#include "include/utime.h"

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace tracing {

using trace_id_t = std::array<uint8_t, 16>;
using span_id_t = std::array<uint8_t, 8>;

// Ids derived from a request id. Every OSD that handles the same request
// derives the same ids, without sending anything new on the wire:
// - trace_id and root_span_id place a request that no client traced: its ops
//   hang off the root span, which is never exported.
// - primary_span_id is the span id of the request's op on the primary, which
//   the replicas' sub-ops and the replies take as their parent.
struct RequestTrace {
  trace_id_t trace_id;
  span_id_t root_span_id;
  span_id_t primary_span_id;
};

// `cluster` is a hash of the cluster fsid, so that clusters sharing one
// tracing backend do not mix up requests that happen to have the same id;
// the other arguments are the fields of the osd_reqid_t.
RequestTrace request_trace(uint64_t cluster, uint8_t name_type, int64_t name_num,
                           int32_t inc, uint64_t tid);

// The timeline of an operation that has already happened, as recorded by the
// op tracker. Tracer::record_op() turns it into a trace after the fact, so
// building it costs nothing on the I/O path.
struct OpTimeline {
  std::string name;
  utime_t start;
  utime_t end;
  bool complete = true;  // false: the op is still in flight at `end`
  std::vector<std::pair<utime_t, std::string>> events;
  std::vector<std::pair<std::string, std::string>> attributes;
  std::vector<std::pair<std::string, int64_t>> int_attributes;
  // where the op's span goes. With trace_id and parent_span_id, it is a child
  // of that span, which may never be exported; with neither, it starts a new
  // trace, with trace_id if given. span_id, if given, is the span's own id,
  // which spans on other daemons may already refer to.
  std::optional<trace_id_t> trace_id;
  std::optional<span_id_t> parent_span_id;
  std::optional<span_id_t> span_id;

  // some stamps can be unset (zero), so events outside the lifetime are ignored
  bool in_lifetime(utime_t stamp) const {
    return stamp >= start && stamp <= end;
  }
};

// the time between two consecutive events of an OpTimeline
struct OpPhase {
  std::string name;  // "<event> -> <next event>"
  utime_t start;
  utime_t end;
};

// the phases of `t` that took at least `min_share` of the op, in order. An op
// still in flight ends with a "<last event> -> (in flight)" phase.
std::vector<OpPhase> op_phases(const OpTimeline& t, double min_share);

// like op_phases(), but named after what the op was doing, for the events the
// OSD records: "receive", "queued for PG", "waiting for rw locks", "execute",
// "local commit", "reply". Waiting for replicas becomes one phase per replica,
// "replica osd.N", all starting when the sub-ops were sent, so the slowest
// replica is the longest bar. Phases may overlap. Unknown events keep the
// "<event> -> <next event>" names.
std::vector<OpPhase> named_phases(const OpTimeline& t, double min_share);

} // namespace tracing

#ifdef HAVE_JAEGER
#include <shared_mutex>

#include "opentelemetry/trace/provider.h"

using jspan = opentelemetry::trace::Span;
using jspan_ptr = opentelemetry::nostd::shared_ptr<jspan>;
using jspan_context = opentelemetry::trace::SpanContext;
using jspan_attribute = opentelemetry::common::AttributeValue;

namespace tracing {

static constexpr int TraceIdkSize = 16;
static constexpr int SpanIdkSize = 8;
static_assert(TraceIdkSize == opentelemetry::trace::TraceId::kSize);
static_assert(SpanIdkSize == opentelemetry::trace::SpanId::kSize);

class Tracer {
 private:
  using tracer_ptr = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>;
  const static tracer_ptr noop_tracer;
  const static jspan_ptr noop_span;
  CephContext* cct = nullptr;;
  std::string service_name;
  mutable std::shared_mutex tracer_lock;  ///< protects tracer, which reconfigure() replaces
  tracer_ptr tracer;

  // a tracer exporting as trace_exporter and its options currently say
  tracer_ptr make_tracer();
  tracer_ptr get_tracer() const {
    std::shared_lock l(tracer_lock);
    return tracer;
  }

 public:

  Tracer() = default;

  void init(CephContext* _cct, opentelemetry::nostd::string_view service_name);
  // re-create the exporter after trace_exporter or one of its options
  // changed; spans already started still go to the previous exporter
  void reconfigure();

  bool is_enabled() const;
  // creates and returns a new span with `trace_name`
  // this span represents a trace, since it has no parent.
  jspan_ptr start_trace(opentelemetry::nostd::string_view trace_name);

  // creates and returns a new span with `trace_name`
  // if false is given to `trace_is_enabled` param, noop span will be returned
  jspan_ptr start_trace(opentelemetry::nostd::string_view trace_name, bool trace_is_enabled);

  // creates and returns a new span with `span_name` which parent span is `parent_span'.
  // Without tracing, returns `parent_span` itself, so that the context it may
  // carry (see context_span()) stays in place for code that swaps spans.
  jspan_ptr add_span(opentelemetry::nostd::string_view span_name, const jspan_ptr& parent_span);
  // creates and return a new span with `span_name`
  // the span is added to the trace which it's context is `parent_ctx`.
  // parent_ctx contains the required information of the trace. Only if this
  // daemon traces and the sender sampled the trace: a context that a client
  // passes along just to place slow-op traces must not cost live spans.
  jspan_ptr add_span(opentelemetry::nostd::string_view span_name, const jspan_context& parent_ctx);

  // a span that records nothing but carries new trace and span ids, marked
  // not sampled, for passing a trace context along without the cost of live
  // spans; record_op() can later export a span with those ids
  jspan_ptr context_span();

  // exports `timeline` as a trace with its recorded timestamps: one span for
  // the op, placed as its trace_id, parent_span_id and span_id say, and a
  // child span for each phase that took a noticeable share of it. Works
  // whether or not jaeger_tracing_enable is set. Returns the trace id as hex,
  // or an empty string if nothing was exported.
  std::string record_op(const OpTimeline& timeline);
};

// the ids of a valid context
inline bool context_ids(const jspan_context& ctx, trace_id_t* trace_id, span_id_t* span_id) {
  if (!ctx.IsValid()) {
    return false;
  }
  auto t = ctx.trace_id().Id();
  auto s = ctx.span_id().Id();
  std::copy(t.begin(), t.end(), trace_id->begin());
  std::copy(s.begin(), s.end(), span_id->begin());
  return true;
}

inline void encode(const jspan_context& span_ctx, bufferlist& bl, uint64_t f = 0) {
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

inline void decode(jspan_context& span_ctx, bufferlist::const_iterator& bl) {
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

using jspan_attribute = Value;

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

using jspan_context = opentelemetry::v1::trace::SpanContext;

class jspan {
  jspan_context _ctx;
public:
  template <typename T>
  void SetAttribute(std::string_view key, const T& value) const noexcept {}
  void AddEvent(std::string_view) {}
  void AddEvent(std::string_view, std::initializer_list<std::pair<std::string_view, jspan_attribute>> fields) {}
  template <typename T> void AddEvent(std::string_view name, const T& fields = {}) {}
  jspan_context GetContext() const { return _ctx; }
  void UpdateName(std::string_view) {}
  bool IsRecording() { return false; }
};

class jspan_ptr {
  jspan span;
public:
  jspan& operator*() { return span; }
  const jspan& operator*() const { return span; }
  jspan* operator->() { return &span; }
  const jspan* operator->() const { return &span; }
  operator bool() const { return false; }
  jspan* get() { return &span; }
  const jspan* get() const { return &span; }
};

namespace tracing {

struct Tracer {
  void init(CephContext* _cct, std::string_view service_name) {}
  void reconfigure() {}
  bool is_enabled() const { return false; }
  jspan_ptr start_trace(std::string_view, bool enabled = true) { return {}; }
  jspan_ptr add_span(std::string_view, const jspan_ptr&) { return {}; }
  jspan_ptr add_span(std::string_view span_name, const jspan_context& parent_ctx) { return {}; }
  jspan_ptr context_span() { return {}; }
  std::string record_op(const OpTimeline&) { return {}; }
};

inline bool context_ids(const jspan_context&, trace_id_t*, span_id_t*) {
  return false;
}

inline void encode(const jspan_context& span_ctx, bufferlist& bl, uint64_t f = 0) {
  ENCODE_START(1, 1, bl);
  // jaeger is missing, set "is_valid" to false.
  bool is_valid = false;
  encode(is_valid, bl);
  ENCODE_FINISH(bl);
}

inline void decode(jspan_context& span_ctx, bufferlist::const_iterator& bl) {
  DECODE_START(254, bl);
  // jaeger is missing, consume the buffer but do not decode it.
  DECODE_FINISH(bl);
}

} // namespace tracing

#endif // !HAVE_JAEGER
