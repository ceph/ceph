// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/ceph_context.h"
#include "tracer.h"
#include "common/debug.h"

#include <algorithm>

namespace tracing {

// splitmix64's finalizer: a fixed, well-mixing hash of 64 bits, the same on
// every OSD whatever its build, which std::hash does not promise
static uint64_t mix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ull;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
  return x ^ (x >> 31);
}

RequestTrace request_trace(uint64_t cluster, uint8_t name_type, int64_t name_num,
                           int32_t inc, uint64_t tid) {
  auto hash = [&](uint64_t seed) {
    uint64_t h = mix64(seed ^ cluster);
    h = mix64(h ^ name_type);
    h = mix64(h ^ static_cast<uint64_t>(name_num));
    h = mix64(h ^ static_cast<uint32_t>(inc));
    return mix64(h ^ tid);
  };
  const uint64_t words[] = {hash(1), hash(2), hash(3), hash(4)};
  RequestTrace r;
  auto put = [](uint8_t* out, uint64_t v) {  // big-endian, as the ids print
    for (int i = 7; i >= 0; --i, v >>= 8) {
      out[i] = v & 0xff;
    }
  };
  put(r.trace_id.data(), words[0]);
  put(r.trace_id.data() + 8, words[1]);
  put(r.root_span_id.data(), words[2]);
  put(r.primary_span_id.data(), words[3]);
  // all-zero ids are invalid; vanishingly unlikely, but cheap to rule out
  if (words[0] == 0 && words[1] == 0) {
    r.trace_id[15] = 1;
  }
  if (words[2] == 0) {
    r.root_span_id[7] = 1;
  }
  if (words[3] == 0) {
    r.primary_span_id[7] = 1;
  }
  return r;
}

std::vector<OpPhase> op_phases(const OpTimeline& t, double min_share) {
  std::vector<OpPhase> phases;
  const double total = t.end - t.start;
  auto add = [&](utime_t start, utime_t end, std::string name) {
    const double length = end - start;
    if (length > 0 && length >= total * min_share) {
      phases.push_back({std::move(name), start, end});
    }
  };
  const std::pair<utime_t, std::string>* prev = nullptr;
  for (const auto& event : t.events) {
    if (!t.in_lifetime(event.first)) {
      continue;
    }
    if (prev) {
      add(prev->first, event.first, prev->second + " -> " + event.second);
    }
    prev = &event;
  }
  if (prev && !t.complete) {
    add(prev->first, t.end, prev->second + " -> (in flight)");
  }
  return phases;
}

// what the op is doing from `event` until the next one, or "" if unknown
static std::string_view phase_after(std::string_view event) {
  static constexpr std::pair<std::string_view, std::string_view> names[] = {
    {"initiated", "receive"}, {"header_read", "receive"},
    {"throttled", "receive"}, {"all_read", "receive"},
    {"dispatched", "dispatch"},
    {"queued_for_pg", "queued for PG"},
    {"reached_pg", "in PG"},
    {"started", "execute"}, {"sub_op_started", "execute"},
    {"commit_sent", "finish"},
  };
  for (const auto& [e, name] : names) {
    if (event == e) {
      return name;
    }
  }
  // the reasons an op is delayed, e.g. "waiting for rw locks"
  if (event.starts_with("waiting for ") && !event.starts_with("waiting for subops")) {
    return event;
  }
  return {};
}

std::vector<OpPhase> named_phases(const OpTimeline& t, double min_share) {
  static constexpr std::string_view subops_sent = "waiting for subops from ";
  static constexpr std::string_view replica_acked = "sub_op_commit_rec from ";
  static constexpr std::string_view local_commit = "op_commit";

  std::vector<const std::pair<utime_t, std::string>*> events;
  for (const auto& event : t.events) {
    if (t.in_lifetime(event.first)) {
      events.push_back(&event);
    }
  }
  std::vector<OpPhase> phases;
  const double total = t.end - t.start;
  auto add = [&](utime_t start, utime_t end, std::string name) {
    const double length = end - start;
    if (length > 0 && length >= total * min_share) {
      phases.push_back({std::move(name), start, end});
    }
  };

  // replication: from sending the sub-ops until the last acknowledgement,
  // one parallel phase per replica and one for the local commit
  std::optional<utime_t> sent, all_done;
  for (auto e : events) {
    if (!sent && e->second.starts_with(subops_sent)) {
      sent = e->first;
    } else if (sent && (e->second.starts_with(replica_acked) || e->second == local_commit)) {
      all_done = e->first;
      add(*sent, e->first, e->second == local_commit
          ? std::string("local commit")
          : "replica " + e->second.substr(replica_acked.size()));
    }
  }

  // everything else: consecutive events, merging neighbours of the same name
  std::string open_name;
  utime_t open_start;
  auto close = [&](utime_t end) {
    if (!open_name.empty()) {
      add(open_start, end, std::move(open_name));
      open_name.clear();
    }
  };
  for (size_t i = 0; i + 1 < events.size(); ++i) {
    const auto& [stamp, event] = *events[i];
    const utime_t next = events[i + 1]->first;
    if (sent && all_done && stamp >= *sent && next <= *all_done) {
      close(stamp);  // the replication phases cover this stretch
      continue;
    }
    std::string name;
    if (all_done && stamp == *all_done) {
      name = "reply";  // from the last acknowledgement until the reply
    } else if (auto known = phase_after(event); !known.empty()) {
      name = known;
    } else {
      name = event + " -> " + events[i + 1]->second;
    }
    if (name != open_name) {
      close(stamp);
      open_name = std::move(name);
      open_start = stamp;
    }
  }
  if (!events.empty()) {
    close(events.back()->first);
    if (!t.complete) {
      add(events.back()->first, t.end, events.back()->second + " -> (in flight)");
    }
  }
  std::sort(phases.begin(), phases.end(),
            [](const OpPhase& a, const OpPhase& b) { return a.start < b.start; });
  return phases;
}

} // namespace tracing

#ifdef HAVE_JAEGER
#include <map>
#include <optional>
#include <cstring>
#include <random>

#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/random_id_generator.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/trace/default_span.h"
#include "opentelemetry/exporters/jaeger/jaeger_exporter.h"
#ifdef HAVE_OTLP
#include "opentelemetry/exporters/otlp/otlp_http_exporter.h"
#endif

#define dout_subsys ceph_subsys_trace
#undef dout_prefix
#define dout_prefix (*_dout << "otel_tracing: ")

namespace tracing {

const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> Tracer::noop_tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("no-op", OPENTELEMETRY_SDK_VERSION);
const jspan_ptr Tracer::noop_span = noop_tracer->StartSpan("noop");

namespace {

namespace otel_trace = opentelemetry::trace;
namespace nostd = opentelemetry::nostd;

// Random ids, unless the thread preset the ids of the next span: spans that
// record_op() exports after the fact must have the ids that other spans, or
// other daemons, already refer to. The SDK takes the span id first, then a
// trace id for a span without a parent.
class PresetIdGenerator : public opentelemetry::sdk::trace::IdGenerator {
  opentelemetry::sdk::trace::RandomIdGenerator random;
 public:
  static thread_local std::optional<span_id_t> next_span_id;
  static thread_local std::optional<trace_id_t> next_trace_id;

  otel_trace::SpanId GenerateSpanId() noexcept override {
    if (next_span_id) {
      otel_trace::SpanId id(nostd::span<const uint8_t, SpanIdkSize>(next_span_id->data(), SpanIdkSize));
      next_span_id.reset();
      return id;
    }
    return random.GenerateSpanId();
  }
  otel_trace::TraceId GenerateTraceId() noexcept override {
    if (next_trace_id) {
      otel_trace::TraceId id(nostd::span<const uint8_t, TraceIdkSize>(next_trace_id->data(), TraceIdkSize));
      next_trace_id.reset();
      return id;
    }
    return random.GenerateTraceId();
  }
};
thread_local std::optional<span_id_t> PresetIdGenerator::next_span_id;
thread_local std::optional<trace_id_t> PresetIdGenerator::next_trace_id;

} // anonymous namespace

using bufferlist = ceph::buffer::list;

// the exporter selected by trace_exporter. The exporters are created with new
// and owned through SpanExporter: make_unique<JaegerExporter> would instantiate
// JaegerExporter's implicit destructor here, where the ThriftSender it owns is
// an incomplete type
static std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> make_exporter(CephContext* cct) {
  using exporter_ptr = std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>;
  if (cct->_conf.get_val<std::string>("trace_exporter") == "otlp") {
#ifdef HAVE_OTLP
    opentelemetry::exporter::otlp::OtlpHttpExporterOptions options;
    options.url = cct->_conf.get_val<std::string>("trace_otlp_endpoint");
    options.content_type = opentelemetry::exporter::otlp::HttpRequestContentType::kBinary;
    ldout(cct, 1) << "exporting spans over OTLP/HTTP to " << options.url << dendl;
    return exporter_ptr(new opentelemetry::exporter::otlp::OtlpHttpExporter(options));
#else
    lderr(cct) << "trace_exporter is otlp, but this build has no OTLP support;"
               << " exporting over Jaeger UDP instead" << dendl;
#endif
  }
  opentelemetry::exporter::jaeger::JaegerExporterOptions options;
  options.endpoint = cct->_conf.get_val<std::string>("jaeger_agent_host");
  options.server_port = cct->_conf.get_val<int64_t>("jaeger_agent_port");
  ldout(cct, 1) << "exporting spans to " << options.endpoint << ":"
                << options.server_port << dendl;
  return exporter_ptr(new opentelemetry::exporter::jaeger::JaegerExporter(options));
}

Tracer::tracer_ptr Tracer::make_tracer() {
  const opentelemetry::sdk::trace::BatchSpanProcessorOptions processor_options;
  const auto jaeger_resource = opentelemetry::sdk::resource::Resource::Create(std::move(opentelemetry::sdk::resource::ResourceAttributes{{"service.name", service_name}}));
  auto processor = std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>(new opentelemetry::sdk::trace::BatchSpanProcessor(make_exporter(cct), processor_options));
  const auto provider = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(
    new opentelemetry::sdk::trace::TracerProvider(
      std::move(processor), jaeger_resource,
      std::unique_ptr<opentelemetry::sdk::trace::Sampler>(new opentelemetry::sdk::trace::AlwaysOnSampler),
      std::unique_ptr<opentelemetry::sdk::trace::IdGenerator>(new PresetIdGenerator)));
  opentelemetry::trace::Provider::SetTracerProvider(provider);
  return provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);
}

void Tracer::init(CephContext* _cct, opentelemetry::nostd::string_view _service_name) {
  ceph_assert(_cct);
  cct = _cct;
  std::unique_lock l(tracer_lock);
  if (!tracer) {
    ldout(cct, 3) << "tracer was not loaded, initializing tracing" << dendl;
    service_name = std::string(_service_name);
    tracer = make_tracer();
  }
}

void Tracer::reconfigure() {
  if (!cct || !get_tracer()) {
    return;  // not initialized; init() will read the new settings
  }
  // the previous provider flushes its pending spans when its last span ends
  auto t = make_tracer();
  std::unique_lock l(tracer_lock);
  tracer = std::move(t);
}

jspan_ptr Tracer::start_trace(opentelemetry::nostd::string_view trace_name) {
  ceph_assert(cct);
  if (is_enabled()) {
    auto t = get_tracer();
    ceph_assert(t);
    ldout(cct, 20) << "start trace for " << trace_name << " " << dendl;
    return t->StartSpan(trace_name);
  }
  return noop_span;
}

jspan_ptr Tracer::start_trace(opentelemetry::nostd::string_view trace_name, bool trace_is_enabled) {
  ceph_assert(cct);
  ldout(cct, 20) << "start trace enabled " << trace_is_enabled << " " << dendl;
  if (trace_is_enabled) {
    auto t = get_tracer();
    ceph_assert(t);
    ldout(cct, 20) << "start trace for " << trace_name << " " << dendl;
    return t->StartSpan(trace_name);
  }
  return noop_tracer->StartSpan(trace_name);
}

jspan_ptr Tracer::add_span(opentelemetry::nostd::string_view span_name, const jspan_ptr& parent_span) {
  if (is_enabled() && parent_span && parent_span->IsRecording()) {
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_span->GetContext();
    ldout(cct, 20) << "adding span " << span_name << " " << dendl;
    return get_tracer()->StartSpan(span_name, span_opts);
  }
  // keep the context that a non-recording parent may carry
  return parent_span ? parent_span : noop_span;
}

jspan_ptr Tracer::add_span(opentelemetry::nostd::string_view span_name, const jspan_context& parent_ctx) {
  if (is_enabled() && parent_ctx.IsValid() && parent_ctx.IsSampled()) {
    auto t = get_tracer();
    ceph_assert(t);
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_ctx;
    ldout(cct, 20) << "adding span " << span_name << " " << dendl;
    return t->StartSpan(span_name, span_opts);
  }
  return noop_span;
}

bool Tracer::is_enabled() const {
  return cct->_conf->jaeger_tracing_enable;
}

// phases shorter than this share of the op are kept as span events only
static constexpr double min_phase_share = 0.01;

jspan_ptr Tracer::context_span() {
  // not the SDK's generator: these ids need not be secret, only unique, and
  // this runs for every request
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  trace_id_t trace_id;
  span_id_t span_id;
  uint64_t words[3];
  do {
    for (auto& w : words) {
      w = rng();
    }
    std::memcpy(trace_id.data(), words, trace_id.size());
    std::memcpy(span_id.data(), words + 2, span_id.size());
  } while ((words[0] == 0 && words[1] == 0) || words[2] == 0);
  otel_trace::SpanContext ctx(
    otel_trace::TraceId(nostd::span<const uint8_t, TraceIdkSize>(trace_id.data(), TraceIdkSize)),
    otel_trace::SpanId(nostd::span<const uint8_t, SpanIdkSize>(span_id.data(), SpanIdkSize)),
    otel_trace::TraceFlags(0),  // not sampled: receivers create no live spans
    false);
  return jspan_ptr(new otel_trace::DefaultSpan(ctx));
}

std::string Tracer::record_op(const OpTimeline& t) {
  auto tracer = get_tracer();
  if (!tracer) {
    return {};
  }
  namespace otel_common = opentelemetry::common;
  namespace otel_trace = opentelemetry::trace;
  // the timeline is in wall-clock time; the SDK measures durations with the
  // steady clock, so map each stamp onto it relative to now
  const auto sys_now = std::chrono::system_clock::now();
  const auto steady_now = std::chrono::steady_clock::now();
  auto sys_time = [](utime_t u) {
    return std::chrono::system_clock::time_point(
      std::chrono::duration_cast<std::chrono::system_clock::duration>(
        std::chrono::nanoseconds(u.to_nsec())));
  };
  auto steady_time = [&](utime_t u) {
    return otel_common::SteadyTimestamp(steady_now - (sys_now - sys_time(u)));
  };
  auto start_opts = [&](utime_t u) {
    otel_trace::StartSpanOptions opts;
    opts.start_system_time = otel_common::SystemTimestamp(sys_time(u));
    opts.start_steady_time = steady_time(u);
    return opts;
  };
  auto end_opts = [&](utime_t u) {
    otel_trace::EndSpanOptions opts;
    opts.end_steady_time = steady_time(u);
    return opts;
  };

  auto opts = start_opts(t.start);
  if (t.trace_id && t.parent_span_id) {
    // a remote parent known only by its ids; it may never be exported
    opts.parent = otel_trace::SpanContext(
      otel_trace::TraceId(nostd::span<const uint8_t, TraceIdkSize>(
        t.trace_id->data(), TraceIdkSize)),
      otel_trace::SpanId(nostd::span<const uint8_t, SpanIdkSize>(
        t.parent_span_id->data(), SpanIdkSize)),
      otel_trace::TraceFlags(otel_trace::TraceFlags::kIsSampled),
      true);
  } else {
    PresetIdGenerator::next_trace_id = t.trace_id;
  }
  PresetIdGenerator::next_span_id = t.span_id;
  auto span = tracer->StartSpan(t.name, opts);
  // the presets are for the op span only
  PresetIdGenerator::next_trace_id.reset();
  PresetIdGenerator::next_span_id.reset();
  for (const auto& [key, value] : t.attributes) {
    span->SetAttribute(key, opentelemetry::nostd::string_view(value));
  }
  for (const auto& [key, value] : t.int_attributes) {
    span->SetAttribute(key, value);
  }
  const double total = t.end - t.start;
  span->SetAttribute("duration_s", total);
  if (!t.complete) {
    span->SetAttribute("in_flight", true);
  }
  for (const auto& [stamp, name] : t.events) {
    if (t.in_lifetime(stamp)) {
      span->AddEvent(name, otel_common::SystemTimestamp(sys_time(stamp)));
    }
  }
  // e.g. "queued for PG", "replica osd.2"
  for (const auto& phase : named_phases(t, min_phase_share)) {
    auto phase_opts = start_opts(phase.start);
    phase_opts.parent = span->GetContext();
    tracer->StartSpan(phase.name, phase_opts)->End(end_opts(phase.end));
  }
  span->End(end_opts(t.end));

  char trace_id[2 * TraceIdkSize];
  span->GetContext().trace_id().ToLowerBase16(trace_id);
  ldout(cct, 10) << "recorded op trace " << std::string_view(trace_id, sizeof(trace_id))
                 << " for " << t.name << " lasting " << total << "s" << dendl;
  return std::string(trace_id, sizeof(trace_id));
}

} // namespace tracing

#endif // HAVE_JAEGER

