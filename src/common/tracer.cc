// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tracer.h"
#include <arpa/inet.h>
#include <yaml-cpp/yaml.h>
#ifdef __linux__
#include <linux/types.h>
#else
typedef int64_t __s64;
#endif

#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "jaeger_tracing "

namespace jaeger_tracing {

  std::shared_ptr<opentracing::v3::Tracer> tracer = nullptr;

  void init_tracer(const char* tracer_name) {
    if (!tracer) {
	YAML::Node yaml;
	try{
	  yaml = YAML::LoadFile("../src/jaegertracing/config.yml");
	dout(3) << "yaml loaded" << yaml << dendl;
	}
	catch(std::exception &e) {
	  dout(3) << "failed to load yaml file using default config" << dendl;
	  auto yaml_config = R"cfg(
disabled: false
reporter:
    logSpans: false
    queueSize: 100
    bufferFlushInterval: 10
sampler:
    type: const
    param: 1
headers:
    jaegerDebugHeader: debug-id
    jaegerBaggageHeader: baggage
    TraceContextHeaderName: trace-id
baggage_restrictions:
    denyBaggageOnInitializationFailure: false
    refreshInterval: 60
)cfg";
	yaml = YAML::Load(yaml_config);
	dout(3) << "yaml loaded" << yaml << dendl;
      }
      static auto configuration = jaegertracing::Config::parse(yaml);
      tracer = jaegertracing::Tracer::make( tracer_name, configuration,
	    jaegertracing::logging::consoleLogger());
      dout(3) << "tracer_jaeger init successful" << dendl;
    }
    //incase of stale tracer, configure with a new global tracer
    if (opentracing::Tracer::Global() != tracer) {
      opentracing::Tracer::InitGlobal(
	  std::static_pointer_cast<opentracing::Tracer>(tracer));
    }
  }

  jspan new_span(const char* span_name) {
    return opentracing::Tracer::Global()->StartSpan(span_name);
 }

  jspan child_span(const char* span_name, const jspan& parent_span) {
    //no parent check if parent not found span will still be constructed
    return opentracing::Tracer::Global()->StartSpan(span_name,
	{opentracing::ChildOf(&parent_span->context())});
 }

  void finish_span(const jspan& span) {
    if (span) {
      span->Finish();
    }
  }

  void set_span_tag(const jspan& span, const char* key, const char* value) {
    if (span) {
      span->SetTag(key, value);
    }
  }
}
