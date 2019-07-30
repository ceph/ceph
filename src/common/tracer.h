#ifndef TRACER_H_
#define TRACER_H_

#include <iostream>

#include <yaml-cpp/yaml.h>

#include <jaegertracing/Tracer.h>

typedef std::unique_ptr<opentracing::Span> jspan;

class JTracer {

public:
static void setUpTracer(const char* serviceToTrace) {
  static auto configYAML = YAML::LoadFile("../jaegertracing/config.yml");
  static auto config = jaegertracing::Config::parse(configYAML);
  static auto tracer = jaegertracing::Tracer::make(
      serviceToTrace, config, jaegertracing::logging::consoleLogger());
  opentracing::Tracer::InitGlobal(
      std::static_pointer_cast<opentracing::Tracer>(tracer));
}

static jspan tracedSubroutine(
    jspan& parentSpan,
    const char* subRoutineContext) {
  auto span = opentracing::Tracer::Global()->StartSpan(
      subRoutineContext, {opentracing::ChildOf(&parentSpan->context())});
  span->Finish();
  return span;
}

static jspan tracedFunction(const char* funcContext) {
  auto span = opentracing::Tracer::Global()->StartSpan(funcContext);
  span->Finish();
  return span;
}

static std::string inject(jspan& span, const char* name) {
  std::stringstream ss;
  if (!span) {
    auto span = opentracing::Tracer::Global()->StartSpan(name);
  }
  auto err = opentracing::Tracer::Global()->Inject(span->context(), ss);
  assert(err);
  return ss.str();
}

static jspan extract(const char* name,
	     std::string t_meta) {
  std::stringstream ss(t_meta);
  //    if(!tracer){
  //    }
  // setUpTracer("Extract-service");
  auto span_context_maybe = opentracing::Tracer::Global()->Extract(ss);
  assert(span_context_maybe);

  // Propogation span
  auto span = opentracing::Tracer::Global()->StartSpan(
      "propagationSpan", {ChildOf(span_context_maybe->get())});
  span->Finish();

  return span;
}

};

#endif

