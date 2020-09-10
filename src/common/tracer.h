#ifndef TRACER_H_
#define TRACER_H_

#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1

#include <arpa/inet.h>
#include <yaml-cpp/yaml.h>

#include <jaegertracing/Tracer.h>

namespace jaeger_tracing{

  typedef opentracing::Span jspan;

  void init_tracer(const char* tracerName);

  //method to create a root jspan
  std::unique_ptr<jspan> new_span(const char* span_name);

  //method to create a child_span used given parent_span
  std::unique_ptr<jspan> child_span(const char* span_name, const jspan* const parent_span);

  //method to finish tracing of a single jspan
  void finish_span(jspan* jspan);

  //setting tags in spans
  void set_span_tag(jspan* jspan, const char* key, const char* value);
}

#endif
