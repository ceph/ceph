// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef TRACER_H_
#define TRACER_H_

#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1

#include <jaegertracing/Tracer.h>

typedef std::unique_ptr<opentracing::Span> jspan;

namespace jaeger_tracing {

  extern std::shared_ptr<opentracing::v3::Tracer> tracer;

  void init_tracer(const char* tracer_name);

  //create a root jspan
  jspan new_span(const char*);

  //create a child_span used given parent_span
  jspan child_span(const char*, const jspan&);

  //finish tracing of a single jspan
  void finish_span(const jspan&);

  //setting tags in sundefined reference topans
  void set_span_tag(const jspan&, const char*, const char*);
}
#endif // TRACER_H_
