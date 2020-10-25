//
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
// Demonstrates basic usage of the OpenTracing API. Uses OpenTracing's
// mocktracer to capture all the recorded spans as JSON.

#ifndef TRACER_H_
#define TRACER_H_

#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1

#include <jaegertracing/Tracer.h>

typedef std::unique_ptr<opentracing::Span> jspan;

namespace jaeger_tracing{
//#ifdef HAVE_JAEGER

  extern std::shared_ptr<opentracing::v3::Tracer> tracer;

  void init_tracer(const char* tracer_name);

  //method to create a root jspan
  jspan new_span(const char*);

  //method to create a child_span used given parent_span
  jspan child_span(const char*, const jspan&);

  //method to finish tracing of a single jspan
  void finish_span(const jspan&);

  //setting tags in sundefined reference topans
  void set_span_tag(const jspan&, const char*, const char*);

//  void set_span_log(const jspan&, 
//#else
//  typedef char jspan;
//  int* child_span(...) {return nullptr;}
//  int* new_span(...) {return nullptr;}
//  void finish_span(...) {}
//  void init_tracer(...) {}
//  void set_span_tag(...) {}
//#endif // HAVE_JAEGER
}
#endif // TRACER_H_
