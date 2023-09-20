// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/async/yield_context.h"
namespace opentelemetry::trace {
  class Span;
}

class DoutPrefixProvider;

// this struct holds information which is created at the frontend
// and should trickle down through all function calls to the backend
struct req_context {
  const DoutPrefixProvider* dpp{nullptr};
  optional_yield y;
  const opentelemetry::trace::Span* span{nullptr};
};

