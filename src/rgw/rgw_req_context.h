// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once
#include "common/async/yield_context.h"

class DoutPrefixProvider;

// this struct holds information which is created at the frontend
// and should trickle down through all function calls to the backend
struct req_context {
  const DoutPrefixProvider* dpp{nullptr};
  optional_yield y;
  const jspan* span;
};

