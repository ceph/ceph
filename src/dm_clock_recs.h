// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once


namespace crimson {
  namespace dmclock {
      typedef uint64_t Counter;

      enum class PhaseType {
          reservation, priority
      };

      struct RequestParams {
      };

      struct ReplyParams {
          PhaseType phase;

          ReplyParams(const PhaseType& _phase) :
              phase(_phase)
          {
              // empty
          }

          ReplyParams(const ReplyParams& other) :
              phase(other.phase)
          {
              // empty
          }

          ReplyParams& operator=(const ReplyParams& other)
          {
              phase = other.phase;
              return *this;
          }
      };
  }
}
