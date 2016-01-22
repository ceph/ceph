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

      // S is server id type
      template<typename S>
      struct RespParams {
          S         server;
          PhaseType phase;

          RespParams(const S& _server, const PhaseType& _phase) :
              server(_server),
              phase(_phase)
          {
              // empty
          }

          RespParams(const RespParams& other) :
              server(other.server),
              phase(other.phase)
          {
              // empty
          }
#if 0
          RespParams& operator=(const RespParams& other)
          {
              server = other.server;
              phase = other.phase;
              return *this;
          }
#endif
      };
  }
}
