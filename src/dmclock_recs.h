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

    // TODO rename to ReqParams
    template<typename C>
    struct ReqParams {
      C        client;
      uint32_t delta; // count of all replies since last request
      uint32_t rho;   // count of reservation replies since last request

      ReqParams(const C& _client, uint32_t _delta, uint32_t _rho) :
	client(_client),
	delta(_delta),
	rho(_rho)
      {
	// empty
      }

      ReqParams(const ReqParams& other) :
	client(other.client),
	delta(other.delta),
	rho(other.rho)
      {
	// empty
      }
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
    };
  }
}
