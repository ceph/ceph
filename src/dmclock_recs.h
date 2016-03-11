// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <ostream>
#include <assert.h>


namespace crimson {
  namespace dmclock {
    using Counter = uint64_t;

    enum class PhaseType { reservation, priority };

    std::ostream& operator<<(std::ostream& out, PhaseType phase);

    template<typename C>
    struct ReqParams {
      C        client;

      // count of all replies since last request; MUSTN'T BE 0
      uint32_t delta;

      // count of reservation replies since last request; MUSTN'T BE 0
      uint32_t rho;

      ReqParams(const C& _client, uint32_t _delta, uint32_t _rho) :
	client(_client),
	delta(_delta),
	rho(_rho)
      {
	assert(0 != delta && 0 != rho && rho <= delta);
      }

      ReqParams(const ReqParams& other) :
	client(other.client),
	delta(other.delta),
	rho(other.rho)
      {
	// empty
      }

      friend std::ostream& operator<<(std::ostream& out, const ReqParams& rp) {
	out << "ReqParams{ client:" << rp.client << ", delta:" << rp.delta <<
	  ", rho:" << rp.rho << " }";
	return out;
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

      friend std::ostream& operator<<(std::ostream& out, const RespParams& rp) {
	out << "RespParams{ server:" << rp.server <<
	  ", phase:" << rp.phase << " }";
	return out;
      }
    };
  }
}
