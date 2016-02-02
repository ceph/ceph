// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once

#include <map>
#include <chrono>

#include "dmclock_util.h"
#include "dmclock_recs.h"


namespace crimson {
  namespace dmclock {
    struct ServerInfo {
      Counter delta_prev_req;
      Counter rho_prev_req;

#if 0
      // track last update to allow clean-up
      decltype(std::chrono::steady_clock::now()) last_update;
#endif
    };

    // S is server identifier type
    template<typename S>
    class ServiceTracker {
      Counter                delta_counter; // # reqs completed
      Counter                rho_counter;   // # reqs completed via reservation
      std::map<S,ServerInfo> service_map;
      mutable std::mutex     data_mtx;      // protects Counters and map

      using DataGuard = std::lock_guard<decltype(data_mtx)>;

    public:

      ServiceTracker() :
	delta_counter(0),
	rho_counter(0)
      {
	// empty
      }

      void trackResponse(const RespParams<S>& resp_params) {
	DataGuard g(data_mtx);
	++delta_counter;
	if (PhaseType::reservation == resp_params.phase) {
	  ++rho_counter;
	}
      }

      template<typename C>
      ReqParams<C> getRequestParams(const C& client, const S& server) {
	DataGuard g(data_mtx);
	auto it = service_map.find(server);
	if (service_map.end() == it) {
	  service_map[server] = ServerInfo{delta_counter, rho_counter};
	  return ReqParams<C>(client, 1, 1);
	} else {
	  ReqParams<C> result(
	    client,
	    uint32_t(1 + delta_counter - it->second.delta_prev_req),
	    uint32_t(1 + rho_counter - it->second.rho_prev_req));

	  it->second.delta_prev_req = delta_counter;
	  it->second.rho_prev_req = rho_counter;

	  return result;
	}
      }
    }; // class ServiceTracker
  }
}
