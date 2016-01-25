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
      Counter delta_last;
      Counter rho_last;
      // track last update to allow clean-up
      std::chrono::steady_clock last_update; 
    };

    // S is server identifier type
    template<typename S>
    class ServiceTracker {
      Counter                delta_counter; // # reqs completed
      Counter                rho_counter;   // # reqs completed via reservation
      std::map<S,ServerInfo> service_map;
      mutable std::mutex     data_mtx;      // protects Counters and map

    public:

      ServiceTracker() :
	delta_counter(0),
	rho_counter(0)
      {
	// empty
      }

      void trackResponse(const RespParams<S>& resp_params) {
	Guard g(data_mtx);
	++delta_counter;
	if (PhaseType::reservation == resp_params.phase) {
	  ++rho_counter;
	}
	
	auto now = std::chrono::steady_clock::now();
	auto it = service_map.find(resp_params.server);
	if (service_map.end() == it) {
	  service_map[resp_params.server] =
	    ServerInfo{delta_counter, rho_counter, now};
	} else {
	  it->second.delta_last = delta_counter;
	  it->second.rho_last = rho_counter;
	  it->second.last_update = now;
	}
      }

      template<typename C>
      RequestParams<S> getRequestParams(const C& client, const S& server) const {
	Guard g(data_mtx);
	auto it = service_map.find(server);
	if (service_map.end() == it) {
	  return RequestParams<S>(client, 0, 0);
	} else {
	  return RequestParams<S>(
	    client,
	    uint32_t(delta_counter - it->second.delta_last),
	    uint32_t(rho_counter - it->second.rho_last));
	}
      }
    };
  }
}
