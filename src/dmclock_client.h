// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once

#include <map>

#include "dmclock_util.h"
#include "dmclock_recs.h"


namespace crimson {
  namespace dmclock {
    struct ServerInfo {
      Counter delta_last;
      Counter rho_last;
    };

    // S is server identifier type
    template<typename S>
    class ServiceTracker {
      Counter delta_counter; // # requests completed
      Counter rho_counter;   // # requests completed via reservation
      std::map<S,ServerInfo> service_map;
      std::mutex             mtx_data;

    public:

      ServiceTracker() :
	delta_counter(0),
	rho_counter(0)
      {
	// empty
      }

      void trackResponse(const RespParams<S>& resp_params) {
	Guard g(mtx_data);
	++delta_counter;
	if (PhaseType::reservation == resp_params.phase) {
	  ++rho_counter;
	}
	auto it = service_map.find(resp_params.server);
	if (service_map.end() == it) {
	  service_map[resp_params.server] = ServerInfo{delta_counter, rho_counter};
	} else {
	  it->second.delta_last = delta_counter;
	  it->second.rho_last = rho_counter;
	}
      }
    };
  }
}
