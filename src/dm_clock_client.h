// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once


#include "dm_clock_recs.h"


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
          std::map<S,ServerInfo> serviceMap;
            
      };
  }
}
