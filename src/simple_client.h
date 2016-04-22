// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once

#include "simple_recs.h"


namespace crimson {
  namespace simple_scheduler {

    // S is server identifier type
    template<typename S>
    class ServiceTracker {

    public:

      // we have to start the counters at 1, as 0 is used in the
      // cleaning process
      ServiceTracker()
      {
	// emptry
      }


      void track_resp(const S& server_id, const NullData& ignore) {
	// empty
      }


      /*
       * Returns the ReqParams for the given server.
       */
      ReqParams get_req_params(const S& server) {
	return ReqParams();
      } // get_req_params
    }; // class ServiceTracker
  } // namespace simple_scheduler
} // namespace crimson
