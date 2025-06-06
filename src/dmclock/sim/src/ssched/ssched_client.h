// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#pragma once

#include "ssched_recs.h"


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
	// empty
      }

      void track_resp(const S& server_id,
		      const NullData& ignore,
		      uint64_t request_cost) {
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
