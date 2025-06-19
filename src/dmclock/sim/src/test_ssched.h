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


#include "ssched_server.h"
#include "ssched_client.h"

#include "sim_recs.h"
#include "sim_server.h"
#include "sim_client.h"

#include "simulate.h"


namespace crimson {
  namespace test_simple_scheduler {

    namespace ssched = crimson::simple_scheduler;
    namespace sim = crimson::qos_simulation;

    using Time = double;

    struct SimpleAccum {
      uint32_t request_count = 0;
    };

    using SimpleQueue = ssched::SimpleQueue<ClientId,sim::TestRequest,Time>;

    using SimpleServer = sim::SimulatedServer<SimpleQueue,
					      ssched::ReqParams,
					      ssched::NullData,
					      SimpleAccum>;
    using SimpleClient = sim::SimulatedClient<ssched::ServiceTracker<ServerId>,
					      ssched::ReqParams,
					      ssched::NullData,
					      SimpleAccum>;

    using CreateQueueF =
      std::function<SimpleQueue*(SimpleQueue::CanHandleRequestFunc,
				 SimpleQueue::HandleRequestFunc)>;


    using MySim = sim::Simulation<ServerId,ClientId,SimpleServer,SimpleClient>;
  
    using SubmitFunc = SimpleClient::SubmitFunc;

    extern void simple_server_accumulate_f(SimpleAccum& a,
					   const ssched::NullData& add_info);

    extern void simple_client_accumulate_f(SimpleAccum& a,
					   const ssched::NullData& ignore);
  } // namespace test_simple
} // namespace crimson
