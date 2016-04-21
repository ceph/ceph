// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "simple_server.h"
#include "simple_client.h"

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
					      ssched::ReqParams<ClientId>,
					      ssched::RespParams<ServerId>,
					      ssched::NullData,
					      SimpleAccum>;
    using SimpleClient = sim::SimulatedClient<ssched::ServiceTracker<ServerId>,
					      ssched::ReqParams<ClientId>,
					      ssched::RespParams<ServerId>,
					      SimpleAccum>;

    using CreateQueueF =
      std::function<SimpleQueue*(SimpleQueue::CanHandleRequestFunc,
				 SimpleQueue::HandleRequestFunc)>;


    using MySim = sim::Simulation<ServerId,ClientId,SimpleServer,SimpleClient>;
  
    using SubmitFunc = SimpleClient::SubmitFunc;

    extern void simple_server_accumulate_f(SimpleAccum& a,
					   const ssched::NullData& add_info);

    extern void simple_client_accumulate_f(SimpleAccum& a,
					   const ssched::RespParams<ServerId>& r);
  } // namespace test_simple
} // namespace crimson
