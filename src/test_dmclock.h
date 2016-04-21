// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "dmclock_recs.h"
#include "dmclock_server.h"
#include "dmclock_client.h"

#include "sim_recs.h"
#include "sim_server.h"
#include "sim_client.h"

#include "simulate.h"


namespace crimson {
  namespace test_dmc {
    
    namespace dmc = crimson::dmclock;
    namespace sim = crimson::qos_simulation;

    struct DmcAccum {
      uint64_t reservation_count = 0;
      uint64_t proportion_count = 0;
    };

    using DmcQueue = dmc::PriorityQueue<ClientId,sim::TestRequest>;

    using DmcServer = sim::SimulatedServer<DmcQueue,
					   dmc::ReqParams<ClientId>,
					   dmc::PhaseType,
					   DmcAccum>;

    using DmcClient = sim::SimulatedClient<dmc::ServiceTracker<ServerId>,
					   dmc::ReqParams<ClientId>,
					   dmc::PhaseType,
					   DmcAccum>;

    using CreateQueueF = std::function<DmcQueue*(DmcQueue::CanHandleRequestFunc,
						 DmcQueue::HandleRequestFunc)>;

    using MySim = sim::Simulation<ServerId,ClientId,DmcServer,DmcClient>;

    using SubmitFunc = DmcClient::SubmitFunc;

    extern void dmc_server_accumulate_f(DmcAccum& a,
					const dmc::PhaseType& phase);

    extern void dmc_client_accumulate_f(DmcAccum& a,
					const dmc::PhaseType& phase);
  } // namespace test_dmc
} // namespace crimson
