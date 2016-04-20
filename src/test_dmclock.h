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


namespace test_dmc {
    namespace dmc = crimson::dmclock;

    using DmcServerAddInfo = crimson::dmclock::PhaseType;

    struct DmcAccum {
        uint64_t reservation_count = 0;
        uint64_t proportion_count = 0;
    };

    using DmcQueue = dmc::PriorityQueue<ClientId,TestRequest>;

    using DmcServer = SimulatedServer<DmcQueue,
				      dmc::ClientInfo,
				      dmc::ReqParams<ClientId>,
				      dmc::RespParams<ServerId>,
				      DmcServerAddInfo,
				      DmcAccum>;

    using DmcClient = SimulatedClient<dmc::ServiceTracker<ServerId>,
				      dmc::ReqParams<ClientId>,
				      dmc::RespParams<ServerId>,
				      DmcAccum>;

    using CreateQueueF = std::function<DmcQueue*(DmcQueue::CanHandleRequestFunc,
                                                 DmcQueue::HandleRequestFunc)>;

    using MySim = Simulation<ServerId,ClientId,DmcServer,DmcClient>;

    using SubmitFunc = DmcClient::SubmitFunc;

    extern void dmc_server_accumulate_f(DmcAccum& a,
                                        const DmcServerAddInfo& add_info);

    extern void dmc_client_accumulate_f(DmcAccum& a,
                                        const dmc::RespParams<ServerId>& r);
}; // namespace test_dmc
