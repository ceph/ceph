// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "simple_server.h"

#include "test_recs.h"
#include "test_server.h"
#include "test_client.h"

#include "simulate.h"


namespace test_simple {

  namespace simp = crimson::simple_scheduler;

  using Time = double;

  struct ClientInfo {
  };

  struct SimpleAddInfo {
  };

  struct SimpleAccum {
  };

  using SimpleServer = TestServer<simp::SimpleQueue<ClientId,TestRequest,Time>,
				  ClientInfo,
				  dmc::ReqParams<ClientId>,
				  dmc::RespParams<ServerId>,
				  SimpleAddInfo,
				  SimpleAccum>;
#if 0
    using DmcClient = TestClient<dmc::ServiceTracker<ServerId>,
                                 dmc::ReqParams<ClientId>,
                                 dmc::RespParams<ServerId>,
                                 DmcAccum>;

    using MySim = Simulation<ServerId,ClientId,DmcServer,DmcClient>;

    using SubmitFunc = DmcClient::SubmitFunc;

#endif

  extern void simple_server_accumulate_f(SimpleAccum& a,
					 const SimpleServerAddInfo& add_info);

  extern void simple_client_accumulate_f(SimpleAccum& a,
					 const dmc::RespParams<ServerId>& r);
}; // namespace test_simple
