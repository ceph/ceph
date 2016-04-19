// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "simple_server.h"
#include "simple_client.h"

#include "test_recs.h"
#include "test_server.h"
#include "test_client.h"

#include "simulate.h"


namespace test_simple {

  namespace simp = crimson::simple_scheduler;

  using Time = double;

  struct ClientInfo {
  };

  struct SimpleAccum {
  };

  using SimpleQueue = simp::SimpleQueue<ClientId,TestRequest,Time>;

  using SimpleServer = TestServer<SimpleQueue,
				  ClientInfo,
				  simp::ReqParams<ClientId>,
				  simp::RespParams<ServerId>,
				  simp::NullData,
				  SimpleAccum>;
  using SimpleClient = TestClient<simp::ServiceTracker<ServerId>,
				  simp::ReqParams<ClientId>,
				  simp::RespParams<ServerId>,
				  SimpleAccum>;

  using CreateQueueF =
    std::function<SimpleQueue*(SimpleQueue::CanHandleRequestFunc,
			       SimpleQueue::HandleRequestFunc)>;


  using MySim = Simulation<ServerId,ClientId,SimpleServer,SimpleClient>;
  
  using SubmitFunc = SimpleClient::SubmitFunc;

  extern void simple_server_accumulate_f(SimpleAccum& a,
					 const simp::NullData& add_info);

  extern void simple_client_accumulate_f(SimpleAccum& a,
					 const simp::RespParams<ServerId>& r);
}; // namespace test_simple
