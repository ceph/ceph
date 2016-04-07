// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "dmclock_recs.h"
#include "dmclock_server.h"
#include "dmclock_client.h"

#include "test_recs.h"
#include "test_server.h"
#include "test_client.h"

#include "simulate.h"


using namespace std::placeholders;

namespace dmc = crimson::dmclock;

using DmcServerAddInfo = crimson::dmclock::PhaseType;


struct DmcAccum {
  uint64_t reservation_count = 0;
  uint64_t proportion_count = 0;
};



void dmc_server_accumulate_f(DmcAccum& a, const DmcServerAddInfo& add_info) {
  if (dmc::PhaseType::reservation == add_info) {
    ++a.reservation_count;
  } else {
    ++a.proportion_count;
  }
}


void dmc_client_accumulate_f(DmcAccum& a, const dmc::RespParams<ServerId>& r) {
  if (dmc::PhaseType::reservation == r.phase) {
    ++a.reservation_count;
  } else {
    ++a.proportion_count;
  }
}


#if 0 // do last
void client_data(std::ostream& out, clients, int head_w, int data_w) {
  // report how many ops were done by reservation and proportion for
  // each client

  {
    std::cout << std::setw(head_w) << "res_ops:";
    int total = 0;
    for (auto const &c : clients) {
      auto r = c.second->get_accumulator().reservation_count;
      total += r;
      if (!client_disp_filter(c.first)) continue;
      std::cout << std::setw(data_w) << r;
    }
    std::cout << std::setw(data_w) << std::setprecision(data_prec) <<
      std::fixed << total << std::endl;
  }

  {
    std::cout << std::setw(head_w) << "prop_ops:";
    int total = 0;
    for (auto const &c : clients) {
      auto p = c.second->get_accumulator().proportion_count;
      total += p;
      if (!client_disp_filter(c.first)) continue;
      std::cout << std::setw(data_w) << p;
    }
    std::cout << std::setw(data_w) << std::setprecision(data_prec) <<
      std::fixed << total << std::endl;
  }
}


void server_data(std::ostream& out, servers, int head_w, int data_w) {
  {
    std::cout << std::setw(head_w) << "res_ops:";
    int total = 0;
    for (auto const &s : servers) {
      auto rc = s.second->get_accumulator().reservation_count;
      total += rc;
      if (!server_disp_filter(s.first)) continue;
      std::cout << std::setw(data_w) << rc;
    }
    std::cout << std::setw(data_w) << std::setprecision(data_prec) <<
      std::fixed << total << std::endl;
  }

  {
    std::cout << std::setw(head_w) << "prop_ops:";
    int total = 0;
    for (auto const &s : servers) {
      auto pc = s.second->get_accumulator().proportion_count;
      total += pc;
      if (!server_disp_filter(s.first)) continue;
      std::cout << std::setw(data_w) << pc;
    }
    std::cout << std::setw(data_w) << std::setprecision(data_prec) <<
      std::fixed << total << std::endl;
  }
}
#endif // do last

const double client_reservation = 20.0;
const double client_limit = 60.0;
const double client_weight = 1.0;

dmc::ClientInfo client_info =
{ client_weight, client_reservation, client_limit };

// construct servers


dmc::ClientInfo client_info_f(const ClientId& c) {
  return client_info;
}


using DmcServer = TestServer<dmc::PriorityQueue<ClientId,TestRequest>,
			     dmc::ClientInfo,
			     dmc::ReqParams<ClientId>,
			     dmc::RespParams<ServerId>,
			     DmcServerAddInfo,
			     DmcAccum>;

using DmcClient = TestClient<dmc::ServiceTracker<ServerId>,
			     dmc::ReqParams<ClientId>,
			     dmc::RespParams<ServerId>,
			     DmcAccum>;

using SubmitFunc = DmcClient::SubmitFunc;


int main(int argc, char* argv[]) {
  // server params

  const uint server_count = 100;
  const uint server_iops = 40;
  const uint server_threads = 1;
  const bool server_soft_limit = false;

  // client params

  const uint client_total_ops = 1000;
  const uint client_count = 100;
  const uint client_wait_count = 1;
  const uint client_iops_goal = 50;
  const uint client_outstanding_ops = 100;
  const std::chrono::seconds client_wait(10);

  using MySim = Simulation<ServerId,ClientId,DmcServer,DmcClient>;

  MySim *simulation;

  // lambda to post a request to the identified server; called by client
  SubmitFunc server_post_f =
    [&simulation](const ServerId& server,
		  const TestRequest& request,
		  const dmc::ReqParams<ClientId>& req_params) {
    DmcServer& s = simulation->get_server(server);
    s.post(request, req_params);
  };

  static std::vector<CliInst> no_wait =
    { { req_op, client_total_ops, client_iops_goal, client_outstanding_ops } };
  static std::vector<CliInst> wait =
    { { wait_op, client_wait },
      { req_op, client_total_ops, client_iops_goal, client_outstanding_ops } };

#if 0
  SelectFunc 
#if 0
    std::bind(server_alternate_f, _1, i)
#elif 1
    std::bind(server_alt_range_f, _1, i, 8)
#elif 0
    std::bind(server_random_f, _1)
#elif 0
    std::bind(server_ran_range_f, _1, i, 8)
#else
    server_0_f
#endif
    ;
#endif

  simulation = new MySim();

  ClientBasedServerSelectFunc server_select_f =
    simulation->make_server_select_alt_range(8);

  DmcServer::ClientRespFunc client_response_f =
    [&simulation](ClientId client_id,
		  const TestResponse& resp,
		  const dmc::RespParams<ServerId>& resp_params) {
    simulation->get_client(client_id).receive_response(resp, resp_params);
  };

  auto create_server_f = [&](ServerId id) -> DmcServer* {
    return new DmcServer(id,
			 server_iops, server_threads,
			 client_info_f,
			 client_response_f,
			 dmc_server_accumulate_f,
			 server_soft_limit);
  };

  auto create_client_f = [&](ClientId id) -> DmcClient* {
    return new DmcClient(id,
			 server_post_f,
			 std::bind(server_select_f, _1, id),
			 dmc_client_accumulate_f,
			 id < (client_count - client_wait_count) ? no_wait : wait);
  };

  simulation->add_servers(server_count, create_server_f);
  simulation->add_clients(client_count, create_client_f);

  simulation->run();
}
