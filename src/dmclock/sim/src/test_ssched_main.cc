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


#include "test_ssched.h"


#ifdef PROFILE
#include "profile.h"
#endif


namespace test = crimson::test_simple_scheduler;
namespace ssched = crimson::simple_scheduler;
namespace sim = crimson::qos_simulation;

using namespace std::placeholders;


namespace crimson {
  namespace test_simple_scheduler {
    void client_data(std::ostream& out,
		     test::MySim* sim,
		     test::MySim::ClientFilter client_disp_filter,
		     int head_w, int data_w, int data_prec);

    void server_data(std::ostream& out,
		     test::MySim* sim,
		     test::MySim::ServerFilter server_disp_filter,
		     int head_w, int data_w, int data_prec);
  } // namespace test_simple
} // namespace crimson


using Cost = uint32_t;
    

int main(int argc, char* argv[]) {
  // server params

  const unsigned server_count = 100;
  const unsigned server_iops = 40;
  const unsigned server_threads = 1;

  // client params

  const unsigned client_total_ops = 1000;
  const unsigned client_count = 100;
  const unsigned client_server_select_range = 10;
  const unsigned client_wait_count = 1;
  const unsigned client_iops_goal = 50;
  const unsigned client_outstanding_ops = 100;
  const std::chrono::seconds client_wait(10);

  auto client_disp_filter = [=] (const ClientId& i) -> bool {
    return i < 3 || i >= (client_count - 3);
  };

  auto server_disp_filter = [=] (const ServerId& i) -> bool {
    return i < 3 || i >= (server_count - 3);
  };


  test::MySim *simulation;

  // lambda to post a request to the identified server; called by client
  test::SubmitFunc server_post_f =
    [&simulation](const ServerId& server_id,
		  sim::TestRequest&& request,
		  const ClientId& client_id,
		  const ssched::ReqParams& req_params) {
    auto& server = simulation->get_server(server_id);
    server.post(std::move(request), client_id, req_params, 1u);
  };

  static std::vector<sim::CliInst> no_wait =
    { { sim::req_op, client_total_ops, client_iops_goal, client_outstanding_ops } };
  static std::vector<sim::CliInst> wait =
    { { sim::wait_op, client_wait },
      { sim::req_op, client_total_ops, client_iops_goal, client_outstanding_ops } };

  simulation = new test::MySim();

#if 1
  test::MySim::ClientBasedServerSelectFunc server_select_f =
    simulation->make_server_select_alt_range(client_server_select_range);
#elif 0
  test::MySim::ClientBasedServerSelectFunc server_select_f =
    std::bind(&test::MySim::server_select_random, simulation, _1, _2);
#else
  test::MySim::ClientBasedServerSelectFunc server_select_f =
    std::bind(&test::MySim::server_select_0, simulation, _1, _2);
#endif

  test::SimpleServer::ClientRespFunc client_response_f =
    [&simulation](ClientId client_id,
		  const sim::TestResponse& resp,
		  const ServerId& server_id,
		  const ssched::NullData& resp_params,
		  Cost request_cost) {
    simulation->get_client(client_id).receive_response(resp,
						       server_id,
						       resp_params,
						       request_cost);
  };

  test::CreateQueueF create_queue_f =
    [&](test::SimpleQueue::CanHandleRequestFunc can_f,
	test::SimpleQueue::HandleRequestFunc handle_f) -> test::SimpleQueue* {
    return new test::SimpleQueue(can_f, handle_f);
  };

  auto create_server_f = [&](ServerId id) -> test::SimpleServer* {
    return new test::SimpleServer(id,
				  server_iops, server_threads,
				  client_response_f,
				  test::simple_server_accumulate_f,
				  create_queue_f);
  };

  auto create_client_f = [&](ClientId id) -> test::SimpleClient* {
    return new test::SimpleClient(id,
				  server_post_f,
				  std::bind(server_select_f, _1, id),
				  test::simple_client_accumulate_f,
				  id < (client_count - client_wait_count)
				  ? no_wait : wait);
  };

  simulation->add_servers(server_count, create_server_f);
  simulation->add_clients(client_count, create_client_f);

  simulation->run();
  simulation->display_stats(std::cout,
			    &test::server_data, &test::client_data,
			    server_disp_filter, client_disp_filter);
} // main


void test::client_data(std::ostream& out,
		       test::MySim* sim,
		       test::MySim::ClientFilter client_disp_filter,
		       int head_w, int data_w, int data_prec) {
  // empty
}


void test::server_data(std::ostream& out,
		       test::MySim* sim,
		       test::MySim::ServerFilter server_disp_filter,
		       int head_w, int data_w, int data_prec) {
  out << std::setw(head_w) << "requests:";
  int total_req = 0;
  for (unsigned i = 0; i < sim->get_server_count(); ++i) {
    const auto& server = sim->get_server(i);
    auto req_count = server.get_accumulator().request_count;
    total_req += req_count;
    if (!server_disp_filter(i)) continue;
    out << std::setw(data_w) << req_count;
  }
  out << std::setw(data_w) << std::setprecision(data_prec) <<
    std::fixed << total_req << std::endl;

#ifdef PROFILE
    crimson::ProfileCombiner<std::chrono::nanoseconds> art_combiner;
    crimson::ProfileCombiner<std::chrono::nanoseconds> rct_combiner;
    for (unsigned i = 0; i < sim->get_server_count(); ++i) {
      const auto& q = sim->get_server(i).get_priority_queue();
      const auto& art = q.add_request_timer;
      art_combiner.combine(art);
      const auto& rct = q.request_complete_timer;
      rct_combiner.combine(rct);
    }
    out << "Server add_request_timer: count:" << art_combiner.get_count() <<
      ", mean:" << art_combiner.get_mean() <<
      ", std_dev:" << art_combiner.get_std_dev() <<
      ", low:" << art_combiner.get_low() <<
      ", high:" << art_combiner.get_high() << std::endl;
    out << "Server request_complete_timer: count:" << rct_combiner.get_count() <<
      ", mean:" << rct_combiner.get_mean() <<
      ", std_dev:" << rct_combiner.get_std_dev() <<
      ", low:" << rct_combiner.get_low() <<
      ", high:" << rct_combiner.get_high() << std::endl;
    out << "Server combined mean: " <<
      (art_combiner.get_mean() + rct_combiner.get_mean()) <<
      std::endl;
#endif
}
