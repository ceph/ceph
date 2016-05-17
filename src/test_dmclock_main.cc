// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "test_dmclock.h"

#ifdef PROFILE
#include "profile.h"
#endif


namespace dmc = crimson::dmclock;
namespace test = crimson::test_dmc;
namespace sim = crimson::qos_simulation;

using namespace std::placeholders;


namespace crimson {
    namespace test_dmc {
        void server_data(std::ostream& out,
                         test::MySim* sim,
                         test::MySim::ServerFilter server_disp_filter,
                         int head_w, int data_w, int data_prec);

        void client_data(std::ostream& out,
                         test::MySim* sim,
                         test::MySim::ClientFilter client_disp_filter,
                         int head_w, int data_w, int data_prec);
    }
}


int main(int argc, char* argv[]) {
    // server params

    const uint server_count = 100;
    const uint server_iops = 40;
    const uint server_threads = 1;
    const bool server_soft_limit = true;

    // client params

    const uint client_total_ops = 1000;
    const uint client_count = 100;
    const uint client_server_select_range = 10;
    const uint client_wait_count = 1;
    const uint client_iops_goal = 50;
    const uint client_outstanding_ops = 100;
    const std::chrono::seconds client_wait(10);

    // client info
  
    const double client_reservation = 20.0;
    const double client_limit = 60.0;
    const double client_weight = 1.0;

    test::dmc::ClientInfo client_info(client_reservation,
                                      client_weight,
                                      client_limit);

    auto client_info_f = [=](const ClientId& c) -> test::dmc::ClientInfo {
        return client_info;
    };

    auto client_disp_filter = [=] (const ClientId& i) -> bool {
        return i < 3 || i >= (client_count - 3);
    };

    auto server_disp_filter = [=] (const ServerId& i) -> bool {
        return i < 3 || i >= (server_count - 3);
    };


    test::MySim *simulation;
  

    // lambda to post a request to the identified server; called by client
    test::SubmitFunc server_post_f =
        [&simulation](const ServerId& server,
                      const sim::TestRequest& request,
                      const ClientId& client_id,
                      const test::dmc::ReqParams& req_params) {
        test::DmcServer& s = simulation->get_server(server);
        s.post(request, client_id, req_params);
    };

    static std::vector<sim::CliInst> no_wait =
        { { sim::req_op, client_total_ops, client_iops_goal, client_outstanding_ops } };
    static std::vector<sim::CliInst> wait =
        { { sim::wait_op, client_wait },
          { sim::req_op, client_total_ops, client_iops_goal, client_outstanding_ops } };

    simulation = new test::MySim();

    test::MySim::ClientBasedServerSelectFunc server_select_f =
      simulation->make_server_select_alt_range(client_server_select_range);

    test::DmcServer::ClientRespFunc client_response_f =
        [&simulation](ClientId client_id,
                      const sim::TestResponse& resp,
                      const ServerId& server_id,
                      const dmc::PhaseType& phase) {
        simulation->get_client(client_id).receive_response(resp,
                                                           server_id,
                                                           phase);
    };

    test::CreateQueueF create_queue_f =
        [&](test::DmcQueue::CanHandleRequestFunc can_f,
            test::DmcQueue::HandleRequestFunc handle_f) -> test::DmcQueue* {
        return new test::DmcQueue(client_info_f, can_f, handle_f, server_soft_limit);
    };

  
    auto create_server_f = [&](ServerId id) -> test::DmcServer* {
        return new test::DmcServer(id,
                                   server_iops, server_threads,
                                   client_response_f,
                                   test::dmc_server_accumulate_f,
                                   create_queue_f);
    };

    auto create_client_f = [&](ClientId id) -> test::DmcClient* {
        return new test::DmcClient(id,
                                   server_post_f,
                                   std::bind(server_select_f, _1, id),
                                   test::dmc_client_accumulate_f,
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
    // report how many ops were done by reservation and proportion for
    // each client

    int total_r = 0;
    out << std::setw(head_w) << "res_ops:";
    for (uint i = 0; i < sim->get_client_count(); ++i) {
        const auto& client = sim->get_client(i);
        auto r = client.get_accumulator().reservation_count;
        total_r += r;
        if (!client_disp_filter(i)) continue;
        out << std::setw(data_w) << r;
    }
    out << std::setw(data_w) << std::setprecision(data_prec) <<
        std::fixed << total_r << std::endl;

    int total_p = 0;
    out << std::setw(head_w) << "prop_ops:";
    for (uint i = 0; i < sim->get_client_count(); ++i) {
        const auto& client = sim->get_client(i);
        auto p = client.get_accumulator().proportion_count;
        total_p += p;
        if (!client_disp_filter(i)) continue;
        out << std::setw(data_w) << p;
    }
    out << std::setw(data_w) << std::setprecision(data_prec) <<
        std::fixed << total_p << std::endl;
}


void test::server_data(std::ostream& out,
		 test::MySim* sim,
		 test::MySim::ServerFilter server_disp_filter,
		 int head_w, int data_w, int data_prec) {
    out << std::setw(head_w) << "res_ops:";
    int total_r = 0;
    for (uint i = 0; i < sim->get_server_count(); ++i) {
        const auto& server = sim->get_server(i);
        auto rc = server.get_accumulator().reservation_count;
        total_r += rc;
        if (!server_disp_filter(i)) continue;
        out << std::setw(data_w) << rc;
    }
    out << std::setw(data_w) << std::setprecision(data_prec) <<
        std::fixed << total_r << std::endl;

    out << std::setw(head_w) << "prop_ops:";
    int total_p = 0;
    for (uint i = 0; i < sim->get_server_count(); ++i) {
        const auto& server = sim->get_server(i);
        auto pc = server.get_accumulator().proportion_count;
        total_p += pc;
        if (!server_disp_filter(i)) continue;
        out << std::setw(data_w) << pc;
    }
    out << std::setw(data_w) << std::setprecision(data_prec) <<
        std::fixed << total_p << std::endl;

#ifdef PROFILE
    crimson::ProfileCombiner<std::chrono::nanoseconds> art_combiner;
    crimson::ProfileCombiner<std::chrono::nanoseconds> rct_combiner;
    for (uint i = 0; i < sim->get_server_count(); ++i) {
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
