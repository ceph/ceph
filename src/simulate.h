// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <unistd.h>

#include <memory>
#include <chrono>
#include <map>
#include <random>
#include <iostream>
#include <iomanip>

#include "simulate_common.h"



template<typename TS, typename TC, typename ClientInfo>
void simulate(std::function<TS*(ServerId)> create_server_f,
              std::function<TC*(ClientId)> create_client_f) {
  using ClientMap = std::map<ClientId,TC*>;
  using ServerMap = std::map<ServerId,TS*>;

  std::cout << "simulation started" << std::endl;

  // simulation params

  const TimePoint early_time = now();
  const chrono::seconds skip_amount(0); // skip first 2 secondsd of data
  const chrono::seconds measure_unit(2); // calculate in groups of 5 seconds
  const chrono::seconds report_unit(1); // unit to output reports in

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

  ClientMap clients;

  std::vector<ServerId> server_ids;

  ServerMap servers;
  for (uint i = 0; i < server_count; ++i) {
    server_ids.push_back(i);
    servers[i] =
      new TS(i,
             server_iops, server_threads,
             client_info_f, client_response_f, dmc_server_accumulate_f,
             server_soft_limit);
  }

  // construct clients

  // lambda to choose a server based on a seed and client; called by client
  auto server_alternate_f =
    [&server_ids, &server_count](uint64_t seed, uint16_t client_idx) -> const ServerId& {
    int index = (client_idx + seed) % server_count;
    return server_ids[index];
  };

  // lambda to choose a server alternately in a range
  auto server_alt_range_f =
    [&server_ids, &server_count, &client_count]
    (uint64_t seed, uint16_t client_idx, uint16_t servers_per) -> const ServerId& {
    double factor = double(server_count) / client_count;
    uint offset = seed % servers_per;
    uint index = (uint(0.5 + client_idx * factor) + offset) % server_count;
    return server_ids[index];
  };

  std::default_random_engine
    srv_rand(std::chrono::system_clock::now().time_since_epoch().count());

  // lambda to choose a server randomly
  auto server_random_f =
    [&server_ids, &srv_rand, &server_count] (uint64_t seed) -> const ServerId& {
    int index = srv_rand() % server_count;
    return server_ids[index];
  };

  // lambda to choose a server randomly
  auto server_ran_range_f =
    [&server_ids, &srv_rand, &server_count, &client_count]
    (uint64_t seed, uint16_t client_idx, uint16_t servers_per) -> const ServerId& {
    double factor = double(server_count) / client_count;
    uint offset = srv_rand() % servers_per;
    uint index = (uint(0.5 + client_idx * factor) + offset) % server_count;
    return server_ids[index];
  };


  // lambda to always choose the first server
  SelectFunc server_0_f =
    [server_ids] (uint64_t seed) -> const ServerId& {
    return server_ids[0];
  };

  // lambda to post a request to the identified server; called by client
  SubmitFunc server_post_f =
    [&servers](const ServerId& server,
	       const TestRequest& request,
	       const dmc::ReqParams<ClientId>& req_params) {
    auto i = servers.find(server);
    assert(servers.end() != i);
    i->second->post(request, req_params);
  };

  for (uint i = 0; i < client_count; ++i) {
    static std::vector<CliInst> no_wait =
      { { req_op, client_total_ops, client_iops_goal, client_outstanding_ops } };
    static std::vector<CliInst> wait =
      { { wait_op, client_wait },
	{ req_op, client_total_ops, client_iops_goal, client_outstanding_ops } };

    SelectFunc server_select_f =
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

    clients[i] =
      new TC(i,
	     server_post_f,
	     server_select_f,
	     dmc_client_accumulate_f,
	     i < (client_count - client_wait_count) ? no_wait : wait
	);
  } // for

  TimePoint clients_created_time = now();

  // clients are now running; wait for all to finish

  for (auto const &i : clients) {
    i.second->wait_until_done();
  }

  // compute and display stats

  const TimePoint late_time = now();
  TimePoint earliest_start = late_time;
  TimePoint latest_start = early_time;
  TimePoint earliest_finish = late_time;
  TimePoint latest_finish = early_time;

  for (auto const &c : clients) {
    auto start = c.second->get_op_times().front();
    auto end = c.second->get_op_times().back();

    if (start < earliest_start) { earliest_start = start; }
    if (start > latest_start) { latest_start = start; }
    if (end < earliest_finish) { earliest_finish = end; }
    if (end > latest_finish) { latest_finish = end; }
  }

  double ops_factor =
    std::chrono::duration_cast<std::chrono::duration<double>>(measure_unit) /
    std::chrono::duration_cast<std::chrono::duration<double>>(report_unit);

  const auto start_edge = clients_created_time + skip_amount;

  std::map<ClientId,std::vector<double>> ops_data;

  for (auto const &c : clients) {
    auto it = c.second->get_op_times().begin();
    const auto end = c.second->get_op_times().end();
    while (it != end && *it < start_edge) { ++it; }

    for (auto time_edge = start_edge + measure_unit;
	 time_edge < latest_finish;
	 time_edge += measure_unit) {
      int count = 0;
      for (; it != end && *it < time_edge; ++count, ++it) { /* empty */ }
      double ops_per_second = double(count) / ops_factor;
      ops_data[c.first].push_back(ops_per_second);
    }
  }

  const int head_w = 12;
  const int data_w = 8;
  const int data_prec = 2;

  auto client_disp_filter = [=] (ClientId i) -> bool {
    return i < 3 || i >= (client_count - 3);
  };

  auto server_disp_filter = [=] (ServerId i) -> bool {
    return i < 3 || i >= (server_count - 3);
  };

  std::cout << "==== Client Data ====" << std::endl;

  std::cout << std::setw(head_w) << "client:";
  for (auto const &c : clients) {
    if (!client_disp_filter(c.first)) continue;
    std::cout << std::setw(data_w) << c.first;
  }
  std::cout << std::setw(data_w) << "total" << std::endl;

  {
    bool has_data;
    size_t i = 0;
    do {
      std::string line_header = "t_" + std::to_string(i) + ":";
      std::cout << std::setw(head_w) << line_header;
      has_data = false;
      double total = 0.0;
      for (auto const &c : clients) {
	double data = 0.0;
	if (i < ops_data[c.first].size()) {
	  data = ops_data[c.first][i];
	  has_data = true;
	}
	total += data;

	if (!client_disp_filter(c.first)) continue;

	std::cout << std::setw(data_w) << std::setprecision(data_prec) <<
	  std::fixed << data;
      }
      std::cout << std::setw(data_w) << std::setprecision(data_prec) <<
	std::fixed << total << std::endl;
      ++i;
    } while(has_data);
  }

#if 0
  std::cout << client_data(head_w, data_w);
#endif

  std::cout << std::endl << "==== Server Data ====" << std::endl;

  std::cout << std::setw(head_w) << "server:";
  for (auto const &s : servers) {
    if (!server_disp_filter(s.first)) continue;
    std::cout << std::setw(data_w) << s.first;
  }
  std::cout << std::setw(data_w) << "total" << std::endl;

#if 0
  std::cout << server_data(head_w, data_w);
#endif

  // clean up clients then servers

  for (auto i = clients.begin(); i != clients.end(); ++i) {
    delete i->second;
    i->second = nullptr;
  }

  for (auto i = servers.begin(); i != servers.end(); ++i) {
    delete i->second;
    i->second = nullptr;
  }

  std::cout << "simulation complete" << std::endl;
} // simulate<TS,TC>
