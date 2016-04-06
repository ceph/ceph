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


template<typename TS, typename TC>
void simulate(uint server_count, std::function<TS*(ServerId)> create_server_f,
              uint client_count, std::function<TC*(ClientId)> create_client_f) {
  using ClientMap = std::map<ClientId,TC*>;
  using ServerMap = std::map<ServerId,TS*>;

  std::cout << "simulation started" << std::endl;

  // simulation params

  const TimePoint early_time = now();
  const std::chrono::seconds skip_amount(0); // skip first 2 secondsd of data
  const std::chrono::seconds measure_unit(2); // calculate in groups of 5 seconds
  const std::chrono::seconds report_unit(1); // unit to output reports in

  ClientMap clients;

  std::vector<ServerId> server_ids;

  ServerMap servers;
  for (uint i = 0; i < server_count; ++i) {
    server_ids.push_back(i);
    servers[i] = create_server_f(i);
  }

  // construct clients

  for (uint i = 0; i < client_count; ++i) {
    clients[i] = create_client_f(i);
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
