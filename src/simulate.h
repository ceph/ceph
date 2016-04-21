// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <assert.h>

#include <memory>
#include <chrono>
#include <map>
#include <random>
#include <iostream>
#include <iomanip>
#include <string>


namespace crimson {
  namespace qos_simulation {

    template<typename ServerId, typename ClientId, typename TS, typename TC>
    class Simulation {
  
    public:

      using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

    protected:

      using ClientMap = std::map<ClientId,TC*>;
      using ServerMap = std::map<ServerId,TS*>;

      uint server_count = 0;
      uint client_count = 0;

      ServerMap servers;
      ClientMap clients;
      std::vector<ServerId> server_ids;

      TimePoint early_time;
      TimePoint clients_created_time;
      TimePoint late_time;

      std::default_random_engine prng;

      bool has_run = false;


    public:

      double fmt_tp(const TimePoint& t) {
	auto c = t.time_since_epoch().count();
	return uint64_t(c / 1000000.0 + 0.5) % 100000 / 1000.0;
      }

      TimePoint now() {
	return std::chrono::steady_clock::now();
      }

      using ClientBasedServerSelectFunc =
	std::function<const ServerId&(uint64_t, uint16_t)>;

      using ClientFilter = std::function<bool(const ClientId&)>;

      using ServerFilter = std::function<bool(const ServerId&)>;

      using ServerDataOutF =
	std::function<void(std::ostream& out,
			   Simulation* sim, ServerFilter,
			   int header_w, int data_w, int data_prec)>;

      using ClientDataOutF =
	std::function<void(std::ostream& out,
			   Simulation* sim, ClientFilter,
			   int header_w, int data_w, int data_prec)>;

      Simulation() :
	early_time(now()),
	prng(std::chrono::system_clock::now().time_since_epoch().count())
      {
	// empty
      }

      uint get_client_count() const { return client_count; }
      uint get_server_count() const { return server_count; }
      TC& get_client(ClientId id) { return *clients[id]; }
      TS& get_server(ServerId id) { return *servers[id]; }
      const ServerId& get_server_id(uint index) const { return server_ids[index]; }


      void add_servers(uint count,
		       std::function<TS*(ServerId)> create_server_f) {
	for (uint i = 0; i < count; ++i) {
	  server_ids.push_back(server_count + i);
	  servers[i] = create_server_f(server_count + i);
	}
	server_count += count;
      }


      void add_clients(uint count,
		       std::function<TC*(ClientId)> create_client_f) {
	for (uint i = 0; i < count; ++i) {
	  clients[i] = create_client_f(client_count + i);
	}
	client_count += count;

	clients_created_time = now();
      }


      void run() {
	assert(server_count > 0);
	assert(client_count > 0);

	std::cout << "simulation started" << std::endl;

	// clients are now running; wait for all to finish

	for (auto const &i : clients) {
	  i.second->wait_until_done();
	}

	late_time = now();

	std::cout << "simulation complete" << std::endl;

	has_run = true;
      } // run


      void display_stats(std::ostream& out,
			 ServerDataOutF server_out_f, ClientDataOutF client_out_f,
			 ServerFilter server_filter = [](const ServerId&) { return true; },
			 ClientFilter client_filter = [](const ClientId&) { return true; },
			 int head_w = 12, int data_w = 8, int data_prec = 2) {
	assert(has_run);
    
	const std::chrono::seconds skip_amount(0); // skip first 2 secondsd of data
	const std::chrono::seconds measure_unit(2); // calculate in groups of 5 seconds
	const std::chrono::seconds report_unit(1); // unit to output reports in

	// compute and display stats

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

	out << "==== Client Data ====" << std::endl;

	out << std::setw(head_w) << "client:";
	for (auto const &c : clients) {
	  if (!client_filter(c.first)) continue;
	  out << std::setw(data_w) << c.first;
	}
	out << std::setw(data_w) << "total" << std::endl;

	{
	  bool has_data;
	  size_t i = 0;
	  do {
	    std::string line_header = "t_" + std::to_string(i) + ":";
	    out << std::setw(head_w) << line_header;
	    has_data = false;
	    double total = 0.0;
	    for (auto const &c : clients) {
	      double data = 0.0;
	      if (i < ops_data[c.first].size()) {
		data = ops_data[c.first][i];
		has_data = true;
	      }
	      total += data;

	      if (!client_filter(c.first)) continue;

	      out << std::setw(data_w) << std::setprecision(data_prec) <<
		std::fixed << data;
	    }
	    out << std::setw(data_w) << std::setprecision(data_prec) <<
	      std::fixed << total << std::endl;
	    ++i;
	  } while(has_data);
	}

	client_out_f(out, this, client_filter, head_w, data_w, data_prec);

	out << std::endl << "==== Server Data ====" << std::endl;

	out << std::setw(head_w) << "server:";
	for (auto const &s : servers) {
	  if (!server_filter(s.first)) continue;
	  out << std::setw(data_w) << s.first;
	}
	out << std::setw(data_w) << "total" << std::endl;

	server_out_f(out, this, server_filter, head_w, data_w, data_prec);

	// clean up clients then servers

	for (auto i = clients.begin(); i != clients.end(); ++i) {
	  delete i->second;
	  i->second = nullptr;
	}

	for (auto i = servers.begin(); i != servers.end(); ++i) {
	  delete i->second;
	  i->second = nullptr;
	}
      } // display_stats


      // **** server selection functions ****


      const ServerId& server_select_alternate(uint64_t seed, uint16_t client_idx) {
	uint index = (client_idx + seed) % server_count;
	return server_ids[index];
      }


      // returns a lambda using the range specified as servers_per (client)
      ClientBasedServerSelectFunc make_server_select_alt_range(uint16_t servers_per) {
	return [servers_per,this](uint64_t seed, uint16_t client_idx)
	  -> const ServerId& {
	  double factor = double(server_count) / client_count;
	  uint offset = seed % servers_per;
	  uint index = (uint(0.5 + client_idx * factor) + offset) % server_count;
	  return server_ids[index];
	};
      }


      // function to choose a server randomly
      const ServerId& server_select_random(uint64_t seed, uint16_t client_idx) {
	uint index = prng() % server_count;
	return server_ids[index];
      }

  
      // function to choose a server randomly
      ClientBasedServerSelectFunc make_server_select_ran_range(uint16_t servers_per) {
	return [servers_per,this](uint64_t seed, uint16_t client_idx)
	  -> const ServerId& {
	  double factor = double(server_count) / client_count;
	  uint offset = prng() % servers_per;
	  uint index = (uint(0.5 + client_idx * factor) + offset) % server_count;
	  return server_ids[index];
	};
      }


      // function to always choose the first server
      const ServerId& server_select_0(uint64_t seed, uint16_t client_idx) {
	return server_ids[0];
      }
    }; // class Simulation

  }; // namespace qos_simulation
}; // namespace crimson
