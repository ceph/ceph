// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#pragma once


#include <string.h>

#include <chrono>
#include <vector>
#include <sstream>
#include <iomanip>

#include "ConfUtils.h"


namespace crimson {
  namespace qos_simulation {

    struct cli_group_t {
      uint client_count;
      std::chrono::seconds client_wait;
      uint client_total_ops;
      uint client_server_select_range;
      uint client_iops_goal;
      uint client_outstanding_ops;
      double client_reservation;
      double client_limit;
      double client_weight;

      cli_group_t(uint _client_count = 100,
		  uint _client_wait = 0,
		  uint _client_total_ops = 1000,
		  uint _client_server_select_range = 10,
		  uint _client_iops_goal = 50,
		  uint _client_outstanding_ops = 100,
		  double _client_reservation = 20.0,
		  double _client_limit = 60.0,
		  double _client_weight = 1.0) :
	client_count(_client_count),
	client_wait(std::chrono::seconds(_client_wait)),
	client_total_ops(_client_total_ops),
	client_server_select_range(_client_server_select_range),
	client_iops_goal(_client_iops_goal),
	client_outstanding_ops(_client_outstanding_ops),
	client_reservation(_client_reservation),
	client_limit(_client_limit),
	client_weight(_client_weight)
      {
	// empty
      }

      friend std::ostream& operator<<(std::ostream& out,
	  const cli_group_t& cli_group) {
	out <<
	  "client_count = " << cli_group.client_count << "\n" <<
	  "client_wait = " << cli_group.client_wait.count() << "\n" <<
	  "client_total_ops = " << cli_group.client_total_ops << "\n" <<
	  "client_server_select_range = " << cli_group.client_server_select_range << "\n" <<
	  "client_iops_goal = " << cli_group.client_iops_goal << "\n" <<
	  "client_outstanding_ops = " << cli_group.client_outstanding_ops << "\n" <<
	  std::fixed << std::setprecision(1) <<
	  "client_reservation = " << cli_group.client_reservation << "\n" <<
	  "client_limit = " << cli_group.client_limit << "\n" <<
	  "client_weight = " << cli_group.client_weight;
	return out;
      }
    }; // class cli_group_t


    struct srv_group_t {
      uint server_count;
      uint server_iops;
      uint server_threads;

      srv_group_t(uint _server_count = 100,
		  uint _server_iops = 40,
		  uint _server_threads = 1) :
	server_count(_server_count),
	server_iops(_server_iops),
	server_threads(_server_threads)
      {
	// empty
      }

      friend std::ostream& operator<<(std::ostream& out,
	  const srv_group_t& srv_group) {
	out <<
	  "server_count = " << srv_group.server_count << "\n" <<
	  "server_iops = " << srv_group.server_iops << "\n" <<
	  "server_threads = " << srv_group.server_threads;
	return out;
      }
    }; // class srv_group_t


    struct sim_config_t {
      uint server_groups;
      uint client_groups;
      bool server_random_selection;
      bool server_soft_limit;

      std::vector<cli_group_t> cli_group;
      std::vector<srv_group_t> srv_group;

      sim_config_t(uint _server_groups = 1,
		   uint _client_groups = 1,
		   bool _server_random_selection = false,
		   bool _server_soft_limit = true) :
	server_groups(_server_groups),
	client_groups(_client_groups),
	server_random_selection(_server_random_selection),
	server_soft_limit(_server_soft_limit)
      {
	srv_group.reserve(server_groups);
	cli_group.reserve(client_groups);
      }

      friend std::ostream& operator<<(std::ostream& out,
	  const sim_config_t& sim_config) {
	out <<
	  "server_groups = " << sim_config.server_groups << "\n" <<
	  "client_groups = " << sim_config.client_groups << "\n" <<
	  "server_random_selection = " << sim_config.server_random_selection << "\n" <<
	  "server_soft_limit = " << sim_config.server_soft_limit;
	return out;
      }
    }; // class sim_config_t


    bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, ...);
    void ceph_argparse_early_args(std::vector<const char*>& args, std::string *conf_file_list);
    int parse_config_file(const std::string &fname, sim_config_t &g_conf);

  }; // namespace qos_simulation
}; // namespace crimson
