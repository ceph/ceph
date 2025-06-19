// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#pragma once


#include <string.h>

#include <chrono>
#include <vector>
#include <sstream>
#include <iomanip>

#include "ConfUtils.h"

#include "sim_recs.h"


namespace crimson {
  namespace qos_simulation {

    struct cli_group_t {
      unsigned client_count;
      std::chrono::seconds client_wait;
      unsigned client_total_ops;
      unsigned client_server_select_range;
      unsigned client_iops_goal;
      unsigned client_outstanding_ops;
      double client_reservation;
      double client_limit;
      double client_weight;
      Cost client_req_cost;

      cli_group_t(unsigned _client_count = 100,
		  unsigned _client_wait = 0,
		  unsigned _client_total_ops = 1000,
		  unsigned _client_server_select_range = 10,
		  unsigned _client_iops_goal = 50,
		  unsigned _client_outstanding_ops = 100,
		  double _client_reservation = 20.0,
		  double _client_limit = 60.0,
		  double _client_weight = 1.0,
		  Cost _client_req_cost = 1u) :
	client_count(_client_count),
	client_wait(std::chrono::seconds(_client_wait)),
	client_total_ops(_client_total_ops),
	client_server_select_range(_client_server_select_range),
	client_iops_goal(_client_iops_goal),
	client_outstanding_ops(_client_outstanding_ops),
	client_reservation(_client_reservation),
	client_limit(_client_limit),
	client_weight(_client_weight),
	client_req_cost(_client_req_cost)
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
	  "client_weight = " << cli_group.client_weight << "\n" <<
	  "client_req_cost = " << cli_group.client_req_cost;
	return out;
      }
    }; // class cli_group_t


    struct srv_group_t {
      unsigned server_count;
      unsigned server_iops;
      unsigned server_threads;

      srv_group_t(unsigned _server_count = 100,
		  unsigned _server_iops = 40,
		  unsigned _server_threads = 1) :
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
      unsigned server_groups;
      unsigned client_groups;
      bool server_random_selection;
      bool server_soft_limit;
      double anticipation_timeout;

      std::vector<cli_group_t> cli_group;
      std::vector<srv_group_t> srv_group;

      sim_config_t(unsigned _server_groups = 1,
		   unsigned _client_groups = 1,
		   bool _server_random_selection = false,
		   bool _server_soft_limit = true,
		   double _anticipation_timeout = 0.0) :
	server_groups(_server_groups),
	client_groups(_client_groups),
	server_random_selection(_server_random_selection),
	server_soft_limit(_server_soft_limit),
	anticipation_timeout(_anticipation_timeout)
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
	  "server_soft_limit = " << sim_config.server_soft_limit << "\n" <<
	  std::fixed << std::setprecision(3) << 
	  "anticipation_timeout = " << sim_config.anticipation_timeout;
	return out;
      }
    }; // class sim_config_t


    bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, ...);
    void ceph_argparse_early_args(std::vector<const char*>& args, std::string *conf_file_list);
    int parse_config_file(const std::string &fname, sim_config_t &g_conf);

  }; // namespace qos_simulation
}; // namespace crimson
