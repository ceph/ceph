// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#pragma once


#include <string.h>

#include <chrono>
#include <vector>
#include <sstream>

#include "ConfUtils.h"


namespace crimson {
  namespace qos_simulation {

    struct cli_type_t {
      std::chrono::seconds client_wait;
      uint client_total_ops;
      uint client_server_select_range;
      uint client_iops_goal;
      uint client_outstanding_ops;
      double client_reservation;
      double client_limit;
      double client_weight;

      cli_type_t() :
	client_wait(std::chrono::seconds(0)),
	client_total_ops(1000),
	client_server_select_range(10),
	client_iops_goal(50),
	client_outstanding_ops(100),
	client_reservation(20.0),
	client_limit(60.0),
	client_weight(1.0)
      {
	// empty
      }

      friend std::ostream& operator<<(std::ostream& out,
	  const cli_type_t& cli_type) {
	out <<
	  "client_wait:" << cli_type.client_wait.count() << "\n" <<
	  "client_total_ops:" << cli_type.client_total_ops << "\n" <<
	  "client_server_select_range:" << cli_type.client_server_select_range << "\n" <<
	  "client_iops_goal:" << cli_type.client_iops_goal << "\n" <<
	  "client_outstanding_ops:" << cli_type.client_outstanding_ops << "\n" <<
	  "client_reservation:" << cli_type.client_reservation << "\n" <<
	  "client_limit:" << cli_type.client_limit << "\n" <<
	  "client_weight:" << cli_type.client_weight;
	return out;
      }
    }; // class cli_type_t


    struct srv_type_t {
      uint server_iops;
      uint server_threads;

      srv_type_t() :
	server_iops(40),
	server_threads(1)
      {
	// empty
      }

      friend std::ostream& operator<<(std::ostream& out,
	  const srv_type_t& srv_type) {
	out <<
	  "server_iops:" << srv_type.server_iops << "\n" <<
	  "server_threads:" << srv_type.server_threads;
	return out;
      }
    }; // class srv_type_t


    struct sim_config_t {
      uint server_count;
      uint client_count;
      uint server_types;
      uint client_types;
      bool server_random_selection;
      bool server_soft_limit;

      std::vector<cli_type_t> cli_type;
      std::vector<srv_type_t> srv_type;

      sim_config_t() :
	server_count(100),
	client_count(100),
	server_types(1),
	client_types(1),
	server_random_selection(false),
	server_soft_limit(true)
      {
	srv_type.reserve(server_types);
	cli_type.reserve(client_types);
      }

      friend std::ostream& operator<<(std::ostream& out,
	  const sim_config_t& sim_config) {
	out <<
	  "server_count:" << sim_config.server_count << "\n" <<
	  "client_count:" << sim_config.client_count << "\n" <<
	  "server_types:" << sim_config.server_types << "\n" <<
	  "client_types:" << sim_config.client_types << "\n" <<
	  "server_random_selection:" << sim_config.server_random_selection << "\n" <<
	  "server_soft_limit:" << sim_config.server_soft_limit;
	return out;
      }
    }; // class sim_config_t


    bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, ...);
    void ceph_argparse_early_args(std::vector<const char*>& args, std::string *conf_file_list);
    int parse_config_file(const std::string &fname, sim_config_t &g_conf);

  }; // namespace qos_simulation
}; // namespace crimson
