// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "include/rados/librados.hpp"
#include "test/librados/test_shared.h"

std::string create_one_pool_pp(const std::string &pool_name,
			    librados::Rados &cluster);
std::string create_one_pool_pp(const std::string &pool_name,
			       librados::Rados &cluster,
			       const std::map<std::string, std::string> &config);
// k_per_zone > 0 activates stretch-pool mode: the function will also
// set num_zones=2, allow_ec_optimizations, and run "osd pool stretch set".
// The cluster CRUSH map must already have two datacenter-type buckets.
std::string create_one_ec_pool_pp(
  const std::string &pool_name,
  librados::Rados &cluster,
  bool fast_ec = false,
  int k_per_zone = 0,
  int m_per_zone = 0);
std::string create_ec_pool_pp(
  const std::string &pool_name,
  librados::Rados &cluster,
  bool fast_ec = false,
  int k_per_zone = 0,
  int m_per_zone = 0);
std::string create_pool_pp(const std::string &pool_name,
                            librados::Rados &cluster);
std::string set_allow_ec_overwrites_pp(const std::string &pool_name,
				       librados::Rados &cluster, bool allow);
std::string connect_cluster_pp(librados::Rados &cluster);
std::string connect_cluster_pp(librados::Rados &cluster,
			       const std::map<std::string, std::string> &config);
int destroy_one_pool_pp(const std::string &pool_name, librados::Rados &cluster);
int destroy_one_ec_pool_pp(const std::string &pool_name, librados::Rados &cluster);
int destroy_pool_pp(const std::string &pool_name, librados::Rados &cluster);
int set_config(librados::Rados &cluster, const std::string& who, const std::string& name, const std::string &val);
std::string get_config(librados::Rados &cluster, const std::string& who, const std::string& name);
std::string set_pool_flags_pp(const std::string &pool_name, librados::Rados &cluster, int64_t flags, bool set_not_unset);

// The following are convenient macros for defining test combinations
// with each of the gtest suites.
#define INSTANTIATE_TEST_SUITE_P_EC(CLASS) \
INSTANTIATE_TEST_SUITE_P( CLASS ## ParamCombination, CLASS, \
::testing::Combine( \
::testing::Bool(),   /* fast_ec */ \
::testing::Bool()))  /* split_ops */

#define INSTANTIATE_TEST_SUITE_P_REPLICA(CLASS) \
INSTANTIATE_TEST_SUITE_P( CLASS ## ParamCombination, CLASS, \
::testing::Bool()) /* split_ops */
