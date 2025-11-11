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
std::string create_one_ec_pool_pp(const std::string &pool_name,
			    librados::Rados &cluster);
std::string create_ec_pool_pp(const std::string &pool_name,
                            librados::Rados &cluster,
                            bool fast_ec);
std::string set_allow_ec_overwrites_pp(const std::string &pool_name,
				       librados::Rados &cluster, bool allow);
std::string connect_cluster_pp(librados::Rados &cluster);
std::string connect_cluster_pp(librados::Rados &cluster,
			       const std::map<std::string, std::string> &config);
int destroy_one_pool_pp(const std::string &pool_name, librados::Rados &cluster);
int destroy_one_ec_pool_pp(const std::string &pool_name, librados::Rados &cluster);
int destroy_ec_pool_pp(const std::string &pool_name, librados::Rados &cluster);
std::string set_pool_flags_pp(const std::string &pool_name, librados::Rados &cluster, int64_t flags, bool set_not_unset);
std::string set_split_ops_pp(const std::string &pool_name, librados::Rados &cluster, bool set_not_unset);

// The following are convenient macros for defining test combinations
// with each of the gtest suites.

// This test suite is planned to have a second boolean 
// once split ops are implemented
#define INSTANTIATE_TEST_SUITE_P_EC(CLASS) \
INSTANTIATE_TEST_SUITE_P( CLASS ## ParamCombination, CLASS, \
::testing::Bool())   /* fast_ec */

// This test suite is planned to have a boolean 
// once split ops are implemented
#define INSTANTIATE_TEST_SUITE_P_REPLICA(CLASS) \
INSTANTIATE_TEST_SUITE_P( CLASS ## ParamCombination, CLASS)
