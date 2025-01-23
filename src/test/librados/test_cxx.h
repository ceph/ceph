// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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
std::string set_allow_ec_overwrites_pp(const std::string &pool_name,
				       librados::Rados &cluster, bool allow);
std::string connect_cluster_pp(librados::Rados &cluster);
std::string connect_cluster_pp(librados::Rados &cluster,
			       const std::map<std::string, std::string> &config);
int destroy_one_pool_pp(const std::string &pool_name, librados::Rados &cluster);
int destroy_one_ec_pool_pp(const std::string &pool_name, librados::Rados &cluster);
