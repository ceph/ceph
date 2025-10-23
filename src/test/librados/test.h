// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_TEST_RADOS_API_TEST_H
#define CEPH_TEST_RADOS_API_TEST_H

#include "include/rados/librados.h"
#include "test/librados/test_shared.h"

#include <map>
#include <string>
#include <unistd.h>

std::string create_one_pool(const std::string &pool_name, rados_t *cluster);
std::string create_pool(const std::string &pool_name, rados_t *cluster);
std::string create_one_ec_pool(const std::string &pool_name, rados_t *cluster, bool fast_ec = false);
std::string create_ec_pool(const std::string &pool_name, rados_t *cluster, bool fast_ec = false);
std::string set_pool_flags(const std::string &pool_name, rados_t *cluster, int64_t flags, bool set_not_unset);
std::string set_split_ops(const std::string &pool_name, rados_t *cluster, bool set_not_unset);
std::string connect_cluster(rados_t *cluster);
int destroy_one_pool(const std::string &pool_name, rados_t *cluster);
int destroy_one_ec_pool(const std::string &pool_name, rados_t *cluster);
int destroy_pool(const std::string &pool_name, rados_t *cluster);
int destroy_ec_pool(const std::string &pool_name, rados_t *cluster);
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


#endif
