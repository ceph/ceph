// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_RADOS_TESTCASE_H
#define CEPH_TEST_RADOS_TESTCASE_H

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "gtest/gtest.h"

#include <string>

/**
 * These test cases create a temporary pool that lives as long as the
 * test case.  Each test within a test case gets a new ioctx set to a
 * unique namespace within the pool.
 *
 * Since pool creation and deletion is slow, this allows many tests to
 * run faster.
 */
class RadosTest : public ::testing::Test {
public:
  RadosTest() {}
  virtual ~RadosTest() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_default_namespace(rados_ioctx_t ioctx);
  static rados_t s_cluster;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  rados_t cluster;
  rados_ioctx_t ioctx;
};

class RadosTestPP : public ::testing::Test {
public:
  RadosTestPP() : cluster(s_cluster) {}
  virtual ~RadosTestPP() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_default_namespace(librados::IoCtx ioctx);
  static librados::Rados s_cluster;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  std::string ns;
};

class RadosTestEC : public ::testing::Test {
public:
  RadosTestEC() {}
  virtual ~RadosTestEC() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_default_namespace(rados_ioctx_t ioctx);
  static rados_t s_cluster;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  rados_t cluster;
  rados_ioctx_t ioctx;
  uint64_t alignment;
};

class RadosTestECPP : public ::testing::Test {
public:
  RadosTestECPP() : cluster(s_cluster) {};
  virtual ~RadosTestECPP() {};
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_default_namespace(librados::IoCtx ioctx);
  static librados::Rados s_cluster;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  std::string ns;
  uint64_t alignment;
};

#endif
