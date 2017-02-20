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
 * test case.  We initially use the default namespace and assume
 * test will whatever namespaces it wants.  After each test all objects
 * are removed.
 *
 * Since pool creation and deletion is slow, this allows many tests to
 * run faster.
 */
class RadosTestNS : public ::testing::Test {
public:
  RadosTestNS() {}
  virtual ~RadosTestNS() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_all_objects(rados_ioctx_t ioctx);
  static rados_t s_cluster;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  rados_t cluster;
  rados_ioctx_t ioctx;
};

class RadosTestPPNS : public ::testing::Test {
public:
  RadosTestPPNS() : cluster(s_cluster) {}
  virtual ~RadosTestPPNS() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_all_objects(librados::IoCtx ioctx);
  static librados::Rados s_cluster;
  static bool is_connected;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  librados::Rados &cluster;
  librados::IoCtx ioctx;
};

class RadosTestParamPPNS : public ::testing::TestWithParam<const char*> {
public:
  RadosTestParamPPNS() : cluster(s_cluster) {}
  virtual ~RadosTestParamPPNS() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
protected:
  static void cleanup_all_objects(librados::IoCtx ioctx);
  static librados::Rados s_cluster;
  static bool is_connected;
  static std::string pool_name;
  static std::string cache_pool_name;

  virtual void SetUp();
  virtual void TearDown();
  librados::Rados &cluster;
  librados::IoCtx ioctx;
};

class RadosTestECNS : public RadosTestNS {
public:
  RadosTestECNS() {}
  virtual ~RadosTestECNS() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static rados_t s_cluster;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  rados_t cluster;
  rados_ioctx_t ioctx;
  uint64_t alignment;
};

class RadosTestECPPNS : public RadosTestPPNS {
public:
  RadosTestECPPNS() : cluster(s_cluster) {}
  virtual ~RadosTestECPPNS() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static librados::Rados s_cluster;
  static bool is_connected;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  uint64_t alignment;
};

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
  static void cleanup_namespace(rados_ioctx_t ioctx, std::string ns);
  static rados_t s_cluster;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string nspace;
};

class RadosTestPP : public ::testing::Test {
public:
  RadosTestPP() : cluster(s_cluster) {}
  virtual ~RadosTestPP() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_default_namespace(librados::IoCtx ioctx);
  static void cleanup_namespace(librados::IoCtx ioctx, std::string ns);
  static librados::Rados s_cluster;
  static bool is_connected;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  std::string nspace;
};

class RadosTestParamPP : public ::testing::TestWithParam<const char*> {
public:
  RadosTestParamPP() : cluster(s_cluster) {}
  virtual ~RadosTestParamPP() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
protected:
  static void cleanup_default_namespace(librados::IoCtx ioctx);
  static void cleanup_namespace(librados::IoCtx ioctx, std::string ns);
  static librados::Rados s_cluster;
  static bool is_connected;
  static std::string pool_name;
  static std::string cache_pool_name;

  virtual void SetUp();
  virtual void TearDown();
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  std::string nspace;
};

class RadosTestEC : public RadosTest {
public:
  RadosTestEC() {}
  virtual ~RadosTestEC() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static rados_t s_cluster;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string nspace;
  uint64_t alignment;
};

class RadosTestECPP : public RadosTestPP {
public:
  RadosTestECPP() : cluster(s_cluster) {}
  virtual ~RadosTestECPP() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static librados::Rados s_cluster;
  static bool is_connected;
  static std::string pool_name;

  virtual void SetUp();
  virtual void TearDown();
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  std::string nspace;
  uint64_t alignment;
};

/**
 * Test case without creating a temporary pool in advance.
 * This is necessary for scenarios such that we need to
 * manually create a pool, start some long-runing tasks and
 * then the related pool is suddenly gone.
 */
class RadosTestNP: public ::testing::Test {
public:
  RadosTestNP() {}
  virtual ~RadosTestNP() {}
};

#endif
