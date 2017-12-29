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
  RadosTestNS(bool c=false) : cleanup(c) {}
  ~RadosTestNS() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_all_objects(rados_ioctx_t ioctx);
  static rados_t s_cluster;
  static std::string pool_name;

  void SetUp() override;
  void TearDown() override;
  rados_t cluster = nullptr;
  rados_ioctx_t ioctx = nullptr;
  bool cleanup;
};

struct RadosTestNSCleanup : public RadosTestNS {
  RadosTestNSCleanup() : RadosTestNS(true) {}
};

class RadosTestPPNS : public ::testing::Test {
public:
  RadosTestPPNS(bool c=false) : cluster(s_cluster), cleanup(c) {}
  ~RadosTestPPNS() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_all_objects(librados::IoCtx ioctx);
  static librados::Rados s_cluster;
  static std::string pool_name;

  void SetUp() override;
  void TearDown() override;
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  bool cleanup;
};

struct RadosTestPPNSCleanup : public RadosTestPPNS {
  RadosTestPPNSCleanup() : RadosTestPPNS(true) {}
};

class RadosTestParamPPNS : public ::testing::TestWithParam<const char*> {
public:
  RadosTestParamPPNS(bool c=false) : cluster(s_cluster), cleanup(c) {}
  ~RadosTestParamPPNS() override {}
  static void SetUpTestCase();
  static void TearDownTestCase();
protected:
  static void cleanup_all_objects(librados::IoCtx ioctx);
  static librados::Rados s_cluster;
  static std::string pool_name;
  static std::string cache_pool_name;

  void SetUp() override;
  void TearDown() override;
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  bool cleanup;
};

class RadosTestECNS : public RadosTestNS {
public:
  RadosTestECNS(bool c=false) : cleanup(c) {}
  ~RadosTestECNS() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static rados_t s_cluster;
  static std::string pool_name;

  void SetUp() override;
  void TearDown() override;
  rados_t cluster = nullptr; 
  rados_ioctx_t ioctx = nullptr;
  uint64_t alignment = 0;
  bool cleanup;
};

struct RadosTestECNSCleanup : public RadosTestECNS {
  RadosTestECNSCleanup() : RadosTestECNS(true) {}
};

class RadosTestECPPNS : public RadosTestPPNS {
public:
  RadosTestECPPNS(bool c=false) : cluster(s_cluster), cleanup(c) {}
  ~RadosTestECPPNS() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static librados::Rados s_cluster;
  static std::string pool_name;

  void SetUp() override;
  void TearDown() override;
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  uint64_t alignment = 0;
  bool cleanup;
};

struct RadosTestECPPNSCleanup : public RadosTestECPPNS {
  RadosTestECPPNSCleanup() : RadosTestECPPNS(true) {}
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
  RadosTest(bool c=false) : cleanup(c) {}
  ~RadosTest() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_default_namespace(rados_ioctx_t ioctx);
  static void cleanup_namespace(rados_ioctx_t ioctx, std::string ns);
  static rados_t s_cluster;
  static std::string pool_name;

  void SetUp() override;
  void TearDown() override;
  rados_t cluster = nullptr;
  rados_ioctx_t ioctx = nullptr;
  std::string nspace;
  bool cleanup;
};

class RadosTestPP : public ::testing::Test {
public:
  RadosTestPP(bool c=false) : cluster(s_cluster), cleanup(c) {}
  ~RadosTestPP() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void cleanup_default_namespace(librados::IoCtx ioctx);
  static void cleanup_namespace(librados::IoCtx ioctx, std::string ns);
  static librados::Rados s_cluster;
  static std::string pool_name;

  void SetUp() override;
  void TearDown() override;
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  bool cleanup;
  std::string nspace;
};

class RadosTestParamPP : public ::testing::TestWithParam<const char*> {
public:
  RadosTestParamPP(bool c=false) : cluster(s_cluster), cleanup(c) {}
  ~RadosTestParamPP() override {}
  static void SetUpTestCase();
  static void TearDownTestCase();
protected:
  static void cleanup_default_namespace(librados::IoCtx ioctx);
  static void cleanup_namespace(librados::IoCtx ioctx, std::string ns);
  static librados::Rados s_cluster;
  static std::string pool_name;
  static std::string cache_pool_name;

  void SetUp() override;
  void TearDown() override;
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  bool cleanup;
  std::string nspace;
};

class RadosTestEC : public RadosTest {
public:
  RadosTestEC(bool c=false) : cleanup(c) {}
  ~RadosTestEC() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static rados_t s_cluster;
  static std::string pool_name;

  void SetUp() override;
  void TearDown() override;
  rados_t cluster = nullptr;
  rados_ioctx_t ioctx = nullptr;
  bool cleanup;
  std::string nspace;
  uint64_t alignment = 0;
};

class RadosTestECPP : public RadosTestPP {
public:
  RadosTestECPP(bool c=false) : cluster(s_cluster), cleanup(c) {}
  ~RadosTestECPP() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static librados::Rados s_cluster;
  static std::string pool_name;

  void SetUp() override;
  void TearDown() override;
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  bool cleanup;
  std::string nspace;
  uint64_t alignment = 0;
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
  ~RadosTestNP() override {}
};

#endif
