// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <string_view>

#include "gtest/gtest.h"
#include "include/rados/librados.hpp"

using namespace std::literals::string_view_literals;

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

class RadosTestPPBase {
public:
  RadosTestPPBase() : cluster(s_cluster) {};
  ~RadosTestPPBase() = default;
protected:
  static librados::Rados s_cluster;
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  bool split_ops = false;

  static void cleanup_default_namespace(librados::IoCtx ioctx);
  static void cleanup_namespace(librados::IoCtx ioctx, std::string ns);

  uint64_t get_perf_counter_by_path(std::string_view path);

  template<typename... Args>
  ::testing::AssertionResult AssertOperateSplitOp(bool splits, int rc, Args... args)
  {
    auto split_op_stat =  "objecter.split_op_reads"sv;
    int64_t before_count = get_perf_counter_by_path(split_op_stat);

    // Perform the I/O operation
    int ret = ioctx.operate(std::forward<Args>(args)...);
    if (ret != rc) {
      return ::testing::AssertionFailure()
             << "ioctx.operate() Incorrect rc " << rc << " != " << ret;
    }

    int64_t actual_count = get_perf_counter_by_path(split_op_stat);
    int64_t expected_count = before_count + ((splits && split_ops) ? 1 : 0);

    if (actual_count == expected_count) {
      return ::testing::AssertionSuccess();
    }

    return ::testing::AssertionFailure()
           << "Perf counter '" << split_op_stat << "' has incorrect value after operate().\n"
           << "       Expected: " << expected_count
           << " (before: " << before_count << ", split_ops: " << split_ops << ")\n"
           << "         Actual: " << actual_count;
  }

  template<typename... Args>
  ::testing::AssertionResult AssertOperateWithSplitOp(int rc, Args... args) {
    return AssertOperateSplitOp(true, rc, std::forward<Args>(args)...);
  }
  template<typename... Args>
  ::testing::AssertionResult AssertOperateWithoutSplitOp(int rc, Args... args) {
    return AssertOperateSplitOp(false, rc, std::forward<Args>(args)...);
  }
};

class RadosTestPP : public RadosTestPPBase, public ::testing::Test {
public:
  RadosTestPP(bool c=false) : cleanup(c) {}
  ~RadosTestPP() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static std::string pool_name;

  void SetUp() override;
  void TearDown() override;
  librados::IoCtx ioctx;
  bool cleanup;
  std::string nspace;
};

class RadosTestParamPP : public RadosTestPPBase,
                         public ::testing::TestWithParam<const char*> {
public:
  RadosTestParamPP(bool c=false) : cleanup(c) {}
  ~RadosTestParamPP() override {}
  static void SetUpTestCase();
  static void TearDownTestCase();
protected:
  static void cleanup_default_namespace(librados::IoCtx ioctx);
  static void cleanup_namespace(librados::IoCtx ioctx, std::string ns);
  static std::string pool_name;
  static std::string cache_pool_name;

  void SetUp() override;
  void TearDown() override;
  bool cleanup;
  std::string nspace;
};

class RadosTestECPP : public RadosTestPPBase,
                      public ::testing::TestWithParam<std::tuple<bool, bool>> {
  bool ec_overwrites_set = false;
public:
  RadosTestECPP(bool c=false) : cleanup(c) {}
  ~RadosTestECPP() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  void set_allow_ec_overwrites();
  static std::string pool_name_default;
  static std::string pool_name_fast;
  static std::string pool_name_fast_split;

  std::string pool_name;
  void SetUp() override;
  void TearDown() override;
  bool cleanup;
  std::string nspace;
  uint64_t alignment = 0;
  bool fast_ec = false;
};
