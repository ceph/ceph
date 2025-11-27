// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "gtest/gtest.h"
#include "include/rados/librados.hpp"
#include "common/json/OSDStructures.h"

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
  static void cleanup_default_namespace(librados::IoCtx ioctx);
  static void cleanup_namespace(librados::IoCtx ioctx, std::string ns);
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

class RadosTestECPP : public RadosTestPPBase,
                      public ::testing::TestWithParam<bool> {
  bool ec_overwrites_set = false;
public:
  RadosTestECPP(bool c=false) : cluster(s_cluster), cleanup(c) {}
  ~RadosTestECPP() override {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static librados::Rados s_cluster;
  void set_allow_ec_overwrites();
  int freeze_omap_journal();
  int unfreeze_omap_journal();
  void write_omap_keys(
    std::string oid, 
    int min_index, 
    int max_index, 
    std::set<std::string> &keys_written
  );
  void read_omap_keys(
    std::string oid, 
    std::set<std::string> &keys_read
  );
  void check_returned_keys(
    std::list<std::pair<std::string, std::string>> expected_ranges,
    std::set<std::string> returned_keys
  );
  void remove_omap_range(
    std::string oid, 
    std::string start, 
    std::string end
  );
  int request_osd_map(
    std::string pool_name, 
    std::string oid, 
    std::string nspace, 
    ceph::messaging::osd::OSDMapReply* reply
  );
  int set_osd_upmap(
    std::string pgid,
    std::vector<int> up_osds
  );
  int wait_for_upmap(
    std::string pool_name,
    std::string oid,
    std::string nspace,
    int desired_primary,
    std::chrono::seconds timeout
  );
  void check_xattr_read(
    std::string oid,
    std::string xattr_key,
    std::string xattr_value,
    int expected_size,
    int expected_ret,
    int expected_err
  );
  void check_omap_read(
    std::string oid,
    std::string omap_key,
    std::string omap_value,
    int expected_size,
    int expected_err
  );
  void print_osd_map(std::string message, std::vector<int> osd_vec);
  static std::string pool_name_default;
  static std::string pool_name_fast;

  std::string pool_name;
  void SetUp() override;
  void TearDown() override;
  librados::Rados &cluster;
  librados::IoCtx ioctx;
  bool cleanup;
  std::string nspace;
  uint64_t alignment = 0;
};
