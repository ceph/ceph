// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "gtest/gtest.h"
#include "include/rados/librados.hpp"

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
  void set_allow_ec_overwrites();
  static librados::Rados s_cluster;
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
