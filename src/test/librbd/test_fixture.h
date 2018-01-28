// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/int_types.h"
#include "include/rados/librados.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "gtest/gtest.h"
#include <set>
#include <string>

using namespace ceph;

class TestFixture : public ::testing::Test {
public:

  TestFixture();

  static void SetUpTestCase();
  static void TearDownTestCase();

  static std::string get_temp_image_name();

  void SetUp() override;
  void TearDown() override;

  int open_image(const std::string &image_name, librbd::ImageCtx **ictx);
  void close_image(librbd::ImageCtx *ictx);

  int snap_create(librbd::ImageCtx &ictx, const std::string &snap_name);
  int snap_protect(librbd::ImageCtx &ictx, const std::string &snap_name);

  int flatten(librbd::ImageCtx &ictx, librbd::ProgressContext &prog_ctx);
  int resize(librbd::ImageCtx *ictx, uint64_t size);

  int lock_image(librbd::ImageCtx &ictx, ClsLockType lock_type,
                 const std::string &cookie);
  int unlock_image();

  int acquire_exclusive_lock(librbd::ImageCtx &ictx);

  static std::string _pool_name;
  static librados::Rados _rados;
  static rados_t _cluster;
  static uint64_t _image_number;
  static std::string _data_pool;

  librados::IoCtx m_ioctx;
  librbd::RBD m_rbd;

  std::string m_image_name;
  uint64_t m_image_size;

  std::set<librbd::ImageCtx *> m_ictxs;

  std::string m_lock_object;
  std::string m_lock_cookie;
};
