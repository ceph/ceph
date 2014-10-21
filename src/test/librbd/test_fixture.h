// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/int_types.h"
#include "include/rados/librados.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "gtest/gtest.h"
#include <set>
#include <string>

using namespace ceph;

bool get_features(uint64_t *features);
bool is_feature_enabled(uint64_t feature);
int create_image_pp(librbd::RBD &rbd, librados::IoCtx &ioctx,
                    const std::string &name, uint64_t size);

#define REQUIRE_FEATURE(feature) { 	  \
  if (!is_feature_enabled(feature)) { 	  \
    std::cout << "SKIPPING" << std::endl; \
    return SUCCEED(); 			  \
  } 					  \
}

class TestFixture : public ::testing::Test {
public:

  TestFixture();

  static void SetUpTestCase();
  static void TearDownTestCase();

  static std::string get_temp_image_name();

  virtual void SetUp();
  virtual void TearDown();

  int open_image(const std::string &image_name, librbd::ImageCtx **ictx);
  void close_image(librbd::ImageCtx *ictx);

  int lock_image(librbd::ImageCtx &ictx, ClsLockType lock_type,
                 const std::string &cookie);
  int unlock_image();

  static std::string _pool_name;
  static librados::Rados _rados;
  static uint64_t _image_number;

  librados::IoCtx m_ioctx;
  librbd::RBD m_rbd;

  std::string m_image_name;
  uint64_t m_image_size;

  std::set<librbd::ImageCtx *> m_ictxs;

  std::string m_lock_object;
  std::string m_lock_cookie;
};
