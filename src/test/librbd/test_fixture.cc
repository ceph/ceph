// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "include/stringify.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/internal.h"
#include "test/librados/test.h"
#include <iostream>
#include <sstream>
#include <stdlib.h>

bool get_features(uint64_t *features) {
  const char *c = getenv("RBD_FEATURES");
  if (c == NULL) {
    return false;
  }

  std::stringstream ss(c);
  if (!(ss >> *features)) {
    return false;
  }
  return true;
}

bool is_feature_enabled(uint64_t feature) {
  uint64_t features;
  return (get_features(&features) && (features & feature) == feature);
}

int create_image_pp(librbd::RBD &rbd, librados::IoCtx &ioctx,
                    const std::string &name, uint64_t size) {
  uint64_t features = 0;
  get_features(&features);
  int order = 0;
  return rbd.create2(ioctx, name.c_str(), size, features, &order);
}

std::string TestFixture::_pool_name;
librados::Rados TestFixture::_rados;
uint64_t TestFixture::_image_number = 0;

TestFixture::TestFixture() {
}

void TestFixture::SetUpTestCase() {
  _pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(_pool_name, _rados));
}

void TestFixture::TearDownTestCase() {
  ASSERT_EQ(0, destroy_one_pool_pp(_pool_name, _rados));
}

std::string TestFixture::get_temp_image_name() {
  ++_image_number;
  return "image" + stringify(_image_number);
}

void TestFixture::SetUp() {
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), m_ioctx));

  m_image_name = get_temp_image_name();
  m_image_size = 2 << 20;
  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, m_image_name, m_image_size));
}

void TestFixture::TearDown() {
  unlock_image();
  for (std::set<librbd::ImageCtx *>::iterator iter = m_ictxs.begin();
       iter != m_ictxs.end(); ++iter) {
    librbd::close_image(*iter);
  }

  m_ioctx.close();
}

int TestFixture::open_image(const std::string &image_name,
			    librbd::ImageCtx **ictx) {
  *ictx = new librbd::ImageCtx(image_name.c_str(), "", NULL, m_ioctx, false);
  m_ictxs.insert(*ictx);
  return librbd::open_image(*ictx);
}

void TestFixture::close_image(librbd::ImageCtx *ictx) {
  m_ictxs.erase(ictx);
  librbd::close_image(ictx);
}

int TestFixture::lock_image(librbd::ImageCtx &ictx, ClsLockType lock_type,
			    const std::string &cookie) {
  int r = rados::cls::lock::lock(&m_ioctx, ictx.header_oid, RBD_LOCK_NAME,
      			   lock_type, cookie, "internal", "", utime_t(),
      			   0);
  if (r == 0) {
    m_lock_object = ictx.header_oid;
    m_lock_cookie = cookie;
  }
  return r;
}

int TestFixture::unlock_image() {
  int r = 0;
  if (!m_lock_cookie.empty()) {
    r = rados::cls::lock::unlock(&m_ioctx, m_lock_object, RBD_LOCK_NAME,
      			   m_lock_cookie);
    m_lock_cookie = "";
  }
  return r;
}
