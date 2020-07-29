// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "include/stringify.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Operations.h"
#include "librbd/api/Io.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/internal.h"
#include "test/librados/test.h"
#include "test/librados/test_cxx.h"
#include <iostream>
#include <sstream>
#include <stdlib.h>

std::string TestFixture::_pool_name;
librados::Rados TestFixture::_rados;
rados_t TestFixture::_cluster;
uint64_t TestFixture::_image_number = 0;
std::string TestFixture::_data_pool;

TestFixture::TestFixture() : m_image_size(0) {
}

void TestFixture::SetUpTestCase() {
  ASSERT_EQ("", connect_cluster(&_cluster));
  _pool_name = get_temp_pool_name("test-librbd-");
  ASSERT_EQ("", create_one_pool_pp(_pool_name, _rados));

  bool created = false;
  ASSERT_EQ(0, create_image_data_pool(_rados, _data_pool, &created));
  if (!_data_pool.empty()) {
    printf("using image data pool: %s\n", _data_pool.c_str());
    if (!created) {
      _data_pool.clear();
    }
  }
}

void TestFixture::TearDownTestCase() {
  rados_shutdown(_cluster);
  if (!_data_pool.empty()) {
    ASSERT_EQ(0, _rados.pool_delete(_data_pool.c_str()));
  }

  ASSERT_EQ(0, destroy_one_pool_pp(_pool_name, _rados));
}

std::string TestFixture::get_temp_image_name() {
  ++_image_number;
  return "image" + stringify(_image_number);
}

void TestFixture::SetUp() {
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), m_ioctx));
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());

  m_image_name = get_temp_image_name();
  m_image_size = 2 << 20;
  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, m_image_name, m_image_size));
}

void TestFixture::TearDown() {
  unlock_image();
  for (std::set<librbd::ImageCtx *>::iterator iter = m_ictxs.begin();
       iter != m_ictxs.end(); ++iter) {
    (*iter)->state->close();
  }

  m_ioctx.close();
}

int TestFixture::open_image(const std::string &image_name,
			    librbd::ImageCtx **ictx) {
  *ictx = new librbd::ImageCtx(image_name.c_str(), "", nullptr, m_ioctx, false);
  m_ictxs.insert(*ictx);

  return (*ictx)->state->open(0);
}

int TestFixture::snap_create(librbd::ImageCtx &ictx,
                             const std::string &snap_name) {
  librbd::NoOpProgressContext prog_ctx;
  return ictx.operations->snap_create(cls::rbd::UserSnapshotNamespace(),
				      snap_name.c_str(), 0, prog_ctx);
}

int TestFixture::snap_protect(librbd::ImageCtx &ictx,
                              const std::string &snap_name) {
  return ictx.operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
                                       snap_name.c_str());
}

int TestFixture::flatten(librbd::ImageCtx &ictx,
                         librbd::ProgressContext &prog_ctx) {
  return ictx.operations->flatten(prog_ctx);
}

int TestFixture::resize(librbd::ImageCtx *ictx, uint64_t size){
  librbd::NoOpProgressContext prog_ctx;
  return ictx->operations->resize(size, true, prog_ctx);
}

void TestFixture::close_image(librbd::ImageCtx *ictx) {
  m_ictxs.erase(ictx);

  ictx->state->close();
}

int TestFixture::lock_image(librbd::ImageCtx &ictx, ClsLockType lock_type,
			    const std::string &cookie) {
  int r = rados::cls::lock::lock(&ictx.md_ctx, ictx.header_oid, RBD_LOCK_NAME,
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

int TestFixture::acquire_exclusive_lock(librbd::ImageCtx &ictx) {
  int r = librbd::api::Io<>::write(ictx, 0, 0, {}, 0);
  if (r != 0) {
    return r;
  }

  std::shared_lock owner_locker{ictx.owner_lock};
  ceph_assert(ictx.exclusive_lock != nullptr);
  return ictx.exclusive_lock->is_lock_owner() ? 0 : -EINVAL;
}
