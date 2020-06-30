// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_types.h"
#include "test/rbd_mirror/test_fixture.h"
#include "include/stringify.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/internal.h"
#include "test/librados/test_cxx.h"
#include "tools/rbd_mirror/Threads.h"

namespace rbd {
namespace mirror {

std::string TestFixture::_local_pool_name;
std::string TestFixture::_remote_pool_name;
std::shared_ptr<librados::Rados> TestFixture::_rados;
uint64_t TestFixture::_image_number = 0;
std::string TestFixture::_data_pool;

TestFixture::TestFixture() {
}

void TestFixture::SetUpTestCase() {
  _rados = std::shared_ptr<librados::Rados>(new librados::Rados());
  ASSERT_EQ("", connect_cluster_pp(*_rados.get()));
  ASSERT_EQ(0, _rados->conf_set("rbd_cache", "false"));

  _local_pool_name = get_temp_pool_name("test-rbd-mirror-");
  ASSERT_EQ(0, _rados->pool_create(_local_pool_name.c_str()));

  librados::IoCtx local_ioctx;
  ASSERT_EQ(0, _rados->ioctx_create(_local_pool_name.c_str(), local_ioctx));
  local_ioctx.application_enable("rbd", true);

  _remote_pool_name = get_temp_pool_name("test-rbd-mirror-");
  ASSERT_EQ(0, _rados->pool_create(_remote_pool_name.c_str()));

  librados::IoCtx remote_ioctx;
  ASSERT_EQ(0, _rados->ioctx_create(_remote_pool_name.c_str(), remote_ioctx));
  remote_ioctx.application_enable("rbd", true);

  ASSERT_EQ(0, create_image_data_pool(_data_pool));
  if (!_data_pool.empty()) {
    printf("using image data pool: %s\n", _data_pool.c_str());
  }
}

void TestFixture::TearDownTestCase() {
  if (!_data_pool.empty()) {
    ASSERT_EQ(0, _rados->pool_delete(_data_pool.c_str()));
  }

  ASSERT_EQ(0, _rados->pool_delete(_remote_pool_name.c_str()));
  ASSERT_EQ(0, _rados->pool_delete(_local_pool_name.c_str()));
  _rados->shutdown();
}

void TestFixture::SetUp() {
  static bool seeded = false;
  if (!seeded) {
    seeded = true;
    int seed = getpid();
    cout << "seed " << seed << std::endl;
    srand(seed);
  }

  ASSERT_EQ(0, _rados->ioctx_create(_local_pool_name.c_str(), m_local_io_ctx));
  ASSERT_EQ(0, _rados->ioctx_create(_remote_pool_name.c_str(), m_remote_io_ctx));
  m_image_name = get_temp_image_name();

  m_threads = new rbd::mirror::Threads<>(reinterpret_cast<CephContext*>(
    m_local_io_ctx.cct()));
}

void TestFixture::TearDown() {
  for (auto image_ctx : m_image_ctxs) {
    image_ctx->state->close();
  }

  m_remote_io_ctx.close();
  m_local_io_ctx.close();

  delete m_threads;
}

int TestFixture::create_image(librbd::RBD &rbd, librados::IoCtx &ioctx,
                              const std::string &name, uint64_t size) {
  int order = 18;
  return rbd.create2(ioctx, name.c_str(), size,
                     (RBD_FEATURES_ALL & ~RBD_FEATURES_IMPLICIT_ENABLE),
                     &order);
}

int TestFixture::open_image(librados::IoCtx &io_ctx,
                            const std::string &image_name,
                            librbd::ImageCtx **image_ctx) {
  *image_ctx = new librbd::ImageCtx(image_name.c_str(), "", nullptr, io_ctx,
                                    false);
  m_image_ctxs.insert(*image_ctx);
  return (*image_ctx)->state->open(0);
}

int TestFixture::create_snap(librbd::ImageCtx *image_ctx, const char* snap_name,
                             librados::snap_t *snap_id) {
  librbd::NoOpProgressContext prog_ctx;
  int r = image_ctx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     snap_name, 0, prog_ctx);
  if (r < 0) {
    return r;
  }

  r = image_ctx->state->refresh();
  if (r < 0) {
    return r;
  }

  if (image_ctx->snap_ids.count({cls::rbd::UserSnapshotNamespace(),
				 snap_name}) == 0) {
    return -ENOENT;
  }

  if (snap_id != nullptr) {
    *snap_id = image_ctx->snap_ids[{cls::rbd::UserSnapshotNamespace(),
				    snap_name}];
  }
  return 0;
}

std::string TestFixture::get_temp_image_name() {
  ++_image_number;
  return "image" + stringify(_image_number);
}

int TestFixture::create_image_data_pool(std::string &data_pool) {
  std::string pool;
  int r = _rados->conf_get("rbd_default_data_pool", pool);
  if (r != 0) {
    return r;
  } else if (pool.empty()) {
    return 0;
  }

  r = _rados->pool_create(pool.c_str());
  if (r < 0) {
    return r;
  }

  librados::IoCtx data_ioctx;
  r = _rados->ioctx_create(pool.c_str(), data_ioctx);
  if (r < 0) {
    return r;
  }

  data_ioctx.application_enable("rbd", true);
  data_pool = pool;
  return 0;
}

} // namespace mirror
} // namespace rbd
