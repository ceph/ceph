// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_fixture.h"
#include "include/stringify.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "test/librados/test.h"
#include "tools/rbd_mirror/Threads.h"

namespace rbd {
namespace mirror {

std::string TestFixture::_local_pool_name;
std::string TestFixture::_remote_pool_name;
std::shared_ptr<librados::Rados> TestFixture::_rados;
uint64_t TestFixture::_image_number = 0;

TestFixture::TestFixture() {
}

void TestFixture::SetUpTestCase() {
  _rados = std::shared_ptr<librados::Rados>(new librados::Rados());
  ASSERT_EQ("", connect_cluster_pp(*_rados.get()));
  ASSERT_EQ(0, _rados->conf_set("rbd_cache", "false"));

  _local_pool_name = get_temp_pool_name("test-rbd-mirror-");
  ASSERT_EQ(0, _rados->pool_create(_local_pool_name.c_str()));

  _remote_pool_name = get_temp_pool_name("test-rbd-mirror-");
  ASSERT_EQ(0, _rados->pool_create(_remote_pool_name.c_str()));
}

void TestFixture::TearDownTestCase() {
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

  m_threads = new rbd::mirror::Threads(reinterpret_cast<CephContext*>(
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
  return rbd.create2(ioctx, name.c_str(), size, RBD_FEATURES_ALL, &order);
}

int TestFixture::open_image(librados::IoCtx &io_ctx,
                            const std::string &image_name,
                            librbd::ImageCtx **image_ctx) {
  *image_ctx = new librbd::ImageCtx(image_name.c_str(), "", NULL, io_ctx,
                                    false);
  m_image_ctxs.insert(*image_ctx);
  return (*image_ctx)->state->open();
}

int TestFixture::create_snap(librbd::ImageCtx *image_ctx, const char* snap_name,
                             librados::snap_t *snap_id) {
  int r = image_ctx->operations->snap_create(snap_name);
  if (r < 0) {
    return r;
  }

  r = image_ctx->state->refresh();
  if (r < 0) {
    return r;
  }

  if (image_ctx->snap_ids.count(snap_name) == 0) {
    return -ENOENT;
  }

  if (snap_id != nullptr) {
    *snap_id = image_ctx->snap_ids[snap_name];
  }
  return 0;
}

std::string TestFixture::get_temp_image_name() {
  ++_image_number;
  return "image" + stringify(_image_number);
}

} // namespace mirror
} // namespace rbd
