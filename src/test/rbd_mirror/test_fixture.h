// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_RBD_MIRROR_TEST_FIXTURE_H
#define CEPH_TEST_RBD_MIRROR_TEST_FIXTURE_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <set>

namespace librbd {
class ImageCtx;
class RBD;
}

namespace rbd {
namespace mirror {

template <typename> class Threads;

class TestFixture : public ::testing::Test {
public:
  TestFixture();

  static void SetUpTestCase();
  static void TearDownTestCase();

  void SetUp() override;
  void TearDown() override;

  librados::IoCtx m_local_io_ctx;
  librados::IoCtx m_remote_io_ctx;

  std::string m_image_name;
  uint64_t m_image_size = 1 << 24;

  std::set<librbd::ImageCtx *> m_image_ctxs;

  Threads<librbd::ImageCtx> *m_threads = nullptr;


  int create_image(librbd::RBD &rbd, librados::IoCtx &ioctx,
                   const std::string &name, uint64_t size);
  int open_image(librados::IoCtx &io_ctx, const std::string &image_name,
                 librbd::ImageCtx **image_ctx);

  int create_snap(librbd::ImageCtx *image_ctx, const char* snap_name,
                  librados::snap_t *snap_id = nullptr);

  static std::string get_temp_image_name();
  static int create_image_data_pool(std::string &data_pool);

  static std::string _local_pool_name;
  static std::string _remote_pool_name;
  static std::shared_ptr<librados::Rados> _rados;
  static uint64_t _image_number;
  static std::string _data_pool;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_TEST_RBD_MIRROR_TEST_FIXTURE_H
