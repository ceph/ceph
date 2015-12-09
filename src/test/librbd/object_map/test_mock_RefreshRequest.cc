// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/object_map/mock/MockInvalidateRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/ObjectMap.h"
#include "librbd/object_map/RefreshRequest.h"

// template definitions
#include "librbd/object_map/RefreshRequest.cc"

namespace librbd {
namespace object_map {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::WithArg;

class TestMockObjectMapRefreshRequest : public TestMockFixture {
public:
  static const uint64_t TEST_SNAP_ID = 123;

  typedef RefreshRequest<MockImageCtx> MockRefreshRequest;

  void expect_object_map_load(MockImageCtx &mock_image_ctx,
                              ceph::BitVector<2> *object_map, int r) {
    std::string oid(ObjectMap::object_map_name(mock_image_ctx.id, TEST_SNAP_ID));
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(oid, _, "rbd", "object_map_load", _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      assert(object_map);
      object_map->set_crc_enabled(false);

      bufferlist bl;
      ::encode(*object_map, bl);

      std::string str(bl.c_str(), bl.length());
      expect.WillOnce(DoAll(WithArg<5>(CopyInBufferlist(str)), Return(0)));
    }
  }

  void expect_get_image_size(MockImageCtx &mock_image_ctx, uint64_t size) {
    EXPECT_CALL(mock_image_ctx, get_image_size(TEST_SNAP_ID))
                  .WillOnce(Return(size));
  }

  void expect_invalidate_request(MockImageCtx &mock_image_ctx,
                                 MockInvalidateRequest &invalidate_request) {
    EXPECT_CALL(invalidate_request, send())
                  .WillOnce(FinishRequest(&invalidate_request, 0,
                                          &mock_image_ctx));
  }

  void expect_truncate_request(MockImageCtx &mock_image_ctx) {
    std::string oid(ObjectMap::object_map_name(mock_image_ctx.id, TEST_SNAP_ID));
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx), truncate(oid, 0, _))
                  .WillOnce(Return(0));
  }

  void expect_object_map_resize(MockImageCtx &mock_image_ctx,
                                uint64_t num_objects, int r) {
    std::string oid(ObjectMap::object_map_name(mock_image_ctx.id, TEST_SNAP_ID));
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(oid, _, "rbd", "object_map_resize", _, _, _));
    expect.WillOnce(Return(r));
  }

  void init_object_map(MockImageCtx &mock_image_ctx,
                       ceph::BitVector<2> *object_map) {
    uint64_t num_objs = Striper::get_num_objects(
      mock_image_ctx.layout, mock_image_ctx.image_ctx->size);
    object_map->resize(num_objs);
    for (uint64_t i = 0; i < num_objs; ++i) {
      (*object_map)[i] = rand() % 3;
    }
  }
};

TEST_F(TestMockObjectMapRefreshRequest, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, &on_disk_object_map, 0);
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(on_disk_object_map, object_map);
}

TEST_F(TestMockObjectMapRefreshRequest, LoadError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, nullptr, -ENOENT);

  MockInvalidateRequest invalidate_request;
  expect_invalidate_request(mock_image_ctx, invalidate_request);
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);

  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapRefreshRequest, LoadCorrupt) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, nullptr, -EINVAL);

  MockInvalidateRequest invalidate_request;
  expect_invalidate_request(mock_image_ctx, invalidate_request);
  expect_truncate_request(mock_image_ctx);
  expect_object_map_resize(mock_image_ctx, on_disk_object_map.size(), 0);
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);

  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapRefreshRequest, TooSmall) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  ceph::BitVector<2> small_object_map;

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, &small_object_map, 0);

  MockInvalidateRequest invalidate_request;
  expect_invalidate_request(mock_image_ctx, invalidate_request);
  expect_object_map_resize(mock_image_ctx, on_disk_object_map.size(), 0);
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);

  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapRefreshRequest, TooLarge) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  ceph::BitVector<2> large_object_map;
  large_object_map.resize(on_disk_object_map.size() * 2);

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, &large_object_map, 0);
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapRefreshRequest, ResizeError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  ceph::BitVector<2> small_object_map;

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, &small_object_map, 0);

  MockInvalidateRequest invalidate_request;
  expect_invalidate_request(mock_image_ctx, invalidate_request);
  expect_object_map_resize(mock_image_ctx, on_disk_object_map.size(), -ESTALE);
  expect_get_image_size(mock_image_ctx, mock_image_ctx.image_ctx->size);

  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace object_map
} // namespace librbd

