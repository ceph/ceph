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
#include "librbd/object_map/LockRequest.h"

namespace librbd {

namespace {

struct MockObjectMapImageCtx : public MockImageCtx {
  MockObjectMapImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace object_map {

template <>
class LockRequest<MockObjectMapImageCtx> {
public:
  static LockRequest *s_instance;
  static LockRequest *create(MockObjectMapImageCtx &image_ctx, Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  LockRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct InvalidateRequest<MockObjectMapImageCtx> :
    public MockInvalidateRequestBase<MockObjectMapImageCtx> {
};

LockRequest<MockObjectMapImageCtx> *LockRequest<MockObjectMapImageCtx>::s_instance = nullptr;

} // namespace object_map
} // namespace librbd

// template definitions
#include "librbd/object_map/RefreshRequest.cc"
#include "librbd/object_map/LockRequest.cc"

namespace librbd {
namespace object_map {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockObjectMapRefreshRequest : public TestMockFixture {
public:
  static const uint64_t TEST_SNAP_ID = 123;

  typedef RefreshRequest<MockObjectMapImageCtx> MockRefreshRequest;
  typedef LockRequest<MockObjectMapImageCtx> MockLockRequest;
  typedef InvalidateRequest<MockObjectMapImageCtx> MockInvalidateRequest;

  void expect_object_map_lock(MockObjectMapImageCtx &mock_image_ctx,
                              MockLockRequest &mock_lock_request) {
    EXPECT_CALL(mock_lock_request, send())
                  .WillOnce(FinishRequest(&mock_lock_request, 0,
                                          &mock_image_ctx));
  }

  void expect_object_map_load(MockObjectMapImageCtx &mock_image_ctx,
                              ceph::BitVector<2> *object_map, uint64_t snap_id,
                              int r) {
    std::string oid(ObjectMap::object_map_name(mock_image_ctx.id, snap_id));
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(oid, _, StrEq("rbd"), StrEq("object_map_load"), _, _, _));
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

  void expect_get_image_size(MockObjectMapImageCtx &mock_image_ctx, uint64_t snap_id,
                             uint64_t size) {
    EXPECT_CALL(mock_image_ctx, get_image_size(snap_id))
                  .WillOnce(Return(size));
  }

  void expect_invalidate_request(MockObjectMapImageCtx &mock_image_ctx,
                                 MockInvalidateRequest &invalidate_request) {
    EXPECT_CALL(invalidate_request, send())
                  .WillOnce(FinishRequest(&invalidate_request, 0,
                                          &mock_image_ctx));
  }

  void expect_truncate_request(MockObjectMapImageCtx &mock_image_ctx) {
    std::string oid(ObjectMap::object_map_name(mock_image_ctx.id, TEST_SNAP_ID));
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx), truncate(oid, 0, _))
                  .WillOnce(Return(0));
  }

  void expect_object_map_resize(MockObjectMapImageCtx &mock_image_ctx,
                                uint64_t num_objects, int r) {
    std::string oid(ObjectMap::object_map_name(mock_image_ctx.id, TEST_SNAP_ID));
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(oid, _, StrEq("rbd"), StrEq("object_map_resize"), _, _, _));
    expect.WillOnce(Return(r));
  }

  void init_object_map(MockObjectMapImageCtx &mock_image_ctx,
                       ceph::BitVector<2> *object_map) {
    uint64_t num_objs = Striper::get_num_objects(
      mock_image_ctx.layout, mock_image_ctx.image_ctx->size);
    object_map->resize(num_objs);
    for (uint64_t i = 0; i < num_objs; ++i) {
      (*object_map)[i] = rand() % 3;
    }
  }
};

TEST_F(TestMockObjectMapRefreshRequest, SuccessHead) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockObjectMapImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockLockRequest mock_lock_request;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   CEPH_NOSNAP, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, CEPH_NOSNAP,
                        mock_image_ctx.image_ctx->size);
  expect_object_map_lock(mock_image_ctx, mock_lock_request);
  expect_object_map_load(mock_image_ctx, &on_disk_object_map, CEPH_NOSNAP, 0);
  expect_get_image_size(mock_image_ctx, CEPH_NOSNAP,
                        mock_image_ctx.image_ctx->size);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(on_disk_object_map, object_map);
}

TEST_F(TestMockObjectMapRefreshRequest, SuccessSnapshot) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockObjectMapImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, &on_disk_object_map, TEST_SNAP_ID, 0);
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(on_disk_object_map, object_map);
}

TEST_F(TestMockObjectMapRefreshRequest, LoadError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockObjectMapImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, nullptr, TEST_SNAP_ID, -ENOENT);

  MockInvalidateRequest invalidate_request;
  expect_invalidate_request(mock_image_ctx, invalidate_request);
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);

  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapRefreshRequest, LoadCorrupt) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockObjectMapImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, nullptr, TEST_SNAP_ID, -EINVAL);

  MockInvalidateRequest invalidate_request;
  expect_invalidate_request(mock_image_ctx, invalidate_request);
  expect_truncate_request(mock_image_ctx);
  expect_object_map_resize(mock_image_ctx, on_disk_object_map.size(), 0);
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);

  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapRefreshRequest, TooSmall) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockObjectMapImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  ceph::BitVector<2> small_object_map;

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, &small_object_map, TEST_SNAP_ID, 0);

  MockInvalidateRequest invalidate_request;
  expect_invalidate_request(mock_image_ctx, invalidate_request);
  expect_object_map_resize(mock_image_ctx, on_disk_object_map.size(), 0);
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);

  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapRefreshRequest, TooLarge) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockObjectMapImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  ceph::BitVector<2> large_object_map;
  large_object_map.resize(on_disk_object_map.size() * 2);

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, &large_object_map, TEST_SNAP_ID, 0);
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapRefreshRequest, ResizeError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockObjectMapImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  ceph::BitVector<2> small_object_map;

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);
  expect_object_map_load(mock_image_ctx, &small_object_map, TEST_SNAP_ID, 0);

  MockInvalidateRequest invalidate_request;
  expect_invalidate_request(mock_image_ctx, invalidate_request);
  expect_object_map_resize(mock_image_ctx, on_disk_object_map.size(), -ESTALE);
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        mock_image_ctx.image_ctx->size);

  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapRefreshRequest, LargeImageError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockObjectMapImageCtx mock_image_ctx(*ictx);

  ceph::BitVector<2> on_disk_object_map;
  init_object_map(mock_image_ctx, &on_disk_object_map);

  C_SaferCond ctx;
  ceph::BitVector<2> object_map;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &object_map,
                                                   TEST_SNAP_ID, &ctx);

  InSequence seq;
  expect_get_image_size(mock_image_ctx, TEST_SNAP_ID,
                        std::numeric_limits<int64_t>::max());

  MockInvalidateRequest invalidate_request;
  expect_invalidate_request(mock_image_ctx, invalidate_request);

  req->send();
  ASSERT_EQ(-EFBIG, ctx.wait());
}

} // namespace object_map
} // namespace librbd

