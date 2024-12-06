// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/object_map/RefreshRequest.h"
#include "librbd/object_map/UnlockRequest.h"
#include "librbd/object_map/UpdateRequest.h"
#include <boost/scope_exit.hpp>

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace object_map {

template <>
struct RefreshRequest<MockTestImageCtx> {
  Context *on_finish = nullptr;
  ceph::BitVector<2u> *object_map = nullptr;
  static RefreshRequest *s_instance;
  static RefreshRequest *create(MockTestImageCtx &image_ctx, ceph::shared_mutex*,
                                ceph::BitVector<2u> *object_map,
                                uint64_t snap_id, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    s_instance->object_map = object_map;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  RefreshRequest() {
    s_instance = this;
  }
};

template <>
struct UnlockRequest<MockTestImageCtx> {
  Context *on_finish = nullptr;
  static UnlockRequest *s_instance;
  static UnlockRequest *create(MockTestImageCtx &image_ctx,
                               Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());
  UnlockRequest() {
    s_instance = this;
  }
};

template <>
struct UpdateRequest<MockTestImageCtx> {
  Context *on_finish = nullptr;
  static UpdateRequest *s_instance;
  static UpdateRequest *create(MockTestImageCtx &image_ctx, ceph::shared_mutex*,
                               ceph::BitVector<2u> *object_map,
                               uint64_t snap_id,
                               uint64_t start_object_no, uint64_t end_object_no,
                               uint8_t new_state,
                               const boost::optional<uint8_t> &current_state,
                               const ZTracer::Trace &parent_trace,
                               bool ignore_enoent, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    s_instance->construct(snap_id, start_object_no, end_object_no, new_state,
                          current_state, ignore_enoent);
    return s_instance;
  }

  MOCK_METHOD6(construct, void(uint64_t snap_id, uint64_t start_object_no,
                               uint64_t end_object_no, uint8_t new_state,
                               const boost::optional<uint8_t> &current_state,
                               bool ignore_enoent));
  MOCK_METHOD0(send, void());
  UpdateRequest() {
    s_instance = this;
  }
};

RefreshRequest<MockTestImageCtx> *RefreshRequest<MockTestImageCtx>::s_instance = nullptr;
UnlockRequest<MockTestImageCtx> *UnlockRequest<MockTestImageCtx>::s_instance = nullptr;
UpdateRequest<MockTestImageCtx> *UpdateRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace object_map
} // namespace librbd

#include "librbd/ObjectMap.cc"

namespace librbd {

using testing::_;
using testing::InSequence;
using testing::Invoke;

class TestMockObjectMap : public TestMockFixture {
public:
  typedef ObjectMap<MockTestImageCtx> MockObjectMap;
  typedef object_map::RefreshRequest<MockTestImageCtx> MockRefreshRequest;
  typedef object_map::UnlockRequest<MockTestImageCtx> MockUnlockRequest;
  typedef object_map::UpdateRequest<MockTestImageCtx> MockUpdateRequest;

  void expect_refresh(MockTestImageCtx &mock_image_ctx,
                      MockRefreshRequest &mock_refresh_request,
                      const ceph::BitVector<2u> &object_map, int r) {
    EXPECT_CALL(mock_refresh_request, send())
      .WillOnce(Invoke([&mock_image_ctx, &mock_refresh_request, &object_map, r]() {
          *mock_refresh_request.object_map = object_map;
          mock_image_ctx.image_ctx->op_work_queue->queue(mock_refresh_request.on_finish, r);
        }));
  }

  void expect_unlock(MockTestImageCtx &mock_image_ctx,
                     MockUnlockRequest &mock_unlock_request, int r) {
    EXPECT_CALL(mock_unlock_request, send())
      .WillOnce(Invoke([&mock_image_ctx, &mock_unlock_request, r]() {
          mock_image_ctx.image_ctx->op_work_queue->queue(mock_unlock_request.on_finish, r);
        }));
  }

  void expect_update(MockTestImageCtx &mock_image_ctx,
                     MockUpdateRequest &mock_update_request,
                     uint64_t snap_id, uint64_t start_object_no,
                     uint64_t end_object_no, uint8_t new_state,
                     const boost::optional<uint8_t> &current_state,
                     bool ignore_enoent, Context **on_finish) {
    EXPECT_CALL(mock_update_request, construct(snap_id, start_object_no,
                                               end_object_no, new_state,
                                               current_state, ignore_enoent))
      .Times(1);
    EXPECT_CALL(mock_update_request, send())
      .WillOnce(Invoke([&mock_update_request, on_finish]() {
          *on_finish = mock_update_request.on_finish;
        }));
  }

};

TEST_F(TestMockObjectMap, NonDetainedUpdate) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  ceph::BitVector<2u> object_map;
  object_map.resize(4);
  MockRefreshRequest mock_refresh_request;
  expect_refresh(mock_image_ctx, mock_refresh_request, object_map, 0);

  MockUpdateRequest mock_update_request;
  Context *finish_update_1;
  expect_update(mock_image_ctx, mock_update_request, CEPH_NOSNAP,
                0, 1, 1, {}, false, &finish_update_1);
  Context *finish_update_2;
  expect_update(mock_image_ctx, mock_update_request, CEPH_NOSNAP,
                1, 2, 1, {}, false, &finish_update_2);

  MockUnlockRequest mock_unlock_request;
  expect_unlock(mock_image_ctx, mock_unlock_request, 0);

  MockObjectMap *mock_object_map = new MockObjectMap(mock_image_ctx, CEPH_NOSNAP);
  BOOST_SCOPE_EXIT(&mock_object_map) {
    mock_object_map->put();
  } BOOST_SCOPE_EXIT_END

  C_SaferCond open_ctx;
  mock_object_map->open(&open_ctx);
  ASSERT_EQ(0, open_ctx.wait());

  C_SaferCond update_ctx1;
  C_SaferCond update_ctx2;
  {
    std::shared_lock image_locker{mock_image_ctx.image_lock};
    mock_object_map->aio_update(CEPH_NOSNAP, 0, 1, {}, {}, false, false, &update_ctx1);
    mock_object_map->aio_update(CEPH_NOSNAP, 1, 1, {}, {}, false, false, &update_ctx2);
  }

  finish_update_2->complete(0);
  ASSERT_EQ(0, update_ctx2.wait());

  finish_update_1->complete(0);
  ASSERT_EQ(0, update_ctx1.wait());

  C_SaferCond close_ctx;
  mock_object_map->close(&close_ctx);
  ASSERT_EQ(0, close_ctx.wait());
}

TEST_F(TestMockObjectMap, DetainedUpdate) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  ceph::BitVector<2u> object_map;
  object_map.resize(4);
  MockRefreshRequest mock_refresh_request;
  expect_refresh(mock_image_ctx, mock_refresh_request, object_map, 0);

  MockUpdateRequest mock_update_request;
  Context *finish_update_1;
  expect_update(mock_image_ctx, mock_update_request, CEPH_NOSNAP,
                1, 4, 1, {}, false, &finish_update_1);
  Context *finish_update_2 = nullptr;
  expect_update(mock_image_ctx, mock_update_request, CEPH_NOSNAP,
                1, 3, 1, {}, false, &finish_update_2);
  Context *finish_update_3 = nullptr;
  expect_update(mock_image_ctx, mock_update_request, CEPH_NOSNAP,
                2, 3, 1, {}, false, &finish_update_3);
  Context *finish_update_4 = nullptr;
  expect_update(mock_image_ctx, mock_update_request, CEPH_NOSNAP,
                0, 2, 1, {}, false, &finish_update_4);

  MockUnlockRequest mock_unlock_request;
  expect_unlock(mock_image_ctx, mock_unlock_request, 0);

  MockObjectMap *mock_object_map = new MockObjectMap(mock_image_ctx, CEPH_NOSNAP);
  BOOST_SCOPE_EXIT(&mock_object_map) {
    mock_object_map->put();
  } BOOST_SCOPE_EXIT_END

  C_SaferCond open_ctx;
  mock_object_map->open(&open_ctx);
  ASSERT_EQ(0, open_ctx.wait());

  C_SaferCond update_ctx1;
  C_SaferCond update_ctx2;
  C_SaferCond update_ctx3;
  C_SaferCond update_ctx4;
  {
    std::shared_lock image_locker{mock_image_ctx.image_lock};
    mock_object_map->aio_update(CEPH_NOSNAP, 1, 4, 1, {}, {}, false,
                               false, &update_ctx1);
    mock_object_map->aio_update(CEPH_NOSNAP, 1, 3, 1, {}, {}, false,
                               false, &update_ctx2);
    mock_object_map->aio_update(CEPH_NOSNAP, 2, 3, 1, {}, {}, false,
                               false, &update_ctx3);
    mock_object_map->aio_update(CEPH_NOSNAP, 0, 2, 1, {}, {}, false,
                               false, &update_ctx4);
  }

  // updates 2, 3, and 4 are blocked on update 1
  ASSERT_EQ(nullptr, finish_update_2);
  finish_update_1->complete(0);
  ASSERT_EQ(0, update_ctx1.wait());

  // updates 3 and 4 are blocked on update 2
  ASSERT_NE(nullptr, finish_update_2);
  ASSERT_EQ(nullptr, finish_update_3);
  ASSERT_EQ(nullptr, finish_update_4);
  finish_update_2->complete(0);
  ASSERT_EQ(0, update_ctx2.wait());

  ASSERT_NE(nullptr, finish_update_3);
  ASSERT_NE(nullptr, finish_update_4);
  finish_update_3->complete(0);
  finish_update_4->complete(0);
  ASSERT_EQ(0, update_ctx3.wait());
  ASSERT_EQ(0, update_ctx4.wait());

  C_SaferCond close_ctx;
  mock_object_map->close(&close_ctx);
  ASSERT_EQ(0, close_ctx.wait());
}

} // namespace librbd

