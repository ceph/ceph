// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "include/rbd_types.h"
#include "common/ceph_mutex.h"
#include "librbd/object_map/DiffRequest.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

#include "librbd/object_map/DiffRequest.cc"

using ::testing::_;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::StrEq;
using ::testing::WithArg;

namespace librbd {
namespace object_map {

static constexpr uint8_t from_beginning_table[][2] = {
  //        to                expected
  { OBJECT_NONEXISTENT,   DIFF_STATE_HOLE },
  { OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED }
};

static constexpr uint8_t from_beginning_intermediate_table[][4] = {
  //   intermediate               to             diff-iterate expected       deep-copy expected
  { OBJECT_NONEXISTENT,   OBJECT_NONEXISTENT,   DIFF_STATE_HOLE,          DIFF_STATE_HOLE },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_NONEXISTENT,   DIFF_STATE_HOLE,          DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_NONEXISTENT,   DIFF_STATE_HOLE,          DIFF_STATE_HOLE_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_NONEXISTENT,   DIFF_STATE_HOLE,          DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED }
};

static constexpr uint8_t from_snap_table[][3] = {
  //       from                   to                expected
  { OBJECT_NONEXISTENT,   OBJECT_NONEXISTENT,   DIFF_STATE_HOLE },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA },
  { OBJECT_PENDING,       OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA }
};

static constexpr uint8_t from_snap_intermediate_table[][5] = {
  //       from              intermediate               to             diff-iterate expected       deep-copy expected
  { OBJECT_NONEXISTENT,   OBJECT_NONEXISTENT,   OBJECT_NONEXISTENT,   DIFF_STATE_HOLE,          DIFF_STATE_HOLE },
  { OBJECT_NONEXISTENT,   OBJECT_NONEXISTENT,   OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_NONEXISTENT,   OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_NONEXISTENT,   OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS,        OBJECT_NONEXISTENT,   DIFF_STATE_HOLE,          DIFF_STATE_HOLE_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS,        OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS,        OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS,        OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_PENDING,       OBJECT_NONEXISTENT,   DIFF_STATE_HOLE,          DIFF_STATE_HOLE_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_PENDING,       OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_PENDING,       OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_PENDING,       OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS_CLEAN,  OBJECT_NONEXISTENT,   DIFF_STATE_HOLE,          DIFF_STATE_HOLE_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS_CLEAN,  OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_NONEXISTENT,   OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_NONEXISTENT,   OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS,        OBJECT_NONEXISTENT,   OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_NONEXISTENT,   OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_NONEXISTENT,   OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS,        OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS,        OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS,        OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS,        OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_PENDING,       OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS,        OBJECT_PENDING,       OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_PENDING,       OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_PENDING,       OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS_CLEAN,  OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS_CLEAN,  OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS,        OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA,          DIFF_STATE_DATA },
  { OBJECT_PENDING,       OBJECT_NONEXISTENT,   OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_PENDING,       OBJECT_NONEXISTENT,   OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_NONEXISTENT,   OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_NONEXISTENT,   OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS,        OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS,        OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS,        OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS,        OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_PENDING,       OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_PENDING,       OBJECT_PENDING,       OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_PENDING,       OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_PENDING,       OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS_CLEAN,  OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS_CLEAN,  OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_PENDING,       OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_NONEXISTENT,   OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_NONEXISTENT,   OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_NONEXISTENT,   OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_NONEXISTENT,   OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS,        OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS,        OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS,        OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS,        OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_PENDING,       OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_PENDING,       OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_PENDING,       OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_PENDING,       OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS_CLEAN,  OBJECT_NONEXISTENT,   DIFF_STATE_HOLE_UPDATED,  DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS,        DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS_CLEAN,  OBJECT_PENDING,       DIFF_STATE_DATA_UPDATED,  DIFF_STATE_DATA_UPDATED },
  { OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS_CLEAN,  OBJECT_EXISTS_CLEAN,  DIFF_STATE_DATA,          DIFF_STATE_DATA }
};

class TestMockObjectMapDiffRequest : public TestMockFixture,
                                     public ::testing::WithParamInterface<bool> {
public:
  typedef DiffRequest<MockTestImageCtx> MockDiffRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));
  }

  bool is_diff_iterate() const {
    return GetParam();
  }

  void expect_get_flags(MockTestImageCtx& mock_image_ctx, uint64_t snap_id,
                        int32_t flags, int r) {
    EXPECT_CALL(mock_image_ctx, get_flags(snap_id, _))
      .WillOnce(WithArg<1>(Invoke([flags, r](uint64_t *out_flags) {
        *out_flags = flags;
        return r;
      })));
  }

  template <typename Lambda>
  void expect_load_map(MockTestImageCtx& mock_image_ctx, uint64_t snap_id,
                       const BitVector<2>& object_map, int r,
                       Lambda&& lambda) {
    std::string snap_oid(ObjectMap<>::object_map_name(mock_image_ctx.id,
                                                      snap_id));
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(snap_oid, _, StrEq("rbd"), StrEq("object_map_load"), _,
                     _, _, _))
      .WillOnce(WithArg<5>(Invoke([object_map, r, lambda=std::move(lambda)]
                                  (bufferlist* out_bl) {
        lambda();

        auto out_object_map{object_map};
        out_object_map.set_crc_enabled(false);
        encode(out_object_map, *out_bl);
        return r;
      })));
  }

  void expect_load_map(MockTestImageCtx& mock_image_ctx, uint64_t snap_id,
                       const BitVector<2>& object_map, int r) {
    expect_load_map(mock_image_ctx, snap_id, object_map, r, [](){});
  }

  librbd::ImageCtx* m_image_ctx = nullptr;
  BitVector<2> m_object_diff_state;
};

TEST_P(TestMockObjectMapDiffRequest, InvalidStartSnap) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, CEPH_NOSNAP, 0,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_P(TestMockObjectMapDiffRequest, StartEndSnapEqual) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 1, 1, is_diff_iterate(),
                                 &m_object_diff_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0U, m_object_diff_state.size());
}

TEST_P(TestMockObjectMapDiffRequest, FastDiffDisabled) {
  // negative test -- object-map implicitly enables fast-diff
  REQUIRE(!is_feature_enabled(RBD_FEATURE_OBJECT_MAP));

  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_P(TestMockObjectMapDiffRequest, FastDiffInvalid) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, {}, {}, {}, {}, {}}}
  };

  InSequence seq;
  expect_get_flags(mock_image_ctx, 1U, RBD_FLAG_FAST_DIFF_INVALID, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_beginning_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  for (uint32_t i = 0; i < object_count; i++) {
    object_map_1[i] = from_beginning_table[i][0];
    expected_diff_state[i] = from_beginning_table[i][1];
  }

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);
  expect_load_map(mock_image_ctx, 1U, object_map_1, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, 1, is_diff_iterate(),
                                 &m_object_diff_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(expected_diff_state, m_object_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToSnapIntermediateSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_beginning_intermediate_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  for (uint32_t i = 0; i < object_count; i++) {
    object_map_1[i] = from_beginning_intermediate_table[i][0];
    object_map_2[i] = from_beginning_intermediate_table[i][1];
    if (is_diff_iterate()) {
      expected_diff_state[i] = from_beginning_intermediate_table[i][2];
    } else {
      expected_diff_state[i] = from_beginning_intermediate_table[i][3];
    }
  }

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);
  expect_load_map(mock_image_ctx, 1U, object_map_1, 0);

  expect_get_flags(mock_image_ctx, 2U, 0, 0);
  expect_load_map(mock_image_ctx, 2U, object_map_2, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, 2, is_diff_iterate(),
                                 &m_object_diff_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(expected_diff_state, m_object_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToHead) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_beginning_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  for (uint32_t i = 0; i < object_count; i++) {
    object_map_head[i] = from_beginning_table[i][0];
    expected_diff_state[i] = from_beginning_table[i][1];
  }

  InSequence seq;

  expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(expected_diff_state, m_object_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToHeadIntermediateSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_beginning_intermediate_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  for (uint32_t i = 0; i < object_count; i++) {
    object_map_1[i] = from_beginning_intermediate_table[i][0];
    object_map_head[i] = from_beginning_intermediate_table[i][1];
    if (is_diff_iterate()) {
      expected_diff_state[i] = from_beginning_intermediate_table[i][2];
    } else {
      expected_diff_state[i] = from_beginning_intermediate_table[i][3];
    }
  }

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);
  expect_load_map(mock_image_ctx, 1U, object_map_1, 0);

  expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(expected_diff_state, m_object_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_snap_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  for (uint32_t i = 0; i < object_count; i++) {
    object_map_1[i] = from_snap_table[i][0];
    object_map_2[i] = from_snap_table[i][1];
    expected_diff_state[i] = from_snap_table[i][2];
  }

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);
  expect_load_map(mock_image_ctx, 1U, object_map_1, 0);

  expect_get_flags(mock_image_ctx, 2U, 0, 0);
  expect_load_map(mock_image_ctx, 2U, object_map_2, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 1, 2, is_diff_iterate(),
                                 &m_object_diff_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(expected_diff_state, m_object_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToSnapIntermediateSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_snap_intermediate_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}},
    {3U, {"snap3", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count);
  BitVector<2> object_map_3;
  object_map_3.resize(object_count);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  for (uint32_t i = 0; i < object_count; i++) {
    object_map_1[i] = from_snap_intermediate_table[i][0];
    object_map_2[i] = from_snap_intermediate_table[i][1];
    object_map_3[i] = from_snap_intermediate_table[i][2];
    if (is_diff_iterate()) {
      expected_diff_state[i] = from_snap_intermediate_table[i][3];
    } else {
      expected_diff_state[i] = from_snap_intermediate_table[i][4];
    }
  }

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);
  expect_load_map(mock_image_ctx, 1U, object_map_1, 0);

  expect_get_flags(mock_image_ctx, 2U, 0, 0);
  expect_load_map(mock_image_ctx, 2U, object_map_2, 0);

  expect_get_flags(mock_image_ctx, 3U, 0, 0);
  expect_load_map(mock_image_ctx, 3U, object_map_3, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 1, 3, is_diff_iterate(),
                                 &m_object_diff_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(expected_diff_state, m_object_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToHead) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_snap_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  for (uint32_t i = 0; i < object_count; i++) {
    object_map_1[i] = from_snap_table[i][0];
    object_map_head[i] = from_snap_table[i][1];
    expected_diff_state[i] = from_snap_table[i][2];
  }

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);
  expect_load_map(mock_image_ctx, 1U, object_map_1, 0);

  expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 1, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(expected_diff_state, m_object_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToHeadIntermediateSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_snap_intermediate_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  for (uint32_t i = 0; i < object_count; i++) {
    object_map_1[i] = from_snap_intermediate_table[i][0];
    object_map_2[i] = from_snap_intermediate_table[i][1];
    object_map_head[i] = from_snap_intermediate_table[i][2];
    if (is_diff_iterate()) {
      expected_diff_state[i] = from_snap_intermediate_table[i][3];
    } else {
      expected_diff_state[i] = from_snap_intermediate_table[i][4];
    }
  }

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);
  expect_load_map(mock_image_ctx, 1U, object_map_1, 0);

  expect_get_flags(mock_image_ctx, 2U, 0, 0);
  expect_load_map(mock_image_ctx, 2U, object_map_2, 0);

  expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 1, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(expected_diff_state, m_object_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, StartSnapDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  InSequence seq;

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 1, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_P(TestMockObjectMapDiffRequest, EndSnapDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  expect_load_map(mock_image_ctx, 1U, object_map_1, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 1, 2, is_diff_iterate(),
                                 &m_object_diff_state, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateSnapDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  object_map_1[1] = OBJECT_EXISTS_CLEAN;
  expect_load_map(mock_image_ctx, 1U, object_map_1, 0,
                  [&mock_image_ctx]() { mock_image_ctx.snap_info.erase(2); });

  expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);

  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  object_map_head[1] = OBJECT_EXISTS_CLEAN;
  expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;
  ASSERT_EQ(expected_diff_state, m_object_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, LoadObjectMapDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);

  BitVector<2> object_map_head;
  expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, -ENOENT);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_P(TestMockObjectMapDiffRequest, LoadIntermediateObjectMapDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);

  BitVector<2> object_map_1;
  expect_load_map(mock_image_ctx, 1U, object_map_1, -ENOENT);

  expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);

  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  object_map_head[1] = OBJECT_EXISTS_CLEAN;
  expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;
  ASSERT_EQ(expected_diff_state, m_object_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, LoadObjectMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);

  BitVector<2> object_map_1;
  expect_load_map(mock_image_ctx, 1U, object_map_1, -EPERM);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_P(TestMockObjectMapDiffRequest, ObjectMapTooSmall) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  MockTestImageCtx mock_image_ctx(*m_image_ctx);
  mock_image_ctx.snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, mock_image_ctx.size, {},
          {}, {}, {}}}
  };

  InSequence seq;

  expect_get_flags(mock_image_ctx, 1U, 0, 0);

  BitVector<2> object_map_1;
  expect_load_map(mock_image_ctx, 1U, object_map_1, 0);

  C_SaferCond ctx;
  auto req = new MockDiffRequest(&mock_image_ctx, 0, CEPH_NOSNAP,
                                 is_diff_iterate(), &m_object_diff_state,
                                 &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

INSTANTIATE_TEST_SUITE_P(MockObjectMapDiffRequestTests,
                         TestMockObjectMapDiffRequest, ::testing::Bool());

} // namespace object_map
} // librbd
