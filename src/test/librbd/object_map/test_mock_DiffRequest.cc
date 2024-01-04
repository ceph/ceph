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

void noop(MockTestImageCtx&) {}

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

static constexpr uint8_t shrink_table[][2] = {
  //      shrunk             deep-copy expected
  { OBJECT_NONEXISTENT,   DIFF_STATE_HOLE },
  { OBJECT_EXISTS,        DIFF_STATE_HOLE_UPDATED },
  { OBJECT_PENDING,       DIFF_STATE_HOLE_UPDATED },
  { OBJECT_EXISTS_CLEAN,  DIFF_STATE_HOLE_UPDATED }
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
    return !GetParam();
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

  template <typename F>
  int do_diff(F&& f, uint64_t start_snap_id, uint64_t end_snap_id,
              uint64_t start_object_no, uint64_t end_object_no) {
    InSequence seq;

    MockTestImageCtx mock_image_ctx(*m_image_ctx);
    std::forward<F>(f)(mock_image_ctx);

    C_SaferCond ctx;
    auto req = new MockDiffRequest(&mock_image_ctx, start_snap_id,
                                   end_snap_id, start_object_no, end_object_no,
                                   &m_diff_state, &ctx);
    req->send();
    return ctx.wait();
  }

  template <typename F>
  void test_diff_iterate(F&& f, uint64_t start_snap_id, uint64_t end_snap_id,
                         const BitVector<2>& expected_diff_state) {
    // ranged -- run through all ranges (substrings) in expected_diff_state
    for (uint64_t i = 0; i < expected_diff_state.size(); i++) {
      for (uint64_t j = i + 1; j <= expected_diff_state.size(); j++) {
        ASSERT_EQ(0, do_diff(std::forward<F>(f), start_snap_id, end_snap_id,
                             i, j));
        ASSERT_EQ(j - i, m_diff_state.size());
        for (uint64_t k = 0; k < m_diff_state.size(); k++) {
          ASSERT_EQ(expected_diff_state[i + k], m_diff_state[k]);
        }
      }
    }

    // unranged -- equivalent to i=0, j=expected_diff_state.size() range
    ASSERT_EQ(0, do_diff(std::forward<F>(f), start_snap_id, end_snap_id,
                         0, UINT64_MAX - 1));
    ASSERT_EQ(expected_diff_state, m_diff_state);
  }

  template <typename F>
  void test_deep_copy(F&& f, uint64_t start_snap_id, uint64_t end_snap_id,
                      const BitVector<2>& expected_diff_state) {
    ASSERT_EQ(0, do_diff(std::forward<F>(f), start_snap_id, end_snap_id,
                         0, UINT64_MAX));
    ASSERT_EQ(expected_diff_state, m_diff_state);
  }

  librbd::ImageCtx* m_image_ctx = nullptr;
  BitVector<2> m_diff_state;
};

TEST_P(TestMockObjectMapDiffRequest, InvalidStartSnap) {
  if (is_diff_iterate()) {
    ASSERT_EQ(-EINVAL, do_diff(noop, CEPH_NOSNAP, CEPH_NOSNAP, 123, 456));
  } else {
    ASSERT_EQ(-EINVAL, do_diff(noop, CEPH_NOSNAP, CEPH_NOSNAP, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, InvalidEndSnap) {
  if (is_diff_iterate()) {
    ASSERT_EQ(-EINVAL, do_diff(noop, 2, 1, 123, 456));
  } else {
    ASSERT_EQ(-EINVAL, do_diff(noop, 2, 1, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, StartEndSnapEqual) {
  BitVector<2> expected_diff_state;

  if (is_diff_iterate()) {
    ASSERT_EQ(0, do_diff(noop, 1, 1, 123, 456));
  } else {
    ASSERT_EQ(0, do_diff(noop, 1, 1, 0, UINT64_MAX));
  }
  ASSERT_EQ(expected_diff_state, m_diff_state);
}

TEST_P(TestMockObjectMapDiffRequest, InvalidStartObject) {
  if (is_diff_iterate()) {
    ASSERT_EQ(-EINVAL, do_diff(noop, 0, 1, UINT64_MAX, UINT64_MAX));
  } else {
    ASSERT_EQ(-EINVAL, do_diff(noop, 0, 1, 123, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, InvalidEndObject) {
  if (is_diff_iterate()) {
    ASSERT_EQ(-EINVAL, do_diff(noop, 0, 1, 456, 123));
  } else {
    SUCCEED();
  }
}

TEST_P(TestMockObjectMapDiffRequest, StartEndObjectEqual) {
  BitVector<2> expected_diff_state;

  if (is_diff_iterate()) {
    ASSERT_EQ(0, do_diff(noop, 0, 1, 123, 123));
    ASSERT_EQ(expected_diff_state, m_diff_state);
  } else {
    SUCCEED();
  }
}

TEST_P(TestMockObjectMapDiffRequest, FastDiffDisabled) {
  // negative test -- object-map implicitly enables fast-diff
  REQUIRE(!is_feature_enabled(RBD_FEATURE_OBJECT_MAP));

  if (is_diff_iterate()) {
    ASSERT_EQ(-EINVAL, do_diff(noop, 0, CEPH_NOSNAP, 123, 456));
  } else {
    ASSERT_EQ(-EINVAL, do_diff(noop, 0, CEPH_NOSNAP, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_beginning_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
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

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 0, 1, expected_diff_state);
  } else {
    test_deep_copy(load, 0, 1, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToSnapEmpty) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  m_image_ctx->size = 0;
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, {}, {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  BitVector<2> expected_diff_state;

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 0, 1, expected_diff_state);
  } else {
    test_deep_copy(load, 0, 1, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToSnapIntermediateSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_beginning_intermediate_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
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

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    test_diff_iterate(load, 0, 2, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    test_deep_copy(load, 0, 2, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToSnapIntermediateSnapGrow) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_1 = std::size(from_beginning_intermediate_table);
  uint32_t object_count_2 = object_count_1 + std::size(from_beginning_table);
  m_image_ctx->size = object_count_2 * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}},
          object_count_2 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count_2);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count_2);
  for (uint32_t i = 0; i < object_count_1; i++) {
    object_map_1[i] = from_beginning_intermediate_table[i][0];
    object_map_2[i] = from_beginning_intermediate_table[i][1];
    if (is_diff_iterate()) {
      expected_diff_state[i] = from_beginning_intermediate_table[i][2];
    } else {
      expected_diff_state[i] = from_beginning_intermediate_table[i][3];
    }
  }
  for (uint32_t i = object_count_1; i < object_count_2; i++) {
    object_map_2[i] = from_beginning_table[i - object_count_1][0];
    expected_diff_state[i] = from_beginning_table[i - object_count_1][1];
  }

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    test_diff_iterate(load, 0, 2, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    test_deep_copy(load, 0, 2, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToSnapIntermediateSnapGrowFromZero) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_2 = std::size(from_beginning_table);
  m_image_ctx->size = object_count_2 * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, {}, {}, {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}},
          object_count_2 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  BitVector<2> object_map_2;
  object_map_2.resize(object_count_2);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count_2);
  for (uint32_t i = 0; i < object_count_2; i++) {
    object_map_2[i] = from_beginning_table[i][0];
    expected_diff_state[i] = from_beginning_table[i][1];
  }

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    test_diff_iterate(load, 0, 2, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    test_deep_copy(load, 0, 2, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToSnapIntermediateSnapShrink) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_2 = std::size(from_beginning_intermediate_table);
  uint32_t object_count_1 = object_count_2 + std::size(shrink_table);
  m_image_ctx->size = object_count_2 * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}},
          object_count_2 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count_2);
  BitVector<2> expected_diff_state;
  if (is_diff_iterate()) {
    expected_diff_state.resize(object_count_2);
  } else {
    expected_diff_state.resize(object_count_1);
  }
  for (uint32_t i = 0; i < object_count_2; i++) {
    object_map_1[i] = from_beginning_intermediate_table[i][0];
    object_map_2[i] = from_beginning_intermediate_table[i][1];
    if (is_diff_iterate()) {
      expected_diff_state[i] = from_beginning_intermediate_table[i][2];
    } else {
      expected_diff_state[i] = from_beginning_intermediate_table[i][3];
    }
  }
  for (uint32_t i = object_count_2; i < object_count_1; i++) {
    object_map_1[i] = shrink_table[i - object_count_2][0];
    if (!is_diff_iterate()) {
      expected_diff_state[i] = shrink_table[i - object_count_2][1];
    }
  }

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    test_diff_iterate(load, 0, 2, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    test_deep_copy(load, 0, 2, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToSnapIntermediateSnapShrinkToZero) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_1 = std::size(shrink_table);
  m_image_ctx->size = 0;
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, {}, {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_2;
  BitVector<2> expected_diff_state;
  if (!is_diff_iterate()) {
    expected_diff_state.resize(object_count_1);
  }
  for (uint32_t i = 0; i < object_count_1; i++) {
    object_map_1[i] = shrink_table[i][0];
    if (!is_diff_iterate()) {
      expected_diff_state[i] = shrink_table[i][1];
    }
  }

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    test_diff_iterate(load, 0, 2, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    test_deep_copy(load, 0, 2, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToHead) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_beginning_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);

  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  for (uint32_t i = 0; i < object_count; i++) {
    object_map_head[i] = from_beginning_table[i][0];
    expected_diff_state[i] = from_beginning_table[i][1];
  }

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    test_deep_copy(load, 0, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToHeadEmpty) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  m_image_ctx->size = 0;

  BitVector<2> object_map_head;
  BitVector<2> expected_diff_state;

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    test_deep_copy(load, 0, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToHeadIntermediateSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_beginning_intermediate_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
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

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_deep_copy(load, 0, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToHeadIntermediateSnapGrow) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_1 = std::size(from_beginning_intermediate_table);
  uint32_t object_count_head = object_count_1 + std::size(from_beginning_table);
  m_image_ctx->size = object_count_head * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count_head);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count_head);
  for (uint32_t i = 0; i < object_count_1; i++) {
    object_map_1[i] = from_beginning_intermediate_table[i][0];
    object_map_head[i] = from_beginning_intermediate_table[i][1];
    if (is_diff_iterate()) {
      expected_diff_state[i] = from_beginning_intermediate_table[i][2];
    } else {
      expected_diff_state[i] = from_beginning_intermediate_table[i][3];
    }
  }
  for (uint32_t i = object_count_1; i < object_count_head; i++) {
    object_map_head[i] = from_beginning_table[i - object_count_1][0];
    expected_diff_state[i] = from_beginning_table[i - object_count_1][1];
  }

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_deep_copy(load, 0, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToHeadIntermediateSnapGrowFromZero) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_head = std::size(from_beginning_table);
  m_image_ctx->size = object_count_head * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, {}, {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  BitVector<2> object_map_head;
  object_map_head.resize(object_count_head);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count_head);
  for (uint32_t i = 0; i < object_count_head; i++) {
    object_map_head[i] = from_beginning_table[i][0];
    expected_diff_state[i] = from_beginning_table[i][1];
  }

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_deep_copy(load, 0, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToHeadIntermediateSnapShrink) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_head = std::size(from_beginning_intermediate_table);
  uint32_t object_count_1 = object_count_head + std::size(shrink_table);
  m_image_ctx->size = object_count_head * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count_head);
  BitVector<2> expected_diff_state;
  if (is_diff_iterate()) {
    expected_diff_state.resize(object_count_head);
  } else {
    expected_diff_state.resize(object_count_1);
  }
  for (uint32_t i = 0; i < object_count_head; i++) {
    object_map_1[i] = from_beginning_intermediate_table[i][0];
    object_map_head[i] = from_beginning_intermediate_table[i][1];
    if (is_diff_iterate()) {
      expected_diff_state[i] = from_beginning_intermediate_table[i][2];
    } else {
      expected_diff_state[i] = from_beginning_intermediate_table[i][3];
    }
  }
  for (uint32_t i = object_count_head; i < object_count_1; i++) {
    object_map_1[i] = shrink_table[i - object_count_head][0];
    if (!is_diff_iterate()) {
      expected_diff_state[i] = shrink_table[i - object_count_head][1];
    }
  }

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_deep_copy(load, 0, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromBeginningToHeadIntermediateSnapShrinkToZero) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_1 = std::size(shrink_table);
  m_image_ctx->size = 0;
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_head;
  BitVector<2> expected_diff_state;
  if (!is_diff_iterate()) {
    expected_diff_state.resize(object_count_1);
  }
  for (uint32_t i = 0; i < object_count_1; i++) {
    object_map_1[i] = shrink_table[i][0];
    if (!is_diff_iterate()) {
      expected_diff_state[i] = shrink_table[i][1];
    }
  }

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_deep_copy(load, 0, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_snap_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
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

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, 0, 0);
    expect_load_map(mock_image_ctx, 2, object_map_2, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, 2, expected_diff_state);
  } else {
    test_deep_copy(load, 1, 2, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToSnapGrow) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_1 = std::size(from_snap_table);
  uint32_t object_count_2 = object_count_1 + std::size(from_beginning_table);
  m_image_ctx->size = object_count_2 * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}},
          object_count_2 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count_2);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count_2);
  for (uint32_t i = 0; i < object_count_1; i++) {
    object_map_1[i] = from_snap_table[i][0];
    object_map_2[i] = from_snap_table[i][1];
    expected_diff_state[i] = from_snap_table[i][2];
  }
  for (uint32_t i = object_count_1; i < object_count_2; i++) {
    object_map_2[i] = from_beginning_table[i - object_count_1][0];
    expected_diff_state[i] = from_beginning_table[i - object_count_1][1];
  }

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, 0, 0);
    expect_load_map(mock_image_ctx, 2, object_map_2, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, 2, expected_diff_state);
  } else {
    test_deep_copy(load, 1, 2, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToSnapGrowFromZero) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_2 = std::size(from_beginning_table);
  m_image_ctx->size = object_count_2 * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, {}, {}, {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}},
          object_count_2 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  BitVector<2> object_map_2;
  object_map_2.resize(object_count_2);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count_2);
  for (uint32_t i = 0; i < object_count_2; i++) {
    object_map_2[i] = from_beginning_table[i][0];
    expected_diff_state[i] = from_beginning_table[i][1];
  }

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, 0, 0);
    expect_load_map(mock_image_ctx, 2, object_map_2, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, 2, expected_diff_state);
  } else {
    test_deep_copy(load, 1, 2, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToSnapShrink) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_2 = std::size(from_snap_table);
  uint32_t object_count_1 = object_count_2 + std::size(shrink_table);
  m_image_ctx->size = object_count_2 * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}},
          object_count_2 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count_2);
  BitVector<2> expected_diff_state;
  if (is_diff_iterate()) {
    expected_diff_state.resize(object_count_2);
  } else {
    expected_diff_state.resize(object_count_1);
  }
  for (uint32_t i = 0; i < object_count_2; i++) {
    object_map_1[i] = from_snap_table[i][0];
    object_map_2[i] = from_snap_table[i][1];
    expected_diff_state[i] = from_snap_table[i][2];
  }
  for (uint32_t i = object_count_2; i < object_count_1; i++) {
    object_map_1[i] = shrink_table[i - object_count_2][0];
    if (!is_diff_iterate()) {
      expected_diff_state[i] = shrink_table[i - object_count_2][1];
    }
  }

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, 0, 0);
    expect_load_map(mock_image_ctx, 2, object_map_2, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, 2, expected_diff_state);
  } else {
    test_deep_copy(load, 1, 2, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToSnapShrinkToZero) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_1 = std::size(shrink_table);
  m_image_ctx->size = 0;
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, {}, {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_2;
  BitVector<2> expected_diff_state;
  if (!is_diff_iterate()) {
    expected_diff_state.resize(object_count_1);
  }
  for (uint32_t i = 0; i < object_count_1; i++) {
    object_map_1[i] = shrink_table[i][0];
    if (!is_diff_iterate()) {
      expected_diff_state[i] = shrink_table[i][1];
    }
  }

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, 0, 0);
    expect_load_map(mock_image_ctx, 2, object_map_2, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, 2, expected_diff_state);
  } else {
    test_deep_copy(load, 1, 2, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToSnapIntermediateSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_snap_intermediate_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {3U, {"snap3", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
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

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, 0, 0);
    expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    expect_get_flags(mock_image_ctx, 3, 0, 0);
    expect_load_map(mock_image_ctx, 3, object_map_3, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, 3, expected_diff_state);
  } else {
    test_deep_copy(load, 1, 3, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToHead) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_snap_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
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

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, CEPH_NOSNAP, expected_diff_state);
  } else {
    test_deep_copy(load, 1, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToHeadGrow) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_1 = std::size(from_snap_table);
  uint32_t object_count_head = object_count_1 + std::size(from_beginning_table);
  m_image_ctx->size = object_count_head * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count_head);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count_head);
  for (uint32_t i = 0; i < object_count_1; i++) {
    object_map_1[i] = from_snap_table[i][0];
    object_map_head[i] = from_snap_table[i][1];
    expected_diff_state[i] = from_snap_table[i][2];
  }
  for (uint32_t i = object_count_1; i < object_count_head; i++) {
    object_map_head[i] = from_beginning_table[i - object_count_1][0];
    expected_diff_state[i] = from_beginning_table[i - object_count_1][1];
  }

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, CEPH_NOSNAP, expected_diff_state);
  } else {
    test_deep_copy(load, 1, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToHeadGrowFromZero) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_head = std::size(from_beginning_table);
  m_image_ctx->size = object_count_head * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, {}, {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  BitVector<2> object_map_head;
  object_map_head.resize(object_count_head);
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count_head);
  for (uint32_t i = 0; i < object_count_head; i++) {
    object_map_head[i] = from_beginning_table[i][0];
    expected_diff_state[i] = from_beginning_table[i][1];
  }

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, CEPH_NOSNAP, expected_diff_state);
  } else {
    test_deep_copy(load, 1, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToHeadShrink) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_head = std::size(from_snap_table);
  uint32_t object_count_1 = object_count_head + std::size(shrink_table);
  m_image_ctx->size = object_count_head * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count_head);
  BitVector<2> expected_diff_state;
  if (is_diff_iterate()) {
    expected_diff_state.resize(object_count_head);
  } else {
    expected_diff_state.resize(object_count_1);
  }
  for (uint32_t i = 0; i < object_count_head; i++) {
    object_map_1[i] = from_snap_table[i][0];
    object_map_head[i] = from_snap_table[i][1];
    expected_diff_state[i] = from_snap_table[i][2];
  }
  for (uint32_t i = object_count_head; i < object_count_1; i++) {
    object_map_1[i] = shrink_table[i - object_count_head][0];
    if (!is_diff_iterate()) {
      expected_diff_state[i] = shrink_table[i - object_count_head][1];
    }
  }

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, CEPH_NOSNAP, expected_diff_state);
  } else {
    test_deep_copy(load, 1, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToHeadShrinkToZero) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count_1 = std::size(shrink_table);
  m_image_ctx->size = 0;
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}},
          object_count_1 * (1 << m_image_ctx->order), {}, {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count_1);
  BitVector<2> object_map_head;
  BitVector<2> expected_diff_state;
  if (!is_diff_iterate()) {
    expected_diff_state.resize(object_count_1);
  }
  for (uint32_t i = 0; i < object_count_1; i++) {
    object_map_1[i] = shrink_table[i][0];
    if (!is_diff_iterate()) {
      expected_diff_state[i] = shrink_table[i][1];
    }
  }

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, CEPH_NOSNAP, expected_diff_state);
  } else {
    test_deep_copy(load, 1, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, FromSnapToHeadIntermediateSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = std::size(from_snap_intermediate_table);
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
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

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, 0, 0);
    expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, CEPH_NOSNAP, expected_diff_state);
  } else {
    test_deep_copy(load, 1, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, StartSnapDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  if (is_diff_iterate()) {
    ASSERT_EQ(-ENOENT, do_diff(noop, 1, 2, 0, object_count));
  } else {
    ASSERT_EQ(-ENOENT, do_diff(noop, 1, 2, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, EndSnapDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);

  if (is_diff_iterate()) {
    ASSERT_EQ(-ENOENT, do_diff(noop, 0, 2, 0, object_count));
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    };
    ASSERT_EQ(-ENOENT, do_diff(load, 0, 2, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateSnapDNEFromBeginning) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  object_map_head[1] = OBJECT_EXISTS_CLEAN;
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0,
                      [&mock_image_ctx]() { mock_image_ctx.snap_info.erase(2); });
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_deep_copy(load, 0, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateSnapDNEFromSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  object_map_head[1] = OBJECT_EXISTS_CLEAN;
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0,
                    [&mock_image_ctx]() { mock_image_ctx.snap_info.erase(2); });
    expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, CEPH_NOSNAP, expected_diff_state);
  } else {
    test_deep_copy(load, 1, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, StartObjectMapDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, -ENOENT);
  };
  if (is_diff_iterate()) {
    ASSERT_EQ(-ENOENT, do_diff(load, 1, 2, 0, object_count));
  } else {
    ASSERT_EQ(-ENOENT, do_diff(load, 1, 2, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, EndObjectMapDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count);
  object_map_2[1] = OBJECT_EXISTS_CLEAN;
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, -ENOENT);
    };
    ASSERT_EQ(-ENOENT, do_diff(load, 0, 2, 0, object_count));
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, -ENOENT);
    };
    ASSERT_EQ(-ENOENT, do_diff(load, 0, 2, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateObjectMapDNEFromBeginning) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  object_map_head[1] = OBJECT_EXISTS_CLEAN;
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, -ENOENT);
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_deep_copy(load, 0, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateObjectMapDNEFromSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_2;
  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  object_map_head[1] = OBJECT_EXISTS_CLEAN;
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, 0, 0);
    expect_load_map(mock_image_ctx, 2, object_map_2, -ENOENT);
    expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
  };
  if (is_diff_iterate()) {
    test_diff_iterate(load, 1, CEPH_NOSNAP, expected_diff_state);
  } else {
    test_deep_copy(load, 1, CEPH_NOSNAP, expected_diff_state);
  }
}

TEST_P(TestMockObjectMapDiffRequest, StartFastDiffInvalid) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  auto get_flags = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, RBD_FLAG_FAST_DIFF_INVALID, 0);
  };
  if (is_diff_iterate()) {
    ASSERT_EQ(-EINVAL, do_diff(get_flags, 1, 2, 0, object_count));
  } else {
    ASSERT_EQ(-EINVAL, do_diff(get_flags, 1, 2, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, EndFastDiffInvalid) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);

  if (is_diff_iterate()) {
    auto get_flags = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 2, RBD_FLAG_FAST_DIFF_INVALID, 0);
    };
    ASSERT_EQ(-EINVAL, do_diff(get_flags, 0, 2, 0, object_count));
  } else {
    auto get_flags = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, 2, RBD_FLAG_FAST_DIFF_INVALID, 0);
    };
    ASSERT_EQ(-EINVAL, do_diff(get_flags, 0, 2, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateFastDiffInvalidFromBeginning) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  object_map_head[1] = OBJECT_EXISTS_CLEAN;
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto get_flags = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, RBD_FLAG_FAST_DIFF_INVALID, 0);
    };
    ASSERT_EQ(-EINVAL, do_diff(get_flags, 0, CEPH_NOSNAP, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateFastDiffInvalidFromSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);

  auto get_flags = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, RBD_FLAG_FAST_DIFF_INVALID, 0);
  };
  if (is_diff_iterate()) {
    ASSERT_EQ(-EINVAL, do_diff(get_flags, 1, CEPH_NOSNAP, 0, object_count));
  } else {
    ASSERT_EQ(-EINVAL, do_diff(get_flags, 1, CEPH_NOSNAP, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, StartObjectMapLoadError) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, -EPERM);
  };
  if (is_diff_iterate()) {
    ASSERT_EQ(-EPERM, do_diff(load, 1, 2, 0, object_count));
  } else {
    ASSERT_EQ(-EPERM, do_diff(load, 1, 2, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, EndObjectMapLoadError) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count);
  object_map_2[1] = OBJECT_EXISTS_CLEAN;
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, -EPERM);
    };
    ASSERT_EQ(-EPERM, do_diff(load, 0, 2, 0, object_count));
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, -EPERM);
    };
    ASSERT_EQ(-EPERM, do_diff(load, 0, 2, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateObjectMapLoadErrorFromBeginning) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  object_map_head[1] = OBJECT_EXISTS_CLEAN;
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, -EPERM);
    };
    ASSERT_EQ(-EPERM, do_diff(load, 0, CEPH_NOSNAP, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateObjectMapLoadErrorFromSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_2;

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, 0, 0);
    expect_load_map(mock_image_ctx, 2, object_map_2, -EPERM);
  };
  if (is_diff_iterate()) {
    ASSERT_EQ(-EPERM, do_diff(load, 1, CEPH_NOSNAP, 0, object_count));
  } else {
    ASSERT_EQ(-EPERM, do_diff(load, 1, CEPH_NOSNAP, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, StartObjectMapTooSmall) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count - 1);

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
  };
  if (is_diff_iterate()) {
    ASSERT_EQ(-EINVAL, do_diff(load, 1, 2, 0, object_count));
  } else {
    ASSERT_EQ(-EINVAL, do_diff(load, 1, 2, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, EndObjectMapTooSmall) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count - 1);

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    ASSERT_EQ(-EINVAL, do_diff(load, 0, 2, 0, object_count));
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, 2, 0, 0);
      expect_load_map(mock_image_ctx, 2, object_map_2, 0);
    };
    ASSERT_EQ(-EINVAL, do_diff(load, 0, 2, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateObjectMapTooSmallFromBeginning) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count - 1);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count);
  object_map_head[1] = OBJECT_EXISTS_CLEAN;
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    };
    ASSERT_EQ(-EINVAL, do_diff(load, 0, CEPH_NOSNAP, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, IntermediateObjectMapTooSmallFromSnap) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}},
    {2U, {"snap2", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count);
  BitVector<2> object_map_2;
  object_map_2.resize(object_count - 1);

  auto load = [&](MockTestImageCtx& mock_image_ctx) {
    expect_get_flags(mock_image_ctx, 1, 0, 0);
    expect_load_map(mock_image_ctx, 1, object_map_1, 0);
    expect_get_flags(mock_image_ctx, 2, 0, 0);
    expect_load_map(mock_image_ctx, 2, object_map_2, 0);
  };
  if (is_diff_iterate()) {
    ASSERT_EQ(-EINVAL, do_diff(load, 1, CEPH_NOSNAP, 0, object_count));
  } else {
    ASSERT_EQ(-EINVAL, do_diff(load, 1, CEPH_NOSNAP, 0, UINT64_MAX));
  }
}

TEST_P(TestMockObjectMapDiffRequest, ObjectMapTooLarge) {
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

  uint32_t object_count = 5;
  m_image_ctx->size = object_count * (1 << m_image_ctx->order);
  m_image_ctx->snap_info = {
    {1U, {"snap1", {cls::rbd::UserSnapshotNamespace{}}, m_image_ctx->size, {},
          {}, {}, {}}}
  };

  BitVector<2> object_map_1;
  object_map_1.resize(object_count + 12);
  BitVector<2> object_map_head;
  object_map_head.resize(object_count + 34);
  object_map_head[1] = OBJECT_EXISTS_CLEAN;
  BitVector<2> expected_diff_state;
  expected_diff_state.resize(object_count);
  expected_diff_state[1] = DIFF_STATE_DATA_UPDATED;

  if (is_diff_iterate()) {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_diff_iterate(load, 0, CEPH_NOSNAP, expected_diff_state);
  } else {
    auto load = [&](MockTestImageCtx& mock_image_ctx) {
      expect_get_flags(mock_image_ctx, 1, 0, 0);
      expect_load_map(mock_image_ctx, 1, object_map_1, 0);
      expect_get_flags(mock_image_ctx, CEPH_NOSNAP, 0, 0);
      expect_load_map(mock_image_ctx, CEPH_NOSNAP, object_map_head, 0);
    };
    test_deep_copy(load, 0, CEPH_NOSNAP, expected_diff_state);
  }
}

INSTANTIATE_TEST_SUITE_P(MockObjectMapDiffRequestTests,
                         TestMockObjectMapDiffRequest, ::testing::Bool());

} // namespace object_map
} // librbd
