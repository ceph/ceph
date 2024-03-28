// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/migration/MockSnapshotInterface.h"
#include "include/rbd_types.h"
#include "common/ceph_mutex.h"
#include "librbd/migration/RawFormat.h"
#include "librbd/migration/SourceSpecBuilder.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "json_spirit/json_spirit.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }

  static MockTestImageCtx *create(const std::string &image_name,
                                  const std::string &image_id,
                                  librados::snap_t snap_id, librados::IoCtx& p,
                                  bool read_only) {
    auto ictx = ImageCtx::create(
            image_name, image_id, snap_id, p, read_only);
    auto mock_image_ctx = new MockTestImageCtx(*ictx);
    mock_image_ctx->m_image_ctx = ictx;
    m_ictxs.insert(mock_image_ctx);
    return mock_image_ctx;
  }

  ImageCtx* m_image_ctx = nullptr;

  static std::set<MockTestImageCtx *> m_ictxs;
};

std::set<librbd::MockTestImageCtx *> MockTestImageCtx::m_ictxs;

} // anonymous namespace

namespace migration {

template<>
struct SourceSpecBuilder<librbd::MockTestImageCtx> {

  MOCK_CONST_METHOD4(build_snapshot, int(librbd::MockTestImageCtx*,
                                         const json_spirit::mObject&, uint64_t,
                                         std::shared_ptr<SnapshotInterface>*));

};

} // namespace migration
} // namespace librbd

#include "librbd/migration/RawFormat.cc"

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::ReturnRef;
using ::testing::WithArg;
using ::testing::WithArgs;

namespace librbd {
namespace migration {

using ::testing::Invoke;

class TestMockMigrationRawFormat : public TestMockFixture {
public:
  typedef RawFormat<MockTestImageCtx> MockRawFormat;
  typedef SourceSpecBuilder<MockTestImageCtx> MockSourceSpecBuilder;

  librbd::ImageCtx *m_image_ctx;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));

    json_spirit::mObject stream_obj;
    stream_obj["type"] = "file";
    json_object["stream"] = stream_obj;

    MockTestImageCtx::m_ictxs.clear();
  }

  void TearDown() {
    for (auto iter = MockTestImageCtx::m_ictxs.begin();
         iter != MockTestImageCtx::m_ictxs.end(); ++iter) {
      auto mock_image_ctx = *iter;
      auto ictx = mock_image_ctx->m_image_ctx;
      delete mock_image_ctx;
      ictx->state->close();
    }

    TestMockFixture::TearDown();
  }

  void expect_build_snapshot(MockSourceSpecBuilder& mock_source_spec_builder,
                             uint64_t index,
                             MockSnapshotInterface* mock_snapshot_interface,
                             int r) {
    EXPECT_CALL(mock_source_spec_builder, build_snapshot(_, _, index, _))
      .WillOnce(WithArgs<3>(Invoke([mock_snapshot_interface, r]
        (std::shared_ptr<SnapshotInterface>* ptr) {
          ptr->reset(mock_snapshot_interface);
          return r;
        })));
  }

  void expect_snapshot_open(MockSnapshotInterface& mock_snapshot_interface,
                            int r) {
    EXPECT_CALL(mock_snapshot_interface, open(_, _))
      .WillOnce(WithArg<1>(Invoke([r](Context* ctx) { ctx->complete(r); })));
  }

  void expect_snapshot_open(MockSnapshotInterface& mock_snapshot_interface,
                            Context** ctx) {
    EXPECT_CALL(mock_snapshot_interface, open(_, _))
            .WillOnce(WithArg<1>(Invoke(
                    [ctx](Context* ctx2) { *ctx = ctx2; })));
  }

  void expect_snapshot_close(MockSnapshotInterface& mock_snapshot_interface,
                             int r) {
    EXPECT_CALL(mock_snapshot_interface, close(_))
      .WillOnce(Invoke([r](Context* ctx) { ctx->complete(r); }));
  }

  void expect_snapshot_get_info(MockSnapshotInterface& mock_snapshot_interface,
                                const SnapInfo& snap_info) {
    EXPECT_CALL(mock_snapshot_interface, get_snap_info())
      .WillOnce(ReturnRef(snap_info));
  }

  void expect_snapshot_read(MockSnapshotInterface& mock_snapshot_interface,
                            const io::Extents& image_extents,
                            const bufferlist& bl, int r) {
    EXPECT_CALL(mock_snapshot_interface, read(_, image_extents, _))
      .WillOnce(WithArgs<0, 2>(Invoke([bl, image_extents, r]
        (io::AioCompletion* aio_comp, io::ReadResult& read_result) {
          aio_comp->read_result = std::move(read_result);
          aio_comp->read_result.set_image_extents(image_extents);
          aio_comp->set_request_count(1);
          auto ctx = new io::ReadResult::C_ImageReadRequest(aio_comp, 0,
                                                            image_extents);
          ctx->bl = std::move(bl);
          ctx->complete(r);
        })));
  }

  void expect_snapshot_list_snap(MockSnapshotInterface& mock_snapshot_interface,
                                 const io::Extents& image_extents,
                                 const io::SparseExtents& sparse_extents,
                                 int r) {
    EXPECT_CALL(mock_snapshot_interface, list_snap(image_extents, _, _))
      .WillOnce(WithArgs<1, 2>(Invoke(
        [sparse_extents, r](io::SparseExtents* out_sparse_extents,
                            Context* ctx) {
          out_sparse_extents->insert(sparse_extents);
          ctx->complete(r);
        })));
  }

  void expect_close(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, close(_))
      .WillOnce(Invoke([r](Context* ctx) {
                  ctx->complete(r);
                }));
  }

  json_spirit::mObject json_object;
};

TEST_F(TestMockMigrationRawFormat, OpenClose) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                       &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  mock_raw_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationRawFormat, OpenError) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx = nullptr;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  Context* open_ctx;
  expect_snapshot_open(*mock_snapshot_interface, &open_ctx);

  MockRawFormat mock_raw_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx;
  mock_raw_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                       &mock_src_image_ctx, &ctx);

  expect_snapshot_close(*mock_snapshot_interface, 0);
  expect_close(*mock_src_image_ctx, 0);

  open_ctx->complete(-ENOENT);
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockMigrationRawFormat, OpenSnapshotError) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx = nullptr;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface_head = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface_head, 0);

  auto mock_snapshot_interface_1 = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, 1,
                        mock_snapshot_interface_1, 0);

  Context* open_ctx;
  expect_snapshot_open(*mock_snapshot_interface_1, &open_ctx);
  expect_snapshot_open(*mock_snapshot_interface_head, 0);

  json_spirit::mArray snapshots;
  snapshots.push_back(json_spirit::mObject{});
  json_object["snapshots"] = snapshots;

  MockRawFormat mock_raw_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx;
  mock_raw_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                       &mock_src_image_ctx, &ctx);

  expect_snapshot_close(*mock_snapshot_interface_1, 0);
  expect_snapshot_close(*mock_snapshot_interface_head, 0);
  expect_close(*mock_src_image_ctx, 0);
  open_ctx->complete(-ENOENT);

  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockMigrationRawFormat, GetSnapshots) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                       &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  SnapInfos snap_infos;
  mock_raw_format.get_snapshots(&snap_infos, &ctx2);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_TRUE(snap_infos.empty());

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationRawFormat, GetImageSize) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);

  SnapInfo snap_info{{}, {}, 123, {}, 0, 0, {}};
  expect_snapshot_get_info(*mock_snapshot_interface, snap_info);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                       &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  uint64_t size;
  mock_raw_format.get_image_size(CEPH_NOSNAP, &size, &ctx2);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(size, 123);

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationRawFormat, GetImageSizeSnapshotDNE) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                       &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  uint64_t size;
  mock_raw_format.get_image_size(0, &size, &ctx2);
  ASSERT_EQ(-ENOENT, ctx2.wait());

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationRawFormat, Read) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);

  bufferlist expect_bl;
  expect_bl.append(std::string(123, '1'));
  expect_snapshot_read(*mock_snapshot_interface, {{123, 123}}, expect_bl, 0);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                       &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl;
  io::ReadResult read_result{&bl};
  ASSERT_TRUE(mock_raw_format.read(aio_comp, CEPH_NOSNAP, {{123, 123}},
                                   std::move(read_result), 0, 0, {}));
  ASSERT_EQ(123, ctx2.wait());
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationRawFormat, ListSnaps) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);

  SnapInfo snap_info{{}, {}, 123, {}, 0, 0, {}};
  expect_snapshot_get_info(*mock_snapshot_interface, snap_info);
  io::SparseExtents sparse_extents;
  sparse_extents.insert(0, 123, {io::SPARSE_EXTENT_STATE_DATA, 123});
  expect_snapshot_list_snap(*mock_snapshot_interface, {{0, 123}},
                            sparse_extents, 0);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                       &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SnapshotDelta snapshot_delta;
  mock_raw_format.list_snaps({{0, 123}}, {CEPH_NOSNAP}, 0, &snapshot_delta, {},
                             &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  io::SnapshotDelta expected_snapshot_delta;
  expected_snapshot_delta[{CEPH_NOSNAP, CEPH_NOSNAP}] = sparse_extents;
  ASSERT_EQ(expected_snapshot_delta, snapshot_delta);

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationRawFormat, ListSnapsError) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);


  expect_snapshot_open(*mock_snapshot_interface, 0);

  SnapInfo snap_info{{}, {}, 123, {}, 0, 0, {}};
  expect_snapshot_get_info(*mock_snapshot_interface, snap_info);
  io::SparseExtents sparse_extents;
  sparse_extents.insert(0, 123, {io::SPARSE_EXTENT_STATE_DATA, 123});
  expect_snapshot_list_snap(*mock_snapshot_interface, {{0, 123}},
                            sparse_extents, -EINVAL);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                       &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SnapshotDelta snapshot_delta;
  mock_raw_format.list_snaps({{0, 123}}, {CEPH_NOSNAP}, 0, &snapshot_delta, {},
                             &ctx2);
  ASSERT_EQ(-EINVAL, ctx2.wait());

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationRawFormat, ListSnapsMerge) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface_head = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface_head, 0);

  auto mock_snapshot_interface_1 = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, 1,
                        mock_snapshot_interface_1, 0);

  auto mock_snapshot_interface_2 = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, 2,
                        mock_snapshot_interface_2, 0);


  expect_snapshot_open(*mock_snapshot_interface_1, 0);
  expect_snapshot_open(*mock_snapshot_interface_2, 0);
  expect_snapshot_open(*mock_snapshot_interface_head, 0);

  SnapInfo snap_info_head{{}, {}, 256, {}, 0, 0, {}};
  SnapInfo snap_info_1{snap_info_head};
  snap_info_1.size = 123;
  expect_snapshot_get_info(*mock_snapshot_interface_1, snap_info_1);
  io::SparseExtents sparse_extents_1;
  sparse_extents_1.insert(0, 123, {io::SPARSE_EXTENT_STATE_DATA, 123});
  expect_snapshot_list_snap(*mock_snapshot_interface_1, {{0, 123}},
                            sparse_extents_1, 0);

  SnapInfo snap_info_2{snap_info_head};
  snap_info_2.size = 64;
  expect_snapshot_get_info(*mock_snapshot_interface_2, snap_info_2);
  io::SparseExtents sparse_extents_2;
  sparse_extents_2.insert(0, 32, {io::SPARSE_EXTENT_STATE_DATA, 32});
  expect_snapshot_list_snap(*mock_snapshot_interface_2, {{0, 123}},
                            sparse_extents_2, 0);

  expect_snapshot_get_info(*mock_snapshot_interface_head, snap_info_head);
  io::SparseExtents sparse_extents_head;
  sparse_extents_head.insert(0, 16, {io::SPARSE_EXTENT_STATE_DATA, 16});
  expect_snapshot_list_snap(*mock_snapshot_interface_head, {{0, 123}},
                            sparse_extents_head, 0);

  expect_snapshot_close(*mock_snapshot_interface_1, 0);
  expect_snapshot_close(*mock_snapshot_interface_2, 0);
  expect_snapshot_close(*mock_snapshot_interface_head, 0);

  json_spirit::mArray snapshots;
  snapshots.push_back(json_spirit::mObject{});
  snapshots.push_back(json_spirit::mObject{});
  json_object["snapshots"] = snapshots;

  MockRawFormat mock_raw_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                       &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SnapshotDelta snapshot_delta;
  mock_raw_format.list_snaps({{0, 123}}, {1, CEPH_NOSNAP}, 0, &snapshot_delta,
                             {}, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  io::SnapshotDelta expected_snapshot_delta;
  expected_snapshot_delta[{1, 1}] = sparse_extents_1;
  sparse_extents_2.erase(0, 16);
  sparse_extents_2.insert(64, 59, {io::SPARSE_EXTENT_STATE_ZEROED, 59});
  expected_snapshot_delta[{CEPH_NOSNAP, 2}] = sparse_extents_2;
  expected_snapshot_delta[{CEPH_NOSNAP, CEPH_NOSNAP}] = sparse_extents_head;
  ASSERT_EQ(expected_snapshot_delta, snapshot_delta);

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

} // namespace migration
} // namespace librbd
