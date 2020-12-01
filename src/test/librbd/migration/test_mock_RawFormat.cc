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
};

} // anonymous namespace

namespace migration {

template<>
struct SourceSpecBuilder<librbd::MockTestImageCtx> {

  MOCK_CONST_METHOD3(build_snapshot, int(const json_spirit::mObject&, uint64_t,
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
  }

  void expect_build_snapshot(MockSourceSpecBuilder& mock_source_spec_builder,
                             uint64_t index,
                             MockSnapshotInterface* mock_snapshot_interface,
                             int r) {
    EXPECT_CALL(mock_source_spec_builder, build_snapshot(_, index, _))
      .WillOnce(WithArgs<2>(Invoke([mock_snapshot_interface, r]
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

  void expect_close(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, close(_))
      .WillOnce(Invoke([this, r](Context* ctx) {
                  ctx->complete(r);
                }));
  }

  json_spirit::mObject json_object;
};

TEST_F(TestMockMigrationRawFormat, OpenClose) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);
  SnapInfo snap_info{{}, {}, 123, {}, 0, 0, {}};
  expect_snapshot_get_info(*mock_snapshot_interface, snap_info);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object,
                                &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  mock_raw_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationRawFormat, OpenError) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, -ENOENT);

  expect_close(mock_image_ctx, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object,
                                &mock_source_spec_builder);

  C_SaferCond ctx;
  mock_raw_format.open(&ctx);
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockMigrationRawFormat, GetSnapshots) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);
  SnapInfo snap_info{{}, {}, 123, {}, 0, 0, {}};
  expect_snapshot_get_info(*mock_snapshot_interface, snap_info);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object,
                                &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  FormatInterface::SnapInfos snap_infos;
  mock_raw_format.get_snapshots(&snap_infos, &ctx2);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_TRUE(snap_infos.empty());

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationRawFormat, GetImageSize) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);
  SnapInfo snap_info{{}, {}, 123, {}, 0, 0, {}};
  expect_snapshot_get_info(*mock_snapshot_interface, snap_info);

  expect_snapshot_get_info(*mock_snapshot_interface, snap_info);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object,
                                &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(&ctx1);
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
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);
  SnapInfo snap_info{{}, {}, 123, {}, 0, 0, {}};
  expect_snapshot_get_info(*mock_snapshot_interface, snap_info);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object,
                                &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(&ctx1);
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
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);
  SnapInfo snap_info{{}, {}, 123, {}, 0, 0, {}};
  expect_snapshot_get_info(*mock_snapshot_interface, snap_info);

  bufferlist expect_bl;
  expect_bl.append(std::string(123, '1'));
  expect_snapshot_read(*mock_snapshot_interface, {{123, 123}}, expect_bl, 0);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object,
                                &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(&ctx1);
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
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_snapshot_interface = new MockSnapshotInterface();
  expect_build_snapshot(mock_source_spec_builder, CEPH_NOSNAP,
                        mock_snapshot_interface, 0);

  expect_snapshot_open(*mock_snapshot_interface, 0);
  SnapInfo snap_info{{}, {}, 123, {}, 0, 0, {}};
  expect_snapshot_get_info(*mock_snapshot_interface, snap_info);

  expect_snapshot_close(*mock_snapshot_interface, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object,
                                &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SnapshotDelta snapshot_delta;
  mock_raw_format.list_snaps({{0, 123}}, {}, 0, &snapshot_delta, {}, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

} // namespace migration
} // namespace librbd
