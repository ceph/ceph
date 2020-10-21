// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/migration/MockStreamInterface.h"
#include "include/rbd_types.h"
#include "common/ceph_mutex.h"
#include "librbd/migration/FileStream.h"
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

  MOCK_CONST_METHOD2(build_stream, int(const json_spirit::mObject&,
                                       std::unique_ptr<StreamInterface>*));

};

} // namespace migration
} // namespace librbd

#include "librbd/migration/RawFormat.cc"

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
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

  void expect_build_stream(MockSourceSpecBuilder& mock_source_spec_builder,
                           MockStreamInterface* mock_stream_interface, int r) {
    EXPECT_CALL(mock_source_spec_builder, build_stream(_, _))
      .WillOnce(WithArgs<1>(Invoke([mock_stream_interface, r]
        (std::unique_ptr<StreamInterface>* ptr) {
          ptr->reset(mock_stream_interface);
          return r;
        })));
  }

  void expect_stream_open(MockStreamInterface& mock_stream_interface, int r) {
    EXPECT_CALL(mock_stream_interface, open(_))
      .WillOnce(Invoke([r](Context* ctx) { ctx->complete(r); }));
  }

  void expect_stream_close(MockStreamInterface& mock_stream_interface, int r) {
    EXPECT_CALL(mock_stream_interface, close(_))
      .WillOnce(Invoke([r](Context* ctx) { ctx->complete(r); }));
  }

  void expect_stream_get_size(MockStreamInterface& mock_stream_interface,
                              uint64_t size, int r) {
    EXPECT_CALL(mock_stream_interface, get_size(_, _))
      .WillOnce(Invoke([size, r](uint64_t* out_size, Context* ctx) {
          *out_size = size;
          ctx->complete(r);
        }));
  }

  void expect_stream_read(MockStreamInterface& mock_stream_interface,
                          const io::Extents& byte_extents,
                          const bufferlist& bl, int r) {
    EXPECT_CALL(mock_stream_interface, read(byte_extents, _, _))
      .WillOnce(WithArgs<1, 2>(Invoke([bl, r]
        (bufferlist* out_bl, Context* ctx) {
          *out_bl = bl;
          ctx->complete(r);
        })));
  }

  json_spirit::mObject json_object;
};

TEST_F(TestMockMigrationRawFormat, OpenClose) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);
  expect_stream_get_size(*mock_stream_interface, 0, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object,
                                &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  mock_raw_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationRawFormat, GetSnapshots) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);
  expect_stream_get_size(*mock_stream_interface, 0, 0);

  expect_stream_close(*mock_stream_interface, 0);

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

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);
  expect_stream_get_size(*mock_stream_interface, 0, 0);

  expect_stream_get_size(*mock_stream_interface, 123, 0);

  expect_stream_close(*mock_stream_interface, 0);

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

TEST_F(TestMockMigrationRawFormat, GetImageSizeSnapshot) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);
  expect_stream_get_size(*mock_stream_interface, 0, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object,
                                &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_raw_format.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  uint64_t size;
  mock_raw_format.get_image_size(0, &size, &ctx2);
  ASSERT_EQ(-EINVAL, ctx2.wait());

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationRawFormat, Read) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);
  expect_stream_get_size(*mock_stream_interface, 0, 0);

  bufferlist expect_bl;
  expect_bl.append(std::string(123, '1'));
  expect_stream_read(*mock_stream_interface, {{123, 123}}, expect_bl, 0);

  expect_stream_close(*mock_stream_interface, 0);

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

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);
  expect_stream_get_size(*mock_stream_interface, 0, 0);

  expect_stream_close(*mock_stream_interface, 0);

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
