// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd_types.h"
#include "common/ceph_mutex.h"
#include "librbd/migration/FileStream.h"
#include "librbd/migration/RawFormat.h"
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
struct FileStream<librbd::MockTestImageCtx> : public StreamInterface {
  static FileStream* s_instance;
  static FileStream* create(librbd::MockTestImageCtx*,
                            const json_spirit::mObject&) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD1(open, void(Context*));
  MOCK_METHOD1(close, void(Context*));

  MOCK_METHOD2(get_size, void(uint64_t*, Context*));

  MOCK_METHOD3(read, void(const io::Extents&, bufferlist*, Context*));
  void read(io::Extents&& byte_extents, bufferlist* bl, Context* on_finish) {
    read(byte_extents, bl, on_finish);
  }

  FileStream() {
    s_instance = this;
  }
};

FileStream<librbd::MockTestImageCtx>* FileStream<librbd::MockTestImageCtx>::s_instance = nullptr;

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
  typedef FileStream<MockTestImageCtx> MockFileStream;

  librbd::ImageCtx *m_image_ctx;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));

    json_spirit::mObject stream_obj;
    stream_obj["type"] = "file";
    json_object["stream"] = stream_obj;
  }

  void expect_stream_open(MockFileStream& mock_file_stream, int r) {
    EXPECT_CALL(mock_file_stream, open(_))
      .WillOnce(Invoke([r](Context* ctx) { ctx->complete(r); }));
  }

  void expect_stream_close(MockFileStream& mock_file_stream, int r) {
    EXPECT_CALL(mock_file_stream, close(_))
      .WillOnce(Invoke([r](Context* ctx) { ctx->complete(r); }));
  }

  void expect_stream_get_size(MockFileStream& mock_file_stream,
                              uint64_t size, int r) {
    EXPECT_CALL(mock_file_stream, get_size(_, _))
      .WillOnce(Invoke([size, r](uint64_t* out_size, Context* ctx) {
          *out_size = size;
          ctx->complete(r);
        }));
  }

  void expect_stream_read(MockFileStream& mock_file_stream,
                          const io::Extents& byte_extents,
                          const bufferlist& bl, int r) {
    EXPECT_CALL(mock_file_stream, read(byte_extents, _, _))
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
  auto mock_file_stream = new MockFileStream();
  expect_stream_open(*mock_file_stream, 0);

  expect_stream_close(*mock_file_stream, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object);

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
  auto mock_file_stream = new MockFileStream();
  expect_stream_open(*mock_file_stream, 0);

  expect_stream_close(*mock_file_stream, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object);

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
  auto mock_file_stream = new MockFileStream();
  expect_stream_open(*mock_file_stream, 0);

  expect_stream_get_size(*mock_file_stream, 123, 0);

  expect_stream_close(*mock_file_stream, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object);

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
  auto mock_file_stream = new MockFileStream();
  expect_stream_open(*mock_file_stream, 0);

  expect_stream_close(*mock_file_stream, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object);

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
  auto mock_file_stream = new MockFileStream();
  expect_stream_open(*mock_file_stream, 0);

  bufferlist expect_bl;
  expect_bl.append(std::string(123, '1'));
  expect_stream_read(*mock_file_stream, {{123, 123}}, expect_bl, 0);

  expect_stream_close(*mock_file_stream, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_raw_format.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl;
  io::ReadResult read_result{&bl};
  mock_raw_format.read(aio_comp, CEPH_NOSNAP, {{123, 123}},
                       std::move(read_result), 0, 0, {});
  ASSERT_EQ(123, ctx2.wait());
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  mock_raw_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationRawFormat, ListSnaps) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;
  auto mock_file_stream = new MockFileStream();
  expect_stream_open(*mock_file_stream, 0);

  expect_stream_close(*mock_file_stream, 0);

  MockRawFormat mock_raw_format(&mock_image_ctx, json_object);

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
