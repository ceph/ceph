// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"
#include "librbd/crypto/CryptoImageDispatch.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Utils.h"

namespace librbd {

namespace util {

inline ImageCtx *get_image_ctx(MockImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util

} // namespace librbd

#include "librbd/crypto/CryptoImageDispatch.cc"

namespace librbd {
namespace crypto {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Pointee;
using ::testing::WithArg;

struct TestMockCryptoCryptoImageDispatch : public TestMockFixture {
  typedef CryptoImageDispatch<librbd::MockImageCtx> MockCryptoImageDispatch;

  MockCryptoInterface crypto;
  MockImageCtx* mock_image_ctx;
  MockCryptoImageDispatch* mock_crypto_image_dispatch;

  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  C_SaferCond dispatched_cond;
  Context *on_dispatched = &dispatched_cond;
  io::AioCompletion *inner_aio_comp;
  io::ReadResult* outer_read_result;
  io::ReadResult::C_ImageReadRequest* inner_read_ctx;
  ceph::bufferlist read_bl;
  io::DispatchResult dispatch_result;
  std::atomic<uint32_t> image_dispatch_flags;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockImageCtx(*ictx);
    mock_crypto_image_dispatch = new MockCryptoImageDispatch(
            mock_image_ctx, &crypto);
    outer_read_result = new io::ReadResult(&read_bl);
  }

  void TearDown() override {
    C_SaferCond cond;
    Context *on_finish = &cond;
    mock_crypto_image_dispatch->shut_down(on_finish);
    ASSERT_EQ(0, cond.wait());

    delete outer_read_result;
    delete mock_crypto_image_dispatch;
    delete mock_image_ctx;

    TestMockFixture::TearDown();
  }

  void expect_image_read(const io::Extents& extents) {
    EXPECT_CALL(*mock_image_ctx->io_image_dispatcher, send(_))
            .WillOnce(Invoke([this, extents](
                    io::ImageDispatchSpec* spec) {
                auto* read = boost::get<io::ImageDispatchSpec::Read>(
                        &spec->request);
                ASSERT_TRUE(read != nullptr);

                ASSERT_EQ(extents, spec->image_extents);

                spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;

                spec->aio_comp->read_result = std::move(read->read_result);
                spec->aio_comp->read_result.set_image_extents(extents);
                spec->aio_comp->set_request_count(1);

                inner_read_ctx = new io::ReadResult::C_ImageReadRequest(
                        spec->aio_comp, 0, extents);
            }));
  }

  void expect_decrypt(const ceph::bufferlist& bl, uint64_t offset, int r,
                      const ceph::bufferlist& out_bl = {}) {
    EXPECT_CALL(crypto, decrypt(Pointee(bl), offset)).WillOnce(
                    WithArg<0>(Invoke([r, out_bl](ceph::bufferlist* data) {
                      if (r == 0) {
                        *data = out_bl;
                      }
                      return r;
                    })));
  }
};

TEST_F(TestMockCryptoCryptoImageDispatch, ReadError) {
  C_SaferCond ctx;
  auto aio_comp = librbd::io::AioCompletion::create_and_start(
    &ctx, mock_image_ctx->image_ctx, librbd::io::AIO_TYPE_READ);
  expect_image_read({{0, 4096}});
  ASSERT_TRUE(mock_crypto_image_dispatch->read(
          aio_comp, {{0, 1}}, std::move(*outer_read_result),
          mock_image_ctx->get_data_io_context(), 0, 0, {}, 0,
          &image_dispatch_flags, &dispatch_result, &on_finish, on_dispatched));
  inner_read_ctx->complete(-EIO);
  ASSERT_EQ(-EIO, ctx.wait());
}

TEST_F(TestMockCryptoCryptoImageDispatch, ReadDecryptError) {
  C_SaferCond ctx;
  auto aio_comp = librbd::io::AioCompletion::create_and_start(
    &ctx, mock_image_ctx->image_ctx, librbd::io::AIO_TYPE_READ);
  expect_image_read({{0, 4096}});
  ASSERT_TRUE(mock_crypto_image_dispatch->read(
          aio_comp, {{0, 1}}, std::move(*outer_read_result),
          mock_image_ctx->get_data_io_context(), 0, 0, {}, 0,
          &image_dispatch_flags, &dispatch_result, &on_finish, on_dispatched));
  inner_read_ctx->bl.append(std::string(4096, '1'));
  expect_decrypt(inner_read_ctx->bl, 0, -EIO);
  inner_read_ctx->complete(4096);
  ASSERT_EQ(-EIO, ctx.wait());
}

TEST_F(TestMockCryptoCryptoImageDispatch, Read) {
  C_SaferCond ctx;
  auto aio_comp = librbd::io::AioCompletion::create_and_start(
    &ctx, mock_image_ctx->image_ctx, librbd::io::AIO_TYPE_READ);
  expect_image_read({{0, 4096}, {4096, 4096}});
  ASSERT_TRUE(mock_crypto_image_dispatch->read(
          aio_comp, {{1, 5}, {4097, 5}}, std::move(*outer_read_result),
          mock_image_ctx->get_data_io_context(), 0, 0, {}, 0,
          &image_dispatch_flags, &dispatch_result, &on_finish, on_dispatched));
  ceph::bufferlist bl1, bl2, outbl1, outbl2;
  bl1.append(
          std::string(1, '1') + std::string("olleh") + std::string(4090, '1'));
  bl2.append(
          std::string(1, '1') + std::string("dlrow") + std::string(4090, '1'));
  outbl1.append(
          std::string(1, '1') + std::string("hello") + std::string(4090, '1'));
  outbl2.append(
          std::string(1, '1') + std::string("world") + std::string(4090, '1'));
  inner_read_ctx->bl.append(bl1);
  inner_read_ctx->bl.append(bl2);
  expect_decrypt(bl1, 0, 0, outbl1);
  expect_decrypt(bl2, 4096, 0, outbl2);
  inner_read_ctx->complete(8192);
  ASSERT_EQ(10, ctx.wait());
  std::string expected = "helloworld";
  ASSERT_EQ(ceph::bufferlist::static_from_string(expected) , read_bl);
}

TEST_F(TestMockCryptoCryptoImageDispatch, Write) {
  C_SaferCond ctx;
  auto aio_comp = librbd::io::AioCompletion::create_and_start(
    &ctx, mock_image_ctx->image_ctx, librbd::io::AIO_TYPE_WRITE);
  bufferlist bl;
  bl.append("1");
  ASSERT_TRUE(mock_crypto_image_dispatch->write(
          aio_comp, {{0, 1}}, std::move(bl), 0, {}, 0, &image_dispatch_flags,
          &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(-EROFS, ctx.wait());
}

TEST_F(TestMockCryptoCryptoImageDispatch, Discard) {
  C_SaferCond ctx;
  auto aio_comp = librbd::io::AioCompletion::create_and_start(
    &ctx, mock_image_ctx->image_ctx, librbd::io::AIO_TYPE_DISCARD);
  ASSERT_TRUE(mock_crypto_image_dispatch->discard(
          aio_comp, {{0, 1}},
          mock_image_ctx->image_ctx->discard_granularity_bytes, {}, 0,
          &image_dispatch_flags, &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(-EROFS, ctx.wait());
}

TEST_F(TestMockCryptoCryptoImageDispatch, WriteSame) {
  C_SaferCond ctx;
  auto aio_comp = librbd::io::AioCompletion::create_and_start(
    &ctx, mock_image_ctx->image_ctx, librbd::io::AIO_TYPE_WRITESAME);
  bufferlist bl;
  bl.append("1");
  ASSERT_TRUE(mock_crypto_image_dispatch->write_same(
          aio_comp, {{0, 1}}, std::move(bl), 0, {}, 0, &image_dispatch_flags,
          &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(-EROFS, ctx.wait());
}

TEST_F(TestMockCryptoCryptoImageDispatch, CompareAndWrite) {
  C_SaferCond ctx;
  auto aio_comp = librbd::io::AioCompletion::create_and_start(
    &ctx, mock_image_ctx->image_ctx, librbd::io::AIO_TYPE_COMPARE_AND_WRITE);
  bufferlist write_bl;
  write_bl.append("1");
  bufferlist cmp_bl;
  cmp_bl.append("1");
  uint64_t mismatch_offset;
  ASSERT_TRUE(mock_crypto_image_dispatch->compare_and_write(
          aio_comp, {{0, 1}}, std::move(cmp_bl), std::move(write_bl),
          &mismatch_offset, 0, {}, 0, &image_dispatch_flags, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(-EROFS, ctx.wait());
}

} // namespace crypto
} // namespace librbd
