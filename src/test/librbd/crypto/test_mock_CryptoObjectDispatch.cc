// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"
#include "librbd/crypto/CryptoObjectDispatch.h"
#include "librbd/io/ObjectDispatchSpec.h"

namespace librbd {

namespace util {

inline ImageCtx *get_image_ctx(MockImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

#include "librbd/crypto/CryptoObjectDispatch.cc"

namespace librbd {
namespace crypto {

using ::testing::_;
using ::testing::Invoke;

struct TestMockCryptoCryptoObjectDispatch : public TestMockFixture {
  typedef CryptoObjectDispatch<librbd::MockImageCtx> MockCryptoObjectDispatch;

  MockCryptoInterface* crypto;
  MockImageCtx* mock_image_ctx;
  MockCryptoObjectDispatch* mock_crypto_object_dispatch;

  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  C_SaferCond dispatched_cond;
  Context *on_dispatched = &dispatched_cond;
  Context *dispatcher_ctx;
  ceph::bufferlist data;
  io::DispatchResult dispatch_result;
  io::Extents extent_map;
  int object_dispatch_flags = 0;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    crypto = new MockCryptoInterface();
    mock_image_ctx = new MockImageCtx(*ictx);
    mock_crypto_object_dispatch = new MockCryptoObjectDispatch(mock_image_ctx, crypto);
    data.append("X");
  }

  void TearDown() override {
    C_SaferCond cond;
    Context *on_finish = &cond;
    mock_crypto_object_dispatch->shut_down(on_finish);
    ASSERT_EQ(0, cond.wait());

    delete mock_image_ctx;

    TestMockFixture::TearDown();
  }

  void expect_object_read() {
    EXPECT_CALL(*mock_image_ctx->io_object_dispatcher, send(_))
            .WillOnce(Invoke([this](io::ObjectDispatchSpec* spec) {
                auto* read = boost::get<io::ObjectDispatchSpec::ReadRequest>(
                        &spec->request);
                ASSERT_TRUE(read != nullptr);

                spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                dispatcher_ctx = &spec->dispatcher_ctx;
            }));
  }

  void expect_object_write() {
    EXPECT_CALL(*mock_image_ctx->io_object_dispatcher, send(_))
            .WillOnce(Invoke([this](io::ObjectDispatchSpec* spec) {
                auto* write = boost::get<io::ObjectDispatchSpec::WriteRequest>(
                        &spec->request);
                ASSERT_TRUE(write != nullptr);

                spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                dispatcher_ctx = &spec->dispatcher_ctx;
            }));
  }

  void expect_object_write_same() {
    EXPECT_CALL(*mock_image_ctx->io_object_dispatcher, send(_))
            .WillOnce(Invoke([this](io::ObjectDispatchSpec* spec) {
                auto* write_same = boost::get<
                        io::ObjectDispatchSpec::WriteSameRequest>(
                                &spec->request);
                ASSERT_TRUE(write_same != nullptr);

                spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                dispatcher_ctx = &spec->dispatcher_ctx;
            }));
  }

  void expect_encrypt(int count = 1) {
    EXPECT_CALL(*crypto, encrypt(_, _)).Times(count);
  }

  void expect_decrypt() {
    EXPECT_CALL(*crypto, decrypt(_, _));
  }
};

TEST_F(TestMockCryptoCryptoObjectDispatch, Flush) {
  ASSERT_FALSE(mock_crypto_object_dispatch->flush(
          io::FLUSH_SOURCE_USER, {}, nullptr, nullptr, &on_finish, nullptr));
  ASSERT_EQ(on_finish, &finished_cond); // not modified
  on_finish->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, Discard) {
  expect_object_write_same();
  ASSERT_TRUE(mock_crypto_object_dispatch->discard(
          0, 0, 4096, mock_image_ctx->snapc, 0, {}, &object_dispatch_flags,
          nullptr, &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);

  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0);
  ASSERT_EQ(0, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, ReadFail) {
  expect_object_read();
  ASSERT_TRUE(mock_crypto_object_dispatch->read(
      0, {{0, 4096}}, CEPH_NOSNAP, 0, {}, &data, &extent_map, nullptr,
      &object_dispatch_flags, &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish, &finished_cond);

  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(-EIO);
  ASSERT_EQ(-EIO, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, Read) {
  expect_object_read();
  ASSERT_TRUE(mock_crypto_object_dispatch->read(
          0, {{0, 4096}}, CEPH_NOSNAP, 0, {}, &data, &extent_map, nullptr,
          &object_dispatch_flags, &dispatch_result, &on_finish,
          on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish, &finished_cond);
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  expect_decrypt();
  dispatcher_ctx->complete(0);
  ASSERT_EQ(4096, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, Write) {
  expect_encrypt();
  ASSERT_TRUE(mock_crypto_object_dispatch->write(
        0, 0, std::move(data), mock_image_ctx->snapc, 0, 0, std::nullopt, {},
        nullptr, nullptr, &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_CONTINUE);
  ASSERT_EQ(on_finish, &finished_cond); // not modified
  on_finish->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, CompareAndWrite) {
  ceph::bufferlist cmp_data(data);

  expect_encrypt(2);
  ASSERT_TRUE(mock_crypto_object_dispatch->compare_and_write(
          0, 0, std::move(cmp_data), std::move(data),
          mock_image_ctx->snapc, 0, {}, nullptr, nullptr, nullptr,
          &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_CONTINUE);
  ASSERT_EQ(on_finish, &finished_cond); // not modified
  on_finish->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, WriteSame) {
  io::LightweightBufferExtents buffer_extents;

  expect_object_write();
  ASSERT_TRUE(mock_crypto_object_dispatch->write_same(
          0, 0, 4096, std::move(buffer_extents), std::move(data),
          mock_image_ctx->snapc, 0, {}, nullptr, nullptr, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);

  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0);
  ASSERT_EQ(0, dispatched_cond.wait());
}

} // namespace io
} // namespace librbd
