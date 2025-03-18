// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"

namespace librbd {
namespace util {

inline ImageCtx *get_image_ctx(MockImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

#include "librbd/crypto/luks/FormatRequest.cc"

namespace librbd {
namespace crypto {
namespace luks {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

struct TestMockCryptoLuksFormatRequest : public TestMockFixture {
  typedef FormatRequest<librbd::MockImageCtx> MockFormatRequest;

  const size_t OBJECT_SIZE = 4 * 1024 * 1024;
  const size_t IMAGE_SIZE = 1024 * 1024 * 1024;
  const char* passphrase_cstr = "password";
  std::string passphrase = passphrase_cstr;

  MockImageCtx* mock_image_ctx;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  io::AioCompletion* aio_comp;
  ceph::bufferlist header_bl;
  std::unique_ptr<CryptoInterface> crypto;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockImageCtx(*ictx);
  }

  void TearDown() override {
    delete mock_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_get_stripe_period() {
    EXPECT_CALL(*mock_image_ctx, get_stripe_period()).WillOnce(Return(
            OBJECT_SIZE));
  }

  void expect_get_image_size(uint64_t image_size) {
    EXPECT_CALL(*mock_image_ctx, get_image_size(CEPH_NOSNAP)).WillOnce(Return(
            image_size));
  }

  void expect_image_write() {
    EXPECT_CALL(*mock_image_ctx->io_image_dispatcher, send(_))
            .WillOnce(Invoke([this](io::ImageDispatchSpec* spec) {
                auto* write = std::get_if<io::ImageDispatchSpec::Write>(
                        &spec->request);
                ASSERT_TRUE(write != nullptr);

                ASSERT_EQ(1, spec->image_extents.size());
                ASSERT_EQ(0, spec->image_extents[0].first);
                ASSERT_GT(spec->image_extents[0].second, 0);

                spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                aio_comp = spec->aio_comp;
                header_bl = write->bl;
            }));
  }

  void complete_aio(int r) {
    if (r < 0) {
      aio_comp->fail(r);
    } else {
      aio_comp->set_request_count(1);
      aio_comp->add_request();
      aio_comp->complete_request(r);
    }
  }

  void verify_header(const char* expected_format, size_t expected_key_length,
                     uint64_t expected_sector_size, bool magic_switched) {
    Header header(mock_image_ctx->cct);

    ASSERT_EQ(0, header.init());

    if (magic_switched) {
      Header non_switched_header(mock_image_ctx->cct);
      ASSERT_EQ(0, non_switched_header.init());
      ASSERT_EQ(0, non_switched_header.write(header_bl));
      ASSERT_EQ(-EINVAL, non_switched_header.load(expected_format));
      ASSERT_EQ(0, Magic::replace_magic(mock_image_ctx->cct, header_bl));
    }
    ASSERT_EQ(0, header.write(header_bl));
    ASSERT_EQ(0, header.load(expected_format));

    ASSERT_EQ(expected_sector_size, header.get_sector_size());
    ASSERT_EQ(0, header.get_data_offset() % OBJECT_SIZE);

    char volume_key[64];
    size_t volume_key_size = sizeof(volume_key);
    ASSERT_EQ(0, header.read_volume_key(
            passphrase_cstr, strlen(passphrase_cstr),
            reinterpret_cast<char*>(volume_key), &volume_key_size));

    ASSERT_EQ(expected_key_length, crypto->get_key_length());
    ASSERT_EQ(0, std::memcmp(
            volume_key, crypto->get_key(), expected_key_length));
    ASSERT_EQ(expected_sector_size, crypto->get_block_size());
    ASSERT_EQ(header.get_data_offset(), crypto->get_data_offset());
  }
};

TEST_F(TestMockCryptoLuksFormatRequest, LUKS1) {
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS1,
          RBD_ENCRYPTION_ALGORITHM_AES128, std::move(passphrase), &crypto,
          on_finish, true);
  expect_get_stripe_period();
  expect_get_image_size(IMAGE_SIZE);
  expect_image_write();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  complete_aio(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NO_FATAL_FAILURE(verify_header(CRYPT_LUKS1, 32, 512, false));
}

TEST_F(TestMockCryptoLuksFormatRequest, AES128) {
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS2,
          RBD_ENCRYPTION_ALGORITHM_AES128, std::move(passphrase), &crypto,
          on_finish, true);
  expect_get_stripe_period();
  expect_get_image_size(IMAGE_SIZE);
  expect_image_write();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  complete_aio(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NO_FATAL_FAILURE(verify_header(CRYPT_LUKS2, 32, 4096, false));
}

TEST_F(TestMockCryptoLuksFormatRequest, AES256) {
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS2,
          RBD_ENCRYPTION_ALGORITHM_AES256, std::move(passphrase), &crypto,
          on_finish, true);
  expect_get_stripe_period();
  expect_get_image_size(IMAGE_SIZE);
  expect_image_write();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  complete_aio(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NO_FATAL_FAILURE(verify_header(CRYPT_LUKS2, 64, 4096, false));
}

TEST_F(TestMockCryptoLuksFormatRequest, LUKS1OnCloned) {
  mock_image_ctx->parent = mock_image_ctx;
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS1,
          RBD_ENCRYPTION_ALGORITHM_AES256, std::move(passphrase), &crypto,
          on_finish, true);
  expect_get_stripe_period();
  expect_get_image_size(IMAGE_SIZE);
  expect_image_write();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  complete_aio(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NO_FATAL_FAILURE(verify_header(CRYPT_LUKS1, 64, 512, true));
}

TEST_F(TestMockCryptoLuksFormatRequest, LUKS2OnCloned) {
  mock_image_ctx->parent = mock_image_ctx;
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS2,
          RBD_ENCRYPTION_ALGORITHM_AES256, std::move(passphrase), &crypto,
          on_finish, true);
  expect_get_stripe_period();
  expect_get_image_size(IMAGE_SIZE);
  expect_image_write();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  complete_aio(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NO_FATAL_FAILURE(verify_header(CRYPT_LUKS2, 64, 4096, true));
}

TEST_F(TestMockCryptoLuksFormatRequest, ImageTooSmall) {
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS2,
          RBD_ENCRYPTION_ALGORITHM_AES256, std::move(passphrase), &crypto,
          on_finish, true);
  expect_get_stripe_period();
  expect_get_image_size(1024*1024);
  mock_format_request->send();
  ASSERT_EQ(-ENOSPC, finished_cond.wait());
}

TEST_F(TestMockCryptoLuksFormatRequest, WriteFail) {
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS2,
          RBD_ENCRYPTION_ALGORITHM_AES256, std::move(passphrase), &crypto,
          on_finish, true);
  expect_get_stripe_period();
  expect_get_image_size(IMAGE_SIZE);
  expect_image_write();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  complete_aio(-123);
  ASSERT_EQ(-123, finished_cond.wait());
}

} // namespace luks
} // namespace crypto
} // namespace librbd
