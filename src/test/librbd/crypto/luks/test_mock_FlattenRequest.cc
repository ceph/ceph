// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"
#include "test/librbd/mock/crypto/MockEncryptionFormat.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace util {

inline ImageCtx *get_image_ctx(MockImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

#include "librbd/crypto/luks/FlattenRequest.cc"

namespace librbd {
namespace crypto {
namespace luks {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

struct TestMockCryptoLuksFlattenRequest : public TestMockFixture {
  typedef FlattenRequest<MockTestImageCtx> MockFlattenRequest;

  const size_t OBJECT_SIZE = 4 * 1024 * 1024;
  const uint64_t DATA_OFFSET = MockCryptoInterface::DATA_OFFSET;
  const char* passphrase_cstr = "password";
  std::string passphrase = passphrase_cstr;

  MockTestImageCtx* mock_image_ctx;
  MockFlattenRequest* mock_flatten_request;
  MockEncryptionFormat* mock_encryption_format;
  MockCryptoInterface mock_crypto;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  Context* image_read_request;
  io::AioCompletion* aio_comp;
  ceph::bufferlist header_bl;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockTestImageCtx(*ictx);
    mock_encryption_format = new MockEncryptionFormat();
    mock_image_ctx->encryption_format.reset(mock_encryption_format);
    mock_flatten_request = MockFlattenRequest::create(
            mock_image_ctx, on_finish);
  }

  void TearDown() override {
    delete mock_image_ctx;
    TestMockFixture::TearDown();
  }

  void generate_header(const char* type, const char* alg, size_t key_size,
                       const char* cipher_mode, uint32_t sector_size,
                       bool magic_switched) {
    Header header(mock_image_ctx->cct);

    ASSERT_EQ(0, header.init());
    ASSERT_EQ(0, header.format(type, alg, nullptr, key_size, cipher_mode,
                               sector_size, OBJECT_SIZE, true));
    ASSERT_EQ(0, header.add_keyslot(passphrase_cstr, strlen(passphrase_cstr)));
    ASSERT_LT(0, header.read(&header_bl));
    if (magic_switched) {
      ASSERT_EQ(0, Magic::replace_magic(mock_image_ctx->cct, header_bl));
    }
  }

  void expect_get_crypto() {
    EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(
            Return(&mock_crypto));
  }

  void expect_image_read(uint64_t offset, uint64_t length) {
    EXPECT_CALL(*mock_image_ctx->io_image_dispatcher, send(_))
            .WillOnce(Invoke([this, offset,
                              length](io::ImageDispatchSpec* spec) {
                auto* read = std::get_if<io::ImageDispatchSpec::Read>(
                        &spec->request);
                ASSERT_TRUE(read != nullptr);

                ASSERT_EQ(1, spec->image_extents.size());
                ASSERT_EQ(offset, spec->image_extents[0].first);
                ASSERT_EQ(length, spec->image_extents[0].second);

                spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                auto aio_comp = spec->aio_comp;
                aio_comp->set_request_count(1);
                aio_comp->read_result = std::move(read->read_result);
                aio_comp->read_result.set_image_extents(spec->image_extents);
                auto ctx = new io::ReadResult::C_ImageReadRequest(
                        aio_comp, 0, spec->image_extents);
                if (header_bl.length() < offset + length) {
                  header_bl.append_zero(offset + length - header_bl.length());
                }
                ctx->bl.substr_of(header_bl, offset, length);
                image_read_request = ctx;
            }));
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

                // patch header_bl with write
                bufferlist bl;
                bl.substr_of(header_bl, write->bl.length(),
                             header_bl.length() - write->bl.length());
                header_bl = write->bl;
                header_bl.claim_append(bl);
            }));
  }

  void expect_image_flush(int r) {
    EXPECT_CALL(*mock_image_ctx->io_image_dispatcher, send(_)).WillOnce(
            Invoke([r](io::ImageDispatchSpec* spec) {
              ASSERT_TRUE(std::get_if<io::ImageDispatchSpec::Flush>(
                      &spec->request) != nullptr);
              spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
              spec->aio_comp->set_request_count(1);
              spec->aio_comp->add_request();
              spec->aio_comp->complete_request(r);
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

  void verify_header(const char* expected_format) {
    Header header(mock_image_ctx->cct);

    ASSERT_EQ(0, header.init());
    ASSERT_EQ(0, header.write(header_bl));
    ASSERT_EQ(0, header.load(expected_format));
  }
};

TEST_F(TestMockCryptoLuksFlattenRequest, LUKS1) {
  generate_header(CRYPT_LUKS1, "aes", 32, "xts-plain64", 512, true);
  expect_get_crypto();
  expect_image_read(0, DATA_OFFSET);
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_write();
  image_read_request->complete(DATA_OFFSET);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(0);
  complete_aio(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NO_FATAL_FAILURE(verify_header(CRYPT_LUKS1));
  ASSERT_EQ(mock_encryption_format, mock_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLuksFlattenRequest, LUKS2) {
  generate_header(CRYPT_LUKS2, "aes", 32, "xts-plain64", 4096, true);
  expect_get_crypto();
  expect_image_read(0, DATA_OFFSET);
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_write();
  image_read_request->complete(DATA_OFFSET);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(0);
  complete_aio(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NO_FATAL_FAILURE(verify_header(CRYPT_LUKS2));
  ASSERT_EQ(mock_encryption_format, mock_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLuksFlattenRequest, FailedRead) {
  generate_header(CRYPT_LUKS2, "aes", 32, "xts-plain64", 4096, true);
  expect_get_crypto();
  expect_image_read(0, DATA_OFFSET);
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  image_read_request->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_EQ(mock_encryption_format, mock_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLuksFlattenRequest, AlreadyFlattened) {
  generate_header(CRYPT_LUKS2, "aes", 32, "xts-plain64", 4096, false);
  expect_get_crypto();
  expect_image_read(0, DATA_OFFSET);
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_write();
  image_read_request->complete(DATA_OFFSET);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(0);
  complete_aio(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NO_FATAL_FAILURE(verify_header(CRYPT_LUKS2));
  ASSERT_EQ(mock_encryption_format, mock_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLuksFlattenRequest, FailedWrite) {
  generate_header(CRYPT_LUKS2, "aes", 32, "xts-plain64", 4096, true);
  expect_get_crypto();
  expect_image_read(0, DATA_OFFSET);
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_write();
  image_read_request->complete(DATA_OFFSET);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  complete_aio(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_EQ(mock_encryption_format, mock_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLuksFlattenRequest, FailedFlush) {
  generate_header(CRYPT_LUKS2, "aes", 32, "xts-plain64", 4096, true);
  expect_get_crypto();
  expect_image_read(0, DATA_OFFSET);
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_write();
  image_read_request->complete(DATA_OFFSET);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(-EIO);
  complete_aio(0);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_EQ(mock_encryption_format, mock_image_ctx->encryption_format.get());
}

} // namespace luks
} // namespace crypto
} // namespace librbd
