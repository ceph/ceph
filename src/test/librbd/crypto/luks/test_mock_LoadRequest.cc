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

#include "librbd/crypto/luks/LoadRequest.cc"

namespace librbd {
namespace crypto {
namespace luks {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

struct TestMockCryptoLuksLoadRequest : public TestMockFixture {
  typedef LoadRequest<librbd::MockImageCtx> MockLoadRequest;

  const size_t OBJECT_SIZE = 4 * 1024 * 1024;
  const char* passphrase_cstr = "password";
  std::string passphrase = passphrase_cstr;

  MockImageCtx* mock_image_ctx;
  std::unique_ptr<CryptoInterface> crypto;
  MockLoadRequest* mock_load_request;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  Context* image_read_request;
  ceph::bufferlist header_bl;
  uint64_t data_offset;
  std::string detected_format_name;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockImageCtx(*ictx);
    mock_load_request = MockLoadRequest::create(
        mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS2, std::move(passphrase),
        &crypto, &detected_format_name, on_finish);
    detected_format_name = "";
  }

  void TearDown() override {
    delete mock_image_ctx;
    TestMockFixture::TearDown();
  }

  // returns data offset in bytes
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

    data_offset = header.get_data_offset();
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

  void expect_get_image_size(uint64_t size) {
    EXPECT_CALL(*mock_image_ctx, get_image_size(_)).WillOnce(
        Return(size));
  }

  void expect_get_stripe_period(uint64_t period) {
    EXPECT_CALL(*mock_image_ctx, get_stripe_period()).WillOnce(
        Return(period));
  }
};

TEST_F(TestMockCryptoLuksLoadRequest, AES128) {
  generate_header(CRYPT_LUKS2, "aes", 32, "xts-plain64", 4096, false);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  expect_get_image_size(OBJECT_SIZE << 5);
  expect_get_stripe_period(OBJECT_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto.get(), nullptr);
  ASSERT_EQ("LUKS2", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, AES256) {
  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096, false);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  expect_get_image_size(OBJECT_SIZE << 5);
  expect_get_stripe_period(OBJECT_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto.get(), nullptr);
  ASSERT_EQ("LUKS2", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, LUKS1) {
  delete mock_load_request;
  mock_load_request = MockLoadRequest::create(
      mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS1, {passphrase_cstr}, &crypto,
      &detected_format_name, on_finish);
  generate_header(CRYPT_LUKS1, "aes", 32, "xts-plain64", 512, false);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  expect_get_image_size(OBJECT_SIZE << 5);
  expect_get_stripe_period(OBJECT_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto.get(), nullptr);
  ASSERT_EQ("LUKS1", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, LUKS1ViaLUKS) {
  delete mock_load_request;
  mock_load_request = MockLoadRequest::create(
      mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS, {passphrase_cstr}, &crypto,
      &detected_format_name, on_finish);
  generate_header(CRYPT_LUKS1, "aes", 32, "xts-plain64", 512, false);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  expect_get_image_size(OBJECT_SIZE << 5);
  expect_get_stripe_period(OBJECT_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto.get(), nullptr);
  ASSERT_EQ("LUKS1", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, UnknownFormat) {
  header_bl.append_zero(MAXIMUM_HEADER_SIZE);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();

  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);

  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(crypto.get(), nullptr);
  ASSERT_EQ("<unknown>", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, WrongFormat) {
  generate_header(CRYPT_LUKS1, "aes", 32, "xts-plain64", 512, false);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();

  expect_image_read(DEFAULT_INITIAL_READ_SIZE,
                    MAXIMUM_HEADER_SIZE - DEFAULT_INITIAL_READ_SIZE);
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  image_read_request->complete(MAXIMUM_HEADER_SIZE - DEFAULT_INITIAL_READ_SIZE);

  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(crypto.get(), nullptr);
  ASSERT_EQ("LUKS", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, UnsupportedAlgorithm) {
  generate_header(CRYPT_LUKS2, "twofish", 32, "xts-plain64", 4096, false);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
  ASSERT_EQ(crypto.get(), nullptr);
  ASSERT_EQ("LUKS2", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, UnsupportedCipherMode) {
  generate_header(CRYPT_LUKS2, "aes", 32, "cbc-essiv:sha256", 4096, false);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
  ASSERT_EQ(crypto.get(), nullptr);
  ASSERT_EQ("LUKS2", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, BadSize) {
  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096, false);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  expect_get_image_size(OBJECT_SIZE - 1);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(crypto.get(), nullptr);
  ASSERT_EQ("LUKS2", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, BadStripePattern) {
  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096, false);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  expect_get_image_size(OBJECT_SIZE << 5);
  expect_get_stripe_period(OBJECT_SIZE * 3);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(crypto.get(), nullptr);
  ASSERT_EQ("LUKS2", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, HeaderBiggerThanInitialRead) {
  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096, false);
  mock_load_request->set_initial_read_size(4096);
  expect_image_read(0, 4096);
  mock_load_request->send();

  expect_get_image_size(OBJECT_SIZE << 5);
  expect_get_stripe_period(OBJECT_SIZE);
  expect_image_read(4096, MAXIMUM_HEADER_SIZE - 4096);
  image_read_request->complete(4096); // complete initial read

  image_read_request->complete(MAXIMUM_HEADER_SIZE - 4096);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto.get(), nullptr);
  ASSERT_EQ("LUKS2", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, LUKS1FormattedClone) {
  mock_image_ctx->parent = mock_image_ctx;
  delete mock_load_request;
  mock_load_request = MockLoadRequest::create(
      mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS1, {passphrase_cstr}, &crypto,
      &detected_format_name, on_finish);
  generate_header(CRYPT_LUKS1, "aes", 64, "xts-plain64", 512, true);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  expect_get_image_size(OBJECT_SIZE << 5);
  expect_get_stripe_period(OBJECT_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto.get(), nullptr);
  ASSERT_EQ("LUKS1", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, LUKS2FormattedClone) {
  mock_image_ctx->parent = mock_image_ctx;
  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096, true);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  expect_get_image_size(OBJECT_SIZE << 5);
  expect_get_stripe_period(OBJECT_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto.get(), nullptr);
  ASSERT_EQ("LUKS2", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, KeyslotsBiggerThanInitialRead) {
  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096, false);
  mock_load_request->set_initial_read_size(16384);
  expect_image_read(0, 16384);
  mock_load_request->send();

  expect_get_image_size(OBJECT_SIZE << 5);
  expect_get_stripe_period(OBJECT_SIZE);
  expect_image_read(16384, data_offset - 16384);
  image_read_request->complete(16384); // complete initial read

  image_read_request->complete(data_offset - 16384);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto.get(), nullptr);
  ASSERT_EQ("LUKS2", detected_format_name);
}

TEST_F(TestMockCryptoLuksLoadRequest, WrongPassphrase) {
  delete mock_load_request;
  mock_load_request = MockLoadRequest::create(
      mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS2, "wrong", &crypto,
      &detected_format_name, on_finish);

  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096, false);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  expect_get_image_size(OBJECT_SIZE << 5);
  expect_get_stripe_period(OBJECT_SIZE);
  mock_load_request->send();

  // crypt_volume_key_get will fail, we will retry reading more
  expect_image_read(DEFAULT_INITIAL_READ_SIZE,
                    data_offset - DEFAULT_INITIAL_READ_SIZE);
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);

  image_read_request->complete(data_offset - DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(-EPERM, finished_cond.wait());
  ASSERT_EQ(crypto.get(), nullptr);
  ASSERT_EQ("LUKS2", detected_format_name);
}

} // namespace luks
} // namespace crypto
} // namespace librbd
