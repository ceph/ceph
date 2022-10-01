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
  ceph::ref_t<CryptoInterface> crypto;
  MockLoadRequest* mock_load_request;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  Context* image_read_request;
  ceph::bufferlist header_bl;
  uint64_t data_offset;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockImageCtx(*ictx);
    crypto = nullptr;
    mock_load_request = MockLoadRequest::create(
            mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS2, std::move(passphrase),
            &crypto, on_finish);
  }

  void TearDown() override {
    delete mock_image_ctx;
    if (crypto != nullptr) {
      crypto->put();
      crypto = nullptr;
    }
    TestMockFixture::TearDown();
  }

  // returns data offset in bytes
  void generate_header(const char* type, const char* alg, size_t key_size,
                       const char* cipher_mode, uint32_t sector_size) {
    Header header(mock_image_ctx->cct);

    ASSERT_EQ(0, header.init());
    ASSERT_EQ(0, header.format(type, alg, nullptr, key_size, cipher_mode,
                               sector_size, OBJECT_SIZE, true));
    ASSERT_EQ(0, header.add_keyslot(passphrase_cstr, strlen(passphrase_cstr)));
    ASSERT_LE(0, header.read(&header_bl));

    data_offset = header.get_data_offset();
  }

  void expect_image_read(uint64_t offset, uint64_t length) {
    EXPECT_CALL(*mock_image_ctx->io_image_dispatcher, send(_))
            .WillOnce(Invoke([this, offset,
                              length](io::ImageDispatchSpec* spec) {
                auto* read = boost::get<io::ImageDispatchSpec::Read>(
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
};

TEST_F(TestMockCryptoLuksLoadRequest, AES128) {
  generate_header(CRYPT_LUKS2, "aes", 32, "xts-plain64", 4096);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto, nullptr);
}

TEST_F(TestMockCryptoLuksLoadRequest, AES256) {
  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto, nullptr);
}

TEST_F(TestMockCryptoLuksLoadRequest, LUKS1) {
  delete mock_load_request;
  mock_load_request = MockLoadRequest::create(
          mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS1, {passphrase_cstr},
          &crypto, on_finish);
  generate_header(CRYPT_LUKS1, "aes", 32, "xts-plain64", 512);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto, nullptr);
}

TEST_F(TestMockCryptoLuksLoadRequest, WrongFormat) {
  generate_header(CRYPT_LUKS1, "aes", 32, "xts-plain64", 512);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();

  expect_image_read(DEFAULT_INITIAL_READ_SIZE,
                    MAXIMUM_HEADER_SIZE - DEFAULT_INITIAL_READ_SIZE);
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE); // complete 1st read

  image_read_request->complete(
          MAXIMUM_HEADER_SIZE - DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(crypto, nullptr);
}

TEST_F(TestMockCryptoLuksLoadRequest, UnsupportedAlgorithm) {
  generate_header(CRYPT_LUKS2, "twofish", 32, "xts-plain64", 4096);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
  ASSERT_EQ(crypto, nullptr);
}

TEST_F(TestMockCryptoLuksLoadRequest, UnsupportedCipherMode) {
  generate_header(CRYPT_LUKS2, "aes", 32, "cbc-essiv:sha256", 4096);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
  ASSERT_EQ(crypto, nullptr);
}

TEST_F(TestMockCryptoLuksLoadRequest, HeaderBiggerThanInitialRead) {
  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096);
  mock_load_request->set_initial_read_size(4096);
  expect_image_read(0, 4096);
  mock_load_request->send();

  expect_image_read(4096, MAXIMUM_HEADER_SIZE - 4096);
  image_read_request->complete(4096); // complete initial read

  image_read_request->complete(MAXIMUM_HEADER_SIZE - 4096);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto, nullptr);
}

TEST_F(TestMockCryptoLuksLoadRequest, KeyslotsBiggerThanInitialRead) {
  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096);
  mock_load_request->set_initial_read_size(16384);
  expect_image_read(0, 16384);
  mock_load_request->send();

  expect_image_read(16384, data_offset - 16384);
  image_read_request->complete(16384); // complete initial read

  image_read_request->complete(data_offset - 16384);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_NE(crypto, nullptr);
}

TEST_F(TestMockCryptoLuksLoadRequest, WrongPassphrase) {
  delete mock_load_request;
  mock_load_request = MockLoadRequest::create(
        mock_image_ctx, RBD_ENCRYPTION_FORMAT_LUKS2, "wrong", &crypto,
        on_finish);

  generate_header(CRYPT_LUKS2, "aes", 64, "xts-plain64", 4096);
  expect_image_read(0, DEFAULT_INITIAL_READ_SIZE);
  mock_load_request->send();

  // crypt_volume_key_get will fail, we will retry reading more
  expect_image_read(DEFAULT_INITIAL_READ_SIZE,
                    data_offset - DEFAULT_INITIAL_READ_SIZE);
  image_read_request->complete(DEFAULT_INITIAL_READ_SIZE);

  image_read_request->complete(data_offset - DEFAULT_INITIAL_READ_SIZE);
  ASSERT_EQ(-EPERM, finished_cond.wait());
  ASSERT_EQ(crypto, nullptr);
}

} // namespace luks
} // namespace crypto
} // namespace librbd
