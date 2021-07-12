// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"
#include "test/librbd/mock/crypto/MockEncryptionFormat.h"
#include "librbd/crypto/Utils.h"

namespace librbd {
namespace util {

inline ImageCtx *get_image_ctx(MockImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

#include "librbd/crypto/FormatRequest.cc"

namespace librbd {

namespace operation {

template <>
class MetadataSetRequest<MockImageCtx> {
public:
  Context *on_finish = nullptr;
  std::string key;
  std::string value;
  static MetadataSetRequest *s_instance;
  static MetadataSetRequest *create(
          MockImageCtx &image_ctx, Context *on_finish, const std::string &key,
          const std::string &value) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    s_instance->key = key;
    s_instance->value = value;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  MetadataSetRequest() {
    s_instance = this;
  }
};

MetadataSetRequest<MockImageCtx> *MetadataSetRequest<
        MockImageCtx>::s_instance = nullptr;

} // namespace operation

namespace crypto {

namespace util {

template <> void set_crypto(
        MockImageCtx *image_ctx, ceph::ref_t<CryptoInterface> crypto) {
  image_ctx->crypto = crypto.get();
}

} // namespace util

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

template <>
struct ShutDownCryptoRequest<MockImageCtx> {
  Context *on_finish = nullptr;
  static ShutDownCryptoRequest *s_instance;
  static ShutDownCryptoRequest *create(
          MockImageCtx* image_ctx, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  ShutDownCryptoRequest() {
    s_instance = this;
  }
};

ShutDownCryptoRequest<MockImageCtx> *ShutDownCryptoRequest<
        MockImageCtx>::s_instance = nullptr;

struct TestMockCryptoFormatRequest : public TestMockFixture {
  typedef FormatRequest<librbd::MockImageCtx> MockFormatRequest;
  typedef ShutDownCryptoRequest<MockImageCtx> MockShutDownCryptoRequest;
  typedef operation::MetadataSetRequest<MockImageCtx> MockMetadataSetRequest;

  MockImageCtx* mock_image_ctx;
  MockImageCtx* mock_parent_image_ctx;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  MockEncryptionFormat* mock_encryption_format;
  Context* format_context;
  MockCryptoInterface* crypto;
  MockCryptoInterface* old_crypto;
  MockCryptoInterface* parent_crypto;
  MockFormatRequest* mock_format_request;
  std::string key = std::string(64, '0');
  std::string parent_key = std::string(64, '1');

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockImageCtx(*ictx);
    mock_parent_image_ctx = new MockImageCtx(*ictx);
    mock_encryption_format = new MockEncryptionFormat();
    crypto = new MockCryptoInterface();
    old_crypto = new MockCryptoInterface();
    parent_crypto = new MockCryptoInterface();
    mock_image_ctx->crypto = old_crypto;
    mock_parent_image_ctx->crypto = parent_crypto;
    mock_format_request = MockFormatRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  }

  void TearDown() override {
    crypto->put();
    old_crypto->put();
    parent_crypto->put();
    delete mock_image_ctx;
    delete mock_parent_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_test_journal_feature(bool has_journal=false) {
    EXPECT_CALL(*mock_image_ctx, test_features(
            RBD_FEATURE_JOURNALING)).WillOnce(Return(has_journal));
  }

  void expect_encryption_format() {
    EXPECT_CALL(*mock_encryption_format, format(
            mock_image_ctx, _)).WillOnce(
                    WithArg<1>(Invoke([this](Context* ctx) {
                      format_context = ctx;
    })));
  }

  void expect_image_flush(int r) {
    EXPECT_CALL(*mock_image_ctx->io_image_dispatcher, send(_)).WillOnce(
            Invoke([r](io::ImageDispatchSpec* spec) {
              ASSERT_TRUE(boost::get<io::ImageDispatchSpec::Flush>(
                      &spec->request) != nullptr);
              spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
              spec->aio_comp->set_request_count(1);
              spec->aio_comp->add_request();
              spec->aio_comp->complete_request(r);
            }));
  }
};

TEST_F(TestMockCryptoFormatRequest, JournalEnabled) {
  expect_test_journal_feature(true);
  mock_format_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
  ASSERT_EQ(old_crypto, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, ClonedAlreadyFormatted) {
  mock_image_ctx->is_formatted_clone = true;
  expect_test_journal_feature(false);
  mock_format_request->send();
  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(old_crypto, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, FailShutDownCrypto) {
  expect_test_journal_feature(false);
  MockShutDownCryptoRequest mock_shutdown_crypto_request;
  EXPECT_CALL(mock_shutdown_crypto_request, send());
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  mock_shutdown_crypto_request.on_finish->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_EQ(old_crypto, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, FormatFail) {
  mock_image_ctx->crypto = nullptr;
  expect_test_journal_feature(false);
  expect_encryption_format();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  format_context->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, Success) {
  mock_image_ctx->crypto = nullptr;
  expect_test_journal_feature(false);
  expect_encryption_format();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(0);
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  format_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(crypto, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, FailFlush) {
  mock_image_ctx->crypto = nullptr;
  expect_test_journal_feature(false);
  expect_encryption_format();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(-EIO);
  format_context->complete(0);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, CryptoAlreadyLoaded) {
  expect_test_journal_feature(false);
  MockShutDownCryptoRequest mock_shutdown_crypto_request;
  EXPECT_CALL(mock_shutdown_crypto_request, send());
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_format();
  mock_shutdown_crypto_request.on_finish->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(0);
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  format_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(crypto, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, PlaintextParent) {
  mock_parent_image_ctx->crypto = nullptr;
  mock_image_ctx->crypto = nullptr;
  mock_image_ctx->parent = mock_parent_image_ctx;
  expect_test_journal_feature(false);
  expect_encryption_format();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(0);
  MockMetadataSetRequest mock_metadata_set_request;
  EXPECT_CALL(mock_metadata_set_request, send());
  format_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  ASSERT_STREQ(".rbd_encryption_parent_cryptor",
               mock_metadata_set_request.key.c_str());

  ParentCryptoParams cryptor;
  auto encoded_cryptor = bufferlist::static_from_string(
          mock_metadata_set_request.value);
  auto it = encoded_cryptor.cbegin();
  cryptor.decode(it);
  ASSERT_STREQ("", cryptor.wrapped_key.c_str());
  ASSERT_EQ(0, cryptor.block_size);
  ASSERT_EQ(0, cryptor.data_offset);

  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  mock_metadata_set_request.on_finish->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(crypto, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, FailMetadataSet) {
  mock_parent_image_ctx->crypto = nullptr;
  mock_image_ctx->parent = mock_parent_image_ctx;
  expect_test_journal_feature(false);
  MockShutDownCryptoRequest mock_shutdown_crypto_request;
  EXPECT_CALL(mock_shutdown_crypto_request, send());
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_format();
  mock_shutdown_crypto_request.on_finish->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(0);
  MockMetadataSetRequest mock_metadata_set_request;
  EXPECT_CALL(mock_metadata_set_request, send());
  format_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  mock_metadata_set_request.on_finish->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_EQ(old_crypto, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, EncryptedParent) {
  mock_image_ctx->parent = mock_parent_image_ctx;
  expect_test_journal_feature(false);
  MockShutDownCryptoRequest mock_shutdown_crypto_request;
  EXPECT_CALL(mock_shutdown_crypto_request, send());
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_format();
  mock_shutdown_crypto_request.on_finish->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(0);
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  EXPECT_CALL(*crypto, get_key()).WillOnce(Return(
          reinterpret_cast<unsigned char*>(key.data())));
  EXPECT_CALL(*crypto, get_key_length()).WillOnce(Return(key.size()));
  EXPECT_CALL(*parent_crypto, get_key()).WillOnce(Return(
          reinterpret_cast<unsigned char*>(parent_key.data())));
  EXPECT_CALL(*parent_crypto, get_key_length()).WillOnce(Return(
          parent_key.size()));
  MockMetadataSetRequest mock_metadata_set_request;
  EXPECT_CALL(mock_metadata_set_request, send());
  format_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  ASSERT_STREQ(".rbd_encryption_parent_cryptor",
               mock_metadata_set_request.key.c_str());

  ParentCryptoParams cryptor;
  auto encoded_cryptor = bufferlist::static_from_string(
          mock_metadata_set_request.value);
  auto it = encoded_cryptor.cbegin();
  cryptor.decode(it);
  std::string decoded_key;
  int r = util::key_wrap(
          mock_image_ctx->cct, CipherMode::CIPHER_MODE_DEC,
          reinterpret_cast<unsigned char*>(key.data()), key.size(),
          reinterpret_cast<unsigned char*>(cryptor.wrapped_key.data()),
          cryptor.wrapped_key.size(), &decoded_key);
  ASSERT_EQ(0, r);
  EXPECT_EQ(parent_key, decoded_key);
  ASSERT_EQ(4096, cryptor.block_size);
  ASSERT_EQ(4 * 1024 * 1024, cryptor.data_offset);

  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  mock_metadata_set_request.on_finish->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(crypto, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, InsufficientWrappingKeyLength) {
  mock_image_ctx->parent = mock_parent_image_ctx;
  expect_test_journal_feature(false);
  MockShutDownCryptoRequest mock_shutdown_crypto_request;
  EXPECT_CALL(mock_shutdown_crypto_request, send());
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_format();
  mock_shutdown_crypto_request.on_finish->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(0);
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  EXPECT_CALL(*crypto, get_key()).WillOnce(Return(
          reinterpret_cast<unsigned char*>(key.data())));
  EXPECT_CALL(*crypto, get_key_length()).WillOnce(Return(16));
  EXPECT_CALL(*parent_crypto, get_key()).WillOnce(Return(
          reinterpret_cast<unsigned char*>(parent_key.data())));
  EXPECT_CALL(*parent_crypto, get_key_length()).WillOnce(Return(
          parent_key.size()));
  format_context->complete(0);
  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(old_crypto, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoFormatRequest, InvalidWrappedKeyLength) {
  mock_image_ctx->parent = mock_parent_image_ctx;
  expect_test_journal_feature(false);
  MockShutDownCryptoRequest mock_shutdown_crypto_request;
  EXPECT_CALL(mock_shutdown_crypto_request, send());
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_format();
  mock_shutdown_crypto_request.on_finish->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_image_flush(0);
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  EXPECT_CALL(*crypto, get_key()).WillOnce(Return(
          reinterpret_cast<unsigned char*>(key.data())));
  EXPECT_CALL(*crypto, get_key_length()).WillOnce(Return(key.size()));
  EXPECT_CALL(*parent_crypto, get_key()).WillOnce(Return(
          reinterpret_cast<unsigned char*>(parent_key.data())));
  EXPECT_CALL(*parent_crypto, get_key_length()).WillOnce(Return(20));
  format_context->complete(0);
  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(old_crypto, mock_image_ctx->crypto);
}

} // namespace crypto
} // namespace librbd
