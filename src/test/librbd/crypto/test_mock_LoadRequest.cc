// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoObjectDispatch.h"
#include "librbd/crypto/Utils.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"
#include "test/librbd/mock/crypto/MockEncryptionFormat.h"
#include "test/librbd/mock/io/MockObjectDispatch.h"

namespace librbd {
namespace crypto {

template <>
struct CryptoObjectDispatch<MockImageCtx> : public io::MockObjectDispatch {

  static CryptoObjectDispatch* create(
          MockImageCtx* image_ctx,ceph::ref_t<CryptoInterface> crypto) {
    return new CryptoObjectDispatch();
  }

  CryptoObjectDispatch() {
  }
};

} // namespace crypto
} // namespace librbd

#include "librbd/crypto/LoadRequest.cc"

namespace librbd {
namespace crypto {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;
using ::testing::WithArgs;

struct TestMockCryptoLoadRequest : public TestMockFixture {
  typedef LoadRequest<librbd::MockImageCtx> MockLoadRequest;

  MockImageCtx* mock_image_ctx;
  MockImageCtx* mock_parent_image_ctx;
  MockImageCtx* mock_grandparent_image_ctx;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  MockEncryptionFormat* mock_encryption_format;
  MockCryptoInterface* crypto;
  Context* load_context;
  MockLoadRequest* mock_load_request;
  const std::string key1 = std::string(64, '0');
  const std::string key2 = std::string(64, '1');
  const std::string key3 = std::string(64, '2');

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockImageCtx(*ictx);
    mock_parent_image_ctx = new MockImageCtx(*ictx);
    mock_grandparent_image_ctx = new MockImageCtx(*ictx);
    mock_image_ctx->parent = mock_parent_image_ctx;
    mock_parent_image_ctx->parent = mock_grandparent_image_ctx;
    mock_encryption_format = new MockEncryptionFormat();
    crypto = new MockCryptoInterface();
    mock_load_request = MockLoadRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  }

  void TearDown() override {
    crypto->put();
    delete mock_image_ctx;
    delete mock_parent_image_ctx;
    delete mock_grandparent_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_test_journal_feature() {
    expect_test_journal_feature(mock_image_ctx, false);
    expect_test_journal_feature(mock_parent_image_ctx, false);
    expect_test_journal_feature(mock_grandparent_image_ctx, false);
  }

  void expect_test_journal_feature(MockImageCtx* ctx, bool has_journal=false) {
    EXPECT_CALL(*ctx, test_features(
            RBD_FEATURE_JOURNALING)).WillOnce(Return(has_journal));
  }

  void expect_encryption_load() {
    EXPECT_CALL(*mock_encryption_format, load(
            mock_image_ctx, _)).WillOnce(
                    WithArgs<1>(Invoke([this](Context* ctx) {
                      load_context = ctx;
    })));
  }

  void expect_metadata_get(
          MockImageCtx* mock_image_ctx, bufferlist&& bl, int r) {
    bufferlist in_bl;
    ceph::encode(".rbd_encryption_parent_cryptor", in_bl);

    bufferlist out_bl;
    ceph::encode(bl.to_str(), out_bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx->md_ctx),
                exec(mock_image_ctx->header_oid, _, StrEq("rbd"),
                     StrEq("metadata_get"), ContentsEqual(in_bl), _, _, _))
                     .WillOnce(DoAll(WithArg<5>(CopyInBufferlist(out_bl)),
                             Return(r)));
  }

  void expect_metadata_get_none(MockImageCtx* mock_image_ctx, int r) {
    expect_metadata_get(mock_image_ctx, bufferlist(), r);
  }

  void expect_metadata_get_plain(MockImageCtx* ctx) {
    ParentCryptoParams cryptor("", 0, 0);
    bufferlist cryptor_bl;
    cryptor.encode(cryptor_bl);
    expect_metadata_get(ctx, std::move(cryptor_bl), 0);
  }

  void expect_metadata_get(MockImageCtx* ctx, const std::string& key,
                           const std::string& wrapping_key) {
    std::string wrapped_key;
    int r = util::key_wrap(
            ctx->cct, CipherMode::CIPHER_MODE_ENC,
            reinterpret_cast<const unsigned char*>(wrapping_key.data()),
            wrapping_key.size(),
            reinterpret_cast<const unsigned char*>(key.data()),
            key.size(), &wrapped_key);
    ASSERT_EQ(0, r);
    ParentCryptoParams cryptor(wrapped_key, 4096, 4 * 1024 * 1024);
    bufferlist cryptor_bl;
    cryptor.encode(cryptor_bl);
    expect_metadata_get(ctx, std::move(cryptor_bl), 0);
  }

};

TEST_F(TestMockCryptoLoadRequest, CryptoAlreadyLoaded) {
  mock_image_ctx->crypto = new MockCryptoInterface();
  mock_load_request->send();
  ASSERT_EQ(-EEXIST, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, JournalEnabled) {
  expect_test_journal_feature(mock_image_ctx, true);
  mock_load_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, JournalEnabledOnAncestor) {
  expect_test_journal_feature(mock_image_ctx, false);
  expect_test_journal_feature(mock_parent_image_ctx, false);
  expect_test_journal_feature(mock_grandparent_image_ctx, true);
  mock_load_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, LoadFail) {
  expect_test_journal_feature();
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, Success) {
  mock_image_ctx->parent = nullptr;
  expect_test_journal_feature(mock_image_ctx, false);
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(crypto, mock_image_ctx->crypto);
}

TEST_F(TestMockCryptoLoadRequest, PlaintextParent) {
  expect_test_journal_feature();
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  expect_metadata_get_plain(mock_image_ctx);
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(crypto, mock_image_ctx->crypto);
  ASSERT_EQ(nullptr, mock_parent_image_ctx->crypto);
  ASSERT_EQ(nullptr, mock_grandparent_image_ctx->crypto);
}

TEST_F(TestMockCryptoLoadRequest, NoFormattedAncestors) {
  expect_test_journal_feature();
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  expect_metadata_get_none(mock_image_ctx, -ENOENT);
  expect_metadata_get_none(mock_parent_image_ctx, -ENOENT);
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(crypto, mock_image_ctx->crypto);
  ASSERT_EQ(crypto, mock_parent_image_ctx->crypto);
  ASSERT_EQ(crypto, mock_grandparent_image_ctx->crypto);
}

TEST_F(TestMockCryptoLoadRequest, AllAncestorsFormatted) {
  expect_test_journal_feature();
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  expect_metadata_get(mock_image_ctx, key2, key1);
  EXPECT_CALL(*crypto, get_key()).WillOnce(Return(
          reinterpret_cast<const unsigned char*>(key1.data())));
  EXPECT_CALL(*crypto, get_key_length()).WillOnce(Return(key1.size()));
  expect_metadata_get(mock_parent_image_ctx, key3, key2);
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(crypto, mock_image_ctx->crypto);
  std::string parent_key(
          reinterpret_cast<const char*>(
                  mock_parent_image_ctx->crypto->get_key()),
          mock_parent_image_ctx->crypto->get_key_length());
  ASSERT_EQ(key2, parent_key);
  std::string grandparent_key(
          reinterpret_cast<const char*>(
                  mock_grandparent_image_ctx->crypto->get_key()),
          mock_grandparent_image_ctx->crypto->get_key_length());
  ASSERT_EQ(key3, grandparent_key);
}

TEST_F(TestMockCryptoLoadRequest, AllFormattedExceptForGrandchild) {
  expect_test_journal_feature();
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  expect_metadata_get_none(mock_image_ctx, -ENOENT);
  EXPECT_CALL(*crypto, get_key()).WillOnce(Return(
          reinterpret_cast<const unsigned char*>(key1.data())));
  EXPECT_CALL(*crypto, get_key_length()).WillOnce(Return(key1.size()));
  expect_metadata_get(mock_parent_image_ctx, key2, key1);
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(crypto, mock_image_ctx->crypto);
  ASSERT_EQ(crypto, mock_parent_image_ctx->crypto);
  std::string grandparent_key(
          reinterpret_cast<const char*>(
                  mock_grandparent_image_ctx->crypto->get_key()),
          mock_grandparent_image_ctx->crypto->get_key_length());
  ASSERT_EQ(key2, grandparent_key);
}

TEST_F(TestMockCryptoLoadRequest, FailReadMetadata) {
  expect_test_journal_feature();
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  expect_metadata_get(mock_image_ctx, key2, key1);
  EXPECT_CALL(*crypto, get_key()).WillOnce(Return(
          reinterpret_cast<const unsigned char*>(key1.data())));
  EXPECT_CALL(*crypto, get_key_length()).WillOnce(Return(key1.size()));
  expect_metadata_get_none(mock_parent_image_ctx, -EIO);
  load_context->complete(0);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->crypto);
  ASSERT_EQ(nullptr, mock_parent_image_ctx->crypto);
  ASSERT_EQ(nullptr, mock_grandparent_image_ctx->crypto);
}

} // namespace crypto
} // namespace librbd
