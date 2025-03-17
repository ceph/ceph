// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoObjectDispatch.h"
#include "librbd/crypto/Utils.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"
#include "test/librbd/mock/crypto/MockEncryptionFormat.h"
#include "test/librbd/mock/io/MockObjectDispatch.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }

  MockTestImageCtx *parent = nullptr;
};

} // anonymous namespace

namespace util {

inline ImageCtx *get_image_ctx(MockTestImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util

namespace crypto {
namespace util {

template <>
void set_crypto(MockTestImageCtx *image_ctx,
                std::unique_ptr<MockEncryptionFormat> encryption_format) {
  image_ctx->encryption_format = std::move(encryption_format);
}

} // namespace util
} // namespace crypto
} // namespace librbd

#include "librbd/crypto/LoadRequest.cc"

namespace librbd {
namespace crypto {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArgs;

struct TestMockCryptoLoadRequest : public TestMockFixture {
  typedef LoadRequest<librbd::MockTestImageCtx> MockLoadRequest;

  MockTestImageCtx* mock_image_ctx;
  MockTestImageCtx* mock_parent_image_ctx;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  MockEncryptionFormat* mock_encryption_format;
  MockEncryptionFormat* cloned_encryption_format;
  Context* load_context;
  MockLoadRequest* mock_load_request;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockTestImageCtx(*ictx);
    mock_parent_image_ctx = new MockTestImageCtx(*ictx);
    mock_image_ctx->parent = mock_parent_image_ctx;
    mock_encryption_format = new MockEncryptionFormat();
    std::vector<std::unique_ptr<MockEncryptionFormat>> formats;
    formats.emplace_back(mock_encryption_format);
    mock_load_request = MockLoadRequest::create(
          mock_image_ctx, std::move(formats), on_finish);
  }

  void TearDown() override {
    delete mock_image_ctx;
    delete mock_parent_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_test_journal_feature(MockTestImageCtx* ictx,
                                   bool has_journal = false) {
    EXPECT_CALL(*ictx, test_features(
            RBD_FEATURE_JOURNALING)).WillOnce(Return(has_journal));
  }

  void expect_encryption_load(MockEncryptionFormat* encryption_format,
                              MockTestImageCtx* ictx,
                              std::string detected_format = "SOMEFORMAT") {
    EXPECT_CALL(*encryption_format, load(
            ictx, _, _)).WillOnce(
                    WithArgs<1, 2>(Invoke([this, detected_format](
                            std::string* detected_format_name, Context* ctx) {
                      if (!detected_format.empty()) {
                        *detected_format_name = detected_format;
                      }
                      load_context = ctx;
    })));
  }

  void expect_encryption_format_clone(MockEncryptionFormat* encryption_format) {
    cloned_encryption_format = new MockEncryptionFormat();
    EXPECT_CALL(*encryption_format, clone()).WillOnce(
            Invoke([this]() {
              return std::unique_ptr<MockEncryptionFormat>(
                      cloned_encryption_format);
            }));
  }

  void expect_image_flush(int r = 0) {
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

  void expect_invalidate_cache(int r = 0) {
    EXPECT_CALL(*mock_image_ctx->io_image_dispatcher,
                 invalidate_cache(_)).WillOnce(
                         Invoke([r](Context* ctx) {
                           ctx->complete(r);
                         }));
  }
};

TEST_F(TestMockCryptoLoadRequest, NoFormats) {
  delete mock_load_request;
  std::vector<std::unique_ptr<MockEncryptionFormat>> formats;
  mock_load_request = MockLoadRequest::create(
        mock_image_ctx, std::move(formats), on_finish);
  mock_load_request->send();
  ASSERT_EQ(-EINVAL, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, CryptoAlreadyLoaded) {
  mock_image_ctx->encryption_format.reset(new MockEncryptionFormat());
  mock_load_request->send();
  ASSERT_EQ(-EEXIST, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, JournalEnabled) {
  expect_test_journal_feature(mock_image_ctx, true);
  mock_load_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, JournalEnabledOnParent) {
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx, true);
  mock_load_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, LoadFail) {
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx);
  expect_image_flush();
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(-EINVAL);
  ASSERT_EQ(-EINVAL, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, Success) {
  delete mock_load_request;
  mock_image_ctx->parent = nullptr;
  mock_encryption_format = new MockEncryptionFormat();
  std::vector<std::unique_ptr<MockEncryptionFormat>> formats;
  formats.emplace_back(mock_encryption_format);
  mock_load_request = MockLoadRequest::create(
        mock_image_ctx, std::move(formats), on_finish);
  expect_test_journal_feature(mock_image_ctx);
  expect_image_flush();
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_invalidate_cache();
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(mock_encryption_format, mock_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLoadRequest, FlushFail) {
  delete mock_load_request;
  mock_image_ctx->parent = nullptr;
  mock_encryption_format = new MockEncryptionFormat();
  std::vector<std::unique_ptr<MockEncryptionFormat>> formats;
  formats.emplace_back(mock_encryption_format);
  mock_load_request = MockLoadRequest::create(
        mock_image_ctx, std::move(formats), on_finish);
  expect_test_journal_feature(mock_image_ctx);
  expect_image_flush(-EIO);
  mock_load_request->send();
  ASSERT_EQ(-EIO, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, InvalidateCacheFail) {
  delete mock_load_request;
  mock_image_ctx->parent = nullptr;
  mock_encryption_format = new MockEncryptionFormat();
  std::vector<std::unique_ptr<MockEncryptionFormat>> formats;
  formats.emplace_back(mock_encryption_format);
  mock_load_request = MockLoadRequest::create(
        mock_image_ctx, std::move(formats), on_finish);
  expect_test_journal_feature(mock_image_ctx);
  expect_image_flush();
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_invalidate_cache(-EIO);
  load_context->complete(0);
  ASSERT_EQ(-EIO, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, LoadClonedEncryptedParent) {
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx);
  expect_image_flush();
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_format_clone(mock_encryption_format);
  expect_encryption_load(cloned_encryption_format, mock_parent_image_ctx);
  load_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_invalidate_cache();
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(mock_encryption_format, mock_image_ctx->encryption_format.get());
  ASSERT_EQ(cloned_encryption_format,
            mock_parent_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLoadRequest, LoadClonedParentFail) {
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx);
  expect_image_flush();
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_format_clone(mock_encryption_format);
  expect_encryption_load(cloned_encryption_format, mock_parent_image_ctx);
  load_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->encryption_format.get());
  ASSERT_EQ(nullptr, mock_parent_image_ctx->encryption_format.get());
}


TEST_F(TestMockCryptoLoadRequest, LoadClonedPlaintextParent) {
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx);
  expect_image_flush();
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_format_clone(mock_encryption_format);
  expect_encryption_load(
          cloned_encryption_format, mock_parent_image_ctx,
          LoadRequest<MockImageCtx>::UNKNOWN_FORMAT);
  load_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_invalidate_cache();
  load_context->complete(-EINVAL);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(mock_encryption_format, mock_image_ctx->encryption_format.get());
  ASSERT_EQ(nullptr, mock_parent_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLoadRequest, LoadClonedParentDetectionError) {
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx);
  expect_image_flush();
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_format_clone(mock_encryption_format);
  expect_encryption_load(
          cloned_encryption_format, mock_parent_image_ctx, "");
  load_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(-EINVAL);
  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->encryption_format.get());
  ASSERT_EQ(nullptr, mock_parent_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLoadRequest, LoadParentFail) {
  delete mock_load_request;
  mock_encryption_format = new MockEncryptionFormat();
  auto mock_parent_encryption_format = new MockEncryptionFormat();
  std::vector<std::unique_ptr<MockEncryptionFormat>> formats;
  formats.emplace_back(mock_encryption_format);
  formats.emplace_back(mock_parent_encryption_format);
  mock_load_request = MockLoadRequest::create(
        mock_image_ctx,
        std::move(formats),
        on_finish);
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx);
  expect_image_flush();
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_load(mock_parent_encryption_format, mock_parent_image_ctx);
  load_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(-EINVAL);
  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->encryption_format.get());
  ASSERT_EQ(nullptr, mock_parent_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLoadRequest, EncryptedParent) {
  delete mock_load_request;
  mock_encryption_format = new MockEncryptionFormat();
  auto mock_parent_encryption_format = new MockEncryptionFormat();
  std::vector<std::unique_ptr<MockEncryptionFormat>> formats;
  formats.emplace_back(mock_encryption_format);
  formats.emplace_back(mock_parent_encryption_format);
  mock_load_request = MockLoadRequest::create(
        mock_image_ctx,
        std::move(formats),
        on_finish);
  expect_test_journal_feature(mock_image_ctx);
  expect_test_journal_feature(mock_parent_image_ctx);
  expect_image_flush();
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_encryption_load(mock_parent_encryption_format, mock_parent_image_ctx);
  load_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_invalidate_cache();
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(mock_encryption_format, mock_image_ctx->encryption_format.get());
  ASSERT_EQ(mock_parent_encryption_format,
            mock_parent_image_ctx->encryption_format.get());
}

TEST_F(TestMockCryptoLoadRequest, TooManyFormats) {
  delete mock_load_request;
  mock_encryption_format = new MockEncryptionFormat();
  auto mock_parent_encryption_format = new MockEncryptionFormat();
  std::vector<std::unique_ptr<MockEncryptionFormat>> formats;
  formats.emplace_back(mock_encryption_format);
  formats.emplace_back(mock_parent_encryption_format);
  mock_image_ctx->parent = nullptr;
  mock_load_request = MockLoadRequest::create(
        mock_image_ctx,
        std::move(formats),
        on_finish);
  expect_test_journal_feature(mock_image_ctx);
  expect_image_flush();
  expect_encryption_load(mock_encryption_format, mock_image_ctx);
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(0);
  ASSERT_EQ(-EINVAL, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->encryption_format.get());
}

} // namespace crypto
} // namespace librbd
