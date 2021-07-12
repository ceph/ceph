// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"
#include "librbd/io/Utils.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
    static MockTestImageCtx *s_instance;

    static MockTestImageCtx *create(const std::string &image_name,
                                    const std::string &image_id,
                                    const char *snap, librados::IoCtx &p,
                                    bool read_only) {
      ceph_assert(s_instance != nullptr);
      return s_instance;
    }

    std::map<uint64_t, io::CopyupRequest<MockTestImageCtx>*> copyup_list;

    MockTestImageCtx(librbd::ImageCtx &image_ctx)
            : librbd::MockImageCtx(image_ctx) {
    }
};

MockTestImageCtx *MockTestImageCtx::s_instance = nullptr;

} // anonymous namespace

} // namespace librbd

#include "librbd/AsyncObjectThrottle.cc"
template class librbd::AsyncObjectThrottle<librbd::MockTestImageCtx>;

namespace librbd {
namespace io {
namespace util {

namespace {

struct Mock {
    static Mock* s_instance;

    Mock() {
      s_instance = this;
    }

    MOCK_METHOD4(trigger_copyup,
            bool(MockTestImageCtx *image_ctx, uint64_t object_no,
                 IOContext io_context, Context* on_finish));
};

Mock *Mock::s_instance = nullptr;

} // anonymous namespace

template <> bool trigger_copyup(
        MockTestImageCtx *image_ctx, uint64_t object_no, IOContext io_context,
        Context* on_finish) {

  return Mock::s_instance->trigger_copyup(
          image_ctx, object_no, io_context, on_finish);
}

} // namespace util
} // namespace io

namespace util {

inline ImageCtx *get_image_ctx(MockTestImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

#include "librbd/crypto/FlattenRequest.cc"

namespace librbd {

namespace operation {

template <>
class MetadataRemoveRequest<MockTestImageCtx> {
public:
  Context *on_finish = nullptr;
  std::string key;
  static MetadataRemoveRequest *s_instance;
  static MetadataRemoveRequest *create(
          MockTestImageCtx &image_ctx, Context *on_finish,
          const std::string &key) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    s_instance->key = key;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  MetadataRemoveRequest() {
    s_instance = this;
  }
};

MetadataRemoveRequest<MockTestImageCtx> *MetadataRemoveRequest<
        MockTestImageCtx>::s_instance = nullptr;

} // namespace operation

namespace crypto {

struct MockTestEncryptionFormat : EncryptionFormat<MockTestImageCtx> {

  MOCK_METHOD2(format, void(MockTestImageCtx* ictx, Context* on_finish));
  MOCK_METHOD2(load, void(MockTestImageCtx* ictx, Context* on_finish));
  MOCK_METHOD2(flatten, void(MockTestImageCtx* ictx, Context* on_finish));
  MOCK_METHOD0(get_crypto, ceph::ref_t<CryptoInterface>());
};

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

struct TestMockCryptoFlattenRequest : public TestMockFixture {
  typedef FlattenRequest<librbd::MockTestImageCtx> MockFlattenRequest;
  typedef operation::MetadataRemoveRequest<
          MockTestImageCtx> MockMetadataRemoveRequest;
  typedef io::util::Mock MockUtils;

  MockTestImageCtx* mock_image_ctx;
  MockTestImageCtx* mock_raw_image_ctx;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  MockCryptoInterface* crypto;
  MockTestEncryptionFormat* mock_encryption_format;
  MockFlattenRequest* mock_flatten_request;
  MockMetadataRemoveRequest mock_metadata_remove_request;
  Context* flatten_context;
  Context* copyup_context;
  MockUtils mock_utils;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockTestImageCtx(*ictx);
    mock_raw_image_ctx = new MockTestImageCtx(*ictx);
    MockTestImageCtx::s_instance = mock_raw_image_ctx;
    mock_encryption_format = new MockTestEncryptionFormat();
    crypto = new MockCryptoInterface();
    mock_image_ctx->crypto = crypto;
    mock_flatten_request = MockFlattenRequest::create(
            mock_image_ctx, mock_encryption_format, on_finish);
  }

  void TearDown() override {
    crypto->put();
    if (MockTestImageCtx::s_instance != nullptr) {
      delete MockTestImageCtx::s_instance;
    }
    delete mock_encryption_format;
    delete mock_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_raw_image_open(int r = 0) {
    EXPECT_CALL(*mock_raw_image_ctx->state, open(
            false, _)).WillOnce(
                    WithArg<1>(Invoke([this, r](Context* ctx) {
                      MockTestImageCtx::s_instance = nullptr;
                      ctx->complete(r);
                    })));
  }

  void expect_raw_image_close(int r = 0) {
    EXPECT_CALL(*mock_raw_image_ctx->state, close(
            _)).WillOnce(
                    WithArg<0>(Invoke([this, r](Context* ctx) {
                      ctx->complete(r);
                      delete mock_raw_image_ctx;
                    })));
  }

  void expect_flatten_header() {
    EXPECT_CALL(mock_utils,
                trigger_copyup(mock_raw_image_ctx, 0, _, _)).WillOnce(
                        WithArg<3>(Invoke([this](Context* ctx) {
                          copyup_context = ctx;
                          return true;
                        })));
  }

  void expect_metadata_remove(int r = 0) {
    EXPECT_CALL(mock_metadata_remove_request, send()).WillOnce(
                    Invoke([this, r]() {
                      mock_metadata_remove_request.on_finish->complete(r);
                      return true;
    }));
  }

  void expect_crypto_flatten(int r = 0) {
    EXPECT_CALL(*mock_encryption_format, flatten(
            mock_raw_image_ctx, _)).WillOnce(
                    WithArg<1>(Invoke([this, r](Context* ctx) {
                      ctx->complete(r);
                    })));
  }
};

TEST_F(TestMockCryptoFlattenRequest, CryptoNotLoaded) {
  delete mock_flatten_request;
  mock_flatten_request = MockFlattenRequest::create(
          mock_image_ctx, nullptr, on_finish);
  mock_image_ctx->crypto = nullptr;
  mock_flatten_request->send();
  ASSERT_EQ(0, finished_cond.wait());
}

TEST_F(TestMockCryptoFlattenRequest, NoEncryptionFormat) {
  delete mock_flatten_request;
  mock_flatten_request = MockFlattenRequest::create(
          mock_image_ctx, nullptr, on_finish);
  mock_flatten_request->send();
  ASSERT_EQ(-EINVAL, finished_cond.wait());
}

TEST_F(TestMockCryptoFlattenRequest, ErrorOpeningRawImage) {
  expect_raw_image_open(-EIO);
  mock_flatten_request->send();
  ASSERT_EQ(-EIO, finished_cond.wait());
  delete mock_raw_image_ctx;
}

TEST_F(TestMockCryptoFlattenRequest, FailFlattenHeader) {
  expect_raw_image_open();
  expect_flatten_header();
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_raw_image_close();
  copyup_context->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
}

TEST_F(TestMockCryptoFlattenRequest, FailMetadataRemove) {
  expect_raw_image_open();
  expect_flatten_header();
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_metadata_remove(-EIO);
  expect_raw_image_close(-ENOTSUP);
  copyup_context->complete(0);
  ASSERT_EQ(-EIO, finished_cond.wait());
}

TEST_F(TestMockCryptoFlattenRequest, FailCryptoFlatten) {
  expect_raw_image_open();
  expect_flatten_header();
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_metadata_remove();
  expect_crypto_flatten(-EIO);
  expect_raw_image_close();
  copyup_context->complete(0);
  ASSERT_EQ(-EIO, finished_cond.wait());
}

TEST_F(TestMockCryptoFlattenRequest, FailClosingRawImage) {
  expect_raw_image_open();
  expect_flatten_header();
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_metadata_remove();
  expect_crypto_flatten();
  expect_raw_image_close(-EIO);
  copyup_context->complete(0);
  ASSERT_EQ(-EIO, finished_cond.wait());
}

TEST_F(TestMockCryptoFlattenRequest, Success) {
  expect_raw_image_open();
  expect_flatten_header();
  mock_flatten_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_metadata_remove();
  expect_crypto_flatten();
  expect_raw_image_close();
  copyup_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
}

} // namespace crypto
} // namespace librbd
