// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd_types.h"
#include "common/ceph_mutex.h"
#include "librbd/migration/HttpClient.h"
#include "librbd/migration/HttpStream.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "json_spirit/json_spirit.h"
#include <boost/beast/http.hpp>

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace migration {

template <>
struct HttpClient<MockTestImageCtx> {
  static HttpClient* s_instance;
  static HttpClient* create(MockTestImageCtx*, const std::string&) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD1(open, void(Context*));
  MOCK_METHOD1(close, void(Context*));
  MOCK_METHOD2(get_size, void(uint64_t*, Context*));

  MOCK_METHOD3(issue, void(const boost::beast::http::request<
                             boost::beast::http::empty_body>&,
                           boost::beast::http::response<
                             boost::beast::http::string_body>*,
                           Context*));

  template <class Body, typename Completion>
  void issue(boost::beast::http::request<Body>&& request,
             Completion&& completion) {
    struct ContextImpl : public Context {
      boost::beast::http::response<boost::beast::http::string_body> res;
      Completion completion;

      ContextImpl(Completion&& completion) : completion(std::move(completion)) {
      }

      void finish(int r) override {
        completion(r, std::move(res));
      }
    };

    auto ctx = new ContextImpl(std::move(completion));
    issue(request, &ctx->res, ctx);
  }

  HttpClient() {
    s_instance = this;
  }
};

HttpClient<MockTestImageCtx>* HttpClient<MockTestImageCtx>::s_instance = nullptr;

} // namespace migration

namespace util {

inline ImageCtx *get_image_ctx(MockTestImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

#include "librbd/migration/HttpStream.cc"

namespace librbd {
namespace migration {

using ::testing::_;
using ::testing::Invoke;
using ::testing::InSequence;

class TestMockMigrationHttpStream : public TestMockFixture {
public:
  typedef HttpStream<MockTestImageCtx> MockHttpStream;
  typedef HttpClient<MockTestImageCtx> MockHttpClient;

  librbd::ImageCtx *m_image_ctx;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));
    json_object["url"] = "http://some.site/file";
  }

  void expect_open(MockHttpClient& mock_http_client, int r) {
    EXPECT_CALL(mock_http_client, open(_))
      .WillOnce(Invoke([r](Context* ctx) { ctx->complete(r); }));
  }

  void expect_close(MockHttpClient& mock_http_client, int r) {
    EXPECT_CALL(mock_http_client, close(_))
      .WillOnce(Invoke([r](Context* ctx) { ctx->complete(r); }));
  }

  void expect_get_size(MockHttpClient& mock_http_client, uint64_t size, int r) {
    EXPECT_CALL(mock_http_client, get_size(_, _))
      .WillOnce(Invoke([size, r](uint64_t* out_size, Context* ctx) {
        *out_size = size;
        ctx->complete(r);
      }));
  }

  void expect_read(MockHttpClient& mock_http_client, io::Extent byte_extent,
                   const std::string& data, int r) {
    std::stringstream byte_range;
    byte_range << byte_extent.first << "-"
               << (byte_extent.first + byte_extent.second - 1);

    EXPECT_CALL(mock_http_client, issue(_, _, _))
      .WillOnce(Invoke(
        [data, r, byte_range=byte_range.str()]
        (const boost::beast::http::request<
           boost::beast::http::empty_body>& request,
         boost::beast::http::response<
           boost::beast::http::string_body>* response, Context* ctx) {
        ASSERT_EQ("bytes=" + byte_range,
                  request[boost::beast::http::field::range]);

        response->result(boost::beast::http::status::partial_content);
        response->set(boost::beast::http::field::content_range,
                      "bytes " + byte_range + "/*");
        response->body() = data;
        response->prepare_payload();
        ctx->complete(r);
      }));
  }

  json_spirit::mObject json_object;
};

TEST_F(TestMockMigrationHttpStream, OpenClose) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_http_client = new MockHttpClient();
  expect_open(*mock_http_client, 0);

  expect_close(*mock_http_client, 0);

  MockHttpStream mock_http_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_http_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  mock_http_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationHttpStream, GetSize) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_http_client = new MockHttpClient();
  expect_open(*mock_http_client, 0);

  expect_get_size(*mock_http_client, 128, 0);

  expect_close(*mock_http_client, 0);

  MockHttpStream mock_http_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_http_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  uint64_t size;
  mock_http_stream.get_size(&size, &ctx2);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(128, size);

  C_SaferCond ctx3;
  mock_http_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationHttpStream, Read) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_http_client = new MockHttpClient();
  expect_open(*mock_http_client, 0);

  expect_read(*mock_http_client, {0, 128}, std::string(128, '1'), 0);
  expect_read(*mock_http_client, {256, 64}, std::string(64, '2'), 0);

  expect_close(*mock_http_client, 0);

  MockHttpStream mock_http_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_http_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  bufferlist bl;
  mock_http_stream.read({{0, 128}, {256, 64}}, &bl, &ctx2);
  ASSERT_EQ(192, ctx2.wait());

  bufferlist expect_bl;
  expect_bl.append(std::string(128, '1'));
  expect_bl.append(std::string(64, '2'));
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  mock_http_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

} // namespace migration
} // namespace librbd
