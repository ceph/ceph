// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd_types.h"
#include "common/ceph_mutex.h"
#include "librbd/migration/HttpClient.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include <unistd.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace util {

inline ImageCtx *get_image_ctx(MockTestImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

#include "librbd/migration/HttpClient.cc"

using EmptyHttpRequest = boost::beast::http::request<
  boost::beast::http::empty_body>;
using HttpResponse = boost::beast::http::response<
  boost::beast::http::string_body>;

namespace boost {
namespace beast {
namespace http {

template <typename Body>
bool operator==(const boost::beast::http::request<Body>& lhs,
                const boost::beast::http::request<Body>& rhs) {
  return (lhs.method() == rhs.method() &&
          lhs.target() == rhs.target());
}

template <typename Body>
bool operator==(const boost::beast::http::response<Body>& lhs,
                const boost::beast::http::response<Body>& rhs) {
  return (lhs.result() == rhs.result() &&
          lhs.body() == rhs.body());
}

} // namespace http
} // namespace beast
} // namespace boost

namespace librbd {
namespace migration {

using ::testing::Invoke;

class TestMockMigrationHttpClient : public TestMockFixture {
public:
  typedef HttpClient<MockTestImageCtx> MockHttpClient;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));

    create_acceptor(false);
  }

  void TearDown() override {
    m_acceptor.reset();

    TestMockFixture::TearDown();
  }

  // if we have a racing where another thread manages to bind and listen the
  // port picked by this acceptor, try again.
  static constexpr int MAX_BIND_RETRIES = 60;

  void create_acceptor(bool reuse) {
    for (int retries = 0;; retries++) {
      try {
	m_acceptor.emplace(*m_image_ctx->asio_engine,
                       boost::asio::ip::tcp::endpoint(
                         boost::asio::ip::tcp::v4(), m_server_port), reuse);
	// yay!
	break;
      } catch (const boost::system::system_error& e) {
	if (retries == MAX_BIND_RETRIES) {
	  throw;
	}
	if (e.code() != boost::system::errc::address_in_use) {
	  throw;
	}
      }
      // backoff a little bit
      sleep(1);
    }
    m_server_port = m_acceptor->local_endpoint().port();
  }

  std::string get_local_url(UrlScheme url_scheme) {
    std::stringstream sstream;
    switch (url_scheme) {
    case URL_SCHEME_HTTP:
      sstream << "http://127.0.0.1";
      break;
    case URL_SCHEME_HTTPS:
      sstream << "https://localhost";
      break;
    default:
      ceph_assert(false);
      break;
    }

    sstream << ":" << m_server_port << "/target";
    return sstream.str();
  }

  void client_accept(boost::asio::ip::tcp::socket* socket, bool close,
                     Context* on_connect) {
    m_acceptor->async_accept(
      boost::asio::make_strand(m_image_ctx->asio_engine->get_executor()),
      [socket, close, on_connect]
      (auto ec, boost::asio::ip::tcp::socket in_socket) {
        if (close) {
          in_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both);
        } else {
          ASSERT_FALSE(ec) << "Unexpected error: " << ec;
          *socket = std::move(in_socket);
        }
        on_connect->complete(0);
      });
  }

  template <typename Body>
  void client_read_request(boost::asio::ip::tcp::socket& socket,
                           boost::beast::http::request<Body>& expected_req) {
    boost::beast::http::request<Body> req;
    boost::beast::error_code ec;
    boost::beast::http::read(socket, m_buffer, req, ec);
    ASSERT_FALSE(ec) << "Unexpected errror: " << ec;

    expected_req.target("/target");
    ASSERT_EQ(expected_req, req);
  }

  void client_write_response(boost::asio::ip::tcp::socket& socket,
                             HttpResponse& expected_res) {
    expected_res.set(boost::beast::http::field::server,
                     BOOST_BEAST_VERSION_STRING);
    expected_res.set(boost::beast::http::field::content_type, "text/plain");
    expected_res.content_length(expected_res.body().size());
    expected_res.prepare_payload();

    boost::beast::error_code ec;
    boost::beast::http::write(socket, expected_res, ec);
    ASSERT_FALSE(ec) << "Unexpected errror: " << ec;
  }

  template <typename Stream>
  void client_ssl_handshake(Stream& stream, bool ignore_failure,
                            Context* on_handshake) {
    stream.async_handshake(
      boost::asio::ssl::stream_base::server,
      [ignore_failure, on_handshake](auto ec) {
        ASSERT_FALSE(!ignore_failure && ec) << "Unexpected error: " << ec;
        on_handshake->complete(-ec.value());
      });
  }

  template <typename Stream>
  void client_ssl_shutdown(Stream& stream, Context* on_shutdown) {
    stream.async_shutdown(
      [on_shutdown](auto ec) {
        ASSERT_FALSE(ec) << "Unexpected error: " << ec;
        on_shutdown->complete(-ec.value());
      });
  }

  void load_server_certificate(boost::asio::ssl::context& ctx) {
    ctx.set_options(
        boost::asio::ssl::context::default_workarounds |
        boost::asio::ssl::context::no_sslv2 |
        boost::asio::ssl::context::single_dh_use);
    ctx.use_certificate_chain(
        boost::asio::buffer(CERTIFICATE.data(), CERTIFICATE.size()));
    ctx.use_private_key(
        boost::asio::buffer(KEY.data(), KEY.size()),
        boost::asio::ssl::context::file_format::pem);
    ctx.use_tmp_dh(
        boost::asio::buffer(DH.data(), DH.size()));
  }

  // dummy self-signed cert for localhost
  const std::string CERTIFICATE =
      "-----BEGIN CERTIFICATE-----\n"
      "MIIDXzCCAkegAwIBAgIUYH6rAaq66LC6yJ3XK1WEMIfmY4cwDQYJKoZIhvcNAQEL\n"
      "BQAwPzELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAlZBMQ8wDQYDVQQHDAZNY0xlYW4x\n"
      "EjAQBgNVBAMMCWxvY2FsaG9zdDAeFw0yMDExMDIyMTM5NTVaFw00ODAzMjAyMTM5\n"
      "NTVaMD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJWQTEPMA0GA1UEBwwGTWNMZWFu\n"
      "MRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK\n"
      "AoIBAQCeRkyxjP0eNHxzj4/R+Bg/31p7kEjB5d/LYtrzBIYNe+3DN8gdReixEpR5\n"
      "lgTLDsl8gfk2HRz4cnAiseqYL6GKtw/cFadzLyXTbW4iavmTWiGYw/8RJlKunbhA\n"
      "hDjM6H99ysLf0NS6t14eK+bEJIW1PiTYRR1U5I4kSIjpCX7+nJVuwMEZ2XBpN3og\n"
      "nHhv2hZYTdzEkQEyZHz4V/ApfD7rlja5ecd/vJfPJeA8nudnGCh3Uo6f8I9TObAj\n"
      "8hJdfRiRBvnA4NnkrMrxW9UtVjScnw9Xia11FM/IGJIgMpLQ5dqBw930p6FxMYtn\n"
      "tRD1AF9sT+YjoCaHv0hXZvBEUEF3AgMBAAGjUzBRMB0GA1UdDgQWBBTQoIiX3+p/\n"
      "P4Xz2vwERz6pbjPGhzAfBgNVHSMEGDAWgBTQoIiX3+p/P4Xz2vwERz6pbjPGhzAP\n"
      "BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQCVKoYAw+D1qqWRDSh3\n"
      "2KlKMnT6sySo7XmReGArj8FTKiZUprByj5CfAtaiDSdPOpcg3EazWbdasZbMmSQm\n"
      "+jpe5WoKnxL9b12lwwUYHrLl6RlrDHVkIVlXLNbJFY5TpfjvZfHpwVAygF3fnbgW\n"
      "PPuODUNAS5NDwST+t29jBZ/wwU0pyW0CS4K5d3XMGHBc13j2V/FyvmsZ5xfA4U9H\n"
      "oEnmZ/Qm+FFK/nR40rTAZ37cuv4ysKFtwvatNgTfHGJwaBUkKFdDbcyxt9abCi6x\n"
      "/K+ScoJtdIeVcfx8Fnc5PNtSpy8bHI3Zy4IEyw4kOqwwI1h37iBafZ2WdQkTxlAx\n"
      "JIDj\n"
      "-----END CERTIFICATE-----\n";
  const std::string KEY =
      "-----BEGIN PRIVATE KEY-----\n"
      "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCeRkyxjP0eNHxz\n"
      "j4/R+Bg/31p7kEjB5d/LYtrzBIYNe+3DN8gdReixEpR5lgTLDsl8gfk2HRz4cnAi\n"
      "seqYL6GKtw/cFadzLyXTbW4iavmTWiGYw/8RJlKunbhAhDjM6H99ysLf0NS6t14e\n"
      "K+bEJIW1PiTYRR1U5I4kSIjpCX7+nJVuwMEZ2XBpN3ognHhv2hZYTdzEkQEyZHz4\n"
      "V/ApfD7rlja5ecd/vJfPJeA8nudnGCh3Uo6f8I9TObAj8hJdfRiRBvnA4NnkrMrx\n"
      "W9UtVjScnw9Xia11FM/IGJIgMpLQ5dqBw930p6FxMYtntRD1AF9sT+YjoCaHv0hX\n"
      "ZvBEUEF3AgMBAAECggEACCaYpoAbPOX5Dr5y6p47KXboIvrgNFQRPVke62rtOF6M\n"
      "dQQ3YwKJpCzPxp8qKgbd63KKEfZX2peSHMdKzIGPcSRSRcQ7tlvUN9on1M/rgGIg\n"
      "3swhI5H0qhdnOLNWdX73qdO6S2pmuiLdTvJ11N4IoLfNj/GnPAr1Ivs1ScL6bkQv\n"
      "UybaNQ/g2lB0tO7vUeVe2W/AqsIb1eQlf2g+SH7xRj2bGQkr4cWTylqfiVoL/Xic\n"
      "QVTCks3BWaZhYIhTFgvqVhXZpp52O9J+bxsWJItKQrrCBemxwp82xKbiW/KoI9L1\n"
      "wSnKvxx7Q3RUN5EvXeOpTRR8QIpBoxP3TTeoj+EOMQKBgQDQb/VfLDlLgfYJpgRC\n"
      "hKCLW90un9op3nA2n9Dmm9TTLYOmUyiv5ub8QDINEw/YK/NE2JsTSUk2msizqTLL\n"
      "Z82BFbz9kPlDbJ5MgxG5zXeLvOLurAFmZk/z5JJO+65PKjf0QVLncSAJvMCeNFuC\n"
      "2yZrEzbrItrjQsN6AedWdx6TTwKBgQDCZAsSI3lQgOh2q1GSxjuIzRAc7JnSGBvD\n"
      "nG8+SkfKAy7BWe638772Dgx8KYO7TLI4zlm8c9Tr/nkZsGWmM5S2DMI69PWOQWNa\n"
      "R6QzOFFwNg2JETH7ow+x8+9Q9d3WsPzROz3r5uDXgEk0glthaymVoPILFOiYpz3r\n"
      "heUbd6mFWQKBgQCCJBVJGhyf54IOHij0u0heGrpr/QTDNY5MnNZa1hs4y2cydyOl\n"
      "SH8aKp7ViPxQlYhriO6ySQS8YkJD4rXDSImIOmFo1Ja9oVjpHsD3iLFGf2YVbTHm\n"
      "lKUA+8raI8x+wzZyfELeHMTLL534aWpltp0zJ6kXgQi38pyIVh3x36gogwKBgQCt\n"
      "nba5k49VVFzLKEXqBjzD+QqMGtFjcH7TnZNJmgQ2K9OFgzIPf5atomyKNHXgQibn\n"
      "T32cMAQaZqR4SjDvWSBX3FtZVtE+Ja57woKn8IPj6ZL7Oa1fpwpskIbM01s31cln\n"
      "gjbSy9lC/+PiDw9YmeKBLkcfmKQJO021Xlf6yUxRuQKBgBWPODUO8oKjkuJXNI/w\n"
      "El9hNWkd+/SZDfnt93dlVyXTtTF0M5M95tlOgqvLtWKSyB/BOnoZYWqR8luMl15d\n"
      "bf75j5mB0lHMWtyQgvZSkFqe9Or7Zy7hfTShDlZ/w+OXK7PGesaE1F14irShXSji\n"
      "yn5DZYAZ5pU52xreJeYvDngO\n"
      "-----END PRIVATE KEY-----\n";
  const std::string DH =
      "-----BEGIN DH PARAMETERS-----\n"
     "MIIBCAKCAQEA4+DA1j0gDWS71okwHpnvA65NmmR4mf+B3H39g163zY5S+cnWS2LI\n"
     "dvqnUDpw13naWtQ+Nu7I4rk1XoPaxOPSTu1MTbtYOxxU9M1ceBu4kQjDeHwasPVM\n"
     "zyEs1XXX3tsbPUxAuayX+AgW6QQAQUEjKDnv3FzVnQTFjwI49LqjnrSjbgQcoMaH\n"
     "EdGGUc6t1/We2vtsJZx0/dbaMkzFYO8dAbEYHL4sPKQb2mLpCPJZC3vwzpFkHFCd\n"
     "QSnLW2qRhy+66Mf8shdr6uvpoMcnKMOAvjKdXl9PBeJM9eJPz2lC4tnTiM3DqNzK\n"
     "Hn8+Pu3KkSIFL/5uBVu1fZSq+lFIEI23wwIBAg==\n"
     "-----END DH PARAMETERS-----\n";

  librbd::ImageCtx *m_image_ctx;

  std::optional<boost::asio::ip::tcp::acceptor> m_acceptor;
  boost::beast::flat_buffer m_buffer;
  uint64_t m_server_port = 0;
};

TEST_F(TestMockMigrationHttpClient, OpenCloseHttp) {
  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx;
  client_accept(&socket, false, &on_connect_ctx);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx.wait());
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  http_client.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationHttpClient, OpenCloseHttps) {
  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx;
  client_accept(&socket, false, &on_connect_ctx);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTPS));
  http_client.set_ignore_self_signed_cert(true);

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx.wait());

  boost::asio::ssl::context ssl_context{boost::asio::ssl::context::tlsv12};
  load_server_certificate(ssl_context);
  boost::asio::ssl::stream<boost::asio::ip::tcp::socket> ssl_stream{
    std::move(socket), ssl_context};

  C_SaferCond on_ssl_handshake_ctx;
  client_ssl_handshake(ssl_stream, false, &on_ssl_handshake_ctx);
  ASSERT_EQ(0, on_ssl_handshake_ctx.wait());

  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  http_client.close(&ctx2);

  C_SaferCond on_ssl_shutdown_ctx;
  client_ssl_shutdown(ssl_stream, &on_ssl_shutdown_ctx);
  ASSERT_EQ(0, on_ssl_shutdown_ctx.wait());

  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationHttpClient, OpenHttpsHandshakeFail) {
  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx;
  client_accept(&socket, false, &on_connect_ctx);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTPS));

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx.wait());

  boost::asio::ssl::context ssl_context{boost::asio::ssl::context::tlsv12};
  load_server_certificate(ssl_context);
  boost::asio::ssl::stream<boost::asio::ip::tcp::socket> ssl_stream{
    std::move(socket), ssl_context};

  C_SaferCond on_ssl_handshake_ctx;
  client_ssl_handshake(ssl_stream, true, &on_ssl_handshake_ctx);
  ASSERT_NE(0, on_ssl_handshake_ctx.wait());
  ASSERT_NE(0, ctx1.wait());
}

TEST_F(TestMockMigrationHttpClient, OpenInvalidUrl) {
  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx, "ftp://nope/");

  C_SaferCond ctx;
  http_client.open(&ctx);
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMigrationHttpClient, OpenResolveFail) {
  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx, "http://foo.example");

  C_SaferCond ctx;
  http_client.open(&ctx);
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockMigrationHttpClient, OpenConnectFail) {
  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             "http://localhost:2/");

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(-ECONNREFUSED, ctx1.wait());
}

TEST_F(TestMockMigrationHttpClient, IssueHead) {
  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx;
  client_accept(&socket, false, &on_connect_ctx);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx.wait());
  ASSERT_EQ(0, ctx1.wait());

  EmptyHttpRequest req;
  req.method(boost::beast::http::verb::head);

  C_SaferCond ctx2;
  HttpResponse res;
  http_client.issue(EmptyHttpRequest{req},
    [&ctx2, &res](int r, HttpResponse&& response) mutable {
      res = std::move(response);
      ctx2.complete(r);
    });

  HttpResponse expected_res;
  client_read_request(socket, req);
  client_write_response(socket, expected_res);

  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(expected_res, res);

  C_SaferCond ctx3;
  http_client.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationHttpClient, IssueGet) {
  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx;
  client_accept(&socket, false, &on_connect_ctx);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx.wait());
  ASSERT_EQ(0, ctx1.wait());

  EmptyHttpRequest req;
  req.method(boost::beast::http::verb::get);

  C_SaferCond ctx2;
  HttpResponse res;
  http_client.issue(EmptyHttpRequest{req},
    [&ctx2, &res](int r, HttpResponse&& response) mutable {
      res = std::move(response);
      ctx2.complete(r);
    });

  HttpResponse expected_res;
  expected_res.body() = "test";
  client_read_request(socket, req);
  client_write_response(socket, expected_res);

  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(expected_res, res);

  C_SaferCond ctx3;
  http_client.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationHttpClient, IssueSendFailed) {
  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx1;
  client_accept(&socket, false, &on_connect_ctx1);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx1.wait());
  ASSERT_EQ(0, ctx1.wait());

  // close connection to client
  boost::system::error_code ec;
  socket.close(ec);

  C_SaferCond on_connect_ctx2;
  client_accept(&socket, false, &on_connect_ctx2);

  // send request via closed connection
  EmptyHttpRequest req;
  req.method(boost::beast::http::verb::get);

  C_SaferCond ctx2;
  http_client.issue(EmptyHttpRequest{req},
    [&ctx2](int r, HttpResponse&&) mutable {
      ctx2.complete(r);
    });

  // connection will be reset and request retried
  ASSERT_EQ(0, on_connect_ctx2.wait());
  HttpResponse expected_res;
  expected_res.body() = "test";
  client_read_request(socket, req);
  client_write_response(socket, expected_res);
  ASSERT_EQ(0, ctx2.wait());

  C_SaferCond ctx3;
  http_client.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationHttpClient, IssueReceiveFailed) {
  boost::asio::ip::tcp::socket socket1(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx1;
  client_accept(&socket1, false, &on_connect_ctx1);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx1.wait());
  ASSERT_EQ(0, ctx1.wait());

  // send request via closed connection
  EmptyHttpRequest req;
  req.method(boost::beast::http::verb::get);

  C_SaferCond ctx2;
  http_client.issue(EmptyHttpRequest{req},
    [&ctx2](int r, HttpResponse&&) mutable {
      ctx2.complete(r);
    });

  // close connection to client after reading request
  client_read_request(socket1, req);

  C_SaferCond on_connect_ctx2;
  boost::asio::ip::tcp::socket socket2(*m_image_ctx->asio_engine);
  client_accept(&socket2, false, &on_connect_ctx2);

  boost::system::error_code ec;
  socket1.close(ec);
  ASSERT_EQ(0, on_connect_ctx2.wait());

  // connection will be reset and request retried
  HttpResponse expected_res;
  expected_res.body() = "test";
  client_read_request(socket2, req);
  client_write_response(socket2, expected_res);
  ASSERT_EQ(0, ctx2.wait());

  C_SaferCond ctx3;
  http_client.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationHttpClient, IssueResetFailed) {
  m_server_port = 0;
  create_acceptor(true);

  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx1;
  client_accept(&socket, false, &on_connect_ctx1);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx1.wait());
  ASSERT_EQ(0, ctx1.wait());

  // send requests then close connection
  EmptyHttpRequest req;
  req.method(boost::beast::http::verb::get);

  C_SaferCond ctx2;
  http_client.issue(EmptyHttpRequest{req},
    [&ctx2](int r, HttpResponse&&) mutable {
      ctx2.complete(r);
    });

  C_SaferCond ctx3;
  http_client.issue(EmptyHttpRequest{req},
    [&ctx3](int r, HttpResponse&&) mutable {
      ctx3.complete(r);
    });

  client_read_request(socket, req);
  client_read_request(socket, req);

  // close connection to client and verify requests are failed
  m_acceptor.reset();
  boost::system::error_code ec;
  socket.close(ec);

  ASSERT_EQ(-ECONNREFUSED, ctx2.wait());
  ASSERT_EQ(-ECONNREFUSED, ctx3.wait());

  // additional request will retry the failed connection
  create_acceptor(true);

  C_SaferCond on_connect_ctx2;
  client_accept(&socket, false, &on_connect_ctx2);

  C_SaferCond ctx4;
  http_client.issue(EmptyHttpRequest{req},
    [&ctx4](int r, HttpResponse&&) mutable {
      ctx4.complete(r);
    });

  ASSERT_EQ(0, on_connect_ctx2.wait());
  client_read_request(socket, req);

  HttpResponse expected_res;
  expected_res.body() = "test";
  client_write_response(socket, expected_res);
  ASSERT_EQ(0, ctx4.wait());

  C_SaferCond ctx5;
  http_client.close(&ctx5);
  ASSERT_EQ(0, ctx5.wait());
}

TEST_F(TestMockMigrationHttpClient, IssuePipelined) {
  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx;
  client_accept(&socket, false, &on_connect_ctx);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx.wait());
  ASSERT_EQ(0, ctx1.wait());

  // issue two pipelined (concurrent) get requests
  EmptyHttpRequest req1;
  req1.method(boost::beast::http::verb::get);

  C_SaferCond ctx2;
  HttpResponse res1;
  http_client.issue(EmptyHttpRequest{req1},
    [&ctx2, &res1](int r, HttpResponse&& response) mutable {
      res1 = std::move(response);
      ctx2.complete(r);
    });

  EmptyHttpRequest req2;
  req2.method(boost::beast::http::verb::get);

  C_SaferCond ctx3;
  HttpResponse res2;
  http_client.issue(EmptyHttpRequest{req2},
    [&ctx3, &res2](int r, HttpResponse&& response) mutable {
      res2 = std::move(response);
      ctx3.complete(r);
    });

  client_read_request(socket, req1);
  client_read_request(socket, req2);

  // read the responses sequentially
  HttpResponse expected_res1;
  expected_res1.body() = "test";
  client_write_response(socket, expected_res1);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(expected_res1, res1);

  HttpResponse expected_res2;
  expected_res2.body() = "test";
  client_write_response(socket, expected_res2);
  ASSERT_EQ(0, ctx3.wait());
  ASSERT_EQ(expected_res2, res2);

  C_SaferCond ctx4;
  http_client.close(&ctx4);
  ASSERT_EQ(0, ctx4.wait());
}

TEST_F(TestMockMigrationHttpClient, IssuePipelinedRestart) {
  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx1;
  client_accept(&socket, false, &on_connect_ctx1);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx1.wait());
  ASSERT_EQ(0, ctx1.wait());

  // issue two pipelined (concurrent) get requests
  EmptyHttpRequest req1;
  req1.keep_alive(false);
  req1.method(boost::beast::http::verb::get);

  C_SaferCond on_connect_ctx2;
  client_accept(&socket, false, &on_connect_ctx2);

  C_SaferCond ctx2;
  HttpResponse res1;
  http_client.issue(EmptyHttpRequest{req1},
    [&ctx2, &res1](int r, HttpResponse&& response) mutable {
      res1 = std::move(response);
      ctx2.complete(r);
    });

  EmptyHttpRequest req2;
  req2.method(boost::beast::http::verb::get);

  C_SaferCond ctx3;
  HttpResponse res2;
  http_client.issue(EmptyHttpRequest{req2},
    [&ctx3, &res2](int r, HttpResponse&& response) mutable {
      res2 = std::move(response);
      ctx3.complete(r);
    });

  client_read_request(socket, req1);
  client_read_request(socket, req2);

  // read the responses sequentially
  HttpResponse expected_res1;
  expected_res1.body() = "test";
  expected_res1.keep_alive(false);
  client_write_response(socket, expected_res1);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(expected_res1, res1);

  // second request will need to be re-sent due to 'need_eof' condition
  ASSERT_EQ(0, on_connect_ctx2.wait());
  client_read_request(socket, req2);

  HttpResponse expected_res2;
  expected_res2.body() = "test";
  client_write_response(socket, expected_res2);
  ASSERT_EQ(0, ctx3.wait());
  ASSERT_EQ(expected_res2, res2);

  C_SaferCond ctx4;
  http_client.close(&ctx4);
  ASSERT_EQ(0, ctx4.wait());
}

TEST_F(TestMockMigrationHttpClient, ShutdownInFlight) {
  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx;
  client_accept(&socket, false, &on_connect_ctx);

  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx.wait());
  ASSERT_EQ(0, ctx1.wait());

  EmptyHttpRequest req;
  req.method(boost::beast::http::verb::get);

  C_SaferCond ctx2;
  http_client.issue(EmptyHttpRequest{req},
    [&ctx2](int r, HttpResponse&&) mutable {
      ctx2.complete(r);
    });

  client_read_request(socket, req);

  C_SaferCond ctx3;
  http_client.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
  ASSERT_EQ(-ESHUTDOWN, ctx2.wait());
}

TEST_F(TestMockMigrationHttpClient, GetSize) {
  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx;
  client_accept(&socket, false, &on_connect_ctx);

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx.wait());
  ASSERT_EQ(0, ctx1.wait());

  uint64_t size = 0;
  C_SaferCond ctx2;
  http_client.get_size(&size, &ctx2);

  EmptyHttpRequest expected_req;
  expected_req.method(boost::beast::http::verb::head);
  client_read_request(socket, expected_req);

  HttpResponse expected_res;
  expected_res.body() = std::string(123, '1');
  client_write_response(socket, expected_res);

  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(123, size);

  C_SaferCond ctx3;
  http_client.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationHttpClient, GetSizeError) {
  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx;
  client_accept(&socket, false, &on_connect_ctx);

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx.wait());
  ASSERT_EQ(0, ctx1.wait());

  uint64_t size = 0;
  C_SaferCond ctx2;
  http_client.get_size(&size, &ctx2);

  EmptyHttpRequest expected_req;
  expected_req.method(boost::beast::http::verb::head);
  client_read_request(socket, expected_req);

  HttpResponse expected_res;
  expected_res.result(boost::beast::http::status::internal_server_error);
  client_write_response(socket, expected_res);

  ASSERT_EQ(-EIO, ctx2.wait());

  C_SaferCond ctx3;
  http_client.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationHttpClient, Read) {
  MockTestImageCtx mock_test_image_ctx(*m_image_ctx);
  MockHttpClient http_client(&mock_test_image_ctx,
                             get_local_url(URL_SCHEME_HTTP));

  boost::asio::ip::tcp::socket socket(*m_image_ctx->asio_engine);
  C_SaferCond on_connect_ctx;
  client_accept(&socket, false, &on_connect_ctx);

  C_SaferCond ctx1;
  http_client.open(&ctx1);
  ASSERT_EQ(0, on_connect_ctx.wait());
  ASSERT_EQ(0, ctx1.wait());

  bufferlist bl;
  C_SaferCond ctx2;
  http_client.read({{0, 128}, {256, 64}}, &bl, &ctx2);

  EmptyHttpRequest expected_req1;
  expected_req1.method(boost::beast::http::verb::get);
  expected_req1.set(boost::beast::http::field::range, "bytes=0-127");
  client_read_request(socket, expected_req1);

  EmptyHttpRequest expected_req2;
  expected_req2.method(boost::beast::http::verb::get);
  expected_req2.set(boost::beast::http::field::range, "bytes=256-319");
  client_read_request(socket, expected_req2);

  HttpResponse expected_res1;
  expected_res1.result(boost::beast::http::status::partial_content);
  expected_res1.body() = std::string(128, '1');
  client_write_response(socket, expected_res1);

  HttpResponse expected_res2;
  expected_res2.result(boost::beast::http::status::partial_content);
  expected_res2.body() = std::string(64, '2');
  client_write_response(socket, expected_res2);

  ASSERT_EQ(192, ctx2.wait());

  bufferlist expect_bl;
  expect_bl.append(std::string(128, '1'));
  expect_bl.append(std::string(64, '2'));
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  http_client.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

} // namespace migration
} // namespace librbd
