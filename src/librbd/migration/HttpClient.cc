// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/HttpClient.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/asio/Utils.h"
#include "librbd/migration/Utils.h"
#include <boost/asio/buffer.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl/ssl_stream.hpp>

namespace librbd {
namespace migration {

template <typename I>
struct HttpClient<I>::HttpSessionInterface {
  virtual ~HttpSessionInterface() {}

  virtual void init(Context* on_finish) = 0;
  virtual void shut_down(Context* on_finish) = 0;
};

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::HttpClient::" \
                           << "HttpSession " << this << " " << __func__ \
                           << ": "

template <typename I>
template <typename D>
class HttpClient<I>::HttpSession : public HttpSessionInterface {
public:
  void init(Context* on_finish) override {
    resolve_host(on_finish);
  }

  void shut_down(Context* on_finish) override {
    disconnect(new LambdaContext([this, on_finish](int r) {
      handle_shut_down(r, on_finish); }));
  }

protected:
  HttpClient* m_http_client;

  HttpSession(HttpClient* http_client)
    : m_http_client(http_client), m_resolver(http_client->m_strand) {
  }

  virtual void connect(boost::asio::ip::tcp::resolver::results_type results,
                       Context* on_finish) = 0;
  virtual void disconnect(Context* on_finish) = 0;

  void close() {
    boost::system::error_code ec;
    boost::beast::get_lowest_layer(derived().stream()).socket().close(ec);
  }

private:
  boost::asio::ip::tcp::resolver m_resolver;

  D& derived() {
    return static_cast<D&>(*this);
  }

  void resolve_host(Context* on_finish) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << dendl;

    m_resolver.async_resolve(
      m_http_client->m_url_spec.host, m_http_client->m_url_spec.port,
      asio::util::get_callback_adapter(
        [this, on_finish](int r, auto results) {
          handle_resolve_host(r, results, on_finish); }));
  }

  void handle_resolve_host(
      int r, boost::asio::ip::tcp::resolver::results_type results,
      Context* on_finish) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << "r=" << r << dendl;

    if (r < 0) {
      if (r == -boost::asio::error::host_not_found) {
        r = -ENOENT;
      }

      lderr(cct) << "failed to resolve host '"
                 << m_http_client->m_url_spec.host << "': "
                 << cpp_strerror(r) << dendl;
      on_finish->complete(r);
      return;
    }

    connect(results, new LambdaContext([this, on_finish](int r) {
      handle_connect(r, on_finish); }));
  }

  void handle_connect(int r, Context* on_finish) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to connect to host '"
                 << m_http_client->m_url_spec.host << "': "
                 << cpp_strerror(r) << dendl;
      on_finish->complete(r);
      return;
    }

    on_finish->complete(0);
  }

  void handle_shut_down(int r, Context* on_finish) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to close stream: '" << cpp_strerror(r) << dendl;
      on_finish->complete(r);
      return;
    }

    on_finish->complete(0);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::HttpClient::" \
                           << "PlainHttpSession " << this << " " << __func__ \
                           << ": "

template <typename I>
class HttpClient<I>::PlainHttpSession : public HttpSession<PlainHttpSession> {
public:
  PlainHttpSession(HttpClient* http_client)
    : HttpSession<PlainHttpSession>(http_client),
      m_stream(http_client->m_strand) {
  }
  ~PlainHttpSession() override {
    this->close();
  }


  inline boost::beast::tcp_stream&
  stream() {
    return m_stream;
  }

protected:
  void connect(boost::asio::ip::tcp::resolver::results_type results,
               Context* on_finish) override {
    auto http_client = this->m_http_client;
    auto cct = http_client->m_cct;
    ldout(cct, 15) << dendl;

    m_stream.async_connect(
      results,
      asio::util::get_callback_adapter(
        [on_finish](int r, auto endpoint) { on_finish->complete(r); }));
  }

  void disconnect(Context* on_finish) override {
    auto http_client = this->m_http_client;
    auto cct = http_client->m_cct;
    ldout(cct, 15) << dendl;

    boost::system::error_code ec;
    m_stream.socket().shutdown(
      boost::asio::ip::tcp::socket::shutdown_both, ec);

    on_finish->complete(-ec.value());
  }

private:
  boost::beast::tcp_stream m_stream;

};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::HttpClient::" \
                           << "SslHttpSession " << this << " " << __func__ \
                           << ": "

template <typename I>
class HttpClient<I>::SslHttpSession : public HttpSession<SslHttpSession> {
public:
  SslHttpSession(HttpClient* http_client)
    : HttpSession<SslHttpSession>(http_client),
      m_stream(http_client->m_strand, http_client->m_ssl_context) {
  }
  ~SslHttpSession() override {
    this->close();
  }

  inline boost::beast::ssl_stream<boost::beast::tcp_stream>&
  stream() {
    return m_stream;
  }

protected:
  void connect(boost::asio::ip::tcp::resolver::results_type results,
               Context* on_finish) override {
    auto http_client = this->m_http_client;
    auto cct = http_client->m_cct;
    ldout(cct, 15) << dendl;

    boost::beast::get_lowest_layer(m_stream).async_connect(
      results,
      asio::util::get_callback_adapter(
        [this, on_finish](int r, auto endpoint) {
          handle_connect(r, on_finish); }));
  }

  void disconnect(Context* on_finish) override {
    auto http_client = this->m_http_client;
    auto cct = http_client->m_cct;
    ldout(cct, 15) << dendl;

    if (m_ssl_enabled) {
      m_stream.async_shutdown(
        asio::util::get_callback_adapter([this, on_finish](int r) {
          shutdown(r, on_finish); }));
    } else {
      shutdown(0, on_finish);
    }
  }

private:
  boost::beast::ssl_stream<boost::beast::tcp_stream> m_stream;
  bool m_ssl_enabled = false;

  void handle_connect(int r, Context* on_finish) {
    auto http_client = this->m_http_client;
    auto cct = http_client->m_cct;
    ldout(cct, 15) << dendl;

    if (r < 0) {
      lderr(cct) << "failed to connect to host '"
                 << http_client->m_url_spec.host << "': "
                 << cpp_strerror(r) << dendl;
      on_finish->complete(r);
      return;
    }

    handshake(on_finish);
  }

  void handshake(Context* on_finish) {
    auto http_client = this->m_http_client;
    auto cct = http_client->m_cct;
    ldout(cct, 15) << dendl;

    auto& host = http_client->m_url_spec.host;
    m_stream.set_verify_mode(
      boost::asio::ssl::verify_peer |
      boost::asio::ssl::verify_fail_if_no_peer_cert);
    m_stream.set_verify_callback(
      [host, next=boost::asio::ssl::host_name_verification(host),
       ignore_self_signed=http_client->m_ignore_self_signed_cert]
      (bool preverified, boost::asio::ssl::verify_context& ctx) {
        if (!preverified && ignore_self_signed) {
          auto ec = X509_STORE_CTX_get_error(ctx.native_handle());
          switch (ec) {
          case X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT:
          case X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN:
            // ignore self-signed cert issues
            preverified = true;
            break;
          default:
            break;
          }
        }
        return next(preverified, ctx);
      });

    // Set SNI Hostname (many hosts need this to handshake successfully)
    if(!SSL_set_tlsext_host_name(m_stream.native_handle(),
                                 http_client->m_url_spec.host.c_str())) {
      int r = -::ERR_get_error();
      lderr(cct) << "failed to initialize SNI hostname: " << cpp_strerror(r)
                 << dendl;
      on_finish->complete(r);
      return;
    }

    // Perform the SSL/TLS handshake
    m_stream.async_handshake(
      boost::asio::ssl::stream_base::client,
      asio::util::get_callback_adapter(
        [this, on_finish](int r) { handle_handshake(r, on_finish); }));
  }

  void handle_handshake(int r, Context* on_finish) {
    auto http_client = this->m_http_client;
    auto cct = http_client->m_cct;
    ldout(cct, 15) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to complete handshake: " << cpp_strerror(r)
                 << dendl;
      disconnect(new LambdaContext([r, on_finish](int) {
        on_finish->complete(r); }));
      return;
    }

    m_ssl_enabled = true;
    on_finish->complete(0);
  }

  void shutdown(int r, Context* on_finish) {
    auto http_client = this->m_http_client;
    auto cct = http_client->m_cct;
    ldout(cct, 15) << "r=" << r << dendl;

    boost::system::error_code ec;
    boost::beast::get_lowest_layer(m_stream).socket().shutdown(
      boost::asio::ip::tcp::socket::shutdown_both, ec);

    on_finish->complete(-ec.value());
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::HttpClient: " << this \
                           << " " << __func__ << ": "

template <typename I>
HttpClient<I>::HttpClient(I* image_ctx, const std::string& url)
  : m_cct(image_ctx->cct), m_asio_engine(image_ctx->asio_engine), m_url(url),
    m_strand(*m_asio_engine),
    m_ssl_context(boost::asio::ssl::context::sslv23_client) {
    m_ssl_context.set_default_verify_paths();
}

template <typename I>
void HttpClient<I>::open(Context* on_finish) {
  ldout(m_cct, 10) << "url=" << m_url << dendl;

  int r = util::parse_url(m_cct, m_url, &m_url_spec);
  if (r < 0) {
    lderr(m_cct) << "failed to parse url '" << m_url << "': " << cpp_strerror(r)
                 << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  // initial bootstrap connection -- later IOs might rebuild the session
  create_http_session(on_finish);
}

template <typename I>
void HttpClient<I>::close(Context* on_finish) {
  // execute within the strand to ensure all IO completes
  boost::asio::post(
    m_strand, [this, on_finish]() {
      if (m_http_session == nullptr) {
        on_finish->complete(0);
        return;
      }

      m_http_session->shut_down(on_finish);
    });
}

template <typename I>
void HttpClient<I>::create_http_session(Context* on_finish) {
  ldout(m_cct, 15) << dendl;

  ceph_assert(m_http_session == nullptr);
  switch (m_url_spec.scheme) {
  case URL_SCHEME_HTTP:
    m_http_session = std::make_unique<PlainHttpSession>(this);
    break;
  case URL_SCHEME_HTTPS:
    m_http_session = std::make_unique<SslHttpSession>(this);
    break;
  default:
    ceph_assert(false);
    break;
  }

  m_http_session->init(on_finish);
}

} // namespace migration
} // namespace librbd

template class librbd::migration::HttpClient<librbd::ImageCtx>;
