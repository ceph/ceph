// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/HttpClient.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ReadResult.h"
#include "librbd/migration/Utils.h"
#include <boost/asio/buffer.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/lexical_cast.hpp>
#include <deque>

namespace librbd {
namespace migration {

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::HttpClient::" \
                           << "HttpSession " << this << " " << __func__ \
                           << ": "

/**
 * boost::beast utilizes non-inheriting template classes for handling plain vs
 * encrypted TCP streams. Utilize a base-class for handling the majority of the
 * logic for handling connecting, disconnecting, reseting, and sending requests.
 */

template <typename I>
template <typename D>
class HttpClient<I>::HttpSession : public HttpSessionInterface {
public:
  void init(Context* on_finish) override {
    ceph_assert(m_http_client->m_strand.running_in_this_thread());

    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << dendl;

    ceph_assert(m_state == STATE_UNINITIALIZED);
    m_state = STATE_CONNECTING;

    resolve_host(on_finish);
  }

  void shut_down(Context* on_finish) override {
    ceph_assert(m_http_client->m_strand.running_in_this_thread());

    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << dendl;

    ceph_assert(on_finish != nullptr);
    ceph_assert(m_on_shutdown == nullptr);
    m_on_shutdown = on_finish;

    auto current_state = m_state;
    if (current_state == STATE_UNINITIALIZED) {
      // never initialized or resolve/connect failed
      on_finish->complete(0);
      return;
    }

    m_state = STATE_SHUTTING_DOWN;
    if (current_state != STATE_READY) {
      // delay shutdown until current state transition completes
      return;
    }

    disconnect(new LambdaContext([this](int r) { handle_shut_down(r); }));
  }

  void issue(std::shared_ptr<Work>&& work) override {
    ceph_assert(m_http_client->m_strand.running_in_this_thread());

    auto cct = m_http_client->m_cct;
    ldout(cct, 20) << "work=" << work.get() << dendl;

    if (is_shutdown()) {
      lderr(cct) << "cannot issue HTTP request, client is shutdown"
                 << dendl;
      work->complete(-ESHUTDOWN, {});
      return;
    }

    bool first_issue = m_issue_queue.empty();
    m_issue_queue.emplace_back(work);
    if (m_state == STATE_READY && first_issue) {
      ldout(cct, 20) << "sending http request: work=" << work.get() << dendl;
      finalize_issue(std::move(work));
    } else if (m_state == STATE_UNINITIALIZED) {
      ldout(cct, 20) << "resetting HTTP session: work=" << work.get() << dendl;
      m_state = STATE_RESET_CONNECTING;
      resolve_host(nullptr);
    } else {
      ldout(cct, 20) << "queueing HTTP request: work=" << work.get() << dendl;
    }
  }

  void finalize_issue(std::shared_ptr<Work>&& work) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 20) << "work=" << work.get() << dendl;

    ++m_in_flight_requests;
    (*work)(derived().stream());
  }

  void handle_issue(boost::system::error_code ec,
                    std::shared_ptr<Work>&& work) override {
    ceph_assert(m_http_client->m_strand.running_in_this_thread());

    auto cct = m_http_client->m_cct;
    ldout(cct, 20) << "work=" << work.get() << ", r=" << -ec.value() << dendl;

    ceph_assert(m_in_flight_requests > 0);
    --m_in_flight_requests;
    if (maybe_finalize_reset()) {
      // previous request is attempting reset to this request will be resent
      return;
    }

    ceph_assert(!m_issue_queue.empty());
    m_issue_queue.pop_front();

    if (is_shutdown()) {
      lderr(cct) << "client shutdown during in-flight request" << dendl;
      work->complete(-ESHUTDOWN, {});

      maybe_finalize_shutdown();
      return;
    }

    if (ec) {
      if (ec == boost::asio::error::bad_descriptor ||
          ec == boost::asio::error::broken_pipe ||
          ec == boost::asio::error::connection_reset ||
          ec == boost::asio::error::operation_aborted ||
          ec == boost::asio::ssl::error::stream_truncated ||
          ec == boost::beast::http::error::end_of_stream ||
          ec == boost::beast::http::error::partial_message) {
        ldout(cct, 5) << "remote peer stream closed, retrying request" << dendl;
        m_issue_queue.push_front(work);
      } else if (ec == boost::beast::error::timeout) {
        lderr(cct) << "timed-out while issuing request" << dendl;
        work->complete(-ETIMEDOUT, {});
      } else {
        lderr(cct) << "failed to issue request: " << ec.message() << dendl;
        work->complete(-ec.value(), {});
      }

      // attempt to recover the connection
      reset();
      return;
    }

    bool first_receive = m_receive_queue.empty();
    m_receive_queue.push_back(work);
    if (first_receive) {
      receive(std::move(work));
    }

    // TODO disable pipelining for non-idempotent requests

    // pipeline the next request into the stream
    if (!m_issue_queue.empty()) {
      work = m_issue_queue.front();
      ldout(cct, 20) << "sending http request: work=" << work.get() << dendl;
      finalize_issue(std::move(work));
    }
  }

protected:
  HttpClient* m_http_client;

  HttpSession(HttpClient* http_client)
    : m_http_client(http_client), m_resolver(http_client->m_strand) {
  }

  virtual void connect(boost::asio::ip::tcp::resolver::results_type results,
                       Context* on_finish) = 0;
  virtual void disconnect(Context* on_finish) = 0;

  void close_socket() {
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << dendl;

    boost::system::error_code ec;
    boost::beast::get_lowest_layer(derived().stream()).socket().close(ec);
  }

private:
  enum State {
    STATE_UNINITIALIZED,
    STATE_CONNECTING,
    STATE_READY,
    STATE_RESET_PENDING,
    STATE_RESET_DISCONNECTING,
    STATE_RESET_CONNECTING,
    STATE_SHUTTING_DOWN,
    STATE_SHUTDOWN,
  };

  State m_state = STATE_UNINITIALIZED;
  boost::asio::ip::tcp::resolver m_resolver;

  Context* m_on_shutdown = nullptr;

  uint64_t m_in_flight_requests = 0;
  std::deque<std::shared_ptr<Work>> m_issue_queue;
  std::deque<std::shared_ptr<Work>> m_receive_queue;

  boost::beast::flat_buffer m_buffer;
  std::optional<boost::beast::http::parser<false, EmptyBody>> m_header_parser;
  std::optional<boost::beast::http::parser<false, StringBody>> m_parser;

  D& derived() {
    return static_cast<D&>(*this);
  }

  void resolve_host(Context* on_finish) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << dendl;

    shutdown_socket();
    m_resolver.async_resolve(
      m_http_client->m_url_spec.host, m_http_client->m_url_spec.port,
      [this, on_finish](boost::system::error_code ec, auto results) {
          handle_resolve_host(ec, results, on_finish); });
  }

  void handle_resolve_host(
      boost::system::error_code ec,
      boost::asio::ip::tcp::resolver::results_type results,
      Context* on_finish) {
    auto cct = m_http_client->m_cct;
    int r = -ec.value();
    ldout(cct, 15) << "r=" << r << dendl;

    if (ec) {
      if (ec == boost::asio::error::host_not_found) {
        r = -ENOENT;
      } else if (ec == boost::asio::error::host_not_found_try_again) {
        // TODO: add retry throttle
        r = -EAGAIN;
      }

      lderr(cct) << "failed to resolve host '"
                 << m_http_client->m_url_spec.host << "': "
                 << cpp_strerror(r) << dendl;
      advance_state(STATE_UNINITIALIZED, r, on_finish);
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
      advance_state(STATE_UNINITIALIZED, r, on_finish);
      return;
    }

    advance_state(STATE_READY, 0, on_finish);
  }

  void handle_shut_down(int r) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to disconnect stream: '" << cpp_strerror(r)
                 << dendl;
    }

    // cancel all in-flight send/receives (if any)
    shutdown_socket();

    maybe_finalize_shutdown();
  }

  void maybe_finalize_shutdown() {
    if (m_in_flight_requests > 0) {
      return;
    }

    // cancel any queued IOs
    fail_queued_work(-ESHUTDOWN);

    advance_state(STATE_SHUTDOWN, 0, nullptr);
  }

  bool is_shutdown() const {
    ceph_assert(m_http_client->m_strand.running_in_this_thread());
    return (m_state == STATE_SHUTTING_DOWN || m_state == STATE_SHUTDOWN);
  }

  void reset() {
    ceph_assert(m_http_client->m_strand.running_in_this_thread());
    ceph_assert(m_state == STATE_READY);

    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << dendl;

    m_state = STATE_RESET_PENDING;
    maybe_finalize_reset();
  }

  bool maybe_finalize_reset() {
    if (m_state != STATE_RESET_PENDING) {
      return false;
    }

    if (m_in_flight_requests > 0) {
      return true;
    }

    ceph_assert(m_http_client->m_strand.running_in_this_thread());
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << dendl;

    m_buffer.clear();

    // move in-flight request back to the front of the issue queue
    m_issue_queue.insert(m_issue_queue.begin(),
                         m_receive_queue.begin(), m_receive_queue.end());
    m_receive_queue.clear();

    m_state = STATE_RESET_DISCONNECTING;
    disconnect(new LambdaContext([this](int r) { handle_reset(r); }));
    return true;
  }

  void handle_reset(int r) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to disconnect stream: '" << cpp_strerror(r)
                 << dendl;
    }

    advance_state(STATE_RESET_CONNECTING, r, nullptr);
  }

  int shutdown_socket() {
    if (!boost::beast::get_lowest_layer(
          derived().stream()).socket().is_open()) {
      return 0;
    }

    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << dendl;

    boost::system::error_code ec;
    boost::beast::get_lowest_layer(derived().stream()).socket().shutdown(
      boost::asio::ip::tcp::socket::shutdown_both, ec);

    if (ec && ec != boost::beast::errc::not_connected) {
      lderr(cct) << "failed to shutdown socket: " << ec.message() << dendl;
      return -ec.value();
    }

    close_socket();
    return 0;
  }

  void receive(std::shared_ptr<Work>&& work) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << "work=" << work.get() << dendl;

    ceph_assert(!m_receive_queue.empty());
    ++m_in_flight_requests;

    // receive the response for this request
    m_parser.emplace();
    if (work->header_only()) {
      // HEAD requests don't transfer data but the parser still cares about max
      // content-length
      m_header_parser.emplace();
      m_header_parser->body_limit(std::numeric_limits<uint64_t>::max());

      boost::beast::http::async_read_header(
        derived().stream(), m_buffer, *m_header_parser,
        [this, work=std::move(work)]
        (boost::beast::error_code ec, std::size_t) mutable {
          handle_receive(ec, std::move(work));
        });
    } else {
      m_parser->body_limit(1 << 25); // max RBD object size
      boost::beast::http::async_read(
        derived().stream(), m_buffer, *m_parser,
        [this, work=std::move(work)]
        (boost::beast::error_code ec, std::size_t) mutable {
          handle_receive(ec, std::move(work));
        });
    }
  }

  void handle_receive(boost::system::error_code ec,
                      std::shared_ptr<Work>&& work) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 15) << "work=" << work.get() << ", r=" << -ec.value() << dendl;

    ceph_assert(m_in_flight_requests > 0);
    --m_in_flight_requests;
    if (maybe_finalize_reset()) {
      // previous request is attempting reset to this request will be resent
      return;
    }

    ceph_assert(!m_receive_queue.empty());
    m_receive_queue.pop_front();

    if (is_shutdown()) {
      lderr(cct) << "client shutdown with in-flight request" << dendl;
      work->complete(-ESHUTDOWN, {});

      maybe_finalize_shutdown();
      return;
    }

    if (ec) {
      if (ec == boost::asio::error::bad_descriptor ||
          ec == boost::asio::error::broken_pipe ||
          ec == boost::asio::error::connection_reset ||
          ec == boost::asio::error::operation_aborted ||
          ec == boost::asio::ssl::error::stream_truncated ||
          ec == boost::beast::http::error::end_of_stream ||
          ec == boost::beast::http::error::partial_message) {
        ldout(cct, 5) << "remote peer stream closed, retrying request" << dendl;
        m_receive_queue.push_front(work);
      } else if (ec == boost::beast::error::timeout) {
        lderr(cct) << "timed-out while issuing request" << dendl;
        work->complete(-ETIMEDOUT, {});
      } else {
        lderr(cct) << "failed to issue request: " << ec.message() << dendl;
        work->complete(-ec.value(), {});
      }

      reset();
      return;
    }

    Response response;
    if (work->header_only()) {
      m_parser.emplace(std::move(*m_header_parser));
    }
    response = m_parser->release();

    // basic response code handling in a common location
    int r = 0;
    auto result = response.result();
    if (result == boost::beast::http::status::not_found) {
      lderr(cct) << "requested resource does not exist" << dendl;
      r = -ENOENT;
    } else if (result == boost::beast::http::status::forbidden) {
      lderr(cct) << "permission denied attempting to access resource" << dendl;
      r = -EACCES;
    } else if (boost::beast::http::to_status_class(result) !=
                 boost::beast::http::status_class::successful) {
      lderr(cct) << "failed to retrieve size: HTTP " << result << dendl;
      r = -EIO;
    }

    bool need_eof = response.need_eof();
    if (r < 0) {
      work->complete(r, {});
    } else {
      work->complete(0, std::move(response));
    }

    if (need_eof) {
      ldout(cct, 20) << "reset required for non-pipelined response: "
                     << "work=" << work.get() << dendl;
      reset();
    } else if (!m_receive_queue.empty()) {
      auto work = m_receive_queue.front();
      receive(std::move(work));
    }
  }

  void advance_state(State next_state, int r, Context* on_finish) {
    auto cct = m_http_client->m_cct;
    auto current_state = m_state;
    ldout(cct, 15) << "current_state=" << current_state << ", "
                   << "next_state=" << next_state << ", "
                   << "r=" << r << dendl;

    m_state = next_state;
    if (current_state == STATE_CONNECTING) {
      if (next_state == STATE_UNINITIALIZED) {
        shutdown_socket();
        on_finish->complete(r);
        return;
      } else if (next_state == STATE_READY) {
        on_finish->complete(r);
        return;
      }
    } else if (current_state == STATE_SHUTTING_DOWN) {
      if (next_state == STATE_READY) {
        // shut down requested while connecting/resetting
        disconnect(new LambdaContext([this](int r) { handle_shut_down(r); }));
        return;
      } else if (next_state == STATE_UNINITIALIZED ||
                 next_state == STATE_SHUTDOWN ||
                 next_state == STATE_RESET_CONNECTING) {
        ceph_assert(m_on_shutdown != nullptr);
        m_on_shutdown->complete(r);
        return;
      }
    } else if (current_state == STATE_RESET_DISCONNECTING) {
      // disconnected from peer -- ignore errors and reconnect
      ceph_assert(next_state == STATE_RESET_CONNECTING);
      ceph_assert(on_finish == nullptr);
      shutdown_socket();
      resolve_host(nullptr);
      return;
    } else if (current_state == STATE_RESET_CONNECTING) {
      ceph_assert(on_finish == nullptr);
      if (next_state == STATE_READY) {
        // restart queued IO
        if (!m_issue_queue.empty()) {
          auto& work = m_issue_queue.front();
          finalize_issue(std::move(work));
        }
        return;
      } else if (next_state == STATE_UNINITIALIZED) {
        shutdown_socket();

        // fail all queued IO
        fail_queued_work(r);
        return;
      }
    }

    lderr(cct) << "unexpected state transition: "
               << "current_state=" << current_state << ", "
               << "next_state=" << next_state << dendl;
    ceph_assert(false);
  }

  void complete_work(std::shared_ptr<Work> work, int r, Response&& response) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 20) << "work=" << work.get() << ", r=" << r << dendl;

    work->complete(r, std::move(response));
  }

  void fail_queued_work(int r) {
    auto cct = m_http_client->m_cct;
    ldout(cct, 10) << "r=" << r << dendl;

    for (auto& work : m_issue_queue) {
      complete_work(work, r, {});
    }
    m_issue_queue.clear();
    ceph_assert(m_receive_queue.empty());
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
    this->close_socket();
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
      [on_finish](boost::system::error_code ec, const auto& endpoint) {
        on_finish->complete(-ec.value());
      });
  }

  void disconnect(Context* on_finish) override {
    on_finish->complete(0);
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
    this->close_socket();
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
      [this, on_finish](boost::system::error_code ec, const auto& endpoint) {
        handle_connect(-ec.value(), on_finish);
      });
  }

  void disconnect(Context* on_finish) override {
    auto http_client = this->m_http_client;
    auto cct = http_client->m_cct;
    ldout(cct, 15) << dendl;

    if (!m_ssl_enabled) {
      on_finish->complete(0);
      return;
    }

    m_stream.async_shutdown(
      asio::util::get_callback_adapter([this, on_finish](int r) {
        shutdown(r, on_finish); }));
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

    on_finish->complete(r);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::HttpClient: " << this \
                           << " " << __func__ << ": "

template <typename I>
HttpClient<I>::HttpClient(I* image_ctx, const std::string& url)
  : m_cct(image_ctx->cct), m_image_ctx(image_ctx),
    m_asio_engine(image_ctx->asio_engine), m_url(url),
    m_strand(boost::asio::make_strand(*m_asio_engine)),
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

  boost::asio::post(m_strand, [this, on_finish]() mutable {
    create_http_session(on_finish); });
}

template <typename I>
void HttpClient<I>::close(Context* on_finish) {
  boost::asio::post(m_strand, [this, on_finish]() mutable {
    shut_down_http_session(on_finish); });
}

template <typename I>
void HttpClient<I>::get_size(uint64_t* size, Context* on_finish) {
  ldout(m_cct, 10) << dendl;

  Request req;
  req.method(boost::beast::http::verb::head);

  issue(
    std::move(req), [this, size, on_finish](int r, Response&& response) {
      handle_get_size(r, std::move(response), size, on_finish);
    });
}

template <typename I>
void HttpClient<I>::handle_get_size(int r, Response&& response, uint64_t* size,
                                    Context* on_finish) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve size: " << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  } else if (!response.has_content_length()) {
    lderr(m_cct) << "failed to retrieve size: missing content-length" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  auto content_length = response[boost::beast::http::field::content_length];
  try {
    *size = boost::lexical_cast<uint64_t>(content_length);
  } catch (boost::bad_lexical_cast&) {
    lderr(m_cct) << "invalid content-length in response" << dendl;
    on_finish->complete(-EBADMSG);
    return;
  }

  on_finish->complete(0);
}

template <typename I>
void HttpClient<I>::read(io::Extents&& byte_extents, bufferlist* data,
                         Context* on_finish) {
  ldout(m_cct, 20) << dendl;

  auto aio_comp = io::AioCompletion::create_and_start(
    on_finish, librbd::util::get_image_ctx(m_image_ctx), io::AIO_TYPE_READ);
  aio_comp->set_request_count(byte_extents.size());

  // utilize ReadResult to assemble multiple byte extents into a single bl
  // since boost::beast doesn't support multipart responses out-of-the-box
  io::ReadResult read_result{data};
  aio_comp->read_result = std::move(read_result);
  aio_comp->read_result.set_image_extents(byte_extents);

  // issue a range get request for each extent
  uint64_t buffer_offset = 0;
  for (auto [byte_offset, byte_length] : byte_extents) {
    auto ctx = new io::ReadResult::C_ImageReadRequest(
      aio_comp, buffer_offset, {{byte_offset, byte_length}});
    buffer_offset += byte_length;

    Request req;
    req.method(boost::beast::http::verb::get);

    std::stringstream range;
    ceph_assert(byte_length > 0);
    range << "bytes=" << byte_offset << "-" << (byte_offset + byte_length - 1);
    req.set(boost::beast::http::field::range, range.str());

    issue(
      std::move(req),
      [this, byte_offset=byte_offset, byte_length=byte_length, ctx]
      (int r, Response&& response) {
        handle_read(r, std::move(response), byte_offset, byte_length, &ctx->bl,
                    ctx);
     });
  }
}

template <typename I>
void HttpClient<I>::handle_read(int r, Response&& response,
                                uint64_t byte_offset, uint64_t byte_length,
                                bufferlist* data, Context* on_finish) {
  ldout(m_cct, 20) << "bytes=" << byte_offset << "~" << byte_length << ", "
                   << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to read requested byte range: "
                 << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  } else if (response.result() != boost::beast::http::status::partial_content) {
    lderr(m_cct) << "failed to retrieve requested byte range: HTTP "
                 << response.result() << dendl;
    on_finish->complete(-EIO);
    return;
  } else if (byte_length != response.body().size()) {
    lderr(m_cct) << "unexpected short range read: "
                 << "wanted=" << byte_length << ", "
                 << "received=" << response.body().size() << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  data->clear();
  data->append(response.body());
  on_finish->complete(data->length());
}

template <typename I>
void HttpClient<I>::issue(std::shared_ptr<Work>&& work) {
  boost::asio::post(m_strand, [this, work=std::move(work)]() mutable {
    m_http_session->issue(std::move(work)); });
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

template <typename I>
void HttpClient<I>::shut_down_http_session(Context* on_finish) {
  ldout(m_cct, 15) << dendl;

  if (m_http_session == nullptr) {
    on_finish->complete(0);
    return;
  }

  m_http_session->shut_down(on_finish);
}

} // namespace migration
} // namespace librbd

template class librbd::migration::HttpClient<librbd::ImageCtx>;
