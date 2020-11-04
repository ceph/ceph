// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_HTTP_CLIENT_H
#define CEPH_LIBRBD_MIGRATION_HTTP_CLIENT_H

#include "include/common_fwd.h"
#include "include/int_types.h"
#include "librbd/migration/Types.h"
#include <boost/asio/io_context_strand.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/beast/version.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>
#include <boost/beast/ssl/ssl_stream.hpp>
#include <memory>
#include <string>
#include <utility>

struct Context;

namespace librbd {

struct AsioEngine;
struct ImageCtx;

namespace migration {

template <typename ImageCtxT>
class HttpClient {
public:
  static HttpClient* create(ImageCtxT* image_ctx, const std::string& url) {
    return new HttpClient(image_ctx, url);
  }

  HttpClient(ImageCtxT* image_ctx, const std::string& url);
  HttpClient(const HttpClient&) = delete;
  HttpClient& operator=(const HttpClient&) = delete;

  void open(Context* on_finish);
  void close(Context* on_finish);

  void get_size(uint64_t* size, Context* on_finish);

  void set_ignore_self_signed_cert(bool ignore) {
    m_ignore_self_signed_cert = ignore;
  }

  using StringBody = boost::beast::http::string_body;

  template <class Body, typename Completion>
  void issue(boost::beast::http::request<Body>&& request,
             Completion&& completion) {
    struct WorkImpl : Work {
      boost::beast::http::request<Body> request;
      Completion completion;

      WorkImpl(boost::beast::http::request<Body>&& request,
               Completion&& completion)
        : request(std::move(request)), completion(std::move(completion)) {
      }
      WorkImpl(const WorkImpl&) = delete;
      WorkImpl& operator=(const WorkImpl&) = delete;

      bool need_eof() const override {
        return request.need_eof();
      }

      bool header_only() const override {
        return (request.method() == boost::beast::http::verb::head);
      }

      void complete(
          int r, boost::beast::http::response<StringBody>&& response) override {
        completion(r, std::move(response));
      }

      void operator()(
          HttpSessionInterface* http_session,
          boost::beast::tcp_stream& stream) override {
        boost::beast::http::async_write(
          stream, request,
          [http_session, work=this->shared_from_this()]
          (boost::beast::error_code ec, std::size_t) mutable {
            http_session->handle_issue(ec, std::move(work));
          });
      }

      void operator()(
          HttpSessionInterface* http_session,
          boost::beast::ssl_stream<boost::beast::tcp_stream>& stream) override {
        boost::beast::http::async_write(
          stream, request,
          [http_session, work=this->shared_from_this()]
          (boost::beast::error_code ec, std::size_t) mutable {
            http_session->handle_issue(ec, std::move(work));
          });
      }
    };

    initialize_default_fields(request);
    issue(std::make_shared<WorkImpl>(std::move(request),
                                     std::move(completion)));
  }

private:
  struct Work;
  struct HttpSessionInterface {
    virtual ~HttpSessionInterface() {}

    virtual void init(Context* on_finish) = 0;
    virtual void shut_down(Context* on_finish) = 0;

    virtual void issue(std::shared_ptr<Work>&& work) = 0;
    virtual void handle_issue(boost::system::error_code ec,
                              std::shared_ptr<Work>&& work) = 0;
  };

  struct Work : public std::enable_shared_from_this<Work> {
    virtual ~Work() {}
    virtual bool need_eof() const = 0;
    virtual bool header_only() const = 0;
    virtual void complete(
        int r, boost::beast::http::response<StringBody>&&) = 0;
    virtual void operator()(
        HttpSessionInterface* http_session,
        boost::beast::tcp_stream& stream) = 0;
    virtual void operator()(
        HttpSessionInterface* http_session,
        boost::beast::ssl_stream<boost::beast::tcp_stream>& stream) = 0;
  };

  struct HttpSessionInterface;
  template <typename D> struct HttpSession;
  struct PlainHttpSession;
  struct SslHttpSession;

  CephContext* m_cct;
  std::shared_ptr<AsioEngine> m_asio_engine;
  std::string m_url;

  UrlSpec m_url_spec;

  bool m_ignore_self_signed_cert = false;

  boost::asio::io_context::strand m_strand;

  boost::asio::ssl::context m_ssl_context;
  std::unique_ptr<HttpSessionInterface> m_http_session;

  template <typename Fields>
  void initialize_default_fields(Fields& fields) const {
    fields.target(m_url_spec.path);
    fields.set(boost::beast::http::field::host, m_url_spec.host);
    fields.set(boost::beast::http::field::user_agent,
               BOOST_BEAST_VERSION_STRING);
  }

  void handle_get_size(
    int r, boost::beast::http::response<StringBody>&& response,
    uint64_t* size, Context* on_finish);

  void issue(std::shared_ptr<Work>&& work);

  void create_http_session(Context* on_finish);
  void shut_down_http_session(Context* on_finish);
};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::HttpClient<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_HTTP_CLIENT_H
