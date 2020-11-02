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

  void set_ignore_self_signed_cert(bool ignore) {
    m_ignore_self_signed_cert = ignore;
  }

private:
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

  void create_http_session(Context* on_finish);
};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::HttpClient<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_HTTP_CLIENT_H
