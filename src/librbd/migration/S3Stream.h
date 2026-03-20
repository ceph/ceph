// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_S3_STREAM_H
#define CEPH_LIBRBD_MIGRATION_S3_STREAM_H

#include "include/int_types.h"
#include "librbd/migration/StreamInterface.h"
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/string_body.hpp>
#include <json_spirit/json_spirit.h>
#include <memory>
#include <string>

struct Context;

namespace librbd {

struct AsioEngine;
struct ImageCtx;

namespace migration {

template <typename> class HttpClient;

template <typename ImageCtxT>
class S3Stream : public StreamInterface {
public:
  static S3Stream* create(ImageCtxT* image_ctx,
                            const json_spirit::mObject& json_object) {
    return new S3Stream(image_ctx, json_object);
  }

  S3Stream(ImageCtxT* image_ctx, const json_spirit::mObject& json_object);
  ~S3Stream() override;

  S3Stream(const S3Stream&) = delete;
  S3Stream& operator=(const S3Stream&) = delete;

  void open(Context* on_finish) override;
  void close(Context* on_finish) override;

  void get_size(uint64_t* size, Context* on_finish) override;

  void read(io::Extents&& byte_extents, bufferlist* data,
            Context* on_finish) override;

  void list_sparse_extents(io::Extents&& byte_extents,
                           io::SparseExtents* sparse_extents,
                           Context* on_finish) override;

private:
  using HttpRequest = boost::beast::http::request<
    boost::beast::http::empty_body>;
  using HttpResponse = boost::beast::http::response<
    boost::beast::http::string_body>;

  struct HttpProcessor;

  ImageCtxT* m_image_ctx;
  CephContext* m_cct;
  std::shared_ptr<AsioEngine> m_asio_engine;
  json_spirit::mObject m_json_object;

  std::string m_url;
  std::string m_access_key;
  std::string m_secret_key;

  std::unique_ptr<HttpProcessor> m_http_processor;
  std::unique_ptr<HttpClient<ImageCtxT>> m_http_client;

  void process_request(HttpRequest& http_request);

};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::S3Stream<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_S3_STREAM_H
