// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/S3Stream.h"
#include "common/armor.h"
#include "common/ceph_crypto.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ReadResult.h"
#include "librbd/migration/HttpClient.h"
#include "librbd/migration/HttpProcessorInterface.h"
#include <boost/beast/http.hpp>

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <time.h>

namespace librbd {
namespace migration {

using HttpRequest = boost::beast::http::request<boost::beast::http::empty_body>;

namespace {

const std::string URL_KEY {"url"};
const std::string ACCESS_KEY {"access_key"};
const std::string SECRET_KEY {"secret_key"};

} // anonymous namespace

template <typename I>
struct S3Stream<I>::HttpProcessor : public HttpProcessorInterface {
  S3Stream* s3stream;

  HttpProcessor(S3Stream* s3stream) : s3stream(s3stream) {
  }

  void process_request(EmptyRequest& request) override {
    s3stream->process_request(request);
  }
};

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::S3Stream: " << this \
                           << " " << __func__ << ": "

template <typename I>
S3Stream<I>::S3Stream(I* image_ctx, const json_spirit::mObject& json_object)
  : m_image_ctx(image_ctx), m_cct(image_ctx->cct),
    m_asio_engine(image_ctx->asio_engine), m_json_object(json_object),
    m_http_processor(std::make_unique<HttpProcessor>(this)) {
}

template <typename I>
S3Stream<I>::~S3Stream() {
}

template <typename I>
void S3Stream<I>::open(Context* on_finish) {
  auto& url_value = m_json_object[URL_KEY];
  if (url_value.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate '" << URL_KEY << "' key" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  auto& access_key = m_json_object[ACCESS_KEY];
  if (access_key.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate '" << ACCESS_KEY << "' key" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  auto& secret_key = m_json_object[SECRET_KEY];
  if (secret_key.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate '" << SECRET_KEY << "' key" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  m_url = url_value.get_str();

  librados::Rados rados(m_image_ctx->md_ctx);
  int r = 0;
  m_access_key = access_key.get_str();
  if (util::is_config_key_uri(m_access_key)) {
    r = util::get_config_key(rados, m_access_key, &m_access_key);
    if (r < 0) {
      lderr(m_cct) << "failed to retrieve access key from config: "
                   << cpp_strerror(r) << dendl;
      on_finish->complete(r);
      return;
    }
  }

  m_secret_key = secret_key.get_str();
  if (util::is_config_key_uri(m_secret_key)) {
    r = util::get_config_key(rados, m_secret_key, &m_secret_key);
    if (r < 0) {
      lderr(m_cct) << "failed to retrieve secret key from config: "
                   << cpp_strerror(r) << dendl;
      on_finish->complete(r);
      return;
    }
  }

  ldout(m_cct, 10) << "url=" << m_url << ", "
                   << "access_key=" << m_access_key << dendl;

  m_http_client.reset(HttpClient<I>::create(m_image_ctx, m_url));
  m_http_client->set_http_processor(m_http_processor.get());
  m_http_client->open(on_finish);
}

template <typename I>
void S3Stream<I>::close(Context* on_finish) {
  ldout(m_cct, 10) << dendl;

  if (!m_http_client) {
    on_finish->complete(0);
    return;
  }

  m_http_client->close(on_finish);
}

template <typename I>
void S3Stream<I>::get_size(uint64_t* size, Context* on_finish) {
  ldout(m_cct, 10) << dendl;

  m_http_client->get_size(size, on_finish);
}

template <typename I>
void S3Stream<I>::read(io::Extents&& byte_extents, bufferlist* data,
                       Context* on_finish) {
  ldout(m_cct, 20) << "byte_extents=" << byte_extents << dendl;

  m_http_client->read(std::move(byte_extents), data, on_finish);
}

template <typename I>
void S3Stream<I>::process_request(HttpRequest& http_request) {
  ldout(m_cct, 20) << dendl;

  // format RFC 1123 date/time
  auto time = ceph::real_clock::to_time_t(ceph::real_clock::now());
  struct tm timeInfo;
  gmtime_r(&time, &timeInfo);

  std::string date = fmt::format("{:%a, %d %b %Y %H:%M:%S %z}", timeInfo);
  http_request.set(boost::beast::http::field::date, date);

  // note: we don't support S3 subresources
  std::string canonicalized_resource = std::string(http_request.target());

  std::string string_to_sign = fmt::format(
    "{}\n\n\n{}\n{}",
    std::string(boost::beast::http::to_string(http_request.method())),
    date, canonicalized_resource);

  // create HMAC-SHA1 signature from secret key + string-to-sign
  sha1_digest_t digest;
  ceph::crypto::HMACSHA1 hmac(
    reinterpret_cast<const unsigned char*>(m_secret_key.data()),
    m_secret_key.size());
  hmac.Update(reinterpret_cast<const unsigned char*>(string_to_sign.data()),
              string_to_sign.size());
  hmac.Final(reinterpret_cast<unsigned char*>(digest.v));

  // base64 encode the result
  char buf[64];
  int r = ceph_armor(std::begin(buf), std::begin(buf) + sizeof(buf),
                     reinterpret_cast<const char *>(digest.v),
                     reinterpret_cast<const char *>(digest.v + digest.SIZE));
  if (r < 0) {
    ceph_abort("ceph_armor failed");
  }

  // store the access-key + signature in the HTTP authorization header
  std::string signature = std::string(std::begin(buf), std::begin(buf) + r);
  std::string authorization = fmt::format("AWS {}:{}", m_access_key, signature);
  http_request.set(boost::beast::http::field::authorization, authorization);

  ldout(m_cct, 20) << "string_to_sign=" << string_to_sign << ", "
                   << "authorization=" << authorization << dendl;
}

} // namespace migration
} // namespace librbd

template class librbd::migration::S3Stream<librbd::ImageCtx>;
