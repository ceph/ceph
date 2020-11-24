// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/HttpStream.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ReadResult.h"
#include "librbd/migration/HttpClient.h"
#include <boost/beast/http.hpp>

namespace librbd {
namespace migration {

namespace {

const std::string URL_KEY {"url"};

} // anonymous namespace

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::HttpStream: " << this \
                           << " " << __func__ << ": "

template <typename I>
HttpStream<I>::HttpStream(I* image_ctx, const json_spirit::mObject& json_object)
  : m_image_ctx(image_ctx), m_cct(image_ctx->cct),
    m_asio_engine(image_ctx->asio_engine), m_json_object(json_object) {
}

template <typename I>
HttpStream<I>::~HttpStream() {
}

template <typename I>
void HttpStream<I>::open(Context* on_finish) {
  auto& url_value = m_json_object[URL_KEY];
  if (url_value.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate '" << URL_KEY << "' key" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  m_url = url_value.get_str();
  ldout(m_cct, 10) << "url=" << m_url << dendl;

  m_http_client.reset(HttpClient<I>::create(m_image_ctx, m_url));
  m_http_client->open(on_finish);
}

template <typename I>
void HttpStream<I>::close(Context* on_finish) {
  ldout(m_cct, 10) << dendl;

  if (!m_http_client) {
    on_finish->complete(0);
  }

  m_http_client->close(on_finish);
}

template <typename I>
void HttpStream<I>::get_size(uint64_t* size, Context* on_finish) {
  ldout(m_cct, 10) << dendl;

  m_http_client->get_size(size, on_finish);
}

template <typename I>
void HttpStream<I>::read(io::Extents&& byte_extents, bufferlist* data,
                         Context* on_finish) {
  using HttpRequest = boost::beast::http::request<
    boost::beast::http::empty_body>;

  ldout(m_cct, 20) << dendl;

  auto aio_comp = io::AioCompletion::create_and_start(
    on_finish, util::get_image_ctx(m_image_ctx), io::AIO_TYPE_READ);
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

    HttpRequest req;
    req.method(boost::beast::http::verb::get);

    std::stringstream range;
    ceph_assert(byte_length > 0);
    range << "bytes=" << byte_offset << "-" << (byte_offset + byte_length - 1);
    req.set(boost::beast::http::field::range, range.str());

    m_http_client->issue(std::move(req),
      [this, byte_offset=byte_offset, byte_length=byte_length, ctx](int r, HttpResponse&& response) {
        handle_read(r, std::move(response), byte_offset, byte_length, &ctx->bl,
                    ctx);
     });
  }
}

template <typename I>
void HttpStream<I>::handle_read(int r, HttpResponse&& response,
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

} // namespace migration
} // namespace librbd

template class librbd::migration::HttpStream<librbd::ImageCtx>;
