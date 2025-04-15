// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/HttpStream.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/asio/Utils.h"
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
    return;
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
  ldout(m_cct, 20) << "byte_extents=" << byte_extents << dendl;

  m_http_client->read(std::move(byte_extents), data, on_finish);
}

template <typename I>
void HttpStream<I>::list_sparse_extents(io::Extents&& byte_extents,
                                        io::SparseExtents* sparse_extents,
                                        Context* on_finish) {
  // no sparseness information -- list the full range as DATA
  for (auto [byte_offset, byte_length] : byte_extents) {
    sparse_extents->insert(byte_offset, byte_length,
                           {io::SPARSE_EXTENT_STATE_DATA, byte_length});
  }
  on_finish->complete(0);
}

} // namespace migration
} // namespace librbd

template class librbd::migration::HttpStream<librbd::ImageCtx>;
