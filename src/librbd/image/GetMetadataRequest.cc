// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/GetMetadataRequest.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/ceph_assert.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include <boost/algorithm/string/predicate.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::GetMetadataRequest: " \
                           << this << " " << __func__ << ": "

#define MAX_KEYS 64U

namespace librbd {
namespace image {
namespace {

static const std::string INTERNAL_KEY_PREFIX{".rbd"};

} // anonymous namespace

using util::create_rados_callback;

template <typename I>
GetMetadataRequest<I>::GetMetadataRequest(
    IoCtx &io_ctx, const std::string &oid, bool filter_internal,
    const std::string& filter_key_prefix, const std::string& last_key,
    uint32_t max_results, KeyValues* key_values, Context *on_finish)
  : m_io_ctx(io_ctx), m_oid(oid), m_filter_internal(filter_internal),
    m_filter_key_prefix(filter_key_prefix), m_last_key(last_key),
    m_max_results(max_results), m_key_values(key_values),
    m_on_finish(on_finish),
    m_cct(reinterpret_cast<CephContext*>(m_io_ctx.cct())) {
}

template <typename I>
void GetMetadataRequest<I>::send() {
  metadata_list();
}

template <typename I>
void GetMetadataRequest<I>::metadata_list() {
  ldout(m_cct, 15) << "start_key=" << m_last_key << dendl;

  m_expected_results = MAX_KEYS;
  if (m_max_results > 0) {
    m_expected_results = std::min<uint32_t>(
      m_expected_results, m_max_results - m_key_values->size());
  }

  librados::ObjectReadOperation op;
  cls_client::metadata_list_start(&op, m_last_key, m_expected_results);

  auto aio_comp = create_rados_callback<
    GetMetadataRequest<I>, &GetMetadataRequest<I>::handle_metadata_list>(this);
  m_out_bl.clear();
  m_io_ctx.aio_operate(m_oid, aio_comp, &op, &m_out_bl);
  aio_comp->release();
}

template <typename I>
void GetMetadataRequest<I>::handle_metadata_list(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  KeyValues metadata;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = cls_client::metadata_list_finish(&it, &metadata);
  }

  if (r == -ENOENT || r == -EOPNOTSUPP) {
    finish(0);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to retrieve image metadata: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  for (auto it = metadata.begin(); it != metadata.end(); ++it) {
    if (m_filter_internal &&
        boost::starts_with(it->first, INTERNAL_KEY_PREFIX)) {
      continue;
    } else if (!m_filter_key_prefix.empty() &&
               !boost::starts_with(it->first, m_filter_key_prefix)) {
      continue;
    }
    m_key_values->insert({it->first, std::move(it->second)});
  }
  if (!metadata.empty()) {
    m_last_key = metadata.rbegin()->first;
  }

  if (metadata.size() == m_expected_results &&
      (m_max_results == 0 || m_key_values->size() < m_max_results)) {
    metadata_list();
    return;
  }

  finish(0);
}

template <typename I>
void GetMetadataRequest<I>::finish(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::GetMetadataRequest<librbd::ImageCtx>;
