// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MetadataCopyRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"
#include "librbd/image/GetMetadataRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::MetadataCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace deep_copy {

namespace {

const uint64_t MAX_METADATA_ITEMS = 128;

} // anonymous namespace

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
MetadataCopyRequest<I>::MetadataCopyRequest(I *src_image_ctx, I *dst_image_ctx,
                                            Context *on_finish)
  : m_src_image_ctx(src_image_ctx), m_dst_image_ctx(dst_image_ctx),
    m_on_finish(on_finish), m_cct(dst_image_ctx->cct) {

  ldout(m_cct, 20) << "src_image_id=" << m_src_image_ctx->id
                   << ", dst_image_id=" << m_dst_image_ctx->id
		   << dendl;
}

template <typename I>
void MetadataCopyRequest<I>::send() {
  list_src_metadata();
}

template <typename I>
void MetadataCopyRequest<I>::list_src_metadata() {
  ldout(m_cct, 20) << "start_key=" << m_last_metadata_key << dendl;

  m_metadata.clear();
  auto ctx = create_context_callback<
    MetadataCopyRequest<I>,
    &MetadataCopyRequest<I>::handle_list_src_metadata>(this);
  auto req = image::GetMetadataRequest<I>::create(
    m_src_image_ctx->md_ctx, m_src_image_ctx->header_oid, true, "",
    m_last_metadata_key, MAX_METADATA_ITEMS, &m_metadata, ctx);
  req->send();
}

template <typename I>
void MetadataCopyRequest<I>::handle_list_src_metadata(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve metadata: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_metadata.empty()) {
    finish(0);
    return;
  }

  m_last_metadata_key = m_metadata.rbegin()->first;
  m_more_metadata = (m_metadata.size() >= MAX_METADATA_ITEMS);
  set_dst_metadata();
}

template <typename I>
void MetadataCopyRequest<I>::set_dst_metadata() {
  ldout(m_cct, 20) << "count=" << m_metadata.size() << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::metadata_set(&op, m_metadata);

  librados::AioCompletion *aio_comp = create_rados_callback<
    MetadataCopyRequest<I>,
    &MetadataCopyRequest<I>::handle_set_dst_metadata>(this);
  m_dst_image_ctx->md_ctx.aio_operate(m_dst_image_ctx->header_oid, aio_comp,
                                      &op);
  aio_comp->release();
}

template <typename I>
void MetadataCopyRequest<I>::handle_set_dst_metadata(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to set metadata: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_more_metadata) {
    list_src_metadata();
    return;
  }

  finish(0);
}

template <typename I>
void MetadataCopyRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;
  m_on_finish->complete(r);
  delete this;
}

} // namespace deep_copy
} // namespace librbd

template class librbd::deep_copy::MetadataCopyRequest<librbd::ImageCtx>;
