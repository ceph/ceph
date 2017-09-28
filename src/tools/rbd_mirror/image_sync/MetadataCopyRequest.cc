// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_sync/MetadataCopyRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_sync::MetadataCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_sync {

namespace {

const uint64_t MAX_METADATA_ITEMS = 128;

} // anonymous namespace

using librbd::util::create_rados_callback;

template <typename I>
void MetadataCopyRequest<I>::send() {
  list_remote_metadata();
}

template <typename I>
void MetadataCopyRequest<I>::list_remote_metadata() {
  dout(20) << "start_key=" << m_last_metadata_key << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::metadata_list_start(&op, m_last_metadata_key, MAX_METADATA_ITEMS);

  librados::AioCompletion *aio_comp = create_rados_callback<
    MetadataCopyRequest<I>,
    &MetadataCopyRequest<I>::handle_list_remote_data>(this);
  m_out_bl.clear();
  m_remote_image_ctx->md_ctx.aio_operate(m_remote_image_ctx->header_oid,
                                         aio_comp, &op, &m_out_bl);
  aio_comp->release();
}

template <typename I>
void MetadataCopyRequest<I>::handle_list_remote_data(int r) {
  dout(20) << "r=" << r << dendl;

  Metadata metadata;
  if (r == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    r = librbd::cls_client::metadata_list_finish(&it, &metadata);
  }

  if (r < 0) {
    derr << "failed to retrieve metadata: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (metadata.empty()) {
    finish(0);
    return;
  }

  m_last_metadata_key = metadata.rbegin()->first;
  m_more_metadata = (metadata.size() >= MAX_METADATA_ITEMS);
  set_local_metadata(metadata);
}

template <typename I>
void MetadataCopyRequest<I>::set_local_metadata(const Metadata& metadata) {
  dout(20) << "count=" << metadata.size() << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::metadata_set(&op, metadata);

  librados::AioCompletion *aio_comp = create_rados_callback<
    MetadataCopyRequest<I>,
    &MetadataCopyRequest<I>::handle_set_local_metadata>(this);
  m_local_image_ctx->md_ctx.aio_operate(m_local_image_ctx->header_oid, aio_comp,
                                        &op);
  aio_comp->release();
}

template <typename I>
void MetadataCopyRequest<I>::handle_set_local_metadata(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to set metadata: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_more_metadata) {
    list_remote_metadata();
    return;
  }

  finish(0);
}

template <typename I>
void MetadataCopyRequest<I>::finish(int r) {
  dout(20) << "r=" << r << dendl;
  m_on_finish->complete(r);
  delete this;
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_sync::MetadataCopyRequest<librbd::ImageCtx>;
