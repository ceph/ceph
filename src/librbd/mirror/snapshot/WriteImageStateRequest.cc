// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/WriteImageStateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/Utils.h"

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::WriteImageStateRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

namespace {

static size_t header_length() {
  bufferlist bl;
  ImageStateHeader header;

  using ceph::encode;
  encode(header, bl);

  return bl.length();
}

}
using librbd::util::create_rados_callback;

template <typename I>
WriteImageStateRequest<I>::WriteImageStateRequest(I *image_ctx,
                                                  uint64_t snap_id,
                                                  const ImageState &image_state,
                                                  Context *on_finish)
  : m_image_ctx(image_ctx), m_snap_id(snap_id), m_image_state(image_state),
    m_on_finish(on_finish), m_object_size(
      1 << image_ctx->config.template get_val<uint64_t>("rbd_default_order")) {
  bufferlist bl;
  encode(m_image_state, bl);

  m_object_count = 1 + (header_length() + bl.length()) / m_object_size;
  ImageStateHeader header(m_object_count);

  encode(header, m_bl);
  m_bl.claim_append(bl);
}

template <typename I>
void WriteImageStateRequest<I>::send() {
  write_object();
}

template <typename I>
void WriteImageStateRequest<I>::write_object() {
  CephContext *cct = m_image_ctx->cct;
  ceph_assert(m_object_count > 0);

  m_object_count--;

  auto oid = util::image_state_object_name(m_image_ctx, m_snap_id,
                                           m_object_count);
  ldout(cct, 15) << oid << dendl;

  size_t off = m_object_count * m_object_size;
  size_t len = std::min(m_bl.length() - off, m_object_size);
  bufferlist bl;
  bl.substr_of(m_bl, off, len);

  librados::ObjectWriteOperation op;
  op.write_full(bl);

  librados::AioCompletion *comp = create_rados_callback<
    WriteImageStateRequest<I>,
    &WriteImageStateRequest<I>::handle_write_object>(this);
  int r = m_image_ctx->md_ctx.aio_operate(oid, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void WriteImageStateRequest<I>::handle_write_object(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to write object: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (m_object_count == 0) {
    finish(0);
    return;
  }

  write_object();
}

template <typename I>
void WriteImageStateRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::WriteImageStateRequest<librbd::ImageCtx>;
