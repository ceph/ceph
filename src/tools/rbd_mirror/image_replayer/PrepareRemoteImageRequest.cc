// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_client.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::" \
                           << "PrepareRemoteImageRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void PrepareRemoteImageRequest<I>::send() {
  get_remote_mirror_uuid();
}

template <typename I>
void PrepareRemoteImageRequest<I>::get_remote_mirror_uuid() {
  dout(20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_uuid_get_start(&op);

  librados::AioCompletion *aio_comp = create_rados_callback<
    PrepareRemoteImageRequest<I>,
    &PrepareRemoteImageRequest<I>::handle_get_remote_mirror_uuid>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void PrepareRemoteImageRequest<I>::handle_get_remote_mirror_uuid(int r) {
  if (r >= 0) {
    bufferlist::iterator it = m_out_bl.begin();
    r = librbd::cls_client::mirror_uuid_get_finish(&it, m_remote_mirror_uuid);
    if (r >= 0 && m_remote_mirror_uuid->empty()) {
      r = -ENOENT;
    }
  }

  dout(20) << "r=" << r << dendl;
  if (r < 0) {
    if (r == -ENOENT) {
      dout(5) << "remote mirror uuid missing" << dendl;
    } else {
      derr << "failed to retrieve remote mirror uuid: " << cpp_strerror(r)
           << dendl;
    }
    finish(r);
    return;
  }

  get_remote_image_id();
}

template <typename I>
void PrepareRemoteImageRequest<I>::get_remote_image_id() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    PrepareRemoteImageRequest<I>,
    &PrepareRemoteImageRequest<I>::handle_get_remote_image_id>(this);
  auto req = GetMirrorImageIdRequest<I>::create(m_io_ctx, m_global_image_id,
                                                m_remote_image_id, ctx);
  req->send();
}

template <typename I>
void PrepareRemoteImageRequest<I>::handle_get_remote_image_id(int r) {
  dout(20) << "r=" << r << ", "
           << "remote_image_id=" << *m_remote_image_id << dendl;

  if (r < 0) {
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void PrepareRemoteImageRequest<I>::finish(int r) {
  dout(20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::PrepareRemoteImageRequest<librbd::ImageCtx>;
