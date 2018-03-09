// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_client.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::" \
                           << "GetMirrorImageIdRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_rados_callback;

template <typename I>
void GetMirrorImageIdRequest<I>::send() {
  dout(20) << dendl;
  get_image_id();
}

template <typename I>
void GetMirrorImageIdRequest<I>::get_image_id() {
  dout(20) << dendl;

  // attempt to cross-reference a image id by the global image id
  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_image_id_start(&op, m_global_image_id);

  librados::AioCompletion *aio_comp = create_rados_callback<
    GetMirrorImageIdRequest<I>,
    &GetMirrorImageIdRequest<I>::handle_get_image_id>(
      this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GetMirrorImageIdRequest<I>::handle_get_image_id(int r) {
  if (r == 0) {
    bufferlist::iterator iter = m_out_bl.begin();
    r = librbd::cls_client::mirror_image_get_image_id_finish(
      &iter, m_image_id);
  }

  dout(20) << "r=" << r << ", "
           << "image_id=" << *m_image_id << dendl;

  if (r < 0) {
    if (r == -ENOENT) {
      dout(10) << "global image " << m_global_image_id << " not registered"
               << dendl;
    } else {
      derr << "failed to retrieve image id: " << cpp_strerror(r) << dendl;
    }
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void GetMirrorImageIdRequest<I>::finish(int r) {
  dout(20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::GetMirrorImageIdRequest<librbd::ImageCtx>;
