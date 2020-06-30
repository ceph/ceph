// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CloseImageRequest.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::CloseImageRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;

template <typename I>
CloseImageRequest<I>::CloseImageRequest(I **image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish) {
}

template <typename I>
void CloseImageRequest<I>::send() {
  close_image();
}

template <typename I>
void CloseImageRequest<I>::close_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    CloseImageRequest<I>, &CloseImageRequest<I>::handle_close_image>(this);
  (*m_image_ctx)->state->close(ctx);
}

template <typename I>
void CloseImageRequest<I>::handle_close_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": error encountered while closing image: " << cpp_strerror(r)
         << dendl;
  }

  delete *m_image_ctx;
  *m_image_ctx = nullptr;

  m_on_finish->complete(0);
  delete this;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::CloseImageRequest<librbd::ImageCtx>;

