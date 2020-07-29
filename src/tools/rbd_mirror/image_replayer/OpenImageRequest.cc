// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "OpenImageRequest.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include <type_traits>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::OpenImageRequest: " \
                           << this << " " << __func__ << " "

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;

template <typename I>
OpenImageRequest<I>::OpenImageRequest(librados::IoCtx &io_ctx, I **image_ctx,
                                      const std::string &image_id,
                                      bool read_only, Context *on_finish)
  : m_io_ctx(io_ctx), m_image_ctx(image_ctx), m_image_id(image_id),
    m_read_only(read_only), m_on_finish(on_finish) {
}

template <typename I>
void OpenImageRequest<I>::send() {
  send_open_image();
}

template <typename I>
void OpenImageRequest<I>::send_open_image() {
  dout(20) << dendl;

  *m_image_ctx = I::create("", m_image_id, nullptr, m_io_ctx, m_read_only);

  if (!m_read_only) {
    // ensure non-primary images can be modified
    (*m_image_ctx)->read_only_mask = ~librbd::IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
  }

  Context *ctx = create_context_callback<
    OpenImageRequest<I>, &OpenImageRequest<I>::handle_open_image>(
      this);
  (*m_image_ctx)->state->open(0, ctx);
}

template <typename I>
void OpenImageRequest<I>::handle_open_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to open image '" << m_image_id << "': "
         << cpp_strerror(r) << dendl;
    *m_image_ctx = nullptr;
  }

  finish(r);
}

template <typename I>
void OpenImageRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::OpenImageRequest<librbd::ImageCtx>;
