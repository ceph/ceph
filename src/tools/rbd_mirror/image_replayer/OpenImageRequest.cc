// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "OpenImageRequest.h"
#include "CloseImageRequest.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include <type_traits>

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
                                      bool read_only, ContextWQ *work_queue,
                                      Context *on_finish)
  : m_io_ctx(io_ctx), m_image_ctx(image_ctx), m_image_id(image_id),
    m_read_only(read_only), m_work_queue(work_queue), m_on_finish(on_finish) {
}

template <typename I>
void OpenImageRequest<I>::send() {
  send_open_image();
}

template <typename I>
void OpenImageRequest<I>::send_open_image() {
  dout(20) << dendl;

  *m_image_ctx = I::create("", m_image_id, nullptr, m_io_ctx, m_read_only);

  Context *ctx = create_context_callback<
    OpenImageRequest<I>, &OpenImageRequest<I>::handle_open_image>(
      this);
  (*m_image_ctx)->state->open(false, ctx);
}

template <typename I>
void OpenImageRequest<I>::handle_open_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to open image '" << m_image_id << "': "
         << cpp_strerror(r) << dendl;
    send_close_image(r);
    return;
  }

  finish(0);
}

template <typename I>
void OpenImageRequest<I>::send_close_image(int r) {
  dout(20) << dendl;

  if (m_ret_val == 0 && r < 0) {
    m_ret_val = r;
  }

  Context *ctx = create_context_callback<
    OpenImageRequest<I>, &OpenImageRequest<I>::handle_close_image>(
      this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    m_image_ctx, m_work_queue, true, ctx);
  request->send();
}

template <typename I>
void OpenImageRequest<I>::handle_close_image(int r) {
  dout(20) << dendl;

  assert(r == 0);
  finish(m_ret_val);
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
