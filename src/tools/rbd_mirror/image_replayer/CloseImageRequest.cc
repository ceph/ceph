// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CloseImageRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::CloseImageRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;

template <typename I>
CloseImageRequest<I>::CloseImageRequest(I **image_ctx, ContextWQ *work_queue,
                                        bool destroy_only, Context *on_finish)
  : m_image_ctx(image_ctx), m_work_queue(work_queue),
    m_destroy_only(destroy_only), m_on_finish(on_finish) {
}

template <typename I>
void CloseImageRequest<I>::send() {
  close_image();
}

template <typename I>
void CloseImageRequest<I>::close_image() {
  if (m_destroy_only) {
    switch_thread_context();
    return;
  }

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

  switch_thread_context();
}

template <typename I>
void CloseImageRequest<I>::switch_thread_context() {
  dout(20) << dendl;

  // swap the librbd thread context for the rbd-mirror thread context
  Context *ctx = create_context_callback<
    CloseImageRequest<I>, &CloseImageRequest<I>::handle_switch_thread_context>(
      this);
  m_work_queue->queue(ctx, 0);
}

template <typename I>
void CloseImageRequest<I>::handle_switch_thread_context(int r) {
  dout(20) << dendl;

  assert(r == 0);

  delete *m_image_ctx;
  *m_image_ctx = nullptr;

  m_on_finish->complete(0);
  delete this;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::CloseImageRequest<librbd::ImageCtx>;

