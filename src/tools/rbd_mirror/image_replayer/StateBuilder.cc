// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "StateBuilder.h"
#include "include/ceph_assert.h"
#include "include/Context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_sync/Types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::" \
                           << "StateBuilder: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename I>
StateBuilder<I>::StateBuilder(const std::string& global_image_id)
  : global_image_id(global_image_id) {
  dout(10) << "global_image_id=" << global_image_id << dendl;
}

template <typename I>
StateBuilder<I>::~StateBuilder() {
  ceph_assert(local_image_ctx == nullptr);
  ceph_assert(remote_image_ctx == nullptr);
  ceph_assert(m_sync_point_handler == nullptr);
}

template <typename I>
bool StateBuilder<I>::is_local_primary() const  {
  return (!local_image_id.empty() &&
          local_promotion_state == librbd::mirror::PROMOTION_STATE_PRIMARY);
}

template <typename I>
bool StateBuilder<I>::is_linked() const {
  return ((local_promotion_state ==
             librbd::mirror::PROMOTION_STATE_NON_PRIMARY) &&
          is_linked_impl());
}

template <typename I>
void StateBuilder<I>::close_local_image(Context* on_finish) {
  if (local_image_ctx == nullptr) {
    on_finish->complete(0);
    return;
  }

  dout(10) << dendl;
  auto ctx = new LambdaContext([this, on_finish](int r) {
      handle_close_local_image(r, on_finish);
    });
  auto request = image_replayer::CloseImageRequest<I>::create(
    &local_image_ctx, ctx);
  request->send();
}

template <typename I>
void StateBuilder<I>::handle_close_local_image(int r, Context* on_finish) {
  dout(10) << "r=" << r << dendl;

  ceph_assert(local_image_ctx == nullptr);
  if (r < 0) {
    derr << "failed to close local image for image " << global_image_id << ": "
         << cpp_strerror(r) << dendl;
  }

  on_finish->complete(r);
}

template <typename I>
void StateBuilder<I>::close_remote_image(Context* on_finish) {
  if (remote_image_ctx == nullptr) {
    on_finish->complete(0);
    return;
  }

  dout(10) << dendl;
  auto ctx = new LambdaContext([this, on_finish](int r) {
      handle_close_remote_image(r, on_finish);
    });
  auto request = image_replayer::CloseImageRequest<I>::create(
    &remote_image_ctx, ctx);
  request->send();
}

template <typename I>
void StateBuilder<I>::handle_close_remote_image(int r, Context* on_finish) {
  dout(10) << "r=" << r << dendl;

  ceph_assert(remote_image_ctx == nullptr);
  if (r < 0) {
    derr << "failed to close remote image for image " << global_image_id << ": "
         << cpp_strerror(r) << dendl;
  }

  on_finish->complete(r);
}

template <typename I>
void StateBuilder<I>::destroy_sync_point_handler() {
  if (m_sync_point_handler == nullptr) {
    return;
  }

  dout(15) << dendl;
  m_sync_point_handler->destroy();
  m_sync_point_handler = nullptr;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::StateBuilder<librbd::ImageCtx>;
