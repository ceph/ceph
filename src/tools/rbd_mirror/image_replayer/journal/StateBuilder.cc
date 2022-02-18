// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "StateBuilder.h"
#include "include/ceph_assert.h"
#include "include/Context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "tools/rbd_mirror/image_replayer/journal/CreateLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/PrepareReplayRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/Replayer.h"
#include "tools/rbd_mirror/image_replayer/journal/SyncPointHandler.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::journal::" \
                           << "StateBuilder: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

template <typename I>
StateBuilder<I>::StateBuilder(const std::string& global_image_id)
  : image_replayer::StateBuilder<I>(global_image_id) {
}

template <typename I>
StateBuilder<I>::~StateBuilder() {
  ceph_assert(remote_journaler == nullptr);
}

template <typename I>
void StateBuilder<I>::close(Context* on_finish) {
  dout(10) << dendl;

  // close the remote journaler after closing the local image
  // in case we have lost contact w/ the remote cluster and
  // will block
  on_finish = new LambdaContext([this, on_finish](int) {
      shut_down_remote_journaler(on_finish);
    });
  on_finish = new LambdaContext([this, on_finish](int) {
      this->close_local_image(on_finish);
    });
  this->close_remote_image(on_finish);
}

template <typename I>
bool StateBuilder<I>::is_disconnected() const {
  return (remote_client_state == cls::journal::CLIENT_STATE_DISCONNECTED);
}

template <typename I>
bool StateBuilder<I>::is_linked_impl() const {
  ceph_assert(!this->remote_mirror_uuid.empty());
  return (local_primary_mirror_uuid == this->remote_mirror_uuid);
}

template <typename I>
cls::rbd::MirrorImageMode StateBuilder<I>::get_mirror_image_mode() const {
  return cls::rbd::MIRROR_IMAGE_MODE_JOURNAL;
}

template <typename I>
image_sync::SyncPointHandler* StateBuilder<I>::create_sync_point_handler() {
  dout(10) << dendl;

  this->m_sync_point_handler = SyncPointHandler<I>::create(this);
  return this->m_sync_point_handler;
}

template <typename I>
BaseRequest* StateBuilder<I>::create_local_image_request(
    Threads<I>* threads,
    librados::IoCtx& local_io_ctx,
    const std::string& global_image_id,
    PoolMetaCache* pool_meta_cache,
    ProgressContext* progress_ctx,
    Context* on_finish) {
  return CreateLocalImageRequest<I>::create(
    threads, local_io_ctx, this->remote_image_ctx, this->global_image_id,
    pool_meta_cache, progress_ctx, this, on_finish);
}

template <typename I>
BaseRequest* StateBuilder<I>::create_prepare_replay_request(
    const std::string& local_mirror_uuid,
    ProgressContext* progress_ctx,
    bool* resync_requested,
    bool* syncing,
    Context* on_finish) {
  return PrepareReplayRequest<I>::create(
    local_mirror_uuid, this->remote_promotion_state, progress_ctx, this,
    resync_requested, syncing, on_finish);
}

template <typename I>
image_replayer::Replayer* StateBuilder<I>::create_replayer(
    Threads<I>* threads,
    InstanceWatcher<I>* instance_watcher,
    const std::string& local_mirror_uuid,
    PoolMetaCache* pool_meta_cache,
    ReplayerListener* replayer_listener) {
  return Replayer<I>::create(
    threads, local_mirror_uuid, this, replayer_listener);
}

template <typename I>
void StateBuilder<I>::shut_down_remote_journaler(Context* on_finish) {
  if (remote_journaler == nullptr) {
    on_finish->complete(0);
    return;
  }

  dout(10) << dendl;
  auto ctx = new LambdaContext([this, on_finish](int r) {
      handle_shut_down_remote_journaler(r, on_finish);
    });
  remote_journaler->shut_down(ctx);
}

template <typename I>
void StateBuilder<I>::handle_shut_down_remote_journaler(int r,
                                                        Context* on_finish) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to shut down remote journaler: " << cpp_strerror(r)
         << dendl;
  }

  delete remote_journaler;
  remote_journaler = nullptr;
  on_finish->complete(r);
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::journal::StateBuilder<librbd::ImageCtx>;
