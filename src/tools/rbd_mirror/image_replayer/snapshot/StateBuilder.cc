// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "StateBuilder.h"
#include "include/ceph_assert.h"
#include "include/Context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/mirror/snapshot/ImageMeta.h"
#include "tools/rbd_mirror/image_replayer/snapshot/CreateLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/snapshot/PrepareReplayRequest.h"
#include "tools/rbd_mirror/image_replayer/snapshot/Replayer.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::snapshot::" \
                           << "StateBuilder: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

template <typename I>
StateBuilder<I>::StateBuilder(const std::string& global_image_id)
  : image_replayer::StateBuilder<I>(global_image_id) {
}

template <typename I>
StateBuilder<I>::~StateBuilder() {
  ceph_assert(local_image_meta == nullptr);
}

template <typename I>
void StateBuilder<I>::close(Context* on_finish) {
  dout(10) << dendl;

  delete local_image_meta;
  local_image_meta = nullptr;

  // close the remote image after closing the local
  // image in case the remote cluster is unreachable and
  // we cannot close it.
  on_finish = new LambdaContext([this, on_finish](int) {
      this->close_remote_image(on_finish);
    });
  this->close_local_image(on_finish);
}

template <typename I>
bool StateBuilder<I>::is_disconnected() const {
  return false;
}

template <typename I>
bool StateBuilder<I>::is_linked_impl() const {
  // the remote has to have us registered as a peer
  return !remote_mirror_peer_uuid.empty();
}

template <typename I>
cls::rbd::MirrorImageMode StateBuilder<I>::get_mirror_image_mode() const {
  return cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT;
}

template <typename I>
image_sync::SyncPointHandler* StateBuilder<I>::create_sync_point_handler() {
  dout(10) << dendl;

  // TODO
  ceph_assert(false);
  return nullptr;
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
    threads, local_io_ctx, this->remote_image_ctx, global_image_id,
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
    threads, instance_watcher, local_mirror_uuid, pool_meta_cache, this,
    replayer_listener);
}

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::snapshot::StateBuilder<librbd::ImageCtx>;
