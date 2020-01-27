// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_STATE_BUILDER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_STATE_BUILDER_H

#include "tools/rbd_mirror/image_replayer/StateBuilder.h"
#include <string>

struct Context;

namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

template <typename> class SyncPointHandler;

template <typename ImageCtxT>
class StateBuilder : public image_replayer::StateBuilder<ImageCtxT> {
public:
  static StateBuilder* create(const std::string& global_image_id) {
    return new StateBuilder(global_image_id);
  }

  StateBuilder(const std::string& global_image_id);
  ~StateBuilder() override;

  void close(Context* on_finish) override;

  bool is_disconnected() const override;

  cls::rbd::MirrorImageMode get_mirror_image_mode() const override;

  image_sync::SyncPointHandler* create_sync_point_handler() override;

  BaseRequest* create_local_image_request(
      Threads<ImageCtxT>* threads,
      librados::IoCtx& local_io_ctx,
      ImageCtxT* remote_image_ctx,
      const std::string& global_image_id,
      ProgressContext* progress_ctx,
      Context* on_finish) override;

  BaseRequest* create_prepare_replay_request(
      const std::string& local_mirror_uuid,
      ProgressContext* progress_ctx,
      bool* resync_requested,
      bool* syncing,
      Context* on_finish) override;

  image_replayer::Replayer* create_replayer(
      Threads<ImageCtxT>* threads,
      const std::string& local_mirror_uuid,
      ReplayerListener* replayer_listener) override;

  SyncPointHandler<ImageCtxT>* sync_point_handler = nullptr;

};

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::snapshot::StateBuilder<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_STATE_BUILDER_H
