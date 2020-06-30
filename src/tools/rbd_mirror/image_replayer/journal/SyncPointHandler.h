// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_SYNC_POINT_HANDLER_H
#define RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_SYNC_POINT_HANDLER_H

#include "tools/rbd_mirror/image_sync/Types.h"
#include "librbd/journal/Types.h"

struct Context;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

template <typename> class StateBuilder;

template <typename ImageCtxT>
class SyncPointHandler : public image_sync::SyncPointHandler {
public:
  using SyncPoint = image_sync::SyncPoint;
  using SyncPoints = image_sync::SyncPoints;

  static SyncPointHandler* create(StateBuilder<ImageCtxT>* state_builder) {
    return new SyncPointHandler(state_builder);
  }
  SyncPointHandler(StateBuilder<ImageCtxT>* state_builder);

  SyncPoints get_sync_points() const override;
  librbd::SnapSeqs get_snap_seqs() const override;

  void update_sync_points(const librbd::SnapSeqs& snap_seqs,
                          const SyncPoints& sync_points,
                          bool sync_complete,
                          Context* on_finish) override;

private:
  StateBuilder<ImageCtxT>* m_state_builder;

  librbd::journal::MirrorPeerClientMeta m_client_meta_copy;

  void handle_update_sync_points(int r, Context* on_finish);

};

} // namespace journal
} // namespace image_sync
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::journal::SyncPointHandler<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_SYNC_POINT_HANDLER_H
