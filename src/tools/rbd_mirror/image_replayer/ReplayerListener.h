// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_LISTENER_H
#define RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_LISTENER_H

namespace rbd {
namespace mirror {
namespace image_replayer {

struct ReplayerListener {
  virtual ~ReplayerListener() {}

  virtual void handle_notification() = 0;

  virtual void create_mirror_snapshot_start(
      const cls::rbd::MirrorSnapshotNamespace &remote_group_snap_ns,
      int64_t *local_group_pool_id, std::string *local_group_id,
      std::string *local_group_snap_id, Context *on_finish) = 0;
  virtual void create_mirror_snapshot_finish(
      const std::string &remote_group_snap_id, uint64_t snap_id,
      Context *on_finish) = 0;
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#endif // RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_LISTENER_H
