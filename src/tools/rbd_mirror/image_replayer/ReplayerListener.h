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
  virtual void notify_group_snap_image_complete(
      int64_t local_pool_id,
      const std::string &local_image_id,
      const std::string &remote_group_snap_id,
      uint64_t local_snap_id) = 0;
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#endif // RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_LISTENER_H
