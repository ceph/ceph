// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_LISTENER_H
#define RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_LISTENER_H

namespace rbd {
namespace mirror {
namespace image_replayer {

struct ReplayerListener {
  virtual ~ReplayerListener() {}

  virtual void handle_notification() = 0;
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#endif // RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_LISTENER_H
