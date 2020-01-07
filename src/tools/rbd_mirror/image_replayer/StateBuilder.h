// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_STATE_BUILDER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_STATE_BUILDER_H

#include "cls/rbd/cls_rbd_types.h"

struct Context;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename ImageCtxT>
class StateBuilder {
public:
  StateBuilder(const StateBuilder&) = delete;
  StateBuilder& operator=(const StateBuilder&) = delete;

  virtual ~StateBuilder();

  virtual void destroy() {
    delete this;
  }

  virtual void close(Context* on_finish) = 0;

  virtual bool is_disconnected() const = 0;

  virtual bool is_local_primary() const = 0;
  virtual bool is_linked() const = 0;

  virtual cls::rbd::MirrorImageMode get_mirror_image_mode() const = 0;

  std::string global_image_id;

  std::string local_image_id;
  ImageCtxT* local_image_ctx = nullptr;

  std::string remote_mirror_uuid;
  std::string remote_image_id;

protected:
  StateBuilder(const std::string& global_image_id);

  void close_local_image(Context* on_finish);

private:

  void handle_close_local_image(int r, Context* on_finish);
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::StateBuilder<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_STATE_BUILDER_H
