// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRRORING_WATCHER_H
#define CEPH_LIBRBD_MIRRORING_WATCHER_H

#include "include/int_types.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Watcher.h"
#include "librbd/mirroring_watcher/Types.h"

namespace librbd {

namespace watcher {
namespace util {
template <typename> struct HandlePayloadVisitor;
}
}

template <typename ImageCtxT = librbd::ImageCtx>
class MirroringWatcher : public Watcher {
  friend struct watcher::util::HandlePayloadVisitor<MirroringWatcher<ImageCtxT>>;

public:
  MirroringWatcher(librados::IoCtx &io_ctx, ContextWQ *work_queue);

  static int notify_mode_updated(librados::IoCtx &io_ctx,
                                 cls::rbd::MirrorMode mirror_mode);
  static void notify_mode_updated(librados::IoCtx &io_ctx,
                                  cls::rbd::MirrorMode mirror_mode,
                                  Context *on_finish);

  static int notify_image_updated(librados::IoCtx &io_ctx,
                                  cls::rbd::MirrorImageState mirror_image_state,
                                  const std::string &image_id,
                                  const std::string &global_image_id);
  static void notify_image_updated(librados::IoCtx &io_ctx,
                                   cls::rbd::MirrorImageState mirror_image_state,
                                   const std::string &image_id,
                                   const std::string &global_image_id,
                                   Context *on_finish);

  virtual void handle_mode_updated(cls::rbd::MirrorMode mirror_mode) = 0;
  virtual void handle_image_updated(cls::rbd::MirrorImageState state,
                                    const std::string &image_id,
                                    const std::string &global_image_id) = 0;

private:
  bool handle_payload(const mirroring_watcher::ModeUpdatedPayload &payload,
                      Context *on_notify_ack);
  bool handle_payload(const mirroring_watcher::ImageUpdatedPayload &payload,
                      Context *on_notify_ack);
  bool handle_payload(const mirroring_watcher::UnknownPayload &payload,
                      Context *on_notify_ack);

  void handle_notify(uint64_t notify_id, uint64_t handle,
                     uint64_t notifier_id, bufferlist &bl) override;
};

} // namespace librbd

extern template class librbd::MirroringWatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRRORING_WATCHER_H
