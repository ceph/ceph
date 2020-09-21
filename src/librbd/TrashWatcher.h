// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_TRASH_WATCHER_H
#define CEPH_LIBRBD_TRASH_WATCHER_H

#include "include/int_types.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Watcher.h"
#include "librbd/trash_watcher/Types.h"

namespace librbd {

namespace asio { struct ContextWQ; }
namespace watcher {
namespace util {
template <typename> struct HandlePayloadVisitor;
} // namespace util
} // namespace watcher

template <typename ImageCtxT = librbd::ImageCtx>
class TrashWatcher : public Watcher {
  friend struct watcher::util::HandlePayloadVisitor<TrashWatcher<ImageCtxT>>;
public:
  TrashWatcher(librados::IoCtx &io_ctx, asio::ContextWQ *work_queue);

  static void notify_image_added(librados::IoCtx &io_ctx,
                                 const std::string& image_id,
                                 const cls::rbd::TrashImageSpec& spec,
                                 Context *on_finish);
  static void notify_image_removed(librados::IoCtx &io_ctx,
                                   const std::string& image_id,
                                   Context *on_finish);

protected:
  virtual void handle_image_added(const std::string &image_id,
                                  const cls::rbd::TrashImageSpec& spec) = 0;
  virtual void handle_image_removed(const std::string &image_id) = 0;

private:
  void handle_notify(uint64_t notify_id, uint64_t handle,
                     uint64_t notifier_id, bufferlist &bl) override;

  bool handle_payload(const trash_watcher::ImageAddedPayload &payload,
                      Context *on_notify_ack);
  bool handle_payload(const trash_watcher::ImageRemovedPayload &payload,
                      Context *on_notify_ack);
  bool handle_payload(const trash_watcher::UnknownPayload &payload,
                      Context *on_notify_ack);
};

} // namespace librbd

extern template class librbd::TrashWatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_TRASH_WATCHER_H
