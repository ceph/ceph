// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRRORING_WATCHER_H
#define CEPH_LIBRBD_MIRRORING_WATCHER_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectWatcher.h"
#include "librbd/mirroring_watcher/Types.h"

namespace librbd {

template <typename ImageCtxT = librbd::ImageCtx>
class MirroringWatcher : public ObjectWatcher<ImageCtxT> {
public:
  typedef typename std::decay<decltype(*ImageCtxT::op_work_queue)>::type ContextWQT;

  MirroringWatcher(librados::IoCtx &io_ctx, ContextWQT *work_queue);

  static int notify_mode_updated(librados::IoCtx &io_ctx,
                                 cls::rbd::MirrorMode mirror_mode);
  static int notify_image_updated(librados::IoCtx &io_ctx,
                                  cls::rbd::MirrorImageState mirror_image_state,
                                  const std::string &image_id,
                                  const std::string &global_image_id);

  virtual void handle_mode_updated(cls::rbd::MirrorMode mirror_mode,
                                   Context *on_ack) = 0;
  virtual void handle_image_updated(cls::rbd::MirrorImageState state,
                                    const std::string &image_id,
                                    const std::string &global_image_id,
                                    Context *on_ack) = 0;

protected:
  virtual std::string get_oid() const;

  virtual void handle_notify(uint64_t notify_id, uint64_t handle,
                             bufferlist &bl);

private:
  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    MirroringWatcher *mirroring_watcher;
    Context *on_notify_ack;

    HandlePayloadVisitor(MirroringWatcher *mirroring_watcher,
                         Context *on_notify_ack)
      : mirroring_watcher(mirroring_watcher), on_notify_ack(on_notify_ack) {
    }

    template <typename Payload>
    inline void operator()(const Payload &payload) const {
      mirroring_watcher->handle_payload(payload, on_notify_ack);
    }
  };

  void handle_payload(const mirroring_watcher::ModeUpdatedPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const mirroring_watcher::ImageUpdatedPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const mirroring_watcher::UnknownPayload &payload,
                      Context *on_notify_ack);

};

} // namespace librbd

extern template class librbd::MirroringWatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRRORING_WATCHER_H
