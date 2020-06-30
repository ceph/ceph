// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_WATCHER_NOTIFY_LOCK_OWNER_H
#define CEPH_LIBRBD_IMAGE_WATCHER_NOTIFY_LOCK_OWNER_H

#include "include/buffer.h"
#include "librbd/watcher/Types.h"

class Context;

namespace librbd {

struct ImageCtx;

namespace watcher { class Notifier; }

namespace image_watcher {

class NotifyLockOwner {
public:
  static NotifyLockOwner *create(ImageCtx &image_ctx,
                                 watcher::Notifier &notifier,
                                 bufferlist &&bl, Context *on_finish) {
    return new NotifyLockOwner(image_ctx, notifier, std::move(bl), on_finish);
  }

  NotifyLockOwner(ImageCtx &image_ctx, watcher::Notifier &notifier,
                  bufferlist &&bl, Context *on_finish);

  void send();

private:
  ImageCtx &m_image_ctx;
  watcher::Notifier &m_notifier;

  bufferlist m_bl;
  watcher::NotifyResponse m_notify_response;
  Context *m_on_finish;

  void send_notify();
  void handle_notify(int r);

  void finish(int r);
};

} // namespace image_watcher
} // namespace librbd

#endif // CEPH_LIBRBD_IMAGE_WATCHER_NOTIFY_LOCK_OWNER_H
