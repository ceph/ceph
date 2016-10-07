// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_LOCK_NOTIFY_LOCK_OWNER_H
#define CEPH_LIBRBD_LOCK_NOTIFY_LOCK_OWNER_H

#include "include/buffer.h"

class Context;
class CephContext;

namespace librbd {

namespace object_watcher { class Notifier; }

namespace lock {

class NotifyLockOwner {
public:
  static NotifyLockOwner *create(CephContext *cct,
                                 object_watcher::Notifier &notifier,
                                 bufferlist &&bl, Context *on_finish) {
    return new NotifyLockOwner(cct, notifier, std::move(bl), on_finish);
  }

  NotifyLockOwner(CephContext *cct, object_watcher::Notifier &notifier,
                  bufferlist &&bl, Context *on_finish);

  void send();

private:
  CephContext *m_cct;
  object_watcher::Notifier &m_notifier;

  bufferlist m_bl;
  bufferlist m_out_bl;
  Context *m_on_finish;

  void send_notify();
  void handle_notify(int r);

  void finish(int r);
};

} // namespace lock
} // namespace librbd

#endif // CEPH_LIBRBD_LOCK_NOTIFY_LOCK_OWNER_H
