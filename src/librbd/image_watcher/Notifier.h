// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_WATCHER_NOTIFIER_H
#define CEPH_LIBRBD_IMAGE_WATCHER_NOTIFIER_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include <list>

namespace librbd {

struct ImageCtx;

namespace image_watcher {

class Notifier {
public:
  static const uint64_t NOTIFY_TIMEOUT;

  Notifier(ImageCtx &image_ctx);
  ~Notifier();

  void flush(Context *on_finish);
  void notify(bufferlist &bl, bufferlist *out_bl, Context *on_finish);

private:
  typedef std::list<Context*> Contexts;

  struct C_AioNotify : public Context {
    Notifier *notifier;
    Context *on_finish;

    C_AioNotify(Notifier *notifier, Context *on_finish)
      : notifier(notifier), on_finish(on_finish) {
    }
    virtual void finish(int r) override {
      notifier->handle_notify(r, on_finish);
    }
  };

  ImageCtx &m_image_ctx;

  Mutex m_aio_notify_lock;
  size_t m_pending_aio_notifies = 0;
  Contexts m_aio_notify_flush_ctxs;

  void handle_notify(int r, Context *on_finish);

};

} // namespace image_watcher
} // namespace librbd

#endif // CEPH_LIBRBD_IMAGE_WATCHER_NOTIFIER_H
