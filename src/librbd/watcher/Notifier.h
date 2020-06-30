// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_WATCHER_NOTIFIER_H
#define CEPH_LIBRBD_WATCHER_NOTIFIER_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include <list>

namespace librbd {

namespace asio { struct ContextWQ; }

namespace watcher {

struct NotifyResponse;

class Notifier {
public:
  static const uint64_t NOTIFY_TIMEOUT;

  Notifier(asio::ContextWQ *work_queue, librados::IoCtx &ioctx,
           const std::string &oid);
  ~Notifier();

  void flush(Context *on_finish);
  void notify(bufferlist &bl, NotifyResponse *response, Context *on_finish);

private:
  typedef std::list<Context*> Contexts;

  struct C_AioNotify : public Context {
    Notifier *notifier;
    NotifyResponse *response;
    Context *on_finish;
    bufferlist out_bl;

    C_AioNotify(Notifier *notifier, NotifyResponse *response,
                Context *on_finish);

    void finish(int r) override;
  };

  asio::ContextWQ *m_work_queue;
  librados::IoCtx &m_ioctx;
  CephContext *m_cct;
  std::string m_oid;

  ceph::mutex m_aio_notify_lock;
  size_t m_pending_aio_notifies = 0;
  Contexts m_aio_notify_flush_ctxs;

  void handle_notify(int r, Context *on_finish);

};

} // namespace watcher
} // namespace librbd

#endif // CEPH_LIBRBD_WATCHER_NOTIFIER_H
