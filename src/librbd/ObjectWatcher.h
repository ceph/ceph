// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_WATCHER_H
#define CEPH_LIBRBD_OBJECT_WATCHER_H

#include "include/rados/librados.hpp"
#include "common/RWLock.h"
#include "librbd/ImageCtx.h"
#include <string>
#include <type_traits>

class Context;

namespace librbd {

template <typename ImageCtxT = librbd::ImageCtx>
class ObjectWatcher {
public:
  typedef typename std::decay<decltype(*ImageCtxT::op_work_queue)>::type ContextWQT;

  ObjectWatcher(librados::IoCtx &io_ctx, ContextWQT *work_queue);
  virtual ~ObjectWatcher();

  ObjectWatcher(const ObjectWatcher&) = delete;
  ObjectWatcher& operator= (const ObjectWatcher&) = delete;

  void register_watch(Context *on_finish);
  virtual void unregister_watch(Context *on_finish);

protected:
  librados::IoCtx &m_io_ctx;
  CephContext *m_cct;

  virtual std::string get_oid() const = 0;

  virtual void handle_notify(uint64_t notify_id, uint64_t handle,
                             bufferlist &bl);
  void acknowledge_notify(uint64_t notify_id, uint64_t handle, bufferlist &out);

  virtual void pre_unwatch(Context *on_finish);
  virtual void post_rewatch(Context *on_finish);

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * REGISTER_WATCH
   *    |
   *    |   /-------------------------------------\
   *    |   |                                     |
   *    v   v   (watch error)                     |
   * REGISTERED * * * * * * * > PRE_UNWATCH       |
   *    |                         |               |
   *    |                         v               |
   *    |                       UNWATCH           |
   *    |                         |               |
   *    |                         v               |
   *    |                       REWATCH           |
   *    |                         |               |
   *    |                         v               |
   *    |                       POST_REWATCH      |
   *    |                         |               |
   *    v                         \---------------/
   * UNREGISTER_WATCH
   *    |
   *    v
   * UNREGISTERED
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  struct WatchCtx : public librados::WatchCtx2 {
    ObjectWatcher *object_watcher;

    WatchCtx(ObjectWatcher *object_watcher) : object_watcher(object_watcher) {
    }

    virtual void handle_notify(uint64_t notify_id,
                               uint64_t handle,
                               uint64_t notifier_id,
                               bufferlist& bl) {
      object_watcher->handle_notify(notify_id, handle, bl);
    }

    virtual void handle_error(uint64_t handle, int err) {
      object_watcher->handle_error(handle, err);
    }
  };

  enum WatchState {
    WATCH_STATE_UNREGISTERED,
    WATCH_STATE_REGISTERING,
    WATCH_STATE_REGISTERED,
    WATCH_STATE_UNREGISTERING,
    WATCH_STATE_REREGISTERING
  };

  ContextWQT* m_work_queue;

  mutable RWLock m_watch_lock;
  WatchCtx m_watch_ctx;
  uint64_t m_watch_handle = 0;
  WatchState m_watch_state = WATCH_STATE_UNREGISTERED;

  Context *m_on_register_watch = nullptr;
  Context *m_on_unregister_watch = nullptr;

  void handle_register_watch(int r);

  void unregister_watch_();
  void handle_unregister_watch(int r);

  void handle_error(uint64_t handle, int err);

  void handle_pre_unwatch(int r);

  void unwatch();
  void handle_unwatch(int r);

  void rewatch();
  void handle_rewatch(int r);

  void handle_post_watch(int r);

  bool pending_unregister_watch(int r);

};

} // namespace librbd

extern template class librbd::ObjectWatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OBJECT_WATCHER_H
