// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_WATCHER_H
#define CEPHFS_MIRROR_WATCHER_H

#include <string_view>

#include "common/ceph_mutex.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"

class ContextWQ;

namespace cephfs {
namespace mirror {

// generic watcher class -- establish watch on a given rados object
// and invoke handle_notify() when notified. On notify error, try
// to re-establish the watch. Errors during rewatch are notified via
// handle_rewatch_complete().

class Watcher {
public:
  Watcher(librados::IoCtx &ioctx, std::string_view oid, ContextWQ *work_queue);
  virtual ~Watcher();

  void register_watch(Context *on_finish);
  void unregister_watch(Context *on_finish);

  struct ErrorListener {
    virtual ~ErrorListener() {
    }
    virtual void set_blocklisted_ts() = 0;
    virtual void set_failed_ts() = 0;
  };

protected:
  std::string m_oid;

  void acknowledge_notify(uint64_t notify_if, uint64_t handle, bufferlist &bl);

  bool is_registered() const {
    return m_state == STATE_IDLE && m_watch_handle != 0;
  }
  bool is_unregistered() const {
    return m_state == STATE_IDLE && m_watch_handle == 0;
  }

  virtual void handle_rewatch_complete(int r) { }

private:
  enum State {
    STATE_IDLE,
    STATE_REGISTERING,
    STATE_REWATCHING
  };

  struct WatchCtx : public librados::WatchCtx2 {
    Watcher &watcher;

    WatchCtx(Watcher &parent) : watcher(parent) {}

    void handle_notify(uint64_t notify_id,
                       uint64_t handle,
                       uint64_t notifier_id,
                       bufferlist& bl) override;
    void handle_error(uint64_t handle, int err) override;
  };

  struct C_RegisterWatch : public Context {
    Watcher *watcher;
    Context *on_finish;

    C_RegisterWatch(Watcher *watcher, Context *on_finish)
      : watcher(watcher),
        on_finish(on_finish) {
    }

    void finish(int r) override {
      watcher->handle_register_watch(r, on_finish);
    }
  };

  librados::IoCtx &m_ioctx;
  ContextWQ *m_work_queue;

  mutable ceph::shared_mutex m_lock;
  State m_state;
  bool m_watch_error = false;
  bool m_watch_blocklisted = false;
  uint64_t m_watch_handle;
  WatchCtx m_watch_ctx;
  Context *m_unregister_watch_ctx = nullptr;

  virtual void handle_notify(uint64_t notify_id, uint64_t handle,
                     uint64_t notifier_id, bufferlist& bl) = 0;
  void handle_error(uint64_t handle, int err);

  void rewatch();
  void handle_rewatch(int r);
  void handle_rewatch_callback(int r);
  void handle_register_watch(int r, Context *on_finish);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_WATCHER_H
