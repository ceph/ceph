// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_WATCHER_H
#define CEPH_LIBRBD_WATCHER_H

#include "common/AsyncOpTracker.h"
#include "common/ceph_mutex.h"
#include "common/RWLock.h"
#include "include/rados/librados.hpp"
#include "librbd/watcher/Notifier.h"
#include "librbd/watcher/Types.h"
#include <string>
#include <utility>

namespace librbd {

namespace asio { struct ContextWQ; }
namespace watcher { struct NotifyResponse; }

class Watcher {
public:
  struct C_NotifyAck : public Context {
    Watcher *watcher;
    CephContext *cct;
    uint64_t notify_id;
    uint64_t handle;
    bufferlist out;

    C_NotifyAck(Watcher *watcher, uint64_t notify_id, uint64_t handle);
    void finish(int r) override;
  };

  Watcher(librados::IoCtx& ioctx, asio::ContextWQ *work_queue,
          const std::string& oid);
  virtual ~Watcher();

  void register_watch(Context *on_finish);
  virtual void unregister_watch(Context *on_finish);
  void flush(Context *on_finish);

  bool notifications_blocked() const;
  virtual void block_notifies(Context *on_finish);
  void unblock_notifies();

  std::string get_oid() const;
  void set_oid(const string& oid);

  uint64_t get_watch_handle() const {
    std::shared_lock watch_locker{m_watch_lock};
    return m_watch_handle;
  }

  bool is_registered() const {
    std::shared_lock locker{m_watch_lock};
    return is_registered(m_watch_lock);
  }
  bool is_unregistered() const {
    std::shared_lock locker{m_watch_lock};
    return is_unregistered(m_watch_lock);
  }
  bool is_blacklisted() const {
    std::shared_lock locker{m_watch_lock};
    return m_watch_blacklisted;
  }

protected:
  enum WatchState {
    WATCH_STATE_IDLE,
    WATCH_STATE_REGISTERING,
    WATCH_STATE_REWATCHING
  };

  librados::IoCtx& m_ioctx;
  asio::ContextWQ *m_work_queue;
  std::string m_oid;
  CephContext *m_cct;
  mutable ceph::shared_mutex m_watch_lock;
  uint64_t m_watch_handle;
  watcher::Notifier m_notifier;

  WatchState m_watch_state;
  bool m_watch_blacklisted = false;

  AsyncOpTracker m_async_op_tracker;

  bool is_registered(const ceph::shared_mutex&) const {
    return (m_watch_state == WATCH_STATE_IDLE && m_watch_handle != 0);
  }
  bool is_unregistered(const ceph::shared_mutex&) const {
    return (m_watch_state == WATCH_STATE_IDLE && m_watch_handle == 0);
  }

  void send_notify(bufferlist &payload,
                   watcher::NotifyResponse *response = nullptr,
                   Context *on_finish = nullptr);

  virtual void handle_notify(uint64_t notify_id, uint64_t handle,
                             uint64_t notifier_id, bufferlist &bl) = 0;

  virtual void handle_error(uint64_t cookie, int err);

  void acknowledge_notify(uint64_t notify_id, uint64_t handle,
                          bufferlist &out);

  virtual void handle_rewatch_complete(int r) { }

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * UNREGISTERED
   *    |
   *    | (register_watch)
   *    |
   * REGISTERING
   *    |
   *    v      (watch error)
   * REGISTERED * * * * * * * > ERROR
   *    |   ^                     |
   *    |   |                     | (rewatch)
   *    |   |                     v
   *    |   |                   REWATCHING
   *    |   |                     |
   *    |   |                     |
   *    |   \---------------------/
   *    |
   *    | (unregister_watch)
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
       : watcher(watcher), on_finish(on_finish) {
    }
    void finish(int r) override {
      watcher->handle_register_watch(r, on_finish);
    }
  };

  WatchCtx m_watch_ctx;
  Context *m_unregister_watch_ctx = nullptr;

  bool m_watch_error = false;

  uint32_t m_blocked_count = 0;

  void handle_register_watch(int r, Context *on_finish);

  void rewatch();
  void handle_rewatch(int r);
  void handle_rewatch_callback(int r);

};

} // namespace librbd

#endif // CEPH_LIBRBD_WATCHER_H
