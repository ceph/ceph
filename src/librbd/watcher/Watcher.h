// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_WATCHER_H
#define CEPH_LIBRBD_WATCHER_H

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "include/Context.h"
#include "include/rbd/librbd.hpp"
#include "librbd/object_watcher/Notifier.h"
#include "librbd/watcher/WatcherTypes.h"
#include <set>
#include <string>
#include <utility>

class ContextWQ;

namespace librbd {

template <typename T> class TaskFinisher;

namespace watcher {

template <typename Payload>
class Watcher {
public:
  Watcher(librados::IoCtx& ioctx, ContextWQ *work_queue,
          const std::string& oid);
  virtual ~Watcher();

  void register_watch(Context *on_finish);
  void unregister_watch(Context *on_finish);
  void flush(Context *on_finish);

  uint64_t get_watch_handle() const {
    RWLock::RLocker watch_locker(m_watch_lock);
    return m_watch_handle;
  }

  bool is_registered() const {
    RWLock::RLocker locker(m_watch_lock);
    return m_watch_state == WATCH_STATE_REGISTERED;
  }

protected:
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

  enum WatchState {
    WATCH_STATE_UNREGISTERED,
    WATCH_STATE_REGISTERED,
    WATCH_STATE_ERROR,
    WATCH_STATE_REWATCHING
  };

  struct WatchCtx : public librados::WatchCtx2 {
    Watcher &watcher;

    WatchCtx(Watcher &parent) : watcher(parent) {}

    virtual void handle_notify(uint64_t notify_id,
                               uint64_t handle,
      			       uint64_t notifier_id,
                               bufferlist& bl);
    virtual void handle_error(uint64_t handle, int err);
  };

  struct C_RegisterWatch : public Context {
    Watcher *watcher;
    Context *on_finish;

    C_RegisterWatch(Watcher *watcher, Context *on_finish)
       : watcher(watcher), on_finish(on_finish) {
    }
    virtual void finish(int r) override {
      watcher->handle_register_watch(r);
      on_finish->complete(r);
    }
  };
  struct C_NotifyAck : public Context {
    Watcher *watcher;
    uint64_t notify_id;
    uint64_t handle;
    bufferlist out;

    C_NotifyAck(Watcher *watcher, uint64_t notify_id, uint64_t handle);
    virtual void finish(int r);
  };

  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    Watcher *watcher;
    uint64_t notify_id;
    uint64_t handle;

    HandlePayloadVisitor(Watcher *watcher_, uint64_t notify_id_,
        uint64_t handle_)
      : watcher(watcher_), notify_id(notify_id_), handle(handle_)
    {
    }

    template <typename P>
    inline void operator()(const P &payload) const {
      C_NotifyAck *ctx = new C_NotifyAck(watcher, notify_id, handle);
      if (watcher->handle_payload(payload, ctx)) {
        ctx->complete(0);
      }
    }
  };

  class EncodePayloadVisitor : public boost::static_visitor<void> {
  public:
    explicit EncodePayloadVisitor(bufferlist &bl) : m_bl(bl) {}

    template <typename P>
    inline void operator()(const P &payload) const {
      ::encode(static_cast<uint32_t>(P::NOTIFY_OP), m_bl);
      payload.encode(m_bl);
    }

  private:
    bufferlist &m_bl;
  };

  class DecodePayloadVisitor : public boost::static_visitor<void> {
  public:
    DecodePayloadVisitor(__u8 version, bufferlist::iterator &iter)
      : m_version(version), m_iter(iter) {}

    template <typename P>
    inline void operator()(P &payload) const {
      payload.decode(m_version, m_iter);
    }

  private:
    __u8 m_version;
    bufferlist::iterator &m_iter;
  };

  struct NotifyMessage {
    NotifyMessage(Watcher *watcher) : watcher(watcher) { }
    NotifyMessage(const Payload &payload_)
      : payload(payload_), watcher(nullptr) {}

    Payload payload;
    Watcher *watcher;

    void encode(bufferlist& bl) const {
      ENCODE_START(4, 1, bl);
      boost::apply_visitor(EncodePayloadVisitor(bl), payload);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& it) {
      DECODE_START(1, it);

      uint32_t notify_op;
      ::decode(notify_op, it);
      payload = std::move(watcher->decode_payload_type(notify_op));
      apply_visitor(DecodePayloadVisitor(struct_v, it), payload);

      DECODE_FINISH(it);
    }
  };

  static const TaskCode TASK_CODE_REREGISTER_WATCH;

  virtual Payload decode_payload_type(uint32_t notify_op) = 0;
  template <typename I>
  bool handle_payload(const I& payload, C_NotifyAck *ctx) {
    return true;
  }

  librados::IoCtx& m_ioctx;
  CephContext *m_cct;
  ContextWQ *m_work_queue;
  std::string m_oid;

  mutable RWLock m_watch_lock;
  WatchCtx m_watch_ctx;
  uint64_t m_watch_handle;
  WatchState m_watch_state;
  Context *m_unregister_watch_ctx = nullptr;

  TaskFinisher<Task> *m_task_finisher;

  object_watcher::Notifier m_notifier;

  void handle_register_watch(int r);

  void process_payload(uint64_t notify_id, uint64_t handle,
                       const Payload &payload);

  void handle_notify(uint64_t notify_id, uint64_t handle, bufferlist &bl);
  virtual void handle_error(uint64_t cookie, int err);
  void acknowledge_notify(uint64_t notify_id, uint64_t handle, bufferlist &out);

  void rewatch();
  void handle_rewatch(int r);

  void send_notify(const Payload& payload);

  WRITE_CLASS_ENCODER(NotifyMessage);
};

} // namespace watcher
} // namespace librbd

#include "librbd/managed_lock/LockWatcherTypes.h"
extern template class librbd::watcher::Watcher<librbd::managed_lock::LockPayload>;

#endif // CEPH_LIBRBD_WATCHER_H
