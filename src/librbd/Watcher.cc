// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Watcher.h"
#include "librbd/watcher/RewatchRequest.h"
#include "librbd/Utils.h"
#include "librbd/TaskFinisher.h"
#include "include/encoding.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include <boost/bind.hpp>

// re-include our assert to clobber the system one; fix dout:
#include "include/assert.h"

#define dout_subsys ceph_subsys_rbd

namespace librbd {

using namespace watcher;

using util::create_context_callback;
using util::create_rados_callback;
using std::string;

namespace {

struct C_UnwatchAndFlush : public Context {
  librados::Rados rados;
  Context *on_finish;
  bool flushing = false;
  int ret_val = 0;

  C_UnwatchAndFlush(librados::IoCtx &io_ctx, Context *on_finish)
    : rados(io_ctx), on_finish(on_finish) {
  }

  void complete(int r) override {
    if (ret_val == 0 && r < 0) {
      ret_val = r;
    }

    if (!flushing) {
      flushing = true;

      librados::AioCompletion *aio_comp = create_rados_callback(this);
      r = rados.aio_watch_flush(aio_comp);
      assert(r == 0);
      aio_comp->release();
      return;
    }

    // ensure our reference to the RadosClient is released prior
    // to completing the callback to avoid racing an explicit
    // librados shutdown
    Context *ctx = on_finish;
    r = ret_val;
    delete this;

    ctx->complete(r);
  }

  void finish(int r) override {
  }
};

} // anonymous namespace

#undef dout_prefix
#define dout_prefix *_dout << "librbd::Watcher::C_NotifyAck " << this << " " \
                           << __func__ << ": "

Watcher::C_NotifyAck::C_NotifyAck(Watcher *watcher, uint64_t notify_id,
                                  uint64_t handle)
  : watcher(watcher), cct(watcher->m_cct), notify_id(notify_id),
    handle(handle) {
  ldout(cct, 10) << "id=" << notify_id << ", " << "handle=" << handle << dendl;
}

void Watcher::C_NotifyAck::finish(int r) {
  ldout(cct, 10) << "r=" << r << dendl;
  assert(r == 0);
  watcher->acknowledge_notify(notify_id, handle, out);
}

#undef dout_prefix
#define dout_prefix *_dout << "librbd::Watcher: " << this << " " << __func__ \
                           << ": "

Watcher::Watcher(librados::IoCtx& ioctx, ContextWQ *work_queue,
                          const string& oid)
  : m_ioctx(ioctx), m_work_queue(work_queue), m_oid(oid),
    m_cct(reinterpret_cast<CephContext *>(ioctx.cct())),
    m_watch_lock(util::unique_lock_name("librbd::Watcher::m_watch_lock", this)),
    m_watch_handle(0), m_notifier(work_queue, ioctx, oid),
    m_watch_state(WATCH_STATE_UNREGISTERED), m_watch_ctx(*this) {
}

Watcher::~Watcher() {
  RWLock::RLocker l(m_watch_lock);
  assert(m_watch_state != WATCH_STATE_REGISTERED);
}

void Watcher::register_watch(Context *on_finish) {
  ldout(m_cct, 10) << dendl;

  RWLock::RLocker watch_locker(m_watch_lock);
  assert(m_watch_state == WATCH_STATE_UNREGISTERED);
  m_watch_state = WATCH_STATE_REGISTERING;

  librados::AioCompletion *aio_comp = create_rados_callback(
                                         new C_RegisterWatch(this, on_finish));
  int r = m_ioctx.aio_watch(m_oid, aio_comp, &m_watch_handle, &m_watch_ctx);
  assert(r == 0);
  aio_comp->release();
}

void Watcher::handle_register_watch(int r, Context *on_finish) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  Context *unregister_watch_ctx = nullptr;
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    assert(m_watch_state == WATCH_STATE_REGISTERING);

    std::swap(unregister_watch_ctx, m_unregister_watch_ctx);
    if (r < 0) {
      lderr(m_cct) << "failed to register watch: " << cpp_strerror(r)
                   << dendl;
      m_watch_handle = 0;
      m_watch_state = WATCH_STATE_UNREGISTERED;
    } else {
      m_watch_state = WATCH_STATE_REGISTERED;
    }
  }

  on_finish->complete(r);

  // wake up pending unregister request
  if (unregister_watch_ctx != nullptr) {
    unregister_watch_ctx->complete(0);
  }
}

void Watcher::unregister_watch(Context *on_finish) {
  ldout(m_cct, 10) << dendl;

  {
    RWLock::WLocker watch_locker(m_watch_lock);
    if (m_watch_state == WATCH_STATE_REGISTERING ||
        m_watch_state == WATCH_STATE_REWATCHING) {
      ldout(m_cct, 10) << "delaying unregister until register completed"
                       << dendl;

      assert(m_unregister_watch_ctx == nullptr);
      m_unregister_watch_ctx = new FunctionContext([this, on_finish](int r) {
          unregister_watch(on_finish);
        });
      return;
    }

    if (m_watch_state == WATCH_STATE_REGISTERED ||
        m_watch_state == WATCH_STATE_ERROR) {
      m_watch_state = WATCH_STATE_UNREGISTERED;

      librados::AioCompletion *aio_comp = create_rados_callback(
                        new C_UnwatchAndFlush(m_ioctx, on_finish));
      int r = m_ioctx.aio_unwatch(m_watch_handle, aio_comp);
      assert(r == 0);
      aio_comp->release();
      return;
    }
  }

  on_finish->complete(0);
}

bool Watcher::notifications_blocked() const {
  RWLock::RLocker locker(m_watch_lock);

  bool blocked = (m_blocked_count > 0);
  ldout(m_cct, 5) << "blocked=" << blocked << dendl;
  return blocked;
}

void Watcher::block_notifies(Context *on_finish) {
  {
    RWLock::WLocker locker(m_watch_lock);
    ++m_blocked_count;
    ldout(m_cct, 5) << "blocked_count=" << m_blocked_count << dendl;
  }
  m_async_op_tracker.wait_for_ops(on_finish);
}

void Watcher::unblock_notifies() {
  RWLock::WLocker locker(m_watch_lock);
  assert(m_blocked_count > 0);
  --m_blocked_count;
  ldout(m_cct, 5) << "blocked_count=" << m_blocked_count << dendl;
}

void Watcher::flush(Context *on_finish) {
  m_notifier.flush(on_finish);
}

std::string Watcher::get_oid() const {
  RWLock::RLocker locker(m_watch_lock);
  return m_oid;
}

void Watcher::set_oid(const string& oid) {
  RWLock::WLocker l(m_watch_lock);
  assert(m_watch_state == WATCH_STATE_UNREGISTERED);

  m_oid = oid;
}

void Watcher::handle_error(uint64_t handle, int err) {
  lderr(m_cct) << "handle=" << handle << ": " << cpp_strerror(err) << dendl;

  RWLock::WLocker l(m_watch_lock);
  if (m_watch_state == WATCH_STATE_REGISTERED) {
    m_watch_state = WATCH_STATE_ERROR;

    FunctionContext *ctx = new FunctionContext(
        boost::bind(&Watcher::rewatch, this));
    m_work_queue->queue(ctx);
  }
}

void Watcher::acknowledge_notify(uint64_t notify_id, uint64_t handle,
	                         bufferlist &out) {
  m_ioctx.notify_ack(m_oid, notify_id, handle, out);
}

void Watcher::rewatch() {
  ldout(m_cct, 10) << dendl;

  RWLock::WLocker l(m_watch_lock);
  if (m_watch_state != WATCH_STATE_ERROR) {
    return;
  }
  m_watch_state = WATCH_STATE_REWATCHING;

  Context *ctx = create_context_callback<Watcher,
                                         &Watcher::handle_rewatch>(this);
  RewatchRequest *req = RewatchRequest::create(m_ioctx, m_oid, m_watch_lock,
                                               &m_watch_ctx,
                                               &m_watch_handle, ctx);
  req->send();
}

void Watcher::handle_rewatch(int r) {
  ldout(m_cct, 10) "r=" << r << dendl;

  WatchState next_watch_state = WATCH_STATE_REGISTERED;
  if (r < 0) {
    // only EBLACKLISTED or ENOENT can be returned
    assert(r == -EBLACKLISTED || r == -ENOENT);
    next_watch_state = WATCH_STATE_UNREGISTERED;
  }

  Context *unregister_watch_ctx = nullptr;
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    assert(m_watch_state == WATCH_STATE_REWATCHING);
    m_watch_state = next_watch_state;

    std::swap(unregister_watch_ctx, m_unregister_watch_ctx);

    m_work_queue->queue(
      create_context_callback<Watcher,
                              &Watcher::handle_rewatch_complete>(this), r);
  }

  // wake up pending unregister request
  if (unregister_watch_ctx != nullptr) {
    unregister_watch_ctx->complete(0);
  }
}

void Watcher::send_notify(bufferlist& payload,
                          watcher::NotifyResponse *response,
                          Context *on_finish) {
  m_notifier.notify(payload, response, on_finish);
}

void Watcher::WatchCtx::handle_notify(uint64_t notify_id, uint64_t handle,
                                      uint64_t notifier_id, bufferlist& bl) {
  // if notifications are blocked, finish the notification w/o
  // bubbling the notification up to the derived class
  watcher.m_async_op_tracker.start_op();
  if (watcher.notifications_blocked()) {
    bufferlist bl;
    watcher.acknowledge_notify(notify_id, handle, bl);
  } else {
    watcher.handle_notify(notify_id, handle, notifier_id, bl);
  }
  watcher.m_async_op_tracker.finish_op();
}

void Watcher::WatchCtx::handle_error(uint64_t handle, int err) {
  watcher.handle_error(handle, err);
}

} // namespace librbd
