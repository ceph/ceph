// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ObjectWatcher.h"
#include "include/Context.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ObjectWatcher: " << get_oid() << ": " \
                           << __func__

namespace librbd {

using util::create_context_callback;
using util::create_rados_safe_callback;

namespace {

struct C_UnwatchAndFlush : public Context {
  librados::Rados rados;
  Context *on_finish;
  bool flushing = false;
  int ret_val = 0;

  C_UnwatchAndFlush(librados::IoCtx &io_ctx, Context *on_finish)
    : rados(io_ctx), on_finish(on_finish) {
  }

  virtual void complete(int r) override {
    if (ret_val == 0 && r < 0) {
      ret_val = r;
    }

    if (!flushing) {
      flushing = true;

      librados::AioCompletion *aio_comp = create_rados_safe_callback(this);
      r = rados.aio_watch_flush(aio_comp);
      assert(r == 0);
      aio_comp->release();
    } else {
      Context::complete(ret_val);
    }
  }

  virtual void finish(int r) override {
    on_finish->complete(r);
  }
};

} // anonymous namespace

template <typename I>
ObjectWatcher<I>::ObjectWatcher(librados::IoCtx &io_ctx, ContextWQT *work_queue)
  : m_io_ctx(io_ctx), m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
    m_work_queue(work_queue),
    m_watch_lock(util::unique_lock_name("librbd::ObjectWatcher::m_watch_lock", this)),
    m_watch_ctx(this) {
}

template <typename I>
ObjectWatcher<I>::~ObjectWatcher() {
  RWLock::RLocker watch_locker(m_watch_lock);
  assert(m_watch_state == WATCH_STATE_UNREGISTERED);
}

template <typename I>
void ObjectWatcher<I>::register_watch(Context *on_finish) {
  ldout(m_cct, 5) << dendl;

  {
    RWLock::WLocker watch_locker(m_watch_lock);
    assert(on_finish != nullptr);
    assert(m_on_register_watch == nullptr);
    assert(m_watch_state == WATCH_STATE_UNREGISTERED);

    m_watch_state = WATCH_STATE_REGISTERING;
    m_on_register_watch = on_finish;
  }

  librados::AioCompletion *aio_comp = create_rados_safe_callback<
    ObjectWatcher<I>, &ObjectWatcher<I>::handle_register_watch>(this);
  int r = m_io_ctx.aio_watch(get_oid(), aio_comp, &m_watch_handle,
                             &m_watch_ctx);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ObjectWatcher<I>::handle_register_watch(int r) {
  ldout(m_cct, 20) << ": r=" << r << dendl;

  Context *on_register_watch = nullptr;
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    assert(m_watch_state == WATCH_STATE_REGISTERING);

    std::swap(on_register_watch, m_on_register_watch);
    if (r < 0) {
      lderr(m_cct) << ": failed to register watch: " << cpp_strerror(r)
                   << dendl;

      m_watch_state = WATCH_STATE_UNREGISTERED;
      m_watch_handle = 0;
    } else {
      m_watch_state = WATCH_STATE_REGISTERED;
    }
  }
  on_register_watch->complete(r);
}

template <typename I>
void ObjectWatcher<I>::unregister_watch(Context *on_finish) {
  ldout(m_cct, 5) << dendl;

  RWLock::WLocker watch_locker(m_watch_lock);
  assert(on_finish != nullptr);
  assert(m_on_unregister_watch == nullptr);
  assert(m_watch_state != WATCH_STATE_UNREGISTERED &&
         m_watch_state != WATCH_STATE_REGISTERING);

  m_on_unregister_watch = on_finish;
  if (m_watch_state == WATCH_STATE_REGISTERED) {
    unregister_watch_();
  }
}

template <typename I>
void ObjectWatcher<I>::unregister_watch_() {
  assert(m_watch_lock.is_wlocked());
  assert(m_on_unregister_watch != nullptr);
  assert(m_watch_state == WATCH_STATE_REGISTERED);
  m_watch_state = WATCH_STATE_UNREGISTERING;

  Context *ctx = create_context_callback<
    ObjectWatcher<I>, &ObjectWatcher<I>::handle_unregister_watch>(this);
  librados::AioCompletion *aio_comp = create_rados_safe_callback(
      new C_UnwatchAndFlush(m_io_ctx, ctx));
  int r = m_io_ctx.aio_unwatch(m_watch_handle, aio_comp);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ObjectWatcher<I>::handle_unregister_watch(int r) {
  ldout(m_cct, 20) << ": r=" << r << dendl;

  Context *on_unregister_watch = nullptr;
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    assert(m_watch_state == WATCH_STATE_UNREGISTERING);

    if (r < 0) {
      lderr(m_cct) << ": error encountered unregister watch: "
                   << cpp_strerror(r) << dendl;
    }

    m_watch_state = WATCH_STATE_UNREGISTERED;
    m_watch_handle = 0;
    std::swap(on_unregister_watch, m_on_unregister_watch);
  }

  on_unregister_watch->complete(r);
}

template <typename I>
void ObjectWatcher<I>::pre_unwatch(Context *on_finish) {
  ldout(m_cct, 20) << dendl;

  on_finish->complete(0);
}

template <typename I>
void ObjectWatcher<I>::post_rewatch(Context *on_finish) {
  ldout(m_cct, 20) << dendl;

  on_finish->complete(0);
}

template <typename I>
void ObjectWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
                                     bufferlist &bl) {
  ldout(m_cct, 15) << ": notify_id=" << notify_id << ", "
                   << "handle=" << handle << dendl;
}

template <typename I>
void ObjectWatcher<I>::acknowledge_notify(uint64_t notify_id, uint64_t handle,
                                          bufferlist &out) {
  ldout(m_cct, 15) << ": notify_id=" << notify_id << ", "
                   << "handle=" << handle << dendl;
  m_io_ctx.notify_ack(get_oid(), notify_id, handle, out);
}

template <typename I>
void ObjectWatcher<I>::handle_error(uint64_t handle, int err) {
  lderr(m_cct) << ": handle=" << handle << ", " << "err=" << err << dendl;

  RWLock::WLocker watch_locker(m_watch_lock);
  if (m_watch_state != WATCH_STATE_REGISTERED) {
    return;
  }

  m_watch_state = WATCH_STATE_REREGISTERING;
  Context *pre_unwatch_ctx = new FunctionContext([this](int r) {
      assert(r == 0);
      Context *ctx = create_context_callback<
        ObjectWatcher<I>, &ObjectWatcher<I>::handle_pre_unwatch>(this);
      pre_unwatch(ctx);
    });
  m_work_queue->queue(pre_unwatch_ctx, 0);
}

template <typename I>
void ObjectWatcher<I>::handle_pre_unwatch(int r) {
  ldout(m_cct, 20) << dendl;

  assert(r == 0);
  unwatch();
}

template <typename I>
void ObjectWatcher<I>::unwatch() {
  ldout(m_cct, 20) << dendl;

  {
    RWLock::RLocker watch_locker(m_watch_lock);
    assert(m_watch_state == WATCH_STATE_REREGISTERING);
  }

  Context *ctx = create_context_callback<
    ObjectWatcher<I>, &ObjectWatcher<I>::handle_unwatch>(this);
  librados::AioCompletion *aio_comp = create_rados_safe_callback(
    new C_UnwatchAndFlush(m_io_ctx, ctx));
  int r = m_io_ctx.aio_unwatch(m_watch_handle, aio_comp);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ObjectWatcher<I>::handle_unwatch(int r) {
  ldout(m_cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << ": error encountered during unwatch: " << cpp_strerror(r)
                 << dendl;
  }

  // handling pending unregister (if any)
  if (pending_unregister_watch(r)) {
    return;
  }

  rewatch();
}

template <typename I>
void ObjectWatcher<I>::rewatch() {
  ldout(m_cct, 20) << dendl;

  {
    RWLock::RLocker watch_locker(m_watch_lock);
    assert(m_watch_state == WATCH_STATE_REREGISTERING);
  }

  librados::AioCompletion *aio_comp = create_rados_safe_callback<
    ObjectWatcher<I>, &ObjectWatcher<I>::handle_rewatch>(this);
  int r = m_io_ctx.aio_watch(get_oid(), aio_comp, &m_watch_handle,
                                       &m_watch_ctx);
  assert(r == 0);
  aio_comp->release();

}

template <typename I>
void ObjectWatcher<I>::handle_rewatch(int r) {
  ldout(m_cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << ": error encountered during re-watch: " << cpp_strerror(r)
                 << dendl;
    m_watch_handle = 0;

    if (!pending_unregister_watch(0)) {
      rewatch();
    }
    return;
  }

  Context *ctx = create_context_callback<
    ObjectWatcher<I>, &ObjectWatcher<I>::handle_post_watch>(this);
  post_rewatch(ctx);
}

template <typename I>
void ObjectWatcher<I>::handle_post_watch(int r) {
  ldout(m_cct, 20) << dendl;

  assert(r == 0);

  RWLock::WLocker watch_locker(m_watch_lock);
  m_watch_state = WATCH_STATE_REGISTERED;

  // handling pending unregister (if any)
  if (m_on_unregister_watch != nullptr) {
    unregister_watch_();
    return;
  }
}

template <typename I>
bool ObjectWatcher<I>::pending_unregister_watch(int r) {
  Context *on_unregister_watch = nullptr;
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    assert(m_watch_state == WATCH_STATE_REREGISTERING);

    if (m_on_unregister_watch != nullptr) {
      m_watch_state = WATCH_STATE_UNREGISTERED;
      std::swap(on_unregister_watch, m_on_unregister_watch);
    }
  }

  if (on_unregister_watch != nullptr) {
    on_unregister_watch->complete(r);
    return true;
  }

  return false;
}

} // namespace librbd

template class librbd::ObjectWatcher<librbd::ImageCtx>;
