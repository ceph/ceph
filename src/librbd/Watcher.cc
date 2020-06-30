// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Watcher.h"
#include "librbd/watcher/RewatchRequest.h"
#include "librbd/Utils.h"
#include "librbd/TaskFinisher.h"
#include "librbd/asio/ContextWQ.h"
#include "include/encoding.h"
#include "common/errno.h"
#include <boost/bind.hpp>

// re-include our assert to clobber the system one; fix dout:
#include "include/ceph_assert.h"

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
      ceph_assert(r == 0);
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
  ceph_assert(r == 0);
  watcher->acknowledge_notify(notify_id, handle, out);
}

#undef dout_prefix
#define dout_prefix *_dout << "librbd::Watcher: " << this << " " << __func__ \
                           << ": "

Watcher::Watcher(librados::IoCtx& ioctx, asio::ContextWQ *work_queue,
                          const string& oid)
  : m_ioctx(ioctx), m_work_queue(work_queue), m_oid(oid),
    m_cct(reinterpret_cast<CephContext *>(ioctx.cct())),
    m_watch_lock(ceph::make_shared_mutex(
      util::unique_lock_name("librbd::Watcher::m_watch_lock", this))),
    m_watch_handle(0), m_notifier(work_queue, ioctx, oid),
    m_watch_state(WATCH_STATE_IDLE), m_watch_ctx(*this) {
}

Watcher::~Watcher() {
  std::shared_lock l{m_watch_lock};
  ceph_assert(is_unregistered(m_watch_lock));
}

void Watcher::register_watch(Context *on_finish) {
  ldout(m_cct, 10) << dendl;

  std::unique_lock watch_locker{m_watch_lock};
  ceph_assert(is_unregistered(m_watch_lock));
  m_watch_state = WATCH_STATE_REGISTERING;
  m_watch_blacklisted = false;

  librados::AioCompletion *aio_comp = create_rados_callback(
    new C_RegisterWatch(this, on_finish));
  int r = m_ioctx.aio_watch(m_oid, aio_comp, &m_watch_handle, &m_watch_ctx);
  ceph_assert(r == 0);
  aio_comp->release();
}

void Watcher::handle_register_watch(int r, Context *on_finish) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  bool watch_error = false;
  Context *unregister_watch_ctx = nullptr;
  {
    std::unique_lock watch_locker{m_watch_lock};
    ceph_assert(m_watch_state == WATCH_STATE_REGISTERING);

    m_watch_state = WATCH_STATE_IDLE;
    if (r < 0) {
      lderr(m_cct) << "failed to register watch: " << cpp_strerror(r)
                   << dendl;
      m_watch_handle = 0;
    }

    if (m_unregister_watch_ctx != nullptr) {
      std::swap(unregister_watch_ctx, m_unregister_watch_ctx);
    } else if (r == 0 && m_watch_error) {
      lderr(m_cct) << "re-registering watch after error" << dendl;
      m_watch_state = WATCH_STATE_REWATCHING;
      watch_error = true;
    } else {
      m_watch_blacklisted = (r == -EBLACKLISTED);
    }
  }

  on_finish->complete(r);

  if (unregister_watch_ctx != nullptr) {
    unregister_watch_ctx->complete(0);
  } else if (watch_error) {
    rewatch();
  }
}

void Watcher::unregister_watch(Context *on_finish) {
  ldout(m_cct, 10) << dendl;

  {
    std::unique_lock watch_locker{m_watch_lock};
    if (m_watch_state != WATCH_STATE_IDLE) {
      ldout(m_cct, 10) << "delaying unregister until register completed"
                       << dendl;

      ceph_assert(m_unregister_watch_ctx == nullptr);
      m_unregister_watch_ctx = new LambdaContext([this, on_finish](int r) {
          unregister_watch(on_finish);
        });
      return;
    } else if (is_registered(m_watch_lock)) {
      librados::AioCompletion *aio_comp = create_rados_callback(
        new C_UnwatchAndFlush(m_ioctx, on_finish));
      int r = m_ioctx.aio_unwatch(m_watch_handle, aio_comp);
      ceph_assert(r == 0);
      aio_comp->release();

      m_watch_handle = 0;
      m_watch_blacklisted = false;
      return;
    }
  }

  on_finish->complete(0);
}

bool Watcher::notifications_blocked() const {
  std::shared_lock locker{m_watch_lock};

  bool blocked = (m_blocked_count > 0);
  ldout(m_cct, 5) << "blocked=" << blocked << dendl;
  return blocked;
}

void Watcher::block_notifies(Context *on_finish) {
  {
    std::unique_lock locker{m_watch_lock};
    ++m_blocked_count;
    ldout(m_cct, 5) << "blocked_count=" << m_blocked_count << dendl;
  }
  m_async_op_tracker.wait_for_ops(on_finish);
}

void Watcher::unblock_notifies() {
  std::unique_lock locker{m_watch_lock};
  ceph_assert(m_blocked_count > 0);
  --m_blocked_count;
  ldout(m_cct, 5) << "blocked_count=" << m_blocked_count << dendl;
}

void Watcher::flush(Context *on_finish) {
  m_notifier.flush(on_finish);
}

std::string Watcher::get_oid() const {
  std::shared_lock locker{m_watch_lock};
  return m_oid;
}

void Watcher::set_oid(const string& oid) {
  std::unique_lock watch_locker{m_watch_lock};
  ceph_assert(is_unregistered(m_watch_lock));

  m_oid = oid;
}

void Watcher::handle_error(uint64_t handle, int err) {
  lderr(m_cct) << "handle=" << handle << ": " << cpp_strerror(err) << dendl;

  std::unique_lock watch_locker{m_watch_lock};
  m_watch_error = true;

  if (is_registered(m_watch_lock)) {
    m_watch_state = WATCH_STATE_REWATCHING;
    if (err == -EBLACKLISTED) {
      m_watch_blacklisted = true;
    }

    auto ctx = new LambdaContext(
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

  Context *unregister_watch_ctx = nullptr;
  {
    std::unique_lock watch_locker{m_watch_lock};
    ceph_assert(m_watch_state == WATCH_STATE_REWATCHING);

    if (m_unregister_watch_ctx != nullptr) {
      m_watch_state = WATCH_STATE_IDLE;
      std::swap(unregister_watch_ctx, m_unregister_watch_ctx);
    } else {
      m_watch_error = false;
      auto ctx = create_context_callback<
        Watcher, &Watcher::handle_rewatch>(this);
      auto req = RewatchRequest::create(m_ioctx, m_oid, m_watch_lock,
                                        &m_watch_ctx, &m_watch_handle, ctx);
      req->send();
      return;
    }
  }

  unregister_watch_ctx->complete(0);
}

void Watcher::handle_rewatch(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  bool watch_error = false;
  Context *unregister_watch_ctx = nullptr;
  {
    std::unique_lock watch_locker{m_watch_lock};
    ceph_assert(m_watch_state == WATCH_STATE_REWATCHING);

    m_watch_blacklisted = false;
    if (m_unregister_watch_ctx != nullptr) {
      ldout(m_cct, 10) << "image is closing, skip rewatch" << dendl;
      m_watch_state = WATCH_STATE_IDLE;
      std::swap(unregister_watch_ctx, m_unregister_watch_ctx);
    } else if (r  == -EBLACKLISTED) {
      lderr(m_cct) << "client blacklisted" << dendl;
      m_watch_blacklisted = true;
    } else if (r == -ENOENT) {
      ldout(m_cct, 5) << "object does not exist" << dendl;
    } else if (r < 0) {
      lderr(m_cct) << "failed to rewatch: " << cpp_strerror(r) << dendl;
      watch_error = true;
    } else if (m_watch_error) {
      lderr(m_cct) << "re-registering watch after error" << dendl;
      watch_error = true;
    }
  }

  if (unregister_watch_ctx != nullptr) {
    unregister_watch_ctx->complete(0);
    return;
  } else if (watch_error) {
    rewatch();
    return;
  }

  auto ctx = create_context_callback<
    Watcher, &Watcher::handle_rewatch_callback>(this);
  m_work_queue->queue(ctx, r);
}

void Watcher::handle_rewatch_callback(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  handle_rewatch_complete(r);

  bool watch_error = false;
  Context *unregister_watch_ctx = nullptr;
  {
    std::unique_lock watch_locker{m_watch_lock};
    ceph_assert(m_watch_state == WATCH_STATE_REWATCHING);

    if (m_unregister_watch_ctx != nullptr) {
      m_watch_state = WATCH_STATE_IDLE;
      std::swap(unregister_watch_ctx, m_unregister_watch_ctx);
    } else if (r == -EBLACKLISTED || r == -ENOENT) {
      m_watch_state = WATCH_STATE_IDLE;
    } else if (r < 0 || m_watch_error) {
      watch_error = true;
    } else {
      m_watch_state = WATCH_STATE_IDLE;
    }
  }

  if (unregister_watch_ctx != nullptr) {
    unregister_watch_ctx->complete(0);
  } else if (watch_error) {
    rewatch();
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
