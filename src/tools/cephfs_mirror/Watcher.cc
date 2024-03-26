// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "include/stringify.h"
#include "aio_utils.h"
#include "watcher/RewatchRequest.h"
#include "Watcher.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::Watcher " << __func__

using cephfs::mirror::watcher::RewatchRequest;

namespace cephfs {
namespace mirror {

namespace {

struct C_UnwatchAndFlush : public Context {
  librados::Rados rados;
  Context *on_finish;
  bool flushing = false;
  int ret_val = 0;

  C_UnwatchAndFlush(librados::IoCtx &ioctx, Context *on_finish)
    : rados(ioctx), on_finish(on_finish) {
  }

  void complete(int r) override {
    if (ret_val == 0 && r < 0) {
      ret_val = r;
    }

    if (!flushing) {
      flushing = true;

      librados::AioCompletion *aio_comp =
        librados::Rados::aio_create_completion(
          this, &rados_callback<Context, &Context::complete>);
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

Watcher::Watcher(librados::IoCtx &ioctx, std::string_view oid, ContextWQ *work_queue)
  : m_oid(oid),
    m_ioctx(ioctx),
    m_work_queue(work_queue),
    m_lock(ceph::make_shared_mutex("cephfs::mirror::snap_watcher")),
    m_state(STATE_IDLE),
    m_watch_ctx(*this) {
}

Watcher::~Watcher() {
}

void Watcher::register_watch(Context *on_finish) {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  m_state = STATE_REGISTERING;

  on_finish = new C_RegisterWatch(this, on_finish);
  librados::AioCompletion *aio_comp =
    librados::Rados::aio_create_completion(on_finish, &rados_callback<Context, &Context::complete>);
  int r = m_ioctx.aio_watch(m_oid, aio_comp, &m_watch_handle, &m_watch_ctx);
  ceph_assert(r == 0);
  aio_comp->release();
}

void Watcher::handle_register_watch(int r, Context *on_finish) {
  dout(20) << ": r=" << r << dendl;

  bool watch_error = false;
  Context *unregister_watch_ctx = nullptr;
  {
    std::scoped_lock locker(m_lock);
    ceph_assert(m_state == STATE_REGISTERING);

    m_state = STATE_IDLE;
    if (r < 0) {
      derr << ": failed to register watch: " << cpp_strerror(r) << dendl;
      m_watch_handle = 0;
    }

    if (m_unregister_watch_ctx != nullptr) {
      std::swap(unregister_watch_ctx, m_unregister_watch_ctx);
    } else if (r == 0 && m_watch_error) {
      derr << ": re-registering after watch error" << dendl;
      m_state = STATE_REGISTERING;
      watch_error = true;
    } else {
      m_watch_blocklisted = (r == -EBLOCKLISTED);
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
  dout(20) << dendl;

  {
    std::scoped_lock locker(m_lock);
    if (m_state != STATE_IDLE) {
      dout(10) << ": delaying unregister -- watch register in progress" << dendl;
      ceph_assert(m_unregister_watch_ctx == nullptr);
      m_unregister_watch_ctx = new LambdaContext([this, on_finish](int r) {
                                                   unregister_watch(on_finish);
                                                 });
      return;
    } else if (is_registered()) {
      // watch is registered -- unwatch
      librados::AioCompletion *aio_comp =
        librados::Rados::aio_create_completion(new C_UnwatchAndFlush(m_ioctx, on_finish),
                                               &rados_callback<Context, &Context::complete>);
      int r = m_ioctx.aio_unwatch(m_watch_handle, aio_comp);
      ceph_assert(r == 0);
      aio_comp->release();
      m_watch_handle = 0;
      m_watch_blocklisted = false;
      return;
    }
  }

  on_finish->complete(0);
}

void Watcher::handle_error(uint64_t handle, int err) {
  derr << ": handle=" << handle << ": " << cpp_strerror(err) << dendl;

  std::scoped_lock locker(m_lock);
  m_watch_error = true;

  if (is_registered()) {
    m_state = STATE_REWATCHING;
    if (err == -EBLOCKLISTED) {
      m_watch_blocklisted = true;
    }
    m_work_queue->queue(new LambdaContext([this] {
                                            rewatch();
                                          }), 0);
  }
}

void Watcher::rewatch() {
  dout(20) << dendl;

  Context *unregister_watch_ctx = nullptr;
  {
    std::unique_lock locker(m_lock);
    ceph_assert(m_state == STATE_REWATCHING);

    if (m_unregister_watch_ctx != nullptr) {
      m_state = STATE_IDLE;
      std::swap(unregister_watch_ctx, m_unregister_watch_ctx);
    } else {
      m_watch_error = false;
      Context *ctx = new C_CallbackAdapter<Watcher, &Watcher::handle_rewatch>(this);
      auto req = RewatchRequest::create(m_ioctx, m_oid, m_lock,
                                        &m_watch_ctx, &m_watch_handle, ctx);
      req->send();
      return;
    }
  }

  unregister_watch_ctx->complete(0);
}

void Watcher::handle_rewatch(int r) {
  dout(20) << ": r=" << r << dendl;

  bool watch_error = false;
  Context *unregister_watch_ctx = nullptr;
  {
    std::scoped_lock locker(m_lock);
    ceph_assert(m_state == STATE_REWATCHING);

    m_watch_blocklisted = false;
    if (m_unregister_watch_ctx != nullptr) {
      dout(10) << ": skipping rewatch -- unregistering" << dendl;
      m_state = STATE_IDLE;
      std::swap(unregister_watch_ctx, m_unregister_watch_ctx);
    } else if (r == -EBLOCKLISTED) {
      m_watch_blocklisted = true;
      derr << ": client blocklisted" << dendl;
    } else if (r == -ENOENT) {
      dout(5) << ": object " << m_oid << " does not exist" << dendl;
    } else if  (r < 0) {
      derr << ": failed to rewatch: " << cpp_strerror(r) << dendl;
      watch_error = true;
    } else if (m_watch_error) {
      derr << ": re-registering watch after error" << dendl;
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

  Context *ctx = new C_CallbackAdapter<Watcher, &Watcher::handle_rewatch_callback>(this);
  m_work_queue->queue(ctx, r);
}

void Watcher::handle_rewatch_callback(int r) {
  dout(10) << ": r=" << r << dendl;
  handle_rewatch_complete(r);

  bool watch_error = false;
  Context *unregister_watch_ctx = nullptr;
  {
    std::scoped_lock locker(m_lock);
    ceph_assert(m_state == STATE_REWATCHING);

    if (m_unregister_watch_ctx != nullptr) {
      m_state = STATE_IDLE;
      std::swap(unregister_watch_ctx, m_unregister_watch_ctx);
    } else if (r == -EBLOCKLISTED || r == -ENOENT) {
      m_state = STATE_IDLE;
    } else if (r < 0 || m_watch_error) {
      watch_error = true;
    } else {
      m_state = STATE_IDLE;
    }
  }

  if (unregister_watch_ctx != nullptr) {
    unregister_watch_ctx->complete(0);
  } else if (watch_error) {
    rewatch();
  }
}

void Watcher::acknowledge_notify(uint64_t notify_id, uint64_t handle, bufferlist &bl) {
  m_ioctx.notify_ack(m_oid, notify_id, handle, bl);
}

void Watcher::WatchCtx::handle_notify(uint64_t notify_id, uint64_t handle,
                                        uint64_t notifier_id, bufferlist& bl) {
  dout(20) << ": notify_id=" << notify_id << ", handle=" << handle
           << ", notifier_id=" << notifier_id << dendl;
  watcher.handle_notify(notify_id, handle, notifier_id, bl);
}

void Watcher::WatchCtx::handle_error(uint64_t handle, int err) {
  dout(20) << dendl;
  watcher.handle_error(handle, err);
}

} // namespace mirror
} // namespace cephfs
