// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/Replay.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::Replay: "

namespace librbd {
namespace journal {

namespace {

static const uint64_t IN_FLIGHT_IO_LOW_WATER_MARK(32);
static const uint64_t IN_FLIGHT_IO_HIGH_WATER_MARK(64);

static NoOpProgressContext no_op_progress_callback;

} // anonymous namespace

template <typename I>
Replay<I>::Replay(I &image_ctx)
  : m_image_ctx(image_ctx), m_lock("Replay<I>::m_lock") {
}

template <typename I>
Replay<I>::~Replay() {
  assert(m_in_flight_aio == 0);
  assert(m_aio_modify_unsafe_contexts.empty());
  assert(m_aio_modify_safe_contexts.empty());
  assert(m_op_contexts.empty());
}

template <typename I>
void Replay<I>::process(bufferlist::iterator *it, Context *on_ready,
                        Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  journal::EventEntry event_entry;
  try {
    ::decode(event_entry, *it);
  } catch (const buffer::error &err) {
    lderr(cct) << "failed to decode event entry: " << err.what() << dendl;
    on_ready->complete(-EINVAL);
    return;
  }

  Mutex::Locker locker(m_lock);
  RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
  boost::apply_visitor(EventVisitor(this, on_ready, on_safe),
                       event_entry.event);
}

template <typename I>
void Replay<I>::flush(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  on_finish = util::create_async_context_callback(
    m_image_ctx, on_finish);
  {
    Mutex::Locker locker(m_lock);
    assert(m_flush_ctx == nullptr);
    m_flush_ctx = on_finish;

    if (m_in_flight_aio != 0) {
      flush_aio();
    }

    if (!m_op_contexts.empty() || m_in_flight_aio != 0) {
      return;
    }
  }
  on_finish->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::AioDiscardEvent &event,
                             Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": AIO discard event" << dendl;

  bool flush_required;
  AioCompletion *aio_comp = create_aio_modify_completion(on_ready, on_safe,
                                                         &flush_required);
  AioImageRequest<I>::aio_discard(&m_image_ctx, aio_comp, event.offset,
                                  event.length);
  if (flush_required) {
    flush_aio();
  }
}

template <typename I>
void Replay<I>::handle_event(const journal::AioWriteEvent &event,
                             Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": AIO write event" << dendl;

  bufferlist data = event.data;
  bool flush_required;
  AioCompletion *aio_comp = create_aio_modify_completion(on_ready, on_safe,
                                                         &flush_required);
  AioImageRequest<I>::aio_write(&m_image_ctx, aio_comp, event.offset,
                                event.length, data.c_str(), 0);
  if (flush_required) {
    flush_aio();
  }
}

template <typename I>
void Replay<I>::handle_event(const journal::AioFlushEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": AIO flush event" << dendl;

  AioCompletion *aio_comp = create_aio_flush_completion(on_ready, on_safe);
  AioImageRequest<I>::aio_flush(&m_image_ctx, aio_comp);
}

template <typename I>
void Replay<I>::handle_event(const journal::OpFinishEvent &event,
                             Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Op finish event" << dendl;
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapCreateEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap create event" << dendl;

  // TODO not-ready until state machine lets us know
  Context *on_finish = create_op_context_callback(on_safe);
  m_image_ctx.operations->snap_create(event.snap_name.c_str(), on_finish);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapRemoveEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap remove event" << dendl;

  Context *on_finish = create_op_context_callback(on_safe);
  m_image_ctx.operations->snap_remove(event.snap_name.c_str(), on_finish);
  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapRenameEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap rename event" << dendl;

  Context *on_finish = create_op_context_callback(on_safe);
  m_image_ctx.operations->snap_rename(event.snap_id, event.snap_name.c_str(),
                                      on_finish);
  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapProtectEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap protect event" << dendl;

  Context *on_finish = create_op_context_callback(on_safe);
  m_image_ctx.operations->snap_protect(event.snap_name.c_str(), on_finish);
  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapUnprotectEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap unprotect event"
                 << dendl;

  Context *on_finish = create_op_context_callback(on_safe);
  m_image_ctx.operations->snap_unprotect(event.snap_name.c_str(), on_finish);
  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapRollbackEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap rollback start event"
                 << dendl;

  Context *on_finish = create_op_context_callback(on_safe);
  m_image_ctx.operations->snap_rollback(event.snap_name.c_str(),
                                        no_op_progress_callback, on_finish);
  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::RenameEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Rename event" << dendl;

  Context *on_finish = create_op_context_callback(on_safe);
  m_image_ctx.operations->rename(event.image_name.c_str(), on_finish);
  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::ResizeEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Resize start event" << dendl;

  // TODO not-ready until state machine lets us know
  Context *on_finish = create_op_context_callback(on_safe);
  m_image_ctx.operations->resize(event.size, no_op_progress_callback,
                                 on_finish);
}

template <typename I>
void Replay<I>::handle_event(const journal::FlattenEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Flatten start event" << dendl;

  Context *on_finish = create_op_context_callback(on_safe);
  m_image_ctx.operations->flatten(no_op_progress_callback, on_finish);
  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::UnknownEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": unknown event" << dendl;
  on_ready->complete(0);
  on_safe->complete(0);
}

template <typename I>
void Replay<I>::flush_aio() {
  assert(m_lock.is_locked());

  AioCompletion *aio_comp = create_aio_flush_completion(nullptr, nullptr);
  AioImageRequest<I>::aio_flush(&m_image_ctx, aio_comp);
}

template <typename I>
void Replay<I>::handle_aio_modify_complete(Context *on_safe, int r) {
  Mutex::Locker locker(m_lock);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": on_safe=" << on_safe << ", "
                 << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "AIO modify op failed: " << cpp_strerror(r) << dendl;
    on_safe->complete(r);
    return;
  }

  // will be completed after next flush operation completes
  m_aio_modify_safe_contexts.insert(on_safe);
}

template <typename I>
void Replay<I>::handle_aio_flush_complete(Context *on_flush_safe,
                                          Contexts &on_safe_ctxs, int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": " << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "AIO flush failed: " << cpp_strerror(r) << dendl;
  }

  Context *on_aio_ready = nullptr;
  Context *on_flush = nullptr;
  {
    Mutex::Locker locker(m_lock);
    assert(m_in_flight_aio >= on_safe_ctxs.size());
    m_in_flight_aio -= on_safe_ctxs.size();

    std::swap(on_aio_ready, m_on_aio_ready);
    if (m_op_contexts.empty() && m_in_flight_aio == 0) {
      on_flush = m_flush_ctx;
    }

    // strip out previously failed on_safe contexts
    for (auto it = on_safe_ctxs.begin(); it != on_safe_ctxs.end(); ) {
      if (m_aio_modify_safe_contexts.erase(*it)) {
        ++it;
      } else {
        it = on_safe_ctxs.erase(it);
      }
    }
  }

  if (on_aio_ready != nullptr) {
    ldout(cct, 10) << "resuming paused AIO" << dendl;
    on_aio_ready->complete(0);
  }
  for (auto ctx : on_safe_ctxs) {
    ctx->complete(r);
  }
  if (on_flush_safe != nullptr) {
    on_flush_safe->complete(r);
  }
  if (on_flush != nullptr) {
    on_flush->complete(r);
  }
}

template <typename I>
Context *Replay<I>::create_op_context_callback(Context *on_safe) {
  assert(m_lock.is_locked());

  C_OpOnFinish *on_finish;
  {
    on_finish = new C_OpOnFinish(this);
    m_op_contexts[on_finish] = on_safe;
  }
  return on_finish;
}

template <typename I>
void Replay<I>::handle_op_context_callback(Context *on_op_finish, int r) {
  Context *on_safe = nullptr;
  Context *on_flush = nullptr;
  {
    Mutex::Locker locker(m_lock);
    auto it = m_op_contexts.find(on_op_finish);
    assert(it != m_op_contexts.end());

    on_safe = it->second;
    m_op_contexts.erase(it);
    if (m_op_contexts.empty() && m_in_flight_aio == 0) {
      on_flush = m_flush_ctx;
    }
  }

  on_safe->complete(r);
  if (on_flush != nullptr) {
    on_flush->complete(0);
  }
}

template <typename I>
AioCompletion *Replay<I>::create_aio_modify_completion(Context *on_ready,
                                                       Context *on_safe,
                                                       bool *flush_required) {
  CephContext *cct = m_image_ctx.cct;
  assert(m_lock.is_locked());
  assert(m_on_aio_ready == nullptr);

  ++m_in_flight_aio;
  m_aio_modify_unsafe_contexts.push_back(on_safe);

  // FLUSH if we hit the low-water mark -- on_safe contexts are
  // completed by flushes-only so that we don't move the journal
  // commit position until safely on-disk

  // when safe, the completion of the next flush will fire the on_safe
  // callback
  AioCompletion *aio_comp = AioCompletion::create<Context>(
    new C_AioModifyComplete(this, on_safe));

  *flush_required = (m_aio_modify_unsafe_contexts.size() ==
                       IN_FLIGHT_IO_LOW_WATER_MARK);
  if (*flush_required) {
    ldout(cct, 10) << "hit AIO replay low-water mark: scheduling flush"
                   << dendl;
  }

  // READY for more events if:
  // * not at high-water mark for IO
  // * in-flight ops are at a consistent point (snap create has IO flushed,
  //   shrink has adjusted clip boundary, etc) -- should have already been
  //   flagged not-ready
  if (m_in_flight_aio == IN_FLIGHT_IO_HIGH_WATER_MARK) {
    ldout(cct, 10) << "hit AIO replay high-water mark: pausing replay"
                   << dendl;
    m_on_aio_ready = on_ready;
  } else {
    on_ready->complete(0);
  }

  return aio_comp;
}

template <typename I>
AioCompletion *Replay<I>::create_aio_flush_completion(Context *on_ready,
                                                      Context *on_safe) {
  // associate all prior write/discard ops to this flush request
  AioCompletion *aio_comp = AioCompletion::create<Context>(
      new C_AioFlushComplete(this, on_safe,
                             std::move(m_aio_modify_unsafe_contexts)));
  m_aio_modify_unsafe_contexts.clear();

  if (on_ready != nullptr) {
    on_ready->complete(0);
  }
  return aio_comp;
}

} // namespace journal
} // namespace librbd

template class librbd::journal::Replay<librbd::ImageCtx>;
