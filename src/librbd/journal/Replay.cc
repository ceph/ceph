// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/Replay.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
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

template <typename I, typename E>
struct ExecuteOp : public Context {
  I &image_ctx;
  E event;
  Context *on_op_complete;

  ExecuteOp(I &image_ctx, const E &event, Context *on_op_complete)
    : image_ctx(image_ctx), event(event), on_op_complete(on_op_complete) {
  }

  void execute(const journal::SnapCreateEvent &_) {
    image_ctx.operations->execute_snap_create(event.snap_name.c_str(),
                                              on_op_complete,
                                              event.op_tid, false);
  }

  void execute(const journal::SnapRemoveEvent &_) {
    image_ctx.operations->execute_snap_remove(event.snap_name.c_str(),
                                              on_op_complete);
  }

  void execute(const journal::SnapRenameEvent &_) {
    image_ctx.operations->execute_snap_rename(event.snap_id,
                                              event.snap_name.c_str(),
                                              on_op_complete);
  }

  void execute(const journal::SnapProtectEvent &_) {
    image_ctx.operations->execute_snap_protect(event.snap_name.c_str(),
                                               on_op_complete);
  }

  void execute(const journal::SnapUnprotectEvent &_) {
    image_ctx.operations->execute_snap_unprotect(event.snap_name.c_str(),
                                                 on_op_complete);
  }

  void execute(const journal::SnapRollbackEvent &_) {
    image_ctx.operations->execute_snap_rollback(event.snap_name.c_str(),
                                                no_op_progress_callback,
                                                on_op_complete);
  }

  void execute(const journal::RenameEvent &_) {
    image_ctx.operations->execute_rename(event.image_name.c_str(),
                                         on_op_complete);
  }

  void execute(const journal::ResizeEvent &_) {
    image_ctx.operations->execute_resize(event.size, no_op_progress_callback,
                                         on_op_complete, event.op_tid);
  }

  void execute(const journal::FlattenEvent &_) {
    image_ctx.operations->execute_flatten(no_op_progress_callback,
                                          on_op_complete);
  }

  virtual void finish(int r) override {
    CephContext *cct = image_ctx.cct;
    if (r < 0) {
      lderr(cct) << "ExecuteOp: " << __func__ << ": r=" << r << dendl;
      on_op_complete->complete(r);
      return;
    }

    ldout(cct, 20) << "ExecuteOp: " << __func__ << dendl;
    RWLock::RLocker owner_locker(image_ctx.owner_lock);
    execute(event);
  }
};

template <typename I>
struct C_RefreshIfRequired : public Context {
  I &image_ctx;
  Context *on_finish;

  C_RefreshIfRequired(I &image_ctx, Context *on_finish)
    : image_ctx(image_ctx), on_finish(on_finish) {
  }
  virtual ~C_RefreshIfRequired() {
    delete on_finish;
  }

  virtual void finish(int r) override {
    CephContext *cct = image_ctx.cct;
    Context *ctx = on_finish;
    on_finish = nullptr;

    if (r < 0) {
      lderr(cct) << "C_RefreshIfRequired: " << __func__ << ": r=" << r << dendl;
      image_ctx.op_work_queue->queue(ctx, r);
      return;
    }

    if (image_ctx.state->is_refresh_required()) {
      ldout(cct, 20) << "C_RefreshIfRequired: " << __func__ << ": "
                     << "refresh required" << dendl;
      image_ctx.state->refresh(ctx);
      return;
    }

    image_ctx.op_work_queue->queue(ctx, 0);
  }
};

} // anonymous namespace

template <typename I>
Replay<I>::Replay(I &image_ctx)
  : m_image_ctx(image_ctx), m_lock("Replay<I>::m_lock") {
}

template <typename I>
Replay<I>::~Replay() {
  assert(m_in_flight_aio_flush == 0);
  assert(m_in_flight_aio_modify == 0);
  assert(m_aio_modify_unsafe_contexts.empty());
  assert(m_aio_modify_safe_contexts.empty());
  assert(m_op_events.empty());
  assert(m_in_flight_op_events == 0);
}

template <typename I>
void Replay<I>::process(bufferlist::iterator *it, Context *on_ready,
                        Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": "
                 << "on_ready=" << on_ready << ", on_safe=" << on_safe << dendl;

  on_ready = util::create_async_context_callback(m_image_ctx, on_ready);

  journal::EventEntry event_entry;
  try {
    ::decode(event_entry, *it);
  } catch (const buffer::error &err) {
    lderr(cct) << "failed to decode event entry: " << err.what() << dendl;
    on_ready->complete(-EINVAL);
    return;
  }

  RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
  boost::apply_visitor(EventVisitor(this, on_ready, on_safe),
                       event_entry.event);
}

template <typename I>
void Replay<I>::shut_down(bool cancel_ops, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  AioCompletion *flush_comp = nullptr;
  on_finish = util::create_async_context_callback(
    m_image_ctx, on_finish);

  {
    Mutex::Locker locker(m_lock);

    // safely commit any remaining AIO modify operations
    if ((m_in_flight_aio_flush + m_in_flight_aio_modify) != 0) {
      flush_comp = create_aio_flush_completion(nullptr);
    }

    for (auto &op_event_pair : m_op_events) {
      OpEvent &op_event = op_event_pair.second;
      if (cancel_ops) {
        // cancel ops that are waiting to start (waiting for
        // OpFinishEvent or waiting for ready)
        if (op_event.on_start_ready == nullptr &&
            op_event.on_op_finish_event != nullptr) {
          Context *on_op_finish_event = nullptr;
          std::swap(on_op_finish_event, op_event.on_op_finish_event);
          m_image_ctx.op_work_queue->queue(on_op_finish_event, -ERESTART);
        }
      } else if (op_event.on_op_finish_event != nullptr) {
        // start ops waiting for OpFinishEvent
        Context *on_op_finish_event = nullptr;
        std::swap(on_op_finish_event, op_event.on_op_finish_event);
        m_image_ctx.op_work_queue->queue(on_op_finish_event, 0);
      } else if (op_event.on_start_ready != nullptr) {
        // waiting for op ready
        op_event_pair.second.finish_on_ready = true;
      }
    }

    assert(m_flush_ctx == nullptr);
    if (m_in_flight_op_events > 0 || flush_comp != nullptr) {
      std::swap(m_flush_ctx, on_finish);
    }
  }

  // execute the following outside of lock scope
  if (flush_comp != nullptr) {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    AioImageRequest<I>::aio_flush(&m_image_ctx, flush_comp);
  }
  if (on_finish != nullptr) {
    on_finish->complete(0);
  }
}

template <typename I>
void Replay<I>::flush(Context *on_finish) {
  AioCompletion *aio_comp;
  {
    Mutex::Locker locker(m_lock);
    aio_comp = create_aio_flush_completion(
      util::create_async_context_callback(m_image_ctx, on_finish));
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  AioImageRequest<I>::aio_flush(&m_image_ctx, aio_comp);
}

template <typename I>
void Replay<I>::replay_op_ready(uint64_t op_tid, Context *on_resume) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": op_tid=" << op_tid << dendl;

  Mutex::Locker locker(m_lock);
  auto op_it = m_op_events.find(op_tid);
  assert(op_it != m_op_events.end());

  OpEvent &op_event = op_it->second;
  assert(op_event.op_in_progress &&
         op_event.on_op_finish_event == nullptr &&
         op_event.on_finish_ready == nullptr &&
         op_event.on_finish_safe == nullptr);

  // resume processing replay events
  Context *on_start_ready = nullptr;
  std::swap(on_start_ready, op_event.on_start_ready);
  on_start_ready->complete(0);

  // cancel has been requested -- send error to paused state machine
  if (!op_event.finish_on_ready && m_flush_ctx != nullptr) {
    m_image_ctx.op_work_queue->queue(on_resume, -ERESTART);
    return;
  }

  // resume the op state machine once the associated OpFinishEvent
  // is processed
  op_event.on_op_finish_event = new FunctionContext(
    [on_resume](int r) {
      on_resume->complete(r);
    });

  // shut down request -- don't expect OpFinishEvent
  if (op_event.finish_on_ready) {
    m_image_ctx.op_work_queue->queue(on_resume, 0);
  }
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
    m_lock.Lock();
    AioCompletion *flush_comp = create_aio_flush_completion(nullptr);
    m_lock.Unlock();

    AioImageRequest<I>::aio_flush(&m_image_ctx, flush_comp);
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
    m_lock.Lock();
    AioCompletion *flush_comp = create_aio_flush_completion(nullptr);
    m_lock.Unlock();

    AioImageRequest<I>::aio_flush(&m_image_ctx, flush_comp);
  }
}

template <typename I>
void Replay<I>::handle_event(const journal::AioFlushEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": AIO flush event" << dendl;

  AioCompletion *aio_comp;
  {
    Mutex::Locker locker(m_lock);
    aio_comp = create_aio_flush_completion(on_safe);
  }
  AioImageRequest<I>::aio_flush(&m_image_ctx, aio_comp);

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::OpFinishEvent &event,
                             Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Op finish event: "
                 << "op_tid=" << event.op_tid << dendl;

  bool op_in_progress;
  bool filter_ret_val;
  Context *on_op_complete = nullptr;
  Context *on_op_finish_event = nullptr;
  {
    Mutex::Locker locker(m_lock);
    auto op_it = m_op_events.find(event.op_tid);
    if (op_it == m_op_events.end()) {
      ldout(cct, 10) << "unable to locate associated op: assuming previously "
                     << "committed." << dendl;
      on_ready->complete(0);
      m_image_ctx.op_work_queue->queue(on_safe, 0);
      return;
    }

    OpEvent &op_event = op_it->second;
    assert(op_event.on_finish_safe == nullptr);
    op_event.on_finish_ready = on_ready;
    op_event.on_finish_safe = on_safe;
    op_in_progress = op_event.op_in_progress;
    std::swap(on_op_complete, op_event.on_op_complete);
    std::swap(on_op_finish_event, op_event.on_op_finish_event);

    // special errors which indicate op never started but was recorded
    // as failed in the journal
    filter_ret_val = (op_event.op_finish_error_codes.count(event.r) != 0);
  }

  if (event.r < 0) {
    if (op_in_progress) {
      // bubble the error up to the in-progress op to cancel it
      on_op_finish_event->complete(event.r);
    } else {
      // op hasn't been started -- bubble the error up since
      // our image is now potentially in an inconsistent state
      // since simple errors should have been caught before
      // creating the op event
      delete on_op_complete;
      delete on_op_finish_event;
      handle_op_complete(event.op_tid, filter_ret_val ? 0 : event.r);
    }
    return;
  }

  // journal recorded success -- apply the op now
  on_op_finish_event->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapCreateEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap create event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EEXIST};

  // avoid lock cycles
  m_image_ctx.op_work_queue->queue(new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapCreateEvent>(m_image_ctx, event,
                                                            on_op_complete)),
    0);

  // do not process more events until the state machine is ready
  // since it will affect IO
  op_event->op_in_progress = true;
  op_event->on_start_ready = on_ready;
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapRemoveEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap remove event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapRemoveEvent>(m_image_ctx, event,
                                                            on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-ENOENT};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapRenameEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap rename event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapRenameEvent>(m_image_ctx, event,
                                                            on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EEXIST};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapProtectEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap protect event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapProtectEvent>(m_image_ctx, event,
                                                             on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EBUSY};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapUnprotectEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap unprotect event"
                 << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapUnprotectEvent>(m_image_ctx,
                                                               event,
                                                               on_op_complete));

  // ignore errors recorded in the journal
  op_event->op_finish_error_codes = {-EBUSY};

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EINVAL};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapRollbackEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap rollback start event"
                 << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapRollbackEvent>(m_image_ctx,
                                                              event,
                                                              on_op_complete));

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::RenameEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Rename event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::RenameEvent>(m_image_ctx, event,
                                                        on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EEXIST};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::ResizeEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Resize start event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  // avoid lock cycles
  m_image_ctx.op_work_queue->queue(new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::ResizeEvent>(m_image_ctx, event,
                                                        on_op_complete)), 0);

  // do not process more events until the state machine is ready
  // since it will affect IO
  op_event->op_in_progress = true;
  op_event->on_start_ready = on_ready;
}

template <typename I>
void Replay<I>::handle_event(const journal::FlattenEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Flatten start event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::FlattenEvent>(m_image_ctx, event,
                                                         on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EINVAL};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::DemoteEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Demote event" << dendl;
  on_ready->complete(0);
  on_safe->complete(0);
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
void Replay<I>::handle_aio_modify_complete(Context *on_ready, Context *on_safe,
                                           int r) {
  Mutex::Locker locker(m_lock);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": on_ready=" << on_ready << ", "
                 << "on_safe=" << on_safe << ", r=" << r << dendl;

  if (on_ready != nullptr) {
    on_ready->complete(0);
  }
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
    assert(m_in_flight_aio_flush > 0);
    assert(m_in_flight_aio_modify >= on_safe_ctxs.size());
    --m_in_flight_aio_flush;
    m_in_flight_aio_modify -= on_safe_ctxs.size();

    std::swap(on_aio_ready, m_on_aio_ready);
    if (m_in_flight_op_events == 0 &&
        (m_in_flight_aio_flush + m_in_flight_aio_modify) == 0) {
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

  if (on_flush_safe != nullptr) {
    on_safe_ctxs.push_back(on_flush_safe);
  }
  for (auto ctx : on_safe_ctxs) {
    ldout(cct, 20) << "completing safe context: " << ctx << dendl;
    ctx->complete(r);
  }

  if (on_flush != nullptr) {
    ldout(cct, 20) << "completing flush context: " << on_flush << dendl;
    on_flush->complete(r);
  }
}

template <typename I>
Context *Replay<I>::create_op_context_callback(uint64_t op_tid,
                                               Context *on_ready,
                                               Context *on_safe,
                                               OpEvent **op_event) {
  CephContext *cct = m_image_ctx.cct;

  assert(m_lock.is_locked());
  if (m_op_events.count(op_tid) != 0) {
    lderr(cct) << "duplicate op tid detected: " << op_tid << dendl;

    // on_ready is already async but on failure invoke on_safe async
    // as well
    on_ready->complete(0);
    m_image_ctx.op_work_queue->queue(on_safe, -EINVAL);
    return nullptr;
  }

  ++m_in_flight_op_events;
  *op_event = &m_op_events[op_tid];
  (*op_event)->on_start_safe = on_safe;

  Context *on_op_complete = new C_OpOnComplete(this, op_tid);
  (*op_event)->on_op_complete = on_op_complete;
  return on_op_complete;
}

template <typename I>
void Replay<I>::handle_op_complete(uint64_t op_tid, int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": op_tid=" << op_tid << ", "
                 << "r=" << r << dendl;

  OpEvent op_event;
  bool shutting_down = false;
  {
    Mutex::Locker locker(m_lock);
    auto op_it = m_op_events.find(op_tid);
    assert(op_it != m_op_events.end());

    op_event = std::move(op_it->second);
    m_op_events.erase(op_it);

    shutting_down = (m_flush_ctx != nullptr);
  }

  assert(op_event.on_start_ready == nullptr || (r < 0 && r != -ERESTART));
  if (op_event.on_start_ready != nullptr) {
    // blocking op event failed before it became ready
    assert(op_event.on_finish_ready == nullptr &&
           op_event.on_finish_safe == nullptr);

    op_event.on_start_ready->complete(0);
  } else {
    // event kicked off by OpFinishEvent
    assert((op_event.on_finish_ready != nullptr &&
            op_event.on_finish_safe != nullptr) || shutting_down);
  }

  if (op_event.on_op_finish_event != nullptr) {
    op_event.on_op_finish_event->complete(r);
  }

  if (op_event.on_finish_ready != nullptr) {
    op_event.on_finish_ready->complete(0);
  }

  // filter out errors caused by replay of the same op
  if (r < 0 && op_event.ignore_error_codes.count(r) != 0) {
    r = 0;
  }

  op_event.on_start_safe->complete(r);
  if (op_event.on_finish_safe != nullptr) {
    op_event.on_finish_safe->complete(r);
  }

  // shut down request might have occurred while lock was
  // dropped -- handle if pending
  Context *on_flush = nullptr;
  {
    Mutex::Locker locker(m_lock);
    assert(m_in_flight_op_events > 0);
    --m_in_flight_op_events;
    if (m_in_flight_op_events == 0 &&
        (m_in_flight_aio_flush + m_in_flight_aio_modify) == 0) {
      on_flush = m_flush_ctx;
    }
  }
  if (on_flush != nullptr) {
    m_image_ctx.op_work_queue->queue(on_flush, 0);
  }
}

template <typename I>
AioCompletion *Replay<I>::create_aio_modify_completion(Context *on_ready,
                                                       Context *on_safe,
                                                       bool *flush_required) {
  Mutex::Locker locker(m_lock);
  CephContext *cct = m_image_ctx.cct;
  assert(m_on_aio_ready == nullptr);

  ++m_in_flight_aio_modify;
  m_aio_modify_unsafe_contexts.push_back(on_safe);

  // FLUSH if we hit the low-water mark -- on_safe contexts are
  // completed by flushes-only so that we don't move the journal
  // commit position until safely on-disk

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
  if (m_in_flight_aio_modify == IN_FLIGHT_IO_HIGH_WATER_MARK) {
    ldout(cct, 10) << "hit AIO replay high-water mark: pausing replay"
                   << dendl;
    assert(m_on_aio_ready == nullptr);
    std::swap(m_on_aio_ready, on_ready);
  }

  // when the modification is ACKed by librbd, we can process the next
  // event. when flushed, the completion of the next flush will fire the
  // on_safe callback
  AioCompletion *aio_comp = AioCompletion::create<Context>(
    new C_AioModifyComplete(this, on_ready, on_safe));
  return aio_comp;
}

template <typename I>
AioCompletion *Replay<I>::create_aio_flush_completion(Context *on_safe) {
  assert(m_lock.is_locked());

  ++m_in_flight_aio_flush;

  // associate all prior write/discard ops to this flush request
  AioCompletion *aio_comp = AioCompletion::create<Context>(
      new C_AioFlushComplete(this, on_safe,
                             std::move(m_aio_modify_unsafe_contexts)));
  m_aio_modify_unsafe_contexts.clear();
  return aio_comp;
}

} // namespace journal
} // namespace librbd

template class librbd::journal::Replay<librbd::ImageCtx>;
