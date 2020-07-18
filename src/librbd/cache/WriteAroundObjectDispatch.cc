// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/WriteAroundObjectDispatch.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::WriteAroundObjectDispatch: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace cache {

using librbd::util::data_object_name;

template <typename I>
WriteAroundObjectDispatch<I>::WriteAroundObjectDispatch(
    I* image_ctx, size_t max_dirty, bool writethrough_until_flush)
  : m_image_ctx(image_ctx), m_init_max_dirty(max_dirty), m_max_dirty(max_dirty),
    m_lock(ceph::make_mutex(util::unique_lock_name(
      "librbd::cache::WriteAroundObjectDispatch::lock", this))) {
  if (writethrough_until_flush) {
    m_max_dirty = 0;
  }
}

template <typename I>
WriteAroundObjectDispatch<I>::~WriteAroundObjectDispatch() {
}

template <typename I>
void WriteAroundObjectDispatch<I>::init() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  // add ourself to the IO object dispatcher chain
  if (m_init_max_dirty > 0) {
    m_image_ctx->disable_zero_copy = true;
  }
  m_image_ctx->io_object_dispatcher->register_dispatch(this);
}

template <typename I>
void WriteAroundObjectDispatch<I>::shut_down(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  on_finish->complete(0);
}

template <typename I>
bool WriteAroundObjectDispatch<I>::read(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    librados::snap_t snap_id, int op_flags, const ZTracer::Trace &parent_trace,
    ceph::bufferlist* read_data, io::Extents* extent_map,
    int* object_dispatch_flags, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  return dispatch_unoptimized_io(object_no, object_off, object_len,
                                 dispatch_result, on_dispatched);
}

template <typename I>
bool WriteAroundObjectDispatch<I>::discard(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    const ::SnapContext &snapc, int discard_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  return dispatch_io(object_no, object_off, object_len, 0, dispatch_result,
                     on_finish, on_dispatched);
}

template <typename I>
bool WriteAroundObjectDispatch<I>::write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context**on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << data.length() << dendl;

  return dispatch_io(object_no, object_off, data.length(), op_flags,
                     dispatch_result, on_finish, on_dispatched);
}

template <typename I>
bool WriteAroundObjectDispatch<I>::write_same(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    io::LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context**on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  return dispatch_io(object_no, object_off, object_len, 0, dispatch_result,
                     on_finish, on_dispatched);
}

template <typename I>
bool WriteAroundObjectDispatch<I>::compare_and_write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
    ceph::bufferlist&& write_data, const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
    int* object_dispatch_flags, uint64_t* journal_tid,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  return dispatch_unoptimized_io(object_no, object_off, cmp_data.length(),
                                 dispatch_result, on_dispatched);
}

template <typename I>
bool WriteAroundObjectDispatch<I>::flush(
    io::FlushSource flush_source, const ZTracer::Trace &parent_trace,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  std::lock_guard locker{m_lock};
  if (flush_source == io::FLUSH_SOURCE_USER && !m_user_flushed) {
    m_user_flushed = true;
    if (m_max_dirty == 0 && m_init_max_dirty > 0) {
      ldout(cct, 5) << "first user flush: enabling write-around" << dendl;
      m_max_dirty = m_init_max_dirty;
    }
  }

  if (m_in_flight_io_tids.empty()) {
    // no in-flight IO (also implies no queued/blocked IO)
    return false;
  }

  auto tid = ++m_last_tid;
  auto ctx = util::create_async_context_callback(*m_image_ctx, *on_finish);

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  *on_finish = new LambdaContext([this, tid](int r) {
      handle_in_flight_flush_complete(r, tid);
    });

  if (m_queued_ios.empty() && m_blocked_ios.empty()) {
    // immediately allow the flush to be dispatched
    ldout(cct, 20) << "dispatching: tid=" << tid << dendl;
    m_in_flight_flushes.emplace(tid, ctx);
    return false;
  }

  // cannot dispatch the flush until after preceeding IO is dispatched
  ldout(cct, 20) << "queueing: tid=" << tid << dendl;
  m_queued_flushes.emplace(tid, QueuedFlush{ctx, on_dispatched});
  return true;
}

template <typename I>
bool WriteAroundObjectDispatch<I>::dispatch_unoptimized_io(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;

  m_lock.lock();
  auto in_flight_extents_it = m_in_flight_extents.find(object_no);
  if (in_flight_extents_it == m_in_flight_extents.end() ||
      !in_flight_extents_it->second.intersects(object_off, object_len)) {
    // no IO in-flight to the specified extent
    m_lock.unlock();
    return false;
  }

  // write IO is in-flight -- it needs to complete before the unoptimized
  // IO can be dispatched
  auto tid = ++m_last_tid;
  ldout(cct, 20) << "blocked by in-flight IO: tid=" << tid << dendl;
  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  m_blocked_unoptimized_ios[object_no].emplace(
    tid, BlockedIO{object_off, object_len, nullptr, on_dispatched});
  m_lock.unlock();

  return true;
}

template <typename I>
bool WriteAroundObjectDispatch<I>::dispatch_io(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    int op_flags, io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;

  m_lock.lock();
  if (m_max_dirty == 0) {
    // write-through mode is active -- no-op the cache
    m_lock.unlock();
    return false;
  }

  if ((op_flags & LIBRADOS_OP_FLAG_FADVISE_FUA) != 0) {
    // force unit access flag is set -- disable write-around
    m_lock.unlock();
    return dispatch_unoptimized_io(object_no, object_off, object_len,
                                   dispatch_result, on_dispatched);
  }

  auto tid = ++m_last_tid;
  auto ctx = util::create_async_context_callback(*m_image_ctx, *on_finish);

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  *on_finish = new LambdaContext(
    [this, tid, object_no, object_off, object_len](int r) {
      handle_in_flight_io_complete(r, tid, object_no, object_off, object_len);
    });

  bool blocked = block_overlapping_io(&m_in_flight_extents[object_no],
                                      object_off, object_len);
  if (blocked) {
    ldout(cct, 20) << "blocked on overlap: tid=" << tid << dendl;
    m_queued_or_blocked_io_tids.insert(tid);
    m_blocked_ios[object_no].emplace(tid, BlockedIO{object_off, object_len, ctx,
                                                    on_dispatched});
    m_lock.unlock();
  } else if (can_dispatch_io(tid, object_len)) {
    m_lock.unlock();

    ldout(cct, 20) << "dispatching: tid=" << tid << dendl;
    on_dispatched->complete(0);
    ctx->complete(0);
  } else {
    ldout(cct, 20) << "queueing: tid=" << tid << dendl;
    m_queued_or_blocked_io_tids.insert(tid);
    m_queued_ios.emplace(tid, QueuedIO{object_len, ctx, on_dispatched});
    m_lock.unlock();
  }
  return true;
}

template <typename I>
bool WriteAroundObjectDispatch<I>::block_overlapping_io(
    InFlightObjectExtents* in_flight_object_extents, uint64_t object_off,
    uint64_t object_len) {
  if (in_flight_object_extents->intersects(object_off, object_len)) {
    return true;
  }

  in_flight_object_extents->insert(object_off, object_len);
  return false;
}

template <typename I>
void WriteAroundObjectDispatch<I>::unblock_overlapping_ios(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    Contexts* unoptimized_io_dispatches) {
  auto cct = m_image_ctx->cct;
  ceph_assert(ceph_mutex_is_locked(m_lock));

  auto in_flight_extents_it = m_in_flight_extents.find(object_no);
  ceph_assert(in_flight_extents_it != m_in_flight_extents.end());

  auto& in_flight_object_extents = in_flight_extents_it->second;
  in_flight_object_extents.erase(object_off, object_len);

  // handle unoptimized IOs that were blocked by in-flight IO
  InFlightObjectExtents blocked_unoptimized_ios;
  auto blocked_unoptimized_ios_it = m_blocked_unoptimized_ios.find(object_no);
  if (blocked_unoptimized_ios_it != m_blocked_unoptimized_ios.end()) {
    auto& blocked_unoptimized_object_ios = blocked_unoptimized_ios_it->second;
    for (auto it = blocked_unoptimized_object_ios.begin();
         it != blocked_unoptimized_object_ios.end();) {
      auto& blocked_io = it->second;
      if (!in_flight_object_extents.intersects(blocked_io.offset,
                                               blocked_io.length)) {
        unoptimized_io_dispatches->emplace(it->first, blocked_io.on_dispatched);
        it = blocked_unoptimized_object_ios.erase(it);
      } else {
        blocked_unoptimized_ios.union_insert(blocked_io.offset,
                                             blocked_io.length);
        ++it;
      }
    }

    if (blocked_unoptimized_object_ios.empty()) {
      m_blocked_unoptimized_ios.erase(blocked_unoptimized_ios_it);
    }
  }

  // handle optimized IOs that were blocked
  auto blocked_io_it = m_blocked_ios.find(object_no);
  if (blocked_io_it != m_blocked_ios.end()) {
    auto& blocked_object_ios = blocked_io_it->second;

    auto blocked_object_ios_it = blocked_object_ios.begin();
    while (blocked_object_ios_it != blocked_object_ios.end()) {
      auto next_blocked_object_ios_it = blocked_object_ios_it;
      ++next_blocked_object_ios_it;

      auto& blocked_io = blocked_object_ios_it->second;
      if (blocked_unoptimized_ios.intersects(blocked_io.offset,
                                             blocked_io.length) ||
          block_overlapping_io(&in_flight_object_extents, blocked_io.offset,
                               blocked_io.length)) {
        break;
      }

      // move unblocked IO to the queued list, which will get processed when
      // there is capacity
      auto tid = blocked_object_ios_it->first;
      ldout(cct, 20) << "queueing unblocked: tid=" << tid << dendl;
      m_queued_ios.emplace(tid, blocked_io);

      blocked_object_ios.erase(blocked_object_ios_it);
      blocked_object_ios_it = next_blocked_object_ios_it;
    }

    if (blocked_object_ios.empty()) {
      m_blocked_ios.erase(blocked_io_it);
    }
  }

  if (in_flight_object_extents.empty()) {
    m_in_flight_extents.erase(in_flight_extents_it);
  }
}

template <typename I>
bool WriteAroundObjectDispatch<I>::can_dispatch_io(
    uint64_t tid, uint64_t length) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  if (m_in_flight_bytes == 0 || m_in_flight_bytes + length <= m_max_dirty) {
    // no in-flight IO or still under max write-around in-flight limit.
    // allow the dispatcher to proceed to send the IO but complete it back
    // to the invoker.
    m_in_flight_bytes += length;
    m_in_flight_io_tids.insert(tid);
    return true;
  }

  return false;
}

template <typename I>
void WriteAroundObjectDispatch<I>::handle_in_flight_io_complete(
    int r, uint64_t tid, uint64_t object_no, uint64_t object_off,
    uint64_t object_len) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << ", tid=" << tid << dendl;

  m_lock.lock();
  m_in_flight_io_tids.erase(tid);
  ceph_assert(m_in_flight_bytes >= object_len);
  m_in_flight_bytes -= object_len;

  if (r < 0) {
    lderr(cct) << "IO error encountered: tid=" << tid << ": "
               << cpp_strerror(r) << dendl;
    if (m_pending_flush_error == 0) {
      m_pending_flush_error = r;
    }
  }

  // any overlapping blocked IOs can be queued now
  Contexts unoptimized_io_dispatches;
  unblock_overlapping_ios(object_no, object_off, object_len,
                          &unoptimized_io_dispatches);

  // collect any flushes that are ready for completion
  int pending_flush_error = 0;
  auto finished_flushes = collect_finished_flushes();
  if (!finished_flushes.empty()) {
    std::swap(pending_flush_error, m_pending_flush_error);
  }

  // collect any queued IOs that are ready for dispatch
  auto ready_ios = collect_ready_ios();

  // collect any queued flushes that were tied to queued IOs
  auto ready_flushes = collect_ready_flushes();
  m_lock.unlock();

  // dispatch any ready unoptimized IOs
  for (auto& it : unoptimized_io_dispatches) {
    ldout(cct, 20) << "dispatching unoptimized IO: tid=" << it.first << dendl;
    it.second->complete(0);
  }

  // complete flushes that were waiting on in-flight IO
  // (and propogate any IO error to first flush)
  for (auto& it : finished_flushes) {
    ldout(cct, 20) << "completing flush: tid=" << it.first << ", "
                   << "r=" << pending_flush_error << dendl;
    it.second->complete(pending_flush_error);
  }

  // dispatch any ready queued IOs
  for (auto& it : ready_ios) {
    ldout(cct, 20) << "dispatching IO: tid=" << it.first << dendl;
    it.second.on_dispatched->complete(0);
    it.second.on_finish->complete(0);
  }

  // dispatch any ready flushes
  for (auto& it : ready_flushes) {
    ldout(cct, 20) << "dispatching flush: tid=" << it.first << dendl;
    it.second->complete(0);
  }
}

template <typename I>
void WriteAroundObjectDispatch<I>::handle_in_flight_flush_complete(
    int r, uint64_t tid) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << ", tid=" << tid << dendl;

  m_lock.lock();

  // move the in-flight flush to the pending completion list
  auto it = m_in_flight_flushes.find(tid);
  ceph_assert(it != m_in_flight_flushes.end());

  m_pending_flushes.emplace(it->first, it->second);
  m_in_flight_flushes.erase(it);

  // collect any flushes that are ready for completion
  int pending_flush_error = 0;
  auto finished_flushes = collect_finished_flushes();
  if (!finished_flushes.empty()) {
    std::swap(pending_flush_error, m_pending_flush_error);
  }
  m_lock.unlock();

  // complete flushes that were waiting on in-flight IO
  // (and propogate any IO errors)
  for (auto& it : finished_flushes) {
    ldout(cct, 20) << "completing flush: tid=" << it.first << dendl;
    it.second->complete(pending_flush_error);
    pending_flush_error = 0;
  }
}

template <typename I>
typename WriteAroundObjectDispatch<I>::QueuedIOs
WriteAroundObjectDispatch<I>::collect_ready_ios() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  QueuedIOs queued_ios;

  while (true) {
    auto it = m_queued_ios.begin();
    if (it == m_queued_ios.end() ||
        !can_dispatch_io(it->first, it->second.length)) {
      break;
    }

    queued_ios.emplace(it->first, it->second);
    m_queued_or_blocked_io_tids.erase(it->first);
    m_queued_ios.erase(it);
  }
  return queued_ios;
}

template <typename I>
typename WriteAroundObjectDispatch<I>::Contexts
WriteAroundObjectDispatch<I>::collect_ready_flushes() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  Contexts ready_flushes;
  auto io_tid_it = m_queued_or_blocked_io_tids.begin();
  while (true) {
    auto it = m_queued_flushes.begin();
    if (it == m_queued_flushes.end() ||
        (io_tid_it != m_queued_or_blocked_io_tids.end() &&
         *io_tid_it < it->first)) {
      break;
    }

    m_in_flight_flushes.emplace(it->first, it->second.on_finish);
    ready_flushes.emplace(it->first, it->second.on_dispatched);
    m_queued_flushes.erase(it);
  }

  return ready_flushes;
}

template <typename I>
typename WriteAroundObjectDispatch<I>::Contexts
WriteAroundObjectDispatch<I>::collect_finished_flushes() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  Contexts finished_flushes;
  auto io_tid_it = m_in_flight_io_tids.begin();
  while (true) {
    auto it = m_pending_flushes.begin();
    if (it == m_pending_flushes.end() ||
        (io_tid_it != m_in_flight_io_tids.end() && *io_tid_it < it->first)) {
      break;
    }

    finished_flushes.emplace(it->first, it->second);
    m_pending_flushes.erase(it);
  }
  return finished_flushes;
}

} // namespace cache
} // namespace librbd

template class librbd::cache::WriteAroundObjectDispatch<librbd::ImageCtx>;
