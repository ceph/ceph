// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/FlushTracker.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::FlushTracker: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

template <typename I>
FlushTracker<I>::FlushTracker(I* image_ctx)
  : m_image_ctx(image_ctx),
    m_lock(ceph::make_shared_mutex(
      util::unique_lock_name("librbd::io::FlushTracker::m_lock", this))) {
}

template <typename I>
FlushTracker<I>::~FlushTracker() {
  std::unique_lock locker{m_lock};
  ceph_assert(m_flush_contexts.empty());
}

template <typename I>
void FlushTracker<I>::shut_down() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  std::unique_lock locker{m_lock};
  Contexts flush_ctxs;
  for (auto& [flush_tid, ctxs] : m_flush_contexts) {
    flush_ctxs.insert(flush_ctxs.end(), ctxs.begin(), ctxs.end());
  }
  m_flush_contexts.clear();
  locker.unlock();

  for (auto ctx : flush_ctxs) {
    ctx->complete(0);
  }
}

template <typename I>
uint64_t FlushTracker<I>::start_io(uint64_t tid) {
  auto cct = m_image_ctx->cct;

  std::unique_lock locker{m_lock};
  auto [it, inserted] = m_tid_to_flush_tid.insert({tid, ++m_next_flush_tid});
  auto flush_tid = it->second;
  m_in_flight_flush_tids.insert(flush_tid);
  locker.unlock();

  ldout(cct, 20) << "tid=" << tid << ", flush_tid=" << flush_tid << dendl;
  return flush_tid;
}

template <typename I>
void FlushTracker<I>::finish_io(uint64_t tid) {
  auto cct = m_image_ctx->cct;

  std::unique_lock locker{m_lock};
  auto tid_to_flush_tid_it = m_tid_to_flush_tid.find(tid);
  if (tid_to_flush_tid_it == m_tid_to_flush_tid.end()) {
    return;
  }

  auto flush_tid = tid_to_flush_tid_it->second;
  m_tid_to_flush_tid.erase(tid_to_flush_tid_it);
  m_in_flight_flush_tids.erase(flush_tid);

  ldout(cct, 20) << "tid=" << tid << ", flush_tid=" << flush_tid << dendl;
  auto oldest_flush_tid = std::numeric_limits<uint64_t>::max();
  if (!m_in_flight_flush_tids.empty()) {
    oldest_flush_tid = *m_in_flight_flush_tids.begin();
  }

  // all flushes tagged before the oldest tid should be completed
  Contexts flush_ctxs;
  auto flush_contexts_it = m_flush_contexts.begin();
  while (flush_contexts_it != m_flush_contexts.end()) {
    if (flush_contexts_it->first >= oldest_flush_tid) {
      ldout(cct, 20) << "pending IOs: [" << m_in_flight_flush_tids << "], "
                     << "pending flushes=" << m_flush_contexts << dendl;
      break;
    }

    auto& ctxs = flush_contexts_it->second;
    flush_ctxs.insert(flush_ctxs.end(), ctxs.begin(), ctxs.end());
    flush_contexts_it = m_flush_contexts.erase(flush_contexts_it);
  }
  locker.unlock();

  if (!flush_ctxs.empty()) {
    ldout(cct, 20) << "completing flushes: " << flush_ctxs << dendl;
    for (auto ctx : flush_ctxs) {
      ctx->complete(0);
    }
  }
}

template <typename I>
void FlushTracker<I>::flush(Context* on_finish) {
  auto cct = m_image_ctx->cct;

  std::unique_lock locker{m_lock};
  if (m_in_flight_flush_tids.empty()) {
    locker.unlock();
    on_finish->complete(0);
    return;
  }

  auto flush_tid = *m_in_flight_flush_tids.rbegin();
  m_flush_contexts[flush_tid].push_back(on_finish);
  ldout(cct, 20) << "flush_tid=" << flush_tid << ", ctx=" << on_finish << ", "
                 << "flush_contexts=" << m_flush_contexts << dendl;
}

} // namespace io
} // namespace librbd

template class librbd::io::FlushTracker<librbd::ImageCtx>;
