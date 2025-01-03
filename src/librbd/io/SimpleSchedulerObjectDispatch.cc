// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/SimpleSchedulerObjectDispatch.h"
#include "include/neorados/RADOS.hpp"
#include "common/ceph_time.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/io/FlushTracker.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/Utils.h"

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/rolling_count.hpp>
#include <boost/accumulators/statistics/rolling_sum.hpp>
#include <boost/accumulators/statistics/stats.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::SimpleSchedulerObjectDispatch: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace io {

using namespace boost::accumulators;
using ceph::operator<<;
using librbd::util::data_object_name;

static const int LATENCY_STATS_WINDOW_SIZE = 10;

class LatencyStats {
private:
  accumulator_set<uint64_t, stats<tag::rolling_count, tag::rolling_sum>> m_acc;

public:
  LatencyStats()
    : m_acc(tag::rolling_window::window_size = LATENCY_STATS_WINDOW_SIZE) {
  }

  bool is_ready() const {
    return rolling_count(m_acc) == LATENCY_STATS_WINDOW_SIZE;
  }

  void add(uint64_t latency) {
    m_acc(latency);
  }

  uint64_t avg() const {
    auto count = rolling_count(m_acc);

    if (count > 0) {
      return rolling_sum(m_acc);
    }
    return 0;
  }
};

template <typename I>
bool SimpleSchedulerObjectDispatch<I>::ObjectRequests::try_delay_request(
    uint64_t object_off, ceph::bufferlist&& data, IOContext io_context,
    int op_flags, int object_dispatch_flags, Context* on_dispatched) {
  if (!m_delayed_requests.empty()) {
    if (!m_io_context || *m_io_context != *io_context ||
        op_flags != m_op_flags || data.length() == 0 ||
        intersects(object_off, data.length())) {
      return false;
    }
  } else {
    m_io_context = io_context;
    m_op_flags = op_flags;
  }

  if (data.length() == 0) {
    // a zero length write is usually a special case,
    // and we don't want it to be merged with others
    ceph_assert(m_delayed_requests.empty());
    m_delayed_request_extents.insert(0, UINT64_MAX);
  } else {
    m_delayed_request_extents.insert(object_off, data.length());
  }
  m_object_dispatch_flags |= object_dispatch_flags;

  if (!m_delayed_requests.empty()) {
    // try to merge front to an existing request
    auto iter = m_delayed_requests.find(object_off + data.length());
    if (iter != m_delayed_requests.end()) {
      auto new_iter = m_delayed_requests.insert({object_off, {}}).first;
      new_iter->second.data = std::move(data);
      new_iter->second.data.append(std::move(iter->second.data));
      new_iter->second.requests = std::move(iter->second.requests);
      new_iter->second.requests.push_back(on_dispatched);
      m_delayed_requests.erase(iter);

      if (new_iter != m_delayed_requests.begin()) {
        auto prev = new_iter;
        try_merge_delayed_requests(--prev, new_iter);
      }
      return true;
    }

    // try to merge back to an existing request
    iter = m_delayed_requests.lower_bound(object_off);
    if (iter != m_delayed_requests.begin() &&
        (iter == m_delayed_requests.end() || iter->first > object_off)) {
      iter--;
    }
    if (iter != m_delayed_requests.end() &&
        iter->first + iter->second.data.length() == object_off) {
      iter->second.data.append(std::move(data));
      iter->second.requests.push_back(on_dispatched);

      auto next = iter;
      if (++next != m_delayed_requests.end()) {
        try_merge_delayed_requests(iter, next);
      }
      return true;
    }
  }

  // create a new request
  auto iter = m_delayed_requests.insert({object_off, {}}).first;
  iter->second.data = std::move(data);
  iter->second.requests.push_back(on_dispatched);
  return true;
}

template <typename I>
void SimpleSchedulerObjectDispatch<I>::ObjectRequests::try_merge_delayed_requests(
    typename std::map<uint64_t, MergedRequests>::iterator &iter1,
    typename std::map<uint64_t, MergedRequests>::iterator &iter2) {
  if (iter1->first + iter1->second.data.length() != iter2->first) {
    return;
  }

  iter1->second.data.append(std::move(iter2->second.data));
  iter1->second.requests.insert(iter1->second.requests.end(),
                                iter2->second.requests.begin(),
                                iter2->second.requests.end());
  m_delayed_requests.erase(iter2);
}

template <typename I>
void SimpleSchedulerObjectDispatch<I>::ObjectRequests::dispatch_delayed_requests(
    I *image_ctx, LatencyStats *latency_stats, ceph::mutex *latency_stats_lock) {
  for (auto &it : m_delayed_requests) {
    auto offset = it.first;
    auto &merged_requests = it.second;

    auto ctx = new LambdaContext(
        [requests=std::move(merged_requests.requests), latency_stats,
         latency_stats_lock, start_time=ceph_clock_now()](int r) {
          if (latency_stats) {
	    std::lock_guard locker{*latency_stats_lock};
            auto latency = ceph_clock_now() - start_time;
            latency_stats->add(latency.to_nsec());
          }
          for (auto on_dispatched : requests) {
            on_dispatched->complete(r);
          }
        });

    auto req = ObjectDispatchSpec::create_write(
        image_ctx, OBJECT_DISPATCH_LAYER_SCHEDULER,
        m_object_no, offset, std::move(merged_requests.data), m_io_context,
        m_op_flags, 0, std::nullopt, 0, {}, ctx);

    req->object_dispatch_flags = m_object_dispatch_flags;
    req->send();
  }

  m_dispatch_time = {};
}

template <typename I>
SimpleSchedulerObjectDispatch<I>::SimpleSchedulerObjectDispatch(
    I* image_ctx)
  : m_image_ctx(image_ctx),
    m_flush_tracker(new FlushTracker<I>(image_ctx)),
    m_lock(ceph::make_mutex(librbd::util::unique_lock_name(
      "librbd::io::SimpleSchedulerObjectDispatch::lock", this))),
    m_max_delay(image_ctx->config.template get_val<uint64_t>(
      "rbd_io_scheduler_simple_max_delay")) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 5) << "ictx=" << image_ctx << dendl;

  I::get_timer_instance(cct, &m_timer, &m_timer_lock);

  if (m_max_delay == 0) {
    m_latency_stats = std::make_unique<LatencyStats>();
  }
}

template <typename I>
SimpleSchedulerObjectDispatch<I>::~SimpleSchedulerObjectDispatch() {
  delete m_flush_tracker;
}

template <typename I>
void SimpleSchedulerObjectDispatch<I>::init() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  // add ourself to the IO object dispatcher chain
  m_image_ctx->io_object_dispatcher->register_dispatch(this);
}

template <typename I>
void SimpleSchedulerObjectDispatch<I>::shut_down(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  m_flush_tracker->shut_down();
  on_finish->complete(0);
}

template <typename I>
bool SimpleSchedulerObjectDispatch<I>::read(
    uint64_t object_no, ReadExtents* extents, IOContext io_context,
    int op_flags, int read_flags, const ZTracer::Trace &parent_trace,
    uint64_t* version, int* object_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " " << extents
                 << dendl;

  std::lock_guard locker{m_lock};
  for (auto& extent : *extents) {
    if (intersects(object_no, extent.offset, extent.length)) {
      dispatch_delayed_requests(object_no);
      break;
    }
  }

  return false;
}

template <typename I>
bool SimpleSchedulerObjectDispatch<I>::discard(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    IOContext io_context, int discard_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  std::lock_guard locker{m_lock};
  dispatch_delayed_requests(object_no);
  register_in_flight_request(object_no, {}, on_finish);

  return false;
}

template <typename I>
bool SimpleSchedulerObjectDispatch<I>::write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    IOContext io_context, int op_flags, int write_flags,
    std::optional<uint64_t> assert_version,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << data.length() << dendl;

  std::lock_guard locker{m_lock};

  // don't try to batch assert version writes
  if (assert_version.has_value() ||
      (write_flags & OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE) != 0) {
    dispatch_delayed_requests(object_no);
    return false;
  }

  if (try_delay_write(object_no, object_off, std::move(data), io_context,
                      op_flags, *object_dispatch_flags, on_dispatched)) {

    auto dispatch_seq = ++m_dispatch_seq;
    m_flush_tracker->start_io(dispatch_seq);
    *on_finish = new LambdaContext(
      [this, dispatch_seq, ctx=*on_finish](int r) {
        ctx->complete(r);
        m_flush_tracker->finish_io(dispatch_seq);
      });

    *dispatch_result = DISPATCH_RESULT_COMPLETE;
    return true;
  }

  dispatch_delayed_requests(object_no);
  register_in_flight_request(object_no, ceph_clock_now(), on_finish);

  return false;
}

template <typename I>
bool SimpleSchedulerObjectDispatch<I>::write_same(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
    IOContext io_context, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;

  std::lock_guard locker{m_lock};
  dispatch_delayed_requests(object_no);
  register_in_flight_request(object_no, {}, on_finish);

  return false;
}

template <typename I>
bool SimpleSchedulerObjectDispatch<I>::compare_and_write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
    ceph::bufferlist&& write_data, IOContext io_context, int op_flags,
    const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
    int* object_dispatch_flags, uint64_t* journal_tid,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << cmp_data.length() << dendl;

  std::lock_guard locker{m_lock};
  dispatch_delayed_requests(object_no);
  register_in_flight_request(object_no, {}, on_finish);

  return false;
}

template <typename I>
bool SimpleSchedulerObjectDispatch<I>::flush(
    FlushSource flush_source, const ZTracer::Trace &parent_trace,
    uint64_t* journal_tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  {
    std::lock_guard locker{m_lock};
    dispatch_all_delayed_requests();
  }

  *dispatch_result = DISPATCH_RESULT_CONTINUE;
  m_flush_tracker->flush(on_dispatched);

  return true;
}

template <typename I>
bool SimpleSchedulerObjectDispatch<I>::intersects(
    uint64_t object_no, uint64_t object_off, uint64_t len) const {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  auto cct = m_image_ctx->cct;

  auto it = m_requests.find(object_no);
  bool intersects = (it != m_requests.end()) &&
      it->second->intersects(object_off, len);

  ldout(cct, 20) << intersects << dendl;

  return intersects;
}

template <typename I>
bool SimpleSchedulerObjectDispatch<I>::try_delay_write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    IOContext io_context, int op_flags, int object_dispatch_flags,
    Context* on_dispatched) {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  auto cct = m_image_ctx->cct;

  if (m_latency_stats && !m_latency_stats->is_ready()) {
    ldout(cct, 20) << "latency stats not collected yet" << dendl;
    return false;
  }

  auto it = m_requests.find(object_no);
  if (it == m_requests.end()) {
    ldout(cct, 20) << "no pending requests" << dendl;
    return false;
  }

  auto &object_requests = it->second;
  bool delayed = object_requests->try_delay_request(
      object_off, std::move(data), io_context, op_flags, object_dispatch_flags,
      on_dispatched);

  ldout(cct, 20) << "delayed: " << delayed << dendl;

  // schedule dispatch on the first request added
  if (delayed && !object_requests->is_scheduled_dispatch()) {
    auto dispatch_time = ceph::real_clock::now();
    if (m_latency_stats) {
      dispatch_time += std::chrono::nanoseconds(m_latency_stats->avg() / 2);
    } else {
      dispatch_time += std::chrono::milliseconds(m_max_delay);
    }
    object_requests->set_scheduled_dispatch(dispatch_time);
    m_dispatch_queue.push_back(object_requests);
    if (m_dispatch_queue.front() == object_requests) {
      schedule_dispatch_delayed_requests();
    }
  }

  return delayed;
}

template <typename I>
void SimpleSchedulerObjectDispatch<I>::dispatch_all_delayed_requests() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  while (!m_requests.empty()) {
    auto it = m_requests.begin();
    dispatch_delayed_requests(it->second);
    m_requests.erase(it);
  }
}

template <typename I>
void SimpleSchedulerObjectDispatch<I>::register_in_flight_request(
    uint64_t object_no, const utime_t &start_time, Context **on_finish) {
  auto res = m_requests.insert(
      {object_no, std::make_shared<ObjectRequests>(object_no)});
  ceph_assert(res.second);
  auto it = res.first;

  auto dispatch_seq = ++m_dispatch_seq;
  m_flush_tracker->start_io(dispatch_seq);

  it->second->set_dispatch_seq(dispatch_seq);
  *on_finish = new LambdaContext(
    [this, object_no, dispatch_seq, start_time, ctx=*on_finish](int r) {
      ctx->complete(r);

      std::unique_lock locker{m_lock};
      if (m_latency_stats && start_time != utime_t()) {
        auto latency = ceph_clock_now() - start_time;
        m_latency_stats->add(latency.to_nsec());
      }

      auto it = m_requests.find(object_no);
      if (it == m_requests.end() ||
          it->second->get_dispatch_seq() != dispatch_seq) {
        ldout(m_image_ctx->cct, 20) << "already dispatched" << dendl;
      } else {
        dispatch_delayed_requests(it->second);
        m_requests.erase(it);
      }
      locker.unlock();

      m_flush_tracker->finish_io(dispatch_seq);
    });
}

template <typename I>
void SimpleSchedulerObjectDispatch<I>::dispatch_delayed_requests(
    uint64_t object_no) {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  auto cct = m_image_ctx->cct;

  auto it = m_requests.find(object_no);
  if (it == m_requests.end()) {
    ldout(cct, 20) << "object_no=" << object_no << ": not found" << dendl;
    return;
  }

  dispatch_delayed_requests(it->second);
  m_requests.erase(it);
}

template <typename I>
void SimpleSchedulerObjectDispatch<I>::dispatch_delayed_requests(
    ObjectRequestsRef object_requests) {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  auto cct = m_image_ctx->cct;

  ldout(cct, 20) << "object_no=" << object_requests->get_object_no() << ", "
                 << object_requests->delayed_requests_size() << " requests, "
                 << "dispatch_time=" << object_requests->get_dispatch_time()
                 << dendl;

  if (!object_requests->is_scheduled_dispatch()) {
    return;
  }

  object_requests->dispatch_delayed_requests(m_image_ctx, m_latency_stats.get(),
                                             &m_lock);

  ceph_assert(!m_dispatch_queue.empty());
  if (m_dispatch_queue.front() == object_requests) {
    m_dispatch_queue.pop_front();
    schedule_dispatch_delayed_requests();
  }
}

template <typename I>
void SimpleSchedulerObjectDispatch<I>::schedule_dispatch_delayed_requests() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  auto cct = m_image_ctx->cct;

  std::lock_guard timer_locker{*m_timer_lock};

  if (m_timer_task != nullptr) {
    ldout(cct, 20) << "canceling task " << m_timer_task << dendl;

    bool canceled = m_timer->cancel_event(m_timer_task);
    ceph_assert(canceled);
    m_timer_task = nullptr;
  }

  if (m_dispatch_queue.empty()) {
    ldout(cct, 20) << "nothing to schedule" << dendl;
    return;
  }

  auto object_requests = m_dispatch_queue.front().get();

  while (!object_requests->is_scheduled_dispatch()) {
    ldout(cct, 20) << "garbage collecting " << object_requests << dendl;
    m_dispatch_queue.pop_front();

    if (m_dispatch_queue.empty()) {
      ldout(cct, 20) << "nothing to schedule" << dendl;
      return;
    }
    object_requests = m_dispatch_queue.front().get();
  }

  m_timer_task = new LambdaContext(
    [this, object_no=object_requests->get_object_no()](int r) {
      ceph_assert(ceph_mutex_is_locked(*m_timer_lock));
      auto cct = m_image_ctx->cct;
      ldout(cct, 20) << "running timer task " << m_timer_task << dendl;

      m_timer_task = nullptr;
      m_image_ctx->asio_engine->post(
        [this, object_no]() {
          std::lock_guard locker{m_lock};
          dispatch_delayed_requests(object_no);
        });
    });

  ldout(cct, 20) << "scheduling task " << m_timer_task << " at "
                 << object_requests->get_dispatch_time() << dendl;

  m_timer->add_event_at(object_requests->get_dispatch_time(), m_timer_task);
}

} // namespace io
} // namespace librbd

template class librbd::io::SimpleSchedulerObjectDispatch<librbd::ImageCtx>;
