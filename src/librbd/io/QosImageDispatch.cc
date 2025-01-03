// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/QosImageDispatch.h"
#include "common/dout.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/FlushTracker.h"
#include <utility>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::QosImageDispatch: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace io {

namespace {

uint64_t get_extent_length(const Extents& extents) {
  uint64_t length = 0;
  for (auto& extent : extents) {
    length += extent.second;
  }
  return length;
}

uint64_t calculate_tokens(bool read_op, uint64_t extent_length, uint64_t flag) {
  if (read_op && ((flag & IMAGE_DISPATCH_FLAG_QOS_WRITE_MASK) != 0)) {
    return 0;
  } else if (!read_op && ((flag & IMAGE_DISPATCH_FLAG_QOS_READ_MASK) != 0)) {
    return 0;
  }

  return (((flag & IMAGE_DISPATCH_FLAG_QOS_BPS_MASK) != 0) ? extent_length : 1);
}

static const std::pair<uint64_t, const char*> throttle_flags[] = {
  {IMAGE_DISPATCH_FLAG_QOS_IOPS_THROTTLE,       "rbd_qos_iops_throttle"       },
  {IMAGE_DISPATCH_FLAG_QOS_BPS_THROTTLE,        "rbd_qos_bps_throttle"        },
  {IMAGE_DISPATCH_FLAG_QOS_READ_IOPS_THROTTLE,  "rbd_qos_read_iops_throttle"  },
  {IMAGE_DISPATCH_FLAG_QOS_WRITE_IOPS_THROTTLE, "rbd_qos_write_iops_throttle" },
  {IMAGE_DISPATCH_FLAG_QOS_READ_BPS_THROTTLE,   "rbd_qos_read_bps_throttle"   },
  {IMAGE_DISPATCH_FLAG_QOS_WRITE_BPS_THROTTLE,  "rbd_qos_write_bps_throttle"  }
};

} // anonymous namespace

template <typename I>
QosImageDispatch<I>::QosImageDispatch(I* image_ctx)
  : m_image_ctx(image_ctx), m_flush_tracker(new FlushTracker<I>(image_ctx)) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "ictx=" << image_ctx << dendl;

  SafeTimer *timer;
  ceph::mutex *timer_lock;
  ImageCtx::get_timer_instance(cct, &timer, &timer_lock);
  for (auto [flag, name] : throttle_flags) {
    m_throttles.emplace_back(
      flag,
      new TokenBucketThrottle(cct, name, 0, 0, timer, timer_lock));
  }
}

template <typename I>
QosImageDispatch<I>::~QosImageDispatch() {
  for (auto t : m_throttles) {
    delete t.second;
  }
}

template <typename I>
void QosImageDispatch<I>::shut_down(Context* on_finish) {
  m_flush_tracker->shut_down();
  on_finish->complete(0);
}

template <typename I>
void QosImageDispatch<I>::apply_qos_schedule_tick_min(uint64_t tick) {
  for (auto pair : m_throttles) {
    pair.second->set_schedule_tick_min(tick);
  }
}

template <typename I>
void QosImageDispatch<I>::apply_qos_limit(uint64_t flag, uint64_t limit,
                                          uint64_t burst, uint64_t burst_seconds) {
  auto cct = m_image_ctx->cct;
  TokenBucketThrottle *throttle = nullptr;
  for (auto pair : m_throttles) {
    if (flag == pair.first) {
      throttle = pair.second;
      break;
    }
  }
  ceph_assert(throttle != nullptr);

  int r = throttle->set_limit(limit, burst, burst_seconds);
  if (r < 0) {
    lderr(cct) << throttle->get_name() << ": invalid qos parameter: "
               << "burst(" << burst << ") is less than "
               << "limit(" << limit << ")" << dendl;
    // if apply failed, we should at least make sure the limit works.
    throttle->set_limit(limit, 0, 1);
  }

  if (limit) {
    m_qos_enabled_flag |= flag;
  } else {
    m_qos_enabled_flag &= ~flag;
  }
}

template <typename I>
void QosImageDispatch<I>::apply_qos_exclude_ops(uint64_t exclude_ops) {
  m_qos_exclude_ops = exclude_ops;
}

template <typename I>
bool QosImageDispatch<I>::read(
    AioCompletion* aio_comp, Extents &&image_extents, ReadResult &&read_result,
    IOContext io_context, int op_flags, int read_flags,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (m_qos_exclude_ops & RBD_IO_OPERATION_READ ||
      m_qos_enabled_flag == 0) {
    return false;
  }

  if (needs_throttle(true, image_extents, tid, image_dispatch_flags,
                     dispatch_result, on_finish, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool QosImageDispatch<I>::write(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (m_qos_exclude_ops & RBD_IO_OPERATION_WRITE ||
      m_qos_enabled_flag == 0) {
    return false;
  }

  if (needs_throttle(false, image_extents, tid, image_dispatch_flags,
                     dispatch_result, on_finish, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool QosImageDispatch<I>::discard(
    AioCompletion* aio_comp, Extents &&image_extents,
    uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (m_qos_exclude_ops & RBD_IO_OPERATION_DISCARD ||
      m_qos_enabled_flag == 0) {
    return false;
  }

  if (needs_throttle(false, image_extents, tid, image_dispatch_flags,
                     dispatch_result, on_finish, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool QosImageDispatch<I>::write_same(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (m_qos_exclude_ops & RBD_IO_OPERATION_WRITE_SAME ||
      m_qos_enabled_flag == 0) {
    return false;
  }

  if (needs_throttle(false, image_extents, tid, image_dispatch_flags,
                     dispatch_result, on_finish, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool QosImageDispatch<I>::compare_and_write(
    AioCompletion* aio_comp, Extents &&image_extents,
    bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (m_qos_exclude_ops & RBD_IO_OPERATION_COMPARE_AND_WRITE ||
      m_qos_enabled_flag == 0) {
    return false;
  }

  if (needs_throttle(false, image_extents, tid, image_dispatch_flags,
                     dispatch_result, on_finish, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool QosImageDispatch<I>::flush(
    AioCompletion* aio_comp, FlushSource flush_source,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  *dispatch_result = DISPATCH_RESULT_CONTINUE;
  m_flush_tracker->flush(on_dispatched);
  return true;
}

template <typename I>
void QosImageDispatch<I>::handle_finished(int r, uint64_t tid) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  m_flush_tracker->finish_io(tid);
}

template <typename I>
bool QosImageDispatch<I>::set_throttle_flags(
    std::atomic<uint32_t>* image_dispatch_flags, uint32_t flags) {
  uint32_t expected = image_dispatch_flags->load();
  uint32_t desired;
  do {
    desired = expected | flags;
  } while (!image_dispatch_flags->compare_exchange_weak(expected, desired));

  return ((desired & IMAGE_DISPATCH_FLAG_QOS_MASK) ==
             IMAGE_DISPATCH_FLAG_QOS_MASK);
}

template <typename I>
bool QosImageDispatch<I>::needs_throttle(
    bool read_op, const Extents& image_extents, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  auto extent_length = get_extent_length(image_extents);
  uint32_t flags_to_set = 0;

  if (!read_op) {
    m_flush_tracker->start_io(tid);
    *on_finish = new LambdaContext([this, tid, on_finish=*on_finish](int r) {
        handle_finished(r, tid);
        on_finish->complete(r);
      });
  }
  *dispatch_result = DISPATCH_RESULT_CONTINUE;

  auto qos_enabled_flag = m_qos_enabled_flag;
  for (auto [flag, throttle] : m_throttles) {
    if ((qos_enabled_flag & flag) == 0) {
      flags_to_set |= flag;
      continue;
    }

    auto tokens = calculate_tokens(read_op, extent_length, flag);
    if (tokens > 0 &&
        throttle->get(tokens, this, &QosImageDispatch<I>::handle_throttle_ready,
                      Tag{image_dispatch_flags, on_dispatched}, flag)) {
      ldout(cct, 15) << "on_dispatched=" << on_dispatched << ", "
                     << "flag=" << flag << dendl;
    } else {
      flags_to_set |= flag;
    }
  }

  // flags_to_set should never be zero because a single op cannot
  // activate all throttles (and m_throttles cannot be empty)
  ceph_assert(flags_to_set != 0);

  return !set_throttle_flags(image_dispatch_flags, flags_to_set);
}

template <typename I>
void QosImageDispatch<I>::handle_throttle_ready(Tag&& tag, uint64_t flag) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 15) << "on_dispatched=" << tag.on_dispatched << ", "
                 << "flag=" << flag << dendl;

  if (set_throttle_flags(tag.image_dispatch_flags, flag)) {
    // timer_lock is held -- so dispatch from outside the timer thread
    m_image_ctx->asio_engine->post(tag.on_dispatched, 0);
  }
}

} // namespace io
} // namespace librbd

template class librbd::io::QosImageDispatch<librbd::ImageCtx>;
