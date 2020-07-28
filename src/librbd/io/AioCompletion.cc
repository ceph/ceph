// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/AioCompletion.h"
#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"

#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Types.h"
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>

#ifdef WITH_LTTNG
#include "tracing/librbd.h"
#else
#define tracepoint(...)
#endif

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::AioCompletion: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

int AioCompletion::wait_for_complete() {
  tracepoint(librbd, aio_wait_for_complete_enter, this);
  {
    std::unique_lock<std::mutex> locker(lock);
    while (state != AIO_STATE_COMPLETE) {
      cond.wait(locker);
    }
  }
  tracepoint(librbd, aio_wait_for_complete_exit, 0);
  return 0;
}

void AioCompletion::finalize() {
  ceph_assert(ictx != nullptr);
  CephContext *cct = ictx->cct;

  // finalize any pending error results since we won't be
  // atomically incrementing rval anymore
  int err_r = error_rval;
  if (err_r < 0) {
    rval = err_r;
  }

  ssize_t r = rval;
  ldout(cct, 20) << "r=" << r << dendl;
  if (r >= 0 && aio_type == AIO_TYPE_READ) {
    read_result.assemble_result(cct);
  }
}

void AioCompletion::complete() {
  ceph_assert(ictx != nullptr);

  ssize_t r = rval;
  if ((aio_type == AIO_TYPE_CLOSE) || (aio_type == AIO_TYPE_OPEN && r < 0)) {
    ictx = nullptr;
    external_callback = false;
  } else {
    CephContext *cct = ictx->cct;

    tracepoint(librbd, aio_complete_enter, this, r);
    if (ictx->perfcounter != nullptr) {
      ceph::timespan elapsed = coarse_mono_clock::now() - start_time;
      switch (aio_type) {
      case AIO_TYPE_GENERIC:
      case AIO_TYPE_OPEN:
        break;
      case AIO_TYPE_READ:
        ictx->perfcounter->tinc(l_librbd_rd_latency, elapsed); break;
      case AIO_TYPE_WRITE:
        ictx->perfcounter->tinc(l_librbd_wr_latency, elapsed); break;
      case AIO_TYPE_DISCARD:
        ictx->perfcounter->tinc(l_librbd_discard_latency, elapsed); break;
      case AIO_TYPE_FLUSH:
        ictx->perfcounter->tinc(l_librbd_flush_latency, elapsed); break;
      case AIO_TYPE_WRITESAME:
        ictx->perfcounter->tinc(l_librbd_ws_latency, elapsed); break;
      case AIO_TYPE_COMPARE_AND_WRITE:
        ictx->perfcounter->tinc(l_librbd_cmp_latency, elapsed); break;
      default:
        lderr(cct) << "completed invalid aio_type: " << aio_type << dendl;
        break;
      }
    }
  }

  state = AIO_STATE_CALLBACK;
  if (complete_cb) {
    if (external_callback) {
      complete_external_callback();
    } else {
      complete_cb(rbd_comp, complete_arg);
      complete_event_socket();
      notify_callbacks_complete();
    }
  } else {
    complete_event_socket();
    notify_callbacks_complete();
  }

  if (image_dispatcher_ctx != nullptr) {
    image_dispatcher_ctx->complete(rval);
  }

  // note: possible for image to be closed after op marked finished
  if (async_op.started()) {
    async_op.finish_op();
  }
  tracepoint(librbd, aio_complete_exit);
}

void AioCompletion::init_time(ImageCtx *i, aio_type_t t) {
  if (ictx == nullptr) {
    ictx = i;
    aio_type = t;
    start_time = coarse_mono_clock::now();
  }
}

void AioCompletion::start_op() {
  ceph_assert(ictx != nullptr);

  if (aio_type == AIO_TYPE_OPEN || aio_type == AIO_TYPE_CLOSE) {
    // no need to track async open/close operations
    return;
  }

  ceph_assert(!async_op.started());
  async_op.start_op(*ictx);
}

void AioCompletion::queue_complete() {
  uint32_t zero = 0;
  pending_count.compare_exchange_strong(zero, 1);
  ceph_assert(zero == 0);

  add_request();

  // ensure completion fires in clean lock context
  boost::asio::post(ictx->asio_engine->get_api_strand(), [this]() {
      complete_request(0);
    });
}

void AioCompletion::block(CephContext* cct) {
  ldout(cct, 20) << dendl;
  ceph_assert(!was_armed);

  get();
  ++pending_count;
}

void AioCompletion::unblock(CephContext* cct) {
  ldout(cct, 20) << dendl;
  ceph_assert(was_armed);

  uint32_t previous_pending_count = pending_count--;
  ceph_assert(previous_pending_count > 0);

  if (previous_pending_count == 1) {
    queue_complete();
  }
  put();
}

void AioCompletion::fail(int r)
{
  ceph_assert(ictx != nullptr);
  ceph_assert(r < 0);

  bool queue_required = true;
  if (aio_type == AIO_TYPE_CLOSE || aio_type == AIO_TYPE_OPEN) {
    // executing from a safe context and the ImageCtx has been destructed
    queue_required = false;
  } else {
    CephContext *cct = ictx->cct;
    lderr(cct) << cpp_strerror(r) << dendl;
  }

  ceph_assert(!was_armed);
  was_armed = true;

  rval = r;

  uint32_t previous_pending_count = pending_count.load();
  if (previous_pending_count == 0) {
    if (queue_required) {
      queue_complete();
    } else {
      complete();
    }
  }
}

void AioCompletion::set_request_count(uint32_t count) {
  ceph_assert(ictx != nullptr);
  CephContext *cct = ictx->cct;

  ceph_assert(!was_armed);
  was_armed = true;

  ldout(cct, 20) << "pending=" << count << dendl;
  uint32_t previous_pending_count = pending_count.fetch_add(count);
  if (previous_pending_count == 0 && count == 0) {
    queue_complete();
  }
}

void AioCompletion::complete_request(ssize_t r)
{
  uint32_t previous_pending_count = pending_count--;
  ceph_assert(previous_pending_count > 0);
  auto pending_count = previous_pending_count - 1;

  ceph_assert(ictx != nullptr);
  CephContext *cct = ictx->cct;

  if (r > 0) {
    rval += r;
  } else if (r != -EEXIST) {
    // might race w/ another thread setting an error code but
    // first one wins
    int zero = 0;
    error_rval.compare_exchange_strong(zero, r);
  }

  ldout(cct, 20) << "cb=" << complete_cb << ", "
                 << "pending=" << pending_count << dendl;
  if (pending_count == 0) {
    finalize();
    complete();
  }
  put();
}

bool AioCompletion::is_complete() {
  tracepoint(librbd, aio_is_complete_enter, this);
  bool done = (this->state != AIO_STATE_PENDING);
  tracepoint(librbd, aio_is_complete_exit, done);
  return done;
}

ssize_t AioCompletion::get_return_value() {
  tracepoint(librbd, aio_get_return_value_enter, this);
  ssize_t r = rval;
  tracepoint(librbd, aio_get_return_value_exit, r);
  return r;
}

void AioCompletion::complete_external_callback() {
  get();

  // ensure librbd external users never experience concurrent callbacks
  // from multiple librbd-internal threads.
  boost::asio::dispatch(ictx->asio_engine->get_api_strand(), [this]() {
      complete_cb(rbd_comp, complete_arg);
      complete_event_socket();
      notify_callbacks_complete();
      put();
    });
}

void AioCompletion::complete_event_socket() {
  if (ictx != nullptr && event_notify && ictx->event_socket.is_valid()) {
    ictx->event_socket_completions.push(this);
    ictx->event_socket.notify();
  }
}

void AioCompletion::notify_callbacks_complete() {
  state = AIO_STATE_COMPLETE;

  std::unique_lock<std::mutex> locker(lock);
  cond.notify_all();
}

} // namespace io
} // namespace librbd
