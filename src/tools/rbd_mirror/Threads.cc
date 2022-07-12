// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/Threads.h"
#include "common/Timer.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/asio/ContextWQ.h"

namespace rbd {
namespace mirror {

template <typename I>
Threads<I>::Threads(std::shared_ptr<librados::Rados>& rados) {
  auto cct = static_cast<CephContext*>(rados->cct());
  asio_engine = new librbd::AsioEngine(rados);
  work_queue = asio_engine->get_work_queue();

  timer = new SafeTimer(cct, timer_lock, true);
  timer->init();
}

template <typename I>
Threads<I>::~Threads() {
  {
    std::lock_guard timer_locker{timer_lock};
    timer->shutdown();
  }
  delete timer;

  work_queue->drain();
  delete asio_engine;
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::Threads<librbd::ImageCtx>;
