// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/Threads.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"

namespace rbd {
namespace mirror {

Threads::Threads(CephContext *cct) : timer_lock("Threads::timer_lock") {
  thread_pool = new ThreadPool(cct, "Journaler::thread_pool", "tp_journal",
                               cct->_conf->rbd_op_threads, "rbd_op_threads");
  thread_pool->start();

  work_queue = new ContextWQ("Journaler::work_queue",
                             cct->_conf->rbd_op_thread_timeout, thread_pool);

  timer = new SafeTimer(cct, timer_lock, true);
  timer->init();
}

Threads::~Threads() {
  {
    Mutex::Locker timer_locker(timer_lock);
    timer->shutdown();
  }
  delete timer;

  work_queue->drain();
  delete work_queue;

  thread_pool->stop();
  delete thread_pool;
}

} // namespace mirror
} // namespace rbd
