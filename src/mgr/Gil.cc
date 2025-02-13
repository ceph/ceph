// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "Python.h"
#include "mgr/mgr_perf_counters.h"
#include "common/BackTrace.h"
#include "common/debug.h"
#include <atomic>

static std::atomic<PyThreadState *> g_last_gil_ts { 0 };

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr

template <int LogLevelV>
void _dump_backtrace(const uint64_t avg_ns,
                     const uint64_t dynamic_threshold,
                     const std::chrono::nanoseconds wait_duration) {
  dout(LogLevelV) << "GIL acquisition average is " << avg_ns << "ns, "
          << "threshold is " << dynamic_threshold << "ns"
          << " wait_duration is " << wait_duration.count() << "ns" << dendl;
  dout(LogLevelV) << "Last GIL thread state: " << g_last_gil_ts.load(std::memory_order_relaxed) << dendl;
  dout(LogLevelV) << "GIL BackTrace:" << ClibBackTrace(0)  << dendl;
}



#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

#include "Gil.h"

SafeThreadState::SafeThreadState(PyThreadState *ts_)
    : ts(ts_)
{
  ceph_assert(ts != nullptr);
  thread = pthread_self();
}


Gil::Gil(SafeThreadState &ts, bool new_thread) : pThreadState(ts)
{
  // Acquire the GIL, set the current thread state
  auto start = std::chrono::high_resolution_clock::now();
  PyEval_RestoreThread(pThreadState.ts);
  auto wait_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::high_resolution_clock::now() - start);
  dout(25) << "GIL acquired for thread state " << pThreadState.ts << " in "
           << wait_duration.count() << "ns" << dendl;

  perfcounter->tinc(l_mgr_gil_acquisition_avg, wait_duration);
  auto current_avg = perfcounter->get_tavg_ns(l_mgr_gil_acquisition_avg);
  uint64_t avg_ns = (current_avg.first != 0) ? (current_avg.second / current_avg.first) : 0;
  uint64_t dynamic_threshold = avg_ns * 10;

  if (wait_duration.count() > dynamic_threshold || wait_duration.count() > fixed_threshold_ns) {
    _dump_backtrace<30>(avg_ns, dynamic_threshold, wait_duration);
  }

  //
  // If called from a separate OS thread (i.e. a thread not created
  // by Python, that does't already have a python thread state that
  // was created when that thread was active), we need to manually
  // create and switch to a python thread state specifically for this
  // OS thread.
  //
  // Note that instead of requring the caller to set new_thread == true
  // when calling this from a separate OS thread, we could figure out
  // if this was necessary automatically, as follows:
  //
  //   if (pThreadState->thread_id != PyThread_get_thread_ident()) {
  //
  // However, this means we're accessing pThreadState->thread_id, but
  // the Python C API docs say that "The only public data member is
  // PyInterpreterState *interp", i.e. doing this would violate
  // something that's meant to be a black box.
  //
  if (new_thread) {
    pNewThreadState = PyThreadState_New(pThreadState.ts->interp);
    PyThreadState_Swap(pNewThreadState);
    dout(20) << "Switched to new thread state " << pNewThreadState << dendl;
  } else {
    ceph_assert(pthread_self() == pThreadState.thread);
  }
}

Gil::~Gil()
{
  if (pNewThreadState != nullptr) {
    dout(20) << "Destroying new thread state " << pNewThreadState << dendl;
    PyThreadState_Swap(pThreadState.ts);
    PyThreadState_Clear(pNewThreadState);
    PyThreadState_Delete(pNewThreadState);
  }
  g_last_gil_ts.store(pThreadState.ts, std::memory_order_relaxed);
  // Release the GIL, reset the thread state to NULL
  PyEval_SaveThread();
  dout(25) << "GIL released for thread state " << pThreadState.ts << dendl;
}

without_gil_t::without_gil_t()
{
  assert(PyGILState_Check());
  release_gil();
}

without_gil_t::~without_gil_t()
{
  if (save) {
    acquire_gil();
  }
}

void without_gil_t::release_gil()
{
  save = PyEval_SaveThread();
}

void without_gil_t::acquire_gil()
{
  assert(save);
  PyEval_RestoreThread(save);
  save = nullptr;
}

with_gil_t::with_gil_t(without_gil_t& allow_threads)
  : allow_threads{allow_threads}
{
  allow_threads.acquire_gil();
}

with_gil_t::~with_gil_t()
{
  allow_threads.release_gil();
}
