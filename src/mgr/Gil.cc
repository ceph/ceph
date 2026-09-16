// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include <frameobject.h>
#include "mgr/mgr_perf_counters.h"
#include "common/perf_counters.h"
#include "common/BackTrace.h"

#include "common/debug.h"
#include "common/ceph_time.h"

#include <atomic>
#include <cstring>
#include <mutex>
#include <set>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

#include "Gil.h"

thread_local const char *gil_thread_module_name = "(unknown)";
thread_local bool gil_thread_tag_attempted = false;
static std::mutex g_intern_mutex;
static std::set<std::string> g_interned_module_names;
static std::atomic<const char *> g_last_holder{"(none)"};

void gil_tag_thread_module(const std::string &name)
{
  if (strcmp(gil_thread_module_name, name.c_str()) == 0) {
    gil_thread_tag_attempted = true;
    return;
  }
  std::lock_guard<std::mutex> l(g_intern_mutex);
  const std::string &interned = *g_interned_module_names.insert(name).first;
  gil_thread_module_name = interned.c_str();
  gil_thread_tag_attempted = true;
}

static void gil_ensure_thread_tagged()
{
  if (gil_thread_tag_attempted) {
    return;
  }
  static const char marker[] = "/pybind/mgr/";
  PyFrameObject *frame = PyEval_GetFrame(); // borrowed
  if (frame == nullptr) {
    // Not inside any Python call right now
    return;
  }
  gil_thread_tag_attempted = true;
  Py_XINCREF(frame);
  while (frame != nullptr) {
    PyCodeObject *code = PyFrame_GetCode(frame); // new ref
    if (code != nullptr) {
      if (code->co_filename != nullptr && PyUnicode_Check(code->co_filename)) {
        const char *fn = PyUnicode_AsUTF8(code->co_filename);
        if (fn != nullptr) {
          const char *pos = strstr(fn, marker);
          if (pos != nullptr) {
            const char *name_start = pos + (sizeof(marker) - 1);
            const char *slash = strchr(name_start, '/');
            if (slash != nullptr && slash > name_start) {
              gil_tag_thread_module(std::string(name_start, slash - name_start));
              Py_DECREF(code);
              Py_DECREF(frame);
              return;
            }
          }
        }
      }
      Py_DECREF(code);
    }
    PyFrameObject *back = PyFrame_GetBack(frame); // new ref
    Py_DECREF(frame);
    frame = back;
  }
}

const char *gil_get_last_holder()
{
  return g_last_holder.load(std::memory_order_relaxed);
}

static void gil_set_last_holder(const char *name)
{
  g_last_holder.store(name, std::memory_order_relaxed);
}

static constexpr uint64_t gil_fixed_threshold_ns = 2'000'000; // 2 ms

static void record_gil_wait(const char *what, int counter_id,
                             std::chrono::nanoseconds wait_duration)
{
  gil_ensure_thread_tagged();

  auto prior_avg = perfcounter->get_tavg_ns(counter_id);
  uint64_t avg_ns = (prior_avg.second != 0)
    ? (prior_avg.first / prior_avg.second) : 0;

  perfcounter->tinc(counter_id, wait_duration);

  uint64_t dynamic_threshold = avg_ns * 10;
  auto wait_ns = static_cast<uint64_t>(wait_duration.count());
  if ((avg_ns > 0 && wait_ns > dynamic_threshold) || 
    wait_ns > gil_fixed_threshold_ns) {
    dout(20) << what << ": module '" << gil_thread_module_name
             << "' waited " << wait_ns << "ns for the GIL (prior avg "
             << avg_ns << "ns, dynamic threshold " << dynamic_threshold
             << "ns); last known holder: '" << gil_get_last_holder() << "'"
             << dendl;
    dout(20) << what << ": backtrace: " << ClibBackTrace(0) << dendl;
  }
}

static void assert_gil()
{
  /* Using PyGILState_Check() isn't appropriate:
   *
   * https://docs.python.org/3/c-api/init.html#c.PyGILState_Check
   *
   * "Only if it has had its thread state initialized via PyGILState_Ensure()
   * will it return 1."
   *
   * We got away with it for a while due to:
   *
   * "Note: If the current Python process has ever created a subinterpreter,
   * this function will always return 1."
   *
   * Instead, use PyThreadState_Get() and use ts->thread_id to confirm that it's
   * the right thread (ts->thread_id isn't necessarily stable, and may need to
   * change in the future).  Once we no longer need to support python versions
   * prior to 3.13, this can be PyThreadState_GetUnchecked().
   */
  auto *ts = PyThreadState_Get();
  ceph_assert(ts != nullptr);
  ceph_assert(ts->thread_id == PyThread_get_thread_ident());
}

SafeThreadState::SafeThreadState(PyThreadState *ts_)
    : ts(ts_)
{
  ceph_assert(ts != nullptr);
  thread = pthread_self();
}

Gil::Gil(SafeThreadState &ts, bool new_thread) : pThreadState(ts)
{
  acquire(new_thread);
}

Gil::Gil(SafeThreadState &ts, bool new_thread, const std::string &module_name)
  : pThreadState(ts)
{
  // Tag before acquiring, so if this acquisition itself is slow enough
  // to be logged by record_gil_wait() below, it's logged under the
  // right module. Remember the thread's previous tag so ~Gil() can put
  // it back -- e.g. dispatch_remote() calling from module A into module
  // B constructs one of these on A's thread; without restoring, A's
  // thread would stay mislabeled as B for the rest of its life.
  if (!module_name.empty()) {
    pPreviousModuleName = gil_thread_module_name;
    pRestoreModuleName = true;
    gil_tag_thread_module(module_name);
  }
  acquire(new_thread);
}

void Gil::acquire(bool new_thread)
{
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
  auto start = ceph::mono_clock::now();
  if (new_thread) {
    pNewThreadState = PyThreadState_New(pThreadState.ts->interp);
    PyEval_RestoreThread(pNewThreadState);
    dout(20) << "Switched to new thread state " << pNewThreadState << dendl;
  } else {
    // Acquire the GIL, set the current thread state
    PyEval_RestoreThread(pThreadState.ts);
    ceph_assert(pthread_self() == pThreadState.thread);
  }
  auto wait_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
    ceph::mono_clock::now() - start);
  dout(25) << "GIL acquired for thread state " << pThreadState.ts << " in "
           << wait_duration.count() << "ns" << dendl;
  record_gil_wait("Gil::Gil", l_mgr_gil_acquisition_avg, wait_duration);
  assert_gil();
}

Gil::~Gil()
{
  gil_set_last_holder(gil_thread_module_name);
  // Release the GIL, reset the thread state to NULL
  if (pNewThreadState != nullptr) {
    dout(20) << "Destroying new thread state " << pNewThreadState << dendl;
    PyThreadState_Clear(pNewThreadState);
    PyEval_SaveThread();
    PyThreadState_Delete(pNewThreadState);
  } else {
    PyEval_SaveThread();
  }
  dout(25) << "GIL released for thread state " << pThreadState.ts << dendl;
  if (pRestoreModuleName) {
    gil_thread_module_name = pPreviousModuleName;
  }
}

without_gil_t::without_gil_t()
{
  assert_gil();
  gil_ensure_thread_tagged();
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
  gil_set_last_holder(gil_thread_module_name);
  save = PyEval_SaveThread();
}

void without_gil_t::acquire_gil()
{
  assert(save);
  auto start = ceph::mono_clock::now();
  PyEval_RestoreThread(save);
  auto wait_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
    ceph::mono_clock::now() - start);
  record_gil_wait("without_gil_t::acquire_gil", l_mgr_gil_reacquire_avg, wait_duration);
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
