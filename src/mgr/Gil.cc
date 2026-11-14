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

#include "common/ceph_time.h"
#include "common/debug.h"
#include "global/global_context.h"

#include <fmt/format.h>

#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

#include "Gil.h"

using namespace std::literals;

namespace {

// Empty when the thread isn't tagged with a module.
thread_local std::string thread_module_name;

// CPython's default sys.getswitchinterval(): how long a waiter lets the
// holder run before asking it to drop the GIL.
constexpr auto slow_wait_threshold = 5ms;
constexpr auto slow_wait_log_interval = 1s;

// At debug_mgr 5, at most one slow wait is logged per interval; the rest
// are summed up in the next report.
struct slow_wait_sampler {
  ceph::mono_time next_report;
  uint64_t unlogged = 0;
  ceph::timespan worst_unlogged{};
  std::string worst_unlogged_module;
};
// Taken with the GIL held; a leaf lock, never held while logging.
std::mutex slow_wait_sampler_lock;
slow_wait_sampler sampler;

std::string module_root()
{
  auto root = g_conf().get_val<std::string>("mgr_module_path");
  if (!root.empty() && root.back() != '/') {
    root += '/';
  }
  return root;
}

// "balancer" for <root>/balancer/module.py, empty for files outside a
// module package (mgr_module.py, stdlib, ...).
std::string_view module_package(std::string_view path, std::string_view root)
{
  if (root.empty() || !path.starts_with(root)) {
    return {};
  }
  path.remove_prefix(root.size());
  const auto slash = path.find('/');
  return slash == path.npos ? std::string_view{} : path.substr(0, slash);
}

// Calls fn(frame, code, filename) from the innermost frame outwards.
// Needs the GIL and no pending exception.
template <typename Fn>
void walk_python_frames(Fn &&fn)
{
  PyFrameObject *frame = PyEval_GetFrame(); // borrowed
  Py_XINCREF(frame);
  while (frame != nullptr) {
    PyCodeObject *code = PyFrame_GetCode(frame);
    if (const char *filename = PyUnicode_AsUTF8(code->co_filename)) {
      fn(frame, code, filename);
    } else {
      PyErr_Clear();
    }
    Py_DECREF(code);
    PyFrameObject *back = PyFrame_GetBack(frame);
    Py_DECREF(frame);
    frame = back;
  }
}

struct python_context {
  std::string module;
  std::string frame; // empty when the thread has no Python frames
};

// From the Python stack: the package of the outermost frame under
// mgr_module_path, which is what a thread a module started itself runs
// (inner frames may be shared packages such as orchestrator), and the
// innermost frame inside a module, so a wait in a MgrModule helper such
// as get() names the module code that called it.
python_context get_python_context()
{
  python_context ctx;
  if (PyErr_Occurred()) {
    ctx.frame = "?";
    return ctx;
  }
  const auto root = module_root();
  bool innermost = true;
  bool have_module_frame = false;
  walk_python_frames([&](PyFrameObject *frame, PyCodeObject *code,
                         std::string_view path) {
    const auto package = module_package(path, root);
    if (!package.empty()) {
      ctx.module = package;
    }
    if (innermost || (!have_module_frame && !package.empty())) {
      const char *name = PyUnicode_AsUTF8(code->co_name);
      if (name == nullptr) {
        PyErr_Clear();
        name = "?";
      }
      ctx.frame = fmt::format("{} ({}:{})", name, path,
                              PyFrame_GetLineNumber(frame));
      have_module_frame = !package.empty();
    }
    innermost = false;
  });
  return ctx;
}

std::string_view tagged_module()
{
  if (thread_module_name.empty()) {
    return "(unknown)";
  }
  return thread_module_name;
}

struct slow_wait {
  const char *what;
  ceph::timespan wait;
};

// Walks the Python stack, so only stream it once the line is gathered.
std::ostream &operator<<(std::ostream &out, const slow_wait &w)
{
  const auto ctx = get_python_context();
  std::string_view module = thread_module_name;
  if (module.empty()) {
    module = ctx.module.empty() ? "(unknown)" : std::string_view{ctx.module};
  }
  out << w.what << ": module '" << module << "' waited " << w.wait
      << " for the GIL";
  if (!ctx.frame.empty()) {
    out << ", python frame: " << ctx.frame;
  }
  return out;
}

void log_slow_wait(const char *what, ceph::timespan wait)
{
  if (g_conf()->subsys.should_gather<ceph_subsys_mgr, 20>()) {
    dout(20) << slow_wait{what, wait} << dendl;
    return;
  }
  if (!g_conf()->subsys.should_gather<ceph_subsys_mgr, 5>()) {
    return;
  }
  std::unique_lock l{slow_wait_sampler_lock};
  const auto now = ceph::mono_clock::now();
  if (now < sampler.next_report) {
    ++sampler.unlogged;
    if (wait > sampler.worst_unlogged) {
      sampler.worst_unlogged = wait;
      sampler.worst_unlogged_module = tagged_module();
    }
    return;
  }
  sampler.next_report = now + slow_wait_log_interval;
  const auto unlogged = std::exchange(sampler.unlogged, 0);
  const auto worst = std::exchange(sampler.worst_unlogged, {});
  const auto worst_module = std::exchange(sampler.worst_unlogged_module, {});
  l.unlock();
  dout(5) << slow_wait{what, wait};
  if (unlogged > 0) {
    *_dout << " (" << unlogged << " slow waits not logged since the last "
           << "report, worst " << worst << " in '" << worst_module << "')";
  }
  *_dout << dendl;
}

void record_gil_wait(const char *what, int counter, ceph::timespan wait)
{
  if (perfcounter) {
    perfcounter->tinc_with_max(counter, wait);
  }
  if (wait <= slow_wait_threshold) {
    return;
  }
  if (perfcounter) {
    perfcounter->inc(l_mgr_gil_slow_waits);
  }
  log_slow_wait(what, wait);
}

} // anonymous namespace

void gil_tag_thread_module(const std::string &name)
{
  thread_module_name = name;
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
  // Tag before acquiring, so a slow acquisition is logged under this
  // module. ~Gil() puts the previous tag back: dispatch_remote() runs
  // module B on module A's thread, which must not stay tagged as B.
  if (!module_name.empty()) {
    pPreviousModuleName = std::exchange(thread_module_name, module_name);
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
  ceph::timespan wait;
  if (new_thread) {
    pNewThreadState = PyThreadState_New(pThreadState.ts->interp);
    const auto start = ceph::mono_clock::now();
    PyEval_RestoreThread(pNewThreadState);
    wait = ceph::mono_clock::now() - start;
    dout(20) << "Switched to new thread state " << pNewThreadState << dendl;
  } else {
    // Acquire the GIL, set the current thread state
    const auto start = ceph::mono_clock::now();
    PyEval_RestoreThread(pThreadState.ts);
    wait = ceph::mono_clock::now() - start;
    ceph_assert(pthread_self() == pThreadState.thread);
  }
  dout(25) << "GIL acquired for thread state " << pThreadState.ts << " in "
           << wait << dendl;
  record_gil_wait("Gil::acquire", l_mgr_gil_acquire_lat, wait);
  assert_gil();
}

Gil::~Gil()
{
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
  if (pPreviousModuleName) {
    thread_module_name = std::move(*pPreviousModuleName);
  }
}

without_gil_t::without_gil_t()
{
  assert_gil();
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
  const auto start = ceph::mono_clock::now();
  PyEval_RestoreThread(save);
  const ceph::timespan wait = ceph::mono_clock::now() - start;
  save = nullptr;
  record_gil_wait("without_gil_t::acquire_gil", l_mgr_gil_reacquire_lat, wait);
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
