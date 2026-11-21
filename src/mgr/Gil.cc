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


// For the GIL holder, which CPython only keeps in its internal state.
#define Py_BUILD_CORE 1
#include "Python.h"
#include <frameobject.h>
#include "internal/pycore_runtime.h"
#include "mgr/mgr_perf_counters.h"
#include "common/perf_counters.h"

#include "common/ceph_time.h"
#include "common/debug.h"
#include "global/global_context.h"

#include <fmt/format.h>

#include <sstream>
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

// The thread state a tagged Gil last released the GIL from, and its
// module: Gil deletes a new thread state right after releasing, so a
// waiter can't look it up anymore. Written by ~Gil before releasing and
// read by a waiter after acquiring, so the GIL serializes them.
struct {
  PyThreadState *ts = nullptr;
  std::string module;
} last_tagged_release;

// CPython's default sys.getswitchinterval(): how long a waiter lets the
// holder run before asking it to drop the GIL.
constexpr auto slow_wait_threshold = 5ms;
constexpr auto slow_wait_log_interval = 1s;

// At debug_mgr 5, at most one slow wait is logged per interval; the rest
// are summed up in the next report. Only used with the GIL held.
struct {
  ceph::mono_time next_report;
  uint64_t unlogged = 0;
  ceph::timespan worst_unlogged{};
  std::string worst_unlogged_module;
} slow_wait_rate_limit;

// Read once: called with the GIL held, and get_val() takes a lock.
std::string_view module_root()
{
  static const std::string root =
    g_conf().get_val<std::string>("mgr_module_path");
  return root;
}

// "balancer" for <root>/balancer/module.py, empty for files outside a
// module package (mgr_module.py, stdlib, <root>2/...).
std::string_view module_package(std::string_view path, std::string_view root)
{
  if (root.ends_with('/')) {
    root.remove_suffix(1);
  }
  if (root.empty() || !path.starts_with(root) ||
      !path.substr(root.size()).starts_with('/')) {
    return {};
  }
  path.remove_prefix(root.size() + 1);
  const auto slash = path.find('/');
  return slash == path.npos ? std::string_view{} : path.substr(0, slash);
}

struct python_context {
  std::string module;
  std::string frame; // empty when the thread has no Python frames
};

// From the Python stack starting at frame (borrowed): the package of the
// outermost frame under mgr_module_path, which is what a thread a module
// started itself runs (inner frames may be shared packages such as
// orchestrator), and the innermost frame inside a module, so a wait in a
// MgrModule helper such as get() names the module code that called it.
// Needs the GIL.
python_context get_python_context(PyFrameObject *frame)
{
  python_context ctx;
  if (PyErr_Occurred()) {
    ctx.frame = "?";
    return ctx;
  }
  const auto root = module_root();
  bool have_module_frame = false;
  Py_XINCREF(frame);
  while (frame != nullptr) {
    PyCodeObject *code = PyFrame_GetCode(frame);
    const char *path = PyUnicode_AsUTF8(code->co_filename);
    if (path == nullptr) {
      PyErr_Clear();
      path = "?";
    }
    const auto package = module_package(path, root);
    if (!package.empty()) {
      ctx.module = package;
    }
    if (ctx.frame.empty() || (!have_module_frame && !package.empty())) {
      const char *name = PyUnicode_AsUTF8(code->co_name);
      if (name == nullptr) {
        PyErr_Clear();
        name = "?";
      }
      ctx.frame = fmt::format("{} ({}:{})", name, path,
                              PyFrame_GetLineNumber(frame));
      have_module_frame = !package.empty();
    }
    Py_DECREF(code);
    PyFrameObject *back = PyFrame_GetBack(frame);
    Py_DECREF(frame);
    frame = back;
  }
  return ctx;
}

// The thread state holding the GIL, or nullptr if it's free. CPython sets
// last_holder on every take, including its own thread switches. Needs no
// GIL; never dereference the result, the thread can exit at any time.
PyThreadState *gil_holder()
{
  auto *gil = PyInterpreterState_Main()->ceval.gil;
  if (__atomic_load_n(reinterpret_cast<int *>(&gil->locked),
                      __ATOMIC_RELAXED) != 1) {
    return nullptr;
  }
  return static_cast<PyThreadState *>(__atomic_load_n(
    reinterpret_cast<void **>(&gil->last_holder), __ATOMIC_RELAXED));
}

// With the GIL held: a new reference to ts's current frame, or nullptr if
// ts has exited or has no Python frames. Thread states are only deleted
// with the GIL held (see ~Gil; CPython does the same when a thread
// exits), so none is freed under us. One may be added without the GIL
// (PyThreadState_New in Gil::acquire); missing it is fine, a new thread
// state can't be the holder we're looking for.
PyFrameObject *live_thread_frame(PyThreadState *ts)
{
  for (auto *i = PyInterpreterState_Head(); i; i = PyInterpreterState_Next(i)) {
    for (auto *t = PyInterpreterState_ThreadHead(i); t;
         t = PyThreadState_Next(t)) {
      if (t == ts) {
        return PyThreadState_GetFrame(t);
      }
    }
  }
  return nullptr;
}

// The log line for a slow wait. Walks Python stacks, so only call it for
// a line that is logged.
std::string describe_slow_wait(const char *what, ceph::timespan wait,
                               PyThreadState *holder)
{
  const auto ctx = get_python_context(PyEval_GetFrame());
  std::string_view module = thread_module_name;
  if (module.empty()) {
    module = ctx.module;
  }
  std::ostringstream out;
  out << what << ": module '" << (module.empty() ? "(unknown)" : module)
      << "' waited " << wait << " for the GIL";
  if (!ctx.frame.empty()) {
    out << ", python frame: " << ctx.frame;
  }
  if (holder != nullptr && holder != PyThreadState_Get()) {
    PyFrameObject *frame = live_thread_frame(holder);
    const auto held = get_python_context(frame);
    Py_XDECREF(frame);
    std::string_view held_module = held.module;
    if (held_module.empty() && holder == last_tagged_release.ts) {
      held_module = last_tagged_release.module;
    }
    out << ", held by module '"
        << (held_module.empty() ? "(unknown)" : held_module) << "'";
    if (!held.frame.empty()) {
      out << " at " << held.frame;
    }
  }
  return out.str();
}

// Called with the GIL held, right after a slow acquire.
void log_slow_wait(const char *what, ceph::timespan wait,
                   PyThreadState *holder)
{
  if (g_conf()->subsys.should_gather<ceph_subsys_mgr, 20>()) {
    dout(20) << describe_slow_wait(what, wait, holder) << dendl;
    return;
  }
  if (!g_conf()->subsys.should_gather<ceph_subsys_mgr, 5>()) {
    return;
  }
  auto &limit = slow_wait_rate_limit;
  const auto now = ceph::mono_clock::now();
  if (now < limit.next_report) {
    ++limit.unlogged;
    if (wait > limit.worst_unlogged) {
      limit.worst_unlogged = wait;
      limit.worst_unlogged_module =
        thread_module_name.empty() ? "(unknown)" : thread_module_name;
    }
    return;
  }
  limit.next_report = now + slow_wait_log_interval;
  dout(5) << describe_slow_wait(what, wait, holder);
  if (limit.unlogged > 0) {
    *_dout << " (" << limit.unlogged << " slow waits not logged since the "
           << "last report, worst " << limit.worst_unlogged << " in '"
           << limit.worst_unlogged_module << "')";
  }
  *_dout << dendl;
  limit.unlogged = 0;
  limit.worst_unlogged = {};
}

void record_gil_wait(const char *what, int counter, ceph::timespan wait,
                     PyThreadState *holder)
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
  log_slow_wait(what, wait, holder);
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
  PyThreadState *holder;
  if (new_thread) {
    pNewThreadState = PyThreadState_New(pThreadState.ts->interp);
    holder = gil_holder();
    const auto start = ceph::mono_clock::now();
    PyEval_RestoreThread(pNewThreadState);
    wait = ceph::mono_clock::now() - start;
    dout(20) << "Switched to new thread state " << pNewThreadState << dendl;
  } else {
    // Acquire the GIL, set the current thread state
    holder = gil_holder();
    const auto start = ceph::mono_clock::now();
    PyEval_RestoreThread(pThreadState.ts);
    wait = ceph::mono_clock::now() - start;
    ceph_assert(pthread_self() == pThreadState.thread);
  }
  dout(25) << "GIL acquired for thread state "
           << (new_thread ? pNewThreadState : pThreadState.ts) << " in "
           << wait << dendl;
  record_gil_wait("Gil::acquire", l_mgr_gil_acquire_lat, wait, holder);
  assert_gil();
}

Gil::~Gil()
{
  if (!thread_module_name.empty()) {
    last_tagged_release.ts = pNewThreadState ? pNewThreadState
                                             : pThreadState.ts;
    last_tagged_release.module = thread_module_name;
  }
  // Release the GIL, reset the thread state to NULL
  if (pNewThreadState != nullptr) {
    dout(20) << "Destroying new thread state " << pNewThreadState << dendl;
    PyThreadState_Clear(pNewThreadState);
    // Deletes it with the GIL held, then releases the GIL.
    PyThreadState_DeleteCurrent();
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
  PyThreadState *holder = gil_holder();
  const auto start = ceph::mono_clock::now();
  PyEval_RestoreThread(save);
  const ceph::timespan wait = ceph::mono_clock::now() - start;
  save = nullptr;
  record_gil_wait("without_gil_t::acquire_gil", l_mgr_gil_reacquire_lat, wait,
                  holder);
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
