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

#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

#include "Gil.h"

SafeThreadState::SafeThreadState(PyThreadState *ts_)
    : ts(ts_)
{
  assert(ts != nullptr);
  thread = pthread_self();
}

Gil::Gil(SafeThreadState &ts, bool new_thread) : pThreadState(ts)
{
  // Acquire the GIL, set the current thread state
  PyEval_RestoreThread(pThreadState.ts);
  dout(25) << "GIL acquired for thread state " << pThreadState.ts << dendl;

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
    assert(pthread_self() == pThreadState.thread);
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
  // Release the GIL, reset the thread state to NULL
  PyEval_SaveThread();
  dout(25) << "GIL released for thread state " << pThreadState.ts << dendl;
}

