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

#pragma once

struct _ts;
typedef struct _ts PyThreadState;

#include <pthread.h>


/**
 * Wrap PyThreadState to carry a record of which POSIX thread
 * the thread state relates to.  This allows the Gil class to
 * validate that we're being used from the right thread.
 */
class SafeThreadState
{
  public:
  SafeThreadState(PyThreadState *ts_);

  SafeThreadState()
    : ts(nullptr), thread(0)
  {
  }

  PyThreadState *ts;
  pthread_t thread;

  void set(PyThreadState *ts_)
  {
    ts = ts_;
    thread = pthread_self();
  }
};

//
// Use one of these in any scope in which you need to hold Python's
// Global Interpreter Lock.
//
// Do *not* nest these, as a second GIL acquire will deadlock (see
// https://docs.python.org/2/c-api/init.html#c.PyEval_RestoreThread)
//
// If in doubt, explicitly put a scope around the block of code you
// know you need the GIL in.
//
// See the comment in Gil::Gil for when to set new_thread == true
//
class Gil {
public:
  Gil(const Gil&) = delete;
  Gil& operator=(const Gil&) = delete;

  Gil(SafeThreadState &ts, bool new_thread = false);
  ~Gil();

private:
  SafeThreadState &pThreadState;
  PyThreadState *pNewThreadState = nullptr;
};

