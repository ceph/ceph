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

  Gil(PyThreadState *ts, bool new_thread = false);
  ~Gil();

private:
  PyThreadState *pThreadState;
  PyThreadState *pNewThreadState = nullptr;
};

