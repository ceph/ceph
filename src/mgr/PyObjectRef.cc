// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#include "PyObjectRef.h"

#include "include/ceph_assert.h"

namespace {
void assert_gil() {
  // PyGILState_Check() is unreliable after subinterpreters have been created.
  auto* state = PyThreadState_Get();
  ceph_assert(state->thread_id == PyThread_get_thread_ident());
}
}

void PyObjectRef::Decref::operator()(PyObject* object) const noexcept {
  assert_gil();
  Py_DECREF(object);
}

PyObjectRef PyObjectRef::clone() const & noexcept {
  if (!object) {
    return {};
  }
  assert_gil();
  Py_INCREF(object.get());
  return PyObjectRef(object.get());
}

PyObjectRef PyObjectRef::adopt_new_reference(PyObject* object) noexcept {
  if (object) {
    assert_gil();
  }
  return PyObjectRef(object);
}
