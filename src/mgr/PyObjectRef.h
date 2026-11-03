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
#pragma once

#include <Python.h>
#include <memory>

// Each handle owns one Python reference. Adoption, cloning and destruction
// require the GIL; moving a handle transfers ownership without changing refcounts.
// Raw pointers enter only through the explicit C API adoption boundary.
class [[nodiscard]] PyObjectRef {
  struct Decref {
    void operator()(PyObject* object) const noexcept;
  };
  std::unique_ptr<PyObject, Decref> object;
  explicit PyObjectRef(PyObject* object) noexcept : object(object) {}

public:
  PyObjectRef() noexcept = default;
  PyObjectRef(const PyObjectRef&) = delete;
  PyObjectRef& operator=(const PyObjectRef&) = delete;
  PyObjectRef(PyObjectRef&&) noexcept = default;
  PyObjectRef& operator=(PyObjectRef&&) noexcept = default;

  // Consume a new reference returned by the C API, including nullptr on error.
  // No INCREF occurs here: passing a non-owning pointer violates this contract.
  static PyObjectRef adopt_new_reference(PyObject* object) noexcept;
  explicit operator bool() const noexcept { return bool(object); }
  PyObject* get() const & noexcept { return object.get(); }
  PyObject* get() const && = delete;
  PyObjectRef clone() const & noexcept;
  PyObjectRef clone() const && = delete;
  void reset() noexcept { object.reset(); }
  [[nodiscard]] PyObject* into_python() && noexcept { return object.release(); }
};
