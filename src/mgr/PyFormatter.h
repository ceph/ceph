// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat Inc
 *
 * Author: John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef PY_FORMATTER_H_
#define PY_FORMATTER_H_

// Python.h comes first because otherwise it clobbers ceph's assert
#include <Python.h>

#include <stack>
#include <string>
#include <string_view>
#include <sstream>
#include <memory>
#include <list>

#include "common/Formatter.h"
#include "include/ceph_assert.h"

class PyFormatter : public ceph::Formatter
{
public:
  PyFormatter (const PyFormatter&) = delete;
  PyFormatter& operator= (const PyFormatter&) = delete;
  PyFormatter(bool pretty = false, bool array = false)
  {
    // It is forbidden to instantiate me outside of the GIL,
    // because I construct python objects right away

    // Initialise cursor to an empty dict
    if (!array) {
      root = cursor = PyDict_New();
    } else {
      root = cursor = PyList_New(0);
    }
  }

  ~PyFormatter() override
  {
    cursor = NULL;
    Py_DECREF(root);
    root = NULL;
  }

  // Obscure, don't care.
  void open_array_section_in_ns(std::string_view name, const char *ns) override
  {ceph_abort();}
  void open_object_section_in_ns(std::string_view name, const char *ns) override
  {ceph_abort();}

  void reset() override
  {
    const bool array = PyList_Check(root);
    Py_DECREF(root);
    if (array) {
      root = cursor = PyList_New(0);
    } else {
      root = cursor = PyDict_New();
    }
  }

  void set_status(int status, const char* status_name) override {}
  void output_header() override {};
  void output_footer() override {};
  void enable_line_break() override {};

  void open_array_section(std::string_view name) override;
  void open_object_section(std::string_view name) override;
  void close_section() override
  {
    ceph_assert(cursor != root);
    ceph_assert(!stack.empty());
    cursor = stack.top();
    stack.pop();
  }
  void dump_bool(std::string_view name, bool b) override;
  void dump_null(std::string_view name) override;
  void dump_unsigned(std::string_view name, uint64_t u) override;
  void dump_int(std::string_view name, int64_t u) override;
  void dump_float(std::string_view name, double d) override;
  void dump_string(std::string_view name, std::string_view s) override;
  std::ostream& dump_stream(std::string_view name) override;
  void dump_format_va(std::string_view name, const char *ns, bool quoted, const char *fmt, va_list ap) override;

  void flush(std::ostream& os) override
  {
      // This class is not a serializer: this doesn't make sense
      ceph_abort();
  }

  int get_len() const override
  {
      // This class is not a serializer: this doesn't make sense
      ceph_abort();
      return 0;
  }

  void write_raw_data(const char *data) override
  {
      // This class is not a serializer: this doesn't make sense
      ceph_abort();
  }

  virtual PyObject *get()
  {
    finish_pending_streams();

    Py_INCREF(root);
    return root;
  }

  void finish_pending_streams();

protected:
  PyObject *root;
  PyObject *cursor;
  std::stack<PyObject *> stack;
private:
  virtual void dump_pyobject(std::string_view name, PyObject *p);

  class PendingStream {
    public:
    PyObject *cursor;
    std::string name;
    std::stringstream stream;
  };

  std::list<std::shared_ptr<PendingStream> > pending_streams;

};

class PyFormatterRO : public PyFormatter {
public:
  using PyFormatter::PyFormatter;

  /// Insert a leaf or complex object under 'name', freezing it immediately
  void dump_pyobject(std::string_view name, PyObject* p) override {
    PyObject* frozen = to_readonly(p);
    if (PyDict_Check(cursor)) {
      PyObject* key = PyUnicode_DecodeUTF8(name.data(), name.size(), nullptr);
      PyDict_SetItem(cursor, key, frozen);
      Py_DECREF(key);
    } else if (PyList_Check(cursor)) {
      PyList_Append(cursor, frozen);
    } else {
      ceph_abort();
    }
    Py_DECREF(frozen);
  }

  /// Open a new object section for subsequent dump calls
  void open_object_section(std::string_view name) override {
    section_names_.push(std::string(name));
    stack.push(cursor);
    cursor = PyDict_New();
  }

  /// Open a new array section for subsequent dump calls
  void open_array_section(std::string_view name) override {
    section_names_.push(std::string(name));
    stack.push(cursor);
    cursor = PyList_New(0);
  }

  /// Close the current section, freezing it and inserting into parent
  void close_section() override;

  /// Return the immutable root directly (no additional work)
  PyObject* get() override {
    finish_pending_streams();
    PyObject* frozen_root = to_readonly(root);
    Py_INCREF(frozen_root);
    return frozen_root;
  }

private:
  /// Convert any object to its read-only form
  PyObject* to_readonly(PyObject* obj) {
    if      (PyDict_Check(obj))      return proxy_dict(obj);
    else if (PyList_Check(obj))      return to_tuple(obj);
    else if (PyTuple_Check(obj))     return readonly_primitive(obj);
    else if (PySet_Check(obj))       return to_frozenset(obj);
    else                              return readonly_primitive(obj);
  }

  /// Primitives and tuples: return as-is (with new reference)
  PyObject* readonly_primitive(PyObject* obj) {
    Py_INCREF(obj);
    return obj;
  }

  /// Wrap a dict in a mapping proxy to make it read-only
  PyObject* proxy_dict(PyObject* dict) {
    return PyDictProxy_New(dict);
  }

  /// Convert list to tuple
  PyObject* to_tuple(PyObject* list) {
    return PySequence_Tuple(list);
  }

  /// Convert set to frozenset
  PyObject* to_frozenset(PyObject* set) {
    return PyFrozenSet_New(set);
  }
  std::stack<std::string> section_names_;
};

#endif

