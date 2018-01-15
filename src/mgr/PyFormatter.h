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
#include "PythonCompat.h"

#include <stack>
#include <memory>
#include <list>

#include "common/Formatter.h"
#include "include/assert.h"

class PyFormatter : public ceph::Formatter
{
public:
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
  void open_array_section_in_ns(const char *name, const char *ns) override
  {ceph_abort();}
  void open_object_section_in_ns(const char *name, const char *ns) override
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

  void open_array_section(const char *name) override;
  void open_object_section(const char *name) override;
  void close_section() override
  {
    assert(cursor != root);
    assert(!stack.empty());
    cursor = stack.top();
    stack.pop();
  }
  void dump_bool(const char *name, bool b) override;
  void dump_unsigned(const char *name, uint64_t u) override;
  void dump_int(const char *name, int64_t u) override;
  void dump_float(const char *name, double d) override;
  void dump_string(const char *name, std::string_view s) override;
  std::ostream& dump_stream(const char *name) override;
  void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap) override;

  void flush(std::ostream& os) override
  {
      // This class is not a serializer: this doens't make sense
      ceph_abort();
  }

  int get_len() const override
  {
      // This class is not a serializer: this doens't make sense
      ceph_abort();
      return 0;
  }

  void write_raw_data(const char *data) override
  {
      // This class is not a serializer: this doens't make sense
      ceph_abort();
  }

  PyObject *get()
  {
    finish_pending_streams();

    Py_INCREF(root);
    return root;
  }

  void finish_pending_streams();

private:
  PyObject *root;
  PyObject *cursor;
  std::stack<PyObject *> stack;

  void dump_pyobject(const char *name, PyObject *p);

  class PendingStream {
    public:
    PyObject *cursor;
    std::string name;
    std::stringstream stream;
  };

  std::list<std::shared_ptr<PendingStream> > pending_streams;

};

#endif

