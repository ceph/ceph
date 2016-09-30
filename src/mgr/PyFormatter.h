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
#include "Python.h"
// Python's pyconfig-64.h conflicts with ceph's acconfig.h
#undef HAVE_SYS_WAIT_H
#undef HAVE_UNISTD_H
#undef HAVE_UTIME_H
#undef _POSIX_C_SOURCE
#undef _XOPEN_SOURCE

#include <stack>
#include <memory>
#include <list>

#include "common/Formatter.h"

class PyFormatter : public ceph::Formatter
{
public:
  PyFormatter(bool pretty = false, bool array = false)
  {
    // Initialise cursor to an empty dict
    if (!array) {
      root = cursor = PyDict_New();
    } else {
      root = cursor = PyList_New(0);
    }
  }

  ~PyFormatter()
  {
    cursor = NULL;
    Py_DECREF(root);
    root = NULL;
  }

  // Obscure, don't care.
  void open_array_section_in_ns(const char *name, const char *ns)
  {assert(0);}
  void open_object_section_in_ns(const char *name, const char *ns)
  {assert(0);}

  void reset()
  {
    const bool array = PyList_Check(root);
    Py_DECREF(root);
    if (array) {
      root = cursor = PyList_New(0);
    } else {
      root = cursor = PyDict_New();
    }
  }

  virtual void set_status(int status, const char* status_name) {}
  virtual void output_header() {};
  virtual void output_footer() {};


  virtual void open_array_section(const char *name);
  void open_object_section(const char *name);
  void close_section()
  {
    assert(cursor != root);
    assert(!stack.empty());
    cursor = stack.top();
    stack.pop();
  }
  void dump_bool(const char *name, bool b);
  void dump_unsigned(const char *name, uint64_t u);
  void dump_int(const char *name, int64_t u);
  void dump_float(const char *name, double d);
  void dump_string(const char *name, const std::string& s);
  std::ostream& dump_stream(const char *name);
  void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap);

  void flush(std::ostream& os)
  {
      // This class is not a serializer: this doens't make sense
      assert(0);
  }

  int get_len() const
  {
      // This class is not a serializer: this doens't make sense
      assert(0);
      return 0;
  }

  void write_raw_data(const char *data)
  {
      // This class is not a serializer: this doens't make sense
      assert(0);
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

