// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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


#include "PyFormatter.h"
#include <fstream>
#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << __func__ << " "


#define LARGE_SIZE 1024


void PyFormatter::open_array_section(std::string_view name)
{
  PyObject *list = PyList_New(0);
  dump_pyobject(name, list);
  stack.push(cursor);
  cursor = list;
}

void PyFormatter::open_object_section(std::string_view name)
{
  PyObject *dict = PyDict_New();
  dump_pyobject(name, dict);
  stack.push(cursor);
  cursor = dict;
}

void PyFormatter::dump_null(std::string_view name)
{
  dump_pyobject(name, Py_None);
}

void PyFormatter::dump_unsigned(std::string_view name, uint64_t u)
{
  PyObject *p = PyLong_FromUnsignedLong(u);
  ceph_assert(p);
  dump_pyobject(name, p);
}

void PyFormatter::dump_int(std::string_view name, int64_t u)
{
  PyObject *p = PyLong_FromLongLong(u);
  ceph_assert(p);
  dump_pyobject(name, p);
}

void PyFormatter::dump_float(std::string_view name, double d)
{
  dump_pyobject(name, PyFloat_FromDouble(d));
}

void PyFormatter::dump_string(std::string_view name, std::string_view s)
{
  dump_pyobject(name, PyUnicode_FromString(s.data()));
}

void PyFormatter::dump_bool(std::string_view name, bool b)
{
  if (b) {
    Py_INCREF(Py_True);
    dump_pyobject(name, Py_True);
  } else {
    Py_INCREF(Py_False);
    dump_pyobject(name, Py_False);
  }
}

std::ostream& PyFormatter::dump_stream(std::string_view name)
{
  // Give the caller an ostream, construct a PyString,
  // and remember the association between the two.  On flush,
  // we'll read from the ostream into the PyString
  auto ps = std::make_shared<PendingStream>();
  ps->cursor = cursor;
  ps->name = name;

  pending_streams.push_back(ps);

  return ps->stream;
}

void PyFormatter::dump_format_va(std::string_view name, const char *ns, bool quoted, const char *fmt, va_list ap)
{
  char buf[LARGE_SIZE];
  vsnprintf(buf, LARGE_SIZE, fmt, ap);

  dump_pyobject(name, PyUnicode_FromString(buf));
}

/**
 * Steals reference to `p`
 */
void PyFormatter::dump_pyobject(std::string_view name, PyObject *p)
{
  if (PyList_Check(cursor)) {
    PyList_Append(cursor, p);
    Py_DECREF(p);
  } else if (PyDict_Check(cursor)) {
    PyObject *key = PyUnicode_DecodeUTF8(name.data(), name.size(), nullptr);
    PyDict_SetItem(cursor, key, p);
    Py_DECREF(key);
    Py_DECREF(p);
  } else {
    ceph_abort();
  }
}

void PyFormatter::finish_pending_streams()
{
  for (const auto &i : pending_streams) {
    PyObject *tmp_cur = cursor;
    cursor = i->cursor;
    dump_pyobject(
        i->name.c_str(),
        PyUnicode_FromString(i->stream.str().c_str()));
    cursor = tmp_cur;
  }

  pending_streams.clear();
}
