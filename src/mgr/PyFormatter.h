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

#include "common/JSONFormatter.h"
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
  void dump_pyobject(std::string_view name, PyObject *p);
private:
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
  PyObject* get() override {
    finish_pending_streams();
    if (!is_converted_to_readonly) {
      convert_to_readonly();
    }
    Py_INCREF(root);
    return root;
  }

  void reset() override {
    PyFormatter::reset();
    is_converted_to_readonly = false;
  }
private:
  bool is_converted_to_readonly = false;

  /// Convert entire data structure to read-only once
  void convert_to_readonly() {
    PyObject* readonly_root = make_immutable(root);

    if (readonly_root != root) {
      Py_DECREF(root);
      root = readonly_root;
      cursor = root;
    }
    is_converted_to_readonly = true;
  }

  /// Recursively convert object to immutable equivalent
  PyObject* make_immutable(PyObject* obj) {
    if (PyList_Check(obj)) {
      return convert_list_to_tuple(obj);
    }
    if (PyDict_Check(obj)) {
      return convert_dict_to_proxy(obj);
    }
    if (PySet_Check(obj)) {
      return convert_set_to_frozenset(obj);
    }
    if (PyTuple_Check(obj)) {
      return convert_tuple_contents(obj);
    }
    // Already immutable
    Py_INCREF(obj);
    return obj;
  }

  PyObject* convert_list_to_tuple(PyObject* list) {
    Py_ssize_t size = PyList_Size(list);
    PyObject* tuple = PyTuple_New(size);

    for (Py_ssize_t i = 0; i < size; i++) {
      PyObject* item = PyList_GetItem(list, i);
      PyObject* immutable_item = make_immutable(item);
      if (!immutable_item) {
        Py_DECREF(tuple);
        Py_INCREF(list);
        return list;
      }
      PyTuple_SET_ITEM(tuple, i, immutable_item);
    }
    return tuple;
  }

  PyObject* convert_dict_to_proxy(PyObject* dict) {
    PyObject* immutable_dict = PyDict_New();
    PyObject* key, *value;
    Py_ssize_t pos = 0;

    while (PyDict_Next(dict, &pos, &key, &value)) {
      PyObject* immutable_value = make_immutable(value);
      if (!immutable_value) {
        Py_DECREF(immutable_dict);
        Py_INCREF(dict);
        return dict;
      }
      
      if (PyDict_SetItem(immutable_dict, key, immutable_value) < 0) {
        Py_DECREF(immutable_value);
        Py_DECREF(immutable_dict);
        Py_INCREF(dict);
        return dict;
      }
      Py_DECREF(immutable_value);
    }

    PyObject* types_module = PyImport_ImportModule("types");
    if (types_module) {
      PyObject* mapping_proxy_type = PyObject_GetAttrString(types_module, "MappingProxyType");
      if (mapping_proxy_type) {
        PyObject* proxy = PyObject_CallFunctionObjArgs(mapping_proxy_type, immutable_dict, nullptr);
        Py_DECREF(mapping_proxy_type);
        Py_DECREF(types_module);
        Py_DECREF(immutable_dict);
        
        if (proxy) {
          return proxy;
        }
      }
      Py_DECREF(types_module);
    }
    return immutable_dict;
  }

  PyObject* convert_set_to_frozenset(PyObject* set) {
    PyObject* frozenset = PyFrozenSet_New(set);
    if (frozenset) {
      return frozenset;
    }

    // Fallback
    Py_INCREF(set);
    return set;
  }

  PyObject* convert_tuple_contents(PyObject* tuple) {
    Py_ssize_t size = PyTuple_Size(tuple);
    PyObject* new_tuple = PyTuple_New(size);
    if (!new_tuple) {
      Py_INCREF(tuple);
      return tuple;
    }

    for (Py_ssize_t i = 0; i < size; i++) {
      PyObject* item = PyTuple_GetItem(tuple, i);
      PyObject* immutable_item = make_immutable(item);

      if (!immutable_item) {
        Py_DECREF(new_tuple);
        Py_INCREF(tuple);
        return tuple;
      }
      PyTuple_SET_ITEM(new_tuple, i, immutable_item);
    }
    return new_tuple;
  }
};

#endif

