// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <Python.h>

#include "PyUtil.h"

PyObject *get_python_typed_option_value(
  Option::type_t type,
  const std::string& value)
{
  switch (type) {
  case Option::TYPE_INT:
  case Option::TYPE_UINT:
  case Option::TYPE_SIZE:
    return PyLong_FromString((char *)value.c_str(), nullptr, 0);
  case Option::TYPE_SECS:
  case Option::TYPE_FLOAT:
    {
      PyObject *s = PyUnicode_FromString(value.c_str());
      PyObject *f = PyFloat_FromString(s);
      Py_DECREF(s);
      return f;
    }
  case Option::TYPE_BOOL:
    if (value == "1" || value == "true" || value == "True" ||
	value == "on" || value == "yes") {
      Py_INCREF(Py_True);
      return Py_True;
    } else {
      Py_INCREF(Py_False);
      return Py_False;
    }
  case Option::TYPE_STR:
  case Option::TYPE_ADDR:
  case Option::TYPE_ADDRVEC:
  case Option::TYPE_UUID:
    break;
  }
  return PyUnicode_FromString(value.c_str());
}
