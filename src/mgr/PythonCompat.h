#pragma once

#include <Python.h>

// Python's pyconfig-64.h conflicts with ceph's acconfig.h
#undef HAVE_SYS_WAIT_H
#undef HAVE_UNISTD_H
#undef HAVE_UTIME_H
#undef _POSIX_C_SOURCE
#undef _XOPEN_SOURCE

#if PY_MAJOR_VERSION >= 3
inline PyObject* PyString_FromString(const char *v) {
  return PyUnicode_FromFormat("%s", v);
}
inline char* PyString_AsString(PyObject *string) {
  return PyUnicode_AsUTF8(string);
}
inline long PyInt_AsLong(PyObject *io) {
  return PyLong_AsLong(io);
}
inline PyObject* PyInt_FromLong(long ival) {
  return PyLong_FromLong(ival);
}
inline int PyString_Check(PyObject *o) {
  return PyUnicode_Check(o);
}
#endif
