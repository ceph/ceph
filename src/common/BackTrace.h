// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_BACKTRACE_H
#define CEPH_BACKTRACE_H

#include "acconfig.h"
#include <iosfwd>
#ifdef HAVE_EXECINFO_H
#include <execinfo.h>
#endif
#include <stdlib.h>

#include <list>
#include <string>

namespace ceph {

class Formatter;

struct BackTrace {
  virtual ~BackTrace() {}
  virtual void print(std::ostream& out) const = 0;
  virtual void dump(Formatter *f) const = 0;
};

inline std::ostream& operator<<(std::ostream& out, const BackTrace& bt) {
  bt.print(out);
  return out;
}


struct ClibBackTrace : public BackTrace {
  const static int max = 32;

  int skip;
  void *array[max]{};
  size_t size;
  char **strings;

  explicit ClibBackTrace(int s) {
#ifdef HAVE_EXECINFO_H
    skip = s;
    size = backtrace(array, max);
    strings = backtrace_symbols(array, size);
#else
    skip = 0;
    size = 0;
    strings = nullptr;
#endif
  }
  ~ClibBackTrace() {
    free(strings);
  }

  ClibBackTrace(const ClibBackTrace& other);
  const ClibBackTrace& operator=(const ClibBackTrace& other);

  void print(std::ostream& out) const override;
  void dump(Formatter *f) const override;

  static std::string demangle(const char* name);
};


struct PyBackTrace : public BackTrace {
  std::list<std::string> strings;

  explicit PyBackTrace(std::list<std::string>& s) : strings(s) {}

  void dump(Formatter *f) const override;
  void print(std::ostream& out) const override;
};


}

#endif
