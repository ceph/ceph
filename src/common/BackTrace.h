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
  const static int max = 32;

  int skip;
  void *array[max]{};
  size_t size;
  char **strings;

  std::list<std::string> src_strings;

  explicit BackTrace(std::list<std::string>& s)
    : skip(0),
      size(s.size()) {
    src_strings = s;
    strings = (char **)malloc(sizeof(*strings) * src_strings.size());
    unsigned i = 0;
    for (auto& s : src_strings) {
      strings[i++] = (char *)s.c_str();
    }
  }
  explicit BackTrace(int s) : skip(s) {
#ifdef HAVE_EXECINFO_H
    size = backtrace(array, max);
    strings = backtrace_symbols(array, size);
#else
    skip = 0;
    size = 0;
    strings = nullptr;
#endif
  }
  ~BackTrace() {
    free(strings);
  }

  BackTrace(const BackTrace& other);
  const BackTrace& operator=(const BackTrace& other);

  void print(std::ostream& out) const;
  void dump(Formatter *f) const;
  static std::string demangle(const char* name);
};

inline std::ostream& operator<<(std::ostream& out, const BackTrace& bt) {
  bt.print(out);
  return out;
}

}

#endif
