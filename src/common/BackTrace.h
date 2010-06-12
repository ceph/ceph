#ifndef CEPH_BACKTRACE_H
#define CEPH_BACKTRACE_H

#include <execinfo.h>
#include <stdlib.h>

namespace ceph {

struct BackTrace {
  const static int max = 100;

  int skip;
  void *array[max];
  size_t size;
  char **strings;

  BackTrace(int s) : skip(s) {
    size = backtrace(array, max);
    strings = backtrace_symbols(array, size);
  }
  ~BackTrace() {
    free(strings);
  }

  void print(std::ostream& out);
};

}

#endif
