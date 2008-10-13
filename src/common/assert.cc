
#include "include/assert.h"
#include "config.h"

#include <execinfo.h>

void print_trace (ostream& out)
{
  int max = 100;
  void *array[max];
  size_t size;
  char **strings;
  size_t i;
  
  size = backtrace(array, max);
  strings = backtrace_symbols(array, size);

  int skip = 2;  // skip print_trace and assert
  out << " --- BACKTRACE " << (size-skip) << " frames ---" << std::endl;

  for (i = skip; i < size; i++)
    out << " " << (i-skip+1) << ": " << strings[i] << std::endl;
  out << " NOTE: a copy of the executable, or `objdump -rdS <executable>` is needed to interpret this." << std::endl;
  free(strings);
}

void __ceph_assert_fail(const char *assertion, const char *file, int line, const char *func)
{
  _dout_lock.TryLock();
  *_dout << file << ":" << line << ": FAILED assert in \'" << func << "\': " << assertion << std::endl;
  print_trace(*_dout);
  _dout->flush();

  cerr   << file << ":" << line << ": FAILED assert in \'" << func << "\': " << assertion << std::endl;
  print_trace(cerr);
  cerr.flush();


  char *p = 0;
  while (1)
    *p-- = 0;  // make myself core dump.
}
