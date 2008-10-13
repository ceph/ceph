
#include "include/assert.h"
#include "config.h"

void __ceph_assert_fail(const char *assertion, const char *file, int line, const char *func)
{
  _dout_lock.TryLock();
  *_dout << file << ":" << line << ": FAILED assert in \'" << func << "\': " << assertion << std::endl;
  cerr   << file << ":" << line << ": FAILED assert in \'" << func << "\': " << assertion << std::endl;
  _dout->flush();
  cerr.flush();

  char *p = 0;
  while (1)
    *p-- = 0;  // make myself core dump.
}
