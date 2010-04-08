
#include "include/assert.h"
#include "config.h"

#include "BackTrace.h"

#include "common/tls.h"

namespace ceph {

void __ceph_assert_fail(const char *assertion, const char *file, int line, const char *func)
{
  BackTrace *bt = new BackTrace(2);

  _dout_lock.TryLock();
  *_dout << file << ": In function '" << func << "':" << std::endl;
  *_dout << file << ":" << line << ": FAILED assert(" << assertion << ")" << std::endl;
  bt->print(*_dout);
  *_dout << " NOTE: a copy of the executable, or `objdump -rdS <executable>` is needed to interpret this." << std::endl;

  _dout->flush();

  cerr << file << ": In function '" << func << "':" << std::endl;
  cerr << file << ":" << line << ": FAILED assert(" << assertion << ")" << std::endl;
  bt->print(cerr);
  cerr << " NOTE: a copy of the executable, or `objdump -rdS <executable>` is needed to interpret this." << std::endl;
  cerr.flush();

  if (1) {
    throw new FailedAssertion(bt);
  } else {
    // make myself core dump.
    char *p = 0;
    while (1)
      *p-- = 0;
  }
}

void __ceph_assert_warn(const char *assertion, const char *file, int line, const char *func)
{
  *_dout << "WARNING: assert(" << assertion << ") at: " << file << ":" << line << ": " << func << "()" << std::endl;
}

}
