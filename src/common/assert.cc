
#include "include/assert.h"
#include "config.h"

#include "BackTrace.h"

namespace ceph {

void __ceph_assert_fail(const char *assertion, const char *file, int line, const char *func)
{
  BackTrace *bt = new BackTrace(1);

  _dout_lock.Lock();
  *_dout << file << ": In function '" << func << "', "
	 << "In thread " << hex << pthread_self() << dec << std::endl;
  *_dout << file << ":" << line << ": FAILED assert(" << assertion << ")" << std::endl;
  bt->print(*_dout);
  *_dout << " NOTE: a copy of the executable, or `objdump -rdS <executable>` is needed to interpret this." << std::endl;

  _dout->flush();

  if (1) {
    throw FailedAssertion(bt);
  } else {
    // make myself core dump.
    char *p = 0;
    while (1)
      *p-- = 0;
  }
}

void __ceph_assert_warn(const char *assertion, const char *file,
			int line, const char *func)
{
  derr << "WARNING: assert(" << assertion << ") at: " << file << ":" << line << ": " << func << "()" << dendl;
}

}
