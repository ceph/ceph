
#include "include/assert.h"
#include "config.h"

#include "BackTrace.h"

#include "common/tls.h"

void __ceph_assert_fail(const char *assertion, const char *file, int line, const char *func)
{
  BackTrace bt(1);

  _dout_lock.TryLock();
  *_dout << file << ": In function '" << func << "':" << std::endl;
  *_dout << file << ":" << line << ": FAILED assert(" << assertion << ")" << std::endl;
  bt.print(*_dout);
  *_dout << " NOTE: a copy of the executable, or `objdump -rdS <executable>` is needed to interpret this." << std::endl;

  _dout->flush();

  cerr << file << ": In function '" << func << "':" << std::endl;
  cerr << file << ":" << line << ": FAILED assert(" << assertion << ")" << std::endl;
  bt.print(cerr);
  cerr << " NOTE: a copy of the executable, or `objdump -rdS <executable>` is needed to interpret this." << std::endl;
  cerr.flush();

  char *p = 0;
  while (1)
    *p-- = 0;  // make myself core dump.
}
