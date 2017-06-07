#include <unistd.h>

namespace ceph {

  // page size crap, see page.h
  int _get_bits_of(int v) {
    int n = 0;
    while (v) {
      n++;
      v = v >> 1;
    }
    return n;
  }

  unsigned _page_size = sysconf(_SC_PAGESIZE);
  unsigned long _page_mask = ~(unsigned long)(_page_size - 1);
  unsigned _page_shift = _get_bits_of(_page_size - 1);

}
