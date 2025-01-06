#include <unistd.h>

#ifdef _WIN32
#include <windows.h>
#endif

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

  #ifdef _WIN32
  unsigned _get_page_size() {
    SYSTEM_INFO system_info;
    GetSystemInfo(&system_info);
    return system_info.dwPageSize;
  }

  unsigned _page_size = _get_page_size();
  #else
  unsigned _page_size = sysconf(_SC_PAGESIZE);
  #endif
  unsigned long _page_mask = ~(unsigned long)(_page_size - 1);
  unsigned _page_shift = _get_bits_of(_page_size - 1);

}
