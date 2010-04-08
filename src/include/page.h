#ifndef __CEPH_PAGE_H
#define __CEPH_PAGE_H

namespace ceph {
  // these are in common/page.cc
  extern unsigned _page_size;
  extern unsigned long _page_mask;
  extern unsigned _page_shift;
}

#ifndef PAGE_SIZE
#define PAGE_SIZE ceph::_page_size
#endif
#ifndef PAGE_MASK
#define PAGE_MASK ceph::_page_mask
#endif
#ifndef PAGE_SHIFT
#define PAGE_SHIFT ceph::_page_shift
#endif

#endif
