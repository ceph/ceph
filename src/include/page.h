#ifndef CEPH_PAGE_H
#define CEPH_PAGE_H

namespace ceph {
  // these are in common/page.cc
  extern unsigned _page_size;
  extern unsigned long _page_mask;
  extern unsigned _page_shift;
}

#undef PAGE_SIZE
#define PAGE_SIZE ceph::_page_size

#undef PAGE_MASK
#define PAGE_MASK ceph::_page_mask

#undef PAGE_SHIFT
#define PAGE_SHIFT ceph::_page_shift

#endif
