#ifndef __CEPH_PAGE_H
#define __CEPH_PAGE_H

// these are in config.cc
extern unsigned _page_size;
extern unsigned long _page_mask;
extern unsigned _page_shift;

#define PAGE_SIZE _page_size
#define PAGE_MASK _page_mask
#define PAGE_SHIFT _page_shift

/*
#define PAGE_SIZE 4096
#define PAGE_MASK (~(4095))
#define PAGE_SHIFT 12
*/

#endif
