// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013- Sage Weil <sage@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_PAGESET_H
#define CEPH_PAGESET_H

#include <algorithm>
#include <atomic>
#include <cassert>
#include <mutex>
#include <vector>
#include <boost/intrusive/avl_set.hpp>
#include <boost/intrusive_ptr.hpp>

#include "include/encoding.h"
#include "include/Spinlock.h"


struct Page {
  char *const data;
  boost::intrusive::avl_set_member_hook<> hook;
  uint64_t offset;

  // avoid RefCountedObject because it has a virtual destructor
  std::atomic<uint16_t> nrefs;
  void get() { ++nrefs; }
  void put() { if (--nrefs == 0) delete this; }

  typedef boost::intrusive_ptr<Page> Ref;
  friend void intrusive_ptr_add_ref(Page *p) { p->get(); }
  friend void intrusive_ptr_release(Page *p) { p->put(); }

  // key-value comparison functor for avl
  struct Less {
    bool operator()(uint64_t offset, const Page &page) const {
      return offset < page.offset;
    }
    bool operator()(const Page &page, uint64_t offset) const {
      return page.offset < offset;
    }
    bool operator()(const Page &lhs, const Page &rhs) const {
      return lhs.offset < rhs.offset;
    }
  };
  void encode(bufferlist &bl, size_t page_size) const {
    bl.append(buffer::copy(data, page_size));
    ::encode(offset, bl);
  }
  void decode(bufferlist::iterator &p, size_t page_size) {
    ::decode_array_nohead(data, page_size, p);
    ::decode(offset, p);
  }

  static Ref create(size_t page_size, uint64_t offset = 0) {
    // ensure proper alignment of the Page
    const auto align = alignof(Page);
    page_size = (page_size + align - 1) & ~(align - 1);
    // allocate the Page and its data in a single buffer
    auto buffer = new char[page_size + sizeof(Page)];
    // place the Page structure at the end of the buffer
    return new (buffer + page_size) Page(buffer, offset);
  }

  // copy disabled
  Page(const Page&) = delete;
  const Page& operator=(const Page&) = delete;

 private: // private constructor, use create() instead
  Page(char *data, uint64_t offset) : data(data), offset(offset), nrefs(1) {}

  static void operator delete(void *p) {
    delete[] reinterpret_cast<Page*>(p)->data;
  }
};

class PageSet {
 public:
  // alloc_range() and get_range() return page refs in a vector
  typedef std::vector<Page::Ref> page_vector;

 private:
  // store pages in a boost intrusive avl_set
  typedef Page::Less page_cmp;
  typedef boost::intrusive::member_hook<Page,
          boost::intrusive::avl_set_member_hook<>,
          &Page::hook> member_option;
  typedef boost::intrusive::avl_set<Page,
          boost::intrusive::compare<page_cmp>, member_option> page_set;

  typedef typename page_set::iterator iterator;

  page_set pages;
  uint64_t page_size;

  typedef Spinlock lock_type;
  lock_type mutex;

  void free_pages(iterator cur, iterator end) {
    while (cur != end) {
      Page *page = &*cur;
      cur = pages.erase(cur);
      page->put();
    }
  }

  int count_pages(uint64_t offset, uint64_t len) const {
    // count the overlapping pages
    int count = 0;
    if (offset % page_size) {
      count++;
      size_t rem = page_size - offset % page_size;
      len = len <= rem ? 0 : len - rem;
    }
    count += len / page_size;
    if (len % page_size)
      count++;
    return count;
  }

 public:
  explicit PageSet(size_t page_size) : page_size(page_size) {}
  PageSet(PageSet &&rhs)
    : pages(std::move(rhs.pages)), page_size(rhs.page_size) {}
  ~PageSet() {
    free_pages(pages.begin(), pages.end());
  }

  // disable copy
  PageSet(const PageSet&) = delete;
  const PageSet& operator=(const PageSet&) = delete;

  bool empty() const { return pages.empty(); }
  size_t size() const { return pages.size(); }
  size_t get_page_size() const { return page_size; }

  // allocate all pages that intersect the range [offset,length)
  void alloc_range(uint64_t offset, uint64_t length, page_vector &range) {
    // loop in reverse so we can provide hints to avl_set::insert_check()
    //	and get O(1) insertions after the first
    uint64_t position = offset + length - 1;

    range.resize(count_pages(offset, length));
    auto out = range.rbegin();

    std::lock_guard<lock_type> lock(mutex);
    iterator cur = pages.end();
    while (length) {
      const uint64_t page_offset = position & ~(page_size-1);

      typename page_set::insert_commit_data commit;
      auto insert = pages.insert_check(cur, page_offset, page_cmp(), commit);
      if (insert.second) {
        auto page = Page::create(page_size, page_offset);
        cur = pages.insert_commit(*page, commit);

        // assume that the caller will write to the range [offset,length),
        //  so we only need to zero memory outside of this range

        // zero end of page past offset + length
        if (offset + length < page->offset + page_size)
          std::fill(page->data + offset + length - page->offset,
                    page->data + page_size, 0);
        // zero front of page between page_offset and offset
        if (offset > page->offset)
          std::fill(page->data, page->data + offset - page->offset, 0);
      } else { // exists
        cur = insert.first;
      }
      // add a reference to output vector
      out->reset(&*cur);
      ++out;

      auto c = std::min(length, (position & (page_size-1)) + 1);
      position -= c;
      length -= c;
    }
    // make sure we sized the vector correctly
    assert(out == range.rend());
  }

  // return all allocated pages that intersect the range [offset,length)
  void get_range(uint64_t offset, uint64_t length, page_vector &range) {
    auto cur = pages.lower_bound(offset & ~(page_size-1), page_cmp());
    while (cur != pages.end() && cur->offset < offset + length)
      range.push_back(&*cur++);
  }

  void free_pages_after(uint64_t offset) {
    std::lock_guard<lock_type> lock(mutex);
    auto cur = pages.lower_bound(offset & ~(page_size-1), page_cmp());
    if (cur == pages.end())
      return;
    if (cur->offset < offset)
      cur++;
    free_pages(cur, pages.end());
  }

  void encode(bufferlist &bl) const {
    ::encode(page_size, bl);
    unsigned count = pages.size();
    ::encode(count, bl);
    for (auto p = pages.rbegin(); p != pages.rend(); ++p)
      p->encode(bl, page_size);
  }
  void decode(bufferlist::iterator &p) {
    assert(empty());
    ::decode(page_size, p);
    unsigned count;
    ::decode(count, p);
    auto cur = pages.end();
    for (unsigned i = 0; i < count; i++) {
      auto page = Page::create(page_size);
      page->decode(p, page_size);
      cur = pages.insert_before(cur, *page);
    }
  }
};

#endif // CEPH_PAGESET_H
