/// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "MemObject.h"

// MemObject
int MemObject::getattr(const char *name, ceph::buffer::ptr& value)
{
  string k(name);
  std::lock_guard<std::mutex> lock(xattr_mutex);
  if (!xattr.count(k)) {
    return -ENODATA;
  }
  value = xattr[k];
  return 0;
}

int MemObject::getattrs(std::map<std::string,ceph::buffer::ptr,std::less<>>& aset)
{
  std::lock_guard<std::mutex> lock(xattr_mutex);
  aset = xattr;
  return 0;
}

int MemObject::setattrs(std::map<std::string,ceph::buffer::ptr>& aset)
{
  std::lock_guard<std::mutex> lock(xattr_mutex); 
  for (auto p = aset.begin(); p != aset.end(); ++p) {
    xattr[p->first] = p->second;
  }
  return 0;
}

int MemObject::rmattr(const char *name)
{
  std::lock_guard<std::mutex> lock(xattr_mutex);
  auto i = xattr.find(name);
  if (i == xattr.end()) {
    return -ENODATA;
  }
  xattr.erase(i);
  return 0;
}

int MemObject::rmattrs()
{
  std::lock_guard<std::mutex> lock(xattr_mutex);
  xattr.clear();
  return 0;
}

int MemObject::omap_get(ceph::buffer::list *header, std::map<std::string, ceph::buffer::list> *out)
{
  std::lock_guard lock{omap_mutex};
  *header = omap_header;
  *out = omap;
  return 0;
}

int MemObject::omap_get_header(ceph::buffer::list *header)
{
  std::lock_guard lock{omap_mutex};
  *header = omap_header;
  return 0;
}

int MemObject::omap_get_keys(std::set<std::string> *keys)
{
  std::lock_guard lock{omap_mutex};
  for (auto p = omap.begin(); p != omap.end(); ++p) {
    keys->insert(p->first);
  }
  return 0;
}

int MemObject::omap_get_values(const std::set<std::string> &keys, std::map<std::string, ceph::buffer::list> *out)
{
  std::lock_guard lock{omap_mutex};
  for (auto p = keys.begin(); p != keys.end(); ++p) {
    auto q = omap.find(*p);
    if (q != omap.end()) {
      out->insert(*q);
    }
  }
  return 0;
}

int MemObject::omap_check_keys(const std::set<std::string> &keys, std::set<std::string> *out)
{
  std::lock_guard lock{omap_mutex};
  for (auto p = keys.begin(); p != keys.end(); ++p) {
    auto q = omap.find(*p);
    if (q != omap.end()) {
      out->insert(*p);
    }
  }
  return 0;
}

int MemObject::omap_clear()
{
  std::lock_guard<std::mutex> lock(omap_mutex);
  omap.clear();
  omap_header.clear();
  return 0;
}

int MemObject::omap_setkeys(ceph::buffer::list& aset_bl)
{
  using ceph::decode;

  std::lock_guard lock{omap_mutex};
  auto p = aset_bl.cbegin();
  __u32 num;
  decode(num, p);
  while (num--) {
    std::string key;
    decode(key, p);
    decode(omap[key], p);
  }
  return 0;
}

int MemObject::omap_rmkeys(ceph::buffer::list& keys_bl)
{
  using ceph::decode;

  std::lock_guard lock{omap_mutex};
  auto p = keys_bl.cbegin();
  __u32 num;
  decode(num, p);
  while (num--) {
    std::string key;
    decode(key, p);
    omap.erase(key);
  }
  return 0;
}

int MemObject::omap_rmkeyrange(const std::string& first, const std::string& last)
{
  std::lock_guard lock{omap_mutex};
  auto p = omap.lower_bound(first);
  auto e = omap.lower_bound(last);
  omap.erase(p, e);
  return 0;
}

int MemObject::omap_setheader(const ceph::buffer::list &bl)
{
  std::lock_guard lock{omap_mutex};
  omap_header = bl;
  return 0;
}

int MemObject::clone(MemObject *src)
{
  clone_range(src, 0, src->get_size(), 0);
  // take xattr and omap locks with std::lock()
  std::scoped_lock l{src->xattr_mutex,
                     xattr_mutex,
                     src->omap_mutex,
                     omap_mutex};
  
  omap_header = src->omap_header;
  omap = src->omap;
  xattr = src->xattr;
  return 0;
}

void MemObject::encode_base(ceph::buffer::list& bl) const
{
  using ceph::encode;

  encode(xattr, bl);
  encode(omap_header, bl);
  encode(omap, bl);
}

void MemObject::decode_base(ceph::buffer::list::const_iterator& p)
{
  using ceph::decode;

  decode(xattr, p);
  decode(omap_header, p);
  decode(omap, p);
}

void MemObject::dump(ceph::Formatter *f) const
{
  f->dump_int("data_len", get_size());
  f->dump_int("omap_header_len", omap_header.length());

  f->open_array_section("xattrs");
  for (auto p = xattr.begin(); p != xattr.end(); ++p) {
    f->open_object_section("xattr");
    f->dump_string("name", p->first);
    f->dump_int("length", p->second.length());
    f->close_section();
  }
  f->close_section();

  f->open_array_section("omap");
  for (auto p = omap.begin(); p != omap.end(); ++p) {
    f->open_object_section("pair");
    f->dump_string("key", p->first);
    f->dump_int("length", p->second.length());
    f->close_section();
  }
  f->close_section();
}

// BufferlistObject
int BufferlistObject::read(uint64_t offset, uint64_t len,
                                     ceph::buffer::list &bl)
{
  std::lock_guard<decltype(mutex)> lock(mutex);
  bl.substr_of(data, offset, len);
  return bl.length();
}

int BufferlistObject::write(uint64_t offset, const ceph::buffer::list &src)
{
  unsigned len = src.length();

  std::lock_guard<decltype(mutex)> lock(mutex);

  // before
  ceph::buffer::list newdata;
  if (get_size() >= offset) {
    newdata.substr_of(data, 0, offset);
  } else {
    if (get_size()) {
      newdata.substr_of(data, 0, get_size());
    }
    newdata.append_zero(offset - get_size());
  }

  newdata.append(src);

  // after
  if (get_size() > offset + len) {
    ceph::buffer::list tail;
    tail.substr_of(data, offset + len, get_size() - (offset + len));
    newdata.append(tail);
  }

  data = std::move(newdata);
  return 0;
}

int BufferlistObject::clone_range(MemObject *src, uint64_t srcoff, uint64_t len,
                                  uint64_t dstoff)
{
  auto srcbl = dynamic_cast<BufferlistObject*>(src);
  if (srcbl == nullptr)
    return -ENOTSUP;

  ceph::buffer::list bl;
  {
    std::lock_guard<decltype(srcbl->mutex)> lock(srcbl->mutex);
    if (srcoff == dstoff && len == src->get_size()) {
      data = srcbl->data;
      return 0;
    }
    bl.substr_of(srcbl->data, srcoff, len);
  }
  return write(dstoff, bl);
}

int BufferlistObject::truncate(uint64_t size)
{
  std::lock_guard<decltype(mutex)> lock(mutex);
  if (get_size() > size) {
    ceph::buffer::list bl;
    bl.substr_of(data, 0, size);
    data = std::move(bl);
  } else if (get_size() == size) {
    // do nothing
  } else {
    data.append_zero(size - get_size());
  }
  return 0;
}

// VectorObject
int VectorObject::read(uint64_t offset, uint64_t len, ceph::buffer::list &bl)
{
  const auto start = offset;
  const auto end = offset + len;
  buffer::ptr buf(len);

  assert(data.size() >= end);

  std::lock_guard<decltype(mutex)> lock(mutex);

  std::vector<char> tmp(data.begin() + start, data.begin() + end);
  buf.copy_in(0, len, &tmp[0]);
  bl.append(std::move(buf));
  return len;
}

int VectorObject::write(uint64_t offset, const ceph::buffer::list &src)
{
  unsigned len = src.length();
  std::vector<char> buf(len);

  // Copy the bufferlist data into buf
  src.begin().copy(len, &buf[0]);

  return _write(offset, buf);
}

int VectorObject::_write(uint64_t offset, std::vector<char>& buf) {
  std::lock_guard<decltype(mutex)> lock(mutex);

  // If we have a hole, fill it with zeros
  if (offset > data.size())
    data.insert(data.end(), offset - data.size(), 0);

  // Move buf to our data vector the specified offset. 
  if (offset + buf.size() > data.size())
    data.resize(offset + buf.size());
  std::move(buf.begin(), buf.end(), data.begin() + offset);

  return 0;
}

int VectorObject::clone_range(MemObject *o, uint64_t srcoff, uint64_t len,
                              uint64_t dstoff)
{
  auto vo = dynamic_cast<VectorObject*>(o);
  if (vo == nullptr) return -ENOTSUP;

  vector<char> buf;
  {
    std::lock_guard<decltype(vo->mutex)> lock(vo->mutex);
    if (srcoff == dstoff && len == vo->get_size()) {
     data = vo->data;
     return 0;
    }
    buf = vector<char>(vo->data.begin() + srcoff, vo->data.begin() + len);
  }
  return _write(dstoff, buf);
}

int VectorObject::truncate(uint64_t size)
{
  std::lock_guard<decltype(mutex)> lock(mutex);
  data.resize(size, 0);
  data.shrink_to_fit();
  return 0;
}

// PageSetObject

#if defined(__GLIBCXX__)
// use a thread-local vector for the pages returned by PageSet, so we
// can avoid allocations in read/write()
thread_local PageSet::page_vector PageSetObject::tls_pages;
#define DEFINE_PAGE_VECTOR(name)
#else
#define DEFINE_PAGE_VECTOR(name) PageSet::page_vector name;
#endif

int PageSetObject::read(uint64_t offset, uint64_t len, ceph::buffer::list& bl)
{
  const auto start = offset;
  const auto end = offset + len;
  auto remaining = len;

  DEFINE_PAGE_VECTOR(tls_pages);
  data.get_range(offset, len, tls_pages);

  // allocate a buffer for the data
  ceph::buffer::ptr buf(len);

  auto p = tls_pages.begin();
  while (remaining) {
    // no more pages in range
    if (p == tls_pages.end() || (*p)->offset >= end) {
      buf.zero(offset - start, remaining);
      break;
    }
    auto page = *p;

    // fill any holes between pages with zeroes
    if (page->offset > offset) {
      const auto count = std::min(remaining, page->offset - offset);
      buf.zero(offset - start, count);
      remaining -= count;
      offset = page->offset;
      if (!remaining)
        break;
    }

    // read from page
    const auto page_offset = offset - page->offset;
    const auto count = std::min(remaining, data.get_page_size() - page_offset);

    buf.copy_in(offset - start, count, page->data + page_offset);

    remaining -= count;
    offset += count;

    ++p;
  }

  tls_pages.clear(); // drop page refs

  bl.append(std::move(buf));
  return len;
}

int PageSetObject::write(uint64_t offset, const ceph::buffer::list &src)
{
  unsigned len = src.length();

  DEFINE_PAGE_VECTOR(tls_pages);
  // make sure the page range is allocated
  data.alloc_range(offset, src.length(), tls_pages);

  auto page = tls_pages.begin();

  auto p = src.begin();
  while (len > 0) {
    unsigned page_offset = offset - (*page)->offset;
    unsigned pageoff = data.get_page_size() - page_offset;
    unsigned count = std::min(len, pageoff);
    p.copy(count, (*page)->data + page_offset);
    offset += count;
    len -= count;
    if (count == pageoff)
      ++page;
  }
  if (data_len < offset)
    data_len = offset;
  tls_pages.clear(); // drop page refs
  return 0;
}

int PageSetObject::clone_range(MemObject *src, uint64_t srcoff,
                               uint64_t len, uint64_t dstoff)
{
  const int64_t delta = dstoff - srcoff;

  auto &src_data = static_cast<PageSetObject*>(src)->data;
  const uint64_t src_page_size = src_data.get_page_size();

  auto &dst_data = data;
  const auto dst_page_size = dst_data.get_page_size();

  DEFINE_PAGE_VECTOR(tls_pages);
  PageSet::page_vector dst_pages;

  while (len) {
    // limit to 16 pages at a time so tls_pages doesn't balloon in size
    auto count = std::min(len, (uint64_t)src_page_size * 16);
    src_data.get_range(srcoff, count, tls_pages);

    // allocate the destination range
    // TODO: avoid allocating pages for holes in the source range
    dst_data.alloc_range(srcoff + delta, count, dst_pages);
    auto dst_iter = dst_pages.begin();

    for (auto &src_page : tls_pages) {
      auto sbegin = std::max(srcoff, src_page->offset);
      auto send = std::min(srcoff + count, src_page->offset + src_page_size);

      // zero-fill holes before src_page
      if (srcoff < sbegin) {
        while (dst_iter != dst_pages.end()) {
          auto &dst_page = *dst_iter;
          auto dbegin = std::max(srcoff + delta, dst_page->offset);
          auto dend = std::min(sbegin + delta, dst_page->offset + dst_page_size);
          std::fill(dst_page->data + dbegin - dst_page->offset,
                    dst_page->data + dend - dst_page->offset, 0);
          if (dend < dst_page->offset + dst_page_size)
            break;
          ++dst_iter;
        }
        const auto c = sbegin - srcoff;
        count -= c;
        len -= c;
      }

      // copy data from src page to dst pages
      while (dst_iter != dst_pages.end()) {
        auto &dst_page = *dst_iter;
        auto dbegin = std::max(sbegin + delta, dst_page->offset);
        auto dend = std::min(send + delta, dst_page->offset + dst_page_size);

        std::copy(src_page->data + (dbegin - delta) - src_page->offset,
                  src_page->data + (dend - delta) - src_page->offset,
                  dst_page->data + dbegin - dst_page->offset);
        if (dend < dst_page->offset + dst_page_size)
          break;
        ++dst_iter;
      }

      const auto c = send - sbegin;
      count -= c;
      len -= c;
      srcoff = send;
      dstoff = send + delta;
    }
    tls_pages.clear(); // drop page refs

    // zero-fill holes after the last src_page
    if (count > 0) {
      while (dst_iter != dst_pages.end()) {
        auto &dst_page = *dst_iter;
        auto dbegin = std::max(dstoff, dst_page->offset);
        auto dend = std::min(dstoff + count, dst_page->offset + dst_page_size);
        std::fill(dst_page->data + dbegin - dst_page->offset,
                  dst_page->data + dend - dst_page->offset, 0);
        ++dst_iter;
      }
      srcoff += count;
      dstoff += count;
      len -= count;
    }
    dst_pages.clear(); // drop page refs
  }

  // update object size
  if (data_len < dstoff)
    data_len = dstoff;
  return 0;
}

int PageSetObject::truncate(uint64_t size)
{
  data.free_pages_after(size);
  data_len = size;

  const auto page_size = data.get_page_size();
  const auto page_offset = size & ~(page_size-1);
  if (page_offset == size)
    return 0;

  DEFINE_PAGE_VECTOR(tls_pages);
  // write zeroes to the rest of the last page
  data.get_range(page_offset, page_size, tls_pages);
  if (tls_pages.empty())
    return 0;

  auto page = tls_pages.begin();
  auto data = (*page)->data;
  std::fill(data + (size - page_offset), data + page_size, 0);
  tls_pages.clear(); // drop page ref
  return 0;
}

