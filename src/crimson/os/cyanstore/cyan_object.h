// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstddef>
#include <map>
#include <string>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/buffer.h"

namespace crimson::os {

struct Object : public boost::intrusive_ref_counter<
  Object,
  boost::thread_unsafe_counter>
{
  using bufferlist = ceph::bufferlist;

  // object data in fixed-size pages, overwritten in place; a missing page
  // reads as zeros, and bytes past size are always zero
  static constexpr uint64_t DATA_PAGE_SIZE = 64 << 10;
  std::map<uint64_t, ceph::bufferptr> pages; // page index -> page
  uint64_t size = 0;
  // use transparent comparator for better performance, see
  // https://en.cppreference.com/w/cpp/utility/functional/less_void
  std::map<std::string,bufferlist,std::less<>> xattr;
  bufferlist omap_header;
  std::map<std::string,bufferlist> omap;

  typedef boost::intrusive_ptr<Object> Ref;

  Object() = default;

  // interface for object data
  size_t get_size() const;
  ceph::bufferlist read(uint64_t offset, uint64_t len) const;
  int write(uint64_t offset, const bufferlist &bl);
  int clone(Object *src, uint64_t srcoff, uint64_t len,
	     uint64_t dstoff);
  int truncate(uint64_t offset);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& p);

private:
  ceph::bufferptr& get_writable_page(uint64_t idx);
  void zero_tail(uint64_t from);
};
using ObjectRef = boost::intrusive_ptr<Object>;

}
