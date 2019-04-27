// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <cstddef>
#include <map>
#include <string>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "include/buffer.h"

namespace ceph::os {

struct Object : public boost::intrusive_ref_counter<
  Object,
  boost::thread_unsafe_counter>
{
  using bufferlist = ceph::bufferlist;

  bufferlist data;
  // use transparent comparator for better performance, see
  // https://en.cppreference.com/w/cpp/utility/functional/less_void
  std::map<std::string,bufferptr,std::less<>> xattr;
  bufferlist omap_header;
  std::map<std::string,bufferlist> omap;

  typedef boost::intrusive_ptr<Object> Ref;

  Object() = default;

  // interface for object data
  size_t get_size() const;
  int read(uint64_t offset, uint64_t len, bufferlist &bl);
  int write(uint64_t offset, const bufferlist &bl);
  int clone(Object *src, uint64_t srcoff, uint64_t len,
	     uint64_t dstoff);
  int truncate(uint64_t offset);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& p);
};
}
