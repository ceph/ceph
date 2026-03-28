// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OLD_INODE_H
#define CEPH_OLD_INODE_H

#include <list>
#include <memory> // for std::allocator
#include <string>

#include "include/cephfs/types.h" // for inode_t
#include "include/encoding.h"
#include "include/object.h" // for snapid_t

template<template<typename> class Allocator = std::allocator>
struct old_inode_t {
  snapid_t first;
  inode_t<Allocator> inode;
  xattr_map<Allocator> xattrs;

  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static std::list<old_inode_t> generate_test_instances();
};

// These methods may be moved back to mdstypes.cc when we have pmr
template<template<typename> class Allocator>
void old_inode_t<Allocator>::encode(ceph::buffer::list& bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  encode(first, bl);
  encode(inode, bl, features);
  encode(xattrs, bl);
  ENCODE_FINISH(bl);
}

template<template<typename> class Allocator>
void old_inode_t<Allocator>::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(first, bl);
  decode(inode, bl);
  decode_noshare<Allocator>(xattrs, bl);
  DECODE_FINISH(bl);
}

template<template<typename> class Allocator>
void old_inode_t<Allocator>::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("first", first);
  inode.dump(f);
  f->open_object_section("xattrs");
  for (const auto &p : xattrs) {
    std::string v(p.second.c_str(), p.second.length());
    f->dump_string(p.first.c_str(), v);
  }
  f->close_section();
}

template<template<typename> class Allocator>
auto old_inode_t<Allocator>::generate_test_instances() -> std::list<old_inode_t<Allocator>>
{
  std::list<old_inode_t<Allocator>> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().first = 2;
  std::list<inode_t<Allocator>> ils = inode_t<Allocator>::generate_test_instances();
  ls.back().inode = ils.back();
  ls.back().xattrs["user.foo"] = ceph::buffer::copy("asdf", 4);
  ls.back().xattrs["user.unprintable"] = ceph::buffer::copy("\000\001\002", 3);
  return ls;
}

template<template<typename> class Allocator>
inline void encode(const old_inode_t<Allocator> &c, ::ceph::buffer::list &bl, uint64_t features)
{
  ENCODE_DUMP_PRE();
  c.encode(bl, features);
  ENCODE_DUMP_POST(cl);
}
template<template<typename> class Allocator>
inline void decode(old_inode_t<Allocator> &c, ::ceph::buffer::list::const_iterator &p)
{
  c.decode(p);
}

#endif
