// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "object.h"
#include "common/Formatter.h"

#include <cstdio>

void object_t::encode(ceph::buffer::list &bl) const {
  using ceph::encode;
  encode(name, bl);
}

void object_t::decode(ceph::buffer::list::const_iterator &bl) {
  using ceph::decode;
  decode(name, bl);
}

void object_t::dump(ceph::Formatter *f) const {
  f->dump_string("name", name);
}

std::list<object_t> object_t::generate_test_instances() {
  std::list<object_t> o;
  o.emplace_back();
  o.push_back(object_t{"myobject"});
  return o;
}

std::ostream& operator<<(std::ostream& out, const object_t& o) {
  return out << o.name;
}

const char *file_object_t::c_str() const {
  if (!buf[0])
    snprintf(buf, sizeof(buf), "%llx.%08llx", (long long unsigned)ino, (long long unsigned)bno);
  return buf;
}

std::ostream& operator<<(std::ostream& out, const snapid_t& s) {
  if (s == CEPH_NOSNAP)
    return out << "head";
  else if (s == CEPH_SNAPDIR)
    return out << "snapdir";
  else
    return out << std::hex << s.val << std::dec;
}

auto fmt::formatter<snapid_t>::format(const snapid_t& snp, format_context& ctx) const -> format_context::iterator
{
  if (snp == CEPH_NOSNAP) {
    return fmt::format_to(ctx.out(), "head");
  }
  if (snp == CEPH_SNAPDIR) {
    return fmt::format_to(ctx.out(), "snapdir");
  }
  return fmt::format_to(ctx.out(), FMT_COMPILE("{:x}"), snp.val);
}

void sobject_t::dump(ceph::Formatter *f) const {
  f->dump_stream("oid") << oid;
  f->dump_stream("snap") << snap;
}

std::list<sobject_t> sobject_t::generate_test_instances() {
  std::list<sobject_t> o;
  o.emplace_back();
  o.push_back(sobject_t{object_t("myobject"), 123});
  return o;
}

std::ostream& operator<<(std::ostream& out, const sobject_t &o) {
  return out << o.oid << "/" << o.snap;
}
