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

#include "filepath.h"
#include "common/Formatter.h"

#include <ostream>

void filepath::rebuild_path() {
  path.clear();
  for (unsigned i=0; i<bits.size(); i++) {
    if (i) path += "/";
    path += bits[i];
  }
}

void filepath::parse_bits() const {
  bits.clear();
  int off = 0;
  while (off < (int)path.length()) {
    int nextslash = path.find('/', off);
    if (nextslash < 0) 
      nextslash = path.length();  // no more slashes
    if (((nextslash - off) > 0) || encoded) {
      // skip empty components unless they were introduced deliberately
      // see commit message for more detail
      bits.push_back( path.substr(off,nextslash-off) );
    }
    off = nextslash+1;
  }
}

void filepath::set_path(std::string_view s) {
  if (!s.empty() && s[0] == '/') {
    path = s.substr(1);
    ino = 1;
  } else {
    ino = 0;
    path = s;
  }
  bits.clear();
}

filepath filepath::prefixpath(int s) const {
  filepath t(ino);
  for (int i=0; i<s; i++)
    t.push_dentry(bits[i]);
  return t;
}

filepath filepath::postfixpath(int s) const {
  filepath t;
  for (unsigned i=s; i<bits.size(); i++)
    t.push_dentry(bits[i]);
  return t;
}

void filepath::pop_dentry() {
  if (bits.empty() && path.length() > 0) 
    parse_bits();
  bits.pop_back();
  rebuild_path();
}

void filepath::push_dentry(std::string_view s) {
  if (bits.empty() && path.length() > 0) 
    parse_bits();
  if (!bits.empty())
    path += "/";
  path += s;
  bits.emplace_back(s);
}

void filepath::push_front_dentry(const std::string& s) {
  bits.insert(bits.begin(), s);
  rebuild_path();
}

void filepath::append(const filepath& a) {
  ceph_assert(a.pure_relative());
  for (unsigned i=0; i<a.depth(); i++) 
    push_dentry(a[i]);
}

void filepath::encode(ceph::buffer::list& bl) const {
  using ceph::encode;
  __u8 struct_v = 1;
  encode(struct_v, bl);
  encode(ino, bl);
  encode(path, bl);
}

void filepath::decode(ceph::buffer::list::const_iterator& blp) {
  using ceph::decode;
  bits.clear();
  __u8 struct_v;
  decode(struct_v, blp);
  decode(ino, blp);
  decode(path, blp);
  encoded = true;
}

void filepath::dump(ceph::Formatter *f) const {
  f->dump_unsigned("base_ino", ino);
  f->dump_string("relative_path", path);
}

void filepath::generate_test_instances(std::list<filepath*>& o) {
  o.push_back(new filepath);
  o.push_back(new filepath("/usr/bin", 0));
  o.push_back(new filepath("/usr/sbin", 1));
  o.push_back(new filepath("var/log", 1));
  o.push_back(new filepath("foo/bar", 101));
}

bool filepath::is_last_dot_or_dotdot() const {
  if (depth() > 0) {
    std::string dname = last_dentry();
    if (dname == "." || dname == "..") {
      return true;
    }
  }

  return false;
}

std::ostream& operator<<(std::ostream& out, const filepath& path)
{
  if (path.get_ino()) {
    out << '#' << path.get_ino();
    if (path.length())
      out << '/';
  }
  return out << path.get_path();
}
