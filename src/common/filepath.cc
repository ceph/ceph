// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "common/strescape.h"

#include <ostream>

using namespace std::literals::string_view_literals;

/* Trim given path to final 10 components and return it by prefixing it with
 * "..."  to indicate that the path has been trimmed. */
std::string filepath::get_trimmed_path() const
{
  _check_bits();
  auto const d = depth();
  if (d > 10) {
    auto fp = filepath();
    fp.push_dentry("..."sv);
    for (auto i = d; i > d-10; --i) {
      fp.push_dentry((*this)[i-1]);
    }
    return std::string(fp.get_path());
  } else {
    return std::string(get_path());
  }
}

void filepath::_rebuild_path() const
{
  _path.clear();
  auto it = _bits.begin();
  if (it != _bits.end()) {
    auto& b = *it;
    if (b.size() == 1) {
      /* Empty dentry for snappath */
      ceph_assert(b[0] == '\0');
      _path.emplace_back('/');
      ++it;
    }
  }
  unsigned components = 0;
  for (; it != _bits.end(); ++it) {
    if (components > 0) {
      _path.push_back('/');
    }
    auto& b = *it;
    ceph_assert(b.size() > 1); /* empty only valid at start to indicate snappath */
    _path.insert(_path.end(), b.begin(), b.end());
    _path.pop_back(); /* drop bits NUL */
    ++components;
  }
  _path.emplace_back('\0');
  _bits_dirty = false;
}

void filepath::_parse_bits() const
{
  _bits.clear();
  _bits_dirty = false;
  /* The code originally kept an empty dentry at the beginning of the
   * bits vector to indicate that this is a snap path. Maintain that behavior.
   */
  auto p = std::string_view(_path.data(), _path.size()-1); /* exclude NUL */
  auto it = p.begin();
  if (p[0] == '/') {
    /* snappath */
    auto& b = _bits.emplace_back();
    b.emplace_back('\0');
    ++it;
  }
  while (it != p.end()) {
    auto remainder = std::string_view(it, p.end());
    auto nextslash = remainder.find('/');
    auto& b = _bits.emplace_back();
    if (nextslash == p.npos)  {
      b.assign(remainder.begin(), remainder.end());
      it = p.end();
    } else {
      b.assign(remainder.begin(), remainder.begin()+nextslash);
      it += nextslash+1;
    }
    b.emplace_back('\0'); /* NUL terminate */
  }
}

void filepath::set_trimmed() {
  if (trimmed)
    return;
  // indicates that the path has been shortened.
  _check_bits();
  push_dentry("..."sv);
  trimmed = true;
}

void filepath::set_string(std::string_view s)
{
  /* Yup, this is super evil. We use this to encode a symlink target. */
  clear();
  _path.assign(s.begin(), s.end());
  _path.emplace_back('\0');
}

void filepath::_set_path(std::string_view s)
{
  _path.clear(); /* remove everything include NUL */
  if (!s.empty()) {
    auto it = s.begin();
    while (*it == '/') {
      _ino = 1; /* root */
      ++it;
    }
    unsigned components = 0;
    while (it != s.end()) {
      auto remainder = std::string_view(it, s.end());
      auto pos = remainder.find('/');
      if (components > 0) {
        _path.emplace_back('/');
      }
      ++components;
      if (pos == remainder.npos) {
        _path.insert(_path.end(), remainder.begin(), remainder.end());
        break;
      } else {
        _path.insert(_path.end(), remainder.begin(), remainder.begin()+pos);
        /* chomp duplicate '/' */
        it += pos;
        while (*it == '/') {
          ++it;
        }
        if (it == s.end()) {
          /* a final '/' in a path is semantically significant, equivalent to ".../." , add it back: */
          _path.push_back('/');
          _path.push_back('.');
        }
      }
    }
  }
  _path.emplace_back('\0'); /* NUL terminate */
}

filepath filepath::prefixpath(int s) const
{
  filepath t(_ino);
  _check_bits();
  for (int i=0; i<s; i++) {
    t.push_dentry((*this)[i]);
  }
  return t;
}

filepath filepath::postfixpath(int s) const
{
  filepath t;
  _check_bits();
  for (unsigned i=s; i<_bits.size(); i++) {
    t.push_dentry((*this)[i]);
  }
  return t;
}

void filepath::pop_dentry()
{
  _check_bits();
  _bits.pop_back();
  _bits_dirty = true;
}

void filepath::push_dentry(std::string_view s)
{
  _check_bits();
  auto& b = _bits.emplace_back();
  b.assign(s.begin(), s.end());
  b.emplace_back('\0');
  _bits_dirty = true;
}

void filepath::push_front_dentry(std::string_view s)
{
  _check_bits();
  auto it = _bits.emplace(_bits.begin());
  auto& b = *it;
  b.assign(s.begin(), s.end());
  b.emplace_back('\0');
  _bits_dirty = true;
}

void filepath::append(const filepath& a)
{
  ceph_assert(a.pure_relative());
  _check_bits();
  for (auto&& component : a) {
    push_dentry(component);
  }
}

void filepath::encode(ceph::buffer::list& bl) const
{
  using ceph::encode;
  __u8 struct_v = 1;
  encode(struct_v, bl);
  encode(_ino, bl);
  encode(get_path(), bl);
}

void filepath::decode(ceph::buffer::list::const_iterator& blp)
{
  using ceph::decode;
  clear();
  __u8 struct_v;
  decode(struct_v, blp);
  decode(_ino, blp);
  std::string p;
  decode(p, blp);
  /* N.B. may be used as std::string storage (xattr / symlink target). Do not
   * use _set_path() for normalization. Also, it may be a snappath beginning
   * with '/'.
   */
  _path.assign(p.begin(), p.end());
  _path.emplace_back('\0');
}

void filepath::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("base_ino", _ino);
  f->dump_string("relative_path", get_path());
}

std::list<filepath> filepath::generate_test_instances()
{
  std::list<filepath> o;
  o.emplace_back();
  o.push_back(filepath("/usr/bin", 0));
  o.push_back(filepath("/usr/sbin", 1));
  o.push_back(filepath("var/log", 1));
  o.push_back(filepath("foo/bar", 101));
  return o;
}

bool filepath::is_last_dot_or_dotdot() const
{
  if (depth() > 0) {
    auto&& dname = last_dentry();
    if (dname == "." || dname == "..") {
      return true;
    }
  }

  return false;
}

void filepath::print(std::ostream& out) const
{
  if (get_ino()) {
    out << '#' << get_ino();
  }
  auto&& path = get_path();
  out << '/';
  /* N.B.: when path is a snappath, it will have a leading '/' so that it
   * appears as #ino//snappath in logs.
   */
  out << binstrprint(path);
}
