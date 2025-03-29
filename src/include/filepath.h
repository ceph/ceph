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


#ifndef CEPH_FILEPATH_H
#define CEPH_FILEPATH_H

/*
 * BUG:  /a/b/c is equivalent to a/b/c in dentry-breakdown, but not string.
 *   -> should it be different?  how?  should this[0] be "", with depth 4?
 *
 */


#include <iosfwd>
#include <list>
#include <string>
#include <string_view>
#include <vector>

#include "buffer.h"
#include "encoding.h"
#include "include/fs_types.h" // for inodeno_t

namespace ceph { class Formatter; }

class filepath {
  inodeno_t ino = 0;   // base inode.  ino=0 implies pure relative path.
  std::string path;     // relative path.

  /** bits - path segments
   * this is ['a', 'b', 'c'] for both the aboslute and relative case.
   *
   * NOTE: this value is LAZILY maintained... i.e. it's a cache
   */
  mutable std::vector<std::string> bits;
  bool encoded = false;

  void rebuild_path();
  void parse_bits() const;

 public:
  filepath() = default;
  filepath(std::string_view p, inodeno_t i) : ino(i), path(p) {}
  filepath(const filepath& o) {
    ino = o.ino;
    path = o.path;
    bits = o.bits;
    encoded = o.encoded;
  }
  filepath(inodeno_t i) : ino(i) {}
  filepath& operator=(const char* path) {
    set_path(path);
    return *this;
  }

  /*
   * if we are fed a relative path as a string, either set ino=0 (strictly
   * relative) or 1 (absolute).  throw out any leading '/'.
   */
  filepath(std::string_view s) { set_path(s); }
  filepath(const char* s) { set_path(s); }

  void set_path(std::string_view s, inodeno_t b) {
    path = s;
    ino = b;
  }

  void set_path(std::string_view s);

  // accessors
  inodeno_t get_ino() const { return ino; }
  const std::string& get_path() const { return path; }
  const char *c_str() const { return path.c_str(); }

  int length() const { return path.length(); }
  unsigned depth() const {
    if (bits.empty() && path.length() > 0) parse_bits();
    return bits.size();
  }
  bool empty() const { return path.length() == 0 && ino == 0; }

  bool absolute() const { return ino == 1; }
  bool pure_relative() const { return ino == 0; }
  bool ino_relative() const { return ino > 0; }

  const std::string& operator[](int i) const {
    if (bits.empty() && path.length() > 0) parse_bits();
    return bits[i];
  }

  auto begin() const {
    if (bits.empty() && path.length() > 0) parse_bits();
    return std::as_const(bits).begin();
  }
  auto rbegin() const {
    if (bits.empty() && path.length() > 0) parse_bits();
    return std::as_const(bits).rbegin();
  }

  auto end() const {
    if (bits.empty() && path.length() > 0) parse_bits();
    return std::as_const(bits).end();
  }
  auto rend() const {
    if (bits.empty() && path.length() > 0) parse_bits();
    return std::as_const(bits).rend();
  }

  const std::string& last_dentry() const {
    if (bits.empty() && path.length() > 0) parse_bits();
    ceph_assert(!bits.empty());
    return bits[ bits.size()-1 ];
  }

  filepath prefixpath(int s) const;
  filepath postfixpath(int s) const;

  // modifiers
  //  string can be relative "a/b/c" (ino=0) or absolute "/a/b/c" (ino=1)
  void _set_ino(inodeno_t i) { ino = i; }
  void clear() {
    ino = 0;
    path = "";
    bits.clear();
  }

  void pop_dentry();
  void push_dentry(std::string_view s);
  void push_dentry(const std::string& s) {
    push_dentry(std::string_view(s));
  }
  void push_dentry(const char *cs) {
    push_dentry(std::string_view(cs, strlen(cs)));
  }
  void push_front_dentry(const std::string& s);
  void append(const filepath& a);

  // encoding
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& blp);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<filepath*>& o);

  bool is_last_dot_or_dotdot() const;

  bool is_last_snap() const {
    // walk into snapdir?
    return depth() > 0 && bits[0].length() == 0;
  }
};

WRITE_CLASS_ENCODER(filepath)

std::ostream& operator<<(std::ostream& out, const filepath& path);

#endif
