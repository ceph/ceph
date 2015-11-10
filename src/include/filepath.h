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
#include <string>
#include <vector>
using namespace std;

#include "buffer.h"
#include "encoding.h"
#include "include/types.h"
#include "include/fs_types.h"

#include "common/Formatter.h"


class filepath {
  inodeno_t ino;   // base inode.  ino=0 implies pure relative path.
  string path;     // relative path.

  /** bits - path segments
   * this is ['a', 'b', 'c'] for both the aboslute and relative case.
   *
   * NOTE: this value is LAZILY maintained... i.e. it's a cache
   */
  mutable vector<string> bits;
  bool encoded;

  void rebuild_path() {
    path.clear();
    for (unsigned i=0; i<bits.size(); i++) {
      if (i) path += "/";
      path += bits[i];
    }
  }
  void parse_bits() const {
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

 public:
  filepath() : ino(0), encoded(false) { }
  filepath(const string& s, inodeno_t i) : ino(i), path(s), encoded(false) { }
  filepath(const char* s, inodeno_t i) : ino(i), path(s), encoded(false) { }
  filepath(const filepath& o) {
    ino = o.ino;
    path = o.path;
    bits = o.bits;
    encoded = o.encoded;
  }
  filepath(inodeno_t i) : ino(i), encoded(false) { }
  
  void set_path(const char *s, inodeno_t b) {
    path = s;
    ino = b;
  }

  /*
   * if we are fed a relative path as a string, either set ino=0 (strictly
   * relative) or 1 (absolute).  throw out any leading '/'.
   */
  filepath(const char *s) : encoded(false) {
    set_path(s);
  }
  void set_path(const char *s) {
    if (s[0] == '/') {
      path = s + 1;    
      ino = 1;
    } else {
      ino = 0;
      path = s;
    }
    bits.clear();
  }


  // accessors
  inodeno_t get_ino() const { return ino; }
  const string& get_path() const { return path; }
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
  
  const string& operator[](int i) const {
    if (bits.empty() && path.length() > 0) parse_bits();
    return bits[i];
  }

  const string& last_dentry() const {
    if (bits.empty() && path.length() > 0) parse_bits();
    return bits[ bits.size()-1 ];
  }

  filepath prefixpath(int s) const {
    filepath t(ino);
    for (int i=0; i<s; i++)
      t.push_dentry(bits[i]);
    return t;
  }
  filepath postfixpath(int s) const {
    filepath t;
    for (unsigned i=s; i<bits.size(); i++)
      t.push_dentry(bits[i]);
    return t;
  }


  // modifiers
  //  string can be relative "a/b/c" (ino=0) or absolute "/a/b/c" (ino=1)
  void _set_ino(inodeno_t i) { ino = i; }
  void clear() {
    ino = 0;
    path = "";
    bits.clear();
  }

  void pop_dentry() {
    if (bits.empty() && path.length() > 0) 
      parse_bits();
    bits.pop_back();
    rebuild_path();
  }    
  void push_dentry(const string& s) {
    if (bits.empty() && path.length() > 0) 
      parse_bits();
    if (!bits.empty())
      path += "/";
    path += s;
    bits.push_back(s);
  }
  void push_dentry(const char *cs) {
    string s = cs;
    push_dentry(s);
  }
  void push_front_dentry(const string& s) {
    bits.insert(bits.begin(), s);
    rebuild_path();
  }
  void append(const filepath& a) {
    assert(a.pure_relative());
    for (unsigned i=0; i<a.depth(); i++) 
      push_dentry(a[i]);
  }

  // encoding
  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(ino, bl);
    ::encode(path, bl);
  }
  void decode(bufferlist::iterator& blp) {
    bits.clear();
    __u8 struct_v;
    ::decode(struct_v, blp);
    ::decode(ino, blp);
    ::decode(path, blp);
    encoded = true;
  }
  void dump(Formatter *f) const {
    f->dump_unsigned("base_ino", ino);
    f->dump_string("relative_path", path);
  }
  static void generate_test_instances(list<filepath*>& o) {
    o.push_back(new filepath);
    o.push_back(new filepath("/usr/bin", 0));
    o.push_back(new filepath("/usr/sbin", 1));
    o.push_back(new filepath("var/log", 1));
    o.push_back(new filepath("foo/bar", 101));
  }
};

WRITE_CLASS_ENCODER(filepath)

inline ostream& operator<<(ostream& out, const filepath& path)
{
  if (path.get_ino()) {
    out << '#' << path.get_ino();
    if (path.depth())
      out << '/';
  }
  return out << path.get_path();
}

#endif
