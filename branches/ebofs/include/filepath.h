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


#ifndef __FILEPATH_H
#define __FILEPATH_H


/*
 * BUG:  /a/b/c is equivalent to a/b/c in dentry-breakdown, but not string.
 *   -> should it be different?  how?  should this[0] be "", with depth 4?
 *
 */


#include <iostream>
#include <string>
#include <vector>
using namespace std;

#include "buffer.h"
#include "encodable.h"

class filepath {
  /** path
   */
  inodeno_t ino;   // base inode
  string path;     // relative path

  /** bits - path segemtns
   * this is ['a', 'b', 'c'] for both the aboslute and relative case.
   *
   * NOTE: this value is LAZILY maintained... i.e. it's a cache
   */
  mutable vector<string> bits;

  void rebuild_path() {
    if (absolute()) 
      path = "/";
    else 
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
      // skip trailing/duplicate slash(es)
      int nextslash = path.find('/', off);
      if (nextslash == off) {
        off++;
        continue;
      }
      if (nextslash < 0) 
        nextslash = path.length();  // no more slashes
      
      bits.push_back( path.substr(off,nextslash-off) );
      off = nextslash+1;
    }
  }

 public:
  filepath() : ino(0) {}
  filepath(const string& s, inodeno_t i=1) : ino(i), path(s) { }
  filepath(const char* s, inodeno_t i=1) : ino(i), path(s) { }
  filepath(const filepath& o) {
    ino = o.ino;
    path = o.path;
    bits = o.bits;
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
  bool empty() const { return path.length() == 0; }

  bool absolute() const { return ino > 0; }
  bool relative() const { return !absolute(); }
  
  const string& operator[](int i) const {
    if (bits.empty() && path.length() > 0) parse_bits();
    return bits[i];
  }

  const string& last_dentry() const {
    if (bits.empty() && path.length() > 0) parse_bits();
    return bits[ bits.size()-1 ];
  }

  filepath prefixpath(int s) const {
    filepath t;
    t.ino = ino;    
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
  void set_path(const string& s) {
    if (s.length() && s[0] == '/') {
      ino = 1;  // relative to root
      path = string(s, 1);
    } else {
      ino = 0;  // strictly relative   
      path = s;
    }
    bits.clear();
  }
  void set_path(const char *s) {
    if (s[0] == '/') {
      ino = 1;  // relative to root
      path = s + 1;
    } else {
      ino = 0;  // strictly relative   
      path = s;
    }
    bits.clear();
  }
  void set_ino(inodeno_t i) { ino = i; }
  void clear() {
    ino = 0;
    path = "";
    bits.clear();
  }

  void pop_dentry() {
    if (bits.empty() && path.length() > 0) parse_bits();
    bits.pop_back();
    rebuild_path();
  }    
  void push_dentry(const string& s) {
    if (bits.empty() && path.length() > 0) parse_bits();
    bits.push_back(s);
    if (path.length() && path[path.length()-1] != '/')
      path += "/";
    path += s;
  }
  void append(const filepath& a) {
    for (unsigned i=0; i<a.depth(); i++) 
      push_dentry(a[i]);
  }

  // encoding
  void _encode(bufferlist& bl) {
    ::_encode_simple(ino, bl);
    ::_encode_simple(path, bl);
  }
  void _decode(bufferlist& bl, int& off) {
    bits.clear();
    ::_decode(ino, bl, off);
    ::_decode(path, bl, off);
  }
  void _decode(bufferlist::iterator& blp) {
    bits.clear();
    ::_decode_simple(ino, blp);
    ::_decode_simple(path, blp);
  }

};

inline ostream& operator<<(ostream& out, filepath& path)
{
  if (path.get_ino() > 1)
    out << '#' << hex << path.get_ino() << dec;
  if (path.get_ino() > 0 && path.depth())
    out << '/';
  return out << path.get_path();
}

#endif
