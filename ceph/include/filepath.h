// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include <ext/rope>
using namespace __gnu_cxx;

#include "buffer.h"


class filepath {
  string path;
  vector<string> bits;

  void rebuild() {
    if (absolute()) 
      path = "/";
    else 
      path.clear();
    for (unsigned i=0; i<bits.size(); i++) {
      if (i) path += "/";
      path += bits[i];
    }
  }
  void parse() {
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
  filepath() {}
  filepath(const string& s) {
    set_path(s);
  }
  filepath(const char* s) {
    set_path(s);
  }

  bool absolute() { return path[0] == '/'; }
  bool relative() { return !absolute(); }
  
  void set_path(const string& s) {
    path = s;
    parse();
  }
  void set_path(const char *s) {
    path = s;
    parse();
  }

  string& get_path() {
    return path;
  }
  int length() const {
    return path.length();
  }

  const char *c_str() const {
    return path.c_str();
  }


  filepath prefixpath(int s) const {
    filepath t;
    for (int i=0; i<s; i++)
      t.add_dentry(bits[i]);
    return t;
  }
  filepath postfixpath(int s) const {
    filepath t;
    for (unsigned i=s; i<bits.size(); i++)
      t.add_dentry(bits[i]);
    return t;
  }
  void add_dentry(const string& s) {
    bits.push_back(s);
    if (path.length())
      path += "/";
    path += s;
  }
  void append(const filepath& a) {
    for (unsigned i=0; i<a.depth(); i++) 
      add_dentry(a[i]);
  }

  void pop_dentry() {
    bits.pop_back();
    rebuild();
  }    
    


  void clear() {
    path = "";
    bits.clear();
  }

  const string& operator[](int i) const {
    return bits[i];
  }

  const string& last_bit() const {
    return bits[ bits.size()-1 ];
  }

  unsigned depth() const {
    return bits.size();
  }
  bool empty() {
    return bits.size() == 0;
  }

  
  void _rope(crope& r) {
    char n = bits.size();
    r.append((char*)&n, sizeof(char));
    for (vector<string>::iterator it = bits.begin();
         it != bits.end();
         it++) { 
      r.append((*it).c_str(), (*it).length()+1);
    }
  }

  void _unrope(crope& r, int& off) {
    clear();

    char n;
    r.copy(off, sizeof(char), (char*)&n);
    off += sizeof(char);
    for (int i=0; i<n; i++) {
      string s = r.c_str() + off;
      off += s.length() + 1;
      add_dentry(s);
    }
  }

  void _encode(bufferlist& bl) {
    char n = bits.size();
    bl.append((char*)&n, sizeof(char));
    for (vector<string>::iterator it = bits.begin();
         it != bits.end();
         it++) { 
      bl.append((*it).c_str(), (*it).length()+1);
    }
  }

  void _decode(bufferlist& bl, int& off) {
    clear();

    char n;
    bl.copy(off, sizeof(char), (char*)&n);
    off += sizeof(char);
    for (int i=0; i<n; i++) {
      string s = bl.c_str() + off;
      off += s.length() + 1;
      add_dentry(s);
    }
  }

};

inline ostream& operator<<(ostream& out, filepath& path)
{
  return out << path.get_path();
}

#endif
