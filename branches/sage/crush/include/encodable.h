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

#ifndef __ENCODABLE_H
#define __ENCODABLE_H

#include "buffer.h"

#include <set>
#include <map>
#include <deque>
#include <vector>
#include <string>
#include <ext/hash_map>


// ==================================================================
// simple


// raw
template<class T>
inline void _encode_raw(const T& t, bufferlist& bl)
{
  bl.append((char*)&t, sizeof(t));
}
template<class T>
inline void _decode_raw(T& t, bufferlist::iterator &p)
{
  p.copy(sizeof(t), (char*)&t);
}

#include <set>
#include <map>
#include <deque>
#include <vector>
#include <string>
#include <ext/hash_map>

// list
template<class T>
inline void _encode_simple(const std::list<T>& ls, bufferlist& bl)
{
  // should i pre- or post- count?
  if (!ls.empty()) {
    unsigned pos = bl.length();
    uint32_t n = 0;
    _encode_raw(n, bl);
    for (typename std::list<T>::const_iterator p = ls.begin(); p != ls.end(); ++p) {
      n++;
      _encode_simple(*p, bl);
    }
    bl.copy_in(pos, sizeof(n), (char*)&n);
  } else {
    uint32_t n = ls.size();    // FIXME: this is slow on a list.
    _encode_raw(n, bl);
    for (typename std::list<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
      _encode_simple(*p, bl);
  }
}
template<class T>
inline void _decode_simple(std::list<T>& ls, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  ls.clear();
  while (n--) {
    T v;
    _decode_simple(v, p);
    ls.push_back(v);
  }
}

// deque
template<class T>
inline void _encode_simple(const std::deque<T>& ls, bufferlist& bl)
{
  uint32_t n = ls.size();
  _encode_raw(n, bl);
  for (typename std::deque<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
    _encode_simple(*p, bl);
}
template<class T>
inline void _decode_simple(std::deque<T>& ls, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  ls.clear();
  while (n--) {
    T v;
    _decode_simple(v, p);
    ls.push_back(v);
  }
}

// set
template<class T>
inline void _encode_simple(const std::set<T>& s, bufferlist& bl)
{
  uint32_t n = s.size();
  _encode_raw(n, bl);
  for (typename std::set<T>::const_iterator p = s.begin(); p != s.end(); ++p)
    _encode_simple(*p, bl);
}
template<class T>
inline void _decode_simple(std::set<T>& s, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  s.clear();
  while (n--) {
    T v;
    _decode_simple(v, p);
    s.insert(v);
  }
}

// vector
template<class T>
inline void _encode_simple(const std::vector<T>& v, bufferlist& bl)
{
  uint32_t n = v.size();
  _encode_raw(n, bl);
  for (typename std::vector<T>::const_iterator p = v.begin(); p != v.end(); ++p)
    _encode_simple(*p, bl);
}
template<class T>
inline void _decode_simple(std::vector<T>& v, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  v.resize(n);
  for (uint32_t i=0; i<n; i++) 
    _decode_simple(v[i], p);
}

// map
template<class T, class U>
inline void _encode_simple(const std::map<T,U>& m, bufferlist& bl)
{
  uint32_t n = m.size();
  _encode_raw(n, bl);
  for (typename std::map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    _encode_simple(p->first, bl);
    _encode_simple(p->second, bl);
  }
}
template<class T, class U>
inline void _decode_simple(std::map<T,U>& m, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  m.clear();
  while (n--) {
    T k;
    _decode_simple(k, p);
    _decode_simple(m[k], p);
  }
}

// hash_map
template<class T, class U>
inline void _encode_simple(const __gnu_cxx::hash_map<T,U>& m, bufferlist& bl)
{
  uint32_t n = m.size();
  _encode_raw(n, bl);
  for (typename __gnu_cxx::hash_map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    _encode_simple(p->first, bl);
    _encode_simple(p->second, bl);
  }
}
template<class T, class U>
inline void _decode_simple(__gnu_cxx::hash_map<T,U>& m, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  m.clear();
  while (n--) {
    T k;
    _decode_simple(k, p);
    _decode_simple(m[k], p);
  }
}

// string
inline void _encode_simple(const std::string& s, bufferlist& bl) 
{
  uint32_t len = s.length();
  _encode_raw(len, bl);
  bl.append(s.data(), len);
}
inline void _decode_simple(std::string& s, bufferlist::iterator& p)
{
  uint32_t len;
  _decode_raw(len, p);
  s.clear();
  p.copy(len, s);
}

// const char* (encode only, string compatible)
inline void _encode_simple(const char *s, bufferlist& bl) 
{
  uint32_t len = strlen(s);
  _encode_raw(len, bl);
  bl.append(s, len);
}

// bufferptr (encapsulated)
inline void _encode_simple(const buffer::ptr& bp, bufferlist& bl) 
{
  uint32_t len = bp.length();
  _encode_raw(len, bl);
  bl.append(bp);
}
inline void _decode_simple(buffer::ptr& bp, bufferlist::iterator& p)
{
  uint32_t len;
  _decode_raw(len, p);

  bufferlist s;
  p.copy(len, s);

  if (s.buffers().size() == 1)
    bp = s.buffers().front();
  else
    bp = buffer::copy(s.c_str(), s.length());
}

// bufferlist (encapsulated)
inline void _encode_simple(const bufferlist& s, bufferlist& bl) 
{
  uint32_t len = s.length();
  _encode_raw(len, bl);
  bl.append(s);
}
inline void _encode_simple_destructively(bufferlist& s, bufferlist& bl) 
{
  uint32_t len = s.length();
  _encode_raw(len, bl);
  bl.claim_append(s);
}
inline void _decode_simple(bufferlist& s, bufferlist::iterator& p)
{
  uint32_t len;
  _decode_raw(len, p);
  s.clear();
  p.copy(len, s);
}

// base
template<class T>
inline void _encode_simple(const T& t, bufferlist& bl)
{
  _encode_raw(t, bl);
}
template<class T>
inline void _decode_simple(T& t, bufferlist::iterator& p)
{
  _decode_raw(t, p);
}




// ==================================================================
// complex

// list
template<class T>
inline void _encode_complex(const std::list<T>& ls, bufferlist& bl)
{
  uint32_t n = ls.size();
  _encode_raw(n, bl);
  for (typename std::list<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
    _encode_complex(*p, bl);
}
template<class T>
inline void _decode_complex(std::list<T>& ls, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  ls.clear();
  while (n--) {
    T v;
    _decode_complex(v, p);
    ls.push_back(v);
  }
}

// deque
template<class T>
inline void _encode_complex(const std::deque<T>& ls, bufferlist& bl)
{
  uint32_t n = ls.size();
  _encode_raw(n, bl);
  for (typename std::deque<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
    _encode_complex(*p, bl);
}
template<class T>
inline void _decode_complex(std::deque<T>& ls, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  ls.clear();
  while (n--) {
    T v;
    _decode_complex(v, p);
    ls.push_back(v);
  }
}

// set
template<class T>
inline void _encode_complex(const std::set<T>& s, bufferlist& bl)
{
  uint32_t n = s.size();
  _encode_raw(n, bl);
  for (typename std::set<T>::const_iterator p = s.begin(); p != s.end(); ++p)
    _encode_complex(*p, bl);
}
template<class T>
inline void _decode_complex(std::set<T>& s, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  s.clear();
  while (n--) {
    T v;
    _decode_complex(v, p);
    s.insert(v);
  }
}

// vector
template<class T>
inline void _encode_complex(const std::vector<T>& v, bufferlist& bl)
{
  uint32_t n = v.size();
  _encode_raw(n, bl);
  for (typename std::vector<T>::const_iterator p = v.begin(); p != v.end(); ++p)
    _encode_complex(*p, bl);
}
template<class T>
inline void _decode_complex(std::vector<T>& v, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  v.resize(n);
  for (uint32_t i=0; i<n; i++) 
    _decode_complex(v[i], p);
}

// map
template<class T, class U>
inline void _encode_complex(const std::map<T,U>& m, bufferlist& bl)
{
  uint32_t n = m.size();
  _encode_raw(n, bl);
  for (typename std::map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    _encode_simple(p->first, bl);
    _encode_complex(p->second, bl);
  }
}
template<class T, class U>
inline void _decode_complex(std::map<T,U>& m, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  m.clear();
  while (n--) {
    T k;
    _decode_simple(k, p);
    _decode_complex(m[k], p);
  }
}

// hash_map
template<class T, class U>
inline void _encode_complex(const __gnu_cxx::hash_map<T,U>& m, bufferlist& bl)
{
  uint32_t n = m.size();
  _encode_raw(n, bl);
  for (typename __gnu_cxx::hash_map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    _encode_simple(p->first, bl);
    _encode_complex(p->second, bl);
  }
}
template<class T, class U>
inline void _decode_complex(__gnu_cxx::hash_map<T,U>& m, bufferlist::iterator& p)
{
  uint32_t n;
  _decode_raw(n, p);
  m.clear();
  while (n--) {
    T k;
    _decode_simple(k, p);
    _decode_complex(m[k], p);
  }
}

// base case
template<class T>
inline void _encode_complex(const T& t, bufferlist& bl)
{
  t._encode(bl);
}
template<class T>
inline void _decode_complex(T& t, bufferlist::iterator& p)
{
  t._decode(p);
}

#endif
