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

// list
template<class T>
inline void _encode_complex(const std::list<T>& ls, bufferlist& bl)
{
  uint32_t n = ls.size();
  _encoderaw(n, bl);
  for (typename std::list<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
    _encode_complex(*p, bl);
}
template<class T>
inline void _decode_complex(std::list<T>& ls, bufferlist& bl, int& off)
{
  uint32_t n;
  _decoderaw(n, bl, off);
  ls.clear();
  while (n--) {
    T v;
    _decode_complex(v, bl, off);
    ls.push_back(v);
  }
}

// deque
template<class T>
inline void _encode_complex(const std::deque<T>& ls, bufferlist& bl)
{
  uint32_t n = ls.size();
  _encoderaw(n, bl);
  for (typename std::deque<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
    _encode_complex(*p, bl);
}
template<class T>
inline void _decode_complex(std::deque<T>& ls, bufferlist& bl, int& off)
{
  uint32_t n;
  _decoderaw(n, bl, off);
  ls.clear();
  while (n--) {
    T v;
    _decode_complex(v, bl, off);
    ls.push_back(v);
  }
}

// set
template<class T>
inline void _encode_complex(const std::set<T>& s, bufferlist& bl)
{
  uint32_t n = s.size();
  _encoderaw(n, bl);
  for (typename std::set<T>::const_iterator p = s.begin(); p != s.end(); ++p)
    _encode_complex(*p, bl);
}
template<class T>
inline void _decode_complex(std::set<T>& s, bufferlist& bl, int& off)
{
  uint32_t n;
  _decoderaw(n, bl, off);
  s.clear();
  while (n--) {
    T v;
    _decode_complex(v, bl, off);
    s.insert(v);
  }
}

// vector
template<class T>
inline void _encode_complex(const std::vector<T>& v, bufferlist& bl)
{
  uint32_t n = v.size();
  _encoderaw(n, bl);
  for (typename std::vector<T>::const_iterator p = v.begin(); p != v.end(); ++p)
    _encode_complex(*p, bl);
}
template<class T>
inline void _decode_complex(std::vector<T>& v, bufferlist& bl, int& off)
{
  uint32_t n;
  _decoderaw(n, bl, off);
  v.resize(n);
  for (uint32_t i=0; i<n; i++) 
    _decode_complex(v[i], bl, off);
}

// map
template<class T, class U>
inline void _encode_complex(const std::map<T,U>& m, bufferlist& bl)
{
  uint32_t n = m.size();
  _encoderaw(n, bl);
  for (typename std::map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    _encode(p->first, bl);
    _encode_complex(p->second, bl);
  }
}
template<class T, class U>
inline void _decode_complex(std::map<T,U>& m, bufferlist& bl, int& off)
{
  uint32_t n;
  _decoderaw(n, bl, off);
  m.clear();
  while (n--) {
    T k;
    _decode(k, bl, off);
    _decode_complex(m[k], bl, off);
  }
}

// hash_map
template<class T, class U>
inline void _encode_complex(const __gnu_cxx::hash_map<T,U>& m, bufferlist& bl)
{
  uint32_t n = m.size();
  _encoderaw(n, bl);
  for (typename __gnu_cxx::hash_map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    _encode(p->first, bl);
    _encode_complex(p->second, bl);
  }
}
template<class T, class U>
inline void _decode_complex(__gnu_cxx::hash_map<T,U>& m, bufferlist& bl, int& off)
{
  uint32_t n;
  _decoderaw(n, bl, off);
  m.clear();
  while (n--) {
    T k;
    _decode(k, bl, off);
    _decode_complex(m[k], bl, off);
  }
}

// base case
template<class T>
inline void _encode_complex(const T& t, bufferlist& bl)
{
  t._encode(bl);
}
template<class T>
inline void _decode_complex(T& t, bufferlist& bl, int& off)
{
  t._decode(bl, off);
}

#endif
