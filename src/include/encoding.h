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

#ifndef __ENCODING_H
#define __ENCODING_H

#include "inttypes.h"
#include "byteorder.h"
#include "buffer.h"

// --------------------------------------
// base types

template<class T>
inline void encode_raw(const T& t, bufferlist& bl)
{
  bl.append((char*)&t, sizeof(t));
}
template<class T>
inline void decode_raw(T& t, bufferlist::iterator &p)
{
  p.copy(sizeof(t), (char*)&t);
}

#define WRITE_RAW_ENCODER(type)						\
  inline void encode(const type &v, bufferlist& bl) { encode_raw(v, bl); } \
  inline void decode(type &v, bufferlist::iterator& p) { decode_raw(v, p); }

WRITE_RAW_ENCODER(__u8)
WRITE_RAW_ENCODER(__s8)
WRITE_RAW_ENCODER(bool)
WRITE_RAW_ENCODER(char)
WRITE_RAW_ENCODER(__le64)
WRITE_RAW_ENCODER(__le32)
WRITE_RAW_ENCODER(__le16)

// FIXME: we need to choose some portable floating point encoding here
WRITE_RAW_ENCODER(float)
WRITE_RAW_ENCODER(double)



// -----------------------------------
// int types

#define WRITE_INTTYPE_ENCODER(type, etype)				\
  inline void encode(__##type v, bufferlist& bl) {			\
    __##etype e = init_##etype(v);					\
    encode_raw(e, bl);							\
  }									\
  inline void decode(__##type &v, bufferlist::iterator& p) {		\
    __##etype e;							\
    decode_raw(e, p);							\
    v = e;								\
  }

WRITE_INTTYPE_ENCODER(u64, le64)
WRITE_INTTYPE_ENCODER(s64, le64)
WRITE_INTTYPE_ENCODER(u32, le32)
WRITE_INTTYPE_ENCODER(s32, le32)
WRITE_INTTYPE_ENCODER(u16, le16)
WRITE_INTTYPE_ENCODER(s16, le16)



#define WRITE_CLASS_ENCODER(cl) \
  inline void encode(const cl &c, bufferlist &bl) { c.encode(bl); }	\
  inline void decode(cl &c, bufferlist::iterator &p) { c.decode(p); }

#define WRITE_CLASS_ENCODER_MEMBER(cl) \
  inline void encode(const cl &c, bufferlist &bl) const { c.encode(bl); }	\
  inline void decode(cl &c, bufferlist::iterator &p) { c.decode(p); }


// array
template<class A>
inline void encode_array_nohead(const A a[], int n, bufferlist &bl)
{
  for (int i=0; i<n; i++)
    encode(a[n], bl);
}
template<class A>
inline void decode_array_nohead(A a[], int n, bufferlist::iterator &p)
{
  for (int i=0; i<n; i++)
    decode(a[n], p);
}



// -----------------------------
// STL container types

#include <set>
#include <map>
#include <deque>
#include <vector>
#include <string>
#include <ext/hash_map>
#include <ext/hash_set>
#include "triple.h"

// pair
template<class A, class B>
inline void encode(const std::pair<A,B> &p, bufferlist &bl)
{
  encode(p.first, bl);
  encode(p.second, bl);
}
template<class A, class B>
inline void decode(std::pair<A,B> &pa, bufferlist::iterator &p)
{
  decode(pa.first, p);
  decode(pa.second, p);
}

// triple
template<class A, class B, class C>
inline void encode(const triple<A,B,C> &t, bufferlist &bl)
{
  encode(t.first, bl);
  encode(t.second, bl);
  encode(t.third, bl);
}
template<class A, class B, class C>
inline void decode(triple<A,B,C> &t, bufferlist::iterator &p)
{
  decode(t.first, p);
  decode(t.second, p);
  decode(t.third, p);
}


// list
template<class T>
inline void encode(const std::list<T>& ls, bufferlist& bl)
{
  // should i pre- or post- count?
  if (!ls.empty()) {
    unsigned pos = bl.length();
    unsigned n = 0;
    encode(n, bl);
    for (typename std::list<T>::const_iterator p = ls.begin(); p != ls.end(); ++p) {
      n++;
      encode(*p, bl);
    }
    __le32 en;
    en = n;
    bl.copy_in(pos, sizeof(en), (char*)&en);
  } else {
    __u32 n = ls.size();    // FIXME: this is slow on a list.
    encode(n, bl);
    for (typename std::list<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
      encode(*p, bl);
  }
}
template<class T>
inline void decode(std::list<T>& ls, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    T v;
    decode(v, p);
    ls.push_back(v);
  }
}

// deque
template<class T>
inline void encode(const std::deque<T>& ls, bufferlist& bl)
{
  __u32 n = ls.size();
  encode(n, bl);
  for (typename std::deque<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode(std::deque<T>& ls, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    T v;
    decode(v, p);
    ls.push_back(v);
  }
}

// set
template<class T>
inline void encode(const std::set<T>& s, bufferlist& bl)
{
  __u32 n = s.size();
  encode(n, bl);
  for (typename std::set<T>::const_iterator p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode(std::set<T>& s, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  s.clear();
  while (n--) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

// vector (pointers)
/*template<class T>
inline void encode(const std::vector<T*>& v, bufferlist& bl)
{
  __u32 n = v.size();
  encode(n, bl);
  for (typename std::vector<T*>::const_iterator p = v.begin(); p != v.end(); ++p)
    encode(**p, bl);
}
template<class T>
inline void decode(std::vector<T*>& v, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  v.resize(n);
  for (__u32 i=0; i<n; i++) 
    v[i] = new T(p);
}
*/
// vector
template<class T>
inline void encode(const std::vector<T>& v, bufferlist& bl)
{
  __u32 n = v.size();
  encode(n, bl);
  for (typename std::vector<T>::const_iterator p = v.begin(); p != v.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode(std::vector<T>& v, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  v.resize(n);
  for (__u32 i=0; i<n; i++) 
    decode(v[i], p);
}

template<class T>
inline void encode_nohead(const std::vector<T>& v, bufferlist& bl)
{
  for (typename std::vector<T>::const_iterator p = v.begin(); p != v.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode_nohead(int len, std::vector<T>& v, bufferlist::iterator& p)
{
  v.resize(len);
  for (__u32 i=0; i<v.size(); i++) 
    decode(v[i], p);
}

// map (pointers)
/*
template<class T, class U>
inline void encode(const std::map<T,U*>& m, bufferlist& bl)
{
  __u32 n = m.size();
  encode(n, bl);
  for (typename std::map<T,U*>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(*p->second, bl);
  }
}
template<class T, class U>
inline void decode(std::map<T,U*>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    m[k] = new U(p);
  }
  }*/

// map
template<class T, class U>
inline void encode(const std::map<T,U>& m, bufferlist& bl)
{
  __u32 n = m.size();
  encode(n, bl);
  for (typename std::map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U>
inline void decode(std::map<T,U>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}
template<class T, class U>
inline void encode_nohead(const std::map<T,U>& m, bufferlist& bl)
{
  for (typename std::map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U>
inline void decode_nohead(int n, std::map<T,U>& m, bufferlist::iterator& p)
{
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}

// multimap
template<class T, class U>
inline void encode(const std::multimap<T,U>& m, bufferlist& bl)
{
  __u32 n = m.size();
  encode(n, bl);
  for (typename std::multimap<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U>
inline void decode(std::multimap<T,U>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    typename std::multimap<T,U>::iterator it;
    U u;
    it = m.insert(std::pair<T,U>(k, u));
    decode(it->second, p);
  }
}

// hash_map
template<class T, class U>
inline void encode(const __gnu_cxx::hash_map<T,U>& m, bufferlist& bl)
{
  __u32 n = m.size();
  encode(n, bl);
  for (typename __gnu_cxx::hash_map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U>
inline void decode(__gnu_cxx::hash_map<T,U>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}

// hash_set
template<class T>
inline void encode(const __gnu_cxx::hash_set<T>& m, bufferlist& bl)
{
  __u32 n = m.size();
  encode(n, bl);
  for (typename __gnu_cxx::hash_set<T>::const_iterator p = m.begin(); p != m.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode(__gnu_cxx::hash_set<T>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    m.insert(k);
  }
}

// string
inline void encode(const std::string& s, bufferlist& bl) 
{
  __u32 len = s.length();
  encode(len, bl);
  bl.append(s.data(), len);
}
inline void decode(std::string& s, bufferlist::iterator& p)
{
  __u32 len;
  decode(len, p);
  s.clear();
  p.copy(len, s);
}

// const char* (encode only, string compatible)
inline void encode(const char *s, bufferlist& bl) 
{
  __u32 len = strlen(s);
  encode(len, bl);
  bl.append(s, len);
}



// -----------------------------
// buffers

// bufferptr (encapsulated)
inline void encode(const buffer::ptr& bp, bufferlist& bl) 
{
  __u32 len = bp.length();
  encode(len, bl);
  if (len)
    bl.append(bp);
}
inline void decode(buffer::ptr& bp, bufferlist::iterator& p)
{
  __u32 len;
  decode(len, p);

  bufferlist s;
  p.copy(len, s);

  if (len) {
    if (s.buffers().size() == 1)
      bp = s.buffers().front();
    else
      bp = buffer::copy(s.c_str(), s.length());
  }
}

// bufferlist (encapsulated)
inline void encode(const bufferlist& s, bufferlist& bl) 
{
  __u32 len = s.length();
  encode(len, bl);
  bl.append(s);
}
inline void encode_destructively(bufferlist& s, bufferlist& bl) 
{
  __u32 len = s.length();
  encode(len, bl);
  bl.claim_append(s);
}
inline void decode(bufferlist& s, bufferlist::iterator& p)
{
  __u32 len;
  decode(len, p);
  s.clear();
  p.copy(len, s);
}

inline void encode_nohead(const bufferlist& s, bufferlist& bl) 
{
  bl.append(s);
}
inline void decode_nohead(int len, bufferlist& s, bufferlist::iterator& p)
{
  s.clear();
  p.copy(len, s);
}


// full bl decoder
template<class T>
inline void decode(T o, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  decode(o, p);
  assert(p.end());
}

#endif
