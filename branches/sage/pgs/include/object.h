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

#ifndef __OBJECT_H
#define __OBJECT_H

#include <iostream>
#include <iomanip>
using namespace std;


typedef __uint32_t objectrev_t;

struct object_t {
  static const __uint32_t MAXREV = 0xffffffffU;

  __uint64_t ino;  // "file" identifier
  __uint32_t bno;  // "block" in that "file"
  objectrev_t rev; // revision.  normally ctime (as epoch).

  object_t() : ino(0), bno(0), rev(0) {}
  object_t(__uint64_t i, __uint32_t b) : ino(i), bno(b), rev(0) {}
  object_t(__uint64_t i, __uint32_t b, __uint32_t r) : ino(i), bno(b), rev(r) {}
};


inline bool operator==(const object_t l, const object_t r) {
  return (l.ino == r.ino) && (l.bno == r.bno) && (l.rev == r.rev);
}
inline bool operator!=(const object_t l, const object_t r) {
  return (l.ino != r.ino) || (l.bno != r.bno) || (l.rev != r.rev);
}
inline bool operator>(const object_t l, const object_t r) {
  if (l.ino > r.ino) return true;
  if (l.ino < r.ino) return false;
  if (l.bno > r.bno) return true;
  if (l.bno < r.bno) return false;
  if (l.rev > r.rev) return true;
  return false;
}
inline bool operator<(const object_t l, const object_t r) {
  if (l.ino < r.ino) return true;
  if (l.ino > r.ino) return false;
  if (l.bno < r.bno) return true;
  if (l.bno > r.bno) return false;
  if (l.rev < r.rev) return true;
  return false;
}
inline bool operator>=(const object_t l, const object_t r) { 
  return !(l < r);
}
inline bool operator<=(const object_t l, const object_t r) {
  return !(l > r);
}
inline ostream& operator<<(ostream& out, const object_t o) {
  out << hex << o.ino << '.';
  out.setf(ios::right);
  out.fill('0');
  out << setw(8) << o.bno << dec;
  out.unsetf(ios::right);
  if (o.rev) 
    out << '.' << o.rev;
  return out;
}


namespace __gnu_cxx {
#ifndef __LP64__
  template<> struct hash<__uint64_t> {
    size_t operator()(__uint64_t __x) const { 
      static hash<__uint32_t> H;
      return H((__x >> 32) ^ (__x & 0xffffffff)); 
    }
  };
#endif

  template<> struct hash<object_t> {
    size_t operator()(const object_t &r) const { 
      static hash<__uint64_t>  H;
      static hash<__uint32_t> I;
      return H(r.ino) ^ I(r.bno);
    }
  };
}


#endif
