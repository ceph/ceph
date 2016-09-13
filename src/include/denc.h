// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Allen Samuels <allen.samuels@sandisk.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

// If you #include "include/encoding.h" you get the old-style *and*
// the new-style definitions.  (The old-style needs denc_traits<> in
// order to disable the container helpers when new-style traits are
// present.)

// You can also just #include "include/denc.h" and get only the
// new-style helpers.  The eventual goal is to drop the legacy
// definitions.

#ifndef _ENC_DEC_H
#define _ENC_DEC_H

#include <set>
#include <map>
#include <vector>
#include <string>
#include <string.h>
#include <type_traits>
#include <boost/intrusive/set.hpp>

#include "include/int_types.h"
#include "include/intarith.h"
#include "include/memory.h"
#include "byteorder.h"
#include "buffer.h"

template<typename T, typename VVV=void>
struct denc_traits {
  enum { supported = 0 };
  enum { featured = false };
  enum { bounded = false };
};


// hack for debug only; FIXME
//#include <iostream>
//using std::cout;



/*

  top level level functions look like so
  ======================================

    inline void denc(const T& o, size_t& p, uint64_t features=0);
    inline void denc(const T& o, buffer::list::contiguous_appender& p,
                     uint64_t features=0);
    inline void denc(T& o, buffer::ptr::iterator& p);

  or (for featured objects)

    inline void denc(const T& o, size_t& p, uint64_t features);
    inline void denc(const T& o, buffer::list::contiguous_appender& p,
                     uint64_t features);
    inline void denc(T& o, buffer::ptr::iterator& p);

  - These are symmetrical, so that they can be used from the magic DENC
  method of writing the bound_encode/encode/decode methods all in one go;
  they differ only in the type of p.  The feature argument for decode is
  ignored.

  - These are automatically fabricated via a template that calls into
  the denc_traits<> methods (see below), provided denc_traits<T>::supported
  is defined and true.  They never need to be written explicitly.


  static denc_traits<> definitions look like so
  =============================================

    template<>
    struct denc_traits<T> {
      enum { supported = true };
      enum { bounded = false };
      enum { featured = false };
      static void bound_encode(const T &o, size_t& p, uint64_t f=0);
      static void encode(const T &o, buffer::list::contiguous_appender& p,
		         uint64_t f=0);
      static void decode(T& o, buffer::ptr::iterator &p);
    };

  or (for featured objects)

    template<>
    struct denc_traits<T> {
      enum { supported = true };
      enum { bounded = false };
      enum { featured = true };
      static void bound_encode(const T &o, size_t& p, uint64_t f);
      static void encode(const T &o, buffer::list::contiguous_appender& p,
		         uint64_t f);
      static void decode(T& o, buffer::ptr::iterator &p);
    };

  - denc_traits<T> is normally declared via the WRITE_CLASS_DENC(type) macro,
  which is used in place of the old-style WRITE_CLASS_ENCODER(type) macro.
  There are _FEATURED and _BOUNDED variants.  The class traits simply call
  into class methods of the same name (see below).

  - denc_traits<T> can also be written explicitly for some type to indicate
  how it should be encoded.  This is the "source of truth" for how a type
  is encoded.

  - denc_traits<T> are declared for the base integer types, string, bufferptr,
  and bufferlist base types.

  - denc_traits<std::foo<T>>-like traits are declared for standard container
  types.


  class methods look like so
  ==========================

    void bound_encode(size_t& p) const;
    void encode(buffer::list::contiguous_appender& p) const;
    void decode(buffer::ptr::iterator &p);

  or (for featured objects)

    void bound_encode(size_t& p, uint64_t f) const;
    void encode(buffer::list::contiguous_appender& p, uint64_t f) const;
    void decode(buffer::ptr::iterator &p);

  - These are normally invoked by the denc_traits<> methods that are
  declared via WRITE_CLASS_DENC, although you can also invoke them explicitly
  in your code.

  - These can be defined either explicitly (as above), or can be "magically"
  defined all in one go using the DENC macro and DENC_{START,FINISH} helpers
  (which work like the legacy {ENCODE,DECODE}_{START,FINISH} macros):

    class foo_t {
      ...
      DENC(foo_t, v, p) {
        DENC_START(1, 1, p);
        denc(v.foo, p);
        denc(v.bar, p);
        denc(v.baz, p);
        DENC_FINISH(p);
      }
      ...
    };
    WRITE_CLASS_DENC(foo_t)

  */


// ---------------------------------------------------------------------
// raw types

#define WRITE_RAW_DENC(type)						\
  template<>								\
  struct denc_traits<type> {						\
    enum { supported = 2 };						\
    enum { featured = false };						\
    enum { bounded = true };						\
    static void bound_encode(const type &o, size_t& p, uint64_t f=0) {	\
      p += sizeof(type);						\
    }									\
    static void encode(const type &o,					\
		       buffer::list::contiguous_appender& p,		\
		       uint64_t f=0) {					\
      p.append((const char*)&o, sizeof(o));				\
    }									\
    static void decode(type& o, buffer::ptr::iterator &p) {		\
      o = *(type *)p.get_pos_add(sizeof(o));				\
    }									\
  };

WRITE_RAW_DENC(ceph_le64)
WRITE_RAW_DENC(ceph_le32)
WRITE_RAW_DENC(ceph_le16)
WRITE_RAW_DENC(uint8_t);
#ifndef _CHAR_IS_SIGNED
WRITE_RAW_DENC(int8_t);
#endif



// -----------------------------------------------------------------------
// integer types

// itype == internal type
// otype == external type, i.e., the type on the wire

// NOTE: set supported == 2 instead of true.  This prevents these from
// getting glued into the legacy encode/decode methods; the overhead
// of setting up a contiguous_appender etc is likely to be slower.

#define WRITE_INT_DENC(itype, etype)					\
  template<>								\
  struct denc_traits<itype> {						\
    enum { supported = 2 };						\
    enum { featured = false };						\
    enum { bounded = true };						\
    static void bound_encode(const itype &o, size_t& p, uint64_t f=0) {	\
      p += sizeof(etype);						\
    }									\
    static void encode(const itype &o, buffer::list::contiguous_appender& p, \
		       uint64_t f=0) {					\
      *(etype *)p.get_pos_add(sizeof(etype)) = o;			\
    }									\
    static void decode(itype& o, buffer::ptr::iterator &p) {		\
      o = *(etype*)p.get_pos_add(sizeof(etype));				\
    }									\
  };

WRITE_INT_DENC(uint16_t, __le16);
WRITE_INT_DENC(int16_t, __le16);
WRITE_INT_DENC(uint32_t, __le32);
WRITE_INT_DENC(int32_t, __le32);
WRITE_INT_DENC(uint64_t, __le64);
WRITE_INT_DENC(int64_t, __le64);


// varint
//
// high bit of each byte indicates another byte follows.
template<typename T>
inline void denc_varint(T v, size_t& p) {
  p += sizeof(T) + 1;
}

template<typename T>
inline void denc_varint(T v, bufferlist::contiguous_appender& p) {
  uint8_t byte = v & 0x7f;
  v >>= 7;
  while (v) {
    byte |= 0x80;
    *(__u8*)p.get_pos_add(1) = byte;
    byte = (v & 0x7f);
    v >>= 7;
  }
  *(__u8*)p.get_pos_add(1) = byte;
}

template<typename T>
inline void denc_varint(T& v, bufferptr::iterator& p) {
  uint8_t byte = *(__u8*)p.get_pos_add(1);
  v = byte & 0x7f;
  int shift = 7;
  while (byte & 0x80) {
    byte = *(__u8*)p.get_pos_add(1);
    v |= (T)(byte & 0x7f) << shift;
    shift += 7;
  }
}


// signed varint encoding
//
// low bit = 1 = negative, 0 = positive
// high bit of every byte indicates whether another byte follows.
inline void denc_signed_varint(int64_t v, size_t& p) {
  p += sizeof(v) + 2;
}
inline void denc_signed_varint(int64_t v, bufferlist::contiguous_appender& p) {
  if (v < 0) {
    v = (-v << 1) | 1;
  } else {
    v <<= 1;
  }
  denc_varint(v, p);
}

template<typename T>
inline void denc_signed_varint(T& v, bufferptr::iterator& p)
{
  int64_t i;
  denc_varint(i, p);
  if (i & 1) {
    v = -(i >> 1);
  } else {
    v = i >> 1;
  }
}

// varint + lowz encoding
//
// first(low) 2 bits = how many low zero bits (nibbles)
// high bit of each byte = another byte follows
// (so, 5 bits data in first byte, 7 bits data thereafter)
inline void denc_varint_lowz(uint64_t v, size_t& p) {
  p += sizeof(v) + 2;
}
inline void denc_varint_lowz(uint64_t v, bufferlist::contiguous_appender& p) {
  int lowznib = v ? (ctz(v) / 4) : 0;
  if (lowznib > 3)
    lowznib = 3;
  v >>= lowznib * 4;
  v <<= 2;
  v |= lowznib;
  denc_varint(v, p);
}

template<typename T>
inline void denc_varint_lowz(T& v, bufferptr::iterator& p)
{
  uint64_t i;
  denc_varint(i, p);
  int lowznib = (i & 3);
  i >>= 2;
  i <<= lowznib * 4;
  v = i;
}

// signed varint + lowz encoding
//
// first low bit = 1 for negative, 0 for positive
// next 2 bits = how many low zero bits (nibbles)
// high bit of each byte = another byte follows
// (so, 4 bits data in first byte, 7 bits data thereafter)
inline void denc_signed_varint_lowz(int64_t v, size_t& p) {
  p += sizeof(v) + 2;
}
inline void denc_signed_varint_lowz(int64_t v,
				    bufferlist::contiguous_appender& p) {
  bool negative = false;
  if (v < 0) {
    v = -v;
    negative = true;
  }
  int lowznib = v ? (ctz(v) / 4) : 0;
  if (lowznib > 3)
    lowznib = 3;
  v >>= lowznib * 4;
  v <<= 3;
  v |= lowznib << 1;
  v |= (int)negative;
  denc_varint(v, p);
}

template<typename T>
inline void denc_signed_varint_lowz(T& v, bufferptr::iterator& p)
{
  int64_t i;
  denc_varint(i, p);
  int lowznib = (i & 6) >> 1;
  if (i & 1) {
    i >>= 3;
    i <<= lowznib * 4;
    v = -i;
  } else {
    i >>= 3;
    i <<= lowznib * 4;
    v = i;
  }
}


// LBA
//
// first 1-3 bits = how many low zero bits
//     *0 = 12 (common 4 K alignment case)
//    *01 = 16
//   *011 = 20
//   *111 = byte
// then 28-30 bits of data
// then last bit = another byte follows
// high bit of each subsequent byte = another byte follows
inline void denc_lba(uint64_t v, size_t& p) {
  p += sizeof(v) + 2;
}

inline void denc_lba(uint64_t v, bufferlist::contiguous_appender& p) {
  int low_zero_nibbles = v ? (int)(ctz(v) / 4) : 0;
  int pos;
  uint32_t word;
  int t = low_zero_nibbles - 3;
  if (t < 0) {
    pos = 3;
    word = 0x7;
  } else if (t < 3) {
    v >>= (low_zero_nibbles * 4);
    pos = t + 1;
    word = (1 << t) - 1;
  } else {
    v >>= 20;
    pos = 3;
    word = 0x3;
  }
  word |= (v << pos) & 0x7fffffff;
  v >>= 31 - pos;
  if (!v) {
    *(__le32*)p.get_pos_add(sizeof(uint32_t)) = word;
    return;
  }
  word |= 0x80000000;
  *(__le32*)p.get_pos_add(sizeof(uint32_t)) = word;
  uint8_t byte = v & 0x7f;
  v >>= 7;
  while (v) {
    byte |= 0x80;
    *(__u8*)p.get_pos_add(1) = byte;
    byte = (v & 0x7f);
    v >>= 7;
  }
  *(__u8*)p.get_pos_add(1) = byte;
}

inline void denc_lba(uint64_t& v, bufferptr::iterator& p) {
  uint32_t word = *(__le32*)p.get_pos_add(sizeof(uint32_t));
  int shift;
  switch (word & 7) {
  case 0:
  case 2:
  case 4:
  case 6:
    v = (uint64_t)(word & 0x7ffffffe) << (12 - 1);
    shift = 12 + 30;
    break;
  case 1:
  case 5:
    v = (uint64_t)(word & 0x7ffffffc) << (16 - 2);
    shift = 16 + 29;
    break;
  case 3:
    v = (uint64_t)(word & 0x7ffffff8) << (20 - 3);
    shift = 20 + 28;
    break;
  case 7:
    v = (uint64_t)(word & 0x7ffffff8) >> 3;
    shift = 28;
  }
  uint8_t byte = word >> 24;
  while (byte & 0x80) {
    byte = *(__u8*)p.get_pos_add(1);
    v |= (uint64_t)(byte & 0x7f) << shift;
    shift += 7;
  }
}


// ---------------------------------------------------------------------
// denc top-level methods that call into denc_traits<T> methods

template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
			       !traits::featured>::type denc(
  const T& o,
  size_t& p,
  uint64_t f=0)
{
  traits::bound_encode(o, p);
}
template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
			       traits::featured>::type denc(
  const T& o,
  size_t& p,
  uint64_t f)
{
  traits::bound_encode(o, p, f);
}

template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
			       !traits::featured>::type denc(
  const T& o,
  buffer::list::contiguous_appender& p,
  uint64_t features=0)
{
  traits::encode(o, p);
}
template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
			       traits::featured>::type denc(
  const T& o,
  buffer::list::contiguous_appender& p,
  uint64_t features)
{
  traits::encode(o, p, features);
}

template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
			       !traits::featured>::type denc(
  T& o,
  buffer::ptr::iterator& p,
  uint64_t features=0)
{
  traits::decode(o, p);
}
template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
			       traits::featured>::type denc(
  T& o,
  buffer::ptr::iterator& p,
  uint64_t features=0)
{
  traits::decode(o, p);
}


// ---------------------------------------------------------------------
// base types and containers

//
// std::string
//
template<>
struct denc_traits<std::string> {
  enum { supported = true };
  enum { featured = false };
  enum { bounded = false };
  static void bound_encode(const std::string& s, size_t& p, uint64_t f=0) {
    p += sizeof(uint32_t) + s.size();
  }
  static void encode(const std::string& s, buffer::list::contiguous_appender& p,
	      uint64_t f=0) {
    ::denc((uint32_t)s.size(), p);
    memcpy(p.get_pos_add(s.size()), s.data(), s.size());
  }
  static void decode(std::string& s, buffer::ptr::iterator& p, uint64_t f=0) {
    uint32_t len;
    ::denc(len, p);
    s.clear();
    if (len) {
      s.append(p.get_pos_add(len), len);
    }
  }
};

//
// bufferptr
//
template<>
struct denc_traits<bufferptr> {
  enum { supported = 2 };
  enum { featured = false };
  enum { bounded = false };
  static void bound_encode(const bufferptr& v, size_t& p, uint64_t f=0) {
    p += sizeof(uint32_t) + v.length();
  }
  static void encode(const bufferptr& v, buffer::list::contiguous_appender& p,
	      uint64_t f=0) {
    ::denc((uint32_t)v.length(), p);
    p.append(v);
  }
  static void decode(bufferptr& v, buffer::ptr::iterator& p, uint64_t f=0) {
    uint32_t len;
    ::denc(len, p);
    v = p.get_ptr(len);
  }
};

//
// bufferlist
//
template<>
struct denc_traits<bufferlist> {
  enum { supported = 2 };
  enum { featured = false };
  enum { bounded = false };
  static void bound_encode(const bufferlist& v, size_t& p, uint64_t f=0) {
    p += sizeof(uint32_t) + v.length();
  }
  static void encode(const bufferlist& v, buffer::list::contiguous_appender& p,
	      uint64_t f=0) {
    ::denc((uint32_t)v.length(), p);
    p.append(v);
  }
  static void decode(bufferlist& v, buffer::ptr::iterator& p, uint64_t f=0) {
    uint32_t len;
    ::denc(len, p);
    v.clear();
    v.push_back(p.get_ptr(len));
  }
};

//
// std::pair<A, B>
//
template<typename A, typename B>
struct denc_traits<
  std::pair<A, B>,
  typename std::enable_if<denc_traits<A>::supported &&
			  denc_traits<B>::supported>::type> {
  typedef denc_traits<A> a_traits;
  typedef denc_traits<B> b_traits;

  enum { supported = true };
  enum { featured = a_traits::featured || b_traits::featured };
  enum { bounded = a_traits::bounded && b_traits::bounded };

  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) &&
				 !featured>::type
  bound_encode(const std::pair<A,B>& v, size_t& p) {
    denc(v.first, p);
    denc(v.second, p);
  }
  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) &&
				 featured, void>::type
  bound_encode(const std::pair<A,B>& v, size_t& p, uint64_t f) {
    denc(v.first, p, f);
    denc(v.second, p, f);
  }

  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) &&
				 !featured>::type
  encode(const std::pair<A,B>& v, bufferlist::contiguous_appender& p) {
    denc(v.first, p);
    denc(v.second, p);
  }
  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) &&
				 featured, void>::type
  encode(const std::pair<A,B>& v, bufferlist::contiguous_appender& p,
	   uint64_t f) {
    denc(v.first, p, f);
    denc(v.second, p, f);
  }

  static void decode(std::pair<A,B>& v, buffer::ptr::iterator& p) {
    denc(v.first, p);
    denc(v.second, p);
  }
};

//
// std::list<T>
//
template<typename T>
struct denc_traits<
  std::list<T>,
  typename std::enable_if<denc_traits<T>::supported>::type> {
  typedef denc_traits<T> traits;

  enum { supported = true };
  enum { featured = traits::featured };
  enum { bounded = false };

  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 !traits::bounded &&
                                 !traits::featured>::type
  bound_encode(const std::list<T>& s, size_t& p) {
    p += sizeof(uint32_t);
    for (const T& e : s) {
      denc(e, p);
    }
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 traits::bounded &&
                                 !traits::featured, void>::type
  bound_encode(const std::list<T>& s, size_t& p) {
    size_t elem_size = 0;
    denc(*(const T*)nullptr, elem_size);
    p += sizeof(uint32_t) + elem_size * s.size();
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 !traits::bounded &&
                                 traits::featured, void>::type
  bound_encode(const std::list<T>& s, size_t& p, uint64_t f) {
    p += sizeof(uint32_t);
    for (const T& e : s) {
      denc(e, p, f);
    }
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 traits::bounded &&
                                 traits::featured>::type
  bound_encode(const std::list<T>& s, size_t& p, uint64_t f) {
    size_t elem_size = 0;
    denc(*(const T*)nullptr, elem_size, f);
    p += sizeof(uint32_t) + elem_size * s.size();
  }

  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 !traits::featured>::type
  encode(const std::list<T>& s, buffer::list::contiguous_appender& p) {
    denc((uint32_t)s.size(), p);
    for (const T& e : s) {
      denc(e, p);
    }
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 traits::featured>::type
    encode(const std::list<T>& s, buffer::list::contiguous_appender& p,
	   uint64_t f) {
    denc((uint32_t)s.size(), p);
    for (const T& e : s) {
      denc(e, p, f);
    }
  }
  static void decode(std::list<T>& s, buffer::ptr::iterator& p) {
    s.clear();
    uint32_t num;
    denc(num, p);
    while (num--) {
      s.emplace_back(T());
      denc(s.back(), p);
    }
  }
};

//
// std::vector<T>
//
template<typename T>
struct denc_traits<
  std::vector<T>,
  typename std::enable_if<denc_traits<T>::supported>::type> {
  typedef denc_traits<T> traits;

  enum { supported = true };
  enum { featured = traits::featured };
  enum { bounded = false };

  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 !traits::bounded &&
                                 !traits::featured>::type
  bound_encode(const std::vector<T>& s, size_t& p) {
    p += sizeof(uint32_t);
    for (const T& e : s) {
      denc(e, p);
    }
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 traits::bounded &&
                                 !traits::featured, void>::type
  bound_encode(const std::vector<T>& s, size_t& p) {
    size_t elem_size = 0;
    denc(*(const T*)nullptr, elem_size);
    p += sizeof(uint32_t) + elem_size * s.size();
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 !traits::bounded &&
                                 traits::featured, void>::type
  bound_encode(const std::vector<T>& s, size_t& p, uint64_t f) {
    p += sizeof(uint32_t);
    for (const T& e : s) {
      denc(e, p, f);
    }
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 traits::bounded &&
                                 traits::featured>::type
  bound_encode(const std::vector<T>& s, size_t& p, uint64_t f) {
    size_t elem_size = 0;
    denc(*(const T*)nullptr, elem_size, f);
    p += sizeof(uint32_t) + elem_size * s.size();
  }

  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 !traits::featured>::type
  encode(const std::vector<T>& s, buffer::list::contiguous_appender& p) {
    denc((uint32_t)s.size(), p);
    for (const T& e : s) {
      denc(e, p);
    }
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 traits::featured>::type
    encode(const std::vector<T>& s, buffer::list::contiguous_appender& p,
	   uint64_t f) {
    denc((uint32_t)s.size(), p);
    for (const T& e : s) {
      denc(e, p, f);
    }
  }
  static void decode(std::vector<T>& s, buffer::ptr::iterator& p) {
    s.clear();
    uint32_t num;
    denc(num, p);
    s.resize(num);
    for (unsigned i=0; i<num; ++i) {
      denc(s[i], p);
    }
  }
};

//
// std::set<T>
//
template<typename T>
struct denc_traits<
  std::set<T>,
  typename std::enable_if<denc_traits<T>::supported>::type> {
  typedef denc_traits<T> traits;

  enum { supported = true };
  enum { featured = traits::featured };
  enum { bounded = false };

  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 !traits::bounded &&
                                 !traits::featured>::type
  bound_encode(const std::set<T>& s, size_t& p) {
    p += sizeof(uint32_t);
    for (const T& e : s) {
      denc(e, p);
    }
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 traits::bounded &&
                                 !traits::featured, void>::type
  bound_encode(const std::set<T>& s, size_t& p) {
    size_t elem_size = 0;
    denc(*(const T*)nullptr, elem_size);
    p += sizeof(uint32_t) + elem_size * s.size();
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 !traits::bounded &&
                                 traits::featured, void>::type
  bound_encode(const std::set<T>& s, size_t& p, uint64_t f) {
    p += sizeof(uint32_t);
    for (const T& e : s) {
      denc(e, p, f);
    }
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 traits::bounded &&
                                 !traits::featured>::type
  bound_encode(const std::set<T>& s, size_t& p, uint64_t f) {
    size_t elem_size = 0;
    denc(*(const T*)nullptr, elem_size, f);
    p += sizeof(uint32_t) + elem_size * s.size();
  }

  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 !traits::featured>::type
  encode(const std::set<T>& s, buffer::list::contiguous_appender& p) {
    denc((uint32_t)s.size(), p);
    for (const T& e : s) {
      denc(e, p);
    }
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 traits::featured>::type
    encode(const std::set<T>& s, buffer::list::contiguous_appender& p,
	   uint64_t f) {
    denc((uint32_t)s.size(), p);
    for (const T& e : s) {
      denc(e, p, f);
    }
  }
  static void decode(std::set<T>& s, buffer::ptr::iterator& p) {
    s.clear();
    uint32_t num;
    denc(num, p);
    while (num--) {
      T temp;
      denc(temp, p);
      s.insert(temp);
    }
  }
};

//
// std::map<A, B>
//
template<typename A, typename B>
struct denc_traits<
  std::map<A, B>,
  typename std::enable_if<denc_traits<A>::supported &&
			  denc_traits<B>::supported>::type> {
  typedef denc_traits<A> a_traits;
  typedef denc_traits<B> b_traits;

  enum { supported = true };
  enum { featured = a_traits::featured || b_traits::featured };
  enum { bounded = a_traits::bounded && b_traits::bounded };

  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) &&
				 !bounded &&
				 !featured>::type
  bound_encode(const std::map<A,B>& v, size_t& p) {
    denc((uint32_t)v.size(), p);
    for (const auto& i : v) {
      denc(i.first, p);
      denc(i.second, p);
    }
  }
  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) &&
				 !bounded &&
				 featured, void>::type
  bound_encode(const std::map<A,B>& v, size_t& p, uint64_t f) {
    denc((uint32_t)v.size(), p);
    for (const auto& i : v) {
      denc(i.first, p, f);
      denc(i.second, p, f);
    }
  }
  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) &&
				 bounded &&
				 !featured>::type
  bound_encode(const std::map<A,B>& v, size_t& p) {
    denc((uint32_t)v.size(), p);
    size_t elem_size = 0;
    denc(*(A*)nullptr, elem_size);
    denc(*(B*)nullptr, elem_size);
    p += v.size() * elem_size;
  }
  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) &&
				 bounded &&
				 featured, void>::type
  bound_encode(const std::map<A,B>& v, size_t& p, uint64_t f) {
    denc((uint32_t)v.size(), p);
    size_t elem_size = 0;
    denc(*(A*)nullptr, elem_size, f);
    denc(*(B*)nullptr, elem_size, f);
    p += v.size() * elem_size;
  }

  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) &&
				 !featured>::type
  encode(const std::map<A,B>& v, bufferlist::contiguous_appender& p) {
    denc((uint32_t)v.size(), p);
    for (const auto& i : v) {
      denc(i.first, p);
      denc(i.second, p);
    }
  }
  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) &&
				 featured, void>::type
  encode(const std::map<A,B>& v, bufferlist::contiguous_appender& p,
	   uint64_t f) {
    denc((uint32_t)v.size(), p);
    for (const auto& i : v) {
      denc(i.first, p, f);
      denc(i.second, p, f);
    }
  }

  static void decode(std::map<A,B>& v, buffer::ptr::iterator& p) {
    v.clear();
    uint32_t num;
    denc(num, p);
    A key;
    while (num--) {
      denc(key, p);
      denc(v[key], p);
    }
  }
};


// ----------------------------------------------------------------------
// class helpers

// Write denc_traits<> for a class that defines bound_encode/encode/decode
// methods.

#define WRITE_CLASS_DENC(T) _DECLARE_CLASS_DENC(T, false)
#define WRITE_CLASS_DENC_BOUNDED(T) _DECLARE_CLASS_DENC(T, true)
#define _DECLARE_CLASS_DENC(T, b)					\
  template<> struct denc_traits<T> {					\
    enum { supported = true };						\
    enum { featured = false };						\
    enum { bounded = b };						\
    static void bound_encode(const T& v, size_t& p, uint64_t f=0) {	\
      v.bound_encode(p);						\
    }									\
    static void encode(const T& v, buffer::list::contiguous_appender& p, \
		       uint64_t f=0) {					\
      v.encode(p);							\
    }									\
    static void decode(T& v, buffer::ptr::iterator& p) {		\
      v.decode(p);							\
    }									\
  };

#define WRITE_CLASS_DENC_FEATURED(T) _DECLARE_CLASS_DENC_FEATURED(T, false)
#define WRITE_CLASS_DENC_FEATURED_BOUNDED(T) _DECLARE_CLASS_DENC_FEATURED(T, true)
#define _DECLARE_CLASS_DENC_FEATURED(T, b)				\
  template<> struct denc_traits<T> {					\
    enum { supported = true };						\
    enum { featured = true };						\
    enum { bounded = b };						\
    static void bound_encode(const T& v, size_t& p, uint64_t f) {	\
      v.bound_encode(p, f);						\
    }									\
    static void encode(const T& v, buffer::list::contiguous_appender& p, \
		       uint64_t f) {					\
      v.encode(p, f);							\
    }									\
    static void decode(T& v, buffer::ptr::iterator& p) {		\
      v.decode(p);							\
    }									\
  };


// ----------------------------------------------------------------------
// encode/decode wrappers

// These glue the new-style denc world into old-style calls to encode
// and decode by calling into denc_traits<> methods (when present).

template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported == 1 &&
			       !traits::featured>::type encode(
  const T& o,
  bufferlist& bl,
  uint64_t features_unused=0)
{
  size_t len = 0;
  traits::bound_encode(o, len);
  auto a = bl.get_contiguous_appender(len);
  traits::encode(o, a);
}

template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported == 1 &&
			       traits::featured>::type encode(
  const T& o, bufferlist& bl,
  uint64_t features)
{
  size_t len = 0;
  traits::bound_encode(o, len, features);
  auto a = bl.get_contiguous_appender(len);
  traits::encode(o, a, features);
}

template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported == 1 &&
			       !traits::featured>::type decode(
  T& o,
  bufferlist::iterator& p)
{
  if (p.end())
    throw buffer::end_of_buffer();
  // ensure we get a contigous buffer... until the end of the
  // bufferlist.  we don't really know how much we'll need here,
  // unfortunately.  hopefully it is already contiguous and we're just
  // bumping the raw ref and initializing the ptr tmp fields.
  bufferptr tmp;
  bufferlist::iterator t = p;
  t.copy_shallow(p.get_bl().length() - p.get_off(), tmp);
  auto cp = tmp.begin();
  traits::decode(o, cp);
  p.advance((ssize_t)cp.get_offset());
}

template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported == 1 &&
			       traits::featured>::type decode(
  T& o,
  bufferlist::iterator& p)
{
  if (p.end())
    throw buffer::end_of_buffer();
  bufferptr tmp;
  bufferlist::iterator t = p;
  t.copy_shallow(p.get_bl().length() - p.get_off(), tmp);
  auto cp = tmp.begin();
  traits::decode(o, cp);
  p.advance((ssize_t)cp.get_offset());
}


// ----------------------------------------------------------------
// DENC

// These are some class methods we need to do the version and length
// wrappers for DENC_{START,FINISH} for inter-version
// interoperability.

#define DENC_HELPERS							\
  /* bound_encode */							\
  static void _denc_start(size_t& p,					\
			  __u8 *struct_v,				\
			  __u8 *struct_compat,				\
			  char **, uint32_t *) {			\
    p += 2 + 4;								\
  }									\
  static void _denc_finish(size_t& p,					\
			   __u8 *struct_v,				\
			   __u8 *struct_compat,				\
			   char **, uint32_t *) { }			\
  /* encode */								\
  static void _denc_start(bufferlist::contiguous_appender& p,		\
			  __u8 *struct_v,				\
			  __u8 *struct_compat,				\
			  char **len_pos,				\
			  uint32_t *start_oob_off) {			\
    denc(*struct_v, p);							\
    denc(*struct_compat, p);						\
    *len_pos = p.get_pos_add(4);					\
    *start_oob_off = p.get_out_of_band_offset();			\
  }									\
  static void _denc_finish(bufferlist::contiguous_appender& p,		\
			   __u8 *struct_v,				\
			   __u8 *struct_compat,				\
			   char **len_pos,				\
			   uint32_t *start_oob_off) {			\
    *(__le32*)*len_pos = p.get_pos() - *len_pos - sizeof(uint32_t) +	\
      p.get_out_of_band_offset() - *start_oob_off;			\
  }									\
  /* decode */								\
  static void _denc_start(buffer::ptr::iterator& p,			\
			  __u8 *struct_v,				\
			  __u8 *struct_compat,				\
			  char **start_pos,				\
			  uint32_t *struct_len) {			\
    denc(*struct_v, p);							\
    denc(*struct_compat, p);						\
    denc(*struct_len, p);						\
    *start_pos = const_cast<char*>(p.get_pos());			\
  }									\
  static void _denc_finish(buffer::ptr::iterator& p,			\
			   __u8 *struct_v, __u8 *struct_compat,		\
			   char **start_pos,				\
			   uint32_t *struct_len) {			\
    const char *pos = p.get_pos();					\
    char *end = *start_pos + *struct_len;				\
    assert(pos <= end);							\
    if (pos < end) {							\
      p.advance(end - pos);						\
    }									\
  }

// Helpers for versioning the encoding.  These correspond to the
// {ENCODE,DECODE}_{START,FINISH} macros.

#define DENC_START(v, compat, p)					\
  __u8 struct_v = v;							\
  __u8 struct_compat = compat;						\
  char *_denc_pchar;							\
  uint32_t _denc_u32;							\
  _denc_start(p, &struct_v, &struct_compat, &_denc_pchar, &_denc_u32);	\
  do {

#define DENC_FINISH(p)							\
  } while (false);							\
  _denc_finish(p, &struct_v, &struct_compat, &_denc_pchar, &_denc_u32);


// ----------------------------------------------------------------------

// Helpers for writing a unified bound_encode/encode/decode
// implementation that won't screw up buffer size estimations.

#define DENC(Type, v, p)						\
  DENC_HELPERS								\
  void bound_encode(size_t& p) const {					\
    _denc_friend(*this, p);						\
  }									\
  void encode(bufferlist::contiguous_appender& p) const {		\
    _denc_friend(*this, p);						\
  }									\
  void decode(buffer::ptr::iterator& p) {				\
    _denc_friend(*this, p);						\
  }									\
  template<typename T, typename P>					\
  friend typename std::enable_if<boost::is_same<T,Type>::value ||	\
  boost::is_same<T,const Type>::value>::type				\
  _denc_friend(T& v, P& p)

#define DENC_FEATURED(Type, v, p, f)					\
  DENC_HELPERS								\
  void bound_encode(size_t& p, uint64_t f) const {			\
    _denc_friend(*this, p, f);						\
  }									\
  void encode(bufferlist::contiguous_appender& p, uint64_t f) const {	\
    _denc_friend(*this, p, f);						\
  }									\
  void decode(buffer::ptr::iterator& p) {				\
    _denc_friend(*this, p, 0);						\
  }									\
  template<typename T, typename P>					\
  friend typename std::enable_if<boost::is_same<T,Type>::value ||	\
  boost::is_same<T,const Type>::value>::type				\
  _denc_friend(T& v, P& p, uint64_t f)

#endif
