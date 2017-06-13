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

#include <array>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/optional.hpp>

#include "include/assert.h"	// boost clobbers this
#include "include/intarith.h"
#include "include/int_types.h"
#include "include/memory.h"

#include "buffer.h"
#include "byteorder.h"

template<typename T, typename=void>
struct denc_traits {
  static constexpr bool supported = false;
  static constexpr bool featured = false;
  static constexpr bool bounded = false;
  static constexpr bool need_contiguous = true;
};


// hack for debug only; FIXME
//#include <iostream>
//using std::cout;

// Define this to compile in a dump of all encoded objects to disk to
// populate ceph-object-corpus.  Note that there is an almost
// identical implementation in encoding.h, but you only need to define
// ENCODE_DUMP_PATH here.
//
// See src/test/encoding/generate-corpus-objects.sh.
//
//#define ENCODE_DUMP_PATH /tmp/something

#ifdef ENCODE_DUMP_PATH
# include <stdio.h>
# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# define ENCODE_STR(x) #x
# define ENCODE_STRINGIFY(x) ENCODE_STR(x)
# define DENC_DUMP_PRE(Type)			\
  char *__denc_dump_pre = p.get_pos();
  // this hackery with bits below is just to get a semi-reasonable
  // distribution across time.  it is somewhat exponential but not
  // quite.
# define DENC_DUMP_POST(Type)			\
  do {									\
    static int i = 0;							\
    i++;								\
    int bits = 0;							\
    for (unsigned t = i; t; bits++)					\
      t &= t - 1;							\
    if (bits > 2)							\
      break;								\
    char fn[PATH_MAX];							\
    snprintf(fn, sizeof(fn),						\
	     ENCODE_STRINGIFY(ENCODE_DUMP_PATH) "/%s__%d.%x", #Type,		\
	     getpid(), i++);						\
    int fd = ::open(fn, O_WRONLY|O_TRUNC|O_CREAT, 0644);		\
    if (fd >= 0) {							\
      size_t len = p.get_pos() - __denc_dump_pre;			\
      int r = ::write(fd, __denc_dump_pre, len);			\
      (void)r;								\
      ::close(fd);							\
    }									\
  } while (0)
#else
# define DENC_DUMP_PRE(Type)
# define DENC_DUMP_POST(Type)
#endif


/*

  top level level functions look like so
  ======================================

    inline void denc(const T& o, size_t& p, uint64_t features=0);
    inline void denc(const T& o, buffer::list::contiguous_appender& p,
                     uint64_t features=0);
    inline void denc(T& o, buffer::ptr::iterator& p, uint64_t features=0);

  or (for featured objects)

    inline void denc(const T& o, size_t& p, uint64_t features);
    inline void denc(const T& o, buffer::list::contiguous_appender& p,
                     uint64_t features);
    inline void denc(T& o, buffer::ptr::iterator& p, uint64_t features);

  - These are symmetrical, so that they can be used from the magic DENC
  method of writing the bound_encode/encode/decode methods all in one go;
  they differ only in the type of p.

  - These are automatically fabricated via a template that calls into
  the denc_traits<> methods (see below), provided denc_traits<T>::supported
  is defined and true.  They never need to be written explicitly.


  static denc_traits<> definitions look like so
  =============================================

    template<>
    struct denc_traits<T> {
      static constexpr bool supported = true;
      static constexpr bool bounded = false;
      static constexpr bool featured = false;
      static constexpr bool need_contiguous = true;
      static void bound_encode(const T &o, size_t& p, uint64_t f=0);
      static void encode(const T &o, buffer::list::contiguous_appender& p,
		         uint64_t f=0);
      static void decode(T& o, buffer::ptr::iterator &p, uint64_t f=0);
    };

  or (for featured objects)

    template<>
    struct denc_traits<T> {
      static constexpr bool supported = true;
      static constexpr bool bounded = false;
      static constexpr bool featured = true;
      static constexpr bool need_contiguous = true;
      static void bound_encode(const T &o, size_t& p, uint64_t f);
      static void encode(const T &o, buffer::list::contiguous_appender& p,
		         uint64_t f);
      static void decode(T& o, buffer::ptr::iterator &p, uint64_t f=0);
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

  - These methods are optimised for contiguous buffer, but denc() will try
    rebuild a contigous one if the decoded bufferlist is segmented. If you are
    concerned about the cost, you might want to define yet another method:

    void decode(buffer::list::iterator &p);

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
namespace _denc {
  template<class T, class...> struct is_any_of : std::false_type
  {};
  template<class T, class Head, class... Tail>
  struct is_any_of<T, Head, Tail...> : std::conditional<
    std::is_same<T, Head>::value,
    std::true_type,
    is_any_of<T, Tail...>>::type
  {};
}


template<typename T>
struct denc_traits<
  T,
  typename std::enable_if<
    _denc::is_any_of<T, ceph_le64, ceph_le32, ceph_le16, uint8_t
#ifndef _CHAR_IS_SIGNED
		     , int8_t
#endif
		     >::value>::type> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = false;
  static void bound_encode(const T &o, size_t& p, uint64_t f=0) {
    p += sizeof(T);
  }
  static void encode(const T &o,
		     buffer::list::contiguous_appender& p,
		     uint64_t f=0) {
    p.append((const char*)&o, sizeof(o));
  }
  static void decode(T& o, buffer::ptr::iterator &p,
		     uint64_t f=0) {
    o = *(T *)p.get_pos_add(sizeof(o));
  }
  static void decode(T& o, buffer::list::iterator &p) {
    p.copy(sizeof(T), reinterpret_cast<char*>(&o));
  }
};


// -----------------------------------------------------------------------
// integer types

// itype == internal type
// otype == external type, i.e., the type on the wire

// NOTE: the overload resolution ensures that the legacy encode/decode methods
// defined for int types is prefered to the ones  defined using the specialized
// template, and hence get selected. This machinary prevents these these from
// getting glued into the legacy encode/decode methods; the overhead of setting
// up a contiguous_appender etc is likely to be slower.
namespace _denc {

template<typename T, typename=void> struct ExtType {
  using type = void;
};

template<typename T> struct ExtType<
  T,
  typename std::enable_if<std::is_same<T, int16_t>::value ||
			  std::is_same<T, uint16_t>::value>::type> {
  using type = __le16;
};

template<typename T> struct ExtType<
  T,
  typename std::enable_if<std::is_same<T, int32_t>::value ||
			  std::is_same<T, uint32_t>::value>::type> {
  using type = __le32;
};

template<typename T> struct ExtType<
  T,
  typename std::enable_if<std::is_same<T, int64_t>::value ||
			  std::is_same<T, uint64_t>::value>::type> {
  using type = __le64;
};

template<> struct ExtType<bool> {
  using type = uint8_t;
};
} // namespace _denc

template<typename T>
struct denc_traits<
  T,
  typename std::enable_if<!std::is_void<typename _denc::ExtType<T>::type>::value>::type>
{
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = false;
  using etype = typename _denc::ExtType<T>::type;
  static void bound_encode(const T &o, size_t& p, uint64_t f=0) {
    p += sizeof(etype);
  }
  static void encode(const T &o, buffer::list::contiguous_appender& p,
		     uint64_t f=0) {
    *(etype *)p.get_pos_add(sizeof(etype)) = o;
  }
  static void decode(T& o, buffer::ptr::iterator &p,
		     uint64_t f=0) {
    o = *(etype*)p.get_pos_add(sizeof(etype));
  }
  static void decode(T& o, buffer::list::iterator &p) {
    etype e;
    p.copy(sizeof(etype), reinterpret_cast<char*>(&e));
    o = e;
  }
};

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
  int64_t i = 0;
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
  uint64_t i = 0;
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
  unsigned lowznib = v ? (ctz(v) / 4) : 0u;
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
  int64_t i = 0;
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
  traits::decode(o, p, features);
}

namespace _denc {
  template<typename T, typename=void>
  struct has_legacy_denc : std::false_type
  {};
  template<typename T>
  struct has_legacy_denc<T,
    decltype(std::declval<T&>()
	     .decode(std::declval<bufferlist::iterator&>()))> : std::true_type
  {
    static void decode(T& v, bufferlist::iterator& p) {
      v.decode(p);
    }
  };
  template<typename T>
  struct has_legacy_denc<T,
    typename std::enable_if<
      !denc_traits<T>::need_contiguous>::type> : std::true_type
  {
    static void decode(T& v, bufferlist::iterator& p) {
      denc_traits<T>::decode(v, p);
    }
  };
}

template<typename T,
	 typename traits=denc_traits<T>,
	 typename has_legacy_denc=_denc::has_legacy_denc<T>>
inline typename std::enable_if<traits::supported &&
			       has_legacy_denc::value>::type denc(
  T& o,
  buffer::list::iterator& p)
{
  has_legacy_denc::decode(o, p);
}

// ---------------------------------------------------------------------
// base types and containers

//
// std::string
//
template<typename A>
struct denc_traits<std::basic_string<char,std::char_traits<char>,A>> {
private:
  using value_type = std::basic_string<char,std::char_traits<char>,A>;

public:
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = false;
  static constexpr bool need_contiguous = false;

  static void bound_encode(const value_type& s, size_t& p, uint64_t f=0) {
    p += sizeof(uint32_t) + s.size();
  }
  static void encode(const value_type& s,
		     buffer::list::contiguous_appender& p,
	      uint64_t f=0) {
    ::denc((uint32_t)s.size(), p);
    memcpy(p.get_pos_add(s.size()), s.data(), s.size());
  }
  static void decode(value_type& s,
		     buffer::ptr::iterator& p,
		     uint64_t f=0) {
    uint32_t len;
    ::denc(len, p);
    decode_nohead(len, s, p);
  }
  static void decode(value_type& s, buffer::list::iterator& p)
  {
    uint32_t len;
    ::denc(len, p);
    s.clear();
    p.copy(len, s);
  }
  static void decode_nohead(size_t len, value_type& s,
			    buffer::ptr::iterator& p) {
    s.clear();
    if (len) {
      s.append(p.get_pos_add(len), len);
    }
  }
  static void encode_nohead(const value_type& s,
			    buffer::list::contiguous_appender& p) {
    p.append(s.data(), s.length());
  }
};

//
// bufferptr
//
template<>
struct denc_traits<bufferptr> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = false;
  static constexpr bool need_contiguous = false;
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
  static void decode(bufferptr& v, buffer::list::iterator& p) {
    uint32_t len;
    ::denc(len, p);
    bufferlist s;
    p.copy(len, s);
    if (len) {
      if (s.get_num_buffers() == 1)
	v = s.front();
      else
	v = buffer::copy(s.c_str(), s.length());
    }
  }
};

//
// bufferlist
//
template<>
struct denc_traits<bufferlist> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = false;
  static constexpr bool need_contiguous = false;
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
  static void decode(bufferlist& v, buffer::list::iterator& p) {
    uint32_t len;
    ::denc(len, p);
    v.clear();
    p.copy(len, v);
  }
  static void encode_nohead(const bufferlist& v,
			    buffer::list::contiguous_appender& p) {
    p.append(v);
  }
  static void decode_nohead(size_t len, bufferlist& v,
			    buffer::ptr::iterator& p) {
    v.clear();
    if (len) {
      v.append(p.get_ptr(len));
    }
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

  static constexpr bool supported = true;
  static constexpr bool featured = a_traits::featured || b_traits::featured ;
  static constexpr bool bounded = a_traits::bounded && b_traits::bounded;
  static constexpr bool need_contiguous = (a_traits::need_contiguous ||
					   b_traits::need_contiguous);

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

  static void decode(std::pair<A,B>& v, buffer::ptr::iterator& p, uint64_t f=0) {
    denc(v.first, p, f);
    denc(v.second, p, f);
  }
  template<typename AA=A>
  static typename std::enable_if<sizeof(AA) && !need_contiguous>::type
    decode(std::pair<A,B>& v, buffer::list::iterator& p,
	    uint64_t f = 0) {
    denc(v.first, p);
    denc(v.second, p);
  }
};

namespace _denc {
  template<template<class...> class C, typename Details, typename ...Ts>
  struct container_base {
  private:
    using container = C<Ts...>;
    using T = typename Details::T;

  public:
    using traits = denc_traits<T>;

    static constexpr bool supported = true;
    static constexpr bool featured = traits::featured;
    static constexpr bool bounded = false;
    static constexpr bool need_contiguous = traits::need_contiguous;

    template<typename U=T>
    static typename std::enable_if<sizeof(U) &&
				   !traits::bounded &&
				   !traits::featured>::type
    bound_encode(const container& s, size_t& p) {
      p += sizeof(uint32_t);
      for (const T& e : s) {
	denc(e, p);
      }
    }
    template<typename U=T>
    static typename std::enable_if<sizeof(U) &&
				   traits::bounded &&
				   !traits::featured, void>::type
    bound_encode(const container& s, size_t& p) {
      size_t elem_size = 0;
      p += sizeof(uint32_t);
      if (!s.empty()) {
	// STL containers use weird element types like std::pair<const K, V>;
	// cast to something we have denc_traits for.
	denc(static_cast<const T&>(*s.begin()), elem_size);
	p += sizeof(uint32_t) + elem_size * s.size();
      }
    }
    template<typename U=T>
    static typename std::enable_if<sizeof(U) &&
				   !traits::bounded &&
				   traits::featured, void>::type
    bound_encode(const container& s, size_t& p, uint64_t f) {
      p += sizeof(uint32_t);
      for (const T& e : s) {
	denc(e, p, f);
      }
    }
    template<typename U=T>
    static typename std::enable_if<sizeof(U) &&
				   traits::bounded &&
				   traits::featured>::type
    bound_encode(const container& s, size_t& p, uint64_t f) {
      size_t elem_size = 0;
      p += sizeof(uint32_t);
      if (!s.empty()) {
	// STL containers use weird element types like std::pair<const K, V>;
	// cast to something we have denc_traits for.
	denc(static_cast<const T&>(*s.begin()), elem_size, f);
	p += elem_size * s.size();
      }
    }

    template<typename U=T>
    static typename std::enable_if<sizeof(U) &&
				   !traits::featured>::type
    encode(const container& s, buffer::list::contiguous_appender& p) {
      denc((uint32_t)s.size(), p);
      encode_nohead(s, p);
    }
    template<typename U=T>
    static typename std::enable_if<sizeof(U) &&
				   traits::featured>::type
    encode(const container& s, buffer::list::contiguous_appender& p,
	   uint64_t f) {
      denc((uint32_t)s.size(), p);
      encode_nohead(s, p, f);
    }
    static void decode(container& s, buffer::ptr::iterator& p, uint64_t f = 0) {
      uint32_t num;
      denc(num, p);
      decode_nohead(num, s, p, f);
    }
    template<typename U=T>
    static typename std::enable_if<sizeof(U) && !need_contiguous>::type
    decode(container& s, buffer::list::iterator& p) {
      uint32_t num;
      denc(num, p);
      decode_nohead(num, s, p);
    }

    // nohead
    template<typename U=T>
    static typename std::enable_if<sizeof(U) &&
				   !traits::featured>::type
    encode_nohead(const container& s, buffer::list::contiguous_appender& p) {
      for (const T& e : s) {
	denc(e, p);
      }
    }
    template<typename U=T>
    static typename std::enable_if<sizeof(U) &&
				   traits::featured>::type
    encode_nohead(const container& s, buffer::list::contiguous_appender& p,
		  uint64_t f) {
      for (const T& e : s) {
	denc(e, p, f);
      }
    }
    static void decode_nohead(size_t num, container& s,
			      buffer::ptr::iterator& p, uint64_t f=0) {
      s.clear();
      Details::reserve(s, num);
      while (num--) {
	T t;
	denc(t, p, f);
	Details::insert(s, std::move(t));
      }
    }
    template<typename U=T>
    static typename std::enable_if<sizeof(U) && !need_contiguous>::type
    decode_nohead(size_t num, container& s,
		  buffer::list::iterator& p) {
      s.clear();
      Details::reserve(s, num);
      while (num--) {
	T t;
	denc(t, p);
	Details::insert(s, std::move(t));
      }
    }
  };

  template<typename T>
  class container_has_reserve {
    template<typename U, U> struct SFINAE_match;
    template<typename U>
    static std::true_type test(SFINAE_match<T(*)(typename T::size_type),
			       &U::reserve>*);

    template<typename U>
    static std::false_type test(...);

  public:
    static constexpr bool value = decltype(
      test<denc_traits<T>>(0))::value;
  };


  template<typename Container,
	   bool Reserve = container_has_reserve<Container>::value>
  struct reserve_switch;

  template<typename Container>
  struct reserve_switch<Container, true> {
    static void reserve(Container& c, size_t s) {
      c.reserve(s);
    }
  };

  template<typename Container>
  struct reserve_switch<Container, false> {
    static void reserve(Container& c, size_t s) {}
  };

  template<typename Container>
  struct container_details_base : public reserve_switch<Container> {
    using T = typename Container::value_type;
  };

  template<typename Container>
  struct pushback_details : public container_details_base<Container> {
    template<typename ...Args>
    static void insert(Container& c, Args&& ...args) {
      c.emplace_back(std::forward<Args>(args)...);
    }
  };
}

template<typename T, typename ...Ts>
struct denc_traits<
  std::list<T, Ts...>,
  typename std::enable_if<denc_traits<T>::supported>::type>
  : public _denc::container_base<std::list,
				 _denc::pushback_details<std::list<T, Ts...>>,
				 T, Ts...> {};

template<typename T, typename ...Ts>
struct denc_traits<
  std::vector<T, Ts...>,
  typename std::enable_if<denc_traits<T>::supported>::type>
  : public _denc::container_base<std::vector,
				 _denc::pushback_details<std::vector<T, Ts...>>,
				 T, Ts...> {};

namespace _denc {
  template<typename Container>
  struct setlike_details : public container_details_base<Container> {
    using T = typename Container::value_type;
    template<typename ...Args>
    static void insert(Container& c, Args&& ...args) {
      c.emplace_hint(c.cend(), std::forward<Args>(args)...);
    }
  };
}

template<typename T, typename ...Ts>
struct denc_traits<
  std::set<T, Ts...>,
  typename std::enable_if<denc_traits<T>::supported>::type>
  : public _denc::container_base<std::set,
				 _denc::setlike_details<std::set<T, Ts...>>,
				 T, Ts...> {};

template<typename T, typename ...Ts>
struct denc_traits<
  boost::container::flat_set<T, Ts...>,
  typename std::enable_if<denc_traits<T>::supported>::type>
  : public _denc::container_base<
  boost::container::flat_set,
  _denc::setlike_details<boost::container::flat_set<T, Ts...>>,
  T, Ts...> {};

namespace _denc {
  template<typename Container>
  struct maplike_details : public container_details_base<Container> {
    using T = std::pair<typename Container::key_type,
			typename Container::mapped_type>;
    template<typename ...Args>
    static void insert(Container& c, Args&& ...args) {
      c.emplace_hint(c.cend(), std::forward<Args>(args)...);
    }
  };
}

template<typename A, typename B, typename ...Ts>
struct denc_traits<
  std::map<A, B, Ts...>,
  typename std::enable_if<denc_traits<A>::supported &&
			  denc_traits<B>::supported>::type>
  : public _denc::container_base<std::map,
				 _denc::maplike_details<std::map<A, B, Ts...>>,
				 A, B, Ts...> {};

template<typename A, typename B, typename ...Ts>
struct denc_traits<
  boost::container::flat_map<A, B, Ts...>,
  typename std::enable_if<denc_traits<A>::supported &&
			  denc_traits<B>::supported>::type>
  : public _denc::container_base<
  boost::container::flat_map,
  _denc::maplike_details<boost::container::flat_map<
			   A, B, Ts...>>,
  A, B, Ts...> {};

template<typename T, size_t N>
struct denc_traits<
  std::array<T, N>,
  typename std::enable_if<denc_traits<T>::supported>::type> {
private:
  using container = std::array<T, N>;
public:
  using traits = denc_traits<T>;

  static constexpr bool supported = true;
  static constexpr bool featured = traits::featured;
  static constexpr bool bounded = traits::bounded;
  static constexpr bool need_contiguous = traits::need_contiguous;

  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
				 !traits::bounded &&
				 !traits::featured>::type
  bound_encode(const container& s, size_t& p) {
    for (const auto& e : s)
      denc(e, p);
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
				 traits::bounded &&
				 !traits::featured, void>::type
  bound_encode(const container& s, size_t& p) {
    size_t elem_size = 0;
    denc(*s.begin(), elem_size);
    p += elem_size * N;
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
				 !traits::bounded &&
				 traits::featured, void>::type
  bound_encode(const container& s, size_t& p, uint64_t f) {
    for (const auto& e : s)
      denc(e, p, f);
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
				 traits::bounded &&
				 traits::featured>::type
  bound_encode(const container& s, size_t& p, uint64_t f) {
    size_t elem_size = 0;
    p += sizeof(uint32_t);
    if (!s.empty()) {
      denc(*s.begin(), elem_size, f);
      p += elem_size * s.size();
    }
  }

  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
				 !traits::featured>::type
  encode(const container& s, buffer::list::contiguous_appender& p) {
    for (const auto& e : s)
      denc(e, p);
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
				 traits::featured>::type
    encode(const container& s, buffer::list::contiguous_appender& p,
	   uint64_t f) {
    for (const auto& e : s)
      denc(e, p, f);
  }
  static void decode(container& s, buffer::ptr::iterator& p, uint64_t f = 0) {
    for (auto& e : s)
      denc(e, p, f);
  }
  template<typename U=T>
  static typename std::enable_if<sizeof(U) &&
                                 !need_contiguous>::type
  decode(container& s, buffer::list::iterator& p) {
    for (auto& e : s) {
      denc(e, p);
    }
  }
};

namespace _denc {
  template<size_t... I>
  struct indices {};

  template<size_t ...S>
  struct build_indices_helper;
  template<size_t N, size_t ...Is>
  struct build_indices_helper<N, N, Is...> {
    using type = indices<Is...>;
  };
  template<size_t N, size_t I, size_t ...Is>
  struct build_indices_helper<N, I, Is...> {
    using type = typename build_indices_helper<N, I + 1, Is..., I>::type;
  };

  template<size_t I>
  struct build_indices {
    using type = typename build_indices_helper<I, 1, 0>::type;
  };
  template<>
  struct build_indices<0> {
    using type = indices<>;
  };
  template<>
  struct build_indices<1> {
    using type = indices<0>;
  };

  template<size_t I>
  using build_indices_t = typename  build_indices<I>::type;

  template<typename ...Ts>
  struct tuple_traits;
  template<typename T, typename ...Ts>
  struct tuple_traits<T, Ts...> {
    static constexpr bool supported = (denc_traits<T>::supported &&
				       tuple_traits<Ts...>::supported);
    static constexpr bool bounded = (denc_traits<T>::bounded &&
				     tuple_traits<Ts...>::bounded);
    static constexpr bool featured = (denc_traits<T>::featured ||
				    tuple_traits<Ts...>::featured);
    static constexpr bool need_contiguous =
      (denc_traits<T>::need_contiguous ||
       tuple_traits<Ts...>::need_contiguous);
  };
  template<>
  struct tuple_traits<> {
    static constexpr bool supported = true;
    static constexpr bool bounded = true;
    static constexpr bool featured = false;
    static constexpr bool need_contiguous = false;
  };
}

template<typename ...Ts>
struct denc_traits<
  std::tuple<Ts...>,
  typename std::enable_if<_denc::tuple_traits<Ts...>::supported>::type> {
private:
  static_assert(sizeof...(Ts) > 0,
		"Zero-length tuples are not supported.");
  using container = std::tuple<Ts...>;

  template<typename T, size_t I, size_t J, size_t ...Is>
  static void bound_encode_helper_nfnb(const T& s, size_t& p,
				       _denc::indices<I, J, Is...>) {
    denc(std::get<I>(s), p);
    bound_encode_helper_nfnb(s, p, _denc::indices<J, Is...>{});
  }
  template<typename T, size_t I>
  static void bound_encode_helper_nfnb(const T& s, size_t& p,
				       _denc::indices<I>) {
    denc(std::get<I>(s), p);
  }

  template<typename T, size_t I, size_t J, size_t ...Is>
  static void bound_encode_helper_nfb(const T& s, size_t& p,
				      _denc::indices<I, J, Is...>) {
    denc(std::get<I>(s), p);
    bound_encode_helper_nfb(s, p, _denc::indices<J, Is...>{});
  }
  template<typename T, size_t I>
  static void bound_encode_helper_nfb(const T& s, size_t& p,
				      _denc::indices<I>) {
    denc(std::get<I>(s), p);
  }

  template<typename T, size_t I, size_t J, size_t ...Is>
  static void bound_encode_helper_fnb(const T& s, size_t& p, uint64_t f,
				      _denc::indices<I, J, Is...>) {
    denc(std::get<I>(s), p, f);
    bound_encode_helper_fnb(s, p, f, _denc::indices<J, Is...>{});
  }
  template<typename T, size_t I>
  static void bound_encode_helper_fnb(const T& s, size_t& p, uint64_t f,
				      _denc::indices<I>) {
    denc(std::get<I>(s), p, f);
  }

  template<typename T, size_t I, size_t J, size_t ...Is>
  static void bound_encode_helper_fb(const T& s, size_t& p, uint64_t f,
				     _denc::indices<I, J, Is...>) {
    denc(std::get<I>(s), p);
    bound_encode_helper_fb(s, p, f, _denc::indices<J, Is...>{});
  }
  template<typename T, size_t I>
  static void bound_encode_helper_fb(const T& s, size_t& p, uint64_t f,
				     _denc::indices<I>) {
    denc(std::get<I>(s), p);
  }

  template<typename T, size_t I, size_t J, size_t ...Is>
  static void encode_helper_nf(const T& s, buffer::list::contiguous_appender& p,
			       _denc::indices<I, J, Is...>) {
    denc(std::get<I>(s), p);
    encode_helper_nf(s, p, _denc::indices<J, Is...>{});
  }
  template<typename T, size_t I>
  static void encode_helper_nf(const T& s, buffer::list::contiguous_appender& p,
			_denc::indices<I>) {
    denc(std::get<I>(s), p);
  }

  template<typename T, size_t I, size_t J, size_t ...Is>
  static void encode_helper_f(const T& s, buffer::list::contiguous_appender& p,
			      uint64_t f, _denc::indices<I, J, Is...>) {
    denc(std::get<I>(s), p, f);
    encode_helper_nf(s, p, f, _denc::indices<J, Is...>{});
  }
  template<typename T, size_t I>
  static void encode_helper_f(const T& s, buffer::list::contiguous_appender& p,
		       uint64_t f, _denc::indices<I>) {
    denc(std::get<I>(s), p, f);
  }

  template<typename T, size_t I, size_t J, size_t ...Is>
  static void decode_helper(T& s, buffer::ptr::iterator& p,
			    _denc::indices<I, J, Is...>) {
    denc(std::get<I>(s), p);
    decode_helper(s, p, _denc::indices<J, Is...>{});
  }
  template<typename T, size_t I>
  static void decode_helper(T& s, buffer::ptr::iterator& p,
		     _denc::indices<I>) {
    denc(std::get<I>(s), p);
  }
  template<typename T, size_t I, size_t J, size_t ...Is>
  static void decode_helper(T& s, buffer::list::iterator& p,
			    _denc::indices<I, J, Is...>) {
    denc(std::get<I>(s), p);
    decode_helper(s, p, _denc::indices<J, Is...>{});
  }
  template<typename T, size_t I>
  static void decode_helper(T& s, buffer::list::iterator& p,
			    _denc::indices<I>) {
    denc(std::get<I>(s), p);
  }

public:
  using traits = _denc::tuple_traits<Ts...>;

  static constexpr bool supported = true;
  static constexpr bool featured = traits::featured;
  static constexpr bool bounded = traits::bounded;
  static constexpr bool need_contiguous = traits::need_contiguous;


  template<typename U = traits>
  static typename std::enable_if<U::supported &&
				 !traits::bounded &&
				 !traits::featured>::type
  bound_encode(const container& s, size_t& p) {
    bound_encode_helper_nfnb(s, p, _denc::build_indices_t<sizeof...(Ts)>{});
  }
  template<typename U = traits>
  static typename std::enable_if<U::supported &&
				 traits::bounded &&
				 !traits::featured, void>::type
  bound_encode(const container& s, size_t& p) {
    bound_encode_helper_nfb(s, p, _denc::build_indices_t<sizeof...(Ts)>{});
  }
  template<typename U = traits>
  static typename std::enable_if<U::traits &&
				 !traits::bounded &&
				 traits::featured, void>::type
  bound_encode(const container& s, size_t& p, uint64_t f) {
    bound_encode_helper_fnb(s, p, _denc::build_indices_t<sizeof...(Ts)>{});
  }
  template<typename U = traits>
  static typename std::enable_if<U::traits &&
				 traits::bounded &&
				 traits::featured>::type
  bound_encode(const container& s, size_t& p, uint64_t f) {
    bound_encode_helper_fb(s, p, _denc::build_indices_t<sizeof...(Ts)>{});
  }

  template<typename U = traits>
  static typename std::enable_if<U::supported &&
				 !traits::featured>::type
  encode(const container& s, buffer::list::contiguous_appender& p) {
    encode_helper_nf(s, p, _denc::build_indices_t<sizeof...(Ts)>{});
  }
  template<typename U = traits>
  static typename std::enable_if<U::supported &&
				 traits::featured>::type
  encode(const container& s, buffer::list::contiguous_appender& p,
	 uint64_t f) {
    encode_helper_f(s, p, f, _denc::build_indices_t<sizeof...(Ts)>{});
  }

  static void decode(container& s, buffer::ptr::iterator& p, uint64_t f = 0) {
    decode_helper(s, p, _denc::build_indices_t<sizeof...(Ts)>{});
  }
  template<typename U = traits>
  static typename std::enable_if<U::supported &&
                                 !need_contiguous>::type
  decode(container& s, buffer::list::iterator& p, uint64_t f = 0) {
    decode_helper(s, p, _denc::build_indices_t<sizeof...(Ts)>{});
  }
};

//
// boost::optional<T>
//
template<typename T>
struct denc_traits<
  boost::optional<T>,
  typename std::enable_if<denc_traits<T>::supported>::type> {
  using traits = denc_traits<T>;

  static constexpr bool supported = true;
  static constexpr bool featured = traits::featured;
  static constexpr bool bounded = false;
  static constexpr bool need_contiguous = traits::need_contiguous;

  template<typename U = T>
  static typename std::enable_if<sizeof(U) && !featured>::type
  bound_encode(const boost::optional<T>& v, size_t& p) {
    p += sizeof(bool);
    if (v)
      denc(*v, p);
  }
  template<typename U = T>
  static typename std::enable_if<sizeof(U) && featured>::type
  bound_encode(const boost::optional<T>& v, size_t& p, uint64_t f) {
    p += sizeof(bool);
    if (v)
      denc(*v, p);
  }

  template<typename U = T>
  static typename std::enable_if<sizeof(U) && !featured>::type
  encode(const boost::optional<T>& v, bufferlist::contiguous_appender& p) {
    denc((bool)v, p);
    if (v)
      denc(*v, p);
  }
  template<typename U = T>
  static typename std::enable_if<sizeof(U) && featured>::type
  encode(const boost::optional<T>& v, bufferlist::contiguous_appender& p,
	 uint64_t f) {
    denc((bool)v, p, f);
    if (v)
      denc(*v, p, f);
  }

  static void decode(boost::optional<T>& v, buffer::ptr::iterator& p,
		     uint64_t f = 0) {
    bool x;
    denc(x, p, f);
    if (x) {
      v = T{};
      denc(*v, p, f);
    } else {
      v = boost::none;
    }
  }

  template<typename U = T>
  static typename std::enable_if<sizeof(U) && !need_contiguous>::type
  decode(boost::optional<T>& v, buffer::list::iterator& p) {
    bool x;
    denc(x, p);
    if (x) {
      v = T{};
      denc(*v, p);
    } else {
      v = boost::none;
    }
  }

  template<typename U = T>
  static typename std::enable_if<sizeof(U) && !featured>::type
  encode_nohead(const boost::optional<T>& v,
		bufferlist::contiguous_appender& p) {
    if (v)
      denc(*v, p);
  }
  template<typename U = T>
  static typename std::enable_if<sizeof(U) && featured>::type
  encode_nohead(const boost::optional<T>& v,
		bufferlist::contiguous_appender& p,
		uint64_t f) {
    if (v)
      denc(*v, p, f);
  }

  static void decode_nohead(bool num, boost::optional<T>& v,
			    buffer::ptr::iterator& p, uint64_t f = 0) {
    if (num) {
      v = T();
      denc(*v, p, f);
    } else {
      v = boost::none;
    }
  }
};

template<>
struct denc_traits<boost::none_t> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = false;

  static void bound_encode(const boost::none_t& v, size_t& p) {
    p += sizeof(bool);
  }

  static void encode(const boost::none_t& v,
		     bufferlist::contiguous_appender& p) {
    denc(false, p);
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
    static constexpr bool supported = true;				\
    static constexpr bool featured = false;				\
    static constexpr bool bounded = b;					\
    static constexpr bool need_contiguous = !_denc::has_legacy_denc<T>::value;\
    static void bound_encode(const T& v, size_t& p, uint64_t f=0) {	\
      v.bound_encode(p);						\
    }									\
    static void encode(const T& v, buffer::list::contiguous_appender& p, \
		       uint64_t f=0) {					\
      v.encode(p);							\
    }									\
    static void decode(T& v, buffer::ptr::iterator& p, uint64_t f=0) {	\
      v.decode(p);							\
    }									\
  };

#define WRITE_CLASS_DENC_FEATURED(T) _DECLARE_CLASS_DENC_FEATURED(T, false)
#define WRITE_CLASS_DENC_FEATURED_BOUNDED(T) _DECLARE_CLASS_DENC_FEATURED(T, true)
#define _DECLARE_CLASS_DENC_FEATURED(T, b)				\
  template<> struct denc_traits<T> {					\
    static constexpr bool supported = true;				\
    static constexpr bool featured = true;				\
    static constexpr bool bounded = b;					\
    static constexpr bool need_contiguous = !_denc::has_legacy_denc<T>::value;\
    static void bound_encode(const T& v, size_t& p, uint64_t f) {	\
      v.bound_encode(p, f);						\
    }									\
    static void encode(const T& v, buffer::list::contiguous_appender& p, \
		       uint64_t f) {					\
      v.encode(p, f);							\
    }									\
    static void decode(T& v, buffer::ptr::iterator& p, uint64_t f=0) {	\
      v.decode(p, f);							\
    }									\
  };


// ----------------------------------------------------------------------
// encode/decode wrappers

// These glue the new-style denc world into old-style calls to encode
// and decode by calling into denc_traits<> methods (when present).

template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
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
inline typename std::enable_if<traits::supported &&
			       traits::featured>::type encode(
  const T& o, bufferlist& bl,
  uint64_t features)
{
  size_t len = 0;
  traits::bound_encode(o, len, features);
  auto a = bl.get_contiguous_appender(len);
  traits::encode(o, a, features);
}

template<typename T,
	 typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
			       !traits::need_contiguous>::type decode(
  T& o,
  bufferlist::iterator& p)
{
  if (p.end())
    throw buffer::end_of_buffer();
  const auto& bl = p.get_bl();
  const auto remaining = bl.length() - p.get_off();
  // it is expensive to rebuild a contigous buffer and drop it, so avoid this.
  if (p.get_current_ptr().get_raw() != bl.back().get_raw() &&
      remaining > CEPH_PAGE_SIZE) {
    traits::decode(o, p);
  } else {
    // ensure we get a contigous buffer... until the end of the
    // bufferlist.  we don't really know how much we'll need here,
    // unfortunately.  hopefully it is already contiguous and we're just
    // bumping the raw ref and initializing the ptr tmp fields.
    bufferptr tmp;
    bufferlist::iterator t = p;
    t.copy_shallow(remaining, tmp);
    auto cp = tmp.begin();
    traits::decode(o, cp);
    p.advance((ssize_t)cp.get_offset());
  }
}

template<typename T,
	 typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
			       traits::need_contiguous>::type decode(
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

// nohead variants
template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
			       !traits::featured>::type encode_nohead(
  const T& o,
  bufferlist& bl)
{
  size_t len = 0;
  traits::bound_encode(o, len);
  auto a = bl.get_contiguous_appender(len);
  traits::encode_nohead(o, a);
}

template<typename T, typename traits=denc_traits<T>>
inline typename std::enable_if<traits::supported &&
			       !traits::featured>::type decode_nohead(
  size_t num,
  T& o,
  bufferlist::iterator& p)
{
  if (!num)
    return;
  if (p.end())
    throw buffer::end_of_buffer();
  bufferptr tmp;
  bufferlist::iterator t = p;
  t.copy_shallow(p.get_bl().length() - p.get_off(), tmp);
  auto cp = tmp.begin();
  traits::decode_nohead(num, o, cp);
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
    DENC_DUMP_PRE(Type);						\
    _denc_friend(*this, p);						\
    DENC_DUMP_POST(Type);						\
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
    DENC_DUMP_PRE(Type);						\
    _denc_friend(*this, p, f);						\
    DENC_DUMP_POST(Type);						\
  }									\
  void decode(buffer::ptr::iterator& p, uint64_t f=0) {			\
    _denc_friend(*this, p, f);						\
  }									\
  template<typename T, typename P>					\
  friend typename std::enable_if<boost::is_same<T,Type>::value ||	\
  boost::is_same<T,const Type>::value>::type				\
  _denc_friend(T& v, P& p, uint64_t f)

#endif
