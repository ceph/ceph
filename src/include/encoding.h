// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_ENCODING_H
#define CEPH_ENCODING_H

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <climits>
#include <concepts>
#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/tuple/tuple.hpp>

#include <fcntl.h>
#include <unistd.h>

#include "common/ceph_time.h"

#include "include/compat.h"
#include "include/int_types.h"

#include "common/convenience.h"

#include "byteorder.h"
#include "buffer.h"

// pull in the new-style encoding so that we get the denc_traits<> definition.
#include "denc.h"

#include "assert.h"

using namespace ceph;

namespace ceph {

/*
 * Notes on feature encoding:
 *
 * - The default encode() methods have a features argument with a default parameter
 *   (which goes to zero).
 * - Normal classes will use WRITE_CLASS_ENCODER, with that features=0 default.
 * - Classes that _require_ features will use WRITE_CLASS_ENCODER_FEATURES, which
 *   does not define the default.  Any caller must explicitly pass it in.
 * - STL container macros have two encode variants: one with a features arg, and one
 *   without.
 *
 * The result:
 * - A feature encode() method will fail to compile if a value is not
 *   passed in.
 * - The feature varianet of the STL templates will be used when the feature arg is
 *   provided.  It will be passed through to any template arg types, but it will be
 *   ignored when not needed.
 */

// --------------------------------------
// base types

template<class T>
inline void encode_raw(const T& t, buffer::list& bl)
{
  bl.append((char*)&t, sizeof(t));
}
template<class T>
inline void decode_raw(T& t, buffer::list::const_iterator& p)
{
  p.copy(sizeof(t), (char*)&t);
}

#define WRITE_RAW_ENCODER(type)						\
  inline void encode(const type& v,					\
                     ::ceph::buffer::list& bl,				\
                     uint64_t features=0) {				\
    ::ceph::encode_raw(v, bl);						\
  }									\
  inline void decode(type& v,						\
                     ::ceph::buffer::list::const_iterator& p) {		\
    ::ceph::decode_raw(v, p);						\
  }

WRITE_RAW_ENCODER(__u8)
#ifndef _CHAR_IS_SIGNED
WRITE_RAW_ENCODER(__s8)
#endif
WRITE_RAW_ENCODER(char)
WRITE_RAW_ENCODER(ceph_le64)
WRITE_RAW_ENCODER(ceph_le32)
WRITE_RAW_ENCODER(ceph_le16)

inline void encode(const bool& v, buffer::list& bl) {
  __u8 vv = v;
  encode_raw(vv, bl);
}
inline void decode(bool& v, buffer::list::const_iterator& p) {
  __u8 vv;
  decode_raw(vv, p);
  v = vv;
}


// -----------------------------------
// int types

#define WRITE_INTTYPE_ENCODER(type, etype)				\
  inline void encode(type v, ::ceph::buffer::list& bl, uint64_t features=0) { \
    ceph_##etype e;					                \
    e = v;                                                              \
    ::ceph::encode_raw(e, bl);						\
  }									\
  inline void decode(type& v, ::ceph::buffer::list::const_iterator& p) {	\
    ceph_##etype e;							\
    ::ceph::decode_raw(e, p);						\
    v = e;								\
  }

WRITE_INTTYPE_ENCODER(uint64_t, le64)
WRITE_INTTYPE_ENCODER(int64_t, le64)
WRITE_INTTYPE_ENCODER(uint32_t, le32)
WRITE_INTTYPE_ENCODER(int32_t, le32)
WRITE_INTTYPE_ENCODER(uint16_t, le16)
WRITE_INTTYPE_ENCODER(int16_t, le16)

// -----------------------------------
// float types
//
// NOTE: The following code assumes all supported platforms use IEEE binary32
// as float and IEEE binary64 as double floating-point format.  The assumption
// is verified by the assertions below.
//
// Under this assumption, we can use raw encoding of floating-point types
// on little-endian machines, but we still need to perform a byte swap
// on big-endian machines to ensure cross-architecture compatibility.
// To achieve that, we bit-cast the values as integers first, which are
// byte-swapped via the ceph_le types as above. The extra conversions
// are optimized away on little-endian machines by the compiler.
#define WRITE_FLTTYPE_ENCODER(type, itype, etype)			\
  static_assert(sizeof(type) == sizeof(itype));				\
  static_assert(std::numeric_limits<type>::is_iec559,			\
	      "floating-point type not using IEEE754 format");		\
  inline void encode(type v, ::ceph::buffer::list& bl, uint64_t features=0) { \
    ceph_##etype e;							\
    e = std::bit_cast<itype>(v);					\
    ::ceph::encode_raw(e, bl);						\
  }									\
  inline void decode(type& v, ::ceph::buffer::list::const_iterator& p) {	\
    ceph_##etype e;							\
    ::ceph::decode_raw(e, p);						\
    itype raw = e;							\
    v = std::bit_cast<type>(raw);					\
  }

WRITE_FLTTYPE_ENCODER(float, uint32_t, le32)
WRITE_FLTTYPE_ENCODER(double, uint64_t, le64)

// see denc.h for ENCODE_DUMP_PATH discussion and definition.
#ifdef ENCODE_DUMP_PATH
# define ENCODE_DUMP_PRE()			\
  unsigned pre_off = bl.length()
# define ENCODE_DUMP_POST(cl)						\
  do {									\
    static int i = 0;							\
    i++;								\
    int bits = 0;							\
    for (unsigned t = i; t; bits++)					\
      t &= t - 1;							\
    if (2 < bits)							\
      break;								\
    char fn[PATH_MAX];							\
    snprintf(fn, sizeof(fn), ENCODE_STRINGIFY(ENCODE_DUMP_PATH) "/%s__%d.%x", #cl, getpid(), i++); \
    int fd = ::open(fn, O_WRONLY|O_TRUNC|O_CREAT|O_CLOEXEC|O_BINARY, 0644);		\
    if (0 <= fd) {							\
      ::ceph::buffer::list sub;						\
      sub.substr_of(bl, pre_off, bl.length() - pre_off);		\
      sub.write_fd(fd);							\
      ::close(fd);							\
    }									\
  } while (0)
#else
# define ENCODE_DUMP_PRE()
# define ENCODE_DUMP_POST(cl)
#endif


#define WRITE_CLASS_ENCODER(cl)						\
  inline void encode(const cl& c, ::ceph::buffer::list& bl, uint64_t features=0) { \
    ENCODE_DUMP_PRE(); c.encode(bl); ENCODE_DUMP_POST(cl); }		\
  inline void decode(cl& c, ::ceph::buffer::list::const_iterator& p) { c.decode(p); }

#define WRITE_CLASS_MEMBER_ENCODER(cl)					\
  inline void encode(const cl& c, ::ceph::buffer::list& bl) const {	\
    ENCODE_DUMP_PRE(); c.encode(bl); ENCODE_DUMP_POST(cl); }		\
  inline void decode(cl& c, ::ceph::buffer::list::const_iterator& p) { c.decode(p); }

#define WRITE_CLASS_ENCODER_FEATURES(cl)				\
  inline void encode(const cl& c, ::ceph::buffer::list& bl, uint64_t features) { \
    ENCODE_DUMP_PRE(); c.encode(bl, features); ENCODE_DUMP_POST(cl); }	\
  inline void decode(cl& c, ::ceph::buffer::list::const_iterator& p) { c.decode(p); }

#define WRITE_CLASS_ENCODER_OPTIONAL_FEATURES(cl)				\
  inline void encode(const cl& c, ::ceph::buffer::list& bl, uint64_t features = 0) { \
    ENCODE_DUMP_PRE(); c.encode(bl, features); ENCODE_DUMP_POST(cl); }	\
  inline void decode(cl& c, ::ceph::buffer::list::const_iterator& p) { c.decode(p); }

namespace encoding_detail {

inline void encode_count(size_t n, buffer::list& bl)
{
  // Legacy container/string lengths are stored as 32-bit values on the wire.
  encode(static_cast<__u32>(n), bl);
}

inline __u32 decode_count(buffer::list::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  return n;
}

inline void append_bytes(const void* data, size_t len, buffer::list& bl)
{
  if (len)
    bl.append(static_cast<const char*>(data), len);
}

inline void encode_bytes(const void* data, size_t len, buffer::list& bl)
{
  encode_count(len, bl);
  append_bytes(data, len, bl);
}

} // namespace encoding_detail

// string
inline void encode(std::string_view s, buffer::list& bl, uint64_t features=0)
{
  encoding_detail::encode_bytes(s.data(), s.length(), bl);
}
inline void encode(const std::string& s, buffer::list& bl, uint64_t features=0)
{
  return encode(std::string_view(s), bl, features);
}
inline void decode(std::string& s, buffer::list::const_iterator& p)
{
  const auto len = encoding_detail::decode_count(p);
  s.clear();
  p.copy(len, s);
}

inline void encode_nohead(std::string_view s, buffer::list& bl)
{
  encoding_detail::append_bytes(s.data(), s.length(), bl);
}
inline void encode_nohead(const std::string& s, buffer::list& bl)
{
  encode_nohead(std::string_view(s), bl);
}
inline void decode_nohead(unsigned len, std::string& s, buffer::list::const_iterator& p)
{
  s.clear();
  p.copy(len, s);
}

// const char* (encode only, string compatible)
inline void encode(const char* s, buffer::list& bl)
{
  encode(std::string_view(s, strlen(s)), bl);
}

// opaque byte vectors
inline void encode(std::vector<uint8_t>& v, buffer::list& bl)
{
  encoding_detail::encode_bytes(v.data(), v.size(), bl);
}

inline void decode(std::vector<uint8_t>& v, buffer::list::const_iterator& p)
{
  const auto len = encoding_detail::decode_count(p);
  v.resize(len);
  p.copy(len, (char*)v.data());
}

// -----------------------------
// buffers

// bufferptr (encapsulated)
inline void encode(const buffer::ptr& bp, buffer::list& bl)
{
  const auto len = bp.length();
  encoding_detail::encode_count(len, bl);
  if (len)
    bl.append(bp);
}
inline void decode(buffer::ptr& bp, buffer::list::const_iterator& p)
{
  const auto len = encoding_detail::decode_count(p);

  buffer::list s;
  p.copy(len, s);

  if (!len) {
    return;
  }

  if (1 == s.get_num_buffers()) {
    bp = s.front();
    return;
  }

  bp = buffer::copy(s.c_str(), s.length());
}

// buffer::list (encapsulated)
inline void encode(const buffer::list& s, buffer::list& bl)
{
  encoding_detail::encode_count(s.length(), bl);
  bl.append(s);
}
inline void encode_destructively(buffer::list& s, buffer::list& bl)
{
  encoding_detail::encode_count(s.length(), bl);
  bl.claim_append(s);
}
inline void decode(buffer::list& s, buffer::list::const_iterator& p)
{
  const auto len = encoding_detail::decode_count(p);
  s.clear();
  p.copy(len, s);
}

inline void encode_nohead(const buffer::list& s, buffer::list& bl)
{
  bl.append(s);
}
inline void decode_nohead(unsigned len, buffer::list& s, buffer::list::const_iterator& p)
{
  s.clear();
  p.copy(len, s);
}

// Time, since the templates are defined in std::chrono. The default encodings
// for time_point and duration are backward-compatible with utime_t, but
// truncate seconds to 32 bits so are not guaranteed to round-trip.

template<clock_with_timespec Clock, typename Duration>
void encode(const std::chrono::time_point<Clock, Duration>& t,
            ceph::buffer::list& bl) {
  auto ts = Clock::to_timespec(t);
  // A 32 bit count of seconds causes me vast unhappiness.
  uint32_t s = ts.tv_sec;
  uint32_t ns = ts.tv_nsec;
  encode(s, bl);
  encode(ns, bl);
}

template<clock_with_timespec Clock, typename Duration>
void decode(std::chrono::time_point<Clock, Duration>& t,
            buffer::list::const_iterator& p) {
  uint32_t s;
  uint32_t ns;
  decode(s, p);
  decode(ns, p);
  struct timespec ts = {
    static_cast<time_t>(s),
    static_cast<long int>(ns)};

  t = Clock::from_timespec(ts);
}

template<std::integral Rep, typename Period>
void encode(const std::chrono::duration<Rep, Period>& d,
            ceph::buffer::list& bl) {
  using namespace std::chrono;
  int32_t s = duration_cast<seconds>(d).count();
  int32_t ns = (duration_cast<nanoseconds>(d) % seconds(1)).count();
  encode(s, bl);
  encode(ns, bl);
}

template<std::integral Rep, typename Period>
void decode(std::chrono::duration<Rep, Period>& d,
            buffer::list::const_iterator& p) {
  int32_t s;
  int32_t ns;
  decode(s, p);
  decode(ns, p);
  d = std::chrono::seconds(s) + std::chrono::nanoseconds(ns);
}

// Provide encodings for chrono::time_point and duration that use
// the underlying representation so are guaranteed to round-trip.

template <std::integral Rep, typename Period>
void round_trip_encode(const std::chrono::duration<Rep, Period>& d,
                       ceph::buffer::list& bl) {
  const Rep r = d.count();
  encode(r, bl);
}

template <std::integral Rep, typename Period>
void round_trip_decode(std::chrono::duration<Rep, Period>& d,
                       buffer::list::const_iterator& p) {
  Rep r;
  decode(r, p);
  d = std::chrono::duration<Rep, Period>(r);
}

template <typename Clock, typename Duration>
void round_trip_encode(const std::chrono::time_point<Clock, Duration>& t,
                       ceph::buffer::list& bl) {
  round_trip_encode(t.time_since_epoch(), bl);
}

template <typename Clock, typename Duration>
void round_trip_decode(std::chrono::time_point<Clock, Duration>& t,
                       buffer::list::const_iterator& p) {
  Duration dur;
  round_trip_decode(dur, p);
  t = std::chrono::time_point<Clock, Duration>(dur);
}

// -----------------------------
// STL container types

namespace encoding_detail {

template<typename TraitsT>
concept needs_legacy_encoding = !TraitsT::supported;

template<typename FirstTraitsT, typename SecondTraitsT>
concept pair_needs_legacy_encoding =
  needs_legacy_encoding<FirstTraitsT> ||
  needs_legacy_encoding<SecondTraitsT>;

template<typename KeyT, typename ValueT>
concept map_decodes_by_emplace =
  std::move_constructible<KeyT> &&
  std::move_constructible<ValueT>;

template<typename OptionalT>
void encode_optional(const OptionalT& p, buffer::list& bl);

template<typename T>
std::optional<T> decode_optional(buffer::list::const_iterator& p);

template<typename FnT>
void for_each_count(unsigned n, FnT&& fn);

template<typename RangeT>
void encode_range_nohead(const RangeT& r, buffer::list& bl);

template<typename RangeT>
void encode_range_nohead(const RangeT& r, buffer::list& bl, uint64_t features);

template<typename RangeT, typename IteratorT>
void decode_range_nohead(RangeT& r, IteratorT& p);

template<typename RangeT>
void encode_range(const RangeT& r, buffer::list& bl);

template<typename RangeT>
void encode_range(const RangeT& r, buffer::list& bl, uint64_t features);

template<typename ContainerT, typename IteratorT>
void decode_by_resize_nohead(unsigned len, ContainerT& c,
                             IteratorT& p);

template<typename ContainerT>
void decode_by_resize(ContainerT& c, buffer::list::const_iterator& p);

template<typename ContainerT, typename IteratorT>
void decode_by_emplace_back(unsigned len, ContainerT& c,
                            IteratorT& p);

template<typename ContainerT>
void decode_by_emplace_back(ContainerT& c, buffer::list::const_iterator& p);

template<typename ContainerT>
void clear_and_reserve(ContainerT& c, size_t n);

template<typename ContainerT, typename IteratorT>
void decode_by_insert(unsigned len, ContainerT& c,
                      IteratorT& p);

template<typename ContainerT>
void decode_by_insert(ContainerT& c, buffer::list::const_iterator& p);

template<typename RangeT>
void encode_shared_ptr_range(const RangeT& r, buffer::list& bl);

template<typename RangeT>
void encode_shared_ptr_range(const RangeT& r, buffer::list& bl,
                             uint64_t features);

template<typename ValueT, typename EncodeFnT>
void append_cached_default_encoding(std::optional<std::string>& bytes,
                                    buffer::list& bl,
                                    EncodeFnT&& encode_value);

template<typename RangeT, typename EncodeFnT>
void encode_shared_ptr_range_with(const RangeT& r, buffer::list& bl,
                                  EncodeFnT&& encode_value);

template<typename ContainerT>
void decode_shared_ptr_sequence(ContainerT& c,
                                buffer::list::const_iterator& p);

template<typename PairT>
void encode_pair_members(const PairT& item, buffer::list& bl);

template<typename PairT>
void encode_pair_members(const PairT& item, buffer::list& bl,
                         uint64_t features);

template<typename PairT>
void decode_pair_members(PairT& item, buffer::list::const_iterator& p);

template<typename MapT>
void encode_pair_range_nohead(const MapT& m, buffer::list& bl);

template<typename MapT>
void encode_pair_range_nohead(const MapT& m, buffer::list& bl,
                              uint64_t features);

template<typename MapT>
void encode_pair_range(const MapT& m, buffer::list& bl);

template<typename MapT>
void encode_pair_range(const MapT& m, buffer::list& bl, uint64_t features);

template<typename MapT, typename IteratorT>
void decode_map_entries_by_subscript(unsigned n, MapT& m,
                                     IteratorT& p);

template<typename MapT, typename IteratorT>
void decode_map_entries_by_emplace(unsigned n, MapT& m,
                                   IteratorT& p);

template<typename MapT, typename IteratorT>
void decode_map_entries(unsigned n, MapT& m, IteratorT& p);

template<typename MapT>
void decode_map_by_subscript(unsigned n, MapT& m,
                             buffer::list::const_iterator& p);

template<typename MapT>
void decode_map_by_subscript(MapT& m, buffer::list::const_iterator& p);

template<typename MapT>
void decode_map(unsigned n, MapT& m, buffer::list::const_iterator& p);

template<typename MapT>
void decode_map(MapT& m, buffer::list::const_iterator& p);

} // namespace encoding_detail

// boost optional
template<typename T>
inline void encode(const boost::optional<T>& p, buffer::list& bl)
{
  encoding_detail::encode_optional(p, bl);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic ignored "-Wuninitialized"
template<typename T>
inline void decode(boost::optional<T>& p, buffer::list::const_iterator& bp)
{
  auto decoded = encoding_detail::decode_optional<T>(bp);
  if (decoded) {
    p = std::move(*decoded);
    return;
  }

  p.reset();
}

// std optional
template<typename T>
inline void encode(const std::optional<T>& p, buffer::list& bl)
{
  encoding_detail::encode_optional(p, bl);
}

template<typename T>
inline void decode(std::optional<T>& p, buffer::list::const_iterator& bp)
{
  p = encoding_detail::decode_optional<T>(bp);
}
#pragma GCC diagnostic pop

// std::tuple
template<typename... Ts>
inline void encode(const std::tuple<Ts...>& t, buffer::list& bl)
{
  ceph::for_each(t, [&bl](const auto& e) {
    encode(e, bl);
  });
}
template<typename... Ts>
inline void decode(std::tuple<Ts...>& t, buffer::list::const_iterator& bp)
{
  ceph::for_each(t, [&bp](auto& e) {
    decode(e, bp);
  });
}

// triple boost::tuple
template<class A, class B, class C>
inline void encode(const boost::tuple<A, B, C>& t, buffer::list& bl)
{
  encode(boost::get<0>(t), bl);
  encode(boost::get<1>(t), bl);
  encode(boost::get<2>(t), bl);
}
template<class A, class B, class C>
inline void decode(boost::tuple<A, B, C>& t, buffer::list::const_iterator& bp)
{
  decode(boost::get<0>(t), bp);
  decode(boost::get<1>(t), bp);
  decode(boost::get<2>(t), bp);
}

// std::pair<A, B>
template<class A, class B,
	 typename a_traits = denc_traits<A>, typename b_traits = denc_traits<B>>
requires encoding_detail::pair_needs_legacy_encoding<a_traits, b_traits>
inline void encode(const std::pair<A, B>& p, buffer::list& bl, uint64_t features)
{
  encoding_detail::encode_pair_members(p, bl, features);
}
template<class A, class B,
	 typename a_traits = denc_traits<A>, typename b_traits = denc_traits<B>>
requires encoding_detail::pair_needs_legacy_encoding<a_traits, b_traits>
inline void encode(const std::pair<A, B>& p, buffer::list& bl)
{
  encoding_detail::encode_pair_members(p, bl);
}
template<class A, class B,
	 typename a_traits = denc_traits<A>, typename b_traits = denc_traits<B>>
requires encoding_detail::pair_needs_legacy_encoding<a_traits, b_traits>
inline void decode(std::pair<A, B>& pa, buffer::list::const_iterator& p)
{
  encoding_detail::decode_pair_members(pa, p);
}

// std::list<T>
template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::list<T, Alloc>& ls, buffer::list& bl)
{
  encoding_detail::encode_range(ls, bl);
}
template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::list<T, Alloc>& ls, buffer::list& bl, uint64_t features)
{
  encoding_detail::encode_range(ls, bl, features);
}

template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(std::list<T, Alloc>& ls, buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_emplace_back(ls, p);
}

// std::list<std::shared_ptr<T>>
template<class T, class Alloc>
inline void encode(const std::list<std::shared_ptr<T>, Alloc>& ls,
                   buffer::list& bl)
{
  encoding_detail::encode_shared_ptr_range(ls, bl);
}
template<class T, class Alloc>
inline void encode(const std::list<std::shared_ptr<T>, Alloc>& ls,
                   buffer::list& bl, uint64_t features)
{
  encoding_detail::encode_shared_ptr_range(ls, bl, features);
}
template<class T, class Alloc>
inline void decode(std::list<std::shared_ptr<T>, Alloc>& ls,
                   buffer::list::const_iterator& p)
{
  encoding_detail::decode_shared_ptr_sequence(ls, p);
}

// std::set<T>
template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::set<T, Comp, Alloc>& s, buffer::list& bl)
{
  encoding_detail::encode_range(s, bl);
}
template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(std::set<T, Comp, Alloc>& s, buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_insert(s, p);
}

template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode_nohead(const std::set<T, Comp, Alloc>& s, buffer::list& bl)
{
  encoding_detail::encode_range_nohead(s, bl);
}
template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode_nohead(unsigned len, std::set<T, Comp, Alloc>& s,
                          buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_insert(len, s, p);
}

// boost::container::flat_set<T>
template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const boost::container::flat_set<T, Comp, Alloc>& s,
                   buffer::list& bl)
{
  encoding_detail::encode_range(s, bl);
}
template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(boost::container::flat_set<T, Comp, Alloc>& s,
                   buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_insert(s, p);
}

template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode_nohead(const boost::container::flat_set<T, Comp, Alloc>& s,
                          buffer::list& bl)
{
  encoding_detail::encode_range_nohead(s, bl);
}
template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode_nohead(unsigned len,
                          boost::container::flat_set<T, Comp, Alloc>& s,
                          buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_insert(len, s, p);
}

// multiset
template<class T, class Comp, class Alloc>
inline void encode(const std::multiset<T, Comp, Alloc>& s, buffer::list& bl)
{
  encoding_detail::encode_range(s, bl);
}
template<class T, class Comp, class Alloc>
inline void decode(std::multiset<T, Comp, Alloc>& s, buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_insert(s, p);
}

template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::vector<T, Alloc>& v, buffer::list& bl, uint64_t features)
{
  encoding_detail::encode_range(v, bl, features);
}
template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::vector<T, Alloc>& v, buffer::list& bl)
{
  encoding_detail::encode_range(v, bl);
}
template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(std::vector<T, Alloc>& v, buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_resize(v, p);
}

template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode_nohead(const std::vector<T, Alloc>& v, buffer::list& bl)
{
  encoding_detail::encode_range_nohead(v, bl);
}
template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode_nohead(unsigned len, std::vector<T, Alloc>& v,
                          buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_resize_nohead(len, v, p);
}

// small vector
template<class T, std::size_t N, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const boost::container::small_vector<T, N, Alloc>& v,
                   buffer::list& bl, uint64_t features)
{
  encoding_detail::encode_range(v, bl, features);
}
template<class T, std::size_t N, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const boost::container::small_vector<T, N, Alloc>& v,
                   buffer::list& bl)
{
  encoding_detail::encode_range(v, bl);
}
template<class T, std::size_t N, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(boost::container::small_vector<T, N, Alloc>& v,
                   buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_resize(v, p);
}

template<class T, std::size_t N, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode_nohead(const boost::container::small_vector<T, N, Alloc>& v,
                          buffer::list& bl)
{
  encoding_detail::encode_range_nohead(v, bl);
}
template<class T, std::size_t N, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode_nohead(unsigned len,
                          boost::container::small_vector<T, N, Alloc>& v,
                          buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_resize_nohead(len, v, p);
}


// vector (shared_ptr)
template<class T, class Alloc>
inline void encode(const std::vector<std::shared_ptr<T>, Alloc>& v,
                   buffer::list& bl,
                   uint64_t features)
{
  encoding_detail::encode_shared_ptr_range(v, bl, features);
}
template<class T, class Alloc>
inline void encode(const std::vector<std::shared_ptr<T>, Alloc>& v,
                   buffer::list& bl)
{
  encoding_detail::encode_shared_ptr_range(v, bl);
}
template<class T, class Alloc>
inline void decode(std::vector<std::shared_ptr<T>, Alloc>& v,
                   buffer::list::const_iterator& p)
{
  encoding_detail::decode_shared_ptr_sequence(v, p);
}

// map
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void encode(const std::map<T, U, Comp, Alloc>& m, buffer::list& bl)
{
  encoding_detail::encode_pair_range(m, bl);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void encode(const std::map<T, U, Comp, Alloc>& m, buffer::list& bl,
                   uint64_t features)
{
  encoding_detail::encode_pair_range(m, bl, features);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void decode(std::map<T, U, Comp, Alloc>& m, buffer::list::const_iterator& p)
{
  encoding_detail::decode_map(m, p);
}

// Compatibility-only: no production callers were found in-tree. Do not add new
// callers unless preserving the destination's current entries is necessary.
// Actual deprecation is a separate public-header compatibility decision.
// Duplicate decoded keys intentionally preserve the existing mapped value for
// std::map.
template<class T, class U, class Comp, class Alloc>
[[maybe_unused]] inline void decode_noclear(std::map<T, U, Comp, Alloc>& m,
                                            buffer::list::const_iterator& p)
{
  encoding_detail::decode_map_entries(encoding_detail::decode_count(p), m, p);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void encode_nohead(const std::map<T, U, Comp, Alloc>& m, buffer::list& bl)
{
  encoding_detail::encode_pair_range_nohead(m, bl);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void encode_nohead(const std::map<T, U, Comp, Alloc>& m,
                          buffer::list& bl,
                          uint64_t features)
{
  encoding_detail::encode_pair_range_nohead(m, bl, features);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void decode_nohead(unsigned n, std::map<T, U, Comp, Alloc>& m,
                          buffer::list::const_iterator& p)
{
  encoding_detail::decode_map(n, m, p);
}

// boost::container::flat-map
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void encode(const boost::container::flat_map<T, U, Comp, Alloc>& m,
                   buffer::list& bl)
{
  encoding_detail::encode_pair_range(m, bl);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void encode(const boost::container::flat_map<T, U, Comp, Alloc>& m,
                   buffer::list& bl, uint64_t features)
{
  encoding_detail::encode_pair_range(m, bl, features);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void decode(boost::container::flat_map<T, U, Comp, Alloc>& m,
                   buffer::list::const_iterator& p)
{
  encoding_detail::decode_map_by_subscript(m, p);
}

// Compatibility-only: no production callers were found in-tree. Do not add new
// callers unless preserving the destination's current entries is necessary.
// Actual deprecation is a separate public-header compatibility decision.
// Duplicate decoded keys intentionally replace the existing mapped value for
// boost::container::flat_map.
template<class T, class U, class Comp, class Alloc>
[[maybe_unused]] inline void decode_noclear(
  boost::container::flat_map<T, U, Comp, Alloc>& m,
  buffer::list::const_iterator& p)
{
  const auto n = encoding_detail::decode_count(p);
  m.reserve(m.size() + n);
  encoding_detail::decode_map_entries_by_subscript(n, m, p);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void encode_nohead(const boost::container::flat_map<T, U, Comp, Alloc>& m,
                          buffer::list& bl)
{
  encoding_detail::encode_pair_range_nohead(m, bl);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void encode_nohead(const boost::container::flat_map<T, U, Comp, Alloc>& m,
                          buffer::list& bl, uint64_t features)
{
  encoding_detail::encode_pair_range_nohead(m, bl, features);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::pair_needs_legacy_encoding<t_traits, u_traits>
inline void decode_nohead(unsigned n,
                          boost::container::flat_map<T, U, Comp, Alloc>& m,
                          buffer::list::const_iterator& p)
{
  encoding_detail::decode_map_by_subscript(n, m, p);
}

// multimap
template<class T, class U, class Comp, class Alloc>
inline void encode(const std::multimap<T, U, Comp, Alloc>& m, buffer::list& bl)
{
  encoding_detail::encode_pair_range(m, bl);
}
template<class T, class U, class Comp, class Alloc>
inline void decode(std::multimap<T, U, Comp, Alloc>& m,
                   buffer::list::const_iterator& p)
{
  m.clear();

  encoding_detail::for_each_count(encoding_detail::decode_count(p), [&m, &p] {
    auto tu = std::pair<T, U> {};
    decode(tu.first, p);
    auto it = m.insert(std::move(tu));
    decode(it->second, p);
  });
}

// std::unordered_map
template<class T, class U, class Hash, class Pred, class Alloc>
inline void encode(const std::unordered_map<T, U, Hash, Pred, Alloc>& m,
                   buffer::list& bl,
                   uint64_t features)
{
  encoding_detail::encode_pair_range(m, bl, features);
}
template<class T, class U, class Hash, class Pred, class Alloc>
inline void encode(const std::unordered_map<T, U, Hash, Pred, Alloc>& m,
                   buffer::list& bl)
{
  encoding_detail::encode_pair_range(m, bl);
}
template<class T, class U, class Hash, class Pred, class Alloc>
inline void decode(std::unordered_map<T, U, Hash, Pred, Alloc>& m,
                   buffer::list::const_iterator& p)
{
  encoding_detail::decode_map(m, p);
}

// std::unordered_set
template<class T, class Hash, class Pred, class Alloc>
inline void encode(const std::unordered_set<T, Hash, Pred, Alloc>& m,
                   buffer::list& bl)
{
  encoding_detail::encode_range(m, bl);
}
template<class T, class Hash, class Pred, class Alloc>
inline void decode(std::unordered_set<T, Hash, Pred, Alloc>& m,
                   buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_insert(m, p);
}

// deque
template<class T, class Alloc>
inline void encode(const std::deque<T, Alloc>& ls, buffer::list& bl,
                   uint64_t features)
{
  encoding_detail::encode_range(ls, bl, features);
}
template<class T, class Alloc>
inline void encode(const std::deque<T, Alloc>& ls, buffer::list& bl)
{
  encoding_detail::encode_range(ls, bl);
}
template<class T, class Alloc>
inline void decode(std::deque<T, Alloc>& ls, buffer::list::const_iterator& p)
{
  encoding_detail::decode_by_emplace_back(ls, p);
}

// std::array<T, N>
template<class T, size_t N, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::array<T, N>& v, buffer::list& bl, uint64_t features)
{
  encoding_detail::encode_range_nohead(v, bl, features);
}
template<class T, size_t N, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::array<T, N>& v, buffer::list& bl)
{
  encoding_detail::encode_range_nohead(v, bl);
}
template<class T, size_t N, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(std::array<T, N>& v, buffer::list::const_iterator& p)
{
  encoding_detail::decode_range_nohead(v, p);
}

namespace encoding_detail {

template<typename OptionalT>
void encode_optional(const OptionalT& p, buffer::list& bl)
{
  __u8 present = static_cast<bool>(p);
  encode(present, bl);
  if (!p) {
    return;
  }

  encode(*p, bl);
}

template<typename T>
std::optional<T> decode_optional(buffer::list::const_iterator& p)
{
  __u8 present;
  decode(present, p);
  if (!present) {
    return std::nullopt;
  }

  T value{};
  decode(value, p);
  return value;
}

template<typename FnT>
void for_each_count(unsigned n, FnT&& fn)
{
  std::ranges::for_each(std::views::iota(0u, n), [&fn](auto) {
    std::invoke(fn);
  });
}

template<typename RangeT>
void encode_range_nohead(const RangeT& r, buffer::list& bl)
{
  std::ranges::for_each(r, [&bl](const auto& item) {
    encode(item, bl);
  });
}

template<typename RangeT>
void encode_range_nohead(const RangeT& r, buffer::list& bl, uint64_t features)
{
  std::ranges::for_each(r, [&bl, features](const auto& item) {
    encode(item, bl, features);
  });
}

template<typename RangeT, typename IteratorT>
void decode_range_nohead(RangeT& r, IteratorT& p)
{
  std::ranges::for_each(r, [&p](auto& item) {
    decode(item, p);
  });
}

template<typename RangeT>
void encode_range(const RangeT& r, buffer::list& bl)
{
  encode_count(std::ranges::size(r), bl);
  encode_range_nohead(r, bl);
}

template<typename RangeT>
void encode_range(const RangeT& r, buffer::list& bl, uint64_t features)
{
  encode_count(std::ranges::size(r), bl);
  encode_range_nohead(r, bl, features);
}

template<typename ContainerT>
void reserve_if_possible(ContainerT& c, size_t n)
{
  if constexpr (requires { c.reserve(n); }) {
    c.reserve(n);
  }
}

template<typename ContainerT>
void clear_and_reserve(ContainerT& c, size_t n)
{
  c.clear();
  reserve_if_possible(c, n);
}

template<typename ContainerT, typename IteratorT>
void decode_by_resize_nohead(unsigned len, ContainerT& c,
                             IteratorT& p)
{
  c.resize(len);
  decode_range_nohead(c, p);
}

template<typename ContainerT>
void decode_by_resize(ContainerT& c, buffer::list::const_iterator& p)
{
  decode_by_resize_nohead(decode_count(p), c, p);
}

template<typename ContainerT, typename IteratorT>
void decode_by_emplace_back(unsigned len, ContainerT& c,
                            IteratorT& p)
{
  clear_and_reserve(c, len);

  for_each_count(len, [&c, &p] {
    c.emplace_back();
    decode(c.back(), p);
  });
}

template<typename ContainerT>
void decode_by_emplace_back(ContainerT& c, buffer::list::const_iterator& p)
{
  decode_by_emplace_back(decode_count(p), c, p);
}

template<typename ContainerT, typename IteratorT>
void decode_by_insert(unsigned len, ContainerT& c,
                      IteratorT& p)
{
  clear_and_reserve(c, len);

  for_each_count(len, [&c, &p] {
    typename ContainerT::value_type v;
    decode(v, p);
    c.insert(std::move(v));
  });
}

template<typename ContainerT>
void decode_by_insert(ContainerT& c, buffer::list::const_iterator& p)
{
  decode_by_insert(decode_count(p), c, p);
}

template<typename RangeT>
void encode_shared_ptr_range(const RangeT& r, buffer::list& bl)
{
  encode_shared_ptr_range_with(r, bl, [](const auto& value, auto& out) {
    encode(value, out);
  });
}

template<typename RangeT>
void encode_shared_ptr_range(const RangeT& r, buffer::list& bl,
                             uint64_t features)
{
  encode_shared_ptr_range_with(r, bl, [features](const auto& value, auto& out) {
    encode(value, out, features);
  });
}

template<typename ValueT, typename EncodeFnT>
void append_cached_default_encoding(std::optional<std::string>& bytes,
                                    buffer::list& bl,
                                    EncodeFnT&& encode_value)
{
  if (!bytes) {
    auto tmp = buffer::list {};
    std::invoke(encode_value, ValueT {}, tmp);
    bytes = tmp.to_str();
  }

  bl.append(std::string_view { *bytes });
}

template<typename RangeT, typename EncodeFnT>
void encode_shared_ptr_range_with(const RangeT& r, buffer::list& bl,
                                  EncodeFnT&& encode_value)
{
  using value_type = typename std::ranges::range_value_t<RangeT>::element_type;

  encode_count(std::ranges::size(r), bl);

  auto null_bytes = std::optional<std::string> {};

  std::ranges::for_each(r, [&bl, &encode_value, &null_bytes](const auto& ref) {
    if (ref) {
      std::invoke(encode_value, *ref, bl);
      return;
    }

    append_cached_default_encoding<value_type>(null_bytes, bl, encode_value);
  });
}

template<typename ContainerT>
void decode_shared_ptr_sequence(ContainerT& c,
                                buffer::list::const_iterator& p)
{
  const auto n = decode_count(p);
  clear_and_reserve(c, n);

  for_each_count(n, [&c, &p] {
    auto ref = std::make_shared<typename ContainerT::value_type::element_type>();
    decode(*ref, p);
    c.emplace_back(std::move(ref));
  });
}

template<typename PairT>
void encode_pair_members(const PairT& item, buffer::list& bl)
{
  encode(item.first, bl);
  encode(item.second, bl);
}

template<typename PairT>
void encode_pair_members(const PairT& item, buffer::list& bl,
                         uint64_t features)
{
  encode(item.first, bl, features);
  encode(item.second, bl, features);
}

template<typename PairT>
void decode_pair_members(PairT& item, buffer::list::const_iterator& p)
{
  decode(item.first, p);
  decode(item.second, p);
}

template<typename MapT>
void encode_pair_range_nohead(const MapT& m, buffer::list& bl)
{
  std::ranges::for_each(m, [&bl](const auto& item) {
    encode_pair_members(item, bl);
  });
}

template<typename MapT>
void encode_pair_range_nohead(const MapT& m, buffer::list& bl,
                              uint64_t features)
{
  std::ranges::for_each(m, [&bl, features](const auto& item) {
    encode_pair_members(item, bl, features);
  });
}

template<typename MapT>
void encode_pair_range(const MapT& m, buffer::list& bl)
{
  encode_count(std::ranges::size(m), bl);
  encode_pair_range_nohead(m, bl);
}

template<typename MapT>
void encode_pair_range(const MapT& m, buffer::list& bl, uint64_t features)
{
  encode_count(std::ranges::size(m), bl);
  encode_pair_range_nohead(m, bl, features);
}

template<typename MapT, typename IteratorT>
void decode_map_entries_by_subscript(unsigned n, MapT& m,
                                     IteratorT& p)
{
  for_each_count(n, [&m, &p] {
    typename MapT::key_type k;
    decode(k, p);
    decode(m[std::move(k)], p);
  });
}

template<typename MapT, typename IteratorT>
void decode_map_entries_by_emplace(unsigned n, MapT& m,
                                   IteratorT& p)
{
  for_each_count(n, [&m, &p] {
    typename MapT::key_type k;
    typename MapT::mapped_type v;
    decode(k, p);
    decode(v, p);
    m.emplace(std::move(k), std::move(v));
  });
}

template<typename MapT, typename IteratorT>
void decode_map_entries(unsigned n, MapT& m, IteratorT& p)
{
  if constexpr (map_decodes_by_emplace<typename MapT::key_type,
                                       typename MapT::mapped_type>) {
    decode_map_entries_by_emplace(n, m, p);
    return;
  }

  decode_map_entries_by_subscript(n, m, p);
}

template<typename MapT>
void decode_map_by_subscript(unsigned n, MapT& m,
                             buffer::list::const_iterator& p)
{
  clear_and_reserve(m, n);
  decode_map_entries_by_subscript(n, m, p);
}

template<typename MapT>
void decode_map_by_subscript(MapT& m, buffer::list::const_iterator& p)
{
  decode_map_by_subscript(decode_count(p), m, p);
}

template<typename MapT>
void decode_map(unsigned n, MapT& m, buffer::list::const_iterator& p)
{
  clear_and_reserve(m, n);
  decode_map_entries(n, m, p);
}

template<typename MapT>
void decode_map(MapT& m, buffer::list::const_iterator& p)
{
  decode_map(decode_count(p), m, p);
}

} // namespace encoding_detail

// full bl decoder
template<class T>
inline void decode(T &o, const buffer::list& bl)
{
  auto p = bl.begin();
  decode(o, p);
  ceph_assert(p.end());
}
}

/*
 * guards
 */

/**
 * start encoding block
 *
 * @param v current (code) version of the encoding
 * @param compat oldest code version that can decode it
 * @param bl buffer::list to encode to
 *
 */
#define ENCODE_START(v, compat, bl)			     \
  __u8 struct_v = v;                                         \
  __u8 struct_compat = compat;		                     \
  ceph_le32 struct_len;				             \
  auto filler = (bl).append_hole(			     \
    ::ceph::encoding_detail::struct_header_len());	     \
  const auto starting_bl_len = (bl).length();		     \
  using ::ceph::encode;					     \
  do {

/**
 * finish encoding block
 *
 * @param bl buffer::list we were encoding to
 * @param new_struct_compat struct-compat value to use
 */
#define ENCODE_FINISH_NEW_COMPAT(bl, new_struct_compat)      \
  } while (false);                                           \
  ::ceph::encoding_detail::finish_encode_struct(              \
    filler, struct_v, struct_compat, struct_len,              \
    new_struct_compat, (bl), starting_bl_len);

#define ENCODE_FINISH(bl) ENCODE_FINISH_NEW_COMPAT(bl, 0)

#define DECODE_ERR_OLDVERSION(func, v, compatv)				\
  (std::string(func) + " no longer understands old encoding version " #v \
   " < " + std::to_string(compatv))

#define DECODE_ERR_NO_COMPAT(func, code_v, v, compatv)					\
  ("Decoder at '" + std::string(func) + "' v=" + std::to_string(code_v) +		\
  " cannot decode v=" + std::to_string(v) + " minimal_decoder=" + std::to_string(compatv))

#define DECODE_ERR_PAST(func) \
  (std::string(func) + " decode past end of struct encoding")

namespace ceph::encoding_detail {

inline constexpr auto struct_header_len() noexcept
{
  return sizeof(__u8) + sizeof(__u8) + sizeof(ceph_le32);
}

struct struct_header final {
  __u8 v = 0;
  __u8 compat = 0;
  __u32 len = 0;
};

template<typename IteratorT>
struct_header read_struct_header(IteratorT& bl)
{
  using ::ceph::decode;

  struct_header header;
  decode(header.v, bl);
  decode(header.compat, bl);
  decode(header.len, bl);

  return header;
}

inline void check_decode_compat(__u8 code_v, __u8 struct_v,
                                __u8 struct_compat,
                                const char* func)
{
  if (struct_compat > code_v) {
    throw ::ceph::buffer::malformed_input(
      DECODE_ERR_NO_COMPAT(func, code_v, struct_v, struct_compat));
  }
}

template<typename IteratorT>
unsigned checked_struct_end(IteratorT& bl, __u32 struct_len,
                            const char* func)
{
  if (struct_len > bl.get_remaining()) {
    throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(func));
  }

  return bl.get_off() + struct_len;
}

template<typename VersionT, typename IteratorT>
unsigned decode_struct_start(__u8 code_v, VersionT& struct_v,
                             __u8& struct_compat, __u32& struct_len,
                             IteratorT& bl, const char* func)
{
  const auto header = read_struct_header(bl);
  struct_v = header.v;
  struct_compat = header.compat;
  struct_len = header.len;

  check_decode_compat(code_v, struct_v, struct_compat, func);
  return checked_struct_end(bl, struct_len, func);
}

template<typename VersionT, typename IteratorT>
unsigned decode_legacy_struct_start(__u8 code_v, VersionT& struct_v,
                                    __u8 compat_v, __u8 len_v,
                                    unsigned skip_v, IteratorT& bl,
                                    const char* func)
{
  using ::ceph::decode;

  decode(struct_v, bl);

  if (compat_v <= struct_v) {
    __u8 struct_compat;
    decode(struct_compat, bl);
    check_decode_compat(code_v, struct_v, struct_compat, func);
  }

  if (skip_v && compat_v > struct_v) {
    if (skip_v > bl.get_remaining()) {
      throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(func));
    }

    bl += skip_v;
  }

  if (len_v > struct_v) {
    return 0;
  }

  __u32 struct_len;
  decode(struct_len, bl);
  return checked_struct_end(bl, struct_len, func);
}

template<typename FillerT>
void finish_encode_struct(FillerT& filler, __u8 struct_v,
                          __u8& struct_compat, ceph_le32& struct_len,
                          __u8 new_struct_compat, const buffer::list& bl,
                          size_t starting_bl_len)
{
  if (new_struct_compat) {
    struct_compat = new_struct_compat;
  }

  struct_len = static_cast<__u32>(bl.length() - starting_bl_len);
  filler.copy_in(sizeof(struct_v),
                 reinterpret_cast<const char*>(&struct_v));
  filler.copy_in(sizeof(struct_compat),
                 reinterpret_cast<const char*>(&struct_compat));
  filler.copy_in(sizeof(struct_len),
                 reinterpret_cast<const char*>(&struct_len));
}

template<typename IteratorT>
void finish_decode_struct(IteratorT& bl, unsigned struct_end,
                          const char* func)
{
  if (!struct_end) {
    return;
  }

  if (struct_end < bl.get_off()) {
    throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(func));
  }

  if (struct_end > bl.get_off()) {
    bl += struct_end - bl.get_off();
  }
}

template<typename PayloadT, typename IteratorT>
void decode_unknown(PayloadT& payload, IteratorT& bl, const char* func)
{
  const auto header = read_struct_header(bl);
  checked_struct_end(bl, header.len, func);

  payload.clear();

  using ::ceph::encode;

  encode(header.v, payload);
  encode(header.compat, payload);
  encode(header.len, payload);
  bl.copy(header.len, payload);
}

} // namespace ceph::encoding_detail

/**
 * check for very old encoding
 *
 * If the encoded data is older than oldestv, raise an exception.
 *
 * @param oldestv oldest version of the code we can successfully decode.
 */
#define DECODE_OLDEST(oldestv)						\
  if (oldestv > struct_v)						\
    throw ::ceph::buffer::malformed_input(DECODE_ERR_OLDVERSION(__PRETTY_FUNCTION__, v, oldestv));

/**
 * start a decoding block
 *
 * @param v current version of the encoding that the code supports/encodes
 * @param bl buffer::list::iterator for the encoded data
 */
#define DECODE_START(_v, bl)						\
  StructVChecker<_v> struct_v;						\
  __u8 struct_compat;							\
  __u32 struct_len;							\
  using ::ceph::decode;							\
  unsigned struct_end = ::ceph::encoding_detail::decode_struct_start( \
    _v, struct_v.v, struct_compat, struct_len, bl, __PRETTY_FUNCTION__); \
  do {

#define DECODE_START_UNCHECKED(v, bl)					\
  __u8 struct_v, struct_compat;						\
  __u32 struct_len;							\
  using ::ceph::decode;							\
  unsigned struct_end = ::ceph::encoding_detail::decode_struct_start( \
    v, struct_v, struct_compat, struct_len, bl, __PRETTY_FUNCTION__); \
  do {

#define DECODE_UNKNOWN(payload, bl)					\
  do {                                                                  \
    ::ceph::encoding_detail::decode_unknown(                           \
      payload, bl, __PRETTY_FUNCTION__);                               \
  } while (0)

/* The checked and unchecked legacy wrappers intentionally differ only in the
 * local struct_v type. Keep shared behavior in decode_legacy_struct_start(). */
#define __DECODE_START_LEGACY_COMPAT_LEN(_v, compatv, lenv, skip_v, bl)	\
  using ::ceph::decode;							\
  StructVChecker<_v> struct_v;						\
  unsigned struct_end = ::ceph::encoding_detail::decode_legacy_struct_start( \
    _v, struct_v.v, compatv, lenv, skip_v, bl, __PRETTY_FUNCTION__);	\
  do {

/**
 * start a decoding block with legacy support for older encoding schemes
 *
 * The old encoding schemes has a __u8 struct_v only, or lacked either
 * the compat version or length.  Skip those fields conditionally.
 *
 * Most of the time, v, compatv, and lenv will all match the version
 * where the structure was switched over to the new macros.
 *
 * @param v current version of the encoding that the code supports/encodes
 * @param compatv oldest version that includes a __u8 compat version field
 * @param lenv oldest version that includes a __u32 length wrapper
 * @param bl buffer::list::iterator containing the encoded data
 */

#define DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, bl)		\
  using ::ceph::decode;							\
  __u8 struct_v;							\
  unsigned struct_end = ::ceph::encoding_detail::decode_legacy_struct_start( \
    v, struct_v, compatv, lenv, 0, bl, __PRETTY_FUNCTION__);		\
  do {

/**
 * start a decoding block with legacy support for older encoding schemes
 *
 * This version of the macro assumes the legacy encoding had a 32 bit
 * version
 *
 * The old encoding schemes has a __u8 struct_v only, or lacked either
 * the compat version or length.  Skip those fields conditionally.
 *
 * Most of the time, v, compatv, and lenv will all match the version
 * where the structure was switched over to the new macros.
 *
 * @param v current version of the encoding that the code supports/encodes
 * @param compatv oldest version that includes a __u8 compat version field
 * @param lenv oldest version that includes a __u32 length wrapper
 * @param bl buffer::list::iterator containing the encoded data
 */
#define DECODE_START_LEGACY_COMPAT_LEN_32(v, compatv, lenv, bl)		\
  __DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, 3u, bl)

#define DECODE_START_LEGACY_COMPAT_LEN_16(v, compatv, lenv, bl)		\
  __DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, 1u, bl)

/**
 * finish decode block
 *
 * @param bl buffer::list::iterator we were decoding from
 */
#define DECODE_FINISH(bl)						\
  } while (false);							\
  ::ceph::encoding_detail::finish_decode_struct(		\
    bl, struct_end, __PRETTY_FUNCTION__);

namespace ceph {

/*
 * Encoders/decoders to read from current offset in a file handle and
 * encode/decode the data according to argument types.
 */
inline ssize_t decode_file(int fd, std::string &str)
{
  buffer::list bl;
  __u32 len = 0;
  bl.read_fd(fd, sizeof(len));
  decode(len, bl);
  bl.read_fd(fd, len);
  decode(str, bl);
  return bl.length();
}

inline ssize_t decode_file(int fd, bufferptr &bp)
{
  buffer::list bl;
  __u32 len = 0;
  bl.read_fd(fd, sizeof(len));
  decode(len, bl);
  bl.read_fd(fd, len);
  auto bli = std::cbegin(bl);

  decode(bp, bli);
  return bl.length();
}
}

#endif
