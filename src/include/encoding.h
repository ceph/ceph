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

#include <concepts>
#include <string_view>
#include <type_traits>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>
#include <optional>

#ifdef ENCODE_DUMP_PATH
#include <climits>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#endif

#include "common/ceph_time.h"

#include "include/compat.h"
#include "include/int_types.h"

#include "common/container_concepts.h"

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
inline void encode_raw(const T& t, bufferlist& bl)
{
  bl.append((char*)&t, sizeof(t));
}
template<class T>
inline void decode_raw(T& t, bufferlist::const_iterator &p)
{
  p.copy(sizeof(t), (char*)&t);
}

#define WRITE_RAW_ENCODER(type)						\
  inline void encode(const type &v, ::ceph::bufferlist& bl, uint64_t features=0) { ::ceph::encode_raw(v, bl); } \
  inline void decode(type &v, ::ceph::bufferlist::const_iterator& p) { ::ceph::decode_raw(v, p); }

WRITE_RAW_ENCODER(__u8)
#ifndef _CHAR_IS_SIGNED
WRITE_RAW_ENCODER(__s8)
#endif
WRITE_RAW_ENCODER(char)
WRITE_RAW_ENCODER(ceph_le64)
WRITE_RAW_ENCODER(ceph_le32)
WRITE_RAW_ENCODER(ceph_le16)

inline void encode(const bool &v, bufferlist& bl) {
  __u8 vv = v;
  encode_raw(vv, bl);
}
inline void decode(bool &v, bufferlist::const_iterator& p) {
  __u8 vv;
  decode_raw(vv, p);
  v = vv;
}


// -----------------------------------
// int types

#define WRITE_INTTYPE_ENCODER(type, etype)				\
  inline void encode(type v, ::ceph::bufferlist& bl, uint64_t features=0) { \
    ceph_##etype e;					                \
    e = v;                                                              \
    ::ceph::encode_raw(e, bl);						\
  }									\
  inline void decode(type &v, ::ceph::bufferlist::const_iterator& p) {	\
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
  inline void encode(type v, ::ceph::bufferlist& bl, uint64_t features=0) { \
    ceph_##etype e;							\
    e = std::bit_cast<itype>(v);					\
    ::ceph::encode_raw(e, bl);						\
  }									\
  inline void decode(type &v, ::ceph::bufferlist::const_iterator& p) {	\
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
    if (bits > 2)							\
      break;								\
    char fn[PATH_MAX];							\
    snprintf(fn, sizeof(fn), ENCODE_STRINGIFY(ENCODE_DUMP_PATH) "/%s__%d.%x", #cl, getpid(), i++); \
    int fd = ::open(fn, O_WRONLY|O_TRUNC|O_CREAT|O_CLOEXEC|O_BINARY, 0644);		\
    if (fd >= 0) {							\
      ::ceph::bufferlist sub;						\
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
  inline void encode(const cl& c, ::ceph::buffer::list &bl, uint64_t features=0) { \
    ENCODE_DUMP_PRE(); c.encode(bl); ENCODE_DUMP_POST(cl); }		\
  inline void decode(cl &c, ::ceph::bufferlist::const_iterator &p) { c.decode(p); }

#define WRITE_CLASS_MEMBER_ENCODER(cl)					\
  inline void encode(const cl &c, ::ceph::bufferlist &bl) const {	\
    ENCODE_DUMP_PRE(); c.encode(bl); ENCODE_DUMP_POST(cl); }		\
  inline void decode(cl &c, ::ceph::bufferlist::const_iterator &p) { c.decode(p); }

#define WRITE_CLASS_ENCODER_FEATURES(cl)				\
  inline void encode(const cl &c, ::ceph::bufferlist &bl, uint64_t features) { \
    ENCODE_DUMP_PRE(); c.encode(bl, features); ENCODE_DUMP_POST(cl); }	\
  inline void decode(cl &c, ::ceph::bufferlist::const_iterator &p) { c.decode(p); }

#define WRITE_CLASS_ENCODER_OPTIONAL_FEATURES(cl)				\
  inline void encode(const cl &c, ::ceph::bufferlist &bl, uint64_t features = 0) { \
    ENCODE_DUMP_PRE(); c.encode(bl, features); ENCODE_DUMP_POST(cl); }	\
  inline void decode(cl &c, ::ceph::bufferlist::const_iterator &p) { c.decode(p); }

template<typename Int>
inline void encode_assign(const auto& t, ::ceph::bufferlist& bl, uint64_t features=0)
{
  Int i = static_cast<Int>(t);
  ::encode(i, bl);
}

template<typename Int>
inline void decode_assign(auto& t, ::ceph::bufferlist::const_iterator& p)
{
  Int i;
  ::decode(i, p);
  t = static_cast<std::remove_reference_t<decltype(t)>>(i);
}

namespace encoding_detail {

inline void encode_count(size_t n, bufferlist& bl)
{
  // Legacy container/string lengths are stored as 32-bit values on the wire.
  encode(static_cast<__u32>(n), bl);
}

inline __u32 decode_count(bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  return n;
}

inline void append_bytes(const void *data, size_t len, bufferlist& bl)
{
  if (len)
    bl.append(static_cast<const char *>(data), len);
}

inline void encode_bytes(const void *data, size_t len, bufferlist& bl)
{
  encode_count(len, bl);
  append_bytes(data, len, bl);
}

} // namespace encoding_detail

// -----------------------------
// buffers

// bufferptr (encapsulated)
inline void encode(const buffer::ptr& bp, bufferlist& bl)
{
  const auto len = bp.length();
  encoding_detail::encode_count(len, bl);
  if (len)
    bl.append(bp);
}
inline void decode(buffer::ptr& bp, bufferlist::const_iterator& p)
{
  const auto len = encoding_detail::decode_count(p);

  bufferlist s;
  p.copy(len, s);

  // Return without assignment:
  if (!len) {
    return;
  }

  // ...if the buffer::list contains only a single buffer, we
  // re-use it:
  if (1 == s.get_num_buffers()) {
    bp = s.front();
    return;
  }

  // ...flatten the buffer::list:
  bp = buffer::copy(s.c_str(), s.length());
}

// bufferlist (encapsulated)
inline void encode(const bufferlist& s, bufferlist& bl)
{
  encoding_detail::encode_count(s.length(), bl);
  bl.append(s);
}
inline void encode_destructively(bufferlist& s, bufferlist& bl)
{
  encoding_detail::encode_count(s.length(), bl);
  bl.claim_append(s);
}
inline void decode(bufferlist& s, bufferlist::const_iterator& p)
{
  const auto len = encoding_detail::decode_count(p);
  s.clear();
  p.copy(len, s);
}

inline void encode_nohead(const bufferlist& s, bufferlist& bl)
{
  bl.append(s);
}
inline void decode_nohead(unsigned len, bufferlist& s, bufferlist::const_iterator& p)
{
  s.clear();
  p.copy(len, s);
}

// STL container types and helper Concepts:
namespace encoding_detail {

template<typename... TraitsT>
concept needs_legacy_encoding = (not TraitsT::supported || ...);

template<typename MapT>
concept map_emplaces_key_value =
  std::constructible_from<typename MapT::value_type,
                          typename MapT::key_type,
                          typename MapT::mapped_type> &&
  requires(MapT& m, typename MapT::key_type& k,
           typename MapT::mapped_type& v) {
    m.emplace(std::move(k), std::move(v));
  };

template<typename OptionalT>
void encode_optional(const OptionalT& p, bufferlist& bl);

template<typename FnT>
void for_each_count(unsigned n, FnT&& fn);

// "nohead" as in just the elements, without the container length:
// (i.e.: [x][x][x] rather than [sz][x][x][x], like encode_range())
template<typename RangeT>
void encode_range_nohead(const RangeT& r, bufferlist& bl);

template<typename RangeT>
void encode_range_nohead(const RangeT& r, bufferlist& bl, uint64_t features);

template<typename RangeT, typename IteratorT>
void decode_range_nohead(RangeT& r, IteratorT& p);

template<typename RangeT>
void encode_range(const RangeT& r, bufferlist& bl);

template<typename RangeT>
void encode_range(const RangeT& r, bufferlist& bl, uint64_t features);

template<typename ContainerT, typename IteratorT>
void decode_by_resize_nohead(unsigned len, ContainerT& c,
                             IteratorT& p);

template<typename ContainerT>
void decode_by_resize(ContainerT& c, bufferlist::const_iterator& p);

template<typename ContainerT, typename IteratorT>
void decode_by_emplace_back(unsigned len, ContainerT& c,
                            IteratorT& p);

template<typename ContainerT>
void decode_by_emplace_back(ContainerT& c, bufferlist::const_iterator& p);

template<typename ContainerT>
void clear_and_reserve(ContainerT& c, size_t n);

template<typename ContainerT, typename IteratorT>
void decode_by_insert(unsigned len, ContainerT& c,
                      IteratorT& p);

template<typename ContainerT>
void decode_by_insert(ContainerT& c, bufferlist::const_iterator& p);

template<typename RangeT>
void encode_shared_ptr_range(const RangeT& r, bufferlist& bl);

template<typename RangeT>
void encode_shared_ptr_range(const RangeT& r, bufferlist& bl,
                             uint64_t features);

template<typename ValueT, typename EncodeFnT>
void append_cached_default_encoding(std::optional<std::string>& bytes,
                                    bufferlist& bl,
                                    EncodeFnT&& encode_value);

template<typename RangeT, typename EncodeFnT>
void encode_shared_ptr_range_with(const RangeT& r, bufferlist& bl,
                                  EncodeFnT&& encode_value);

template<typename ContainerT>
void decode_shared_ptr_sequence(ContainerT& c,
                                bufferlist::const_iterator& p);

template<typename PairT>
void decode_pair_members(PairT& item, bufferlist::const_iterator& p);

template<typename MapT>
void encode_pair_range_nohead(const MapT& m, bufferlist& bl);

template<typename MapT>
void encode_pair_range_nohead(const MapT& m, bufferlist& bl,
                              uint64_t features);

template<typename MapT>
void encode_pair_range(const MapT& m, bufferlist& bl);

template<typename MapT>
void encode_pair_range(const MapT& m, bufferlist& bl, uint64_t features);

template<typename MapT, typename IteratorT>
void decode_map_entries_by_subscript(unsigned n, MapT& m,
                                     IteratorT& p);

template<typename MapT, typename IteratorT>
void decode_map_entries_by_emplace(unsigned n, MapT& m,
                                   IteratorT& p);

template<typename MapT, typename IteratorT>
void decode_map_entries_by_try_emplace(unsigned n, MapT& m,
                                       IteratorT& p);

template<typename MapT, typename IteratorT>
requires map_emplaces_key_value<MapT>
void decode_map_entries_no_clear(unsigned n, MapT& m, IteratorT& p);

template<typename MapT, typename IteratorT>
requires (!map_emplaces_key_value<MapT> &&
          ceph::concepts::has_try_emplace_key<MapT>)
void decode_map_entries_no_clear(unsigned n, MapT& m, IteratorT& p);

template<typename MapT, typename IteratorT>
requires map_emplaces_key_value<MapT>
void decode_map_entries(unsigned n, MapT& m, IteratorT& p);

template<typename MapT, typename IteratorT>
requires (!map_emplaces_key_value<MapT> &&
          ceph::concepts::has_subscript_operator<MapT>)
void decode_map_entries(unsigned n, MapT& m, IteratorT& p);

template<typename MapT>
void decode_map_by_subscript(unsigned n, MapT& m,
                             bufferlist::const_iterator& p);

template<typename MapT>
void decode_map_by_subscript(MapT& m, bufferlist::const_iterator& p);

template<typename MapT>
void decode_map(unsigned n, MapT& m, bufferlist::const_iterator& p);

template<typename MapT>
void decode_map(MapT& m, bufferlist::const_iterator& p);

} // namespace encoding_detail

// std::pair<A, B>
template<class A, class B,
	 typename a_traits = denc_traits<A>, typename b_traits = denc_traits<B>>
requires encoding_detail::needs_legacy_encoding<a_traits, b_traits>
inline void encode(const std::pair<A,B> &p, bufferlist &bl, uint64_t features)
{
  encode(p.first, bl, features);
  encode(p.second, bl, features);
}
template<class A, class B,
	 typename a_traits = denc_traits<A>, typename b_traits = denc_traits<B>>
requires encoding_detail::needs_legacy_encoding<a_traits, b_traits>
inline void encode(const std::pair<A,B> &p, bufferlist &bl)
{
  encode(p.first, bl);
  encode(p.second, bl);
}
template<class A, class B,
	 typename a_traits = denc_traits<A>, typename b_traits = denc_traits<B>>
requires encoding_detail::needs_legacy_encoding<a_traits, b_traits>
inline void decode(std::pair<A,B> &pa, bufferlist::const_iterator &p)
{
  encoding_detail::decode_pair_members(pa, p);
}

namespace encoding_detail {

template<typename OptionalT>
void encode_optional(const OptionalT& p, bufferlist& bl)
{
  __u8 present = static_cast<bool>(p);
  encode(present, bl);
  if (!p) {
    return;
  }

  encode(*p, bl);
}

template<typename FnT>
void for_each_count(unsigned n, FnT&& fn)
{
  for (auto i = 0u; i < n; ++i) {
    fn();
  }
}

template<typename RangeT>
void encode_range_nohead(const RangeT& r, bufferlist& bl)
{
  for (const auto& item : r) {
    encode(item, bl);
  }
}

template<typename RangeT>
void encode_range_nohead(const RangeT& r, bufferlist& bl, uint64_t features)
{
  for (const auto& item : r) {
    encode(item, bl, features);
  }
}

template<typename RangeT, typename IteratorT>
void decode_range_nohead(RangeT& r, IteratorT& p)
{
  for (auto& item : r) {
    decode(item, p);
  }
}

template<typename RangeT>
void encode_range(const RangeT& r, bufferlist& bl)
{
  encode_count(r.size(), bl);
  encode_range_nohead(r, bl);
}

template<typename RangeT>
void encode_range(const RangeT& r, bufferlist& bl, uint64_t features)
{
  encode_count(r.size(), bl);
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
void decode_by_resize(ContainerT& c, bufferlist::const_iterator& p)
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
void decode_by_emplace_back(ContainerT& c, bufferlist::const_iterator& p)
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
void decode_by_insert(ContainerT& c, bufferlist::const_iterator& p)
{
  decode_by_insert(decode_count(p), c, p);
}

template<typename RangeT>
void encode_shared_ptr_range(const RangeT& r, bufferlist& bl)
{
  encode_shared_ptr_range_with(r, bl, [](const auto& value, auto& out) {
    encode(value, out);
  });
}

template<typename RangeT>
void encode_shared_ptr_range(const RangeT& r, bufferlist& bl,
                             uint64_t features)
{
  encode_shared_ptr_range_with(r, bl, [features](const auto& value, auto& out) {
    encode(value, out, features);
  });
}

template<typename ValueT, typename EncodeFnT>
void append_cached_default_encoding(std::optional<std::string>& bytes,
                                    bufferlist& bl,
                                    EncodeFnT&& encode_value)
{
  if (!bytes) {
    auto tmp = bufferlist {};
    encode_value(ValueT {}, tmp);
    bytes = tmp.to_str();
  }

  bl.append(std::string_view { *bytes });
}

template<typename RangeT, typename EncodeFnT>
void encode_shared_ptr_range_with(const RangeT& r, bufferlist& bl,
                                  EncodeFnT&& encode_value)
{
  using value_type = typename RangeT::value_type::element_type;

  encode_count(r.size(), bl);

  auto null_bytes = std::optional<std::string> {};

  for (const auto& ref : r) {
    if (ref) {
      encode_value(*ref, bl);
      continue;
    }

    append_cached_default_encoding<value_type>(null_bytes, bl, encode_value);
  }
}

template<typename ContainerT>
void decode_shared_ptr_sequence(ContainerT& c,
                                bufferlist::const_iterator& p)
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
void decode_pair_members(PairT& item, bufferlist::const_iterator& p)
{
  decode(item.first, p);
  decode(item.second, p);
}

template<typename MapT>
void encode_pair_range_nohead(const MapT& m, bufferlist& bl)
{
  for (const auto& item : m) {
    encode(item.first, bl);
    encode(item.second, bl);
  }
}

template<typename MapT>
void encode_pair_range_nohead(const MapT& m, bufferlist& bl,
                              uint64_t features)
{
  for (const auto& item : m) {
    encode(item.first, bl, features);
    encode(item.second, bl, features);
  }
}

template<typename MapT>
void encode_pair_range(const MapT& m, bufferlist& bl)
{
  encode_count(m.size(), bl);
  encode_pair_range_nohead(m, bl);
}

template<typename MapT>
void encode_pair_range(const MapT& m, bufferlist& bl, uint64_t features)
{
  encode_count(m.size(), bl);
  encode_pair_range_nohead(m, bl, features);
}

// Decodes n kv entries into containers that support operator[]:
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

// Decide between encoding associative containers by operator[] or by
// emplace():
template<typename MapT, typename IteratorT>
void decode_map_entries_by_try_emplace(unsigned n, MapT& m,
                                       IteratorT& p)
{
  for_each_count(n, [&m, &p] {
    typename MapT::key_type k;
    decode(k, p);
    // Preserve an existing mapped value, but still consume the encoded value.
    auto [it, inserted] = m.try_emplace(std::move(k));
    if (inserted) {
      decode(it->second, p);
      return;
    }

    typename MapT::mapped_type discarded;
    decode(discarded, p);
  });
}

template<typename MapT, typename IteratorT>
requires map_emplaces_key_value<MapT>
void decode_map_entries_no_clear(unsigned n, MapT& m, IteratorT& p)
{
  decode_map_entries_by_emplace(n, m, p);
}

template<typename MapT, typename IteratorT>
requires (!map_emplaces_key_value<MapT> &&
          ceph::concepts::has_try_emplace_key<MapT>)
void decode_map_entries_no_clear(unsigned n, MapT& m, IteratorT& p)
{
  decode_map_entries_by_try_emplace(n, m, p);
}

template<typename MapT, typename IteratorT>
requires map_emplaces_key_value<MapT>
void decode_map_entries(unsigned n, MapT& m, IteratorT& p)
{
  decode_map_entries_by_emplace(n, m, p);
}

template<typename MapT, typename IteratorT>
requires (!map_emplaces_key_value<MapT> &&
          ceph::concepts::has_subscript_operator<MapT>)
void decode_map_entries(unsigned n, MapT& m, IteratorT& p)
{
  decode_map_entries_by_subscript(n, m, p);
}

template<typename MapT>
void decode_map_by_subscript(unsigned n, MapT& m,
                             bufferlist::const_iterator& p)
{
  clear_and_reserve(m, n);
  decode_map_entries_by_subscript(n, m, p);
}

template<typename MapT>
void decode_map_by_subscript(MapT& m, bufferlist::const_iterator& p)
{
  decode_map_by_subscript(decode_count(p), m, p);
}

template<typename MapT>
void decode_map(unsigned n, MapT& m, bufferlist::const_iterator& p)
{
  clear_and_reserve(m, n);
  decode_map_entries(n, m, p);
}

template<typename MapT>
void decode_map(MapT& m, bufferlist::const_iterator& p)
{
  decode_map(decode_count(p), m, p);
}

} // namespace encoding_detail

// full bl decoder
template<class T>
inline void decode(T &o, const bufferlist& bl)
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
 * @param bl bufferlist to encode to
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
 * @param bl bufferlist we were encoding to
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

// Fixed size of the versioned encode/decode wrapper header.
inline constexpr auto struct_header_len() noexcept
{
  return sizeof(__u8) + sizeof(__u8) + sizeof(ceph_le32);
}

struct struct_header final {
  __u8 v = 0;
  __u8 compat = 0;
  __u32 len = 0;
};

// Read the encoded version, compatibility version, and payload length.
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

// Reject payloads that require a newer decoder than this code provides.
inline void check_decode_compat(__u8 code_v, __u8 struct_v,
                                __u8 struct_compat,
                                const char *func)
{
  if (struct_compat > code_v) {
    throw ::ceph::buffer::malformed_input(
      DECODE_ERR_NO_COMPAT(func, code_v, struct_v, struct_compat));
  }
}

// Compute the payload end offset after checking that the body is present.
template<typename IteratorT>
unsigned checked_struct_end(IteratorT& bl, __u32 struct_len,
                            const char *func)
{
  if (struct_len > bl.get_remaining()) {
    throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(func));
  }

  return bl.get_off() + struct_len;
}

// DECODE_START path: read and validate the standard wrapper header.
template<typename VersionT, typename IteratorT>
unsigned decode_struct_start(__u8 code_v, VersionT& struct_v,
                             __u8& struct_compat, __u32& struct_len,
                             IteratorT& bl, const char *func)
{
  const auto header = read_struct_header(bl);
  struct_v = header.v;
  struct_compat = header.compat;
  struct_len = header.len;

  check_decode_compat(code_v, struct_v, struct_compat, func);
  return checked_struct_end(bl, struct_len, func);
}

// Legacy decode path: read only the wrapper fields present in this version.
template<typename VersionT, typename IteratorT>
unsigned decode_legacy_struct_start(__u8 code_v, VersionT& struct_v,
                                    __u8 compat_v, __u8 len_v,
                                    unsigned skip_v, IteratorT& bl,
                                    const char *func)
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

// ENCODE_FINISH path: patch version, compatibility, and payload length.
template<typename FillerT>
void finish_encode_struct(FillerT& filler, __u8 struct_v,
                          __u8& struct_compat, ceph_le32& struct_len,
                          __u8 new_struct_compat, const bufferlist& bl,
                          size_t starting_bl_len)
{
  if (new_struct_compat) {
    struct_compat = new_struct_compat;
  }

  struct_len = static_cast<__u32>(bl.length() - starting_bl_len);
  filler.copy_in(sizeof(struct_v),
                 reinterpret_cast<const char *>(&struct_v));
  filler.copy_in(sizeof(struct_compat),
                 reinterpret_cast<const char *>(&struct_compat));
  filler.copy_in(sizeof(struct_len),
                 reinterpret_cast<const char *>(&struct_len));
}

// DECODE_FINISH path: reject overread and skip unread trailing fields.
template<typename IteratorT>
void finish_decode_struct(IteratorT& bl, unsigned struct_end,
                          const char *func)
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

// Preserve an unknown encoded payload, including its wrapper header.
template<typename PayloadT, typename IteratorT>
void decode_unknown(PayloadT& payload, IteratorT& bl, const char *func)
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
 * @param bl bufferlist::iterator for the encoded data
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
 * @param bl bufferlist::iterator containing the encoded data
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
 * @param bl bufferlist::iterator containing the encoded data
 */
#define DECODE_START_LEGACY_COMPAT_LEN_32(v, compatv, lenv, bl)		\
  __DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, 3u, bl)

#define DECODE_START_LEGACY_COMPAT_LEN_16(v, compatv, lenv, bl)		\
  __DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, 1u, bl)

/**
 * finish decode block
 *
 * @param bl bufferlist::iterator we were decoding from
 */
#define DECODE_FINISH(bl)						\
  } while (false);							\
  ::ceph::encoding_detail::finish_decode_struct(		\
    bl, struct_end, __PRETTY_FUNCTION__);

#endif
