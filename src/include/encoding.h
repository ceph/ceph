// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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
#ifndef CEPH_ENCODING_H
#define CEPH_ENCODING_H

#include <concepts>
#include <string_view>

#include "common/ceph_time.h"

#include "include/int_types.h"

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
// To achive that, we reinterpret the values as integers first, which are
// byte-swapped via the ceph_le types as above.  The extra conversions
// are optimized away on little-endian machines by the compiler.
#define WRITE_FLTTYPE_ENCODER(type, itype, etype)			\
  static_assert(sizeof(type) == sizeof(itype));				\
  static_assert(std::numeric_limits<type>::is_iec559,			\
	      "floating-point type not using IEEE754 format");		\
  inline void encode(type v, ::ceph::bufferlist& bl, uint64_t features=0) { \
    ceph_##etype e;							\
    e = *reinterpret_cast<itype *>(&v);					\
    ::ceph::encode_raw(e, bl);						\
  }									\
  inline void decode(type &v, ::ceph::bufferlist::const_iterator& p) {	\
    ceph_##etype e;							\
    ::ceph::decode_raw(e, p);						\
    *reinterpret_cast<itype *>(&v) = e;					\
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
inline void decode(buffer::ptr& bp, bufferlist::const_iterator& p)
{
  __u32 len;
  decode(len, p);

  bufferlist s;
  p.copy(len, s);

  if (len) {
    if (s.get_num_buffers() == 1)
      bp = s.front();
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
inline void decode(bufferlist& s, bufferlist::const_iterator& p)
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
inline void decode_nohead(unsigned len, bufferlist& s, bufferlist::const_iterator& p)
{
  s.clear();
  p.copy(len, s);
}

// Time, since the templates are defined in std::chrono. The default encodings
// for time_point and duration are backward-compatible with utime_t, but
// truncate seconds to 32 bits so are not guaranteed to round-trip.

template<clock_with_timespec Clock, typename Duration>
void encode(const std::chrono::time_point<Clock, Duration>& t,
	    ceph::bufferlist &bl) {
  auto ts = Clock::to_timespec(t);
  // A 32 bit count of seconds causes me vast unhappiness.
  uint32_t s = ts.tv_sec;
  uint32_t ns = ts.tv_nsec;
  encode(s, bl);
  encode(ns, bl);
}

template<clock_with_timespec Clock, typename Duration>
void decode(std::chrono::time_point<Clock, Duration>& t,
	    bufferlist::const_iterator& p) {
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
	    ceph::bufferlist &bl) {
  using namespace std::chrono;
  int32_t s = duration_cast<seconds>(d).count();
  int32_t ns = (duration_cast<nanoseconds>(d) % seconds(1)).count();
  encode(s, bl);
  encode(ns, bl);
}

template<std::integral Rep, typename Period>
void decode(std::chrono::duration<Rep, Period>& d,
	    bufferlist::const_iterator& p) {
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
                       ceph::bufferlist &bl) {
  const Rep r = d.count();
  encode(r, bl);
}

template <std::integral Rep, typename Period>
void round_trip_decode(std::chrono::duration<Rep, Period>& d,
                       bufferlist::const_iterator& p) {
  Rep r;
  decode(r, p);
  d = std::chrono::duration<Rep, Period>(r);
}

template <typename Clock, typename Duration>
void round_trip_encode(const std::chrono::time_point<Clock, Duration>& t,
                       ceph::bufferlist &bl) {
  round_trip_encode(t.time_since_epoch(), bl);
}

template <typename Clock, typename Duration>
void round_trip_decode(std::chrono::time_point<Clock, Duration>& t,
                       bufferlist::const_iterator& p) {
  Duration dur;
  round_trip_decode(dur, p);
  t = std::chrono::time_point<Clock, Duration>(dur);
}

// -----------------------------
// STL container types

template<class A, class B,
	 typename a_traits=denc_traits<A>, typename b_traits=denc_traits<B>>
inline std::enable_if_t<!a_traits::supported || !b_traits::supported>
encode(const std::pair<A,B> &p, bufferlist &bl, uint64_t features);
template<class A, class B,
	 typename a_traits=denc_traits<A>, typename b_traits=denc_traits<B>>
inline std::enable_if_t<!a_traits::supported ||
			!b_traits::supported>
encode(const std::pair<A,B> &p, bufferlist &bl);
template<class A, class B,
	 typename a_traits=denc_traits<A>, typename b_traits=denc_traits<B>>
inline std::enable_if_t<!a_traits::supported ||
			!b_traits::supported>
decode(std::pair<A,B> &pa, bufferlist::const_iterator &p);

// full bl decoder
template<class T>
inline void decode(T &o, const bufferlist& bl)
{
  auto p = bl.begin();
  decode(o, p);
  ceph_assert(p.end());
}

// std::pair<A,B>
template<class A, class B,
	 typename a_traits, typename b_traits>
inline std::enable_if_t<!a_traits::supported || !b_traits::supported>
  encode(const std::pair<A,B> &p, bufferlist &bl, uint64_t features)
{
  encode(p.first, bl, features);
  encode(p.second, bl, features);
}
template<class A, class B,
	 typename a_traits, typename b_traits>
inline std::enable_if_t<!a_traits::supported ||
			!b_traits::supported>
  encode(const std::pair<A,B> &p, bufferlist &bl)
{
  encode(p.first, bl);
  encode(p.second, bl);
}
template<class A, class B, typename a_traits, typename b_traits>
inline std::enable_if_t<!a_traits::supported ||
			!b_traits::supported>
  decode(std::pair<A,B> &pa, bufferlist::const_iterator &p)
{
  decode(pa.first, p);
  decode(pa.second, p);
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
  auto filler = (bl).append_hole(sizeof(struct_v) +	     \
    sizeof(struct_compat) + sizeof(struct_len));	     \
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
  if (new_struct_compat) {                                   \
    struct_compat = new_struct_compat;                       \
  }                                                          \
  struct_len = (bl).length() - starting_bl_len;              \
  filler.copy_in(sizeof(struct_v), (char *)&struct_v);       \
  filler.copy_in(sizeof(struct_compat),			     \
    (char *)&struct_compat);				     \
  filler.copy_in(sizeof(struct_len), (char *)&struct_len);

#define ENCODE_FINISH(bl) ENCODE_FINISH_NEW_COMPAT(bl, 0)

#define DECODE_ERR_OLDVERSION(func, v, compatv)					\
  (std::string(func) + " no longer understands old encoding version " #v " < " + std::to_string(compatv))

#define DECODE_ERR_NO_COMPAT(func, code_v, v, compatv)					\
  ("Decoder at '" + std::string(func) + "' v=" + std::to_string(code_v) +		\
  " cannot decode v=" + std::to_string(v) + " minimal_decoder=" + std::to_string(compatv))

#define DECODE_ERR_PAST(func) \
  (std::string(func) + " decode past end of struct encoding")

/**
 * check for very old encoding
 *
 * If the encoded data is older than oldestv, raise an exception.
 *
 * @param oldestv oldest version of the code we can successfully decode.
 */
#define DECODE_OLDEST(oldestv)						\
  if (struct_v < oldestv)						\
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
  using ::ceph::decode;							\
  decode(struct_v.v, bl);						\
  decode(struct_compat, bl);						\
  if (_v < struct_compat)						\
    throw ::ceph::buffer::malformed_input(DECODE_ERR_NO_COMPAT(__PRETTY_FUNCTION__, _v, struct_v.v, struct_compat)); \
  __u32 struct_len;							\
  decode(struct_len, bl);						\
  if (struct_len > bl.get_remaining())					\
    throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
  unsigned struct_end = bl.get_off() + struct_len;			\
  do {

#define DECODE_START_UNCHECKED(v, bl)					\
  __u8 struct_v, struct_compat;						\
  using ::ceph::decode;							\
  decode(struct_v, bl);						\
  decode(struct_compat, bl);						\
  if (v < struct_compat)						\
    throw ::ceph::buffer::malformed_input(DECODE_ERR_NO_COMPAT(__PRETTY_FUNCTION__, v, struct_v, struct_compat)); \
  __u32 struct_len;							\
  decode(struct_len, bl);						\
  if (struct_len > bl.get_remaining())					\
    throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
  unsigned struct_end = bl.get_off() + struct_len;			\
  do {

#define DECODE_UNKNOWN(payload, bl)					\
  do {                                                                  \
    __u8 struct_v, struct_compat;					\
    using ::ceph::decode;						\
    decode(struct_v, bl);						\
    decode(struct_compat, bl);						\
    __u32 struct_len;							\
    decode(struct_len, bl);						\
    if (struct_len > bl.get_remaining())				\
      throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
    payload.clear();                                                    \
    using ::ceph::encode;						\
    encode(struct_v, payload);                                          \
    encode(struct_compat, payload);                                     \
    encode(struct_len, payload);                                        \
    bl.copy(struct_len, payload);                                       \
  } while (0)

/* BEWARE: any change to this macro MUST be also reflected in the duplicative
 * DECODE_START_LEGACY_COMPAT_LEN! */
#define __DECODE_START_LEGACY_COMPAT_LEN(_v, compatv, lenv, skip_v, bl)	\
  using ::ceph::decode;							\
  StructVChecker<_v> struct_v;						\
  decode(struct_v.v, bl);						\
  if (struct_v.v >= compatv) {						\
    __u8 struct_compat;							\
    decode(struct_compat, bl);					\
    if (_v < struct_compat)						\
      throw ::ceph::buffer::malformed_input(DECODE_ERR_NO_COMPAT(__PRETTY_FUNCTION__, _v, struct_v.v, struct_compat)); \
  } else if (skip_v) {							\
    if (bl.get_remaining() < skip_v)					\
      throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
    bl +=  skip_v;							\
  }									\
  unsigned struct_end = 0;						\
  if (struct_v.v >= lenv) {						\
    __u32 struct_len;							\
    decode(struct_len, bl);						\
    if (struct_len > bl.get_remaining())				\
      throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
    struct_end = bl.get_off() + struct_len;				\
  }									\
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

/* BEWARE: this is duplication of __DECODE_START_LEGACY_COMPAT_LEN which
 * MUST be changed altogether. For the rationale behind code duplication,
 * please `git blame` and refer to the commit message. */
#define DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, bl)		\
  using ::ceph::decode;							\
  __u8 struct_v;							\
  decode(struct_v, bl);							\
  if (struct_v >= compatv) {						\
    __u8 struct_compat;							\
    decode(struct_compat, bl);						\
    if (v < struct_compat)						\
      throw ::ceph::buffer::malformed_input(DECODE_ERR_NO_COMPAT(	\
	__PRETTY_FUNCTION__, v, struct_v, struct_compat));		\
  }									\
  unsigned struct_end = 0;						\
  if (struct_v >= lenv) {						\
    __u32 struct_len;							\
    decode(struct_len, bl);						\
    if (struct_len > bl.get_remaining())				\
      throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
    struct_end = bl.get_off() + struct_len;				\
  }									\
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
  if (struct_end) {							\
    if (bl.get_off() > struct_end)					\
      throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
    if (bl.get_off() < struct_end)					\
      bl += struct_end - bl.get_off();					\
  }

#endif
