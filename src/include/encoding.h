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
#ifndef CEPH_ENCODING_H
#define CEPH_ENCODING_H

#include <set>
#include <map>
#include <deque>
#include <vector>
#include <string>
#include <string_view>
#include <tuple>
#include <boost/container/small_vector.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/tuple/tuple.hpp>

#include "include/unordered_map.h"
#include "include/unordered_set.h"
#include "common/ceph_time.h"

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

// FIXME: we need to choose some portable floating point encoding here
WRITE_RAW_ENCODER(float)
WRITE_RAW_ENCODER(double)

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
    int fd = ::open(fn, O_WRONLY|O_TRUNC|O_CREAT|O_CLOEXEC, 0644);		\
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


// string
inline void encode(std::string_view s, bufferlist& bl, uint64_t features=0)
{
  __u32 len = s.length();
  encode(len, bl);
  if (len)
    bl.append(s.data(), len);
}
inline void encode(const std::string& s, bufferlist& bl, uint64_t features=0)
{
  return encode(std::string_view(s), bl, features);
}
inline void decode(std::string& s, bufferlist::const_iterator& p)
{
  __u32 len;
  decode(len, p);
  s.clear();
  p.copy(len, s);
}

inline void encode_nohead(std::string_view s, bufferlist& bl)
{
  bl.append(s.data(), s.length());
}
inline void encode_nohead(const std::string& s, bufferlist& bl)
{
  encode_nohead(std::string_view(s), bl);
}
inline void decode_nohead(int len, std::string& s, bufferlist::const_iterator& p)
{
  s.clear();
  p.copy(len, s);
}

// const char* (encode only, string compatible)
inline void encode(const char *s, bufferlist& bl) 
{
  encode(std::string_view(s, strlen(s)), bl);
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
inline void decode_nohead(int len, bufferlist& s, bufferlist::const_iterator& p)
{
  s.clear();
  p.copy(len, s);
}

// Time, since the templates are defined in std::chrono

template<typename Clock, typename Duration,
         typename std::enable_if_t<converts_to_timespec_v<Clock>>* = nullptr>
void encode(const std::chrono::time_point<Clock, Duration>& t,
	    ceph::bufferlist &bl) {
  auto ts = Clock::to_timespec(t);
  // A 32 bit count of seconds causes me vast unhappiness.
  uint32_t s = ts.tv_sec;
  uint32_t ns = ts.tv_nsec;
  encode(s, bl);
  encode(ns, bl);
}

template<typename Clock, typename Duration,
         typename std::enable_if_t<converts_to_timespec_v<Clock>>* = nullptr>
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

template<typename Rep, typename Period,
         typename std::enable_if_t<std::is_integral_v<Rep>>* = nullptr>
void encode(const std::chrono::duration<Rep, Period>& d,
	    ceph::bufferlist &bl) {
  using namespace std::chrono;
  uint32_t s = duration_cast<seconds>(d).count();
  uint32_t ns = (duration_cast<nanoseconds>(d) % seconds(1)).count();
  encode(s, bl);
  encode(ns, bl);
}

template<typename Rep, typename Period,
         typename std::enable_if_t<std::is_integral_v<Rep>>* = nullptr>
void decode(std::chrono::duration<Rep, Period>& d,
	    bufferlist::const_iterator& p) {
  uint32_t s;
  uint32_t ns;
  decode(s, p);
  decode(ns, p);
  d = std::chrono::seconds(s) + std::chrono::nanoseconds(ns);
}

// -----------------------------
// STL container types

template<typename T>
inline void encode(const boost::optional<T> &p, bufferlist &bl);
template<typename T>
inline void decode(boost::optional<T> &p, bufferlist::const_iterator &bp);
template<class A, class B, class C>
inline void encode(const boost::tuple<A, B, C> &t, bufferlist& bl);
template<class A, class B, class C>
inline void decode(boost::tuple<A, B, C> &t, bufferlist::const_iterator &bp);
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
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const std::list<T, Alloc>& ls, bufferlist& bl);
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const std::list<T,Alloc>& ls, bufferlist& bl, uint64_t features);
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode(std::list<T,Alloc>& ls, bufferlist::const_iterator& p);
template<class T, class Alloc>
inline void encode(const std::list<std::shared_ptr<T>, Alloc>& ls,
		   bufferlist& bl);
template<class T, class Alloc>
inline void encode(const std::list<std::shared_ptr<T>, Alloc>& ls,
		   bufferlist& bl, uint64_t features);
template<class T, class Alloc>
inline void decode(std::list<std::shared_ptr<T>, Alloc>& ls,
		   bufferlist::const_iterator& p);
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const std::set<T,Comp,Alloc>& s, bufferlist& bl);
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode(std::set<T,Comp,Alloc>& s, bufferlist::const_iterator& p);
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode_nohead(const std::set<T,Comp,Alloc>& s, bufferlist& bl);
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode_nohead(int len, std::set<T,Comp,Alloc>& s, bufferlist::iterator& p);
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const boost::container::flat_set<T, Comp, Alloc>& s, bufferlist& bl);
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode(boost::container::flat_set<T, Comp, Alloc>& s, bufferlist::const_iterator& p);
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode_nohead(const boost::container::flat_set<T, Comp, Alloc>& s,
	      bufferlist& bl);
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode_nohead(int len, boost::container::flat_set<T, Comp, Alloc>& s,
	      bufferlist::iterator& p);
template<class T, class Comp, class Alloc>
inline void encode(const std::multiset<T,Comp,Alloc>& s, bufferlist& bl);
template<class T, class Comp, class Alloc>
inline void decode(std::multiset<T,Comp,Alloc>& s, bufferlist::const_iterator& p);
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const std::vector<T,Alloc>& v, bufferlist& bl, uint64_t features);
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const std::vector<T,Alloc>& v, bufferlist& bl);
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode(std::vector<T,Alloc>& v, bufferlist::const_iterator& p);
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode_nohead(const std::vector<T,Alloc>& v, bufferlist& bl);
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode_nohead(int len, std::vector<T,Alloc>& v, bufferlist::const_iterator& p);
template<class T,class Alloc>
inline void encode(const std::vector<std::shared_ptr<T>,Alloc>& v,
		   bufferlist& bl,
		   uint64_t features);
template<class T, class Alloc>
inline void encode(const std::vector<std::shared_ptr<T>,Alloc>& v,
		   bufferlist& bl);
template<class T, class Alloc>
inline void decode(std::vector<std::shared_ptr<T>,Alloc>& v,
		   bufferlist::const_iterator& p);
// small_vector
template<class T, std::size_t N, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl, uint64_t features);
template<class T, std::size_t N, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl);
template<class T, std::size_t N, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode(boost::container::small_vector<T,N,Alloc>& v, bufferlist::const_iterator& p);
template<class T, std::size_t N, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode_nohead(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl);
template<class T, std::size_t N, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode_nohead(int len, boost::container::small_vector<T,N,Alloc>& v, bufferlist::const_iterator& p);
// std::map
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported ||
			!u_traits::supported>
encode(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
encode(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl, uint64_t features);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
decode(std::map<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p);
template<class T, class U, class Comp, class Alloc>
inline void decode_noclear(std::map<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
encode_nohead(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
encode_nohead(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl, uint64_t features);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
decode_nohead(int n, std::map<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
encode(const boost::container::flat_map<T,U,Comp,Alloc>& m, bufferlist& bl);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
encode(const boost::container::flat_map<T,U,Comp,Alloc>& m, bufferlist& bl,
       uint64_t features);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
decode(boost::container::flat_map<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p);
template<class T, class U, class Comp, class Alloc>
inline void decode_noclear(boost::container::flat_map<T,U,Comp,Alloc>& m,
			   bufferlist::const_iterator& p);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
encode_nohead(const boost::container::flat_map<T,U,Comp,Alloc>& m,
	      bufferlist& bl);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
encode_nohead(const boost::container::flat_map<T,U,Comp,Alloc>& m,
	      bufferlist& bl, uint64_t features);
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
decode_nohead(int n, boost::container::flat_map<T,U,Comp,Alloc>& m,
	      bufferlist::const_iterator& p);
template<class T, class U, class Comp, class Alloc>
inline void encode(const std::multimap<T,U,Comp,Alloc>& m, bufferlist& bl);
template<class T, class U, class Comp, class Alloc>
inline void decode(std::multimap<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p);
template<class T, class U, class Hash, class Pred, class Alloc>
inline void encode(const unordered_map<T,U,Hash,Pred,Alloc>& m, bufferlist& bl,
		   uint64_t features);
template<class T, class U, class Hash, class Pred, class Alloc>
inline void encode(const unordered_map<T,U,Hash,Pred,Alloc>& m, bufferlist& bl);
template<class T, class U, class Hash, class Pred, class Alloc>
inline void decode(unordered_map<T,U,Hash,Pred,Alloc>& m, bufferlist::const_iterator& p);
template<class T, class Hash, class Pred, class Alloc>
inline void encode(const ceph::unordered_set<T,Hash,Pred,Alloc>& m, bufferlist& bl);
template<class T, class Hash, class Pred, class Alloc>
inline void decode(ceph::unordered_set<T,Hash,Pred,Alloc>& m, bufferlist::const_iterator& p);
template<class T, class Alloc>
inline void encode(const std::deque<T,Alloc>& ls, bufferlist& bl, uint64_t features);
template<class T, class Alloc>
inline void encode(const std::deque<T,Alloc>& ls, bufferlist& bl);
template<class T, class Alloc>
inline void decode(std::deque<T,Alloc>& ls, bufferlist::const_iterator& p);
template<class T, size_t N, typename traits = denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const std::array<T, N>& v, bufferlist& bl, uint64_t features);
template<class T, size_t N, typename traits = denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const std::array<T, N>& v, bufferlist& bl);
template<class T, size_t N, typename traits = denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode(std::array<T, N>& v, bufferlist::const_iterator& p);

// full bl decoder
template<class T>
inline void decode(T &o, const bufferlist& bl)
{
  auto p = bl.begin();
  decode(o, p);
  ceph_assert(p.end());
}

// boost optional
template<typename T>
inline void encode(const boost::optional<T> &p, bufferlist &bl)
{
  __u8 present = static_cast<bool>(p);
  encode(present, bl);
  if (p)
    encode(p.get(), bl);
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
template<typename T>
inline void decode(boost::optional<T> &p, bufferlist::const_iterator &bp)
{
  __u8 present;
  decode(present, bp);
  if (present) {
    p = T{};
    decode(p.get(), bp);
  } else {
    p = boost::none;
  }
}
#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

// std::tuple
template<typename... Ts>
inline void encode(const std::tuple<Ts...> &t, bufferlist& bl)
{
  ceph::for_each(t, [&bl](const auto& e) {
      encode(e, bl);
    });
}
template<typename... Ts>
inline void decode(std::tuple<Ts...> &t, bufferlist::const_iterator &bp)
{
  ceph::for_each(t, [&bp](auto& e) {
      decode(e, bp);
    });
}

//triple boost::tuple
template<class A, class B, class C>
inline void encode(const boost::tuple<A, B, C> &t, bufferlist& bl)
{
  encode(boost::get<0>(t), bl);
  encode(boost::get<1>(t), bl);
  encode(boost::get<2>(t), bl);
}
template<class A, class B, class C>
inline void decode(boost::tuple<A, B, C> &t, bufferlist::const_iterator &bp)
{
  decode(boost::get<0>(t), bp);
  decode(boost::get<1>(t), bp);
  decode(boost::get<2>(t), bp);
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

// std::list<T>
template<class T, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode(const std::list<T, Alloc>& ls, bufferlist& bl)
{
  __u32 n = (__u32)(ls.size());  // c++11 std::list::size() is O(1)
  encode(n, bl);
  for (auto p = ls.begin(); p != ls.end(); ++p)
    encode(*p, bl);
}
template<class T, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode(const std::list<T,Alloc>& ls, bufferlist& bl, uint64_t features)
{
  // should i pre- or post- count?
  if (!ls.empty()) {
    unsigned pos = bl.length();
    unsigned n = 0;
    encode(n, bl);
    for (auto p = ls.begin(); p != ls.end(); ++p) {
      n++;
      encode(*p, bl, features);
    }
    ceph_le32 en;
    en = n;
    bl.copy_in(pos, sizeof(en), (char*)&en);
  } else {
    __u32 n = (__u32)(ls.size());    // FIXME: this is slow on a list.
    encode(n, bl);
    for (auto p = ls.begin(); p != ls.end(); ++p)
      encode(*p, bl, features);
  }
}
template<class T, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  decode(std::list<T,Alloc>& ls, bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    ls.emplace_back();
    decode(ls.back(), p);
  }
}

// std::list<std::shared_ptr<T>>
template<class T, class Alloc>
inline void encode(const std::list<std::shared_ptr<T>, Alloc>& ls,
		   bufferlist& bl)
{
  __u32 n = (__u32)(ls.size());  // c++11 std::list::size() is O(1)
  encode(n, bl);
  for (const auto& ref : ls) {
    encode(*ref, bl);
  }
}
template<class T, class Alloc>
inline void encode(const std::list<std::shared_ptr<T>, Alloc>& ls,
		   bufferlist& bl, uint64_t features)
{
  __u32 n = (__u32)(ls.size());  // c++11 std::list::size() is O(1)
  encode(n, bl);
  for (const auto& ref : ls) {
    encode(*ref, bl, features);
  }
}
template<class T, class Alloc>
inline void decode(std::list<std::shared_ptr<T>, Alloc>& ls,
		   bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    auto ref = std::make_shared<T>();
    decode(*ref, p);
    ls.emplace_back(std::move(ref));
  }
}

// std::set<T>
template<class T, class Comp, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode(const std::set<T,Comp,Alloc>& s, bufferlist& bl)
{
  __u32 n = (__u32)(s.size());
  encode(n, bl);
  for (auto p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T, class Comp, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  decode(std::set<T,Comp,Alloc>& s, bufferlist::const_iterator& p)
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

template<class T, class Comp, class Alloc, typename traits>
inline typename std::enable_if<!traits::supported>::type
  encode_nohead(const std::set<T,Comp,Alloc>& s, bufferlist& bl)
{
  for (auto p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T, class Comp, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  decode_nohead(int len, std::set<T,Comp,Alloc>& s, bufferlist::const_iterator& p)
{
  for (int i=0; i<len; i++) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

// boost::container::flat_set<T>
template<class T, class Comp, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
encode(const boost::container::flat_set<T, Comp, Alloc>& s, bufferlist& bl)
{
  __u32 n = (__u32)(s.size());
  encode(n, bl);
  for (const auto& e : s)
    encode(e, bl);
}
template<class T, class Comp, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
decode(boost::container::flat_set<T, Comp, Alloc>& s, bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  s.clear();
  s.reserve(n);
  while (n--) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

template<class T, class Comp, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
encode_nohead(const boost::container::flat_set<T, Comp, Alloc>& s,
	      bufferlist& bl)
{
  for (const auto& e : s)
    encode(e, bl);
}
template<class T, class Comp, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
decode_nohead(int len, boost::container::flat_set<T, Comp, Alloc>& s,
	      bufferlist::iterator& p)
{
  s.reserve(len);
  for (int i=0; i<len; i++) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

// multiset
template<class T, class Comp, class Alloc>
inline void encode(const std::multiset<T,Comp,Alloc>& s, bufferlist& bl)
{
  __u32 n = (__u32)(s.size());
  encode(n, bl);
  for (auto p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T, class Comp, class Alloc>
inline void decode(std::multiset<T,Comp,Alloc>& s, bufferlist::const_iterator& p)
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

template<class T, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode(const std::vector<T,Alloc>& v, bufferlist& bl, uint64_t features)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (auto p = v.begin(); p != v.end(); ++p)
    encode(*p, bl, features);
}
template<class T, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode(const std::vector<T,Alloc>& v, bufferlist& bl)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (auto p = v.begin(); p != v.end(); ++p)
    encode(*p, bl);
}
template<class T, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  decode(std::vector<T,Alloc>& v, bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  v.resize(n);
  for (__u32 i=0; i<n; i++) 
    decode(v[i], p);
}

template<class T, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode_nohead(const std::vector<T,Alloc>& v, bufferlist& bl)
{
  for (auto p = v.begin(); p != v.end(); ++p)
    encode(*p, bl);
}
template<class T, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  decode_nohead(int len, std::vector<T,Alloc>& v, bufferlist::const_iterator& p)
{
  v.resize(len);
  for (__u32 i=0; i<v.size(); i++) 
    decode(v[i], p);
}

// small vector
template<class T, std::size_t N, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl, uint64_t features)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (const auto& i : v)
    encode(i, bl, features);
}
template<class T, std::size_t N, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (const auto& i : v)
    encode(i, bl);
}
template<class T, std::size_t N, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  decode(boost::container::small_vector<T,N,Alloc>& v, bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  v.resize(n);
  for (auto& i : v)
    decode(i, p);
}

template<class T, std::size_t N, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode_nohead(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl)
{
  for (const auto& i : v)
    encode(i, bl);
}
template<class T, std::size_t N, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  decode_nohead(int len, boost::container::small_vector<T,N,Alloc>& v, bufferlist::const_iterator& p)
{
  v.resize(len);
  for (auto& i : v)
    decode(i, p);
}


// vector (shared_ptr)
template<class T,class Alloc>
inline void encode(const std::vector<std::shared_ptr<T>,Alloc>& v,
		   bufferlist& bl,
		   uint64_t features)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (const auto& ref : v) {
    if (ref)
      encode(*ref, bl, features);
    else
      encode(T(), bl, features);
  }
}
template<class T, class Alloc>
inline void encode(const std::vector<std::shared_ptr<T>,Alloc>& v,
		   bufferlist& bl)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (const auto& ref : v) {
    if (ref)
      encode(*ref, bl);
    else
      encode(T(), bl);
  }
}
template<class T, class Alloc>
inline void decode(std::vector<std::shared_ptr<T>,Alloc>& v,
		   bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  v.clear();
  v.reserve(n);
  while (n--) {
    auto ref = std::make_shared<T>();
    decode(*ref, p);
    v.emplace_back(std::move(ref));
  }
}

// map
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
inline std::enable_if_t<!t_traits::supported ||
			!u_traits::supported>
  encode(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl, uint64_t features)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  decode(std::map<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p)
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
template<class T, class U, class Comp, class Alloc>
inline void decode_noclear(std::map<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode_nohead(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl)
{
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode_nohead(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl, uint64_t features)
{
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  decode_nohead(int n, std::map<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p)
{
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}

// boost::container::flat-map
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode(const boost::container::flat_map<T,U,Comp,Alloc>& m, bufferlist& bl)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (typename boost::container::flat_map<T,U,Comp>::const_iterator p
	 = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode(const boost::container::flat_map<T,U,Comp,Alloc>& m, bufferlist& bl,
	 uint64_t features)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  decode(boost::container::flat_map<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  m.reserve(n);
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}
template<class T, class U, class Comp, class Alloc>
inline void decode_noclear(boost::container::flat_map<T,U,Comp,Alloc>& m,
			   bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  m.reserve(m.size() + n);
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode_nohead(const boost::container::flat_map<T,U,Comp,Alloc>& m,
		bufferlist& bl)
{
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode_nohead(const boost::container::flat_map<T,U,Comp,Alloc>& m,
		bufferlist& bl, uint64_t features)
{
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits, typename u_traits>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  decode_nohead(int n, boost::container::flat_map<T,U,Comp,Alloc>& m,
		bufferlist::const_iterator& p)
{
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}

// multimap
template<class T, class U, class Comp, class Alloc>
inline void encode(const std::multimap<T,U,Comp,Alloc>& m, bufferlist& bl)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U, class Comp, class Alloc>
inline void decode(std::multimap<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    typename std::pair<T,U> tu = std::pair<T,U>();
    decode(tu.first, p);
    typename std::multimap<T,U,Comp,Alloc>::iterator it = m.insert(tu);
    decode(it->second, p);
  }
}

// ceph::unordered_map
template<class T, class U, class Hash, class Pred, class Alloc>
inline void encode(const unordered_map<T,U,Hash,Pred,Alloc>& m, bufferlist& bl,
		   uint64_t features)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U, class Hash, class Pred, class Alloc>
inline void encode(const unordered_map<T,U,Hash,Pred,Alloc>& m, bufferlist& bl)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U, class Hash, class Pred, class Alloc>
inline void decode(unordered_map<T,U,Hash,Pred,Alloc>& m, bufferlist::const_iterator& p)
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

// ceph::unordered_set
template<class T, class Hash, class Pred, class Alloc>
inline void encode(const ceph::unordered_set<T,Hash,Pred,Alloc>& m, bufferlist& bl)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (auto p = m.begin(); p != m.end(); ++p)
    encode(*p, bl);
}
template<class T, class Hash, class Pred, class Alloc>
inline void decode(ceph::unordered_set<T,Hash,Pred,Alloc>& m, bufferlist::const_iterator& p)
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

// deque
template<class T, class Alloc>
inline void encode(const std::deque<T,Alloc>& ls, bufferlist& bl, uint64_t features)
{
  __u32 n = ls.size();
  encode(n, bl);
  for (auto p = ls.begin(); p != ls.end(); ++p)
    encode(*p, bl, features);
}
template<class T, class Alloc>
inline void encode(const std::deque<T,Alloc>& ls, bufferlist& bl)
{
  __u32 n = ls.size();
  encode(n, bl);
  for (auto p = ls.begin(); p != ls.end(); ++p)
    encode(*p, bl);
}
template<class T, class Alloc>
inline void decode(std::deque<T,Alloc>& ls, bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    ls.emplace_back();
    decode(ls.back(), p);
  }
}

// std::array<T, N>
template<class T, size_t N, typename traits>
inline std::enable_if_t<!traits::supported>
encode(const std::array<T, N>& v, bufferlist& bl, uint64_t features)
{
  for (const auto& e : v)
    encode(e, bl, features);
}
template<class T, size_t N, typename traits>
inline std::enable_if_t<!traits::supported>
encode(const std::array<T, N>& v, bufferlist& bl)
{
  for (const auto& e : v)
    encode(e, bl);
}
template<class T, size_t N, typename traits>
inline std::enable_if_t<!traits::supported>
decode(std::array<T, N>& v, bufferlist::const_iterator& p)
{
  for (auto& e : v)
    decode(e, p);
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
  (std::string(func) + " no longer understand old encoding version " #v " < " + std::to_string(compatv))

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
#define DECODE_START(v, bl)						\
  __u8 struct_v, struct_compat;						\
  using ::ceph::decode;							\
  decode(struct_v, bl);						\
  decode(struct_compat, bl);						\
  if (v < struct_compat)						\
    throw ::ceph::buffer::malformed_input(DECODE_ERR_OLDVERSION(__PRETTY_FUNCTION__, v, struct_compat)); \
  __u32 struct_len;							\
  decode(struct_len, bl);						\
  if (struct_len > bl.get_remaining())					\
    throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
  unsigned struct_end = bl.get_off() + struct_len;			\
  do {

/* BEWARE: any change to this macro MUST be also reflected in the duplicative
 * DECODE_START_LEGACY_COMPAT_LEN! */
#define __DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, skip_v, bl)	\
  using ::ceph::decode;							\
  __u8 struct_v;							\
  decode(struct_v, bl);						\
  if (struct_v >= compatv) {						\
    __u8 struct_compat;							\
    decode(struct_compat, bl);					\
    if (v < struct_compat)						\
      throw ::ceph::buffer::malformed_input(DECODE_ERR_OLDVERSION(__PRETTY_FUNCTION__, v, struct_compat)); \
  } else if (skip_v) {							\
    if (bl.get_remaining() < skip_v)					\
      throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
    bl.advance(skip_v);							\
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
      throw ::ceph::buffer::malformed_input(DECODE_ERR_OLDVERSION(	\
	__PRETTY_FUNCTION__, v, struct_compat));			\
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
      bl.advance(struct_end - bl.get_off());				\
  }

namespace ceph {

/*
 * Encoders/decoders to read from current offset in a file handle and
 * encode/decode the data according to argument types.
 */
inline ssize_t decode_file(int fd, std::string &str)
{
  bufferlist bl;
  __u32 len = 0;
  bl.read_fd(fd, sizeof(len));
  decode(len, bl);
  bl.read_fd(fd, len);
  decode(str, bl);
  return bl.length();
}

inline ssize_t decode_file(int fd, bufferptr &bp)
{
  bufferlist bl;
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
