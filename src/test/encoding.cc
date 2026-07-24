/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/buffer.h"
#include "include/encoding.h"

#include <fmt/format.h>
#include "gtest/gtest.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <functional>
#include <iostream> // for std::cout
#include <list>
#include <memory>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/optional.hpp>
#include <boost/tuple/tuple.hpp>

using namespace std;

template < typename T >
static void test_encode_and_decode(const T& src)
{
  buffer::list bl(1000000);
  encode(src, bl);
  T dst;
  auto i = bl.cbegin();
  decode(dst, i);
  ASSERT_EQ(src, dst)
    << "Encoding roundtrip changed the string: orig=" << src
    << ", but new=" << dst;
}

TEST(EncodingRoundTrip, StringSimple) {
  string my_str("I am the very model of a modern major general");
  test_encode_and_decode < std::string >(my_str);
}

TEST(EncodingRoundTrip, StringEmpty) {
  string my_str("");
  test_encode_and_decode < std::string >(my_str);
}

TEST(EncodingRoundTrip, StringNewline) {
  string my_str("foo bar baz\n");
  test_encode_and_decode < std::string >(my_str);
}

template <typename Size, typename T>
static void test_encode_and_nohead_nohead(Size len, const T& src)
{
  buffer::list bl(1000000);
  encode(len, bl);
  encode_nohead(src, bl);
  T dst;
  auto i = bl.cbegin();
  decode(len, i);
  decode_nohead(len, dst, i);
  ASSERT_EQ(src, dst)
    << "Encoding roundtrip changed the string: orig=" << src
    << ", but new=" << dst;
}

TEST(EncodingRoundTrip, StringNoHead) {
  const string str("The quick brown fox jumps over the lazy dog");
  auto size = str.size();
  test_encode_and_nohead_nohead(static_cast<int>(size), str);
  test_encode_and_nohead_nohead(static_cast<unsigned>(size), str);
  test_encode_and_nohead_nohead(static_cast<uint32_t>(size), str);
  test_encode_and_nohead_nohead(static_cast<__u32>(size), str);
  test_encode_and_nohead_nohead(static_cast<size_t>(size), str);
}

TEST(EncodingRoundTrip, BufferListNoHead) {
  buffer::list bl;
  bl.append("is this a dagger which i see before me?");
  auto size = bl.length();
  test_encode_and_nohead_nohead(static_cast<int>(size), bl);
  test_encode_and_nohead_nohead(static_cast<unsigned>(size), bl);
  test_encode_and_nohead_nohead(static_cast<uint32_t>(size), bl);
  test_encode_and_nohead_nohead(static_cast<__u32>(size), bl);
  test_encode_and_nohead_nohead(static_cast<size_t>(size), bl);
}

typedef std::multimap < int, std::string > multimap_t;
typedef multimap_t::value_type my_val_ty;

namespace std {
static std::ostream& operator<<(std::ostream& oss, const multimap_t &multimap)
{
  for (multimap_t::const_iterator m = multimap.begin();
       m != multimap.end();
       ++m)
  {
    oss << m->first << "->" << m->second << " ";
  }
  return oss;
}
}

TEST(EncodingRoundTrip, Multimap) {
  multimap_t multimap;
  multimap.insert( my_val_ty(1, "foo") );
  multimap.insert( my_val_ty(2, "bar") );
  multimap.insert( my_val_ty(2, "baz") );
  multimap.insert( my_val_ty(3, "lucky number 3") );
  multimap.insert( my_val_ty(10000, "large number") );

  test_encode_and_decode < multimap_t >(multimap);
}



///////////////////////////////////////////////////////
// ConstructorCounter
///////////////////////////////////////////////////////
template <typename T>
class ConstructorCounter
{
public:
  ConstructorCounter() : data(0)
  {
    default_ctor++;
  }

  explicit ConstructorCounter(const T& data_)
    : data(data_)
  {
    one_arg_ctor++;
  }

  ConstructorCounter(const ConstructorCounter &rhs)
    : data(rhs.data)
  {
    copy_ctor++;
  }

  ConstructorCounter &operator=(const ConstructorCounter &rhs)
  {
    data = rhs.data;
    assigns++;
    return *this;
  }

  static void init(void)
  {
    default_ctor = 0;
    one_arg_ctor = 0;
    copy_ctor = 0;
    assigns = 0;
  }

  static int get_default_ctor(void)
  {
    return default_ctor;
  }

  static int get_one_arg_ctor(void)
  {
    return one_arg_ctor;
  }

  static int get_copy_ctor(void)
  {
    return copy_ctor;
  }

  static int get_assigns(void)
  {
    return assigns;
  }

  bool operator<(const ConstructorCounter &rhs) const
  {
    return data < rhs.data;
  }

  bool operator==(const ConstructorCounter &rhs) const
  {
    return data == rhs.data;
  }

  friend void decode(ConstructorCounter &s, buffer::list::const_iterator& p)
  {
    decode(s.data, p);
  }

  friend void encode(const ConstructorCounter &s, buffer::list& p)
  {
    encode(s.data, p);
  }

  friend ostream& operator<<(ostream &oss, const ConstructorCounter &cc)
  {
    oss << cc.data;
    return oss;
  }

  T data;
private:
  static int default_ctor;
  static int one_arg_ctor;
  static int copy_ctor;
  static int assigns;
};

template class ConstructorCounter <int32_t>;
template class ConstructorCounter <int16_t>;

typedef ConstructorCounter <int32_t> my_key_t;
typedef ConstructorCounter <int16_t> my_val_t;
typedef std::multimap < my_key_t, my_val_t > multimap2_t;
typedef multimap2_t::value_type val2_ty;

template <class T> int ConstructorCounter<T>::default_ctor = 0;
template <class T> int ConstructorCounter<T>::one_arg_ctor = 0;
template <class T> int ConstructorCounter<T>::copy_ctor = 0;
template <class T> int ConstructorCounter<T>::assigns = 0;

static std::ostream& operator<<(std::ostream& oss, const multimap2_t &multimap)
{
  for (multimap2_t::const_iterator m = multimap.begin();
       m != multimap.end();
       ++m)
  {
    oss << m->first << "->" << m->second << " ";
  }
  return oss;
}

TEST(EncodingRoundTrip, MultimapConstructorCounter) {
  multimap2_t multimap2;
  multimap2.insert( val2_ty(my_key_t(1), my_val_t(10)) );
  multimap2.insert( val2_ty(my_key_t(2), my_val_t(20)) );
  multimap2.insert( val2_ty(my_key_t(2), my_val_t(30)) );
  multimap2.insert( val2_ty(my_key_t(3), my_val_t(40)) );
  multimap2.insert( val2_ty(my_key_t(10000), my_val_t(1)) );

  my_key_t::init();
  my_val_t::init();
  test_encode_and_decode < multimap2_t >(multimap2);

  EXPECT_EQ(5, my_key_t::get_default_ctor());
  EXPECT_EQ(0, my_key_t::get_one_arg_ctor());
  EXPECT_EQ(5, my_key_t::get_copy_ctor());
  EXPECT_EQ(0, my_key_t::get_assigns());

  EXPECT_EQ(5, my_val_t::get_default_ctor());
  EXPECT_EQ(0, my_val_t::get_one_arg_ctor());
  EXPECT_EQ(5, my_val_t::get_copy_ctor());
  EXPECT_EQ(0, my_val_t::get_assigns());
}

namespace ceph {
// make sure that the legacy encode/decode methods are selected
// over the ones defined using templates. the later is likely to
// be slower, see also the definition of "WRITE_INT_DENC" in
// include/denc.h
template<>
void encode<uint64_t, denc_traits<uint64_t>>(const uint64_t&,
                                             buffer::list&,
                                             uint64_t features) {
  static_assert(denc_traits<uint64_t>::supported,
                "should support new encoder");
  static_assert(!denc_traits<uint64_t>::featured,
                "should not be featured");
  ASSERT_EQ(0UL, features);
  // make sure the test fails if i get called
  ASSERT_TRUE(false);
}

template<>
void encode<ceph_le64, denc_traits<ceph_le64>>(const ceph_le64&,
                                               buffer::list&,
                                               uint64_t features) {
  static_assert(denc_traits<ceph_le64>::supported,
                "should support new encoder");
  static_assert(!denc_traits<ceph_le64>::featured,
                "should not be featured");
  ASSERT_EQ(0UL, features);
  // make sure the test fails if i get called
  ASSERT_TRUE(false);
}
}

namespace {
  // search `underlying_type` in denc.h for supported underlying types
  enum class Colour : int8_t { R,G,B };
  ostream& operator<<(ostream& os, Colour c) {
    switch (c) {
    case Colour::R:
      return os << "Colour::R";
    case Colour::G:
      return os << "Colour::G";
    case Colour::B:
      return os << "Colour::B";
    default:
      return os << "Colour::???";
    }
  }
}

TEST(EncodingRoundTrip, Integers) {
  // int types
  {
    uint64_t i = 42;
    test_encode_and_decode(i);
  }
  {
    int16_t i = 42;
    test_encode_and_decode(i);
  }
  {
    bool b = true;
    test_encode_and_decode(b);
  }
  {
    bool b = false;
    test_encode_and_decode(b);
  }
  // raw encoder
  {
    ceph_le64 i;
    i = 42;
    test_encode_and_decode(i);
  }
  // enum
  {
    test_encode_and_decode(Colour::R);
    // this should not build, as the size of unsigned is not the same on
    // different archs, that's why denc_traits<> intentionally leaves
    // `int` and `unsigned int` out of supported types.
    //
    // enum E { R, G, B };
    // test_encode_and_decode(R);
  }
}

TEST(EncodingException, Macros) {
  const struct {
    buffer::malformed_input exc;
    std::string expected_what;
  } tests[] = {
    {
      DECODE_ERR_OLDVERSION(__PRETTY_FUNCTION__, 100, 200),
      fmt::format("{} no longer understands old encoding version 100 < 200: Malformed input",
                  __PRETTY_FUNCTION__)
    },
    {
      DECODE_ERR_PAST(__PRETTY_FUNCTION__),
      fmt::format("{} decode past end of struct encoding: Malformed input",
                  __PRETTY_FUNCTION__)
    }
  };
  for (auto& [exec, expected_what] : tests) {
    try {
      throw exec;
    } catch (const exception& e) {
      ASSERT_NE(string::npos, string(e.what()).find(expected_what));
    }
  }
}


TEST(small_encoding, varint) {
  uint32_t v[][4] = {
    /* value, varint bytes, signed varint bytes, signed varint bytes (neg) */
    {0, 1, 1, 1},
    {1, 1, 1, 1},
    {2, 1, 1, 1},
    {31, 1, 1, 1},
    {32, 1, 1, 1},
    {0xff, 2, 2, 2},
    {0x100, 2, 2, 2},
    {0xfff, 2, 2, 2},
    {0x1000, 2, 2, 2},
    {0x2000, 2, 3, 3},
    {0x3fff, 2, 3, 3},
    {0x4000, 3, 3, 3},
    {0x4001, 3, 3, 3},
    {0x10001, 3, 3, 3},
    {0x20001, 3, 3, 3},
    {0x40001, 3, 3, 3},
    {0x80001, 3, 3, 3},
    {0x7f0001, 4, 4, 4},
    {0xff00001, 4, 5, 5},
    {0x1ff00001, 5, 5, 5},
    {0xffff0001, 5, 5, 5},
    {0xffffffff, 5, 5, 5},
    {1074790401, 5, 5, 5},
    {0, 0, 0, 0}
  };
  for (unsigned i=0; v[i][1]; ++i) {
    {
      buffer::list bl;
      {
         auto app = bl.get_contiguous_appender(16, true);
         denc_varint(v[i][0], app);
      }
      cout << std::hex << v[i][0] << "\t" << v[i][1] << "\t";
      bl.hexdump(cout, false);
      cout << std::endl;
      ASSERT_EQ(v[i][1], bl.length());
      uint32_t u;
      auto p = bl.begin().get_current_ptr().cbegin();
      denc_varint(u, p);
      ASSERT_EQ(v[i][0], u);
    }
    {
      buffer::list bl;
      {
         auto app = bl.get_contiguous_appender(16, true);
         denc_signed_varint(v[i][0], app);
      }
      cout << std::hex << v[i][0] << "\t" << v[i][2] << "\t";
      bl.hexdump(cout, false);
      cout << std::endl;
      ASSERT_EQ(v[i][2], bl.length());
      int32_t u;
      auto p = bl.begin().get_current_ptr().cbegin();
      denc_signed_varint(u, p);
      ASSERT_EQ((int32_t)v[i][0], u);
    }
    {
      buffer::list bl;
      int64_t x = -(int64_t)v[i][0];
      {
         auto app = bl.get_contiguous_appender(16, true);
         denc_signed_varint(x, app);
      }
      cout << std::dec << x << std::hex << "\t" << v[i][3] << "\t";
      bl.hexdump(cout, false);
      cout << std::endl;
      ASSERT_EQ(v[i][3], bl.length());
      int64_t u;
      auto p = bl.begin().get_current_ptr().cbegin();
      denc_signed_varint(u, p);
      ASSERT_EQ(x, u);
    }
  }
}

TEST(small_encoding, varint_lowz) {
  uint32_t v[][4] = {
    /* value, bytes encoded */
    {0, 1, 1, 1},
    {1, 1, 1, 1},
    {2, 1, 1, 1},
    {15, 1, 1, 1},
    {16, 1, 1, 1},
    {31, 1, 2, 2},
    {63, 2, 2, 2},
    {64, 1, 1, 1},
    {0xff, 2, 2, 2},
    {0x100, 1, 1, 1},
    {0x7ff, 2, 2, 2},
    {0xfff, 2, 3, 3},
    {0x1000, 1, 1, 1},
    {0x4000, 1, 1, 1},
    {0x8000, 1, 1, 1},
    {0x10000, 1, 2, 2},
    {0x20000, 2, 2, 2},
    {0x40000, 2, 2, 2},
    {0x80000, 2, 2, 2},
    {0x7f0000, 2, 2, 2},
    {0xffff0000, 4, 4, 4},
    {0xffffffff, 5, 5, 5},
    {0x41000000, 3, 4, 4},
    {0, 0, 0, 0}
  };
  for (unsigned i=0; v[i][1]; ++i) {
    {
      buffer::list bl;
      {
         auto app = bl.get_contiguous_appender(16, true);
         denc_varint_lowz(v[i][0], app);
      }
      cout << std::hex << v[i][0] << "\t" << v[i][1] << "\t";
      bl.hexdump(cout, false);
      cout << std::endl;
      ASSERT_EQ(v[i][1], bl.length());
      uint32_t u;
      auto p = bl.begin().get_current_ptr().cbegin();
      denc_varint_lowz(u, p);
      ASSERT_EQ(v[i][0], u);
    }
    {
      buffer::list bl;
      int64_t x = v[i][0];
      {
         auto app = bl.get_contiguous_appender(16, true);
         denc_signed_varint_lowz(x, app);
      }
      cout << std::hex << x << "\t" << v[i][1] << "\t";
      bl.hexdump(cout, false);
      cout << std::endl;
      ASSERT_EQ(v[i][2], bl.length());
      int64_t u;
      auto p = bl.begin().get_current_ptr().cbegin();
      denc_signed_varint_lowz(u, p);
      ASSERT_EQ(x, u);
    }
    {
      buffer::list bl;
      int64_t x = -(int64_t)v[i][0];
      {
         auto app = bl.get_contiguous_appender(16, true);
         denc_signed_varint_lowz(x, app);
      }
      cout << std::dec << x << "\t" << v[i][1] << "\t";
      bl.hexdump(cout, false);
      cout << std::endl;
      ASSERT_EQ(v[i][3], bl.length());
      int64_t u;
      auto p = bl.begin().get_current_ptr().cbegin();
      denc_signed_varint_lowz(u, p);
      ASSERT_EQ(x, u);
    }    
  }
}

TEST(small_encoding, lba) {
  uint64_t v[][2] = {
    /* value, bytes encoded */
    {0, 4},
    {1, 4},
    {0xff, 4},
    {0x10000, 4},
    {0x7f0000, 4},
    {0xffff0000, 4},
    {0x0fffffff, 4},
    {0x1fffffff, 5},
    {0xffffffff, 5},
    {0x3fffffff000, 4},
    {0x7fffffff000, 5},
    {0x1fffffff0000, 4},
    {0x3fffffff0000, 5},
    {0xfffffff00000, 4},
    {0x1fffffff00000, 5},
    {0x41000000, 4},
    {0, 0}
  };
  for (unsigned i=0; v[i][1]; ++i) {
    buffer::list bl;
    {
       auto app = bl.get_contiguous_appender(16, true);
       denc_lba(v[i][0], app);
    }
    cout << std::hex << v[i][0] << "\t" << v[i][1] << "\t";
    bl.hexdump(cout, false);
    cout << std::endl;
    ASSERT_EQ(v[i][1], bl.length());
    uint64_t u;
    auto p = bl.begin().get_current_ptr().cbegin();
    denc_lba(u, p);
    ASSERT_EQ(v[i][0], u);
  }

}

/*
 * encoding.h Compatibility Coverage
 *
 * New tests for interface-preserving modernization of
 * include/encoding.h belong below this section.
 */

namespace {

struct encoding_compat_record final {
  uint32_t n = 0;
  std::string s;

  static inline uint64_t last_features = 0;
  static inline unsigned feature_encode_calls = 0;

  static void reset_counters()
  {
    last_features = 0;
    feature_encode_calls = 0;
  }

  friend bool operator==(const encoding_compat_record& lhs,
                         const encoding_compat_record& rhs)
  {
    return lhs.n == rhs.n && lhs.s == rhs.s;
  }

  friend bool operator<(const encoding_compat_record& lhs,
                        const encoding_compat_record& rhs)
  {
    return std::tie(lhs.n, lhs.s) < std::tie(rhs.n, rhs.s);
  }

  friend void encode(const encoding_compat_record& v, buffer::list& bl)
  {
    encode(v.n, bl);
    encode(v.s, bl);
  }

  friend void encode(const encoding_compat_record& v,
                     buffer::list& bl,
                     uint64_t features)
  {
    last_features = features;
    ++feature_encode_calls;
    encode(v, bl);
  }

  friend void decode(encoding_compat_record& v, buffer::list::const_iterator& p)
  {
    decode(v.n, p);
    decode(v.s, p);
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const encoding_compat_record& v)
  {
    return os << "{n=" << v.n << ", s=" << v.s << "}";
  }
};

struct encoding_compat_record_hash final {
  std::size_t operator()(const encoding_compat_record& v) const
  {
    return std::hash<uint32_t> {}(v.n) ^ (std::hash<std::string> {}(v.s) << 1);
  }
};

struct encoding_compat_v1_record final {
  uint32_t n = 0;
  std::string s;

  void decode(buffer::list::const_iterator& p)
  {
    DECODE_START(1, p);
    decode(n, p);
    decode(s, p);
    DECODE_FINISH(p);
  }

  friend bool operator==(const encoding_compat_v1_record& lhs,
                         const encoding_compat_v1_record& rhs)
  {
    return lhs.n == rhs.n && lhs.s == rhs.s;
  }
};

struct encoding_compat_v2_record final {
  uint32_t n = 0;
  std::string s;
  std::string extra;

  void encode(buffer::list& bl) const
  {
    ENCODE_START(2, 1, bl);
    encode(n, bl);
    encode(s, bl);
    encode(extra, bl);
    ENCODE_FINISH(bl);
  }
};

struct encoding_macro_record final {
  __u8 value = 0;

  void encode(buffer::list& bl) const
  {
    ENCODE_START(1, 1, bl);
    encode(value, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& p)
  {
    DECODE_START(1, p);
    decode(value, p);
    DECODE_FINISH(p);
  }
};

struct encoding_new_compat_record final {
  __u8 value = 0;

  void encode(buffer::list& bl) const
  {
    ENCODE_START(1, 1, bl);
    encode(value, bl);
    ENCODE_FINISH_NEW_COMPAT(bl, 2);
  }
};

static const encoding_compat_record alpha { 1, "alpha" };
static const encoding_compat_record bravo { 2, "bravo" };
static const encoding_compat_record bravo_alt { 2, "bravo-alt" };
static const encoding_compat_record charlie { 3, "charlie" };

template <typename T>
T encoding_compat_round_trip(const T& src)
{
  buffer::list bl;
  encode(src, bl);

  T dst;
  auto p = bl.cbegin();
  decode(dst, p);

  EXPECT_TRUE(p.end());
  return dst;
}

template <typename T>
void expect_encoding_compat_round_trip(const T& src)
{
  EXPECT_EQ(src, encoding_compat_round_trip(src));
}

template <typename T>
void expect_encoding_compat_nohead_round_trip(const T& src, unsigned n)
{
  buffer::list bl;
  encode_nohead(src, bl);

  T dst;
  auto p = bl.cbegin();
  decode_nohead(n, dst, p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(src, dst);
}

template <typename T>
void expect_nohead_decode_replaces_existing(const T& src, T dst)
{
  buffer::list bl;
  encode_nohead(src, bl);

  auto p = bl.cbegin();
  decode_nohead(src.size(), dst, p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(src, dst);
}

void append_compat_le32(std::string& out, uint32_t v)
{
  for (unsigned i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>(v >> (i * 8)));
  }
}

void append_compat_le64(std::string& out, uint64_t v)
{
  for (unsigned i = 0; i < 8; ++i) {
    out.push_back(static_cast<char>(v >> (i * 8)));
  }
}

std::string float_wire_bytes(uint32_t raw)
{
  auto out = std::string {};
  append_compat_le32(out, raw);
  return out;
}

std::string float_wire_bytes(uint64_t raw)
{
  auto out = std::string {};
  append_compat_le64(out, raw);
  return out;
}

void expect_float_wire_format(float v, uint32_t raw)
{
  buffer::list bl;
  encode(v, bl);

  EXPECT_EQ(float_wire_bytes(raw), bl.to_str());

  float out = 0;
  auto p = bl.cbegin();
  decode(out, p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(raw, std::bit_cast<uint32_t>(out));
}

void expect_float_wire_format(double v, uint64_t raw)
{
  buffer::list bl;
  encode(v, bl);

  EXPECT_EQ(float_wire_bytes(raw), bl.to_str());

  double out = 0;
  auto p = bl.cbegin();
  decode(out, p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(raw, std::bit_cast<uint64_t>(out));
}

void append_compat_string(std::string& out, std::string_view v)
{
  append_compat_le32(out, v.size());
  out.append(v);
}

void append_compat_record(std::string& out, const encoding_compat_record& v)
{
  append_compat_le32(out, v.n);
  append_compat_string(out, v.s);
}

void encode_macro_header(buffer::list& bl, __u8 struct_v,
                         __u8 struct_compat, __u32 struct_len)
{
  encode(struct_v, bl);
  encode(struct_compat, bl);
  encode(struct_len, bl);
}

void decode_macro_overread(buffer::list::const_iterator& p)
{
  DECODE_START(1, p);
  p += 2;
  DECODE_FINISH(p);
}

void decode_macro_unknown(buffer::list& payload,
                          buffer::list::const_iterator& p)
{
  DECODE_UNKNOWN(payload, p);
}

std::pair<__u8, __u32> decode_unchecked_macro_header(buffer::list::const_iterator& p)
{
  DECODE_START_UNCHECKED(3, p);
  DECODE_FINISH(p);

  return { struct_v, struct_len };
}

__u8 decode_legacy_macro_value(buffer::list::const_iterator& p)
{
  __u8 value = 0;

  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
  decode(value, p);
  DECODE_FINISH(p);

  return value;
}

__u8 decode_legacy_macro_value_16(buffer::list::const_iterator& p)
{
  __u8 value = 0;

  DECODE_START_LEGACY_COMPAT_LEN_16(2, 2, 2, p);
  decode(value, p);
  DECODE_FINISH(p);

  return value;
}

__u8 decode_legacy_macro_value_32(buffer::list::const_iterator& p)
{
  __u8 value = 0;

  DECODE_START_LEGACY_COMPAT_LEN_32(2, 2, 2, p);
  decode(value, p);
  DECODE_FINISH(p);

  return value;
}

template <typename T>
std::string encode_compat_to_string(const T& v)
{
  buffer::list bl;
  encode(v, bl);
  return bl.to_str();
}

template <typename T>
void expect_compat_wire_format(const T& v, std::string_view expected)
{
  EXPECT_EQ(expected, encode_compat_to_string(v));
}

template <typename T>
void expect_feature_compat_wire_format(const T& v, std::string_view expected)
{
  buffer::list bl;
  encode(v, bl, 123);

  EXPECT_EQ(expected, bl.to_str());
}

template <typename T>
void expect_feature_encode_calls(const T& v, std::size_t expected_calls)
{
  buffer::list bl;

  encoding_compat_record::reset_counters();
  encode(v, bl, 123);

  EXPECT_EQ(uint64_t { 123 }, encoding_compat_record::last_features);
  EXPECT_EQ(expected_calls, encoding_compat_record::feature_encode_calls);
}

template <std::ranges::sized_range RangeT>
buffer::list make_decode_noclear_payload(const RangeT& entries)
{
  auto bl = buffer::list {};
  encode(static_cast<__u32>(std::ranges::size(entries)), bl);

  std::ranges::for_each(entries, [&bl](const auto& item) {
    encode(item.first, bl);
    encode(item.second, bl);
  });

  return bl;
}

} // namespace

TEST(EncodingCompatibility, LegacySequenceContainersRoundTrip)
{
  expect_encoding_compat_round_trip(
    std::vector<encoding_compat_record> { alpha, bravo, charlie });
  expect_encoding_compat_round_trip(
    std::list<encoding_compat_record> { alpha, bravo, charlie });
  expect_encoding_compat_round_trip(
    std::deque<encoding_compat_record> { alpha, bravo, charlie });
  expect_encoding_compat_round_trip(
    boost::container::small_vector<encoding_compat_record, 4> {
      alpha, bravo, charlie });
  expect_encoding_compat_round_trip(
    std::array<encoding_compat_record, 3> { alpha, bravo, charlie });
}

TEST(EncodingCompatibility, BufferTypesRoundTrip)
{
  bufferptr ptr_src("bufferptr-value", 15);
  auto ptr_dst = encoding_compat_round_trip(ptr_src);

  ASSERT_EQ(ptr_src.length(), ptr_dst.length());
  EXPECT_EQ(std::string_view(ptr_src.c_str(), ptr_src.length()),
            std::string_view(ptr_dst.c_str(), ptr_dst.length()));

  buffer::list list_src;
  list_src.append("first-", 6);
  list_src.append("second", 6);
  auto list_dst = encoding_compat_round_trip(list_src);

  EXPECT_EQ(list_src.length(), list_dst.length());
  EXPECT_EQ(list_src.to_str(), list_dst.to_str());
}

TEST(EncodingCompatibility, FloatingPointWireFormat)
{
  expect_float_wire_format(std::bit_cast<float>(uint32_t { 0x00000000 }),
                           uint32_t { 0x00000000 });
  expect_float_wire_format(std::bit_cast<float>(uint32_t { 0x80000000 }),
                           uint32_t { 0x80000000 });
  expect_float_wire_format(std::bit_cast<float>(uint32_t { 0x3f800000 }),
                           uint32_t { 0x3f800000 });
  expect_float_wire_format(std::bit_cast<float>(uint32_t { 0x3dfbe76d }),
                           uint32_t { 0x3dfbe76d });
  expect_float_wire_format(std::bit_cast<float>(uint32_t { 0x7fc00001 }),
                           uint32_t { 0x7fc00001 });

  expect_float_wire_format(std::bit_cast<double>(uint64_t { 0x0000000000000000 }),
                           uint64_t { 0x0000000000000000 });
  expect_float_wire_format(std::bit_cast<double>(uint64_t { 0x8000000000000000 }),
                           uint64_t { 0x8000000000000000 });
  expect_float_wire_format(std::bit_cast<double>(uint64_t { 0x3ff0000000000000 }),
                           uint64_t { 0x3ff0000000000000 });
  expect_float_wire_format(std::bit_cast<double>(uint64_t { 0x3fbf7ced916872b0 }),
                           uint64_t { 0x3fbf7ced916872b0 });
  expect_float_wire_format(std::bit_cast<double>(uint64_t { 0x7ff8000000000001 }),
                           uint64_t { 0x7ff8000000000001 });
}

TEST(EncodingCompatibility, LegacySetContainersRoundTrip)
{
  expect_encoding_compat_round_trip(
    std::set<encoding_compat_record> { alpha, bravo, charlie });
  expect_encoding_compat_round_trip(
    boost::container::flat_set<encoding_compat_record> { alpha, bravo, charlie });
  expect_encoding_compat_round_trip(
    std::multiset<encoding_compat_record> { alpha, bravo, bravo, charlie });
  expect_encoding_compat_round_trip(
    std::unordered_set<encoding_compat_record, encoding_compat_record_hash> {
      alpha, bravo, charlie });
}

TEST(EncodingCompatibility, LegacyMapContainersRoundTrip)
{
  expect_encoding_compat_round_trip(
    std::map<encoding_compat_record, encoding_compat_record> {
      { alpha, bravo },
      { bravo, charlie },
    });
  expect_encoding_compat_round_trip(
    boost::container::flat_map<encoding_compat_record, encoding_compat_record> {
      { alpha, bravo },
      { bravo, charlie },
    });
  expect_encoding_compat_round_trip(
    std::multimap<encoding_compat_record, encoding_compat_record> {
      { alpha, bravo },
      { alpha, bravo_alt },
      { bravo, charlie },
    });
  expect_encoding_compat_round_trip(
    std::unordered_map<encoding_compat_record,
                       encoding_compat_record,
                       encoding_compat_record_hash> {
      { alpha, bravo },
      { bravo, charlie },
    });
}

TEST(EncodingCompatibility, LegacyNoHeadRoundTrip)
{
  expect_encoding_compat_nohead_round_trip(
    std::vector<encoding_compat_record> { alpha, bravo, charlie }, 3);
  expect_encoding_compat_nohead_round_trip(
    boost::container::small_vector<encoding_compat_record, 4> {
      alpha, bravo, charlie }, 3);
  expect_encoding_compat_nohead_round_trip(
    std::set<encoding_compat_record> { alpha, bravo, charlie }, 3);
  expect_encoding_compat_nohead_round_trip(
    boost::container::flat_set<encoding_compat_record> {
      alpha, bravo, charlie }, 3);
  expect_encoding_compat_nohead_round_trip(
    std::map<encoding_compat_record, encoding_compat_record> {
      { alpha, bravo },
      { bravo, charlie },
    }, 2);
  expect_encoding_compat_nohead_round_trip(
    boost::container::flat_map<encoding_compat_record, encoding_compat_record> {
      { alpha, bravo },
      { bravo, charlie },
    }, 2);
}

TEST(EncodingCompatibility, LegacyNoHeadDecodeClearsDestination)
{
  expect_nohead_decode_replaces_existing(
    std::vector<encoding_compat_record> { alpha, bravo },
    std::vector<encoding_compat_record> { charlie });
  expect_nohead_decode_replaces_existing(
    boost::container::small_vector<encoding_compat_record, 4> { alpha, bravo },
    boost::container::small_vector<encoding_compat_record, 4> { charlie });
  expect_nohead_decode_replaces_existing(
    std::set<encoding_compat_record> { alpha, bravo },
    std::set<encoding_compat_record> { charlie });
  expect_nohead_decode_replaces_existing(
    boost::container::flat_set<encoding_compat_record> { alpha, bravo },
    boost::container::flat_set<encoding_compat_record> { charlie });
  expect_nohead_decode_replaces_existing(
    std::map<encoding_compat_record, encoding_compat_record> {
      { alpha, bravo },
    },
    std::map<encoding_compat_record, encoding_compat_record> {
      { charlie, alpha },
    });
  expect_nohead_decode_replaces_existing(
    boost::container::flat_map<encoding_compat_record,
                               encoding_compat_record> {
      { alpha, bravo },
    },
    boost::container::flat_map<encoding_compat_record,
                               encoding_compat_record> {
      { charlie, alpha },
    });
}

TEST(EncodingCompatibility, LegacySetNoHeadEncodingWireFormat)
{
  const std::set<encoding_compat_record> src { alpha, bravo };
  std::string expected;
  append_compat_record(expected, alpha);
  append_compat_record(expected, bravo);

  buffer::list bl;
  encode_nohead(src, bl);

  EXPECT_EQ(expected, bl.to_str());
}

TEST(EncodingCompatibility, LegacyFlatSetNoHeadEncodingWireFormat)
{
  const boost::container::flat_set<encoding_compat_record> src { alpha, bravo };
  std::string expected;
  append_compat_record(expected, alpha);
  append_compat_record(expected, bravo);

  buffer::list bl;
  encode_nohead(src, bl);

  EXPECT_EQ(expected, bl.to_str());
}

TEST(EncodingCompatibility, LegacyOptionalTupleAndPairRoundTrip)
{
  expect_encoding_compat_round_trip(
    std::optional<encoding_compat_record> { alpha });
  expect_encoding_compat_round_trip(
    std::optional<encoding_compat_record> {});
  expect_encoding_compat_round_trip(
    boost::optional<encoding_compat_record> { bravo });
  expect_encoding_compat_round_trip(
    boost::optional<encoding_compat_record> {});
  expect_encoding_compat_round_trip(
    std::tuple<encoding_compat_record, std::string, uint32_t> {
      alpha, "tuple", 7 });
  expect_encoding_compat_round_trip(
    std::pair<encoding_compat_record, encoding_compat_record> {
      alpha, bravo });

  buffer::list bl;
  encode(std::string_view { "string-view" }, bl);

  std::string string_view_out;
  auto p = bl.cbegin();
  decode(string_view_out, p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ("string-view", string_view_out);

  const auto src = boost::make_tuple(alpha, bravo, charlie);
  const auto dst = encoding_compat_round_trip(src);
  EXPECT_EQ(boost::get<0>(src), boost::get<0>(dst));
  EXPECT_EQ(boost::get<1>(src), boost::get<1>(dst));
  EXPECT_EQ(boost::get<2>(src), boost::get<2>(dst));
}

TEST(EncodingCompatibility, SharedPtrContainersRoundTrip)
{
  using compat_ptr = std::shared_ptr<encoding_compat_record>;

  const std::list<compat_ptr> list_src {
    std::make_shared<encoding_compat_record>(alpha),
    nullptr,
    std::make_shared<encoding_compat_record>(bravo),
  };
  auto list_dst = encoding_compat_round_trip(list_src);

  ASSERT_EQ(list_src.size(), list_dst.size());
  auto list_it = list_dst.begin();
  ASSERT_TRUE(*list_it);
  EXPECT_EQ(alpha, **list_it);
  ++list_it;
  ASSERT_TRUE(*list_it);
  EXPECT_EQ(encoding_compat_record {}, **list_it);
  ++list_it;
  ASSERT_TRUE(*list_it);
  EXPECT_EQ(bravo, **list_it);

  const std::vector<compat_ptr> vector_src {
    std::make_shared<encoding_compat_record>(alpha),
    nullptr,
    std::make_shared<encoding_compat_record>(bravo),
  };
  auto vector_dst = encoding_compat_round_trip(vector_src);

  ASSERT_EQ(vector_src.size(), vector_dst.size());
  ASSERT_TRUE(vector_dst[0]);
  ASSERT_TRUE(vector_dst[1]);
  ASSERT_TRUE(vector_dst[2]);
  EXPECT_EQ(alpha, *vector_dst[0]);
  EXPECT_EQ(encoding_compat_record {}, *vector_dst[1]);
  EXPECT_EQ(bravo, *vector_dst[2]);
}

TEST(EncodingCompatibility, FeatureEncodingPropagatesToLegacyContainerElements)
{
  expect_feature_encode_calls(
    std::vector<encoding_compat_record> { alpha, bravo, charlie }, 3);
  expect_feature_encode_calls(
    std::list<encoding_compat_record> { alpha, bravo, charlie }, 3);
  expect_feature_encode_calls(
    std::deque<encoding_compat_record> { alpha, bravo, charlie }, 3);
  expect_feature_encode_calls(
    boost::container::small_vector<encoding_compat_record, 4> {
      alpha, bravo, charlie }, 3);
  expect_feature_encode_calls(
    std::array<encoding_compat_record, 3> { alpha, bravo, charlie }, 3);
  expect_feature_encode_calls(
    std::map<encoding_compat_record, encoding_compat_record> {
      { alpha, bravo },
      { bravo, charlie },
    }, 4);
  expect_feature_encode_calls(
    boost::container::flat_map<encoding_compat_record, encoding_compat_record> {
      { alpha, bravo },
      { bravo, charlie },
    }, 4);
  expect_feature_encode_calls(
    std::unordered_map<encoding_compat_record,
                       encoding_compat_record,
                       encoding_compat_record_hash> {
      { alpha, bravo },
      { bravo, charlie },
    }, 4);
  expect_feature_encode_calls(
    std::vector<std::shared_ptr<encoding_compat_record>> {
      std::make_shared<encoding_compat_record>(alpha),
      nullptr,
      std::make_shared<encoding_compat_record>(bravo),
    }, 3);
}

TEST(EncodingCompatibility, DecodeNoClearMapPreservesDuplicateKeys)
{
  const std::map<encoding_compat_record, encoding_compat_record> src {
    { alpha, bravo },
  };
  buffer::list bl;
  encode(src, bl);

  std::map<encoding_compat_record, encoding_compat_record> dst {
    { alpha, charlie },
    { charlie, alpha },
  };
  auto p = bl.cbegin();
  decode_noclear(dst, p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(charlie, dst.at(alpha));
  EXPECT_EQ(alpha, dst.at(charlie));
}

TEST(EncodingCompatibility, DecodeNoClearFlatMapReplacesDuplicateKeys)
{
  const boost::container::flat_map<encoding_compat_record,
                                  encoding_compat_record> src {
    { alpha, bravo },
  };
  buffer::list bl;
  encode(src, bl);

  boost::container::flat_map<encoding_compat_record,
                             encoding_compat_record> dst {
    { alpha, charlie },
    { charlie, alpha },
  };
  auto p = bl.cbegin();
  decode_noclear(dst, p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(bravo, dst.at(alpha));
  EXPECT_EQ(alpha, dst.at(charlie));
}

TEST(EncodingCompatibility, DecodeNoClearPrimitiveMapPreservesDuplicateKeys)
{
  const auto entries = std::array {
    std::pair { uint64_t { 7 }, std::bit_cast<float>(uint32_t { 0x3f800000 }) },
    std::pair { uint64_t { 11 }, std::bit_cast<float>(uint32_t { 0x40000000 }) },
  };
  auto bl = make_decode_noclear_payload(entries);

  std::map<uint64_t, float> dst {
    { uint64_t { 7 }, std::bit_cast<float>(uint32_t { 0x40400000 }) },
    { uint64_t { 13 }, std::bit_cast<float>(uint32_t { 0x40800000 }) },
  };
  auto p = bl.cbegin();
  decode_noclear(dst, p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(3u, dst.size());
  EXPECT_EQ(uint32_t { 0x40400000 }, std::bit_cast<uint32_t>(dst.at(7)));
  EXPECT_EQ(uint32_t { 0x40000000 }, std::bit_cast<uint32_t>(dst.at(11)));
  EXPECT_EQ(uint32_t { 0x40800000 }, std::bit_cast<uint32_t>(dst.at(13)));
}

TEST(EncodingCompatibility, DecodeNoClearPrimitiveFlatMapReplacesDuplicateKeys)
{
  const auto entries = std::array {
    std::pair { uint64_t { 7 }, std::bit_cast<float>(uint32_t { 0x3f800000 }) },
    std::pair { uint64_t { 11 }, std::bit_cast<float>(uint32_t { 0x40000000 }) },
  };
  auto bl = make_decode_noclear_payload(entries);

  boost::container::flat_map<uint64_t, float> dst {
    { uint64_t { 7 }, std::bit_cast<float>(uint32_t { 0x40400000 }) },
    { uint64_t { 13 }, std::bit_cast<float>(uint32_t { 0x40800000 }) },
  };
  auto p = bl.cbegin();
  decode_noclear(dst, p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(3u, dst.size());
  EXPECT_EQ(uint32_t { 0x3f800000 }, std::bit_cast<uint32_t>(dst.at(7)));
  EXPECT_EQ(uint32_t { 0x40000000 }, std::bit_cast<uint32_t>(dst.at(11)));
  EXPECT_EQ(uint32_t { 0x40800000 }, std::bit_cast<uint32_t>(dst.at(13)));
}

TEST(EncodingCompatibility, DecodeFinishSkipsUnknownTrailingFields)
{
  const encoding_compat_v2_record src { 7, "known", "unknown" };
  buffer::list bl;
  src.encode(bl);

  encoding_compat_v1_record dst;
  auto p = bl.cbegin();
  dst.decode(p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ((encoding_compat_v1_record { 7, "known" }), dst);
}

TEST(EncodingCompatibility, MacroDecodeRejectsNewerCompat)
{
  buffer::list bl;
  encode_macro_header(bl, 2, 2, 0);

  encoding_macro_record dst;
  auto p = bl.cbegin();

  EXPECT_THROW(dst.decode(p), buffer::malformed_input);
}

TEST(EncodingCompatibility, MacroDecodeRejectsShortPayload)
{
  buffer::list bl;
  encode_macro_header(bl, 1, 1, 1);

  encoding_macro_record dst;
  auto p = bl.cbegin();

  EXPECT_THROW(dst.decode(p), buffer::malformed_input);
}

TEST(EncodingCompatibility, MacroDecodeFinishSkipsUnreadBytes)
{
  buffer::list bl;
  encode_macro_header(bl, 1, 1, 2);
  encode(__u8 { 17 }, bl);
  encode(__u8 { 19 }, bl);

  encoding_macro_record dst;
  auto p = bl.cbegin();
  dst.decode(p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(__u8 { 17 }, dst.value);
}

TEST(EncodingCompatibility, MacroDecodeFinishRejectsOverread)
{
  buffer::list bl;
  encode_macro_header(bl, 1, 1, 1);
  encode(__u8 { 17 }, bl);
  encode(__u8 { 19 }, bl);

  auto p = bl.cbegin();

  EXPECT_THROW(decode_macro_overread(p), buffer::malformed_input);
}

TEST(EncodingCompatibility, MacroDecodeUnknownPreservesEnvelope)
{
  const encoding_macro_record src { 42 };
  buffer::list bl;
  src.encode(bl);

  buffer::list payload;
  auto p = bl.cbegin();
  decode_macro_unknown(payload, p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(bl.to_str(), payload.to_str());
}

TEST(EncodingCompatibility, MacroEncodeFinishNewCompatUpdatesEnvelope)
{
  const encoding_new_compat_record src { 43 };
  buffer::list bl;
  src.encode(bl);

  encoding_macro_record dst;
  auto p = bl.cbegin();

  EXPECT_THROW(dst.decode(p), buffer::malformed_input);
}

TEST(EncodingCompatibility, MacroDecodeStartUncheckedExposesHeaderFields)
{
  buffer::list bl;
  encode_macro_header(bl, 2, 1, 0);

  auto p = bl.cbegin();
  const auto [struct_v, struct_len] = decode_unchecked_macro_header(p);

  EXPECT_TRUE(p.end());
  EXPECT_EQ(__u8 { 2 }, struct_v);
  EXPECT_EQ(__u32 { 0 }, struct_len);
}

TEST(EncodingCompatibility, MacroLegacyCompatLenDecodesOldPayload)
{
  buffer::list bl;
  encode(__u8 { 1 }, bl);
  encode(__u8 { 23 }, bl);

  auto p = bl.cbegin();

  EXPECT_EQ(__u8 { 23 }, decode_legacy_macro_value(p));
  EXPECT_TRUE(p.end());
}

TEST(EncodingCompatibility, MacroLegacyCompatLenDecodesCurrentPayload)
{
  buffer::list bl;
  encode_macro_header(bl, 2, 1, 1);
  encode(__u8 { 29 }, bl);

  auto p = bl.cbegin();

  EXPECT_EQ(__u8 { 29 }, decode_legacy_macro_value(p));
  EXPECT_TRUE(p.end());
}

TEST(EncodingCompatibility, MacroLegacyCompatLenRejectsNewerCompat)
{
  buffer::list bl;
  encode_macro_header(bl, 2, 3, 0);

  auto p = bl.cbegin();

  EXPECT_THROW(decode_legacy_macro_value(p), buffer::malformed_input);
}

TEST(EncodingCompatibility, MacroLegacyCompatLen16SkipsOldVersionBytes)
{
  buffer::list bl;
  encode(__u16 { 1 }, bl);
  encode(__u8 { 31 }, bl);

  auto p = bl.cbegin();

  EXPECT_EQ(__u8 { 31 }, decode_legacy_macro_value_16(p));
  EXPECT_TRUE(p.end());
}

TEST(EncodingCompatibility, MacroLegacyCompatLen32SkipsOldVersionBytes)
{
  buffer::list bl;
  encode(__u32 { 1 }, bl);
  encode(__u8 { 37 }, bl);

  auto p = bl.cbegin();

  EXPECT_EQ(__u8 { 37 }, decode_legacy_macro_value_32(p));
  EXPECT_TRUE(p.end());
}

TEST(EncodingCompatibility, RepresentativeWireFormats)
{
  std::string string_wire;
  append_compat_string(string_wire, "hello");
  expect_compat_wire_format(std::string { "hello" }, string_wire);

  std::string vector_u64_wire;
  append_compat_le32(vector_u64_wire, 2);
  append_compat_le64(vector_u64_wire, 11);
  append_compat_le64(vector_u64_wire, 22);
  expect_compat_wire_format(std::vector<uint64_t> { 11, 22 },
                            vector_u64_wire);

  std::string vector_record_wire;
  append_compat_le32(vector_record_wire, 2);
  append_compat_record(vector_record_wire, alpha);
  append_compat_record(vector_record_wire, bravo);
  expect_compat_wire_format(std::vector<encoding_compat_record> { alpha, bravo },
                            vector_record_wire);

  std::string array_record_wire;
  append_compat_record(array_record_wire, alpha);
  append_compat_record(array_record_wire, bravo);
  expect_compat_wire_format(std::array<encoding_compat_record, 2> { alpha, bravo },
                            array_record_wire);

  std::string shared_ptr_vector_wire;
  append_compat_le32(shared_ptr_vector_wire, 5);
  append_compat_record(shared_ptr_vector_wire, alpha);
  append_compat_record(shared_ptr_vector_wire, encoding_compat_record {});
  append_compat_record(shared_ptr_vector_wire, encoding_compat_record {});
  append_compat_record(shared_ptr_vector_wire, bravo);
  append_compat_record(shared_ptr_vector_wire, encoding_compat_record {});
  expect_compat_wire_format(
    std::vector<std::shared_ptr<encoding_compat_record>> {
      std::make_shared<encoding_compat_record>(alpha),
      nullptr,
      nullptr,
      std::make_shared<encoding_compat_record>(bravo),
      nullptr,
    },
    shared_ptr_vector_wire);

  std::string shared_ptr_list_wire;
  append_compat_le32(shared_ptr_list_wire, 5);
  append_compat_record(shared_ptr_list_wire, alpha);
  append_compat_record(shared_ptr_list_wire, encoding_compat_record {});
  append_compat_record(shared_ptr_list_wire, encoding_compat_record {});
  append_compat_record(shared_ptr_list_wire, bravo);
  append_compat_record(shared_ptr_list_wire, encoding_compat_record {});
  expect_compat_wire_format(
    std::list<std::shared_ptr<encoding_compat_record>> {
      std::make_shared<encoding_compat_record>(alpha),
      nullptr,
      nullptr,
      std::make_shared<encoding_compat_record>(bravo),
      nullptr,
    },
    shared_ptr_list_wire);

  expect_feature_compat_wire_format(
    std::vector<std::shared_ptr<encoding_compat_record>> {
      std::make_shared<encoding_compat_record>(alpha),
      nullptr,
      nullptr,
      std::make_shared<encoding_compat_record>(bravo),
      nullptr,
    },
    shared_ptr_vector_wire);
}
