// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include "common/mini_flat_map.h"

struct Value {
  int value;

  explicit Value() : value(0) {
  }
  explicit Value(int value) : value(value) {
  }

  friend std::ostream &operator<<(std::ostream &out, const Value &rhs) {
    return out << rhs.value;
  }

  bool operator==(const Value &other) const {
    return value == other.value;
  }
};

namespace fmt {
template <>
struct formatter<Value> : private formatter<int> {
  using formatter<int>::parse;
  template <typename FormatContext>
  auto format(const Value& v, FormatContext& ctx) const {
    return formatter<int>::format(v.value, ctx);
  }
};
} // namespace fmt

struct Key {
  int8_t k;

  Key(int8_t k) : k(k) {
  }

  explicit constexpr operator int8_t() const {
    return k;
  }
  Key &operator++() {
    k++;
    return *this;
  }

  friend std::ostream &operator<<(std::ostream &out, const Key &rhs) {
    return out << static_cast<uint32_t>(rhs.k);
  }
  friend bool operator==(const Key &lhs, const Key &rhs) {
    return lhs.k == rhs.k;
  }
};

namespace fmt {
template <>
struct formatter<Key> : private formatter<int> {
  using formatter<int>::parse;
  template <typename FormatContext>
  auto format(const Key& k, FormatContext& ctx) const {
    return formatter<int>::format(k.k, ctx);
  }
};
} // namespace fmt


TEST(mini_flat_map, copy_operator_and_element_access) {
  mini_flat_map<Key, Value> m(4);
  m[0] = Value(1);
  ASSERT_EQ(1, m[0].value);
  Key non_const_key(0);
  ASSERT_EQ(1, m[non_const_key].value);
  ASSERT_TRUE(m.contains(0));
  mini_flat_map<Key, Value> m2 = m;
  ASSERT_EQ(m, m2);
  mini_flat_map<Key, Value> m3(m);
  ASSERT_TRUE(m3.contains(0));
  ASSERT_TRUE(m.contains(0));
}

TEST(mini_flat_map, fmt_formatting) {
  mini_flat_map<Key, Value> m(4);
  m[0] = Value(100);
  m[2] = Value(200);
  EXPECT_EQ("{0:100,2:200}", m.fmt_print());
  // compare to operator<<
  std::ostringstream oss;
  oss << m;
  EXPECT_EQ(m.fmt_print(), oss.str());
  // use indirectly in fmt::format
  EXPECT_EQ("{0:100,2:200}", fmt::format("{}", m));
}

TEST(mini_flat_map, fmt_formatting_empty) {
  mini_flat_map<Key, Value> m(10);
  EXPECT_EQ("{}", m.fmt_print());
  // compare to operator<<
  const auto as_fmt = fmt::format("{}", m);
  EXPECT_EQ("{}", as_fmt);
  std::ostringstream oss;
  oss << m;
  EXPECT_EQ(as_fmt, oss.str());
}

TEST(mini_flat_map, fmt_formatting_one) {
  mini_flat_map<Key, Value> m(4);
  m[2] = Value(100);
  const auto using_fmt = fmt::format("{}", m);
  EXPECT_EQ("{2:100}", using_fmt);
  // compare to operator<<
  std::ostringstream oss;
  oss << m;
  EXPECT_EQ(using_fmt, oss.str());
}

TEST(mini_flat_map, fmt_formatting_full) {
  mini_flat_map<Key, Value> m(4);
  m[3] = Value(300);
  m[2] = Value(200);
  m[1] = Value(100);
  m[0] = Value(1);
  const auto using_fmt = fmt::format("{}", m);
  EXPECT_EQ("{0:1,1:100,2:200,3:300}", using_fmt);
  // compare to operator<<
  std::ostringstream oss;
  oss << m;
  EXPECT_EQ(using_fmt, oss.str());
}

TEST(mini_flat_map, iterators) {
  mini_flat_map<Key, Value> m(4);
  m[0] = Value(1);
  m[2] = Value(2);
  Value values[] = {Value(1), Value(2)};
  Key keys[] = {Key(0), Key(2)};

  int i = 0;
  for (auto &&[k, v] : m) {
    ASSERT_EQ(keys[i], k);
    ASSERT_EQ(values[i], v);
    i++;
  }
  ASSERT_EQ(2, i);

  const mini_flat_map<Key, Value> m2 = m;
  i = 0;
  // This loop tests const iterator.
  for (auto &&[k, v] : m2) {
    ASSERT_EQ(keys[i], k);
    ASSERT_EQ(values[i], v);
    i++;
  }
  ASSERT_EQ(2, i);
}

TEST(mini_flat_map, capacity) {
  mini_flat_map<Key, Value> m(4);
  ASSERT_FALSE(m.contains(Key(0)));
  Key k(1);
  ASSERT_FALSE(m.contains(k));
  ASSERT_TRUE(m.empty());
  ASSERT_EQ(0, m.size());
  ASSERT_EQ(4, m.max_size());

  m[k] = Value(2);
  ASSERT_TRUE(m.contains(k));
  ASSERT_FALSE(m.empty());
  ASSERT_EQ(1, m.size());
  ASSERT_EQ(4, m.max_size());
}

TEST(mini_flat_map, clear) {
  mini_flat_map<Key, Value> m(4);
  m[1] = Value(2);
  ASSERT_TRUE(m.contains(1));
  m.clear();
  ASSERT_FALSE(m.contains(1));
  ASSERT_TRUE(m.empty());
}
// No insert, insert_range, insert_or_assign, emplace_hint, try_emplace,

TEST(mini_flat_map, emplace_erase) {
  mini_flat_map<Key, Value> m(4);
  m.emplace(1, 2);
  ASSERT_TRUE(m.contains(1));
  m.erase(Key(1));
  ASSERT_FALSE(m.contains(1));
  m.emplace(1, 2);
  ASSERT_TRUE(m.contains(1));
  auto it = m.begin();
  m.erase(it);
  ASSERT_EQ(m.end(), it);
  ASSERT_FALSE(m.contains(1));
  m.emplace(1, 2);
  ASSERT_TRUE(m.contains(1));
  auto cit = m.cbegin();
  m.erase(cit);
  ASSERT_EQ(m.cend(), cit);
  ASSERT_FALSE(m.contains(1));
}
// no erase(range)

TEST(mini_flat_map, swap) {
  mini_flat_map<Key, Value> m(4);
  m[1] = Value(2);
  mini_flat_map<Key, Value> m2(4);
  m2.swap(m);
  ASSERT_TRUE(m.empty());
  ASSERT_FALSE(m2.empty());
}
// No extract, merge

TEST(mini_flat_map, lookup) {
  mini_flat_map<Key, Value> m(4);
  ASSERT_EQ(0, m.count(Key(0)));
  ASSERT_EQ(0, m.count(Key(1)));
  m[1] = Value(2);
  ASSERT_EQ(0, m.count(Key(0)));
  ASSERT_EQ(1, m.count(Key(1)));
}
// NO equal_range, lower_bound, upper_bound
