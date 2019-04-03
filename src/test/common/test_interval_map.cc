// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/mpl/apply.hpp>
#include "include/buffer.h"
#include "common/interval_map.h"

using namespace std;

template<typename T>
class IntervalMapTest : public ::testing::Test {
public:
  using TestType = T;
};

template <typename _key>
struct bufferlist_test_type {
  using key = _key;
  using value = bufferlist;

  struct make_splitter {
    template <typename merge_t>
    struct apply {
      bufferlist split(
	key offset,
	key len,
	bufferlist &bu) const {
	bufferlist bl;
	bl.substr_of(bu, offset, len);
	return bl;
      }
      bool can_merge(const bufferlist &left, const bufferlist &right) const {
	return merge_t::value;
      }
      bufferlist merge(bufferlist &&left, bufferlist &&right) const {
	bufferlist bl;
	left.claim_append(right);
	return std::move(left);
      }
      uint64_t length(const bufferlist &r) const {
	return r.length();
      }
    };
  };

  struct generate_random {
    bufferlist operator()(key len) {
      bufferlist bl;
      boost::random::mt19937 rng;
      boost::random::uniform_int_distribution<> chr(0,255);
      for (key i = 0; i < len; ++i) {
	bl.append((char)chr(rng));
      }
      return bl;
    }
  };
};

using IntervalMapTypes = ::testing::Types< bufferlist_test_type<uint64_t> >;

TYPED_TEST_CASE(IntervalMapTest, IntervalMapTypes);

#define USING(_can_merge)					 \
  using TT = typename TestFixture::TestType;                     \
  using key = typename TT::key; (void)key(0);	                 \
  using val = typename TT::value; (void)val(0);			 \
  using splitter = typename boost::mpl::apply<                   \
    typename TT::make_splitter,                                  \
    _can_merge>;                                                 \
  using imap = interval_map<key, val, splitter>; (void)imap();	 \
  typename TT::generate_random gen;                              \
  val v(gen(5));	                                         \
  splitter split; (void)split.split(0, 0, v);

#define USING_NO_MERGE USING(std::false_type)
#define USING_WITH_MERGE USING(std::true_type)

TYPED_TEST(IntervalMapTest, empty) {
  USING_NO_MERGE;
  imap m;
  ASSERT_TRUE(m.empty());
}

TYPED_TEST(IntervalMapTest, insert) {
  USING_NO_MERGE;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(10, 5, vals[2]);
  m.insert(5, 5, vals[1]);
  ASSERT_EQ(m.ext_count(), 3u);

  unsigned i = 0;
  for (auto &&ext: m) {
    ASSERT_EQ(ext.get_len(), 5u);
    ASSERT_EQ(ext.get_off(), 5u * i);
    ASSERT_EQ(ext.get_val(), vals[i]);
    ++i;
  }
  ASSERT_EQ(i, m.ext_count());
}

TYPED_TEST(IntervalMapTest, insert_begin_overlap) {
  USING_NO_MERGE;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(5, 5, vals[1]);
  m.insert(10, 5, vals[2]);
  m.insert(1, 5, vals[0]);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 1u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[0]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 6u);
  ASSERT_EQ(iter.get_len(), 4u);
  ASSERT_EQ(iter.get_val(), split.split(1, 4, vals[1]));
  ++iter;

  ASSERT_EQ(iter.get_off(), 10u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, insert_end_overlap) {
  USING_NO_MERGE;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(5, 5, vals[1]);
  m.insert(8, 5, vals[2]);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[0]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 5u);
  ASSERT_EQ(iter.get_len(), 3u);
  ASSERT_EQ(iter.get_val(), split.split(0, 3, vals[1]));
  ++iter;

  ASSERT_EQ(iter.get_off(), 8u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, insert_middle_overlap) {
  USING_NO_MERGE;
  imap m;
  vector<val> vals{gen(5), gen(7), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(10, 5, vals[2]);
  m.insert(4, 7, vals[1]);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0u);
  ASSERT_EQ(iter.get_len(), 4u);
  ASSERT_EQ(iter.get_val(), split.split(0, 4, vals[0]));
  ++iter;

  ASSERT_EQ(iter.get_off(), 4u);
  ASSERT_EQ(iter.get_len(), 7u);
  ASSERT_EQ(iter.get_val(), vals[1]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 11u);
  ASSERT_EQ(iter.get_len(), 4u);
  ASSERT_EQ(iter.get_val(), split.split(1, 4, vals[2]));
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, insert_single_exact_overlap) {
  USING_NO_MERGE;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, gen(5));
  m.insert(5, 5, vals[1]);
  m.insert(10, 5, vals[2]);
  m.insert(0, 5, vals[0]);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[0]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 5u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[1]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 10u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, insert_single_exact_overlap_end) {
  USING_NO_MERGE;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(5, 5, vals[1]);
  m.insert(10, 5, gen(5));
  m.insert(10, 5, vals[2]);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[0]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 5u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[1]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 10u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, erase) {
  USING_NO_MERGE;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(5, 5, vals[1]);
  m.insert(10, 5, vals[2]);

  m.erase(3, 5);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0u);
  ASSERT_EQ(iter.get_len(), 3u);
  ASSERT_EQ(iter.get_val(), split.split(0, 3, vals[0]));
  ++iter;

  ASSERT_EQ(iter.get_off(), 8u);
  ASSERT_EQ(iter.get_len(), 2u);
  ASSERT_EQ(iter.get_val(), split.split(3, 2, vals[1]));
  ++iter;

  ASSERT_EQ(iter.get_off(), 10u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, erase_exact) {
  USING_NO_MERGE;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(5, 5, vals[1]);
  m.insert(10, 5, vals[2]);

  m.erase(5, 5);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[0]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 10u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, get_containing_range) {
  USING_NO_MERGE;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(10, 5, vals[1]);
  m.insert(20, 5, vals[2]);
  m.insert(30, 5, vals[3]);

  auto rng = m.get_containing_range(5, 21);
  auto iter = rng.first;

  ASSERT_EQ(iter.get_off(), 10u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[1]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 20u);
  ASSERT_EQ(iter.get_len(), 5u);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, rng.second);
}

TYPED_TEST(IntervalMapTest, merge) {
  USING_WITH_MERGE;
  imap m;
  m.insert(10, 4, gen(4));
  m.insert(11, 1, gen(1));
}
