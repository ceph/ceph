// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include <list>
#include <deque>
#include <vector>

#include "include/seq_util.h"

#include "gtest/gtest.h"

using namespace std;

template <typename SeqT>
auto reverse_copy(const SeqT& xs)
{
 return SeqT(rbegin(xs), rend(xs));
}

TEST(util, test_splice_copy)
{
 const list   init_l { 1, 2, 3, 4, 5 };
 const vector init_v { 1, 2, 3, 4, 5 };

 const list   xsl { 6, 7, 8, 9, 10 };
 const vector xsv { 6, 7, 8, 9, 10 };

 const vector chkv { 6, 7, 8, 9, 10, 1, 2, 3, 4, 5 };

 // void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
 //             const SeqT1& src)
 {
 vector v(init_v);

 ceph::util::splice_copy(v, begin(v), xsv);

 ASSERT_EQ(v, chkv);
 }

 // void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
 //             const SeqT1&& src)
 {
 vector v(init_v);

 ceph::util::splice_copy(v, begin(v), vector { 6, 7, 8, 9, 10 });

 ASSERT_EQ(v, chkv);
 }

 // void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
 //             const SeqT1& src, typename SeqT1::const_iterator src_pos)
 {
 vector v(init_v);

 ceph::util::splice_copy(v, begin(v), xsv, begin(xsv));
                   
 ASSERT_EQ(v, chkv);
 }

 // void splice_copy(SeqT0& dest, typename SeqT0::iterator pos,
 //             const SeqT1& src, const typename SeqT1::size_type src_offset)
 {
 vector v(init_v);

 ceph::util::splice_copy(v, begin(v), xsv, 0);
                   
 ASSERT_EQ(v, chkv);
 }


 // void splice_copy(SeqT0& dest, typename SeqT0::const_iterator pos,
 //             SeqT1&& src, const long src_offset)
 {
 vector v(init_v);

 ceph::util::splice_copy(v, begin(v), vector { 6, 7, 8, 9, 10 }, 0);
                   
 ASSERT_EQ(v, chkv);
 }

 // void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
 //             SeqT1_ConstIterator src_begin, SeqT1_ConstIterator src_end)
 {
 vector v(init_v);

 ceph::util::splice_copy(v, begin(v), cbegin(xsv), cend(xsv));
                   
 ASSERT_EQ(v, chkv);
 }

 // void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
 //             const SeqT1& src, const typename SeqT1::size_type src_begin_offset, const typename SeqT1::size_type src_end_offset)
 {
 vector v(init_v);

 ceph::util::splice_copy(v, begin(v),
                    xsv, 0, xsv.size());

 ASSERT_EQ(v, chkv);
 }

 // void splice_copy(SeqT0& dest, typename SeqT0::const_iterator pos, 
 //             SeqT1&& src, const typename SeqT1::size_type src_begin_offset, const typename SeqT1::size_type src_end_offset)
 {
 vector v(init_v);

 ceph::util::splice_copy(v, begin(v),
                    vector { 6, 7, 8, 9, 10 }, 0, xsv.size());

 ASSERT_EQ(v, chkv);
 }

 // Finally, we'll try some mixed operations:
 {
 const vector vchk { 1, 2, 3, 4, 5, 10, 9, 8, 7, 6 };

 auto ys(reverse_copy(xsv));

 auto v(init_v);
 
 ceph::util::splice_copy(v, end(v), cbegin(ys), cend(ys));

 ASSERT_EQ(vchk, v);
 }

 {
 const list lchk { 1, 2, 3, 4, 5, 10, 9, 8, 7, 6 };

 auto ys(reverse_copy(xsv));

 auto l(init_l);
 
 ceph::util::splice_copy(l, end(l), cbegin(ys), cend(ys));

 ASSERT_EQ(lchk, l);
 }
}

template <template <class, class> typename SeqT>
auto check_push_front()
{
 const SeqT expected_xs { 11, 10, 1, 2, 3, 4, 5 };
 
 SeqT xs { 1, 2, 3, 4, 5 };

 // rvalue:
 ceph::util::push_front(xs, 10);

 // lvalue:
 int foo = 11;
 ceph::util::push_front(xs, foo);

 // We can't assert here because Google Test has no way of knowing
 // which test we actually belong to:
 return expected_xs == xs;
}

TEST(util, test_push_front)
{
 /* Note that we're not going to support std::forward_list<> for now-- it's
 "special" and almost certainly more work than it's worth until there's a
 clear need. Code trying to use it will fail to compile. Supporting
 std::array<> doesn't really make much sense since the size can't change
 dynamically.
 */

 ASSERT_TRUE(check_push_front<list>());
 ASSERT_TRUE(check_push_front<deque>());
 ASSERT_TRUE(check_push_front<vector>());
}
