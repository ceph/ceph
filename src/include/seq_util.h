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
 * @author Jesse Williamson <jwilliamson@suse.de>
 *
*/

#ifndef CEPH_SEQ_UTIL
 #define CEPH_SEQ_UTIL 1

#include <iterator>

#include <boost/tti/tti.hpp>

namespace ceph::util::detail {

BOOST_TTI_HAS_MEMBER_FUNCTION(push_front)

} // namespace ceph::util::detail

// splice_copy():
//  - This operation differs from std::list<>::splice() in that it makes
//  copies rather than moving data, hence the name splice_copy().
namespace ceph::util {

template <typename SeqT0, typename SeqT1>
void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
                 const SeqT1& src)
{
 dest.insert(pos, std::begin(src), std::end(src));
}

template <typename SeqT0, typename SeqT1>
void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
                 const SeqT1&& src)
{
 ceph::util::splice_copy(dest, pos, src);
}

template <typename SeqT0, typename SeqT1>
void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
                 const SeqT1& src, typename SeqT1::const_iterator src_pos)
{
 dest.insert(pos, src_pos, std::end(src));
}

template <typename SeqT0, typename SeqT1,
          typename OffsetT>
std::enable_if_t<std::is_integral_v<OffsetT>> 
splice_copy(SeqT0& dest, typename SeqT0::iterator pos,
            const SeqT1& src, const OffsetT src_offset)
{
 auto i = std::begin(src);

 std::advance(i, src_offset);

 ceph::util::splice_copy(dest, pos, src, i);
}

template <typename SeqT0, typename SeqT1_ConstIterator>
void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
                 SeqT1_ConstIterator src_begin, SeqT1_ConstIterator src_end)
{
 dest.insert(pos, src_begin, src_end);
}

template <typename SeqT0, typename SeqT1,
          typename SizeType0, typename SizeType1>
void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
                 const SeqT1& src, const SizeType0 src_begin_offset, const SizeType1 src_end_offset)
{
 auto b = std::begin(src);
 std::advance(b, src_begin_offset);

 auto e = std::begin(src);
 std::advance(e, src_end_offset);

 ceph::util::splice_copy(dest, pos, b, e);
}

template <typename SeqT0, typename SeqT1,
          typename SizeType0, typename SizeType1>
void splice_copy(SeqT0& dest, typename SeqT0::iterator pos, 
                 const SeqT1&& src, const SizeType0 src_begin_offset, const SizeType1 src_end_offset)
{
 auto b = std::begin(src);
 std::advance(b, src_begin_offset);

 auto e = std::begin(src);
 std::advance(e, src_end_offset);

 ceph::util::splice_copy(dest, pos, b, e);
}

} // namespace ceph::util

// push_front():
namespace ceph::util {

template <typename SeqT0, typename T>
void push_front(SeqT0& dest, const T&& x)
{
 using namespace ceph::util::detail;

 using push_front_t = void (SeqT0::*)(const typename SeqT0::value_type&&);

 if constexpr(has_member_function_push_front<push_front_t>::value) 
   dest.push_front(x);
 else
   dest.insert(std::begin(dest), x);
}

template <typename SeqT0, typename T>
void push_front(SeqT0& dest, const T& x)
{
 using namespace ceph::util::detail;

 using push_front_t = void (SeqT0::*)(const typename SeqT0::value_type&&);

 if constexpr(has_member_function_push_front<push_front_t>::value) 
   dest.push_front(x);
 else
   dest.insert(std::begin(dest), x);
}

} // namespace ceph::util

#endif
