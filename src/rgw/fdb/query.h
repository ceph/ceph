// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#ifndef CEPH_FDB_QUERY_H
 #define CEPH_FDB_QUERY_H

/* As you may have already suspected, queries in libfdb are actually a compiled
 * interval algebra, with a selector essentially being the output. Of the various
 * ways to handle queries, this was the one that I felt had both expressive power
 * and elegance, with a perhaps surprisingly /low/ amount of pure "machinery". */
namespace ceph::libfdb::query {

/* These options are passed through to FoundationDB. result_limit is a range-read
 * limit requested from the server, not a local post-filter limit. */
struct query_options final
{
 int result_limit = 0;          // FDB range-read limit (0 == unlimited)

 int target_bytes = 0;          // FDB range-read target bytes (0 == unlimited)

 bool reverse_order = false;

 FDBStreamingMode streaming_mode = FDB_STREAMING_MODE_ITERATOR;

 public:
 constexpr bool operator==(const query_options&) const noexcept = default;
};

struct expression_tag {};

template <typename T>
concept expression =
 std::derived_from<std::remove_cvref_t<T>, expression_tag>;

struct interval_bound final
{
 std::string key;
 bool inclusive;

 explicit constexpr interval_bound(const concepts::libfdb_key auto& key_,
                                   const bool inclusive_)
  : key(detail::as_libfdb_key_view(key_)),
    inclusive(inclusive_)
 {}

 constexpr bool operator==(const interval_bound&) const noexcept = default;
};

constexpr interval_bound closed(const concepts::libfdb_key auto& key)
{
 return interval_bound(key, true);
}

constexpr interval_bound open(const concepts::libfdb_key auto& key)
{
 return interval_bound(key, false);
}

/* Prefix queries assume user keys do not start with 0xFF. Appending 0xFF
would exclude valid keys (beginning with prefix + 0xFF), so we build the
next-in-lexicographic-order prefix bound instead. Callers can still specify
an explicit interval if they need something special. */
constexpr std::string successor(const std::string_view prefix)
{
 if (prefix.empty()) {
  return "\xFF";
 }

 if (0xFF == static_cast<unsigned char>(prefix.front())) {
  throw libfdb_exception("requested prefix has no finite successor");
 }

 const auto i = prefix.find_last_not_of(static_cast<char>(0xFF));
 auto end_key = std::string(prefix.substr(0, i + 1));

 end_key.back() = static_cast<char>(static_cast<unsigned char>(end_key.back()) + 1);

 return end_key;
}

struct interval final : expression_tag
{
 std::string begin_key;
 std::string end_key;
 bool begin_inclusive = true;
 bool end_inclusive = false;
 query_options options;

 constexpr interval(interval_bound begin, interval_bound end)
  : begin_key(std::move(begin.key)),
    end_key(std::move(end.key)),
    begin_inclusive(begin.inclusive),
    end_inclusive(end.inclusive)
 {}

 constexpr interval(const concepts::libfdb_key auto& begin_key_,
                    const concepts::libfdb_key auto& end_key_)
  : interval(closed(begin_key_), open(end_key_))
 {}

 constexpr interval(const concepts::libfdb_key auto& begin_key_,
                    interval_bound end)
  : interval(closed(begin_key_), std::move(end))
 {}

 constexpr interval(interval_bound begin,
                    const concepts::libfdb_key auto& end_key_)
  : interval(std::move(begin), open(end_key_))
 {}

 constexpr explicit interval(const concepts::libfdb_key auto& prefix)
  : begin_key(detail::as_libfdb_key_view(prefix)),
    end_key(successor(detail::as_libfdb_key_view(prefix)))
 {}

 constexpr bool operator==(const interval& rhs) const noexcept
 {
  return begin_key == rhs.begin_key and
         end_key == rhs.end_key and
         begin_inclusive == rhs.begin_inclusive and
         end_inclusive == rhs.end_inclusive and
         options == rhs.options;
 }
};

template <typename T>
concept query_interval =
 std::same_as<std::remove_cvref_t<T>, interval>;

template <typename T>
concept non_interval_expression =
 expression<T> and not query_interval<T>;

constexpr interval_bound begin_bound(const interval& x)
{
 return interval_bound(x.begin_key, x.begin_inclusive);
}

constexpr interval_bound end_bound(const interval& x)
{
 return interval_bound(x.end_key, x.end_inclusive);
}

constexpr bool is_empty(const interval& x)
{
 if (x.begin_key < x.end_key) {
  return false;
 }

 if (x.end_key < x.begin_key) {
  return true;
 }

 return not (x.begin_inclusive and x.end_inclusive);
}

constexpr interval empty()
{
 return interval(closed(""), open(""));
}

constexpr interval universal()
{
 return interval(closed(""), open(std::string_view("\xff", 1)));
}

constexpr bool is_universal(const interval& x)
{
 return x == universal();
}

constexpr interval singleton(const concepts::libfdb_key auto& key)
{
 return interval(closed(key), closed(key));
}

constexpr interval prefix(const concepts::libfdb_key auto& key)
{
 return interval(key);
}

constexpr interval between(interval_bound begin, interval_bound end)
{
 return interval(std::move(begin), std::move(end));
}

constexpr interval between(const concepts::libfdb_key auto& begin_key,
                           const concepts::libfdb_key auto& end_key)
{
 return between(closed(begin_key), open(end_key));
}

constexpr interval with_options(interval x, const query_options& options)
{
 x.options = options;
 return x;
}

enum struct inclusivity_merge { all, any };

constexpr bool merge_inclusive(const bool lhs,
                               const bool rhs,
                               const inclusivity_merge merge)
{
 if (inclusivity_merge::all == merge) {
  return lhs and rhs;
 }

 return lhs or rhs;
}

constexpr interval_bound min_key_bound(interval_bound lhs,
                                       interval_bound rhs,
                                       const inclusivity_merge merge)
{
 if (lhs.key < rhs.key) {
  return lhs;
 }

 if (rhs.key < lhs.key) {
  return rhs;
 }

 lhs.inclusive = merge_inclusive(lhs.inclusive, rhs.inclusive, merge);
 return lhs;
}

constexpr interval_bound max_key_bound(interval_bound lhs,
                                       interval_bound rhs,
                                       const inclusivity_merge merge)
{
 if (lhs.key < rhs.key) {
  return rhs;
 }

 if (rhs.key < lhs.key) {
  return lhs;
 }

 lhs.inclusive = merge_inclusive(lhs.inclusive, rhs.inclusive, merge);
 return lhs;
}

constexpr interval_bound max_begin(interval_bound lhs, interval_bound rhs)
{
 return max_key_bound(std::move(lhs), std::move(rhs), inclusivity_merge::all);
}

constexpr interval_bound min_end(interval_bound lhs, interval_bound rhs)
{
 return min_key_bound(std::move(lhs), std::move(rhs), inclusivity_merge::all);
}

constexpr interval_bound difference_end_before(const interval_bound& removed_begin)
{
 return interval_bound(removed_begin.key, not removed_begin.inclusive);
}

constexpr interval_bound difference_begin_after(const interval_bound& removed_end)
{
 return interval_bound(removed_end.key, not removed_end.inclusive);
}

constexpr interval intersection(interval lhs, const interval& rhs)
{
 auto out = with_options(interval(max_begin(begin_bound(lhs), begin_bound(rhs)),
                                  min_end(end_bound(lhs), end_bound(rhs))),
                         lhs.options);

 if (is_empty(out)) {
  return empty();
 }

 return out;
}

constexpr bool definitely_before(const interval& lhs, const interval& rhs)
{
 if (lhs.end_key < rhs.begin_key) {
  return true;
 }

 if (rhs.begin_key < lhs.end_key) {
  return false;
 }

 return not lhs.end_inclusive and not rhs.begin_inclusive;
}

constexpr bool definitely_after(const interval& lhs, const interval& rhs)
{
 return definitely_before(rhs, lhs);
}

constexpr bool can_coalesce(const interval& lhs, const interval& rhs)
{
 return not definitely_before(lhs, rhs) and
        not definitely_after(lhs, rhs);
}

constexpr interval_bound min_begin(interval_bound lhs, interval_bound rhs)
{
 return min_key_bound(std::move(lhs), std::move(rhs), inclusivity_merge::any);
}

constexpr interval_bound max_end(interval_bound lhs, interval_bound rhs)
{
 return max_key_bound(std::move(lhs), std::move(rhs), inclusivity_merge::any);
}

constexpr interval coalesce(interval lhs, const interval& rhs)
{
 if (is_empty(lhs)) {
  return rhs;
 }

 if (is_empty(rhs)) {
  return lhs;
 }

 return with_options(interval(min_begin(begin_bound(lhs), begin_bound(rhs)),
                              max_end(end_bound(lhs), end_bound(rhs))),
                     lhs.options);
}

template <expression LhsT, expression RhsT>
struct difference_expr final : expression_tag
{
 LhsT lhs;
 RhsT rhs;
};

template <expression LhsT, expression RhsT>
struct set_union_expr final : expression_tag
{
 LhsT lhs;
 RhsT rhs;
};

template <typename LhsT, typename RhsT>
requires expression<LhsT> and expression<RhsT>
constexpr auto difference(LhsT&& lhs, RhsT&& rhs)
{
 return difference_expr<std::remove_cvref_t<LhsT>,
                        std::remove_cvref_t<RhsT>> {
  .lhs = std::forward<LhsT>(lhs),
  .rhs = std::forward<RhsT>(rhs)
 };
}

template <typename LhsT, typename RhsT>
requires expression<LhsT> and expression<RhsT>
constexpr auto set_union(LhsT&& lhs, RhsT&& rhs)
{
 return set_union_expr<std::remove_cvref_t<LhsT>,
                       std::remove_cvref_t<RhsT>> {
  .lhs = std::forward<LhsT>(lhs),
  .rhs = std::forward<RhsT>(rhs)
 };
}

template <typename ExprT>
requires expression<ExprT>
constexpr auto complement(ExprT&& expr)
{
 return difference(universal(), std::forward<ExprT>(expr));
}

template <typename SinkT>
constexpr void for_each_interval(const interval& x, SinkT&& sink)
{
 if (is_empty(x)) {
  return;
 }

 std::invoke(std::forward<SinkT>(sink), x);
}

template <expression LhsT, expression RhsT, typename SinkT>
constexpr void for_each_interval(const difference_expr<LhsT, RhsT>& expr,
                                 SinkT&& sink)
{
 if constexpr (query_interval<RhsT>) {
  if (is_empty(expr.rhs)) {
   for_each_interval(expr.lhs, std::forward<SinkT>(sink));
   return;
  }
 }

 for_each_interval(expr.lhs, [&](const interval& lhs) {
  auto cursor = begin_bound(lhs);
  bool emitted_tail = false;

  auto emit_remaining = [&sink, &lhs, &cursor, &emitted_tail](const interval& rhs) {
   if (emitted_tail) {
    return;
   }

   auto remaining = with_options(interval(cursor, end_bound(lhs)),
                                 lhs.options);

   if (is_empty(remaining)) {
    emitted_tail = true;
    return;
   }

   if (definitely_before(rhs, remaining)) {
    return;
   }

   if (definitely_after(rhs, remaining)) {
    std::invoke(sink, remaining);
    emitted_tail = true;
    return;
   }

   const auto overlap = intersection(remaining, rhs);

   if (is_empty(overlap)) {
    return;
   }

   for_each_interval(with_options(interval(cursor, difference_end_before(begin_bound(overlap))),
                                  lhs.options),
                     sink);
   cursor = difference_begin_after(end_bound(overlap));
  };

  for_each_interval(expr.rhs, emit_remaining);

  if (emitted_tail) {
   return;
  }

  for_each_interval(with_options(interval(cursor, end_bound(lhs)),
                                 lhs.options),
                    sink);
 });
}

template <typename SinkT>
constexpr void emit_union(const interval& lhs, const interval& rhs, SinkT&& sink)
{
 if (is_empty(lhs)) {
  for_each_interval(rhs, std::forward<SinkT>(sink));
  return;
 }

 if (is_empty(rhs)) {
  for_each_interval(lhs, std::forward<SinkT>(sink));
  return;
 }

 if (can_coalesce(lhs, rhs)) {
  for_each_interval(coalesce(lhs, rhs), std::forward<SinkT>(sink));
  return;
 }

 if (definitely_before(lhs, rhs)) {
  for_each_interval(lhs, sink);
  for_each_interval(rhs, sink);
  return;
 }

 for_each_interval(rhs, sink);
 for_each_interval(lhs, std::forward<SinkT>(sink));
}

constexpr bool interval_less(const interval& lhs, const interval& rhs)
{
 if (lhs.begin_key < rhs.begin_key) {
  return true;
 }

 if (rhs.begin_key < lhs.begin_key) {
  return false;
 }

 if (lhs.begin_inclusive != rhs.begin_inclusive) {
  return lhs.begin_inclusive;
 }

 if (lhs.end_key < rhs.end_key) {
  return true;
 }

 if (rhs.end_key < lhs.end_key) {
  return false;
 }

 return lhs.end_inclusive and not rhs.end_inclusive;
}

template <expression LhsT, expression RhsT, typename SinkT>
constexpr void for_each_interval(const set_union_expr<LhsT, RhsT>& expr,
                                 SinkT&& sink)
{
 if constexpr (query_interval<LhsT> and query_interval<RhsT>) {
  emit_union(expr.lhs, expr.rhs, std::forward<SinkT>(sink));
  return;
 }

 std::vector<interval> intervals;
 intervals.reserve(4);

 auto collect = [&intervals](const interval& x) {
  intervals.push_back(x);
 };

 for_each_interval(expr.lhs, collect);
 for_each_interval(expr.rhs, collect);

 if (intervals.empty()) {
  return;
 }

 std::ranges::sort(intervals, interval_less);

 auto current = intervals.front();

 for (const auto& next : intervals | std::views::drop(1)) {
  if (can_coalesce(current, next)) {
   current = coalesce(std::move(current), next);
   continue;
  }

  for_each_interval(current, sink);
  current = next;
 }

 for_each_interval(current, std::forward<SinkT>(sink));
}

template <typename ExprT>
requires expression<ExprT>
constexpr std::size_t interval_count(const ExprT& expr)
{
 std::size_t n = 0;

 for_each_interval(expr, [&n](const interval&) {
  ++n;
 });

 return n;
}

template <typename ExprT>
requires expression<ExprT>
constexpr bool is_empty_expression(const ExprT& expr)
{
 return 0 == interval_count(expr);
}

} // namespace ceph::libfdb::query

namespace ceph::libfdb {

using range_endpoint = query::interval_bound;
using select = query::interval;

constexpr range_endpoint inclusive(const concepts::libfdb_key auto& key)
{
 return query::closed(key);
}

constexpr range_endpoint exclusive(const concepts::libfdb_key auto& key)
{
 return query::open(key);
}

} // namespace ceph::libfdb

#endif
