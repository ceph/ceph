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

#include "common/container_concepts.h"
#include "interval.h"

#include <compare>
#include <concepts>
#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

/* libfdb queries are small expression trees over FoundationDB's lexicographic
 * keyspace. The interval algebra is generic and option-free; this header adapts
 * it to executable FoundationDB ranges and preserves select compatibility. */
namespace ceph::libfdb::query {

namespace core = ::ceph::libfdb::interval;

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

struct byte_string_domain final
{
 using value_type = std::string;

 static constexpr std::strong_ordering compare(const std::string_view lhs,
                                               const std::string_view rhs) noexcept
 {
  return lhs <=> rhs;
 }

 static constexpr bool empty_prefix(const std::string_view prefix) noexcept
 {
  return prefix.empty();
 }

 static constexpr std::optional<std::string> successor(const std::string_view prefix)
 {
  constexpr auto max_byte = static_cast<unsigned char>(0xFF);

  for (auto i = prefix.rbegin(); i != prefix.rend(); ++i) {
   const auto byte = static_cast<unsigned char>(*i);

   if (max_byte == byte) {
    continue;
   }

   auto out = std::string(prefix.begin(), i.base());
   out.back() = static_cast<char>(1 + byte);

   return out;
  }

  return std::nullopt;
 }
};

using boundary = core::boundary<byte_string_domain>;
using boundary_ref = core::boundary_ref<byte_string_domain>;
using core_interval = core::query<byte_string_domain>;

constexpr std::string successor(const std::string_view prefix)
{
 if (prefix.empty()) {
  return "\xFF";
 }

 if (auto end = byte_string_domain::successor(prefix)) {
  return std::move(*end);
 }

 throw libfdb_exception("requested prefix has no finite successor");
}

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

struct interval final
{
 using domain_type = byte_string_domain;
 using boundary_type = boundary_ref;

 std::string begin_key;
 std::string end_key;
 bool begin_inclusive = true;
 bool end_inclusive = false;
 query_options options;

 constexpr interval(std::string begin_key_,
                    std::string end_key_,
                    const bool begin_inclusive_,
                    const bool end_inclusive_)
  : begin_key(std::move(begin_key_)),
    end_key(std::move(end_key_)),
    begin_inclusive(begin_inclusive_),
    end_inclusive(end_inclusive_)
 {}

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

 constexpr boundary_ref lower() const noexcept
 {
  if (begin_inclusive) {
   return boundary_ref::closed(begin_key);
  }

  return boundary_ref::open(begin_key);
 }

 constexpr boundary_ref upper() const noexcept
 {
  if (end_inclusive) {
   return boundary_ref::closed(end_key);
  }

  return boundary_ref::open(end_key);
 }

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
concept byte_interval_expression =
 core::domain_expression<T> &&
 std::same_as<core::expression_domain_t<T>, byte_string_domain>;

template <typename T>
concept configured_expression =
 requires {
  typename std::remove_cvref_t<T>::interval_expression_type;
  typename std::remove_cvref_t<T>::domain_type;
 } &&
 byte_interval_expression<typename std::remove_cvref_t<T>::interval_expression_type> &&
 std::same_as<typename std::remove_cvref_t<T>::domain_type, byte_string_domain>;

template <byte_interval_expression ExprT>
struct configured final
{
 using interval_expression_type = ExprT;
 using domain_type = byte_string_domain;

 ExprT expression;
 query_options options;
};

template <typename T>
concept expression =
 byte_interval_expression<T> || configured_expression<T>;

template <typename T>
concept non_interval_expression =
 expression<T> and not query_interval<T>;

namespace detail {

constexpr std::string_view key_view(const concepts::libfdb_key auto& key)
{
 return ::ceph::libfdb::detail::as_libfdb_key_view(key);
}

constexpr std::string key_string(const concepts::libfdb_key auto& key)
{
 return std::string(key_view(key));
}

template <core::boundary_view_for<byte_string_domain> LowerT,
          core::boundary_view_for<byte_string_domain> UpperT>
constexpr interval to_select(const LowerT& lower,
                             const UpperT& upper,
                             const query_options& options)
{
 auto out = interval(
  lower.finite() ? std::string(lower.finite_key()) : std::string(),
  upper.finite() ? std::string(upper.finite_key()) : std::string("\xFF", 1),
  core::boundary_kind::negative_infinity == lower.kind() || lower.inclusive(),
  upper.finite() && upper.inclusive());
 out.options = options;

 return out;
}

template <core::interval_view IntervalT>
constexpr interval to_select(const IntervalT& x, const query_options& options)
{
 if (core::is_empty(x)) {
  return to_select(boundary::closed(""), boundary::open(""), options);
 }

 return to_select(x.lower(), x.upper(), options);
}

template <expression ExprT>
constexpr query_options options_of(const ExprT& expr)
{
 if constexpr (requires { { expr.options } -> std::same_as<const query_options&>; }) {
  return expr.options;
 } else {
  return {};
 }
}

template <expression ExprT>
requires configured_expression<std::remove_cvref_t<ExprT>>
constexpr decltype(auto) core_expression_of(ExprT&& expr)
{
 return (std::forward<ExprT>(expr).expression);
}

template <expression ExprT>
requires (!configured_expression<std::remove_cvref_t<ExprT>>)
constexpr decltype(auto) core_expression_of(ExprT&& expr)
{
 return std::forward<ExprT>(expr);
}

template <expression ExprT>
constexpr auto configure(ExprT&& expr, const query_options& options)
{
 auto core_expr = core_expression_of(std::forward<ExprT>(expr));

 return configured<std::remove_cvref_t<decltype(core_expr)>> {
  .expression = std::move(core_expr),
  .options = options
 };
}

template <expression OptionsT, byte_interval_expression ExprT>
constexpr auto with_options_of(const OptionsT& options_source, ExprT&& expr)
{
 return configure(std::forward<ExprT>(expr), options_of(options_source));
}

template <typename SinkT>
struct select_interval_sink final
{
 std::remove_reference_t<SinkT>& sink;
 query_options options;

 template <core::interval_view IntervalT>
 constexpr void operator()(const IntervalT& x)
 {
  std::invoke(sink, to_select(x, options));
 }

 template <core::boundary_view_for<byte_string_domain> LowerT,
           core::boundary_view_for<byte_string_domain> UpperT>
 constexpr void emit_interval(const LowerT& lower, const UpperT& upper)
 {
  std::invoke(sink, to_select(lower, upper, options));
 }
};

} // namespace detail

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
 return core::is_empty(x);
}

constexpr interval empty()
{
 return interval(closed(""), open(""));
}

constexpr interval universal()
{
 return interval(closed(""), open(std::string_view("\xFF", 1)));
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
 const auto key_view = detail::key_view(key);

 if (key_view.empty()) {
  return universal();
 }

 if (auto end = byte_string_domain::successor(key_view)) {
  return interval(closed(key_view), open(*end));
 }

 return empty();
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

template <expression ExprT>
requires (!query_interval<ExprT>)
constexpr auto with_options(ExprT&& expr, const query_options& options)
{
 return detail::configure(std::forward<ExprT>(expr), options);
}

constexpr interval intersection(interval lhs, const interval& rhs)
{
 return detail::to_select(core::intersection(lhs, rhs), lhs.options);
}

template <typename LhsT, typename RhsT>
requires expression<LhsT> and expression<RhsT> and
        (not (query_interval<LhsT> and query_interval<RhsT>))
constexpr auto intersection(LhsT&& lhs, RhsT&& rhs)
{
 return detail::with_options_of(lhs,
  core::intersection(detail::core_expression_of(std::forward<LhsT>(lhs)),
                     detail::core_expression_of(std::forward<RhsT>(rhs))));
}

template <typename LhsT, typename RhsT>
requires expression<LhsT> and expression<RhsT>
constexpr auto difference(LhsT&& lhs, RhsT&& rhs)
{
 // Query options are execution options for compiled ranges, not part of
 // interval algebra; rhs shapes the result but does not emit ranges, so its
 // options wouldn't have a job to do here:
 return detail::with_options_of(lhs,
  core::difference(detail::core_expression_of(std::forward<LhsT>(lhs)),
                   detail::core_expression_of(std::forward<RhsT>(rhs))));
}

template <typename LhsT, typename RhsT>
requires expression<LhsT> and expression<RhsT>
constexpr auto set_union(LhsT&& lhs, RhsT&& rhs)
{
 return detail::with_options_of(lhs,
  core::set_union(detail::core_expression_of(std::forward<LhsT>(lhs)),
                  detail::core_expression_of(std::forward<RhsT>(rhs))));
}

template <expression ExprT>
constexpr auto complement(ExprT&& expr)
{
 return detail::with_options_of(expr,
  core::difference(universal(),
                   detail::core_expression_of(std::forward<ExprT>(expr))));
}

template <expression ExprT, typename SinkT>
constexpr void for_each_interval(const ExprT& expr, SinkT&& sink)
{
 detail::select_interval_sink<SinkT> emit { sink, detail::options_of(expr) };

 core::for_each_interval(detail::core_expression_of(expr), emit);
}

template <expression ExprT>
constexpr auto compile_intervals(const ExprT& expr)
{
 std::vector<interval> out;
 out.reserve(core::interval_view<ExprT> ? 1 : 4);

 for_each_interval(expr, [&out](interval x) {
  ceph::util::emplace_append(out, std::move(x));
 });

 return out;
}

template <std::size_t Capacity, expression ExprT>
constexpr auto compile_intervals(const ExprT& expr)
{
 core::static_interval_set<byte_string_domain, Capacity> out;

 for_each_interval(expr, [&out](const interval& x) {
  ceph::util::emplace_append(out, core_interval::between(x.lower(), x.upper()));
 });

 return out;
}

template <expression ExprT>
constexpr std::size_t interval_count(const ExprT& expr)
{
 std::size_t count = 0;

 for_each_interval(expr, [&count](const interval&) {
  ++count;
 });

 return count;
}

template <expression ExprT>
constexpr bool is_empty_expression(const ExprT& expr)
{
 return 0 == interval_count(expr);
}

template <expression ExprT>
constexpr bool contains(const ExprT& expr, const concepts::libfdb_key auto& key)
{
 const auto key_view = detail::key_view(key);
 bool found = false;

 for_each_interval(expr, [&found, key_view](const interval& x) {
  if (found) {
   return;
  }

  found = core::contains(x, key_view);
 });

 return found;
}

template <expression LhsT, expression RhsT>
constexpr bool is_disjoint(const LhsT& lhs, const RhsT& rhs)
{
 return is_empty_expression(intersection(lhs, rhs));
}

template <expression LhsT, expression RhsT>
constexpr bool intersects(const LhsT& lhs, const RhsT& rhs)
{
 return not is_disjoint(lhs, rhs);
}

template <expression LhsT, expression RhsT>
constexpr bool encloses(const LhsT& outer, const RhsT& inner)
{
 return is_empty_expression(difference(inner, outer));
}

constexpr boundary closed_bound(const concepts::libfdb_key auto& key)
{
 return boundary::closed(detail::key_string(key));
}

constexpr boundary open_bound(const concepts::libfdb_key auto& key)
{
 return boundary::open(detail::key_string(key));
}

constexpr auto lower_at_or_after(const concepts::libfdb_key auto& key)
{
 return core::lower_at_or_after<byte_string_domain>(detail::key_string(key));
}

constexpr auto lower_after(const concepts::libfdb_key auto& key)
{
 return core::lower_after<byte_string_domain>(detail::key_string(key));
}

constexpr auto upper_at_or_before(const concepts::libfdb_key auto& key)
{
 return core::upper_at_or_before<byte_string_domain>(detail::key_string(key));
}

constexpr auto upper_before(const concepts::libfdb_key auto& key)
{
 return core::upper_before<byte_string_domain>(detail::key_string(key));
}

constexpr auto between(core::lower_endpoint<byte_string_domain> lower,
                       core::upper_endpoint<byte_string_domain> upper)
{
 return detail::to_select(core::between(std::move(lower), std::move(upper)), {});
}

constexpr auto from(core::lower_endpoint<byte_string_domain> lower)
{
 return detail::configure(core::from(std::move(lower)), {});
}

constexpr auto until(core::upper_endpoint<byte_string_domain> upper)
{
 return detail::configure(core::until(std::move(upper)), {});
}

constexpr auto at(const concepts::libfdb_key auto& key)
{
 return singleton(key);
}

constexpr auto at_or_after(const concepts::libfdb_key auto& key)
{
 return from(lower_at_or_after(key));
}

constexpr auto after(const concepts::libfdb_key auto& key)
{
 return from(lower_after(key));
}

constexpr auto at_or_before(const concepts::libfdb_key auto& key)
{
 return until(upper_at_or_before(key));
}

constexpr auto before(const concepts::libfdb_key auto& key)
{
 return until(upper_before(key));
}

constexpr auto closed_between(const concepts::libfdb_key auto& lower,
                              const concepts::libfdb_key auto& upper)
{
 return between(lower_at_or_after(lower), upper_at_or_before(upper));
}

constexpr auto open_between(const concepts::libfdb_key auto& lower,
                            const concepts::libfdb_key auto& upper)
{
 return between(lower_after(lower), upper_before(upper));
}

template <expression ExprT>
constexpr auto starting_at(ExprT&& expr, const concepts::libfdb_key auto& key)
{
 return intersection(std::forward<ExprT>(expr), at_or_after(key));
}

template <expression ExprT>
constexpr auto starting_after(ExprT&& expr, const concepts::libfdb_key auto& key)
{
 return intersection(std::forward<ExprT>(expr), after(key));
}

template <expression ExprT>
constexpr auto ending_at(ExprT&& expr, const concepts::libfdb_key auto& key)
{
 return intersection(std::forward<ExprT>(expr), at_or_before(key));
}

template <expression ExprT>
constexpr auto ending_before(ExprT&& expr, const concepts::libfdb_key auto& key)
{
 return intersection(std::forward<ExprT>(expr), before(key));
}

template <expression ExprT>
constexpr auto without(ExprT&& expr, const concepts::libfdb_key auto& key)
{
 return difference(std::forward<ExprT>(expr), at(key));
}

template <expression ExprT>
constexpr auto without_prefix(ExprT&& expr, const concepts::libfdb_key auto& key)
{
 return difference(std::forward<ExprT>(expr), prefix(key));
}

constexpr auto prefix_starting_at(const concepts::libfdb_key auto& prefix_key,
                                  const concepts::libfdb_key auto& cursor)
{
 return starting_at(prefix(prefix_key), cursor);
}

constexpr auto prefix_starting_after(const concepts::libfdb_key auto& prefix_key,
                                     const concepts::libfdb_key auto& cursor)
{
 return starting_after(prefix(prefix_key), cursor);
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
