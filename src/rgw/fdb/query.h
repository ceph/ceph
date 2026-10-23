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
#include "fdb_errors.h"

#include <string>
#include <vector>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>

#include <cstddef>
#include <utility>
#include <compare>
#include <concepts>
#include <functional>
#include <type_traits>

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

 constexpr bool operator==(const query_options&) const noexcept = default;
};

struct byte_string_domain final
{
 using value_type = std::string;

 // Ordinary FDB keys occupy the closed-open byte interval ["", "\xFF").
 static constexpr std::string minimum_key()
 {
  return {};
 }

 static constexpr std::string_view keyspace_limit_view()
 {
  return std::string_view("\xFF", 1);
 }

 static constexpr std::string keyspace_limit()
 {
  return std::string(keyspace_limit_view());
 }

 // Keys beginning with 0xFF belong to FoundationDB's reserved keyspace:
 static constexpr bool is_reserved_key(const std::string_view key) noexcept
 {
  return key.starts_with('\xFF');
 }

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
  constexpr unsigned char max_byte = 0xFF;

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

// User-facing query helpers are clamped to ordinary FDB keyspace:
constexpr core_interval keyspace()
{
 return core_interval::closed_open(byte_string_domain::minimum_key(),
                                   byte_string_domain::keyspace_limit());
}

// Return the finite byte-string successor when one exists:
constexpr std::string successor(const std::string_view prefix)
{
 if (prefix.empty()) {
  return byte_string_domain::keyspace_limit();
 }

 if (auto upper = byte_string_domain::successor(prefix)) {
  return std::move(*upper);
 }

 throw libfdb_exception(std::errc::invalid_argument, "requested prefix has no finite successor");
}

struct interval_bound final
{
 std::string key;
 bool inclusive;

 explicit constexpr interval_bound(std::string&& key_value, const bool is_inclusive)
  : key(std::move(key_value)), inclusive(is_inclusive)
 {}

 explicit constexpr interval_bound(const concepts::libfdb_key auto& key_value,
                                   const bool is_inclusive)
  : key(::ceph::libfdb::detail::as_libfdb_key_view(key_value)),
    inclusive(is_inclusive)
 {}

 constexpr bool operator==(const interval_bound&) const noexcept = default;
};

constexpr interval_bound closed(concepts::libfdb_key auto&& key)
{
 return interval_bound(std::forward<decltype(key)>(key), true);
}

constexpr interval_bound open(concepts::libfdb_key auto&& key)
{
 return interval_bound(std::forward<decltype(key)>(key), false);
}

struct interval;

constexpr interval detail_make_interval(std::string begin_key,
                                        std::string end_key,
                                        bool begin_inclusive,
                                        bool end_inclusive);

struct interval final
{
 using domain_type = byte_string_domain;
 using boundary_type = boundary_ref;

 std::string begin_key;
 std::string end_key;
 bool begin_inclusive = true;
 bool end_inclusive = false;
 query_options options;

 constexpr interval(std::string lower,
                    std::string upper,
                    const bool includes_begin,
                    const bool includes_end)
  : begin_key(std::move(lower)),
    end_key(std::move(upper)),
    begin_inclusive(includes_begin),
    end_inclusive(includes_end)
 {
  normalize_keyspace();
 }

 constexpr interval(interval_bound lower, interval_bound upper)
  : begin_key(std::move(lower.key)),
    end_key(std::move(upper.key)),
    begin_inclusive(lower.inclusive),
    end_inclusive(upper.inclusive)
 {
  normalize_keyspace();
 }

 constexpr interval(concepts::libfdb_key auto&& lower, concepts::libfdb_key auto&& upper)
  : interval(closed(std::forward<decltype(lower)>(lower)), open(std::forward<decltype(upper)>(upper)))
 {}

 constexpr interval(concepts::libfdb_key auto&& lower, interval_bound upper)
  : interval(closed(std::forward<decltype(lower)>(lower)), std::move(upper))
 {}

 constexpr interval(interval_bound lower,
                    concepts::libfdb_key auto&& upper)
  : interval(std::move(lower), open(std::forward<decltype(upper)>(upper)))
 {}

 constexpr explicit interval(const concepts::libfdb_key auto& prefix)
  : begin_key(::ceph::libfdb::detail::as_libfdb_key_view(prefix)),
    end_key(successor(::ceph::libfdb::detail::as_libfdb_key_view(prefix)))
 {
  normalize_keyspace();
 }

 constexpr boundary_ref lower() const& noexcept
 {
  if (begin_inclusive) {
   return boundary_ref::closed(begin_key);
  }

  return boundary_ref::open(begin_key);
 }

 boundary_ref lower() const&& = delete;

 constexpr boundary_ref upper() const& noexcept
 {
  if (end_inclusive) {
   return boundary_ref::closed(end_key);
  }

  return boundary_ref::open(end_key);
 }

 boundary_ref upper() const&& = delete;

 constexpr bool operator==(const interval& rhs) const noexcept
 {
  return begin_key == rhs.begin_key and
         end_key == rhs.end_key and
         begin_inclusive == rhs.begin_inclusive and
         end_inclusive == rhs.end_inclusive and
         options == rhs.options;
 }

 private:
 struct unchecked_select_t final {};

 constexpr interval(unchecked_select_t,
                    std::string lower,
                    std::string upper,
                    const bool includes_begin,
                    const bool includes_end)
  : begin_key(std::move(lower)),
    end_key(std::move(upper)),
    begin_inclusive(includes_begin),
    end_inclusive(includes_end)
 {}

 friend constexpr interval detail_make_interval(std::string lower,
                                                std::string upper,
                                                bool includes_begin,
                                                bool includes_end)
 {
  return interval(unchecked_select_t {},
                  std::move(lower),
                  std::move(upper),
                  includes_begin,
                  includes_end);
 }

 constexpr void normalize_keyspace()
 {
  if (byte_string_domain::is_reserved_key(end_key)) {
   end_key = byte_string_domain::keyspace_limit();
   end_inclusive = false;
  }
 }
};

template <typename T>
concept query_interval =
 std::same_as<std::remove_cvref_t<T>, interval>;

template <typename T>
concept byte_interval_expression =
 core::expression<T> &&
 std::same_as<core::expression_domain_t<T>, byte_string_domain>;

template <typename T>
concept configured_expression =
 requires(const std::remove_cvref_t<T>& configured) {
  typename std::remove_cvref_t<T>::interval_expression_type;
  typename std::remove_cvref_t<T>::domain_type;
  requires byte_interval_expression<
             typename std::remove_cvref_t<T>::interval_expression_type>;
  requires std::same_as<typename std::remove_cvref_t<T>::domain_type,
                        byte_string_domain>;
  { configured.expression } ->
   std::same_as<const typename std::remove_cvref_t<T>::interval_expression_type&>;
  { configured.options } -> std::same_as<const query_options&>;
 };

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

constexpr std::string key_string(std::string key)
{
 return key;
}

// Clamp an executable selector to ordinary FDB keyspace:
constexpr interval clamp_to_keyspace(interval&& x)
{
 if (byte_string_domain::is_reserved_key(x.end_key)) {
  x.end_key = byte_string_domain::keyspace_limit();
  x.end_inclusive = false;
 }

 return std::move(x);
}

template <core::boundary_view_for<byte_string_domain> BoundT>
constexpr std::string_view bound_key_view(const BoundT& bound)
{
 if (bound.finite()) {
  return std::string_view(bound.finite_key());
 }
 return core::boundary_kind::negative_infinity == bound.kind()
  ? std::string_view {} : byte_string_domain::keyspace_limit_view();
}

// These views cannot allocate or accidentally copy an owning key.
struct keyspace_bounds final
{
 std::string_view lower;
 std::string_view upper;
 bool includes_lower;
 bool includes_upper;

 template <core::boundary_view_for<byte_string_domain> LowerT,
           core::boundary_view_for<byte_string_domain> UpperT>
 constexpr keyspace_bounds(const LowerT& lower_bound, const UpperT& upper_bound)
  : lower(bound_key_view(lower_bound)), upper(bound_key_view(upper_bound)),
    includes_lower(core::boundary_kind::negative_infinity == lower_bound.kind() ||
                   lower_bound.inclusive()),
    includes_upper(upper_bound.finite() && upper_bound.inclusive())
 {
  if (byte_string_domain::is_reserved_key(upper)) {
   upper = byte_string_domain::keyspace_limit_view();
   includes_upper = false;
  }
 }

 constexpr bool empty() const noexcept
 {
  const auto order = byte_string_domain::compare(lower, upper);
  return std::strong_ordering::greater == order ||
         (std::strong_ordering::equal == order && !(includes_lower && includes_upper));
 }
};

constexpr bool is_empty_select(const interval& x)
{
 return keyspace_bounds(x.lower(), x.upper()).empty();
}

constexpr interval to_select(const keyspace_bounds& bounds, const query_options& options)
{
 const bool empty = bounds.empty();
 auto out = ::ceph::libfdb::query::detail_make_interval(
  empty ? std::string {} : std::string(bounds.lower),
  empty ? std::string {} : std::string(bounds.upper),
  empty || bounds.includes_lower, !empty && bounds.includes_upper);
 out.options = options;
 return out;
}

template <core::boundary_view_for<byte_string_domain> LowerT,
          core::boundary_view_for<byte_string_domain> UpperT>
constexpr interval to_select(const LowerT& lower,
                             const UpperT& upper,
                             const query_options& options)
{
 return to_select(keyspace_bounds(lower, upper), options);
}

constexpr interval to_select(interval&& x, const query_options& options)
{
 if (is_empty_select(x)) {
  return to_select(keyspace_bounds(x.lower(), x.upper()), options);
 }
 x.options = options;
 return clamp_to_keyspace(std::move(x));
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
 }

 return {};
}

template <expression LHS_T, expression RHS_T>
constexpr query_options combined_options(const LHS_T& lhs, const RHS_T& rhs)
{
 const auto left = options_of(lhs);
 const auto right = options_of(rhs);

 if (left == right or right == query_options {}) {
  return left;
 }
 if (left == query_options {}) {
  return right;
 }

 throw libfdb_exception(std::errc::invalid_argument,
   "conflicting query options; configure operands consistently");
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

template <typename ExprT>
struct is_keyspace_bounded : std::false_type {};

template <>
struct is_keyspace_bounded<interval> : std::true_type {};

template <byte_interval_expression ExprT>
struct is_keyspace_bounded<configured<ExprT>> : is_keyspace_bounded<ExprT> {};

template <byte_interval_expression LHS_T, byte_interval_expression RHS_T>
struct is_keyspace_bounded<core::detail::difference_expr<LHS_T, RHS_T>>
 : is_keyspace_bounded<LHS_T>
{};

template <byte_interval_expression LHS_T, byte_interval_expression RHS_T>
struct is_keyspace_bounded<core::detail::intersection_expr<LHS_T, RHS_T>>
 : std::bool_constant<is_keyspace_bounded<LHS_T>::value ||
                      is_keyspace_bounded<RHS_T>::value>
{};

template <byte_interval_expression LHS_T, byte_interval_expression RHS_T>
struct is_keyspace_bounded<core::detail::set_union_expr<LHS_T, RHS_T>>
 : std::bool_constant<is_keyspace_bounded<LHS_T>::value &&
                      is_keyspace_bounded<RHS_T>::value>
{};

template <typename ExprT>
inline constexpr bool is_keyspace_bounded_v =
 is_keyspace_bounded<std::remove_cvref_t<ExprT>>::value;

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
  emit_interval(x.lower(), x.upper());
 }

 template <core::boundary_view_for<byte_string_domain> LowerT,
           core::boundary_view_for<byte_string_domain> UpperT>
 constexpr void emit_interval(const LowerT& lower, const UpperT& upper)
 {
  const keyspace_bounds bounds(lower, upper);
  if (not bounds.empty()) {
   std::invoke(sink, to_select(bounds, options));
  }
 }
};

struct interval_counter final
{
 std::size_t count = 0;

 template <core::interval_view IntervalT>
 constexpr void operator()(const IntervalT& x)
 {
  emit_interval(x.lower(), x.upper());
 }

 template <core::boundary_view_for<byte_string_domain> LowerT,
           core::boundary_view_for<byte_string_domain> UpperT>
 constexpr void emit_interval(const LowerT& lower, const UpperT& upper)
 {
  count += !keyspace_bounds(lower, upper).empty();
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
 return detail::is_empty_select(x);
}

constexpr interval empty()
{
 return interval(closed(""), open(""));
}

constexpr interval universal()
{
 // libfdb user-facing "universal" lowers the algebraic meaning to FDB key-space:
 return detail_make_interval(byte_string_domain::minimum_key(),
                             byte_string_domain::keyspace_limit(),
                             true,
                             false);
}

constexpr bool is_universal(const interval& x)
{
 // This is a selector-shape check, not full expression normalization.
 return x.begin_key.empty() and x.end_key == byte_string_domain::keyspace_limit_view() and
        x.begin_inclusive and not x.end_inclusive;
}

constexpr interval singleton(concepts::libfdb_key auto&& key)
{
 const auto key_view = detail::key_view(key);

 if (byte_string_domain::is_reserved_key(key_view)) {
  return empty();
 }

 auto lower = closed(std::forward<decltype(key)>(key));
 auto upper = closed(lower.key);
 return interval(std::move(lower), std::move(upper));
}

constexpr interval prefix(concepts::libfdb_key auto&& key)
{
 const auto key_view = detail::key_view(key);

 if (key_view.empty()) {
  return universal();
 }

 if (byte_string_domain::is_reserved_key(key_view)) {
  return empty();
 }

 if (auto upper = byte_string_domain::successor(key_view)) {
  // Prefix math is pure byte-string math; the final selector is keyspace-clamped.
  return interval(closed(std::forward<decltype(key)>(key)), open(std::move(*upper)));
 }

 return empty();
}

constexpr interval between(interval_bound lower, interval_bound upper)
{
 return detail::to_select(interval(std::move(lower), std::move(upper)), {});
}

constexpr interval between(concepts::libfdb_key auto&& begin_key, concepts::libfdb_key auto&& end_key)
{
 return between(closed(std::forward<decltype(begin_key)>(begin_key)),
                 open(std::forward<decltype(end_key)>(end_key)));
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
 return detail::to_select(core::intersection(lhs, rhs), detail::combined_options(lhs, rhs));
}

template <typename LHS_T, typename RHS_T>
requires expression<LHS_T> and expression<RHS_T> and
        (not (query_interval<LHS_T> and query_interval<RHS_T>))
constexpr auto intersection(LHS_T&& lhs, RHS_T&& rhs)
{
 const auto options = detail::combined_options(lhs, rhs);
 return detail::configure(
  core::intersection(detail::core_expression_of(std::forward<LHS_T>(lhs)),
                     detail::core_expression_of(std::forward<RHS_T>(rhs))), options);
}

template <typename LHS_T, typename RHS_T>
requires expression<LHS_T> and expression<RHS_T>
constexpr auto difference(LHS_T&& lhs, RHS_T&& rhs)
{
 // Query options are execution options for compiled ranges, not part of
 // interval algebra; rhs shapes the result but does not emit ranges, so its
 // options wouldn't have a job to do here:
 return detail::with_options_of(lhs,
  core::difference(detail::core_expression_of(std::forward<LHS_T>(lhs)),
                   detail::core_expression_of(std::forward<RHS_T>(rhs))));
}

template <typename LHS_T, typename RHS_T>
requires expression<LHS_T> and expression<RHS_T>
constexpr auto set_union(LHS_T&& lhs, RHS_T&& rhs)
{
 const auto options = detail::combined_options(lhs, rhs);
 return detail::configure(
  core::set_union(detail::core_expression_of(std::forward<LHS_T>(lhs)),
                  detail::core_expression_of(std::forward<RHS_T>(rhs))), options);
}

template <expression ExprT>
constexpr auto complement(ExprT&& expr)
{
 // Complements are relative to public libfdb key-space, not algebraic +/-infinity.
 return detail::with_options_of(expr,
  core::difference(universal(),
                   detail::core_expression_of(std::forward<ExprT>(expr))));
}

template <typename SinkT>
constexpr void for_each_interval(const interval& expr, SinkT&& sink)
{
 if (detail::is_empty_select(expr)) {
  return;
 }

 std::invoke(std::forward<SinkT>(sink), detail::clamp_to_keyspace(interval {expr}));
}

template <non_interval_expression ExprT, typename SinkT>
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

 ::ceph::libfdb::query::for_each_interval(expr, [&out](interval x) {
  ceph::util::emplace_append(out, std::move(x));
 });

 return out;
}

template <std::size_t Capacity, expression ExprT>
constexpr auto compile_intervals(const ExprT& expr)
{
 core::static_interval_set<byte_string_domain, Capacity, interval> out {empty()};

 ::ceph::libfdb::query::for_each_interval(expr, [&out](interval x) {
  try {
   ceph::util::emplace_append(out, std::move(x));
  } catch (const std::length_error& error) {
   throw libfdb_exception(std::errc::value_too_large, error.what());
  }
 });

 return out;
}

template <expression ExprT>
inline constexpr std::size_t interval_capacity_v =
 core::interval_capacity_v<decltype(detail::core_expression_of(std::declval<const ExprT&>()))>;

// Infer fixed storage from the expression type; no capacity guess or counting pass is needed.
template <expression ExprT>
requires (std::dynamic_extent != interval_capacity_v<ExprT>)
constexpr auto compile_intervals(std::in_place_t, const ExprT& expr)
{
 return ::ceph::libfdb::query::compile_intervals<interval_capacity_v<ExprT>>(expr);
}

template <expression ExprT>
constexpr std::size_t interval_count(const ExprT& expr)
{
 detail::interval_counter counter;
 core::for_each_interval(detail::core_expression_of(expr), counter);
 return counter.count;
}

// Relationship predicates are bounded to ordinary FoundationDB keyspace.
// Common expression shapes are answered structurally; others use the same
// canonical interval stream as query execution:
template <expression ExprT>
constexpr bool is_empty_expression(const ExprT& expr)
{
 if constexpr (detail::is_keyspace_bounded_v<ExprT>) {
  return core::is_empty_expression(detail::core_expression_of(expr));
 }

 return 0 == ::ceph::libfdb::query::interval_count(expr);
}

template <expression ExprT>
constexpr bool contains(const ExprT& expr, const concepts::libfdb_key auto& key)
{
 const auto key_view = detail::key_view(key);

 if (byte_string_domain::is_reserved_key(key_view)) {
  return false;
 }

 return core::contains(detail::core_expression_of(expr), key_view);
}

template <expression LHS_T, expression RHS_T>
constexpr bool intersects(const LHS_T& lhs, const RHS_T& rhs)
{
 if constexpr (detail::is_keyspace_bounded_v<LHS_T> &&
               detail::is_keyspace_bounded_v<RHS_T>) {
  return core::intersects(detail::core_expression_of(lhs), detail::core_expression_of(rhs));
 }

 return not ::ceph::libfdb::query::is_empty_expression(::ceph::libfdb::query::intersection(lhs, rhs));
}

template <expression LHS_T, expression RHS_T>
constexpr bool is_disjoint(const LHS_T& lhs, const RHS_T& rhs)
{
 return not ::ceph::libfdb::query::intersects(lhs, rhs);
}

template <expression LHS_T, expression RHS_T>
constexpr bool encloses(const LHS_T& outer, const RHS_T& inner)
{
 if constexpr (detail::is_keyspace_bounded_v<LHS_T> &&
               detail::is_keyspace_bounded_v<RHS_T>) {
  return core::encloses(detail::core_expression_of(outer), detail::core_expression_of(inner));
 }

 return ::ceph::libfdb::query::is_empty_expression(::ceph::libfdb::query::difference(inner, outer));
}

constexpr boundary closed_bound(concepts::libfdb_key auto&& key)
{
 return boundary::closed(detail::key_string(std::forward<decltype(key)>(key)));
}

constexpr boundary open_bound(concepts::libfdb_key auto&& key)
{
 return boundary::open(detail::key_string(std::forward<decltype(key)>(key)));
}

constexpr auto lower_at_or_after(concepts::libfdb_key auto&& key)
{
 return core::lower_at_or_after<byte_string_domain>(detail::key_string(std::forward<decltype(key)>(key)));
}

constexpr auto lower_after(concepts::libfdb_key auto&& key)
{
 return core::lower_after<byte_string_domain>(detail::key_string(std::forward<decltype(key)>(key)));
}

constexpr auto upper_at_or_before(concepts::libfdb_key auto&& key)
{
 return core::upper_at_or_before<byte_string_domain>(detail::key_string(std::forward<decltype(key)>(key)));
}

constexpr auto upper_before(concepts::libfdb_key auto&& key)
{
 return core::upper_before<byte_string_domain>(detail::key_string(std::forward<decltype(key)>(key)));
}

constexpr auto between(core::lower_endpoint<byte_string_domain> lower,
                       core::upper_endpoint<byte_string_domain> upper)
{
 return detail::to_select(interval {std::move(lower.value), std::move(upper.value),
  core::endpoint_inclusion::included == lower.inclusion,
  core::endpoint_inclusion::included == upper.inclusion}, {});
}

constexpr auto from(core::lower_endpoint<byte_string_domain> lower)
{
 // The missing upper side means public libfdb universal, not algebraic +infinity.
 return detail::to_select(interval {std::move(lower.value), byte_string_domain::keyspace_limit(),
  core::endpoint_inclusion::included == lower.inclusion, false}, {});
}

constexpr auto until(core::upper_endpoint<byte_string_domain> upper)
{
 // The missing lower side means public libfdb universal, not algebraic -infinity.
 return detail::to_select(interval {byte_string_domain::minimum_key(), std::move(upper.value),
  true, core::endpoint_inclusion::included == upper.inclusion}, {});
}

constexpr auto at(concepts::libfdb_key auto&& key)
{
 return singleton(std::forward<decltype(key)>(key));
}

constexpr auto at_or_after(concepts::libfdb_key auto&& key)
{
 return from(lower_at_or_after(std::forward<decltype(key)>(key)));
}

constexpr auto after(concepts::libfdb_key auto&& key)
{
 return from(lower_after(std::forward<decltype(key)>(key)));
}

constexpr auto at_or_before(concepts::libfdb_key auto&& key)
{
 return until(upper_at_or_before(std::forward<decltype(key)>(key)));
}

constexpr auto before(concepts::libfdb_key auto&& key)
{
 return until(upper_before(std::forward<decltype(key)>(key)));
}

constexpr auto closed_between(concepts::libfdb_key auto&& lower, concepts::libfdb_key auto&& upper)
{
 return between(lower_at_or_after(std::forward<decltype(lower)>(lower)),
                 upper_at_or_before(std::forward<decltype(upper)>(upper)));
}

constexpr auto open_between(concepts::libfdb_key auto&& lower, concepts::libfdb_key auto&& upper)
{
 return between(lower_after(std::forward<decltype(lower)>(lower)),
                 upper_before(std::forward<decltype(upper)>(upper)));
}

template <expression ExprT>
constexpr auto starting_at(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return ::ceph::libfdb::query::intersection(std::forward<ExprT>(expr),
   at_or_after(std::forward<decltype(key)>(key)));
}

template <expression ExprT>
constexpr auto starting_after(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return ::ceph::libfdb::query::intersection(std::forward<ExprT>(expr),
   after(std::forward<decltype(key)>(key)));
}

template <expression ExprT>
constexpr auto ending_at(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return ::ceph::libfdb::query::intersection(std::forward<ExprT>(expr),
   at_or_before(std::forward<decltype(key)>(key)));
}

template <expression ExprT>
constexpr auto ending_before(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return ::ceph::libfdb::query::intersection(std::forward<ExprT>(expr),
   before(std::forward<decltype(key)>(key)));
}

template <expression ExprT>
constexpr auto without(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return ::ceph::libfdb::query::difference(std::forward<ExprT>(expr),
   at(std::forward<decltype(key)>(key)));
}

template <expression ExprT>
constexpr auto without_prefix(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return ::ceph::libfdb::query::difference(std::forward<ExprT>(expr),
   prefix(std::forward<decltype(key)>(key)));
}

constexpr auto prefix_starting_at(concepts::libfdb_key auto&& prefix_key,
                                  concepts::libfdb_key auto&& cursor)
{
 return starting_at(prefix(std::forward<decltype(prefix_key)>(prefix_key)),
                     std::forward<decltype(cursor)>(cursor));
}

constexpr auto prefix_starting_after(concepts::libfdb_key auto&& prefix_key,
                                     concepts::libfdb_key auto&& cursor)
{
 return starting_after(prefix(std::forward<decltype(prefix_key)>(prefix_key)),
                        std::forward<decltype(cursor)>(cursor));
}

} // namespace ceph::libfdb::query

namespace ceph::libfdb {

using range_endpoint = query::interval_bound;
using select = query::interval;

constexpr range_endpoint inclusive(concepts::libfdb_key auto&& key)
{
 return query::closed(std::forward<decltype(key)>(key));
}

constexpr range_endpoint exclusive(concepts::libfdb_key auto&& key)
{
 return query::open(std::forward<decltype(key)>(key));
}

} // namespace ceph::libfdb

#endif
