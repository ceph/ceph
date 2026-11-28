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

#include "key.h"
#include "interval.h"
#include "fdb_errors.h"

#include <string>
#include <vector>
#include <optional>
#include <stdexcept>
#include <string_view>

#include <cstddef>
#include <utility>
#include <compare>
#include <concepts>
#include <functional>
#include <memory>
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
  : key(std::move(key_value)),
    inclusive(is_inclusive)
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

namespace detail {

constexpr void normalize_keyspace(interval& x);

} // namespace detail

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
  detail::normalize_keyspace(*this);
 }

 constexpr interval(interval_bound lower, interval_bound upper)
  : begin_key(std::move(lower.key)),
    end_key(std::move(upper.key)),
    begin_inclusive(lower.inclusive),
    end_inclusive(upper.inclusive)
 {
  detail::normalize_keyspace(*this);
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
  detail::normalize_keyspace(*this);
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

};

template <typename T>
concept query_interval =
 std::same_as<std::remove_cvref_t<T>, interval>;

template <typename T>
concept byte_interval_expression =
 core::detail::plain_expression<T> &&
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

constexpr void normalize_keyspace(interval& x)
{
 if (byte_string_domain::is_reserved_key(x.end_key)) {
  x.end_key = byte_string_domain::keyspace_limit();
  x.end_inclusive = false;
 }
}

// Clamp an executable selector to ordinary FDB keyspace:
constexpr interval clamp_to_keyspace(interval&& x)
{
 normalize_keyspace(x);
 return std::move(x);
}

constexpr bool is_empty_select(const interval& x) noexcept
{
 const auto reserved = byte_string_domain::is_reserved_key(x.end_key);
 const auto upper = reserved ? byte_string_domain::keyspace_limit_view() : std::string_view(x.end_key);
 if (const auto order = byte_string_domain::compare(x.begin_key, upper); std::strong_ordering::equal != order) {
  return std::strong_ordering::greater == order;
 }

 return not (x.begin_inclusive and x.end_inclusive and not reserved);
}

template <core::boundary_view_for<byte_string_domain> LowerT,
          core::boundary_view_for<byte_string_domain> UpperT>
constexpr interval to_unchecked_select(const LowerT& lower,
                                       const UpperT& upper,
                                       const query_options& options)
{
 // Core infinity is algebraic; libfdb lowers it to ordinary FDB keyspace.
 auto out = ::ceph::libfdb::query::detail_make_interval(
  lower.finite() ? std::string(lower.finite_key()) : std::string(),
  upper.finite() ? std::string(upper.finite_key()) : byte_string_domain::keyspace_limit(),
  core::boundary_kind::negative_infinity == lower.kind() || lower.inclusive(),
  upper.finite() && upper.inclusive());
 out.options = options;

 return out;
}

template <core::boundary_view_for<byte_string_domain> LowerT,
          core::boundary_view_for<byte_string_domain> UpperT>
constexpr interval to_select(const LowerT& lower,
                             const UpperT& upper,
                             const query_options& options)
{
 return clamp_to_keyspace(to_unchecked_select(lower, upper, options));
}

constexpr interval to_select(interval&& x, const query_options& options)
{
 if (is_empty_select(x)) {
  return to_select(boundary::closed(""), boundary::open(""), options);
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

template <byte_interval_expression LHS_T, byte_interval_expression RHS_T>
struct is_keyspace_bounded<core::detail::symmetric_difference_expr<LHS_T, RHS_T>>
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
  if (auto selection = to_select(lower, upper, options); !is_empty_select(selection)) {
   std::invoke(sink, std::move(selection));
  }
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

constexpr bool is_empty(const interval& x) noexcept
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

// Modify one concrete interval. Prepare replacements before changing lhs, including
// when rhs aliases lhs; an rvalue rhs can donate its tighter endpoint strings.
template <query_interval RhsT>
constexpr interval& assign_intersection(interval& lhs, RhsT&& rhs)
{
 const auto options = detail::combined_options(lhs, rhs);
 const auto lhs_bounds = core::detail::bounds_of(lhs);
 const auto rhs_bounds = core::detail::bounds_of(rhs);
 const auto bounds = core::detail::intersection_bounds(lhs_bounds, rhs_bounds);

 if (!bounds) {
  lhs.begin_key.clear();
  lhs.end_key.clear();
  lhs.begin_inclusive = true;
  lhs.end_inclusive = false;
  lhs.options = options;
  return lhs;
 }

 const auto& [lower, upper] = *bounds;
 const bool includes_begin = lower.inclusive();
 const bool reserved_end = byte_string_domain::is_reserved_key(upper.finite_key());
 const bool includes_end = !reserved_end && upper.inclusive();
 std::optional<std::string> next_begin;
 std::optional<std::string> next_end;

 if (std::addressof(lower.finite_key()) != std::addressof(lhs.begin_key)) {
  next_begin.emplace(std::forward<RhsT>(rhs).begin_key);
 }
 if (reserved_end && lhs.end_key != byte_string_domain::keyspace_limit_view()) {
  next_end.emplace(byte_string_domain::keyspace_limit());
 }
 if (!reserved_end && std::addressof(upper.finite_key()) != std::addressof(lhs.end_key)) {
  next_end.emplace(std::forward<RhsT>(rhs).end_key);
 }

 if (next_begin) {
  lhs.begin_key = std::move(*next_begin);
 }
 if (next_end) {
  lhs.end_key = std::move(*next_end);
 }
 lhs.begin_inclusive = includes_begin;
 lhs.end_inclusive = includes_end;
 lhs.options = options;
 return lhs;
}

template <query_interval RhsT>
constexpr interval& operator&=(interval& lhs, RhsT&& rhs)
{
 return assign_intersection(lhs, std::forward<RhsT>(rhs));
}

template <query_interval RhsT>
constexpr interval intersection(interval lhs, RhsT&& rhs)
{
 assign_intersection(lhs, std::forward<RhsT>(rhs));
 return lhs;
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

template <typename LHS_T, typename RHS_T>
requires expression<LHS_T> and expression<RHS_T>
constexpr auto symmetric_difference(LHS_T&& lhs, RHS_T&& rhs)
{
 const auto options = detail::combined_options(lhs, rhs);
 return detail::configure(
  core::symmetric_difference(detail::core_expression_of(std::forward<LHS_T>(lhs)),
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

namespace detail {

template <byte_interval_expression ExprT>
constexpr auto bounded_geometry(const ExprT& geometry)
{
 return core::detail::intersection_expr<const ExprT&, interval> {
  .lhs = geometry, .rhs = query::universal()};
}

} // namespace detail

// Unlike a single interval, this value can retain disconnected results. Geometry
// uses the core's canonical storage; one options value also survives empty sets.
struct interval_set final
{
 using domain_type = byte_string_domain;
 using interval_expression_type = core::interval_set<domain_type>;

 interval_expression_type expression;
 query_options options;

 public:
 constexpr interval_set() = default;
 constexpr interval_set(const interval_set&) = default;
 constexpr interval_set(interval_set&&) noexcept = default;

 template <query::expression ExprT>
 explicit constexpr interval_set(const ExprT& source)
  : options(detail::options_of(source))
 {
  expression = detail::bounded_geometry(detail::core_expression_of(source));
 }

 constexpr interval_set& operator=(interval_set other) noexcept
 {
  swap(other);
  return *this;
 }

 template <query::expression ExprT>
 constexpr interval_set& operator=(const ExprT& source)
 {
  interval_set next {source};
  swap(next);
  return *this;
 }

 constexpr void swap(interval_set& other) noexcept
 {
  expression.swap(other.expression);
  std::swap(options, other.options);
 }
};

constexpr interval_set with_options(interval_set x, const query_options& options)
{
 x.options = options;
 return x;
}

namespace detail {

template <byte_interval_expression ExprT>
constexpr interval_set& assign_geometry(interval_set& target, const ExprT& geometry,
                                        const query_options& options)
{
 static_assert(std::is_nothrow_copy_assignable_v<query_options>);
 target.expression = bounded_geometry(geometry);
 target.options = options;
 return target;
}

// Pure-core operands retain their algebraic universe. An FDB operand selects
// the FDB overload, whose stronger shared core constraint resolves mixed ADL.
template <typename T>
concept fdb_operand = query_interval<T> or configured_expression<T>;

template <typename LhsT, typename RhsT>
concept fdb_operands =
 core::same_expression_domain<LhsT, RhsT> and expression<LhsT> and expression<RhsT> and
 (fdb_operand<LhsT> or fdb_operand<RhsT>);

} // namespace detail

template <expression RhsT>
constexpr interval_set& assign_intersection(interval_set& lhs, const RhsT& rhs)
{
 const auto options = detail::combined_options(lhs, rhs);
 const auto& geometry = detail::core_expression_of(rhs);
 return detail::assign_geometry(lhs,
  core::detail::intersection_expr<const interval_set::interval_expression_type&, decltype(geometry)> {
   .lhs = lhs.expression, .rhs = geometry}, options);
}

template <expression RhsT>
constexpr interval_set& assign_set_union(interval_set& lhs, const RhsT& rhs)
{
 const auto options = detail::combined_options(lhs, rhs);
 const auto& geometry = detail::core_expression_of(rhs);
 return detail::assign_geometry(lhs,
  core::detail::set_union_expr<const interval_set::interval_expression_type&, decltype(geometry)> {
   .lhs = lhs.expression, .rhs = geometry}, options);
}

template <expression RhsT>
constexpr interval_set& assign_difference(interval_set& lhs, const RhsT& rhs)
{
 const auto& geometry = detail::core_expression_of(rhs);
 return detail::assign_geometry(lhs,
  core::detail::difference_expr<const interval_set::interval_expression_type&, decltype(geometry)> {
   .lhs = lhs.expression, .rhs = geometry}, lhs.options);
}

template <expression RhsT>
constexpr interval_set& assign_symmetric_difference(interval_set& lhs, const RhsT& rhs)
{
 const auto options = detail::combined_options(lhs, rhs);
 const auto& geometry = detail::core_expression_of(rhs);
 return detail::assign_geometry(lhs,
  core::detail::symmetric_difference_expr<const interval_set::interval_expression_type&, decltype(geometry)> {
   .lhs = lhs.expression, .rhs = geometry}, options);
}

constexpr interval_set& assign_complement(interval_set& x)
{
 const auto universe = query::universal();
 return detail::assign_geometry(x,
  core::detail::difference_expr<const interval&, const interval_set::interval_expression_type&> {
   .lhs = universe, .rhs = x.expression}, x.options);
}

template <typename LhsT, typename RhsT>
requires detail::fdb_operands<LhsT, RhsT>
constexpr auto operator&(LhsT&& lhs, RhsT&& rhs)
{
 return query::intersection(std::forward<LhsT>(lhs), std::forward<RhsT>(rhs));
}

template <typename LhsT, typename RhsT>
requires detail::fdb_operands<LhsT, RhsT>
constexpr auto operator|(LhsT&& lhs, RhsT&& rhs)
{
 return query::set_union(std::forward<LhsT>(lhs), std::forward<RhsT>(rhs));
}

template <typename LhsT, typename RhsT>
requires detail::fdb_operands<LhsT, RhsT>
constexpr auto operator-(LhsT&& lhs, RhsT&& rhs)
{
 return query::difference(std::forward<LhsT>(lhs), std::forward<RhsT>(rhs));
}

template <typename LhsT, typename RhsT>
requires detail::fdb_operands<LhsT, RhsT>
constexpr auto operator^(LhsT&& lhs, RhsT&& rhs)
{
 return query::symmetric_difference(std::forward<LhsT>(lhs), std::forward<RhsT>(rhs));
}

template <core::expression ExprT>
requires expression<ExprT> and detail::fdb_operand<ExprT>
constexpr auto operator~(ExprT&& x)
{
 return query::complement(std::forward<ExprT>(x));
}

template <expression RhsT>
constexpr interval_set& operator&=(interval_set& lhs, const RhsT& rhs)
{
 return query::assign_intersection(lhs, rhs);
}

template <expression RhsT>
constexpr interval_set& operator|=(interval_set& lhs, const RhsT& rhs)
{
 return query::assign_set_union(lhs, rhs);
}

template <expression RhsT>
constexpr interval_set& operator-=(interval_set& lhs, const RhsT& rhs)
{
 return query::assign_difference(lhs, rhs);
}

template <expression RhsT>
constexpr interval_set& operator^=(interval_set& lhs, const RhsT& rhs)
{
 return query::assign_symmetric_difference(lhs, rhs);
}

template <typename SinkT>
constexpr void for_each_interval(interval selection, SinkT&& sink)
{
 detail::normalize_keyspace(selection);

 if (detail::is_empty_select(selection)) {
  return;
 }

 std::invoke(std::forward<SinkT>(sink), std::move(selection));
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

 query::for_each_interval(expr, [&out](interval x) {
  out.push_back(std::move(x));
 });

 return out;
}

template <std::size_t Capacity, expression ExprT>
constexpr auto compile_intervals(const ExprT& expr)
{
 core::static_interval_set<byte_string_domain, Capacity, interval> out {empty()};

 query::for_each_interval(expr, [&out](interval x) {
  try {
   out.push_back(std::move(x));
  } catch (const std::length_error& error) {
   throw libfdb_exception(std::errc::value_too_large, error.what());
  }
 });

 return out;
}

template <expression ExprT>
inline constexpr std::size_t interval_capacity_v =
 core::interval_capacity_v<decltype(detail::core_expression_of(std::declval<const ExprT&>()))>;

template <expression ExprT>
requires (std::dynamic_extent != interval_capacity_v<ExprT>)
constexpr auto compile_intervals(std::in_place_t, const ExprT& expr)
{
 return query::compile_intervals<interval_capacity_v<ExprT>>(expr);
}

template <expression ExprT>
constexpr std::size_t interval_count(const ExprT& expr)
{
 if constexpr (query_interval<ExprT>) {
  return query::is_empty(expr) ? 0 : 1;
 }

 std::size_t count = 0;

 query::for_each_interval(expr, [&count](const interval&) { ++count; });

 return count;
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

 return 0 == query::interval_count(expr);
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

 const auto& left = detail::core_expression_of(lhs);
 const auto& right = detail::core_expression_of(rhs);
 return not query::is_empty_expression(
  core::detail::intersection_expr<decltype(left), decltype(right)> {.lhs = left, .rhs = right});
}

template <expression LHS_T, expression RHS_T>
constexpr bool is_disjoint(const LHS_T& lhs, const RHS_T& rhs)
{
 return not query::intersects(lhs, rhs);
}

template <expression LHS_T, expression RHS_T>
constexpr bool encloses(const LHS_T& outer, const RHS_T& inner)
{
 if constexpr (detail::is_keyspace_bounded_v<LHS_T> &&
               detail::is_keyspace_bounded_v<RHS_T>) {
  return core::encloses(detail::core_expression_of(outer), detail::core_expression_of(inner));
 }

 const auto& inside = detail::core_expression_of(inner);
 const auto& outside = detail::core_expression_of(outer);
 return query::is_empty_expression(
  core::detail::difference_expr<decltype(inside), decltype(outside)> {.lhs = inside, .rhs = outside});
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
 return query::intersection(std::forward<ExprT>(expr),
   at_or_after(std::forward<decltype(key)>(key)));
}

template <expression ExprT>
constexpr auto starting_after(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return query::intersection(std::forward<ExprT>(expr),
   after(std::forward<decltype(key)>(key)));
}

template <expression ExprT>
constexpr auto ending_at(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return query::intersection(std::forward<ExprT>(expr),
   at_or_before(std::forward<decltype(key)>(key)));
}

template <expression ExprT>
constexpr auto ending_before(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return query::intersection(std::forward<ExprT>(expr),
   before(std::forward<decltype(key)>(key)));
}

template <expression ExprT>
constexpr auto without(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return query::difference(std::forward<ExprT>(expr),
   at(std::forward<decltype(key)>(key)));
}

template <expression ExprT>
constexpr auto without_prefix(ExprT&& expr, concepts::libfdb_key auto&& key)
{
 return query::difference(std::forward<ExprT>(expr),
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
