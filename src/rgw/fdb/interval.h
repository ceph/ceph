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

#ifndef CEPH_FDB_INTERVAL_H
#define CEPH_FDB_INTERVAL_H

#include "common/container_concepts.h"

#include <array>
#include <vector>
#include <optional>

#include <ranges>
#include <iterator>
#include <algorithm>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <utility>
#include <compare>
#include <concepts>
#include <stdexcept>
#include <functional>
#include <type_traits>

// Defines a pure interval algebra used to wrangle ranges over ordered domains; the
// query layer supplies the FoundationDB-specific key domain:
namespace ceph::libfdb::interval {

template <typename DomainT>
concept ordered_domain =
 requires(const typename DomainT::value_type& lhs,
          const typename DomainT::value_type& rhs) {
  typename DomainT::value_type;
  requires std::copy_constructible<typename DomainT::value_type>;
  { DomainT::compare(lhs, rhs) } -> std::same_as<std::strong_ordering>;
 };

template <typename DomainT>
concept successor_domain =
 ordered_domain<DomainT> &&
 requires(const typename DomainT::value_type& value) {
  { DomainT::successor(value) } -> std::same_as<std::optional<typename DomainT::value_type>>;
 };

namespace detail {

struct expression_tag {};

template <typename DomainT, typename ValueT>
concept comparable_value =
 ordered_domain<DomainT> &&
 requires(const typename DomainT::value_type& key, const ValueT& value) {
  { DomainT::compare(key, value) } -> std::same_as<std::strong_ordering>;
  { DomainT::compare(value, key) } -> std::same_as<std::strong_ordering>;
 };

} // namespace detail

enum struct boundary_kind : std::uint8_t
{
 negative_infinity,
 finite,
 positive_infinity
};

template <ordered_domain DomainT>
class boundary;

namespace detail {

struct boundary_view_ops
{
 constexpr bool operator==(const boundary_view_ops&) const = default;

 constexpr bool finite(this const auto& self) noexcept
 {
  return boundary_kind::finite == self.kind();
 }

 template <typename SelfT, typename ValueT>
 requires comparable_value<typename std::remove_cvref_t<SelfT>::domain_type, ValueT>
 constexpr bool allows_after(this const SelfT& self, const ValueT& value)
 {
  using domain_type = typename std::remove_cvref_t<SelfT>::domain_type;

  if (boundary_kind::negative_infinity == self.kind()) {
   return true;
  }

  if (boundary_kind::positive_infinity == self.kind()) {
   return false;
  }

  const auto order = domain_type::compare(self.finite_key(), value);
  return std::strong_ordering::less == order ||
         (std::strong_ordering::equal == order && self.inclusive());
 }

 template <typename SelfT, typename ValueT>
 requires comparable_value<typename std::remove_cvref_t<SelfT>::domain_type, ValueT>
 constexpr bool allows_before(this const SelfT& self, const ValueT& value)
 {
  using domain_type = typename std::remove_cvref_t<SelfT>::domain_type;

  if (boundary_kind::positive_infinity == self.kind()) {
   return true;
  }

  if (boundary_kind::negative_infinity == self.kind()) {
   return false;
  }

  const auto order = domain_type::compare(value, self.finite_key());
  return std::strong_ordering::less == order ||
         (std::strong_ordering::equal == order && self.inclusive());
 }

 template <typename SelfT>
 constexpr auto with_inclusive(this const SelfT& self, const bool inclusive)
 {
  using domain_type = typename std::remove_cvref_t<SelfT>::domain_type;

  if (boundary_kind::negative_infinity == self.kind()) {
   return boundary<domain_type>::negative_infinity();
  }

  if (boundary_kind::positive_infinity == self.kind()) {
   return boundary<domain_type>::positive_infinity();
  }

  if (inclusive) {
   return boundary<domain_type>::closed(self.finite_key());
  }

  return boundary<domain_type>::open(self.finite_key());
 }
};

} // namespace detail

template <ordered_domain DomainT>
class boundary final : public detail::boundary_view_ops
{
 public:
 using domain_type = DomainT;
 using value_type = typename DomainT::value_type;

 private:
 boundary_kind bound_kind = boundary_kind::finite;
 std::optional<value_type> key;
 bool includes_key = false;

 public:
 static constexpr boundary negative_infinity() noexcept
 {
  return boundary(boundary_kind::negative_infinity, std::nullopt, false);
 }

 static constexpr boundary positive_infinity() noexcept
 {
  return boundary(boundary_kind::positive_infinity, std::nullopt, false);
 }

 static constexpr boundary closed(value_type key)
 {
  return boundary(boundary_kind::finite, std::move(key), true);
 }

 static constexpr boundary open(value_type key)
 {
  return boundary(boundary_kind::finite, std::move(key), false);
 }

 constexpr boundary_kind kind() const noexcept
 {
  return bound_kind;
 }

 constexpr const value_type& finite_key() const noexcept
 {
  return *key;
 }

 constexpr bool inclusive() const noexcept
 {
  return includes_key;
 }

 constexpr bool operator==(const boundary&) const = default;

 private:
 constexpr boundary(const boundary_kind kind,
                    std::optional<value_type> key_value,
                    const bool inclusive)
  : bound_kind(kind),
    key(std::move(key_value)),
    includes_key(inclusive)
 {}
};

template <ordered_domain DomainT>
class boundary_ref final : public detail::boundary_view_ops
{
 public:
 using domain_type = DomainT;
 using value_type = typename DomainT::value_type;

 private:
 boundary_kind bound_kind = boundary_kind::finite;

 // Infinities cannot be keys, so the non-owning pointer is NULL for them.
 const value_type *key = nullptr;

 bool includes_key = false;

 public:
 static constexpr boundary_ref negative_infinity() noexcept
 {
  return boundary_ref(boundary_kind::negative_infinity, nullptr, false);
 }

 static constexpr boundary_ref positive_infinity() noexcept
 {
  return boundary_ref(boundary_kind::positive_infinity, nullptr, false);
 }

 static constexpr boundary_ref closed(const value_type& key) noexcept
 {
  return boundary_ref(boundary_kind::finite, &key, true);
 }

 static constexpr boundary_ref open(const value_type& key) noexcept
 {
  return boundary_ref(boundary_kind::finite, &key, false);
 }

 static constexpr boundary_ref closed(value_type&&) = delete;
 static constexpr boundary_ref open(value_type&&) = delete;

 constexpr boundary_kind kind() const noexcept
 {
  return bound_kind;
 }

 constexpr const value_type& finite_key() const noexcept
 {
  return *key;
 }

 constexpr bool inclusive() const noexcept
 {
  return includes_key;
 }

 private:
 constexpr boundary_ref(const boundary_kind kind,
                        const value_type *key_value,
                        const bool inclusive) noexcept
  : bound_kind(kind),
    key(key_value),
    includes_key(boundary_kind::finite == kind and inclusive)
 {}
};

template <typename BoundT, typename DomainT>
concept boundary_view_for =
 requires(const std::remove_cvref_t<BoundT>& bound) {
  typename std::remove_cvref_t<BoundT>::domain_type;
  requires std::same_as<typename std::remove_cvref_t<BoundT>::domain_type, DomainT>;
  { bound.kind() } -> std::same_as<boundary_kind>;
  { bound.finite() } -> std::same_as<bool>;
  { bound.finite_key() } -> std::same_as<const typename DomainT::value_type&>;
  { bound.inclusive() } -> std::same_as<bool>;
 };

template <typename BoundT>
concept boundary_view =
 requires {
  typename std::remove_cvref_t<BoundT>::domain_type;
  requires ordered_domain<typename std::remove_cvref_t<BoundT>::domain_type>;
  requires boundary_view_for<BoundT, typename std::remove_cvref_t<BoundT>::domain_type>;
 };

template <boundary_view BoundT>
using boundary_domain_t = typename std::remove_cvref_t<BoundT>::domain_type;

template <boundary_view BoundT>
constexpr auto materialize_boundary(const BoundT& bound, const bool inclusive)
{
 using domain_type = boundary_domain_t<BoundT>;

 if (boundary_kind::negative_infinity == bound.kind()) {
  return boundary<domain_type>::negative_infinity();
 }

 if (boundary_kind::positive_infinity == bound.kind()) {
  return boundary<domain_type>::positive_infinity();
 }

 if (inclusive) {
  return boundary<domain_type>::closed(bound.finite_key());
 }

 return boundary<domain_type>::open(bound.finite_key());
}

template <boundary_view BoundT>
constexpr auto materialize_boundary(const BoundT& bound)
{
 return materialize_boundary(bound, bound.inclusive());
}

namespace detail {

// Geometry borrows keys; only an owning result materializes them.
template <boundary_view BoundT>
constexpr auto reference_boundary(BoundT& bound, const bool inclusive)
{
 using reference_type = boundary_ref<boundary_domain_t<BoundT>>;

 if (boundary_kind::negative_infinity == bound.kind()) {
  return reference_type::negative_infinity();
 }
 if (boundary_kind::positive_infinity == bound.kind()) {
  return reference_type::positive_infinity();
 }
 return inclusive ? reference_type::closed(bound.finite_key())
                  : reference_type::open(bound.finite_key());
}

template <boundary_view BoundT>
constexpr auto reference_boundary(BoundT& bound)
{
 return reference_boundary(bound, bound.inclusive());
}

} // namespace detail

template <ordered_domain DomainT>
constexpr auto closed_bound(typename DomainT::value_type key)
{
 return boundary<DomainT>::closed(std::move(key));
}

template <ordered_domain DomainT>
constexpr auto open_bound(typename DomainT::value_type key)
{
 return boundary<DomainT>::open(std::move(key));
}

template <ordered_domain DomainT>
class query final : public detail::expression_tag
{
 public:
 using domain_type = DomainT;
 using value_type = typename DomainT::value_type;
 using boundary_type = boundary<DomainT>;

 private:
 boundary_type begin_bound = boundary_type::negative_infinity();
 boundary_type end_bound = boundary_type::positive_infinity();
 bool is_empty = false;

 public:
 constexpr query() = default;

 constexpr query(boundary_type lower, boundary_type upper)
  : begin_bound(std::move(lower)),
    end_bound(std::move(upper))
 {}

 static constexpr query empty() noexcept
 {
  query out;
  out.is_empty = true;

  return out;
 }

 static constexpr query universal() noexcept
 {
  return query {};
 }

 static constexpr query singleton(value_type key)
 {
  auto upper_key = key;

  return query(boundary_type::closed(std::move(key)),
               boundary_type::closed(std::move(upper_key)));
 }

 static constexpr query closed(value_type begin_key, value_type end_key)
 {
  return query(boundary_type::closed(std::move(begin_key)),
               boundary_type::closed(std::move(end_key)));
 }

 static constexpr query open(value_type begin_key, value_type end_key)
 {
  return query(boundary_type::open(std::move(begin_key)),
               boundary_type::open(std::move(end_key)));
 }

 static constexpr query closed_open(value_type begin_key, value_type end_key)
 {
  return query(boundary_type::closed(std::move(begin_key)),
               boundary_type::open(std::move(end_key)));
 }

 static constexpr query between(boundary_type lower, boundary_type upper)
 {
  return query(std::move(lower), std::move(upper));
 }

 template <boundary_view_for<DomainT> LowerT,
           boundary_view_for<DomainT> UpperT>
 static constexpr query between(const LowerT& lower, const UpperT& upper)
 {
  return query(materialize_boundary(lower),
               materialize_boundary(upper));
 }

 static constexpr query prefix(value_type prefix)
 requires successor_domain<DomainT>
 {
  if (auto successor = DomainT::successor(prefix)) {
   return closed_open(std::move(prefix), std::move(*successor));
  }

  return query(boundary_type::closed(std::move(prefix)),
               boundary_type::positive_infinity());
 }

 constexpr const boundary_type& lower() const noexcept
 {
  return begin_bound;
 }

 constexpr const boundary_type& upper() const noexcept
 {
  return end_bound;
 }

 constexpr bool explicitly_empty() const noexcept
 {
  return is_empty;
 }

 constexpr bool operator==(const query& rhs) const
 {
  return begin_bound == rhs.begin_bound and
         end_bound == rhs.end_bound and
         is_empty == rhs.is_empty;
 }
};

// Adapters may retain their interval representation, including execution metadata.
template <ordered_domain DomainT, std::size_t Capacity, typename IntervalT = query<DomainT>>
requires std::same_as<typename IntervalT::domain_type, DomainT>
class static_interval_set final
{
 public:
 using value_type = IntervalT;
 using iterator = typename std::array<value_type, Capacity>::iterator;
 using const_iterator = typename std::array<value_type, Capacity>::const_iterator;

 private:
 std::array<value_type, Capacity> intervals {};
 std::size_t interval_count = 0;

 template <std::size_t ...Indices>
 constexpr static_interval_set(const value_type& initial, std::index_sequence<Indices...>)
  : intervals {((void)Indices, initial)...}
 {}

 public:
 constexpr static_interval_set() = default;

 // Initialize unused slots without requiring an adapter's default interval.
 explicit constexpr static_interval_set(const value_type& initial)
  : static_interval_set(initial, std::make_index_sequence<Capacity> {})
 {}

 constexpr void push_back(value_type value)
 {
  if (Capacity == interval_count) {
   throw std::length_error { "static interval set capacity exceeded" };
  }

  intervals[interval_count++] = std::move(value);
 }

 constexpr bool empty() const noexcept
 {
  return 0 == interval_count;
 }

 constexpr std::size_t size() const noexcept
 {
  return interval_count;
 }

 constexpr const value_type& operator[](const std::size_t index) const noexcept
 {
  return intervals[index];
 }

 constexpr const_iterator begin() const noexcept
 {
  return std::begin(intervals);
 }

 constexpr iterator begin() noexcept
 {
  return std::begin(intervals);
 }

 constexpr iterator end() noexcept
 {
  return std::next(std::begin(intervals), static_cast<std::ptrdiff_t>(interval_count));
 }

 constexpr const_iterator end() const noexcept
 {
  return std::next(std::begin(intervals),
                   static_cast<std::ptrdiff_t>(interval_count));
 }

 constexpr bool operator==(const static_interval_set& rhs) const
 {
  return std::ranges::equal(*this, rhs);
 }

};

namespace detail {

template <typename BoundT>
using boundary_storage =
 std::conditional_t<std::is_lvalue_reference_v<BoundT>,
                    BoundT,
                    std::remove_cvref_t<BoundT>>;

template <typename T>
constexpr bool explicitly_empty(const T& x)
{
 if constexpr (requires { { x.explicitly_empty() } -> std::same_as<bool>; }) {
  return x.explicitly_empty();
 }

 return false;
}

template <typename IntervalT>
struct interval_bounds final
{
 using domain_type = typename std::remove_cvref_t<IntervalT>::domain_type;
 using lower_result = decltype(std::declval<const IntervalT&>().lower());
 using upper_result = decltype(std::declval<const IntervalT&>().upper());

 private:
 boundary_storage<lower_result> lower_bound;
 boundary_storage<upper_result> upper_bound;

 public:
 constexpr explicit interval_bounds(const IntervalT& interval)
  : lower_bound(interval.lower()),
    upper_bound(interval.upper())
 {}

 constexpr const auto& lower() const noexcept
 {
  return lower_bound;
 }

 constexpr const auto& upper() const noexcept
 {
  return upper_bound;
 }
};

template <typename IntervalT>
constexpr auto bounds_of(const IntervalT& x)
{
 return interval_bounds<IntervalT> { x };
}

} // namespace detail

template <typename T>
concept interval_view =
 requires(const std::remove_cvref_t<T>& x) {
  typename std::remove_cvref_t<T>::domain_type;
  requires ordered_domain<typename std::remove_cvref_t<T>::domain_type>;
  { x.lower() } ->
   boundary_view_for<typename std::remove_cvref_t<T>::domain_type>;
  { x.upper() } ->
   boundary_view_for<typename std::remove_cvref_t<T>::domain_type>;
 };

template <typename T>
concept canonical_interval =
 interval_view<T> &&
 std::same_as<std::remove_cvref_t<T>,
              query<typename std::remove_cvref_t<T>::domain_type>>;

template <typename T>
concept expression =
 requires {
  typename std::remove_cvref_t<T>::domain_type;
  requires ordered_domain<typename std::remove_cvref_t<T>::domain_type>;
 } &&
 (std::derived_from<std::remove_cvref_t<T>, detail::expression_tag> ||
  interval_view<T>);

template <typename LHS_T, typename RHS_T>
concept same_expression_domain =
 expression<LHS_T> &&
 expression<RHS_T> &&
 std::same_as<typename std::remove_cvref_t<LHS_T>::domain_type,
              typename std::remove_cvref_t<RHS_T>::domain_type>;

namespace detail {

template <expression LHS_T, expression RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
struct difference_expr final : expression_tag
{
 using domain_type = typename std::remove_cvref_t<LHS_T>::domain_type;

 LHS_T lhs;
 RHS_T rhs;
};

template <expression LHS_T, expression RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
struct intersection_expr final : expression_tag
{
 using domain_type = typename std::remove_cvref_t<LHS_T>::domain_type;

 LHS_T lhs;
 RHS_T rhs;
};

template <expression LHS_T, expression RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
struct set_union_expr final : expression_tag
{
 using domain_type = typename std::remove_cvref_t<LHS_T>::domain_type;

 LHS_T lhs;
 RHS_T rhs;
};

} // namespace detail

// A conservative bound comes from the expression's shape, without inspecting keys.
template <typename ExprT>
struct interval_capacity : std::integral_constant<std::size_t, std::dynamic_extent> {};

template <interval_view IntervalT>
struct interval_capacity<IntervalT> : std::integral_constant<std::size_t, 1> {};

template <typename ExprT>
inline constexpr std::size_t interval_capacity_v = interval_capacity<std::remove_cvref_t<ExprT>>::value;

namespace detail {

template <typename LHS_T, typename RHS_T>
consteval std::size_t combined_interval_capacity()
{
 constexpr auto lhs = interval_capacity_v<LHS_T>;
 constexpr auto rhs = interval_capacity_v<RHS_T>;
 if (std::dynamic_extent == lhs || std::dynamic_extent == rhs ||
     std::numeric_limits<std::size_t>::max() - lhs <= rhs) {
  return std::dynamic_extent;
 }
 return lhs + rhs;
}

} // namespace detail

template <expression LHS_T, expression RHS_T>
struct interval_capacity<detail::difference_expr<LHS_T, RHS_T>>
 : std::integral_constant<std::size_t, detail::combined_interval_capacity<LHS_T, RHS_T>()>
{};

template <expression LHS_T, expression RHS_T>
struct interval_capacity<detail::intersection_expr<LHS_T, RHS_T>>
 : std::integral_constant<std::size_t, detail::combined_interval_capacity<LHS_T, RHS_T>()>
{};

template <expression LHS_T, expression RHS_T>
struct interval_capacity<detail::set_union_expr<LHS_T, RHS_T>>
 : std::integral_constant<std::size_t, detail::combined_interval_capacity<LHS_T, RHS_T>()>
{};

template <ordered_domain DomainT, typename SinkT>
constexpr void for_each_interval(const query<DomainT>& x, SinkT&& sink);

template <interval_view IntervalT, typename SinkT>
requires (!canonical_interval<IntervalT>)
constexpr void for_each_interval(const IntervalT& x, SinkT&& sink);

template <expression LHS_T, expression RHS_T, typename SinkT>
constexpr void for_each_interval(const detail::difference_expr<LHS_T, RHS_T>& expression,
                                 SinkT&& sink);

template <expression LHS_T, expression RHS_T, typename SinkT>
constexpr void for_each_interval(const detail::intersection_expr<LHS_T, RHS_T>& expression,
                                 SinkT&& sink);

template <expression LHS_T, expression RHS_T, typename SinkT>
constexpr void for_each_interval(const detail::set_union_expr<LHS_T, RHS_T>& expression,
                                 SinkT&& sink);

namespace detail {

enum struct inclusivity_merge : std::uint8_t
{
 all,
 any
};

constexpr bool merge_inclusive(const bool lhs,
                               const bool rhs,
                               const inclusivity_merge merge) noexcept
{
 return inclusivity_merge::all == merge ? lhs && rhs : lhs || rhs;
}

template <boundary_view LHS_T, boundary_view RHS_T>
requires std::same_as<boundary_domain_t<LHS_T>, boundary_domain_t<RHS_T>>
constexpr std::strong_ordering compare_position(const LHS_T& lhs, const RHS_T& rhs)
{
 if (lhs.kind() != rhs.kind()) {
  return lhs.kind() <=> rhs.kind();
 }

 if (!lhs.finite()) {
  return std::strong_ordering::equal;
 }

 return boundary_domain_t<LHS_T>::compare(lhs.finite_key(), rhs.finite_key());
}

template <boundary_view LowerT, boundary_view UpperT>
requires std::same_as<boundary_domain_t<LowerT>, boundary_domain_t<UpperT>>
constexpr bool empty_bounds(const LowerT& lower, const UpperT& upper)
{
 const auto order = compare_position(lower, upper);

 if (std::strong_ordering::equal != order) {
  return std::strong_ordering::greater == order;
 }

 return !(lower.inclusive() && upper.inclusive());
}

template <typename BoundsT>
constexpr bool empty_bounds(const BoundsT& bounds)
{
 return empty_bounds(bounds.lower(), bounds.upper());
}

template <typename IntervalT, typename BoundsT>
constexpr bool empty_interval(const IntervalT& interval, const BoundsT& bounds)
{
 return explicitly_empty(interval) || empty_bounds(bounds);
}

template <typename SinkT, boundary_view LowerT, boundary_view UpperT>
requires std::same_as<boundary_domain_t<LowerT>, boundary_domain_t<UpperT>>
constexpr void emit_bounds(SinkT&& sink, const LowerT& lower, const UpperT& upper)
{
 using domain_type = boundary_domain_t<LowerT>;

 if (empty_bounds(lower, upper)) {
  return;
 }

 // Keep the else to discard the unsupported call during template instantiation;
 // an early return cannot do that, and separate guards repeat the condition:
 if constexpr (requires(SinkT&& s) { std::forward<SinkT>(s).emit_interval(lower, upper); }) {
  std::forward<SinkT>(sink).emit_interval(lower, upper);
 } else {
  std::invoke(std::forward<SinkT>(sink), query<domain_type>::between(lower, upper));
 }
}

template <boundary_view LHS_T, boundary_view RHS_T>
requires std::same_as<boundary_domain_t<LHS_T>, boundary_domain_t<RHS_T>>
constexpr auto min_bound(const LHS_T& lhs, const RHS_T& rhs, const inclusivity_merge merge)
{
 const auto order = compare_position(lhs, rhs);

 if (std::strong_ordering::less == order) {
  return reference_boundary(lhs);
 }

 if (std::strong_ordering::greater == order) {
  return reference_boundary(rhs);
 }

 return reference_boundary(lhs, merge_inclusive(lhs.inclusive(), rhs.inclusive(), merge));
}

template <boundary_view LHS_T, boundary_view RHS_T>
requires std::same_as<boundary_domain_t<LHS_T>, boundary_domain_t<RHS_T>>
constexpr auto max_bound(const LHS_T& lhs, const RHS_T& rhs, const inclusivity_merge merge)
{
 const auto order = compare_position(lhs, rhs);

 if (std::strong_ordering::less == order) {
  return reference_boundary(rhs);
 }

 if (std::strong_ordering::greater == order) {
  return reference_boundary(lhs);
 }

 return reference_boundary(lhs, merge_inclusive(lhs.inclusive(), rhs.inclusive(), merge));
}

} // namespace detail

template <typename ExpressionT>
using expression_domain_t = typename std::remove_cvref_t<ExpressionT>::domain_type;

template <typename ExpressionT>
using expression_value_t = typename expression_domain_t<ExpressionT>::value_type;

template <interval_view IntervalT>
constexpr bool is_empty(const IntervalT& x)
{
 return detail::empty_interval(x, detail::bounds_of(x));
}

template <interval_view IntervalT, typename ValueT>
requires detail::comparable_value<expression_domain_t<IntervalT>, ValueT>
constexpr bool contains(const IntervalT& x, const ValueT& value)
{
 const auto bounds = detail::bounds_of(x);

 if (detail::empty_interval(x, bounds)) {
  return false;
 }

 return bounds.lower().allows_after(value) &&
        bounds.upper().allows_before(value);
}

template <expression LHS_T, expression RHS_T, typename ValueT>
requires detail::comparable_value<expression_domain_t<LHS_T>, ValueT>
constexpr bool contains(const detail::difference_expr<LHS_T, RHS_T>& expression,
                        const ValueT& value);

template <expression LHS_T, expression RHS_T, typename ValueT>
requires detail::comparable_value<expression_domain_t<LHS_T>, ValueT>
constexpr bool contains(const detail::intersection_expr<LHS_T, RHS_T>& expression,
                        const ValueT& value);

template <expression LHS_T, expression RHS_T, typename ValueT>
requires detail::comparable_value<expression_domain_t<LHS_T>, ValueT>
constexpr bool contains(const detail::set_union_expr<LHS_T, RHS_T>& expression,
                        const ValueT& value);

template <expression ExprT, typename ValueT>
requires (!interval_view<ExprT>) &&
 detail::comparable_value<expression_domain_t<ExprT>, ValueT>
constexpr bool contains(const ExprT& expression, const ValueT& value)
{
 bool found = false;

 interval::for_each_interval(expression, [&found, &value](const auto& x) {
  found = found || interval::contains(x, value);
 });

 return found;
}

template <expression LHS_T, expression RHS_T, typename ValueT>
requires detail::comparable_value<expression_domain_t<LHS_T>, ValueT>
constexpr bool contains(const detail::difference_expr<LHS_T, RHS_T>& expression,
                        const ValueT& value)
{
 return interval::contains(expression.lhs, value) &&
        !interval::contains(expression.rhs, value);
}

template <expression LHS_T, expression RHS_T, typename ValueT>
requires detail::comparable_value<expression_domain_t<LHS_T>, ValueT>
constexpr bool contains(const detail::intersection_expr<LHS_T, RHS_T>& expression,
                        const ValueT& value)
{
 return interval::contains(expression.lhs, value) &&
        interval::contains(expression.rhs, value);
}

template <expression LHS_T, expression RHS_T, typename ValueT>
requires detail::comparable_value<expression_domain_t<LHS_T>, ValueT>
constexpr bool contains(const detail::set_union_expr<LHS_T, RHS_T>& expression,
                        const ValueT& value)
{
 return interval::contains(expression.lhs, value) ||
        interval::contains(expression.rhs, value);
}

template <interval_view LHS_T, interval_view RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr auto intersection(const LHS_T& lhs, const RHS_T& rhs)
{
 using domain_type = typename std::remove_cvref_t<LHS_T>::domain_type;
 const auto lhs_bounds = detail::bounds_of(lhs);
 const auto rhs_bounds = detail::bounds_of(rhs);

 if (detail::empty_interval(lhs, lhs_bounds) ||
     detail::empty_interval(rhs, rhs_bounds)) {
  return query<domain_type>::empty();
 }

 auto out = query<domain_type>::between(
  detail::max_bound(lhs_bounds.lower(), rhs_bounds.lower(), detail::inclusivity_merge::all),
  detail::min_bound(lhs_bounds.upper(), rhs_bounds.upper(), detail::inclusivity_merge::all));

 if (interval::is_empty(out)) {
  return query<domain_type>::empty();
 }

 return out;
}

namespace detail {

template <interval_view LHS_T, interval_view RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool definitely_before(const LHS_T& lhs, const RHS_T& rhs)
{
 const auto lhs_bounds = detail::bounds_of(lhs);
 const auto rhs_bounds = detail::bounds_of(rhs);
 const auto order = compare_position(lhs_bounds.upper(), rhs_bounds.lower());

 if (std::strong_ordering::equal != order) {
  return std::strong_ordering::less == order;
 }

 return !lhs_bounds.upper().inclusive() && !rhs_bounds.lower().inclusive();
}

template <interval_view LHS_T, interval_view RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool disjoint_before(const LHS_T& lhs, const RHS_T& rhs)
{
 const auto lhs_bounds = bounds_of(lhs);
 const auto rhs_bounds = bounds_of(rhs);
 const auto order = compare_position(lhs_bounds.upper(), rhs_bounds.lower());

 if (std::strong_ordering::equal != order) {
  return std::strong_ordering::less == order;
 }

 return !lhs_bounds.upper().inclusive() || !rhs_bounds.lower().inclusive();
}

template <interval_view LHS_T, interval_view RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool definitely_after(const LHS_T& lhs, const RHS_T& rhs)
{
 return definitely_before(rhs, lhs);
}

template <interval_view LHS_T, interval_view RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool can_coalesce(const LHS_T& lhs, const RHS_T& rhs)
{
 return !definitely_before(lhs, rhs) &&
        !definitely_after(lhs, rhs);
}

template <interval_view LHS_T, interval_view RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr auto coalesce(const LHS_T& lhs, const RHS_T& rhs)
{
 using domain_type = typename std::remove_cvref_t<LHS_T>::domain_type;
 const auto lhs_bounds = detail::bounds_of(lhs);
 const auto rhs_bounds = detail::bounds_of(rhs);
 const auto lhs_empty = detail::empty_interval(lhs, lhs_bounds);
 const auto rhs_empty = detail::empty_interval(rhs, rhs_bounds);

 if (lhs_empty && rhs_empty) {
  return query<domain_type>::empty();
 }

 if (lhs_empty) {
  return query<domain_type>::between(rhs_bounds.lower(), rhs_bounds.upper());
 }

 if (rhs_empty) {
  return query<domain_type>::between(lhs_bounds.lower(), lhs_bounds.upper());
 }

 return query<domain_type>::between(
  min_bound(lhs_bounds.lower(), rhs_bounds.lower(), inclusivity_merge::any),
  max_bound(lhs_bounds.upper(), rhs_bounds.upper(), inclusivity_merge::any));
}

template <boundary_view BoundT>
constexpr auto difference_end_before(const BoundT& removed_begin)
{
 return reference_boundary(removed_begin, !removed_begin.inclusive());
}

template <boundary_view BoundT>
constexpr auto difference_begin_after(const BoundT& removed_end)
{
 return reference_boundary(removed_end, !removed_end.inclusive());
}

template <std::ranges::random_access_range IntervalsT, typename SinkT>
requires (!std::is_lvalue_reference_v<IntervalsT>)
constexpr void emit_coalesced_intervals(IntervalsT&& intervals, SinkT&& sink);

template <interval_view LHS_T, interval_view RHS_T, typename SinkT>
requires same_expression_domain<LHS_T, RHS_T>
constexpr void emit_difference(const LHS_T& lhs, const RHS_T& rhs, SinkT&& sink)
{
 const auto lhs_bounds = bounds_of(lhs);
 const auto rhs_bounds = bounds_of(rhs);

 if (empty_interval(lhs, lhs_bounds)) {
  return;
 }

 if (empty_interval(rhs, rhs_bounds) ||
     definitely_before(rhs, lhs) ||
     definitely_after(rhs, lhs)) {
  interval::for_each_interval(lhs, std::forward<SinkT>(sink));
  return;
 }

 emit_bounds(sink, lhs_bounds.lower(), difference_end_before(rhs_bounds.lower()));
 emit_bounds(std::forward<SinkT>(sink),
             difference_begin_after(rhs_bounds.upper()), lhs_bounds.upper());
}

} // namespace detail

template <typename LHS_T, typename RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr auto difference(LHS_T&& lhs, RHS_T&& rhs)
{
 using lhs_type = std::remove_cvref_t<LHS_T>;
 using rhs_type = std::remove_cvref_t<RHS_T>;

 return detail::difference_expr<lhs_type, rhs_type> {
  .lhs = std::forward<LHS_T>(lhs),
  .rhs = std::forward<RHS_T>(rhs)
 };
}

template <typename LHS_T, typename RHS_T>
requires same_expression_domain<LHS_T, RHS_T> &&
        (!interval_view<LHS_T> || !interval_view<RHS_T>)
constexpr auto intersection(LHS_T&& lhs, RHS_T&& rhs)
{
 using lhs_type = std::remove_cvref_t<LHS_T>;
 using rhs_type = std::remove_cvref_t<RHS_T>;

 return detail::intersection_expr<lhs_type, rhs_type> {
  .lhs = std::forward<LHS_T>(lhs),
  .rhs = std::forward<RHS_T>(rhs)
 };
}

template <typename LHS_T, typename RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr auto set_union(LHS_T&& lhs, RHS_T&& rhs)
{
 using lhs_type = std::remove_cvref_t<LHS_T>;
 using rhs_type = std::remove_cvref_t<RHS_T>;

 return detail::set_union_expr<lhs_type, rhs_type> {
  .lhs = std::forward<LHS_T>(lhs),
  .rhs = std::forward<RHS_T>(rhs)
 };
}

template <expression ExprT>
constexpr auto complement(ExprT&& expression)
{
 using domain_type = typename std::remove_cvref_t<ExprT>::domain_type;

 return difference(query<domain_type>::universal(), std::forward<ExprT>(expression));
}

template <ordered_domain DomainT, typename SinkT>
constexpr void for_each_interval(const query<DomainT>& x, SinkT&& sink)
{
 if (interval::is_empty(x)) {
  return;
 }

 std::invoke(std::forward<SinkT>(sink), x);
}

template <interval_view IntervalT, typename SinkT>
requires (!canonical_interval<IntervalT>)
constexpr void for_each_interval(const IntervalT& x, SinkT&& sink)
{
 const auto bounds = detail::bounds_of(x);

 if (detail::empty_interval(x, bounds)) {
  return;
 }

 std::invoke(std::forward<SinkT>(sink), x);
}

namespace detail {

template <ordered_domain DomainT>
struct interval_ref final
{
 using domain_type = DomainT;
 boundary_ref<DomainT> lower_bound;
 boundary_ref<DomainT> upper_bound;

 constexpr const auto& lower() const& noexcept { return lower_bound; }
 constexpr const auto& upper() const& noexcept { return upper_bound; }
 const auto& lower() const&& = delete;
 const auto& upper() const&& = delete;
};

template <interval_view LHS_T, interval_view RHS_T, typename SinkT>
requires same_expression_domain<LHS_T, RHS_T>
constexpr void emit_intersection(const LHS_T& lhs, const RHS_T& rhs, SinkT&& sink)
{
 const auto lhs_bounds = bounds_of(lhs);
 const auto rhs_bounds = bounds_of(rhs);
 if (empty_interval(lhs, lhs_bounds) || empty_interval(rhs, rhs_bounds)) {
  return;
 }
 emit_bounds(std::forward<SinkT>(sink),
   max_bound(lhs_bounds.lower(), rhs_bounds.lower(), inclusivity_merge::all),
   min_bound(lhs_bounds.upper(), rhs_bounds.upper(), inclusivity_merge::all));
}

template <typename OutT, interval_view IntervalT>
constexpr void append_materialized_interval(OutT& out, const IntervalT& interval)
{
 using domain_type = typename std::remove_cvref_t<IntervalT>::domain_type;

 ceph::util::emplace_append(out, query<domain_type>::between(interval.lower(), interval.upper()));
}

// Bound stack bytes as well as cardinality: domain values need not be small.
inline constexpr std::size_t inline_interval_bytes = 4096;

template <expression ExprT>
inline constexpr bool uses_inline_intervals_v = interval_capacity_v<ExprT> <=
 inline_interval_bytes / sizeof(query<typename std::remove_cvref_t<ExprT>::domain_type>);

template <expression ExprT>
using interval_buffer = std::conditional_t<uses_inline_intervals_v<ExprT>,
 static_interval_set<typename std::remove_cvref_t<ExprT>::domain_type, interval_capacity_v<ExprT>>,
 std::vector<query<typename std::remove_cvref_t<ExprT>::domain_type>>>;

template <expression ExprT>
constexpr auto canonical_intervals(const ExprT& expression)
{
 if constexpr (interval_view<ExprT>) {
  return std::span {std::addressof(expression), interval::is_empty(expression)
    ? std::size_t {0} : std::size_t {1}};
 } else {
  interval_buffer<ExprT> out;
  interval::for_each_interval(expression, [&out](const auto& interval) {
   append_materialized_interval(out, interval);
  });
  return out;
 }
}

template <expression ExprT, typename SinkT>
constexpr void collect_union_intervals(const ExprT& expression, SinkT& sink)
{
 interval::for_each_interval(expression, sink);
}

template <expression LHS_T, expression RHS_T, typename SinkT>
constexpr void collect_union_intervals(const set_union_expr<LHS_T, RHS_T>& expression, SinkT& sink)
{
 collect_union_intervals(expression.lhs, sink);
 collect_union_intervals(expression.rhs, sink);
}

struct interval_counter final
{
 std::size_t count = 0;

 template <interval_view IntervalT>
 constexpr void operator()(const IntervalT&) { ++count; }

 template <boundary_view LowerT, boundary_view UpperT>
 constexpr void emit_interval(const LowerT&, const UpperT&) { ++count; }
};

} // namespace detail

template <expression LHS_T, expression RHS_T, typename SinkT>
constexpr void for_each_interval(const detail::difference_expr<LHS_T, RHS_T>& expression,
                                 SinkT&& sink)
{
 using domain_type = typename std::remove_cvref_t<LHS_T>::domain_type;

 if constexpr (interval_view<LHS_T> && interval_view<RHS_T>) {
  detail::emit_difference(expression.lhs, expression.rhs, std::forward<SinkT>(sink));
  return;
 }

 if constexpr (interval_view<RHS_T>) {
  if (interval::is_empty(expression.rhs)) {
   interval::for_each_interval(expression.lhs, std::forward<SinkT>(sink));
   return;
  }
 }

 std::optional<decltype(detail::canonical_intervals(expression.rhs))> rhs_intervals;

 interval::for_each_interval(expression.lhs, [&](const auto& lhs) {
  if (not rhs_intervals) {
   rhs_intervals.emplace(detail::canonical_intervals(expression.rhs));
  }
  if (std::empty(*rhs_intervals)) {
   interval::for_each_interval(lhs, sink);
   return;
  }
  const auto lhs_bounds = detail::bounds_of(lhs);
  auto cursor = materialize_boundary(lhs_bounds.lower());

  for (const auto& rhs : *rhs_intervals) {
   const detail::interval_ref<domain_type> remaining {
    detail::reference_boundary(cursor), detail::reference_boundary(lhs_bounds.upper())
   };
   if (interval::is_empty(remaining)) {
    return;
   }

   if (detail::definitely_before(rhs, remaining)) {
    continue;
   }

   if (detail::definitely_after(rhs, remaining)) {
    detail::emit_bounds(sink, cursor, lhs_bounds.upper());
    return;
   }

   const auto rhs_bounds = detail::bounds_of(rhs);
   const auto lower = detail::max_bound(cursor, rhs_bounds.lower(), detail::inclusivity_merge::all);
   const auto upper = detail::min_bound(lhs_bounds.upper(), rhs_bounds.upper(),
                                      detail::inclusivity_merge::all);
   if (detail::empty_bounds(lower, upper)) {
    continue;
   }

   detail::emit_bounds(sink, cursor, detail::difference_end_before(lower));
   cursor = materialize_boundary(detail::difference_begin_after(upper));
  }

  detail::emit_bounds(sink, cursor, lhs_bounds.upper());
 });
}

template <expression LHS_T, expression RHS_T, typename SinkT>
constexpr void for_each_interval(const detail::intersection_expr<LHS_T, RHS_T>& expression,
                                 SinkT&& sink)
{
 if constexpr (interval_view<RHS_T> && !canonical_interval<RHS_T>) {
  interval::for_each_interval(expression.rhs, [&sink, &expression](const auto& rhs) {
   interval::for_each_interval(expression.lhs, [&sink, &rhs](const auto& lhs) {
    detail::emit_intersection(lhs, rhs, sink);
   });
  });

  return;
 }

 if constexpr (interval_view<RHS_T>) {
  interval::for_each_interval(expression.lhs, [&sink, &expression](const auto& lhs) {
   detail::emit_intersection(lhs, expression.rhs, sink);
  });
  return;
 }

 if constexpr (interval_view<LHS_T> && !canonical_interval<LHS_T>) {
  interval::for_each_interval(expression.lhs, [&sink, &expression](const auto& lhs) {
   interval::for_each_interval(expression.rhs, [&sink, &lhs](const auto& rhs) {
    detail::emit_intersection(lhs, rhs, sink);
   });
  });
  return;
 }

 if constexpr (interval_view<LHS_T>) {
  interval::for_each_interval(expression.rhs, [&sink, &expression](const auto& rhs) {
   detail::emit_intersection(expression.lhs, rhs, sink);
  });
  return;
 }

 using domain_type = typename std::remove_cvref_t<LHS_T>::domain_type;
 std::vector<query<domain_type>> rhs_intervals;

 interval::for_each_interval(expression.rhs, [&rhs_intervals](const auto& rhs) {
  detail::append_materialized_interval(rhs_intervals, rhs);
 });

 if (rhs_intervals.empty()) {
  return;
 }

 std::vector<query<domain_type>> overlaps;

 interval::for_each_interval(expression.lhs, [&rhs_intervals, &overlaps](const auto& lhs) {
  for (const auto& rhs : rhs_intervals) {
   if (auto overlap = interval::intersection(lhs, rhs);
       !interval::is_empty(overlap)) {
    ceph::util::emplace_append(overlaps, std::move(overlap));
   }
  }
 });

 detail::emit_coalesced_intervals(std::move(overlaps), std::forward<SinkT>(sink));
}

namespace detail {

template <ordered_domain DomainT>
constexpr bool interval_less(const query<DomainT>& lhs, const query<DomainT>& rhs)
{
 const auto begin_order = compare_position(lhs.lower(), rhs.lower());

 if (std::strong_ordering::equal != begin_order) {
  return std::strong_ordering::less == begin_order;
 }

 if (lhs.lower().inclusive() != rhs.lower().inclusive()) {
  return lhs.lower().inclusive();
 }

 const auto end_order = compare_position(lhs.upper(), rhs.upper());

 if (std::strong_ordering::equal != end_order) {
  return std::strong_ordering::less == end_order;
 }

 return lhs.upper().inclusive() && !rhs.upper().inclusive();
}

template <std::ranges::random_access_range IntervalsT, typename SinkT>
requires (!std::is_lvalue_reference_v<IntervalsT>)
constexpr void emit_coalesced_intervals(IntervalsT&& intervals, SinkT&& sink)
{
 using domain_type = typename std::ranges::range_value_t<IntervalsT>::domain_type;
 if (intervals.empty()) {
  return;
 }

 std::ranges::sort(intervals, interval_less<domain_type>);

 auto current = std::move(*std::ranges::begin(intervals));

 for (auto& next : intervals | std::views::drop(1)) {
  if (can_coalesce(current, next)) {
   current = coalesce(current, next);
   continue;
  }

  interval::for_each_interval(current, sink);
  current = std::move(next);
 }

 interval::for_each_interval(current, std::forward<SinkT>(sink));
}

template <interval_view LHS_T, interval_view RHS_T, typename SinkT>
requires same_expression_domain<LHS_T, RHS_T>
constexpr void emit_union(const LHS_T& lhs, const RHS_T& rhs, SinkT&& sink)
{
 if (interval::is_empty(lhs)) {
  interval::for_each_interval(rhs, std::forward<SinkT>(sink));
  return;
 }

 if (interval::is_empty(rhs)) {
  interval::for_each_interval(lhs, std::forward<SinkT>(sink));
  return;
 }

 if (can_coalesce(lhs, rhs)) {
  const auto lhs_bounds = bounds_of(lhs);
  const auto rhs_bounds = bounds_of(rhs);
  emit_bounds(std::forward<SinkT>(sink),
    min_bound(lhs_bounds.lower(), rhs_bounds.lower(), inclusivity_merge::any),
    max_bound(lhs_bounds.upper(), rhs_bounds.upper(), inclusivity_merge::any));
  return;
 }

 if (definitely_before(lhs, rhs)) {
  interval::for_each_interval(lhs, sink);
  interval::for_each_interval(rhs, std::forward<SinkT>(sink));
  return;
 }

 interval::for_each_interval(rhs, sink);
 interval::for_each_interval(lhs, std::forward<SinkT>(sink));
}

} // namespace detail

template <expression LHS_T, expression RHS_T, typename SinkT>
constexpr void for_each_interval(const detail::set_union_expr<LHS_T, RHS_T>& expression,
                                 SinkT&& sink)
{
 if constexpr (interval_view<LHS_T> && interval_view<RHS_T>) {
  detail::emit_union(expression.lhs, expression.rhs, std::forward<SinkT>(sink));
  return;
 }

 using expression_type = detail::set_union_expr<LHS_T, RHS_T>;
 detail::interval_buffer<expression_type> intervals;

 auto collect = [&intervals](const auto& x) {
  detail::append_materialized_interval(intervals, x);
 };

 if constexpr (detail::uses_inline_intervals_v<expression_type>) {
  detail::collect_union_intervals(expression, collect);
 } else {
  // Retain child coalescing for large trees instead of materializing every leaf together.
  interval::for_each_interval(expression.lhs, collect);
  interval::for_each_interval(expression.rhs, collect);
 }

 detail::emit_coalesced_intervals(std::move(intervals), std::forward<SinkT>(sink));
}

template <expression ExprT>
constexpr auto compile_intervals(const ExprT& expression)
{
 using domain_type = typename std::remove_cvref_t<ExprT>::domain_type;
 std::vector<query<domain_type>> out;
 out.reserve(interval_view<ExprT> ? 1 : 4);

 interval::for_each_interval(expression, [&out](const auto& interval) {
  detail::append_materialized_interval(out, interval);
 });

 return out;
}

template <std::size_t Capacity, expression ExprT>
constexpr auto compile_intervals(const ExprT& expression)
{
 using domain_type = typename std::remove_cvref_t<ExprT>::domain_type;
 static_interval_set<domain_type, Capacity> out;

 interval::for_each_interval(expression, [&out](const auto& interval) {
  detail::append_materialized_interval(out, interval);
 });

 return out;
}

// Infer fixed storage from the expression type, including during constant evaluation.
template <expression ExprT>
requires (std::dynamic_extent != interval_capacity_v<ExprT>)
constexpr auto compile_intervals(std::in_place_t, const ExprT& expression)
{
 return interval::compile_intervals<interval_capacity_v<ExprT>>(expression);
}

template <expression ExprT>
constexpr std::size_t interval_count(const ExprT& expression)
{
 detail::interval_counter counter;
 interval::for_each_interval(expression, counter);
 return counter.count;
}

template <expression ExprT>
constexpr bool is_empty_expression(const ExprT& expression);

template <expression LHS_T, expression RHS_T>
constexpr bool is_empty_expression(const detail::set_union_expr<LHS_T, RHS_T>& expression)
{
 return interval::is_empty_expression(expression.lhs) &&
        interval::is_empty_expression(expression.rhs);
}

template <expression ExprT>
constexpr bool is_empty_expression(const ExprT& expression)
{
 if constexpr (interval_view<ExprT>) {
  return interval::is_empty(expression);
 }

 return 0 == interval::interval_count(expression);
}

enum struct endpoint_inclusion : std::uint8_t
{
 excluded,
 included
};

namespace detail {

constexpr bool includes_endpoint(const endpoint_inclusion inclusion) noexcept
{
 return endpoint_inclusion::included == inclusion;
}

template <typename DomainT, typename ValueT>
concept domain_value_source =
 std::constructible_from<typename DomainT::value_type, ValueT>;

template <ordered_domain DomainT, typename ValueT>
requires domain_value_source<DomainT, ValueT>
constexpr auto value_for(ValueT&& value)
{
 return typename DomainT::value_type(std::forward<ValueT>(value));
}

template <ordered_domain DomainT>
constexpr auto boundary_for(typename DomainT::value_type value,
                            const endpoint_inclusion inclusion)
{
 using boundary_type = boundary<DomainT>;

 if (includes_endpoint(inclusion)) {
  return boundary_type::closed(std::move(value));
 }

 return boundary_type::open(std::move(value));
}

template <typename EndpointT, ordered_domain DomainT, typename ValueT>
requires domain_value_source<DomainT, ValueT>
constexpr EndpointT endpoint_for(ValueT&& value, const endpoint_inclusion inclusion)
{
 return EndpointT {
  .value = value_for<DomainT>(std::forward<ValueT>(value)),
  .inclusion = inclusion
 };
}

} // namespace detail

template <ordered_domain DomainT>
struct lower_endpoint final
{
 using domain_type = DomainT;
 using value_type = typename DomainT::value_type;
 using boundary_type = boundary<DomainT>;

 value_type value;
 endpoint_inclusion inclusion = endpoint_inclusion::included;

 constexpr boundary_type as_boundary(this auto&& self)
 {
  return detail::boundary_for<DomainT>(std::forward_like<decltype(self)>(self.value), self.inclusion);
 }
};

template <ordered_domain DomainT>
struct upper_endpoint final
{
 using domain_type = DomainT;
 using value_type = typename DomainT::value_type;
 using boundary_type = boundary<DomainT>;

 value_type value;
 endpoint_inclusion inclusion = endpoint_inclusion::excluded;

 constexpr boundary_type as_boundary(this auto&& self)
 {
  return detail::boundary_for<DomainT>(std::forward_like<decltype(self)>(self.value), self.inclusion);
 }
};

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto lower_at_or_after(ValueT&& value)
{
 return detail::endpoint_for<lower_endpoint<DomainT>, DomainT>(
  std::forward<ValueT>(value),
  endpoint_inclusion::included);
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto lower_after(ValueT&& value)
{
 return detail::endpoint_for<lower_endpoint<DomainT>, DomainT>(
  std::forward<ValueT>(value),
  endpoint_inclusion::excluded);
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto upper_at_or_before(ValueT&& value)
{
 return detail::endpoint_for<upper_endpoint<DomainT>, DomainT>(
  std::forward<ValueT>(value),
  endpoint_inclusion::included);
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto upper_before(ValueT&& value)
{
 return detail::endpoint_for<upper_endpoint<DomainT>, DomainT>(
  std::forward<ValueT>(value),
  endpoint_inclusion::excluded);
}

template <ordered_domain DomainT>
constexpr auto between(lower_endpoint<DomainT> lower, upper_endpoint<DomainT> upper)
{
 return query<DomainT>::between(std::move(lower).as_boundary(), std::move(upper).as_boundary());
}

template <ordered_domain DomainT>
constexpr auto from(lower_endpoint<DomainT> lower)
{
 return query<DomainT>::between(std::move(lower).as_boundary(),
                                boundary<DomainT>::positive_infinity());
}

template <ordered_domain DomainT>
constexpr auto until(upper_endpoint<DomainT> upper)
{
 return query<DomainT>::between(boundary<DomainT>::negative_infinity(),
                                std::move(upper).as_boundary());
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto at(ValueT&& value)
{
 return query<DomainT>::singleton(
  detail::value_for<DomainT>(std::forward<ValueT>(value)));
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto at_or_after(ValueT&& value)
{
 return from(lower_at_or_after<DomainT>(std::forward<ValueT>(value)));
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto after(ValueT&& value)
{
 return from(lower_after<DomainT>(std::forward<ValueT>(value)));
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto at_or_before(ValueT&& value)
{
 return until(upper_at_or_before<DomainT>(std::forward<ValueT>(value)));
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto before(ValueT&& value)
{
 return until(upper_before<DomainT>(std::forward<ValueT>(value)));
}

template <ordered_domain DomainT, typename LowerT, typename UpperT>
requires detail::domain_value_source<DomainT, LowerT> &&
         detail::domain_value_source<DomainT, UpperT>
constexpr auto between(LowerT&& lower, UpperT&& upper)
{
 return between(lower_at_or_after<DomainT>(std::forward<LowerT>(lower)),
                upper_before<DomainT>(std::forward<UpperT>(upper)));
}

template <ordered_domain DomainT, typename LowerT, typename UpperT>
requires detail::domain_value_source<DomainT, LowerT> &&
         detail::domain_value_source<DomainT, UpperT>
constexpr auto closed_between(LowerT&& lower, UpperT&& upper)
{
 return between(lower_at_or_after<DomainT>(std::forward<LowerT>(lower)),
                upper_at_or_before<DomainT>(std::forward<UpperT>(upper)));
}

template <ordered_domain DomainT, typename LowerT, typename UpperT>
requires detail::domain_value_source<DomainT, LowerT> &&
         detail::domain_value_source<DomainT, UpperT>
constexpr auto open_between(LowerT&& lower, UpperT&& upper)
{
 return between(lower_after<DomainT>(std::forward<LowerT>(lower)),
                upper_before<DomainT>(std::forward<UpperT>(upper)));
}

template <successor_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto prefix(ValueT&& value)
{
 auto prefix_value = detail::value_for<DomainT>(std::forward<ValueT>(value));

 if constexpr (requires {
                { DomainT::empty_prefix(prefix_value) } -> std::same_as<bool>;
               }) {
  if (DomainT::empty_prefix(prefix_value)) {
   return query<DomainT>::universal();
  }
 }

 return query<DomainT>::prefix(std::move(prefix_value));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto starting_at(ExprT&& expression, ValueT&& value)
{
 using domain_type = expression_domain_t<ExprT>;

 return intersection(std::forward<ExprT>(expression),
                     at_or_after<domain_type>(std::forward<ValueT>(value)));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto starting_after(ExprT&& expression, ValueT&& value)
{
 using domain_type = expression_domain_t<ExprT>;

 return intersection(std::forward<ExprT>(expression),
                     after<domain_type>(std::forward<ValueT>(value)));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto ending_at(ExprT&& expression, ValueT&& value)
{
 using domain_type = expression_domain_t<ExprT>;

 return intersection(std::forward<ExprT>(expression),
                     at_or_before<domain_type>(std::forward<ValueT>(value)));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto ending_before(ExprT&& expression, ValueT&& value)
{
 using domain_type = expression_domain_t<ExprT>;

 return intersection(std::forward<ExprT>(expression),
                     before<domain_type>(std::forward<ValueT>(value)));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto without(ExprT&& expression, ValueT&& value)
{
 using domain_type = expression_domain_t<ExprT>;

 return difference(std::forward<ExprT>(expression),
                   at<domain_type>(std::forward<ValueT>(value)));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto without_prefix(ExprT&& expression, ValueT&& value)
{
 using domain_type = expression_domain_t<ExprT>;

 return difference(std::forward<ExprT>(expression),
                   prefix<domain_type>(std::forward<ValueT>(value)));
}

template <successor_domain DomainT, typename PrefixT, typename CursorT>
requires detail::domain_value_source<DomainT, PrefixT> &&
         detail::domain_value_source<DomainT, CursorT>
constexpr auto prefix_starting_at(PrefixT&& prefix_value, CursorT&& cursor)
{
 return starting_at(prefix<DomainT>(std::forward<PrefixT>(prefix_value)),
                    std::forward<CursorT>(cursor));
}

template <successor_domain DomainT, typename PrefixT, typename CursorT>
requires detail::domain_value_source<DomainT, PrefixT> &&
         detail::domain_value_source<DomainT, CursorT>
constexpr auto prefix_starting_after(PrefixT&& prefix_value, CursorT&& cursor)
{
 return starting_after(prefix<DomainT>(std::forward<PrefixT>(prefix_value)),
                       std::forward<CursorT>(cursor));
}

template <expression LHS_T, expression RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool intersects(const LHS_T& lhs, const RHS_T& rhs);

template <interval_view OuterT, interval_view InnerT>
requires same_expression_domain<OuterT, InnerT>
constexpr bool encloses(const OuterT& outer, const InnerT& inner);

template <interval_view LHS_T, interval_view RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool intersects(const LHS_T& lhs, const RHS_T& rhs)
{
 const auto lhs_bounds = detail::bounds_of(lhs);
 const auto rhs_bounds = detail::bounds_of(rhs);

 if (detail::empty_interval(lhs, lhs_bounds) ||
     detail::empty_interval(rhs, rhs_bounds)) {
  return false;
 }

 return !detail::disjoint_before(lhs, rhs) &&
        !detail::disjoint_before(rhs, lhs);
}

namespace detail {

template <expression LHS_T, expression RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool canonical_intersects(const LHS_T& lhs, const RHS_T& rhs)
{
 const auto lhs_intervals = canonical_intervals(lhs);
 const auto rhs_intervals = canonical_intervals(rhs);
 auto lhs_it = std::begin(lhs_intervals);
 auto rhs_it = std::begin(rhs_intervals);

 while (lhs_it != std::end(lhs_intervals) &&
        rhs_it != std::end(rhs_intervals)) {
  if (disjoint_before(*lhs_it, *rhs_it)) {
   ++lhs_it;
   continue;
  }

  if (disjoint_before(*rhs_it, *lhs_it)) {
   ++rhs_it;
   continue;
  }

  return true;
 }

 return false;
}

template <expression OuterT, expression InnerT>
requires same_expression_domain<OuterT, InnerT>
constexpr bool canonical_encloses(const OuterT& outer, const InnerT& inner)
{
 const auto outer_intervals = canonical_intervals(outer);
 const auto inner_intervals = canonical_intervals(inner);
 auto outer_it = std::begin(outer_intervals);
 const auto outer_end = std::end(outer_intervals);

 return std::ranges::all_of(inner_intervals, [&outer_it, outer_end](const auto& inner_interval) {
  while (outer_it != outer_end && definitely_before(*outer_it, inner_interval)) {
   ++outer_it;
  }

  return outer_it != outer_end &&
         interval::encloses(*outer_it, inner_interval);
 });
}

} // namespace detail

template <expression LHS_T, expression RHS_T, interval_view OtherT>
requires same_expression_domain<LHS_T, OtherT>
constexpr bool intersects(const detail::set_union_expr<LHS_T, RHS_T>& lhs, const OtherT& rhs)
{
 return interval::intersects(lhs.lhs, rhs) ||
        interval::intersects(lhs.rhs, rhs);
}

template <interval_view OtherT, expression LHS_T, expression RHS_T>
requires same_expression_domain<OtherT, LHS_T>
constexpr bool intersects(const OtherT& lhs, const detail::set_union_expr<LHS_T, RHS_T>& rhs)
{
 return interval::intersects(lhs, rhs.lhs) ||
        interval::intersects(lhs, rhs.rhs);
}

template <expression LHS_T, expression RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool intersects(const LHS_T& lhs, const RHS_T& rhs)
{
 return detail::canonical_intersects(lhs, rhs);
}

template <expression LHS_T, expression RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool is_disjoint(const LHS_T& lhs, const RHS_T& rhs)
{
 return !interval::intersects(lhs, rhs);
}

template <expression LHS_T, expression RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool encloses(const LHS_T& outer, const RHS_T& inner);

template <interval_view OuterT, interval_view InnerT>
requires same_expression_domain<OuterT, InnerT>
constexpr bool encloses(const OuterT& outer, const InnerT& inner)
{
 const auto outer_bounds = detail::bounds_of(outer);
 const auto inner_bounds = detail::bounds_of(inner);
 if (detail::empty_interval(inner, inner_bounds)) {
  return true;
 }
 if (detail::empty_interval(outer, outer_bounds)) {
  return false;
 }

 const auto lower = detail::compare_position(outer_bounds.lower(), inner_bounds.lower());
 const auto upper = detail::compare_position(outer_bounds.upper(), inner_bounds.upper());
 return (std::strong_ordering::less == lower ||
         (std::strong_ordering::equal == lower &&
          (outer_bounds.lower().inclusive() || !inner_bounds.lower().inclusive()))) &&
        (std::strong_ordering::greater == upper ||
         (std::strong_ordering::equal == upper &&
          (outer_bounds.upper().inclusive() || !inner_bounds.upper().inclusive())));
}

template <interval_view OuterT, expression LHS_T, expression RHS_T>
requires same_expression_domain<OuterT, LHS_T>
constexpr bool encloses(const OuterT& outer, const detail::set_union_expr<LHS_T, RHS_T>& inner)
{
 return interval::encloses(outer, inner.lhs) &&
        interval::encloses(outer, inner.rhs);
}

template <expression LHS_T, expression RHS_T>
requires same_expression_domain<LHS_T, RHS_T>
constexpr bool encloses(const LHS_T& outer, const RHS_T& inner)
{
 return detail::canonical_encloses(outer, inner);
}

} // namespace ceph::libfdb::interval

#endif
