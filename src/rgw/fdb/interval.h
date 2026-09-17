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

#include <algorithm>
#include <array>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

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
  return kind_;
 }

 constexpr const value_type& finite_key() const noexcept
 {
  return *key_;
 }

 constexpr bool inclusive() const noexcept
 {
  return inclusive_;
 }

 constexpr bool operator==(const boundary&) const = default;

 private:
 constexpr boundary(const boundary_kind kind,
                    std::optional<value_type> key,
                    const bool inclusive)
  : kind_(kind),
    key_(std::move(key)),
    inclusive_(inclusive)
 {}

 boundary_kind kind_ = boundary_kind::finite;
 std::optional<value_type> key_;
 bool inclusive_ = false;
};

template <ordered_domain DomainT>
class boundary_ref final : public detail::boundary_view_ops
{
 public:
 using domain_type = DomainT;
 using value_type = typename DomainT::value_type;

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
  return kind_;
 }

 constexpr const value_type& finite_key() const noexcept
 {
  return *key_;
 }

 constexpr bool inclusive() const noexcept
 {
  return inclusive_;
 }

 private:
 constexpr boundary_ref(const boundary_kind kind,
                        const value_type* key,
                        const bool inclusive) noexcept
  : kind_(kind),
    key_(key),
    inclusive_(boundary_kind::finite == kind && inclusive)
 {}

 boundary_kind kind_ = boundary_kind::finite;

 // Infinities cannot be keys, so the non-owning pointer uses null for that state.
 const value_type *key_ = nullptr;

 bool inclusive_ = false;
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

 constexpr query() = default;

 constexpr query(boundary_type begin, boundary_type end)
  : begin_(std::move(begin)),
    end_(std::move(end))
 {}

 static constexpr query empty() noexcept
 {
  query out;
  out.empty_ = true;

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

 static constexpr query between(boundary_type begin, boundary_type end)
 {
  return query(std::move(begin), std::move(end));
 }

 template <boundary_view_for<DomainT> LowerT,
           boundary_view_for<DomainT> UpperT>
 static constexpr query between(const LowerT& begin, const UpperT& end)
 {
  return query(materialize_boundary(begin),
               materialize_boundary(end));
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
  return begin_;
 }

 constexpr const boundary_type& upper() const noexcept
 {
  return end_;
 }

 constexpr bool explicitly_empty() const noexcept
 {
  return empty_;
 }

 constexpr bool operator==(const query& rhs) const
 {
  return begin_ == rhs.begin_ &&
         end_ == rhs.end_ &&
         empty_ == rhs.empty_;
 }

 private:
 boundary_type begin_ = boundary_type::negative_infinity();
 boundary_type end_ = boundary_type::positive_infinity();
 bool empty_ = false;
};

template <ordered_domain DomainT, std::size_t Capacity>
class static_interval_set final
{
 public:
 using value_type = query<DomainT>;
 using const_iterator = typename std::array<value_type, Capacity>::const_iterator;

 constexpr void push_back(value_type value)
 {
  if (Capacity == size_) {
   throw std::length_error { "static interval set capacity exceeded" };
  }

  intervals_[size_++] = std::move(value);
 }

 constexpr bool empty() const noexcept
 {
  return 0 == size_;
 }

 constexpr std::size_t size() const noexcept
 {
  return size_;
 }

 constexpr const value_type& operator[](const std::size_t index) const noexcept
 {
  return intervals_[index];
 }

 constexpr const_iterator begin() const noexcept
 {
  return std::begin(intervals_);
 }

 constexpr const_iterator end() const noexcept
 {
  return std::next(std::begin(intervals_),
                   static_cast<std::ptrdiff_t>(size_));
 }

 constexpr bool operator==(const static_interval_set& rhs) const
 {
  return std::ranges::equal(*this, rhs);
 }

 private:
 std::array<value_type, Capacity> intervals_ {};
 std::size_t size_ = 0;
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

 constexpr explicit interval_bounds(const IntervalT& interval)
  : lower_(interval.lower()),
    upper_(interval.upper())
 {}

 constexpr const auto& lower() const noexcept
 {
  return lower_;
 }

 constexpr const auto& upper() const noexcept
 {
  return upper_;
 }

 private:
 boundary_storage<lower_result> lower_;
 boundary_storage<upper_result> upper_;
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

 if constexpr (requires(SinkT&& s) {
                std::forward<SinkT>(s).emit_interval(lower, upper);
               }) {
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
  return materialize_boundary(lhs);
 }

 if (std::strong_ordering::greater == order) {
  return materialize_boundary(rhs);
 }

 return materialize_boundary(lhs, merge_inclusive(lhs.inclusive(), rhs.inclusive(), merge));
}

template <boundary_view LHS_T, boundary_view RHS_T>
requires std::same_as<boundary_domain_t<LHS_T>, boundary_domain_t<RHS_T>>
constexpr auto max_bound(const LHS_T& lhs, const RHS_T& rhs, const inclusivity_merge merge)
{
 const auto order = compare_position(lhs, rhs);

 if (std::strong_ordering::less == order) {
  return materialize_boundary(rhs);
 }

 if (std::strong_ordering::greater == order) {
  return materialize_boundary(lhs);
 }

 return materialize_boundary(lhs, merge_inclusive(lhs.inclusive(), rhs.inclusive(), merge));
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
 return materialize_boundary(removed_begin, !removed_begin.inclusive());
}

template <boundary_view BoundT>
constexpr auto difference_begin_after(const BoundT& removed_end)
{
 return materialize_boundary(removed_end, !removed_end.inclusive());
}

template <ordered_domain DomainT, typename SinkT>
constexpr void emit_coalesced_intervals(std::vector<query<DomainT>> intervals, SinkT&& sink);

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

template <interval_view LHS_T, interval_view RHS_T, typename SinkT>
requires same_expression_domain<LHS_T, RHS_T>
constexpr void emit_intersection(const LHS_T& lhs, const RHS_T& rhs, SinkT&& sink)
{
 interval::for_each_interval(interval::intersection(lhs, rhs), std::forward<SinkT>(sink));
}

template <typename OutT, interval_view IntervalT>
constexpr void append_materialized_interval(OutT& out, const IntervalT& interval)
{
 using domain_type = typename std::remove_cvref_t<IntervalT>::domain_type;

 ceph::util::emplace_append(out, query<domain_type>::between(interval.lower(), interval.upper()));
}

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

 interval::for_each_interval(expression.lhs, [&](const auto& lhs) {
  auto cursor = materialize_boundary(lhs.lower());
  bool emitted_tail = false;

  auto emit_remaining = [&sink, &lhs, &cursor, &emitted_tail](const auto& rhs) {
   if (emitted_tail) {
    return;
   }

   const auto remaining = query<domain_type>::between(cursor, lhs.upper());

   if (interval::is_empty(remaining)) {
    emitted_tail = true;
    return;
   }

   if (detail::definitely_before(rhs, remaining)) {
    return;
   }

   if (detail::definitely_after(rhs, remaining)) {
    std::invoke(sink, remaining);
    emitted_tail = true;
    return;
   }

   const auto overlap = interval::intersection(remaining, rhs);

   if (interval::is_empty(overlap)) {
    return;
   }

   detail::emit_bounds(sink, cursor, detail::difference_end_before(overlap.lower()));

   cursor = detail::difference_begin_after(overlap.upper());
  };

  interval::for_each_interval(expression.rhs, emit_remaining);

  if (emitted_tail) {
   return;
  }

  detail::emit_bounds(std::forward<SinkT>(sink), cursor, lhs.upper());
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

template <ordered_domain DomainT, typename SinkT>
constexpr void emit_coalesced_intervals(std::vector<query<DomainT>> intervals, SinkT&& sink)
{
 if (intervals.empty()) {
  return;
 }

 std::ranges::sort(intervals, interval_less<DomainT>);

 auto current = std::move(intervals.front());

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
  interval::for_each_interval(coalesce(lhs, rhs), std::forward<SinkT>(sink));
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

 using domain_type = typename std::remove_cvref_t<LHS_T>::domain_type;
 std::vector<query<domain_type>> intervals;
 intervals.reserve(4);

 auto collect = [&intervals](const auto& x) {
  detail::append_materialized_interval(intervals, x);
 };

 interval::for_each_interval(expression.lhs, collect);
 interval::for_each_interval(expression.rhs, collect);

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

template <expression ExprT>
constexpr std::size_t interval_count(const ExprT& expression)
{
 std::size_t count = 0;

 interval::for_each_interval(expression, [&count](const auto&) { ++count; });

 return count;
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
constexpr auto boundary_for(const typename DomainT::value_type& value,
                            const endpoint_inclusion inclusion)
{
 using boundary_type = boundary<DomainT>;

 if (includes_endpoint(inclusion)) {
  return boundary_type::closed(value);
 }

 return boundary_type::open(value);
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

 constexpr boundary_type as_boundary() const
 {
  return detail::boundary_for<DomainT>(value, inclusion);
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

 constexpr boundary_type as_boundary() const
 {
  return detail::boundary_for<DomainT>(value, inclusion);
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
 return query<DomainT>::between(lower.as_boundary(), upper.as_boundary());
}

template <ordered_domain DomainT>
constexpr auto from(lower_endpoint<DomainT> lower)
{
 return query<DomainT>::between(lower.as_boundary(),
                                boundary<DomainT>::positive_infinity());
}

template <ordered_domain DomainT>
constexpr auto until(upper_endpoint<DomainT> upper)
{
 return query<DomainT>::between(boundary<DomainT>::negative_infinity(),
                                upper.as_boundary());
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
 const auto lhs_intervals = interval::compile_intervals(lhs);
 const auto rhs_intervals = interval::compile_intervals(rhs);
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
 const auto outer_intervals = interval::compile_intervals(outer);
 const auto inner_intervals = interval::compile_intervals(inner);
 auto outer_it = std::begin(outer_intervals);
 const auto outer_end = std::end(outer_intervals);

 return std::ranges::all_of(inner_intervals, [&outer_it, outer_end](const auto& inner_interval) {
  while (outer_it != outer_end && definitely_before(*outer_it, inner_interval)) {
   ++outer_it;
  }

  return outer_it != outer_end &&
         interval::is_empty_expression(interval::difference(inner_interval, *outer_it));
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
 return interval::is_empty_expression(interval::difference(inner, outer));
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
