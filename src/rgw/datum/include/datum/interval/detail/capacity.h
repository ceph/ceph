// Internal structural cardinality: no source observation or provider customization is required:
#ifndef CEPH_RGW_DATUM_INTERVAL_DETAIL_CAPACITY_H
#define CEPH_RGW_DATUM_INTERVAL_DETAIL_CAPACITY_H

#include <array>
#include <cstddef>
#include <ranges>
#include <span>
#include <type_traits>

namespace ceph::salad::datum::interval::detail {

// N4950 [array.overview], [views.span], [range.ref.view], and [range.owning.view]
// specify the fixed cardinalities and ownership-preserving wrappers used here:
// https://timsong-cpp.github.io/cppwp/n4950/array.overview
// dynamic_extent also represents an unproved or overflowing bound:
template <typename T>
struct component_bound : std::integral_constant<std::size_t, std::dynamic_extent> {};

template <typename T>
inline constexpr auto component_bound_v = component_bound<std::remove_cvref_t<T>>::value;

constexpr std::size_t add_component_bounds(const std::size_t left, const std::size_t right)
{
    if (left == std::dynamic_extent || right == std::dynamic_extent ||
        left > std::dynamic_extent - right) {
        return std::dynamic_extent;
    }

    return left + right;
}

template <typename T, std::size_t Size>
struct component_bound<T[Size]> : std::integral_constant<std::size_t, Size> {};

template <typename T, std::size_t Size>
struct component_bound<std::array<T, Size>> : std::integral_constant<std::size_t, Size> {};

template <typename T, std::size_t Extent>
struct component_bound<std::span<T, Extent>> : std::integral_constant<std::size_t, Extent> {};

template <typename T>
struct component_bound<std::ranges::empty_view<T>> : std::integral_constant<std::size_t, 0> {};

template <typename T>
struct component_bound<std::ranges::single_view<T>> : std::integral_constant<std::size_t, 1> {};

template <typename RangeT>
struct component_bound<std::ranges::ref_view<RangeT>>
    : std::integral_constant<std::size_t, component_bound_v<RangeT>> {};

template <typename RangeT>
struct component_bound<std::ranges::owning_view<RangeT>>
    : std::integral_constant<std::size_t, component_bound_v<RangeT>> {};

template <typename ViewT, typename FunctionT>
struct component_bound<std::ranges::transform_view<ViewT, FunctionT>>
    : std::integral_constant<std::size_t, component_bound_v<ViewT>> {};

template <typename ViewT, typename PredicateT>
struct component_bound<std::ranges::filter_view<ViewT, PredicateT>>
    : std::integral_constant<std::size_t, component_bound_v<ViewT>> {};

template <typename ViewT>
struct component_bound<std::ranges::take_view<ViewT>>
    : std::integral_constant<std::size_t, component_bound_v<ViewT>> {};

template <typename ViewT>
struct component_bound<std::ranges::drop_view<ViewT>>
    : std::integral_constant<std::size_t, component_bound_v<ViewT>> {};

template <typename ViewT>
struct component_bound<std::ranges::reverse_view<ViewT>>
    : std::integral_constant<std::size_t, component_bound_v<ViewT>> {};

template <typename ViewT>
struct component_bound<std::ranges::common_view<ViewT>>
    : std::integral_constant<std::size_t, component_bound_v<ViewT>> {};

} // ceph::salad::datum::interval::detail

#endif
