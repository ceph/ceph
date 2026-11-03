// Datum: Native size reductions with consistent borrowed-callback semantics.
#ifndef CEPH_RGW_DATUM_COMPAT_ALGORITHM_H
 #define CEPH_RGW_DATUM_COMPAT_ALGORITHM_H

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <ranges>
#include <type_traits>
#include <utility>

namespace ceph::salad::datum::compat {

// This is the size-only capability used by two consumers, not a generic fold substitute.
// The synchronous operation borrows its callable on every library implementation.
template <std::ranges::input_range RangeT, typename FunctionT>
requires std::invocable<FunctionT&, std::size_t, std::ranges::range_reference_t<RangeT>> and
         std::same_as<std::invoke_result_t<FunctionT&, std::size_t,
                                          std::ranges::range_reference_t<RangeT>>, std::size_t>
constexpr std::size_t fold_size(RangeT&& values, std::size_t initial, FunctionT&& function)
{
#if defined(__cpp_lib_ranges_fold) and __cpp_lib_ranges_fold >= 202207L
    return std::ranges::fold_left(std::forward<RangeT>(values), initial, std::ref(function));
#else
    for (auto&& value : std::ranges::ref_view {values}) {
        initial = std::invoke(function, std::move(initial), std::forward<decltype(value)>(value));
    }

    return initial;
#endif
}

} // ceph::salad::datum::compat

#endif // CEPH_RGW_DATUM_COMPAT_ALGORITHM_H
