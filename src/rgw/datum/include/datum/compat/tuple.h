// Datum: Typed tuple bindings for compilers without C++26 binding packs.

#ifndef CEPH_RGW_DATUM_COMPAT_TUPLE_H
 #define CEPH_RGW_DATUM_COMPAT_TUPLE_H

#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

namespace ceph::salad::datum::compat::detail {

// Native C++26 binding packs replace this tuple adaptation.
// Callbacks run synchronously; caller-supplied access preserves friendship.
template <std::size_t Index, typename TupleT, typename AccessorT>
using segment_reference_t = std::conditional_t<
    std::is_lvalue_reference_v<decltype(std::declval<AccessorT&>().template operator()<Index>(
        std::declval<const std::remove_reference_t<TupleT>&>()))>,
    std::tuple_element_t<Index, const std::remove_reference_t<TupleT>>&,
    std::tuple_element_t<Index, const std::remove_reference_t<TupleT>>&&>;

template <typename ReferenceT>
constexpr ReferenceT bind_segment(ReferenceT value) noexcept
{
    return std::forward<ReferenceT>(value);
}

template <typename TupleT, typename AccessorT, typename ConsumerT>
constexpr decltype(auto) apply_const_tuple(const TupleT& source, AccessorT&& access, ConsumerT&& consumer)
{
    return [&]<std::size_t ...Indexes>(std::index_sequence<Indexes...>) -> decltype(auto) {
        // Braces sequence access and retain converted temporaries through the consumer call.
        return std::apply(std::forward<ConsumerT>(consumer),
            std::tuple<segment_reference_t<Indexes, TupleT, AccessorT>...> {
                ::ceph::salad::datum::compat::detail::bind_segment<
                    segment_reference_t<Indexes, TupleT, AccessorT>>(
                    access.template operator()<Indexes>(source))...});
    }(std::make_index_sequence<std::tuple_size_v<std::remove_cvref_t<TupleT>>> {});
}

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_COMPAT_TUPLE_H
