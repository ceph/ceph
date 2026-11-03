// Shared byte concepts and descriptor observation, independent of transfer dialect.
#ifndef CEPH_RGW_DATUM_DETAIL_BYTE_H
 #define CEPH_RGW_DATUM_DETAIL_BYTE_H

#include <concepts>
#include <cstddef>
#include <type_traits>

#include <ranges>

namespace ceph::salad::datum {

template <typename T>
concept byte_value =
    std::same_as<std::remove_cv_t<T>, std::byte> || std::same_as<std::remove_cv_t<T>, char> ||
    std::same_as<std::remove_cv_t<T>, unsigned char> ||
    std::same_as<std::remove_cv_t<T>, signed char>;

template <typename T>
concept byte_block = std::ranges::contiguous_range<T> and std::ranges::sized_range<T> and
                     byte_value<std::ranges::range_value_t<T>>;

template <typename T>
concept borrowed_byte_block = byte_block<T> and std::ranges::borrowed_range<T>;

// A written span may escape; temporary owning destinations cannot supply it.
template <typename T>
concept writable_byte_output =
    borrowed_byte_block<T> and
    (!std::is_const_v<std::remove_reference_t<std::ranges::range_reference_t<T>>>);

namespace detail {

template <typename T>
concept fragment_input_range =
    std::ranges::input_range<T> and byte_block<std::ranges::range_reference_t<T>>;

template <typename T>
concept fragment_range = std::ranges::forward_range<T> and fragment_input_range<T>;

} // namespace detail

template <typename T>
concept byte_fragments =
    detail::fragment_range<const std::remove_reference_t<T>> || detail::fragment_range<T>;

template <typename T>
concept writable_byte_fragments =
    byte_fragments<T> and detail::fragment_input_range<T> and
    (!std::is_const_v<std::remove_reference_t<
        std::ranges::range_reference_t<std::ranges::range_reference_t<T>>>>);

template <typename T>
concept byte_source = byte_block<T> || byte_fragments<T>;

namespace detail {

template <typename ReferenceT>
concept stable_byte_block =
    std::is_reference_v<ReferenceT> || std::ranges::borrowed_range<ReferenceT>;

// Preserve const observation when available; mutable views may cache iteration.
template <typename T>
using fragment_source_t =
    std::conditional_t<fragment_range<const std::remove_reference_t<T>>,
                       const std::remove_reference_t<T>, std::remove_reference_t<T>>;

template <typename T>
using fragment_input_source_t =
    std::conditional_t<fragment_input_range<const std::remove_reference_t<T>>,
                       const std::remove_reference_t<T>, std::remove_reference_t<T>>;

template <byte_fragments FragmentsT>
constexpr auto& read_fragments(FragmentsT& source)
{
    return static_cast<fragment_source_t<FragmentsT>&>(source);
}

template <byte_value T, byte_value U>
constexpr T byte_cast(U value)
{
    if constexpr (std::same_as<std::remove_cv_t<U>, std::byte>) {
        return static_cast<T>(std::to_integer<unsigned char>(value));
    }

    return static_cast<T>(static_cast<unsigned char>(value));
}

} // namespace detail

} // namespace ceph::salad::datum

#endif // CEPH_RGW_DATUM_DETAIL_BYTE_H
