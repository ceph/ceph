// Common Ceph byte vocabulary. Native storage and ownership stay at this boundary.
#ifndef CEPH_RGW_DATUM_ADAPTERS_CEPH_DATUM_H
 #define CEPH_RGW_DATUM_ADAPTERS_CEPH_DATUM_H

#include "datum/detail/byte.h"
#include "datum/extent.h"
#include "include/buffer.h"
#include "common/error_code.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <algorithm>
#include <ranges>

namespace ceph::salad::datum {

// The returned standard view borrows the list and its payload. Keep both alive.
// Iterators also depend on this transform descriptor; it is not a borrowed range.
inline auto fragments(const ::ceph::buffer::list& source)
{
    return source.buffers() | std::views::transform([](const auto& block) {
               const auto size = block.length();
               return std::span<const char> {size ? block.c_str() : nullptr, size};
           });
}

void fragments(const ::ceph::buffer::list&&) = delete;

namespace detail {

inline void check_native_extent(const ::ceph::buffer::list& source, const byte_extent selected)
{
    if (!extent_fits(selected.offset, selected.length, source.length())) {
        throw std::out_of_range {"byte extent exceeds its Ceph buffer"};
    }
}

} // namespace detail

template <writable_byte_output OutputT>
auto copy(const ::ceph::buffer::list& source, const byte_extent selected, OutputT&& output)
{
    detail::check_native_extent(source, selected);
    if (selected.length > std::ranges::size(output)) {
        throw std::length_error {"byte destination has insufficient capacity"};
    }
    using byte_type = std::remove_reference_t<std::ranges::range_reference_t<OutputT>>;
    auto destination =
        std::span<byte_type> {std::ranges::data(output), std::ranges::size(output)}.first(
            static_cast<std::size_t>(selected.length));
    if (!destination.empty()) {
        auto input = source.cbegin(static_cast<unsigned>(selected.offset));
        if constexpr (!std::is_volatile_v<byte_type>) {
            input.copy(static_cast<unsigned>(selected.length), reinterpret_cast<char *>(destination.data()));
        }

        if constexpr (std::is_volatile_v<byte_type>) {
            for (auto& value : destination) {
                value = detail::byte_cast<std::remove_cv_t<byte_type>>(*input);
                ++input;
            }
        }
    }

    return destination;
}

template <writable_byte_output OutputT>
auto copy(const ::ceph::buffer::list& source, const std::uint64_t offset, OutputT&& output)
{
    return ceph::salad::datum::copy(source, byte_extent {offset, std::ranges::size(output)},
                                   std::forward<OutputT>(output));
}

template <writable_byte_output OutputT>
auto copy(const ::ceph::buffer::list& source, OutputT&& output)
{
    return ceph::salad::datum::copy(source, byte_extent {0, source.length()}, std::forward<OutputT>(output));
}

// This result owns an independent payload. Native append/cursor copy-to-list can share storage.
inline ::ceph::buffer::list copy(const ::ceph::buffer::list& source, const byte_extent selected)
{
    detail::check_native_extent(source, selected);
    ::ceph::buffer::list result;
    if (selected.length) {
        ::ceph::buffer::ptr payload {
            ::ceph::buffer::create(static_cast<unsigned>(selected.length))};
        copy(source, selected, std::span<char> {payload.c_str(), payload.length()});
        result.append(std::move(payload));
    }

    return result;
}

// A slice retains native references. Externally backed buffers retain their native obligations.
inline ::ceph::buffer::list slice(const ::ceph::buffer::list& source, const byte_extent selected)
{
    detail::check_native_extent(source, selected);
    ::ceph::buffer::list result;
    if (selected.length) {
        result.substr_of(source, static_cast<unsigned>(selected.offset), static_cast<unsigned>(selected.length));
    }

    return result;
}

// Borrow one fragment when possible; otherwise gather exactly storage.size() bytes.
// The cursor supplies a remaining count and advancing chunks, including empty chunks.
// The result borrows from input for one fragment, or from storage after gathering.
// Scratch storage must not overlap unread input when gathering.
template <typename CursorT>
std::span<const char> read_contiguous(CursorT& input, const std::span<char> storage)
{
    if (storage.size() > input.get_remaining()) {
        throw ::ceph::buffer::end_of_buffer {};
    }
    if (storage.empty()) {
        return storage;
    }

    const char *bytes = nullptr;
    std::size_t first;
    do
     {
        first = input.get_ptr_and_advance(storage.size(), &bytes);
     }
    while (0 == first);

    if (first == storage.size()) {
        return {bytes, storage.size()};
    }

    std::copy_n(bytes, first, storage.data());
    for (auto copied = first; copied < storage.size();) {
        const char *fragment = nullptr;
        const auto count = input.get_ptr_and_advance(storage.size() - copied, &fragment);
        std::copy_n(fragment, count, storage.data() + copied);
        copied += count;
    }

    return storage;
}

} // namespace ceph::salad::datum

#endif // CEPH_RGW_DATUM_ADAPTERS_CEPH_DATUM_H
