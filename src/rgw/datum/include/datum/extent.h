// Composed half-open byte geometry, independent of storage and interval ownership.
#ifndef CEPH_RGW_DATUM_EXTENT_H
 #define CEPH_RGW_DATUM_EXTENT_H

#include <compare>
#include <concepts>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <optional>
#include <stdexcept>

#include <algorithm>

namespace ceph::salad::datum {

template <typename PointT, typename LengthT = PointT>
struct extent final
{
    PointT offset {};
    LengthT length {};

    constexpr bool operator==(const extent&) const = default;
    constexpr auto operator<=>(const extent&) const = default;
};

using byte_extent = extent<std::uint64_t>;

struct byte_mapping final
{
    byte_extent logical {};
    std::uint64_t backing_offset {};

    constexpr bool operator==(const byte_mapping&) const = default;
    constexpr auto operator<=>(const byte_mapping&) const = default;
};

namespace detail {

template <std::unsigned_integral OffsetT, std::unsigned_integral LengthT,
          std::unsigned_integral CapacityT>
constexpr bool extent_fits(const OffsetT offset, const LengthT length, const CapacityT capacity) noexcept
{
    using common_type = std::common_type_t<OffsetT, LengthT, CapacityT>;

    const auto first = static_cast<common_type>(offset);
    const auto count = static_cast<common_type>(length);
    const auto available = static_cast<common_type>(capacity);

    return first <= available and count <= available - first;
}

} // namespace detail

template <std::unsigned_integral IntegerT>
requires(!std::same_as<IntegerT, bool>)
constexpr IntegerT end_offset(const extent<IntegerT> value)
{
    if (value.length > std::numeric_limits<IntegerT>::max() - value.offset) {
        throw std::overflow_error {"extent endpoint is not representable"};
    }

    return static_cast<IntegerT>(value.offset + value.length);
}

template <std::unsigned_integral IntegerT>
requires(!std::same_as<IntegerT, bool>)
constexpr extent<IntegerT> subextent(const extent<IntegerT> source, const extent<IntegerT> relative)
{
    (void)end_offset(source);
    if (!detail::extent_fits(relative.offset, relative.length, source.length)) {
        throw std::out_of_range {"relative extent exceeds its source"};
    }

    return {static_cast<IntegerT>(source.offset + relative.offset), relative.length};
}

template <std::unsigned_integral IntegerT>
requires(!std::same_as<IntegerT, bool>)
constexpr std::optional<extent<IntegerT>> intersection(const extent<IntegerT> left, const extent<IntegerT> right)
{
    const auto last = std::min(end_offset(left), end_offset(right));
    const auto first = std::max(left.offset, right.offset);
    if (last <= first) {
        return std::nullopt;
    }

    return extent<IntegerT> {first, static_cast<IntegerT>(last - first)};
}

constexpr byte_mapping subextent(const byte_mapping source, const byte_extent relative)
{
    (void)end_offset(byte_extent {source.backing_offset, source.logical.length});
    const auto selected = subextent(source.logical, relative);

    return {selected, source.backing_offset + relative.offset};
}

namespace detail {

// GCC 14 can materialize the mapped aggregate during optional result conversion.
#if defined(__GNUC__) and __GNUC__ == 14 and !defined(__clang__)
inline constexpr bool scalar_mapping_clip = true;
#else
inline constexpr bool scalar_mapping_clip = false;
#endif

template <bool Scalar>
constexpr std::optional<byte_mapping> clip_mapping(const byte_mapping source, const byte_extent wanted)
{
    (void)end_offset(byte_extent {source.backing_offset, source.logical.length});

    if constexpr (Scalar) {
        const auto last = std::min(end_offset(source.logical), end_offset(wanted));
        const auto first = std::max(source.logical.offset, wanted.offset);
        if (last <= first) {
            return std::nullopt;
        }

        return byte_mapping {{first, last - first},
                             source.backing_offset + (first - source.logical.offset)};
    }

    const auto selected = intersection(source.logical, wanted);
    if (!selected) {
        return std::nullopt;
    }

    return byte_mapping {*selected,
                         source.backing_offset + (selected->offset - source.logical.offset)};
}

} // namespace detail

constexpr std::optional<byte_mapping> clip(const byte_mapping source, const byte_extent wanted)
{
    return detail::clip_mapping<detail::scalar_mapping_clip>(source, wanted);
}

} // namespace ceph::salad::datum

#endif // CEPH_RGW_DATUM_EXTENT_H
