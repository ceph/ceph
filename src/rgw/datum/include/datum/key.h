// Datum: Byte-string domains, structured segment keys and integer encodings.
// A reusable ordered-domain profile above the interval kernel.
//
// Please keep this layer independent of reserved backend keyspaces and application naming
// conventions.
// Put domain-specific behavior in consumer profiles or adapters.

// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
// vim: ts=8 sw=4 sts=4 expandtab ft=cpp

#ifndef CEPH_RGW_DATUM_KEY_H
 #define CEPH_RGW_DATUM_KEY_H

#include "datum/interval/query.h"

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <optional>
#include <stdexcept>
#include <tuple>
#include <utility>

#include <array>
#include <vector>

#include <algorithm>
#include <iterator>

#include <charconv>
#include <string>
#include <string_view>

namespace ceph::salad::datum::key {

template <typename DomainT>
concept ordered_domain = interval::ordered_domain<DomainT>;

struct byte_string_domain final
{
    using value_type = std::string;

public:
    static constexpr std::strong_ordering compare(const std::string_view lhs,
                                                  const std::string_view rhs) noexcept
    {
        return lhs <=> rhs;
    }

public:
    static constexpr bool empty_prefix(const std::string_view prefix) noexcept
    {
        return std::empty(prefix);
    }

public:
    static constexpr std::optional<std::string> successor(const std::string_view prefix)
    {
        constexpr auto max_byte = static_cast<unsigned char>(0xFF);

        for (auto i = std::size(prefix); 0 != i; --i) {
            const auto byte = static_cast<unsigned char>(prefix[i - 1]);

            if (max_byte == byte) {
                continue;
            }

            auto out = std::string {prefix.substr(0, i)};
            out.back() = static_cast<char>(1 + byte);

            return out;
        }

        return std::nullopt;
    }

};

using byte_query = interval::query<byte_string_domain>;
using interval::after;
using interval::at;
using interval::at_or_after;
using interval::at_or_before;
using interval::before;
using interval::between;
using interval::closed_between;
using interval::encloses;
using interval::endpoint_inclusion;
using interval::ending_at;
using interval::ending_before;
using interval::from;
using interval::intersects;
using interval::is_disjoint;
using interval::lower_after;
using interval::lower_at_or_after;
using interval::lower_endpoint;
using interval::open_between;
using interval::prefix;
using interval::prefix_starting_after;
using interval::prefix_starting_at;
using interval::starting_after;
using interval::starting_at;
using interval::upper_at_or_before;
using interval::upper_before;
using interval::upper_endpoint;
using interval::until;
using interval::without;
using interval::without_prefix;

// Datum: Explicit integer segment encodings and their decoders.
// Reusable encoding profiles for consumers of the key layout machinery.
//
// Please keep this layer independent of application field meanings and backend schema
// versions.
// Put domain-specific behavior in consumer profiles or adapters.

namespace detail {

constexpr char hex_digit(const std::uint8_t value) noexcept
{
    constexpr std::string_view digits = "0123456789ABCDEF";

    return digits[value];
}

constexpr std::optional<std::uint8_t> hex_value(const char value) noexcept
{
    if ('0' <= value && value <= '9') {
        return static_cast<std::uint8_t>(value - '0');
    }
    if ('A' <= value && value <= 'F') {
        return static_cast<std::uint8_t>(10 + value - 'A');
    }

    return std::nullopt;
}

} // detail

template <std::unsigned_integral IntegerT>
requires (!std::same_as<std::remove_cv_t<IntegerT>, bool>)
constexpr auto fixed_width_hexadecimal_segment(IntegerT value)
{
    std::array<char, sizeof(value) * 2> out;

    for (auto i = std::size(out); 0 != i; --i) {
        out[i - 1] = detail::hex_digit(static_cast<std::uint8_t>(value & 0xF));
        value >>= 4;
    }

    return out;
}

constexpr std::string decimal_segment(std::uint64_t value)
{
    std::array<char, 1 + std::numeric_limits<std::uint64_t>::digits10> digits {};

    const auto result = std::to_chars(std::begin(digits), std::end(digits), value);

    return {std::begin(digits), result.ptr};
}

template <std::unsigned_integral IntegerT>
requires (!std::same_as<std::remove_cv_t<IntegerT>, bool>)
constexpr std::optional<IntegerT>
decode_fixed_width_hexadecimal_segment(const std::string_view segment)
{
    if (sizeof(IntegerT) * 2 != std::size(segment)) {
        return std::nullopt;
    }

    IntegerT value = 0;
    for (const auto ch : segment) {
        const auto decoded = detail::hex_value(ch);

        if (!decoded) {
            return std::nullopt;
        }

        value = static_cast<IntegerT>((value << 4) | *decoded);
    }

    return value;
}

// Datum: An explicit order-preserving segmented key format.
// A reusable key encoding profile; consumers choose their own keyspaces.
//
// Please keep this layer independent of application records, native prefixes and backend
// policy.
// Put domain-specific behavior in consumer profiles or adapters.

template <typename SegmentT>
concept decoded_segment =
    std::same_as<SegmentT, std::remove_cvref_t<SegmentT>> &&
    !std::same_as<SegmentT, std::string_view> && std::constructible_from<SegmentT, std::string>;

namespace detail {

constexpr auto segment_escape = static_cast<char>(0xFF);

template <std::size_t N>
constexpr std::string_view segment_view(const char (&segment)[N])
{
    // Char array overloads model NUL-terminated string literals; raw byte arrays
    // should use std::string_view so that the intended size is explicit.
    if ('\0' != segment[N - 1]) {
        throw std::invalid_argument {"segment must be NUL-terminated"};
    }

    return std::string_view {segment, N - 1};
}

template <typename SegmentT>
requires (!std::is_bounded_array_v<std::remove_reference_t<SegmentT>>) &&
        std::convertible_to<const SegmentT&, std::string_view>
constexpr std::string_view segment_view(const SegmentT& segment)
{
    return std::string_view {segment};
}

template <std::size_t N>
constexpr std::string_view segment_view(const std::array<char, N>& segment)
{
    return {std::data(segment), std::size(segment)};
}

template <typename SegmentT>
concept segment_source = requires(const SegmentT& segment) {
    { segment_view(segment) } -> std::same_as<std::string_view>;
};

template <typename ...SegmentT>
concept decoded_segments = 0 < sizeof...(SegmentT) && (decoded_segment<SegmentT> && ...);

constexpr void require_valid_segment(const std::string_view segment)
{
    // A leading escape marker would consume the preceding segment terminator.
    if (!std::empty(segment) && segment_escape == segment.front()) {
        throw std::invalid_argument {"key segment must not start with 0xFF"};
    }
}

constexpr void require_valid_keyspace_segment(const std::string_view segment)
{
    if (std::empty(segment)) {
        throw std::invalid_argument {"keyspace segment must not be empty"};
    }
    require_valid_segment(segment);
}

struct scanned_segment
{
    std::string_view value;
    std::size_t first_zero;
    std::size_t encoded_size;
};

constexpr scanned_segment scan_segment(std::string_view value)
{
    require_valid_segment(value);
    const auto first_zero = value.find('\0');
    const auto escapes = first_zero == std::string_view::npos
                             ? 0
                             : 1 + std::ranges::count(value.substr(first_zero + 1), '\0');

    return {value, first_zero, std::size(value) + 1 + escapes};
}

constexpr void append_segment_contents(std::string& out, std::string_view segment,
                                       std::size_t first_zero)
{
    out.append(segment.substr(0, first_zero));
    if (first_zero != std::string_view::npos) {
        // Dense escapes need one cheap pass, rather than a search/append per byte.
        for (const char value : segment.substr(first_zero)) {
            out.push_back(value);
            if (value == '\0') {
                out.push_back(segment_escape);
            }
        }
    }
}

constexpr void append_segment_contents(std::string& out, std::string_view segment)
{
    append_segment_contents(out, segment, segment.find('\0'));
}

constexpr void append_segment(std::string& out, std::string_view segment, std::size_t first_zero)
{
    append_segment_contents(out, segment, first_zero);
    out.push_back('\0');
}

constexpr void append_segment(std::string& out, std::string_view segment)
{
    append_segment(out, segment, segment.find('\0'));
}

constexpr bool valid_keyspace_segment(const std::string_view segment) noexcept
{
    return !std::empty(segment) && segment_escape != segment.front();
}

template <typename TupleT, std::size_t ...Indexes>
consteval bool tuple_segments_impl(std::index_sequence<Indexes...>)
{
    using tuple_type = std::remove_cvref_t<TupleT>;

    return (segment_source<std::tuple_element_t<Indexes, tuple_type>> && ...);
}

template <typename TupleT, std::size_t ...Indexes>
consteval bool decoded_tuple_impl(std::index_sequence<Indexes...>)
{
    using tuple_type = std::remove_cvref_t<TupleT>;

    return (decoded_segment<std::tuple_element_t<Indexes, tuple_type>> && ...);
}

template <typename TupleT>
concept tuple = requires { typename std::tuple_size<std::remove_cvref_t<TupleT>>::type; };

template <typename TupleT>
concept non_empty_tuple = tuple<TupleT> && 0 < std::tuple_size_v<std::remove_cvref_t<TupleT>>;

template <typename TupleT, std::size_t N, std::size_t ...Indexes>
constexpr std::remove_cvref_t<TupleT> tuple_from_segments(std::array<std::string, N>&& segments,
                                                          std::index_sequence<Indexes...>)
{
    using tuple_type = std::remove_cvref_t<TupleT>;

    return tuple_type {std::tuple_element_t<Indexes, tuple_type> {std::move(segments[Indexes])}...};
}

constexpr bool decode_segment(std::string_view& bytes, std::string& segment)
{
    while (!std::empty(bytes)) {
        const auto zero = bytes.find('\0');
        if (zero == std::string_view::npos) {
            return false;
        }
        segment.append(bytes.substr(0, zero));
        const bool escaped = zero + 1 < std::size(bytes) && segment_escape == bytes[zero + 1];
        bytes.remove_prefix(zero + (escaped ? 2 : 1));
        if (!escaped) {
            return true;
        }
        segment.push_back('\0');
    }

    return false;
}

template <std::size_t N>
constexpr std::optional<std::array<std::string, N>> decode_fixed_segments(std::string_view bytes)
{
    std::array<std::string, N> out;
    for (auto& segment : out) {
        if (!decode_segment(bytes, segment)) {
            return std::nullopt;
        }
    }
    if (!std::empty(bytes) || !valid_keyspace_segment(out.front())) {
        return std::nullopt;
    }

    return out;
}

} // detail

template <typename ...Segments>
concept key_segments = 0 < sizeof...(Segments) && (detail::segment_source<Segments> && ...);

template <typename TupleT>
concept tuple_segments =
    detail::non_empty_tuple<TupleT> &&
    detail::tuple_segments_impl<TupleT>(
        std::make_index_sequence<std::tuple_size_v<std::remove_cvref_t<TupleT>>> {});

template <typename TupleT>
concept decoded_tuple =
    detail::non_empty_tuple<TupleT> &&
    detail::decoded_tuple_impl<TupleT>(
        std::make_index_sequence<std::tuple_size_v<std::remove_cvref_t<TupleT>>> {});

class encoded_key final
{
    std::string encoded_bytes;

public:
    constexpr encoded_key() = delete;

public:
    constexpr auto operator<=>(const encoded_key& rhs) const noexcept
    {
        return encoded_bytes <=> rhs.encoded_bytes;
    }

    constexpr bool operator==(const encoded_key& rhs) const noexcept = default;

    template <detail::segment_source SegmentT>
    friend constexpr encoded_key operator/(encoded_key lhs, const SegmentT& segment)
    {
        const auto value = detail::segment_view(segment);
        detail::require_valid_segment(value);
        detail::append_segment(lhs.encoded_bytes, value);

        return lhs;
    }

public:
    friend constexpr std::string_view key_view(const encoded_key& key) noexcept;

public:
    friend constexpr std::string take_key(encoded_key&& key) noexcept;

private:
    explicit constexpr encoded_key(std::string bytes) : encoded_bytes(std::move(bytes))
    {}

private:
    template <typename ...Segments>
    requires key_segments<Segments...>
    friend constexpr encoded_key compose(const Segments& ...segments);

};

constexpr std::string_view key_view(const encoded_key& key) noexcept { return key.encoded_bytes; }

// Transfers an encoded key into an owning API without copying its bytes:
constexpr std::string take_key(encoded_key&& key) noexcept { return std::move(key.encoded_bytes); }

template <typename ...Segments>
requires key_segments<Segments...>
constexpr encoded_key compose(const Segments& ...segments)
{
    const std::array scanned {detail::scan_segment(detail::segment_view(segments))...};
    detail::require_valid_keyspace_segment(scanned.front().value);
    std::string out;
    out.reserve(std::ranges::fold_left(
        scanned, std::size_t {0},
        [](std::size_t bytes, const auto& segment) { return bytes + segment.encoded_size; }));
    for (const auto& segment : scanned) {
        detail::append_segment(out, segment.value, segment.first_zero);
    }

    return encoded_key {std::move(out)};
}

template <tuple_segments TupleT>
constexpr encoded_key compose(const TupleT& segments)
{
    const auto& [...segment] = segments;

    return compose(std::as_const(segment)...);
}

template <detail::segment_source SegmentT>
constexpr encoded_key keyspace(const SegmentT& segment)
{
    return compose(segment);
}

constexpr std::optional<std::vector<std::string>> decode_segments(std::string_view bytes)
{
    std::vector<std::string> out;
    while (!std::empty(bytes)) {
        auto& segment = out.emplace_back();
        if (!detail::decode_segment(bytes, segment)) {
            return std::nullopt;
        }
    }
    if (std::empty(out) || !detail::valid_keyspace_segment(out.front())) {
        return std::nullopt;
    }

    return out;
}

namespace detail {

template <decoded_tuple TupleT>
constexpr std::optional<std::remove_cvref_t<TupleT>> decode_tuple(const std::string_view bytes)
{
    using tuple_type = std::remove_cvref_t<TupleT>;

    constexpr auto count = std::tuple_size_v<tuple_type>;

    auto decoded = decode_fixed_segments<count>(bytes);

    if (!decoded) {
        return std::nullopt;
    }

    return tuple_from_segments<tuple_type>(std::move(*decoded), std::make_index_sequence<count> {});
}

} // detail

template <decoded_tuple TupleT>
constexpr std::optional<std::remove_cvref_t<TupleT>> decode(const std::string_view bytes)
{
    return detail::decode_tuple<TupleT>(bytes);
}

template <typename ...SegmentT>
requires detail::decoded_segments<SegmentT...>
constexpr std::optional<std::tuple<SegmentT...>> decode(const std::string_view bytes)
{
    return detail::decode_tuple<std::tuple<SegmentT...>>(bytes);
}

template <decoded_tuple TupleT>
constexpr std::optional<std::remove_cvref_t<TupleT>> decode(const encoded_key& key)
{
    return decode<TupleT>(key_view(key));
}

template <typename ...SegmentT>
requires detail::decoded_segments<SegmentT...>
constexpr std::optional<std::tuple<SegmentT...>> decode(const encoded_key& key)
{
    return decode<SegmentT...>(key_view(key));
}

constexpr std::vector<std::string> segments(const encoded_key& key)
{
    return *decode_segments(key_view(key));
}

constexpr byte_query at(const encoded_key& key) { return at<byte_string_domain>(key_view(key)); }

constexpr byte_query prefix(const encoded_key& key)
{
    return prefix<byte_string_domain>(key_view(key));
}

template <detail::segment_source SegmentT>
constexpr byte_query prefix(const encoded_key& parent, const SegmentT& next_segment_prefix)
{
    const auto value = detail::segment_view(next_segment_prefix);
    detail::require_valid_segment(value);
    auto out = std::string {key_view(parent)};
    detail::append_segment_contents(out, value);

    return prefix<byte_string_domain>(std::move(out));
}

} // ceph::salad::datum::key

#endif // CEPH_RGW_DATUM_KEY_H
