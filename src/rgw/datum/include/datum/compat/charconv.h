// Datum: Decimal construction with a native constexpr path where available.

#ifndef CEPH_RGW_DATUM_COMPAT_CHARCONV_H
 #define CEPH_RGW_DATUM_COMPAT_CHARCONV_H

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <array>

#include <charconv>
#include <iterator>
#include <string>

// Keep the constexpr fallback available for direct checks on newer libraries too.
namespace ceph::salad::datum::compat::detail {

template <std::integral IntegerT>
requires (!std::same_as<std::remove_cv_t<IntegerT>, bool>)
constexpr std::to_chars_result decimal_chars(char *first, char *last, const IntegerT value)
{
    using magnitude_type = std::make_unsigned_t<IntegerT>;

    bool negative = false;
    if constexpr (std::is_signed_v<IntegerT>) {
        negative = value < 0;
    }

    auto magnitude = static_cast<magnitude_type>(value);
    if (negative) {
        magnitude = static_cast<magnitude_type>(magnitude_type {0} - magnitude);
    }

    std::size_t length = negative;
    auto remaining = magnitude;

    do
     {
        ++length;
        remaining /= 10;
     }
    while (remaining);

    if (static_cast<std::size_t>(last - first) < length) {
        return {last, std::errc::value_too_large};
    }

    auto next = first + length;
    const auto end = next;

    do
     {
        *--next = static_cast<char>('0' + magnitude % 10);
        magnitude /= 10;
     }
    while (magnitude);

    if (negative) {
        *first = '-';
    }

    return {end, std::errc {}};
}

} // ceph::salad::datum::compat::detail

namespace ceph::salad::datum::compat {

// Decimal output into caller storage; runtime always uses the standard conversion.
template <std::integral IntegerT>
requires (!std::same_as<std::remove_cv_t<IntegerT>, bool>)
constexpr std::to_chars_result to_decimal_chars(char *first, char *last, const IntegerT value)
{
#if !defined(__cpp_lib_constexpr_charconv) || __cpp_lib_constexpr_charconv < 202207L
    if consteval {
        return detail::decimal_chars(first, last, value);
    }
#endif

    return std::to_chars(first, last, value);
}

constexpr std::string decimal_string(const std::uint64_t value)
{
    std::array<char, 1 + std::numeric_limits<std::uint64_t>::digits10> digits {};
    const auto result = to_decimal_chars(std::begin(digits), std::end(digits), value);

    return {std::begin(digits), result.ptr};
}

} // ceph::salad::datum::compat

#endif // CEPH_RGW_DATUM_COMPAT_CHARCONV_H
