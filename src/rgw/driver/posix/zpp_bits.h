/*
MIT License

Copyright (c) 2021 Eyal Z

Permission is hereby granted, free of charge, to any person obtaining a copy
*/
#ifndef ZPP_BITS_H
#define ZPP_BITS_H

#include <algorithm>
#include <array>
#include <bit>
#include <climits>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <span>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#if __has_include("zpp_throwing.h")
#include "zpp_throwing.h"
#endif

#ifdef __cpp_exceptions
#include <stdexcept>
#endif

#ifndef ZPP_BITS_AUTODETECT_MEMBERS_MODE
#define ZPP_BITS_AUTODETECT_MEMBERS_MODE (0)
#endif

#ifndef ZPP_BITS_INLINE
#if defined __clang__ || defined __GNUC__
#define ZPP_BITS_INLINE __attribute__((always_inline))
#if defined __clang__
#define ZPP_BITS_CONSTEXPR_INLINE_LAMBDA __attribute__((always_inline)) constexpr
#else
#define ZPP_BITS_CONSTEXPR_INLINE_LAMBDA constexpr __attribute__((always_inline))
#endif
#elif defined _MSC_VER
#define ZPP_BITS_INLINE [[msvc::forceinline]]
#define ZPP_BITS_CONSTEXPR_INLINE_LAMBDA /*constexpr*/ [[msvc::forceinline]]
#endif
#else // ZPP_BITS_INLINE
#define ZPP_BITS_CONSTEXPR_INLINE_LAMBDA constexpr
#endif // ZPP_BITS_INLINE

#if defined ZPP_BITS_INLINE_MODE && !ZPP_BITS_INLINE_MODE
#undef ZPP_BITS_INLINE
#define ZPP_BITS_INLINE
#undef ZPP_BITS_CONSTEXPR_INLINE_LAMBDA
#define ZPP_BITS_CONSTEXPR_INLINE_LAMBDA constexpr
#endif

#ifndef ZPP_BITS_INLINE_DECODE_VARINT
#define ZPP_BITS_INLINE_DECODE_VARINT (0)
#endif

namespace zpp::bits
{
using default_size_type = std::uint32_t;

#ifndef __cpp_lib_bit_cast
namespace std
{
using namespace ::std;
template <class ToType,
          class FromType,
          class = enable_if_t<sizeof(ToType) == sizeof(FromType) &&
                              is_trivially_copyable_v<ToType> &&
                              is_trivially_copyable_v<FromType>>>
constexpr ToType bit_cast(FromType const & from) noexcept
{
    return __builtin_bit_cast(ToType, from);
}
} // namespace std
#endif

enum class kind
{
    in,
    out
};

template <std::size_t Count = std::numeric_limits<std::size_t>::max()>
struct members
{
    constexpr static std::size_t value = Count;
};

template <auto Protocol, std::size_t Members = std::numeric_limits<std::size_t>::max()>
struct protocol
{
    constexpr static auto value = Protocol;
    constexpr static auto members = Members;
};

template <auto Id>
struct serialization_id
{
    constexpr static auto value = Id;
};

constexpr auto success(std::errc code)
{
    return std::errc{} == code;
}

constexpr auto failure(std::errc code)
{
    return std::errc{} != code;
}

struct [[nodiscard]] errc
{
    constexpr errc(std::errc code = {}) : code(code)
    {
    }

#if __has_include("zpp_throwing.h")
    constexpr zpp::throwing<void> operator co_await() const
    {
        if (failure(code)) [[unlikely]] {
            return code;
        }
        return zpp::void_v;
    }
#endif

    constexpr operator std::errc() const
    {
        return code;
    }

    constexpr void or_throw() const
    {
        if (failure(code)) [[unlikely]] {
#ifdef __cpp_exceptions
            throw std::system_error(std::make_error_code(code));
#else
            std::abort();
#endif
        }
    }

    std::errc code;
};

constexpr auto success(errc code)
{
    return std::errc{} == code;
}

constexpr auto failure(errc code)
{
    return std::errc{} != code;
}

struct access
{
    struct any
    {
        template <typename Type>
        operator Type();
    };

    template <typename Item>
    constexpr static auto make(auto &&... arguments)
    {
        return Item{std::forward<decltype(arguments)>(arguments)...};
    }

    template <typename Item>
    constexpr static auto placement_new(void * address,
                                        auto &&... arguments)
    {
        return ::new (address)
            Item(std::forward<decltype(arguments)>(arguments)...);
    }

    template <typename Item>
    constexpr static auto make_unique(auto &&... arguments)
    {
        return std::unique_ptr<Item>(
            new Item(std::forward<decltype(arguments)>(arguments)...));
    }

    template <typename Item>
    constexpr static void destruct(Item & item)
    {
        item.~Item();
    }

    template <typename Type>
    constexpr static auto number_of_members();

    constexpr static auto max_visit_members = 50;

    ZPP_BITS_INLINE constexpr static decltype(auto) visit_members(
        auto && object,
        auto && visitor) requires(0 <=
                                  number_of_members<decltype(object)>()) &&
        (number_of_members<decltype(object)>() <= max_visit_members)
    {
        constexpr auto count = number_of_members<decltype(object)>();

        // clang-format off
        if constexpr (count == 0) { return visitor(); } else if constexpr (count == 1) { auto && [a1] = object; return visitor(a1); } else if constexpr (count == 2) { auto && [a1, a2] = object; return visitor(a1, a2); /*......................................................................................................................................................................................................................................................................*/ } else if constexpr (count == 3) { auto && [a1, a2, a3] = object; return visitor(a1, a2, a3); } else if constexpr (count == 4) { auto && [a1, a2, a3, a4] = object; return visitor(a1, a2, a3, a4); } else if constexpr (count == 5) { auto && [a1, a2, a3, a4, a5] = object; return visitor(a1, a2, a3, a4, a5); } else if constexpr (count == 6) { auto && [a1, a2, a3, a4, a5, a6] = object; return visitor(a1, a2, a3, a4, a5, a6); } else if constexpr (count == 7) { auto && [a1, a2, a3, a4, a5, a6, a7] = object; return visitor(a1, a2, a3, a4, a5, a6, a7); } else if constexpr (count == 8) { auto && [a1, a2, a3, a4, a5, a6, a7, a8] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8); } else if constexpr (count == 9) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9); } else if constexpr (count == 10) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10); } else if constexpr (count == 11) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11); } else if constexpr (count == 12) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12); } else if constexpr (count == 13) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13); } else if constexpr (count == 14) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14); } else if constexpr (count == 15) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15); } else if constexpr (count == 16) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16); } else if constexpr (count == 17) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17); } else if constexpr (count == 18) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18); } else if constexpr (count == 19) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19); } else if constexpr (count == 20) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20); } else if constexpr (count == 21) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21); } else if constexpr (count == 22) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22); } else if constexpr (count == 23) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23); } else if constexpr (count == 24) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24); } else if constexpr (count == 25) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25); } else if constexpr (count == 26) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26); } else if constexpr (count == 27) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27); } else if constexpr (count == 28) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28); } else if constexpr (count == 29) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29); } else if constexpr (count == 30) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30); } else if constexpr (count == 31) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31); } else if constexpr (count == 32) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32); } else if constexpr (count == 33) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33); } else if constexpr (count == 34) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34); } else if constexpr (count == 35) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35); } else if constexpr (count == 36) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36); } else if constexpr (count == 37) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37); } else if constexpr (count == 38) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38); } else if constexpr (count == 39) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39); } else if constexpr (count == 40) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40); } else if constexpr (count == 41) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41); } else if constexpr (count == 42) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42); } else if constexpr (count == 43) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43); } else if constexpr (count == 44) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44); } else if constexpr (count == 45) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45); } else if constexpr (count == 46) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46); } else if constexpr (count == 47) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47); } else if constexpr (count == 48) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48); } else if constexpr (count == 49) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49); } else if constexpr (count == 50) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50] = object; return visitor(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50);
            // Calls the visitor above with all data members of object.
        }
        // clang-format on
    }

    template <typename Type>
        constexpr static decltype(auto)
        visit_members_types(auto && visitor) requires(0 <= number_of_members<Type>()) &&
        (number_of_members<Type>() <= max_visit_members)

    {
        using type = std::remove_cvref_t<Type>;
        constexpr auto count = number_of_members<Type>();

        // clang-format off
        if constexpr (count == 0) { return visitor.template operator()<>(); } else if constexpr (count == 1) { auto f = [&](auto && object) { auto && [a1] = object; return visitor.template operator()<decltype(a1)>(); }; /*......................................................................................................................................................................................................................................................................*/ return decltype(f(std::declval<type>()))(); } else if constexpr (count == 2) { auto f = [&](auto && object) { auto && [a1, a2] = object; return visitor.template operator()<decltype(a1), decltype(a2)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 3) { auto f = [&](auto && object) { auto && [a1, a2, a3] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 4) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 5) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 6) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 7) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 8) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 9) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 10) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 11) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 12) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 13) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 14) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 15) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 16) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 17) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 18) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 19) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 20) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 21) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 22) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 23) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 24) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 25) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 26) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 27) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 28) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 29) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 30) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 31) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 32) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 33) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 34) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 35) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 36) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 37) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 38) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 39) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 40) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 41) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40), decltype(a41)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 42) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40), decltype(a41), decltype(a42)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 43) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40), decltype(a41), decltype(a42), decltype(a43)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 44) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40), decltype(a41), decltype(a42), decltype(a43), decltype(a44)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 45) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40), decltype(a41), decltype(a42), decltype(a43), decltype(a44), decltype(a45)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 46) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40), decltype(a41), decltype(a42), decltype(a43), decltype(a44), decltype(a45), decltype(a46)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 47) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40), decltype(a41), decltype(a42), decltype(a43), decltype(a44), decltype(a45), decltype(a46), decltype(a47)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 48) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40), decltype(a41), decltype(a42), decltype(a43), decltype(a44), decltype(a45), decltype(a46), decltype(a47), decltype(a48)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 49) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40), decltype(a41), decltype(a42), decltype(a43), decltype(a44), decltype(a45), decltype(a46), decltype(a47), decltype(a48), decltype(a49)>(); }; return decltype(f(std::declval<type>()))(); } else if constexpr (count == 50) { auto f = [&](auto && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50] = object; return visitor.template operator()<decltype(a1), decltype(a2), decltype(a3), decltype(a4), decltype(a5), decltype(a6), decltype(a7), decltype(a8), decltype(a9), decltype(a10), decltype(a11), decltype(a12), decltype(a13), decltype(a14), decltype(a15), decltype(a16), decltype(a17), decltype(a18), decltype(a19), decltype(a20), decltype(a21), decltype(a22), decltype(a23), decltype(a24), decltype(a25), decltype(a26), decltype(a27), decltype(a28), decltype(a29), decltype(a30), decltype(a31), decltype(a32), decltype(a33), decltype(a34), decltype(a35), decltype(a36), decltype(a37), decltype(a38), decltype(a39), decltype(a40), decltype(a41), decltype(a42), decltype(a43), decltype(a44), decltype(a45), decltype(a46), decltype(a47), decltype(a48), decltype(a49), decltype(a50)>(); }; return decltype(f(std::declval<type>()))();
            // Returns visitor.template operator()<member-types...>();
        }
        // clang-format on
    }

    constexpr static auto try_serialize(auto && item)
    {
        if constexpr (requires { serialize(item); }) {
            return serialize(item);
        }
    }

    template <typename Type, typename Archive>
    constexpr static auto has_serialize()
    {
        return requires {
                   requires std::same_as<
                       typename std::remove_cvref_t<Type>::serialize,
                       members<
                           std::remove_cvref_t<Type>::serialize::value>>;
               } ||
               requires(Type && item) {
                   requires std::same_as<
                       std::remove_cvref_t<decltype(try_serialize(
                           item))>,
                       members<std::remove_cvref_t<
                           decltype(try_serialize(item))>::value>>;
               } ||
               requires {
                   requires std::same_as<
                       typename std::remove_cvref_t<Type>::serialize,
                       protocol<
                           std::remove_cvref_t<Type>::serialize::value,
                           std::remove_cvref_t<Type>::serialize::members>>;
               } ||
               requires(Type && item) {
                   requires std::same_as<
                       std::remove_cvref_t<decltype(try_serialize(
                           item))>,
                       protocol<
                           std::remove_cvref_t<decltype(try_serialize(
                               item))>::value,
                           std::remove_cvref_t<decltype(try_serialize(
                               item))>::members>>;
               } ||
               requires(Type && item, Archive && archive) {
                   std::remove_cvref_t<Type>::serialize(archive, item);
               } || requires(Type && item, Archive && archive) {
                        serialize(archive, item);
                    };
    }

    template <typename Type, typename Archive>
    constexpr static auto has_explicit_serialize()
    {
        return requires(Type && item, Archive && archive)
        {
            std::remove_cvref_t<Type>::serialize(archive, item);
        }
        || requires(Type && item, Archive && archive)
        {
            serialize(archive, item);
        };
    }

    template <typename Type>
    struct byte_serializable_visitor;

    template <typename Type>
    constexpr static auto byte_serializable();

    template <typename Type>
    struct endian_independent_byte_serializable_visitor;

    template <typename Type>
    constexpr static auto endian_independent_byte_serializable();

    template <typename Type, typename Self, typename... Visited>
    struct self_referencing_visitor;

    template <typename Type, typename Self = Type, typename... Visited>
    constexpr static auto self_referencing();

    template <typename Type>
    constexpr static auto has_protocol()
    {
        return requires
        {
            requires std::same_as<
                typename std::remove_cvref_t<Type>::serialize,
                protocol<std::remove_cvref_t<Type>::serialize::value,
                         std::remove_cvref_t<Type>::serialize::members>>;
        }
        || requires(Type && item)
        {
            requires std::same_as<
                std::remove_cvref_t<decltype(try_serialize(item))>,
                protocol<
                    std::remove_cvref_t<decltype(try_serialize(item))>::value,
                    std::remove_cvref_t<decltype(try_serialize(
                        item))>::members>>;
        };
    }

    template <typename Type>
    constexpr static auto get_protocol()
    {
        if constexpr (
            requires {
                requires std::same_as<
                    typename std::remove_cvref_t<Type>::serialize,
                    protocol<
                        std::remove_cvref_t<Type>::serialize::value,
                        std::remove_cvref_t<Type>::serialize::members>>;
            }) {
            return std::remove_cvref_t<Type>::serialize::value;
        } else if constexpr (
            requires(Type && item) {
                requires std::same_as<
                    std::remove_cvref_t<decltype(try_serialize(item))>,
                    protocol<std::remove_cvref_t<decltype(try_serialize(
                                 item))>::value,
                             std::remove_cvref_t<decltype(try_serialize(
                                 item))>::members>>;
            }) {
            return std::remove_cvref_t<decltype(try_serialize(
                std::declval<Type>()))>::value;
        } else {
            static_assert(!sizeof(Type));
        }
    }
};

template <typename Type>
struct destructor_guard
{
    ZPP_BITS_INLINE constexpr ~destructor_guard()
    {
        access::destruct(object);
    }

    Type & object;
};

template <typename Type>
destructor_guard(Type) -> destructor_guard<Type>;

namespace traits
{
template <typename Type>
struct is_unique_ptr : std::false_type
{
};

template <typename Type>
struct is_unique_ptr<std::unique_ptr<Type, std::default_delete<Type>>>
    : std::true_type
{
};

template <typename Type>
struct is_shared_ptr : std::false_type
{
};

template <typename Type>
struct is_shared_ptr<std::shared_ptr<Type>> : std::true_type
{
};

template <typename Variant>
struct variant_impl;

template <typename... Types, template <typename...> typename Variant>
struct variant_impl<Variant<Types...>>
{
    using variant_type = Variant<Types...>;

    template <std::size_t Index,
              std::size_t CurrentIndex,
              typename FirstType,
              typename... OtherTypes>
    constexpr static auto get_id()
    {
        if constexpr (Index == CurrentIndex) {
            if constexpr (requires {
                              requires std::same_as<
                                  serialization_id<
                                      FirstType::serialize_id::value>,
                              typename FirstType::serialize_id>;
                          }) {
                return FirstType::serialize_id::value;
            } else if constexpr (
                requires {
                    requires std::same_as<
                        serialization_id<decltype(serialize_id(
                            std::declval<FirstType>()))::value>,
                    decltype(serialize_id(std::declval<FirstType>()))>;
                }) {
                return decltype(serialize_id(
                    std::declval<FirstType>()))::value;
            } else {
                return std::byte{Index};
            }
        } else {
            return get_id<Index, CurrentIndex + 1, OtherTypes...>();
        }
    }

    template <std::size_t Index>
    constexpr static auto id()
    {
        return get_id<Index, 0, Types...>();
    }

    template <std::size_t CurrentIndex = 0>
    ZPP_BITS_INLINE constexpr static auto id(auto index)
    {
        if constexpr (CurrentIndex == (sizeof...(Types) - 1)) {
            return id<CurrentIndex>();
        } else {
            if (index == CurrentIndex) {
                return id<CurrentIndex>();
            } else {
                return id<CurrentIndex + 1>(index);
            }
        }
    }

    template <auto Id, std::size_t CurrentIndex = 0>
    constexpr static std::size_t index()
    {
        static_assert(CurrentIndex < sizeof...(Types));

        if constexpr (variant_impl::id<CurrentIndex>() == Id) {
            return CurrentIndex;
        } else {
            return index<Id, CurrentIndex + 1>();
        }
    }

    template <std::size_t CurrentIndex = 0>
    ZPP_BITS_INLINE constexpr static std::size_t index(auto && id)
    {
        if constexpr (CurrentIndex == sizeof...(Types)) {
            return std::numeric_limits<std::size_t>::max();
        } else {
            if (variant_impl::id<CurrentIndex>() == id) {
                return CurrentIndex;
            } else {
                return index<CurrentIndex + 1>(id);
            }
        }
        return std::numeric_limits<std::size_t>::max();
    }

    template <std::size_t... LeftIndices, std::size_t... RightIndices>
    constexpr static auto unique_ids(std::index_sequence<LeftIndices...>,
                                     std::index_sequence<RightIndices...>)
    {
        auto unique_among_rest = []<auto LeftIndex, auto LeftId>()
        {
            return (... && ((LeftIndex == RightIndices) ||
                            (LeftId != id<RightIndices>())));
        };
        return (... && unique_among_rest.template
                       operator()<LeftIndices, id<LeftIndices>()>());
    }

    template <std::size_t... LeftIndices, std::size_t... RightIndices>
    constexpr static auto
    same_id_types(std::index_sequence<LeftIndices...>,
                  std::index_sequence<RightIndices...>)
    {
        auto same_among_rest = []<auto LeftIndex, auto LeftId>()
        {
            return (... &&
                    (std::same_as<
                        std::remove_cv_t<decltype(LeftId)>,
                        std::remove_cv_t<decltype(id<RightIndices>())>>));
        };
        return (... && same_among_rest.template
                       operator()<LeftIndices, id<LeftIndices>()>());
    }

    template <typename Type, std::size_t... Indices>
    constexpr static std::size_t index_by_type(std::index_sequence<Indices...>)
    {
        return ((std::same_as<
                     Type,
                     std::variant_alternative_t<Indices, variant_type>> *
                 Indices) +
                ...);
    }

    template <typename Type>
    constexpr static std::size_t index_by_type()
    {
        return index_by_type<Type>(
            std::make_index_sequence<std::variant_size_v<variant_type>>{});
    }

    using id_type = decltype(id<0>());
};

template <typename Variant>
struct variant_checker;

template <typename... Types, template <typename...> typename Variant>
struct variant_checker<Variant<Types...>>
{
    using type = variant_impl<Variant<Types...>>;
    static_assert(
        type::unique_ids(std::make_index_sequence<sizeof...(Types)>(),
                   std::make_index_sequence<sizeof...(Types)>()));
    static_assert(
        type::same_id_types(std::make_index_sequence<sizeof...(Types)>(),
                            std::make_index_sequence<sizeof...(Types)>()));
};

template <typename Variant>
using variant = typename variant_checker<Variant>::type;

template <typename Tuple>
struct tuple;

template <typename... Types, template <typename...> typename Tuple>
struct tuple<Tuple<Types...>>
{
    template <std::size_t Index = 0>
    ZPP_BITS_INLINE constexpr static auto visit(auto && tuple, auto && index, auto && visitor)
    {
        if constexpr (Index + 1 == sizeof...(Types)) {
            return visitor(std::get<Index>(tuple));
        } else {
            if (Index == index) {
                return visitor(std::get<Index>(tuple));
            }
            return visit<Index + 1>(tuple, index, visitor);
        }
    }
};

template <typename Type, typename Visitor = std::monostate>
struct visitor
{
    using byte_type = std::byte;
    using view_type = std::span<std::byte>;

    static constexpr bool resizable = false;

    constexpr auto operator()(auto && ... arguments) const
    {
        if constexpr (requires {
                          visitor(std::forward<decltype(arguments)>(
                              arguments)...);
                      }) {
            return visitor(
                std::forward<decltype(arguments)>(arguments)...);
        } else {
            return sizeof...(arguments);
        }
    }

    template <typename...>
    constexpr auto serialize_one(auto && ... arguments) const
    {
        return (*this)(std::forward<decltype(arguments)>(arguments)...);
    }

    template <typename...>
    constexpr auto serialize_many(auto && ... arguments) const
    {
        return (*this)(std::forward<decltype(arguments)>(arguments)...);
    }

    constexpr static auto kind()
    {
        return kind::out;
    }

    std::span<std::byte> data();
    std::span<std::byte> remaining_data();
    std::span<std::byte> processed_data();
    std::size_t position() const;
    std::size_t & position();
    errc enlarge_for(std::size_t);
    void reset(std::size_t = 0);

    [[no_unique_address]] Visitor visitor;
};

constexpr auto get_default_size_type()
{
    return default_size_type{};
}

constexpr auto get_default_size_type(auto option, auto... options)
{
    if constexpr (requires {
                      typename decltype(option)::default_size_type;
                  }) {
        if constexpr (std::is_void_v<typename decltype(option)::default_size_type>) {
            return std::monostate{};
        } else {
            return typename decltype(option)::default_size_type{};
        }
    } else {
        return get_default_size_type(options...);
    }
}

template <typename... Options>
using default_size_type_t =
    std::conditional_t<std::same_as<std::monostate,
                                    decltype(get_default_size_type(
                                        std::declval<Options>()...))>,
                       void,
                       decltype(get_default_size_type(
                           std::declval<Options>()...))>;

template <typename Option, typename... Options>
constexpr auto get_alloc_limit()
{
    if constexpr (requires {
                      std::remove_cvref_t<
                          Option>::alloc_limit_value;
                  }) {
        return std::remove_cvref_t<Option>::alloc_limit_value;
    } else if constexpr (sizeof...(Options) != 0) {
        return get_alloc_limit<Options...>();
    } else {
        return std::numeric_limits<std::size_t>::max();
    }
}

template <typename... Options>
constexpr auto alloc_limit()
{
    if constexpr (sizeof...(Options) != 0) {
        return get_alloc_limit<Options...>();
    } else {
        return std::numeric_limits<std::size_t>::max();
    }
}

template <typename Option, typename... Options>
constexpr auto get_enlarger()
{
    if constexpr (requires {
                      std::remove_cvref_t<
                          Option>::enlarger_value;
                  }) {
        return std::remove_cvref_t<Option>::enlarger_value;
    } else if constexpr (sizeof...(Options) != 0) {
        return get_enlarger<Options...>();
    } else {
        return std::tuple{3, 2};
    }
}

template <typename... Options>
constexpr auto enlarger()
{
    if constexpr (sizeof...(Options) != 0) {
        return get_enlarger<Options...>();
    } else {
        return std::tuple{3, 2};
    }
}

template <typename Type>
constexpr auto underlying_type_generic()
{
    if constexpr (std::is_enum_v<Type>) {
        return std::underlying_type_t<Type>{};
    } else {
        return Type{};
    }
}

template <typename Type>
using underlying_type_t = decltype(underlying_type_generic<Type>());

template <typename Id>
struct id_serializable
{
    using serialize_id = Id;
};

constexpr auto unique(auto && ... values)
{
    auto unique_among_rest = [](auto && value, auto && ... values)
    {
        return (... && ((&value == &values) ||
                        (value != values)));
    };
    return (... && unique_among_rest(values, values...));
}
} // namespace traits

namespace concepts
{
template <typename Type>
concept byte_type = std::same_as<std::remove_cv_t<Type>, char> ||
                    std::same_as<std::remove_cv_t<Type>, unsigned char> ||
                    std::same_as<std::remove_cv_t<Type>, std::byte>;

template <typename Type>
concept byte_view = byte_type<typename std::remove_cvref_t<Type>::value_type> &&
    requires(Type value)
{
    value.data();
    value.size();
};

template <typename Type>
concept has_serialize =
    access::has_serialize<Type,
                          traits::visitor<std::remove_cvref_t<Type>>>();

template <typename Type>
concept has_explicit_serialize = access::has_explicit_serialize<
    Type,
    traits::visitor<std::remove_cvref_t<Type>>>();

template <typename Type>
concept variant = !has_serialize<Type> && requires (Type variant) {
    variant.index();
    std::get_if<0>(&variant);
    std::variant_size_v<std::remove_cvref_t<Type>>;
};

template <typename Type>
concept optional = !has_serialize<Type> && requires (Type optional) {
    optional.value();
    optional.has_value();
    optional.operator bool();
    optional.operator*();
};

template <typename Type>
concept container =
    !has_serialize<Type> && !optional<Type> && requires(Type container)
{
    typename std::remove_cvref_t<Type>::value_type;
    container.size();
    container.begin();
    container.end();
};

template <typename Type>
concept associative_container = container<Type> && requires(Type container)
{
    typename std::remove_cvref_t<Type>::key_type;
};

template <typename Type>
concept tuple = !has_serialize<Type> && !container<Type> && requires(Type tuple)
{
    sizeof(std::tuple_size<std::remove_cvref_t<Type>>);
}
&&!requires(Type tuple)
{
    tuple.index();
};

template <typename Type>
concept owning_pointer = !optional<Type> &&
    (traits::is_unique_ptr<std::remove_cvref_t<Type>>::value ||
    traits::is_shared_ptr<std::remove_cvref_t<Type>>::value);

template <typename Type>
concept bitset =
    !has_serialize<Type> && requires(std::remove_cvref_t<Type> bitset)
{
    bitset.flip();
    bitset.set();
    bitset.test(0);
    bitset.to_ullong();
};

template <typename Type>
concept has_protocol = access::has_protocol<Type>();

template <typename Type>
concept by_protocol = has_protocol<Type> && !has_explicit_serialize<Type>;

template <typename Type>
concept basic_array = std::is_array_v<std::remove_cvref_t<Type>>;

template <typename Type>
concept unspecialized =
    !container<Type> && !owning_pointer<Type> && !tuple<Type> &&
    !variant<Type> && !optional<Type> && !bitset<Type> &&
    !std::is_array_v<std::remove_cvref_t<Type>> && !by_protocol<Type>;

template <typename Type>
concept empty = requires
{
    std::integral_constant<std::size_t, sizeof(Type)>::value;
    requires std::is_empty_v<std::remove_cvref_t<Type>>;
};

template <typename Type>
concept byte_serializable = access::byte_serializable<Type>();

template <typename Type>
concept endian_independent_byte_serializable =
    access::endian_independent_byte_serializable<Type>();

template <typename Archive>
concept endian_aware_archive = requires
{
    requires std::remove_cvref_t<Archive>::endian_aware;
};

template <typename Archive, typename Type>
concept serialize_as_bytes = endian_independent_byte_serializable<Type> ||
    (!endian_aware_archive<Archive> && byte_serializable<Type>);

template <typename Type, typename Reference>
concept type_references = requires
{
    requires container<Type>;
    requires std::same_as<typename std::remove_cvref_t<Type>::value_type,
                          std::remove_cvref_t<Reference>>;
}
|| requires
{
    requires associative_container<Type>;
    requires std::same_as<typename std::remove_cvref_t<Type>::key_type,
                          std::remove_cvref_t<Reference>>;
}
|| requires
{
    requires associative_container<Type>;
    requires std::same_as<typename std::remove_cvref_t<Type>::mapped_type,
                          std::remove_cvref_t<Reference>>;
}
|| requires (Type && value)
{
    requires owning_pointer<Type>;
    requires std::same_as<std::remove_cvref_t<decltype(*value)>,
                          std::remove_cvref_t<Reference>>;
}
|| requires (Type && value)
{
    requires optional<Type>;
    requires std::same_as<std::remove_cvref_t<decltype(*value)>,
                          std::remove_cvref_t<Reference>>;
};

template <typename Type>
concept self_referencing = access::self_referencing<Type>();

template <typename Type>
concept has_fixed_nonzero_size = requires
{
    requires std::integral_constant<std::size_t,
        std::remove_cvref_t<Type>{}.size()>::value != 0;
};

template <typename Type>
concept array =
    basic_array<Type> ||
    (container<Type> && has_fixed_nonzero_size<Type> && requires {
        requires Type {
        }
        .size() * sizeof(typename Type::value_type) == sizeof(Type);
        Type{}.data();
    });

} // namespace concepts

template <typename CharType, std::size_t Size>
struct string_literal : public std::array<CharType, Size + 1>
{
    using base = std::array<CharType, Size + 1>;
    using value_type = typename base::value_type;
    using pointer = typename base::pointer;
    using const_pointer = typename base::const_pointer;
    using iterator = typename base::iterator;
    using const_iterator = typename base::const_iterator;
    using reference = typename base::const_pointer;
    using const_reference = typename base::const_pointer;
    using size_type = default_size_type;

    constexpr string_literal() = default;
    constexpr string_literal(const CharType (&value)[Size + 1])
    {
        std::copy_n(std::begin(value), Size + 1, std::begin(*this));
    }

    constexpr auto operator<=>(const string_literal &) const = default;

    constexpr default_size_type size() const
    {
        return Size;
    }

    constexpr bool empty() const
    {
        return !Size;
    }

    using base::begin;

    constexpr auto end()
    {
        return base::end() - 1;
    }

    constexpr auto end() const
    {
        return base::end() - 1;
    }

    using base::data;
    using base::operator[];
    using base::at;

private:
    using base::cbegin;
    using base::cend;
    using base::rbegin;
    using base::rend;
};

template <typename CharType, std::size_t Size>
string_literal(const CharType (&value)[Size])
    -> string_literal<CharType, Size - 1>;

template <typename Item>
class bytes
{
public:
    using value_type = Item;

    constexpr explicit bytes(std::span<Item> items) :
        m_items(items.data()), m_size(items.size())
    {
    }

    constexpr explicit bytes(std::span<Item> items, auto size) :
        m_items(items.data()), m_size(std::size_t(size))
    {
    }

    constexpr auto data() const
    {
        return m_items;
    }

    constexpr std::size_t size_in_bytes() const
    {
        return m_size * sizeof(Item);
    }

    constexpr std::size_t count() const
    {
        return m_size;
    }

private:
    static_assert(std::is_trivially_copyable_v<Item>);

    Item * m_items;
    std::size_t m_size;
};

template <typename Item>
bytes(std::span<Item>) -> bytes<Item>;

template <typename Item>
bytes(std::span<Item>, std::size_t) -> bytes<Item>;

template <typename Item, std::size_t Count>
bytes(Item(&)[Count]) -> bytes<Item>;

template <concepts::container Container>
bytes(Container && container)
    -> bytes<std::remove_reference_t<decltype(container[0])>>;

template <concepts::container Container>
bytes(Container && container, std::size_t)
    -> bytes<std::remove_reference_t<decltype(container[0])>>;

constexpr auto as_bytes(auto && object)
{
    return bytes(std::span{&object, 1});
}

template <typename Option>
struct option
{
    using zpp_bits_option = void;
    constexpr auto operator()(auto && archive)
    {
        if constexpr (requires {
                          archive.option(static_cast<Option &>(*this));
                      }) {
            archive.option(static_cast<Option &>(*this));
        }
    }
};

inline namespace options
{
struct append : option<append>
{
};

struct reserve : option<reserve>
{
    constexpr explicit reserve(std::size_t size) : size(size)
    {
    }
    std::size_t size{};
};

struct resize : option<resize>
{
    constexpr explicit resize(std::size_t size) : size(size)
    {
    }
    std::size_t size{};
};

template <std::size_t Size>
struct alloc_limit : option<alloc_limit<Size>>
{
    constexpr static auto alloc_limit_value = Size;
};

template <std::size_t Multiplier, std::size_t Divisor = 1>
struct enlarger : option<enlarger<Multiplier, Divisor>>
{
    constexpr static auto enlarger_value =
        std::tuple{Multiplier, Divisor};
};

using exact_enlarger = enlarger<1, 1>;

namespace endian
{
struct big : option<big>
{
    constexpr static auto value = std::endian::big;
};

struct little : option<little>
{
    constexpr static auto value = std::endian::little;
};

using network = big;

using native = std::
    conditional_t<std::endian::native == std::endian::little, little, big>;

using swapped = std::
    conditional_t<std::endian::native == std::endian::little, big, little>;
} // namespace endian

struct no_fit_size : option<no_fit_size>
{
};

struct no_enlarge_overflow : option<no_enlarge_overflow>
{
};

struct enlarge_overflow : option<enlarge_overflow>
{
};

struct no_size : option<no_size>
{
    using default_size_type = void;
};

struct size1b : option<size1b>
{
    using default_size_type = unsigned char;
};

struct size2b : option<size2b>
{
    using default_size_type = std::uint16_t;
};

struct size4b : option<size4b>
{
    using default_size_type = std::uint32_t;
};

struct size8b : option<size8b>
{
    using default_size_type = std::uint64_t;
};

struct size_native : option<size_native>
{
    using default_size_type = std::size_t;
};
} // namespace options

template <typename Type>
constexpr auto access::number_of_members()
{
    using type = std::remove_cvref_t<Type>;
    if constexpr (std::is_array_v<type>) {
        return std::extent_v<type>;
    } else if constexpr (!std::is_class_v<type>) {
        return 0;
    } else if constexpr (concepts::container<type> &&
                         concepts::has_fixed_nonzero_size<type>) {
        return type{}.size();
    } else if constexpr (concepts::tuple<type>) {
        return std::tuple_size_v<type>;
    } else if constexpr (requires {
                             requires std::same_as<
                                 typename type::serialize,
                                 members<type::serialize::value>>;
                             requires type::serialize::value !=
                                 std::numeric_limits<
                                     std::size_t>::max();
                         }) {
        return type::serialize::value;
    } else if constexpr (requires(Type && item) {
                             requires std::same_as<
                                 decltype(try_serialize(item)),
                                 members<decltype(try_serialize(
                                     item))::value>>;
                             requires decltype(try_serialize(
                                 item))::value !=
                                 std::numeric_limits<
                                     std::size_t>::max();
                         }) {
        return decltype(serialize(std::declval<type>()))::value;
    } else if constexpr (requires {
                             requires std::same_as<
                                 typename type::serialize,
                                 protocol<type::serialize::value,
                                          type::serialize::members>>;
                             requires type::serialize::members !=
                                 std::numeric_limits<
                                     std::size_t>::max();
                         }) {
        return type::serialize::members;
    } else if constexpr (requires(Type && item) {
                             requires std::same_as<
                                 decltype(try_serialize(item)),
                                 protocol<decltype(try_serialize(item))::value,
                                          decltype(try_serialize(
                                              item))::members>>;
                             requires decltype(try_serialize(
                                 item))::members !=
                                 std::numeric_limits<
                                     std::size_t>::max();
                         }) {
        return decltype(serialize(std::declval<type>()))::members;
#if ZPP_BITS_AUTODETECT_MEMBERS_MODE == 0
    } else if constexpr (std::is_aggregate_v<type>) {
        // clang-format off
        if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{},  /*.................................................................................................................*/ any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 50; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 49; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 48; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 47; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 46; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 45; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 44; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 43; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 42; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 41; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 40; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 39; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 38; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 37; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 36; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 35; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 34; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 33; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 32; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 31; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 30; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 29; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 28; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 27; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 26; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 25; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 24; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 23; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 22; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 21; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 20; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 19; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 18; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 17; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 16; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 15; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 14; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 13; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 12; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 11; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 10; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 9; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 8; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 7; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}, any{}}; }) { return 6; } else if constexpr (requires { type{any{}, any{}, any{}, any{}, any{}}; }) { return 5; } else if constexpr (requires { type{any{}, any{}, any{}, any{}}; }) { return 4; } else if constexpr (requires { type{any{}, any{}, any{}}; }) { return 3; } else if constexpr (requires { type{any{}, any{}}; }) { return 2; } else if constexpr (requires { type{any{}}; }) { return 1;
            // Returns the number of members
            // clang-format on
        } else if constexpr (concepts::empty<type> && requires {
                                 typename std::void_t<decltype(type{})>;
                             }) {
            return 0;
        } else {
            return -1;
        }
#elif ZPP_BITS_AUTODETECT_MEMBERS_MODE > 0
#if ZPP_BITS_AUTODETECT_MEMBERS_MODE == 1
        // clang-format off
    } else if constexpr (requires { [](Type && object) { auto && [a1] = object; }; }) { return 1; } else if constexpr (requires { [](Type && object) { auto && [a1, a2] = object; }; }) { return 2; /*.................................................................................................................*/ } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3] = object; }; }) { return 3; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4] = object; }; }) { return 4; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5] = object; }; }) { return 5; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6] = object; }; }) { return 6; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7] = object; }; }) { return 7; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8] = object; }; }) { return 8; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9] = object; }; }) { return 9; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10] = object; }; }) { return 10; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11] = object; }; }) { return 11; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12] = object; }; }) { return 12; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13] = object; }; }) { return 13; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14] = object; }; }) { return 14; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15] = object; }; }) { return 15; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16] = object; }; }) { return 16; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17] = object; }; }) { return 17; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18] = object; }; }) { return 18; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19] = object; }; }) { return 19; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20] = object; }; }) { return 20; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21] = object; }; }) { return 21; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22] = object; }; }) { return 22; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23] = object; }; }) { return 23; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24] = object; }; }) { return 24; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25] = object; }; }) { return 25; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26] = object; }; }) { return 26; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27] = object; }; }) { return 27; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28] = object; }; }) { return 28; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29] = object; }; }) { return 29; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30] = object; }; }) { return 30; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31] = object; }; }) { return 31; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32] = object; }; }) { return 32; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33] = object; }; }) { return 33; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34] = object; }; }) { return 34; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35] = object; }; }) { return 35; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36] = object; }; }) { return 36; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37] = object; }; }) { return 37; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38] = object; }; }) { return 38; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39] = object; }; }) { return 39; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40] = object; }; }) { return 40; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41] = object; }; }) { return 41; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42] = object; }; }) { return 42; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43] = object; }; }) { return 43; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44] = object; }; }) { return 44; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45] = object; }; }) { return 45; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46] = object; }; }) { return 46; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47] = object; }; }) { return 47; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48] = object; }; }) { return 48; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49] = object; }; }) { return 49; } else if constexpr (requires { [](Type && object) { auto && [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50] = object; }; }) { return 50;
        // Returns the number of members
        // clang-format on
#else // ZPP_BITS_AUTODETECT_MEMBERS_MODE == 1
#error "Invalid value for ZPP_BITS_AUTODETECT_MEMBERS_MODE"
#endif
#endif
    } else {
        return -1;
    }
}

template <typename Type>
struct access::byte_serializable_visitor
{
    template <typename... Types>
    constexpr auto operator()() {
        using type = std::remove_cvref_t<Type>;

        if constexpr (concepts::empty<type>) {
            return std::false_type{};
        } else if constexpr ((... || has_explicit_serialize<
                                         Types,
                                         traits::visitor<Types>>())) {
            return std::false_type{};
        } else if constexpr ((... || !byte_serializable<Types>())) {
            return std::false_type{};
        } else if constexpr ((0 + ... + sizeof(Types)) != sizeof(type)) {
            return std::false_type{};
        } else if constexpr ((... || concepts::empty<Types>)) {
            return std::false_type{};
        } else {
            return std::true_type{};
        }
    }
};

template <typename Type>
constexpr auto access::byte_serializable()
{
    constexpr auto members_count = number_of_members<Type>();
    using type = std::remove_cvref_t<Type>;

    if constexpr (members_count < 0) {
        return false;
    } else if constexpr (!std::is_trivially_copyable_v<type>) {
        return false;
    } else if constexpr (has_explicit_serialize<type,
                                                traits::visitor<type>>()) {
        return false;
    } else if constexpr (
        !requires {
            requires std::integral_constant<
                int,
                (std::bit_cast<std::remove_all_extents_t<type>>(
                     std::array<
                         std::byte,
                         sizeof(std::remove_all_extents_t<type>)>()),
                 0)>::value == 0;
        }) {
        return false;
    } else if constexpr (concepts::array<type>) {
        return byte_serializable<
            std::remove_cvref_t<decltype(std::declval<type>()[0])>>();
    } else if constexpr (members_count > 0) {
        return visit_members_types<type>(
            byte_serializable_visitor<type>{})();
    } else {
        return true;
    }
}

template <typename Type>
struct access::endian_independent_byte_serializable_visitor
{
    template <typename... Types>
    constexpr auto operator()() {
        using type = std::remove_cvref_t<Type>;

        if constexpr (concepts::empty<type>) {
            return std::false_type{};
        } else if constexpr ((... || has_explicit_serialize<
                                         Types,
                                         traits::visitor<Types>>())) {
            return std::false_type{};
        } else if constexpr ((... || !endian_independent_byte_serializable<Types>())) {
            return std::false_type{};
        } else if constexpr ((0 + ... + sizeof(Types)) != sizeof(type)) {
            return std::false_type{};
        } else if constexpr ((... || concepts::empty<Types>)) {
            return std::false_type{};
        } else if constexpr (!concepts::byte_type<type>) {
            return std::false_type{};
        } else {
            return std::true_type{};
        }
    }
};

template <typename Type>
constexpr auto access::endian_independent_byte_serializable()
{
    constexpr auto members_count = number_of_members<Type>();
    using type = std::remove_cvref_t<Type>;

    if constexpr (members_count < 0) {
        return false;
    } else if constexpr (!std::is_trivially_copyable_v<type>) {
        return false;
    } else if constexpr (has_explicit_serialize<type,
                                                traits::visitor<type>>()) {
        return false;
    } else if constexpr (
        !requires {
            requires std::integral_constant<
                int,
                (std::bit_cast<std::remove_all_extents_t<type>>(
                     std::array<
                         std::byte,
                         sizeof(std::remove_all_extents_t<type>)>()),
                 0)>::value == 0;
        }) {
        return false;
    } else if constexpr (concepts::array<type>) {
        return endian_independent_byte_serializable<
            std::remove_cvref_t<decltype(std::declval<type>()[0])>>();
    } else if constexpr (members_count > 0) {
        return visit_members_types<type>(
            endian_independent_byte_serializable_visitor<type>{})();
    } else {
        return concepts::byte_type<type>;
    }
}

template <typename Type, typename Self, typename... Visited>
struct access::self_referencing_visitor
{
    template <typename... Types>
    constexpr auto operator()() {
        using type = std::remove_cvref_t<Type>;
        using self = std::remove_cvref_t<Self>;

        if constexpr (concepts::empty<type>) {
            return std::false_type{};
        } else if constexpr ((... || concepts::type_references<
                                         std::remove_cvref_t<Types>,
                                         self>)) {
            return std::true_type{};
        } else if constexpr ((sizeof...(Visited) != 0) &&
                             (... || std::same_as<type, Visited>)) {
            return std::false_type{};
        } else if constexpr ((... ||
                              self_referencing<std::remove_cvref_t<Types>,
                                               self,
                                               type,
                                               Visited...>())) {
            return std::true_type{};
        } else {
            return std::false_type{};
        }
    }
};

template <typename Type, typename Self/* = Type*/, typename... Visited>
constexpr auto access::self_referencing()
{
    constexpr auto members_count = number_of_members<Type>();
    using type = std::remove_cvref_t<Type>;
    using self = std::remove_cvref_t<Self>;

    if constexpr (members_count < 0) {
        return false;
    } else if constexpr (has_explicit_serialize<type,
                                                traits::visitor<type>>()) {
        return false;
    } else if constexpr (members_count == 0) {
        return false;
    } else if constexpr (concepts::array<type>) {
        return self_referencing<
            std::remove_cvref_t<decltype(std::declval<type>()[0])>,
            self,
            Visited...>();
    } else {
        return visit_members_types<type>(
            self_referencing_visitor<type, self, Visited...>{})();
    }
}

template <typename Type>
constexpr auto number_of_members()
{
    return access::number_of_members<Type>();
}

ZPP_BITS_INLINE constexpr decltype(auto) visit_members(auto && object,
                                                       auto && visitor)
{
    return access::visit_members(object, visitor);
}

template <typename Type>
constexpr decltype(auto) visit_members_types(auto && visitor)
{
    return access::visit_members_types<Type>(visitor);
}

template <typename Type>
struct optional_ptr : std::unique_ptr<Type>
{
    using base = std::unique_ptr<Type>;
    using base::base;
    using base::operator=;

    constexpr optional_ptr(base && other) noexcept :
        base(std::move(other))
    {
    }
};

template <typename Type, typename...>
optional_ptr(Type *) -> optional_ptr<Type>;

template <typename Archive, typename Type>
ZPP_BITS_INLINE constexpr static auto serialize(
    Archive & archive,
    const optional_ptr<Type> & self) requires(Archive::kind() == kind::out)
{
    if (!self) [[unlikely]] {
        return archive(std::byte(false));
    } else {
        return archive(std::byte(true), *self);
    }
}

template <typename Archive, typename Type>
ZPP_BITS_INLINE constexpr static auto
serialize(Archive & archive,
          optional_ptr<Type> & self) requires(Archive::kind() == kind::in)
{
    std::byte has_value{};
    if (auto result = archive(has_value); failure(result))
        [[unlikely]] {
        return result;
    }

    if (!bool(has_value)) [[unlikely]] {
        self = {};
        return errc{};
    }

    if (auto result =
            archive(static_cast<std::unique_ptr<Type> &>(self));
        failure(result)) [[unlikely]] {
        return result;
    }

    return errc{};
}

template <typename Type, typename SizeType>
struct sized_item : public Type
{
    using Type::Type;
    using Type::operator=;

    constexpr sized_item(Type && other) noexcept(
        std::is_nothrow_move_constructible_v<Type>) :
        Type(std::move(other))
    {
    }

    constexpr sized_item(const Type & other) :
        Type(other)
    {
    }

    ZPP_BITS_INLINE constexpr static auto serialize(auto & archive,
                                                    auto & self)
    {
        if constexpr (std::remove_cvref_t<decltype(archive)>::kind() == kind::out) {
            return archive.template serialize_one<SizeType>(
                static_cast<const Type &>(self));
        } else {
            return archive.template serialize_one<SizeType>(
                static_cast<Type &>(self));
        }
    }
};

template <typename Type, typename SizeType>
auto serialize(const sized_item<Type, SizeType> &)
    -> members<number_of_members<Type>()>;

template <typename Type, typename SizeType>
using sized_t = sized_item<Type, SizeType>;

template <typename Type>
using unsized_t = sized_t<Type, void>;

template <typename Type, typename SizeType>
struct sized_item_ref
{
    constexpr explicit sized_item_ref(Type && value) :
        value(std::forward<Type>(value))
    {
    }

    ZPP_BITS_INLINE constexpr static auto serialize(auto & serializer,
                                                    auto & self)
    {
        return serializer.template serialize_one<SizeType>(self.value);
    }

    Type && value;
};

template <typename SizeType, typename Type>
constexpr auto sized(Type && value)
{
    return sized_item_ref<Type &, SizeType>(value);
}

template <typename Type>
constexpr auto unsized(Type && value)
{
    return sized_item_ref<Type &, void>(value);
}

enum class varint_encoding
{
    normal,
    zig_zag,
};

template <typename Type, varint_encoding Encoding = varint_encoding::normal>
struct varint
{
    varint() = default;

    using value_type = Type;
    static constexpr auto encoding = Encoding;

    constexpr varint(Type value) : value(value)
    {
    }

    constexpr operator Type &() &
    {
        return value;
    }

    constexpr operator Type() const
    {
        return value;
    }

    constexpr decltype(auto) operator*() &
    {
        return (value);
    }

    constexpr auto operator*() const &
    {
        return value;
    }

    Type value{};
};

namespace concepts
{

template <typename Type>
concept varint = requires
{
    requires std::same_as<
        Type,
        zpp::bits::varint<typename Type::value_type, Type::encoding>>;
};

} // namespace concepts

template <typename Type>
constexpr auto varint_max_size = sizeof(Type) * CHAR_BIT / (CHAR_BIT - 1) +
                                 1;

template <varint_encoding Encoding = varint_encoding::normal>
ZPP_BITS_INLINE constexpr auto varint_size(auto value)
{
    if constexpr (Encoding == varint_encoding::zig_zag) {
        return varint_size(std::make_unsigned_t<decltype(value)>((value << 1) ^
                           (value >> (sizeof(value) * CHAR_BIT - 1))));
    } else {
        return ((sizeof(value) * CHAR_BIT) -
                std::countl_zero(
                    std::make_unsigned_t<decltype(value)>(value | 0x1)) +
                (CHAR_BIT - 2)) /
               (CHAR_BIT - 1);
    }
}

template <typename Archive, typename Type, varint_encoding Encoding>
ZPP_BITS_INLINE constexpr auto serialize(
    Archive & archive,
    varint<Type, Encoding> self) requires(Archive::kind() == kind::out)
{
    auto orig_value = std::conditional_t<std::is_enum_v<Type>,
                                         traits::underlying_type_t<Type>,
                                         Type>(self.value);
    auto value = std::make_unsigned_t<Type>(orig_value);
    if constexpr (varint_encoding::zig_zag == Encoding) {
        value =
            (value << 1) ^ (orig_value >> (sizeof(Type) * CHAR_BIT - 1));
    }

    constexpr auto max_size = varint_max_size<Type>;
    if constexpr (Archive::resizable) {
        if (auto result = archive.enlarge_for(max_size); failure(result))
            [[unlikely]] {
            return result;
        }
    }

    auto data = archive.remaining_data();
    if constexpr (!Archive::resizable) {
        auto data_size = data.size();
        if (data_size < max_size) [[unlikely]] {
            if (data_size < varint_size(value)) [[unlikely]] {
                return errc{std::errc::result_out_of_range};
            }
        }
    }

    using byte_type = std::remove_cvref_t<decltype(data[0])>;
    std::size_t position = {};
    while (value >= 0x80) {
        data[position++] = byte_type((value & 0x7f) | 0x80);
        value >>= (CHAR_BIT - 1);
    }
    data[position++] = byte_type(value);

    archive.position() += position;
    return errc{};
}

constexpr auto decode_varint(auto data, auto & value, auto & position)
{
    using value_type = std::remove_cvref_t<decltype(value)>;
    if (data.size() < varint_max_size<value_type>) [[unlikely]] {
        std::size_t shift = 0;
        for (auto & byte_value : data) {
            auto next_byte = value_type(byte_value);
            value |= (next_byte & 0x7f) << shift;
            if (next_byte >= 0x80) [[unlikely]] {
                shift += CHAR_BIT - 1;
                continue;
            }
            position += 1 + std::distance(data.data(), &byte_value);
            return errc{};
        }
        return errc{std::errc::result_out_of_range};
    } else {
        auto p = data.data();
        do {
            // clang-format off
            value_type next_byte;
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 0)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 1)); if (next_byte < 0x80) [[likely]] { break; }
            if constexpr (varint_max_size<value_type> > 2) {
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 2)); if (next_byte < 0x80) [[likely]] { break; }
            if constexpr (varint_max_size<value_type> > 3) {
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 3)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 4)); if (next_byte < 0x80) [[likely]] { break; }
            if constexpr (varint_max_size<value_type> > 5) {
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 5)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 6)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 7)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 8)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x01) << ((CHAR_BIT - 1) * 9)); if (next_byte < 0x80) [[likely]] { break; } }}}
            return errc{std::errc::value_too_large};
            // clang-format on
        } while (false);
        position += std::distance(data.data(), p);
        return errc{};
    }
}

template <typename Archive, typename Type, varint_encoding Encoding>
ZPP_BITS_INLINE constexpr auto serialize(
    Archive & archive,
    varint<Type, Encoding> & self) requires(Archive::kind() == kind::in)
{
    using value_type = std::conditional_t<
        std::is_enum_v<Type>,
        std::make_unsigned_t<traits::underlying_type_t<Type>>,
        std::make_unsigned_t<Type>>;
    value_type value{};
    auto data = archive.remaining_data();

    if constexpr (!ZPP_BITS_INLINE_DECODE_VARINT) {
        auto & position = archive.position();
        if (!data.empty() && !(value_type(data[0]) & 0x80)) [[likely]] {
            value = value_type(data[0]);
            position += 1;
        } else if (auto result =
                       std::is_constant_evaluated()
                           ? decode_varint(data, value, position)
                           : decode_varint(
                                 std::span{
                                     reinterpret_cast<const std::byte *>(
                                         data.data()),
                                     data.size()},
                                 value,
                                 position);
                   failure(result)) [[unlikely]] {
            return result;
        }

        if constexpr (varint_encoding::zig_zag == Encoding) {
            self.value =
                decltype(self.value)((value >> 1) ^ -(value & 0x1));
        } else {
            self.value = decltype(self.value)(value);
        }
        return errc{};
    } else if (data.size() < varint_max_size<value_type>) [[unlikely]] {
        std::size_t shift = 0;
        for (auto & byte_value : data) {
            auto next_byte = decltype(value)(byte_value);
            value |= (next_byte & 0x7f) << shift;
            if (next_byte >= 0x80) [[unlikely]] {
                shift += CHAR_BIT - 1;
                continue;
            }
            if constexpr (varint_encoding::zig_zag == Encoding) {
                self.value =
                    decltype(self.value)((value >> 1) ^ -(value & 0x1));
            } else {
                self.value = decltype(self.value)(value);
            }
            archive.position() +=
                1 + std::distance(data.data(), &byte_value);
            return errc{};
        }
        return errc{std::errc::result_out_of_range};
    } else {
        auto p = data.data();
        do {
            // clang-format off
            value_type next_byte;
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 0)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 1)); if (next_byte < 0x80) [[likely]] { break; }
            if constexpr (varint_max_size<value_type> > 2) {
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 2)); if (next_byte < 0x80) [[likely]] { break; }
            if constexpr (varint_max_size<value_type> > 3) {
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 3)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 4)); if (next_byte < 0x80) [[likely]] { break; }
            if constexpr (varint_max_size<value_type> > 5) {
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 5)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 6)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 7)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x7f) << ((CHAR_BIT - 1) * 8)); if (next_byte < 0x80) [[likely]] { break; }
            next_byte = value_type(*p++); value |= ((next_byte & 0x01) << ((CHAR_BIT - 1) * 9)); if (next_byte < 0x80) [[likely]] { break; } }}}
            return errc{std::errc::value_too_large};
            // clang-format on
        } while (false);
        if constexpr (varint_encoding::zig_zag == Encoding) {
            self.value =
                decltype(self.value)((value >> 1) ^ -(value & 0x1));
        } else {
            self.value = decltype(self.value)(value);
        }
        archive.position() += std::distance(data.data(), p);
        return errc{};
    }
}

template <typename Archive, typename Type, varint_encoding Encoding>
constexpr auto
serialize(Archive & archive,
          varint<Type, Encoding> && self) requires(Archive::kind() ==
                                                   kind::in) = delete;

using vint32_t = varint<std::int32_t>;
using vint64_t = varint<std::int64_t>;

using vuint32_t = varint<std::uint32_t>;
using vuint64_t = varint<std::uint64_t>;

using vsint32_t = varint<std::int32_t, varint_encoding::zig_zag>;
using vsint64_t = varint<std::int64_t, varint_encoding::zig_zag>;

using vsize_t = varint<std::size_t>;

inline namespace options
{
struct size_varint : option<size_varint>
{
    using default_size_type = vsize_t;
};
} // namespace options

template <concepts::byte_view ByteView, typename... Options>
class basic_out
{
public:
    template <concepts::byte_view, typename...>
    friend class basic_out;

    template <typename>
    friend struct option;

    template <typename... Types>
    using template_type = basic_out<Types...>;

    friend access;

    template <typename, typename>
    friend struct sized_item;

    template <typename, typename>
    friend struct sized_item_ref;

    template <typename, concepts::variant>
    friend struct known_id_variant;

    template <typename, concepts::variant>
    friend struct known_dynamic_id_variant;

    using byte_type = typename ByteView::value_type;

    static constexpr auto endian_aware =
        (... ||
         std::same_as<std::remove_cvref_t<Options>, endian::swapped>);

    using default_size_type = traits::default_size_type_t<Options...>;

    constexpr static auto allocation_limit = traits::alloc_limit<Options...>();

    constexpr static auto enlarger = traits::enlarger<Options...>();

    constexpr static auto no_enlarge_overflow =
        (... ||
         std::same_as<std::remove_cvref_t<Options>, options::no_enlarge_overflow>);

    constexpr static bool resizable = requires(ByteView view)
    {
        view.resize(1);
    };

    using view_type =
        std::conditional_t<resizable,
                           ByteView &,
                           std::remove_cvref_t<decltype(
                               std::span{std::declval<ByteView &>()})>>;

    constexpr explicit basic_out(ByteView && view, Options && ... options) : m_data(view)
    {
        static_assert(!resizable);
        (options(*this), ...);
    }

    constexpr explicit basic_out(ByteView & view, Options && ... options) : m_data(view)
    {
        (options(*this), ...);
    }

    ZPP_BITS_INLINE constexpr auto operator()(auto &&... items)
    {
        return serialize_many(items...);
    }

    constexpr decltype(auto) data()
    {
        return m_data;
    }

    constexpr std::size_t position() const
    {
        return m_position;
    }

    constexpr std::size_t & position()
    {
        return m_position;
    }

    constexpr auto remaining_data()
    {
        return std::span<byte_type>{m_data.data() + m_position,
                                    m_data.size() - m_position};
    }

    constexpr auto processed_data()
    {
        return std::span<byte_type>{m_data.data(), m_position};
    }

    constexpr void reset(std::size_t position = 0)
    {
        m_position = position;
    }

    constexpr static auto kind()
    {
        return kind::out;
    }

    ZPP_BITS_INLINE constexpr errc enlarge_for(auto additional_size)
    {
        auto size = m_data.size();
        if (additional_size > size - m_position) [[unlikely]] {
            constexpr auto multiplier = std::get<0>(enlarger);
            constexpr auto divisor = std::get<1>(enlarger);
            static_assert(multiplier != 0 && divisor != 0);

            auto required_size = size + additional_size;
            if constexpr (!no_enlarge_overflow) {
                if (required_size < size) [[unlikely]] {
                    return std::errc::no_buffer_space;
                }
            }

            auto new_size = required_size;
            if constexpr (multiplier != 1) {
                new_size *= multiplier;
                if constexpr (!no_enlarge_overflow) {
                    if (new_size / multiplier != required_size) [[unlikely]] {
                        return std::errc::no_buffer_space;
                    }
                }
            }
            if constexpr (divisor != 1) {
                new_size /= divisor;
            }
            if constexpr (allocation_limit !=
                          std::numeric_limits<std::size_t>::max()) {
                if (new_size > allocation_limit) [[unlikely]] {
                    return std::errc::no_buffer_space;
                }
            }
            m_data.resize(new_size);
        }
        return {};
    }

protected:
    ZPP_BITS_INLINE constexpr errc serialize_many(auto && first_item,
                                                  auto &&... items)
    {
        if (auto result = serialize_one(first_item); failure(result))
            [[unlikely]] {
            return result;
        }

        return serialize_many(items...);
    }

    ZPP_BITS_INLINE constexpr errc serialize_many()
    {
        return {};
    }

    constexpr auto option(append)
    {
        static_assert(resizable);
        m_position = m_data.size();
    }

    constexpr auto option(reserve size)
    {
        static_assert(resizable);
        m_data.reserve(size.size);
    }

    constexpr auto option(resize size)
    {
        static_assert(resizable);
        m_data.resize(size.size);
        if (m_position > size.size) {
            m_position = size.size;
        }
    }

    ZPP_BITS_INLINE constexpr errc serialize_one(concepts::unspecialized auto && item)
    {
        using type = std::remove_cvref_t<decltype(item)>;
        static_assert(!std::is_pointer_v<type>);

        if constexpr (requires { type::serialize(*this, item); }) {
            return type::serialize(*this, item);
        } else if constexpr (requires { serialize(*this, item); }) {
            return serialize(*this, item);
        } else if constexpr (std::is_fundamental_v<type> || std::is_enum_v<type>) {
            if constexpr (resizable) {
                if (auto result = enlarge_for(sizeof(item));
                    failure(result)) [[unlikely]] {
                    return result;
                }
            } else if (sizeof(item) > m_data.size() - m_position)
                [[unlikely]] {
                return std::errc::result_out_of_range;
            }

            if (std::is_constant_evaluated()) {
                auto value = std::bit_cast<
                    std::array<std::remove_const_t<byte_type>,
                               sizeof(item)>>(item);
                for (std::size_t i = 0; i < sizeof(value); ++i) {
                    if constexpr (endian_aware) {
                        m_data[m_position + i] = value[sizeof(value) - 1 - i];
                    } else {
                        m_data[m_position + i] = value[i];
                    }
                }
            } else {
                if constexpr (endian_aware) {
                    std::reverse_copy(
                        reinterpret_cast<const byte_type *>(&item),
                        reinterpret_cast<const byte_type *>(&item) +
                            sizeof(item),
                        m_data.data() + m_position);
                } else {
                    std::memcpy(
                        m_data.data() + m_position, &item, sizeof(item));
                }
            }
            m_position += sizeof(item);
            return {};
        } else if constexpr (requires {
                                 requires std::same_as<
                                     bytes<typename type::value_type>,
                                     type>;
                             }) {
            static_assert(
                !endian_aware ||
                concepts::byte_type<
                    std::remove_cvref_t<decltype(*item.data())>>);

            auto item_size_in_bytes = item.size_in_bytes();
            if (!item_size_in_bytes) [[unlikely]] {
                return {};
            }

            if constexpr (resizable) {
                if (auto result = enlarge_for(item_size_in_bytes);
                    failure(result)) [[unlikely]] {
                    return result;
                }
            } else if (item_size_in_bytes > m_data.size() - m_position)
                [[unlikely]] {
                return std::errc::result_out_of_range;
            }

            if (std::is_constant_evaluated()) {
                auto count = item.count();
                for (std::size_t index = 0; index < count; ++index) {
                    auto value = std::bit_cast<
                        std::array<std::remove_const_t<byte_type>,
                                   sizeof(typename type::value_type)>>(
                        item.data()[index]);
                    for (std::size_t i = 0;
                         i < sizeof(typename type::value_type);
                         ++i) {
                        m_data[m_position +
                               index * sizeof(typename type::value_type) +
                               i] = value[i];
                    }
                }
            } else {
                // Ignore GCC Issue.
#if !defined __clang__ && defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
                std::memcpy(m_data.data() + m_position,
                            item.data(),
                            item_size_in_bytes);
#if !defined __clang__ && defined __GNUC__
#pragma GCC diagnostic pop
#endif
            }
            m_position += item_size_in_bytes;
            return {};
        } else if constexpr (concepts::empty<type>) {
            return {};
        } else if constexpr (concepts::serialize_as_bytes<decltype(*this),
                                                          type>) {
            return serialize_one(as_bytes(item));
        } else if constexpr (concepts::self_referencing<type>) {
            return visit_members(
                item,
                [&](auto &&... items) constexpr {
                    return serialize_many(items...);
                });
        } else {
            return visit_members(
                item,
                [&](auto &&... items) ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                    return serialize_many(items...);
                });
        }
    }

    template <typename SizeType = default_size_type>
    ZPP_BITS_INLINE constexpr errc serialize_one(concepts::array auto && array)
    {
        using value_type = std::remove_cvref_t<decltype(array[0])>;

        if constexpr (concepts::serialize_as_bytes<decltype(*this),
                                                   value_type>) {
            return serialize_one(bytes(array));
        } else {
            for (auto & item : array) {
                if (auto result = serialize_one(item); failure(result))
                    [[unlikely]] {
                    return result;
                }
            }
            return {};
        }
    }

    template <typename SizeType = default_size_type>
    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::container auto && container)
    {
        using type = std::remove_cvref_t<decltype(container)>;
        using value_type = typename type::value_type;

        if constexpr (concepts::serialize_as_bytes<decltype(*this),
                                                   value_type> &&
                      std::is_base_of_v<std::random_access_iterator_tag,
                                        typename std::iterator_traits<
                                            typename type::iterator>::
                                            iterator_category> &&
                      requires { container.data(); }) {
            auto size = container.size();
            if constexpr (!std::is_void_v<SizeType> &&
                          (concepts::associative_container<
                               decltype(container)> ||
                           requires(type container) {
                               container.resize(1);
                           } ||
                           (
                               requires(type container) {
                                   container = {container.data(), 1};
                               } &&
                               !requires {
                                   requires(type::extent !=
                                            std::dynamic_extent);
                                   requires concepts::
                                       has_fixed_nonzero_size<type>;
                               }))) {
                if (auto result =
                        serialize_one(static_cast<SizeType>(size));
                    failure(result)) [[unlikely]] {
                    return result;
                }
            }
            return serialize_one(bytes(container, size));
        } else {
            if constexpr (!std::is_void_v<SizeType> &&
                          (concepts::associative_container<
                               decltype(container)> ||
                           requires(type container) {
                               container.resize(1);
                           } ||
                           (
                               requires(type container) {
                                   container = {container.data(), 1};
                               } &&
                               !requires {
                                   requires(type::extent !=
                                            std::dynamic_extent);
                                   requires concepts::
                                       has_fixed_nonzero_size<type>;
                               }))) {
                if (auto result = serialize_one(
                        static_cast<SizeType>(container.size()));
                    failure(result)) [[unlikely]] {
                    return result;
                }
            }
            for (auto & item : container) {
                if (auto result = serialize_one(item); failure(result))
                    [[unlikely]] {
                    return result;
                }
            }
            return {};
        }
    }

    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::tuple auto && tuple)
    {
        return serialize_one(tuple,
                             std::make_index_sequence<std::tuple_size_v<
                                 std::remove_cvref_t<decltype(tuple)>>>());
    }

    template <std::size_t... Indices>
    ZPP_BITS_INLINE constexpr errc serialize_one(
        concepts::tuple auto && tuple, std::index_sequence<Indices...>)
    {
        return serialize_many(std::get<Indices>(tuple)...);
    }

    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::optional auto && optional)
    {
        if (!optional) [[unlikely]] {
            return serialize_one(std::byte(false));
        } else {
            return serialize_many(std::byte(true), *optional);
        }
    }

    template <typename KnownId = void>
    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::variant auto && variant)
    {
        using type = std::remove_cvref_t<decltype(variant)>;

        if constexpr (!std::is_void_v<KnownId>) {
            return serialize_one(
                *std::get_if<
                    traits::variant<type>::template index<KnownId::value>()>(
                    std::addressof(variant)));
        } else {
            auto variant_index = variant.index();
            if (std::variant_npos == variant_index) [[unlikely]] {
                return std::errc::invalid_argument;
            }

            return std::visit(
                [index = variant_index,
                 this](auto & object) ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                    return this->serialize_many(
                        traits::variant<type>::id(index), object);
                },
                variant);
        }
    }

    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::owning_pointer auto && pointer)
    {
        if (nullptr == pointer) [[unlikely]] {
            return std::errc::invalid_argument;
        }

        return serialize_one(*pointer);
    }

    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::bitset auto && bitset)
    {
        constexpr auto size = std::remove_cvref_t<decltype(bitset)>{}.size();
        constexpr auto size_in_bytes = (size + (CHAR_BIT - 1)) / CHAR_BIT;

        if constexpr (resizable) {
            if (auto result = enlarge_for(size_in_bytes);
                failure(result)) [[unlikely]] {
                return result;
            }
        } else if (size_in_bytes > m_data.size() - m_position)
            [[unlikely]] {
            return std::errc::result_out_of_range;
        }

        auto data = m_data.data() + m_position;
        for (std::size_t i = 0; i < size; ++i) {
            auto & value = data[i / CHAR_BIT];
            value = byte_type(static_cast<unsigned char>(value) |
                              (bitset[i] << (i & 0x7)));
        }

        m_position += size_in_bytes;
        return {};
    }

    template <typename SizeType = default_size_type>
    ZPP_BITS_INLINE constexpr errc serialize_one(concepts::by_protocol auto && item)
    {
        using type = std::remove_cvref_t<decltype(item)>;
        if constexpr (!std::is_void_v<SizeType>) {
            auto size_position = m_position;
            if (auto result = serialize_one(SizeType{});
                failure(result)) [[unlikely]] {
                return result;
            }

            if constexpr (requires { typename type::serialize; }) {
                constexpr auto protocol = type::serialize::value;
                if (auto result = protocol(*this, item); failure(result))
                    [[unlikely]] {
                    return result;
                }
            } else {
                constexpr auto protocol = decltype(serialize(item))::value;
                if (auto result = protocol(*this, item); failure(result))
                    [[unlikely]] {
                    return result;
                }
            }

            auto current_position = m_position;
            std::size_t message_size =
                    current_position - size_position - sizeof(SizeType);
            if constexpr (concepts::varint<SizeType>) {
                constexpr auto preserialized_varint_size = 1;
                message_size = current_position - size_position -
                               preserialized_varint_size;
                auto move_ahead_count =
                    varint_size(message_size) - preserialized_varint_size;
                if (move_ahead_count) {
                    if constexpr (resizable) {
                        if (auto result = enlarge_for(move_ahead_count);
                            failure(result)) [[unlikely]] {
                            return result;
                        }
                    } else if (move_ahead_count >
                               m_data.size() - current_position)
                        [[unlikely]] {
                        return std::errc::result_out_of_range;
                    }
                    auto data = m_data.data();
                    auto message_start =
                        data + size_position + preserialized_varint_size;
                    auto message_end = data + current_position;
                    if (std::is_constant_evaluated()) {
                        for (auto p = message_end - 1; p >= message_start;
                             --p) {
                            *(p + move_ahead_count) = *p;
                        }
                    } else {
                        std::memmove(message_start + move_ahead_count,
                                     message_start,
                                     message_size);
                    }
                    m_position += move_ahead_count;
                }
            }
            return basic_out<std::span<byte_type, sizeof(SizeType)>>{
                std::span<byte_type, sizeof(SizeType)>{
                    m_data.data() + size_position, sizeof(SizeType)}}(
                SizeType(message_size));
        } else {
            if constexpr (requires {typename type::serialize;}) {
                constexpr auto protocol = type::serialize::value;
                return protocol(*this, item);
            } else {
                constexpr auto protocol = decltype(serialize(item))::value;
                return protocol(*this, item);
            }
        }
    }

    constexpr ~basic_out() = default;

    view_type m_data{};
    std::size_t m_position{};
};

template <concepts::byte_view ByteView = std::vector<std::byte>, typename... Options>
class out : public basic_out<ByteView, Options...>
{
public:
    template <typename... Types>
    using template_type = out<Types...>;

    using base = basic_out<ByteView, Options...>;
    using base::basic_out;

    friend access;

    using base::resizable;
    using base::enlarger;

    constexpr static auto no_fit_size =
        (... ||
         std::same_as<std::remove_cvref_t<Options>, options::no_fit_size>);

    ZPP_BITS_INLINE constexpr auto operator()(auto &&... items)
    {
        if constexpr (resizable && !no_fit_size &&
                      enlarger != std::tuple{1, 1}) {
            auto end = m_data.size();
            auto result = serialize_many(items...);
            if (m_position >= end) {
                m_data.resize(m_position);
            }
            return result;
        } else {
            return serialize_many(items...);
        }
    }

private:
    using base::serialize_many;
    using base::m_data;
    using base::m_position;
};

template <typename Type, typename... Options>
out(Type &, Options &&...) -> out<Type, Options...>;

template <typename Type, typename... Options>
out(Type &&, Options &&...) -> out<Type, Options...>;

template <typename Type, std::size_t Size, typename... Options>
out(Type (&)[Size], Options &&...)
    -> out<std::span<Type, Size>, Options...>;

template <typename Type, typename SizeType, typename... Options>
out(sized_item<Type, SizeType> &, Options &&...)
    -> out<Type, Options...>;

template <typename Type, typename SizeType, typename... Options>
out(const sized_item<Type, SizeType> &, Options &&...)
    -> out<const Type, Options...>;

template <typename Type, typename SizeType, typename... Options>
out(sized_item<Type, SizeType> &&, Options &&...) -> out<Type, Options...>;

template <concepts::byte_view ByteView = std::vector<std::byte>,
          typename... Options>
class in
{
public:
    template <typename... Types>
    using template_type = in<Types...>;

    template <typename>
    friend struct option;

    friend access;

    template <typename, typename>
    friend struct sized_item;

    template <typename, typename>
    friend struct sized_item_ref;

    template <typename, concepts::variant>
    friend struct known_id_variant;

    template <typename, concepts::variant>
    friend struct known_dynamic_id_variant;

    using byte_type = std::add_const_t<typename ByteView::value_type>;

    constexpr static auto endian_aware =
        (... ||
         std::same_as<std::remove_cvref_t<Options>, endian::swapped>);

    using default_size_type = traits::default_size_type_t<Options...>;

    constexpr static auto allocation_limit =
        traits::alloc_limit<Options...>();

    constexpr explicit in(ByteView && view, Options && ... options) : m_data(view)
    {
        static_assert(!resizable);
        (options(*this), ...);
    }

    constexpr explicit in(ByteView & view, Options && ... options) : m_data(view)
    {
        (options(*this), ...);
    }

    ZPP_BITS_INLINE constexpr auto operator()(auto &&... items)
    {
        return serialize_many(items...);
    }

    constexpr decltype(auto) data()
    {
        return m_data;
    }

    constexpr std::size_t position() const
    {
        return m_position;
    }

    constexpr std::size_t & position()
    {
        return m_position;
    }

    constexpr auto remaining_data()
    {
        return std::span<byte_type>{m_data.data() + m_position,
                                    m_data.size() - m_position};
    }

    constexpr auto processed_data()
    {
        return std::span<byte_type>{m_data.data(), m_position};
    }

    constexpr void reset(std::size_t position = 0)
    {
        m_position = position;
    }

    constexpr static auto kind()
    {
        return kind::in;
    }

    constexpr static bool resizable = requires(ByteView view)
    {
        view.resize(1);
    };

    using view_type =
        std::conditional_t<resizable,
                           ByteView &,
                           std::remove_cvref_t<decltype(
                               std::span{std::declval<ByteView &>()})>>;

private:
    ZPP_BITS_INLINE constexpr errc serialize_many(auto && first_item,
                                                  auto &&... items)
    {
        if (auto result = serialize_one(first_item); failure(result))
            [[unlikely]] {
            return result;
        }

        return serialize_many(items...);
    }

    ZPP_BITS_INLINE constexpr errc serialize_many()
    {
        return {};
    }

    ZPP_BITS_INLINE constexpr errc serialize_one(concepts::unspecialized auto && item)
    {
        using type = std::remove_cvref_t<decltype(item)>;
        static_assert(!std::is_pointer_v<type>);

        if constexpr (requires { type::serialize(*this, item); }) {
            return type::serialize(*this, item);
        } else if constexpr (requires { serialize(*this, item); }) {
            return serialize(*this, item);
        } else if constexpr (std::is_fundamental_v<type> || std::is_enum_v<type>) {
            auto size = m_data.size();
            if (sizeof(item) > size - m_position) [[unlikely]] {
                return std::errc::result_out_of_range;
            }
            if (std::is_constant_evaluated()) {
                std::array<std::remove_const_t<byte_type>, sizeof(item)>
                    value;
                for (std::size_t i = 0; i < sizeof(value); ++i) {
                    if constexpr (endian_aware) {
                        value[sizeof(value) - 1 - i] =
                            byte_type(m_data[m_position + i]);
                    } else {
                        value[i] = byte_type(m_data[m_position + i]);
                    }
                }
                item = std::bit_cast<type>(value);
            } else {
                if constexpr (endian_aware) {
                    auto begin = m_data.data() + m_position;
                    std::reverse_copy(
                        begin,
                        begin + sizeof(item),
                        reinterpret_cast<std::remove_const_t<byte_type> *>(
                            &item));
                } else {
                    std::memcpy(
                        &item, m_data.data() + m_position, sizeof(item));
                }
            }
            m_position += sizeof(item);
            return {};
        } else if constexpr (requires {
                                 requires std::same_as<
                                     bytes<typename type::value_type>,
                                     type>;
                             }) {
            static_assert(
                !endian_aware ||
                concepts::byte_type<
                    std::remove_cvref_t<decltype(*item.data())>>);

            auto size = m_data.size();
            auto item_size_in_bytes = item.size_in_bytes();
            if (!item_size_in_bytes) [[unlikely]] {
                return {};
            }

            if (item_size_in_bytes > size - m_position) [[unlikely]] {
                return std::errc::result_out_of_range;
            }
            if (std::is_constant_evaluated()) {
                std::size_t count = item.count();
                for (std::size_t index = 0; index < count; ++index) {
                    std::array<std::remove_const_t<byte_type>,
                               sizeof(typename type::value_type)>
                        value;
                    for (std::size_t i = 0;
                         i < sizeof(typename type::value_type);
                         ++i) {
                        value[i] = byte_type(
                            m_data[m_position +
                                   index *
                                       sizeof(typename type::value_type) +
                                   i]);
                    }
                    item.data()[index] =
                        std::bit_cast<typename type::value_type>(value);
                }
            } else {
                std::memcpy(item.data(),
                            m_data.data() + m_position,
                            item_size_in_bytes);
            }
            m_position += item_size_in_bytes;
            return {};
        } else if constexpr (concepts::empty<type>) {
            return {};
        } else if constexpr (concepts::serialize_as_bytes<decltype(*this),
                                                          type>) {
            return serialize_one(as_bytes(item));
        } else if constexpr (concepts::self_referencing<type>) {
            return visit_members(
                item,
                [&](auto &&... items) constexpr {
                    return serialize_many(items...);
                });
        } else {
            return visit_members(
                item,
                [&](auto &&... items) ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                    return serialize_many(items...);
                });
        }
    }

    template <typename SizeType = default_size_type>
    ZPP_BITS_INLINE constexpr errc serialize_one(concepts::array auto && array)
    {
        using value_type = std::remove_cvref_t<decltype(array[0])>;

        if constexpr (concepts::serialize_as_bytes<decltype(*this),
                                                   value_type>) {
            return serialize_one(bytes(array));
        } else {
            for (auto & item : array) {
                if (auto result = serialize_one(item); failure(result))
                    [[unlikely]] {
                    return result;
                }
            }
            return {};
        }
    }

    template <typename SizeType = default_size_type>
    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::container auto && container)
    {
        using type = std::remove_cvref_t<decltype(container)>;
        using value_type = typename type::value_type;
        constexpr auto is_const = std::is_const_v<
            std::remove_reference_t<decltype(container[0])>>;

        if constexpr (!std::is_void_v<SizeType> &&
                      (requires(type container) { container.resize(1); } ||
                       (
                           requires(type container) {
                               container = {container.data(), 1};
                           } &&
                           !requires {
                               requires(type::extent !=
                                        std::dynamic_extent);
                               requires concepts::has_fixed_nonzero_size<
                                   type>;
                           }))) {
            SizeType size{};
            if (auto result = serialize_one(size); failure(result))
                [[unlikely]] {
                return result;
            }

            if constexpr (requires(type container) {
                              container.resize(size);
                          }) {
                if constexpr (allocation_limit !=
                              std::numeric_limits<std::size_t>::max()) {
                    constexpr auto limit =
                        allocation_limit / sizeof(value_type);
                    if (size > limit) [[unlikely]] {
                        return std::errc::message_size;
                    }
                }
                container.resize(size);
            } else if constexpr (is_const &&
                                 (std::same_as<std::byte, value_type> ||
                                  std::same_as<char, value_type> ||
                                  std::same_as<unsigned char,
                                               value_type>)) {
                if (size > m_data.size() - m_position) [[unlikely]] {
                    return std::errc::result_out_of_range;
                }
                container = {m_data.data() + m_position, size};
                m_position += size;
            } else {
                if (size > container.size()) [[unlikely]] {
                    return std::errc::result_out_of_range;
                }
                container = {container.data(), size};
            }

            if constexpr (
                concepts::serialize_as_bytes<decltype(*this),
                                             value_type> &&
                std::is_base_of_v<
                    std::random_access_iterator_tag,
                    typename std::iterator_traits<
                        typename type::iterator>::iterator_category> &&
                requires { container.data(); } &&
                !(is_const &&
                  (std::same_as<std::byte, value_type> ||
                   std::same_as<char, value_type> ||
                   std::same_as<unsigned char,
                                value_type>)&&requires(type container) {
                      container = {m_data.data(), 1};
                  })) {
                return serialize_one(bytes(container, size));
            }
        }

        if constexpr (concepts::serialize_as_bytes<decltype(*this),
                                                   value_type> &&
                      std::is_base_of_v<std::random_access_iterator_tag,
                                        typename std::iterator_traits<
                                            typename type::iterator>::
                                            iterator_category> &&
                      requires { container.data(); }) {
            if constexpr (is_const &&
                          (std::same_as<std::byte, value_type> ||
                           std::same_as<char, value_type> ||
                           std::same_as<
                               unsigned char,
                               value_type>)&&requires(type container) {
                              container = {m_data.data(), 1};
                          }) {
                if constexpr (requires {
                                  requires(type::extent !=
                                           std::dynamic_extent);
                                  requires concepts::has_fixed_nonzero_size<type>;
                              }) {
                    if (type::extent > m_data.size() - m_position)
                        [[unlikely]] {
                        return std::errc::result_out_of_range;
                    }
                    container = {m_data.data() + m_position, type::extent};
                    m_position += type::extent;
                } else if constexpr (std::is_void_v<SizeType>) {
                    auto size = m_data.size();
                    container = {m_data.data() + m_position,
                                 size - m_position};
                    m_position = size;
                }
                return {};
            } else {
                return serialize_one(bytes(container));
            }
        } else {
            for (auto & item : container) {
                if (auto result = serialize_one(item); failure(result))
                    [[unlikely]] {
                    return result;
                }
            }
            return {};
        }
    }

    template <typename SizeType = default_size_type>
    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::associative_container auto && container)
    {
        using type = typename std::remove_cvref_t<decltype(container)>;

        SizeType size{};

        if constexpr (!std::is_void_v<SizeType>) {
            if (auto result = serialize_one(size); failure(result))
                [[unlikely]] {
                return result;
            }
        } else {
            size = container.size();
        }

        container.clear();

        for (std::size_t index{}; index < size; ++index)
        {
            if constexpr (requires { typename type::mapped_type; }) {
                using value_type = std::pair<typename type::key_type,
                                             typename type::mapped_type>;
                std::aligned_storage_t<sizeof(value_type),
                                       alignof(value_type)>
                    storage;

                auto object = access::placement_new<value_type>(
                    std::addressof(storage));
                destructor_guard guard{*object};
                if (auto result = serialize_one(*object); failure(result))
                    [[unlikely]] {
                    return result;
                }

                container.insert(std::move(*object));
            } else {
                using value_type = typename type::value_type;

                std::aligned_storage_t<sizeof(value_type),
                                       alignof(value_type)>
                    storage;

                auto object = access::placement_new<value_type>(
                    std::addressof(storage));
                destructor_guard guard{*object};
                if (auto result = serialize_one(*object); failure(result))
                    [[unlikely]] {
                    return result;
                }

                container.insert(std::move(*object));
            }
        }

        return {};
    }

    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::tuple auto && tuple)
    {
        return serialize_one(tuple,
                             std::make_index_sequence<std::tuple_size_v<
                                 std::remove_cvref_t<decltype(tuple)>>>());
    }

    template <std::size_t... Indices>
    ZPP_BITS_INLINE constexpr errc serialize_one(
        concepts::tuple auto && tuple, std::index_sequence<Indices...>)
    {
        return serialize_many(std::get<Indices>(tuple)...);
    }

    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::optional auto && optional)
    {
        using value_type = std::remove_reference_t<decltype(*optional)>;

        std::byte has_value{};
        if (auto result = serialize_one(has_value); failure(result))
            [[unlikely]] {
            return result;
        }

        if (!bool(has_value)) [[unlikely]] {
            optional = std::nullopt;
            return {};
        }

        if constexpr (std::is_default_constructible_v<value_type>) {
            if (!optional) {
                optional = value_type{};
            }

            if (auto result = serialize_one(*optional); failure(result))
                [[unlikely]] {
                return result;
            }
        } else {
            std::aligned_storage_t<sizeof(value_type), alignof(value_type)>
                storage;

            auto object =
                access::placement_new<value_type>(std::addressof(storage));
            destructor_guard guard{*object};

            if (auto result = serialize_one(*object); failure(result))
                [[unlikely]] {
                return result;
            }

            optional = std::move(*object);
        }

        return {};
    }

    template <typename KnownId = void,
              typename... Types,
              template <typename...>
              typename Variant>
    ZPP_BITS_INLINE constexpr errc
    serialize_one(Variant<Types...> & variant) requires
        concepts::variant<Variant<Types...>>
    {
        using type = std::remove_cvref_t<decltype(variant)>;

        if constexpr (!std::is_void_v<KnownId>) {
            constexpr auto index =
                traits::variant<type>::template index<KnownId::value>();

            using element_type =
                std::remove_reference_t<decltype(std::get<index>(
                    variant))>;

            if constexpr (std::is_default_constructible_v<element_type>) {
                if (variant.index() !=
                    traits::variant<type>::template index_by_type<
                        element_type>()) {
                    variant = element_type{};
                }
                return serialize_one(*std::get_if<element_type>(&variant));
            } else {
                std::aligned_storage_t<sizeof(element_type),
                                       alignof(element_type)>
                    storage;

                auto object = access::placement_new<element_type>(
                    std::addressof(storage));
                destructor_guard guard{*object};

                if (auto result = serialize_one(*object); failure(result))
                    [[unlikely]] {
                    return result;
                }
                variant = std::move(*object);
            }
        } else {
            typename traits::variant<type>::id_type id;
            if (auto result = serialize_one(id); failure(result))
                [[unlikely]] {
                return result;
            }

            return serialize_one(variant, id);
        }
    }

    template <typename... Types, template <typename...> typename Variant>
    ZPP_BITS_INLINE constexpr errc
    serialize_one(Variant<Types...> & variant,
                  auto && id) requires concepts::variant<Variant<Types...>>
    {
        using type = std::remove_cvref_t<decltype(variant)>;

        auto index = traits::variant<type>::index(id);
        if (index > sizeof...(Types)) [[unlikely]] {
            return std::errc::bad_message;
        }

        constexpr std::tuple loaders{
            [](auto & self,
               auto & variant) ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                if constexpr (std::is_default_constructible_v<Types>) {
                    if (variant.index() !=
                        traits::variant<type>::template index_by_type<
                            Types>()) {
                        variant = Types{};
                    }
                    return self.serialize_one(
                        *std::get_if<Types>(&variant));
                } else {
                    std::aligned_storage_t<sizeof(Types), alignof(Types)>
                        storage;

                    auto object = access::placement_new<Types>(
                        std::addressof(storage));
                    destructor_guard guard{*object};

                    if (auto result = self.serialize_one(*object);
                        failure(result)) [[unlikely]] {
                        return result;
                    }
                    variant = std::move(*object);
                }
            }...};

        return traits::tuple<std::remove_cvref_t<decltype(loaders)>>::
            visit(loaders, index, [&](auto && loader) ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                return loader(*this, variant);
            });
    }

    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::owning_pointer auto && pointer)
    {
        using type = std::remove_reference_t<decltype(*pointer)>;

        auto loaded = access::make_unique<type>();;
        if (auto result = serialize_one(*loaded); failure(result))
            [[unlikely]] {
            return result;
        }

        pointer.reset(loaded.release());
        return {};
    }

    ZPP_BITS_INLINE constexpr errc
    serialize_one(concepts::bitset auto && bitset)
    {
        constexpr auto size = std::remove_cvref_t<decltype(bitset)>{}.size();
        constexpr auto size_in_bytes = (size + (CHAR_BIT - 1)) / CHAR_BIT;

        if (size_in_bytes > m_data.size() - m_position)
            [[unlikely]] {
            return std::errc::result_out_of_range;
        }

        auto data = m_data.data() + m_position;
        for (std::size_t i = 0; i < size; ++i) {
            bitset[i] = (static_cast<unsigned char>(data[i / CHAR_BIT]) >>
                         (i & 0x7)) &
                        0x1;
        }

        m_position += size_in_bytes;
        return {};
    }

    template <typename SizeType = default_size_type>
    ZPP_BITS_INLINE constexpr errc serialize_one(concepts::by_protocol auto && item)
    {
        using type = std::remove_cvref_t<decltype(item)>;
        if constexpr (!std::is_void_v<SizeType>) {
            SizeType size{};
            if (auto result = serialize_one(size); failure(result))
                [[unlikely]] {
                return result;
            }

            if constexpr (requires {typename type::serialize;}) {
                constexpr auto protocol = type::serialize::value;
                return protocol(*this, item, size);
            } else {
                constexpr auto protocol = decltype(serialize(item))::value;
                return protocol(*this, item, size);
            }
        } else {
            if constexpr (requires {typename type::serialize;}) {
                constexpr auto protocol = type::serialize::value;
                return protocol(*this, item);
            } else {
                constexpr auto protocol = decltype(serialize(item))::value;
                return protocol(*this, item);
            }
        }
    }

    view_type m_data{};
    std::size_t m_position{};
};

template <typename Type, std::size_t Size, typename... Options>
in(Type (&)[Size], Options && ...) -> in<std::span<Type, Size>, Options...>;

template <typename Type, typename SizeType, typename... Options>
in(sized_item<Type, SizeType> &, Options && ...)
    -> in<Type, Options...>;

template <typename Type, typename SizeType, typename... Options>
in(const sized_item<Type, SizeType> &, Options && ...)
    -> in<const Type, Options...>;

template <typename Type, typename SizeType, typename... Options>
in(sized_item<Type, SizeType> &&, Options && ...)
    -> in<Type, Options...>;

constexpr auto input(auto && view, auto &&... option)
{
    return in(std::forward<decltype(view)>(view),
              std::forward<decltype(option)>(option)...);
}

constexpr auto output(auto && view, auto &&... option)
{
    return out(std::forward<decltype(view)>(view),
               std::forward<decltype(option)>(option)...);
}

constexpr auto in_out(auto && view, auto &&... option)
{
    return std::tuple{
        in<std::remove_reference_t<typename decltype(in{view})::view_type>,
           decltype(option) &...>(view, option...),
        out(std::forward<decltype(view)>(view),
            std::forward<decltype(option)>(option)...)};
}

template <typename ByteType = std::byte>
constexpr auto data_in_out(auto &&... option)
{
    struct data_in_out
    {
        data_in_out(decltype(option) &&... option) :
            input(data, option...),
            output(data, std::forward<decltype(option)>(option)...)
        {
        }

        std::vector<ByteType> data;
        in<decltype(data), decltype(option) &...> input;
        out<decltype(data), decltype(option)...> output;
    };
    return data_in_out{std::forward<decltype(option)>(option)...};
}

template <typename ByteType = std::byte>
constexpr auto data_in(auto &&... option)
{
    struct data_in
    {
        data_in(decltype(option) &&... option) :
            input(data, std::forward<decltype(option)>(option)...)
        {
        }

        std::vector<ByteType> data;
        in<decltype(data), decltype(option)...> input;
    };
    return data_in{std::forward<decltype(option)>(option)...};
}

template <typename ByteType = std::byte>
constexpr auto data_out(auto &&... option)
{
    struct data_out
    {
        data_out(decltype(option) &&... option) :
            output(data, std::forward<decltype(option)>(option)...)
        {
        }

        std::vector<ByteType> data;
        out<decltype(data), decltype(option)...> output;
    };
    return data_out{std::forward<decltype(option)>(option)...};
}

template <auto Object, std::size_t MaxSize = 0x1000>
constexpr auto to_bytes_one()
{
    constexpr auto size = [] {
        std::array<std::byte, MaxSize> data;
        out out{data};
        out(Object).or_throw();
        return out.position();
    }();

    if constexpr (!size) {
        return string_literal<std::byte, 0>{};
    } else {
        std::array<std::byte, size> data;
        out{data}(Object).or_throw();
        return data;
    }
}

template <auto... Data>
constexpr auto join()
{
    constexpr auto size = (0 + ... + Data.size());
    if constexpr (!size) {
        return string_literal<std::byte, 0>{};
    } else {
        std::array<std::byte, size> data;
        out{data}(Data...).or_throw();
        return data;
    }
}

template <auto Left, auto Right = -1>
constexpr auto slice(auto array)
{
    constexpr auto left = Left;
    constexpr auto right = (-1 == Right) ? array.size() : Right;
    constexpr auto size = right - left;
    static_assert(Left < Right || -1 == Right);

    std::array<std::remove_reference_t<decltype(array[0])>, size> sliced;
    std::copy(std::begin(array) + left,
              std::begin(array) + right,
              std::begin(sliced));
    return sliced;
}

template <auto... Object>
constexpr auto to_bytes()
{
    return join<to_bytes_one<Object>()...>();
}

template <auto Data, typename Type>
constexpr auto from_bytes()
{
    Type object;
    in{Data}(object).or_throw();
    return object;
}

template <auto Data, typename... Types>
constexpr auto from_bytes() requires (sizeof...(Types) > 1)
{
    std::tuple<Types...> object;
    in{Data}(object).or_throw();
    return object;
}

template <auto Id, auto MaxSize = -1>
constexpr auto serialize_id()
{
    constexpr auto serialized_id = slice<0, MaxSize>(to_bytes<Id>());
    if constexpr (sizeof(serialized_id) == 1) {
        return serialization_id<from_bytes<serialized_id, std::byte>()>{};
    } else if constexpr (sizeof(serialized_id) == 2) {
        return serialization_id<from_bytes<serialized_id, std::uint16_t>()>{};
    } else if constexpr (sizeof(serialized_id) == 4) {
        return serialization_id<from_bytes<serialized_id, std::uint32_t>()>{};
    } else if constexpr (sizeof(serialized_id) == 8) {
        return serialization_id<from_bytes<serialized_id, std::uint64_t>()>{};
    } else {
        return serialization_id<serialized_id>{};
    }
}

template <auto Id, auto MaxSize = -1>
using id = decltype(serialize_id<Id, MaxSize>());

template <auto Id, auto MaxSize = -1>
constexpr auto id_v = id<Id, MaxSize>::value;

template <typename Id, concepts::variant Variant>
struct known_id_variant
{
    constexpr explicit known_id_variant(Variant & variant) :
        variant(variant)
    {
    }

    ZPP_BITS_INLINE constexpr static auto serialize(auto & serializer,
                                                    auto & self)
    {
        return serializer.template serialize_one<Id>(self.variant);
    }

    Variant & variant;
};

template <auto Id, auto MaxSize = -1, typename Variant>
constexpr auto known_id(Variant && variant)
{
    return known_id_variant<id<Id, MaxSize>,
                            std::remove_reference_t<Variant>>(variant);
}

template <typename Id, concepts::variant Variant>
struct known_dynamic_id_variant
{
    using id_type =
        std::conditional_t<std::is_integral_v<std::remove_cvref_t<Id>> ||
                               std::is_enum_v<std::remove_cvref_t<Id>>,
                           std::remove_cvref_t<Id>,
                           Id &>;

    constexpr explicit known_dynamic_id_variant(Variant & variant, id_type id) :
        variant(variant),
        id(id)
    {
    }

    ZPP_BITS_INLINE constexpr static auto serialize(auto & serializer,
                                                    auto & self)
    {
        return serializer.template serialize_one(self.variant, self.id);
    }

    Variant & variant;
    id_type id;
};

template <typename Id, typename Variant>
constexpr auto known_id(Id && id, Variant && variant)
{
    return known_dynamic_id_variant<Id, std::remove_reference_t<Variant>>(
        variant, id);
}

template <typename Function>
struct function_traits;

template <typename Return, typename... Arguments>
struct function_traits<Return(*)(Arguments...)>
{
    using parameters_type = std::tuple<std::remove_cvref_t<Arguments>...>;
    using return_type = Return;
};

template <typename Return, typename... Arguments>
struct function_traits<Return(*)(Arguments...) noexcept>
{
    using parameters_type = std::tuple<std::remove_cvref_t<Arguments>...>;
    using return_type = Return;
};

template <typename This, typename Return, typename... Arguments>
struct function_traits<Return(This::*)(Arguments...)>
{
    using parameters_type = std::tuple<std::remove_cvref_t<Arguments>...>;
    using return_type = Return;
};

template <typename This, typename Return, typename... Arguments>
struct function_traits<Return(This::*)(Arguments...) noexcept>
{
    using parameters_type = std::tuple<std::remove_cvref_t<Arguments>...>;
    using return_type = Return;
};

template <typename This, typename Return, typename... Arguments>
struct function_traits<Return(This::*)(Arguments...) const>
{
    using parameters_type = std::tuple<std::remove_cvref_t<Arguments>...>;
    using return_type = Return;
};

template <typename This, typename Return, typename... Arguments>
struct function_traits<Return(This::*)(Arguments...) const noexcept>
{
    using parameters_type = std::tuple<std::remove_cvref_t<Arguments>...>;
    using return_type = Return;
};

template <typename Return>
struct function_traits<Return(*)()>
{
    using parameters_type = void;
    using return_type = Return;
};

template <typename Return>
struct function_traits<Return(*)() noexcept>
{
    using parameters_type = void;
    using return_type = Return;
};

template <typename This, typename Return>
struct function_traits<Return(This::*)()>
{
    using parameters_type = void;
    using return_type = Return;
};

template <typename This, typename Return>
struct function_traits<Return(This::*)() noexcept>
{
    using parameters_type = void;
    using return_type = Return;
};

template <typename This, typename Return>
struct function_traits<Return(This::*)() const>
{
    using parameters_type = void;
    using return_type = Return;
};

template <typename This, typename Return>
struct function_traits<Return(This::*)() const noexcept>
{
    using parameters_type = void;
    using return_type = Return;
};

template <typename Function>
using function_parameters_t =
    typename function_traits<std::remove_cvref_t<Function>>::parameters_type;

template <typename Function>
using function_return_type_t =
    typename function_traits<std::remove_cvref_t<Function>>::return_type;

constexpr auto success(auto && value_or_errc) requires
    std::same_as<decltype(value_or_errc.error()), errc>
{
    return value_or_errc.success();
}

constexpr auto failure(auto && value_or_errc) requires
    std::same_as<decltype(value_or_errc.error()), errc>
{
    return value_or_errc.failure();
}

template <typename Type>
struct [[nodiscard]] value_or_errc
{
    using error_type = errc;
    using value_type = std::conditional_t<
        std::is_void_v<Type>,
        std::nullptr_t,
        std::conditional_t<
            std::is_reference_v<Type>,
            std::add_pointer_t<std::remove_reference_t<Type>>,
            Type>>;

    constexpr value_or_errc() = default;

    constexpr explicit value_or_errc(auto && value) :
        m_return_value(std::forward<decltype(value)>(value))
    {
    }

    constexpr explicit value_or_errc(error_type error) :
        m_error(std::forward<decltype(error)>(error))
    {
    }

    constexpr value_or_errc(value_or_errc && other) noexcept
    {
        if (other.is_value()) {
            if constexpr (!std::is_void_v<Type>) {
                if constexpr (!std::is_reference_v<Type>) {
                    ::new (std::addressof(m_return_value))
                        Type(std::move(other.m_return_value));
                } else {
                    m_return_value = other.m_return_value;
                }
            }
        } else {
            m_failure = other.m_failure;
            std::memcpy(&m_error, &other.m_error, sizeof(m_error));
        }
    }

    constexpr ~value_or_errc()
    {
        if constexpr (!std::is_void_v<Type> &&
                      !std::is_trivially_destructible_v<Type>) {
            if (success()) {
                m_return_value.~Type();
            }
        }
    }

    constexpr bool success() const noexcept
    {
        return !m_failure;
    }

    constexpr bool failure() const noexcept
    {
        return m_failure;
    }

    constexpr decltype(auto) value() & noexcept
    {
        if constexpr (std::is_same_v<Type, decltype(m_return_value)>) {
            return (m_return_value);
        } else {
            return (*m_return_value);
        }
    }

    constexpr decltype(auto) value() && noexcept
    {
        if constexpr (std::is_same_v<Type, decltype(m_return_value)>) {
            return std::forward<Type>(m_return_value);
        } else {
            return std::forward<Type>(*m_return_value);
        }
    }

    constexpr decltype(auto) value() const & noexcept
    {
        if constexpr (std::is_same_v<Type, decltype(m_return_value)>) {
            return (m_return_value);
        } else {
            return (*m_return_value);
        }
    }

    constexpr auto error() const noexcept
    {
        return m_error;
    }

    #if __has_include("zpp_throwing.h")
    constexpr zpp::throwing<Type> operator co_await() &&
    {
        if (failure()) [[unlikely]] {
            return error().code;
        }
        return std::move(*this).value();
    }

    constexpr zpp::throwing<Type> operator co_await() const &
    {
        if (failure()) [[unlikely]] {
            return error().code;
        }
        return value();
    }
#endif

    constexpr decltype(auto) or_throw() &
    {
        if (failure()) [[unlikely]] {
#ifdef __cpp_exceptions
            throw std::system_error(std::make_error_code(error().code));
#else
            std::abort();
#endif
        }
        return value();
    }

    constexpr decltype(auto) or_throw() &&
    {
        if (failure()) [[unlikely]] {
#ifdef __cpp_exceptions
            throw std::system_error(std::make_error_code(error().code));
#else
            std::abort();
#endif
        }
        return std::move(*this).value();
    }

    constexpr decltype(auto) or_throw() const &
    {
        if (failure()) [[unlikely]] {
#ifdef __cpp_exceptions
            throw std::system_error(std::make_error_code(error().code));
#else
            std::abort();
#endif
        }
        return value();
    }

    union
    {
        error_type m_error{};
        value_type m_return_value;
    };
    bool m_failure{};
};

ZPP_BITS_INLINE constexpr auto
apply(auto && function, auto && archive) requires(
    std::remove_cvref_t<decltype(archive)>::kind() == kind::in)
{
    using function_type = std::decay_t<decltype(function)>;

    if constexpr (requires { &function_type::operator(); }) {
        using parameters_type =
            function_parameters_t<decltype(&function_type::operator())>;
        using return_type =
            function_return_type_t<decltype(&function_type::operator())>;
        if constexpr (std::is_void_v<parameters_type>) {
            return std::forward<decltype(function)>(function)();
        } else {
            parameters_type parameters;
            if constexpr (std::is_void_v<return_type>) {
                if (auto result = archive(parameters); failure(result))
                    [[unlikely]] {
                    return result;
                }
                std::apply(std::forward<decltype(function)>(function),
                           std::move(parameters));
                return errc{};
            } else {
                if (auto result = archive(parameters); failure(result))
                    [[unlikely]] {
                    return value_or_errc<return_type>{result};
                }
                return value_or_errc<return_type>{
                    std::apply(std::forward<decltype(function)>(function),
                               std::move(parameters))};
            }
        }
    } else {
        using parameters_type = function_parameters_t<function_type>;
        using return_type = function_return_type_t<function_type>;
        if constexpr (std::is_void_v<parameters_type>) {
            return std::forward<decltype(function)>(function)();
        } else {
            parameters_type parameters;
            if constexpr (std::is_void_v<return_type>) {
                if (auto result = archive(parameters); failure(result))
                    [[unlikely]] {
                    return result;
                }
                std::apply(std::forward<decltype(function)>(function),
                           std::move(parameters));
                return errc{};
            } else {
                if (auto result = archive(parameters); failure(result))
                    [[unlikely]] {
                    return value_or_errc<return_type>{result};
                }
                return value_or_errc<return_type>{
                    std::apply(std::forward<decltype(function)>(function),
                               std::move(parameters))};
            }
        }
    }
}

ZPP_BITS_INLINE constexpr auto
apply(auto && self, auto && function, auto && archive) requires(
    std::remove_cvref_t<decltype(archive)>::kind() == kind::in)
{
    using parameters_type = function_parameters_t<
        std::remove_cvref_t<decltype(function)>>;
    using return_type = function_return_type_t<
        std::remove_cvref_t<decltype(function)>>;
    if constexpr (std::is_void_v<parameters_type>) {
        return (std::forward<decltype(self)>(self).*
                std::forward<decltype(function)>(function))();
    } else {
        parameters_type parameters;
        if constexpr (std::is_void_v<return_type>) {
            if (auto result = archive(parameters); failure(result))
                [[unlikely]] {
                return result;
            }
            // Ignore GCC issue.
#if defined __GNUC__ && !defined __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
            std::apply(
                [&](auto &&... arguments) -> decltype(auto) {
                    return (std::forward<decltype(self)>(self).*
                            std::forward<decltype(function)>(function))(
                        std::forward<decltype(arguments)>(arguments)...);
                },
                std::move(parameters));
#if defined __GNUC__ && !defined __clang__
#pragma GCC diagnostic pop
#endif
            return errc{};
        } else {
            if (auto result = archive(parameters); failure(result))
                [[unlikely]] {
                return value_or_errc<return_type>{result};
            }
            return value_or_errc<return_type>(std::apply(
                [&](auto &&... arguments) -> decltype(auto) {
            // Ignore GCC issue.
#if defined __GNUC__ && !defined __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
                    return (std::forward<decltype(self)>(self).*
                            std::forward<decltype(function)>(function))(
                        std::forward<decltype(arguments)>(arguments)...);
#if defined __GNUC__ && !defined __clang__
#pragma GCC diagnostic pop
#endif
                },
                std::move(parameters)));
        }
    }
}

template <auto Function, auto Id, auto MaxSize = -1>
struct bind
{
    using id = zpp::bits::id<Id, MaxSize>;
    using function_type = decltype(Function);
    using parameters_type =
        typename function_traits<function_type>::parameters_type;
    using return_type =
        typename function_traits<function_type>::return_type;
    static constexpr auto opaque = false;

    ZPP_BITS_INLINE constexpr static decltype(auto) call(auto && archive,
                                                         auto && context)
    {
        if constexpr (std::is_member_function_pointer_v<
                          std::remove_cvref_t<decltype(Function)>>) {
            return apply(context, Function, archive);
        } else {
            return apply(Function, archive);
        }
    }
};

template <auto Function, auto Id, auto MaxSize = -1>
struct bind_opaque
{
    using id = zpp::bits::id<Id, MaxSize>;
    using function_type = decltype(Function);
    using parameters_type =
        typename function_traits<function_type>::parameters_type;
    using return_type =
        typename function_traits<function_type>::return_type;
    static constexpr auto opaque = true;

    ZPP_BITS_INLINE constexpr static decltype(auto) call(auto && in,
                                                         auto && out,
                                                         auto && context)
    {
        if constexpr (std::is_member_function_pointer_v<
                          std::remove_cvref_t<decltype(Function)>>) {
            if constexpr (requires { (context.*Function)(in, out); }) {
                return (context.*Function)(in, out);
            } else if constexpr (requires { (context.*Function)(in); }) {
                return (context.*Function)(in);
            } else if constexpr (requires { (context.*Function)(out); }) {
                return (context.*Function)(out);
            } else if constexpr (
                requires(decltype(in.remaining_data()) & data) {
                    (context.*Function)(data);
                }) {
                struct _
                {
                    decltype(in) archive;
                    decltype(in.remaining_data()) data;
                    constexpr ~_()
                    {
                        archive.position() += data.size();
                    }
                } _{in, in.remaining_data()};
                return (context.*Function)(_.data);
            } else {
                return (context.*Function)();
            }
        } else {
            if constexpr (requires { Function(in, out); }) {
                return Function(in, out);
            } else if constexpr (requires { Function(in); }) {
                return Function(in);
            } else if constexpr (requires { Function(out); }) {
                return Function(out);
            } else if constexpr (
                requires(decltype(in.remaining_data()) & data) {
                    Function(data);
                }) {
                struct _
                {
                    decltype(in) archive;
                    decltype(in.remaining_data()) data;
                    constexpr ~_()
                    {
                        archive.position() += data.size();
                    }
                } _{in, in.remaining_data()};
                return Function(_.data);
            } else {
                return Function();
            }
        }
    }
};

template <typename... Bindings>
struct rpc_impl
{
    using id = std::remove_cvref_t<
        decltype(std::remove_cvref_t<decltype(get<0>(
                     std::tuple<Bindings...>{}))>::id::value)>;

    template <typename In, typename Out>
    struct client
    {
        constexpr client(In && in, Out && out) :
            in(in),
            out(out)
        {
        }

        constexpr client(client && other) = default;

        constexpr ~client()
        {
            static_assert(std::remove_cvref_t<decltype(in)>::kind() == kind::in);
            static_assert(std::remove_cvref_t<decltype(out)>::kind() == kind::out);
        }

        template <typename Id, typename FirstBinding, typename... OtherBindings>
        constexpr auto binding()
        {
            if constexpr (std::same_as<Id, typename FirstBinding::id>) {
                return FirstBinding{};
            } else {
                static_assert(sizeof...(OtherBindings));
                return binding<Id, OtherBindings...>();
            }
        }

        template <typename Id, std::size_t... Indices>
        constexpr auto request(std::index_sequence<Indices...>,
                               auto &&... arguments)
        {
            using request_binding = decltype(binding<Id, Bindings...>());
            using parameters_type =
                typename request_binding::parameters_type;

            if constexpr (std::is_void_v<parameters_type>) {
                static_assert(!sizeof...(arguments));
                return out(Id::value);
            } else if constexpr (request_binding::opaque) {
                return out(Id::value, arguments...);;
            } else if constexpr (std::same_as<
                                     std::tuple<std::remove_cvref_t<
                                         decltype(arguments)>...>,
                                     parameters_type>

            ) {
                return out(Id::value, arguments...);
            } else {
                static_assert(requires {
                    {parameters_type{
                        std::forward_as_tuple<decltype(arguments)...>(
                            arguments...)}};
                });

                return out(
                    Id::value,
                    static_cast<std::conditional_t<
                        std::is_fundamental_v<
                            std::remove_cvref_t<decltype(get<Indices>(
                                std::declval<parameters_type>()))>> ||
                            std::is_enum_v<
                                std::remove_cvref_t<decltype(get<Indices>(
                                    std::declval<parameters_type>()))>>,
                        std::remove_cvref_t<decltype(get<Indices>(
                            std::declval<parameters_type>()))>,
                        const decltype(get<Indices>(
                            std::declval<parameters_type>())) &>>(
                        arguments)...);
            }
        }

        template <typename Id>
        constexpr auto request(auto &&... arguments)
        {
            return request<Id>(
                std::make_index_sequence<sizeof...(arguments)>{},
                arguments...);
        }

        template <auto Id, auto MaxSize = -1>
        constexpr auto request(auto &&... arguments)
        {
            return request<zpp::bits::id<Id, MaxSize>>(arguments...);
        }

        template <typename Id, std::size_t... Indices>
        constexpr auto request_body(std::index_sequence<Indices...>,
                                    auto &&... arguments)
        {
            using request_binding = decltype(binding<Id, Bindings...>());
            using parameters_type =
                typename request_binding::parameters_type;

            if constexpr (std::is_void_v<parameters_type>) {
                static_assert(!sizeof...(arguments));
                return;
            } else if constexpr (request_binding::opaque) {
                return out(arguments...);;
            } else if constexpr (std::same_as<
                                     std::tuple<std::remove_cvref_t<
                                         decltype(arguments)>...>,
                                     parameters_type>

            ) {
                return out(arguments...);
            } else {
                static_assert(requires {
                    {parameters_type{
                        std::forward_as_tuple<decltype(arguments)...>(
                            arguments...)}};
                });

                return out(
                    static_cast<std::conditional_t<
                        std::is_fundamental_v<
                            std::remove_cvref_t<decltype(get<Indices>(
                                std::declval<parameters_type>()))>> ||
                            std::is_enum_v<
                                std::remove_cvref_t<decltype(get<Indices>(
                                    std::declval<parameters_type>()))>>,
                        std::remove_cvref_t<decltype(get<Indices>(
                            std::declval<parameters_type>()))>,
                        const decltype(get<Indices>(
                            std::declval<parameters_type>())) &>>(
                        arguments)...);
            }
        }

        template <typename Id>
        constexpr auto request_body(auto &&... arguments)
        {
            return request_body<Id>(
                std::make_index_sequence<sizeof...(arguments)>{},
                arguments...);
        }

        template <auto Id, auto MaxSize = -1>
        constexpr auto request_body(auto &&... arguments)
        {
            return request_body<zpp::bits::id<Id, MaxSize>>(arguments...);
        }

        template <typename Id>
        constexpr auto response()
        {
            using request_binding = decltype(binding<Id, Bindings...>());
            using return_type = typename request_binding::return_type;

            if constexpr (std::is_void_v<return_type>) {
                return;
#if __has_include("zpp_throwing.h")
            } else if constexpr (requires(return_type && value) {
                                     value.await_ready();
                                 }) {
                using nested_return = std::remove_cvref_t<
                    decltype(std::declval<return_type>().await_resume())>;
                if constexpr (std::is_void_v<nested_return>) {
                    return;
                } else {
                    nested_return return_value;
                    if (auto result = in(return_value); failure(result))
                        [[unlikely]] {
                        return value_or_errc<nested_return>{result};
                    }
                    return value_or_errc<nested_return>{std::move(return_value)};
                }
#endif
            } else {
                return_type return_value;
                if (auto result = in(return_value); failure(result))
                    [[unlikely]] {
                    return value_or_errc<return_type>{result};
                }
                return value_or_errc<return_type>{std::move(return_value)};
            }
        }

        template <auto Id, auto MaxSize = -1>
        constexpr auto response()
        {
            return response<zpp::bits::id<Id, MaxSize>>();
        }

        In & in;
        Out & out;
    };

#if defined __clang__ || !defined __GNUC__ || __GNUC__ >= 12 // GCC issue
    template <typename... Types>
    client(Types && ...) -> client<Types&&...>;
#endif

    template <typename In, typename Out, typename Context = std::monostate>
    struct server
    {
        constexpr server(In && in, Out && out) :
            in(in),
            out(out)
        {
        }

        constexpr server(In && in, Out && out, Context && context) :
            in(in),
            out(out),
            context(context)
        {
        }

        constexpr server(server && other) = default;

        constexpr ~server()
        {
            static_assert(std::remove_cvref_t<decltype(in)>::kind() == kind::in);
            static_assert(std::remove_cvref_t<decltype(out)>::kind() == kind::out);
        }

        template <typename FirstBinding, typename... OtherBindings>
        ZPP_BITS_INLINE constexpr auto
        call_binding(auto & id) requires(!FirstBinding::opaque)
        {
            if (FirstBinding::id::value == id) {
                if constexpr (std::is_void_v<decltype(FirstBinding::call(
                                  in, context))>) {
                    FirstBinding::call(in, context);
                    return errc{};
                } else if constexpr (std::same_as<
                                         decltype(FirstBinding::call(
                                             in, context)),
                                         errc>) {
                    if (auto result = FirstBinding::call(in, context);
                        failure(result)) [[unlikely]] {
                        return result;
                    }
                    return errc{};
                } else if constexpr (std::is_void_v<typename FirstBinding::
                                                        parameters_type>) {
                    return out(FirstBinding::call(in, context));
                } else {
                    if (auto result = FirstBinding::call(in, context);
                        failure(result)) [[unlikely]] {
                        return result.error();
                    } else {
                        return out(result.value());
                    }
                }
            } else {
                if constexpr (!sizeof...(OtherBindings)) {
                    return errc{std::errc::not_supported};
                } else {
                    return call_binding<OtherBindings...>(id);
                }
            }
        }

        template <typename FirstBinding, typename... OtherBindings>
        ZPP_BITS_INLINE constexpr auto
        call_binding(auto & id) requires FirstBinding::opaque
        {
            if (FirstBinding::id::value == id) {
                if constexpr (std::is_void_v<decltype(FirstBinding::call(
                                  in, out, context))>) {
                    FirstBinding::call(in, out, context);
                    return errc{};
                } else if constexpr (std::same_as<
                                         decltype(FirstBinding::call(
                                             in, out, context)),
                                         errc>) {
                    if (auto result = FirstBinding::call(in, out, context);
                        failure(result)) [[unlikely]] {
                        return result;
                    }
                    return errc{};
                } else if constexpr (
                    requires {
                        requires std::same_as<
                            typename decltype(FirstBinding::call(
                                in, out, context))::value_type,
                            value_or_errc<decltype(FirstBinding::call(
                                in, out, context))>>;
                    }) {
                    if (auto result = FirstBinding::call(in, out, context);
                        failure(result)) [[unlikely]] {
                        return result.error();
                    } else {
                        return out(result.value());
                    }
                } else {
                    return out(FirstBinding::call(in, out, context));
                }
            } else {
                if constexpr (!sizeof...(OtherBindings)) {
                    return errc{std::errc::not_supported};
                } else {
                    return call_binding<OtherBindings...>(id);
                }
            }
        }

#if __has_include("zpp_throwing.h")
        template <typename FirstBinding, typename... OtherBindings>
        zpp::throwing<void>
        call_binding_throwing(auto & id) requires(!FirstBinding::opaque)
        {
            if (FirstBinding::id::value == id) {
                if constexpr (std::is_void_v<decltype(FirstBinding::call(
                                  in, context))>) {
                    FirstBinding::call(in, context);
                    co_return;
                } else if constexpr (std::same_as<
                                         decltype(FirstBinding::call(
                                             in, context)),
                                         errc>) {
                    if (auto result = FirstBinding::call(in, context);
                        failure(result)) [[unlikely]] {
                        co_yield result.code;
                    }
                    co_return;
                } else if constexpr (std::is_void_v<typename FirstBinding::
                                                        parameters_type>) {
                    if constexpr (requires {
                                      FirstBinding::call(in, context)
                                          .await_ready();
                                  }) {
                        if constexpr (std::is_void_v<
                                          decltype(FirstBinding::call(
                                                       in, context)
                                                       .await_resume())>) {
                            co_await FirstBinding::call(in, context);
                        } else {
                            co_await out(
                                co_await FirstBinding::call(in, context));
                        }
                    } else {
                        co_await out(FirstBinding::call(in, context));
                    }
                } else {
                    if (auto result = FirstBinding::call(in, context);
                        failure(result)) [[unlikely]] {
                        co_yield result.error().code;
                    } else if constexpr (requires {
                                             result.value().await_ready();
                                         }) {
                        if constexpr (!std::is_void_v<
                                          decltype(result.value()
                                                       .await_resume())>) {
                            co_await out(co_await result.value());
                        }
                        co_return;
                    } else {
                        co_await out(result.value());
                    }
                }
            } else {
                if constexpr (!sizeof...(OtherBindings)) {
                    co_yield std::errc::not_supported;
                } else {
                    co_return co_await call_binding_throwing<
                        OtherBindings...>(id);
                }
            }
        }

        template <typename FirstBinding, typename... OtherBindings>
        zpp::throwing<void>
        call_binding_throwing(auto & id) requires FirstBinding::opaque
        {
            if (FirstBinding::id::value == id) {
                if constexpr (std::is_void_v<decltype(FirstBinding::call(
                                  in, out, context))>) {
                    FirstBinding::call(in, out, context);
                    co_return;
                } else if constexpr (std::same_as<
                                         decltype(FirstBinding::call(
                                             in, out, context)),
                                         errc>) {
                    if (auto result = FirstBinding::call(in, out, context);
                        failure(result)) [[unlikely]] {
                        co_yield result.code;
                    }
                    co_return;
                } else if constexpr (
                    requires {
                        requires std::same_as<
                            typename decltype(FirstBinding::call(
                                in, out, context))::value_type,
                            value_or_errc<decltype(FirstBinding::call(
                                in, out, context))>>;
                    }) {
                    if (auto result = FirstBinding::call(in, out, context);
                        failure(result)) [[unlikely]] {
                        co_yield result.error().code;
                    } else if constexpr (requires {
                                             result.value().await_ready();
                                         }) {
                        if constexpr (!std::is_void_v<
                                          decltype(result.value()
                                                       .await_resume())>) {
                            co_await out(co_await result.value());
                        }
                        co_return;
                    } else {
                        co_await out(result.value());
                    }
                } else {
                    if constexpr (requires {
                                      FirstBinding::call(in, out, context)
                                          .await_ready();
                                  }) {
                        if constexpr (std::is_void_v<
                                          decltype(FirstBinding::call(
                                                       in, out, context)
                                                       .await_resume())>) {
                            co_await FirstBinding::call(in, out, context);
                        } else {
                            co_await out(
                                co_await FirstBinding::call(in, out, context));
                        }
                    } else {
                        co_await out(FirstBinding::call(in, out, context));
                    }
                }
            } else {
                if constexpr (!sizeof...(OtherBindings)) {
                    co_yield std::errc::not_supported;
                } else {
                    co_return co_await call_binding_throwing<
                        OtherBindings...>(id);
                }
            }
        }
#endif

        constexpr auto serve(auto && id)
        {
#if __has_include("zpp_throwing.h")
            if constexpr ((... || requires {
                              std::declval<
                                  typename Bindings::return_type>()
                                  .await_ready();
                          })) {
                return call_binding_throwing<Bindings...>(id);
            } else {
#endif
                return call_binding<Bindings...>(id);
#if __has_include("zpp_throwing.h")
            }
#endif
        }

        constexpr auto serve()
        {
            rpc_impl::id id;
            if (auto result = in(id); failure(result)) [[unlikely]] {
                return decltype(serve(rpc_impl::id{})){result.code};
            }

            return serve(id);
        }

        In & in;
        Out & out;
        [[no_unique_address]] Context context;
    };

#if defined __clang__ || !defined __GNUC__ || __GNUC__ >= 12 // GCC issue
    template <typename... Types>
    server(Types && ...) -> server<Types&&...>;
#endif

#if defined __clang__ || !defined __GNUC__ || __GNUC__ >= 12 // GCC issue
    constexpr static auto client_server(auto && in, auto && out, auto &&... context)
    {
        return std::tuple{client{in, out}, server{in, out, context...}};
    }
#else
    constexpr static auto client_server(auto && in, auto && out)
    {
        return std::tuple{client<decltype(in), decltype(out)>{in, out},
                          server<decltype(in), decltype(out)>{in, out}};
    }

    constexpr static auto client_server(auto && in, auto && out, auto && context)
    {
        return std::tuple{
            client<decltype(in), decltype(out)>{in, out},
            server<decltype(in), decltype(out), decltype(context)>{
                in, out, context}};
    }
#endif
};

template <typename... Bindings>
struct rpc_checker
{
    using check_unique_id = traits::variant<
        std::variant<traits::id_serializable<typename Bindings::id>...>>;
    using type = rpc_impl<Bindings...>;
};

template <typename... Bindings>
using rpc = typename rpc_checker<Bindings...>::type;

struct pb_reserved
{
};

template <std::size_t From, std::size_t To>
struct pb_map
{
    static_assert(From != 0 && To != 0);

    constexpr static unsigned int mapped_field(auto index)
    {
        return ((index + 1) == From) ? To : 0u;
    }
};

template <typename Type, auto FieldNumber>
struct pb_field_fundamental
{
    using value_type = Type;
    using pb_field_type = Type;

    constexpr static auto pb_field_number = FieldNumber;

    constexpr pb_field_fundamental() = default;

    constexpr pb_field_fundamental(Type value) :
        value(value)
    {
    }

    constexpr operator Type &() &
    {
        return value;
    }

    constexpr operator Type() const
    {
        return value;
    }

    Type value{};
};

template <typename Type, auto FieldNumber>
constexpr decltype(auto)
pb_value(pb_field_fundamental<Type, FieldNumber> & pb)
{
    return static_cast<
        typename pb_field_fundamental<Type, FieldNumber>::pb_field_type &>(
        pb);
}

template <typename Type, auto FieldNumber>
constexpr auto pb_value(const pb_field_fundamental<Type, FieldNumber> & pb)
{
    return static_cast<
        typename pb_field_fundamental<Type, FieldNumber>::pb_field_type>(
        pb);
}

template <typename Type, auto FieldNumber>
struct pb_field_struct : Type
{
    using Type::Type;
    using Type::operator=;
    using pb_field_type = Type;

    static constexpr auto pb_field_number = FieldNumber;

    constexpr pb_field_struct(Type && other) noexcept(
        std::is_nothrow_move_constructible_v<Type>) :
        Type(std::move(other))
    {
    }

    constexpr pb_field_struct(const Type & other)
        : Type(other)
    {
    }
};

template <typename Type, auto FieldNumber>
constexpr decltype(auto) pb_value(pb_field_struct<Type, FieldNumber> & pb)
{
    return static_cast<
        typename pb_field_struct<Type, FieldNumber>::pb_field_type &>(pb);
}

template <typename Type, auto FieldNumber>
constexpr decltype(auto)
pb_value(const pb_field_struct<Type, FieldNumber> & pb)
{
    return static_cast<const typename pb_field_struct<Type, FieldNumber>::
                           pb_field_type &>(pb);
}

template <typename Type, auto FieldNumber>
constexpr decltype(auto) pb_value(pb_field_struct<Type, FieldNumber> && pb)
{
    return static_cast<
        typename pb_field_struct<Type, FieldNumber>::pb_field_type &&>(pb);
}

template <typename Type, auto FieldNumber>
using pb_field =
    std::conditional_t<std::is_class_v<Type>,
                       pb_field_struct<Type, FieldNumber>,
                       pb_field_fundamental<Type, FieldNumber>>;

template <typename... Options>
struct pb
{
    using pb_default = pb<>;

    constexpr pb(Options && ...)
    {
    }

    template <std::size_t Index>
    constexpr static auto has_mapped_field(auto option)
    {
        return requires
        {
            requires decltype(option)::mapped_field(Index) != 0;
        };
    }

    template <std::size_t Index>
    constexpr static auto get_mapped_field(auto option)
    {
        if constexpr (requires {
                          requires decltype(option)::mapped_field(Index) != 0;
                      }) {
            return decltype(option)::mapped_field(Index);
        } else {
            return 0u;
        }
    }

    template <std::size_t Index>
    struct field_number_visitor
    {
        template <typename... Types>
        constexpr auto operator()() const
        {
            if constexpr (requires {
                              std::remove_cvref_t<decltype(std::get<Index>(
                                  std::declval<std::tuple<Types...>>()))>::
                                  pb_field_number;
                          }) {
                static_assert(0 !=
                              std::remove_cvref_t<decltype(std::get<Index>(
                                  std::declval<std::tuple<Types...>>()))>::
                                  pb_field_number);
                return std::integral_constant<
                    unsigned int,
                    std::remove_cvref_t<decltype(std::get<Index>(
                        std::declval<std::tuple<Types...>>()))>::
                        pb_field_number>();
            } else {
                return std::integral_constant<unsigned int, 0>{};
            }
        }
    };

    template <typename Type, std::size_t Index>
    constexpr static auto field_number_from_struct()
    {
        constexpr auto explicit_field_number =
            visit_members_types<Type>(field_number_visitor<Index>{})();
        if constexpr (explicit_field_number > 0) {
            return explicit_field_number;
        } else {
            static_assert(
                (0 + ... + std::size_t(has_mapped_field<Index>(Options{}))) <=
                1);

            constexpr auto mapped_field =
                (0 + ... + get_mapped_field<Index>(Options{}));
            if constexpr (mapped_field != 0) {
                return mapped_field;
            } else {
                return Index + 1;
            }
        }
    }

    template <typename Type, std::size_t Index>
    constexpr static auto field_number()
    {
        if constexpr (requires { requires(Type::pb_field_number > 0); }) {
            return Type::pb_field_number;
        } else {
            static_assert(
                (0 + ... + std::size_t(has_mapped_field<Index>(Options{}))) <=
                1);

            constexpr auto mapped_field =
                (0 + ... + get_mapped_field<Index>(Options{}));
            if constexpr (mapped_field != 0) {
                return mapped_field;
            } else {
                return Index + 1;
            }
        }
    }

    template <typename Type, std::size_t... Indices>
    constexpr static auto unique_field_numbers(std::index_sequence<Indices...>)
    {
        return traits::unique(
            std::size_t{field_number_from_struct<Type, Indices>()}...);
    }

    template <typename Type>
    constexpr static auto unique_field_numbers()
    {
        constexpr auto members =
            number_of_members<std::remove_cvref_t<Type>>();
        if constexpr (members >= 0) {
            return unique_field_numbers<std::remove_cvref_t<Type>>(
                std::make_index_sequence<members>());
        } else {
            static_assert(members >= 0);
        }
    }

    template <typename Type>
    constexpr static auto is_pb_field()
    {
        using type = std::remove_cvref_t<Type>;
        return requires
        {
            requires std::same_as<type,
                                  pb_field<typename type::pb_field_type,
                                           type::pb_field_number>>;
        };
    }

    template <typename Type>
    constexpr static auto check_type()
    {
        using type = std::remove_cvref_t<Type>;
        if constexpr (is_pb_field<type>()) {
            return check_type<typename type::pb_field_type>();
        } else if constexpr (!std::is_class_v<type> ||
                             concepts::varint<type> ||
                             concepts::empty<type>) {
            return true;
        } else if constexpr (concepts::associative_container<type> &&
                             requires { typename type::mapped_type; }) {
            static_assert(
                requires {
                    type{}.push_back(typename type::value_type{});
                } ||
                requires { type{}.insert(typename type::value_type{}); });
            static_assert(check_type<typename type::key_type>());
            static_assert(check_type<typename type::mapped_type>());
            return true;
        } else if constexpr (concepts::container<type>) {
            static_assert(
                requires {
                    type{}.push_back(typename type::value_type{});
                } ||
                requires { type{}.insert(typename type::value_type{}); });
            static_assert(check_type<typename type::value_type>());
            return true;
        } else if constexpr (concepts::by_protocol<type>) {
            static_assert(
                std::same_as<pb_default,
                             typename decltype(access::get_protocol<
                                               type>())::pb_default>);
            static_assert(unique_field_numbers<type>());
            return true;
        } else {
            static_assert(!sizeof(Type));
        }
    }

    enum class wire_type : unsigned int
    {
        varint = 0,
        fixed_64 = 1,
        length_delimited = 2,
        fixed_32 = 5,
    };

    constexpr static auto make_tag_explicit(wire_type type, auto field_number)
    {
        return varint{(field_number << 3) |
                      std::underlying_type_t<wire_type>(type)};
    }

    constexpr static auto tag_type(auto tag)
    {
        return wire_type(tag & 0x7);
    }

    constexpr static auto tag_number(auto tag)
    {
        return (unsigned int)(tag >> 3);
    }

    template <typename Type>
    constexpr static auto tag_type()
    {
        using type = std::remove_cvref_t<Type>;
        if constexpr (is_pb_field<type>()) {
            return tag_type<typename type::pb_field_type>();
        } else if constexpr (concepts::varint<type> ||
                      (std::is_enum_v<type> &&
                       !std::same_as<type, std::byte>) ||
                      std::same_as<type, bool>) {
            return wire_type::varint;
        } else if constexpr (std::is_integral_v<type> ||
                             std::is_floating_point_v<type>) {
            if constexpr (sizeof(type) == 4) {
                return wire_type::fixed_32;
            } else if constexpr (sizeof(type) == 8) {
                return wire_type::fixed_64;
            } else {
                static_assert(!sizeof(type));
            }
        } else {
            return wire_type::length_delimited;
        }
    }

    template <typename Type>
    constexpr static auto make_tag_explicit(auto field_number)
    {
        return make_tag_explicit(tag_type<Type>(), field_number);
    }

    template <typename Type, auto Index>
    constexpr static auto make_tag()
    {
        return make_tag_explicit(tag_type<Type>(),
                                 field_number<Type, Index>());
    }

    template <wire_type WireType, typename Type, auto Index>
    constexpr static auto make_tag()
    {
        return make_tag_explicit(WireType, field_number<Type, Index>());
    }

    ZPP_BITS_INLINE constexpr auto
    operator()(auto & archive, auto & item) const requires(
        std::remove_cvref_t<decltype(archive)>::kind() == kind::out)
    {
        using type = std::remove_cvref_t<decltype(item)>;
        static_assert(check_type<type>());

        using archive_type = typename std::remove_cvref_t<decltype(archive)>;
        if constexpr (!concepts::varint<
                          typename archive_type::default_size_type> ||
                      ((std::endian::little != std::endian::native) &&
                       !archive_type::endian_aware)) {
            out out{archive.data(),
                    size_varint{},
                    no_fit_size{},
                    endian::little{},
                    enlarger<std::get<0>(archive_type::enlarger),
                             std::get<1>(archive_type::enlarger)>{},
                    std::conditional_t<archive_type::no_enlarge_overflow,
                                       no_enlarge_overflow,
                                       enlarge_overflow>{},
                    alloc_limit<archive_type::allocation_limit>{}};
            out.position() = archive.position();
            if constexpr (concepts::self_referencing<type>) {
                auto result = visit_members(
                    item,
                    [&](auto &&... items) constexpr {
                        static_assert((... && check_type<decltype(items)>()));
                        return serialize_many(
                            std::make_index_sequence<sizeof...(items)>{},
                            out,
                            items...);
                    });
                archive.position() = out.position();
                return result;
            } else {
                auto result = visit_members(
                    item,
                    [&](auto &&... items) ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                        static_assert((... && check_type<decltype(items)>()));
                        return serialize_many(
                            std::make_index_sequence<sizeof...(items)>{},
                            out,
                            items...);
                    });
                archive.position() = out.position();
                return result;
            }
        } else if constexpr (concepts::self_referencing<type>) {
            return visit_members(
                item,
                [&](auto &&... items) constexpr {
                    static_assert((... && check_type<decltype(items)>()));
                    return serialize_many(
                        std::make_index_sequence<sizeof...(items)>{},
                        archive,
                        items...);
                });
        } else {
            return visit_members(
                item,
                [&](auto &&... items) ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                    static_assert((... && check_type<decltype(items)>()));
                    return serialize_many(
                        std::make_index_sequence<sizeof...(items)>{},
                        archive,
                        items...);
                });
        }
    }

    template <std::size_t FirstIndex, std::size_t... Indices>
    ZPP_BITS_INLINE constexpr static auto serialize_many(
        std::index_sequence<FirstIndex, Indices...>,
        auto & archive,
        auto & first_item,
        auto &... items) requires(std::remove_cvref_t<decltype(archive)>::
                                      kind() == kind::out)
    {
        if (auto result = serialize_one<FirstIndex>(archive, first_item);
            failure(result)) [[unlikely]] {
            return result;
        }

        return serialize_many(
            std::index_sequence<Indices...>{}, archive, items...);
    }

    ZPP_BITS_INLINE constexpr static errc
    serialize_many(std::index_sequence<>, auto & archive) requires(
        std::remove_cvref_t<decltype(archive)>::kind() == kind::out)
    {
        return {};
    }

    template <std::size_t Index, typename TagType = void>
    ZPP_BITS_INLINE constexpr static errc
    serialize_one(auto & archive, auto & item) requires(
        std::remove_cvref_t<decltype(archive)>::kind() == kind::out)
    {
        using type = std::remove_cvref_t<decltype(item)>;
        using tag_type = std::conditional_t<std::is_void_v<TagType>, type, TagType>;

        if constexpr (concepts::empty<type>) {
            return {};
        } else if constexpr (is_pb_field<type>()) {
            return serialize_one<Index, tag_type>(
                archive,
                static_cast<const typename type::pb_field_type &>(item));
        } else if constexpr (std::is_enum_v<type> &&
                             !std::same_as<type, std::byte>) {
            constexpr auto tag = make_tag<tag_type, Index>();
            if (auto result = archive(
                    tag, varint{std::underlying_type_t<type>(item)});
                failure(result)) [[unlikely]] {
                return result;
            }
            return {};
        } else if constexpr (!concepts::container<type>) {
            constexpr auto tag = make_tag<tag_type, Index>();
            if (auto result = archive(tag, item); failure(result))
                [[unlikely]] {
                return result;
            }
            return {};
        } else if constexpr (concepts::associative_container<type> &&
                             requires { typename type::mapped_type; }) {
            constexpr auto tag = make_tag<tag_type, Index>();

            using key_type = std::conditional_t<
                std::is_enum_v<typename type::key_type> &&
                    !std::same_as<typename type::key_type, std::byte>,
                varint<typename type::key_type>,
                typename type::key_type>;

            using mapped_type = std::conditional_t<
                std::is_enum_v<typename type::mapped_type> &&
                    !std::same_as<typename type::mapped_type, std::byte>,
                varint<typename type::mapped_type>,
                typename type::mapped_type>;

            struct value_type
            {
                const key_type & key;
                const mapped_type & value;

                using serialize = protocol<pb_default{}>;
                serialize use();
            };

            for (auto & [key, value] : item) {
                if (auto result = archive(
                        tag, value_type{.key = key, .value = value});
                    failure(result)) [[unlikely]] {
                    return result;
                }
            }

            return {};
        } else if constexpr (requires {
                                 requires std::is_fundamental_v<
                                     typename type::value_type> ||
                                     std::same_as<
                                         typename type::value_type,
                                         std::byte>;
                             }) {
            constexpr auto tag = make_tag<tag_type, Index>();
            auto size = item.size();
            if (!size) [[unlikely]] {
                return {};
            }
            if (auto result = archive(
                    tag,
                    varint{size * sizeof(typename type::value_type)},
                    unsized(item));
                failure(result)) [[unlikely]] {
                return result;
            }
            return {};
        } else if constexpr (requires {
                                 requires concepts::varint<
                                     typename type::value_type>;
                             }) {
            constexpr auto tag = make_tag<tag_type, Index>();

            std::size_t size = {};
            for (auto & element : item) {
                size +=
                    varint_size<type::value_type::encoding>(element.value);
            }
            if (!size) [[unlikely]] {
                return {};
            }
            if (auto result = archive(tag, varint{size}, unsized(item));
                failure(result)) [[unlikely]] {
                return result;
            }
            return {};
        } else if constexpr (requires {
                                 requires std::is_enum_v<
                                     typename type::value_type>;
                             }) {
            constexpr auto tag = make_tag<tag_type, Index>();

            using type = typename type::value_type;
            std::size_t size = {};
            for (auto & element : item) {
                size += varint_size(std::underlying_type_t<type>(element));
            }
            if (!size) [[unlikely]] {
                return {};
            }
            if (auto result = archive(tag, varint{size}); failure(result))
                [[unlikely]] {
                return result;
            }
            for (auto & element : item) {
                if (auto result = archive(
                        varint{std::underlying_type_t<type>(element)});
                    failure(result)) [[unlikely]] {
                    return result;
                }
            }
            return {};
        } else {
            constexpr auto tag =
                make_tag<typename type::value_type, Index>();
            for (auto & element : item) {
                if (auto result = archive(tag, element); failure(result))
                    [[unlikely]] {
                    return result;
                }
            }
            return {};
        }
    }

    ZPP_BITS_INLINE constexpr errc operator()(
        auto & archive,
        auto & item,
        std::size_t size = std::numeric_limits<std::size_t>::max()) const
        requires(std::remove_cvref_t<decltype(archive)>::kind() ==
                 kind::in)
    {
        auto data = archive.remaining_data();
        in in{std::span{data.data(), std::min(size, data.size())},
              size_varint{},
              endian::little{},
              alloc_limit<std::remove_cvref_t<
                  decltype(archive)>::allocation_limit>{}};
        auto result = deserialize_fields(in, item);
        archive.position() += in.position();
        return result;
    }

    ZPP_BITS_INLINE constexpr static errc
    deserialize_fields(auto & archive, auto & item)
    {
        using type = std::remove_cvref_t<decltype(item)>;
        static_assert(check_type<type>());

        auto size = archive.data().size();
        visit_members(
            item, [](auto &&... members) ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                (
                    [](auto && member) ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                        using type = std::remove_cvref_t<decltype(member)>;
                        if constexpr (concepts::container<type> &&
                                      !std::is_fundamental_v<type> &&
                                      !std::same_as<type, std::byte> &&
                                      requires { member.clear(); }) {
                            member.clear();
                        }
                    }(members),
                    ...);
            });

        while (archive.position() < size) {
            vuint32_t tag;
            if (auto result = archive(tag); failure(result)) [[unlikely]] {
                return result;
            }

            if (auto result = deserialize_field(
                    archive, item, tag_number(tag), tag_type(tag));
                failure(result)) [[unlikely]] {
                return result;
            }
        }

        return {};
    }

    template <std::size_t Index = 0>
    ZPP_BITS_INLINE constexpr static auto
    deserialize_field(auto & archive,
                      auto && item,
                      auto field_num,
                      wire_type field_type)
    {
        using type = std::remove_reference_t<decltype(item)>;
        if constexpr (Index >= number_of_members<type>()) {
            if (!field_num) [[unlikely]] {
                return errc{std::errc::protocol_error};
            }
            return errc{};
        } else if (field_number_from_struct<type, Index>() != field_num) {
            return deserialize_field<Index + 1>(
                archive, item, field_num, field_type);
        } else if constexpr (concepts::self_referencing<type>) {
            return visit_members(
                item,
                [&](auto &&... items) constexpr {
                    std::tuple<decltype(items) &...> refs = {items...};
                    auto & item = std::get<Index>(refs);
                    using type = std::remove_reference_t<decltype(item)>;
                    static_assert(check_type<type>());

                    return deserialize_field(archive, field_type, item);
                });
        } else {
            return visit_members(
                item,
                [&](auto &&... items) ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                    std::tuple<decltype(items) &...> refs = {items...};
                    auto & item = std::get<Index>(refs);
                    using type = std::remove_reference_t<decltype(item)>;
                    static_assert(check_type<type>());

                    return deserialize_field(archive, field_type, item);
                });
        }
    }

    ZPP_BITS_INLINE constexpr static auto deserialize_field(
        auto & archive, wire_type field_type, auto & item)
    {
        using type = std::remove_reference_t<decltype(item)>;
        using archive_type = std::remove_reference_t<decltype(archive)>;
        static_assert(check_type<type>());

        if constexpr (std::is_enum_v<type>) {
            varint<type> value;
            if (auto result = archive(value); failure(result))
                [[unlikely]] {
                return result;
            }
            item = value;
            return errc{};
        } else if constexpr (is_pb_field<type>()) {
            return deserialize_field(
                archive,
                field_type,
                static_cast<typename type::pb_field_type &>(item));
        } else if constexpr (!concepts::container<type>) {
            return archive(item);
        } else if constexpr (concepts::associative_container<type> &&
                             requires { typename type::mapped_type; }) {
            using key_type = std::conditional_t<
                std::is_enum_v<typename type::key_type> &&
                    !std::same_as<typename type::key_type, std::byte>,
                varint<typename type::key_type>,
                typename type::key_type>;

            using mapped_type = std::conditional_t<
                std::is_enum_v<typename type::mapped_type> &&
                    !std::same_as<typename type::mapped_type, std::byte>,
                varint<typename type::mapped_type>,
                typename type::mapped_type>;

            struct value_type
            {
                key_type key;
                mapped_type value;

                using serialize = protocol<pb_default{}>;
                serialize use();
            };

            std::aligned_storage_t<sizeof(value_type),
                                   alignof(value_type)>
                storage;

            auto object =
                access::placement_new<value_type>(std::addressof(storage));
            destructor_guard guard{*object};
            if (auto result = archive(*object); failure(result))
                [[unlikely]] {
                return result;
            }

            item.emplace(std::move(object->key), std::move(object->value));
            return errc{};
        } else {
            using orig_value_type = typename type::value_type;
            using value_type = std::conditional_t<
                std::is_enum_v<orig_value_type> &&
                    !std::same_as<orig_value_type, std::byte>,
                varint<orig_value_type>,
                orig_value_type>;

            if constexpr (std::is_fundamental_v<value_type> ||
                          std::same_as<std::byte, value_type> ||
                          concepts::varint<value_type>) {
                auto fetch = [&]() ZPP_BITS_CONSTEXPR_INLINE_LAMBDA {
                    value_type value;
                    if (auto result = archive(value); failure(result))
                        [[unlikely]] {
                        return result;
                    }

                    if constexpr (requires {
                                      item.push_back(
                                          orig_value_type(value));
                                  }) {
                        item.push_back(orig_value_type(value));
                    } else {
                        item.insert(orig_value_type(value));
                    }

                    return errc{};
                };
                if (field_type != wire_type::length_delimited)
                    [[unlikely]] {
                    return fetch();
                }
                vsize_t length;
                if (auto result = archive(length); failure(result))
                    [[unlikely]] {
                    return result;
                }

                if constexpr (requires { item.resize(1); } &&
                              (std::is_fundamental_v<value_type> ||
                               std::same_as<value_type, std::byte>)) {
                    if constexpr (archive_type::allocation_limit !=
                                  std::numeric_limits<
                                      std::size_t>::max()) {
                        if (length > archive_type::allocation_limit)
                            [[unlikely]] {
                            return errc{std::errc::message_size};
                        }
                    }
                    item.resize(length / sizeof(value_type));
                    return archive(unsized(item));
                } else {
                    if constexpr (requires { item.reserve(1); }) {
                        item.reserve(length);
                    }

                    auto end_position = length + archive.position();
                    while (archive.position() < end_position) {
                        if (auto result = fetch(); failure(result))
                            [[unlikely]] {
                            return result;
                        }
                    }

                    return errc{};
                }
            } else {
                std::aligned_storage_t<sizeof(value_type),
                                       alignof(value_type)>
                    storage;

                auto object = access::placement_new<value_type>(
                    std::addressof(storage));
                destructor_guard guard{*object};
                if (auto result = archive(*object); failure(result))
                    [[unlikely]] {
                    return result;
                }

                if constexpr (requires {
                                  item.push_back(std::move(*object));
                              }) {
                    item.push_back(std::move(*object));
                } else {
                    item.insert(std::move(*object));
                }

                return errc{};
            }
        }
    }
};

using pb_protocol = protocol<pb{}>;

template <std::size_t Members = std::numeric_limits<std::size_t>::max()>
using pb_members = protocol<pb{}, Members>;

namespace numbers
{
template <typename Type>
struct big_endian
{
    struct emplace{};

    constexpr big_endian() = default;

    constexpr explicit big_endian(Type value, emplace) : value(value)
    {
    }

    constexpr explicit big_endian(Type value)
    {
        std::array<std::byte, sizeof(value)> data;
        for (std::size_t i = 0; i < sizeof(value); ++i) {
            data[sizeof(value) - 1 - i] = std::byte(value & 0xff);
            value >>= CHAR_BIT;
        }

        this->value = std::bit_cast<Type>(data);
    }

    constexpr auto operator<<(auto value) const
    {
        auto data =
            std::bit_cast<std::array<unsigned char, sizeof(*this)>>(*this);

        for (std::size_t i = 0; i < sizeof(Type); ++i) {
            auto offset = (value % CHAR_BIT);
            auto current = i + (value / CHAR_BIT);

            if (current >= sizeof(Type)) {
                data[i] = 0;
                continue;
            }

            data[i] = data[current] << offset;
            if (current == sizeof(Type) - 1) {
                continue;
            }

            offset = CHAR_BIT - offset;
            if (offset >= 0) {
                data[i] |= data[current + 1] >> offset;
            } else {
                data[i] |= data[current + 1] << (-offset);
            }
        }

        return std::bit_cast<big_endian>(data);
    }

    constexpr auto operator>>(auto value) const
    {
        auto data =
            std::bit_cast<std::array<unsigned char, sizeof(*this)>>(*this);

        for (std::size_t j = 0; j < sizeof(Type); ++j) {
            auto i = sizeof(Type) - 1 - j;
            auto offset = (value % CHAR_BIT);
            auto current = i - (value / CHAR_BIT);

            if (current >= sizeof(Type)) {
                data[i] = 0;
                continue;
            }

            data[i] = data[current] >> offset;
            if (!current) {
                continue;
            }

            offset = CHAR_BIT - offset;
            if (offset >= 0) {
                data[i] |= data[current - 1] << offset;
            } else {
                data[i] |= data[current - 1] >> (-offset);
            }
        }

        return std::bit_cast<big_endian>(data);
    }

    constexpr auto friend operator+(big_endian left, big_endian right)
    {
        auto left_data = std::bit_cast<std::array<unsigned char, sizeof(left)>>(left);
        auto right_data = std::bit_cast<std::array<unsigned char, sizeof(right)>>(right);
        unsigned char remaining{};

        for (std::size_t i = 0; i < sizeof(Type); ++i) {
            auto current = sizeof(Type) - 1 - i;
            std::uint16_t byte_addition =
                std::uint16_t(left_data[current]) +
                std::uint16_t(right_data[current]) + remaining;
            left_data[current] = std::uint8_t(byte_addition & 0xff);
            remaining = std::uint8_t((byte_addition >> CHAR_BIT) & 0xff);
        }

        return std::bit_cast<big_endian>(left_data);
    }

    constexpr big_endian operator~() const
    {
        return big_endian{~value, emplace{}};
    }

    constexpr auto & operator+=(big_endian other)
    {
        *this = (*this) + other;
        return *this;
    }

    constexpr auto friend operator&(big_endian left, big_endian right)
    {
        return big_endian{left.value & right.value, emplace{}};
    }

    constexpr auto friend operator^(big_endian left, big_endian right)
    {
        return big_endian{left.value ^ right.value, emplace{}};
    }

    constexpr auto friend operator|(big_endian left, big_endian right)
    {
        return big_endian{left.value | right.value, emplace{}};
    }

    constexpr auto friend operator<=>(big_endian left,
                                      big_endian right) = default;

    using serialize = members<1>;

    Type value{};
};
} // namespace numbers

template <auto Object, typename Digest = std::array<std::byte, 20>>
requires requires
{
    requires success(in{Digest{}}(std::array<std::byte, 20>{}));
}
constexpr auto sha1()
{
    using numbers::big_endian;
    auto rotate_left = [](auto n, auto c) {
        return (n << c) | (n >> ((sizeof(n) * CHAR_BIT) - c));
    };
    auto align = [](auto v, auto a) { return (v + (a - 1)) / a * a; };

    auto h0 = big_endian{std::uint32_t{0x67452301u}};
    auto h1 = big_endian{std::uint32_t{0xefcdab89u}};
    auto h2 = big_endian{std::uint32_t{0x98badcfeu}};
    auto h3 = big_endian{std::uint32_t{0x10325476u}};
    auto h4 = big_endian{std::uint32_t{0xc3d2e1f0u}};

    constexpr auto original_message = to_bytes<Object>();
    constexpr auto chunk_size = 512 / CHAR_BIT;
    constexpr auto message = to_bytes<
        original_message,
        std::byte{0x80},
        std::array<std::byte,
                   align(original_message.size() + sizeof(std::byte{0x80}),
                         chunk_size) -
                       original_message.size() - sizeof(std::byte{0x80}) -
                       sizeof(std::uint64_t{original_message.size()})>{},
        big_endian<std::uint64_t>{original_message.size() * CHAR_BIT}>();

    for (auto chunk :
         from_bytes<message,
                    std::array<std::array<big_endian<std::uint32_t>, 16>,
                               message.size() / chunk_size>>()) {
        std::array<big_endian<std::uint32_t>, 80> w;
        std::copy(std::begin(chunk), std::end(chunk), std::begin(w));

        for (std::size_t i = 16; i < w.size(); ++i) {
            w[i] = rotate_left(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16],
                               1);
        }

        auto a = h0;
        auto b = h1;
        auto c = h2;
        auto d = h3;
        auto e = h4;

        for (std::size_t i = 0; i < w.size(); ++i) {
            auto f = big_endian{std::uint32_t{}};
            auto k = big_endian{std::uint32_t{}};
            if (i <= 19) {
                f = (b & c) | ((~b) & d);
                k = big_endian{std::uint32_t{0x5a827999u}};
            } else if (i <= 39) {
                f = b ^ c ^ d;
                k = big_endian{std::uint32_t{0x6ed9eba1u}};
            } else if (i <= 59) {
                f = (b & c) | (b & d) | (c & d);
                k = big_endian{std::uint32_t{0x8f1bbcdcu}};
            } else {
                f = b ^ c ^ d;
                k = big_endian{std::uint32_t{0xca62c1d6u}};
            }

            auto temp = rotate_left(a, 5) + f + e + k + w[i];
            e = d;
            d = c;
            c = rotate_left(b, 30);
            b = a;
            a = temp;
        }

        h0 += a;
        h1 += b;
        h2 += c;
        h3 += d;
        h4 += e;
    }

    std::array<std::byte, 20> digest_data;
    out{digest_data}(h0, h1, h2, h3, h4).or_throw();

    Digest digest;
    in{digest_data}(digest).or_throw();
    return digest;
}

template <auto Object, typename Digest = std::array<std::byte, 32>>
requires requires
{
    requires success(in{Digest{}}(std::array<std::byte, 32>{}));
}
constexpr auto sha256()
{
    using numbers::big_endian;
    auto rotate_right = [](auto n, auto c) {
        return (n >> c) | (n << ((sizeof(n) * CHAR_BIT) - c));
    };
    auto align = [](auto v, auto a) { return (v + (a - 1)) / a * a; };

    auto h0 = big_endian{0x6a09e667u};
    auto h1 = big_endian{0xbb67ae85u};
    auto h2 = big_endian{0x3c6ef372u};
    auto h3 = big_endian{0xa54ff53au};
    auto h4 = big_endian{0x510e527fu};
    auto h5 = big_endian{0x9b05688cu};
    auto h6 = big_endian{0x1f83d9abu};
    auto h7 = big_endian{0x5be0cd19u};

    std::array k{big_endian{0x428a2f98u}, big_endian{0x71374491u},
                 big_endian{0xb5c0fbcfu}, big_endian{0xe9b5dba5u},
                 big_endian{0x3956c25bu}, big_endian{0x59f111f1u},
                 big_endian{0x923f82a4u}, big_endian{0xab1c5ed5u},
                 big_endian{0xd807aa98u}, big_endian{0x12835b01u},
                 big_endian{0x243185beu}, big_endian{0x550c7dc3u},
                 big_endian{0x72be5d74u}, big_endian{0x80deb1feu},
                 big_endian{0x9bdc06a7u}, big_endian{0xc19bf174u},
                 big_endian{0xe49b69c1u}, big_endian{0xefbe4786u},
                 big_endian{0x0fc19dc6u}, big_endian{0x240ca1ccu},
                 big_endian{0x2de92c6fu}, big_endian{0x4a7484aau},
                 big_endian{0x5cb0a9dcu}, big_endian{0x76f988dau},
                 big_endian{0x983e5152u}, big_endian{0xa831c66du},
                 big_endian{0xb00327c8u}, big_endian{0xbf597fc7u},
                 big_endian{0xc6e00bf3u}, big_endian{0xd5a79147u},
                 big_endian{0x06ca6351u}, big_endian{0x14292967u},
                 big_endian{0x27b70a85u}, big_endian{0x2e1b2138u},
                 big_endian{0x4d2c6dfcu}, big_endian{0x53380d13u},
                 big_endian{0x650a7354u}, big_endian{0x766a0abbu},
                 big_endian{0x81c2c92eu}, big_endian{0x92722c85u},
                 big_endian{0xa2bfe8a1u}, big_endian{0xa81a664bu},
                 big_endian{0xc24b8b70u}, big_endian{0xc76c51a3u},
                 big_endian{0xd192e819u}, big_endian{0xd6990624u},
                 big_endian{0xf40e3585u}, big_endian{0x106aa070u},
                 big_endian{0x19a4c116u}, big_endian{0x1e376c08u},
                 big_endian{0x2748774cu}, big_endian{0x34b0bcb5u},
                 big_endian{0x391c0cb3u}, big_endian{0x4ed8aa4au},
                 big_endian{0x5b9cca4fu}, big_endian{0x682e6ff3u},
                 big_endian{0x748f82eeu}, big_endian{0x78a5636fu},
                 big_endian{0x84c87814u}, big_endian{0x8cc70208u},
                 big_endian{0x90befffau}, big_endian{0xa4506cebu},
                 big_endian{0xbef9a3f7u}, big_endian{0xc67178f2u}};

    constexpr auto original_message = to_bytes<Object>();
    constexpr auto chunk_size = 512 / CHAR_BIT;
    constexpr auto message = to_bytes<
        original_message,
        std::byte{0x80},
        std::array<std::byte,
                   align(original_message.size() + sizeof(std::byte{0x80}),
                         chunk_size) -
                       original_message.size() - sizeof(std::byte{0x80}) -
                       sizeof(std::uint64_t{original_message.size()})>{},
        big_endian<std::uint64_t>{original_message.size() * CHAR_BIT}>();

    for (auto chunk :
         from_bytes<message,
                    std::array<std::array<big_endian<std::uint32_t>, 16>,
                               message.size() / chunk_size>>()) {
        std::array<big_endian<std::uint32_t>, 64> w;
        std::copy(std::begin(chunk), std::end(chunk), std::begin(w));

        for (std::size_t i = 16; i < w.size(); ++i) {
            auto s0 = rotate_right(w[i - 15], 7) ^
                      rotate_right(w[i - 15], 18) ^ (w[i - 15] >> 3);
            auto s1 = rotate_right(w[i - 2], 17) ^
                      rotate_right(w[i - 2], 19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16] + s0 + w[i - 7] + s1;
        }

        auto a = h0;
        auto b = h1;
        auto c = h2;
        auto d = h3;
        auto e = h4;
        auto f = h5;
        auto g = h6;
        auto h = h7;

        for (std::size_t i = 0; i < w.size(); ++i) {
            auto s1 = rotate_right(e, 6) ^ rotate_right(e, 11) ^
                      rotate_right(e, 25);
            auto ch = (e & f) ^ ((~e) & g);
            auto temp1 = h + s1 + ch + k[i] + w[i];
            auto s0 = rotate_right(a, 2) ^ rotate_right(a, 13) ^
                      rotate_right(a, 22);
            auto maj = (a & b) ^ (a & c) ^ (b & c);
            auto temp2 = s0 + maj;

            h = g;
            g = f;
            f = e;
            e = d + temp1;
            d = c;
            c = b;
            b = a;
            a = temp1 + temp2;
        }

        h0 = h0 + a;
        h1 = h1 + b;
        h2 = h2 + c;
        h3 = h3 + d;
        h4 = h4 + e;
        h5 = h5 + f;
        h6 = h6 + g;
        h7 = h7 + h;
    }

    std::array<std::byte, 32> digest_data;
    out{digest_data}(h0, h1, h2, h3, h4, h5, h6, h7).or_throw();

    Digest digest;
    in{digest_data}(digest).or_throw();
    return digest;
}

inline namespace literals
{
inline namespace string_literals
{
template <string_literal String>
constexpr auto operator""_s()
{
    return String;
}

template <string_literal String>
constexpr auto operator""_b()
{
    return to_bytes<String>();
}

template <string_literal String>
constexpr auto operator""_decode_hex()
{
    constexpr auto tolower = [](auto c) {
        if ('A' <= c && c <= 'Z') {
            return decltype(c)(c - 'A' + 'a');
        }
        return c;
    };

    static_assert(String.size() % 2 == 0);

    static_assert(
        std::find_if(std::begin(String), std::end(String), [&](auto c) {
            return !(('0' <= c && c <= '9') ||
                     ('a' <= tolower(c) && tolower(c) <= 'f'));
        }) == std::end(String));

    auto hex = [](auto c) {
        if ('a' <= c) {
            return c - 'a' + 0xa;
        } else {
            return c - '0';
        }
    };

    std::array<std::byte, String.size() / 2> data;
    for (std::size_t i = 0; auto & b : data) {
        auto left = tolower(String[i]);
        auto right = tolower(String[i + 1]);
        b = std::byte((hex(left) << (CHAR_BIT/2)) | hex(right));
        i += 2;
    }
    return data;
}

template <string_literal String>
constexpr auto operator""_sha1()
{
    return sha1<String>();
}

template <string_literal String>
constexpr auto operator""_sha256()
{
    return sha256<String>();
}

template <string_literal String>
constexpr auto operator""_sha1_int()
{
    return id_v<sha1<String>(), sizeof(int)>;
}

template <string_literal String>
constexpr auto operator""_sha256_int()
{
    return id_v<sha256<String>(), sizeof(int)>;
}
} // namespace string_literals
} // namespace literals

template <typename... Arguments>
using vector1b = sized_t<std::vector<Arguments...>, unsigned char>;
template <typename... Arguments>
using vector2b = sized_t<std::vector<Arguments...>, std::uint16_t>;
template <typename... Arguments>
using vector4b = sized_t<std::vector<Arguments...>, std::uint32_t>;
template <typename... Arguments>
using vector8b = sized_t<std::vector<Arguments...>, std::uint64_t>;
template <typename... Arguments>
using static_vector = unsized_t<std::vector<Arguments...>>;
template <typename... Arguments>
using native_vector = sized_t<std::vector<Arguments...>, typename std::vector<Arguments...>::size_type>;

template <typename... Arguments>
using span1b = sized_t<std::span<Arguments...>, unsigned char>;
template <typename... Arguments>
using span2b = sized_t<std::span<Arguments...>, std::uint16_t>;
template <typename... Arguments>
using span4b = sized_t<std::span<Arguments...>, std::uint32_t>;
template <typename... Arguments>
using span8b = sized_t<std::span<Arguments...>, std::uint64_t>;
template <typename... Arguments>
using static_span = unsized_t<std::span<Arguments...>>;
template <typename... Arguments>
using native_span = sized_t<std::span<Arguments...>, typename std::span<Arguments...>::size_type>;

using string1b = sized_t<std::string, unsigned char>;
using string2b = sized_t<std::string, std::uint16_t>;
using string4b = sized_t<std::string, std::uint32_t>;
using string8b = sized_t<std::string, std::uint64_t>;
using static_string = unsized_t<std::string>;
using native_string = sized_t<std::string, std::string::size_type>;

using string_view1b = sized_t<std::string_view, unsigned char>;
using string_view2b = sized_t<std::string_view, std::uint16_t>;
using string_view4b = sized_t<std::string_view, std::uint32_t>;
using string_view8b = sized_t<std::string_view, std::uint64_t>;
using static_string_view = unsized_t<std::string_view>;
using native_string_view = sized_t<std::string_view, std::string_view::size_type>;

using wstring1b = sized_t<std::wstring, unsigned char>;
using wstring2b = sized_t<std::wstring, std::uint16_t>;
using wstring4b = sized_t<std::wstring, std::uint32_t>;
using wstring8b = sized_t<std::wstring, std::uint64_t>;
using static_wstring = unsized_t<std::wstring>;
using native_wstring = sized_t<std::wstring, std::wstring::size_type>;

using wstring_view1b = sized_t<std::wstring_view, unsigned char>;
using wstring_view2b = sized_t<std::wstring_view, std::uint16_t>;
using wstring_view4b = sized_t<std::wstring_view, std::uint32_t>;
using wstring_view8b = sized_t<std::wstring_view, std::uint64_t>;
using static_wstring_view = unsized_t<std::wstring_view>;
using native_wstring_view = sized_t<std::wstring_view, std::wstring_view::size_type>;

using u8string1b = sized_t<std::u8string, unsigned char>;
using u8string2b = sized_t<std::u8string, std::uint16_t>;
using u8string4b = sized_t<std::u8string, std::uint32_t>;
using u8string8b = sized_t<std::u8string, std::uint64_t>;
using static_u8string = unsized_t<std::u8string>;
using native_u8string = sized_t<std::u8string, std::u8string::size_type>;

using u8string_view1b = sized_t<std::u8string_view, unsigned char>;
using u8string_view2b = sized_t<std::u8string_view, std::uint16_t>;
using u8string_view4b = sized_t<std::u8string_view, std::uint32_t>;
using u8string_view8b = sized_t<std::u8string_view, std::uint64_t>;
using static_u8string_view = unsized_t<std::u8string_view>;
using native_u8string_view = sized_t<std::u8string_view, std::u8string_view::size_type>;

using u16string1b = sized_t<std::u16string, unsigned char>;
using u16string2b = sized_t<std::u16string, std::uint16_t>;
using u16string4b = sized_t<std::u16string, std::uint32_t>;
using u16string8b = sized_t<std::u16string, std::uint64_t>;
using static_u16string = unsized_t<std::u16string>;
using native_u16string = sized_t<std::u16string, std::u16string::size_type>;

using u16string_view1b = sized_t<std::u16string_view, unsigned char>;
using u16string_view2b = sized_t<std::u16string_view, std::uint16_t>;
using u16string_view4b = sized_t<std::u16string_view, std::uint32_t>;
using u16string_view8b = sized_t<std::u16string_view, std::uint64_t>;
using static_u16string_view = unsized_t<std::u16string_view>;
using native_u16string_view = sized_t<std::u16string_view, std::u16string_view::size_type>;

using u32string1b = sized_t<std::u32string, unsigned char>;
using u32string2b = sized_t<std::u32string, std::uint16_t>;
using u32string4b = sized_t<std::u32string, std::uint32_t>;
using u32string8b = sized_t<std::u32string, std::uint64_t>;
using static_u32string = unsized_t<std::u32string>;
using native_u32string = sized_t<std::u32string, std::u32string::size_type>;

using u32string_view1b = sized_t<std::u32string_view, unsigned char>;
using u32string_view2b = sized_t<std::u32string_view, std::uint16_t>;
using u32string_view4b = sized_t<std::u32string_view, std::uint32_t>;
using u32string_view8b = sized_t<std::u32string_view, std::uint64_t>;
using static_u32string_view = unsized_t<std::u32string_view>;
using native_u32string_view = sized_t<std::u32string_view, std::u32string_view::size_type>;

} // namespace zpp::bits

#endif // ZPP_BITS_H

