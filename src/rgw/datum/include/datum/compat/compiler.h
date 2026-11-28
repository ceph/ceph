// Datum: Internal selection of compiler workarounds.

#ifndef CEPH_RGW_DATUM_COMPAT_COMPILER_H
 #define CEPH_RGW_DATUM_COMPAT_COMPILER_H

#include <type_traits>
#include <version>

// Recover GCC's C++23 clipping inlining; leave the native C++26 path unchanged.
#if defined(__GNUC__) and !defined(__clang__) and __cplusplus <= 202302L and \
    __has_cpp_attribute(gnu::flatten)
 #define DATUM_COMPAT_CXX23_FLATTEN [[gnu::flatten]]
#else
 #define DATUM_COMPAT_CXX23_FLATTEN
#endif

// GCC 12/libstdc++ loses string parameter identity during constexpr ownership transfers.
#if defined(__GNUC__) and !defined(__clang__) and __GNUC__ == 12 and defined(__GLIBCXX__)
 #define DATUM_COMPAT_CONSTEXPR_STRING_OWNER 1
#else
 #define DATUM_COMPAT_CONSTEXPR_STRING_OWNER 0
#endif

namespace ceph::salad::datum::compat::detail {

constexpr bool defer_optional_construction() noexcept
{
#if defined(__GNUC__) and !defined(__clang__) and __GNUC__ == 12
    // GCC 12 loses the owner of a conditional optional temporary in constant evaluation.
    return std::is_constant_evaluated();
#else
    return false;
#endif
}

// LLVM #73232: deferred definitions break libstdc++ constexpr string construction.
// Keep this family workaround until a fixed combination is qualified.
#if defined(__clang__) and defined(__GLIBCXX__)
inline constexpr bool needs_constexpr_string_bridge = true;
#else
inline constexpr bool needs_constexpr_string_bridge = false;
#endif

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_COMPAT_COMPILER_H
