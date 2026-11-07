// Datum: Select the standard endpoint owner; isolate the old-compiler workaround.
#ifndef CEPH_RGW_DATUM_COMPAT_INTERVAL_POINT_H
 #define CEPH_RGW_DATUM_COMPAT_INTERVAL_POINT_H

#include "datum/compat/compiler.h"

#include <optional>

#if DATUM_COMPAT_CONSTEXPR_STRING_OWNER
 #include "datum/compat/detail/interval_point.h"
 #include <type_traits>
#endif

namespace ceph::salad::datum::compat::detail {

#if DATUM_COMPAT_CONSTEXPR_STRING_OWNER
 template <typename ValueT>
 using interval_point_storage = std::conditional_t<std::is_same_v<ValueT, std::string>,
                                                   string_point_storage, std::optional<ValueT>>;
#else
 template <typename ValueT>
 using interval_point_storage = std::optional<ValueT>;
#endif

// GCC 12 also needs to avoid passing a string parameter to another by-value factory.
template <typename ValueT>
constexpr bool direct_point_construction() noexcept
{
#if DATUM_COMPAT_CONSTEXPR_STRING_OWNER
    return std::is_same_v<ValueT, std::string> and std::is_constant_evaluated();
#else
    return false;
#endif
}

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_COMPAT_INTERVAL_POINT_H
