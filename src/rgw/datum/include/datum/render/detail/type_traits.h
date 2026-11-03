// Datum: Exact vocabulary-type recognition, with native reflection preferred.
#ifndef CEPH_RGW_DATUM_RENDER_DETAIL_TYPE_TRAITS_H
 #define CEPH_RGW_DATUM_RENDER_DETAIL_TYPE_TRAITS_H

#include <type_traits>
#include <version>

#include <optional>
#include <tuple>
#include <utility>
#include <variant>

#include <chrono>

#if defined(__cpp_lib_reflection)
 #include <meta>
#else
 #include "datum/compat/detail/render.h"
#endif

namespace ceph::salad::datum::render_detail {

#if defined(__cpp_lib_reflection)
// Reflection identifies exact standard specializations without trait scaffolding.
template <typename T>
consteval bool specialization_of(const std::meta::info primary)
{
    using value_type = std::remove_cvref_t<T>;

    constexpr auto reflected = std::meta::dealias(^^value_type);

    return std::meta::has_template_arguments(reflected) and
           primary == std::meta::template_of(reflected);
}

template <typename T>
inline constexpr bool is_optional_v = specialization_of<T>(^^std::optional);

template <typename T>
inline constexpr bool is_time_point_v = specialization_of<T>(^^std::chrono::time_point);

template <typename T>
concept display_tuple = specialization_of<T>(^^std::tuple) || specialization_of<T>(^^std::pair);

template <typename T>
inline constexpr bool is_variant_v = specialization_of<T>(^^std::variant);

#else
template <typename T>
inline constexpr bool is_optional_v = compat::detail::specialization_of<T, std::optional>;

template <typename T>
inline constexpr bool is_time_point_v = compat::detail::specialization_of<T, std::chrono::time_point>;

template <typename T>
concept display_tuple = compat::detail::specialization_of<T, std::tuple> ||
                        compat::detail::specialization_of<T, std::pair>;

template <typename T>
inline constexpr bool is_variant_v = compat::detail::specialization_of<T, std::variant>;
#endif

} // ceph::salad::datum::render_detail

#endif // CEPH_RGW_DATUM_RENDER_DETAIL_TYPE_TRAITS_H
