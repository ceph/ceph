// Datum: Select native range projection; isolate the old-library implementation.
#ifndef CEPH_RGW_DATUM_COMPAT_INTERVAL_PROJECTION_H
 #define CEPH_RGW_DATUM_COMPAT_INTERVAL_PROJECTION_H

#include <ranges>

#if !defined(__cpp_lib_ranges) || __cpp_lib_ranges < 202207L
 #include "datum/compat/detail/interval_projection.h"
#endif

#if !defined(__cpp_explicit_this_parameter) || __cpp_explicit_this_parameter < 202110L
 #include "datum/compat/detail/interval_range.h"
#endif

namespace ceph::salad::datum::compat::detail {

#if defined(__cpp_explicit_this_parameter) and __cpp_explicit_this_parameter >= 202110L
 template <typename DerivedT>
 using interval_range_interface = std::ranges::view_interface<DerivedT>;
#endif

#if defined(__cpp_lib_ranges) and __cpp_lib_ranges >= 202207L
 template <typename ViewT, typename ProjectionT>
 using interval_projection_view = std::ranges::transform_view<ViewT, ProjectionT>;
#else
 template <typename ViewT, typename ProjectionT>
 using interval_projection_view =
     typename decltype(projection_view_type<ViewT, ProjectionT>())::type;
#endif

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_COMPAT_INTERVAL_PROJECTION_H
