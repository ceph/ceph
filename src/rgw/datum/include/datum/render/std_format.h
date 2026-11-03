// Datum: Optional standard formatting above the bounded diagnostic engine.
#ifndef CEPH_RGW_DATUM_RENDER_STD_FORMAT_H
 #define CEPH_RGW_DATUM_RENDER_STD_FORMAT_H

#include "datum/render/display.h"
#include <format>

namespace std {

template <::ceph::salad::datum::render::displayable ValueT>
struct formatter<ValueT, char> : ::ceph::salad::datum::render_detail::formatter<ValueT, ::std::format_error>
{};

template <::ceph::salad::datum::render::displayable ValueT>
struct formatter<::ceph::salad::datum::render::display_view<ValueT>, char>
    : ::ceph::salad::datum::render_detail::formatter<
          ::ceph::salad::datum::render::display_view<ValueT>, ::std::format_error>
{};

} // std

#endif // CEPH_RGW_DATUM_RENDER_STD_FORMAT_H
