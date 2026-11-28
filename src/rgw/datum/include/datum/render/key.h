// Datum: Optional diagnostics for encoded keys, without exposing their contents.
#ifndef CEPH_RGW_DATUM_RENDER_KEY_H
 #define CEPH_RGW_DATUM_RENDER_KEY_H

#include "datum/key.h"
#include "datum/render/display.h"

#include <string_view>
#include <tuple>

namespace ceph::salad::datum::render {

template <>
struct display_schema<key::encoded_key> final
{
    static constexpr std::string_view name = "encoded_key";
    static constexpr auto fields = std::tuple {
        display_field<key::key_view, display_field_policy::redacted> {"bytes"}};
};

} // ceph::salad::datum::render

#endif // CEPH_RGW_DATUM_RENDER_KEY_H
