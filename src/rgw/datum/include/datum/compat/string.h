// Datum: String construction for older constexpr implementations.

#ifndef CEPH_RGW_DATUM_COMPAT_STRING_H
 #define CEPH_RGW_DATUM_COMPAT_STRING_H

#include "datum/compat/compiler.h"

#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace ceph::salad::datum::compat {

// Append avoids Clang/libstdc++ definition-lookup failures in constexpr string construction.
// Remove this bridge once supported combinations pass the direct-construction checks.
constexpr std::string string_from_view(const std::string_view value)
{
    if (detail::needs_constexpr_string_bridge and std::is_constant_evaluated()) {
        std::string out;
        out.append(value);

        return out;
    }

    return std::string {value};
}

namespace detail {

template <typename ValueT, typename SourceT>
constexpr ValueT construct_value(SourceT&& source)
{
    if consteval {
        using source_type = std::decay_t<SourceT>;

        if constexpr (needs_constexpr_string_bridge and std::is_same_v<ValueT, std::string> and
                      (std::is_same_v<source_type, std::string_view> ||
                       std::is_same_v<source_type, const char *> ||
                       std::is_same_v<source_type, char *>)) {
            return string_from_view(std::string_view {source});
        }
    }

    return ValueT(std::forward<SourceT>(source));
}

} // detail

} // ceph::salad::datum::compat

#endif // CEPH_RGW_DATUM_COMPAT_STRING_H
