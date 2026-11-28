// Datum: Explicit diagnostic declarations and preferred native enrollment.
#ifndef CEPH_RGW_DATUM_RENDER_DETAIL_SCHEMA_H
 #define CEPH_RGW_DATUM_RENDER_DETAIL_SCHEMA_H

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <version>

#include <array>
#include <tuple>
#include <utility>

#include <algorithm>
#include <iterator>
#include <ranges>
#include <string_view>

#if defined(__cpp_lib_reflection)
 #include "datum/detail/record.h"
 #include <meta>
#else
 #include "datum/compat/detail/render.h"
#endif

namespace ceph::salad::datum::render_detail {

template <typename EnumT, std::size_t Count>
constexpr std::string_view enum_name(
    const std::array<std::pair<EnumT, std::string_view>, Count>& entries, const EnumT value)
{
    using entry_type = std::pair<EnumT, std::string_view>;
    const auto found = std::ranges::find(entries, value, &entry_type::first);

    return found == std::end(entries) ? std::string_view {} : found->second;
}

// A named alias keeps its label; a specialization uses the template declaration.
#if defined(__cpp_lib_reflection)
consteval std::string_view reflected_record_name(const std::meta::info record)
{
    return std::meta::identifier_of(
        std::meta::has_identifier(record) ? record : std::meta::template_of(record));
}

#endif

} // ceph::salad::datum::render_detail

namespace ceph::salad::datum::render {

enum struct display_field_policy : std::uint8_t { value, redacted };

// Accessors may be member pointers or stateless callables, so borrowed views
// with reference members use the same schema machinery as owning records:
template <auto Accessor, display_field_policy Policy>
struct display_field final
{
#if defined(__cpp_lib_reflection)
    std::string_view name = std::meta::identifier_of(detail::record_member<Accessor>());
#else
    std::string_view name;

    // Native C++26 derives omitted labels; C++23 requires an explicit label.
    constexpr explicit display_field(const std::string_view label) noexcept : name(label)
    {}

#endif
};

template <auto Accessor>
requires std::is_member_object_pointer_v<decltype(Accessor)> and (nullptr != Accessor)
constexpr auto field() noexcept
{
    return display_field<Accessor, display_field_policy::value> {};
}

template <auto Accessor>
constexpr auto field(const std::string_view name) noexcept
{
    return display_field<Accessor, display_field_policy::value> {name};
}

template <typename T>
struct display_schema;

template <typename T>
struct enum_schema;

template <typename T>
struct display_projection;

#if defined(__cpp_lib_reflection)
template <typename EnumT>
requires std::is_enum_v<EnumT>
struct reflected_enum_schema
{
    using entry_type = std::pair<EnumT, std::string_view>;

    // Reflection builds ordinary data; runtime selection needs no meta values.
    static constexpr auto identifiers = [] consteval {
        static constexpr auto enumerators =
            std::define_static_array(std::meta::enumerators_of(^^EnumT));
        std::array<entry_type, std::size(enumerators)> entries {};
        auto next = std::begin(entries);
        template for (constexpr auto enumerator : enumerators) {
            *next++ = entry_type {[:enumerator:], std::meta::identifier_of(enumerator)};
        }

        return entries;
    }();

public:
    static constexpr std::string_view name(const EnumT value)
    {
        return render_detail::enum_name(identifiers, value);
    }

};

#endif

} // ceph::salad::datum::render

namespace ceph::salad::datum::render_detail {

#if defined(__cpp_lib_reflection)
template <std::meta::info Member, std::meta::info ...Redacted>
consteval auto reflected_field()
{
    static_assert(std::meta::has_identifier(Member),
                  "Use an explicit projection for anonymous diagnostic members");
    // Splicing also borrows reference members without an accessor wrapper:
    constexpr auto accessor = [](const auto& value)
        -> std::conditional_t<std::meta::is_bit_field(Member),
                              std::remove_cvref_t<decltype(value.[:Member:])>,
                              decltype((value.[:Member:]))> { return value.[:Member:]; };
    constexpr auto policy = ((Member == Redacted) || ...) ? render::display_field_policy::redacted
                                                          : render::display_field_policy::value;

    return render::display_field<accessor, policy> {std::meta::identifier_of(Member)};
}

#endif

} // ceph::salad::datum::render_detail

namespace ceph::salad::datum::render {

// Enrollment is explicit; a new field requires a fresh confidentiality review.
// Selected or renamed fields still use an explicit schema or projection:
#if defined(__cpp_lib_reflection)
template <std::meta::info Record, std::size_t ApprovedMembers, std::meta::info ...Redacted>
struct reflected_display_schema
{
    using record_type = [:Record:];

    static_assert(!std::is_union_v<record_type>,
                  "Use an explicit projection for active union members");

    static constexpr auto members = detail::record_members<record_type>;

    static_assert(ApprovedMembers == std::size(members),
                  "Review new diagnostic members and their redaction before enrollment");
    static_assert(std::empty(std::meta::bases_of(Record,
                                                 std::meta::access_context::unprivileged())),
                  "Inherited diagnostic state requires an explicit projection");
    static_assert((std::ranges::contains(members, Redacted) and ...),
                  "Redaction must name a member of the displayed record");

    static constexpr std::string_view name = render_detail::reflected_record_name(Record);
    static constexpr auto fields = []<std::size_t ...Index>(std::index_sequence<Index...>) {
        return std::tuple {render_detail::reflected_field<members[Index], Redacted...>()...};
    }(std::make_index_sequence<std::size(members)> {});
};

#endif

} // ceph::salad::datum::render

#endif // CEPH_RGW_DATUM_RENDER_DETAIL_SCHEMA_H
