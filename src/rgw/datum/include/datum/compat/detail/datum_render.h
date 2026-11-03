// Datum: Temporary explicit built-in diagnostic profiles without reflection.
#ifndef CEPH_RGW_DATUM_COMPAT_DETAIL_DATUM_RENDER_H
 #define CEPH_RGW_DATUM_COMPAT_DETAIL_DATUM_RENDER_H

#include "datum/render/detail/datum_types.h"

#include <concepts>
#include <cstddef>
#include <type_traits>

#include <array>
#include <tuple>
#include <utility>

#include <string_view>

namespace ceph::salad::datum::compat::detail {

// These known aggregates have two or three members. Unevaluated decomposition
// rejects added members without constructing, copying, or observing a record.
template <typename RecordT, std::size_t Count>
consteval bool approved_display_members()
{
    static_assert(std::is_aggregate_v<RecordT> and (Count == 2 || Count == 3));
    using check = decltype([](const RecordT& value) {
        if constexpr (Count == 2) {
            [[maybe_unused]] const auto& [first, second] = value;
        }
        if constexpr (Count == 3) {
            [[maybe_unused]] const auto& [first, second, third] = value;
        }
    }(std::declval<const RecordT&>()));

    return std::same_as<check, void>;
}

template <typename RecordT>
struct datum_display_schema;

template <interval::boundary_view BoundaryT>
requires render_detail::interval_value_displayable<typename interval::boundary_domain_t<BoundaryT>::value_type>
struct datum_display_schema<render_detail::interval_boundary_display<BoundaryT>>
{
    using record_type = render_detail::interval_boundary_display<BoundaryT>;
    static_assert(approved_display_members<record_type, 3>());

    static constexpr std::string_view name = "interval_boundary";
    static constexpr auto fields = std::tuple {
        render::field<&record_type::kind>("kind"), render::field<&record_type::key>("key"),
        render::field<&record_type::inclusive>("inclusive")};
};

template <interval::ordered_domain DomainT>
requires render_detail::interval_value_displayable<typename DomainT::value_type>
struct datum_display_schema<interval::lower_endpoint<DomainT>>
{
    using record_type = interval::lower_endpoint<DomainT>;
    static_assert(approved_display_members<record_type, 2>());

    static constexpr std::string_view name = "lower_endpoint";
    static constexpr auto fields = std::tuple {
        render::field<[](const record_type& value) -> decltype(auto) {
            return (value.value);
        }>("value"), render::field<&record_type::inclusion>("inclusion")};
};

template <interval::ordered_domain DomainT>
requires render_detail::interval_value_displayable<typename DomainT::value_type>
struct datum_display_schema<interval::upper_endpoint<DomainT>>
{
    using record_type = interval::upper_endpoint<DomainT>;
    static_assert(approved_display_members<record_type, 2>());

    static constexpr std::string_view name = "upper_endpoint";
    static constexpr auto fields = std::tuple {
        render::field<[](const record_type& value) -> decltype(auto) {
            return (value.value);
        }>("value"), render::field<&record_type::inclusion>("inclusion")};
};

template <render_detail::page_traversal_display_value StateT>
struct datum_display_schema<traversal::page_traversal_result<StateT>>
{
    using record_type = traversal::page_traversal_result<StateT>;
    static_assert(approved_display_members<record_type, 3>());

    static constexpr std::string_view name = "page_traversal_result";
    static constexpr auto fields = std::tuple {
        render::field<&record_type::resume>("resume"), render::field<&record_type::pages>("pages"),
        render::field<&record_type::outcome>("outcome")};
};

template <render_detail::page_traversal_display_value StateT,
          render_detail::page_traversal_display_value ErrorT>
struct datum_display_schema<traversal::page_traversal_failure<StateT, ErrorT>>
{
    using record_type = traversal::page_traversal_failure<StateT, ErrorT>;
    static_assert(approved_display_members<record_type, 3>());

    static constexpr std::string_view name = "page_traversal_failure";
    static constexpr auto fields = std::tuple {
        render::field<[](const record_type& value) -> decltype(auto) {
            return (value.resume);
        }>("resume"), render::field<[](const record_type& value) -> decltype(auto) {
            return (value.error);
        }>("error"), render::field<&record_type::pages>("pages")};
};

template <typename EnumT>
struct datum_enum_schema;

template <>
struct datum_enum_schema<interval::boundary_kind>
{
    using enum_type = interval::boundary_kind;
    using entry_type = std::pair<enum_type, std::string_view>;

    static constexpr std::array identifiers {
        entry_type {enum_type::negative_infinity, "negative_infinity"},
        entry_type {enum_type::finite, "finite"},
        entry_type {enum_type::positive_infinity, "positive_infinity"}};

    static constexpr std::string_view name(const enum_type value)
    {
        return render_detail::enum_name(identifiers, value);
    }

};

template <>
struct datum_enum_schema<interval::endpoint_inclusion>
{
    using enum_type = interval::endpoint_inclusion;
    using entry_type = std::pair<enum_type, std::string_view>;

    static constexpr std::array identifiers {
        entry_type {enum_type::excluded, "excluded"},
        entry_type {enum_type::included, "included"}};

    static constexpr std::string_view name(const enum_type value)
    {
        return render_detail::enum_name(identifiers, value);
    }

};

template <>
struct datum_enum_schema<traversal::page_action>
{
    using enum_type = traversal::page_action;
    using entry_type = std::pair<enum_type, std::string_view>;

    static constexpr std::array identifiers {
        entry_type {enum_type::next, "next"},
        entry_type {enum_type::stop, "stop"}};

    static constexpr std::string_view name(const enum_type value)
    {
        return render_detail::enum_name(identifiers, value);
    }

};

template <>
struct datum_enum_schema<traversal::page_traversal_outcome>
{
    using enum_type = traversal::page_traversal_outcome;
    using entry_type = std::pair<enum_type, std::string_view>;

    static constexpr std::array identifiers {
        entry_type {enum_type::complete, "complete"},
        entry_type {enum_type::stopped, "stopped"},
        entry_type {enum_type::cancelled, "cancelled"},
        entry_type {enum_type::limit_reached, "limit_reached"}};

    static constexpr std::string_view name(const enum_type value)
    {
        return render_detail::enum_name(identifiers, value);
    }

};

} // ceph::salad::datum::compat::detail

#if !defined(__cpp_lib_reflection)
namespace ceph::salad::datum::render {

template <interval::boundary_view BoundaryT>
requires render_detail::interval_value_displayable<typename interval::boundary_domain_t<BoundaryT>::value_type>
struct display_schema<render_detail::interval_boundary_display<BoundaryT>> final
    : compat::detail::datum_display_schema<render_detail::interval_boundary_display<BoundaryT>>
{};

template <interval::ordered_domain DomainT>
requires render_detail::interval_value_displayable<typename DomainT::value_type>
struct display_schema<interval::lower_endpoint<DomainT>> final
    : compat::detail::datum_display_schema<interval::lower_endpoint<DomainT>>
{};

template <interval::ordered_domain DomainT>
requires render_detail::interval_value_displayable<typename DomainT::value_type>
struct display_schema<interval::upper_endpoint<DomainT>> final
    : compat::detail::datum_display_schema<interval::upper_endpoint<DomainT>>
{};

template <render_detail::page_traversal_display_value StateT>
struct display_schema<traversal::page_traversal_result<StateT>> final
    : compat::detail::datum_display_schema<traversal::page_traversal_result<StateT>>
{};

template <render_detail::page_traversal_display_value StateT,
          render_detail::page_traversal_display_value ErrorT>
struct display_schema<traversal::page_traversal_failure<StateT, ErrorT>> final
    : compat::detail::datum_display_schema<traversal::page_traversal_failure<StateT, ErrorT>>
{};

template <>
struct enum_schema<interval::boundary_kind> final : compat::detail::datum_enum_schema<interval::boundary_kind>
{};

template <>
struct enum_schema<interval::endpoint_inclusion> final
    : compat::detail::datum_enum_schema<interval::endpoint_inclusion>
{};

template <>
struct enum_schema<traversal::page_action> final : compat::detail::datum_enum_schema<traversal::page_action>
{};

template <>
struct enum_schema<traversal::page_traversal_outcome> final
    : compat::detail::datum_enum_schema<traversal::page_traversal_outcome>
{};

} // ceph::salad::datum::render
#endif

#endif // CEPH_RGW_DATUM_COMPAT_DETAIL_DATUM_RENDER_H
