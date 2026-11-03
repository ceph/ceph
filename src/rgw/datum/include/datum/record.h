// Datum: Record views, ordered fields and explicit wire schema descriptions.
// C++23 is temporary; native C++26 generation and pack syntax remain preferred.
#ifndef CEPH_RGW_DATUM_RECORD_H
 #define CEPH_RGW_DATUM_RECORD_H

#include "datum/detail/field.h"
#include "datum/ownership.h"
#include "datum/record_definition.h"

#include <concepts>
#include <cstddef>
#include <type_traits>
#include <utility>
#include <version>

#include <array>
#include <tuple>

#include <functional>
#include <iterator>
#include <ranges>

#if defined(__cpp_lib_reflection)
 #include "datum/detail/record.h"
 #include <algorithm>
 #include <meta>
 #include <vector>
#else
 #include "datum/compat/record_view.h"
#endif

namespace ceph::salad::datum {

#if defined(__cpp_lib_reflection)
namespace detail {

template <typename RecordT>
consteval auto view_descriptions()
{
    static_assert(std::is_class_v<RecordT> and !std::is_union_v<RecordT>);
    static_assert(std::empty(std::meta::bases_of(^^RecordT, std::meta::access_context::unchecked())),
                  "Use an explicit projection for records with base classes");
    std::vector<std::meta::info> descriptions;
    template for (constexpr auto member : record_members<RecordT>) {
        static_assert(!std::meta::is_bit_field(member) and std::meta::has_identifier(member),
                      "Use an explicit projection for anonymous members and bit fields");
        using member_type = [:std::meta::type_of(member):];
        using value_type = std::remove_cvref_t<member_type>;
        using projection_type = typename member_view<value_type>::type;

        static_assert(
            requires (const value_type& source) {
                { member_view<value_type>::get(source) } -> std::same_as<projection_type>;
            }, "A member projection must return its declared view type");
        descriptions.push_back(
            std::meta::data_member_spec(^^projection_type, {.name = std::meta::identifier_of(member)}));
    }

    return descriptions;
}

template <typename RecordT>
struct view_members
{
    struct type;
    consteval { std::meta::define_aggregate(^^type, view_descriptions<RecordT>()); }
};

template <typename RecordT>
using generated_view = typename view_members<RecordT>::type;

template <typename RecordT>
inline constexpr std::size_t record_size = std::size(record_members<RecordT>);

} // detail

#else
namespace detail {

template <typename RecordT>
using generated_view = typename compat::detail::view_members<RecordT>::type;

template <typename RecordT>
inline constexpr std::size_t record_size = compat::detail::record_size<RecordT>;

} // detail
#endif

template <typename RecordT>
struct view_of : detail::generated_view<RecordT>
{

private:
#if defined(__cpp_lib_reflection)
    template <std::size_t ...Index>
    constexpr view_of(const RecordT& source, std::index_sequence<Index...>) noexcept(
        (noexcept(member_view<std::remove_cvref_t<decltype(
             source.[:detail::record_members<RecordT>[Index]:])>>::get(
             source.[:detail::record_members<RecordT>[Index]:])) and
         ...))
        : detail::generated_view<RecordT> {
              member_view<std::remove_cvref_t<decltype(
                  source.[:detail::record_members<RecordT>[Index]:])>>::get(
                  source.[:detail::record_members<RecordT>[Index]:])...}
    {}

#else
    template <std::size_t ...Index>
    constexpr view_of(const RecordT& source, std::index_sequence<Index...>) noexcept(
        (noexcept(member_view<compat::detail::record_value<RecordT, Index>>::get(
             compat::detail::record_field<RecordT, Index>::get(source))) and ...))
        : detail::generated_view<RecordT> {
              member_view<compat::detail::record_value<RecordT, Index>>::get(
                  compat::detail::record_field<RecordT, Index>::get(source))...}
    {}

#endif

public:
    constexpr explicit view_of(const RecordT& source) noexcept(noexcept(
        view_of(source, std::make_index_sequence<detail::record_size<RecordT>> {})))
        : view_of(source, std::make_index_sequence<detail::record_size<RecordT>> {})
    {}
    view_of(RecordT&&) = delete;
    view_of(const RecordT&&) = delete;

};

// Datum: Selected field lenses and ordered, policy-controlled observation.
// Reflection maps source members to their generated views before runtime.
//
// Please keep this layer independent of native records, codecs and application
// policy. Observing fields establishes neither wire enrollment nor ownership.

template <auto Member>
struct member_lens;

template <typename RecordT, typename FieldT, FieldT RecordT::*Member>
struct member_lens<Member> final
{

private:
    static consteval std::size_t member_index()
    {
#if defined(__cpp_lib_reflection)
        return std::ranges::find(detail::record_members<RecordT>, detail::record_member<Member>()) -
               std::begin(detail::record_members<RecordT>);
#else
        constexpr auto index = compat::detail::member_index<RecordT, Member>();
        static_assert(index != detail::record_size<RecordT>,
                      "A field must select a direct public source member");

        return index;
#endif
    }

    // Keep available membership checks without requiring metadata for source access.
    static consteval void check_source_member()
    {
#if defined(__cpp_lib_reflection)
        static_cast<void>(detail::record_member<Member>());
#else
        if constexpr (compat::detail::declared_record<RecordT>) {
            static_cast<void>(member_index());
        }
#endif
    }

public:
    using source_type = RecordT;
    using value_type = FieldT;

    static constexpr auto member = Member;

public:
    // Complete generated views before deducing their spliced member types.
    template <typename ValueT>
    requires std::is_member_object_pointer_v<decltype(Member)> and
             (nullptr != Member) and requires { sizeof(ValueT); } and
             (std::same_as<ValueT, RecordT> || std::derived_from<ValueT, view_of<RecordT>>)
    constexpr decltype(auto) operator()(const ValueT& value) const noexcept
    {
        constexpr bool source_record = std::same_as<ValueT, RecordT>;
        if constexpr (source_record) {
            check_source_member();
            return std::as_const(value.*Member);
        }

        if constexpr (!source_record) {
            constexpr auto index = member_index();
            // Generated members retain the public source members' order.
#if defined(__cpp_lib_reflection)
            return (value.[:detail::record_members<detail::generated_view<RecordT>>[index]:]);
#else
            using field = compat::detail::record_field<RecordT, index>;
            return field::get(static_cast<const detail::generated_view<RecordT>&>(value));
#endif
        }
    }

    // A selected reference must not escape a temporary owner or projection.
    template <typename ValueT>
    void operator()(const ValueT&&) const = delete;

};

// The same explicit member selection supplies observation, candidate construction
// and consuming projection. Only complete aggregate layouts infer construction;
// other records use an explicit factory. Codecs and conversions remain external.
template <typename ReaderT, auto Member>
concept field_value_reader =
    std::invocable<ReaderT&, member_lens<Member>> and
    std::same_as<std::invoke_result_t<ReaderT&, member_lens<Member>>,
                 std::remove_cv_t<typename member_lens<Member>::value_type>>;

template <auto ...Member>
requires (std::is_member_object_pointer_v<decltype(Member)> and ...) and ((Member != nullptr) and ...)
struct member_fields : field_order<member_lens<Member> {}...>
{
private:
#if defined(__cpp_lib_reflection)
    static constexpr std::array<std::meta::info, sizeof...(Member)> approved {
        std::meta::reflect_constant(Member)...};

    static consteval bool unique()
    {
        return std::ranges::all_of(approved | std::views::enumerate, [](const auto& entry) {
            const auto [index, member] = entry;

            return !std::ranges::contains(approved | std::views::drop(index + 1), member);
        });
    }

#else
    static consteval bool unique() { return compat::detail::unique_members<Member...>(); }

#endif

    template <typename ReaderT>
    static constexpr bool value_reader = (field_value_reader<ReaderT, Member> and ...);

    template <typename ReaderT>
    static constexpr auto read_values(ReaderT& reader)
    {
        // Braces sequence wire reads left to right, unlike function arguments.
        return std::tuple<std::remove_cv_t<typename member_lens<Member>::value_type>...> {
                    std::invoke(reader, member_lens<Member> {})...
                };
    }

#if defined(__cpp_lib_reflection)
    template <typename RecordT>
    static constexpr auto declaration_slots = [] consteval {
        std::array<std::size_t, sizeof...(Member)> slots {};
        std::size_t index = 0;
        template for (constexpr auto field : detail::record_members<RecordT>) {
            slots[index++] =
                std::ranges::find(approved, std::meta::reflect_constant(&[:field:])) -
                std::begin(approved);
        }

        return slots;
    }();

#else
    template <typename RecordT>
    static constexpr auto declaration_slots =
        compat::detail::with_field_types<RecordT>([]<typename ...FieldT>() {
            return std::array<std::size_t, sizeof...(FieldT)> {
                compat::detail::selected_index<FieldT, Member...>()...};
        });

#endif

    template <typename RecordT, typename ReaderT, std::size_t ...Index>
    static constexpr RecordT build(ReaderT& reader, std::index_sequence<Index...>)
    {
        constexpr bool declaration_order = ((declaration_slots<RecordT>[Index] == Index) and ...);
        if constexpr (declaration_order) {
            return RecordT {std::invoke(reader, member_lens<Member> {})...};
        }

        if constexpr (!declaration_order) {
            auto values = read_values(reader);

            return RecordT {std::get<declaration_slots<RecordT>[Index]>(std::move(values))...};
        }
    }

public:
#if defined(__cpp_lib_reflection)
    template <typename RecordT>
    static consteval bool selects()
    {
        if constexpr (std::is_class_v<RecordT> and !std::is_union_v<RecordT>) {
            // Check private inheritance too; member selection still uses public facts.
            constexpr auto context = std::meta::access_context::unchecked();
            if (!std::empty(std::meta::bases_of(^^RecordT, context))) {
                return false;
            }
            std::size_t matched = 0;
            template for (constexpr auto field : detail::record_members<RecordT>) {
                using field_type = [:std::meta::type_of(field):];

                if constexpr (!std::meta::is_bit_field(field) and !std::is_reference_v<field_type>) {
                    if (std::ranges::contains(approved,
                                              std::meta::reflect_constant(&[:field:]))) {
                        ++matched;
                    }
                }
            }

            // Each direct field contributes once, so this also rejects duplicate selections.
            return sizeof...(Member) == matched;
        }

        return false;
    }

    template <typename RecordT>
    static consteval bool covers()
    {
        if constexpr (std::is_aggregate_v<RecordT> and !std::is_union_v<RecordT>) {
            return sizeof...(Member) == std::size(detail::record_members<RecordT>) and
                   selects<RecordT>();
        }

        return false;
    }

#else
    template <typename RecordT>
    static consteval bool selects()
    {
        if constexpr (compat::detail::declared_record<RecordT>) {
            // Counting distinct source fields also rejects duplicates without another search.
            return compat::detail::with_field_types<RecordT>([]<typename ...FieldT>() {
                return sizeof...(Member) ==
                    (std::size_t {0} + ... +
                        (compat::detail::selected_index<FieldT, Member...>() != sizeof...(Member)));
            });
        }

        return false;
    }

    template <typename RecordT>
    static consteval bool covers()
    {
        if constexpr (compat::detail::declared_record<RecordT>) {
            return sizeof...(Member) == compat::detail::record_size<RecordT> and selects<RecordT>();
        }

        return false;
    }

#endif

    template <owning_value RecordT, typename ReaderT>
    requires (covers<RecordT>()) and value_reader<ReaderT>
    static constexpr RecordT reconstruct(ReaderT&& reader)
    {
        return build<RecordT>(reader, std::make_index_sequence<sizeof...(Member)> {});
    }

    template <typename ReaderT, typename FactoryT>
    requires value_reader<ReaderT> and
             std::invocable<FactoryT,
                            std::remove_cv_t<typename member_lens<Member>::value_type>&&...> and
             owning_value<std::invoke_result_t<
                 FactoryT, std::remove_cv_t<typename member_lens<Member>::value_type>&&...>>
    static constexpr auto construct(ReaderT&& reader, FactoryT&& factory)
    {
#if defined(__cpp_structured_bindings) and __cpp_structured_bindings >= 202411L
        [[maybe_unused]] auto [...values] = read_values(reader);

        return std::invoke(std::forward<FactoryT>(factory), std::move(values)...);
#else
        auto values = read_values(reader);

        return std::apply(std::forward<FactoryT>(factory), std::move(values));
#endif
    }

    template <owning_value RecordT, typename SourceT>
    requires (unique()) &&
            requires (SourceT&& source) { RecordT {(std::forward<SourceT>(source).*Member)...}; }
    static constexpr RecordT project(SourceT&& source) noexcept(noexcept(RecordT {
        (std::forward<SourceT>(source).*Member)...}))
    {
        return RecordT {(std::forward<SourceT>(source).*Member)...};
    }
};

namespace serialization_detail {

struct binary_schema_tag
{};

template <typename RecordT, std::size_t Count>
consteval bool valid_member_count()
{
#if defined(__cpp_lib_reflection)
    constexpr auto context = std::meta::access_context::unprivileged();

    return std::is_aggregate_v<RecordT> and std::empty(std::meta::bases_of(^^RecordT, context)) and
           Count == std::size(detail::record_members<RecordT>);
#else
    if constexpr (compat::detail::declared_record<RecordT>) {
        return Count == compat::detail::record_size<RecordT>;
    }

    return false;
#endif
}

} // serialization_detail


template <auto ...Member>
struct binary_fields : member_fields<Member...>
{};

// A nonempty selection carries its record identity. Profiles remain usable for
// observation and native codecs; binary_schema_type checks schema eligibility.
template <auto First, auto ...Member>
struct binary_fields<First, Member...>
    : member_fields<First, Member...>, serialization_detail::binary_schema_tag
{
    using record_type = typename member_lens<First>::source_type;
    using fields = binary_fields;
};

template <typename RecordT, auto ...Member>
consteval bool valid_binary_fields(binary_fields<Member...>)
{
    return member_fields<Member...>::template covers<RecordT>();
}

// Verify complete member coverage; the approved wire order remains explicit.
template <typename RecordT, auto ...Member>
struct checked_binary_schema : serialization_detail::binary_schema_tag
{
    static_assert(
        valid_binary_fields<RecordT>(binary_fields<Member...> {}),
        "Durable schema must cover every direct aggregate member exactly once; review wire compatibility");

    using fields = binary_fields<Member...>;
    using record_type = RecordT;
};

template <typename T>
struct binary_schema;

namespace serialization_detail {

template <typename RecordT, auto ...Member>
struct direct_binary_schema final : binary_schema_tag
{
    using record_type = RecordT;
    using fields = binary_fields<Member...>;

    static_assert(fields::template selects<RecordT>(),
                  "Binary schema must select distinct direct public members of its record");
};

template <typename RecordT, auto ...Member>
consteval auto describe_binary_schema()
{
    if constexpr (0 != sizeof...(Member)) {
        using fields = binary_fields<Member...>;

        static_assert(fields::template selects<RecordT>(),
                      "Binary schema must select distinct direct public members of its record");

        // Preserve an explicitly qualified record without duplicating the usual descriptor.
        return std::type_identity<std::conditional_t<
            std::same_as<RecordT, typename fields::record_type>, fields,
            direct_binary_schema<RecordT, Member...>>> {};
    }

#if defined(__cpp_lib_reflection)
    if constexpr (0 == sizeof...(Member)) {
        static_assert(std::is_class_v<RecordT> and !std::is_union_v<RecordT>,
                      "Declaration-order binary schema requires a record type");
        static constexpr auto members = detail::record_members<RecordT>;
        template for (constexpr auto field : members) {
            using field_type = [:std::meta::type_of(field):];

            static_assert(
                !std::meta::is_bit_field(field) and !std::is_reference_v<field_type>,
                "Use an explicit field selection to omit bit fields or reference members");
        }

        return []<std::size_t ...Index>(std::index_sequence<Index...>) {
            if constexpr (0 == sizeof...(Index)) {
                return std::type_identity<direct_binary_schema<RecordT>> {};
            }

            if constexpr (0 != sizeof...(Index)) {
                return describe_binary_schema<RecordT, &[:members[Index]:]...>();
            }
        }(std::make_index_sequence<std::size(members)> {});
    }
#else
    if constexpr (0 == sizeof...(Member)) {
        static_assert(compat::detail::declared_record<RecordT>,
            "Declaration-order schema needs complete C++23 record metadata; "
            "declare the record with DATUM_RECORD from <datum/record_definition.h>");
        if constexpr (compat::detail::declared_record<RecordT>) {
            constexpr bool addressable =
                compat::detail::with_field_types<RecordT>([]<typename ...FieldT>() {
                    return (requires { FieldT::member(); } and ...);
                });
            static_assert(addressable,
                          "Use an explicit field selection to omit reference members");
            if constexpr (addressable) {
                return []<std::size_t ...Index>(std::index_sequence<Index...>) {
                    if constexpr (0 == sizeof...(Index)) {
                        return std::type_identity<direct_binary_schema<RecordT>> {};
                    }

                    if constexpr (0 != sizeof...(Index)) {
                        return describe_binary_schema<RecordT,
                            compat::detail::record_field<RecordT, Index>::member()...>();
                    }
                }(std::make_index_sequence<compat::detail::record_size<RecordT>> {});
            }
        }
    }
#endif
}

} // serialization_detail

// Declaration order is the default wire order; an explicit list can reorder or
// omit members. Naming a schema requires no enrollment of the original record.
template <typename RecordT, auto ...Member>
using binary_schema_t =
    typename decltype(serialization_detail::describe_binary_schema<RecordT, Member...>())::type;

template <typename SchemaT>
concept binary_schema_type =
    std::derived_from<SchemaT, serialization_detail::binary_schema_tag> and
    SchemaT::fields::template selects<typename SchemaT::record_type>();


} // ceph::salad::datum

#endif // CEPH_RGW_DATUM_RECORD_H
