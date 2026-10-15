// Datum: Reflected record views, ordered fields and explicit wire schema descriptions.
// Reflection supplies structure; member projections and lifetime stay explicit.
//
// Please keep native record recognition and storage ownership in consumer
// profiles. A view borrows its source and never establishes admission or commit.

#ifndef CEPH_RGW_DATUM_RECORD_H
 #define CEPH_RGW_DATUM_RECORD_H

#include "datum/detail/record.h"
#include "datum/ownership.h"

#include <concepts>
#include <cstddef>
#include <meta>
#include <type_traits>

#include <tuple>
#include <utility>

#include <array>
#include <vector>

#include <algorithm>
#include <functional>
#include <iterator>
#include <ranges>

namespace ceph::salad::datum {

// Consumers may select a value view (for example string_view) instead of the
// default const reference. Projection must not outlive the source's storage.
template <typename T>
struct member_view
{
    using type = const T&;

public:
    static constexpr type get(const T& value) noexcept { return value; }

};

namespace detail {

template <typename RecordT>
consteval auto view_descriptions()
{
    static_assert(std::is_class_v<RecordT> && !std::is_union_v<RecordT>);
    static_assert(std::empty(std::meta::bases_of(^^RecordT, std::meta::access_context::unchecked())),
                  "Use an explicit projection for records with base classes");
    std::vector<std::meta::info> descriptions;
    template for (constexpr auto member : record_members<RecordT>) {
        static_assert(!std::meta::is_bit_field(member) && std::meta::has_identifier(member),
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

} // detail

template <typename RecordT>
struct view_of : detail::generated_view<RecordT>
{

private:
    template <std::size_t ...Index>
    constexpr view_of(const RecordT& source, std::index_sequence<Index...>) noexcept(
        (noexcept(member_view<std::remove_cvref_t<decltype(source.[:detail::record_members<RecordT>[Index]:])>>::get(
             source.[:detail::record_members<RecordT>[Index]:])) &&
         ...))
        : detail::generated_view<RecordT> {
              member_view<std::remove_cvref_t<decltype(source.[:detail::record_members<RecordT>[Index]:])>>::get(
                  source.[:detail::record_members<RecordT>[Index]:])...}
    {}

public:
    constexpr explicit view_of(const RecordT& source) noexcept(noexcept(
        view_of(source, std::make_index_sequence<std::size(detail::record_members<RecordT>)> {})))
        : view_of(source, std::make_index_sequence<std::size(detail::record_members<RecordT>)> {})
    {}
    view_of(RecordT&&) = delete;
    view_of(const RecordT&&) = delete;

};

template <typename RecordT>
view_of(const RecordT&) -> view_of<RecordT>;

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
        return std::ranges::find(detail::record_members<RecordT>, detail::record_member<Member>()) -
               std::begin(detail::record_members<RecordT>);
    }

public:
    using source_type = RecordT;
    using value_type = FieldT;

    static constexpr auto member = Member;

public:
    // Complete generated views before deducing their spliced member types.
    template <typename ValueT>
    requires requires { sizeof(ValueT); } &&
             (std::same_as<ValueT, RecordT> || std::derived_from<ValueT, view_of<RecordT>>)
    constexpr decltype(auto) operator()(const ValueT& value) const noexcept
    {
        constexpr bool source_record = std::same_as<ValueT, RecordT>;
        constexpr auto index = member_index();
        if constexpr (source_record) {
            return std::as_const(value.*Member);
        }

        if constexpr (!source_record) {
            // Generated members retain the public source members' order.
            return (value.[:detail::record_members<detail::generated_view<RecordT>>[index]:]);
        }
    }

    // A selected reference must not escape a temporary owner or projection.
    template <typename ValueT>
    void operator()(const ValueT&&) const = delete;

};

// Enumerate lenses before reading values so policy can omit an observation.
// Effects and exceptions belong to the consumer; a void result is not progress.
template <auto ...Lens>
struct field_order
{
    static constexpr std::size_t size = sizeof...(Lens);

public:
    template <typename SinkT>
    requires (requires(SinkT& sink) {
        { std::invoke(sink, Lens) } -> std::same_as<void>;
    } && ...)
    static constexpr void visit(SinkT&& sink)
    {
        (std::invoke(sink, Lens), ...);
    }

public:
    template <typename ValueT, typename SinkT>
    requires (requires(const ValueT& value, SinkT& sink) {
        { std::invoke(sink, std::invoke(Lens, value)) } -> std::same_as<void>;
    } && ...)
    static constexpr void observe(const ValueT& value, SinkT&& sink)
    {
        visit([&value, &sink](const auto& lens) { std::invoke(sink, std::invoke(lens, value)); });
    }

};

// The same explicit member selection supplies observation, candidate construction
// and consuming projection. Only complete aggregate layouts infer construction;
// other records use an explicit factory. Codecs and conversions remain external.
template <typename ReaderT, auto Member>
concept field_value_reader =
    std::invocable<ReaderT&, member_lens<Member>> &&
    std::same_as<std::invoke_result_t<ReaderT&, member_lens<Member>>,
                 std::remove_cv_t<typename member_lens<Member>::value_type>>;

template <auto ...Member>
requires (std::is_member_object_pointer_v<decltype(Member)> && ...) && ((Member != nullptr) && ...)
struct member_fields : field_order<member_lens<Member> {}...>
{
private:
    static constexpr std::array<std::meta::info, sizeof...(Member)> approved {
        std::meta::reflect_constant(Member)...};

    static consteval bool unique()
    {
        return std::ranges::all_of(approved | std::views::enumerate, [](const auto& entry) {
            const auto [index, member] = entry;

            return !std::ranges::contains(approved | std::views::drop(index + 1), member);
        });
    }

    template <typename ReaderT>
    static constexpr bool value_reader = (field_value_reader<ReaderT, Member> && ...);

    template <typename ReaderT>
    static constexpr auto read_values(ReaderT& reader)
    {
        // Braces sequence wire reads left to right, unlike function arguments.
        return std::tuple<std::remove_cv_t<typename member_lens<Member>::value_type>...> {
                    std::invoke(reader, member_lens<Member> {})...
                };
    }

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

    template <typename RecordT, typename ReaderT, std::size_t ...Index>
    static constexpr RecordT build(ReaderT& reader, std::index_sequence<Index...>)
    {
        constexpr bool declaration_order = ((declaration_slots<RecordT>[Index] == Index) && ...);
        if constexpr (declaration_order) {
            return RecordT {std::invoke(reader, member_lens<Member> {})...};
        }

        if constexpr (!declaration_order) {
            auto values = read_values(reader);

            return RecordT {std::get<declaration_slots<RecordT>[Index]>(std::move(values))...};
        }
    }

public:
    template <typename RecordT>
    static consteval bool selects()
    {
        if constexpr (std::is_class_v<RecordT> && !std::is_union_v<RecordT>) {
            // Check private inheritance too; member selection still uses public facts.
            constexpr auto context = std::meta::access_context::unchecked();
            if (!std::empty(std::meta::bases_of(^^RecordT, context)) || !unique()) {
                return false;
            }
            std::size_t matched = 0;
            template for (constexpr auto field : detail::record_members<RecordT>) {
                using field_type = [:std::meta::type_of(field):];

                if constexpr (!std::meta::is_bit_field(field) && !std::is_reference_v<field_type>) {
                    if (std::ranges::contains(approved,
                                              std::meta::reflect_constant(&[:field:]))) {
                        ++matched;
                    }
                }
            }

            return sizeof...(Member) == matched;
        }

        return false;
    }

    template <typename RecordT>
    static consteval bool covers()
    {
        if constexpr (std::is_aggregate_v<RecordT> && !std::is_union_v<RecordT>) {
            return sizeof...(Member) == std::size(detail::record_members<RecordT>) &&
                   selects<RecordT>();
        }

        return false;
    }

    template <owning_value RecordT, typename ReaderT>
    requires (covers<RecordT>()) && value_reader<ReaderT>
    static constexpr RecordT reconstruct(ReaderT&& reader)
    {
        return build<RecordT>(reader, std::make_index_sequence<sizeof...(Member)> {});
    }

    template <typename ReaderT, typename FactoryT>
    requires value_reader<ReaderT> &&
             std::invocable<FactoryT,
                            std::remove_cv_t<typename member_lens<Member>::value_type>&&...> &&
             owning_value<std::invoke_result_t<
                 FactoryT, std::remove_cv_t<typename member_lens<Member>::value_type>&&...>>
    static constexpr auto construct(ReaderT&& reader, FactoryT&& factory)
    {
        auto [...values] = read_values(reader);

        return std::invoke(std::forward<FactoryT>(factory), std::move(values)...);
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

} // ceph::salad::datum

// Datum: Declaration-order and selected-member binary schema descriptions.
// Generic reflected schema facts; codecs and record enrollment are separate.
//
// Please keep this layer independent of native records, concrete codecs and application
// wire policy.
// Put domain-specific behavior in consumer profiles or adapters.

namespace ceph::salad::datum::serialization_detail {

struct binary_schema_tag
{};

template <typename RecordT, std::size_t Count>
consteval bool valid_member_count()
{
    constexpr auto context = std::meta::access_context::unprivileged();

    return std::is_aggregate_v<RecordT> && std::empty(std::meta::bases_of(^^RecordT, context)) &&
           Count == std::size(detail::record_members<RecordT>);
}

} // ceph::salad::datum::serialization_detail

namespace ceph::salad::datum {

template <auto ...Member>
struct binary_fields : member_fields<Member...>
{};

template <typename RecordT, auto ...Member>
consteval bool valid_binary_fields(binary_fields<Member...>)
{
    return member_fields<Member...>::template covers<RecordT>();
}

// Coverage is reflected; the approved wire order remains explicit:
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
        return std::type_identity<direct_binary_schema<RecordT, Member...>> {};
    }

    if constexpr (0 == sizeof...(Member)) {
        static_assert(std::is_class_v<RecordT> && !std::is_union_v<RecordT>,
                      "Declaration-order binary schema requires a record type");
        static constexpr auto members = detail::record_members<RecordT>;
        template for (constexpr auto field : members) {
            using field_type = [:std::meta::type_of(field):];

            static_assert(
                !std::meta::is_bit_field(field) && !std::is_reference_v<field_type>,
                "Use an explicit field selection to omit bit fields or reference members");
        }

        return []<std::size_t ...Index>(std::index_sequence<Index...>) {
            return std::type_identity<direct_binary_schema<RecordT, &[:members[Index]:]...>> {};
        }(std::make_index_sequence<std::size(members)> {});
    }
}

} // serialization_detail

// Declaration order is the default wire order; an explicit list can reorder or
// omit members. Naming a schema requires no enrollment of the original record.
template <typename RecordT, auto ...Member>
using binary_schema_t =
    typename decltype(serialization_detail::describe_binary_schema<RecordT, Member...>())::type;

template <typename SchemaT>
concept binary_schema_type = std::derived_from<SchemaT, serialization_detail::binary_schema_tag>;

} // ceph::salad::datum

#endif // CEPH_RGW_DATUM_RECORD_H
