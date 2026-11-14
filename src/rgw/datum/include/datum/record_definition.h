// Datum: Complete record declarations and member facts from one field list.
#ifndef CEPH_RGW_DATUM_RECORD_DEFINITION_H
 #define CEPH_RGW_DATUM_RECORD_DEFINITION_H

#include <concepts>
#include <cstddef>
#include <type_traits>

#include <algorithm>
#include <array>
#include <iterator>
#include <ranges>

#include <tuple>
#include <utility>

#include <string_view>

namespace ceph::salad::datum {

template <typename T>
struct member_view;

} // ceph::salad::datum

namespace ceph::salad::datum::compat::detail {

template <typename RecordT>
struct record_definition_tag
{
    using record_type = RecordT;
};

template <typename RecordT, typename FieldT, typename AccessorT>
struct declared_record_field
{
    using value_type = FieldT;

    static consteval auto member() requires (!std::is_reference_v<FieldT>)
    {
        constexpr auto selected = AccessorT::pointer().template operator()<RecordT>();
        static_assert([]<typename OwnerT, typename ValueT>(ValueT OwnerT::*) {
            return std::same_as<OwnerT, RecordT> and std::same_as<ValueT, FieldT>;
        }(selected));

        return selected;
    }

    template <typename ValueT>
    static constexpr decltype(auto) get(const ValueT& source) noexcept
    {
        return AccessorT::accessor()(source);
    }
};

// Only the definition producer supplies these facts. A descriptor list for an
// existing type cannot prove that the type has no other members or base classes.
template <typename RecordT>
concept declared_record = requires {
    typename std::remove_cv_t<RecordT>::datum_cpp23_definition;
    requires std::derived_from<typename std::remove_cv_t<RecordT>::datum_cpp23_definition,
                               record_definition_tag<std::remove_cv_t<RecordT>>>;
    requires std::same_as<
        typename std::remove_cv_t<RecordT>::datum_cpp23_definition::record_type,
        std::remove_cv_t<RecordT>>;
    typename decltype(std::remove_cv_t<RecordT>::datum_cpp23_definition::fields())::type;
};

template <declared_record RecordT>
using record_field_tuple =
    typename decltype(std::remove_cv_t<RecordT>::datum_cpp23_definition::fields())::type;

// Metadata consumers inspect types only; no tuple elements are constructed or copied.
template <declared_record RecordT, typename FunctionT>
consteval decltype(auto) with_field_types(FunctionT&& function)
{
    return [&]<typename ...FieldT>(std::type_identity<std::tuple<FieldT...>>) -> decltype(auto) {
        return std::forward<FunctionT>(function).template operator()<FieldT...>();
    }(std::type_identity<record_field_tuple<RecordT>> {});
}

template <declared_record RecordT, std::size_t Index>
using record_field = std::tuple_element_t<Index, record_field_tuple<RecordT>>;

template <declared_record RecordT, std::size_t Index>
using record_value = std::remove_cvref_t<typename record_field<RecordT, Index>::value_type>;

template <declared_record RecordT>
inline constexpr std::size_t record_size = std::tuple_size_v<record_field_tuple<RecordT>>;

template <auto Left, auto Right>
consteval bool same_member()
{
    if constexpr (std::same_as<decltype(Left), decltype(Right)>) {
        return Left == Right;
    }

    return false;
}

template <auto Member, typename FieldT>
consteval bool matches_member()
{
    if constexpr (requires { FieldT::member(); }) {
        return same_member<Member, FieldT::member()>();
    }

    return false;
}

// The not-found value is the size, matching the standard end iterator convention.
template <std::size_t Count>
consteval std::size_t matching_index(const std::array<bool, Count>& matches)
{
    return std::ranges::find(matches, true) - std::begin(matches);
}

template <declared_record RecordT, auto Member>
consteval std::size_t member_index()
{
    return with_field_types<RecordT>([]<typename ...FieldT>() {
        return matching_index(std::array<bool, sizeof...(FieldT)> {
            matches_member<Member, FieldT>()...});
    });
}

template <typename FieldT, auto ...Member>
consteval std::size_t selected_index()
{
    return matching_index(std::array<bool, sizeof...(Member)> {
        matches_member<Member, FieldT>()...});
}

template <auto Needle, auto ...Member>
consteval std::size_t member_index()
{
    return matching_index(std::array<bool, sizeof...(Member)> {same_member<Needle, Member>()...});
}

template <auto ...Member>
consteval bool unique_members()
{
    // A repeated member's first position cannot equal its later selection position.
    return []<std::size_t ...Index>(std::index_sequence<Index...>) {
        return ((member_index<Member, Member...>() == Index) and ...);
    }(std::make_index_sequence<sizeof...(Member)> {});
}

} // ceph::salad::datum::compat::detail

// A field list calls Field(Type, Name) with an optional initializer expression.
// Use Type{...} for braced initialization and aliases for types containing commas.
// The producer declares the whole record, with no base clause; it is not an
// annotation or ownership certificate for an existing record.
#define DATUM_DETAIL_RECORD_DECLARE(Type, Name, ...) Type Name __VA_OPT__(= (__VA_ARGS__));
// Named accessor classes keep closure types and auto-return ABI tags out of signatures.
#define DATUM_DETAIL_RECORD_FIELD(Type, Name, ...) \
    struct datum_field_##Name final : ::ceph::salad::datum::compat::detail::declared_record_field< \
        record_type, decltype(record_type::Name), datum_field_##Name> \
    { \
        static consteval std::string_view name() noexcept { return #Name; } \
        static consteval auto pointer() { return []<typename DatumRecord> { \
            return &DatumRecord::Name; }; } \
        static consteval auto accessor() { return [](const auto& value) noexcept \
            -> decltype(auto) { return (value.Name); }; } \
    };
#define DATUM_DETAIL_RECORD_DESCRIBE(Type, Name, ...) , std::declval<std::tuple<datum_field_##Name>>()
#define DATUM_DETAIL_RECORD_VIEW(Type, Name, ...) \
    typename ::ceph::salad::datum::member_view<std::remove_cvref_t< \
        decltype(std::declval<const datum_cpp23_record_type&>().Name)>>::type Name;

// The temporary producer reserves datum_cpp23_ names for its metadata.
// A named template gives ordinary records a view type with external identity.
#define DATUM_DETAIL_RECORD_GLOBAL_VIEW(Record, Fields) \
    template <typename datum_cpp23_record_type> \
    struct view_type { Fields(DATUM_DETAIL_RECORD_VIEW) }; \
    static consteval auto view_builder() \
    { \
        return []<typename datum_cpp23_record_type> { \
            return std::type_identity<view_type<datum_cpp23_record_type>> {}; }; \
    }
// A local class cannot declare a member template; its views stay in the local scope.
#define DATUM_DETAIL_RECORD_LOCAL_VIEW(Record, Fields) \
    static consteval auto view_builder() \
    { \
        return []<typename datum_cpp23_record_type> { \
            struct type { Fields(DATUM_DETAIL_RECORD_VIEW) }; \
            return std::type_identity<type> {}; \
        }; \
    }
#define DATUM_DETAIL_RECORD(Record, Fields, View) \
    struct Record \
    { \
        Fields(DATUM_DETAIL_RECORD_DECLARE) \
        struct datum_cpp23_definition final \
            : ::ceph::salad::datum::compat::detail::record_definition_tag<Record> \
        { \
            Fields(DATUM_DETAIL_RECORD_FIELD) \
            static consteval auto fields() \
            { \
                using fields_type = decltype(std::tuple_cat( \
                    std::tuple {} Fields(DATUM_DETAIL_RECORD_DESCRIBE))); \
                return std::type_identity<fields_type> {}; \
            } \
            View(Record, Fields) \
        }; \
    }

// Field lists provide complete C++23 metadata and remain usable with C++26 reflection.
#define DATUM_RECORD(Record, Fields) \
    DATUM_DETAIL_RECORD(Record, Fields, DATUM_DETAIL_RECORD_GLOBAL_VIEW)
#define DATUM_LOCAL_RECORD(Record, Fields) \
    DATUM_DETAIL_RECORD(Record, Fields, DATUM_DETAIL_RECORD_LOCAL_VIEW)

#endif // CEPH_RGW_DATUM_RECORD_DEFINITION_H
