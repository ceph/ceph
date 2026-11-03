// Temporary C++23 declaration facts; native C++26 records remain ordinary C++.
#include <datum/record_definition.h>
#include <datum/ownership.h>

#include <concepts>
#include <cstddef>
#include <type_traits>

#include <tuple>
#include <utility>

#include <array>
#include <memory>
#include <stdexcept>

#include <string>
#include <string_view>

namespace datum = ::ceph::salad::datum;
namespace definition = datum::compat::detail;

namespace {

#define VALUE_FIELDS(Field) \
    Field(std::string, label) \
    Field(unsigned, revision)

struct ordinary_value
{
    std::string label;
    unsigned revision;
};

#if defined(__cpp_lib_reflection)
using value_record = ordinary_value;
#else
DATUM_RECORD(value_record, VALUE_FIELDS);
#endif

// Exercise the producer in every mode, independently of native backend selection.
DATUM_RECORD(defined_value, VALUE_FIELDS);
#undef VALUE_FIELDS

#define OWNED_FIELDS(Field) \
    Field(std::unique_ptr<int>, payload) \
    Field(defined_value, value)
DATUM_RECORD(owned_record, OWNED_FIELDS);
#undef OWNED_FIELDS

#define BORROWED_FIELDS(Field) \
    Field(defined_value, value) \
    Field(std::string_view, observed)
DATUM_RECORD(borrowed_record, BORROWED_FIELDS);
#undef BORROWED_FIELDS

#define REFERENCE_FIELDS(Field) \
    Field(int&, observed) \
    Field(const unsigned, revision)
DATUM_RECORD(reference_record, REFERENCE_FIELDS);
#undef REFERENCE_FIELDS

#define COLLISION_FIELDS(Field) \
    Field(int, record_type) \
    Field(int, project)
DATUM_RECORD(collision_record, COLLISION_FIELDS);
#undef COLLISION_FIELDS

#define EMPTY_FIELDS(Field)
DATUM_RECORD(empty_record, EMPTY_FIELDS);
#undef EMPTY_FIELDS

namespace alias_names {

using record_type = std::string;
using datum_field_value = unsigned;
#define ALIAS_FIELDS(Field) Field(record_type, label) Field(datum_field_value, value)
DATUM_RECORD(record, ALIAS_FIELDS);
#undef ALIAS_FIELDS
using fields = definition::record_field_tuple<record>;
static_assert(std::same_as<std::tuple_element_t<0, fields>::value_type, std::string>);
static_assert(std::same_as<std::tuple_element_t<1, fields>::value_type, unsigned>);
static_assert(std::tuple_element_t<0, fields>::member() == &record::label);
static_assert(std::tuple_element_t<1, fields>::member() == &record::value);

} // alias_names

struct external_record
{
    unsigned value;
};

struct inherited_definition : defined_value
{};

struct immovable
{
    immovable() = delete;
    explicit immovable(int) {}
    immovable(const immovable&) = delete;
    immovable(immovable&&) = delete;
};

#define IMMOVABLE_FIELDS(Field) \
    Field(immovable, value)
DATUM_RECORD(immovable_record, IMMOVABLE_FIELDS);
#undef IMMOVABLE_FIELDS

using choices_type = std::array<int, 2>;
#define DEFAULT_FIELDS(Field) \
    Field(int, revision, 99) \
    Field(choices_type, choices, choices_type {3, 5}) \
    Field(int, following, revision + 1)
DATUM_RECORD(default_record, DEFAULT_FIELDS);
#undef DEFAULT_FIELDS

struct ordinary_defaults
{
    int revision = 99;
    choices_type choices = choices_type {3, 5};
    int following = revision + 1;
};

#define PINNED_DEFAULT_FIELDS(Field) Field(immovable, value, immovable {17})
DATUM_RECORD(pinned_default, PINNED_DEFAULT_FIELDS);
#undef PINNED_DEFAULT_FIELDS

unsigned initializer_calls = 0;
bool fail_initializer = false;
int initial_value()
{
    ++initializer_calls;
    if (fail_initializer) {
        throw std::runtime_error {"record initializer"};
    }

    return 17;
}
#define EVALUATED_FIELDS(Field) \
    Field(int, value, initial_value()) \
    Field(int, following, value + 1)
DATUM_RECORD(evaluated_record, EVALUATED_FIELDS);
#undef EVALUATED_FIELDS

static_assert(std::is_aggregate_v<default_record>);
static_assert(sizeof(default_record) == sizeof(ordinary_defaults));
static_assert(alignof(default_record) == alignof(ordinary_defaults));
static_assert(std::is_trivially_copyable_v<default_record> ==
              std::is_trivially_copyable_v<ordinary_defaults>);
static_assert(std::is_nothrow_default_constructible_v<default_record> ==
              std::is_nothrow_default_constructible_v<ordinary_defaults>);
static_assert(datum::owning_value<default_record>);
static_assert(definition::declared_record<pinned_default>);
static_assert(std::default_initializable<pinned_default>);
static_assert(!std::move_constructible<pinned_default>);
static_assert(!std::is_nothrow_default_constructible_v<evaluated_record>);
constexpr default_record defaults {};
constexpr default_record overridden {.revision = 7};
static_assert(defaults.revision == 99 and defaults.choices == choices_type {3, 5} and
              defaults.following == 100 and overridden.following == 8);

static_assert(std::is_aggregate_v<defined_value>);
static_assert(sizeof(defined_value) == sizeof(ordinary_value));
static_assert(alignof(defined_value) == alignof(ordinary_value));
static_assert(std::is_trivially_copyable_v<defined_value> ==
              std::is_trivially_copyable_v<ordinary_value>);
static_assert(std::is_nothrow_move_constructible_v<defined_value> ==
              std::is_nothrow_move_constructible_v<ordinary_value>);
static_assert(definition::declared_record<defined_value>);
static_assert(definition::declared_record<const defined_value>);
static_assert(definition::declared_record<volatile defined_value>);
static_assert(!definition::declared_record<defined_value&>);
static_assert(!definition::declared_record<external_record>);
static_assert(!definition::declared_record<inherited_definition>);
static_assert(datum::owning_value<value_record>);
static_assert(datum::owning_value<defined_value>);
static_assert(datum::owning_value<owned_record>);
static_assert(datum::owning_value<empty_record>);
static_assert(!datum::owning_value<borrowed_record>);
static_assert(!datum::owning_value<reference_record>);
static_assert(!datum::owning_value<inherited_definition>);
static_assert(!datum::owning_value<immovable_record>);
static_assert(!std::default_initializable<immovable_record>);
static_assert(!std::move_constructible<immovable_record>);
static_assert(definition::declared_record<immovable_record>);
#if !defined(__cpp_lib_reflection)
 struct incomplete_record;
 static_assert(!datum::owning_value<incomplete_record>);
 static_assert(!datum::owning_value<external_record>);
#endif

using value_fields = definition::record_field_tuple<defined_value>;
using first_value = std::tuple_element_t<0, value_fields>;
static_assert(std::is_empty_v<first_value>);
using second_value = std::tuple_element_t<1, value_fields>;
static_assert(2 == std::tuple_size_v<value_fields>);
static_assert(std::same_as<first_value::value_type, std::string>);
static_assert(std::same_as<second_value::value_type, unsigned>);
static_assert(first_value::member() == &defined_value::label);
static_assert(second_value::member() == &defined_value::revision);
static_assert(std::tuple_element_t<0, definition::record_field_tuple<defined_value>>::name() == "label");
static_assert(std::tuple_element_t<1, definition::record_field_tuple<defined_value>>::name() == "revision");
static_assert(std::same_as<decltype(first_value::get(std::declval<const defined_value&>())), const std::string&>);
static_assert(noexcept(first_value::get(std::declval<const defined_value&>())));
using reference_fields = definition::record_field_tuple<reference_record>;
using first_reference = std::tuple_element_t<0, reference_fields>;
using second_reference = std::tuple_element_t<1, reference_fields>;
static_assert(std::same_as<first_reference::value_type, int&>);
static_assert(std::same_as<second_reference::value_type, const unsigned>);
static_assert(std::same_as<decltype(first_reference::get(std::declval<const reference_record&>())), int&>);
template <typename FieldT>
concept has_member_pointer = requires { FieldT::member(); };
static_assert(!has_member_pointer<first_reference>);
static_assert(has_member_pointer<second_reference>);
static_assert(0 == std::tuple_size_v<definition::record_field_tuple<empty_record>>);

#if defined(__cpp_lib_reflection)
template <typename RecordT, std::size_t Index>
consteval bool native_field_matches()
{
    using field = std::tuple_element_t<Index, definition::record_field_tuple<RecordT>>;
    using value_type = typename field::value_type;
    constexpr auto actual = datum::detail::record_members<RecordT>[Index];
    using actual_type = [:std::meta::type_of(actual):];
    static_assert(std::same_as<value_type, actual_type>, "Declared field type must match reflection");
    static_assert(std::meta::identifier_of(actual) ==
                  std::tuple_element_t<Index, definition::record_field_tuple<RecordT>>::name(),
                  "Declared field name must match reflection");
    if constexpr (!std::is_reference_v<value_type>) {
        static_assert(std::meta::reflect_constant(field::member()) ==
                      std::meta::reflect_constant(&[:actual:]),
                      "Declared member identity must match reflection");
    }
    return true;
}

template <typename RecordT>
consteval bool native_definition_matches()
{
    if (!std::empty(std::meta::bases_of(^^RecordT, std::meta::access_context::unchecked()))) {
        return false;
    }

    constexpr auto count = std::tuple_size_v<definition::record_field_tuple<RecordT>>;
    if (count != std::size(datum::detail::record_members<RecordT>)) {
        return false;
    }

    return []<std::size_t ...Index>(std::index_sequence<Index...>) {
        return (native_field_matches<RecordT, Index>() and ...);
    }(std::make_index_sequence<count> {});
}
static_assert(native_definition_matches<defined_value>());
static_assert(native_definition_matches<owned_record>());
static_assert(native_definition_matches<borrowed_record>());
static_assert(native_definition_matches<reference_record>());
static_assert(native_definition_matches<collision_record>());
static_assert(native_definition_matches<empty_record>());
static_assert(native_definition_matches<immovable_record>());
static_assert(native_definition_matches<default_record>());
static_assert(native_definition_matches<pinned_default>());
static_assert(native_definition_matches<evaluated_record>());
#endif

} // anonymous namespace

#include <catch2/catch_test_macros.hpp>

TEST_CASE("declared record facts preserve observation and payload movement", "[datum][definition]")
{
    defined_value source {std::string(128, 'x'), 7};
    CHECK(std::addressof(first_value::get(source)) == std::addressof(source.label));
    CHECK(std::addressof(second_value::get(source)) == std::addressof(source.revision));
    const auto *storage = std::data(source.label);
    owned_record owner {std::make_unique<int>(3), std::move(source)};
    CHECK(std::data(owner.value.label) == storage);
    CHECK(7 == owner.value.revision);
    CHECK(3 == *owner.payload);
    auto moved = std::move(owner);
    CHECK(std::data(moved.value.label) == storage);
    CHECK(3 == *moved.payload);

    int observed = 4;
    const reference_record borrowed {observed, 9};
    CHECK(std::addressof(first_reference::get(borrowed)) == std::addressof(observed));
    observed = 5;
    CHECK(5 == first_reference::get(borrowed));
}

TEST_CASE("declaration facts work for local records without constructing a witness",
          "[datum][definition]")
{
#define LOCAL_FIELDS(Field) \
    Field(int, project) \
    Field(std::unique_ptr<int>, record_type)
    DATUM_LOCAL_RECORD(local_record, LOCAL_FIELDS);
#undef LOCAL_FIELDS
    static_assert(definition::declared_record<local_record>);
    static_assert(datum::owning_value<local_record>);
    local_record source {8, std::make_unique<int>(13)};
    using fields = definition::record_field_tuple<local_record>;
    using first = std::tuple_element_t<0, fields>;
    using second = std::tuple_element_t<1, fields>;
    CHECK(std::tuple_element_t<0, definition::record_field_tuple<local_record>>::name() == "project");
    CHECK(std::tuple_element_t<1, definition::record_field_tuple<local_record>>::name() == "record_type");
    CHECK(std::addressof(first::get(source)) == std::addressof(source.project));
    CHECK(std::addressof(second::get(source)) == std::addressof(source.record_type));
    CHECK(13 == *second::get(source));
}

TEST_CASE("record initializers run only for omitted aggregate members", "[datum][definition]")
{
    initializer_calls = 0;
    fail_initializer = false;
    static_assert(2 == definition::record_size<evaluated_record>);
    static_assert(std::same_as<definition::record_field<evaluated_record, 0>::value_type, int>);
    CHECK(0 == initializer_calls);
    const evaluated_record value {};
    CHECK(17 == value.value);
    CHECK(18 == value.following);
    CHECK(1 == initializer_calls);
    const evaluated_record changed {.value = 41};
    CHECK(42 == changed.following);
    CHECK(1 == initializer_calls);
    const pinned_default pinned {};
    static_cast<void>(pinned);
    fail_initializer = true;
    CHECK_THROWS_AS(evaluated_record {}, std::runtime_error);
    CHECK(2 == initializer_calls);
    fail_initializer = false;
}
