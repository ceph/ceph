// Binary schema description is separate from codec enrollment and wire policy.
#include <datum/record.h>
#include <datum/record_definition.h>

#include <concepts>
#include <string>
#include <type_traits>
#include <vector>

namespace datum = ::ceph::salad::datum;
namespace schema_test {

#define ROW_FIELDS(Field) Field(int, id) Field(std::string, label)
#define REFERENCE_FIELDS(Field) Field(int, id) Field(int&, reference)
#define EMPTY_FIELDS(Field)
#if defined(__cpp_lib_reflection)
struct row { int id; std::string label; };
struct reference_row { int id; int& reference; };
struct empty_row {};
#else
DATUM_RECORD(row, ROW_FIELDS);
DATUM_RECORD(reference_row, REFERENCE_FIELDS);
DATUM_RECORD(empty_row, EMPTY_FIELDS);
#endif
#undef ROW_FIELDS
#undef REFERENCE_FIELDS
#undef EMPTY_FIELDS

using natural = datum::binary_schema_t<row>;
using explicit_natural = datum::binary_schema_t<row, &row::id, &row::label>;
using reversed = datum::binary_schema_t<row, &row::label, &row::id>;
using subset = datum::binary_schema_t<row, &row::id>;
using qualified = datum::binary_schema_t<const row>;
using qualified_selected = datum::binary_schema_t<const row, &row::label>;
using empty = datum::binary_schema_t<empty_row>;
using qualified_empty = datum::binary_schema_t<const empty_row>;
using reference_subset = datum::binary_schema_t<reference_row, &reference_row::id>;
using checked = datum::checked_binary_schema<row, &row::label, &row::id>;
using checked_empty = datum::checked_binary_schema<empty_row>;
static_assert(std::same_as<natural, explicit_natural>);
static_assert(std::same_as<natural, datum::binary_fields<&row::id, &row::label>>);
static_assert(std::same_as<reversed, datum::binary_fields<&row::label, &row::id>>);
static_assert(std::same_as<typename qualified::record_type, const row>);
static_assert(std::same_as<typename qualified::fields, typename natural::fields>);
static_assert(std::same_as<typename qualified_selected::record_type, const row>);
static_assert(std::same_as<typename qualified_empty::record_type, const empty_row>);
static_assert(!std::same_as<qualified, natural>);
static_assert(datum::binary_schema_type<natural> and datum::binary_schema_type<reversed>);
static_assert(datum::binary_schema_type<subset> and datum::binary_schema_type<qualified>);
static_assert(datum::binary_schema_type<qualified_selected>);
static_assert(datum::binary_schema_type<empty> and datum::binary_schema_type<qualified_empty>);
static_assert(datum::binary_schema_type<checked> and datum::binary_schema_type<checked_empty>);
static_assert(datum::binary_schema_type<reference_subset>);
static_assert(!datum::binary_schema_type<datum::binary_fields<>>);
static_assert(!datum::binary_schema_type<int>);
static_assert(natural::fields::covers<row>() and reversed::fields::covers<row>());
static_assert(!subset::fields::covers<row>());
static_assert(reference_subset::fields::selects<reference_row>());
static_assert(!reference_subset::fields::covers<reference_row>());
static_assert(datum::valid_binary_fields<row>(datum::binary_fields<&row::id, &row::label> {}));
static_assert(!datum::valid_binary_fields<row>(datum::binary_fields<&row::id> {}));
static_assert(datum::serialization_detail::valid_member_count<row, 2>());
static_assert(datum::serialization_detail::valid_member_count<const row, 2>());
static_assert(!datum::serialization_detail::valid_member_count<row, 1>());
static_assert(datum::serialization_detail::valid_member_count<empty_row, 0>());

struct reader
{
    std::vector<int>& order;
    template <typename LensT>
    auto operator()(LensT) -> typename LensT::value_type
    {
        if constexpr (std::same_as<LensT, datum::member_lens<&row::id>>) {
            order.push_back(0);
            return 17;
        }

        if constexpr (!std::same_as<LensT, datum::member_lens<&row::id>>) {
            order.push_back(1);
            return "label";
        }
    }
};

} // schema_test

#include <catch2/catch_test_macros.hpp>

TEST_CASE("binary schema descriptors retain explicit wire order and record identity", "[datum][schema]")
{
    using namespace schema_test;
    const row value {17, "label"};
    std::vector<int> order;
    natural::fields::visit([&order](const auto lens) {
        if constexpr (std::same_as<std::remove_cv_t<decltype(lens)>, datum::member_lens<&row::id>>) {
            order.push_back(0);
        }

        if constexpr (!std::same_as<std::remove_cv_t<decltype(lens)>, datum::member_lens<&row::id>>) {
            order.push_back(1);
        }
    });
    CHECK(order == std::vector<int> {0, 1});
    order.clear();
    reversed::fields::observe(value, [&order](const auto& field) {
        if constexpr (std::same_as<std::remove_cvref_t<decltype(field)>, int>) {
            CHECK(17 == field);
            order.push_back(0);
        }

        if constexpr (!std::same_as<std::remove_cvref_t<decltype(field)>, int>) {
            CHECK("label" == field);
            order.push_back(1);
        }
    });
    CHECK(order == std::vector<int> {1, 0});
}

TEST_CASE("checked schema reconstructs declaration order after approved wire reads", "[datum][schema]")
{
    using namespace schema_test;
    std::vector<int> order;
    reader input {order};
    const auto value = checked::fields::reconstruct<row>(input);
    CHECK(order == std::vector<int> {1, 0});
    CHECK(17 == value.id);
    CHECK("label" == value.label);
    order.clear();
    const auto qualified_value = qualified::fields::reconstruct<const row>(input);
    CHECK(order == std::vector<int> {0, 1});
    CHECK(17 == qualified_value.id);
}
