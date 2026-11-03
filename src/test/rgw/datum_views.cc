// Named projections: native C++26 and the temporary declaration-backed C++23 path.
#include <datum/record.h>
#include <datum/record_definition.h>

#include <concepts>
#include <type_traits>
#include <utility>

#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

namespace datum = ::ceph::salad::datum;

namespace views_test {

struct pinned_leaf { int value; };
struct moving_leaf { int value; };
struct failing_leaf { int value; };
struct mismatched_leaf { int value; };

#define ROW_FIELDS(Field) \
    Field(int, field) \
    Field(std::string, project) \
    Field(int, record_type) \
    Field(int, source_value)
#define PINNED_FIELDS(Field) Field(pinned_leaf, pinned)
#define MOVING_FIELDS(Field) Field(moving_leaf, moving)
#define FAILING_FIELDS(Field) Field(failing_leaf, failing)
#define REFERENCE_FIELDS(Field) Field(int&, reference)
#define DEFAULT_REFERENCE_FIELDS(Field) Field(int, value, 3) Field(int&, reference, value)
#define MISMATCHED_FIELDS(Field) Field(mismatched_leaf, value)
#define NAMED_FIELDS(Field) Field(int, DatumRecord)
#define EMPTY_FIELDS(Field)

#if defined(__cpp_lib_reflection)
struct row
{
    int field;
    std::string project;
    int record_type;
    int source_value;
};
struct pinned_row { pinned_leaf pinned; };
struct moving_row { moving_leaf moving; };
struct failing_row { failing_leaf failing; };
struct reference_row { int& reference; };
struct default_reference_row { int value = 3; int& reference = value; };
struct mismatched_row { mismatched_leaf value; };
struct empty_row {};
struct named_row { int DatumRecord; };
#else
DATUM_RECORD(row, ROW_FIELDS);
DATUM_RECORD(pinned_row, PINNED_FIELDS);
DATUM_RECORD(moving_row, MOVING_FIELDS);
DATUM_RECORD(failing_row, FAILING_FIELDS);
DATUM_RECORD(reference_row, REFERENCE_FIELDS);
DATUM_RECORD(default_reference_row, DEFAULT_REFERENCE_FIELDS);
DATUM_RECORD(mismatched_row, MISMATCHED_FIELDS);
DATUM_RECORD(empty_row, EMPTY_FIELDS);
DATUM_RECORD(named_row, NAMED_FIELDS);
#endif
#undef ROW_FIELDS
#undef PINNED_FIELDS
#undef MOVING_FIELDS
#undef FAILING_FIELDS
#undef REFERENCE_FIELDS
#undef DEFAULT_REFERENCE_FIELDS
#undef MISMATCHED_FIELDS
#undef EMPTY_FIELDS
#undef NAMED_FIELDS

struct pinned_projection
{
    int value;

    explicit pinned_projection(int initial) : value(initial)
    {}
    pinned_projection(const pinned_projection&) = delete;
    pinned_projection(pinned_projection&&) = delete;
};

struct moving_projection
{
    int value;

    explicit moving_projection(int initial) : value(initial)
    {}
    moving_projection(const moving_projection&) = delete;
    moving_projection(moving_projection&&) { throw std::runtime_error {"projection move"}; }
};

inline unsigned pinned_reads = 0;
inline unsigned moving_reads = 0;
inline unsigned failing_reads = 0;

class constructor_record
{
    [[maybe_unused]] std::string_view hidden;

public:
    int value;
    const int revision;
    mutable int cache;
    int coordinates[2];
    mismatched_leaf leaf;

public:
    constructor_record() = delete;
    constexpr explicit constructor_record(const int initial)
        : hidden("private"), value(initial), revision(initial + 1), cache(0),
          coordinates {initial, initial + 1}, leaf {initial}
    {}
    constructor_record(const constructor_record&) = delete;
    constructor_record(constructor_record&&) = delete;

public:
    constexpr int method() const noexcept { return value; }

};

struct derived_constructor_record : constructor_record
{
    using constructor_record::constructor_record;
};
struct unrelated_record { int value; };
struct incomplete_record;

} // views_test

namespace ceph::salad::datum {

// These policies intentionally follow the source declarations: generation must be lazy.
template <>
struct member_view<std::string>
{
    using type = std::string_view;
    static constexpr type get(const std::string& value) noexcept { return value; }
};

template <>
struct member_view<views_test::pinned_leaf>
{
    using type = views_test::pinned_projection;
    static type get(const views_test::pinned_leaf& value) noexcept
    {
        ++views_test::pinned_reads;
        return type {value.value};
    }
};

template <>
struct member_view<views_test::moving_leaf>
{
    using type = views_test::moving_projection;
    static type get(const views_test::moving_leaf& value) noexcept
    {
        ++views_test::moving_reads;
        return type {value.value};
    }
};

template <>
struct member_view<views_test::failing_leaf>
{
    using type = int;
    static type get(const views_test::failing_leaf&)
    {
        ++views_test::failing_reads;
        throw std::runtime_error {"projection read"};
    }
};

template <>
struct member_view<views_test::mismatched_leaf>
{
    using type = int;
    static long get(const views_test::mismatched_leaf& value) noexcept { return value.value; }
};

} // ceph::salad::datum

namespace views_test {

using view = datum::view_of<row>;

// Deduction preserves a projection's identity when it is copied or moved.
static_assert(std::same_as<decltype(datum::view_of {std::declval<const row&>()}), view>);
static_assert(std::same_as<decltype(datum::view_of {std::declval<const view&>()}), view>);
static_assert(std::same_as<decltype(datum::view_of {std::declval<view&&>()}), view>);
template <typename SourceT>
concept deduced_view = requires { datum::view_of {std::declval<SourceT>()}; };
static_assert(!deduced_view<row&&> and !deduced_view<const row&&>);

using field_lens = datum::member_lens<&row::field>;
using project_lens = datum::member_lens<&row::project>;
using order = datum::field_order<field_lens {}, project_lens {}>;
using constructor_lens = datum::member_lens<&constructor_record::value>;
using coordinates_lens = datum::member_lens<&constructor_record::coordinates>;
using leaf_lens = datum::member_lens<&constructor_record::leaf>;
using constructor_fields = datum::member_fields<&constructor_record::value,
    &constructor_record::coordinates, &constructor_record::leaf>;
using null_lens = datum::member_lens<static_cast<int constructor_record::*>(nullptr)>;
using method_lens = datum::member_lens<&constructor_record::method>;
static_assert(!std::default_initializable<constructor_record>);
static_assert(!std::copy_constructible<constructor_record>);
static_assert(!std::move_constructible<constructor_record>);
static_assert(!datum::owning_value<constructor_record>);
static_assert(!constructor_fields::covers<constructor_record>());
#if !defined(__cpp_lib_reflection)
static_assert(!datum::compat::detail::declared_record<constructor_record>);
static_assert(!constructor_fields::selects<constructor_record>());
static_assert(!datum::binary_schema_type<datum::binary_fields<&constructor_record::value>>);
#endif
static_assert(std::invocable<constructor_lens, constructor_record&>);
static_assert(std::invocable<constructor_lens, const constructor_record&>);
static_assert(!std::invocable<constructor_lens, constructor_record&&>);
static_assert(!std::invocable<constructor_lens, const constructor_record&&>);
static_assert(!std::invocable<constructor_lens, unrelated_record&>);
static_assert(!std::invocable<constructor_lens, derived_constructor_record&>);
static_assert(!std::invocable<constructor_lens, const incomplete_record&>);
static_assert(!std::invocable<null_lens, const constructor_record&>);
static_assert(!std::invocable<null_lens, constructor_record&&>);
static_assert(!std::invocable<method_lens, const constructor_record&>);
static_assert(std::same_as<std::invoke_result_t<constructor_lens, const constructor_record&>, const int&>);
static_assert(std::same_as<std::invoke_result_t<coordinates_lens, const constructor_record&>, const int (&)[2]>);
static_assert(std::same_as<std::invoke_result_t<leaf_lens, const constructor_record&>, const mismatched_leaf&>);
static_assert(std::same_as<std::invoke_result_t<
    datum::member_lens<&constructor_record::revision>, const constructor_record&>, const int&>);
static_assert(std::same_as<std::invoke_result_t<
    datum::member_lens<&constructor_record::cache>, const constructor_record&>, const int&>);
static_assert(noexcept(constructor_lens {}(std::declval<const constructor_record&>())));
static_assert([] {
    const constructor_record source {17};
    return 17 == constructor_lens {}(source) and
           std::addressof(constructor_lens {}(source)) == std::addressof(source.value) and
           std::addressof(coordinates_lens {}(source)) == std::addressof(source.coordinates) and
           std::addressof(leaf_lens {}(source)) == std::addressof(source.leaf);
}());

using source_only_lens = datum::member_lens<&mismatched_row::value>;
static_assert(std::invocable<source_only_lens, const mismatched_row&>);
static_assert(std::same_as<decltype(source_only_lens {}(std::declval<const mismatched_row&>())),
                           const mismatched_leaf&>);
static_assert(std::constructible_from<view, const row&>);
static_assert(std::constructible_from<view, row&>);
static_assert(!std::constructible_from<view, row&&>);
static_assert(!std::constructible_from<view, const row&&>);
static_assert(std::same_as<decltype(std::declval<view&>().project), std::string_view>);
static_assert(std::same_as<decltype(project_lens {}(std::declval<const view&>())), const std::string_view&>);
static_assert(std::same_as<decltype(project_lens {}(std::declval<const row&>())), const std::string&>);
static_assert(!std::invocable<project_lens, row&&>);
static_assert(!std::invocable<project_lens, view&&>);
static_assert(!std::invocable<project_lens, int&>);
static_assert(std::is_nothrow_constructible_v<datum::view_of<pinned_row>, const pinned_row&>);
static_assert(!std::move_constructible<datum::view_of<pinned_row>>);
static_assert(std::is_nothrow_constructible_v<datum::view_of<moving_row>, const moving_row&>);
static_assert(!std::is_nothrow_move_constructible_v<datum::view_of<moving_row>>);
static_assert(!std::is_nothrow_constructible_v<datum::view_of<failing_row>, const failing_row&>);
static_assert(std::is_empty_v<datum::view_of<empty_row>>);
static_assert(std::is_nothrow_constructible_v<datum::view_of<empty_row>, const empty_row&>);
static_assert([] {
    int value = 3;
    const reference_row source {value};
    datum::view_of projection {source};
    return std::addressof(projection.reference) == std::addressof(value);
}());

static_assert([] {
    const named_row source {13};
    const datum::view_of observed {source};
    return 13 == observed.DatumRecord;
}());

constexpr default_reference_row defaults {};
constexpr datum::view_of default_view {defaults};
static_assert(3 == default_view.value and
              std::addressof(default_view.value) == std::addressof(defaults.value) and
              std::addressof(default_view.reference) == std::addressof(defaults.value));

// Function-local records cannot contain member templates; both paths keep named access.
static_assert([] {
#define LOCAL_FIELDS(Field) \
    Field(int, value, 3) \
    Field(int&, reference)
#if defined(__cpp_lib_reflection)
    struct local_row { int value = 3; int& reference; };
#else
    DATUM_LOCAL_RECORD(local_row, LOCAL_FIELDS);
#endif
#undef LOCAL_FIELDS
    int referred = 5;
    const local_row source {3, referred};
    const datum::view_of observed {source};
    const local_row omitted {.reference = referred};
    const datum::view_of omitted_view {omitted};
    using lens = datum::member_lens<&local_row::value>;
    return 3 == observed.value and
           std::addressof(observed.reference) == std::addressof(referred) and
           std::addressof(lens {}(observed)) == std::addressof(source.value) and
           3 == omitted_view.value and
           std::addressof(omitted_view.value) == std::addressof(omitted.value) and
           std::addressof(omitted_view.reference) == std::addressof(referred);
}());

struct derived_view : view
{
    using view::view;
    int field = 99;
    std::string_view project = "hidden";
};

} // views_test

#include <catch2/catch_test_macros.hpp>

TEST_CASE("named views preserve names, stored projections and derived association", "[datum][view]")
{
    using namespace views_test;
    const row source {7, std::string(128, 'x'), 8, 9};
    const view observed {source};
    CHECK(7 == observed.field);
    CHECK(8 == observed.record_type);
    CHECK(9 == observed.source_value);
    CHECK(std::data(observed.project) == std::data(source.project));
    CHECK(std::addressof(project_lens {}(observed)) == std::addressof(observed.project));
    CHECK(std::addressof(field_lens {}(observed)) == std::addressof(source.field));
    const derived_view derived {source};
    CHECK(99 == derived.field);
    CHECK("hidden" == derived.project);
    CHECK(std::addressof(field_lens {}(derived)) == std::addressof(source.field));
    CHECK(std::data(project_lens {}(derived)) == std::data(source.project));
    int value = 4;
    const reference_row borrowed {value};
    const datum::view_of reference {borrowed};
    value = 5;
    CHECK(5 == reference.reference);
}

TEST_CASE("named projections initialize final immovable values and preserve exceptions",
          "[datum][view]")
{
    using namespace views_test;
    pinned_reads = 0;
    const pinned_row source {{17}};
    const datum::view_of pinned {source};
    CHECK(17 == pinned.pinned.value);
    CHECK(1 == pinned_reads);
    moving_reads = 0;
    const moving_row movable {{23}};
    datum::view_of moving {movable};
    CHECK(23 == moving.moving.value);
    CHECK(1 == moving_reads);
    CHECK_THROWS_AS(decltype(moving) {std::move(moving)}, std::runtime_error);
    failing_reads = 0;
    const failing_row failing {{29}};
    CHECK_THROWS_AS(datum::view_of {failing}, std::runtime_error);
    CHECK(1 == failing_reads);
}

TEST_CASE("field observation borrows mutable move-only sinks and retains order", "[datum][view]")
{
    using namespace views_test;
    const row source {7, "payload", 8, 9};
    unsigned observations = 0;
    auto sink = [token = std::make_unique<int>(1), &observations](const auto& value) mutable {
        CHECK(token);
        if constexpr (std::same_as<std::remove_cvref_t<decltype(value)>, int>) {
            CHECK(0 == observations);
            CHECK(7 == value);
        }

        if constexpr (!std::same_as<std::remove_cvref_t<decltype(value)>, int>) {
            CHECK(1 == observations);
            CHECK("payload" == value);
        }
        ++observations;
    };
    order::observe(source, sink);
    CHECK(2 == observations);
}

TEST_CASE("source lenses observe constructor-bearing records without generated facts",
          "[datum][view][fields]")
{
    using namespace views_test;
    const constructor_record source {41};
    CHECK(41 == constructor_lens {}(source));
    CHECK(std::addressof(constructor_lens {}(source)) == std::addressof(source.value));
    CHECK(std::addressof(coordinates_lens {}(source)) == std::addressof(source.coordinates));
    CHECK(42 == coordinates_lens {}(source)[1]);
    CHECK(std::addressof(leaf_lens {}(source)) == std::addressof(source.leaf));
    CHECK(42 == datum::member_lens<&constructor_record::revision> {}(source));
    CHECK(0 == datum::member_lens<&constructor_record::cache> {}(source));
}

TEST_CASE("source field observations borrow immovable callbacks and preserve failure order",
          "[datum][view][fields]")
{
    using namespace views_test;
    const constructor_record source {41};
    struct receiver
    {
        const constructor_record& source;
        unsigned calls = 0;
        bool correct_addresses = true;
        bool fail = false;

    public:
        explicit receiver(const constructor_record& value) : source(value)
        {}
        receiver(const receiver&) = delete;
        receiver(receiver&&) = delete;

    public:
        void operator()(const int& value) &
        {
            correct_addresses &= &value == &source.value and calls == 0;
            ++calls;
        }

        void operator()(const int (&value)[2]) &
        {
            correct_addresses &= &value == &source.coordinates and calls == 1;
            ++calls;
            if (fail) {
                throw std::runtime_error {"source observation"};
            }
        }

        void operator()(const mismatched_leaf& value) &
        {
            correct_addresses &= &value == &source.leaf and calls == 2;
            ++calls;
        }

    };
    receiver observed {source};
    constructor_fields::observe(source, observed);
    CHECK(3 == observed.calls);
    CHECK(observed.correct_addresses);
    observed.calls = 0;
    constructor_fields::observe(source, std::move(observed));
    CHECK(3 == observed.calls);
    CHECK(observed.correct_addresses);
    observed.calls = 0;
    observed.fail = true;
    CHECK_THROWS_AS(constructor_fields::observe(source, observed), std::runtime_error);
    CHECK(2 == observed.calls);
    CHECK(observed.correct_addresses);
}
