// Selected construction uses native C++26 or the temporary declaration-backed path.
#include <datum/record.h>
#include <datum/record_definition.h>

#include <concepts>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace datum = ::ceph::salad::datum;

namespace fields_test {

struct pinned
{
    int value;

    constexpr explicit pinned(int initial) : value(initial)
    {}
    pinned(const pinned&) = delete;
    pinned(pinned&&) = delete;
};

struct counted
{
    inline static int alive = 0;
    counted() { ++alive; }
    ~counted() { --alive; }
};

using payload_type = std::unique_ptr<counted>;
#define ROW_FIELDS(Field) \
    Field(int, id) \
    Field(std::string, name) \
    Field(payload_type, payload)
#define PINNED_FIELDS(Field) Field(pinned, value)
#define TEXT_FIELDS(Field) Field(std::string, name)
#define REFERENCE_FIELDS(Field) Field(int&, value)
#define EMPTY_FIELDS(Field)
#if defined(__cpp_lib_reflection)
struct row { int id; std::string name; payload_type payload; };
struct pinned_row { pinned value; };
struct text { std::string name; };
struct reference_row { int& value; };
struct empty_row {};
#else
DATUM_RECORD(row, ROW_FIELDS);
DATUM_RECORD(pinned_row, PINNED_FIELDS);
DATUM_RECORD(text, TEXT_FIELDS);
DATUM_RECORD(reference_row, REFERENCE_FIELDS);
DATUM_RECORD(empty_row, EMPTY_FIELDS);
#endif
#undef ROW_FIELDS
#undef PINNED_FIELDS
#undef TEXT_FIELDS
#undef REFERENCE_FIELDS
#undef EMPTY_FIELDS

struct foreign { int extra; };
struct incomplete;

} // fields_test

namespace ceph::salad::datum {

template <>
struct ownership_traits<fields_test::pinned> : std::true_type
{};

// This opaque test leaf owns no borrowed state; its only side effect tracks destruction.
template <>
struct ownership_traits<fields_test::counted> : std::true_type
{};

} // ceph::salad::datum

namespace fields_test {

static_assert(datum::owning_value<row>);

using natural = datum::member_fields<&row::id, &row::name, &row::payload>;
using wire = datum::member_fields<&row::payload, &row::id, &row::name>;
using subset = datum::member_fields<&row::name>;
using repeated = datum::member_fields<&row::id, &row::id>;
using mixed = datum::member_fields<&row::id, &foreign::extra>;
using empty = datum::member_fields<>;
using pinned_fields = datum::member_fields<&pinned_row::value>;
static_assert(natural::selects<row>() and natural::covers<row>());
static_assert(wire::selects<row>() and wire::covers<row>());
static_assert(wire::selects<const row>() and wire::covers<const row>());
static_assert(wire::selects<volatile row>() and wire::covers<volatile row>());
static_assert(!wire::selects<row&>() and !wire::covers<row&>());
static_assert(subset::selects<row>() and !subset::covers<row>());
static_assert(!repeated::selects<row>() and !repeated::covers<row>());
static_assert(!mixed::selects<row>());
static_assert(empty::selects<row>() and !empty::covers<row>());
static_assert(empty::selects<empty_row>() and empty::covers<empty_row>());
static_assert(!empty::covers<reference_row>());
static_assert(!subset::covers<int>());
#if !defined(__cpp_lib_reflection)
static_assert(!empty::selects<foreign>() and !empty::covers<foreign>());
static_assert(!empty::selects<incomplete>() and !empty::covers<incomplete>());
#endif
static_assert([] {
    auto reader = [](auto) constexpr { return pinned {17}; };
    const auto value = pinned_fields::reconstruct<pinned_row>(reader);
    const auto qualified = pinned_fields::reconstruct<const pinned_row>(reader);
    return 17 == value.value.value and 17 == qualified.value.value;
}());
static_assert(noexcept(subset::project<text>(std::declval<row&&>())));
static_assert(!noexcept(subset::project<text>(std::declval<const row&>())));

template <typename ReaderT>
concept can_reconstruct = requires(ReaderT& reader) { natural::reconstruct<row>(reader); };
struct wrong_reader { long operator()(auto) { return 1; } };
struct borrowed_reader
{
    template <typename LensT>
    typename LensT::value_type& operator()(LensT);
};
static_assert(!can_reconstruct<wrong_reader> and !can_reconstruct<borrowed_reader>);

struct reader
{
    std::vector<int>& order;
    std::string name;
    payload_type payload;
    int fail_at = -1;

    template <typename LensT>
    auto operator()(LensT) & -> typename LensT::value_type
    {
        constexpr int index = std::same_as<LensT, datum::member_lens<&row::id>> ? 0 :
            std::same_as<LensT, datum::member_lens<&row::name>> ? 1 : 2;
        order.push_back(index);
        if (fail_at == index) {
            throw std::runtime_error {"reader"};
        }
        if constexpr (0 == index) {
            return 7;
        }

        if constexpr (1 == index) {
            return std::move(name);
        }

        if constexpr (2 == index) {
            return std::move(payload);
        }
    }
};
static_assert(!std::copy_constructible<reader>);
static_assert(can_reconstruct<reader>);

struct factory
{
    std::unique_ptr<int> token;
    unsigned& calls;
    const std::vector<int>& order;
    bool fail = false;

    row operator()(payload_type&& payload, int&& id, std::string&& name) &&
    {
        if (!token || order != std::vector<int> {2, 0, 1}) {
            throw std::logic_error {"factory order or lifetime"};
        }
        ++calls;
        if (fail) {
            throw std::runtime_error {"factory"};
        }
        return row {id, std::move(name), std::move(payload)};
    }
};
static_assert(!std::copy_constructible<factory>);

} // fields_test

#include <catch2/catch_test_macros.hpp>

TEST_CASE("selected reconstruction reads wire order and preserves moved payload storage", "[datum][fields]")
{
    using namespace fields_test;
    std::vector<int> order;
    reader input {order, std::string(128, 'x'), std::make_unique<counted>()};
    const auto bytes = input.name.data();
    const auto payload = input.payload.get();
    const auto result = wire::reconstruct<row>(input);
    CHECK(order == std::vector<int> {2, 0, 1});
    CHECK(7 == result.id);
    CHECK(bytes == result.name.data());
    CHECK(payload == result.payload.get());
    CHECK_FALSE(input.payload);
    CHECK(1 == counted::alive);
}

TEST_CASE("selected factories borrow move-only readers and forward rvalue-only factories", "[datum][fields]")
{
    using namespace fields_test;
    CHECK(0 == counted::alive);
    std::vector<int> order;
    reader input {order, std::string(128, 'y'), std::make_unique<counted>()};
    const auto bytes = input.name.data();
    const auto payload = input.payload.get();
    unsigned calls = 0;
    factory finish {std::make_unique<int>(1), calls, order};
    const auto result = wire::construct(input, std::move(finish));
    CHECK(1 == calls);
    CHECK(order == std::vector<int> {2, 0, 1});
    CHECK(bytes == result.name.data());
    CHECK(payload == result.payload.get());
    CHECK(finish.token);
}

TEST_CASE("selected reader and factory exceptions release partially consumed owners", "[datum][fields]")
{
    using namespace fields_test;
    CHECK(0 == counted::alive);
    std::vector<int> order;
    unsigned calls = 0;
    {
        reader input {order, "name", std::make_unique<counted>(), 0};
        CHECK_THROWS_AS(wire::construct(input, factory {std::make_unique<int>(1), calls, order}), std::runtime_error);
        CHECK(order == std::vector<int> {2, 0});
        CHECK(0 == calls);
        CHECK_FALSE(input.payload);
        CHECK(0 == counted::alive);
    }
    order.clear();
    {
        reader input {order, "name", std::make_unique<counted>()};
        CHECK_THROWS_AS(wire::construct(input,
            factory {std::make_unique<int>(1), calls, order, true}), std::runtime_error);
        CHECK(1 == calls);
        CHECK_FALSE(input.payload);
        CHECK(0 == counted::alive);
    }
}

TEST_CASE("selected consuming projection preserves lvalue copies and rvalue moves", "[datum][fields]")
{
    using namespace fields_test;
    row source {7, std::string(128, 'z'), {}};
    const auto bytes = source.name.data();
    const auto copied = subset::project<text>(source);
    CHECK(source.name == copied.name);
    CHECK(bytes == source.name.data());
    CHECK(bytes != copied.name.data());
    const auto moved = subset::project<text>(std::move(source));
    CHECK(bytes == moved.name.data());
    CHECK(128 == moved.name.size());
}

TEST_CASE("empty selected construction invokes no reader and one factory", "[datum][fields]")
{
    using namespace fields_test;
    auto no_reader = [] {};
    const auto reconstructed = empty::reconstruct<empty_row>(no_reader);
    static_assert(std::same_as<decltype(reconstructed), const empty_row>);
    unsigned calls = 0;
    const auto result = empty::construct(no_reader, [&calls] { ++calls; return empty_row {}; });
    static_assert(std::same_as<decltype(result), const empty_row>);
    CHECK(1 == calls);
}
