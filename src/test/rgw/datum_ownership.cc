// Datum: Ownership contracts and owner-preserving projections in both language modes.

#include <datum/ownership.h>
#include <datum/bytes.h>

#include <memory_resource>
#include <span>
#include <stdexcept>
#include <string_view>

namespace datum = ::ceph::salad::datum;

namespace ownership_test {

struct owned_record
{
    std::string text;
    int number;
};
struct borrowed_record
{
    std::string_view text;
};
struct reference_record
{
    int& number;
};
struct empty_record
{};
struct base
{};
struct derived_record : base
{
    int number;
};
union active_value
{
    int number;
    double fraction;
};
class audited_owner final
{
    std::string text;

public:
    explicit audited_owner(std::string value) : text(std::move(value))
    {}

public:
    const std::string& value() const noexcept { return text; }

};

class counted_owner final
{
    std::string text;

public:
    inline static unsigned copies = 0;
    inline static unsigned moves = 0;

public:
    explicit counted_owner(std::string value) : text(std::move(value))
    {}
    counted_owner(const counted_owner& source) : text(source.text) { ++copies; }
    counted_owner(counted_owner&& source) noexcept : text(std::move(source.text)) { ++moves; }

public:
    const std::string& value() const noexcept { return text; }

};

} // ownership_test

namespace ceph::salad::datum {

template <>
struct ownership_traits<ownership_test::audited_owner> : std::true_type
{};

template <>
struct ownership_traits<ownership_test::counted_owner> : std::true_type
{};

} // ceph::salad::datum

static_assert(datum::owning_value<int>);
static_assert(datum::owning_value<const int>);
static_assert(datum::owning_value<std::byte>);
static_assert(datum::owning_value<std::string>);
static_assert(datum::owning_value<std::vector<std::unique_ptr<int>>>);
static_assert(datum::owning_value<std::deque<int>>);
static_assert(datum::owning_value<std::list<int>>);
static_assert(datum::owning_value<std::set<int>>);
static_assert(datum::owning_value<std::map<std::string, std::vector<int>>>);
static_assert(datum::owning_value<std::pair<int, std::string>>);
static_assert(datum::owning_value<std::tuple<int, std::unique_ptr<std::string>>>);
static_assert(datum::owning_value<std::tuple<>>);
static_assert(datum::owning_value<std::unique_ptr<const int[]>>);
static_assert(datum::owning_value<int[2]>);
static_assert(datum::owning_value<std::array<std::string, 2>>);
static_assert(datum::owning_value<std::optional<std::string>>);
static_assert(datum::owning_value<std::monostate>);
static_assert(datum::owning_value<std::variant<std::monostate, std::string>>);
static_assert(datum::owning_value<std::expected<int, std::string>>);
static_assert(datum::owning_value<std::expected<void, std::string>>);
static_assert(datum::owning_value<std::chrono::milliseconds>);
static_assert(datum::owning_value<std::chrono::system_clock::time_point>);
static_assert(datum::owning_value<ownership_test::audited_owner>);
static_assert(!datum::owning_value<int&>);
static_assert(!datum::owning_value<int *>);
static_assert(!datum::owning_value<std::string_view>);
static_assert(!datum::owning_value<std::span<const int>>);
static_assert(!datum::owning_value<std::vector<std::string_view>>);
static_assert(!datum::owning_value<std::tuple<int&>>);
static_assert(!datum::owning_value<std::map<std::string_view, int>>);
static_assert(!datum::owning_value<std::variant<int, std::string_view>>);
static_assert(!datum::owning_value<std::expected<int, std::string_view>>);
static_assert(!datum::owning_value<std::expected<void, std::string_view>>);
static_assert(!datum::owning_value<std::pmr::vector<int>>);
static_assert(!datum::owning_value<std::set<int, std::greater<int>>>);
static_assert(!datum::owning_value<std::shared_ptr<std::string>>);
static_assert(!datum::owning_value<ownership_test::borrowed_record>);
static_assert(!datum::owning_value<ownership_test::reference_record>);
static_assert(!datum::owning_value<ownership_test::derived_record>);
static_assert(!datum::owning_value<ownership_test::active_value>);
#if defined(__cpp_lib_reflection)
static_assert(datum::owning_value<ownership_test::owned_record>);
static_assert(datum::owning_value<ownership_test::empty_record>);
#else
static_assert(!datum::owning_value<ownership_test::owned_record>);
static_assert(!datum::owning_value<ownership_test::empty_record>);
#endif
#if defined(__cpp_lib_inplace_vector)
static_assert(datum::owning_value<std::inplace_vector<int, 2>>);
static_assert(!datum::owning_value<std::inplace_vector<std::string_view, 2>>);
#endif

template <typename OwnerT, typename LensT = datum::byte_slice>
concept retained_owner = requires {
    datum::retained_view {std::declval<OwnerT>(), std::declval<LensT>()};
};
static_assert(retained_owner<std::string&&>);
static_assert(!retained_owner<std::string&> and !retained_owner<const std::string&>);
static_assert(!retained_owner<const std::string&&>);
static_assert(retained_owner<std::string&&, const datum::byte_slice&>);
static_assert(std::same_as<decltype(datum::retained_view {
                               std::declval<std::string&&>(),
                               std::declval<const datum::byte_slice&>()}),
                           datum::retained_view<std::string, datum::byte_slice>>);
static_assert(!std::constructible_from<datum::retained_view<const std::string, datum::byte_slice>,
                                       const std::string&&, datum::byte_slice>);
static_assert(std::copy_constructible<datum::retained_view<std::string, datum::byte_slice>>);
static_assert(std::constructible_from<datum::retained_view<std::string, datum::byte_slice>,
                                      const datum::retained_view<std::string, datum::byte_slice>&>);
static_assert([] {
    const std::vector<int> source {3, 7};
    const datum::retained_view copied {std::vector<int> {source},
        [](const std::vector<int>& values) { return std::span<const int> {values}; }};
    return copied.get()[1] == 7 and std::data(copied.get()) != std::data(source);
}());

template <typename T>
concept temporary_projection = requires(T value) { std::move(value).get(); };
static_assert(!temporary_projection<datum::retained_view<std::string, datum::byte_slice>>);
static_assert([] {
    auto lens = [](const std::string& text) -> const std::string& { return text; };
    datum::retained_view original {std::string {"constant owner"}, lens};
    auto moved = std::move(original);
    return *moved == "constant owner";
}());
static_assert([] {
    auto lens = [](const std::vector<int>& values) { return std::span<const int> {values}; };
    datum::retained_view original {std::vector {3, 7}, lens};
    auto moved = std::move(original);
    auto copy = moved;
    return moved.get()[0] == 3 and copy.get()[1] == 7 and
           std::data(moved.get()) != std::data(copy.get());
}());

#include <catch2/catch_test_macros.hpp>

TEST_CASE("retained projections recompute after small and large owner moves")
{
    for (const auto size : {8U, 256U}) {
        std::string bytes(size, 'x');
        bytes[2] = 'a';
        datum::retained_view original {std::move(bytes), datum::byte_slice {2, 3}};
        auto moved = std::move(original);
        CHECK((*moved)[0] == 'a');
        CHECK(std::size(*moved) == 3);
        auto copy = moved;
        CHECK(std::data(*copy) != std::data(*moved));
        CHECK((*copy)[0] == 'a');
    }
}

namespace ownership_test {

struct moving_lens final
{
    int& moves;

public:
    explicit moving_lens(int& count) noexcept : moves(count)
    {}
    moving_lens(const moving_lens&) = delete;
    moving_lens(moving_lens&& source) noexcept : moves(source.moves) { ++moves; }

public:
    const int& operator()(const std::vector<int>& values) const noexcept { return values.front(); }
    void operator()(const std::vector<int>&) = delete;
};
struct failing_lens final
{
    int& calls;

public:
    int operator()(const std::string&) const
    {
        ++calls;
        throw std::runtime_error {"projection failed"};
    }
};
struct failing_move final
{
    failing_move() = default;
    failing_move(const failing_move&) = delete;
    failing_move(failing_move&&) { throw std::runtime_error {"projection move failed"}; }

public:
    const std::string& operator()(const std::string& value) const noexcept { return value; }
};

} // ownership_test

TEST_CASE("retained views borrow observations and own move-only projections", "[datum][ownership]")
{
    int moves = 0;
    std::vector source {3, 7};
    const auto *address = std::data(source);
    datum::retained_view original {std::move(source), ownership_test::moving_lens {moves}};
    STATIC_REQUIRE_FALSE(std::copy_constructible<decltype(original)>);
    STATIC_REQUIRE(std::same_as<decltype(original.get()), const int&>);
    STATIC_REQUIRE(noexcept(original.get()));
    CHECK(moves == 1);
    CHECK(std::addressof(original.get()) == address);
    auto moved = std::move(original);
    CHECK(moves == 2);
    CHECK(std::addressof(*moved) == address);
    CHECK(moved.get() == 3);
    CHECK(moves == 2);
    datum::retained_view unique {std::make_unique<int>(9),
        [](const std::unique_ptr<int>& owner) -> const int& { return *owner; }};
    STATIC_REQUIRE_FALSE(std::copy_constructible<decltype(unique)>);
    CHECK(*unique == 9);
    datum::retained_view audited {ownership_test::audited_owner {"audited"},
        [](const ownership_test::audited_owner& owner) -> const std::string& { return owner.value(); }};
    CHECK(*audited == "audited");
}

TEST_CASE("retained projection failures preserve exception contracts", "[datum][ownership]")
{
    int calls = 0;
    datum::retained_view value {std::string {"still owned"}, ownership_test::failing_lens {calls}};
    STATIC_REQUIRE_FALSE(noexcept(value.get()));
    CHECK_THROWS_AS(value.get(), std::runtime_error);
    CHECK_THROWS_AS(value.get(), std::runtime_error);
    CHECK(calls == 2);
    using fallible_view = datum::retained_view<std::string, ownership_test::failing_move>;
    STATIC_REQUIRE_FALSE(std::is_nothrow_constructible_v<
        fallible_view, std::string&&, ownership_test::failing_move>);
    CHECK_THROWS_AS((fallible_view {std::string(256, 'x'), ownership_test::failing_move {}}), std::runtime_error);
}

TEST_CASE("retained owners make copies explicit and preserve const wrapper copies",
          "[datum][ownership]")
{
    using ownership_test::counted_owner;
    const counted_owner source {std::string(128, 'x')};
    const auto lens = [](const counted_owner& owner) -> const std::string& { return owner.value(); };
    counted_owner::copies = 0;
    counted_owner::moves = 0;
    datum::retained_view explicit_copy {counted_owner {source}, lens};
    CHECK(counted_owner::copies == 1);
    CHECK(counted_owner::moves == 1);
    CHECK(explicit_copy.get() == source.value());
    CHECK(explicit_copy.get().data() != source.value().data());
    const auto *address = explicit_copy.get().data();
    const auto moved = std::move(explicit_copy);
    CHECK(counted_owner::copies == 1);
    CHECK(counted_owner::moves == 2);
    CHECK(moved.get().data() == address);
    const auto copied = moved;
    CHECK(counted_owner::copies == 2);
    CHECK(counted_owner::moves == 2);
    CHECK(copied.get() == moved.get());
    CHECK(copied.get().data() != moved.get().data());
}
