// Datum reflection-independent bytes, budgets, clipping, metrics, keys and plans tests.
#include "common/container_concepts.h"
#include "datum_key_plan_case.h"
#include "datum_portable_support.h"
#include <catch2/catch_test_macros.hpp>
#include <datum/budget.h>
#include <datum/bytes.h>
#include <datum/interval/clip.h>
#include <datum/interval/metric.h>
#include <datum/key.h>
#include <datum/plan/query.h>
#include <datum/traversal.h>
#include <algorithm>
#include <array>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

namespace tuple_access_test {

class source final
{
    std::array<std::string_view, 2> fields {"space", "leaf"};

    template <std::size_t Index>
    constexpr const std::string_view& get() const { return fields[Index]; }
    template <std::size_t Index>
    const std::string_view& get() = delete;

    template <datum::key::tuple_segments TupleT>
    friend constexpr datum::key::encoded_key datum::key::compose(const TupleT&);

public:
    constexpr source() = default;
};

// Member lookup must win even when ADL also finds an accessor.
template <std::size_t Index>
constexpr std::string_view get(const source&) { return "trap"; }

// Unrelated namespace helpers must not intercept typed binding inside compose.
struct segment
{
    std::string_view value;
    constexpr operator std::string_view() const noexcept { return value; }
};

template <typename ReferenceT>
ReferenceT bind_segment(const segment&) = delete;

} // tuple_access_test

namespace std {

template <>
struct tuple_size<tuple_access_test::source> : integral_constant<size_t, 2>
{};

template <size_t Index>
struct tuple_element<Index, tuple_access_test::source>
{
    static_assert(Index < 2);
    using type = string_view;
};

} // std

namespace {

constexpr bool private_tuple_members()
{
    tuple_access_test::source input;
    const auto out = datum::key::compose(input);
    const auto explicit_reference = datum::key::compose<tuple_access_test::source&>(input);
    const std::string_view expected {"space\0leaf\0", 11};
    const std::tuple adl_input {tuple_access_test::segment {"space"},
                               tuple_access_test::segment {"leaf"}};
    const auto adl_result = datum::key::compose(adl_input);

    return datum::key::key_view(out) == expected and
           datum::key::key_view(explicit_reference) == expected and
           datum::key::key_view(adl_result) == expected;
}
static_assert(private_tuple_members());

struct size_fold final
{
    std::size_t calls = 0;
    std::size_t fail_at;

    constexpr explicit size_fold(const std::size_t failure) : fail_at(failure)
    {}
    size_fold(const size_fold&) = delete;
    size_fold(size_fold&&) = delete;

    constexpr std::size_t operator()(std::size_t total, const int value) &
    {
        if (++calls == fail_at) {
            throw std::runtime_error {"size fold"};
        }

        return total + value;
    }
};

struct non_size_fold
{
    long operator()(std::size_t, int) const;
};

template <typename FunctionT>
concept accepts_size_fold = requires(const std::array<int, 3>& source, FunctionT& function) {
    datum::compat::fold_size(source, std::size_t {0}, function);
};
static_assert(accepts_size_fold<size_fold> and !accepts_size_fold<non_size_fold>);

constexpr bool size_fold_contract()
{
    const std::array source {1, 2, 3};
    size_fold function {0};
    const auto total = datum::compat::fold_size(source, std::size_t {0}, function);
    const std::array<int, 0> empty {};
    const auto initial = datum::compat::fold_size(empty, std::size_t {7}, function);

    return 6 == total and 3 == function.calls and 7 == initial;
}
static_assert(size_fold_contract());

constexpr bool byte_spellings()
{
    const std::array<signed char, 3> source {-128, -1, 0};
    const std::array fragments {std::span {source}.first(1), std::span {source}.subspan(1)};
    std::array<std::byte, 3> decoded {};
    datum::copy(fragments, std::span {decoded});
    std::array<unsigned char, 3> copied {};
    std::array destinations {std::span {copied}.first(2), std::span {copied}.subspan(2)};
    datum::scatter_bytes(std::span {decoded}, destinations);

    return copied == std::array<unsigned char, 3> {128, 255, 0};
}
static_assert(byte_spellings());

TEST_CASE("byte spellings preserve high bits at runtime and compile time")
{
    CHECK(byte_spellings());
    volatile unsigned char source[2] {128, 255};
    const std::array fragments {std::span {source}};
    std::array<char, 2> result {};
    datum::copy(fragments, std::span {result});
    CHECK(static_cast<unsigned char>(result[0]) == 128);
    CHECK(static_cast<unsigned char>(result[1]) == 255);
}



template <typename T>
concept temporary_cursor = requires { datum::byte_cursor {T {}}; };

static_assert(!temporary_cursor<std::array<std::string_view, 1>>);


static_assert([] {
    const std::array<char, 4> bytes {'a', 'b', 'c', 'd'};
    const std::array fragments {std::span {bytes}.first(1), std::span {bytes}.subspan(1)};
    datum::byte_cursor input {fragments};
    const auto saved = input.mark();
    const auto slice = input.take(3);
    std::array<char, 3> result {};
    datum::copy(slice, std::span {result});
    input.restore(saved);

    return result == std::array {'a', 'b', 'c'} and 0 == input.position();
}());

TEST_CASE("fragment slices retain positions and backing addresses")
{
    const datum::byte_slice_view<std::span<const char> *> empty;

    CHECK(std::begin(empty) == std::end(empty));
    const std::string bytes = "abcdefgh";
    const std::array fragments {std::string_view {}, std::string_view {bytes}.substr(0, 2),
                                std::string_view {}, std::string_view {bytes}.substr(2, 3),
                                std::string_view {bytes}.substr(5)};
    datum::byte_cursor input {fragments};
    const auto saved = input.mark();
    input.consume(1);
    const auto slice = input.take(5);
    STATIC_REQUIRE(std::ranges::forward_range<decltype(slice)>);
    STATIC_REQUIRE(std::ranges::borrowed_range<decltype(slice)>);
    input.consume(2);
    std::string output;
    datum::write_fragments(slice,
                           [&](auto block) { output.append(std::data(block), std::size(block)); });
    CHECK(output == "bcdef");
    CHECK(std::data((*std::begin(slice))) == std::data(bytes) + 1);
    CHECK(0 == input.remaining());
    input.restore(saved);
    CHECK(8 == input.remaining());
    CHECK_THROWS_AS(input.take(9), std::length_error);
    CHECK(0 == input.position());
    datum::byte_cursor other {fragments};
    CHECK_THROWS_AS(other.restore(saved), std::invalid_argument);
    std::array<unsigned char, 4> copied {};
    input.read_into(std::span {copied});
    CHECK(copied == std::array<unsigned char, 4> {'a', 'b', 'c', 'd'});
}

TEST_CASE("byte cursors and projections preserve volatile byte access")
{
    volatile unsigned char bytes[4] {0, 128, 255, 7};
    const std::array fragments {std::span {bytes}.first(1), std::span {bytes}.subspan(1)};
    datum::byte_cursor cursor {fragments};
    const auto slice = cursor.take(3);
    static_assert(std::same_as<std::ranges::range_value_t<decltype(slice)>, std::span<const volatile unsigned char>>);
    std::array<std::byte, 3> output {};
    datum::copy(slice, std::span {output});
    CHECK(output == std::array<std::byte, 3> {std::byte {0}, std::byte {128}, std::byte {255}});
    const auto block = std::span {bytes};
    const auto selected = datum::byte_slice {1, 2}(block);
    static_assert(std::same_as<decltype(selected), const std::span<const volatile unsigned char>>);
    CHECK(std::data(selected) == bytes + 1);
    CHECK(selected[0] == 128);
    std::array<char, 1> tail {};
    cursor.read_into(std::span {tail});
    CHECK(7 == tail[0]);
}

TEST_CASE("byte cursor retains copied progress when a later fragment observation throws")
{
    const std::array bytes {'a', 'b'};
    const std::array blocks {std::span {bytes}.first(1), std::span {bytes}.subspan(1)};
    bool fail = false;
    const auto fragments = blocks | std::views::transform([&fail, &bytes](const auto block) {
                               if (fail and std::data(block) == std::data(bytes) + 1) {
                                   throw std::runtime_error {"fragment observation failed"};
                               }

                               return block;
                           });
    datum::byte_cursor input {fragments};
    std::array output {'x', 'x'};
    fail = true;
    CHECK_THROWS_AS(input.read_into(std::span {output}), std::runtime_error);
    CHECK(1 == input.position());
    CHECK(output == std::array {'a', 'x'});
    fail = false;
    input.read_into(std::span {output}.subspan(1));
    CHECK(output == bytes);
    CHECK(2 == input.position());
}

TEST_CASE("byte cursor handles every fragmentation and bounded read")
{
    const std::string bytes = "0123456789";
    for (std::size_t split = 0; split <= std::size(bytes); ++split) {
        const std::array fragments {std::string_view {bytes}.substr(0, split), std::string_view {},
                                    std::string_view {bytes}.substr(split)};
        for (std::size_t start = 0; start <= std::size(bytes); ++start) {
            for (std::size_t count = 0; count <= std::size(bytes) - start; ++count) {
                datum::byte_cursor input {fragments};
                input.consume(start);
                const auto slice = input.take(count);
                std::string result;
                for (auto block : slice) {
                    result.append(std::data(block), std::size(block));
                }
                CHECK(result == bytes.substr(start, count));
                CHECK(input.position() == start + count);
            }
        }
    }
    const std::array<std::span<const char>, 0> empty {};
    datum::byte_cursor input {empty};

    CHECK(std::begin(input.take(0)) == std::end(input.take(0)));
    CHECK_THROWS_AS(input.consume(1), std::length_error);
}

TEST_CASE("byte transfers distinguish const sources from writable destinations")
{
    struct read_only_fragments final
    {
        std::array<std::string_view, 2> blocks;

    public:
        auto begin() const { return std::begin(blocks); }
        void begin() = delete;

        auto end() const { return std::end(blocks); }
        void end() = delete;

    };

    STATIC_REQUIRE_FALSE(std::ranges::range<read_only_fragments>);
    STATIC_REQUIRE(datum::byte_fragments<read_only_fragments>);
    read_only_fragments source {{"ab", "cd"}};
    std::array storage {'x', 'x', 'x', 'x'};
    CHECK_THROWS_AS(datum::copy(source, std::span {storage}.first(3)), std::length_error);
    CHECK(storage == std::array {'x', 'x', 'x', 'x'});
    const auto bytes = datum::copy(source, storage);
    CHECK(std::string_view {bytes.data(), bytes.size()} == "abcd");
    datum::byte_cursor<const read_only_fragments&> cursor {source};
    std::array<char, 2> prefix;
    cursor.read_into(prefix);
    CHECK(prefix == std::array {'a', 'b'});
    CHECK(2 == cursor.remaining());

    std::array<std::string, 2> destination {"xx", "xxx"};
    CHECK(4 == datum::scatter_bytes(bytes, destination));
    CHECK(destination == std::array<std::string, 2> {"ab", "cdx"});
}

TEST_CASE("fragment sinks borrow immobile consumers and accept single-pass input")
{
    struct sink final
    {
        std::string bytes;

        unsigned calls = 0;

    public:
        sink() = default;
        sink(const sink&) = delete;
        sink(sink&&) = delete;

    public:
        void operator()(const std::string& block)
        {
            bytes += block;
            ++calls;
        }

    };

    std::istringstream input {"a bc def"};
    auto fragments = std::ranges::istream_view<std::string>(input);
    STATIC_REQUIRE(std::ranges::input_range<decltype(fragments)>);
    STATIC_REQUIRE_FALSE(datum::byte_fragments<decltype(fragments)>);
    sink output;
    datum::write_fragments(fragments, output);
    CHECK(output.bytes == "abcdef");
    CHECK(3 == output.calls);
}

TEST_CASE("byte sources compose with fixed storage and a normalized growing sink")
{
    const std::array expected {std::byte {0}, std::byte {127}, std::byte {128}, std::byte {255}};
    const auto check = [&](auto& source, const unsigned expected_calls) {
        std::array<std::byte, 4> fixed {};
        datum::copy(source, fixed);
        CHECK(fixed == expected);

        // A contiguous source becomes one borrowed fragment using a standard view.
        auto fragments = [&] {
            if constexpr (datum::byte_block<decltype(source)>) {
                return datum::fragments(source);
            }

            if constexpr (!datum::byte_block<decltype(source)>) {
                return std::ranges::ref_view {source};
            }
        }();
        std::vector<std::byte> growing;
        unsigned calls = 0;
        datum::write_fragments(fragments, [&](const auto& block) {
            ++calls;
            datum::append_range(growing, std::as_bytes(std::span {block}));
        });
        CHECK(std::ranges::equal(growing, expected));
        CHECK(calls == expected_calls);
    };
    const std::array<char, 4> characters {0, 127, static_cast<char>(128), static_cast<char>(255)};
    const std::array<signed char, 4> signed_bytes {0, 127, -128, -1};
    const std::array<unsigned char, 4> unsigned_bytes {0, 127, 128, 255};
    check(characters, 1);
    check(signed_bytes, 1);
    check(unsigned_bytes, 1);
    check(expected, 1);

    const std::array blocks {std::span {unsigned_bytes}.first(1),
                             std::span {unsigned_bytes}.first(0),
                             std::span {unsigned_bytes}.subspan(1)};
    check(blocks, 3);
    auto generated = std::views::iota(0, 2) | std::views::transform([&](int index) {
        const auto part = std::span {unsigned_bytes}.subspan(index * 2, 2);
        return std::vector<unsigned char>(part.begin(), part.end());
    });
    check(generated, 2);
}


static_assert([] {
    datum::resource_budget budget {{.elements = 3, .depth = 2}};
    budget.charge(datum::resource::elements, 3);
    {
        auto scope = budget.enter_depth();
    }

    return budget.used(datum::resource::elements) == 3 and budget.used(datum::resource::depth) == 0;
}());

TEST_CASE("aggregate charges reject before work and unwind nesting")
{
    datum::resource_budget budget {{.payload_bytes = 9, .allocation_bytes = 9, .depth = 2}};
    const std::string input = "\x04"
                              "abcd"
                              "\x06"
                              "abcdef";
    const std::array fragments {std::string_view {input}.substr(0, 3),
                                std::string_view {input}.substr(3)};
    datum::byte_cursor cursor {fragments};
    // A cooperating codec charges aggregate dimensions before allocating payload.
    auto decode = [&] {
        std::array<unsigned char, 1> size {};
        cursor.read_into(std::span {size});
        budget.charge(datum::resource::payload_bytes, size[0]);
        budget.charge(datum::resource::allocation_bytes, size[0]);
        std::string result(size[0], '\0');
        cursor.read_into(std::span {std::data(result), std::size(result)});

        return result;
    };
    CHECK(decode() == "abcd");
    CHECK_THROWS_AS(decode(), std::length_error);
    CHECK(cursor.remaining() == 6);
    CHECK(budget.used(datum::resource::payload_bytes) == 4);
    CHECK(budget.used(datum::resource::allocation_bytes) == 4);
    {
        auto outer = budget.enter_depth();
        auto inner = budget.enter_depth();
        CHECK_THROWS_AS(budget.enter_depth(), std::length_error);
        CHECK(budget.used(datum::resource::depth) == 2);
    }
    CHECK(budget.used(datum::resource::depth) == 0);
    datum::resource_budget unbounded;
    unbounded.charge(datum::resource::work, std::numeric_limits<std::size_t>::max());
    CHECK_THROWS_AS(unbounded.charge(datum::resource::work, 1), std::length_error);
    CHECK_THROWS_AS(unbounded.charge(static_cast<datum::resource>(99), 0), std::invalid_argument);
}

TEST_CASE("budget admission distinguishes refusal from strict guard failure", "[datum][budget]")
{
    for (const auto resource : {datum::resource::input_bytes, datum::resource::payload_bytes,
                                datum::resource::elements, datum::resource::allocation_bytes,
                                datum::resource::work, datum::resource::depth}) {
        datum::resource_budget budget {{.input_bytes = 3, .payload_bytes = 3, .elements = 3,
                                       .allocation_bytes = 3, .work = 3, .depth = 3}};
        CHECK(budget.try_charge(resource, 2));
        CHECK_FALSE(budget.try_charge(resource, 2));
        CHECK(budget.used(resource) == 2);
        CHECK(budget.try_charge(resource, 1));
        CHECK(budget.try_charge(resource, 0));
        CHECK_FALSE(budget.try_charge(resource, 1));
        CHECK_THROWS_AS(budget.charge(resource, 1), std::length_error);
        CHECK(budget.used(resource) == 3);
    }

    datum::resource_budget zero {{.elements = 0}};
    CHECK(zero.try_charge(datum::resource::elements, 0));
    CHECK_FALSE(zero.try_charge(datum::resource::elements, 1));
    CHECK(zero.used(datum::resource::elements) == 0);
    CHECK_THROWS_AS(zero.try_charge(static_cast<datum::resource>(99), 0), std::invalid_argument);

    datum::resource_budget unbounded;
    CHECK(unbounded.try_charge(datum::resource::work, 1));
    CHECK_FALSE(unbounded.try_charge(datum::resource::work, std::numeric_limits<std::size_t>::max()));
    CHECK(unbounded.used(datum::resource::work) == 1);
    CHECK(unbounded.try_charge(datum::resource::work, std::numeric_limits<std::size_t>::max() - 1));
    CHECK_FALSE(unbounded.try_charge(datum::resource::work, 1));
    CHECK_THROWS_AS(unbounded.charge(datum::resource::work, 1), std::length_error);
    CHECK(unbounded.used(datum::resource::work) == std::numeric_limits<std::size_t>::max());

    constexpr auto admit = [](auto& budget) {
        return budget.try_charge(datum::resource::elements, 1);
    };
    static_assert([&] {
        datum::resource_budget budget {{.elements = 1}};
        return admit(budget) and !admit(budget);
    }());
    constexpr datum::unlimited_budget unlimited;
    static_assert(admit(unlimited));
    static_assert(noexcept(unlimited.try_charge(datum::resource::elements, 1)));
}


using integer_geometry = iv::query<mechanics_domain<int>>;

struct mechanics_segment
{
    integer_geometry geometry;

    std::unique_ptr<std::string> payload;
};

TEST_CASE("clipping preserves origins across holes and adjacent segments")
{
    const std::array rows {
        mechanics_segment {integer_geometry::closed_open(0, 4), std::make_unique<std::string>("a")},
        mechanics_segment {integer_geometry::closed_open(4, 8), std::make_unique<std::string>("b")},
        mechanics_segment {integer_geometry::closed(10, 14), std::make_unique<std::string>("c")}};
    const auto lens = [](const mechanics_segment& row) -> const auto& { return row.geometry; };
    // Exhaust all endpoint inclusions and cuts through/around the origins.
    for (int lower = 0; lower <= 7; ++lower) {
        for (int upper = lower; upper <= 7; ++upper) {
            for (const bool lower_closed : {false, true}) {
                for (const bool upper_closed : {false, true}) {
                    using bound = iv::boundary<mechanics_domain<int>>;

                    const auto lower_bound =
                        lower_closed ? bound::closed(2 * lower) : bound::open(2 * lower);
                    const auto upper_bound =
                        upper_closed ? bound::closed(2 * upper) : bound::open(2 * upper);
                    const auto selection = iv::compile_intervals(integer_geometry::between(lower_bound, upper_bound));
                    const iv::clip_view cuts {rows, selection, lens};
                    STATIC_REQUIRE(std::ranges::input_range<decltype(cuts)>);
                    STATIC_REQUIRE_FALSE(std::ranges::forward_range<decltype(cuts)>);
                    std::array<std::array<bool, 15>, 3> actual {};
                    std::array<std::array<bool, 15>, 3> direct {};
                    const auto mark = [&](auto& points, const auto cut) {
                        const auto origin = std::addressof(cut.source()) - std::data(rows);
                        CHECK(cut.source().payload.get() == rows[origin].payload.get());
                        for (int point = 0; point <= 14; ++point) {
                            points[origin][point] = iv::contains(cut.bounds(), point);
                        }
                    };
                    for (const auto cut : cuts) {
                        mark(actual, cut);
                    }
                    consume_cuts(cuts, [&direct, &mark](const auto cut) { mark(direct, cut); });
                    for (std::size_t origin = 0; origin < std::size(rows); ++origin) {
                        for (int point = 0; point <= 14; ++point) {
                            const bool selected =
                                (point > 2 * lower || (lower_closed and point == 2 * lower)) and
                                (point < 2 * upper || (upper_closed and point == 2 * upper));
                            const bool present = origin == 0   ? point < 4
                                                 : origin == 1 ? point >= 4 and point < 8
                                                               : point >= 10;
                            CHECK(actual[origin][point] == (selected and present));
                            CHECK(direct[origin][point] == (selected and present));
                        }
                    }
                }
            }
        }
    }
    const auto holes = iv::compile_intervals(
        iv::set_union(integer_geometry::closed_open(1, 3), integer_geometry::closed_open(5, 7)));
    const iv::clip_view cuts {rows, holes, lens};
    auto empty = decltype(std::begin(cuts)) {};
    auto empty_copy = empty;
    CHECK(empty_copy == std::default_sentinel);
    CHECK(std::ranges::distance(cuts) == 2);
    auto owning_lens =
        [guard = std::make_unique<bool>(true)](const mechanics_segment& row) -> const auto& {
        if (!*guard) {
            throw std::logic_error {"disabled lens"};
        }

        return row.geometry;
    };
    iv::clip_view movable {rows, holes, std::move(owning_lens)};
    STATIC_REQUIRE(std::ranges::view<decltype(movable)>);
    auto moved = std::move(movable);
    const auto origins = std::move(moved) | std::views::transform([](const auto cut) {
                             return cut.source().payload->size();
                         });
    CHECK(std::ranges::distance(origins) == 2);
    const std::array invalid {mechanics_segment {integer_geometry::closed(0, 4), {}},
                              mechanics_segment {integer_geometry::closed(4, 8), {}}};
    CHECK_THROWS_AS((iv::clip_view {invalid, holes, lens}), std::invalid_argument);
}

TEST_CASE("cuts and byte cursors compose across object and payload holes")
{
    using Q = iv::query<mechanics_domain<unsigned>>;

    const std::array rows {Q::closed_open(0, 4), Q::closed_open(10, 18), Q::closed_open(18, 22),
                           Q::closed_open(40, 43)};
    const std::string bytes = "abcdefghijklmnopqrs";
    const std::array fragments {std::string_view {bytes}.substr(0, 2), std::string_view {},
                                std::string_view {bytes}.substr(2, 6),
                                std::string_view {bytes}.substr(8, 11)};
    const std::array selected {Q::closed_open(1, 3), Q::closed_open(12, 20),
                               Q::closed_open(42, 43)};
    const iv::canonical_range selection {selected};
    const iv::clip_view cuts {rows, selection, std::identity {}};
    datum::byte_cursor input {fragments};
    auto origin = std::cbegin(rows);
    unsigned packed_offset = 0;
    std::string output;
    std::vector<std::size_t> sources;
    for (const auto cut : cuts) {
        while (std::addressof(*origin) != std::addressof(cut.source())) {
            packed_offset += origin->upper().key() - origin->lower().key();
            ++origin;
        }
        const auto lower = cut.bounds().lower().key();
        const auto offset = packed_offset + lower - cut.source().lower().key();
        input.consume(offset - input.position());
        datum::write_fragments(
            input.take(cut.bounds().upper().key() - lower),
            [&output](const auto block) { output.append(std::data(block), std::size(block)); });
        ::ceph::util::push_back(sources, std::addressof(cut.source()) - std::data(rows));
    }
    CHECK(output == "bcghijklmns");
    CHECK(sources == std::vector<std::size_t> {0, 1, 2, 3});
    CHECK(input.position() == std::size(bytes));
    CHECK(input.remaining() == 0);
}

TEST_CASE("iterator copies and moves retain owned clipping geometry")
{
    using Q = iv::query<mechanics_domain<std::string>>;

    for (const auto length : {8U, 128U}) {
        const std::array rows {Q::closed_open(std::string(length, 'a'), std::string(length, 'z'))};
        const auto selection = iv::compile_intervals(Q::universal());
        const iv::clip_view cuts {rows, selection, [](const Q& row) { return row; }};
        using iterator = decltype(std::begin(cuts));

        std::optional<iterator> copied;
        {
            auto original = std::begin(cuts);
            copied.emplace(original);
        }
        CHECK((**copied).bounds().lower().key() == std::string(length, 'a'));
        std::optional<iterator> moved;
        {
            auto original = std::begin(cuts);
            moved.emplace(std::move(original));
        }
        CHECK((**moved).bounds().upper().key() == std::string(length, 'z'));
        auto assigned = std::begin(cuts);
        assigned = *copied;
        copied.reset();
        CHECK((*assigned).bounds().lower().key() == std::string(length, 'a'));
        unsigned visited = 0;
        consume_cuts(cuts, [&visited, length](const auto cut) {
            CHECK(cut.bounds().lower().key() == std::string(length, 'a'));
            CHECK(cut.bounds().upper().key() == std::string(length, 'z'));
            ++visited;
        });
        CHECK(visited == 1);
    }
}

TEST_CASE("clipping handles empty origins and unbounded selections")
{
    using domain = mechanics_domain<int>;
    using Q = iv::query<domain>;
    using B = iv::boundary<domain>;

    const std::array rows {Q::between(B::negative_infinity(), B::open(-4)),
                           Q::empty(),
                           Q::open(-4, 0),
                           Q::singleton(0),
                           Q::open(0, 4),
                           Q::between(B::closed(4), B::positive_infinity()),
                           Q::empty()};
    const auto lens = [](const Q& row) -> const Q& { return row; };
    const std::array selections {
        iv::compile_intervals(Q::universal()), iv::compile_intervals(Q::empty()),
        iv::compile_intervals(Q::singleton(0)), iv::compile_intervals(Q::closed(-4, 4)),
        iv::compile_intervals(std::array {Q::between(B::negative_infinity(), B::closed(-6)),
                                       Q::closed_open(-3, -1), Q::singleton(0), Q::open(2, 6),
                                       Q::between(B::open(8), B::positive_infinity())})};
    const auto prepared_source = iv::compile_intervals(rows);
    for (const auto& selection : selections) {
        const iv::clip_view cuts {rows, selection, lens};
        std::array<std::array<bool, 21>, std::size(rows)> actual {};
        std::array<std::array<bool, 21>, std::size(rows)> direct {};
        const auto mark = [&rows](auto& points, const auto cut) {
            CHECK_FALSE(iv::is_empty(cut.bounds()));
            const auto origin = std::addressof(cut.source()) - std::data(rows);
            for (int point = -10; point <= 10; ++point) {
                points[origin][point + 10] |= iv::contains(cut.bounds(), point);
            }
        };
        for (const auto cut : cuts) {
            mark(actual, cut);
        }
        consume_cuts(cuts, [&direct, &mark](const auto cut) { mark(direct, cut); });
        for (const auto origin : std::views::iota(0UZ, std::size(rows))) {
            for (int point = -10; point <= 10; ++point) {
                CHECK(actual[origin][point + 10] ==
                      (iv::contains(rows[origin], point) and iv::contains(selection, point)));
                CHECK(direct[origin][point + 10] ==
                      (iv::contains(rows[origin], point) and iv::contains(selection, point)));
            }
        }
        const iv::clip_view prepared {prepared_source, selection, std::identity {}};
        for (int point = -10; point <= 10; ++point) {
            const bool present = std::ranges::any_of(
                prepared, [point](const auto cut) { return iv::contains(cut.bounds(), point); });
            CHECK(present ==
                  (iv::contains(prepared_source, point) and iv::contains(selection, point)));
        }
    }
    const std::array<Q, 0> no_rows {};
    const iv::clip_view empty {no_rows, *std::begin(selections), lens};

    CHECK(std::ranges::begin(empty) == std::ranges::end(empty));
    consume_cuts(empty, [](const auto) { FAIL("empty source offered a cut"); });
}

struct admission_domain final
{
    using value_type = int;

    static inline unsigned comparisons = 0;

public:
    static constexpr auto compare(int lhs, int rhs)
    {
        if (!std::is_constant_evaluated()) {
            ++comparisons;
        }

        return lhs <=> rhs;
    }

};

TEST_CASE("identity clipping reuses canonical admission without trusting other lenses")
{
    using Q = iv::query<admission_domain>;

    const std::array rows {Q::closed_open(0, 4), Q::closed_open(8, 12)};
    const iv::canonical_range source {rows};
    const auto selection = iv::compile_intervals(std::array {Q::closed_open(1, 3), Q::closed_open(9, 11)});
    admission_domain::comparisons = 0;
    const iv::clip_view cuts {source, selection, std::identity {}};
    CHECK(admission_domain::comparisons == 0);
    unsigned visited = 0;
    consume_cuts(cuts, [&visited, &rows](const auto cut) {
        REQUIRE(visited < std::size(rows));
        CHECK(std::addressof(cut.source()) == std::addressof(rows[visited]));
        const auto offset = 8 * static_cast<int>(visited);
        CHECK(cut.bounds().lower().key() == offset + 1);
        CHECK(cut.bounds().upper().key() == offset + 3);
        ++visited;
    });
    CHECK(visited == 2);

    unsigned projections = 0;
    const iv::clip_view projected {source, selection, [&projections](const Q& row) -> const Q& {
                                       ++projections;

                                       return row;
                                   }};
    CHECK(projections == std::size(rows));
    CHECK(std::ranges::distance(projected) == 2);
    CHECK_THROWS_AS(
        (iv::clip_view {source, selection, [](const Q&) { return Q::closed_open(0, 20); }}),
        std::invalid_argument);

    const std::array<Q, 0> no_rows {};
    const iv::canonical_range empty_source {no_rows};
    const iv::clip_view empty {empty_source, selection, std::identity {}};

    CHECK(std::begin(empty) == std::end(empty));
}

static_assert([] {
    const std::array rows {integer_geometry::closed_open(0, 4),
                           integer_geometry::closed_open(8, 12)};
    const iv::canonical_range source {rows};
    const auto selection = iv::compile_intervals(integer_geometry::closed_open(1, 11));
    const iv::clip_view cuts {source, selection, std::identity {}};

    // Use C++23 fold_left once all supported libraries provide it.
    int covered = 0;
    for (const auto cut : cuts) {
        covered += cut.bounds().upper().key() - cut.bounds().lower().key();
    }
    return covered == 6;
}());

TEST_CASE("clipping retains one projection across repeated cuts and dereferences")
{
    using Q = integer_geometry;

    const std::array rows {Q::closed(0, 10)};
    const auto selection = iv::compile_intervals(iv::set_union(Q::closed_open(1, 3), Q::closed_open(5, 7)));
    unsigned projections = 0;
    const iv::clip_view cuts {rows, selection, [&projections](const Q& row) -> const Q& {
                                  ++projections;

                                  return row;
                              }};
    projections = 0;
    auto original = std::begin(cuts);
    CHECK((*original).bounds().lower().key() == 1);
    CHECK((*original).bounds().upper().key() == 3);
    auto copied = original;
    ++copied;
    auto moved = std::move(copied);
    CHECK((*moved).bounds().lower().key() == 5);
    CHECK((*original).bounds().lower().key() == 1);
    CHECK(projections == 1);
    ++moved;
    CHECK(moved == std::default_sentinel);
    CHECK(projections == 1);
}

template <typename IteratorT>
concept temporary_dereference = requires(IteratorT position) { *std::move(position); };
template <typename ViewT>
concept temporary_begin = requires(ViewT view) { std::move(view).begin(); };

template <typename RowsT, typename SelectionT, typename LensT>
concept accepts_clip_lens = requires(RowsT& rows, SelectionT& selection, LensT lens) {
    iv::clip_view {rows, selection, std::move(lens)};
};

TEST_CASE("range consumers retain geometry and stops before advancing on exceptions")
{
    using Q = integer_geometry;

    const std::array rows {Q::closed_open(0, 10), Q::closed_open(12, 20)};
    const auto selection = iv::compile_intervals(
        std::array {Q::closed_open(1, 3), Q::closed_open(5, 7), Q::closed_open(13, 15)});
    unsigned projections = 0;
    const iv::clip_view cuts {rows, selection, [&projections](const Q& row) -> const Q& {
                                  ++projections;

                                  return row;
                              }};
    STATIC_REQUIRE(std::input_iterator<decltype(std::begin(cuts))>);
    STATIC_REQUIRE_FALSE(temporary_dereference<decltype(std::begin(cuts))>);
    STATIC_REQUIRE_FALSE(temporary_begin<decltype(cuts)>);
    projections = 0;
    unsigned visited = 0;
    auto consumer = [guard = std::make_unique<bool>(true), &visited](const auto cut) {
        CHECK(*guard);
        CHECK_FALSE(iv::is_empty(cut.bounds()));
        ++visited;
    };
    consume_cuts(cuts, std::move(consumer));
    CHECK(visited == 3);
    CHECK(projections == 2);
    projections = 0;
    CHECK_THROWS_AS(
        consume_cuts(cuts,
                     [](const auto) -> void { throw std::runtime_error {"consumer failed"}; }), std::runtime_error);
    CHECK(projections == 1);
}

TEST_CASE("fixed geometry views preserve endpoint references through noncommon ranges")
{
    using domain = mechanics_domain<int>;

    struct extent final
    {
        int first;
        int last;
    };
    struct extent_view final
    {
        using domain_type = domain;

        const extent& origin;

    public:
        constexpr auto lower() const { return iv::boundary_ref<domain>::closed(origin.first); }
        constexpr auto upper() const { return iv::boundary_ref<domain>::open(origin.last); }

    };
    const std::array rows {extent {0, 4}, extent {4, 8}};
    const std::array wanted {extent {1, 3}, extent {5, 7}};
    const std::array selected {extent_view {wanted[0]}, extent_view {wanted[1]}};
    const auto source =
        rows | std::views::take_while([](const extent& row) { return row.first < 9; });
    const auto selection = selected | std::views::take_while([](const extent_view& row) {
                               return row.lower().key() < 9;
                           });
    STATIC_REQUIRE_FALSE(std::ranges::common_range<decltype(source)>);
    STATIC_REQUIRE_FALSE(std::ranges::common_range<decltype(selection)>);
    const iv::canonical_range prepared {selection};
    const iv::clip_view cuts {source, prepared,
                              [](const extent& row) { return extent_view {row}; }};
    std::size_t visited = 0;
    consume_cuts(cuts, [&visited, &rows, &wanted](const auto cut) {
        REQUIRE(visited < std::size(rows));
        CHECK(std::addressof(cut.source()) == std::addressof(rows[visited]));
        CHECK(std::addressof(cut.bounds().lower().key()) == std::addressof(wanted[visited].first));
        CHECK(std::addressof(cut.bounds().upper().key()) == std::addressof(wanted[visited].last));
        ++visited;
    });
    CHECK(visited == 2);
}

TEST_CASE("clipping checks the read-only lens overload for shallow-const views")
{
    using Q = integer_geometry;

    struct row final
    {
        Q geometry;
    };
    struct lens final
    {
        int operator()(row&) const { return 0; }
        const Q& operator()(const row& value) const { return value.geometry; }

    };
    std::array rows {row {Q::closed_open(0, 4)}};
    const std::span source {rows};
    STATIC_REQUIRE(std::same_as<std::ranges::range_reference_t<decltype(source)>, row&>);
    const auto selection = iv::compile_intervals(Q::closed_open(1, 3));
    const iv::clip_view cuts {source, selection, lens {}};
    unsigned visited = 0;
    consume_cuts(cuts, [&visited, &rows](const auto cut) {
        CHECK(std::addressof(cut.source()) == std::addressof(rows[0]));
        CHECK(cut.bounds().lower().key() == 1);
        CHECK(cut.bounds().upper().key() == 3);
        ++visited;
    });
    CHECK(visited == 1);
    const auto mutable_only = [](row& value) -> const Q& { return value.geometry; };
    STATIC_REQUIRE_FALSE(
        accepts_clip_lens<decltype(source), decltype(selection), decltype(mutable_only)>);
}

struct address final
{
    unsigned value;
    static inline unsigned copies = 0;

public:
    address() = delete;
    constexpr explicit address(unsigned input) : value(input)
    {}
    constexpr address(const address& other) : value(other.value)
    {
        if (!std::is_constant_evaluated()) {
            ++copies;
        }
    }

    constexpr address(address&&) = default;

public:
    address& operator=(const address&) = delete;

};
struct address_domain final
{
    using value_type = address;

public:
    static constexpr std::strong_ordering compare(const address& left, const address& right)
    {
        return left.value <=> right.value;
    }

};
struct byte_quantity final
{
    unsigned value;

public:
    constexpr auto operator<=>(const byte_quantity&) const = default;

};
struct address_metric final
{
    using quantity_type = byte_quantity;

    iv::unsigned_metric<unsigned> checked;

public:
    constexpr byte_quantity zero() const noexcept { return {0}; }

public:
    constexpr byte_quantity distance(const address& first, const address& last) const
    {
        return {checked.distance(first.value, last.value)};
    }

public:
    constexpr address advance(const address& point, const byte_quantity amount) const
    {
        return address {checked.advance(point.value, amount.value)};
    }

public:
    constexpr byte_quantity add(byte_quantity left, byte_quantity right) const
    {
        return {checked.add(left.value, right.value)};
    }

public:
    constexpr byte_quantity subtract(byte_quantity left, byte_quantity right) const
    {
        return {checked.subtract(left.value, right.value)};
    }

};
static_assert(iv::coverage_metric_for<address_metric, address_domain>);
static_assert([] {
    using Q = iv::query<address_domain>;

    const std::array rows {Q::closed_open(address {0}, address {8})};
    const std::array intervals {Q::closed_open(address {2}, address {6})};
    const iv::canonical_range selection {intervals};
    const iv::clip_view cuts {rows, selection, [](const Q& row) { return row; }};
    auto cursor = std::begin(cuts);
    auto assigned = std::begin(cuts);
    assigned = std::move(cursor);

    return (*assigned).bounds().upper().key().value == 6;
}());
static_assert([] {
    using Q = iv::query<address_domain>;

    const std::array rows {Q::closed_open(address {0}, address {8})};
    const std::array intervals {Q::closed_open(address {2}, address {6})};
    const iv::canonical_range selection {intervals};
    const iv::clip_view cuts {rows, selection, [](const Q& row) { return row; }};
    unsigned covered = 0;
    consume_cuts(cuts, [&covered](const auto cut) {
        covered += cut.bounds().upper().key().value - cut.bounds().lower().key().value;
    });

    return covered == 4;
}());

TEST_CASE("range consumers add no point or geometry copies")
{
    using Q = iv::query<address_domain>;

    const std::array rows {Q::closed_open(address {0}, address {8}),
                           Q::closed_open(address {9}, address {12})};
    const std::array intervals {Q::closed_open(address {1}, address {3}),
                                Q::closed_open(address {5}, address {7}),
                                Q::closed_open(address {10}, address {11})};
    const iv::canonical_range selection {intervals};
    const iv::clip_view borrowed {rows, selection, [](const Q& row) -> const Q& { return row; }};
    address::copies = 0;
    unsigned visited = 0;
    consume_cuts(borrowed, [&visited](const auto cut) {
        CHECK_FALSE(iv::is_empty(cut.bounds()));
        ++visited;
    });
    CHECK(visited == 3);
    CHECK(address::copies == 0);
    const iv::canonical_range prepared {rows};
    address::copies = 0;
    const iv::clip_view identity {prepared, selection, std::identity {}};
    visited = 0;
    consume_cuts(identity, [&visited](const auto cut) {
        CHECK_FALSE(iv::is_empty(cut.bounds()));
        ++visited;
    });
    CHECK(visited == 3);
    CHECK(address::copies == 0);
    unsigned projections = 0;
    const iv::clip_view owning {rows, selection, [&projections](const Q& row) {
                                    ++projections;

                                    return row;
                                }};
    projections = 0;
    address::copies = 0;
    visited = 0;
    consume_cuts(owning, [&visited](const auto cut) {
        CHECK_FALSE(iv::is_empty(cut.bounds()));
        ++visited;
    });
    CHECK(visited == 3);
    CHECK(projections == 2);
    CHECK(address::copies == 4);
}

TEST_CASE("clipping admission projects each origin once")
{
    using Q = iv::query<address_domain>;

    const std::array rows {Q::closed_open(address {0}, address {4}), Q::empty(),
                           Q::closed_open(address {4}, address {8})};
    const auto selection = iv::compile_intervals(Q::universal());
    unsigned projections = 0;
    address::copies = 0;
    const iv::clip_view borrowed {rows, selection, [&projections](const Q& row) -> const Q& {
                                      ++projections;

                                      return row;
                                  }};
    CHECK(projections == std::size(rows));
    CHECK(address::copies == 0);
    projections = 0;
    const iv::clip_view owned {rows, selection, [&projections](const Q& row) {
                                   ++projections;

                                   return row;
                               }};
    CHECK(projections == std::size(rows));
    CHECK(address::copies == 4);
    CHECK(std::ranges::distance(borrowed) == 2);
    CHECK(std::ranges::distance(owned) == 2);

    const std::array invalid {Q::closed_open(address {0}, address {4}),
                              Q::closed_open(address {3}, address {8}),
                              Q::closed_open(address {9}, address {12})};
    projections = 0;
    CHECK_THROWS_AS((iv::clip_view {invalid, selection,
                                    [&projections](const Q& row) {
                                        ++projections;

                                        return row;
                                    }}),
                    std::invalid_argument);
    CHECK(projections == 2);
}

static_assert([] {
    using Q = iv::query<address_domain>;

    const std::array intervals {Q::closed_open(address {0}, address {4}),
                                Q::closed_open(address {10}, address {18})};
    const iv::canonical_range coverage {intervals};
    const auto selected = iv::take(coverage, byte_quantity {7}, address_metric {});

    return iv::measure(selected, address_metric {}).value == 7 and
           std::ranges::next(std::begin(selected))->upper().key().value == 13;
}());

TEST_CASE("metrics count covered units rather than coordinate width")
{
    using domain = mechanics_domain<unsigned>;
    using Q = iv::query<domain>;

    const std::array intervals {Q::closed_open(0, 4), Q::closed_open(10, 18)};
    const iv::canonical_range coverage {intervals};
    const iv::unsigned_metric<unsigned> metric;
    CHECK(iv::measure(coverage, metric) == 12);
    const auto selected = iv::take(coverage, 7, metric);
    CHECK(iv::measure(selected, metric) == 7);
    CHECK(std::ranges::next(std::begin(selected))->upper().key() == 13);
    CHECK(std::empty(iv::take(coverage, 0, metric)));
    CHECK(iv::measure(iv::take(coverage, 100, metric), metric) == 12);
    CHECK_THROWS_AS(metric.advance(std::numeric_limits<unsigned>::max(), 1), std::overflow_error);
    const auto unsupported = iv::compile_intervals(Q::closed(0, 4));
    CHECK_THROWS_AS(iv::measure(unsupported, metric), std::domain_error);
    const auto infinite = iv::compile_intervals(Q {});
    CHECK_THROWS_AS(iv::take(infinite, 3, metric), std::domain_error);

    // A value geometry can borrow source endpoints; no default/assignment on points.
    struct row
    {
        address first;
        address last;
    };
    const std::array rows {row {address {0}, address {8}}};
    const auto wanted = iv::compile_intervals(iv::query<address_domain>::closed_open(address {2}, address {6}));
    address::copies = 0;
    const iv::clip_view cuts {rows, wanted, [](const row& source) {
                                  return iv::borrowed_interval<address_domain> {
                                              iv::boundary_ref<address_domain>::closed(source.first),
                                              iv::boundary_ref<address_domain>::open(source.last)
                                          };
                              }};
    const auto position = std::begin(cuts);
    STATIC_REQUIRE_FALSE(temporary_dereference<decltype(position)>);
    CHECK((*position).bounds().lower().key().value == 2);
    CHECK(address::copies == 0);
}

TEST_CASE("coverage consumers request only the arithmetic they use")
{
    using domain = mechanics_domain<unsigned>;
    using Q = iv::query<domain>;

    struct quantity final { unsigned value; };
    struct measurement final
    {
        using quantity_type = quantity;

    public:
        quantity zero() const { return {0}; }
        quantity distance(const unsigned first, const unsigned last) const { return {last - first}; }
        quantity add(const quantity left, const quantity right) const
        {
            return {left.value + right.value};
        }

    };
    struct quota final
    {
        using quantity_type = unsigned;

    public:
        unsigned zero() const { return 0; }
        unsigned distance(const unsigned first, const unsigned last) const { return last - first; }
        unsigned advance(const unsigned point, const unsigned amount) const { return point + amount; }
        unsigned subtract(const unsigned left, const unsigned right) const { return left - right; }

    };

    STATIC_REQUIRE_FALSE(std::totally_ordered<quantity>);
    STATIC_REQUIRE(iv::coverage_metric_for<measurement, domain>);
    STATIC_REQUIRE(iv::coverage_metric_for<quota, domain>);
    const std::array intervals {Q::closed_open(0, 4), Q::closed_open(10, 18)};
    const iv::canonical_range coverage {intervals};
    CHECK(12 == iv::measure(coverage, measurement {}).value);
    const auto selected = iv::take(coverage, 7, quota {});
    CHECK(7 == iv::measure(selected, measurement {}).value);
    CHECK(13 == std::ranges::next(std::begin(selected))->upper().key());
}


template <typename T>
concept hexadecimal_integer = requires(T value) {
    datum::key::fixed_width_hexadecimal_segment(value);
    datum::key::decode_fixed_width_hexadecimal_segment<T>(std::string_view {});
};
static_assert(hexadecimal_integer<unsigned>);
static_assert(!hexadecimal_integer<bool>);
static_assert(!hexadecimal_integer<int>);


namespace {

// sized_range does not require size() to return std::size_t.
struct sized_block final
{
    std::span<unsigned char> bytes;

public:
    constexpr auto begin() const { return std::begin(bytes); }

    constexpr auto end() const { return std::end(bytes); }

public:
    constexpr auto data() const { return std::data(bytes); }

public:
    constexpr unsigned long long size() const { return std::size(bytes); }

};

constexpr bool differing_size_types()
{
    const std::array source {128U, 255U, 7U};
    const std::array<unsigned char, 3> bytes {128, 255, 7};
    std::array<unsigned char, 3> backing {};
    const std::array fragments {sized_block {std::span {backing}.first(1)},
                                sized_block {std::span {backing}.subspan(1)}};
    datum::scatter_bytes(std::span {bytes}, fragments);
    datum::byte_cursor input {fragments};
    std::array<std::byte, 3> copied {};
    input.read_into(std::span {copied}.first(1));
    datum::copy(input.take(2), std::span {copied}.subspan(1));

    return std::ranges::equal(
        copied, source, {}, [](const std::byte value) { return std::to_integer<unsigned>(value); });
}
} // anonymous namespace

static_assert(datum::byte_block<sized_block>);
static_assert(differing_size_types());

namespace {

struct nonaddressable_point final
{
    unsigned value;

public:
    const nonaddressable_point *operator&() const = delete;

};
struct addresses final
{
    using value_type = nonaddressable_point;

public:
    static constexpr auto compare(const nonaddressable_point& lhs, const nonaddressable_point& rhs)
    {
        return lhs.value <=> rhs.value;
    }

};
} // anonymous namespace

static_assert([] {
    const nonaddressable_point first {1};
    const nonaddressable_point last {3};
    const iv::borrowed_interval<addresses> interval {iv::boundary_ref<addresses>::closed(first),
                                                     iv::boundary_ref<addresses>::open(last)};

    return iv::contains(interval, nonaddressable_point {2}) and
           std::addressof(interval.lower().key()) == std::addressof(first);
}());


static_assert([] {
    struct requirement
    {
        int value;

    public:
        constexpr explicit requirement(const int input) : value(input)
        {}

    };
    const auto query = datum::plan::with_requirement(
        datum::plan::make_query(datum::key::byte_query::empty()), requirement {7});

    return !datum::plan::is_resolved(query) and datum::plan::requirement_count(query) == 1 and
           std::get<0>(query.requirements).value == 7;
}());


TEST_CASE("key and plan composition agrees at runtime", "[datum][key][plan]")
{
    CHECK(::datum_test::key_plan());
    CHECK(::datum_test::key_query_ownership());
}

TEST_CASE("Datum key layout and page traversal work without a native context", "[datum][data]")
{
    CHECK(private_tuple_members());
    const std::string embedded {"a\0b", 3};
    const auto key = datum::key::compose("arbitrary", embedded);
    const auto parts = datum::key::decode_segments(datum::key::key_view(key));
    REQUIRE(parts);
    CHECK(*parts == std::vector<std::string> {"arbitrary", embedded});

    std::size_t consumed = 0;
    const auto result = datum::traversal::for_each_page(
        0, [](const int cursor) { return cursor; },
        [](const int cursor, const int&) -> std::optional<int> {
            return cursor == 2 ? std::nullopt : std::optional {cursor + 1};
        },
        [&consumed](int) { ++consumed; });
    CHECK(result.complete());
    CHECK(result.pages == 3);
    CHECK(consumed == 3);
}

TEST_CASE("named byte domains retain borrowed comparisons and neutral prefixes", "[datum][domain]")
{
    struct bucket_keys_domain final : datum::key::byte_string_domain {};
    struct object_keys_domain final : datum::key::byte_string_domain {};
    using query_type = iv::query<object_keys_domain>;

    STATIC_REQUIRE(iv::prefix_domain<object_keys_domain>);
    STATIC_REQUIRE_FALSE(iv::interval_for<iv::query<bucket_keys_domain>, object_keys_domain>);

    struct borrowed_key final
    {
        std::string_view bytes;
        unsigned& observations;

    public:
        operator std::string_view() const noexcept
        {
            ++observations;

            return bytes;
        }

        operator std::string() const = delete;

    };
    unsigned observations = 0;
    const auto names = query_type::closed_open("a", "z");
    CHECK(iv::contains(names, borrowed_key {"middle", observations}));
    CHECK(0 < observations);

    CHECK(query_type::prefix("") == query_type::universal());
    CHECK(object_keys_domain::prefix_upper_bound("ab") == std::optional<std::string> {"ac"});
    CHECK_FALSE(object_keys_domain::prefix_upper_bound(std::string {"\xFF\xFF", 2}));
    const std::string_view extended_key {"ab\0", 3};
    CHECK(iv::contains(query_type::prefix("ab"), extended_key));
    CHECK_FALSE(iv::contains(query_type::singleton("ab"), extended_key));

    const std::array prefixes {
        std::string {}, std::string {"\0", 1}, std::string {"\x7F", 1},
        std::string {"\x80", 1}, std::string {"\xFF", 1}, std::string {"a\0", 2},
        std::string {"a\xFF", 2}, std::string {"\xFF\xFF", 2}};
    const char pivot = static_cast<char>(0x80);
    for (unsigned value = 0; value < 256; ++value) {
        const char byte = static_cast<char>(value);
        CHECK((value <=> 128U) == object_keys_domain::compare(
                  std::string_view {&byte, 1}, std::string_view {&pivot, 1}));
    }
    for (const auto& prefix : prefixes) {
        const auto selected = iv::prefix<object_keys_domain>(prefix);
        CHECK(query_type::prefix(prefix) == selected);
        CHECK(iv::contains(selected, prefix + '\0'));
        for (const auto& candidate : prefixes) {
            CHECK(iv::contains(selected, std::string_view {candidate}) ==
                  std::string_view {candidate}.starts_with(prefix));
        }
        for (unsigned value = 0; value < 256; ++value) {
            const char byte = static_cast<char>(value);
            const std::string_view candidate {&byte, 1};
            CHECK(iv::contains(selected, candidate) == candidate.starts_with(prefix));
        }
    }
}

TEST_CASE("segmented keys reject terminator-consuming prefixes", "[datum][key]")
{
    const auto parent = datum::key::keyspace("space");
    const std::string ambiguous(1, static_cast<char>(0xFF));
    CHECK_THROWS_AS(datum::key::compose("space", ambiguous), std::invalid_argument);
    CHECK_THROWS_AS(datum::key::compose(std::tuple {"space", ambiguous}), std::invalid_argument);
    CHECK_THROWS_AS(parent / ambiguous, std::invalid_argument);
    CHECK_THROWS_AS(datum::key::prefix(parent, ambiguous), std::invalid_argument);

    const std::string admitted {"a\0\xFF", 3};
    const auto encoded = parent / "" / admitted;
    const auto decoded = datum::key::decode<std::string, std::string, std::string>(encoded);
    REQUIRE(decoded.has_value());
    CHECK(std::empty(std::get<1>(*decoded)));
    CHECK(std::get<2>(*decoded) == admitted);
    CHECK(datum::key::key_view(encoded) == std::string_view {"space\0\0a\0\xFF\xFF\0", 12});
}

static_assert([] {
    const auto encoded = datum::key::compose("space", std::string_view {"\0\xFF", 2});
    const auto decoded = datum::key::decode<std::string, std::string>(encoded);

    return decoded and std::get<1>(*decoded) == std::string_view {"\0\xFF", 2};
}());


template <typename RowsT, typename SelectionT, typename LensT>
concept clips_temporary = requires(RowsT rows, SelectionT selection, LensT lens) {
    iv::clip_view {std::move(rows), selection, lens};
};
static_assert(!clips_temporary<
              std::array<mechanics_segment, 1>, iv::canonical_range<std::vector<iv::query<mechanics_domain<int>>>>,
              decltype([](const mechanics_segment& row) -> const auto& { return row.geometry; })>);

constexpr bool empty_byte_ranges()
{
    const std::array<std::span<const char>, 2> source {};
    std::span<char> destination;
    if (!std::empty(datum::copy(source, destination))) {
        return false;
    }
    const std::array<std::span<char>, 2> fragments {};
    if (0 != datum::scatter_bytes(std::span<const char> {}, fragments)) {
        return false;
    }
    datum::byte_cursor cursor {source};
    cursor.read_into(destination);

    return 0 == cursor.position() and std::begin(cursor.take(0)) == std::default_sentinel;
}
static_assert(empty_byte_ranges());

TEST_CASE("portable constant-evaluated byte pipelines agree with runtime operations", "[datum][constexpr]")
{
    CHECK(empty_byte_ranges());
    CHECK(differing_size_types());
}

} // anonymous namespace

TEST_CASE("size folds borrow callbacks and preserve failure prefixes", "[datum][compat]")
{
    CHECK(size_fold_contract());
    const std::array source {1, 2, 3};
    size_fold function {2};
    CHECK_THROWS_AS(datum::compat::fold_size(source, std::size_t {0}, function), std::runtime_error);
    CHECK(2 == function.calls);
}

TEST_CASE("key composition accepts a single-pass range of generated segments")
{
    namespace key = datum::key;
    std::istringstream input {"space bucket object"};
    const auto generated = key::compose_range(std::views::istream<std::string>(input));
    CHECK(generated == key::compose("space", "bucket", "object"));
    CHECK(input.eof());

    const std::array segments {std::string_view {"space"}, std::string_view {"a\0b", 3},
                              std::string_view {}};
    const auto encoded = key::compose_range(segments);
    CHECK(encoded == key::compose(segments[0], segments[1], segments[2]));
    CHECK(key::segments(encoded) == std::vector<std::string> {"space", {"a\0b", 3}, ""});
    CHECK_THROWS_AS(key::compose_range(std::views::empty<std::string_view>), std::invalid_argument);
    CHECK_THROWS_AS(key::compose_range(std::array {std::string_view {}}), std::invalid_argument);
    CHECK_THROWS_AS(key::compose_range(std::array {std::string_view {"space"},
                                                  std::string_view {"\xFF", 1}}),
                    std::invalid_argument);

    unsigned visited = 0;
    auto temporary_segments = std::views::iota(0, 3) | std::views::transform([&](int number) {
        ++visited;
        return std::string(40, static_cast<char>('a' + number));
    });
    const auto from_temporaries = key::compose_range(temporary_segments);
    CHECK(visited == 3);
    CHECK(from_temporaries == key::compose(std::string(40, 'a'), std::string(40, 'b'),
                                          std::string(40, 'c')));
}

TEST_CASE("byte transfers accept ordinary writable ranges directly")
{
    const std::array source {'a', 'b', 'c', 'd'};
    std::array<std::array<unsigned char, 2>, 2> blocks {};
    CHECK(datum::scatter_bytes(source, blocks) == 4);
    std::vector<std::byte> destination(5, std::byte {'x'});
    const auto gathered = datum::copy(blocks, destination);
    CHECK(gathered.data() == destination.data());
    CHECK(gathered.size() == 4);
    CHECK(destination == std::vector {std::byte {'a'}, std::byte {'b'}, std::byte {'c'},
                                      std::byte {'d'}, std::byte {'x'}});
    datum::byte_cursor input {blocks};
    std::vector<char> prefix(3);
    input.read_into(prefix);
    CHECK(prefix == std::vector {'a', 'b', 'c'});
    CHECK(input.remaining() == 1);

    std::array<char, 2> too_short {'x', 'x'};
    CHECK_THROWS_AS(datum::copy(blocks, too_short), std::length_error);
    CHECK(too_short == std::array {'x', 'x'});
    CHECK_THROWS_AS(input.read_into(too_short), std::length_error);
    CHECK(too_short == std::array {'x', 'x'});
    CHECK(input.remaining() == 1);
}

static_assert([] {
    const std::array fragments {std::string_view {"ab"}, std::string_view {"cd"}};
    std::array<char, 4> output {};
    datum::copy(fragments, output);
    const auto encoded = datum::key::compose_range(std::array {std::string_view {"space"}, std::string_view {"key"}});
    return output == std::array {'a', 'b', 'c', 'd'} and
           encoded == datum::key::compose("space", "key");
}());

namespace byte_range_test {

// A contiguous range with ADL access and no data()/size() members.
struct block final
{
    std::array<char, 4> storage;
};
constexpr auto begin(block& value) { return value.storage.data(); }
constexpr auto end(block& value) { return value.storage.data() + value.storage.size(); }
constexpr auto begin(const block& value) { return value.storage.data(); }
constexpr auto end(const block& value) { return value.storage.data() + value.storage.size(); }

template <typename OutputT>
concept can_gather = requires (const std::array<std::string_view, 2>& source, OutputT&& output) {
    datum::copy(source, std::forward<OutputT>(output));
};
static_assert(can_gather<block&>);
static_assert(!can_gather<const block&>);
static_assert(!can_gather<block>);
static_assert(!can_gather<std::vector<char>>);
static_assert(can_gather<std::span<char>>);

} // byte_range_test

TEST_CASE("byte transfers use standard contiguous range access")
{
    const std::array fragments {std::string_view {"ab"}, std::string_view {"cd"}};
    byte_range_test::block output {};
    const auto gathered = datum::copy(fragments, output);
    CHECK(std::string_view {gathered} == "abcd");
    datum::byte_cursor input {fragments};
    input.read_into(output);
    CHECK(output.storage == std::array {'a', 'b', 'c', 'd'});
    std::array<std::array<char, 2>, 2> scattered {};
    const auto& source = output;
    CHECK(datum::scatter_bytes(source, scattered) == 4);
    CHECK(scattered == std::array {std::array {'a', 'b'}, std::array {'c', 'd'}});
}

TEST_CASE("fragment sinks observe owning temporaries and propagate failures in order")
{
    unsigned observations = 0;
    auto fragments = std::views::iota(0, 3) | std::views::transform([&](int index) {
        ++observations;
        return std::string(80, static_cast<char>('a' + index));
    });
    std::string output;
    datum::write_fragments(fragments, [&](const auto& block) {
        // A sink result is deliberately ignored, even when it resembles a stop decision.
        datum::append_range(output, block);
        return datum::acceptance::reject_and_stop;
    });
    CHECK(observations == 3);
    CHECK(output == std::string(80, 'a') + std::string(80, 'b') + std::string(80, 'c'));

    observations = 0;
    output.clear();
    CHECK_THROWS_AS(datum::write_fragments(fragments, [&](const auto& block) {
        if (block.front() == 'b') {
            throw std::runtime_error {"sink failure"};
        }
        datum::append_range(output, block);
    }), std::runtime_error);
    CHECK(observations == 2);
    CHECK(output == std::string(80, 'a'));

    observations = 0;
    std::array<char, 240> gathered {};
    datum::copy(fragments, gathered);
    CHECK(observations == 6); // One capacity pass, then one transfer pass.
    CHECK(std::string_view {gathered.data(), gathered.size()} ==
          std::string(80, 'a') + std::string(80, 'b') + std::string(80, 'c'));
}

TEST_CASE("bounded byte transfers retain mutable descriptors and stop before trailing fragments")
{
    struct block final
    {
        std::array<char, 2> bytes;
        auto begin() { return bytes.begin(); }
        auto end() { return bytes.end(); }
    };
    STATIC_REQUIRE(datum::byte_block<block>);
    STATIC_REQUIRE_FALSE(datum::byte_block<const block>);
    std::array storage {block {{'a', 'b'}}, block {{'c', 'd'}}, block {{'x', 'x'}}};
    unsigned observations = 0;
    auto fragments = storage | std::views::transform([&](auto& value) -> auto& {
        ++observations;
        return value;
    });
    CHECK(datum::scatter_bytes(std::string_view {"ABCD"}, fragments) == 4);
    CHECK(observations == 5); // Capacity sees three blocks; transfer stops after two.
    CHECK(storage[2].bytes == std::array {'x', 'x'});

    observations = 0;
    std::array<char, 6> output {};
    datum::copy(fragments, output);
    CHECK(observations == 6);
    CHECK(std::string_view {output.data(), output.size()} == "ABCDxx");
}
