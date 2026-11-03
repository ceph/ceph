// Byte vocabulary, projected comparisons, and standard-view lifetime contracts.
#include <datum/bytes.h>
#include <datum/compare.h>
#include <datum/extent.h>
#include <datum/interval/clip.h>
#include <datum/interval/metric.h>
#include <datum/plan/query.h>

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <type_traits>

#include <array>
#include <span>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include <algorithm>
#include <functional>
#include <ranges>

#include <string>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

namespace comparison_lookup_test {

struct projection final
{
    constexpr int operator()(const int value) const { return value; }
};

void ascending(projection, std::compare_three_way) = delete;
void reverse(decltype(datum::ascending(projection {}))) = delete;

} // comparison_lookup_test

namespace {

template <typename T>
concept takes_fragment_slice = requires(T& source) { datum::byte_cursor {source}.take(1); };

struct const_fragments final
{
    std::array<std::string_view, 2> blocks;

    auto begin() const { return blocks.begin(); }
    auto end() const { return blocks.end(); }
    void begin() = delete;
    void end() = delete;
};

struct named_row final
{
    std::string name;
    int epoch;
    int instance;
};

template <auto Clip>
constexpr bool mapping_contract()
{
    const datum::byte_mapping source {{4096, 1024}, 128};
    const auto selected = datum::subextent(source, {256, 128});
    const auto overlap = Clip(source, {4352, 128});

    return selected == datum::byte_mapping {{4352, 128}, 384} and overlap and *overlap == selected and
           datum::end_offset(selected.logical) == 4480 and !Clip(source, {5120, 0});
}

template <typename OutputT>
concept copies_to_temporary = requires(OutputT&& output) {
    datum::copy(std::string_view {"ab"}, 0, std::forward<OutputT>(output));
};

template <typename SourceT>
concept makes_fragments = requires(SourceT&& source) {
    datum::fragments(std::forward<SourceT>(source));
};

template <typename SourceT, typename OutputT>
concept copies_whole_source = requires(SourceT&& source, OutputT&& output) {
    datum::copy(std::forward<SourceT>(source), std::forward<OutputT>(output));
};

static_assert(makes_fragments<std::string&> and makes_fragments<std::string_view>);
static_assert(!makes_fragments<std::string> and !makes_fragments<std::array<char, 2>>);
static_assert(!makes_fragments<std::array<unsigned, 2>&>);
static_assert(copies_whole_source<std::string, std::array<char, 4>&>);
static_assert(!copies_whole_source<std::string, std::array<char, 4>>);
static_assert(!copies_whole_source<std::string, const std::array<char, 4>&>);
static_assert(!copies_whole_source<std::array<unsigned, 2>&, std::array<char, 8>&>);

static_assert([] {
    const std::array source {'a', 'b', 'c'};
    std::array<char, 4> output {'?', '?', '?', '!'};
    const auto written = datum::copy(source, output);
    const auto blocks = datum::fragments(source);
    std::array<std::byte, 4> bytes {};
    const auto gathered = datum::copy(blocks, bytes);

    return written.size() == 3 and written.data() == output.data() and output.back() == '!' and
           gathered.size() == 3 and bytes[2] == std::byte {'c'};
}());

static_assert([] {
    const std::array<std::string_view, 2> source {"abc", "def"};
    std::array<char, 3> output {};
    datum::copy(source, datum::byte_extent {2, 3}, output);
    return output == std::array {'c', 'd', 'e'};
}());
static_assert(!copies_to_temporary<std::array<char, 2>>);

static_assert(mapping_contract<datum::clip>());
static_assert(mapping_contract<datum::detail::clip_mapping<true>>());
static_assert(mapping_contract<datum::detail::clip_mapping<false>>());
static_assert(datum::lexicographic()(0, 1) == 0);

} // anonymous namespace

TEST_CASE("whole-source copies preserve output tails and preflight fragmented capacity")
{
    const std::array<std::string_view, 3> source {"ab", "", "cd"};
    std::array<char, 6> output {'?', '?', '?', '?', '!', '!'};
    const auto written = datum::copy(source, output);
    CHECK(std::string_view {written.data(), written.size()} == "abcd");
    CHECK(output[4] == '!');
    CHECK(output[5] == '!');
    std::array<char, 3> short_output {'?', '?', '?'};
    CHECK_THROWS_AS(datum::copy(source, short_output), std::length_error);
    CHECK(short_output == std::array {'?', '?', '?'});
    CHECK_THROWS_AS(datum::copy(std::string_view {"abcd"}, short_output), std::length_error);
    CHECK(short_output == std::array {'?', '?', '?'});

    const_fragments read_only {{"a", "b"}};
    CHECK(datum::copy(read_only, output).size() == 2);
    CHECK(datum::copy(std::string {"xy"}, output).size() == 2);
    CHECK(output[0] == 'x');
    CHECK(output[1] == 'y');
    CHECK(datum::copy(std::views::empty<char>, output).empty());
    CHECK(output[0] == 'x');
    CHECK(datum::copy("z", output).size() == 2);
    CHECK(output[0] == 'z');
    CHECK(output[1] == '\0');

    const std::array seeds {0, 1};
    unsigned observations = 0;
    auto generated = seeds | std::views::transform([&observations](int value) {
        ++observations;
        return std::string(2, static_cast<char>('a' + value));
    });
    CHECK(datum::copy(generated, output).size() == 4);
    CHECK(std::string_view {output.data(), 4} == "aabb");
    CHECK(observations == 4); // One capacity pass, then one transfer pass.
}

TEST_CASE("contiguous fragment views borrow bytes and preserve element constness")
{
    std::array<char, 3> source {'a', 'b', 'c'};
    auto blocks = datum::fragments(source);
    STATIC_REQUIRE(std::ranges::forward_range<decltype(blocks)>);
    STATIC_REQUIRE_FALSE(std::ranges::borrowed_range<decltype(blocks)>);
    STATIC_REQUIRE(std::same_as<std::ranges::range_value_t<decltype(blocks)>, std::span<char>>);
    CHECK(std::ranges::distance(blocks) == 1);
    CHECK(blocks.front().data() == source.data());
    CHECK(datum::scatter_bytes(std::string_view {"ABC"}, blocks) == 3);
    CHECK(source == std::array {'A', 'B', 'C'});
    auto read_only = datum::fragments(std::as_const(source));
    STATIC_REQUIRE(std::same_as<std::ranges::range_value_t<decltype(read_only)>, std::span<const char>>);
    auto from_span = datum::fragments(std::span {source});
    CHECK(from_span.front().data() == source.data());

    volatile unsigned char observed[2] {128, 255};
    auto volatile_blocks = datum::fragments(std::span {observed});
    std::array<std::byte, 3> output {};
    CHECK(datum::copy(volatile_blocks, output).size() == 2);
    CHECK(output[0] == std::byte {128});
    CHECK(output[1] == std::byte {255});
}

TEST_CASE("built-in byte metric counts coverage and retains checked arithmetic")
{
    const std::array ranges {iv::byte_query::closed_open(0, 3),
                             iv::byte_query::closed_open(10, 15)};
    const auto coverage = iv::compile_intervals(ranges);
    CHECK(iv::measure(coverage, iv::bytes) == 8);
    const auto selected = iv::take(coverage, std::uint64_t {5}, iv::bytes);
    STATIC_REQUIRE(iv::canonical_sequence<decltype(selected)>);
    CHECK(iv::measure(selected, iv::bytes) == 5);
    CHECK(iv::contains(selected, std::uint64_t {11}));
    CHECK_FALSE(iv::contains(selected, std::uint64_t {12}));
    CHECK_THROWS_AS(iv::bytes.add(std::numeric_limits<std::uint64_t>::max(), 1), std::overflow_error);
}

TEST_CASE("byte slices reject owning temporary blocks without rejecting synchronous copies")
{
    const std::array seeds {0};
    const auto arrays = seeds | std::views::transform([](int) { return std::array {'a', 'b'}; });
    STATIC_REQUIRE(datum::byte_fragments<decltype(arrays)>);
    STATIC_REQUIRE_FALSE(takes_fragment_slice<decltype(arrays)>);
    using unsafe_slice = datum::byte_slice_view<std::ranges::iterator_t<const decltype(arrays)>>;
    STATIC_REQUIRE_FALSE(std::ranges::range<unsafe_slice>);

    std::array<char, 2> output {};
    datum::copy(arrays, std::span {output});
    CHECK(output == std::array {'a', 'b'});
    datum::byte_cursor cursor {arrays};
    cursor.read_into(std::span {output});
    CHECK(output == std::array {'a', 'b'});

    const std::array backing {'c', 'd'};
    const auto spans =
        seeds | std::views::transform([&backing](int) { return std::span {backing}; });
    STATIC_REQUIRE(takes_fragment_slice<decltype(spans)>);
    datum::byte_cursor borrowed {spans};
    const auto selected = borrowed.take(2);
    datum::copy(selected, std::span {output});
    CHECK(output == backing);
}

TEST_CASE("byte transfers support cached views and const-only source descriptors")
{
    std::array<std::string_view, 3> blocks {"a", "", "bc"};
    auto filtered = blocks | std::views::filter([](auto block) { return !block.empty(); });
    STATIC_REQUIRE_FALSE(std::ranges::range<const decltype(filtered)>);
    STATIC_REQUIRE(datum::byte_fragments<decltype(filtered)>);
    std::array<char, 4> output {'x', 'x', 'x', '!'};
    const auto written = datum::copy(filtered, std::span {output});
    CHECK(std::string_view {written.data(), written.size()} == "abc");
    CHECK(output.back() == '!');
    datum::byte_cursor input {filtered};
    input.consume(1);
    input.read_into(std::span {output}.first(2));
    CHECK(output[0] == 'b');
    CHECK(output[1] == 'c');

    const_fragments source {{"x", "y"}};
    STATIC_REQUIRE_FALSE(std::ranges::range<const_fragments>);
    STATIC_REQUIRE(datum::byte_fragments<const_fragments>);
    CHECK(datum::byte_capacity(source) == 2);
    datum::copy(source, std::span {output});
    CHECK(output[0] == 'x');
    CHECK(output[1] == 'y');

    std::array<std::string, 2> destinations {"xxx", "x"};
    auto writable =
        destinations | std::views::filter([](const auto& value) { return !value.empty(); });
    const std::array bytes {'a', 'b', 'c'};
    CHECK(datum::scatter_bytes(std::span {bytes}, writable) == 3);
    CHECK(destinations[0] == "abc");
    CHECK(destinations[1] == "x");
}

TEST_CASE("filtered clip sources retain their source association and admission")
{
    using query = iv::query<iv::comparison_domain<int>>;
    const std::array rows {query::closed_open(0, 4), query::closed_open(8, 12)};
    auto filtered =
        rows | std::views::filter([](const query& row) { return row.lower().key() == 0; });
    const auto wanted = iv::compile_intervals(std::array {query::closed_open(1, 10)});
    const iv::clip_view cuts {filtered, wanted, std::identity {}};
    unsigned count = 0;
    for (const auto cut : cuts) {
        STATIC_REQUIRE(iv::interval_view<decltype(cut)>);
        CHECK(std::addressof(cut.source()) == std::addressof(rows[0]));
        CHECK(cut.lower().key() == 1);
        CHECK(cut.upper().key() == 4);
        ++count;
    }
    CHECK(count == 1);
}

TEST_CASE("mapped subextents agree with independently enumerated byte pairs")
{
    // Exercise both internal spellings even when this compiler selects only one:
    const std::array implementations {datum::clip, datum::detail::clip_mapping<true>,
                                      datum::detail::clip_mapping<false>};
    for (const auto clip : implementations) {
        for (std::uint64_t first = 0; first < 8; ++first) {
            for (std::uint64_t length = 0; length <= 8 - first; ++length) {
                for (std::uint64_t start = 0; start < 9; ++start) {
                    const datum::byte_mapping source {{first, length}, 20};
                    const auto result = clip(source, {start, 2});
                    std::vector<std::pair<std::uint64_t, std::uint64_t>> expected;
                    for (std::uint64_t index = 0; index < length; ++index) {
                        if (first + index >= start and first + index < start + 2) {
                            expected.emplace_back(first + index, 20 + index);
                        }
                    }
                    if (expected.empty()) {
                        CHECK_FALSE(result);
                        continue;
                    }
                    REQUIRE(result);
                    CHECK(result->logical.offset == expected.front().first);
                    CHECK(result->backing_offset == expected.front().second);
                    CHECK(result->logical.length == expected.size());
                }
            }
        }

        constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
        CHECK(datum::end_offset(datum::byte_extent {maximum, 0}) == maximum);
        CHECK_THROWS_AS(datum::end_offset(datum::byte_extent {maximum, 1}), std::overflow_error);
        CHECK_THROWS_AS(datum::subextent(datum::byte_extent {2, 3}, {3, 1}), std::out_of_range);
        CHECK_THROWS_AS(clip(datum::byte_mapping {{0, 2}, maximum}, {8, 0}), std::overflow_error);
        CHECK_THROWS_AS(clip(datum::byte_mapping {{maximum, 1}, 0}, {8, 0}), std::overflow_error);
        CHECK_THROWS_AS(clip(datum::byte_mapping {{0, 0}, 0}, {maximum, 1}), std::overflow_error);
        CHECK_FALSE(clip(datum::byte_mapping {{maximum, 0}, maximum}, {maximum, 0}));
        CHECK(clip(datum::byte_mapping {{maximum - 4, 4}, maximum - 4}, {maximum - 2, 2}) ==
              datum::byte_mapping {{maximum - 2, 2}, maximum - 2});
    }
}

TEST_CASE("comparison composition agrees with a mixed-direction tuple reference")
{
    std::vector<named_row> rows {{"b", 1, 0}, {"a", 1, 2}, {"a", 3, 1}, {"a", 3, 0}};
    const auto order = datum::lexicographic(datum::ascending(&named_row::name),
                                            datum::reverse(datum::ascending(&named_row::epoch)),
                                            datum::ascending(&named_row::instance));

    for (const auto& left : rows) {
        for (const auto& right : rows) {
            const auto expected = std::tuple {left.name, -left.epoch, left.instance} <=>
                                  std::tuple {right.name, -right.epoch, right.instance};
            CHECK(order(left, right) == expected);
        }
    }
    std::ranges::sort(rows, datum::order_by(order));
    CHECK(rows[0].name == "a");
    CHECK(rows[0].epoch == 3);
    CHECK(rows[0].instance == 0);
    CHECK(rows.back().name == "b");

    unsigned later = 0;
    const auto partial = datum::lexicographic(std::compare_three_way {}, [&later](double, double) {
        ++later;
        return std::strong_ordering::equal;
    });
    const auto nan = std::numeric_limits<double>::quiet_NaN();
    CHECK(partial(nan, 0.0) == std::partial_ordering::unordered);
    CHECK(later == 0);
    const auto less = datum::order_by(partial);
    STATIC_REQUIRE_FALSE(std::invocable<decltype(less), double, double>);

    const auto reversed = datum::reverse(std::less<> {});
    CHECK(reversed(2, 1));
    CHECK_FALSE(reversed(1, 1));
    STATIC_REQUIRE_FALSE(std::invocable<decltype(datum::nth_key<0>), int>);
    STATIC_REQUIRE(
        std::same_as<decltype(datum::nth_key<0>(std::declval<const std::pair<int, int>&>())),
                     const int&>);
    STATIC_REQUIRE(
        std::same_as<decltype(datum::nth_key<0>(std::declval<std::pair<int, int>&&>())), int&&>);
    auto pairs = std::array {std::pair {1, 2}, std::pair {0, 3}};
    std::ranges::sort(pairs, {}, datum::nth_key<0>);
    CHECK(pairs.front().first == 0);
}

TEST_CASE("projected comparisons borrow noncopyable providers and evaluate each key once")
{
    struct projection final
    {
        unsigned calls = 0;

        projection() = default;
        projection(const projection&) = delete;
        int operator()(const int value)
        {
            ++calls;
            return value;
        }
    };
    projection key;
    const auto order = datum::ascending(std::ref(key));
    CHECK(order(1, 2) < 0);
    CHECK(key.calls == 2);
}

TEST_CASE("order_by shares the comparison engine and handles mixed directions at integer limits")
{
    std::vector<named_row> rows {{"b", 1, 0}, {"a", std::numeric_limits<int>::min(), 2},
                                 {"a", std::numeric_limits<int>::max(), 1},
                                 {"a", std::numeric_limits<int>::max(), 0}};
    const auto composed = datum::order_by(datum::lexicographic(
        datum::ascending(&named_row::name), datum::reverse(datum::ascending(&named_row::epoch)),
        datum::ascending(&named_row::instance)));
    const auto order = datum::order_by(datum::ascending(&named_row::name),
        datum::descending(&named_row::epoch), datum::ascending(&named_row::instance));
    STATIC_REQUIRE(std::same_as<decltype(order), decltype(composed)>);
    for (const auto& left : rows) {
        for (const auto& right : rows) {
            CHECK(order(left, right) == composed(left, right));
        }
    }
    std::ranges::sort(rows, order);
    CHECK(rows[0].name == "a");
    CHECK(rows[0].epoch == std::numeric_limits<int>::max());
    CHECK(rows[0].instance == 0);
    CHECK(rows[2].epoch == std::numeric_limits<int>::min());
    CHECK(rows.back().name == "b");
    STATIC_REQUIRE(datum::order_by()(1, 2) == false);
    STATIC_REQUIRE(datum::order_by(datum::ascending(std::identity {}))(1, 2));
    STATIC_REQUIRE(datum::order_by(datum::descending(std::identity {}))(2, 1));
}

TEST_CASE("sorting conveniences retain borrowed providers and owned mutable projections")
{
    struct projection final
    {
        unsigned calls = 0;
        projection() = default;
        projection(const projection&) = delete;
        int operator()(const int value) { ++calls; return value; }
    };
    projection primary;
    unsigned later = 0;
    const auto order = datum::order_by(datum::ascending(std::ref(primary)),
        datum::descending([&later](int value) { ++later; return value; }));
    CHECK(order(1, 2));
    CHECK(primary.calls == 2);
    CHECK(later == 0);
    CHECK_FALSE(order(1, 1));
    CHECK(primary.calls == 4);
    CHECK(later == 2);

    auto state = std::make_unique<unsigned>(0);
    const auto observed = state.get();
    auto mutable_order = datum::order_by(datum::ascending(
        [state = std::move(state)](int value) mutable { ++*state; return value; }));
    STATIC_REQUIRE_FALSE(std::copy_constructible<decltype(mutable_order)>);
    STATIC_REQUIRE_FALSE(std::invocable<const decltype(mutable_order)&, int, int>);
    CHECK(mutable_order(1, 2));
    CHECK(*observed == 2);
}

TEST_CASE("sorting floating projections requires an explicit nonpartial ordering")
{
    const auto partial = datum::order_by(datum::ascending(std::identity {}));
    STATIC_REQUIRE_FALSE(std::invocable<decltype(partial), double, double>);
    const auto total = datum::order_by(datum::ascending(std::identity {}, std::strong_order));
    STATIC_REQUIRE(std::invocable<decltype(total), double, double>);
    std::array values {std::numeric_limits<double>::quiet_NaN(), 2.0, -1.0, 0.0};
    std::ranges::sort(values, total);
    CHECK(std::ranges::is_sorted(values, total));
}

TEST_CASE("sorting helpers use Datum composition despite unrelated ADL names")
{
    const auto increasing = datum::ascending(comparison_lookup_test::projection {});
    const auto decreasing = datum::descending(comparison_lookup_test::projection {});
    CHECK(increasing(1, 2) < 0);
    CHECK(decreasing(1, 2) > 0);
    const auto adapted = datum::order_by(std::compare_three_way {});
    const auto sorted = datum::order_by(std::compare_three_way {});
    STATIC_REQUIRE(std::same_as<decltype(adapted), decltype(sorted)>);
    CHECK(adapted(1, 2));
    CHECK_FALSE(adapted(2, 1));
    CHECK_FALSE(adapted(1, 1));
}

TEST_CASE("boolean lexicographic comparisons retain borrowing and stop at the first decision")
{
    struct primary_order final
    {
        unsigned calls = 0;

        primary_order() = default;
        primary_order(const primary_order&) = delete;

        auto operator()(const int left, const int right)
        {
            ++calls;
            return left <=> right;
        }
    };

    primary_order primary;
    unsigned later = 0;
    const auto less = datum::order_by(datum::lexicographic(std::ref(primary), [&later](int, int) {
        ++later;
        return std::weak_ordering::equivalent;
    }));

    CHECK(less(1, 2));
    CHECK(primary.calls == 1);
    CHECK(later == 0);
    CHECK_FALSE(less(2, 1));
    CHECK(primary.calls == 2);
    CHECK(later == 0);
    CHECK_FALSE(less(1, 1));
    CHECK(primary.calls == 3);
    CHECK(later == 1);

    struct unobserved final
    {
    };
    constexpr auto empty = datum::order_by(datum::lexicographic());
    STATIC_REQUIRE_FALSE(empty(unobserved {}, unobserved {}));

    int observed_calls = 0;
    auto mutable_less = datum::order_by(datum::lexicographic(
        [calls = 0, &observed_calls](const int left, const int right) mutable {
            observed_calls = ++calls;
            return left <=> right;
        }));
    STATIC_REQUIRE(std::invocable<decltype(mutable_less)&, int, int>);
    STATIC_REQUIRE_FALSE(std::invocable<const decltype(mutable_less)&, int, int>);
    CHECK(mutable_less(1, 2));
    CHECK(observed_calls == 1);
    CHECK(mutable_less(1, 2));
    CHECK(observed_calls == 2);
}

TEST_CASE("bounded copies preflight source and output and work with ordinary storage")
{
    const std::array<std::string_view, 4> source {"ab", "", "cde", "f"};
    std::array<char, 4> output {'!', '!', '!', '!'};
    const auto written = datum::copy(source, datum::byte_extent {1, 3}, output);
    CHECK(std::string_view {written.data(), written.size()} == "bcd");
    CHECK(output.back() == '!');
    CHECK_THROWS_AS(datum::copy(source, datum::byte_extent {5, 2}, output), std::out_of_range);
    CHECK(std::string_view {output.data(), output.size()} == "bcd!");
    CHECK_THROWS_AS(datum::copy(source, datum::byte_extent {0, 5}, output), std::length_error);
    CHECK(std::string_view {output.data(), output.size()} == "bcd!");
    datum::copy(std::string_view {"012345"}, 2, output);
    CHECK(std::string_view {output.data(), output.size()} == "2345");
    CHECK(datum::copy(source, datum::byte_extent {6, 0}, output).empty());
    CHECK_THROWS_AS(datum::copy(source, datum::byte_extent {7, 0}, output), std::out_of_range);
}

TEST_CASE("byte mapping members project into clipping without local domains")
{
    const std::array rows {datum::byte_mapping {{0, 4}, 20}, datum::byte_mapping {{8, 4}, 40}};
    const auto wanted = iv::compile_intervals(iv::bounds({2, 8}));
    const iv::clip_view cuts {rows, wanted, &datum::byte_mapping::logical};
    std::vector<datum::byte_mapping> result;
    for (const auto cut : cuts) {
        STATIC_REQUIRE(iv::interval_view<decltype(cut)>);
        const auto selected =
            datum::clip(cut.source(), {cut.lower().key(), cut.upper().key() - cut.lower().key()});
        REQUIRE(selected);
        result.push_back(*selected);
    }
    CHECK(result == std::vector<datum::byte_mapping> {{{2, 2}, 22}, {{8, 2}, 40}});
}

TEST_CASE("synchronous sinks borrow providers and ignore unused result types")
{
    struct sink final
    {
        std::string result;
        sink() = default;
        sink(const sink&) = delete;
        std::string& operator()(const std::string_view block) { return result.append(block); }
    };
    const_fragments source {{"x", "y"}};
    sink output;
    datum::write_fragments(source, output);
    CHECK(output.result == "xy");
}

TEST_CASE("weak key equivalence remains distinct from whole-record equality")
{
    struct record final
    {
        int identity;
        int payload;
        bool operator==(const record&) const = default;
    };
    const record left {1, 20};
    const record right {1, 30};
    const auto order = datum::lexicographic(datum::ascending(&record::identity, std::weak_order));
    STATIC_REQUIRE(std::same_as<decltype(order(left, right)), std::weak_ordering>);
    CHECK(left != right);
    CHECK(order(left, right) == std::weak_ordering::equivalent);
    CHECK_FALSE(datum::order_by(order)(left, right));
    CHECK_FALSE(datum::order_by(order)(right, left));
}

namespace {

struct aliases_only_plan final
{
    using interval_expression_type = iv::query<iv::comparison_domain<int>>;
    using requirement_tuple_type = std::tuple<>;
    using residual_tuple_type = std::tuple<>;
    using control_tuple_type = std::tuple<>;
};

} // anonymous namespace

TEST_CASE("plain query members suffice and composition preserves references and ownership")
{
    using query = iv::query<iv::comparison_domain<int>>;
    struct plain_plan final
    {
        query intervals;
        std::tuple<int&> requirements;
        std::tuple<std::unique_ptr<int>> residuals;
        std::tuple<> controls;
    };
    STATIC_REQUIRE(datum::plan::query<plain_plan>);
    STATIC_REQUIRE_FALSE(datum::plan::query<aliases_only_plan>);
    STATIC_REQUIRE_FALSE(datum::plan::resolved_query<plain_plan>);

    int requirement = 4;
    plain_plan original {query::closed(1, 8), {requirement}, {std::make_unique<int>(9)}, {}};
    const auto address = std::get<0>(original.residuals).get();
    const auto composed = datum::plan::with_control(std::move(original), std::string {"limit"});
    CHECK(datum::plan::requirement_count(composed) == 1);
    CHECK(datum::plan::residual_count(composed) == 1);
    CHECK(datum::plan::control_count(composed) == 1);
    CHECK(std::get<0>(composed.residuals).get() == address);
    requirement = 7;
    CHECK(std::get<0>(composed.requirements) == 7);
    CHECK(datum::plan::control<std::string>(composed) == "limit");
    CHECK(iv::contains(composed.intervals, 5));
}
