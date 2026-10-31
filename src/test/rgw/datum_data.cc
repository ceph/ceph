// Datum reflected records, ownership, diagnostics and optional codecs tests.

#include "common/container_concepts.h"
#include <catch2/catch_test_macros.hpp>
#include <datum/budget.h>
#include <datum/bytes.h>
#include <datum/interval/clip.h>
#include <datum/interval/metric.h>
#include <datum/interval/query.h>
#include <datum/interval/range.h>
#include <datum/key.h>
#include <datum/ownership.h>
#include <datum/plan/query.h>
#include <datum/record.h>
#include <datum/render/datum.h>
#include <datum/render/display.h>
#include <datum/render/fmt.h>
#include <datum/traversal.h>
#include <datum/traversal/scan.h>

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <expected>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <array>
#include <map>
#include <set>
#include <vector>

#include <memory>
#include <memory_resource>

#include <algorithm>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <ranges>

#include <format>
#include <sstream>
#include <string>
#include <string_view>

#ifdef DATUM_HAVE_ZPP
 #include <datum/codec/zpp.h>

#endif

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

// Datum data mechanics: neutral fragmentation, retention and later data profiles.
// These fixtures keep all native storage, codecs and execution outside Datum.

template <typename ViewT, typename ConsumerT>
constexpr void consume_cuts(const ViewT& cuts, ConsumerT&& consumer)
{
    std::ranges::for_each(cuts, std::ref(consumer));
}

constexpr bool byte_spellings()
{
    const std::array<signed char, 3> source {-128, -1, 0};
    const std::array fragments {std::span {source}.first(1), std::span {source}.subspan(1)};
    std::array<std::byte, 3> decoded {};
    datum::gather_bytes(fragments, std::span {decoded});
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
    datum::gather_bytes(fragments, std::span {result});
    CHECK(static_cast<unsigned char>(result[0]) == 128);
    CHECK(static_cast<unsigned char>(result[1]) == 255);
}

struct mechanics_row
{
    std::string key;
    std::vector<unsigned char> payload;
};
struct mechanics_pair
{
    int first;

    int second;
};
template <typename T>
struct mechanics_domain
{
    using value_type = T;

public:
    static constexpr std::strong_ordering compare(const T& left, const T& right)
    {
        return left <=> right;
    }

};
using row_fields = datum::member_fields<&mechanics_row::key, &mechanics_row::payload>;
using reversed_fields = datum::member_fields<&mechanics_pair::second, &mechanics_pair::first>;

static_assert(reversed_fields::covers<mechanics_pair>());
static_assert(!datum::member_fields<&mechanics_pair::first,
                                    &mechanics_pair::first>::covers<mechanics_pair>());
template <auto Member>
concept valid_member_profile = requires { typename datum::member_fields<Member>; };
static_assert(!valid_member_profile<static_cast<int mechanics_pair::*>(nullptr)>);
static_assert([] {
    int sequence = 0;
    const auto value =
        reversed_fields::reconstruct<mechanics_pair>([&](auto) { return ++sequence; });

    return value.first == 2 && value.second == 1;
}());

template <typename T>
concept temporary_cursor = requires { datum::byte_cursor {T {}}; };
template <typename T>
concept temporary_projection = requires(T value) { std::move(value).get(); };
static_assert(!temporary_cursor<std::array<std::string_view, 1>>);
static_assert(!temporary_projection<datum::retained_view<std::string, datum::byte_slice>>);

static_assert([] {
    const std::array<char, 4> bytes {'a', 'b', 'c', 'd'};
    const std::array fragments {std::span {bytes}.first(1), std::span {bytes}.subspan(1)};
    datum::byte_cursor input {fragments};
    const auto saved = input.mark();
    const auto slice = input.take(3);
    std::array<char, 3> result {};
    datum::gather_bytes(slice, std::span {result});
    input.restore(saved);

    return result == std::array {'a', 'b', 'c'} && 0 == input.position();
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
    static_assert(std::same_as<std::ranges::range_value_t<decltype(slice)>,
                               std::span<const volatile unsigned char>>);
    std::array<std::byte, 3> output {};
    datum::gather_bytes(slice, std::span {output});
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
                               if (fail && std::data(block) == std::data(bytes) + 1) {
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
    CHECK_THROWS_AS(datum::gather_bytes(source, std::span {storage}.first(3)), std::length_error);
    CHECK(storage == std::array {'x', 'x', 'x', 'x'});
    const auto bytes = datum::gather_bytes(source, std::span {storage});
    CHECK(std::string_view {bytes.data(), bytes.size()} == "abcd");
    datum::byte_cursor<const read_only_fragments&> cursor {source};
    std::array<char, 2> prefix;
    cursor.read_into(std::span {prefix});
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

TEST_CASE("one field layout reconstructs and moves final owners")
{
    const auto row = row_fields::reconstruct<mechanics_row>([](auto field) {
        using value_type = typename decltype(field)::value_type;

        if constexpr (std::same_as<value_type, std::string>) {
            return std::string(128, 'k');
        }

        if constexpr (!std::same_as<value_type, std::string>) {
            return std::vector<unsigned char>(256, 7);
        }
    });
    CHECK(std::size(row.key) == 128);
    CHECK(std::size(row.payload) == 256);
    auto source = row;
    const auto *key_address = std::data(source.key);
    const auto *payload_address = std::data(source.payload);
    const auto target = row_fields::project<mechanics_row>(std::move(source));
    CHECK(std::data(target.key) == key_address);
    CHECK(std::data(target.payload) == payload_address);
    CHECK_THROWS_AS(row_fields::reconstruct<mechanics_row>(
                        [](auto field) -> typename decltype(field)::value_type {
                            throw std::runtime_error {"bad field"};
                        }),
                    std::runtime_error);
    const auto explicit_record = reversed_fields::construct(
        [](auto) { return 4; },
        [](int second, int first) { return mechanics_pair {first, second}; });
    CHECK(explicit_record.first == 4);
}

static_assert(datum::owning_value<std::map<std::string, std::vector<int>>>);
static_assert(datum::owning_value<std::tuple<int, std::unique_ptr<std::string>>>);
static_assert(datum::owning_value<std::expected<void, std::string>>);
static_assert(!datum::owning_value<std::tuple<int&>>);
static_assert(!datum::owning_value<std::pmr::vector<int>>);
static_assert(!datum::owning_value<std::set<int, std::greater<int>>>);
static_assert(!datum::owning_value<std::shared_ptr<std::string>>);
static_assert([] {
    datum::unlimited_budget budget;
    auto nesting = budget.enter_depth();
    budget.charge(datum::resource::work, 1);

    return true;
}());

static_assert([] {
    datum::resource_budget budget {{.elements = 3, .depth = 2}};
    budget.charge(datum::resource::elements, 3);
    {
        auto scope = budget.enter_depth();
    }

    return budget.used(datum::resource::elements) == 3 && budget.used(datum::resource::depth) == 0;
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
    CHECK_FALSE(unbounded.try_charge(datum::resource::work,
                                     std::numeric_limits<std::size_t>::max()));
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
        return admit(budget) && !admit(budget);
    }());
    constexpr datum::unlimited_budget unlimited;
    static_assert(admit(unlimited));
    static_assert(noexcept(unlimited.try_charge(datum::resource::elements, 1)));
}

TEST_CASE("scan distinguishes skipped consumed and rejected rows")
{
    using domain = mechanics_domain<int>;
    using bound = datum::interval::boundary<domain>;

    const std::array rows {1, 2, 3, 4};
    const auto positions =
        datum::traversal::ordered_positions {[](const int row) { return bound::closed(row); }};
    const auto partial = datum::traversal::scan_range(
        rows,
        [](const int row) {
            if (1 == row) {
                return datum::acceptance::skip;
            }
            if (3 == row) {
                return datum::acceptance::reject_and_stop;
            }

            return datum::acceptance::accept;
        },
        positions);
    CHECK(partial.progress.offered == 3);
    CHECK(partial.progress.consumed == 2);
    CHECK(partial.progress.accepted == 1);
    CHECK(partial.progress.stopped);
    REQUIRE(partial.resume);
    CHECK(partial.resume->key() == 3);
    CHECK(partial.resume->inclusive());
    const auto finished =
        datum::traversal::scan_range(rows, [](int) { return datum::acceptance::skip; }, positions);
    CHECK(finished.progress.consumed == 4);
    CHECK(finished.progress.accepted == 0);
    CHECK_FALSE(finished.resume);

    struct opaque_positions
    {
        std::size_t position(int row) const { return row; }

        std::size_t next(std::size_t position, bool consumed) const
        {
            return position + consumed;
        }

    };
    const auto opaque = datum::traversal::scan_range(
        rows, [](int) { return datum::acceptance::accept_and_stop; }, opaque_positions {});
    CHECK(opaque.resume == 2);
}

TEST_CASE("scan saves a continuation before row ownership moves")
{
    using domain = mechanics_domain<std::string>;
    using bound = datum::interval::boundary<domain>;

    std::array rows {mechanics_row {std::string(128, 'k'), {1, 2}}};
    mechanics_row destination;
    const auto scan = datum::traversal::scan_range(
        rows,
        [&](mechanics_row& row) {
            destination = std::move(row);

            return datum::acceptance::accept_and_stop;
        },
        datum::traversal::ordered_positions {
            [](const mechanics_row& row) { return bound::closed(row.key); }});
    REQUIRE(scan.resume);
    CHECK(scan.resume->key() == destination.key);
    CHECK_FALSE(scan.resume->inclusive());
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
                    const iv::canonical_set selection {
                        integer_geometry::between(lower_bound, upper_bound)};
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
                                (point > 2 * lower || (lower_closed && point == 2 * lower)) &&
                                (point < 2 * upper || (upper_closed && point == 2 * upper));
                            const bool present = origin == 0   ? point < 4
                                                 : origin == 1 ? point >= 4 && point < 8
                                                               : point >= 10;
                            CHECK(actual[origin][point] == (selected && present));
                            CHECK(direct[origin][point] == (selected && present));
                        }
                    }
                }
            }
        }
    }
    const iv::canonical_set holes {
        iv::set_union(integer_geometry::closed_open(1, 3), integer_geometry::closed_open(5, 7))};
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
    const iv::canonical_view selection {selected};
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
        const iv::canonical_set selection {Q::universal()};
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
        iv::canonical_set {Q::universal()}, iv::canonical_set {Q::empty()},
        iv::canonical_set {Q::singleton(0)}, iv::canonical_set {Q::closed(-4, 4)},
        iv::canonical_set {std::array {Q::between(B::negative_infinity(), B::closed(-6)),
                                       Q::closed_open(-3, -1), Q::singleton(0), Q::open(2, 6),
                                       Q::between(B::open(8), B::positive_infinity())}}};
    const iv::canonical_set prepared_source {rows};
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
                      (iv::contains(rows[origin], point) && iv::contains(selection, point)));
                CHECK(direct[origin][point + 10] ==
                      (iv::contains(rows[origin], point) && iv::contains(selection, point)));
            }
        }
        const iv::clip_view prepared {prepared_source, selection, std::identity {}};
        for (int point = -10; point <= 10; ++point) {
            const bool present = std::ranges::any_of(
                prepared, [point](const auto cut) { return iv::contains(cut.bounds(), point); });
            CHECK(present ==
                  (iv::contains(prepared_source, point) && iv::contains(selection, point)));
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
    const iv::canonical_view source {rows};
    const iv::canonical_set selection {std::array {Q::closed_open(1, 3), Q::closed_open(9, 11)}};
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
    const iv::canonical_view empty_source {no_rows};
    const iv::clip_view empty {empty_source, selection, std::identity {}};

    CHECK(std::begin(empty) == std::end(empty));
}

static_assert([] {
    const std::array rows {integer_geometry::closed_open(0, 4),
                           integer_geometry::closed_open(8, 12)};
    const iv::canonical_view source {rows};
    const iv::canonical_set selection {integer_geometry::closed_open(1, 11)};
    const iv::clip_view cuts {source, selection, std::identity {}};

    return std::ranges::fold_left(cuts, 0, [](int sum, const auto cut) {
               return sum + cut.bounds().upper().key() - cut.bounds().lower().key();
           }) == 6;
}());

TEST_CASE("clipping retains one projection across repeated cuts and dereferences")
{
    using Q = integer_geometry;

    const std::array rows {Q::closed(0, 10)};
    const iv::canonical_set selection {iv::set_union(Q::closed_open(1, 3), Q::closed_open(5, 7))};
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
    const iv::canonical_set selection {
        std::array {Q::closed_open(1, 3), Q::closed_open(5, 7), Q::closed_open(13, 15)}};
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
                     [](const auto) -> void { throw std::runtime_error {"consumer failed"}; }),
        std::runtime_error);
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
    const iv::canonical_view prepared {selection};
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
    const iv::canonical_set selection {Q::closed_open(1, 3)};
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
    const iv::canonical_view selection {intervals};
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
    const iv::canonical_view selection {intervals};
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
    const iv::canonical_view selection {intervals};
    const iv::clip_view borrowed {rows, selection, [](const Q& row) -> const Q& { return row; }};
    address::copies = 0;
    unsigned visited = 0;
    consume_cuts(borrowed, [&visited](const auto cut) {
        CHECK_FALSE(iv::is_empty(cut.bounds()));
        ++visited;
    });
    CHECK(visited == 3);
    CHECK(address::copies == 0);
    const iv::canonical_view prepared {rows};
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
    const iv::canonical_set selection {Q::universal()};
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
    const iv::canonical_view coverage {intervals};
    const auto selected = iv::take(coverage, byte_quantity {7}, address_metric {});

    return iv::measure(selected, address_metric {}).value == 7 &&
           std::ranges::next(std::begin(selected))->upper().key().value == 13;
}());

TEST_CASE("metrics count covered units rather than coordinate width")
{
    using domain = mechanics_domain<unsigned>;
    using Q = iv::query<domain>;

    const std::array intervals {Q::closed_open(0, 4), Q::closed_open(10, 18)};
    const iv::canonical_view coverage {intervals};
    const iv::unsigned_metric<unsigned> metric;
    CHECK(iv::measure(coverage, metric) == 12);
    const auto selected = iv::take(coverage, 7, metric);
    CHECK(iv::measure(selected, metric) == 7);
    CHECK(std::ranges::next(std::begin(selected))->upper().key() == 13);
    CHECK(std::empty(iv::take(coverage, 0, metric)));
    CHECK(iv::measure(iv::take(coverage, 100, metric), metric) == 12);
    CHECK_THROWS_AS(metric.advance(std::numeric_limits<unsigned>::max(), 1), std::overflow_error);
    const iv::canonical_set unsupported {Q::closed(0, 4)};
    CHECK_THROWS_AS(iv::measure(unsupported, metric), std::domain_error);
    const iv::canonical_set infinite {Q {}};
    CHECK_THROWS_AS(iv::take(infinite, 3, metric), std::domain_error);

    // A value geometry can borrow source endpoints; no default/assignment on points.
    struct row
    {
        address first;
        address last;
    };
    const std::array rows {row {address {0}, address {8}}};
    const iv::canonical_set wanted {
        iv::query<address_domain>::closed_open(address {2}, address {6})};
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
    const iv::canonical_view coverage {intervals};
    CHECK(12 == iv::measure(coverage, measurement {}).value);
    const auto selected = iv::take(coverage, 7, quota {});
    CHECK(7 == iv::measure(selected, measurement {}).value);
    CHECK(13 == std::ranges::next(std::begin(selected))->upper().key());
}

class constructed_pair final
{
    int first;

    int second;

public:
    constructed_pair() = delete;
    constexpr constructed_pair(int left, int right) : first(left), second(right)
    {}

public:
    constexpr int sum() const { return first + second; }

};
template <>
struct ceph::salad::datum::ownership_traits<constructed_pair> : std::true_type
{};
static_assert([] {
    const auto value = reversed_fields::construct(
        [](auto) { return 3; },
        [](int second, int first) { return constructed_pair {first, second}; });

    return value.sum() == 6;
}());

template <typename RowsT, typename SelectionT, typename LensT>
concept clips_temporary = requires(RowsT rows, SelectionT selection, LensT lens) {
    iv::clip_view {std::move(rows), selection, lens};
};
static_assert(!clips_temporary<
              std::array<mechanics_segment, 1>, iv::canonical_set<mechanics_domain<int>>,
              decltype([](const mechanics_segment& row) -> const auto& { return row.geometry; })>);

// Datum constant-evaluation proofs: compose data facilities before runtime.
// Keep records and effects neutral; these checks need no codec or native backend.

namespace {

struct offsets final
{
    using value_type = unsigned;

public:
    static constexpr auto compare(const unsigned lhs, const unsigned rhs) { return lhs <=> rhs; }

};

struct row final
{
    unsigned first;
    unsigned last;

    std::array<unsigned char, 8> payload;
};

constexpr bool projected_fragments()
{
    const row owner {0, 8, {0, 1, 2, 3, 128, 255, 6, 7}};
    const datum::view_of view {owner};
    const std::array rows {view};
    using query = iv::query<offsets>;

    const auto selected =
        iv::compile_intervals<2>(iv::set_union(query::closed_open(1, 3), query::closed_open(4, 6)));
    const iv::canonical_view wanted {selected};
    const iv::clip_view cuts {rows, wanted, [](const auto& value) {
                                  return iv::borrowed_interval<offsets> {
                                              iv::boundary_ref<offsets>::closed(value.first),
                                              iv::boundary_ref<offsets>::open(value.last)
                                          };
                              }};
    std::array<std::byte, 4> output {};
    const std::array fragments {std::span {owner.payload}.first(3),
                                std::span {owner.payload}.subspan(3)};
    datum::byte_cursor input {fragments};
    datum::resource_budget budget {{.payload_bytes = 4}};
    std::size_t copied = 0;
    for (const auto cut : cuts) {
        const auto lower = cut.bounds().lower().key();
        const auto length = cut.bounds().upper().key() - lower;
        budget.charge(datum::resource::payload_bytes, length);
        input.consume(lower - input.position());
        input.read_into(std::span {output}.subspan(copied, length));
        copied += length;
    }

    return output == std::array<std::byte, 4> {std::byte {1}, std::byte {2}, std::byte {128},
                                               std::byte {255}} &&
           4 == copied && 6 == input.position() &&
           4 == iv::measure(wanted, iv::unsigned_metric<unsigned> {});
}

constexpr bool retained_keys()
{
    auto key = datum::key::compose("record", std::string_view {"a\0b", 3});
    const auto original = std::string {datum::key::key_view(key)};
    datum::retained_view retained {datum::key::take_key(std::move(key)),
                                   datum::byte_slice {0, std::size(original)}};
    const auto moved = std::move(retained);
    const auto bytes = moved.get();
    const auto decoded = datum::key::decode<std::string, std::string>(
        std::string_view {std::data(bytes), std::size(bytes)});

    return decoded && "record" == std::get<0>(*decoded) &&
           std::string_view {"a\0b", 3} == std::get<1>(*decoded);
}

constexpr bool empty_byte_ranges()
{
    const std::array<std::span<const char>, 2> source {};
    std::span<char> destination;
    if (!std::empty(datum::gather_bytes(source, destination))) {
        return false;
    }
    const std::array<std::span<char>, 2> fragments {};
    if (0 != datum::scatter_bytes(std::span<const char> {}, fragments)) {
        return false;
    }
    datum::byte_cursor cursor {source};
    cursor.read_into(destination);

    return 0 == cursor.position() && std::begin(cursor.take(0)) == std::default_sentinel;
}
} // anonymous namespace

static_assert(projected_fragments());
static_assert(retained_keys());
static_assert(empty_byte_ranges());

namespace {

constexpr bool failure_boundaries()
{
    std::array<char, 2> source {'a', 'b'};
    const std::array fragments {std::span {source}};
    datum::byte_cursor input {fragments};
    std::array<char, 3> output {'x', 'x', 'x'};
    try {
        input.read_into(std::span {output});

        return false;
    } catch (const std::length_error&) {
    }
    if (0 != input.position() || output != std::array {'x', 'x', 'x'}) {
        return false;
    }
    datum::resource_budget budget {{.depth = 1}};
    try {
        auto outer = budget.enter_depth();
        auto inner = budget.enter_depth();

        return false;
    } catch (const std::length_error&) {
    }
    if (0 != budget.used(datum::resource::depth)) {
        return false;
    }
    iv::workspace<offsets> scratch {{.work = 0}};
    using query = iv::query<offsets>;

    try {
        iv::evaluate(iv::difference(query::closed(0, 8), query::singleton(4)), scratch);

        return false;
    } catch (const std::length_error&) {
    }

    return 0 == scratch.retained_intervals() && 0 == scratch.work_performed();
}
} // anonymous namespace

static_assert(failure_boundaries());
static_assert(datum::render_detail::is_optional_v<const std::optional<int>&>);
static_assert(datum::render_detail::display_tuple<std::tuple<int, char>>);
static_assert(!datum::render_detail::is_optional_v<int>);

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
    datum::gather_bytes(input.take(2), std::span {copied}.subspan(1));

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

    return iv::contains(interval, nonaddressable_point {2}) &&
           std::addressof(interval.lower().key()) == std::addressof(first);
}());

TEST_CASE("constant-evaluated pipelines agree with runtime operations", "[datum][constexpr]")
{
    CHECK(projected_fragments());
    CHECK(retained_keys());
    CHECK(empty_byte_ranges());
    CHECK(failure_boundaries());
    CHECK(differing_size_types());
}

// Datum formatting tests: shared observations through Ceph's selected fmt target.
// Keep these fixtures neutral; application records belong in consumer tests.

namespace datum_fmt_test {

struct counted_record
{
    std::size_t *observations;
};
struct cycle
{
    const cycle *next;
};
struct record
{
    std::string name;
    std::int64_t revision;
    std::uint64_t count;
};
struct vocabulary
{
    std::tuple<double, std::chrono::milliseconds, std::chrono::duration<int, std::ratio<2, 3>>>
        values;
    std::map<std::string, float> entries;
};
struct enrolled_range final
{
    std::array<int, 2> payload {1, 2};
    std::size_t *fallback_observations;

public:
    auto begin() const
    {
        ++*fallback_observations;

        return std::begin(payload);
    }

    auto end() const { return std::end(payload); }

public:
    operator std::string_view() const
    {
        ++*fallback_observations;

        return "unreviewed";
    }

};
struct elapsed final : std::chrono::milliseconds
{
    using std::chrono::milliseconds::milliseconds;
};
struct flags final
{
    unsigned ready : 1;
    unsigned mode : 3;
};
struct accessor_record final
{
    int value;
    int& reference;
    mutable int counter;
    unsigned bits : 2;
    volatile int observed;
};
// Enrollment shields diagnostic values from unrelated ill-formed recognition.
struct guarded_record
{
    int value;

    using rep = int;
    using period = int;

public:
    template <typename ProbeT = void>

    auto begin() const
    {
        static_assert(!std::same_as<ProbeT, void>, "Do not inspect the unselected range interface");

        return std::addressof(value);
    }

    auto end() const { return std::addressof(value) + 1; }

};
struct guarded_projection final : guarded_record
{};
struct hidden_scalar final
{
    std::size_t *observations;
};
struct entry final
{
    std::string key;
    std::uint64_t version;
    std::string value;
    std::string access_token;
};
struct labeled_record final
{
    int number;
    int hidden;
};
template <typename ValueT>
struct selected_record final
{
    ValueT value;
};
} // datum_fmt_test

namespace ceph::salad::datum::render {

template <>
struct display_schema<datum_fmt_test::counted_record>
{
    static constexpr std::string_view name = "counted";
    static constexpr auto fields = std::tuple {field<[](const auto& value) {
        ++*value.observations;

        return 42;
    }>("value")};
};
template <>
struct display_projection<datum_fmt_test::cycle>
{
    static const auto& get(const datum_fmt_test::cycle& value) { return *value.next; }

};
template <>
struct display_schema<datum_fmt_test::record> : reflected_display_schema<^^datum_fmt_test::record, 3>
{};
template <>
struct display_schema<datum_fmt_test::vocabulary> : reflected_display_schema<^^datum_fmt_test::vocabulary, 2>
{};
template <>
struct display_schema<datum_fmt_test::flags> : reflected_display_schema<^^datum_fmt_test::flags, 2>
{};
template <>
struct display_schema<datum_fmt_test::hidden_scalar>
{
    static constexpr std::string_view name = "hidden";
    static constexpr auto accessor = [](const auto& value) -> int {
        ++*value.observations;
        throw std::runtime_error {"unnecessary observation"};
    };
    static constexpr auto fields =
        std::tuple {display_field<accessor, display_field_policy::redacted> {"value"}};
};
template <>
struct display_schema<datum_fmt_test::enrolled_range>
{
    static constexpr std::string_view name = "enrolled";
    static constexpr auto fields = std::tuple {
        display_field<&datum_fmt_test::enrolled_range::payload, display_field_policy::redacted> {
            "payload"}};
};
template <>
struct display_schema<datum_fmt_test::entry>
{
    static constexpr auto fields =
        std::tuple {field<&datum_fmt_test::entry::key>(), field<&datum_fmt_test::entry::version>(),
                    display_field<&datum_fmt_test::entry::value, display_field_policy::redacted>()};
};
template <>
struct display_schema<datum_fmt_test::labeled_record>
{
    static constexpr std::string_view name = "object";
    static constexpr auto fields = std::tuple {
        field<&datum_fmt_test::labeled_record::number>("visible"),
        display_field<&datum_fmt_test::labeled_record::hidden, display_field_policy::redacted> {
            "concealed"}};
};
template <typename ValueT>
struct display_schema<datum_fmt_test::selected_record<ValueT>>
{
    static constexpr auto fields =
        std::tuple {field<&datum_fmt_test::selected_record<ValueT>::value>()};
};
template <>
struct display_projection<datum_fmt_test::enrolled_range>
{
    static int get(const datum_fmt_test::enrolled_range& value)
    {
        ++*value.fallback_observations;

        return 99;
    }

};
template <>
struct display_schema<datum_fmt_test::guarded_record>
{
    static constexpr auto fields = std::tuple {field<&datum_fmt_test::guarded_record::value>()};
};
template <>
struct display_projection<datum_fmt_test::guarded_projection>
{
    static constexpr int get(const datum_fmt_test::guarded_projection& value)
    {
        return value.value;
    }

};
} // ceph::salad::datum::render

// Accessors preserve reference/cv categories, with a value only for bit fields.
template <typename RecordT, typename ResultT, std::size_t Index>
consteval bool reflected_accessor_returns()
{
    return []<auto Accessor, datum::render::display_field_policy Policy>(
               datum::render::display_field<Accessor, Policy>) {
        return std::same_as<std::invoke_result_t<decltype(Accessor), const RecordT&>, ResultT>;
    }(datum::render_detail::reflected_field<datum::detail::record_members<RecordT>[Index]>());
}
static_assert(reflected_accessor_returns<datum_fmt_test::accessor_record, const int&, 0>());
static_assert(reflected_accessor_returns<datum_fmt_test::accessor_record, int&, 1>());
static_assert(reflected_accessor_returns<datum_fmt_test::accessor_record, int&, 2>());
static_assert(reflected_accessor_returns<datum_fmt_test::accessor_record, unsigned, 3>());
static_assert(
    reflected_accessor_returns<datum_fmt_test::accessor_record, const volatile int&, 4>());
static_assert([] {
    std::string text;
    datum::render::format_to(std::back_inserter(text), datum_fmt_test::guarded_record {7});

    return text == "guarded_record{value=7}";
}());
static_assert([] {
    std::string text;
    datum::render::format_to(std::back_inserter(text), datum_fmt_test::guarded_projection {{7}});

    return text == "7";
}());

static_assert(std::output_iterator<fmt::appender, char>);
static_assert(std::formattable<datum_fmt_test::record, char>);
static_assert(fmt::is_formattable<datum_fmt_test::record>::value);

TEST_CASE("Datum fmt and standard formatting share character observations", "[datum][fmt]")
{
    const std::array values {datum_fmt_test::record {"", 0, 0},
                             datum_fmt_test::record {std::string {"a\0b", 3}, -1,
                                                     std::numeric_limits<std::uint64_t>::max()},
                             datum_fmt_test::record {"a\"b\\c\n", 41, 7},
                             datum_fmt_test::record {std::string(256, 'x'),
                                                     std::numeric_limits<std::int64_t>::min(), 42}};
    for (const auto& value : values) {
        std::string reference;
        datum::render::format_to(std::back_inserter(reference), value);
        CHECK(std::format("{}", value) == reference);
        CHECK(fmt::format("{}", value) == reference);

        fmt::memory_buffer buffer;
        fmt::format_to(fmt::appender {buffer}, "{}", value);
        CHECK(fmt::to_string(buffer) == reference);

        std::array<char, 1024> output;
        const auto last = datum::render::format_to(std::begin(output), value);
        CHECK(std::string(std::begin(output), last) == reference);
    }
}

TEST_CASE("explicit diagnostic tuples infer names without enrolling omitted members",
          "[datum][fmt]")
{
    const datum_fmt_test::entry value {"object/42", 7, "secret", "unapproved"};
    const std::string expected = "entry{key=\"object/42\", version=7, value=<redacted:6 bytes>}";
    CHECK(fmt::format("{}", value) == expected);
    CHECK(std::format("{}", value) == expected);
    std::string bounded;
    const auto result =
        datum::render::format_bounded(std::back_inserter(bounded), value, {.bytes = 35});
    CHECK(result.truncated);
    CHECK(result.written == 35);
    CHECK(bounded == expected.substr(0, 35));
    CHECK(fmt::format("{}", datum_fmt_test::selected_record<int> {42}) ==
          "selected_record{value=42}");
}

TEST_CASE("explicit record and field labels override diagnostic defaults", "[datum][fmt]")
{
    const datum_fmt_test::labeled_record value {3, 9};
    CHECK(fmt::format("{}", value) == "object{visible=3, concealed=<redacted>}");
    CHECK(std::format("{}", value) == "object{visible=3, concealed=<redacted>}");
}

static_assert([] {
    std::string output;
    datum::render::format_to(std::back_inserter(output), datum_fmt_test::selected_record<int> {42});

    return output == "selected_record{value=42}";
}());

TEST_CASE("minimal character sinks and total diagnostic budgets", "[datum][fmt][limits]")
{
    struct character_sink
    {
        std::string *result;

    public:
        auto& operator*() { return *this; }

        auto& operator++(int) { return *this; }
        void operator=(char value) { result->push_back(value); }

    };
    static_assert(datum::render_detail::character_output<character_sink>);
    static_assert(!std::output_iterator<character_sink, char>);
    const datum_fmt_test::record value {"abc", -42, 7};
    std::string text;
    datum::render::format_to(character_sink {&text}, value);
    CHECK(text == fmt::format("{}", value));
    ::ceph::util::clear(text);
    const auto limited =
        datum::render::format_bounded(character_sink {&text}, value, {.bytes = 12});
    CHECK(limited.truncated);
    CHECK(limited.written == 12);
    CHECK(std::size(text) == 12);
    CHECK(text == fmt::format("{}", value).substr(0, 12));
    datum_fmt_test::cycle cycle {};
    cycle.next = &cycle;
    ::ceph::util::clear(text);
    datum::render::format_to(character_sink {&text}, cycle, {.depth = 3});
    CHECK(std::empty(text));
    std::size_t observations = 0;
    const auto range = std::views::iota(0, 100) | std::views::transform([&](int n) {
                           ++observations;

                           return n;
                       });
    datum::render_detail::append_range(character_sink {&text}, range);
    CHECK(observations == 16);
    observations = 0;
    const auto binary = std::views::iota(0, 100) | std::views::transform([&](int n) {
                            ++observations;

                            return static_cast<std::byte>(n);
                        });
    datum::render_detail::append_binary(character_sink {&text}, binary);
    CHECK(observations == 32);
    observations = 0;
    ::ceph::util::clear(text);
    const datum_fmt_test::counted_record counted {&observations};
    const auto stopped =
        datum::render::format_bounded(character_sink {&text}, counted, {.bytes = 14});
    CHECK(stopped.truncated);
    CHECK(observations == 0);
}

TEST_CASE("standard vocabulary preserves units and bounded nested observations", "[datum][fmt]")
{
    const datum_fmt_test::vocabulary value {
        {1.25, std::chrono::milliseconds {3}, std::chrono::duration<int, std::ratio<2, 3>> {4}},
        {{"key", 2.5F}}};
    const auto text = fmt::format("{}", value);
    CHECK(text == "vocabulary{values=(1.25, 3ms, 4[2/3]s), entries=[(\"key\", 2.5)]}");
    CHECK(std::format("{}", value) == text);
    std::string bounded;
    const auto result =
        datum::render::format_bounded(std::back_inserter(bounded), value, {.bytes = 20});
    CHECK(result.truncated);
    CHECK(bounded == text.substr(0, 20));
    ::ceph::util::clear(bounded);
    datum::render::format_to(std::back_inserter(bounded), value, {.depth = 1});
    CHECK(bounded == "vocabulary{values=, entries=}");
}

TEST_CASE("explicit diagnostic enrollment wins over range text and projection recognition",
          "[datum][fmt]")
{
    std::size_t observations = 0;
    const datum_fmt_test::enrolled_range value {{1, 2}, &observations};
    CHECK(fmt::format("{}", value) == "enrolled{payload=<redacted:2 elements>}");
    CHECK(std::format("{}", value) == "enrolled{payload=<redacted:2 elements>}");
    CHECK(0 == observations);
}

TEST_CASE("time diagnostics preserve units without overflowing a nanosecond conversion",
          "[datum][fmt]")
{
    std::string output;
    using duration = std::chrono::hours;
    using point = std::chrono::time_point<std::chrono::system_clock, duration>;

    datum::render_detail::append_value(
        std::back_inserter(output), point {duration {std::numeric_limits<duration::rep>::max()}});
    CHECK(output == std::to_string(std::numeric_limits<duration::rep>::max()) + "h");
    ::ceph::util::clear(output);
    datum::render_detail::append_value(std::back_inserter(output), datum_fmt_test::elapsed {3});
    CHECK(output == "3ms");
}

static_assert([] {
    std::string output;
    datum::render::format_to(std::back_inserter(output), datum_fmt_test::flags {1, 5});

    return output == "flags{ready=1, mode=5}";
}());

TEST_CASE("reflected diagnostics observe bit fields as values", "[datum][fmt]")
{
    CHECK(fmt::format("{}", datum_fmt_test::flags {1, 5}) == "flags{ready=1, mode=5}");
}

TEST_CASE("redacting a scalar skips its unneeded accessor", "[datum][fmt]")
{
    std::size_t observations = 0;
    CHECK(fmt::format("{}", datum_fmt_test::hidden_scalar {&observations}) ==
          "hidden{value=<redacted>}");
    CHECK(0 == observations);
}

namespace interval = ceph::salad::datum::interval;

struct integer_domain final : interval::comparison_domain<int>
{
    static constexpr std::optional<int> prefix_upper_bound(const int value) noexcept
    {
        if (100 == value) {
            return std::nullopt;
        }

        return 1 + value;
    }

};

using integer_query = interval::query<integer_domain>;

struct dense_domain final : interval::comparison_domain<int>
{};

using dense_query = interval::query<dense_domain>;

struct tagged_value final
{
    int value = 0;

public:
    constexpr auto operator<=>(const tagged_value&) const noexcept = default;

};

struct tagged_domain final : interval::comparison_domain<tagged_value>
{
    static constexpr std::optional<tagged_value> prefix_upper_bound(
        const tagged_value value) noexcept
    {
        if (10 == value.value) {
            return std::nullopt;
        }

        return tagged_value {.value = 1 + value.value};
    }

};

using tagged_query = interval::query<tagged_domain>;

static_assert(ceph::salad::datum::render::displayable<integer_query>);
static_assert(ceph::salad::datum::render::displayable<interval::lower_endpoint<integer_domain>>);
static_assert(
    ceph::salad::datum::render::displayable<interval::static_interval_set<integer_domain, 2>>);
static_assert(ceph::salad::datum::render::displayable<
              decltype(interval::without(integer_query::closed_open(1, 9), 5))>);
#ifdef DATUM_HAVE_ZPP
 static_assert(!ceph::salad::datum::render::serializable<integer_query>);
 static_assert(
     !ceph::salad::datum::render::serializable<interval::static_interval_set<integer_domain, 2>>);
#endif

struct projected_integer_interval final
{
    using domain_type = integer_domain;
    using boundary_type = interval::boundary<integer_domain>;

public:
    static constexpr projected_integer_interval closed_open(const int begin_key, const int end_key)
    {
        return {.begin_key = begin_key, .end_key = end_key};
    }

public:
    static constexpr projected_integer_interval empty() { return closed_open(7, 7); }

public:
    constexpr boundary_type lower() const { return boundary_type::closed(begin_key); }

    constexpr boundary_type upper() const { return boundary_type::open(end_key); }

    int begin_key = 0;
    int end_key = 0;

};

struct counted_projected_interval final
{
    using domain_type = integer_domain;
    using boundary_type = interval::boundary<integer_domain>;

public:
    boundary_type lower() const
    {
        ++lower_calls;

        return boundary_type::closed(begin_key);
    }

    boundary_type upper() const
    {
        ++upper_calls;

        return boundary_type::open(end_key);
    }

    int begin_key = 0;
    int end_key = 0;

    mutable std::size_t lower_calls = 0;
    mutable std::size_t upper_calls = 0;

};

struct boundary_recording_sink final
{
    std::vector<integer_query> intervals;
    std::size_t boundary_emissions = 0;
    std::size_t interval_emissions = 0;

public:
    template <interval::boundary_view_for<integer_domain> LowerT,
              interval::boundary_view_for<integer_domain> UpperT>

    void emit_interval(const LowerT& lower, const UpperT& upper)
    {
        ++boundary_emissions;
        ::ceph::util::push_back(intervals, integer_query::between(lower, upper));
    }

public:
    template <interval::interval_view IntervalT>

    void operator()(const IntervalT& interval)
    {
        ++interval_emissions;
        ::ceph::util::push_back(intervals,
                                integer_query::between(interval.lower(), interval.upper()));
    }

};

template <typename DomainT>
auto intervals_of(const interval::expression auto& expression)
{
    auto out = interval::compile_intervals(expression);
    static_assert(std::same_as<typename decltype(out)::value_type, interval::query<DomainT>>);

    return out;
}

constexpr bool contains(const integer_query& query, const int value)
{
    return interval::contains(query, value);
}

constexpr bool equivalent_on_samples(const auto& lhs, const auto& rhs, const auto& samples)
{
    return std::ranges::all_of(samples, [&](const auto& value) {
        return interval::contains(lhs, value) == interval::contains(rhs, value);
    });
}

constexpr bool static_compiler_accepts_projected_intervals()
{
    constexpr auto compiled =
        interval::compile_intervals<1>(projected_integer_interval::closed_open(2, 8));

    return 1 == std::size(compiled) && integer_query::closed_open(2, 8) == compiled[0];
}

constexpr bool static_compiler_coalesces_intervals()
{
    constexpr auto compiled = interval::compile_intervals<1>(
        interval::set_union(integer_query::closed_open(1, 5), integer_query::closed_open(3, 8)));

    return 1 == std::size(compiled) && integer_query::closed_open(1, 8) == compiled[0];
}

constexpr bool static_compiler_orders_disjoint_intervals()
{
    constexpr auto compiled = interval::compile_intervals<2>(interval::set_union(
        integer_query::closed_open(40, 50), integer_query::closed_open(10, 20)));

    return 2 == std::size(compiled) && integer_query::closed_open(10, 20) == compiled[0] &&
           integer_query::closed_open(40, 50) == compiled[1];
}

constexpr bool static_compiler_clips_composite_intervals()
{
    constexpr auto compiled = interval::compile_intervals<2>(
        interval::intersection(integer_query::closed_open(15, 45),
                               interval::set_union(integer_query::closed_open(10, 20),
                                                   integer_query::closed_open(40, 50))));

    return 2 == std::size(compiled) && integer_query::closed_open(15, 20) == compiled[0] &&
           integer_query::closed_open(40, 45) == compiled[1];
}

constexpr bool static_compiler_normalizes_nested_union()
{
    constexpr auto compiled = interval::compile_intervals<2>(interval::set_union(
        interval::set_union(integer_query::closed_open(40, 50), integer_query::closed_open(10, 20)),
        integer_query::closed_open(15, 45)));

    return 1 == std::size(compiled) && integer_query::closed_open(10, 50) == compiled[0];
}

constexpr bool helper_layer_is_compile_time_capable()
{
    constexpr auto page =
        interval::ending_before(interval::starting_after(integer_query::closed_open(1, 10), 3), 8);
    constexpr auto without_marker = interval::without(page, 5);
    constexpr auto compiled = interval::compile_intervals<2>(without_marker);

    return 2 == std::size(compiled) && integer_query::open(3, 5) == compiled[0] &&
           integer_query::open(5, 8) == compiled[1] &&
           interval::intersects(integer_query::closed_open(1, 5),
                                integer_query::closed_open(4, 8)) &&
           interval::is_disjoint(integer_query::closed_open(1, 5),
                                 integer_query::closed_open(5, 8)) &&
           interval::encloses(integer_query::closed_open(1, 10), integer_query::closed_open(3, 8));
}

constexpr bool helper_predicates_accept_expressions()
{
    constexpr auto lhs =
        interval::set_union(integer_query::closed_open(1, 4), integer_query::closed_open(8, 10));
    constexpr auto rhs =
        interval::difference(integer_query::closed_open(3, 9), integer_query::closed_open(5, 7));
    constexpr auto inner = interval::intersection(lhs, integer_query::closed_open(8, 9));

    return interval::intersects(lhs, rhs) && !interval::is_disjoint(lhs, rhs) &&
           interval::encloses(lhs, inner) &&
           !interval::encloses(integer_query::closed_open(1, 4), rhs);
}

constexpr bool helpers_accept_projected_interval_views()
{
    constexpr auto clipped = interval::ending_at(
        interval::starting_after(projected_integer_interval::closed_open(2, 8), 4), 6);
    constexpr auto compiled = interval::compile_intervals<1>(clipped);

    return 1 == std::size(compiled) &&
           integer_query::between(interval::open_bound<integer_domain>(4),
                                  interval::closed_bound<integer_domain>(6)) == compiled[0];
}

constexpr bool helpers_are_generic_over_ordered_domains()
{
    constexpr auto prefix = interval::prefix<tagged_domain>(tagged_value {3});
    constexpr auto page = interval::ending_before(
        interval::starting_at(tagged_query::closed_open(tagged_value {1}, tagged_value {8}),
                              tagged_value {3}),
        tagged_value {6});
    constexpr auto without_marker = interval::without(page, tagged_value {4});
    constexpr auto without_nested = interval::without_prefix(
        tagged_query::closed_open(tagged_value {1}, tagged_value {8}), tagged_value {3});
    constexpr auto clipped =
        interval::between(interval::lower_after<tagged_domain>(tagged_value {3}),
                          interval::upper_at_or_before<tagged_domain>(tagged_value {4}));
    constexpr auto compiled = interval::compile_intervals<2>(without_marker);
    constexpr auto prefix_page =
        interval::prefix_starting_after<tagged_domain>(tagged_value {3}, tagged_value {3});

    return tagged_query::closed_open(tagged_value {3}, tagged_value {4}) == prefix &&
           tagged_query::between(interval::open_bound<tagged_domain>(tagged_value {3}),
                                 interval::closed_bound<tagged_domain>(tagged_value {4})) ==
               clipped &&
           tagged_query::closed_open(tagged_value {3}, tagged_value {6}) == page &&
           tagged_query::closed_open(tagged_value {3}, tagged_value {4}) == compiled[0] &&
           tagged_query::open(tagged_value {4}, tagged_value {6}) == compiled[1] &&
           tagged_query::open(tagged_value {3}, tagged_value {4}) == prefix_page &&
           2 == interval::interval_count(without_nested);
}

constexpr bool dense_domains_do_not_need_prefix_bounds()
{
    constexpr auto query =
        interval::intersection(dense_query::closed_open(1, 10), interval::after<dense_domain>(4));
    constexpr auto compiled = interval::compile_intervals<1>(query);

    return 1 == std::size(compiled) && dense_query::open(4, 10) == compiled[0];
}

static_assert(interval::ordered_domain<integer_domain>);
static_assert(interval::ordered_domain<dense_domain>);
static_assert(!interval::prefix_domain<dense_domain>);
static_assert(interval::interval_view<projected_integer_interval>);
static_assert(interval::interval_view<counted_projected_interval>);
static_assert(interval::expression<integer_query>);
static_assert(interval::expression<projected_integer_interval>);
static_assert(interval::canonical_interval<integer_query>);
static_assert(!interval::canonical_interval<projected_integer_interval>);
static_assert(!std::same_as<projected_integer_interval, integer_query>);
static_assert(interval::expression<decltype(interval::set_union(integer_query::empty(),
                                                                integer_query::universal()))>);

static_assert(contains(integer_query::closed(2, 4), 2));
static_assert(contains(integer_query::closed(2, 4), 4));
static_assert(!contains(integer_query::open(2, 4), 2));
static_assert(!contains(integer_query::open(2, 4), 4));
static_assert(interval::is_empty(integer_query::closed_open(7, 7)));
static_assert(!interval::is_empty(integer_query::singleton(7)));
static_assert(interval::contains(projected_integer_interval::closed_open(2, 4), 2));
static_assert(static_compiler_accepts_projected_intervals());
static_assert(static_compiler_coalesces_intervals());
static_assert(static_compiler_orders_disjoint_intervals());
static_assert(static_compiler_clips_composite_intervals());
static_assert(static_compiler_normalizes_nested_union());
static_assert(helper_layer_is_compile_time_capable());
static_assert(helper_predicates_accept_expressions());
static_assert(helpers_accept_projected_interval_views());
static_assert(helpers_are_generic_over_ordered_domains());
static_assert(dense_domains_do_not_need_prefix_bounds());

TEST_CASE("Datum interval expressions display without compilation", "[datum][interval][display]")
{
    const auto selection = interval::without(integer_query::closed_open(1, 9), 5);
    const auto displayed = std::format("{}", selection);

    CHECK(std::string::npos != displayed.find("interval_difference{"));
    CHECK(std::string::npos !=
          displayed.find("lower=interval_boundary{kind=finite, key=1, inclusive=true}"));
    CHECK(std::string::npos !=
          displayed.find("upper=interval_boundary{kind=finite, key=9, inclusive=false}"));
    CHECK(std::string::npos !=
          displayed.find("interval{lower=interval_boundary{kind=finite, key=5"));

    const auto combined = interval::set_union(integer_query::closed_open(1, 3),
                                              integer_query::closed_open(5, 9));
    CHECK(std::format("{}", combined).starts_with("interval_union{lhs=interval{"));
    const auto overlap = interval::intersection(combined, integer_query::closed_open(2, 7));
    CHECK(std::format("{}", overlap).starts_with("interval_intersection{lhs=interval_union{"));
    const auto compiled = interval::compile_intervals<2>(selection);

    CHECK(std::string::npos !=
          std::format("{}", compiled).find("interval_set{intervals=[interval{"));
}

TEST_CASE("Datum interval primitives describe generic ordered domains", "[datum][interval]")
{
    CHECK(interval::closed_bound<integer_domain>(3).inclusive());
    CHECK(!interval::open_bound<integer_domain>(3).inclusive());

    CHECK(interval::is_empty(integer_query::empty()));
    CHECK(!interval::is_empty(integer_query::universal()));
    CHECK(!interval::is_empty(integer_query::singleton(7)));

    CHECK(1 == std::size(intervals_of<integer_domain>(integer_query::closed_open(2, 5))));
    CHECK(0 == std::size(intervals_of<integer_domain>(integer_query::closed_open(7, 7))));
}

TEST_CASE("Datum interval algorithms accept foreign interval views", "[datum][interval]")
{
    constexpr auto foreign = projected_integer_interval::closed_open(2, 8);
    constexpr auto empty = projected_integer_interval::empty();
    counted_projected_interval counted {.begin_key = 2, .end_key = 8};

    CHECK(interval::contains(foreign, 2));
    CHECK(!interval::contains(foreign, 8));
    CHECK(interval::is_empty(empty));
    CHECK(interval::contains(counted, 2));
    CHECK(1 == counted.lower_calls);
    CHECK(1 == counted.upper_calls);

    counted.lower_calls = 0;
    counted.upper_calls = 0;

    CHECK(integer_query::closed_open(2, 8) == interval::copy_interval(counted));
    CHECK(1 == counted.lower_calls);
    CHECK(1 == counted.upper_calls);

    counted.lower_calls = 0;
    counted.upper_calls = 0;

    interval::for_each_interval(counted, [](const auto&) {});
    CHECK(1 == counted.lower_calls);
    CHECK(1 == counted.upper_calls);

    CHECK(intervals_of<integer_domain>(
              interval::intersection(foreign, integer_query::closed_open(4, 10))) ==
          std::vector {integer_query::closed_open(4, 8)});
    CHECK(interval::compile_intervals(
              interval::intersection(foreign, integer_query::closed_open(4, 10))) ==
          std::vector {integer_query::closed_open(4, 8)});
    CHECK_THROWS_AS(interval::compile_intervals<1>(interval::set_union(
                        integer_query::closed_open(1, 2), integer_query::closed_open(4, 5))),
                    std::length_error);

    CHECK(0 == std::size(intervals_of<integer_domain>(
                   interval::intersection(integer_query::empty(), foreign))));
    CHECK(0 == interval::interval_count(interval::intersection(integer_query::empty(), foreign)));

    CHECK(intervals_of<integer_domain>(
              interval::set_union(foreign, integer_query::closed_open(8, 12))) ==
          std::vector {integer_query::closed_open(2, 12)});

    CHECK(intervals_of<integer_domain>(
              interval::difference(foreign, integer_query::closed_open(4, 6))) ==
          std::vector {integer_query::closed_open(2, 4), integer_query::closed_open(6, 8)});

    boundary_recording_sink sink;
    interval::for_each_interval(interval::difference(foreign, integer_query::closed_open(4, 6)),
                                sink);
    CHECK(2 == sink.boundary_emissions);
    CHECK(0 == sink.interval_emissions);
    CHECK(sink.intervals ==
          std::vector {integer_query::closed_open(2, 4), integer_query::closed_open(6, 8)});

    CHECK(intervals_of<integer_domain>(interval::complement(foreign)) ==
          std::vector {
              integer_query::between(interval::boundary<integer_domain>::negative_infinity(),
                                     interval::open_bound<integer_domain>(2)),
              integer_query::between(interval::closed_bound<integer_domain>(8),
                                     interval::boundary<integer_domain>::positive_infinity())});
}

TEST_CASE("Datum interval prefix queries use the domain's prefix upper bound", "[datum][interval]")
{
    const auto integers = integer_query::prefix(41);
    const auto max_prefix = integer_query::prefix(100);

    CHECK(contains(integers, 41));
    CHECK(!contains(integers, 42));

    CHECK(contains(max_prefix, 100));
    CHECK(max_prefix.upper().above_all());
}

TEST_CASE("Datum interval helpers build common query shapes", "[datum][interval]")
{
    CHECK(interval::endpoint_inclusion::included ==
          interval::lower_at_or_after<integer_domain>(7).inclusion);
    CHECK(interval::endpoint_inclusion::excluded ==
          interval::lower_after<integer_domain>(7).inclusion);
    CHECK(interval::endpoint_inclusion::included ==
          interval::upper_at_or_before<integer_domain>(7).inclusion);
    CHECK(interval::endpoint_inclusion::excluded ==
          interval::upper_before<integer_domain>(7).inclusion);

    CHECK(integer_query::singleton(7) == interval::at<integer_domain>(7));
    CHECK(interval::after<integer_domain>(7) ==
          interval::from(interval::lower_after<integer_domain>(7)));
    CHECK(interval::before<integer_domain>(7) ==
          interval::until(interval::upper_before<integer_domain>(7)));
    CHECK(integer_query::closed_open(2, 5) == interval::between<integer_domain>(2, 5));
    CHECK(integer_query::closed(2, 5) == interval::closed_between<integer_domain>(2, 5));
    CHECK(integer_query::open(2, 5) == interval::open_between<integer_domain>(2, 5));
    CHECK(interval::between(interval::lower_after<integer_domain>(2),
                            interval::upper_at_or_before<integer_domain>(5)) ==
          integer_query::between(interval::open_bound<integer_domain>(2),
                                 interval::closed_bound<integer_domain>(5)));

    const auto page =
        interval::ending_before(interval::starting_at(integer_query::closed_open(1, 10), 3), 8);

    CHECK(integer_query::closed_open(3, 8) == page);
    CHECK(integer_query::open(3, 8) == interval::starting_after(page, 3));
    CHECK(integer_query::closed(3, 6) == interval::ending_at(page, 6));
}

TEST_CASE("Datum interval helper predicates describe interval relations", "[datum][interval]")
{
    const auto low = integer_query::closed_open(1, 5);
    const auto touching = integer_query::closed_open(5, 8);
    const auto overlapping = integer_query::closed_open(4, 8);
    const auto inner = integer_query::closed_open(2, 4);
    const auto split =
        interval::set_union(integer_query::closed_open(1, 3), integer_query::closed_open(7, 9));

    CHECK(interval::is_disjoint(low, touching));
    CHECK(!interval::intersects(low, touching));
    CHECK(interval::intersects(low, overlapping));
    CHECK(interval::encloses(low, inner));
    CHECK(!interval::encloses(inner, low));
    CHECK(interval::encloses(low, integer_query::empty()));
    CHECK(interval::encloses(split, integer_query::closed_open(7, 8)));
    CHECK(!interval::encloses(split, integer_query::closed_open(2, 8)));
}

TEST_CASE("Datum interval set operations emit canonical intervals", "[datum][interval]")
{
    CHECK(intervals_of<integer_domain>(interval::intersection(integer_query::closed_open(1, 10),
                                                              integer_query::closed_open(4, 20))) ==
          std::vector {integer_query::closed_open(4, 10)});

    CHECK(intervals_of<integer_domain>(interval::difference(integer_query::closed_open(1, 10),
                                                            integer_query::closed_open(4, 7))) ==
          std::vector {integer_query::closed_open(1, 4), integer_query::closed_open(7, 10)});
    CHECK(intervals_of<integer_domain>(
              interval::difference(integer_query::open(1, 10), integer_query::singleton(7))) ==
          std::vector {integer_query::open(1, 7), integer_query::open(7, 10)});

    CHECK(intervals_of<integer_domain>(interval::set_union(integer_query::closed_open(1, 4),
                                                           integer_query::closed_open(4, 8))) ==
          std::vector {integer_query::closed_open(1, 8)});

    CHECK(intervals_of<integer_domain>(interval::set_union(integer_query::closed_open(40, 50),
                                                           integer_query::closed_open(10, 20))) ==
          std::vector {integer_query::closed_open(10, 20), integer_query::closed_open(40, 50)});

    CHECK(intervals_of<integer_domain>(interval::set_union(integer_query::closed_open(1, 5),
                                                           integer_query::closed_open(3, 8))) ==
          std::vector {integer_query::closed_open(1, 8)});

    CHECK(intervals_of<integer_domain>(
              interval::intersection(integer_query::closed_open(15, 45),
                                     interval::set_union(integer_query::closed_open(10, 20),
                                                         integer_query::closed_open(40, 50)))) ==
          std::vector {integer_query::closed_open(15, 20), integer_query::closed_open(40, 45)});
    CHECK(2 == interval::interval_count(interval::intersection(
                   integer_query::closed_open(15, 45),
                   interval::set_union(integer_query::closed_open(10, 20),
                                       integer_query::closed_open(40, 50)))));
}

TEST_CASE("Datum interval expressions satisfy sampled algebra laws", "[datum][interval]")
{
    constexpr auto samples = std::array {-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    constexpr auto a = integer_query::closed_open(1, 7);
    constexpr auto b = integer_query::closed_open(3, 9);
    constexpr auto empty = integer_query::empty();
    constexpr auto universal = integer_query::universal();

    static_assert(equivalent_on_samples(interval::intersection(a, universal), a, samples));
    static_assert(equivalent_on_samples(interval::set_union(a, empty), a, samples));
    static_assert(equivalent_on_samples(interval::difference(a, a), empty, samples));

    CHECK(
        equivalent_on_samples(interval::intersection(a, b), interval::intersection(b, a), samples));
    CHECK(equivalent_on_samples(interval::set_union(a, b), interval::set_union(b, a), samples));
    CHECK(equivalent_on_samples(
        interval::complement(interval::set_union(a, b)),
        interval::intersection(interval::complement(a), interval::complement(b)), samples));
}

namespace ownership = ceph::salad::datum;

namespace interval_data_test {

using u64 = std::uint64_t;

struct point
{
    u64 value;

public:
    point() = delete;

    constexpr explicit point(u64 v) : value(v)
    {}

};

struct image_bytes
{
    using value_type = point;

public:
    static constexpr std::strong_ordering compare(const point& a, const point& b)
    {
        return a.value <=> b.value;
    }

};

struct object_bytes : image_bytes
{}; // Same point representation, another domain.

using Q = iv::query<image_bytes>;
using OQ = iv::query<object_bytes>;

struct record
{
    point lower, upper;
    unsigned id;
    std::unique_ptr<std::string> payload;

public:
    record(u64 lo, u64 hi, unsigned id)
        : lower(lo), upper(hi), id(id), payload(std::make_unique<std::string>("payload"))
    {}

};

// Native/domain-specific example: named ctor, no full record copy, no rvalues.
class record_interval
{
    const record *record_;

public:
    using domain_type = image_bytes;

public:
    template <class R>
    requires std::same_as<std::remove_cvref_t<R>, record> && std::is_lvalue_reference_v<R&&>
    constexpr explicit record_interval(R&& r) : record_(std::addressof(r))
    {}

public:
    constexpr auto lower() const { return iv::boundary_ref<image_bytes>::closed(record_->lower); }

    constexpr auto upper() const { return iv::boundary_ref<image_bytes>::open(record_->upper); }

};

struct record_lens
{
    template <class R>
    requires std::constructible_from<record_interval, R&&>
    constexpr record_interval operator()(R&& r) const
    {
        return record_interval {std::forward<R>(r)};
    }

};

struct move_only_lens : record_lens
{
    std::unique_ptr<int> configuration;

public:
    move_only_lens() = delete;

    explicit move_only_lens(int value) : configuration(std::make_unique<int>(value))
    {}

};

struct construction_only_lens : record_lens
{
    construction_only_lens() = delete;

    explicit construction_only_lens(int) {}

    construction_only_lens(construction_only_lens&&) = default;
    construction_only_lens(const construction_only_lens&) = delete;

public:
    construction_only_lens& operator=(construction_only_lens&&) = delete;

};

struct immutable_point
{
    u64 value;

public:
    immutable_point() = delete;

    constexpr explicit immutable_point(u64 v) : value(v)
    {}

    constexpr immutable_point(const immutable_point&) = default;
    constexpr immutable_point(immutable_point&&) = default;

public:
    immutable_point& operator=(const immutable_point&) = delete;

};

struct immutable_offsets
{
    using value_type = immutable_point;

public:
    static constexpr std::strong_ordering compare(const immutable_point& a,
                                                  const immutable_point& b)
    {
        return a.value <=> b.value;
    }

};

struct retained_point
{
    std::shared_ptr<const std::string> owner;

    std::string_view value;

public:
    explicit retained_point(std::shared_ptr<const std::string> p)
        : owner(std::move(p)), value(*owner)
    {}

};

struct retained_keys
{
    using value_type = retained_point;

public:
    static std::strong_ordering compare(const retained_point& a, const retained_point& b)
    {
        return a.value <=> b.value;
    }

};
} // interval_data_test

namespace ceph::salad::datum {

// Audited opaque fixture profiles. Numeric points own their only member;
// retained_point's copied owner keeps the viewed bytes alive. No native type
// or broad pointer/container classification is added to the trait machinery.
template <>
struct ownership_traits<interval_data_test::point> : std::true_type
{};

template <>
struct ownership_traits<interval_data_test::immutable_point> : std::true_type
{};

template <>
struct ownership_traits<interval_data_test::retained_point> : std::true_type
{};
} // ceph::salad::datum

namespace interval_data_test {

struct overload_lens
{
    record_interval operator()(const record& r) const { return record_interval {r}; }

    OQ operator()(const record& r) { return OQ::closed_open(r.lower, r.upper); }

};

struct category_lens
{
    record_interval operator()(record& r) const { return record_interval {r}; }

    OQ operator()(const record& r) const { return OQ::closed_open(r.lower, r.upper); }

};

struct mutable_only_lens
{
    Q operator()(const record& r) { return Q::closed_open(r.lower, r.upper); }

};

struct bad_result_lens
{
    int operator()(const record&) const { return 1; }

};

struct proxy_lens
{
    Q operator()(std::vector<bool>::reference&& bit) const
    {
        return bit ? Q::singleton(point {1}) : Q::empty();
    }

    Q operator()(std::vector<bool>::reference&) const = delete;

};

struct numbers
{
    u64 offset = 0, length = 0;
};

std::istream& operator>>(std::istream& in, numbers& r) { return in >> r.offset >> r.length; }

struct numbers_lens
{
    Q operator()(const numbers& r) const
    {
        if (r.length > std::numeric_limits<u64>::max() - r.offset) {
            throw std::overflow_error("endpoint overflow");
        }

        return Q::closed_open(point {r.offset}, point {r.offset + r.length});
    }

};

// Precisely the PUBLIC boundary_view_for protocol, without convenience methods
// allows_after/allows_before. All algorithms must honor this minimal protocol.
struct protocol_bound
{
    using domain_type = image_bytes;

    iv::boundary<image_bytes> value;

public:
    auto kind() const { return value.kind(); }

public:
    const point& key() const { return value.key(); }

public:
    bool finite() const { return value.finite(); }

public:
    bool inclusive() const { return value.inclusive(); }

public:
    bool below_all() const { return value.below_all(); }

public:
    bool above_all() const { return value.above_all(); }

};

struct protocol_interval
{
    using domain_type = image_bytes;

    protocol_bound lo, hi;
    bool empty = false;

public:
    const protocol_bound& lower() const { return lo; }

    const protocol_bound& upper() const { return hi; }

public:
    bool explicitly_empty() const { return empty; }

};

struct volatile_boundary_interval final
{
    using domain_type = image_bytes;

    volatile protocol_bound bound;

public:
    const volatile protocol_bound& lower() const { return bound; }

    const volatile protocol_bound& upper() const { return bound; }

};

template <class R, class L>
concept can_project = requires { iv::intervals_view {std::declval<R>(), std::declval<L>()}; };
template <class R, class F, class D = image_bytes>
concept can_visit = requires { iv::visit_intervals<D>(std::declval<R>(), std::declval<F>()); };
template <class R, class F>
concept can_visit_inferred =
    requires { iv::visit_intervals(std::declval<R>(), std::declval<F>()); };
template <class I>
concept can_copy = requires(const I& value) { iv::copy_interval(value); };

struct accepting
{
    iv::acceptance operator()(const Q&) { return iv::acceptance::accept; }

};

struct bool_consumer
{
    bool operator()(const Q&) { return true; }

};

struct borrowed_keys
{
    using value_type = std::string_view;

public:
    static constexpr std::strong_ordering compare(std::string_view a, std::string_view b)
    {
        return a <=> b;
    }

};

using BQ = iv::query<borrowed_keys>;

struct borrowed_accepting final
{
    iv::acceptance operator()(const BQ&) { return iv::acceptance::accept; }

};

using borrowed_value_source = decltype(std::views::iota(0, 2) | std::views::transform([](int) {

                                           return BQ::singleton("stable");
                                       }));
template <class D>
concept can_own_batch = requires { typename iv::interval_batch<D, 2>; };

struct owned_record
{
    unsigned id;
    std::string name;
};

struct borrowed_record
{
    std::string name;
    std::string_view borrowed;
};

static_assert(ownership::owning_value<owned_record>); // Actual recursive reflection.
static_assert(!ownership::owning_value<borrowed_record>);
static_assert(!ownership::owning_value<std::string_view>);
static_assert(ownership::owning_value<Q>);
static_assert(ownership::owning_value<std::vector<Q>>);
static_assert(ownership::owning_value<std::inplace_vector<Q, 2>>);
static_assert(ownership::owning_value<iv::interval_batch<image_bytes, 2>>);
static_assert(!ownership::owning_value<BQ>);
static_assert(!ownership::owning_value<std::inplace_vector<BQ, 2>>);
static_assert(!ownership::owning_value<iv::static_interval_set<borrowed_keys, 2>>);
static_assert(iv::interval_for<BQ, borrowed_keys>);
static_assert(iv::interval_range_for<std::array<BQ, 2>&, borrowed_keys>);
static_assert(can_project<std::array<BQ, 2>&, std::identity>);
static_assert(!can_project<borrowed_value_source, std::identity>);
static_assert(!can_own_batch<borrowed_keys>);

using records = std::vector<record>;
using view = decltype(iv::intervals_view {std::declval<records&>(), record_lens {}});
using owning_view = decltype(iv::intervals_view {std::declval<records>(), record_lens {}});
using move_view = decltype(iv::intervals_view {std::declval<records&>(), move_only_lens {7}});
using construction_view =
    decltype(iv::intervals_view {std::declval<records&>(), construction_only_lens {7}});
using category_view = decltype(iv::intervals_view {std::declval<records>(), category_lens {}});
using stream_view = decltype(iv::intervals_view {std::declval<std::ranges::istream_view<numbers>>(),
                                                 numbers_lens {}});
using batch = iv::interval_batch<image_bytes, 8>;

static_assert(iv::ordered_domain<image_bytes>);
static_assert(!std::default_initializable<point>);
static_assert(!std::totally_ordered<point>);
static_assert(!std::three_way_comparable<point>);
static_assert(std::constructible_from<point, u64>);
static_assert(!std::convertible_to<u64, point>);
static_assert(!std::copy_constructible<record>);
static_assert(iv::interval_view<record_interval>);
static_assert(iv::interval_view<protocol_interval>);
static_assert(!iv::interval_view<volatile_boundary_interval>);
static_assert(std::constructible_from<record_interval, record&>);
static_assert(std::constructible_from<record_interval, const record&>);
static_assert(!std::constructible_from<record_interval, record>);
static_assert(!std::constructible_from<record_interval, const record>);
static_assert(!std::convertible_to<record&, record_interval>);
static_assert(!std::invocable<const record_lens&, record>);
static_assert(!std::invocable<const record_lens&, const record>);
static_assert(iv::interval_for<record_interval, image_bytes>);
static_assert(!iv::interval_for<record_interval, object_bytes>);
static_assert(!iv::interval_for<int, image_bytes>);
static_assert(!iv::interval_view<volatile Q>);
static_assert(!iv::interval_for<volatile Q, image_bytes>);
static_assert(!can_copy<volatile Q>);
static_assert(!iv::interval_range_for<int, image_bytes>);
static_assert(!iv::interval_range_for<std::vector<int>, image_bytes>);
static_assert(iv::interval_range_for<view&, image_bytes>);
static_assert(iv::interval_range_for<const view&, image_bytes>);
static_assert(!iv::interval_range_for<view&, object_bytes>);
static_assert(std::ranges::view<view>);
static_assert(!ownership::owning_value<view>);
static_assert(std::ranges::random_access_range<view>);
static_assert(std::ranges::sized_range<view>);
static_assert(!std::ranges::contiguous_range<view>);
static_assert(!std::ranges::borrowed_range<view>);
static_assert(std::ranges::borrowed_range<view&>);
static_assert(!std::ranges::borrowed_range<owning_view>);
static_assert(std::ranges::view<move_view>);
static_assert(!std::copy_constructible<move_view>);
static_assert(!std::default_initializable<move_view>);
static_assert(std::move_constructible<construction_only_lens>);
static_assert(!std::movable<construction_only_lens>);
static_assert(std::ranges::view<construction_view>);
static_assert(std::ranges::forward_range<batch>);
static_assert(std::ranges::sized_range<batch>);
static_assert(std::ranges::contiguous_range<batch>);
static_assert(std::ranges::common_range<batch>);
static_assert(iv::ordered_domain<immutable_offsets>);
static_assert(!std::assignable_from<iv::query<immutable_offsets>&, iv::query<immutable_offsets>>);
static_assert(std::ranges::forward_range<iv::interval_batch<immutable_offsets, 2>>);
static_assert(std::copy_constructible<iv::interval_batch<immutable_offsets, 2>>);
static_assert(!std::is_copy_assignable_v<iv::interval_batch<immutable_offsets, 2>>);
static_assert(!std::is_move_assignable_v<iv::interval_batch<immutable_offsets, 2>>);
static_assert(iv::interval_projection_for<move_only_lens, const record&, image_bytes>);
static_assert(!std::indirectly_regular_unary_invocable<move_only_lens, records::const_iterator>);
static_assert(iv::interval_range_for<category_view&, image_bytes>);
static_assert(!std::ranges::range<const category_view>);
static_assert(std::ranges::input_range<stream_view>);
static_assert(!std::ranges::forward_range<stream_view>);
static_assert(!std::ranges::common_range<stream_view>);
static_assert(!std::ranges::sized_range<stream_view>);
static_assert(!std::ranges::range<const stream_view>);
static_assert(can_project<records&, record_lens>);
static_assert(can_project<records, record_lens>);
static_assert(!can_project<records&, mutable_only_lens>);
static_assert(!can_project<records&, bad_result_lens>);
static_assert(!can_project<std::initializer_list<record>, record_lens>);
static_assert(std::constructible_from<batch, view&>);
static_assert(std::constructible_from<batch, owning_view>);
static_assert(std::constructible_from<batch, std::array<protocol_interval, 2>&>);
static_assert(!std::constructible_from<batch, records&>);
static_assert(!std::constructible_from<batch, std::vector<OQ>&>);
static_assert(!std::convertible_to<view&, batch>);
static_assert(!std::convertible_to<iv::query<object_bytes>, Q>);
// Equality uses domain comparison, without requiring point operators.
static_assert(std::equality_comparable<Q>);
static_assert(std::constructible_from<iv::interval_batch<image_bytes, 2>, batch&>);
static_assert(!std::convertible_to<batch&, iv::interval_batch<image_bytes, 2>>);
static_assert(can_visit<batch&, accepting>);
static_assert(!can_visit<batch&, bool_consumer>);
static_assert(!can_visit<std::vector<OQ>&, accepting>);
static_assert(!can_visit<borrowed_value_source, borrowed_accepting, borrowed_keys>);
static_assert(can_visit_inferred<view&, accepting>);
static_assert(!can_visit_inferred<view&, bool_consumer>);
static_assert(!can_visit_inferred<std::vector<int>&, accepting>);
static_assert(!can_visit_inferred<borrowed_value_source, borrowed_accepting>);
static_assert(!iv::expression<view>); // Runtime algebra participation still separate.
static_assert(!iv::expression<batch>);

void check(bool condition, const char *what)
{
    INFO(what);
    CHECK(condition);
}

template <class E, class F>
void expect_throw(F&& f, const char *what)
{
    INFO(what);
    CHECK_THROWS_AS(std::invoke(f), E);
}

records make_records()
{
    records out;
    ::ceph::util::emplace_back(out, 10, 20, 1);
    ::ceph::util::emplace_back(out, 30, 40, 2);

    return out;
}

void constructors_and_ownership()
{
    auto source = make_records();
    iv::intervals_view geometry {std::as_const(source), record_lens {}};
    check(std::size(geometry) == 2, "range cardinality");
    auto first = *std::begin(geometry);
    check(std::addressof(first.lower().key()) == std::addressof(source[0].lower),
          "borrowed key identity");
    check(iv::contains(first, point {10}) && !iv::contains(first, point {20}),
          "native borrowed view endpoints");
    batch owned {geometry};
    source[0].lower = point {12};
    check((*std::begin(geometry)).lower().key().value == 12, "lazy source observation");
    check(owned[0].lower().key().value == 10, "explicit owning copy independent of source");
    check(source[0].payload && *source[0].payload == "payload", "payload untouched");

    auto own_source = iv::intervals_view {make_records(), record_lens {}};
    check((*std::begin(own_source)).upper().key().value == 20,
          "owning rvalue source survives full expression");
    auto moved = std::move(own_source);
    check(std::size(moved) == 2 && iv::contains(*std::begin(moved), point {15}),
          "moved owning view usable");
    auto move_lens = iv::intervals_view {source, move_only_lens {42}};
    batch moved_lens_batch {move_lens};
    check(std::size(moved_lens_batch) == 2, "move-only nondefault lens stored and invoked");
    check(iv::visit_intervals<image_bytes>(move_lens, accepting {}).accepted == 2,
          "visitation borrows move-only views without requiring a view copy");
    const move_only_lens borrowed_lens {7};
    iv::intervals_view borrowing_lens {source, std::cref(borrowed_lens)};
    batch borrowing_lens_batch {borrowing_lens};
    check(std::size(borrowing_lens_batch) == 2,
          "explicit lens borrowing via standard reference wrapper");
    iv::intervals_view construction_lens {source, construction_only_lens {7}};
    auto reassigned_view = iv::intervals_view {source, construction_only_lens {8}};
    reassigned_view = std::move(construction_lens);
    check(std::size(batch {reassigned_view}) == 2,
          "view assignment without requiring lens assignment");
}

void overload_contracts_and_proxies()
{
    auto source = make_records();
    iv::intervals_view consistent {source, overload_lens {}};
    static_assert(iv::interval_range_for<decltype(consistent), image_bytes>);
    check(iv::contains(*std::begin(consistent), point {12}), "const overload actually invoked");
    std::ranges::transform_view baseline {std::ranges::ref_view {source}, overload_lens {}};
    static_assert(iv::interval_range_for<decltype(baseline), object_bytes>);
    static_assert(!iv::interval_range_for<decltype(baseline), image_bytes>);
    check(iv::contains(*std::begin(baseline), point {12}), "baseline uses other domain overload");

    iv::intervals_view changing_category {make_records(), category_lens {}};
    check(iv::contains(*std::begin(changing_category), point {12}),
          "mutable source category supported");
    std::vector<bool> bits {true, false, true};
    iv::intervals_view proxy_geometry {bits, proxy_lens {}};
    static_assert(iv::interval_range_for<decltype(proxy_geometry), image_bytes>);
    static_assert(
        !std::indirectly_regular_unary_invocable<proxy_lens, std::vector<bool>::iterator>);
    batch copied {proxy_geometry};
    check(std::size(copied) == 3 && iv::contains(copied[0], point {1}),
          "rvalue-only proxy projection");
    check(iv::is_empty(copied[1]) && iv::contains(copied[2], point {1}),
          "empty entries and duplicates retained");
}

void temporary_source_elements()
{
    auto source =
        std::views::iota(0, 3) | std::views::transform([](int n) {
            return Q::closed_open(point {static_cast<u64>(n)}, point {static_cast<u64>(n + 2)});
        });
    iv::intervals_view geometry {source, std::identity {}};
    static_assert(std::same_as<std::ranges::range_reference_t<decltype(geometry)>, Q>);
    auto first = *std::begin(geometry); // Source temporary has died after this line.
    check(iv::contains(first, point {1}), "identity projection from temporary element owns bounds");

    struct borrowed_query
    {
        using domain_type = image_bytes;

        const Q *source;

    public:
        const auto& lower() const { return source->lower(); }

        const auto& upper() const { return source->upper(); }

    public:
        bool explicitly_empty() const { return source->explicitly_empty(); }

    };

    struct borrow_lens
    {
        borrowed_query operator()(const Q& value) const { return {std::addressof(value)}; }

    };

    iv::intervals_view borrowing {source, borrow_lens {}};
    static_assert(std::same_as<std::ranges::range_reference_t<decltype(borrowing)>, Q>);
    auto borrowed_result = *std::begin(borrowing);
    check(iv::contains(borrowed_result, point {1}),
          "borrowed view value from temporary element materialized while alive");
    batch copied {borrowing};
    check(std::size(copied) == 3 && copied[2].lower().key().value == 2,
          "value source bulk conversion");

    std::array stable {Q::singleton(point {9})};
    iv::intervals_view by_reference {stable, std::identity {}};
    static_assert(std::same_as<std::ranges::range_reference_t<decltype(by_reference)>, const Q&>);
    check(std::addressof(*std::begin(by_reference)) == std::addressof(stable[0]),
          "lvalue source remains borrowed and read-only");
}

void foreign_protocol_and_boundaries()
{
    using B = iv::boundary<image_bytes>;

    std::array cases {protocol_interval {{B::closed(point {2})}, {B::open(point {7})}, false},
                      protocol_interval {{B::closed(point {5})}, {B::closed(point {5})}, false},
                      protocol_interval {{B::open(point {5})}, {B::closed(point {5})}, false},
                      protocol_interval {{B::negative_infinity()}, {B::positive_infinity()}, true},
                      protocol_interval {{B::negative_infinity()}, {B::positive_infinity()}, false},
                      protocol_interval {{B::open(point {2})}, {B::positive_infinity()}, false},
                      protocol_interval {{B::closed(point {7})}, {B::open(point {2})}, false}};
    batch copied {cases};
    for (const auto& original : cases) {
        for (u64 sample = 0; sample != 10; ++sample) {
            check(iv::contains(original, point {sample}) ==
                      iv::contains(iv::copy_interval(original), point {sample}),
                  "contains honors the minimal boundary protocol directly");
        }
    }
    check(std::size(copied) == std::size(cases),
          "foreign protocol converted without stronger concept");
    check(iv::contains(copied[0], point {2}) && !iv::contains(copied[0], point {7}),
          "closed/open retained");
    check(iv::contains(copied[1], point {5}), "equal closed bounds are singleton");
    check(iv::is_empty(copied[2]) && iv::is_empty(copied[3]),
          "empty bounds and explicit empty retained");
    check(iv::contains(copied[4], point {0}) && iv::contains(copied[4], point {999}),
          "infinite bounds copied safely");
    check(!iv::contains(copied[5], point {2}) && iv::contains(copied[5], point {3}),
          "open lower bound retained");
    check(iv::is_empty(copied[6]), "reversed bounds are empty");
    auto overlap = iv::intersection(iv::copy_interval(cases[0]), Q::closed(point {4}, point {9}));
    check(iv::contains(overlap, point {4}) && !iv::contains(overlap, point {7}),
          "actual algebra callable on converted foreign view");
}

void single_pass_and_capacity()
{
    std::istringstream input {"10 5 30 2"};
    iv::intervals_view geometry {std::ranges::istream_view<numbers> {input}, numbers_lens {}};
    batch copied {geometry};
    check(std::size(copied) == 2 && copied[1].upper().key().value == 32,
          "single-pass noncommon input");
    std::array<Q, 2> intervals {Q::singleton(point {1}), Q::singleton(point {2})};
    iv::interval_batch<image_bytes, 2> exact {intervals};
    check(std::size(exact) == 2, "exact capacity accepted");
    expect_throw<std::length_error>(
        [&] { iv::interval_batch<image_bytes, 1> too_small {intervals}; },
        "entry capacity must reject");
    expect_throw<std::length_error>([&] { iv::interval_batch<image_bytes, 0> zero {intervals}; },
                                    "zero capacity rejects nonempty source");
    iv::interval_batch<image_bytes, 0> empty {std::array<Q, 0> {}};
    check(std::size(empty) == 0, "zero capacity accepts empty source");
    std::array bad {numbers {std::numeric_limits<u64>::max(), 1}};
    iv::intervals_view overflowing {bad, numbers_lens {}};
    expect_throw<std::overflow_error>([&] { batch value {overflowing}; },
                                      "checked native endpoint conversion must throw");
    std::array limited_source {numbers {10, 5}, numbers {std::numeric_limits<u64>::max(), 1}};
    iv::intervals_view limited {limited_source, numbers_lens {}};
    expect_throw<std::length_error>(
        [&] { iv::interval_batch<image_bytes, 1> value {limited}; },
        "capacity must be checked before projecting an unadmitted extra entry");
}

void consumer_acceptance()
{
    std::array intervals {Q::singleton(point {1}), Q::singleton(point {2}),
                          Q::singleton(point {3})};
    unsigned calls = 0;
    auto sink = [&](const Q&) {
        return ++calls == 2 ? iv::acceptance::reject_and_stop : iv::acceptance::accept;
    };
    auto result = iv::visit_intervals<image_bytes>(intervals, sink);
    check(calls == 2 && result.offered == 2 && result.accepted == 1 && result.stopped,
          "reject isn't acknowledged");
    auto accept_stop = iv::visit_intervals<image_bytes>(
        intervals, [](const Q&) { return iv::acceptance::accept_and_stop; });
    check(accept_stop.offered == 1 && accept_stop.accepted == 1 && accept_stop.stopped,
          "accepted stop counted once");
    auto all = iv::visit_intervals<image_bytes>(intervals, accepting {});
    check(all.accepted == 3 && !all.stopped, "complete enumeration");

    struct overloaded_consumer final
    {
        iv::acceptance operator()(const Q&) { return iv::acceptance::accept; }

        bool operator()(Q&&) { return false; }

    };

    check(iv::visit_intervals<image_bytes>(intervals, overloaded_consumer {}).accepted == 3,
          "actual consumer invocation matches its checked const argument");
    auto move_sink = [counter = std::make_unique<unsigned>(0)](const Q&) mutable {
        return ++*counter == 1 ? iv::acceptance::accept_and_stop : iv::acceptance::accept;
    };
    auto moved = iv::visit_intervals<image_bytes>(intervals, std::move(move_sink));
    check(moved.accepted == 1, "move-only effectful consumer without copies");
    expect_throw<std::runtime_error>(
        [&] {
            (void)iv::visit_intervals<image_bytes>(intervals, [](const Q&) -> iv::acceptance {
                throw std::runtime_error("sink failure");
            });
        },
        "consumer exception propagated");

    std::istringstream input {"10 5 30 2"};
    iv::intervals_view stream {std::ranges::istream_view<numbers> {input}, numbers_lens {}};
    auto stopped = iv::visit_intervals<image_bytes>(
        stream, [](const Q&) { return iv::acceptance::reject_and_stop; });
    check(stopped.offered == 1 && stopped.accepted == 0, "single-pass rejected first entry");
    u64 next_offset = 0;
    input >> next_offset;
    check(next_offset == 30, "no increment/prefetch of next input after stop");

    auto expression = iv::set_union(intervals[0], iv::set_union(intervals[1], intervals[2]));
    unsigned emission_calls = 0;
    iv::for_each_interval(expression, [&](const Q&) {
        ++emission_calls;

        return iv::acceptance::reject_and_stop;
    });
    check(emission_calls == 3, "existing callback ignores return result");
}

void provenance_and_collection_policy()
{
    auto source = make_records();

    struct clipped_piece
    {
        Q interval;

        const record *origin;
    };

    const auto wanted = Q::closed_open(point {12}, point {16});
    std::ranges::transform_view clipped {
        std::ranges::ref_view {std::as_const(source)}, [wanted](const record& r) {
            return clipped_piece {iv::intersection(record_interval {r}, wanted), &r};
        }};
    auto first = *std::begin(clipped);
    check(first.interval.lower().key().value == 12 && first.origin->lower.value == 10,
          "origin survives geometry clip");
    check(first.origin->id == 1 && first.origin->payload.get() == source[0].payload.get(),
          "annotation and native payload identity retained");
    check(first.interval.lower().key().value - first.origin->lower.value == 2,
          "payload displacement computed outside ordered core");
}

struct byte_count
{
    u64 value;
};
enum struct metric_error { overflow };

struct advance_bytes
{
    std::expected<point, metric_error> operator()(const point& p, const byte_count& n) const
    {
        if (n.value > std::numeric_limits<u64>::max() - p.value) {
            return std::unexpected(metric_error::overflow);
        }

        return point {p.value + n.value};
    }

};

static_assert(std::regular_invocable<const advance_bytes&, const point&, const byte_count&>);
static_assert(!std::invocable<const advance_bytes&, const point&, u64>);

void optional_metrics_and_sizes()
{
    auto advanced = advance_bytes {}(point {10}, byte_count {3});
    check(advanced && advanced->value == 13, "independent checked metric");
    check(!advance_bytes {}(point {std::numeric_limits<u64>::max()}, byte_count {1}),
          "metric overflow checked");

    // A native bridge exposes covered_measure separately; iterator distance
    // supplies cardinality here. No incorrect size() CPO customization is added.
    struct honest_range
    {
        std::array<Q, 2> values;

    public:
        auto begin() const { return std::begin(values); }

        auto end() const { return std::end(values); }

    public:
        u64 covered_measure() const { return 100; }

    } range {{Q::closed_open(point {0}, point {40}), Q::closed_open(point {60}, point {120})}};

    static_assert(std::ranges::sized_range<honest_range>);
    check(std::ranges::size(range) == 2 && range.covered_measure() == 100,
          "cardinality independent of metric");

    struct non_regular_call
    {
        mutable unsigned count = 0;

    public:
        unsigned operator()(unsigned) const { return ++count; }

    } impure;

    static_assert(std::regular_invocable<const non_regular_call&, unsigned>);
    // Direct calls only; never pass this known semantic violation to an algorithm
    // whose contract requires regular_invocable modeling.
    check(impure(0) == 1 && impure(0) == 2, "syntax acceptance doesn't prove semantic laws");
    using IQ = iv::query<immutable_offsets>;

    std::array immutable {IQ::closed_open(immutable_point {3}, immutable_point {9})};
    iv::interval_batch<immutable_offsets, 2> constructed {immutable};
    check(std::size(constructed) == 1 && iv::contains(constructed[0], immutable_point {5}),
          "nondefault nonassignable point constructed in place");
}

// A second, nonnumeric domain verifies that the adapter layer needs neither
// point arithmetic nor assumptions about the point's own comparison operators.
struct descending_keys
{
    using value_type = std::string;

public:
    static std::strong_ordering compare(const std::string& a, const std::string& b)
    {
        return b <=> a;
    }

};

void nonnumeric_domain()
{
    using K = iv::query<descending_keys>;

    std::array keys {K::closed_open("delta", "alpha"), K::singleton(std::string {"x\0y", 3})};
    iv::intervals_view geometry {keys, std::identity {}};
    static_assert(iv::interval_range_for<decltype(geometry), descending_keys>);
    iv::interval_batch<descending_keys, 2> copied {geometry};
    keys[0] = K::empty();
    check(iv::contains(copied[0], std::string {"beta"}), "nonnumeric domain comparator used");
    check(!iv::contains(copied[0], std::string {"alpha"}), "custom collation exclusive endpoint");
    check(iv::contains(copied[1], std::string {"x\0y", 3}), "embedded NUL preserved");
    check(iv::is_empty(keys[0]) && !iv::is_empty(copied[0]),
          "string point ownership independent of source");
}

void ownership_profiles()
{
    std::string backing = "borrowed";
    std::array values {BQ::singleton(std::string_view {backing})};
    iv::intervals_view borrowed {values, std::identity {}};
    auto point_copy = iv::copy_interval(*std::begin(borrowed));
    check(std::data(point_copy.lower().key()) == std::data(backing),
          "copying a borrowed point doesn't own its bytes");
    auto visited = iv::visit_intervals<borrowed_keys>(borrowed, [](const BQ& q) {
        return iv::contains(q, std::string_view {"borrowed"}) ? iv::acceptance::accept
                                                              : iv::acceptance::reject_and_stop;
    });
    check(visited.accepted == 1, "scoped borrowed-domain visitation stays available");

    std::weak_ptr<const std::string> lifetime;
    {
        auto copied = [&] {
            auto owner = std::make_shared<const std::string>("retained");
            lifetime = owner;
            using RQ = iv::query<retained_keys>;

            std::array source {RQ::singleton(retained_point {owner})};

            return iv::interval_batch<retained_keys, 1> {source};
        }();
        check(!lifetime.expired() && copied[0].lower().key().value == "retained",
              "audited point copy retains backing owner");
    }
    check(lifetime.expired(), "retained point storage released with batch");
}

constexpr bool constant_evaluation()
{
    std::array source {Q::singleton(point {4}), Q::closed_open(point {10}, point {15})};
    iv::interval_batch<image_bytes, 2> batch {source};
    auto result = iv::visit_intervals(batch, [](const Q&) { return iv::acceptance::accept; });

    return result.accepted == 2 && iv::contains(batch[1], point {12});
}

static_assert(constant_evaluation());
static_assert([] {
    std::array source {Q::closed_open(point {2}, point {8})};
    iv::intervals_view geometry {std::as_const(source), std::identity {}};
    iv::interval_batch<image_bytes, 1> copied {geometry};

    return copied[0] == iv::intersection(*std::ranges::begin(geometry), Q::universal());
}());
static_assert(Q::singleton(point {4}) == Q::closed(point {4}, point {4}));
static_assert(Q::empty() != Q::universal());
static_assert([] {
    using IQ = iv::query<immutable_offsets>;

    iv::static_interval_set<immutable_offsets, 1> entries;
    entries.push_back(IQ::singleton(immutable_point {7}));

    return entries[0] == IQ::singleton(immutable_point {7});
}());

} // interval_data_test

TEST_CASE("Datum interval ranges: constructors and ownership", "[datum][interval][range]")
{
    interval_data_test::constructors_and_ownership();
}

TEST_CASE("Datum interval ranges: overload contracts and proxies", "[datum][interval][range]")
{
    interval_data_test::overload_contracts_and_proxies();
}

TEST_CASE("Datum interval ranges: temporary source elements", "[datum][interval][range]")
{
    interval_data_test::temporary_source_elements();
}

TEST_CASE("Datum interval ranges: foreign protocol and boundaries", "[datum][interval][range]")
{
    interval_data_test::foreign_protocol_and_boundaries();
}

TEST_CASE("Datum interval ranges: single pass and capacity", "[datum][interval][range]")
{
    interval_data_test::single_pass_and_capacity();
}

TEST_CASE("Datum interval ranges: consumer acceptance", "[datum][interval][range]")
{
    interval_data_test::consumer_acceptance();
}

TEST_CASE("Datum interval ranges: provenance and collection policy", "[datum][interval][range]")
{
    interval_data_test::provenance_and_collection_policy();
}

TEST_CASE("Datum interval ranges: optional metrics and sizes", "[datum][interval][range]")
{
    interval_data_test::optional_metrics_and_sizes();
}

TEST_CASE("Datum interval ranges: nonnumeric domain", "[datum][interval][range]")
{
    interval_data_test::nonnumeric_domain();
}

TEST_CASE("Datum interval ranges: ownership profiles", "[datum][interval][range]")
{
    interval_data_test::ownership_profiles();
}

TEST_CASE("SALad range visitation is independent of geometry", "[datum][interval][range]")
{
    using namespace ceph::salad::datum;
    std::array values {std::string {"first"}, std::string {"second"}};
    const auto result = visit_range(values, [](std::string& value) {
        value += " accepted";

        return acceptance::accept_and_stop;
    });
    CHECK(1 == result.offered);
    CHECK(1 == result.accepted);
    CHECK(result.stopped);
    CHECK("first accepted" == values[0]);
    CHECK("second" == values[1]);

    const auto by_value =
        visit_range(std::views::iota(0, 3), [](int&&) { return acceptance::accept; });
    CHECK(3 == by_value.accepted);
    CHECK(!by_value.stopped);
    CHECK(0 == visit_range(std::array<int, 0> {}, [](int&) {
                   return acceptance::reject_and_stop;
               }).offered);
}

// The same fixture file also supplies real compiler-failure probes. Select a
// case with -DDATUM_INTERVAL_NEGATIVE=N -fsyntax-only; ordinary tests use none.
#if defined(DATUM_INTERVAL_NEGATIVE)
 void rejected_interval_usage()
 {
     using namespace interval_data_test;
 #if DATUM_INTERVAL_NEGATIVE == 1
      auto source = make_records();
      iv::intervals_view geometry {source, record_lens {}};
      iv::interval_batch<object_bytes, 2> wrong_domain {geometry};
 #elif DATUM_INTERVAL_NEGATIVE == 2
      record_interval dangling {record {10, 20, 1}};
 #elif DATUM_INTERVAL_NEGATIVE == 3
      auto source = make_records();
      iv::intervals_view mutable_lens {source, mutable_only_lens {}};
 #elif DATUM_INTERVAL_NEGATIVE == 4
      std::array values {Q::singleton(point {1})};
      (void)iv::visit_intervals<image_bytes>(values, bool_consumer {});
 #elif DATUM_INTERVAL_NEGATIVE == 5
      auto source = make_records();
      iv::intervals_view geometry {source, record_lens {}};
      batch implicit_conversion = geometry;
 #elif DATUM_INTERVAL_NEGATIVE == 6
      volatile Q value;
      (void)iv::copy_interval(value);
 #elif DATUM_INTERVAL_NEGATIVE == 7
      volatile_boundary_interval value {{iv::boundary<image_bytes>::negative_infinity()}};
      (void)iv::copy_interval(value);
 #elif DATUM_INTERVAL_NEGATIVE == 8
      iv::intervals_view changed_domain {make_records(), category_lens {}};
      (void)std::as_const(changed_domain).begin();
 #elif DATUM_INTERVAL_NEGATIVE == 9
      iv::interval_batch<borrowed_keys, 2> false_owner;
 #elif DATUM_INTERVAL_NEGATIVE == 10
      borrowed_value_source source;
      iv::intervals_view unsafe_value_projection {source, std::identity {}};
 #elif DATUM_INTERVAL_NEGATIVE == 11
      auto value = iv::boundary_ref<image_bytes>::closed(point {1});
      (void)value;
 #elif DATUM_INTERVAL_NEGATIVE == 12
      auto values = std::views::iota(0, 4) | std::views::transform([](int n) {
                        return iv::query<image_bytes>::singleton(point {static_cast<u64>(n)});
                    });
      iv::canonical_view value {values};
      (void)value;

 #endif
 }
#endif

TEST_CASE("canonical lvalue visitation observes source identity", "[datum][range]")
{
    namespace math = ceph::salad::datum::interval;
    struct domain
    {
        using value_type = std::string;

    public:
        static std::strong_ordering compare(const std::string& lhs, const std::string& rhs)
        {
            return lhs <=> rhs;
        }

    };
    const std::array values {
        math::query<domain>::closed(std::string(100, 'a'), std::string(100, 'b'))};
    bool identical = false;
    const auto result = math::visit_intervals(values, [&](const auto& value) {
        identical = &value == &values.front();

        return ceph::salad::datum::acceptance::accept;
    });
    CHECK(identical);
    CHECK(result.accepted == 1);
}

// Datum data tests: neutral record enrollment, ownership, field codecs and sinks.
// Keep domain profiles in these fixtures, outside the shared mechanisms.

namespace datum_test {

struct record
{
    std::string name;
    std::uint64_t version;

public:
    bool operator==(const record&) const = default;

};
struct collision_record
{
    int project;
    int record_type;
};
class constructed_record final
{
    [[maybe_unused]] int hidden = 3;

public:
    int value;

public:
    constexpr explicit constructed_record(const int initial) : value(initial)
    {}

};
struct borrowed_record
{
    std::string_view name;
};
struct ticks
{
    std::int64_t count;
};
struct failing_field final
{
    int value = 0;
};
struct clock_record
{
    ticks value;
};
struct scalar_record
{
    int first;

    int second;
};
struct entry final
{
    std::string key;
    std::uint64_t version = 99;
    std::string value;

public:
    bool operator==(const entry&) const = default;

};
struct local_entry final
{
    std::string key;
    std::uint64_t version = 99;
    std::string value;

public:
    std::function<void()> on_change;

};
struct record_with_schema_names final
{
    using record_type = scalar_record;
    using fields = datum::binary_fields<&scalar_record::first>;

    int value;
};
static_assert(!datum::binary_schema_type<record_with_schema_names>);
static_assert(!datum::binary_schema_type<datum::binary_fields<>>);
static_assert(datum::binary_schema_type<datum::binary_fields<&borrowed_record::name>>);
static_assert(!datum::binary_schema_type<datum::binary_fields<&entry::key, &entry::key>>);
static_assert(!datum::binary_schema_type<datum::binary_fields<&entry::key, &scalar_record::first>>);
static_assert(std::same_as<datum::binary_schema_t<scalar_record>,
                           datum::binary_fields<&scalar_record::first, &scalar_record::second>>);
static_assert(std::same_as<datum::binary_schema_t<const scalar_record,
                                                 &scalar_record::first>::record_type,
                           const scalar_record>);
static_assert(std::same_as<datum::binary_schema_t<const scalar_record>,
                           datum::binary_schema_t<const scalar_record, &scalar_record::first,
                                                  &scalar_record::second>>);

class private_record final
{
    int hidden;

public:
    static constexpr auto selection() { return datum::binary_fields<&private_record::hidden> {}; }

};
static_assert(!datum::binary_schema_type<decltype(private_record::selection())>);

struct derived_record final : scalar_record
{
    int local;
};
static_assert(!datum::binary_schema_type<datum::binary_fields<&derived_record::local>>);
// A member pointer identifies its declaring class, including inherited spelling.
static_assert(std::same_as<datum::binary_fields<&derived_record::first>::record_type,
                           scalar_record>);

struct projected_text
{
    std::string text;
};
struct projected_record
{
    projected_text value;
};
struct projected_scalar
{
    int value;
};
struct scalar_projection_record
{
    projected_scalar value;
};
struct fallible_projection
{
    int value;
    bool fail;
};
struct fallible_projection_record
{
    fallible_projection value;
};
struct moving_projection
{
    int value;
    bool fail;

public:
    constexpr moving_projection(int number, bool failing) noexcept : value(number), fail(failing)
    {}
    constexpr moving_projection(moving_projection&& source) : value(source.value), fail(source.fail)
    {
        if (fail) {
            throw std::runtime_error {"projection move failed"};
        }
    }

};
struct move_projection_source
{
    int value;
    bool fail;
};
struct move_projection_record
{
    move_projection_source value;
};
struct immovable_projection final
{
    int value;

public:
    constexpr explicit immovable_projection(const int initial) noexcept : value(initial)
    {}
    immovable_projection(const immovable_projection&) = delete;
    immovable_projection(immovable_projection&&) = delete;

};
struct immovable_record final
{
    move_projection_source value;
};
enum struct label { first = 2, alias = 2, last = 9 };
enum struct flag : bool { set = true };
enum struct letter : char { first = 1 };
} // datum_test

namespace ceph::salad::datum {

template <>
struct member_view<datum_test::projected_text>
{
    using type = std::string_view;

public:
    static constexpr type get(const datum_test::projected_text& value) noexcept
    {
        return value.text;
    }

};
template <>
struct member_view<datum_test::projected_scalar>
{
    using type = int;

public:
    static constexpr type get(const datum_test::projected_scalar& value) noexcept
    {
        return value.value;
    }

};
template <>
struct member_view<datum_test::fallible_projection>
{
    using type = int;

public:
    static constexpr type get(const datum_test::fallible_projection& value)
    {
        if (value.fail) {
            throw std::runtime_error {"projection failed"};
        }

        return value.value;
    }

};
template <>
struct member_view<datum_test::move_projection_source>
{
    using type = datum_test::moving_projection;

public:
    static constexpr type get(const datum_test::move_projection_source& value) noexcept
    {
        return {value.value, value.fail};
    }

};
template <>
struct member_view<datum_test::immovable_record>
{
    using type = datum_test::immovable_projection;

public:
    static constexpr type get(const datum_test::immovable_record& value) noexcept
    {
        return type {value.value.value};
    }

};
} // ceph::salad::datum

namespace ceph::salad::datum::render {

template <>
struct enum_schema<datum_test::label> : reflected_enum_schema<datum_test::label>
{};
template <>
struct enum_schema<datum_test::flag> : reflected_enum_schema<datum_test::flag>
{};
template <>
struct enum_schema<datum_test::letter> : reflected_enum_schema<datum_test::letter>
{};
template <>
struct display_schema<datum_test::record> : reflected_display_schema<^^datum_test::record, 2>
{};
} // ceph::salad::datum::render

namespace ceph::salad::datum {

template <>
struct binary_schema<datum_test::record>
    : checked_binary_schema<datum_test::record, &datum_test::record::name,
                            &datum_test::record::version>
{};
#ifdef DATUM_HAVE_ZPP
 template <>
 struct binary_field_codec<datum_test::ticks>
 {
     static constexpr auto apply(auto& archive, auto& value) { return archive(value.count); }

 };
 template <>
 struct binary_field_codec<datum_test::failing_field>
 {
     static zpp::bits::errc apply(auto&, auto&)
     {
         throw std::system_error {std::make_error_code(std::errc::permission_denied),
                                  "custom field representation"};
     }

 };
#endif
template <>
struct binary_schema<datum_test::clock_record>
    : checked_binary_schema<datum_test::clock_record, &datum_test::clock_record::value>
{};
template <>
struct binary_schema<datum_test::scalar_record>
    : checked_binary_schema<datum_test::scalar_record, &datum_test::scalar_record::second,
                            &datum_test::scalar_record::first>
{};
} // ceph::salad::datum

#ifdef DATUM_HAVE_ZPP
 namespace datum_test {

 template <typename T>
 requires datum::binary_record<T>
 auto serialize(const T&) -> zpp::bits::members<datum::binary_member_count<T>>;

 template <typename ArchiveT, typename T>
 requires datum::binary_record<T>
 constexpr auto serialize(ArchiveT& archive, T& value)
 {
     return datum::serialize_schema(
         archive, value, typename datum::binary_schema<std::remove_cvref_t<T>>::fields {});
 }
 } // datum_test

#endif

using scalar_fields = datum::binary_schema<datum_test::scalar_record>::fields;

template <typename ValueT, typename SinkT>
concept observable_scalar = requires(const ValueT& value, SinkT&& sink) {
    scalar_fields::observe(value, std::forward<SinkT>(sink));
};
static_assert(!observable_scalar<datum_test::collision_record, decltype([](const auto&) {})>);
static_assert(!observable_scalar<datum_test::scalar_record, decltype([](int) { return true; })>);
using first_lens = datum::member_lens<&datum_test::scalar_record::first>;

struct incomplete_record;
static_assert(!std::invocable<first_lens, const incomplete_record&>);
static_assert(std::invocable<first_lens, const datum_test::scalar_record&>);
static_assert(!std::invocable<first_lens, datum_test::scalar_record&&>);
static_assert(!std::invocable<first_lens, const datum_test::scalar_record&&>);
static_assert(!std::invocable<first_lens, datum::view_of<datum_test::scalar_record>&&>);
using scalar_projection_lens = datum::member_lens<&datum_test::scalar_projection_record::value>;
// Query the projection before constructing a view: deduction must complete it.
static_assert(
    std::same_as<std::invoke_result_t<scalar_projection_lens,
                                      const datum::view_of<datum_test::scalar_projection_record>&>,
                 const int&>);
static_assert([] {
    const datum_test::scalar_record source {3, 7};
    struct named_view : datum::view_of<datum_test::scalar_record>
    {
        using view_of::view_of;
    };
    const named_view view {source};
    std::array<int, 2> values {};
    auto next = std::begin(values);
    scalar_fields::observe(view, [&next](const int value) { *next++ = value; });
    constexpr datum::member_lens<&datum_test::scalar_record::first> first;
    static_assert(std::same_as<decltype(first(view)), const int&>);

    return values == std::array {7, 3} && &first(view) == &source.first;
}());

TEST_CASE("Datum selected fields preserve schema order and owner/view association", "[datum][data]")
{
    datum_test::scalar_record source {3, 7};
    const datum::view_of view {source};
    std::array<int, 2> values {};
    scalar_fields::observe(view, [next = std::begin(values), token = std::make_unique<int>(1)](
                                     const int field) mutable { *next++ = *token * field; });
    CHECK(values == std::array {7, 3});
    source.first = 11;
    CHECK(datum::member_lens<&datum_test::scalar_record::first> {}(view) == 11);
    CHECK(&datum::member_lens<&datum_test::scalar_record::first> {}(view) == &source.first);
#ifdef DATUM_HAVE_ZPP
     CHECK_FALSE(datum::render::serializable<decltype(view)>);

     const datum_test::clock_record custom {{-123}};
     const datum::view_of custom_view {custom};
     datum::render::binary_buffer observed;
     zpp::bits::out output {observed};
     datum::binary_schema<datum_test::clock_record>::fields::observe(
         custom_view,
         [&output](const auto& field) { datum::serialize_field(output, field).or_throw(); });
     CHECK(observed == datum::render::to_bytes(custom));
#endif
}

TEST_CASE("Datum field lenses retain a consumer's value projections", "[datum][data]")
{
    const datum_test::projected_record source {{"projected"}};
    const datum::view_of view {source};
    constexpr datum::member_lens<&datum_test::projected_record::value> lens;
    static_assert(std::same_as<decltype(lens(source)), const datum_test::projected_text&>);
    static_assert(std::same_as<decltype(lens(view)), const std::string_view&>);
    CHECK(lens(view) == "projected");
    CHECK(std::data(lens(view)) == std::data(source.value.text));
    CHECK(&lens(view) == &view.value);

    struct mutable_record
    {
        mutable int value;
    };
    const mutable_record counter {7};
    constexpr datum::member_lens<&mutable_record::value> counter_lens;
    static_assert(std::same_as<decltype(counter_lens(counter)), const int&>);
    CHECK(&counter_lens(counter) == &counter.value);
}

TEST_CASE("Datum ordered lenses allow policy before observation and propagate consumer failures",
          "[datum][data]")
{
    struct record
    {
        int value;
        unsigned& reads;
    };
    constexpr auto observe = [](const record& value) {
        ++value.reads;

        return value.value;
    };
    using fields = datum::field_order<observe>;

    unsigned reads = 0;
    const record source {7, reads};
    fields::visit([](const auto&) {});
    CHECK(reads == 0);
    fields::visit([&source](const auto& lens) { CHECK(std::invoke(lens, source) == 7); });
    CHECK(reads == 1);

    const datum_test::scalar_record ordered {3, 7};
    unsigned calls = 0;
    CHECK_THROWS_AS(scalar_fields::observe(ordered,
                                           [&calls](int) {
                                               ++calls;
                                               throw std::runtime_error {"consumer failed"};
                                           }),
                    std::runtime_error);
    CHECK(calls == 1);
    using empty_fields = datum::field_order<>;

    empty_fields::observe(ordered, [&calls](int) { ++calls; });
    CHECK(calls == 1);
}

TEST_CASE("generated views construct projections directly without intermediate moves",
          "[datum][view]")
{
    using R = datum_test::record;
    using F = datum_test::fallible_projection_record;
    using M = datum_test::move_projection_record;

    static_assert(std::is_nothrow_constructible_v<datum::view_of<R>, const R&>);
    static_assert(!std::is_nothrow_constructible_v<datum::view_of<F>, const F&>);
    static_assert(std::is_nothrow_constructible_v<datum::view_of<M>, const M&>);
    const F failing_accessor {{7, true}};
    CHECK_THROWS_AS(datum::view_of {failing_accessor}, std::runtime_error);
    const M valid {{7, true}};
    datum::view_of view {valid};
    CHECK(view.value.value == 7);
    // Explicitly moving an existing view still moves its chosen representation.
    CHECK_THROWS_AS(datum::view_of<M>(std::move(view)), std::runtime_error);
    struct source final
    {
        datum_test::immovable_record value;
    };
    const source value {{{9, false}}};
    const datum::view_of immovable {value};
    static_assert(!std::is_move_constructible_v<decltype(immovable)>);
    CHECK(immovable.value.value == 9);
}

#ifdef DATUM_HAVE_ZPP
 template <typename T>
 concept accepts_candidate_decode = requires(datum::render::binary_view bytes) {
     datum::render::decode_bounded<T, 128>(bytes, 128);
 };
 struct no_default_candidate
 {
     no_default_candidate() = delete;

 };
 static_assert(!accepts_candidate_decode<no_default_candidate>);
 static_assert(!accepts_candidate_decode<const datum_test::record>);
 static_assert(accepts_candidate_decode<datum_test::record>);
#endif

static_assert(datum::owning_value<datum_test::record>);
static_assert(!datum::owning_value<datum_test::borrowed_record>);
static_assert(datum::render::displayable<datum_test::record>);
#ifdef DATUM_HAVE_ZPP
 static_assert(datum::render::serializable<datum_test::record>);
#endif
static_assert(datum::render::displayable<datum::traversal::page_traversal_result<int>>);
#ifdef DATUM_HAVE_ZPP
 static_assert(!datum::render::serializable<datum::traversal::page_traversal_result<int>>);

 static_assert([] {
     datum_test::record value {"constant data", 41};
     auto bytes = datum::render::to_bytes(value);
     if (datum::render::from_bytes<datum_test::record>(bytes) != value) {
         return false;
     }
     datum_test::record restored;
     datum::render::to_bytes(value, bytes);
     datum::render::from_bytes(bytes, restored);
     std::string text;
     datum::render::format_to(std::back_inserter(text), restored);

     return restored == value && text == "record{name=\"constant data\", version=41}";
 }());

 static_assert([] {
     const datum_test::clock_record value {{-123}};
     const auto bytes = datum::render::to_bytes(value);

     return bytes == datum::render::to_bytes(std::int64_t {-123}) &&
            datum::render::from_bytes<datum_test::clock_record>(bytes).value.count == -123;
 }());
#endif

static_assert([] {
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    const auto segment = datum::key::fixed_width_hexadecimal_segment(maximum);
    const auto parent = datum::key::compose("neutral", std::string_view {"a\0b", 3});
    const auto child = parent / "leaf";
    const auto decoded = datum::key::decode<std::string, std::string, std::string>(child);
    const auto plan =
        datum::plan::with_control(datum::plan::make_query(datum::key::prefix(parent)), 4);

    return datum::key::decimal_segment(maximum) == "18446744073709551615" &&
           datum::key::decode_fixed_width_hexadecimal_segment<std::uint64_t>(
               std::string_view {segment}) == maximum &&
           decoded && std::get<1>(*decoded) == std::string_view {"a\0b", 3} &&
           datum::plan::is_resolved(plan) && datum::plan::control_count(plan) == 1 &&
           datum::interval::contains(plan.intervals, datum::key::key_view(child));
}());

static_assert([] {
    using schema = datum::render::enum_schema<datum_test::label>;

    return schema::name(datum_test::label::alias) == "first" &&
           std::empty(schema::name(static_cast<datum_test::label>(99)));
}());

static_assert([] {
    std::string text;
    datum::render::format_to(std::back_inserter(text), datum_test::flag {});
    text += ';';
    datum::render::format_to(std::back_inserter(text), static_cast<datum_test::letter>(2));

    return text == "unknown(0);unknown(2)";
}());

static_assert([] {
    std::string text;
    for (const auto value : std::array {std::numeric_limits<std::int64_t>::min(), std::int64_t {0},
                                        std::numeric_limits<std::int64_t>::max()}) {
        datum::render_detail::append_integer(std::back_inserter(text), value);
        text += ';';
    }
    datum::render_detail::append_integer(std::back_inserter(text),
                                         std::numeric_limits<std::uint64_t>::max());

    return text == "-9223372036854775808;0;9223372036854775807;18446744073709551615";
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

    return !datum::plan::is_resolved(query) && datum::plan::requirement_count(query) == 1 &&
           std::get<0>(query.requirements).value == 7;
}());

TEST_CASE("Datum reflected enum tables preserve aliases and unknown values", "[datum][data]")
{
    using schema = datum::render::enum_schema<datum_test::label>;

    CHECK(schema::name(datum_test::label::first) == "first");
    CHECK(schema::name(datum_test::label::alias) == "first");
    CHECK(schema::name(datum_test::label::last) == "last");
    CHECK(std::empty(schema::name(static_cast<datum_test::label>(99))));
}

TEST_CASE("Datum page progress is diagnostic data", "[datum][data]")
{
    const datum::traversal::page_traversal_result<int> progress {
        .resume = 8, .pages = 3, .outcome = datum::traversal::page_traversal_outcome::stopped};
    std::string text;
    datum::render::format_to(std::back_inserter(text), progress);
    CHECK(text == "page_traversal_result{resume=8, pages=3, outcome=stopped}");
}

TEST_CASE("Datum consumer schemas display and round-trip unrelated owning records", "[datum][data]")
{
    const datum_test::record value {"unrelated domain", 41};
    std::string text;
    datum::render::format_to(std::back_inserter(text), value);
    CHECK(text == "record{name=\"unrelated domain\", version=41}");
#ifdef DATUM_HAVE_ZPP
     const auto bytes = datum::render::to_bytes(value);
     CHECK(datum::render::from_bytes<datum_test::record>(bytes) == value);
     auto trailing = bytes;
     ::ceph::util::push_back(trailing, 0);
     CHECK_THROWS_AS(datum::render::from_bytes<datum_test::record>(trailing), std::runtime_error);
     CHECK_THROWS_AS(
         datum::render::from_bytes<datum_test::record>(datum::render::binary_view {bytes}.first(1)),
         std::runtime_error);
#endif
}

#ifdef DATUM_HAVE_ZPP
 TEST_CASE("codec failures retain structured errors and complete-input checks", "[datum][codec]")
 {
     const auto check_error = [](auto&& operation, const std::errc expected) {
         bool caught = false;
         try {
             operation();
         } catch (const std::system_error& error) {
             caught = true;
             CHECK(error.code() == std::make_error_code(expected));
         }
         CHECK(caught);
     };
     const datum_test::record source {"structured errors", 7};
     const auto bytes = datum::render::to_bytes(source);
     const auto incomplete = std::span {bytes}.first(1);
     check_error([&] { datum::render::from_bytes<datum_test::record>(incomplete); },
                 std::errc::result_out_of_range);
     check_error([&] { datum::render::decode_bounded<datum_test::record, 4>(bytes, bytes.size()); },
                 std::errc::message_size);
     std::array<std::uint8_t, 1> storage;
     check_error([&] { datum::render::to_bytes(source, std::span {storage}); },
                 std::errc::result_out_of_range);
     auto trailing = bytes;
     trailing.push_back(0);
     check_error([&] { datum::render::from_bytes<datum_test::record>(trailing); },
                 std::errc::bad_message);
     check_error([&] { datum::render::decode_bounded<datum_test::record, 128>(trailing, 512); },
                 std::errc::bad_message);

     struct custom_record final { datum_test::failing_field field; };
     using wire = datum::binary_fields<&custom_record::field>;
     datum::render::binary_buffer output;
     check_error([&] { datum::render::to_bytes<wire>(custom_record {}, output); },
                 std::errc::permission_denied);
     check_error([&] { datum::render::from_bytes<wire>(bytes); }, std::errc::permission_denied);
 }

 TEST_CASE("Datum field representations are selected by consumer profiles", "[datum][codec]")
 {
     const datum_test::clock_record value {{123}};
     const auto bytes = datum::render::to_bytes(value);
     CHECK(datum::render::from_bytes<datum_test::clock_record>(bytes).value.count == 123);
     CHECK(bytes == datum::render::to_bytes(std::int64_t {123}));
 }
#endif

TEST_CASE("Datum key layout and page traversal work without a native context", "[datum][data]")
{
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
    REQUIRE(decoded);
    CHECK(std::empty(std::get<1>(*decoded)));
    CHECK(std::get<2>(*decoded) == admitted);
    CHECK(datum::key::key_view(encoded) == std::string_view {"space\0\0a\0\xFF\xFF\0", 12});
}

static_assert([] {
    const auto encoded = datum::key::compose("space", std::string_view {"\0\xFF", 2});
    const auto decoded = datum::key::decode<std::string, std::string>(encoded);

    return decoded && std::get<1>(*decoded) == std::string_view {"\0\xFF", 2};
}());

TEST_CASE("generated member views preserve names and reject temporary records", "[datum][view]")
{
    datum_test::record source {"borrowed", 42};
    datum::view_of<datum_test::record> view {source};
    static_assert(std::same_as<decltype(view.name), const std::string&>);
    static_assert(!std::constructible_from<decltype(view), datum_test::record&&>);
    static_assert(!datum::owning_value<decltype(view)>);
    CHECK(&view.name == &source.name);
    CHECK(&view.version == &source.version);
    source.version = 43;
    CHECK(view.version == 43);
    struct derived_view : datum::view_of<datum_test::record>
    {
        using datum::view_of<datum_test::record>::view_of;
    };
    CHECK(derived_view {source}.version == 43);
    datum_test::collision_record collisions {1, 2};
    const datum::view_of named {collisions};
    CHECK(named.project == 1);
    CHECK(named.record_type == 2);
}

static_assert([] {
    datum_test::constructed_record source {7};
    const datum::view_of view {source};
    static_assert(std::same_as<decltype(view.value), const int&>);
    static_assert(!std::constructible_from<decltype(view), datum_test::constructed_record&&>);
    static_assert(!std::constructible_from<decltype(view), const datum_test::constructed_record&&>);
    static_assert(std::size(datum::detail::record_members<datum_test::constructed_record>) == 1);

    return &view.value == &source.value && view.value == 7;
}());

TEST_CASE("generated views observe constructor-bearing public records", "[datum][view]")
{
    datum_test::constructed_record source {41};
    struct constructed_view : datum::view_of<datum_test::constructed_record>
    {
        using datum::view_of<datum_test::constructed_record>::view_of;
    };
    const constructed_view view {source};
    CHECK(&view.value == &source.value);
    source.value = 42;
    CHECK(view.value == 42);
}

#ifdef DATUM_HAVE_ZPP
 TEST_CASE("fixed and fragmented codec endpoints share the enrolled wire", "[datum][bytes]")
 {
     const datum_test::record source {std::string(104, 'x'), 42};
     const auto expected = datum::render::to_bytes(source);
     std::array<std::uint8_t, 512> storage;
     storage.fill(0xAA);
     const auto written = datum::render::to_bytes(source, std::span {storage});
     CHECK(std::ranges::equal(written, expected));
     CHECK(storage[std::size(written)] == 0xAA);
     std::array<std::byte, 512> byte_storage {};
     const auto byte_output = datum::render::to_bytes(source, std::span {byte_storage});
     CHECK(datum::render::from_bytes<datum_test::record>(byte_output) == source);
     CHECK((datum::render::decode_bounded<datum_test::record, 128>(byte_output, 512) == source));
     std::array<std::uint8_t, 512> fragments_storage {};
     const std::array fragments {
         std::span {fragments_storage}.first(3), std::span {fragments_storage}.subspan(3, 9),
         std::span {fragments_storage}.subspan(12, std::size(expected) - 12)};
     CHECK(datum::render::to_bytes(source, fragments, std::span {storage}) == std::size(expected));
     CHECK(datum::render::from_bytes<datum_test::record>(fragments, std::span {storage}) == source);
     CHECK((datum::render::decode_bounded<datum_test::record, 128>(expected, 512) == source));
     CHECK_THROWS_AS((datum::render::decode_bounded<datum_test::record, 16>(expected, 512)),
                     std::runtime_error);
     CHECK_THROWS_AS((datum::render::decode_bounded<datum_test::record, 128>(expected, 16)),
                     std::length_error);
     CHECK_THROWS_AS(datum::render::to_bytes(source, std::span {storage}.first(8)),
                     std::runtime_error);
     auto trailing = expected;
     ::ceph::util::push_back(trailing, 0);
     CHECK_THROWS_AS((datum::render::decode_bounded<datum_test::record, 128>(trailing, 512)),
                     std::runtime_error);
     auto truncated = expected;
     truncated.pop_back();
     CHECK_THROWS_AS((datum::render::decode_bounded<datum_test::record, 128>(truncated, 512)),
                     std::runtime_error);
     const std::array too_small {std::span {fragments_storage}.first(1)};
     fragments_storage.fill(0xAA);
     CHECK_THROWS_AS(datum::scatter_bytes(std::span {expected}, too_small), std::length_error);
     CHECK(fragments_storage.front() == 0xAA);
 }

 static_assert([] {
     datum_test::record source {"constexpr", 7};
     std::array<std::uint8_t, 64> storage {};
     const auto bytes = datum::render::to_bytes(source, std::span {storage});
     const datum::view_of view {source};

     return datum::render::decode_bounded<datum_test::record, 64>(bytes, 64) == source &&
            view.version == 7;
 }());

 TEST_CASE("direct binary schemas choose declaration or explicit wire order", "[datum][schema]")
 {
     using wire = datum::binary_schema_t<datum_test::entry>;
     using reordered = datum::binary_fields<&datum_test::entry::value, &datum_test::entry::version,
                                            &datum_test::entry::key>;

     static_assert(std::same_as<reordered::record_type, datum_test::entry>);
     static_assert(std::same_as<reordered,
                               datum::binary_schema_t<datum_test::entry, &datum_test::entry::value,
                                                      &datum_test::entry::version,
                                                      &datum_test::entry::key>>);

     static_assert(!datum::render::serializable<datum_test::entry>);
     const datum_test::entry source {"object/42", 7, "contents"};
     datum::render::binary_buffer expected;
     zpp::bits::out output {expected};
     output(source.key, source.version, source.value).or_throw();
     const auto bytes = datum::render::to_bytes<wire>(source);
     CHECK(bytes == expected);
     CHECK(datum::render::from_bytes<wire>(bytes) == source);

     ::ceph::util::clear(expected);
     output.reset();
     output(source.value, source.version, source.key).or_throw();
     const auto custom = datum::render::to_bytes<reordered>(source);
     CHECK(custom == expected);
     CHECK(datum::render::from_bytes<reordered>(custom) == source);

     using scalar_wire = datum::binary_schema_t<datum_test::scalar_record>;

     const datum_test::scalar_record scalar {3, 7};
     ::ceph::util::clear(expected);
     output.reset();
     output(scalar.first, scalar.second).or_throw();
     CHECK(datum::render::to_bytes<scalar_wire>(scalar) == expected);
     CHECK(datum::render::to_bytes(scalar) != expected);

     using clock_wire = datum::binary_fields<&datum_test::clock_record::value>;

     const datum_test::clock_record clock {{-123}};
     const auto clock_bytes = datum::render::to_bytes<clock_wire>(clock);
     CHECK(clock_bytes == datum::render::to_bytes(clock));
     CHECK(datum::render::from_bytes<clock_wire>(clock_bytes).value.count == -123);
 }

 TEST_CASE("selected binary schemas omit local fields and preserve their defaults",
           "[datum][schema]")
 {
     using wire = datum::binary_fields<&datum_test::local_entry::value, &datum_test::local_entry::key>;

     unsigned calls = 0;
     const datum_test::local_entry source {"key", 7, "payload", [&calls] { ++calls; }};
     datum::render::binary_buffer expected;
     zpp::bits::out output {expected};
     output(source.value, source.key).or_throw();
     const auto bytes = datum::render::to_bytes<wire>(source);
     CHECK(bytes == expected);
     const auto candidate = datum::render::from_bytes<wire>(bytes);
     CHECK(candidate.key == source.key);
     CHECK(candidate.value == source.value);
     CHECK(candidate.version == 99);
     CHECK_FALSE(candidate.on_change);
     CHECK(calls == 0);

     datum_test::local_entry target {"old", 42, "old", [&calls] { ++calls; }};
     datum::render::from_bytes<wire>(bytes, target);
     CHECK(target.key == source.key);
     CHECK(target.value == source.value);
     CHECK(target.version == 42);
     target.on_change();
     CHECK(calls == 1);
 }

 TEST_CASE("direct binary schemas share storage adapters and decode limits", "[datum][schema]")
 {
     using wire = datum::binary_fields<&datum_test::entry::key, &datum_test::entry::version,
                                      &datum_test::entry::value>;

     const datum_test::entry source {"key", 7, std::string(104, 'x')};
     const auto expected = datum::render::to_bytes<wire>(source);
     std::array<std::byte, 512> storage {};
     const auto written = datum::render::to_bytes<wire>(source, std::span {storage});
     CHECK(std::ranges::equal(written, expected, {},
                              [](std::byte value) { return std::to_integer<std::uint8_t>(value); }));
     CHECK(datum::render::from_bytes<wire>(written) == source);
     datum::render::binary_buffer reusable {1, 2, 3};
     datum::render::to_bytes<wire>(source, reusable);
     CHECK(reusable == expected);

     std::array<std::uint8_t, 512> staging {};
     std::array<std::uint8_t, 512> scattered {};
     const std::array fragments {std::span {scattered}.first(3),
                                 std::span {scattered}.subspan(3, std::size(expected) - 3)};
     CHECK(datum::render::to_bytes<wire>(source, fragments, std::span {staging}) ==
           std::size(expected));
     CHECK(datum::render::from_bytes<wire>(fragments, std::span {staging}) == source);
     CHECK((datum::render::decode_bounded<wire, 128>(expected, 512) == source));
     CHECK_THROWS_AS((datum::render::decode_bounded<wire, 16>(expected, 512)), std::runtime_error);
     CHECK_THROWS_AS((datum::render::decode_bounded<wire, 128>(expected, 1)), std::length_error);
     CHECK_THROWS_AS(datum::render::to_bytes<wire>(source, std::span {staging}.first(8)),
                     std::runtime_error);
     auto damaged = expected;
     ::ceph::util::push_back(damaged, 0);
     CHECK_THROWS_AS(datum::render::from_bytes<wire>(damaged), std::runtime_error);
     damaged = expected;
     damaged.pop_back();
     CHECK_THROWS_AS(datum::render::from_bytes<wire>(damaged), std::runtime_error);
 }

 static_assert([] {
     using wire = datum::binary_fields<&datum_test::entry::key, &datum_test::entry::version,
                                      &datum_test::entry::value>;

     datum_test::entry source {"constexpr", 7, "payload"};
     std::array<std::uint8_t, 64> storage {};
     const auto bytes = datum::render::to_bytes<wire>(source, std::span {storage});

     return datum::render::decode_bounded<wire, 64>(bytes, 64) == source;
 }());

 TEST_CASE("schema inference adds no construction or ownership requirements", "[datum][schema]")
 {
     struct existing_record final
     {
         int selected;
         int local = 17;

     public:
         existing_record() = delete;
         explicit existing_record(const int value) : selected(value)
         {}
         existing_record(const existing_record&) = delete;
         existing_record(existing_record&&) = delete;

     };
     using wire = datum::binary_fields<&existing_record::selected>;

     static_assert(datum::binary_schema_type<wire>);
     static_assert(!datum::owning_value<existing_record>);
     static_assert(!datum::render::candidate_value<existing_record>);
     const existing_record source {42};
     const auto bytes = datum::render::to_bytes<wire>(source);
     existing_record target {0};

     datum::render::from_bytes<wire>(bytes, target);
     CHECK(target.selected == 42);
     CHECK(target.local == 17);

     // Native wire profiles may deliberately repeat a field; schema use may not.
     using repeated = datum::binary_fields<&existing_record::selected, &existing_record::selected>;

     static_assert(!datum::binary_schema_type<repeated>);
     datum::render::binary_buffer observed;
     zpp::bits::out output {observed};
     datum::serialize_schema(output, source, repeated {}).or_throw();
     auto expected = bytes;
     ::ceph::util::append_range(expected, bytes);
     CHECK(observed == expected);
 }

 TEST_CASE("empty records retain explicit binary schema identity", "[datum][schema]")
 {
     struct empty final
     {};
     using wire = datum::binary_schema_t<empty>;

     static_assert(datum::binary_schema_type<wire>);
     const auto bytes = datum::render::to_bytes<wire>(empty {});
     CHECK(std::empty(bytes));
     CHECK_NOTHROW(datum::render::from_bytes<wire>(bytes));
 }

#endif

#if defined(DATUM_DATA_NEGATIVE)
 void invalid_data_boundary()
 {
 #if DATUM_DATA_NEGATIVE == 1
      datum::view_of<datum_test::record> view {datum_test::record {"temporary", 1}};
      (void)view;
 #elif DATUM_DATA_NEGATIVE == 2
      struct bits
      {
          unsigned field : 1;
      };
      bits source {};
      datum::view_of<bits> view {source};
      (void)view;
 #elif DATUM_DATA_NEGATIVE == 3
      const std::array<std::uint8_t, 64> storage {};
  #ifdef DATUM_HAVE_ZPP
       datum::render::to_bytes(datum_test::record {}, std::span {storage});
  #endif
 #elif DATUM_DATA_NEGATIVE == 4
      std::array<signed char, 64> storage {};
  #ifdef DATUM_HAVE_ZPP
       datum::render::to_bytes(datum_test::record {}, std::span {storage});
  #endif
 #elif DATUM_DATA_NEGATIVE == 5
      struct base
      {};
      class derived : private base
      {

      public:
          int value;
      };
      derived source {};
      datum::view_of<derived> view {source};
      (void)view;
 #elif DATUM_DATA_NEGATIVE == 6
      using schema =
          datum::binary_schema_t<datum_test::entry, &datum_test::entry::key, &datum_test::entry::key>;
      (void)sizeof(schema);
 #elif DATUM_DATA_NEGATIVE == 7
      using schema = datum::binary_schema_t<datum_test::entry, &datum_test::scalar_record::first>;

      (void)sizeof(schema);
 #elif DATUM_DATA_NEGATIVE == 8
      struct borrowed
      {
          int& value;
      };
      using schema = datum::binary_schema_t<borrowed>;

      (void)sizeof(schema);
 #elif DATUM_DATA_NEGATIVE == 9
      using schema = datum::binary_schema_t<datum_test::entry>;

  #ifdef DATUM_HAVE_ZPP
       datum::render::to_bytes<schema>(datum_test::scalar_record {});
  #endif
 #elif DATUM_DATA_NEGATIVE == 10
      struct base
      {
          int inherited;
      };
      class derived : private base
      {

      public:
          int selected;
      };
      using schema = datum::binary_schema_t<derived>;

      (void)sizeof(schema);
 #endif
 }
#endif
