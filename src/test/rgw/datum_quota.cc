// Measured prefix selection: bounded output, direct algebra, and exact observation limits:
#include "datum/interval/metric.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;
using query = iv::byte_query;

namespace {

auto coverage(const unsigned mask)
{
    std::vector<query> values;
    for (unsigned point = 0; point != 8; ++point) {
        if (mask & (1U << point)) {
            values.push_back(query::closed_open(point, point + 1));
        }
    }

    return iv::compile_intervals(values);
}

unsigned first_bytes(const unsigned mask, unsigned quota)
{
    unsigned selected = 0;
    for (unsigned point = 0; point != 8; ++point) {
        if (0 == quota) {
            break;
        }
        if (mask & (1U << point)) {
            selected |= 1U << point;
            --quota;
        }
    }

    return selected;
}

unsigned selected_bytes(const auto& selected)
{
    unsigned mask = 0;
    for (unsigned point = 0; point != 8; ++point) {
        if (iv::contains(selected, point)) {
            mask |= 1U << point;
        }
    }

    return mask;
}

struct observations final
{
    unsigned begins = 0;
    unsigned reads = 0;
    unsigned increments = 0;
};

struct counted_coverage final : iv::detail::canonical_tag
{
    using domain_type = iv::byte_domain;

    std::span<const query> values;
    observations* counts;

    // Internal preparation is not an ADL customization point:
    friend void canonical_stream(const counted_coverage&) = delete;

    struct iterator final
    {
        using value_type = query;
        using difference_type = std::ptrdiff_t;
        using iterator_concept = std::forward_iterator_tag;

        const query* position {};
        observations* counts {};

    public:
        const query& operator*() const
        {
            ++counts->reads;

            return *position;
        }

        iterator& operator++()
        {
            ++counts->increments;
            ++position;

            return *this;
        }

        iterator operator++(int)
        {
            const auto previous = *this;
            ++*this;

            return previous;
        }

        bool operator==(const iterator&) const = default;

    };

public:
    iterator begin() const
    {
        ++counts->begins;

        return {values.data(), counts};
    }

    iterator end() const { return {values.data() + values.size(), counts}; }

};

static_assert(iv::canonical_sequence<counted_coverage>);

constexpr bool constant_quota()
{
    const auto selected = iv::take<2>(iv::difference(query::closed_open(0, 15),
                                                   query::closed_open(3, 10)),
                                      std::uint64_t {5}, iv::bytes);

    return selected.size() == 2 and selected[0] == query::closed_open(0, 3) and
           selected[1] == query::closed_open(10, 12) and iv::measure(selected, iv::bytes) == 5;
}

static_assert(constant_quota());

} // anonymous namespace

TEST_CASE("dynamic and bounded direct-expression quotas match a flat-byte oracle")
{
    for (unsigned mask = 0; mask != 256; ++mask) {
        const auto prepared = coverage(mask);
        const auto removed = coverage(255U ^ mask);
        for (unsigned quota = 0; quota != 11; ++quota) {
            const auto expected = first_bytes(mask, quota);
            const auto dynamic = iv::take(prepared, quota, iv::bytes);
            const auto bounded = iv::take<4>(iv::difference(query::closed_open(0, 8), removed),
                                              quota, iv::bytes);
            STATIC_REQUIRE(iv::canonical_sequence<decltype(bounded)>);

            CHECK(expected == selected_bytes(dynamic));
            CHECK(expected == selected_bytes(bounded));
            CHECK(std::ranges::equal(dynamic, bounded));
            CHECK(static_cast<unsigned>(std::popcount(expected)) == iv::measure(bounded, iv::bytes));
        }
    }
    CHECK(constant_quota());
}

TEST_CASE("zero exact and partial quotas stop without another source observation")
{
    const std::array rows {query::closed_open(0, 4), query::closed_open(10, 13),
                           query::closed_open(20, 22)};
    observations counts;
    const counted_coverage source {{}, rows, &counts};

    CHECK(iv::take(source, 0, iv::bytes).empty());
    CHECK(iv::take<0>(source, 0, iv::bytes).empty());
    CHECK(0 == counts.begins);
    CHECK(0 == counts.reads);
    CHECK(0 == counts.increments);

    const auto exact = iv::take<1>(source, 4, iv::bytes);
    CHECK(exact[0] == rows[0]);
    CHECK(1 == counts.begins);
    CHECK(1 == counts.reads);
    CHECK(0 == counts.increments);

    counts = {};
    const auto partial = iv::take(source, 2, iv::bytes);
    CHECK(partial[0] == query::closed_open(0, 2));
    CHECK(1 == counts.reads);
    CHECK(0 == counts.increments);

    counts = {};
    const auto two = iv::take<2>(source, 5, iv::bytes);
    CHECK(two[1] == query::closed_open(10, 11));
    CHECK(2 == counts.reads);
    CHECK(1 == counts.increments);
}

TEST_CASE("bounded quota refusal returns no prefix and leaves borrowed input unchanged")
{
    const std::array rows {query::closed_open(0, 4), query::closed_open(10, 13)};
    const auto original = rows;
    const iv::canonical_range source {rows};

    CHECK_THROWS_AS(iv::take<0>(source, 1, iv::bytes), std::length_error);
    CHECK_THROWS_AS(iv::take<1>(source, 5, iv::bytes), std::length_error);
    CHECK(rows == original);
    CHECK(2 == iv::take<2>(source, 100, iv::bytes).size());
    CHECK(7 == iv::measure(iv::take<2>(source, 100, iv::bytes), iv::bytes));
}

TEST_CASE("quota validation concerns only selected finite half-open geometry")
{
    const std::array rows {query::closed_open(0, 4), query::closed(10, 13)};
    const iv::canonical_range source {rows};
    CHECK(4 == iv::measure(iv::take<1>(source, 4, iv::bytes), iv::bytes));
    CHECK_THROWS_AS(iv::take<2>(source, 5, iv::bytes), std::domain_error);
    CHECK(iv::take<0>(query::universal(), 0, iv::bytes).empty());
    CHECK_THROWS_AS(iv::take<1>(query::universal(), 1, iv::bytes), std::domain_error);
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    const auto high = iv::take<1>(query::closed_open(maximum - 3, maximum), 2, iv::bytes);
    REQUIRE(1 == high.size());
    CHECK(high[0] == query::closed_open(maximum - 3, maximum - 1));
}

TEST_CASE("negative quotas are rejected before source traversal")
{
    struct signed_metric final
    {
        using quantity_type = int;

    public:
        int zero() const { return 0; }
        int distance(const std::uint64_t first, const std::uint64_t last) const
        { return static_cast<int>(last - first); }
        std::uint64_t advance(const std::uint64_t point, const int amount) const
        { return point + static_cast<std::uint64_t>(amount); }
        int subtract(const int left, const int right) const { return left - right; }

    };

    const std::array rows {query::closed_open(0, 4)};
    observations counts;
    const counted_coverage source {{}, rows, &counts};

    CHECK_THROWS_AS(iv::take<1>(source, -1, signed_metric {}), std::domain_error);
    CHECK(0 == counts.begins);
    CHECK(0 == counts.reads);
}
