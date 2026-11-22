// Public retained-coverage calls: inferred storage, explicit policy, and preserved contracts:
#include <datum/interval/clip.h>
#include <datum/interval/range.h>
#include <datum/plan/query.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <ranges>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;
namespace qp = datum::plan;

namespace {

using query = iv::byte_query;
constexpr auto previous = query::closed_open(0, 8);
constexpr auto desired = query::closed_open(5, 12);
constexpr auto changed = iv::compile_intervals(previous ^ desired);
constexpr auto cancelled = iv::compile_intervals(previous ^ previous);
static_assert(changed.size() == 2 and iv::measure(changed, iv::bytes) == 9);
static_assert(cancelled.empty());
constexpr auto outside = iv::compile_intervals(~previous);
static_assert(outside.size() == 2 and !iv::contains(outside, std::uint64_t {3}));
static_assert(std::same_as<decltype(changed), decltype(cancelled)>);
static_assert(std::same_as<decltype(changed), decltype(iv::compile_intervals<2>(previous ^ desired)) const>);
static_assert(std::ranges::contiguous_range<decltype(changed)>);
static_assert(datum::owning_value<decltype(changed)>);
static_assert(!std::ranges::borrowed_range<decltype(changed)>);

constexpr std::array unordered {query::closed_open(8, 12), query::closed_open(0, 5),
                                query::closed_open(3, 8)};
constexpr auto normalized = iv::compile_intervals(unordered);
static_assert(normalized.size() == 1 and normalized[0] == query::closed_open(0, 12));
constexpr auto normalized_limited = iv::compile_intervals<1>(unordered);
static_assert(normalized_limited.size() == 1);
constexpr std::array<query, 0> no_intervals {};
constexpr auto none = iv::compile_intervals(no_intervals);
static_assert(none.empty());

constexpr bool constant_sources()
{
    const query raw[] {query::closed_open(8, 12), query::closed_open(0, 5)};
    const auto from_array = iv::compile_intervals(raw);
    const auto from_span = iv::compile_intervals(std::span {raw});
    const auto transformed = raw | std::views::transform([](const query& value) -> const query& {
        return value;
    });
    const auto from_view = iv::compile_intervals(transformed);
    const iv::intervals_view projected {raw, std::identity {}};
    const auto from_projection = iv::compile_intervals(projected);
    const iv::static_interval_set<iv::byte_domain, 2> fixed {raw};
    const auto from_fixed = iv::compile_intervals(fixed);
    const iv::canonical_range admitted {std::array {previous}};
    const auto from_owned = iv::compile_intervals(admitted ^ desired);
    const auto visible = iv::observe_intervals(previous ^ desired);
    const auto from_observation = iv::compile_intervals(visible);
    auto common = iv::observe_intervals(previous ^ desired) | std::views::common;
    const auto from_common = iv::compile_intervals(common);
    static_assert(std::same_as<decltype(from_common), decltype(changed)>);

    return std::ranges::equal(from_array, from_span) and
           std::ranges::equal(from_span, from_view) and
           std::ranges::equal(from_view, from_projection) and
           std::ranges::equal(from_projection, from_fixed) and
           std::ranges::equal(from_owned, changed) and
           std::ranges::equal(from_observation, changed) and
           std::ranges::equal(from_common, changed);
}

static_assert(constant_sources());
constexpr bool sliced_sources()
{
    const auto source = unordered | std::views::take(2) | std::views::drop(1);
    const auto selected = iv::compile_intervals(source);

    return selected.size() == 1 and selected[0] == query::closed_open(0, 5);
}

static_assert(sliced_sources());
constexpr auto batch = iv::take(previous ^ desired, std::uint64_t {7}, iv::bytes);
static_assert(batch.size() == 2 and iv::measure(batch, iv::bytes) == 7);
static_assert(batch[1] == query::closed_open(8, 10));
constexpr auto description = qp::make_query(previous ^ desired);
constexpr auto described = qp::compile_intervals(description);
static_assert(std::ranges::equal(described, changed));
constexpr auto described_bounded = qp::compile_intervals<2>(description);
static_assert(std::ranges::equal(described_bounded, changed));

constexpr std::array requests {query::closed_open(0, 10), query::closed_open(20, 30),
                               query::closed_open(40, 50)};
constexpr std::array cached {query::closed_open(2, 5), query::closed_open(23, 25),
                             query::closed_open(43, 45)};
constexpr iv::canonical_range request_coverage {requests};
constexpr iv::canonical_range cached_coverage {cached};
constexpr auto read_batch = iv::take(request_coverage - cached_coverage, std::uint64_t {20}, iv::bytes);
static_assert(read_batch.size() == 6 and iv::measure(read_batch, iv::bytes) == 20);

struct requirement final {};

struct signed_metric final
{
    using quantity_type = std::int64_t;

    constexpr quantity_type zero() const { return 0; }
    constexpr quantity_type distance(const std::uint64_t first, const std::uint64_t last) const
    {
        return static_cast<quantity_type>(last - first);
    }

    constexpr quantity_type subtract(const quantity_type left, const quantity_type right) const
    {
        return left - right;
    }

    constexpr std::uint64_t advance(const std::uint64_t first, const quantity_type amount) const
    {
        return first + static_cast<std::uint64_t>(amount);
    }
};

template <typename T>
concept can_compile_description = requires (const T& value) {
    qp::compile_intervals(value);
    qp::compile_intervals(value, iv::dynamic_storage);
};

static_assert(can_compile_description<decltype(description)>);
static_assert(!can_compile_description<decltype(qp::with_requirement(description, requirement {}))>);

using dynamic_result = decltype(iv::compile_intervals(previous, iv::dynamic_storage));
static_assert(std::same_as<dynamic_result, decltype(iv::compile_intervals(previous ^ desired,
                                                                        iv::dynamic_storage))>);
static_assert(std::same_as<dynamic_result, decltype(iv::compile_intervals(unordered,
                                                                        iv::dynamic_storage))>);
static_assert(std::same_as<dynamic_result, decltype(qp::compile_intervals(description,
                                                                        iv::dynamic_storage))>);
static_assert(std::same_as<dynamic_result, decltype(iv::take(previous ^ desired, std::uint64_t {7},
                                                           iv::bytes, iv::dynamic_storage))>);

using dynamic_span = std::span<const query>;
using large_span = std::span<const query, 4096>;
using huge_span = std::span<const query, std::numeric_limits<std::size_t>::max() - 1>;
using huge_coverage = iv::canonical_range<huge_span>;
using overflowing_expression = decltype(std::declval<huge_coverage&>() ^
                                        std::declval<huge_coverage&>());
static_assert(std::same_as<dynamic_result,
                          decltype(iv::compile_intervals(std::declval<dynamic_span>()))>);
static_assert(std::same_as<dynamic_result,
                          decltype(iv::compile_intervals(std::declval<large_span>()))>);
static_assert(std::same_as<dynamic_result,
                          decltype(iv::compile_intervals(std::declval<overflowing_expression>()))>);

struct observed_interval final
{
    using domain_type = iv::byte_domain;

    unsigned& observations;

    auto lower() const
    {
        ++observations;

        return iv::boundary<domain_type>::closed(0);
    }

    auto upper() const
    {
        ++observations;

        return iv::boundary<domain_type>::open(8);
    }
};

struct observed_source final : iv::detail::canonical_tag
{
    using domain_type = iv::byte_domain;

    std::span<const query> values;
    unsigned& begins;

    auto begin() const
    {
        ++begins;

        return values.begin();
    }

    auto end() const { return values.end(); }
};

struct immutable_point final
{
    const int value;
    static inline unsigned copies = 0;

    explicit immutable_point(const int input) : value(input) {}
    immutable_point(const immutable_point& input) : value(input.value) { ++copies; }
};

struct immutable_domain final
{
    using value_type = immutable_point;

    static auto compare(const value_type& left, const value_type& right)
    {
        return left.value <=> right.value;
    }
};

struct fragile_point final
{
    static inline int live = 0;
    static inline int copy_budget = -1;
    int value;

    explicit fragile_point(const int input) : value(input) { ++live; }

    fragile_point(const fragile_point& source) : value(source.value)
    {
        if (copy_budget == 0) {
            throw std::runtime_error {"point copy refused"};
        }
        if (copy_budget > 0) {
            --copy_budget;
        }
        ++live;
    }

    fragile_point(fragile_point&& source) noexcept : value(source.value) { ++live; }
    ~fragile_point() { --live; }
    fragile_point& operator=(const fragile_point&) = default;
    fragile_point& operator=(fragile_point&&) = default;
    auto operator<=>(const fragile_point&) const = default;
};

} // anonymous namespace

TEST_CASE("ordinary compilation infers storage without changing geometry or value-based types")
{
    CHECK(std::ranges::equal(changed, iv::compile_intervals(previous ^ desired, iv::dynamic_storage)));
    CHECK(std::ranges::equal(normalized, iv::compile_intervals(unordered, iv::dynamic_storage)));
    CHECK(constant_sources());
    CHECK((iv::compile_intervals(previous | desired)[0] == query::closed_open(0, 12)));
    CHECK((iv::compile_intervals(previous & desired)[0] == query::closed_open(5, 8)));
    CHECK((iv::compile_intervals(previous - desired)[0] == query::closed_open(0, 5)));
    CHECK(iv::compile_intervals<0>(previous ^ previous).empty());
    CHECK_THROWS_AS(iv::compile_intervals<1>(previous ^ desired), std::length_error);
    CHECK_THROWS_AS(iv::compile_intervals<0>(unordered), std::length_error);
}

TEST_CASE("dynamic policy supplies one owning type for mixed collections and query wrappers")
{
    const std::array selections {
        iv::compile_intervals(previous, iv::dynamic_storage),
        iv::compile_intervals(previous ^ desired, iv::dynamic_storage),
        qp::compile_intervals(description, iv::dynamic_storage)
    };
    CHECK(selections[0].size() == 1);
    CHECK(selections[1].size() == 2);
    CHECK(std::ranges::equal(selections[1], selections[2]));
    iv::workspace<iv::byte_domain> scratch;
    const auto borrowed = iv::evaluate(previous ^ desired, scratch);
    STATIC_REQUIRE(std::same_as<decltype(borrowed),
                               const iv::canonical_range<std::span<const query>>>);
    CHECK(std::ranges::equal(borrowed, changed));

    unsigned residual_calls = 0;
    const auto configured = qp::with_residual(qp::make_query(previous ^ desired),
        [owner = std::make_unique<int>(7), &residual_calls] {
            ++residual_calls;

            return *owner;
        });
    CHECK(std::ranges::equal(qp::compile_intervals(configured), changed));
    CHECK(std::ranges::equal(qp::compile_intervals(configured, iv::dynamic_storage), changed));
    CHECK(residual_calls == 0);
}

TEST_CASE("raw admission supports input-only ranges and nonexpanding standard views")
{
    std::istringstream input {"8 0 3"};
    auto source = std::views::istream<unsigned>(input) | std::views::transform([](const unsigned first) {
        return query::closed_open(first, first + 4);
    });
    const auto streamed = iv::compile_intervals(source);
    REQUIRE(streamed.size() == 2);
    CHECK(streamed[0] == query::closed_open(0, 7));
    CHECK(streamed[1] == query::closed_open(8, 12));
    auto filtered = unordered | std::views::filter([](const auto& value) {
        return value.lower().key() != 0;
    });
    const auto selected = iv::compile_intervals(filtered | std::views::reverse);
    REQUIRE(selected.size() == 1);
    CHECK(selected[0] == query::closed_open(3, 12));
    constexpr auto empty_view = iv::compile_intervals(std::ranges::empty_view<query> {});
    constexpr auto single_view = iv::compile_intervals(std::views::single(previous));
    static_assert(empty_view.empty());
    static_assert(single_view.size() == 1 and single_view[0] == previous);

    // A base() member alone proves no bound: join expands beyond its outer array's extent:
    const std::array groups {std::array {query::closed_open(0, 1), query::closed_open(2, 3)},
                             std::array {query::closed_open(4, 5), query::closed_open(6, 7)}};
    const auto flattened = iv::compile_intervals(groups | std::views::join);
    STATIC_REQUIRE(std::same_as<decltype(flattened), const dynamic_result>);
    CHECK(flattened.size() == 4);
}

TEST_CASE("storage selection does not observe providers or add point assignment requirements")
{
    unsigned observations = 0;
    const observed_interval source {observations};
    const auto selected = iv::compile_intervals(source);
    CHECK(observations == 2);
    REQUIRE(selected.size() == 1);
    CHECK(selected[0] == previous);

    using immutable_query = iv::query<immutable_domain>;
    const auto value = immutable_query::closed(immutable_point {1}, immutable_point {3});
    immutable_point::copies = 0;
    auto retained = iv::compile_intervals(value);
    CHECK(immutable_point::copies == 2);
    STATIC_REQUIRE(std::ranges::viewable_range<decltype(retained)&&>);
    auto values = std::move(retained) | std::views::transform([](const auto& interval) {
        return interval.lower().key().value;
    });
    CHECK(*values.begin() == 1);
}

TEST_CASE("quota policies preserve zero observation and output limits")
{
    const std::array entries {previous};
    unsigned begins = 0;
    const observed_source source {{}, entries, begins};
    CHECK(iv::take(source, std::uint64_t {0}, iv::bytes).empty());
    CHECK(iv::take(source, std::uint64_t {0}, iv::bytes, iv::dynamic_storage).empty());
    CHECK(iv::take<0>(source, std::uint64_t {0}, iv::bytes).empty());
    CHECK(begins == 0);
    CHECK(std::ranges::equal(batch, iv::take(previous ^ desired, std::uint64_t {7}, iv::bytes,
                                            iv::dynamic_storage)));
    CHECK_THROWS_AS(iv::take<1>(previous ^ desired, std::uint64_t {7}, iv::bytes), std::length_error);
    CHECK_THROWS_AS(iv::take(previous, -1, signed_metric {}), std::domain_error);
}

TEST_CASE("failed retention destroys the constructed prefix and preserves the source")
{
    using fragile_query = iv::query<iv::comparison_domain<fragile_point>>;
    const auto source = fragile_query::closed(fragile_point {1}, fragile_point {3});
    const auto live_before = fragile_point::live;
    fragile_point::copy_budget = 1;
    CHECK_THROWS_AS(iv::compile_intervals(source), std::runtime_error);
    CHECK(fragile_point::live == live_before);
    fragile_point::copy_budget = 1;
    CHECK_THROWS_AS(iv::compile_intervals(source, iv::dynamic_storage), std::runtime_error);
    CHECK(fragile_point::live == live_before);
    fragile_point::copy_budget = -1;
    CHECK(iv::compile_intervals(source).size() == 1);
    CHECK(source.lower().key().value == 1);
    CHECK(source.upper().key().value == 3);
    CHECK(fragile_point::live == live_before);
}

TEST_CASE("inferred results retain temporary point owners and support clipping")
{
    using text_query = iv::query<iv::comparison_domain<std::string>>;
    const auto retained = iv::compile_intervals(
        text_query::closed_open(std::string(80, 'a'), std::string(80, 'm')) ^
        text_query::closed_open(std::string(80, 'g'), std::string(80, 'z')));
    REQUIRE(retained.size() == 2);
    CHECK(retained[0] == text_query::closed_open(std::string(80, 'a'), std::string(80, 'g')));
    CHECK(retained[1] == text_query::closed_open(std::string(80, 'm'), std::string(80, 'z')));
    const std::array rows {query::closed_open(1, 4), query::closed_open(7, 11)};
    const iv::clip_view cuts {rows, changed, std::identity {}};
    const auto clipped = iv::compile_intervals(cuts);
    REQUIRE(clipped.size() == 2);
    CHECK(clipped[0] == rows[0]);
    CHECK(clipped[1] == query::closed_open(8, 11));
}
