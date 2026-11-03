// Pure coverage algebra: independent canonical geometry, lifetime, limits, and work bounds:
#include "datum/interval/metric.h"
#include "datum/interval/range.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <ranges>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

namespace {

using domain = iv::comparison_domain<int>;
using query = iv::query<domain>;

// Five atoms include each endpoint, each gap, and both infinities. Maximal bit runs
// determine expected bounds without using the algebra operations under test:
auto expected(const unsigned mask)
{
    const std::array atoms {iv::before<domain>(0), query::singleton(0), query::open(0, 2),
                            query::singleton(2), iv::after<domain>(2)};
    std::vector<query> values;
    for (unsigned first = 0; first != 5;) {
        if (!(mask & (1U << first))) {
            ++first;
            continue;
        }

        auto last = first;
        while (last + 1 != 5 and (mask & (1U << (last + 1)))) {
            ++last;
        }
        values.push_back(query::between(atoms[first].lower(), atoms[last].upper()));
        first = last + 1;
    }

    return values;
}

auto prepared(const unsigned mask)
{
    return iv::canonical_range {expected(mask)};
}

struct bound_sink final
{
    std::vector<query> values;

    void emit_interval(const auto& lower, const auto& upper)
    {
        values.push_back(query::between(lower, upper));
    }
};

void check_paths(const auto& expression, const unsigned mask)
{
    const auto wanted = expected(mask);
    CHECK(std::ranges::equal(iv::compile_intervals(expression), wanted));
    CHECK(std::ranges::equal(iv::compile_intervals<5>(expression), wanted));
    CHECK(iv::interval_count(expression) == wanted.size());
    CHECK(iv::is_empty_expression(expression) == wanted.empty());

    const auto visible = iv::observe_intervals(expression);
    std::vector<query> pulled;
    for (const auto& value : visible) {
        pulled.push_back(iv::copy_interval(value));
    }
    CHECK(pulled == wanted);

    bound_sink sink;
    iv::for_each_interval(expression, sink);
    CHECK(sink.values == wanted);

    iv::workspace<domain> scratch;
    CHECK(std::ranges::equal(iv::evaluate(expression, scratch), wanted));

    const std::array witnesses {-1, 0, 1, 2, 3};
    for (unsigned atom = 0; atom != witnesses.size(); ++atom) {
        CHECK(iv::contains(expression, witnesses[atom]) == bool(mask & (1U << atom)));
    }
}

constexpr bool constant_algebra()
{
    const auto expression = iv::byte_query::closed_open(0, 8) ^ iv::byte_query::closed_open(5, 12);
    const auto selected = iv::compile_intervals<2>(expression);
    const auto limited = iv::take<2>(expression, 7, iv::bytes);
    const auto visible = iv::observe_intervals(expression);
    auto position = visible.begin();
    const auto first = iv::copy_interval(*position++);
    const auto second = iv::copy_interval(*position++);

    return selected.size() == 2 and iv::measure(selected, iv::bytes) == 9 and
           iv::measure(limited, iv::bytes) == 7 and
           first == iv::byte_query::closed_open(0, 5) and
           second == iv::byte_query::closed_open(8, 12) and position == visible.end() and
           iv::is_empty_expression(expression ^ expression);
}

static_assert(constant_algebra());
static_assert(std::same_as<decltype(query::closed(0, 4) & query::closed(2, 6)), query>);
static_assert(iv::is_empty(query::empty() & query::universal()));
static_assert(iv::is_empty(query::universal() & query::empty()));
static_assert((query::open(1, 2) & query::universal()) == query::open(1, 2));

struct other_domain : iv::comparison_domain<int> {};

namespace associated_provider {

struct domain : iv::comparison_domain<int> {};
using query = iv::query<domain>;
void difference(query&&, query&&) = delete;

} // associated_provider

constexpr auto complemented = iv::complement(associated_provider::query::closed(0, 2));
static_assert(iv::contains(complemented, 3));
static_assert(!iv::contains(~associated_provider::query::closed(0, 2), 1));

template <typename LeftT, typename RightT>
concept has_xor = requires (LeftT left, RightT right) { left ^ right; };

static_assert(!has_xor<query, iv::query<other_domain>>);

struct point final
{
    const int value;
    static inline std::size_t copies = 0;
    static inline int copy_budget = -1;

    explicit point(const int input) : value(input)
    {}

    point(const point& source) : value(source.value)
    {
        if (0 == copy_budget) {
            throw std::runtime_error {"point copy budget exhausted"};
        }
        if (copy_budget > 0) {
            --copy_budget;
        }
        ++copies;
    }
};

struct point_domain final
{
    using value_type = point;
    static inline std::size_t comparisons = 0;
    static inline bool reject = false;

    static auto compare(const point& left, const point& right)
    {
        if (reject) {
            throw std::runtime_error {"unexpected geometry observation"};
        }
        ++comparisons;

        return left.value <=> right.value;
    }
};

using point_query = iv::query<point_domain>;

struct owning_accessors final
{
    using domain_type = iv::comparison_domain<std::string>;

    std::string first;
    std::string last;

    auto lower() const { return iv::boundary<domain_type>::closed(first); }
    auto upper() const { return iv::boundary<domain_type>::open(last); }
};

template <typename T>
concept can_observe = requires (T&& value) { iv::observe_intervals(std::forward<T>(value)); };

static_assert(iv::interval_view<owning_accessors>);
static_assert(!iv::stable_interval<owning_accessors>);
static_assert(!can_observe<decltype(iv::symmetric_difference(owning_accessors {},
                                                          owning_accessors {}))>);

template <unsigned Depth, typename CoverageT>
auto nested_xor(const CoverageT& left, const CoverageT& right)
{
    if constexpr (0 == Depth) {
        return left - point_query::empty();
    }

    if constexpr (0 != Depth) {
        return nested_xor<Depth - 1>(left, right) ^ right;
    }
}

template <unsigned Depth, typename CoverageT>
void check_work(const CoverageT& left, const CoverageT& right)
{
    const auto expression = nested_xor<Depth>(left, right);
    point::copies = point_domain::comparisons = 0;
    const auto visible = iv::observe_intervals(expression);
    std::size_t count = 0;
    for (const auto& value : visible) {
        CHECK(value.lower().key().value < value.upper().key().value);
        ++count;
    }
    CHECK(count != 0);
    CHECK(point::copies == 0);
    CHECK(point_domain::comparisons < 64 * (Depth + 1) * (left.size() + right.size()));
}

} // anonymous namespace

TEST_CASE("symmetric difference agrees across canonical consumers and all five-atom sets")
{
    for (unsigned left = 0; left != 32; ++left) {
        const auto lhs = prepared(left);
        for (unsigned right = 0; right != 32; ++right) {
            const auto rhs = prepared(right);
            CAPTURE(left, right);
            check_paths(lhs ^ rhs, left ^ right);
            CHECK(std::ranges::equal(iv::compile_intervals(lhs | rhs), expected(left | right)));
            CHECK(std::ranges::equal(iv::compile_intervals(lhs & rhs), expected(left & right)));
            CHECK(std::ranges::equal(iv::compile_intervals(lhs - rhs), expected(left & ~right)));
            CHECK(std::ranges::equal(iv::compile_intervals(~lhs), expected(31 ^ left)));
        }
    }
}

TEST_CASE("five-atom XOR obeys cancellation associativity and intersection distribution")
{
    std::vector<decltype(prepared(0))> sets;
    for (unsigned mask = 0; mask != 32; ++mask) {
        sets.push_back(prepared(mask));
    }
    for (unsigned a = 0; a != 32; ++a) {
        const auto& x = sets[a];
        CHECK(iv::is_empty_expression(x ^ x));
        for (unsigned b = 0; b != 32; ++b) {
            const auto& y = sets[b];
            CHECK(std::ranges::equal(iv::compile_intervals((x ^ y) ^ y), expected(a)));
            for (unsigned c = 0; c != 32; ++c) {
                const auto& z = sets[c];
                CHECK(std::ranges::equal(iv::compile_intervals((x ^ y) ^ z), expected(a ^ b ^ c)));
                CHECK(std::ranges::equal(iv::compile_intervals(x ^ (y ^ z)), expected(a ^ b ^ c)));
                CHECK(std::ranges::equal(iv::compile_intervals((x & y) ^ (x & z)), expected(a & (b ^ c))));
            }
        }
    }
}

TEST_CASE("boundary parity retains singleton holes and order-only discrete gaps")
{
    check_paths(query::closed(0, 2) ^ query::open(0, 2), 0b01010);
    check_paths(query::universal() ^ query::singleton(0), 0b11101);
    const auto gap = query::open(1, 2);
    const auto result = iv::compile_intervals(gap ^ query::empty());
    REQUIRE(result.size() == 1);
    CHECK(result[0] == gap);
    CHECK_FALSE(iv::contains(result, 1));
    CHECK_FALSE(iv::contains(result, 2));
    CHECK(constant_algebra());
}

TEST_CASE("XOR owns temporary strings and cursor snapshots retain leaf endpoints")
{
    using string_query = iv::query<iv::comparison_domain<std::string>>;
    auto visible = iv::observe_intervals(
        string_query::closed_open(std::string(80, 'a'), std::string(80, 'm')) ^
        string_query::closed_open(std::string(80, 'g'), std::string(80, 'z')));
    auto current = visible.begin();
    const auto snapshot = *current++;
    CHECK(iv::copy_interval(snapshot) ==
          string_query::closed_open(std::string(80, 'a'), std::string(80, 'g')));
    CHECK(iv::copy_interval(*current) ==
          string_query::closed_open(std::string(80, 'm'), std::string(80, 'z')));
    ++current;
    CHECK(current == visible.end());
}

TEST_CASE("foreign owning accessor values survive scoped overlap and XOR preparation")
{
    using string_query = iv::query<owning_accessors::domain_type>;
    const auto retained = [] {
        const owning_accessors left {std::string(80, 'a'), std::string(80, 'm')};
        const owning_accessors right {std::string(80, 'g'), std::string(80, 'z')};
        CHECK(iv::intersection(left, right) ==
              string_query::closed_open(std::string(80, 'g'), std::string(80, 'm')));
        const auto expression = iv::symmetric_difference(left, right);
        iv::workspace<owning_accessors::domain_type> scratch;
        const auto evaluated = iv::evaluate(expression, scratch);
        const auto compiled = iv::compile_intervals(expression);
        CHECK(std::ranges::equal(evaluated, compiled));

        return compiled;
    }();
    REQUIRE(retained.size() == 2);
    CHECK(retained[0] == string_query::closed_open(std::string(80, 'a'), std::string(80, 'g')));
    CHECK(retained[1] == string_query::closed_open(std::string(80, 'm'), std::string(80, 'z')));
}

TEST_CASE("empty scalar overlap borrows points and retained overlap propagates copy failure")
{
    const auto left = point_query::closed_open(point {0}, point {10});
    const auto touching = point_query::closed_open(point {10}, point {20});
    const auto overlapping = point_query::closed_open(point {5}, point {20});
    struct restore final
    {
        ~restore() { point::copy_budget = -1; }
    } reset;
    point::copies = 0;
    point::copy_budget = 0;
    CHECK(iv::is_empty(iv::intersection(left, touching)));
    CHECK(iv::is_empty(iv::intersection(touching, left)));
    CHECK(point::copies == 0);
    CHECK_THROWS_AS(iv::intersection(left, overlapping), std::runtime_error);
    point::copy_budget = -1;
    const auto retained = iv::intersection(left, overlapping);
    CHECK(retained.lower().key().value == 5);
    CHECK(retained.upper().key().value == 10);
    CHECK(point::copies == 2);
}

TEST_CASE("immutable points need no assignment and nested XOR has bounded observation work")
{
    const auto left = iv::compile_intervals(point_query::closed_open(point {0}, point {100}));
    const auto right = iv::compile_intervals(point_query::closed_open(point {20}, point {80}));
    check_work<1>(left, right);
    check_work<2>(left, right);
    check_work<4>(left, right);
    check_work<8>(left, right);
    check_work<16>(left, right);

    const auto bounded = iv::compile_intervals<2>(left ^ right);
    CHECK(bounded.size() == 2);
    point::copies = point_domain::comparisons = 0;
    CHECK(iv::interval_count(left ^ left) == 0);
    CHECK(point::copies == 0);
    CHECK(point_domain::comparisons == 0);
}

TEST_CASE("XOR workspace and output limits publish no partial result")
{
    const auto expression = query::closed(0, 2) ^ query::open(0, 2);
    iv::workspace<domain> scratch {{.intermediate_intervals = 1, .work = 100}};
    CHECK_THROWS_AS(iv::evaluate(expression, scratch), std::length_error);
    CHECK(scratch.retained_intervals() == 0);
    CHECK_THROWS_AS(iv::compile_intervals<1>(expression), std::length_error);

    iv::workspace<domain> exhausted {{.intermediate_intervals = 10, .work = 0}};
    CHECK_THROWS_AS(iv::evaluate(expression, exhausted), std::length_error);
    CHECK(exhausted.retained_intervals() == 0);
}

TEST_CASE("XOR stopping and handled errors perform no later geometry observation")
{
    const auto left = iv::compile_intervals(point_query::closed_open(point {0}, point {100}));
    const auto right = iv::compile_intervals(point_query::closed_open(point {20}, point {80}));
    const auto visible = iv::observe_intervals(left ^ right);
    struct restore final
    {
        ~restore() { point_domain::reject = false; }
    } reset;

    const auto stopped = datum::visit_range(visible, [](const auto&) {
        point_domain::reject = true;

        return datum::acceptance::accept_and_stop;
    });
    CHECK(stopped.accepted == 1);
    point_domain::reject = false;
    const auto failed = datum::visit_range(visible, [](const auto&)
        -> std::expected<datum::acceptance, int> {
        point_domain::reject = true;

        return std::unexpected {7};
    });
    REQUIRE_FALSE(failed);
    CHECK(failed.error().error == 7);
}
