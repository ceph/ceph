// Public algebra observation: endpoint lifetimes, standard pipelines, and controlled effects:
#include "datum/interval/range.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <expected>
#include <memory>
#include <ranges>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;
using domain = iv::comparison_domain<unsigned>;
using query = iv::query<domain>;

namespace {

auto coverage(const unsigned mask)
{
    std::vector<query> values;
    for (unsigned point = 0; point != 6; ++point) {
        if (mask & (1U << point)) {
            values.push_back(query::closed_open(point, point + 1));
        }
    }

    return iv::compile_intervals(values);
}

void check_mask(auto&& expression, const unsigned expected)
{
    const auto visible = iv::observe_intervals(std::forward<decltype(expression)>(expression));
    unsigned actual = 0;
    for (const auto& value : visible) {
        for (unsigned point = 0; point != 6; ++point) {
            if (iv::contains(value, point)) {
                CHECK_FALSE(actual & (1U << point));
                actual |= 1U << point;
            }
        }
    }
    CHECK(expected == actual);
}

constexpr bool constant_observation()
{
    auto visible = iv::observe_intervals(
        iv::difference(query::closed_open(0, 8), query::closed_open(2, 6)));
    auto current = visible.begin();
    const auto first = iv::copy_interval(*current++);
    if (first != query::closed_open(0, 2) || iv::copy_interval(*current) != query::closed_open(6, 8)) {
        return false;
    }
    ++current;

    return current == visible.end();
}

static_assert(constant_observation());

template <typename T>
concept can_observe = requires (T&& value) { iv::observe_intervals(std::forward<T>(value)); };

struct foreign_interval final
{
    using domain_type = domain;

public:
    auto lower() const { return iv::boundary<domain>::closed(1); }
    auto upper() const { return iv::boundary<domain>::open(3); }

};

static_assert(!can_observe<foreign_interval>);
static_assert(can_observe<query>);

} // anonymous namespace

TEST_CASE("public observation agrees with an independent finite-set model")
{
    for (unsigned left = 0; left != 64; ++left) {
        const auto lhs = coverage(left);
        for (unsigned right = 0; right != 64; ++right) {
            const auto rhs = coverage(right);
            check_mask(iv::set_union(lhs, rhs), left | right);
            check_mask(iv::intersection(lhs, rhs), left & right);
            check_mask(iv::difference(lhs, rhs), left & ~right);
            check_mask(iv::difference(iv::set_union(lhs, rhs), iv::intersection(lhs, rhs)),
                       left ^ right);
        }
    }
    CHECK(constant_observation());
}

TEST_CASE("observation owns temporary expressions and their temporary vector leaves")
{
    auto visible = iv::observe_intervals(iv::difference(coverage(0b111111), coverage(0b001100)));
    STATIC_REQUIRE(std::ranges::input_range<const decltype(visible)>);
    STATIC_REQUIRE_FALSE(std::ranges::forward_range<const decltype(visible)>);
    STATIC_REQUIRE_FALSE(std::ranges::sized_range<const decltype(visible)>);
    STATIC_REQUIRE_FALSE(std::ranges::view<decltype(visible)>);
    STATIC_REQUIRE_FALSE(std::ranges::borrowed_range<decltype(visible)>);
    STATIC_REQUIRE(std::movable<decltype(visible)>);
    STATIC_REQUIRE_FALSE(std::copy_constructible<decltype(visible)>);

    std::vector<query> selected;
    for (const auto& value : visible) {
        selected.push_back(iv::copy_interval(value));
    }
    CHECK(selected == std::vector {query::closed_open(0, 2), query::closed_open(4, 6)});

    auto pipeline = iv::observe_intervals(iv::difference(coverage(0b111111), coverage(0b001100)))
        | std::views::filter([](const auto& value) { return 4 == value.lower().key(); })
        | std::views::transform([](const auto& value) { return iv::copy_interval(value); });
    CHECK(std::ranges::equal(pipeline, std::array {query::closed_open(4, 6)}));
}

TEST_CASE("borrowed observations preserve leaf identity and permit nonassignable expression moves")
{
    const auto first = coverage(0b111111);
    const auto removed = coverage(0b001100);
    const auto expression = iv::difference(first, removed);
    STATIC_REQUIRE_FALSE(std::is_move_assignable_v<std::remove_cv_t<decltype(expression)>>);
    auto borrowed = iv::observe_intervals(expression);
    auto owned = iv::observe_intervals(iv::difference(first, removed));
    auto replacement = iv::observe_intervals(iv::difference(first, removed));
    owned = std::move(replacement);

    CHECK(std::addressof((*borrowed.begin()).lower().key()) ==
          std::addressof(first[0].lower().key()));
    CHECK(iv::copy_interval(*owned.begin()) == query::closed_open(0, 2));
    const auto leaf = iv::observe_intervals(first);
    STATIC_REQUIRE(std::ranges::forward_range<const decltype(leaf)>);
    CHECK(std::addressof(*leaf.begin()) == std::addressof(first[0]));
}

TEST_CASE("owned string geometry remains valid through cursor copies and postincrement")
{
    using strings = iv::query<iv::comparison_domain<std::string>>;
    const auto make = [] {
        return iv::observe_intervals(iv::difference(
            strings::closed_open(std::string(80, 'a'), std::string(80, 'z')),
            strings::closed_open(std::string(80, 'g'), std::string(80, 'm'))));
    };
    auto visible = make();
    auto current = visible.begin();
    auto independent = current;
    const auto saved = *current++;

    CHECK(iv::copy_interval(saved) == strings::closed_open(std::string(80, 'a'), std::string(80, 'g')));
    CHECK(iv::copy_interval(*independent) == iv::copy_interval(saved));
    CHECK(iv::copy_interval(*current) == strings::closed_open(std::string(80, 'm'), std::string(80, 'z')));
}

TEST_CASE("lvalue observation does not require movable or copyable expression storage")
{
    struct fixed_coverage final : iv::detail::canonical_tag
    {
        using domain_type = domain;

        std::array<query, 1> values {query::closed_open(1, 3)};

    public:
        fixed_coverage() = default;
        fixed_coverage(const fixed_coverage&) = delete;
        fixed_coverage(fixed_coverage&&) = delete;

    public:
        auto begin() const { return values.begin(); }
        auto end() const { return values.end(); }

    };

    STATIC_REQUIRE(iv::canonical_sequence<fixed_coverage>);
    STATIC_REQUIRE_FALSE(can_observe<fixed_coverage>);
    fixed_coverage source;
    const auto visible = iv::observe_intervals(source);

    CHECK(std::addressof(*visible.begin()) == std::addressof(source.values[0]));
}

TEST_CASE("observed algebra reuses controlled fallible visitation")
{
    const auto expression = iv::difference(query::closed_open(0, 8), query::closed_open(2, 6));
    auto visible = iv::observe_intervals(expression);
    unsigned calls = 0;
    const auto result = iv::visit_intervals(visible, [&](const query& value)
        -> std::expected<datum::acceptance, std::unique_ptr<int>> {
        ++calls;
        CHECK(value == query::closed_open(0, 2));

        return std::unexpected(std::make_unique<int>(17));
    });
    REQUIRE_FALSE(result);
    CHECK(1 == calls);
    CHECK(1 == result.error().progress.offered);
    CHECK(0 == result.error().progress.consumed());
    CHECK(17 == *result.error().error);

    const auto stopped = iv::visit_intervals(iv::observe_intervals(expression), [](const query&) {
        return datum::acceptance::accept_and_stop;
    });
    CHECK(1 == stopped.accepted);
    CHECK(stopped.stopped());
    CHECK_THROWS_AS(iv::visit_intervals(visible, [](const query&) -> datum::acceptance {
        throw std::runtime_error {"consumer failed"};
    }), std::runtime_error);

    const auto prepared = iv::compile_intervals(foreign_interval {});
    const auto observed = iv::observe_intervals(prepared);
    CHECK(iv::copy_interval(*observed.begin()) == query::closed_open(1, 3));
}

TEST_CASE("observation construction is lazy and stopping performs no further preparation")
{
    const std::array rows {query::closed_open(0, 2), query::closed_open(4, 6),
                           query::closed_open(8, 10)};
    unsigned reads = 0;
    bool stopped = false;
    auto projected = rows | std::views::transform([&](const query& value) -> const query& {
        if (stopped) {
            throw std::runtime_error {"observed geometry after consumer stopped"};
        }
        ++reads;

        return value;
    });
    const iv::canonical_range prepared {projected};
    const auto expression = iv::set_union(prepared, query::empty());
    reads = 0;
    auto visible = iv::observe_intervals(expression);
    CHECK(0 == reads);

    unsigned at_stop = 0;
    const auto result = iv::visit_intervals(visible, [&](const query&) {
        at_stop = reads;
        stopped = true;

        return datum::acceptance::accept_and_stop;
    });
    CHECK(1 == result.accepted);
    CHECK(reads > 0);
    CHECK(at_stop == reads);
}
