// Datum core algebra, clipping and traversal tests.

#include "common/container_concepts.h"
#include <catch2/catch_test_macros.hpp>
#include <datum/interval/clip.h>
#include <datum/interval/query.h>
#include <datum/traversal.h>

#include <compare>
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
#include <forward_list>
#include <span>
#include <vector>

#include <memory>

#include <algorithm>
#include <ranges>

#include <string>
#include <string_view>

#include <stop_token>

#ifdef DATUM_HAVE_RAPIDCHECK
 #include <rapidcheck.h>

#endif

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

// Datum algebra tests: independent set-model, constexpr and cost checks.
// Keep fixtures neutral so this suite can run without Ceph or a backend.

namespace {

struct integers : iv::comparison_domain<int>
{};

using Q = iv::query<integers>;

TEST_CASE("prefix construction observes const policies and canonical empty prefixes",
          "[datum][domain][prefix]")
{
    struct const_prefix_domain final : iv::comparison_domain<int>
    {
        static constexpr bool empty_prefix(const int& value) noexcept { return 0 == value; }
        static bool empty_prefix(int&) = delete;

        static constexpr std::optional<int> prefix_upper_bound(const int& value) noexcept
        {
            return 0 == value ? std::nullopt : std::optional {10};
        }

        static std::optional<int> prefix_upper_bound(int&) = delete;
    };
    using query_type = iv::query<const_prefix_domain>;

    STATIC_REQUIRE(iv::prefix_domain<const_prefix_domain>);
    STATIC_REQUIRE_FALSE(iv::prefix_domain<integers>);
    constexpr auto all = query_type::prefix(0);
    constexpr auto selected = query_type::prefix(4);
    STATIC_REQUIRE(all == query_type::universal());
    STATIC_REQUIRE(iv::prefix<const_prefix_domain>(0) == all);
    STATIC_REQUIRE(selected == query_type::closed_open(4, 10));
    STATIC_REQUIRE(iv::prefix<const_prefix_domain>(4) == selected);
    CHECK(iv::contains(all, -1));
    CHECK(iv::contains(selected, 9));
    CHECK_FALSE(iv::contains(selected, 10));
}

TEST_CASE("comparison profiles preserve domain identity and the supplied policy", "[datum][domain]")
{
    struct source_offsets_domain final : iv::comparison_domain<int> {};
    struct destination_offsets_domain final : iv::comparison_domain<int> {};
    STATIC_REQUIRE(iv::ordered_domain<source_offsets_domain>);
    STATIC_REQUIRE_FALSE(iv::interval_for<iv::query<source_offsets_domain>,
                                          destination_offsets_domain>);
    STATIC_REQUIRE_FALSE(iv::ordered_domain<iv::comparison_domain<int, std::less<> {}>>);
    STATIC_REQUIRE_FALSE(iv::ordered_domain<iv::comparison_domain<int, std::weak_order>>);

    struct point final { int value; };
    struct point_order final
    {
        bool reversed;

    public:
        constexpr explicit point_order(const bool reversed) : reversed(reversed)
        {}

    public:
        constexpr auto operator()(const point& lhs, const point& rhs) const noexcept
        {
            return reversed ? rhs.value <=> lhs.value : lhs.value <=> rhs.value;
        }

    };
    using forward_domain = iv::comparison_domain<point, point_order {false}>;
    using reverse_domain = iv::comparison_domain<point, point_order {true}>;

    STATIC_REQUIRE_FALSE(std::three_way_comparable<point>);
    STATIC_REQUIRE_FALSE(std::default_initializable<point_order>);
    STATIC_REQUIRE_FALSE(iv::ordered_domain<iv::comparison_domain<point>>);
    STATIC_REQUIRE(iv::ordered_domain<forward_domain>);
    STATIC_REQUIRE(iv::ordered_domain<reverse_domain>);
    STATIC_REQUIRE(noexcept(forward_domain::compare(point {1}, point {2})));
    CHECK(iv::contains(iv::query<forward_domain>::closed(point {3}, point {8}), point {5}));
    CHECK(iv::contains(iv::query<reverse_domain>::closed(point {8}, point {3}), point {5}));
}

TEST_CASE("floating domains require an explicit strong ordering policy", "[datum][domain]")
{
    STATIC_REQUIRE_FALSE(iv::ordered_domain<iv::comparison_domain<double>>);
    using total_domain = iv::comparison_domain<double, std::strong_order>;
    STATIC_REQUIRE(iv::ordered_domain<total_domain>);
    using total_query = iv::query<total_domain>;

    if constexpr (std::numeric_limits<double>::is_iec559) {
        const auto negative_zero = total_query::singleton(-0.0);
        CHECK(iv::contains(negative_zero, -0.0));
        CHECK_FALSE(iv::contains(negative_zero, +0.0));

        const auto nan = std::numeric_limits<double>::quiet_NaN();
        const auto nan_only = total_query::singleton(nan);
        CHECK(iv::contains(nan_only, nan));
        CHECK_FALSE(iv::contains(nan_only, 0.0));
    }
}

constexpr Q interval(int lower, int upper, unsigned flags)
{
    return Q::between(
        flags & 1 ? iv::boundary<integers>::closed(lower) : iv::boundary<integers>::open(lower),
        flags & 2 ? iv::boundary<integers>::closed(upper) : iv::boundary<integers>::open(upper));
}

constexpr auto observations = std::views::iota(-20, 21);
constexpr std::uint64_t full_mask = (std::uint64_t {1} << 41) - 1;

constexpr std::uint64_t model_mask(int lower, int upper, unsigned flags)
{
    std::uint64_t result = 0;
    for (const int point : observations) {
        if ((point > lower || (point == lower && (flags & 1))) &&
            (point < upper || (point == upper && (flags & 2)))) {
            result |= std::uint64_t {1} << (point + 20);
        }
    }

    return result;
}

template <iv::expression ExprT>
constexpr std::uint64_t mask(const ExprT& expression)
{
    std::uint64_t result = 0;
    for (const int point : observations) {
        if (iv::contains(expression, point)) {
            result |= std::uint64_t {1} << (point + 20);
        }
    }

    return result;
}

template <iv::expression ExprT>
bool canonical_stream(const ExprT& expression, std::uint64_t expected)
{
    const auto intervals = iv::compile_intervals(expression);
    if (!std::ranges::all_of(intervals, [](const auto& value) { return !iv::is_empty(value); })) {
        return false;
    }
    for (const auto& [previous, current] : intervals | std::views::pairwise) {
        if (iv::intersects(previous, current) ||
            previous.upper().kind() == iv::boundary_kind::positive_infinity ||
            current.lower().kind() == iv::boundary_kind::negative_infinity) {
            return false;
        }
        if (previous.upper().finite() && current.lower().finite()) {
            const auto order = integers::compare(previous.upper().key(), current.lower().key());
            if (order == std::strong_ordering::greater ||
                (order == std::strong_ordering::equal &&
                 (previous.upper().inclusive() || current.lower().inclusive()))) {
                return false;
            }
        }
    }
    for (const int point : observations) {
        const auto count = std::ranges::count_if(
            intervals, [point](const auto& value) { return iv::contains(value, point); });
        if (count != (((expected >> (point + 20)) & 1) ? 1 : 0)) {
            return false;
        }
    }

    return true;
}

consteval bool constexpr_predicates()
{
    const auto a = iv::set_union(Q::closed_open(0, 4), Q::closed_open(8, 12));
    const auto b = iv::set_union(Q::closed_open(3, 9), Q::closed_open(16, 20));

    return iv::contains(a, 1) && !iv::contains(a, 6) && iv::intersects(a, b) &&
           iv::encloses(a, Q::closed_open(1, 3)) &&
           !iv::intersects(Q::closed_open(0, 2), Q::closed_open(2, 4)) &&
           iv::intersects(Q::closed(0, 2), Q::closed(2, 4)) &&
           iv::encloses(Q::empty(), Q::empty()) && !iv::is_empty_expression(a);
}

static_assert(constexpr_predicates());

struct counted_point
{
    std::string bytes;
    static inline std::size_t copies = 0;

public:
    explicit counted_point(std::string value) : bytes(std::move(value))
    {}

    counted_point(const counted_point& other) : bytes(other.bytes) { ++copies; }

    counted_point(counted_point&&) = default;

public:
    counted_point& operator=(const counted_point& other)
    {
        bytes = other.bytes;
        ++copies;

        return *this;
    }

    counted_point& operator=(counted_point&&) = default;

};

struct counted_domain
{
    using value_type = counted_point;

    static inline std::size_t comparisons = 0;

public:
    static std::strong_ordering compare(const counted_point& a, const counted_point& b)
    {
        ++comparisons;

        return a.bytes <=> b.bytes;
    }

};

using CQ = iv::query<counted_domain>;

struct bounds_sink
{
    std::size_t emitted = 0;

public:
    template <typename IntervalT>

    void operator()(const IntervalT&)
    {
        static_assert(!std::same_as<IntervalT, IntervalT>,
                      "The boundary protocol must discard the value protocol");
    }

public:
    void emit_interval(const auto&, const auto&) { ++emitted; }

};
} // anonymous namespace

TEST_CASE("compound membership preserves short-circuit observation", "[datum][algebra]")
{
    struct observed_interval final
    {
        using domain_type = integers;

        Q value;

        unsigned& observations;

    public:
        const auto& lower() const
        {
            ++observations;

            return value.lower();
        }

        const auto& upper() const
        {
            ++observations;

            return value.upper();
        }

    };

    unsigned observations = 0;
    const observed_interval right {Q::closed(0, 4), observations};
    const auto present = iv::set_union(Q::closed(0, 4), Q::empty());
    const auto absent = iv::set_union(Q::closed(10, 14), Q::empty());
    CHECK(iv::contains(iv::set_union(present, right), 1));
    CHECK_FALSE(iv::contains(iv::intersection(absent, right), 1));
    CHECK_FALSE(iv::contains(iv::difference(absent, right), 1));
    CHECK(0 == observations);
    CHECK(iv::contains(iv::intersection(present, right), 1));
    CHECK(0 < observations);
}

TEST_CASE("Datum boundary predicates match a dense finite set model", "[datum][algebra]")
{
    // Spaced endpoints leave observations inside every nonempty open interval.
    std::array<Q, 196> intervals;
    std::array<std::uint64_t, 196> models;
    auto out = std::begin(intervals);
    auto oracle = std::begin(models);
    for (const int lo : std::views::iota(-3, 4)) {
        for (const int hi : std::views::iota(-3, 4)) {
            for (const auto flags : std::views::iota(0u, 4u)) {
                *out++ = interval(4 * lo, 4 * hi, flags);
                *oracle++ = model_mask(4 * lo, 4 * hi, flags);
            }
        }
    }
    for (const auto i : std::views::iota(std::size_t {0}, std::size(intervals))) {
        const auto& a = intervals[i];
        const auto ma = models[i];
        REQUIRE(mask(a) == ma);
        for (const auto j : std::views::iota(std::size_t {0}, std::size(intervals))) {
            const auto& b = intervals[j];
            const auto mb = models[j];
            CHECK(iv::intersects(a, b) == ((ma & mb) != 0));
            CHECK(iv::encloses(a, b) == ((mb & ~ma) == 0));
            CHECK(mask(iv::difference(a, b)) == (ma & ~mb));
            CHECK(mask(iv::set_union(a, b)) == (ma | mb));
        }
    }
}

TEST_CASE("Datum structural predicates do not copy long point payloads", "[datum][cost]")
{
    const auto point = [](char tail) { return counted_point {std::string(96, 'k') + tail}; };
    const auto a = CQ::closed(point('a'), point('m'));
    const auto b = CQ::closed(point('p'), point('z'));
    const auto expression = iv::difference(iv::set_union(a, b), CQ::singleton(point('x')));
    const auto probe = point('c');
    counted_point::copies = 0;
    counted_domain::comparisons = 0;
    const bool found = iv::contains(expression, probe);
    const auto membership_comparisons = counted_domain::comparisons;
    const auto membership_copies = counted_point::copies;
    CHECK(found);
    CHECK(membership_copies == 0);
    CHECK(membership_comparisons <= 9);

    counted_point::copies = 0;
    const bool disjoint = iv::is_disjoint(a, b);
    const auto overlap_copies = counted_point::copies;
    CHECK(disjoint);
    CHECK(overlap_copies == 0);

    counted_point::copies = 0;
    const bool contained = iv::encloses(a, a);
    const auto enclosure_copies = counted_point::copies;
    CHECK(contained);
    CHECK(enclosure_copies == 0);

    // A boundary-only sink works on leaves and normalized expressions alike.
    bounds_sink sink;
    counted_point::copies = 0;
    iv::for_each_interval(a, sink);
    const auto leaf_copies = counted_point::copies;
    CHECK(sink.emitted == 1);
    CHECK(leaf_copies == 0);
    iv::for_each_interval(expression, sink);
    CHECK(sink.emitted == 4);
}

TEST_CASE("Datum infinity and exact-key differences preserve endpoint meaning", "[datum][algebra]")
{
    CHECK(iv::encloses(Q::universal(), Q::closed(1, 3)));
    CHECK_FALSE(iv::encloses(Q::closed(1, 3), Q::universal()));
    CHECK(iv::is_empty_expression(iv::set_union(Q::empty(), Q::empty())));
    const auto hole = iv::difference(Q::universal(), Q::singleton(0));
    CHECK_FALSE(iv::contains(hole, 0));
    CHECK(iv::contains(hole, -1));
    CHECK(iv::contains(hole, 1));
    CHECK(canonical_stream(hole, full_mask & ~(std::uint64_t {1} << 20)));
}

#ifdef DATUM_HAVE_RAPIDCHECK
 TEST_CASE("Datum generated composite expressions obey set laws and canonical output",
           "[datum][property]")
 {
     CHECK(rc::check(
         "composite algebra versus independent finite masks",
         [](const std::array<std::uint8_t, 16>& generated) {
             const auto value = [&](std::size_t i) {
                 return interval(4 * (int(generated[4 * i] % 7) - 3),
                                 4 * (int(generated[4 * i + 1] % 7) - 3), generated[4 * i + 2]);
             };
             const auto a = iv::set_union(value(0), value(1));
             const auto b = iv::set_union(value(2), value(3));
             const auto oracle = [&](std::size_t i) {
                 return model_mask(4 * (int(generated[4 * i] % 7) - 3),
                                   4 * (int(generated[4 * i + 1] % 7) - 3), generated[4 * i + 2]);
             };
             const auto ma = oracle(0) | oracle(1);
             const auto mb = oracle(2) | oracle(3);
             const auto combined =
                 iv::intersection(iv::complement(iv::difference(a, b)), iv::set_union(a, b));
             RC_ASSERT(mask(iv::difference(a, b)) == (ma & ~mb));
             RC_ASSERT(mask(a) == ma);
             RC_ASSERT(mask(b) == mb);
             RC_ASSERT(mask(iv::intersection(a, b)) == (ma & mb));
             RC_ASSERT(mask(iv::complement(a)) == (full_mask ^ ma));
             RC_ASSERT(iv::intersects(a, b) == ((ma & mb) != 0));
             RC_ASSERT(iv::encloses(a, b) == ((mb & ~ma) == 0));
             RC_ASSERT(canonical_stream(a, ma));
             RC_ASSERT(canonical_stream(b, mb));
             RC_ASSERT(canonical_stream(combined, (full_mask ^ (ma & ~mb)) & (ma | mb)));
         }));
 }
#endif

TEST_CASE("canonical sets normalize once and admit checked views", "[datum][canonical]")
{
    const std::array raw {Q::closed(8, 10), Q::empty(), Q::closed(1, 4), Q::open(4, 8)};
    const iv::canonical_set prepared {raw};
    REQUIRE(std::size(prepared) == 1);
    CHECK(iv::encloses(prepared, Q::closed(1, 10)));
    const iv::canonical_view borrowed {prepared};
    static_assert(iv::canonical_sequence<decltype(borrowed)>);
    static_assert(std::is_reference_v<decltype(iv::difference(prepared, borrowed).lhs)>);
    CHECK(iv::contains(borrowed, 4));
    CHECK(iv::intersects(borrowed, Q::closed(9, 11)));
    CHECK_THROWS_AS(iv::canonical_view {raw}, std::invalid_argument);
    const std::array holes {Q::closed_open(1, 4), Q::open(4, 10)};
    CHECK(std::size(iv::canonical_view {holes}) == 2);
    const iv::canonical_view retained {std::vector {Q::closed(2, 3)}};
    CHECK(iv::contains(retained, 2));
}

TEST_CASE("canonical sweeps retain every endpoint flag and singleton hole", "[datum][canonical]")
{
    for (int a = -2; a <= 2; ++a) {
        for (int b = -2; b <= 2; ++b) {
            for (unsigned flags = 0; flags != 4; ++flags) {
                const iv::canonical_set left {
                    iv::set_union(interval(a, b, flags), Q::singleton(8))};
                const iv::canonical_set right {
                    iv::set_union(Q::singleton(a), interval(b, 8, flags))};
                const auto l = mask(left);
                const auto r = mask(right);
                CHECK(canonical_stream(iv::set_union(left, right), l | r));
                CHECK(canonical_stream(iv::intersection(left, right), l & r));
                CHECK(canonical_stream(iv::difference(left, right), l & ~r));
                CHECK(iv::intersects(left, right) == bool(l & r));
                CHECK(iv::encloses(left, right) == ((r & ~l) == 0));
            }
        }
    }
}

TEST_CASE("workspace budgets reject before publication and permit reuse", "[datum][workspace]")
{
    const auto expression =
        iv::difference(iv::set_union(Q::closed(1, 5), Q::closed(8, 10)), Q::singleton(3));
    iv::workspace<integers> scratch {{.intermediate_intervals = 16, .work = 100}};
    const auto first = iv::evaluate(expression, scratch);
    static_assert(std::same_as<decltype(first), const std::span<const Q>>);
    CHECK(std::size(first) == 3);
    CHECK(std::data(first) == std::data(scratch.get(0)));
    const auto work = scratch.work_performed();
    CHECK(work > 0);
    CHECK(std::size(iv::evaluate(expression, scratch)) == 3);
    CHECK(scratch.work_performed() == work);
    for (const auto limits : {iv::evaluation_limits {0, 100}, iv::evaluation_limits {16, 0}}) {
        iv::workspace<integers> limited {limits};
        std::size_t published = 0;
        CHECK_THROWS_AS(iv::evaluate(expression, limited, [&](const auto&) { ++published; }),
                        std::length_error);
        CHECK(published == 0);
        CHECK(limited.retained_intervals() == 0);
    }
}

static_assert([] {
    const iv::canonical_set left {iv::set_union(Q::closed(1, 3), Q::closed(5, 7))};
    const iv::canonical_set right {Q::singleton(2)};
    iv::workspace<integers> scratch;

    return std::size(iv::evaluate(iv::difference(left, right), scratch)) == 3 &&
           iv::intersects(left, right) && !iv::encloses(right, left);
}());

TEST_CASE("normalization needs comparison and copy construction, not point assignment",
          "[datum][canonical]")
{
    struct point
    {
        int value;

    public:
        explicit point(int n) : value(n)
        {}
        point(const point&) = default;

    public:
        point& operator=(const point&) = delete;

    };
    struct domain
    {
        using value_type = point;

    public:
        static std::strong_ordering compare(const point& lhs, const point& rhs)
        {
            return lhs.value <=> rhs.value;
        }

    };
    using query = iv::query<domain>;

    const std::array raw {query::closed(point {5}, point {9}), query::closed(point {1}, point {5})};
    const iv::canonical_set<domain> prepared {raw};
    CHECK(std::size(prepared) == 1);
    CHECK(iv::contains(prepared, point {4}));
    CHECK(std::size(iv::compile_intervals(iv::difference(prepared, query::singleton(point {4})))) ==
          2);
}

TEST_CASE("prepared sweeps and consuming helpers copy only retained point values", "[datum][cost]")
{
    const auto point = [](char tail) { return counted_point {std::string(104, 'k') + tail}; };
    const auto lower = point('a');
    const auto upper = point('z');
    counted_point::copies = 0;
    const auto value = iv::between(iv::lower_at_or_after<counted_domain>(lower),
                                   iv::upper_at_or_before<counted_domain>(upper));
    CHECK(counted_point::copies == 2);
    const iv::canonical_set prepared {value};
    const iv::canonical_set removed {CQ::singleton(point('m'))};
    counted_point::copies = 0;
    bounds_sink sink;
    iv::for_each_interval(iv::difference(prepared, removed), sink);
    CHECK(sink.emitted == 2);
    CHECK(counted_point::copies == 0);
    CHECK(iv::intersects(prepared, removed));
    CHECK(iv::encloses(prepared, removed));
    CHECK(counted_point::copies == 0);
    counted_point::copies = 0;
    const auto owned = iv::compile_intervals(iv::difference(prepared, removed));
    CHECK(std::size(owned) == 2);
    CHECK(counted_point::copies == 4);
    counted_point::copies = 0;
    const auto bounded = iv::compile_intervals<2>(iv::difference(prepared, removed));
    CHECK(std::size(bounded) == 2);
    CHECK(counted_point::copies == 4);
}

#ifdef DATUM_HAVE_RAPIDCHECK
 TEST_CASE("runtime canonical ranges obey the independent set model", "[datum][canonical][property]")
 {
     CHECK(rc::check("runtime ranges versus finite masks",
                     [](const std::vector<std::array<unsigned char, 3>>& generated) {
                         std::vector<Q> lhs;
                         std::vector<Q> rhs;
                         std::uint64_t l = 0;
                         std::uint64_t r = 0;
                         std::size_t index = 0;
                         for (const auto& item : generated) {
                             const int lower = 4 * (int(item[0] % 7) - 3);
                             const int upper = 4 * (int(item[1] % 7) - 3);
                             auto& values = index++ % 2 == 0 ? lhs : rhs;
                             auto& bits = index % 2 != 0 ? l : r;
                             ::ceph::util::push_back(values, interval(lower, upper, item[2]));
                             bits |= model_mask(lower, upper, item[2]);
                         }
                         const iv::canonical_set left {lhs};
                         const iv::canonical_set right {rhs};
                         RC_ASSERT(canonical_stream(iv::set_union(left, right), l | r));
                         RC_ASSERT(canonical_stream(iv::intersection(left, right), l & r));
                         RC_ASSERT(canonical_stream(iv::difference(left, right), l & ~r));
                         RC_ASSERT(iv::intersects(left, right) == bool(l & r));
                         RC_ASSERT(iv::encloses(left, right) == ((r & ~l) == 0));
                         RC_ASSERT(canonical_stream(iv::complement(left), full_mask ^ l));
                     }));
 }
#endif

namespace {

// Forward iteration promises dereference, not an arrow operator.
struct interval_iterator final
{
    const Q *current = nullptr;

    using value_type = Q;
    using difference_type = std::ptrdiff_t;
    using iterator_concept = std::forward_iterator_tag;

public:
    constexpr const Q& operator*() const { return *current; }

    constexpr interval_iterator& operator++()
    {
        ++current;

        return *this;
    }

    constexpr interval_iterator operator++(int)
    {
        auto saved = *this;
        ++*this;

        return saved;
    }

    constexpr bool operator==(const interval_iterator&) const = default;

};

struct admission_view final : std::ranges::view_interface<admission_view>
{
    std::span<const Q> mutable_values;
    std::span<const Q> observed_values;

public:
    constexpr admission_view(std::span<const Q> provisional, std::span<const Q> observed)
        : mutable_values(provisional), observed_values(observed)
    {}

public:
    constexpr auto begin() { return std::begin(mutable_values); }

    constexpr auto end() { return std::end(mutable_values); }
    constexpr auto begin() const { return std::begin(observed_values); }

    constexpr auto end() const { return std::end(observed_values); }

};

constexpr bool arrowless_relations()
{
    const std::array values {Q::closed_open(0, 4), Q::closed_open(8, 12)};
    const iv::canonical_view source {
        std::ranges::subrange {interval_iterator {std::data(values)},
                               interval_iterator {std::data(values) + std::size(values)}}};
    const iv::canonical_set inside {Q::closed_open(9, 11)};
    const iv::canonical_set outside {Q::closed_open(5, 7)};

    return iv::intersects(source, inside) && iv::encloses(source, inside) &&
           iv::is_disjoint(source, outside) &&
           3 == iv::interval_count(iv::difference(source, inside));
}
} // anonymous namespace

static_assert(std::forward_iterator<interval_iterator>);
static_assert(arrowless_relations());

TEST_CASE("canonical relations accept forward iterators without arrows", "[datum][canonical]")
{
    CHECK(arrowless_relations());
}

TEST_CASE("canonical admission validates the const observation", "[datum][canonical]")
{
    const std::array ordered {Q::closed_open(0, 4), Q::closed_open(8, 12)};
    const std::array unordered {ordered[1], ordered[0]};
    const iv::canonical_view accepted {admission_view {unordered, ordered}};
    CHECK(iv::contains(accepted, 1));
    CHECK_THROWS_AS(iv::canonical_view(admission_view {ordered, unordered}), std::invalid_argument);
}

namespace {

template <typename T>
concept temporary_stream = requires(T&& value) { iv::detail::canonical_stream(std::move(value)); };

constexpr bool nested_stream_lifetimes()
{
    const std::array left {Q::closed_open(0, 20), Q::closed_open(32, 52)};
    const std::array right {Q::closed_open(8, 28), Q::closed_open(40, 60)};
    const std::array filter {Q::closed_open(4, 14), Q::closed_open(36, 46)};
    const std::array holes {Q::closed_open(7, 10), Q::closed_open(39, 42)};
    const iv::canonical_view a {left}, b {right}, c {filter}, d {holes};
    const auto expression = iv::difference(iv::intersection(iv::set_union(a, b), c), d);
    static_assert(!temporary_stream<decltype(expression)>);
    const auto stream = iv::detail::canonical_stream(expression);
    static_assert(std::ranges::input_range<const decltype(stream)>);
    static_assert(!std::ranges::forward_range<const decltype(stream)>);
    auto cursor = std::ranges::begin(stream);
    const auto saved = *cursor;
    auto independent = cursor;
    ++cursor;
    const auto previous = *independent++;
    if (saved.lower().key() != 4 || saved.upper().key() != 7 || previous.lower().key() != 4 ||
        previous.upper().key() != 7 || (*independent).lower().key() != 10 ||
        (*cursor).lower().key() != 10) {
        return false;
    }
    const auto result = iv::compile_intervals<4>(expression);

    return std::size(result) == 4 && result[0] == Q::closed_open(4, 7) &&
           result[1] == Q::closed_open(10, 14) && result[2] == Q::closed_open(36, 39) &&
           result[3] == Q::closed_open(42, 46) && iv::interval_count(expression) == 4 &&
           iv::interval_count(a) == 2 && !iv::is_empty_expression(expression) &&
           iv::is_empty_expression(iv::difference(a, a));
}
} // anonymous namespace

static_assert(nested_stream_lifetimes());

namespace {

constexpr bool subtraction_storage_identity()
{
    const std::array rows {Q::closed_open(0, 4), Q::closed_open(8, 12)};
    const auto separate = rows;
    const iv::canonical_view full {std::span {rows}};
    const iv::canonical_view prefix {std::span {rows}.first(1)};
    const iv::canonical_view equal {std::span {separate}};
    const iv::canonical_set<integers> empty;
    const auto tail = iv::compile_intervals<1>(iv::set_union(iv::difference(full, prefix), empty));

    return std::size(tail) == 1 && tail[0] == rows[1] &&
           std::empty(
               iv::compile_intervals<0>(iv::set_union(iv::difference(full, equal), empty))) &&
           std::empty(iv::compile_intervals<0>(iv::set_union(iv::difference(full, full), empty)));
}
} // anonymous namespace

static_assert(subtraction_storage_identity());

TEST_CASE("canonical subtraction distinguishes storage identity from equal endpoints",
          "[datum][canonical]")
{
    CHECK(subtraction_storage_identity());
    const iv::canonical_set left {Q::closed(0, 12)};
    const iv::canonical_set right {Q::open(0, 12)};
    const iv::canonical_set<integers> empty;
    const auto result = iv::compile_intervals(iv::set_union(iv::difference(left, right), empty));
    REQUIRE(std::size(result) == 2);
    CHECK(result[0] == Q::singleton(0));
    CHECK(result[1] == Q::singleton(12));
}

TEST_CASE("nested subtraction matches endpoint masks through empty intermediates",
          "[datum][algebra]")
{
    const iv::canonical_set<integers> empty;
    const auto endpoints = std::views::cartesian_product(
        std::views::iota(-3, 4), std::views::iota(-3, 4), std::views::iota(0U, 4U));
    for (const auto [lower, upper, flags] : endpoints) {
        const iv::canonical_set left {interval(4 * lower, 4 * upper, flags)};
        const auto left_mask = model_mask(4 * lower, 4 * upper, flags);
        for (const auto [removed_lower, removed_upper, removed_flags] : endpoints) {
            const iv::canonical_set right {
                interval(4 * removed_lower, 4 * removed_upper, removed_flags)};
            const auto expected =
                left_mask & ~model_mask(4 * removed_lower, 4 * removed_upper, removed_flags);
            CHECK(canonical_stream(iv::set_union(iv::difference(left, right), empty), expected));
        }
    }
    const std::array unbounded {
        Q::empty(), Q::universal(), Q::singleton(0),
        Q::between(iv::boundary<integers>::negative_infinity(), iv::boundary<integers>::closed(0)),
        Q::between(iv::boundary<integers>::open(0), iv::boundary<integers>::positive_infinity())};
    for (const auto& a : unbounded) {
        const iv::canonical_set left {a};
        for (const auto& b : unbounded) {
            const iv::canonical_set right {b};
            CHECK(canonical_stream(iv::set_union(iv::difference(left, right), empty),
                                   mask(a) & ~mask(b)));
        }
    }
}

TEST_CASE("nested canonical cursors keep endpoint storage stable", "[datum][canonical]")
{
    CHECK(nested_stream_lifetimes());
    const iv::canonical_set<integers> empty;
    const iv::canonical_set all {Q::universal()};
    const iv::canonical_set hole {Q::singleton(0)};
    const auto expression = iv::difference(iv::set_union(empty, all), hole);
    const auto result = iv::compile_intervals(expression);
    REQUIRE(std::size(result) == 2);
    CHECK(result[0] ==
          Q::between(iv::boundary<integers>::negative_infinity(), iv::boundary<integers>::open(0)));
    CHECK(result[1] ==
          Q::between(iv::boundary<integers>::open(0), iv::boundary<integers>::positive_infinity()));
    CHECK(std::empty(iv::compile_intervals(iv::intersection(iv::difference(all, all), hole))));
}

TEST_CASE("nested traversal borrows points and preserves the sink protocol", "[datum][cost]")
{
    const auto point = [](char tail) { return counted_point {std::string(104, 'k') + tail}; };
    const iv::canonical_set a {CQ::closed(point('a'), point('z'))};
    const iv::canonical_set<counted_domain> b;
    const iv::canonical_set c {CQ::closed(point('d'), point('w'))};
    const iv::canonical_set d {CQ::open(point('m'), point('p'))};
    const auto expression = iv::difference(iv::intersection(iv::set_union(a, b), c), d);
    counted_point::copies = 0;
    bounds_sink sink;
    iv::for_each_interval(expression, sink);
    CHECK(sink.emitted == 2);
    CHECK(counted_point::copies == 0);
    const auto values = iv::compile_intervals(expression);
    REQUIRE(std::size(values) == 2);
    CHECK(counted_point::copies == 4);
    CHECK(values[0].lower().key().bytes.back() == 'd');
    CHECK(values[0].upper().inclusive());
    CHECK(values[1].lower().inclusive());
    CHECK(values[1].upper().key().bytes.back() == 'w');

    std::size_t published = 0;
    CHECK_THROWS_AS(iv::for_each_interval(
                        expression,
                        [&](const auto& value) {
                            static_assert(std::same_as<std::remove_cvref_t<decltype(value)>, CQ>);
                            if (++published == 2) {
                                throw std::runtime_error {"consumer failure"};
                            }
                        }),
                    std::runtime_error);
    CHECK(published == 2);
    counted_point::copies = 0;
    CHECK(iv::interval_count(expression) == 2);
    CHECK(counted_point::copies == 0);
    iv::workspace<counted_domain> limited {{.intermediate_intervals = 0, .work = 100}};
    published = 0;
    CHECK_THROWS_AS(iv::evaluate(expression, limited, [&](const auto&) { ++published; }),
                    std::length_error);
    CHECK(published == 0);
    CHECK(limited.retained_intervals() == 0);
}

TEST_CASE("interval counting observes bounds without retaining point values", "[datum][cost]")
{
    const auto point = [](char tail) { return counted_point {std::string(104, 'k') + tail}; };
    const std::array rows {CQ::closed(point('a'), point('k')), CQ::closed(point('m'), point('z'))};
    const iv::canonical_view source {rows};
    const std::forward_list linked {rows[0], rows[1]};
    const iv::canonical_view unsized {linked};
    static_assert(!std::ranges::sized_range<const decltype(unsized)>);
    const iv::canonical_set hole {CQ::open(point('d'), point('f'))};
    const iv::canonical_set all {CQ::universal()};
    const auto selected = iv::intersection(iv::difference(source, hole), all);
    counted_point::copies = 0;
    counted_domain::comparisons = 0;
    const std::array<CQ, 0> empty_rows;
    CHECK(iv::interval_count(iv::canonical_view {empty_rows}) == 0);
    CHECK(iv::interval_count(source) == 2);
    CHECK(counted_domain::comparisons == 0);
    CHECK(iv::interval_count(unsized) == 2);
    CHECK(counted_domain::comparisons == 0);
    CHECK_FALSE(iv::is_empty_expression(unsized));
    CHECK(counted_domain::comparisons == 0);
    CHECK(iv::interval_count(selected) == 3);
    CHECK_FALSE(iv::is_empty_expression(selected));
    CHECK(iv::interval_count(iv::set_union(iv::difference(source, source), CQ::empty())) == 0);
    CHECK(counted_point::copies == 0);

    struct observed_interval final
    {
        using domain_type = counted_domain;

        const CQ& value;
        std::size_t& observations;
        bool fail = false;

    public:
        const auto& lower() const
        {
            ++observations;
            if (fail) {
                throw std::runtime_error {"accessor failure"};
            }

            return value.lower();
        }

        const auto& upper() const
        {
            ++observations;

            return value.upper();
        }

    };
    std::size_t observations = 0;
    const std::array observed {observed_interval {rows[0], observations},
                               observed_interval {rows[1], observations}};
    const iv::canonical_view admitted {observed};
    observations = 0;
    CHECK(iv::interval_count(admitted) == 2);
    CHECK(observations == 0);
    const std::forward_list observed_linked {observed[0], observed[1]};
    const iv::canonical_view admitted_unsized {observed_linked};
    observations = 0;
    CHECK(iv::interval_count(admitted_unsized) == 2);
    CHECK_FALSE(iv::is_empty_expression(admitted_unsized));
    CHECK(observations == 0);
    CHECK(iv::interval_count(observed[0]) == 1);
    CHECK(observations == 2);
    CHECK(counted_point::copies == 0);
    const observed_interval failing {rows[0], observations, true};
    CHECK_THROWS_AS(iv::interval_count(failing), std::runtime_error);
}

TEST_CASE("emptiness stops after the first certified streaming result", "[datum][cost]")
{
    std::vector<CQ> rows;
    for (const auto index : std::views::iota(0U, 64U)) {
        const auto key = [](unsigned value) {
            return counted_point {std::to_string(1000 + value)};
        };
        ::ceph::util::emplace_back(rows, CQ::closed_open(key(4 * index), key(4 * index + 2)));
    }
    const iv::canonical_view source {rows};
    // Finite covering bounds make comparisons observe early termination.
    const iv::canonical_set all {CQ::closed(rows.front().lower().key(), rows.back().upper().key())};
    const auto selected = iv::intersection(source, all);
    counted_point::copies = 0;
    counted_domain::comparisons = 0;
    CHECK_FALSE(iv::is_empty_expression(selected));
    const auto first_work = counted_domain::comparisons;
    counted_domain::comparisons = 0;
    CHECK(iv::interval_count(selected) == 64);
    CHECK(counted_domain::comparisons > first_work);
    CHECK(counted_point::copies == 0);
    CHECK(iv::is_empty_expression(iv::difference(source, source)));
    CHECK(iv::is_empty_expression(iv::intersection(source, CQ::empty())));
}

TEST_CASE("admitted count retains size_t capacity for narrow iterator differences",
          "[datum][range]")
{
    struct cursor final
    {
        using iterator_concept = std::forward_iterator_tag;
        using value_type = Q;
        using difference_type = signed char;

        const Q *position = nullptr;

    public:
        const Q& operator*() const { return *position; }

        cursor& operator++()
        {
            ++position;

            return *this;
        }

        cursor operator++(int)
        {
            const auto previous = *this;
            ++*this;

            return previous;
        }

        bool operator==(const cursor&) const = default;

    };
    static_assert(std::forward_iterator<cursor>);
    std::vector<Q> rows;
    for (const auto index : std::views::iota(0, 256)) {
        ::ceph::util::emplace_back(rows, Q::closed_open(2 * index, 2 * index + 1));
    }
    const iv::canonical_view source {std::ranges::subrange {
        cursor {std::data(rows)}, cursor {std::data(rows) + std::size(rows)}}};
    static_assert(!std::ranges::sized_range<const decltype(source)>);
    CHECK(iv::interval_count(source) == 256);
    CHECK_FALSE(iv::is_empty_expression(source));
}

#ifdef DATUM_HAVE_RAPIDCHECK
 TEST_CASE("nested canonical streams match set masks and bounded evaluation",
           "[datum][canonical][property]")
 {
     CHECK(rc::check(
         "nested canonical composition",
         [](const std::vector<std::array<unsigned char, 4>>& generated) {
             std::array<std::vector<Q>, 4> raw;
             std::array<std::uint64_t, 4> bits {};
             for (const auto& item : generated) {
                 const auto leaf = item[3] % 4;
                 const auto lower = 4 * (int(item[0] % 7) - 3);
                 const auto upper = 4 * (int(item[1] % 7) - 3);
                 ::ceph::util::push_back(raw[leaf], interval(lower, upper, item[2]));
                 bits[leaf] |= model_mask(lower, upper, item[2]);
             }
             const iv::canonical_set a {raw[0]}, b {raw[1]}, c {raw[2]}, d {raw[3]};
             const auto check = [&](const auto& expression, const std::uint64_t expected) {
                 RC_ASSERT(canonical_stream(expression, expected));
                 iv::workspace<integers> scratch;
                 const auto bounded = iv::evaluate(expression, scratch);
                 RC_ASSERT(std::ranges::equal(iv::compile_intervals(expression), bounded));
                 RC_ASSERT(iv::interval_count(expression) == std::size(bounded));
                 RC_ASSERT(iv::is_empty_expression(expression) == std::ranges::empty(bounded));
             };
             check(iv::difference(iv::intersection(iv::set_union(a, b), c), d),
                   ((bits[0] | bits[1]) & bits[2]) & ~bits[3]);
             check(iv::intersection(iv::difference(a, b), iv::set_union(c, d)),
                   (bits[0] & ~bits[1]) & (bits[2] | bits[3]));
             check(iv::set_union(iv::difference(a, b), iv::difference(c, d)),
                   (bits[0] & ~bits[1]) | (bits[2] & ~bits[3]));
             check(iv::difference(iv::set_union(a, b), iv::difference(c, d)),
                   (bits[0] | bits[1]) & ~(bits[2] & ~bits[3]));
         }));
 }
#endif

static_assert([] {
    const std::array rows {Q::closed_open(0, 4), Q::closed_open(8, 12)};
    const iv::canonical_view source {rows};
    const auto self = iv::difference(source, source);

    return iv::interval_count(self) == 0 && iv::is_empty_expression(self) &&
           std::empty(iv::compile_intervals<0>(self));
}());

TEST_CASE("canonical self subtraction needs no endpoint observation", "[datum][cost]")
{
    struct observed_interval final
    {
        using domain_type = integers;

        const Q& value;
        std::size_t& calls;
        bool fail = false;

    public:
        const auto& lower() const
        {
            ++calls;
            if (fail) {
                throw std::runtime_error {"accessor failure"};
            }

            return value.lower();
        }

        const auto& upper() const
        {
            ++calls;

            return value.upper();
        }

    };
    const std::array rows {Q::closed_open(0, 4), Q::closed_open(8, 12)};
    std::size_t calls = 0;
    const std::array observed {observed_interval {rows[0], calls},
                               observed_interval {rows[1], calls}};
    const std::forward_list linked {observed[0], observed[1]};
    const iv::canonical_view contiguous {observed};
    const iv::canonical_view noncontiguous {linked};
    const auto check = [&](const auto& source) {
        const auto self = iv::difference(source, source);
        calls = 0;
        CHECK(iv::is_empty_expression(self));
        CHECK(iv::interval_count(self) == 0);
        bool published = false;
        iv::for_each_interval(self, [&published](const auto&) { published = true; });
        CHECK_FALSE(published);
        CHECK(std::empty(iv::compile_intervals<0>(self)));
        CHECK(iv::interval_count(iv::set_union(self, Q::empty())) == 0);
        CHECK(std::empty(iv::compile_intervals<0>(iv::set_union(self, Q::empty()))));
        CHECK(calls == 0);

        // Explicit evaluation keeps its accounting and owning preparation path.
        iv::workspace<integers> scratch;
        CHECK(std::empty(iv::evaluate(self, scratch)));
        CHECK(calls > 0);
    };
    check(contiguous);
    check(noncontiguous);

    const observed_interval failing {rows[0], calls, true};
    const auto foreign = iv::difference(failing, failing);
    CHECK_THROWS_AS(iv::interval_count(foreign), std::runtime_error);
    CHECK_THROWS_AS(iv::is_empty_expression(foreign), std::runtime_error);
}

TEST_CASE("canonical views preserve ordinary copy and move semantics", "[datum][range]")
{
    const std::array rows {CQ::closed_open(counted_point {"a"}, counted_point {"d"}),
                           CQ::closed_open(counted_point {"h"}, counted_point {"k"})};
    const iv::canonical_view borrowed {rows};
    using borrowed_type = std::remove_cvref_t<decltype(borrowed)>;

    static_assert(std::ranges::view<borrowed_type> && std::copy_constructible<borrowed_type>);
    counted_point::copies = 0;
    counted_domain::comparisons = 0;
    const iv::canonical_view copied {borrowed};
    static_assert(std::same_as<std::remove_cvref_t<decltype(copied)>, borrowed_type>);
    CHECK(std::ranges::begin(copied) == std::ranges::begin(borrowed));
    CHECK(counted_point::copies == 0);
    CHECK(counted_domain::comparisons == 0);

    iv::canonical_view owned {std::vector {rows[0], rows[1]}};
    using owned_type = decltype(owned);

    static_assert(std::ranges::view<owned_type> && !std::copy_constructible<owned_type>);
    const auto first = std::addressof(*std::ranges::begin(owned));
    counted_point::copies = 0;
    counted_domain::comparisons = 0;
    const auto relocated = std::move(owned);
    CHECK(std::addressof(*std::ranges::begin(relocated)) == first);
    CHECK(counted_point::copies == 0);
    CHECK(counted_domain::comparisons == 0);
}

TEST_CASE("canonical intersection bounds comparison work on disjoint inputs", "[datum][cost]")
{
    const auto key = [](unsigned coordinate) {
        return counted_point {std::string(104, 'k') + std::to_string(10000 + coordinate)};
    };
    std::vector<CQ> left, right;
    for (unsigned index = 0; index != 64; ++index) {
        ::ceph::util::push_back(left, CQ::closed_open(key(32 * index), key(32 * index + 8)));
        ::ceph::util::push_back(right, CQ::closed_open(key(32 * index + 24), key(32 * index + 28)));
    }
    const iv::canonical_view a {left}, b {right};
    const auto expression = iv::intersection(a, b);
    const iv::canonical_set<counted_domain> empty;
    const auto comparison_limit = 2 * (std::size(left) + std::size(right));
    const auto check = [](const auto& selected, std::size_t limit) {
        counted_point::copies = 0;
        counted_domain::comparisons = 0;
        bounds_sink observed;
        iv::for_each_interval(selected, observed);
        CHECK(observed.emitted == 0);
        CHECK(counted_domain::comparisons <= limit);
        CHECK(counted_point::copies == 0);
    };
    check(expression, comparison_limit);
    check(iv::set_union(expression, empty), 2 * comparison_limit);
    counted_domain::comparisons = 0;
    CHECK(iv::is_empty_expression(expression));
    CHECK(counted_domain::comparisons <= comparison_limit);
    iv::workspace<counted_domain> scratch {
        {.intermediate_intervals = 0, .work = std::size(left) + std::size(right) - 1}};
    CHECK(std::empty(iv::evaluate(expression, scratch)));
    CHECK(scratch.work_performed() == std::size(left) + std::size(right) - 1);
    CHECK(counted_point::copies == 0);
}

// Datum origin-associated cut views and explicit value retention; no native dependencies.

template <typename T>
struct proof_domain final
{
    using value_type = T;

public:
    static constexpr std::strong_ordering compare(const T& lhs, const T& rhs)
    {
        return lhs <=> rhs;
    }

};

template <typename PointT>
struct proof_row final
{
    PointT first;
    PointT last;

    int id;

public:
    constexpr proof_row(PointT lower, PointT upper, const int number)
        : first(std::move(lower)), last(std::move(upper)), id(number)
    {}
    proof_row(const proof_row&) = delete;
    proof_row(proof_row&&) = delete;

public:
    proof_row& operator=(const proof_row&) = delete;

};

using int_domain = proof_domain<int>;
using int_row = proof_row<int>;
using int_view = iv::cut_view<int_row, int_domain>;
using int_cut = iv::cut<int_row, int_domain>;

static_assert(iv::interval_view<int_view> && iv::interval_view<int_cut>);
static_assert(iv::stable_interval<int_view> && iv::stable_interval<int_cut>);
static_assert(std::copy_constructible<int_view> && std::copy_constructible<int_cut>);
static_assert(!std::default_initializable<int_view> && !std::default_initializable<int_cut>);
static_assert(std::constructible_from<int_cut, int_view>);
static_assert(!std::convertible_to<int_view, int_cut>);
static_assert(std::constructible_from<int_view, const int_cut&>);
static_assert(!std::convertible_to<int_cut&, int_view>);
static_assert(!std::constructible_from<int_view, int_cut&&>);
static_assert(!std::constructible_from<int_view, const int_cut&&>);
static_assert(!std::constructible_from<int_view, int_row&&, iv::borrowed_interval<int_domain>>);
static_assert(
    !std::constructible_from<int_view, const int_row&&, iv::borrowed_interval<int_domain>>);
static_assert(!std::copy_constructible<int_row>);
static_assert(sizeof(int_view) == sizeof(iv::borrowed_interval<int_domain>) + sizeof(void *));
static_assert(iv::interval_view<iv::cut_view<const int_row, int_domain>>);

template <typename T>
concept borrows_temporary_bounds = requires(T value) { std::move(value).lower(); };
static_assert(!borrows_temporary_bounds<int_view> && !borrows_temporary_bounds<int_cut>);

constexpr auto borrowed_row = []<typename T>(const proof_row<T>& row) {
    using domain = proof_domain<T>;

    return iv::borrowed_interval<domain> {iv::boundary_ref<domain>::closed(row.first),
                                          iv::boundary_ref<domain>::open(row.last)};
};

constexpr auto owned_row = []<typename T>(const proof_row<T>& row) {
    return iv::query<proof_domain<T>>::closed_open(row.first, row.last);
};

constexpr bool constexpr_retention()
{
    using Q = iv::query<int_domain>;

    const std::array rows {int_row {0, 8, 0}, int_row {12, 20, 1}};
    const std::array wanted {Q::closed_open(2, 6), Q::closed_open(14, 18)};
    const iv::canonical_view selection {wanted};
    const iv::clip_view cuts {rows, selection, owned_row};
    auto current = std::ranges::begin(cuts);
    const iv::cut saved {*current};
    ++current;
    const iv::interval_cut legacy {rows[0], borrowed_row(rows[0])};
    static_assert(std::same_as<decltype(legacy), const int_view>);
    const auto retained = cuts | iv::views::cut | std::ranges::to<std::vector>();
    const iv::cut_view borrowed {saved};
    const auto pure = iv::compile_intervals(iv::set_union(saved, retained[1]));

    return std::addressof(saved.source()) == std::addressof(rows[0]) && iv::contains(saved, 2) &&
           !iv::contains(saved, 6) && iv::contains(borrowed, 5) && std::size(retained) == 2 &&
           iv::contains(retained[1], 17) && std::size(pure) == 2;
}
static_assert(constexpr_retention());

using proof_selection = iv::canonical_set<int_domain>;
using proof_clips = iv::clip_view<std::array<int_row, 2>, proof_selection, decltype(owned_row)>;
using proof_iterator = std::ranges::iterator_t<proof_clips>;
using retained_range = decltype(std::declval<proof_clips&>() | iv::views::cut);

static_assert(std::input_iterator<proof_iterator>);
static_assert(std::indirectly_readable<proof_iterator>);
static_assert(!std::forward_iterator<proof_iterator>);
static_assert(std::same_as<std::iter_value_t<proof_iterator>, int_view>);
static_assert(std::same_as<std::iter_reference_t<proof_iterator>, int_view>);
static_assert(std::same_as<std::iter_rvalue_reference_t<proof_iterator>, int_view>);
static_assert(std::same_as<std::common_reference_t<int_view&&, int_view&>, const int_view&>);
static_assert(std::same_as<std::ranges::range_value_t<retained_range>, int_cut>);
static_assert(std::same_as<std::ranges::range_reference_t<retained_range>, int_cut>);
static_assert(std::same_as<decltype(std::ranges::to<std::vector>(std::declval<proof_clips&>())),
                           std::vector<int_view>>);
static_assert(std::same_as<decltype(std::ranges::to<std::vector>(std::declval<retained_range&>())),
                           std::vector<int_cut>>);

struct tracked_point final
{
    int value;
    int provenance = 0;
    inline static int copies = 0;
    inline static int moves = 0;
    inline static int copies_before_throw = -1;

public:
    tracked_point() = delete;
    constexpr explicit tracked_point(const int coordinate, const int origin = 0)
        : value(coordinate), provenance(origin)
    {}
    constexpr tracked_point(const tracked_point& point)
        : value(point.value), provenance(point.provenance)
    {
        if (!std::is_constant_evaluated()) {
            ++copies;
            if (0 == copies_before_throw) {
                throw std::runtime_error {"endpoint copy failed"};
            }
            if (0 < copies_before_throw) {
                --copies_before_throw;
            }
        }
    }

    constexpr tracked_point(tracked_point&& point) noexcept
        : value(point.value), provenance(point.provenance)
    {
        if (!std::is_constant_evaluated()) {
            ++moves;
        }
    }

public:
    tracked_point& operator=(const tracked_point&) = delete;
    constexpr std::strong_ordering operator<=>(const tracked_point& rhs) const
    {
        return value <=> rhs.value;
    }

};

struct copy_only_point final
{
    int value;
    inline static int copies = 0;

public:
    copy_only_point() = delete;
    constexpr explicit copy_only_point(const int coordinate) : value(coordinate)
    {}
    constexpr copy_only_point(const copy_only_point& other) noexcept : value(other.value)
    {
        if (!std::is_constant_evaluated()) {
            ++copies;
        }
    }

public:
    copy_only_point& operator=(const copy_only_point&) = delete;
    constexpr std::strong_ordering operator<=>(const copy_only_point& rhs) const
    {
        return value <=> rhs.value;
    }

};

static_assert(iv::ordered_domain<proof_domain<tracked_point>>);
static_assert(iv::ordered_domain<proof_domain<copy_only_point>>);
static_assert(!std::default_initializable<tracked_point>);
static_assert(!std::is_copy_assignable_v<tracked_point>);
static_assert(
    !std::is_move_assignable_v<iv::cut<proof_row<tracked_point>, proof_domain<tracked_point>>>);

TEST_CASE("explicit retention works in constant evaluation") { CHECK(constexpr_retention()); }

TEST_CASE("retained strings survive geometry, iterator, view and selection destruction")
{
    using domain = proof_domain<std::string>;
    using row = proof_row<std::string>;
    using Q = iv::query<domain>;

    for (const std::size_t width : {3U, 256U}) {
        const std::array rows {row {std::string(width, 'a'), std::string(width, 'm'), 0},
                               row {std::string(width, 'n'), std::string(width, 'z'), 1}};
        std::vector<iv::cut<row, domain>> saved;
        {
            const std::array wanted {
                Q::closed_open(std::string(width, 'b'), std::string(width, 'f')),
                Q::closed_open(std::string(width, 'h'), std::string(width, 'q')),
                Q::closed_open(std::string(width, 't'), std::string(width, 'y'))};
            const iv::canonical_view selection {wanted};
            const iv::clip_view cuts {rows, selection, owned_row};
            saved = cuts | iv::views::cut | std::ranges::to<std::vector>();
        }
        REQUIRE(std::size(saved) == 4);
        CHECK(saved[0].lower().key() == std::string(width, 'b'));
        CHECK(saved[1].upper().key() == std::string(width, 'm'));
        CHECK(saved[2].lower().key() == std::string(width, 'n'));
        CHECK(saved[3].upper().key() == std::string(width, 'y'));
        CHECK(std::addressof(saved[0].source()) == std::addressof(rows[0]));
        CHECK(std::addressof(saved[3].source()) == std::addressof(rows[1]));
        auto moved = std::move(saved);
        auto copied = moved;
        const iv::cut_view selected {copied[1]};
        CHECK(iv::contains(selected, std::string(width, 'i')));
    }
}

TEST_CASE("copying a cut view preserves its borrow without copying endpoints")
{
    using domain = proof_domain<tracked_point>;

    const proof_row row {tracked_point {0}, tracked_point {8}, 0};
    const iv::cut_view view {row, borrowed_row(row)};
    tracked_point::copies = 0;
    tracked_point::moves = 0;
    const auto copied = view;
    CHECK(tracked_point::copies == 0);
    CHECK(tracked_point::moves == 0);
    CHECK(std::addressof(copied.lower().key()) == std::addressof(row.first));
    const iv::cut retained {view};
    CHECK(tracked_point::copies == 2);
    CHECK(tracked_point::moves == 0);
    CHECK(std::addressof(retained.lower().key()) != std::addressof(row.first));
    CHECK(iv::contains(retained, tracked_point {4}));
    const iv::cut_view borrowed {retained};
    CHECK(std::addressof(borrowed.lower().key()) == std::addressof(retained.lower().key()));
    STATIC_REQUIRE(iv::interval_for<decltype(retained), domain>);
}

TEST_CASE("range retention copies each finite endpoint once and never copies a row")
{
    using point = tracked_point;
    using domain = proof_domain<point>;
    using row = proof_row<point>;
    using Q = iv::query<domain>;

    const std::array rows {row {point {0}, point {8}, 0}, row {point {12}, point {20}, 1}};
    const std::array wanted {Q::closed_open(point {1}, point {3}),
                             Q::closed_open(point {5}, point {7}),
                             Q::closed_open(point {13}, point {19})};
    const iv::canonical_view selection {wanted};
    const iv::clip_view cuts {rows, selection, borrowed_row};
    point::copies = 0;
    const auto saved = cuts | iv::views::cut | std::ranges::to<std::vector>();
    REQUIRE(std::size(saved) == 3);
    CHECK(point::copies == 6);
    CHECK(std::addressof(saved[0].source()) == std::addressof(rows[0]));
    CHECK(std::addressof(saved[1].source()) == std::addressof(rows[0]));
    CHECK(std::addressof(saved[2].source()) == std::addressof(rows[1]));
    // A range of values copies when observed as lvalues, and moves with as_rvalue.
    point::copies = 0;
    const auto copied = saved | iv::views::cut | std::ranges::to<std::vector>();
    CHECK(point::copies == 6);
    auto values = copied;
    point::copies = 0;
    const auto moved =
        values | std::views::as_rvalue | iv::views::cut | std::ranges::to<std::vector>();
    CHECK(point::copies == 0);
    CHECK(iv::contains(moved[2], point {17}));
}

TEST_CASE("retention keeps string_view backing-storage semantics")
{
    using domain = proof_domain<std::string_view>;

    const std::string lower = "abc";
    const std::string upper = "xyz";
    const proof_row row {std::string_view {lower}, std::string_view {upper}, 0};
    const iv::cut_view view {row, borrowed_row(row)};
    const iv::cut retained {view};
    CHECK(std::data(retained.lower().key()) == std::data(lower));
    CHECK(std::data(retained.upper().key()) == std::data(upper));
    STATIC_REQUIRE(iv::interval_for<decltype(retained), domain>);
}

TEST_CASE("retention preserves infinities, inclusion and left-side tie provenance")
{
    using domain = proof_domain<tracked_point>;
    using B = iv::boundary_ref<domain>;

    const proof_row row {tracked_point {0, 101}, tracked_point {8, 102}, 0};
    const iv::cut_view full {
        row, iv::borrowed_interval<domain> {B::negative_infinity(), B::positive_infinity()}};
    tracked_point::copies = 0;
    const iv::cut retained {full};
    CHECK(tracked_point::copies == 0);
    CHECK(iv::contains(retained, tracked_point {-100}));
    CHECK(iv::contains(retained, tracked_point {100}));
    using Q = iv::query<domain>;

    const std::array rows {proof_row {tracked_point {0, 201}, tracked_point {8, 202}, 0}};
    const std::array wanted {Q::closed_open(tracked_point {0, 301}, tracked_point {8, 302})};
    const iv::canonical_view selection {wanted};
    const iv::clip_view cuts {rows, selection, borrowed_row};
    auto current = std::ranges::begin(cuts);
    const iv::cut selected {*current};
    CHECK(selected.lower().key().provenance == 201);
    CHECK(selected.upper().key().provenance == 202);
    CHECK(iv::contains(selected, tracked_point {0}));
    CHECK_FALSE(iv::contains(selected, tracked_point {8}));
}

TEST_CASE("an endpoint copy failure neither advances nor changes the source view")
{
    using domain = proof_domain<tracked_point>;
    using Q = iv::query<domain>;

    const std::array rows {proof_row {tracked_point {0}, tracked_point {8}, 0}};
    const std::array wanted {Q::closed_open(tracked_point {2}, tracked_point {6})};
    const iv::canonical_view selection {wanted};
    const iv::clip_view cuts {rows, selection, borrowed_row};
    auto current = std::ranges::begin(cuts);
    tracked_point::copies_before_throw = 1;
    CHECK_THROWS_AS(iv::cut {*current}, std::runtime_error);
    tracked_point::copies_before_throw = -1;
    CHECK(iv::contains(*current, tracked_point {4}));
    const iv::cut retained {*current};
    CHECK(iv::contains(retained, tracked_point {4}));
    ++current;
    CHECK(current == std::default_sentinel);
}

TEST_CASE("copy-only retention copies each endpoint once before collection")
{
    const proof_row row {copy_only_point {0}, copy_only_point {8}, 0};
    const iv::cut_view view {row, borrowed_row(row)};
    copy_only_point::copies = 0;
    const iv::cut retained {view};
    INFO("copy-only endpoint copies: " << copy_only_point::copies);
    CHECK(copy_only_point::copies == 2);

    CHECK(iv::contains(retained, copy_only_point {4}));
    std::vector<std::remove_cvref_t<decltype(retained)>> saved;
    ::ceph::util::maybe_reserve(saved, 1);
    copy_only_point::copies = 0;
    ::ceph::util::emplace_back(saved, view);
    const std::array views {view};
    CHECK(copy_only_point::copies == 2);

    copy_only_point::copies = 0;
    const auto collected = views | iv::views::cut | std::ranges::to<std::vector>();
    // Standard collection moves the materialized element; a copy-only point
    // implements those moves by copying. Explicit emplace avoids that hop.
    CHECK(copy_only_point::copies == 4);

    CHECK(iv::contains(collected[0], copy_only_point {4}));
}

TEST_CASE("materialization preserves the foreign accessor observation order")
{
    struct traced_bound final
    {
        using domain_type = int_domain;

        iv::boundary_ref<int_domain> value;
        std::string& calls;

    public:
        const int& key() const
        {
            calls += 'K';

            return value.key();
        }

    public:
        iv::boundary_kind kind() const
        {
            calls += 'C';

            return value.kind();
        }

    public:
        bool finite() const
        {
            calls += 'F';

            return value.finite();
        }

    public:
        bool inclusive() const
        {
            calls += 'I';

            return value.inclusive();
        }

    public:
        bool below_all() const
        {
            calls += 'L';

            return value.below_all();
        }

    public:
        bool above_all() const
        {
            calls += 'U';

            return value.above_all();
        }

    };
    const int coordinate = 4;
    for (const auto value : {iv::boundary_ref<int_domain>::negative_infinity(),
                             iv::boundary_ref<int_domain>::closed(coordinate),
                             iv::boundary_ref<int_domain>::positive_infinity()}) {
        std::string calls;
        const traced_bound view {value, calls};
        const auto retained = iv::materialize_boundary(view);
        CHECK(calls == (value.below_all() ? "L" : value.above_all() ? "LU" : "LUIK"));
        CHECK(retained.kind() == value.kind());
        ::ceph::util::clear(calls);
        const auto overridden = iv::materialize_boundary(view, false);
        CHECK(calls == (value.below_all() ? "L" : value.above_all() ? "LU" : "LUK"));
        CHECK_FALSE(overridden.inclusive());
    }
}

TEST_CASE("retained range geometry matches an independent membership oracle")
{
    using Q = iv::query<int_domain>;

    struct interval_row final
    {
        Q geometry;

        int id;
    };
    for (unsigned mask = 0; mask != 16; ++mask) {
        const auto bound = [](const int point, const bool included) {
            return included ? iv::boundary<int_domain>::closed(point)
                            : iv::boundary<int_domain>::open(point);
        };
        const std::array rows {
            interval_row {Q::between(bound(-2, mask & 1), bound(0, mask & 2)), 0},
            interval_row {Q::between(bound(2, mask & 4), bound(4, mask & 8)), 1}};
        for (int lower = -3; lower <= 5; ++lower) {
            for (int upper = lower; upper <= 5; ++upper) {
                for (unsigned selection_mask = 0; selection_mask != 4; ++selection_mask) {
                    const iv::canonical_set selection {Q::between(
                        bound(lower, selection_mask & 1), bound(upper, selection_mask & 2))};
                    const iv::clip_view cuts {rows, selection,
                                              [](const interval_row& row) { return row.geometry; }};
                    const auto saved = cuts | iv::views::cut | std::ranges::to<std::vector>();
                    for (int point = -4; point <= 6; ++point) {
                        for (int origin = 0; origin != 2; ++origin) {
                            const int first = 0 == origin ? -2 : 2;
                            const int last = 0 == origin ? 0 : 4;
                            const unsigned offset = 0 == origin ? 0 : 2;
                            const bool in_row =
                                (first < point || (first == point && (mask & (1U << offset)))) &&
                                (point < last || (last == point && (mask & (2U << offset))));
                            const bool in_selection =
                                (lower < point || (lower == point && (selection_mask & 1))) &&
                                (point < upper || (upper == point && (selection_mask & 2)));
                            const bool actual =
                                std::ranges::any_of(saved, [origin, point](const auto& selected) {
                                    return selected.source().id == origin &&
                                           iv::contains(selected, point);
                                });
                            CHECK(actual == (in_row && in_selection));
                        }
                    }
                }
            }
        }
    }
}

TEST_CASE("native query retention constructs nonassignable endpoints directly")
{
    using domain = proof_domain<copy_only_point>;
    using Q = iv::query<domain>;
    using Ref = iv::boundary_ref<domain>;

    const copy_only_point first {0};
    const copy_only_point last {8};
    copy_only_point::copies = 0;
    const auto endpoint = iv::materialize_boundary(Ref::closed(first), false);
    CHECK(copy_only_point::copies == 1);
    CHECK_FALSE(endpoint.inclusive());
    CHECK(std::addressof(endpoint.key()) != std::addressof(first));
    copy_only_point::copies = 0;
    const auto infinity = iv::materialize_boundary(Ref::negative_infinity(), true);
    CHECK(copy_only_point::copies == 0);
    CHECK(infinity.below_all());
    CHECK_FALSE(infinity.inclusive());

    copy_only_point::copies = 0;
    const auto retained = Q::between(Ref::closed(first), Ref::open(last));
    CHECK(copy_only_point::copies == 2);
    CHECK(iv::contains(retained, copy_only_point {4}));
    CHECK_FALSE(iv::contains(retained, last));

    copy_only_point::copies = 0;
    const auto copied = iv::copy_interval(retained);
    CHECK(copy_only_point::copies == 2);
    CHECK(copied == retained);

    copy_only_point::copies = 0;
    const auto finite = Q::closed_open(first, last);
    // Two argument copies and one consuming copy into each endpoint.
    CHECK(copy_only_point::copies == 4);
    CHECK(finite == retained);

    copy_only_point::copies = 0;
    const auto unbounded = Q::between(Ref::closed(first), Ref::positive_infinity());
    CHECK(copy_only_point::copies == 1);
    CHECK(unbounded.upper().above_all());
    CHECK_FALSE(unbounded.upper().inclusive());

    const auto implicit_empty = Q::between(Ref::closed(first), Ref::open(first));
    copy_only_point::copies = 0;
    const auto empty = iv::copy_interval(implicit_empty);
    CHECK(copy_only_point::copies == 0);
    CHECK(empty.explicitly_empty());
    CHECK(empty.lower().below_all());
    CHECK(empty.upper().above_all());
}

TEST_CASE("native query retention leaves borrowed input usable after a copy failure")
{
    using domain = proof_domain<tracked_point>;
    using Q = iv::query<domain>;
    using Ref = iv::boundary_ref<domain>;

    const tracked_point first {0};
    const tracked_point last {8};
    const iv::borrowed_interval bounds {Ref::closed(first), Ref::open(last)};
    tracked_point::copies_before_throw = 1;
    CHECK_THROWS_AS(Q::between(bounds.lower(), bounds.upper()), std::runtime_error);
    tracked_point::copies_before_throw = -1;
    const auto retained = Q::between(bounds.lower(), bounds.upper());
    CHECK(iv::contains(retained, tracked_point {4}));
    CHECK(bounds.lower().key().value == 0);
    CHECK(bounds.upper().key().value == 8);
}

// Datum traversal tests: generic cancellation, failures and move-only progress.
// No backend or codec is required.

namespace {

namespace op = ceph::salad::datum::traversal;

struct test_page final
{
    std::optional<int> next;
    int value = 0;
};

using page_result = std::expected<test_page, std::string>;

constexpr auto next_page = [](const int&, const test_page& page) { return page.next; };

} // anonymous namespace

TEST_CASE("Datum paged traversal visits a complete sequence")
{
    std::vector<int> visited;
    auto reads = std::size_t {0};
    const auto fetch_page = [&reads](const int& cursor) -> page_result {
        ++reads;

        return test_page {.next = 2 == cursor ? std::nullopt : std::optional {1 + cursor},
                          .value = 10 + cursor};
    };
    const auto result = op::for_each_page(0, fetch_page, next_page, [&visited](test_page page) {
        ::ceph::util::push_back(visited, page.value);
    });

    REQUIRE(result);
    CHECK(result->complete());
    CHECK(op::page_traversal_outcome::complete == result->outcome);
    CHECK_FALSE(result->resume);
    CHECK(3 == result->pages);
    CHECK(3 == reads);
    CHECK(std::vector {10, 11, 12} == visited);
}

TEST_CASE("Datum paged traversal supports throwing readers directly")
{
    std::vector<int> visited;
    const auto result = op::for_each_page(
        0,
        [](const int& cursor) {
            return test_page {.next = 1 == cursor ? std::nullopt : std::optional {1 + cursor},
                              .value = cursor};
        },
        next_page, [&visited](test_page page) { ::ceph::util::push_back(visited, page.value); });

    static_assert(
        std::same_as<std::remove_cvref_t<decltype(result)>, op::page_traversal_result<int>>);

    CHECK(result.complete());
    CHECK(2 == result.pages);
    CHECK(std::vector {0, 1} == visited);

    CHECK_THROWS_AS(op::for_each_page(
                        0,
                        [](const int&) -> test_page { throw std::runtime_error {"read failed"}; },
                        next_page, [](test_page) {}),
                    std::runtime_error);
}

TEST_CASE("Datum throwing traversal unwinds pages and stops before another effect")
{
    auto reads = 0;
    auto destroyed = 0;
    std::vector<int> visited;
    const auto destroy = [&destroyed](int *value) {
        ++destroyed;
        delete value;
    };
    struct owned_page final
    {
        std::unique_ptr<int, decltype(destroy)> value;
        std::optional<int> next;
    };
    const auto fetch = [&](const int& cursor) {
        ++reads;
        if (1 == cursor) {
            throw std::system_error {std::make_error_code(std::errc::io_error), "read failed"};
        }

        return owned_page {.value = {new int {cursor}, destroy}, .next = 1 + cursor};
    };
    const auto next = [](const int&, const owned_page& page) { return page.next; };

    SECTION("reader exception follows one published page")
    {
        CHECK_THROWS_AS(op::for_each_page(0, fetch, next,
                                          [&](owned_page page) {
                                              ::ceph::util::push_back(visited, *page.value);
                                          }),
                        std::system_error);
        CHECK(2 == reads);
        CHECK(1 == destroyed);
        CHECK(std::vector {0} == visited);
    }
    SECTION("visitor exception releases the current page")
    {
        CHECK_THROWS_AS(
            op::for_each_page(0, fetch, next,
                              [](owned_page) { throw std::runtime_error {"visitor failed"}; }),
            std::runtime_error);
        CHECK(1 == reads);
        CHECK(1 == destroyed);
    }
    SECTION("page limit returns the next state without reading it")
    {
        const auto result = op::for_each_page(0, fetch, next, [](owned_page) {},
                                              {.cancellation = {}, .page_limit = 1});
        CHECK(op::page_traversal_outcome::limit_reached == result.outcome);
        CHECK(1 == result.pages);
        CHECK(std::optional {1} == result.resume);
        CHECK(1 == reads);
        CHECK(1 == destroyed);
    }
    SECTION("cancellation after a page returns the next state")
    {
        std::stop_source cancellation;
        const auto result = op::for_each_page(
            0, fetch, next, [&](owned_page) { cancellation.request_stop(); },
            {.cancellation = cancellation.get_token(), .page_limit = std::nullopt});
        CHECK(op::page_traversal_outcome::cancelled == result.outcome);
        CHECK(1 == result.pages);
        CHECK(std::optional {1} == result.resume);
        CHECK(1 == reads);
        CHECK(1 == destroyed);
    }
}

TEST_CASE("Datum paged traversal preserves progress on read failure")
{
    std::vector<int> visited;
    const auto fetch_page = [](const int& cursor) -> page_result {
        if (2 == cursor) {
            return std::unexpected(std::string {"read failed"});
        }

        return test_page {.next = 1 + cursor, .value = cursor};
    };
    const auto result = op::for_each_page(0, fetch_page, next_page, [&visited](test_page page) {
        ::ceph::util::push_back(visited, page.value);
    });

    REQUIRE_FALSE(result);
    CHECK("read failed" == result.error().error);
    CHECK(2 == result.error().resume);
    CHECK(2 == result.error().pages);
    CHECK(std::vector {0, 1} == visited);
}

TEST_CASE("Datum paged traversal obeys a page limit without reading ahead")
{
    auto reads = std::size_t {0};
    const auto fetch_page = [&reads](const int& cursor) -> page_result {
        ++reads;

        return test_page {.next = 1 + cursor, .value = cursor};
    };
    const auto result = op::for_each_page(4, fetch_page, next_page, [](test_page) {},
                                          {.cancellation = {}, .page_limit = 2});

    REQUIRE(result);
    CHECK(op::page_traversal_outcome::limit_reached == result->outcome);
    REQUIRE(result->resume);
    CHECK(6 == *result->resume);
    CHECK(2 == result->pages);
    CHECK(2 == reads);
}

TEST_CASE("SALAD zero-page traversal does not call its source")
{
    auto reads = std::size_t {0};
    const auto fetch_page = [&reads](const int& cursor) -> page_result {
        ++reads;

        return test_page {.next = 1 + cursor};
    };
    const auto result = op::for_each_page(7, fetch_page, next_page, [](test_page) {},
                                          {.cancellation = {}, .page_limit = 0});

    REQUIRE(result);
    REQUIRE(result->resume);
    CHECK(7 == *result->resume);
    CHECK(0 == result->pages);
    CHECK(0 == reads);
}

TEST_CASE("Datum paged traversal returns the next state when stopped")
{
    const auto fetch_page = [](const int& cursor) -> page_result {
        return test_page {.next = 1 + cursor, .value = cursor};
    };
    const auto result = op::for_each_page(0, fetch_page, next_page, [](const test_page& page) {
        return 1 == page.value ? op::page_action::stop : op::page_action::next;
    });

    REQUIRE(result);
    CHECK(op::page_traversal_outcome::stopped == result->outcome);
    REQUIRE(result->resume);
    CHECK(2 == *result->resume);
    CHECK(2 == result->pages);
}

TEST_CASE("SALAD terminal page completion wins over a stop request")
{
    const auto result = op::for_each_page(
        0, [](const int&) -> page_result { return test_page {.next = std::nullopt, .value = 1}; },
        next_page, [](test_page) { return op::page_action::stop; });

    REQUIRE(result);
    CHECK(result->complete());
    CHECK_FALSE(result->resume);
    CHECK(1 == result->pages);
}

TEST_CASE("Datum paged traversal cancellation is resumable")
{
    std::stop_source stop;
    auto reads = std::size_t {0};
    const auto fetch_page = [&reads](const int& cursor) -> page_result {
        ++reads;

        return test_page {.next = 1 + cursor, .value = cursor};
    };
    const auto result =
        op::for_each_page(0, fetch_page, next_page,
                          [&stop](const test_page& page) {
                              if (1 == page.value) {
                                  stop.request_stop();
                              }
                          },
                          {.cancellation = stop.get_token(), .page_limit = std::nullopt});

    REQUIRE(result);
    CHECK(op::page_traversal_outcome::cancelled == result->outcome);
    REQUIRE(result->resume);
    CHECK(2 == *result->resume);
    CHECK(2 == result->pages);
    CHECK(2 == reads);
}

TEST_CASE("SALAD pre-cancelled traversal never calls its source")
{
    std::stop_source stop;
    stop.request_stop();
    auto reads = std::size_t {0};
    const auto fetch_page = [&reads](const int&) -> page_result {
        ++reads;

        return test_page {};
    };
    const auto result =
        op::for_each_page(3, fetch_page, next_page, [](test_page) {},
                          {.cancellation = stop.get_token(), .page_limit = std::nullopt});

    REQUIRE(result);
    REQUIRE(result->resume);
    CHECK(3 == *result->resume);
    CHECK(0 == result->pages);
    CHECK(0 == reads);
}

TEST_CASE("Datum paged traversal rejects an unchanged state before visiting")
{
    auto visits = std::size_t {0};

    CHECK_THROWS_AS(
        op::for_each_page(
            5, [](const int&) -> page_result { return test_page {.next = 5, .value = 42}; },
            next_page, [&visits](test_page) { ++visits; }),
        std::logic_error);
    CHECK(0 == visits);
}

TEST_CASE("Datum paged traversal transfers move-only page contents")
{
    struct move_only_page final
    {
        std::unique_ptr<int> value;
        bool complete = false;
    };

    using move_only_result = std::expected<move_only_page, int>;

    std::vector<int> visited;
    const auto result = op::for_each_page(
        0,
        [](const int& cursor) -> move_only_result {
            return move_only_page {.value = std::make_unique<int>(cursor), .complete = 1 == cursor};
        },
        [](const int& cursor, const move_only_page& page) {
            return page.complete ? std::nullopt : std::optional {1 + cursor};
        },
        [&visited](move_only_page page) { ::ceph::util::push_back(visited, *page.value); });

    static_assert(
        std::same_as<
            std::remove_cvref_t<decltype(result)>,
            std::expected<op::page_traversal_result<int>, op::page_traversal_failure<int, int>>>);

    REQUIRE(result);
    CHECK(std::vector {0, 1} == visited);
}

TEST_CASE("Datum paged traversal does not copy its state")
{
    struct move_only_state final
    {
        std::unique_ptr<int> value;

    public:
        bool operator==(const move_only_state& rhs) const { return *value == *rhs.value; }

    };

    const auto result = op::for_each_page(
        move_only_state {std::make_unique<int>(0)},
        [](const move_only_state& cursor) -> page_result {
            return test_page {.next = 1 == *cursor.value ? std::nullopt : std::optional {1},
                              .value = *cursor.value};
        },
        [](const move_only_state&, const test_page& page) {
            return page.next ? std::optional {move_only_state {std::make_unique<int>(*page.next)}}
                             : std::nullopt;
        },
        [](test_page) {});

    REQUIRE(result);
    CHECK(result->complete());
    CHECK(2 == result->pages);
}

// Trait adaptation must reject void page sources without forming void references.
static_assert(
    !datum::traversal::page_traversal_detail::page_reader<decltype([](const int&) {}), int>);
static_assert(!datum::traversal::page_traversal_detail::page_reader<
              decltype([](const int&) { return std::expected<void, std::error_code> {}; }), int>);
