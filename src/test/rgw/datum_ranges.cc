// Datum interval-range projections, ownership and exact scan continuation tests.

#include <datum/traversal/scan.h>
#include <datum/compat/detail/interval_projection.h>
#include <datum/range.h>
#include <datum/bytes.h>
#include <datum/key.h>

#include "common/container_concepts.h"
#include "datum_portable_support.h"

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <version>

#include <expected>
#include <stdexcept>
#include <utility>

#include <array>
#include <list>
#include <span>
#include <vector>

#include <memory>
#include <memory_resource>

#include <functional>
#include <initializer_list>
#include <iterator>
#include <ranges>

#include <sstream>
#include <string>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

namespace {

// Keep the small scan fixture private; the reflected data suite has its own record profile.
struct mechanics_row
{
    std::string key;
    std::vector<unsigned char> payload;
};

} // anonymous namespace

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
    CHECK(partial.progress.consumed() == 2);
    CHECK(partial.progress.accepted == 1);
    CHECK(partial.progress.stopped());
    CHECK(partial.progress.stop == datum::acceptance::reject_and_stop);
    REQUIRE(partial.resume);
    CHECK(partial.resume->key() == 3);
    CHECK(partial.resume->inclusive());
    const auto finished =
        datum::traversal::scan_range(rows, [](int) { return datum::acceptance::skip; }, positions);
    CHECK(finished.progress.consumed() == 4);
    CHECK(finished.progress.accepted == 0);
    CHECK_FALSE(finished.resume);
    CHECK_FALSE(finished.progress.stop);

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
    CHECK(opaque.progress.stop == datum::acceptance::accept_and_stop);

    const auto empty = datum::visit_range(std::span<const int> {}, [](int) {
        return datum::acceptance::reject_and_stop;
    });
    CHECK(empty.consumed() == 0);
    CHECK_FALSE(empty.stopped());
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
    requires std::same_as<std::remove_cvref_t<R>, record> and std::is_lvalue_reference_v<R&&>
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
    static constexpr std::strong_ordering compare(const immutable_point& a, const immutable_point& b)
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

// Only domain identity, position, key, and inclusion belong to the boundary protocol.
// Predicates derive from kind; intervals need no separate empty-state customization.
struct protocol_bound
{
    using domain_type = image_bytes;

    iv::boundary<image_bytes> value;

public:
    auto kind() const { return value.kind(); }
    const point& key() const { return value.key(); }
    bool inclusive() const { return value.inclusive(); }
};

struct protocol_interval
{
    using domain_type = image_bytes;

    protocol_bound lo, hi;

public:
    const protocol_bound& lower() const { return lo; }
    const protocol_bound& upper() const { return hi; }
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

#if defined(__cpp_lib_reflection)
 static_assert(ownership::owning_value<owned_record>); // Actual recursive reflection.
#else
 static_assert(!ownership::owning_value<owned_record>); // Complete metadata is not yet available.
#endif
static_assert(!ownership::owning_value<borrowed_record>);
static_assert(!ownership::owning_value<std::string_view>);
static_assert(ownership::owning_value<Q>);
static_assert(ownership::owning_value<std::vector<Q>>);
#if defined(__cpp_lib_inplace_vector)
 static_assert(ownership::owning_value<std::inplace_vector<Q, 2>>);
#endif
static_assert(ownership::owning_value<iv::interval_batch<image_bytes, 2>>);
static_assert(!ownership::owning_value<BQ>);
#if defined(__cpp_lib_inplace_vector)
 static_assert(!ownership::owning_value<std::inplace_vector<BQ, 2>>);
#endif
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
using stream_view = decltype(iv::intervals_view {std::declval<std::ranges::istream_view<numbers>>(), numbers_lens {}});
using batch = iv::interval_batch<image_bytes, 8>;

template <typename RangeT>
concept has_member_begin = requires { std::declval<RangeT>().begin(); };

template <typename RangeT>
concept has_member_end = requires { std::declval<RangeT>().end(); };

template <typename RangeT>
concept has_member_size = requires { std::declval<RangeT>().size(); };

static_assert(has_member_begin<view&> and has_member_begin<const view&>);
static_assert(has_member_end<view&> and has_member_end<const view&>);
static_assert(has_member_size<view&> and has_member_size<const view&>);
// Direct iteration must not leave an iterator borrowing a destroyed lens.
static_assert(!has_member_begin<view&&> and !has_member_begin<const view&&>);
static_assert(!has_member_end<view&&> and !has_member_end<const view&&>);
// Preserve the existing value-returning size interface independently of iteration.
static_assert(!has_member_size<view&&> and has_member_size<const view&&>);
static_assert(has_member_begin<category_view&> and !has_member_begin<const category_view&>);
static_assert(has_member_end<category_view&> and !has_member_end<const category_view&>);
static_assert(has_member_size<category_view&> and !has_member_size<const category_view&>);
static_assert(has_member_begin<stream_view&> and !has_member_begin<const stream_view&>);
static_assert(has_member_end<stream_view&> and !has_member_end<const stream_view&>);
static_assert(!has_member_size<stream_view&> and !has_member_size<const stream_view&>);

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
    check(iv::contains(first, point {10}) and !iv::contains(first, point {20}),
          "native borrowed view endpoints");
    batch owned {geometry};
    source[0].lower = point {12};
    check((*std::begin(geometry)).lower().key().value == 12, "lazy source observation");
    check(owned[0].lower().key().value == 10, "explicit owning copy independent of source");
    check(source[0].payload and *source[0].payload == "payload", "payload untouched");

    auto own_source = iv::intervals_view {make_records(), record_lens {}};
    check((*std::begin(own_source)).upper().key().value == 20,
          "owning rvalue source survives full expression");
    auto moved = std::move(own_source);
    check(std::size(moved) == 2 and iv::contains(*std::begin(moved), point {15}),
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
    check(std::size(copied) == 3 and iv::contains(copied[0], point {1}),
          "rvalue-only proxy projection");
    check(iv::is_empty(copied[1]) and iv::contains(copied[2], point {1}),
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
    check(std::size(copied) == 3 and copied[2].lower().key().value == 2,
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

    std::array cases {protocol_interval {{B::closed(point {2})}, {B::open(point {7})}},
                      protocol_interval {{B::closed(point {5})}, {B::closed(point {5})}},
                      protocol_interval {{B::open(point {5})}, {B::closed(point {5})}},
                      protocol_interval {{B::positive_infinity()}, {B::negative_infinity()}},
                      protocol_interval {{B::negative_infinity()}, {B::positive_infinity()}},
                      protocol_interval {{B::open(point {2})}, {B::positive_infinity()}},
                      protocol_interval {{B::closed(point {7})}, {B::open(point {2})}}};
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
    check(iv::contains(copied[0], point {2}) and !iv::contains(copied[0], point {7}),
          "closed/open retained");
    check(iv::contains(copied[1], point {5}), "equal closed bounds are singleton");
    check(iv::is_empty(copied[2]) and iv::is_empty(copied[3]),
          "finite and infinite empty bounds retained");
    check(iv::contains(copied[4], point {0}) and iv::contains(copied[4], point {999}),
          "infinite bounds copied safely");
    check(!iv::contains(copied[5], point {2}) and iv::contains(copied[5], point {3}),
          "open lower bound retained");
    check(iv::is_empty(copied[6]), "reversed bounds are empty");
    auto overlap = iv::intersection(iv::copy_interval(cases[0]), Q::closed(point {4}, point {9}));
    check(iv::contains(overlap, point {4}) and !iv::contains(overlap, point {7}),
          "actual algebra callable on converted foreign view");
}

void single_pass_and_capacity()
{
    std::istringstream input {"10 5 30 2"};
    iv::intervals_view geometry {std::ranges::istream_view<numbers> {input}, numbers_lens {}};
    batch copied {geometry};
    check(std::size(copied) == 2 and copied[1].upper().key().value == 32,
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
    check(calls == 2 and result.offered == 2 and result.accepted == 1 and result.stopped(),
          "reject isn't acknowledged");
    auto accept_stop = iv::visit_intervals<image_bytes>(
        intervals, [](const Q&) { return iv::acceptance::accept_and_stop; });
    check(accept_stop.offered == 1 and accept_stop.accepted == 1 and accept_stop.stopped(),
          "accepted stop counted once");
    auto all = iv::visit_intervals<image_bytes>(intervals, accepting {});
    check(all.accepted == 3 and !all.stopped(), "complete enumeration");

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
    check(stopped.offered == 1 and stopped.accepted == 0, "single-pass rejected first entry");
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
    check(first.interval.lower().key().value == 12 and first.origin->lower.value == 10,
          "origin survives geometry clip");
    check(first.origin->id == 1 and first.origin->payload.get() == source[0].payload.get(),
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
    check(advanced and advanced->value == 13, "independent checked metric");
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
    check(std::ranges::size(range) == 2 and range.covered_measure() == 100,
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
    check(impure(0) == 1 and impure(0) == 2, "syntax acceptance doesn't prove semantic laws");
    using IQ = iv::query<immutable_offsets>;

    std::array immutable {IQ::closed_open(immutable_point {3}, immutable_point {9})};
    iv::interval_batch<immutable_offsets, 2> constructed {immutable};
    check(std::size(constructed) == 1 and iv::contains(constructed[0], immutable_point {5}),
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
    check(iv::is_empty(keys[0]) and !iv::is_empty(copied[0]),
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
        check(!lifetime.expired() and copied[0].lower().key().value == "retained",
              "audited point copy retains backing owner");
    }
    check(lifetime.expired(), "retained point storage released with batch");
}

constexpr bool constant_evaluation()
{
    std::array source {Q::singleton(point {4}), Q::closed_open(point {10}, point {15})};
    iv::interval_batch<image_bytes, 2> batch {source};
    auto result = iv::visit_intervals(batch, [](const Q&) { return iv::acceptance::accept; });

    return result.accepted == 2 and iv::contains(batch[1], point {12});
}

static_assert(constant_evaluation());
static_assert([] {
    std::array source {Q::closed_open(point {2}, point {8})};
    iv::intervals_view geometry {std::as_const(source), std::identity {}};
    iv::interval_batch<image_bytes, 1> copied {geometry};

    return copied[0] == iv::intersection(*std::ranges::begin(geometry), Q::universal());
}());
static_assert([] {
    std::array source {Q::singleton(point {4}), Q::closed_open(point {10}, point {15})};
    iv::intervals_view geometry {source, std::identity {}};
    const auto& fixed = geometry;

    return geometry.size() == 2 and fixed.size() == 2 and
           geometry.end() - geometry.begin() == 2 and fixed.end() - fixed.begin() == 2 and
           std::addressof(*geometry.begin()) == std::addressof(source[0]) and
           std::addressof(*fixed.begin()) == std::addressof(source[0]);
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
    CHECK(result.stopped());
    CHECK("first accepted" == values[0]);
    CHECK("second" == values[1]);

    const auto by_value =
        visit_range(std::views::iota(0, 3), [](int&&) { return acceptance::accept; });
    CHECK(3 == by_value.accepted);
    CHECK(!by_value.stopped());
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
      iv::canonical_range value {values};
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

namespace {

struct borrowed_positions final
{
    std::string_view position(const int&) const;
    std::string_view next(std::string_view, bool) const;
};

struct reference_positions final
{
    const int& position(const int&) const;
    const int& next(const int&, bool) const;
};

struct mismatched_positions final
{
    int position(const int&) const;
    void next(int, bool) const;
};

template <typename PositionsT>
concept scannable = requires(std::array<int, 1>& rows, PositionsT& positions) {
    datum::traversal::scan_range(
        rows, [](int&) { return datum::acceptance::accept; }, positions);
};

static_assert(!scannable<borrowed_positions>);
static_assert(!scannable<reference_positions>);
static_assert(!scannable<mismatched_positions>);

} // anonymous namespace

TEST_CASE("scan borrows immovable callbacks and retains move-only positions", "[datum][scan]")
{
    struct positions final
    {
        unsigned observations = 0;
        unsigned continuations = 0;

    public:
        positions() = default;
        positions(const positions&) = delete;

    public:
        std::unique_ptr<int> position(const int& row)
        {
            ++observations;

            return std::make_unique<int>(row);
        }

        std::unique_ptr<int> next(std::unique_ptr<int> saved, const bool consumed)
        {
            ++continuations;
            *saved += consumed;

            return saved;
        }

    } policy;

    struct consumer final
    {
        unsigned calls = 0;

    public:
        consumer() = default;
        consumer(const consumer&) = delete;

    public:
        datum::acceptance operator()(int& row)
        {
            row += 10;

            return 2 == ++calls ? datum::acceptance::accept_and_stop : datum::acceptance::accept;
        }

    } sink;

    std::array rows {1, 2, 3};
    const auto result = datum::traversal::scan_range(rows, sink, policy);
    REQUIRE(result.resume);
    CHECK(3 == **result.resume);
    CHECK(2 == result.progress.offered);
    CHECK(2 == result.progress.consumed());
    CHECK(2 == result.progress.accepted);
    CHECK(result.progress.stopped());
    CHECK(2 == policy.observations);
    CHECK(1 == policy.continuations);
    CHECK(2 == sink.calls);
    CHECK(rows == std::array {11, 12, 3});
}

TEST_CASE("scan exceptions preserve position and publication ordering", "[datum][scan]")
{
    std::array rows {7, 8};
    for (const int failure : {1, 2, 3}) {
        std::vector<int> events;
        struct positions final
        {
            std::vector<int>& events;
            int failure;

        public:
            std::unique_ptr<int> position(const int& row)
            {
                events.push_back(1);
                if (1 == failure) {
                    throw std::runtime_error {"position failure"};
                }

                return std::make_unique<int>(row);
            }

            std::unique_ptr<int> next(std::unique_ptr<int> saved, bool)
            {
                events.push_back(3);
                if (3 == failure) {
                    throw std::runtime_error {"continuation failure"};
                }

                return saved;
            }

        } policy {events, failure};

        CHECK_THROWS_AS(datum::traversal::scan_range(rows, [&](int&) {
            events.push_back(2);
            if (2 == failure) {
                throw std::runtime_error {"consumer failure"};
            }

            return datum::acceptance::reject_and_stop;
        }, policy), std::runtime_error);
        const std::vector<int> expected = 1 == failure ? std::vector {1} :
                                         2 == failure ? std::vector {1, 2} : std::vector {1, 2, 3};
        CHECK(events == expected);
        CHECK(rows == std::array {7, 8});
    }
}

TEST_CASE("compat projections preserve iterator and owner capabilities", "[datum][range][compat]")
{
    namespace compatibility = datum::compat::detail;
    const auto lens = [](const int& value) -> const int& { return value; };
    std::vector<int> values {2, 4, 6};
    const int *storage = std::data(values);
    compatibility::const_projection_view projected {std::views::all(std::move(values)), lens};
    using view = decltype(projected);
    using iterator = std::ranges::iterator_t<view>;
    using const_iterator = std::ranges::iterator_t<const view>;
    static_assert(std::ranges::view<view> and std::ranges::random_access_range<const view>);
    static_assert(!std::ranges::contiguous_range<view> and !std::copy_constructible<view>);
    static_assert(!std::ranges::borrowed_range<view>);
    static_assert(std::convertible_to<iterator, const_iterator>);
    static_assert(std::same_as<std::iter_reference_t<iterator>, const int&>);
    auto first = projected.begin();
    const_iterator constant = first;
    CHECK(first == constant);
    CHECK(iterator {} == iterator {});
    CHECK(0 == constant - first);
    CHECK(std::addressof(*first) == storage);
    CHECK(3 == projected.size());
    CHECK(3 == std::as_const(projected).end() - first);
    CHECK(4 == first[1]);
    CHECK(6 == *(2 + first));
    CHECK(4 == *(projected.end() - 2));
    CHECK(first < projected.end());
    CHECK(projected.end() > constant);
    CHECK(first <= constant);
    CHECK(first >= constant);
    CHECK((first <=> constant) == 0);
    auto last = projected.end();
    CHECK(6 == *--last);
    CHECK(6 == *last--);
    CHECK(4 == *last);
    CHECK(2 == *first++);
    CHECK(4 == *first);
    first -= 1;
    first += 2;
    CHECK(6 == *first);
    auto moved = std::move(projected);
    CHECK(std::addressof(*moved.begin()) == storage);

    std::array numbers {7, 8, 9};
    auto counted = std::ranges::subrange {std::counted_iterator {std::data(numbers), 3},
                                         std::default_sentinel};
    compatibility::const_projection_view limited {counted, lens};
    static_assert(!std::ranges::common_range<decltype(limited)>);
    static_assert(std::ranges::sized_range<decltype(limited)>);
    auto current = limited.begin();
    auto finish = std::as_const(limited).end();
    decltype(finish) converted_finish = limited.end();
    CHECK(3 == finish - current);
    CHECK(-3 == current - finish);
    CHECK(9 == *(current + 2));
    current += 3;
    CHECK(current == finish);
    CHECK(current == converted_finish);
    CHECK(3 == limited.size());

    std::list bidirectional {10, 20};
    compatibility::const_projection_view linked {std::views::all(bidirectional), lens};
    static_assert(std::ranges::bidirectional_range<decltype(linked)>);
    static_assert(!std::ranges::random_access_range<decltype(linked)>);
    auto reverse = linked.end();
    CHECK(20 == *--reverse);
    CHECK(20 == *reverse--);
    CHECK(10 == *reverse);

    const auto offset = [amount = 2](const int value) { return value + amount; };
    compatibility::const_projection_view shifted {std::views::all(numbers), offset};
    auto replacement = shifted;
    replacement = shifted;
    CHECK(9 == *replacement.begin());
    using shifted_iterator = std::ranges::iterator_t<decltype(shifted)>;
    static_assert(std::ranges::random_access_range<decltype(shifted)>);
    static_assert(std::same_as<typename std::iterator_traits<shifted_iterator>::iterator_category,
                               std::input_iterator_tag>);
    static_assert(std::is_nothrow_copy_assignable_v<decltype(shifted)>);
    static_assert(std::is_nothrow_move_assignable_v<decltype(shifted)>);
    static_assert(std::same_as<
        compatibility::interval_projection_view<std::ranges::ref_view<std::array<int, 3>>,
                                                std::identity>,
        std::ranges::transform_view<std::ranges::ref_view<std::array<int, 3>>, std::identity>>);

    auto move_only = [amount = std::make_unique<int>(2)](const int value) {
        return value + *amount;
    };
    using selected_projection = compatibility::interval_projection_view<
        std::ranges::ref_view<std::array<int, 3>>, decltype(move_only)>;
#if !defined(__cpp_lib_ranges) || __cpp_lib_ranges < 202207L
    static_assert(std::same_as<selected_projection, compatibility::const_projection_view<
                                                      std::ranges::ref_view<std::array<int, 3>>,
                                                      decltype(move_only)>>);
#endif
    selected_projection retained {std::views::all(numbers), std::move(move_only)};
    CHECK(9 == *retained.begin());

    compatibility::const_projection_view<std::ranges::single_view<int>, std::identity> defaults;
    CHECK(0 == *defaults.begin());
    CHECK(1 == defaults.size());

    struct default_projection final
    {
        default_projection() = default;
        default_projection(const default_projection&) noexcept(false) {}
        default_projection(default_projection&&) noexcept(false) {}

        default_projection& operator=(const default_projection&) = delete;
        default_projection& operator=(default_projection&&) = delete;

        int operator()(const int value) const { return 11 + value; }
    };
    static_assert(!compatibility::direct_projector_storage<default_projection>);
    compatibility::const_projection_view<std::ranges::single_view<int>, default_projection> engaged;
    CHECK(11 == *engaged.begin());
}

TEST_CASE("compat projection assignment needs no source copy construction",
          "[datum][range][compat]")
{
    struct source_view final : std::ranges::view_base
    {
        std::span<int> values;
        unsigned *assignments;

    public:
        constexpr source_view(std::span<int> source, unsigned& count)
            : values(source), assignments(std::addressof(count))
        {}
        source_view(const source_view&) = delete;
        source_view(source_view&&) = default;

    public:
        source_view& operator=(const source_view& other) noexcept
        {
            ++*assignments;
            values = other.values;

            return *this;
        }

        source_view& operator=(source_view&&) = default;

    public:
        constexpr auto begin() const { return values.begin(); }
        constexpr auto end() const { return values.end(); }

    };

    namespace compatibility = datum::compat::detail;
    using projection = compatibility::const_projection_view<source_view, std::identity>;
    using borrowed = compatibility::const_projection_view<
        std::ranges::ref_view<std::array<int, 3>>, std::identity>;

    static_assert(std::ranges::view<source_view> and !std::copy_constructible<source_view>);
    static_assert(!std::copy_constructible<projection>);
    static_assert(std::is_copy_assignable_v<projection>);
    static_assert(std::is_trivially_move_assignable_v<projection>);
    static_assert(std::is_nothrow_copy_assignable_v<projection>);
    static_assert(std::is_trivially_copyable_v<borrowed>);
    static_assert(std::is_trivially_copy_assignable_v<borrowed>);
    static_assert(std::is_trivially_move_assignable_v<borrowed>);

    std::array original {2, 4, 6};
    std::array replacement {7, 9};
    unsigned assignments = 0;
    projection first {source_view {original, assignments}, std::identity {}};
    projection second {source_view {replacement, assignments}, std::identity {}};
    first = second;
    CHECK(assignments == 1);
    CHECK(std::addressof(*first.begin()) == std::data(replacement));
    CHECK(std::ranges::distance(first) == 2);
    auto& same = first;
    first = same;
    CHECK(assignments == 1);
    first = std::move(second);
    CHECK(std::addressof(*first.begin()) == std::data(replacement));
}

TEST_CASE("compat projection assignment preserves callable lifetime after exceptions",
          "[datum][range][compat]")
{
    struct projection final
    {
        int& alive;
        int& copies;
        bool& fail_copy;
        bool& fail_move;

    public:
        projection(int& count, int& copied, bool& copying, bool& moving)
            : alive(count), copies(copied), fail_copy(copying), fail_move(moving)
        {
            ++alive;
        }

        projection(const projection& other)
            : alive(other.alive), copies(other.copies), fail_copy(other.fail_copy),
              fail_move(other.fail_move)
        {
            if (fail_copy) {
                throw std::runtime_error {"projection copy failure"};
            }
            ++alive;
            ++copies;
        }

        projection(projection&& other)
            : alive(other.alive), copies(other.copies), fail_copy(other.fail_copy),
              fail_move(other.fail_move)
        {
            if (fail_move) {
                throw std::runtime_error {"projection move failure"};
            }
            ++alive;
        }

        ~projection() { --alive; }

    public:
        projection& operator=(const projection&) = delete;
        projection& operator=(projection&&) = delete;

    public:
        const int& operator()(const int& value) const { return value; }

    };

    std::array numbers {3, 5};
    int alive = 0, copies = 0;
    bool fail_copy = false, fail_move = false;
    {
        datum::compat::detail::const_projection_view first {
            std::views::all(numbers), projection {alive, copies, fail_copy, fail_move}};
        auto second = first;
        CHECK(2 == alive);
        CHECK(1 == copies);
        first = second;
        CHECK(2 == alive);
        CHECK(2 == copies);
        auto& same = first;
        first = same;
        first = std::move(same);
        CHECK(2 == copies);
        CHECK(3 == *first.begin());

        fail_copy = true;
        CHECK_THROWS_AS(first = second, std::runtime_error);
        CHECK(1 == alive);
        fail_copy = false;
        first = second;
        CHECK(2 == alive);
        CHECK(3 == copies);
        CHECK(std::addressof(*first.begin()) == std::data(numbers));

        fail_move = true;
        CHECK_THROWS_AS(first = std::move(second), std::runtime_error);
        CHECK(1 == alive);
        fail_move = false;
        first = std::move(second);
        CHECK(2 == alive);
        CHECK(3 == copies);
        CHECK(3 == *first.begin());
    }
    CHECK(0 == alive);
}

TEST_CASE("append_range joins fragments and preserves string overlap")
{
    using namespace std::string_view_literals;
    const std::array fragments {"pay"sv, "load"sv, "tail"sv};
    std::string result;
    CHECK(std::addressof(datum::append_range(result, fragments | std::views::join)) ==
          std::addressof(result));
    CHECK(result == "payloadtail");

    // The source survives destination reallocation, including with a distinct sentinel.
    result.assign(80, 'x');
    const std::string original = result;
    datum::append_range(result, std::string_view {result});
    CHECK(result == original + original);
    auto prefix = std::ranges::subrange(std::counted_iterator {result.begin(), 60}, std::default_sentinel);
    datum::append_range(result, prefix);
    CHECK(result == std::string(220, 'x'));

    std::array<std::byte, 1024> storage;
    std::pmr::monotonic_buffer_resource resource {storage.data(), storage.size()};
    std::pmr::string allocated {&resource};
    datum::append_range(allocated, fragments | std::views::join);
    CHECK(allocated == "payloadtail");
    CHECK(allocated.get_allocator().resource() == &resource);
}

TEST_CASE("append_range accepts one-pass and non-common sources")
{
    std::istringstream input {"1 2 3"};
    std::vector<int> values {0};
    datum::append_range(values, std::views::istream<int>(input));
    CHECK(values == std::vector {0, 1, 2, 3});
    CHECK(input.eof());

    std::list<int> linked {0};
    datum::append_range(linked, std::views::iota(1) | std::views::take(3));
    CHECK(std::ranges::equal(linked, values));

    std::string letters;
    std::istringstream text {"abc"};
    datum::append_range(letters, std::views::istream<char>(text));
    CHECK(letters == "abc");
}

TEST_CASE("append_range consumes elements only when explicitly requested")
{
    std::array source {std::string(80, 'a'), std::string(80, 'b')};
    const auto original = source;
    std::vector<std::string> copied;
    datum::append_range(copied, source);
    CHECK(source == original);
    CHECK(std::ranges::equal(copied, original));

    std::array owned {std::make_unique<int>(3), std::make_unique<int>(7)};
    std::vector<std::unique_ptr<int>> moved;
#if defined(__cpp_lib_ranges_as_rvalue) and __cpp_lib_ranges_as_rvalue >= 202207L
    datum::append_range(moved, owned | std::views::as_rvalue);
#else
    datum::append_range(moved, std::ranges::subrange {
        std::make_move_iterator(owned.begin()), std::make_move_iterator(owned.end())});
#endif
    REQUIRE(moved.size() == 2);
    CHECK(*moved[0] == 3);
    CHECK(*moved[1] == 7);
    CHECK_FALSE(owned[0]);
    CHECK_FALSE(owned[1]);
}

namespace {

struct native_range_destination final
{
    int total = 0;
    int calls = 0;

public:
    void append_range(const std::array<int, 3>& source)
    {
        ++calls;
        total += *std::ranges::max_element(source);
    }

    template <typename IteratorT>
    auto append(IteratorT, IteratorT)
    {
        static_assert(!std::same_as<IteratorT, IteratorT>,
                      "Native range insertion must not probe an unselected iterator interface");
    }

};

} // anonymous namespace

TEST_CASE("append_range uses a native range insertion without imposing a container shape")
{
    native_range_destination target;
    datum::append_range(target, std::array {1, 5, 3});
    CHECK(target.calls == 1);
    CHECK(target.total == 5);
}

TEST_CASE("append_range checks sized capacity before insertion")
{
    struct bounded_destination final
    {
        std::vector<int> values {1};
        auto size() const { return values.size(); }
        std::size_t max_size() const { return 2; }
        void push_back(int value) { values.push_back(value); }
    } target;
    CHECK_THROWS_AS(datum::append_range(target, std::array {2, 3}), std::length_error);
    CHECK(target.values == std::vector {1});

    std::string unchanged {"original"};
    const std::array fragments {std::string_view {"first"}, std::string_view {"second"}};
    auto failing = fragments | std::views::transform([](std::string_view fragment) {
        if (fragment == "second") {
            throw std::runtime_error {"source failure"};
        }
        return fragment;
    }) | std::views::join;
    CHECK_THROWS_AS(datum::append_range(unchanged, failing), std::runtime_error);
    CHECK(unchanged == "original");
}

static_assert([] {
    std::vector<int> values;
    datum::append_range(values, std::array {2, 4});
    std::string text {"ab"};
    datum::append_range(text, std::string_view {text});
    return values == std::vector {2, 4} and text == "abab";
}());

namespace {

template <typename DestinationT, typename SourceT>
concept can_append_range = requires (DestinationT&& destination, SourceT&& source) {
    datum::append_range(std::forward<DestinationT>(destination), std::forward<SourceT>(source));
};

static_assert(can_append_range<std::vector<int>&, std::array<int, 2>>);
static_assert(!can_append_range<const std::vector<int>&, std::array<int, 2>>);
static_assert(!can_append_range<std::vector<int>, std::array<int, 2>>);
static_assert(!can_append_range<std::vector<int>&, int>);
static_assert(!can_append_range<std::vector<std::unique_ptr<int>>&, std::array<std::unique_ptr<int>, 2>&>);

} // anonymous namespace

TEST_CASE("repeated small range appends preserve amortized container growth")
{
    struct counting_resource final : std::pmr::memory_resource
    {
        std::size_t allocations = 0;

        void *do_allocate(std::size_t bytes, std::size_t alignment) override
        {
            ++allocations;
            return std::pmr::new_delete_resource()->allocate(bytes, alignment);
        }
        void do_deallocate(void *pointer, std::size_t bytes, std::size_t alignment) override
        {
            std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override
        {
            return this == std::addressof(other);
        }
    } resource;
    std::pmr::vector<int> values {&resource};
    std::ranges::for_each(std::views::iota(0, 128), [&](const int value) {
        datum::append_range(values, std::array {value});
    });
    CHECK(std::ranges::equal(values, std::views::iota(0, 128)));
    CHECK(resource.allocations < 32);
}

TEST_CASE("bounded interval range insertion preserves entries and single-pass effects")
{
    using domain = mechanics_domain<int>;
    using query = iv::query<domain>;
    iv::static_interval_set<domain, 4> entries {std::array {query::singleton(1)}};
    std::istringstream input {"2 2 3"};
    auto additions = std::views::istream<int>(input) | std::views::transform([](int value) {
        return query::singleton(value);
    });
    datum::append_range(entries, additions);
    CHECK(std::ranges::equal(entries, std::array {query::singleton(1), query::singleton(2),
                                                 query::singleton(2), query::singleton(3)}));
    CHECK(input.eof());
    entries.append_range(std::views::empty<query>);
    CHECK(entries.size() == 4);
    CHECK_THROWS_AS(entries.append_range(std::array {query::empty()}), std::length_error);
    CHECK(entries.size() == 4);
}

TEST_CASE("bounded range insertion checks capacity before observing each entry")
{
    using domain = mechanics_domain<int>;
    using query = iv::query<domain>;
    unsigned observations = 0;
    auto additions = std::views::iota(1, 4) | std::views::transform([&](int value) {
        ++observations;
        return query::singleton(value);
    });
    iv::static_interval_set<domain, 2> entries;
    CHECK_THROWS_AS(datum::append_range(entries, additions), std::length_error);
    CHECK(observations == 2);
    CHECK(std::ranges::equal(entries, std::array {query::singleton(1), query::singleton(2)}));

    iv::static_interval_set<domain, 0> zero;
    observations = 0;
    CHECK_THROWS_AS(zero.append_range(additions), std::length_error);
    CHECK(observations == 0);

    iv::static_interval_set<domain, 4> partial;
    auto failing = std::views::iota(1, 4) | std::views::transform([](int value) {
        if (value == 2) {
            throw std::runtime_error {"projection failure"};
        }
        return query::singleton(value);
    });
    CHECK_THROWS_AS(partial.append_range(failing), std::runtime_error);
    CHECK(std::ranges::equal(partial, std::array {query::singleton(1)}));
}

static_assert([] {
    using namespace interval_data_test;
    using query = iv::query<immutable_offsets>;
    const std::array source {query::singleton(immutable_point {7}), query::empty()};
    iv::static_interval_set<immutable_offsets, 2> entries;
    datum::append_range(entries, source);
    return entries.size() == 2 and entries[0] == source[0] and iv::is_empty(entries[1]);
}());

namespace range_lookup_test {

// These data members require range access through ADL customization points.
template <typename T, std::size_t Size>
struct source final
{
    std::array<T, Size> values;
    int begin = 0;
    int end = 0;

    friend constexpr auto begin(const source& value) { return value.values.begin(); }
    friend constexpr auto end(const source& value) { return value.values.end(); }
};

// A consumer's unrelated operation must not intercept Datum's internal traversal.
template <typename T, std::size_t Size>
void visit_range(const source<T, Size>&, auto&&) = delete;

} // range_lookup_test

TEST_CASE("range insertion needs only push_back from a minimal destination")
{
    struct destination final
    {
        int total;
        explicit destination(int initial) : total(initial) {}
        destination(const destination&) = delete;
        destination(destination&&) = delete;

        void push_back(int value) { total += value; }
    } target {0};
    STATIC_REQUIRE_FALSE(std::default_initializable<destination>);
    CHECK(std::addressof(datum::append_range(target, std::views::iota(1, 5))) ==
          std::addressof(target));
    CHECK(target.total == 10);
    const range_lookup_test::source<int, 3> extra {{1, 2, 3}};
    STATIC_REQUIRE(std::ranges::input_range<decltype(extra)>);
    datum::append_range(target, extra);
    CHECK(target.total == 16);
}

TEST_CASE("range consumers honor ADL when data members hide range access")
{
    const range_lookup_test::source<std::string_view, 2> fragments {{"ab", "cd"}};
    STATIC_REQUIRE(datum::byte_fragments<decltype(fragments)>);
    CHECK(datum::byte_capacity(fragments) == 4);
    std::array<char, 4> output {};
    CHECK(std::string_view {datum::copy(fragments, output)} == "abcd");
    output.fill('x');
    CHECK(std::string_view {datum::copy(fragments, 0, output)} == "abcd");
    std::string observed;
    datum::write_fragments(fragments, [&observed](const auto block) {
        datum::append_range(observed, block);
    });
    CHECK(observed == "abcd");

    std::array<char, 2> first {}, last {};
    const range_lookup_test::source<std::span<char>, 2> destination {{first, last}};
    CHECK(datum::scatter_bytes(std::string_view {"abcd"}, destination) == 4);
    CHECK(first == std::array {'a', 'b'});
    CHECK(last == std::array {'c', 'd'});

    using domain = mechanics_domain<int>;
    using query = iv::query<domain>;
    const range_lookup_test::source<query, 2> intervals {{query::closed(1, 2), query::closed(2, 3)}};
    const auto coverage = iv::compile_intervals(intervals);
    CHECK(iv::contains(coverage, 2));
    CHECK_FALSE(iv::contains(coverage, 4));

    const range_lookup_test::source<std::string_view, 2> path {{"space", "leaf"}};
    CHECK(datum::key::compose_range(path) == datum::key::compose("space", "leaf"));
}

TEST_CASE("canonical payload ownership follows its storage", "[datum][ownership]")
{
    using query = iv::query<iv::comparison_domain<int>>;
    const auto owned = iv::compile_intervals(query::closed(1, 4));
    const auto bounded = iv::compile_intervals<1>(query::closed(1, 4));
    iv::workspace<iv::comparison_domain<int>> scratch;
    const auto borrowed = iv::evaluate(query::closed(1, 4), scratch);
    STATIC_REQUIRE(datum::owning_value<decltype(owned)>);
    STATIC_REQUIRE(datum::owning_value<decltype(bounded)>);
    STATIC_REQUIRE_FALSE(datum::owning_value<decltype(borrowed)>);

    const iv::canonical_range retained {std::vector {query::closed(1, 4)}};
    const std::array backing {query::closed(1, 4)};
    const iv::canonical_range observed {backing};
    STATIC_REQUIRE(datum::owning_value<decltype(retained)>);
    STATIC_REQUIRE_FALSE(datum::owning_value<decltype(observed)>);
    using borrowed_query = iv::query<iv::comparison_domain<std::string_view>>;
    const iv::canonical_range borrowed_payload {
        std::vector {borrowed_query::closed("a", "z")}};
    STATIC_REQUIRE_FALSE(datum::owning_value<decltype(borrowed_payload)>);
}

TEST_CASE("canonical admission preserves standard range ownership", "[datum][ownership]")
{
    using query = iv::query<iv::comparison_domain<int>>;
    using borrowed = iv::canonical_range<std::span<const query>>;
    STATIC_REQUIRE(std::constructible_from<borrowed, std::span<const query>>);
    STATIC_REQUIRE_FALSE(std::constructible_from<borrowed, std::vector<query>>);
    STATIC_REQUIRE_FALSE(std::constructible_from<borrowed, std::vector<query>&>);
    STATIC_REQUIRE_FALSE(std::constructible_from<borrowed, std::array<query, 1>>);
    STATIC_REQUIRE_FALSE(std::constructible_from<
        iv::canonical_range<std::initializer_list<query>>, std::initializer_list<query>>);

    const iv::canonical_range retained {std::vector {query::closed(1, 4)}};
    STATIC_REQUIRE(datum::owning_value<decltype(retained)>);
    CHECK(iv::contains(retained, 2));
}
