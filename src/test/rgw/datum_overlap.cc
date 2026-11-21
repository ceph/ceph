// Source-preserving overlap traversal, input producers, and scoped lifetimes.
#include "datum/interval/clip.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <expected>
#include <memory>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

namespace {

using domain = iv::comparison_domain<int>;
using query = iv::query<domain>;
using boundary = iv::boundary<domain>;

struct record final
{
    int id;
    query geometry;
};

// A native input iterator may materialize an owning, nondefault, move-only descriptor:
struct value_record final
{
    std::unique_ptr<std::size_t> id;
    datum::byte_extent geometry;
    int *live;

public:
    value_record(const std::size_t index, const datum::byte_extent extent, int& count)
        : id(std::make_unique<std::size_t>(index)),
          geometry(extent),
          live(std::addressof(count))
    {
        ++*live;
    }

    value_record(value_record&&) = default;

public:
    ~value_record()
    {
        if (id) {
            --*live;
        }
    }

};

class input_records final
{
    std::span<const datum::byte_extent> extents;
    std::size_t position = 0;

    class iterator final
    {
        input_records *owner = nullptr;

    public:
        using value_type = value_record;
        using difference_type = std::ptrdiff_t;
        using iterator_concept = std::input_iterator_tag;

    public:
        iterator() = default;
        explicit iterator(input_records& source) : owner(std::addressof(source))
        {}

    public:
        value_record operator*() const
        {
            ++owner->reads;

            return {owner->position, owner->extents[owner->position], owner->live};
        }

        iterator& operator++()
        {
            ++owner->increments;
            ++owner->position;

            return *this;
        }

        void operator++(int) { ++*this; }

        bool operator==(std::default_sentinel_t) const
        {
            return owner->position == owner->extents.size();
        }

    };

public:
    std::size_t reads = 0;
    std::size_t increments = 0;
    unsigned starts = 0;
    int live = 0;

public:
    explicit input_records(const std::span<const datum::byte_extent> source) : extents(source)
    {}

public:
    auto begin()
    {
        if (++starts != 1) {
            throw std::logic_error {"input traversed twice"};
        }

        return iterator {*this};
    }

    std::default_sentinel_t end() const noexcept { return {}; }

};

struct borrowed_projection final
{
    unsigned& calls;

public:
    explicit borrowed_projection(unsigned& count) : calls(count)
    {}

    borrowed_projection(const borrowed_projection&) = delete;
    borrowed_projection(borrowed_projection&&) = delete;

public:
    datum::byte_extent operator()(const value_record& row) const
    {
        ++calls;

        return row.geometry;
    }

};

static_assert(std::ranges::input_range<input_records>);
static_assert(!std::ranges::forward_range<input_records>);
static_assert(!std::copy_constructible<value_record>);
static_assert(!std::default_initializable<value_record>);

template <typename LeftT, typename RightT>
concept overlap_admitted = requires (LeftT&& left, RightT&& right) {
    iv::visit_overlaps(std::forward<LeftT>(left), std::forward<RightT>(right),
        [](const auto&, const auto&) { return datum::acceptance::accept; });
};

static_assert(!overlap_admitted<std::array<int, 1>&, std::array<query, 1>&>);
static_assert(!overlap_admitted<std::array<query, 1>&, std::array<datum::byte_extent, 1>&>);

static_assert([] {
    const std::array left {query::closed_open(0, 8), query::closed_open(8, 16)};
    const std::array right {query::closed_open(0, 4), query::closed_open(4, 12),
                            query::closed_open(12, 16)};
    const auto result = iv::visit_overlaps(left, right, [](const auto&, const auto&) {
        return datum::acceptance::accept;
    });

    return 4 == result.offered and 4 == result.accepted and !result.stopped();
}());

} // anonymous namespace

TEST_CASE("overlap traversal preserves both independent segmentations and origins")
{
    const std::array left {record {10, query::closed_open(0, 8)},
                           record {11, query::closed_open(8, 16)}};
    const std::array right {record {20, query::closed_open(0, 4)},
                            record {21, query::closed_open(4, 12)},
                            record {22, query::closed_open(12, 16)}};
    std::vector<std::tuple<int, int, int, int>> actual;
    const auto result = iv::visit_overlaps(left, right, [&](const auto& lhs, const auto& rhs) {
        CHECK(std::addressof(lhs.source()) == &left[lhs.source().id - 10]);
        CHECK(std::addressof(rhs.source()) == &right[rhs.source().id - 20]);
        CHECK(iv::copy_interval(lhs) == iv::copy_interval(rhs));
        actual.emplace_back(lhs.source().id, rhs.source().id,
                            lhs.lower().key(), lhs.upper().key());

        return datum::acceptance::accept;
    }, &record::geometry, &record::geometry);

    CHECK(actual == std::vector<std::tuple<int, int, int, int>> {
        {10, 20, 0, 4}, {10, 21, 4, 8}, {11, 21, 8, 12}, {11, 22, 12, 16}
    });
    CHECK(4 == result.consumed());
}

TEST_CASE("overlap traversal agrees with a pointwise origin-pair model")
{
    const std::array forms {
        query::empty(), query::universal(), query::singleton(0), query::singleton(4),
        query::open(-4, 4), query::closed(-4, 4), query::closed_open(-4, 4),
        query::between(boundary::open(-4), boundary::closed(4)),
        query::between(boundary::negative_infinity(), boundary::open(0)),
        query::between(boundary::closed(0), boundary::positive_infinity())
    };
    const std::array segments {
        std::vector<query> {query::empty(), query::closed_open(-4, 0), query::singleton(0),
                            query::between(boundary::open(0), boundary::closed(4)), query::empty()},
        std::vector<query> {query::closed(-4, -2), query::open(-2, 2), query::closed(2, 4)},
        std::vector<query> {}, std::vector<query> {query::universal()}
    };
    auto check = [](const auto& left, const auto& right) {
        std::vector<std::array<unsigned, 13>> actual(std::size(left) * std::size(right));
        const auto result = iv::visit_overlaps(left, right, [&](const auto& lhs, const auto& rhs) {
            REQUIRE_FALSE(iv::is_empty(lhs));
            CHECK(iv::copy_interval(lhs) == iv::copy_interval(rhs));
            const auto l = std::ranges::find_if(left, [&](const auto& row) {
                return std::addressof(row) == std::addressof(lhs.source());
            });
            const auto r = std::ranges::find_if(right, [&](const auto& row) {
                return std::addressof(row) == std::addressof(rhs.source());
            });
            REQUIRE(l != std::end(left));
            REQUIRE(r != std::end(right));
            const auto pair = std::ranges::distance(std::begin(left), l) * std::size(right) +
                              std::ranges::distance(std::begin(right), r);

            for (int point = -6; point <= 6; ++point) {
                if (iv::contains(lhs, point)) {
                    CHECK(iv::contains(lhs.source(), point));
                    CHECK(iv::contains(rhs.source(), point));
                    ++actual[pair][point + 6];
                }
            }

            return datum::acceptance::accept;
        });
        CHECK_FALSE(result.stopped());
        std::size_t pair = 0;
        for (const auto& lhs : left) {
            for (const auto& rhs : right) {
                for (int point = -6; point <= 6; ++point) {
                    CHECK(actual[pair][point + 6] ==
                          (iv::contains(lhs, point) and iv::contains(rhs, point)));
                }
                ++pair;
            }
        }
    };
    for (const auto& left : forms) {
        for (const auto& right : forms) {
            check(std::views::single(left), std::views::single(right));
        }
    }
    for (const auto& left : segments) {
        for (const auto& right : segments) {
            check(left, right);
        }
    }
}

TEST_CASE("one-pass owning descriptors and projections are observed once per origin")
{
    const std::array left_extents {datum::byte_extent {0, 8}, datum::byte_extent {8, 8}};
    const std::array right_extents {datum::byte_extent {0, 4}, datum::byte_extent {4, 8},
                                   datum::byte_extent {12, 4}};
    input_records left {left_extents};
    input_records right {right_extents};
    unsigned left_projections = 0;
    unsigned right_projections = 0;
    const borrowed_projection left_lens {left_projections};
    const borrowed_projection right_lens {right_projections};
    auto consumer = [calls = std::make_unique<unsigned>(), &left, &right]
                    (const auto& lhs, const auto& rhs) {
        CHECK(1 == left.live);
        CHECK(1 == right.live);
        CHECK(lhs.source().id);
        CHECK(rhs.source().id);
        ++*calls;

        return datum::acceptance::accept;
    };
    const auto result = iv::visit_overlaps(left, right, consumer, left_lens, right_lens);

    CHECK(4 == result.accepted);
    CHECK(2 == left.reads);
    CHECK(3 == right.reads);
    CHECK(2 == left_projections);
    CHECK(3 == right_projections);
    CHECK(1 == left.starts);
    CHECK(1 == right.starts);
    CHECK(0 == left.live);
    CHECK(0 == right.live);
}

TEST_CASE("overlap failure preserves cut progress without advancing either producer again")
{
    const std::array left_extents {datum::byte_extent {0, 8}, datum::byte_extent {8, 8}};
    const std::array right_extents {datum::byte_extent {0, 4}, datum::byte_extent {4, 8},
                                   datum::byte_extent {12, 4}};
    for (const auto fail : {false, true}) {
        input_records left {left_extents};
        input_records right {right_extents};
        unsigned offered = 0;
        const auto result = iv::visit_overlaps(left, right, [&](const auto&, const auto&)
            -> std::expected<datum::acceptance, std::unique_ptr<int>> {
            if (++offered == 2) {
                if (fail) {
                    return std::unexpected(std::make_unique<int>(42));
                }

                return datum::acceptance::accept_and_stop;
            }

            return datum::acceptance::skip;
        }, &value_record::geometry, &value_record::geometry);

        CHECK(1 == left.reads);
        CHECK(2 == right.reads);
        CHECK(0 == left.increments);
        CHECK(1 == right.increments);
        CHECK(0 == left.live);
        CHECK(0 == right.live);
        if (fail) {
            REQUIRE_FALSE(result);
            CHECK(42 == *result.error().error);
            CHECK(2 == result.error().progress.offered);
            CHECK(1 == result.error().progress.consumed());
            CHECK(0 == result.error().progress.accepted);
            continue;
        }

        REQUIRE(result);
        CHECK(2 == result->consumed());
        CHECK(1 == result->accepted);
        CHECK(result->stop == datum::acceptance::accept_and_stop);
    }
}

TEST_CASE("value descriptors map selected logical cuts to native backing offsets")
{
    struct mapped_record final
    {
        unsigned object;
        datum::byte_mapping geometry;
    };

    const std::array mappings {mapped_record {7, {{10, 8}, 100}},
                               mapped_record {9, {{24, 4}, 200}}};
    auto native_layout = mappings | std::views::transform([](const auto& source) { return source; });
    const std::array selections {datum::byte_extent {12, 3}, datum::byte_extent {16, 10}};
    std::vector<std::tuple<unsigned, std::uint64_t, std::uint64_t, std::uint64_t>> reads;
    const auto result = iv::visit_overlaps(native_layout, selections,
        [&](const auto& source, const auto&) {
            const datum::byte_extent selected {source.lower().key(),
                                               source.upper().key() - source.lower().key()};
            const auto mapped = datum::clip(source.source().geometry, selected);
            REQUIRE(mapped);
            reads.emplace_back(source.source().object, mapped->logical.offset,
                                mapped->backing_offset, mapped->logical.length);

            return datum::acceptance::accept;
        }, [](const mapped_record& source) { return source.geometry.logical; });

    CHECK(3 == result.accepted);
    CHECK(reads == std::vector<std::tuple<unsigned, std::uint64_t, std::uint64_t, std::uint64_t>> {
        {7, 12, 102, 3}, {7, 16, 106, 2}, {9, 24, 200, 2}
    });
}

TEST_CASE("overlap traversal retains string geometry during scoped observation")
{
    using strings = iv::comparison_domain<std::string>;
    using string_query = iv::query<strings>;

    const std::array left {string_query::closed_open(std::string(80, 'a'), std::string(80, 'z'))};
    auto values = left | std::views::transform([](const auto& source) { return source; });
    const std::array right {string_query::closed_open(std::string(80, 'b'), std::string(80, 'c')),
                            string_query::closed_open(std::string(80, 'd'), std::string(80, 'e'))};
    unsigned offered = 0;
    CHECK_THROWS_AS(iv::visit_overlaps(values, right, [&](const auto& lhs, const auto& rhs) {
        CHECK(lhs.lower().key() == rhs.lower().key());
        CHECK(lhs.source().lower().key() == std::string(80, 'a'));
        if (++offered == 2) {
            throw std::runtime_error {"consumer failed"};
        }

        return datum::acceptance::accept;
    }), std::runtime_error);
    CHECK(2 == offered);
}
