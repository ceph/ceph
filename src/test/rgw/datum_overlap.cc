// Source-preserving overlap traversal, input producers, and scoped lifetimes:
#include "datum/interval/clip.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <limits>
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

static_assert([] {
    const std::array origins {datum::byte_extent {4, 2}, datum::byte_extent {10, 3}};
    const std::array selected {datum::byte_extent {11, 1}};
    std::uint64_t packed = 0;
    const auto result = iv::visit_packed_overlaps(origins, selected,
        [&](const auto&, const auto&, const datum::byte_mapping mapped) {
            packed = mapped.backing_offset;

            return datum::acceptance::accept;
        });

    return 1 == result.accepted and 3 == packed;
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

TEST_CASE("rvalue-reference observations preserve original row and endpoint identity")
{
    std::array rows {record {7, query::closed_open(0, 8)}};
    auto referenced = rows | std::views::transform([](record& row) -> record&& {
        return std::move(row);
    });
    const std::array selected {query::closed_open(1, 3), query::closed_open(4, 6)};
    const auto result = iv::visit_overlaps(referenced, selected, [&](const auto& source, const auto&) {
        CHECK(std::addressof(source.source()) == std::addressof(rows[0]));

        return datum::acceptance::accept;
    }, [](const record& row) -> const query&& { return std::move(row.geometry); });

    CHECK(2 == result.accepted);
    CHECK(rows[0].geometry == query::closed_open(0, 8));

    using strings = iv::query<iv::comparison_domain<std::string>>;

    const std::array words {strings::closed_open(std::string(80, 'a'), std::string(80, 'z'))};
    const std::array all {strings::universal()};
    const auto observed = iv::visit_overlaps(words, all, [&](const auto& source, const auto&) {
        CHECK(std::addressof(source.lower().key()) == std::addressof(words[0].lower().key()));
        CHECK(std::addressof(source.upper().key()) == std::addressof(words[0].upper().key()));

        return datum::acceptance::accept;
    }, [](const strings& row) -> const strings&& { return std::move(row); });

    CHECK(1 == observed.accepted);
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
            const auto selected = iv::extent_of(source);
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

TEST_CASE("finite byte bounds convert to extents without losing empty positions")
{
    for (const auto extent : {datum::byte_extent {0, 0}, datum::byte_extent {9, 0},
                              datum::byte_extent {4, 8},
                              datum::byte_extent {std::numeric_limits<std::uint64_t>::max() - 1, 1},
                              datum::byte_extent {std::numeric_limits<std::uint64_t>::max(), 0}}) {
        CHECK(iv::extent_of(iv::bounds(extent)) == extent);
    }

    CHECK_THROWS_AS(iv::extent_of(iv::byte_query::universal()), std::domain_error);
    CHECK_THROWS_AS(iv::extent_of(iv::byte_query::closed(0, 4)), std::domain_error);
    CHECK_THROWS_AS(iv::extent_of(iv::byte_query::closed_open(8, 4)), std::domain_error);
}

TEST_CASE("packed overlaps retain skipped origins and multiple cuts in one-pass inputs")
{
    const std::array origins {datum::byte_extent {0, 4}, datum::byte_extent {9, 0},
                              datum::byte_extent {10, 8}, datum::byte_extent {18, 4},
                              datum::byte_extent {40, 3}};
    const std::array selections {datum::byte_extent {12, 2}, datum::byte_extent {16, 1},
                                 datum::byte_extent {42, 1}};
    input_records source {origins};
    input_records wanted {selections};
    unsigned projections = 0;
    const borrowed_projection lens {projections};
    std::vector<std::tuple<std::size_t, std::size_t, datum::byte_mapping>> actual;
    const auto result = iv::visit_packed_overlaps(source, wanted,
        [&](const auto& left, const auto& right, const datum::byte_mapping mapped) {
            CHECK(iv::extent_of(left) == mapped.logical);
            CHECK(iv::extent_of(right) == mapped.logical);
            actual.emplace_back(*left.source().id, *right.source().id, mapped);

            return datum::acceptance::accept;
        }, lens, &value_record::geometry);

    CHECK(actual == std::vector<std::tuple<std::size_t, std::size_t, datum::byte_mapping>> {
        {2, 0, {{12, 2}, 6}}, {2, 1, {{16, 1}, 10}}, {4, 2, {{42, 1}, 18}}
    });
    CHECK(3 == result.accepted);
    CHECK(5 == source.reads);
    CHECK(5 == projections);
    CHECK(3 == wanted.reads);
    CHECK(0 == source.live);
    CHECK(0 == wanted.live);
}

TEST_CASE("packed overlap stopping preserves prefix positions without visiting later origins")
{
    const std::array origins {datum::byte_extent {0, 4}, datum::byte_extent {10, 4},
                              datum::byte_extent {20, 4}};
    const std::array selections {datum::byte_extent {1, 1}, datum::byte_extent {12, 1},
                                 datum::byte_extent {22, 1}};
    input_records source {origins};
    input_records wanted {selections};
    unsigned offered = 0;
    const auto result = iv::visit_packed_overlaps(source, wanted,
        [&](const auto&, const auto&, const datum::byte_mapping mapped)
            -> std::expected<datum::acceptance, std::unique_ptr<int>> {
            if (++offered == 2) {
                CHECK(6 == mapped.backing_offset);

                return std::unexpected(std::make_unique<int>(42));
            }

            CHECK(1 == mapped.backing_offset);

            return datum::acceptance::accept;
        }, &value_record::geometry, &value_record::geometry);

    REQUIRE_FALSE(result);
    CHECK(42 == *result.error().error);
    CHECK(2 == result.error().progress.offered);
    CHECK(1 == result.error().progress.accepted);
    CHECK(1 == result.error().progress.consumed());
    CHECK(2 == source.reads);
    CHECK(2 == wanted.reads);
    CHECK(1 == source.increments);
    CHECK(1 == wanted.increments);
    CHECK(0 == source.live);
    CHECK(0 == wanted.live);
}

TEST_CASE("packed cuts agree with enumerating all source bytes before selection")
{
    const std::array origins {datum::byte_extent {0, 4}, datum::byte_extent {10, 8},
                              datum::byte_extent {18, 4}, datum::byte_extent {40, 3}};
    const std::array choices {datum::byte_extent {1, 2}, datum::byte_extent {12, 2},
                              datum::byte_extent {16, 4}, datum::byte_extent {42, 1}};
    std::vector<std::pair<std::uint64_t, std::size_t>> packed;
    for (std::size_t origin = 0; origin < origins.size(); ++origin) {
        for (std::uint64_t byte = 0; byte < origins[origin].length; ++byte) {
            packed.emplace_back(origins[origin].offset + byte, origin);
        }
    }

    for (unsigned mask = 0; mask != 16; ++mask) {
        std::vector<datum::byte_extent> wanted;
        for (unsigned index = 0; index != choices.size(); ++index) {
            if (mask & (1U << index)) {
                wanted.push_back(choices[index]);
            }
        }

        using selected_byte = std::tuple<std::uint64_t, std::uint64_t, std::size_t>;

        std::vector<selected_byte> expected;
        for (std::size_t position = 0; position < packed.size(); ++position) {
            const auto [logical, origin] = packed[position];
            if (std::ranges::any_of(wanted, [logical](const auto extent) {
                    return logical >= extent.offset and logical - extent.offset < extent.length;
                })) {
                expected.emplace_back(logical, position, origin);
            }
        }

        std::vector<selected_byte> actual;
        const auto progress = iv::visit_packed_overlaps(origins, wanted,
            [&](const auto& source, const auto&, const datum::byte_mapping mapped) {
                const auto origin = std::addressof(source.source()) - origins.data();
                for (std::uint64_t byte = 0; byte < mapped.logical.length; ++byte) {
                    actual.emplace_back(mapped.logical.offset + byte, mapped.backing_offset + byte,
                                        static_cast<std::size_t>(origin));
                }

                return datum::acceptance::accept;
            });

        CHECK_FALSE(progress.stopped());
        CHECK(actual == expected);
    }
}

TEST_CASE("packed geometry rejects unsupported cuts and leaves an unused tail unprojected")
{
    const std::array source {iv::byte_query::empty(), iv::byte_query::closed_open(10, 14)};
    const std::array all {iv::byte_query::universal()};
    const auto accepted = iv::visit_packed_overlaps(source, all,
        [](const auto&, const auto&, const datum::byte_mapping mapped) {
            CHECK(mapped == datum::byte_mapping {{10, 4}, 0});

            return datum::acceptance::accept;
        });
    CHECK(1 == accepted.accepted);

    const std::array closed {iv::byte_query::closed(10, 12)};
    const auto reject = [](const auto&, const auto&, const auto&) {
        FAIL("unsupported geometry reached the consumer");

        return datum::acceptance::accept;
    };
    CHECK_THROWS_AS(iv::visit_packed_overlaps(closed, all, reject), std::domain_error);
    CHECK_THROWS_AS(iv::visit_packed_overlaps(source, closed, reject), std::domain_error);

    const std::array tail {datum::byte_extent {0, 4},
                           datum::byte_extent {std::numeric_limits<std::uint64_t>::max(), 1}};
    const std::array selection {datum::byte_extent {0, 4}};
    unsigned projections = 0;
    const auto limited = iv::visit_packed_overlaps(tail, selection,
        [](const auto&, const auto&, const auto&) { return datum::acceptance::accept_and_stop; },
        [&](const auto& row) {
            ++projections;

            return row;
        });
    CHECK(1 == limited.accepted);
    CHECK(1 == projections);
}

TEST_CASE("packed mapping preserves representable maximum byte endpoints")
{
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

    const std::array origins {datum::byte_extent {0, maximum - 4},
                              datum::byte_extent {maximum - 2, 2}};
    const std::array selected {datum::byte_extent {maximum - 2, 2}};
    const auto result = iv::visit_packed_overlaps(origins, selected,
        [](const auto&, const auto&, const datum::byte_mapping mapped) {
            CHECK(mapped == datum::byte_mapping {{maximum - 2, 2}, maximum - 4});

            return datum::acceptance::accept;
        });

    CHECK(1 == result.accepted);
}

namespace {

struct const_stable_geometry
{
    using domain_type = domain;

    query geometry = query::closed_open(1, 7);

    boundary lower() { return geometry.lower(); }
    boundary upper() { return geometry.upper(); }
    const boundary& lower() const { return geometry.lower(); }
    const boundary& upper() const { return geometry.upper(); }
};

struct const_unstable_lower final : const_stable_geometry
{
    const boundary& lower() { return geometry.lower(); }
    boundary lower() const { return geometry.lower(); }
};

struct const_unstable_upper final : const_stable_geometry
{
    const boundary& upper() { return geometry.upper(); }
    boundary upper() const { return geometry.upper(); }
};

template <typename GeometryT>
consteval bool const_stability_contract()
{
    return iv::stable_interval<GeometryT> and iv::stable_interval<GeometryT&> and
           iv::stable_interval<const GeometryT&> and iv::stable_interval<GeometryT&&>;
}

template <typename GeometryT>
consteval bool const_instability_contract()
{
    return iv::interval_view<GeometryT> and !iv::stable_interval<GeometryT> and
           !iv::stable_interval<GeometryT&> and !iv::stable_interval<const GeometryT&> and
           !iv::stable_interval<GeometryT&&>;
}

static_assert(const_stability_contract<const_stable_geometry>());
static_assert(const_instability_contract<const_unstable_lower>());
static_assert(const_instability_contract<const_unstable_upper>());

struct referenced_geometry final
{
    template <typename GeometryT>
    GeometryT& operator()(const std::reference_wrapper<GeometryT> row) const { return row.get(); }
};

template <typename GeometryT>
concept clips_referenced_geometry = requires(std::array<std::reference_wrapper<GeometryT>, 1>& rows,
                                            iv::canonical_range<std::span<const query>>& selection) {
    iv::clip_view {rows, selection, referenced_geometry {}};
    iv::visit_overlaps(rows, selection,
        [](const auto&, const auto&) { return datum::acceptance::accept; }, referenced_geometry {});
};

static_assert(clips_referenced_geometry<const_stable_geometry>);
static_assert(!clips_referenced_geometry<const_unstable_lower>);
static_assert(!clips_referenced_geometry<const_unstable_upper>);

} // anonymous namespace

TEST_CASE("stable geometry constraints inspect the const accessors used by consumers")
{
    const_stable_geometry origin;
    const std::array rows {std::ref(origin)};
    const auto selection = iv::compile_intervals(query::closed_open(0, 9));
    const iv::clip_view clipped {rows, selection, referenced_geometry {}};
    auto position = clipped.begin();
    const auto cut = *position;
    CHECK(std::addressof(cut.lower().key()) == std::addressof(origin.geometry.lower().key()));
    CHECK(std::addressof(cut.upper().key()) == std::addressof(origin.geometry.upper().key()));
    ++position;
    CHECK(position == clipped.end());

    const auto progress = iv::visit_overlaps(rows, selection,
        [&origin](const auto& left, const auto& right) {
            CHECK(std::addressof(left.source().get()) == std::addressof(origin));
            CHECK(iv::encloses(left, right));

            return datum::acceptance::accept;
        }, referenced_geometry {});
    CHECK(1 == progress.accepted);
}

namespace {

struct const_only_geometry_rows final
{
    std::array<query, 1> values {query::closed_open(1, 7)};

    auto begin() const { return values.begin(); }
    auto end() const { return values.end(); }
    void begin() = delete;
    void end() = delete;
};

struct geometry_proxy final
{
    query geometry;

    operator int() const { return geometry.lower().key(); }
};

struct proxy_geometry_rows final
{
    std::array<geometry_proxy, 1> values {{{query::closed_open(1, 7)}}};

    struct iterator final
    {
        using value_type = int;
        using difference_type = std::ptrdiff_t;
        using iterator_concept = std::forward_iterator_tag;

        geometry_proxy *position = nullptr;

        geometry_proxy& operator*() const { return *position; }
        iterator& operator++() { ++position; return *this; }
        iterator operator++(int) { auto saved = *this; ++*this; return saved; }
        bool operator==(const iterator&) const = default;
    };

    iterator begin() { return {values.data()}; }
    iterator end() { return {values.data() + values.size()}; }
};

struct cached_geometry
{
    using domain_type = domain;

    query geometry;

    explicit cached_geometry(const query& value) : geometry(value)
    {}
    cached_geometry(const cached_geometry&) = delete;
    explicit cached_geometry(cached_geometry&&) = default;

    const boundary& lower() const { return geometry.lower(); }
    const boundary& upper() const { return geometry.upper(); }
};

struct pinned_geometry final : cached_geometry
{
    using cached_geometry::cached_geometry;

    pinned_geometry(const pinned_geometry&) = delete;
    pinned_geometry(pinned_geometry&&) = delete;
};

template <typename GeometryT>
struct value_geometry_lens final
{
    GeometryT operator()(const query& value) const { return GeometryT {value}; }
};

template <typename GeometryT>
concept caches_geometry = requires(const std::array<query, 1>& rows,
                                   iv::canonical_range<std::span<const query>>& selection) {
    iv::clip_view {rows, selection, value_geometry_lens<GeometryT> {}};
};

static_assert(std::ranges::forward_range<const const_only_geometry_rows>);
static_assert(!std::ranges::range<const_only_geometry_rows>);
static_assert(std::ranges::forward_range<proxy_geometry_rows>);
static_assert(!std::same_as<std::ranges::range_value_t<proxy_geometry_rows>, geometry_proxy>);
static_assert(!std::move_constructible<cached_geometry>);
static_assert(std::is_move_constructible_v<cached_geometry>);
static_assert(caches_geometry<cached_geometry>);
static_assert(!caches_geometry<const cached_geometry>);
static_assert(!caches_geometry<pinned_geometry>);
static_assert(iv::detail::overlap_source<const std::array<query, 1>,
                                        value_geometry_lens<cached_geometry>>);
static_assert(!iv::detail::overlap_source<const std::array<query, 1>,
                                         value_geometry_lens<const cached_geometry>>);

} // anonymous namespace

TEST_CASE("clipping observes const-only sources and preserves the actual row reference type")
{
    const_only_geometry_rows rows;
    const auto selection = iv::compile_intervals(query::closed_open(2, 6));
    const iv::clip_view clipped {rows, selection, std::identity {}};
    STATIC_REQUIRE(std::ranges::input_range<decltype(clipped)>);
    auto position = clipped.begin();
    const auto cut = *position;
    CHECK(std::addressof(cut.source()) == rows.values.data());
    CHECK(2 == cut.lower().key());
    CHECK(6 == cut.upper().key());

    proxy_geometry_rows proxies;
    const iv::clip_view projected {proxies, selection, &geometry_proxy::geometry};
    STATIC_REQUIRE(std::ranges::input_range<decltype(projected)>);
    auto proxy = projected.begin();
    const auto projected_cut = *proxy;
    STATIC_REQUIRE(std::same_as<decltype(projected_cut.source()), const geometry_proxy&>);
    CHECK(std::addressof(projected_cut.source()) == proxies.values.data());
    CHECK(2 == projected_cut.lower().key());
    CHECK(6 == projected_cut.upper().key());
}

TEST_CASE("geometry caches require construction without implicit conversion or assignment")
{
    const std::array rows {query::closed_open(1, 7)};
    const auto selection = iv::compile_intervals(query::closed_open(2, 6));
    const iv::clip_view clipped {rows, selection, value_geometry_lens<cached_geometry> {}};
    STATIC_REQUIRE(std::ranges::input_range<decltype(clipped)>);
    auto position = clipped.begin();
    auto moved = std::move(position);
    position = std::move(moved);
    const auto cut = *position;
    CHECK(2 == cut.lower().key());
    CHECK(6 == cut.upper().key());

    const auto progress = iv::visit_overlaps(rows, selection,
        [](const auto& left, const auto& right) {
            CHECK(iv::encloses(left, right));

            return datum::acceptance::accept;
        }, value_geometry_lens<cached_geometry> {});
    CHECK(1 == progress.accepted);

    const pinned_geometry retained {rows[0]};
    const iv::clip_view borrowed {rows, selection, [&retained](const query&) -> const pinned_geometry& {
        return retained;
    }};
    STATIC_REQUIRE(std::ranges::input_range<decltype(borrowed)>);
    auto observed = borrowed.begin();
    const auto borrowed_cut = *observed;
    CHECK(2 == borrowed_cut.lower().key());
    CHECK(6 == borrowed_cut.upper().key());
}

TEST_CASE("clipping and overlap cuts preserve volatile source qualification")
{
    volatile std::uint64_t origin[1] {1};
    const std::span rows {origin};
    STATIC_REQUIRE(std::ranges::forward_range<decltype(rows)>);
    const auto selection = iv::compile_intervals(iv::byte_query::closed_open(2, 6));
    const auto geometry = [](const volatile std::uint64_t& first) {
        return datum::byte_extent {first, 6};
    };
    const iv::clip_view clipped {rows, selection, geometry};
    auto position = clipped.begin();
    const auto cut = *position;
    STATIC_REQUIRE(std::same_as<decltype(cut.source()), const volatile std::uint64_t&>);
    CHECK(std::addressof(cut.source()) == std::addressof(origin[0]));
    CHECK(2 == cut.lower().key());
    CHECK(6 == cut.upper().key());

    const auto progress = iv::visit_overlaps(rows, selection,
        [&origin](const auto& left, const auto& right) {
            STATIC_REQUIRE(std::same_as<decltype(left.source()), const volatile std::uint64_t&>);
            CHECK(std::addressof(left.source()) == std::addressof(origin[0]));
            CHECK(iv::encloses(left, right));

            return datum::acceptance::accept;
        }, geometry);
    CHECK(1 == progress.accepted);
}
