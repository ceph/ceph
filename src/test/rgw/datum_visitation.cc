#include "datum/range.h"
#include "datum/traversal.h"
#include "datum/traversal/scan.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <expected>
#include <memory>
#include <ranges>
#include <string>

namespace datum = ceph::salad::datum;
namespace traversal = datum::traversal;

namespace scan_contract_test {

struct owned_position final
{
    int value;

    explicit owned_position(const int input) : value(input)
    {}
    owned_position(const owned_position&) = delete;
    explicit owned_position(owned_position&&) = default;
};

struct pinned_position final
{
    int value;

    explicit pinned_position(const int input) : value(input)
    {}
    pinned_position(const pinned_position&) = delete;
    pinned_position(pinned_position&&) = delete;
};

template <typename PositionT>
struct positions final
{
    const PositionT position(const int row) const { return PositionT {row}; }
    const PositionT next(PositionT&& saved, const bool consumed) const
    {
        return PositionT {saved.value + consumed};
    }
};

} // scan_contract_test

namespace ceph::salad::datum {

template <>
struct ownership_traits<scan_contract_test::owned_position> : std::true_type
{};

template <>
struct ownership_traits<scan_contract_test::pinned_position> : std::true_type
{};

} // ceph::salad::datum

static_assert([] {
    const std::array rows {1, 2, 3};
    const auto result = datum::visit_range(rows, [](const int row)
        -> std::expected<datum::acceptance, int> {
        if (3 == row) {
            return std::unexpected(7);
        }

        return datum::acceptance::accept;
    });

    return !result and 7 == result.error().error and
           3 == result.error().progress.offered and 2 == result.error().progress.consumed();
}());

TEST_CASE("fallible visitation preserves progress and does not fetch past failure")
{
    int decoded = 0;
    int accepted = 0;
    auto rows = std::views::iota(1, 6) | std::views::transform([&decoded](const int row) {
        ++decoded;

        return std::make_unique<int>(row);
    });
    const auto result = datum::visit_range(rows, [&accepted](const auto& row)
        -> std::expected<datum::acceptance, std::unique_ptr<int>> {
        if (3 == *row) {
            return std::unexpected(std::make_unique<int>(42));
        }
        if (1 == *row) {
            return datum::acceptance::skip;
        }
        ++accepted;

        return datum::acceptance::accept;
    });

    REQUIRE_FALSE(result);
    CHECK(42 == *result.error().error);
    CHECK(3 == decoded);
    CHECK(1 == accepted);
    CHECK(3 == result.error().progress.offered);
    CHECK(2 == result.error().progress.consumed());
    CHECK(1 == result.error().progress.accepted);
    CHECK(result.error().progress.stop == datum::acceptance::reject_and_stop);
}

TEST_CASE("fallible visitation supports success stops and empty input")
{
    const auto consumer = [](const int row) -> std::expected<datum::acceptance, int> {
        return 2 == row ? datum::acceptance::accept_and_stop : datum::acceptance::skip;
    };
    const auto stopped = datum::visit_range(std::array {1, 2, 3}, consumer);
    REQUIRE(stopped);
    CHECK(2 == stopped->consumed());
    CHECK(1 == stopped->accepted);
    CHECK(stopped->stop == datum::acceptance::accept_and_stop);
    const auto empty = datum::visit_range(std::array<int, 0> {}, consumer);
    REQUIRE(empty);
    CHECK(0 == empty->offered);
    CHECK_FALSE(empty->stopped());
}

TEST_CASE("fallible scanning borrows callbacks and retains the rejected position")
{
    struct consumer final
    {
        int calls = 0;

    public:
        consumer() = default;
        consumer(const consumer&) = delete;
        consumer(consumer&&) = delete;

    public:
        std::expected<datum::acceptance, int> operator()(int& row)
        {
            ++calls;
            const auto value = row;
            row = -1;
            if (2 == value) {
                return std::unexpected(9);
            }

            return datum::acceptance::accept;
        }
    } sink;
    struct positions final
    {
        auto position(const int row) { return std::make_unique<int>(row); }

        auto next(std::unique_ptr<int> saved, const bool consumed)
        {
            *saved += consumed;

            return saved;
        }
    } policy;
    std::array rows {1, 2, 3};
    const auto result = traversal::scan_range(rows, sink, policy);
    REQUIRE_FALSE(result);
    CHECK(9 == result.error().error);
    CHECK(2 == *result.error().resume);
    CHECK(1 == result.error().progress.consumed());
    CHECK(2 == sink.calls);
    CHECK(rows == std::array {-1, -1, 3});
}

TEST_CASE("interval visitation preserves the common fallible visitor contract")
{
    using domain = datum::interval::comparison_domain<int>;
    using query = datum::interval::query<domain>;
    const std::array rows {query::singleton(1), query::singleton(2)};
    const auto result = datum::interval::visit_intervals(rows, [](const query& row)
        -> std::expected<datum::acceptance, int> {
        if (datum::interval::contains(row, 2)) {
            return std::unexpected(11);
        }

        return datum::acceptance::accept;
    });
    REQUIRE_FALSE(result);
    CHECK(11 == result.error().error);
    CHECK(1 == result.error().progress.accepted);
    CHECK(1 == result.error().progress.consumed());
}

namespace {

struct following final
{
    std::optional<int> operator()(int, const int page) const
    {
        return 3 == page ? std::nullopt : std::optional {page + 1};
    }
};

struct fallible_reader final
{
    std::expected<int, int> operator()(const int state) const { return state; }
};

struct other_error_visitor final
{
    std::expected<void, std::string> operator()(int) const { return {}; }
};

template <typename ReaderT, typename VisitorT>
concept page_operation = requires (ReaderT& reader, VisitorT& visitor) {
    traversal::for_each_page(0, reader, following {}, visitor);
};

static_assert(!page_operation<fallible_reader, other_error_visitor>);

} // anonymous namespace

TEST_CASE("page visitor failures retain the failing page and prior completed count")
{
    int reads = 0;
    const auto result = traversal::for_each_page(0, [&reads](const int state) {
        ++reads;

        return state;
    }, following {}, [](const int page) -> std::expected<void, std::unique_ptr<int>> {
        if (1 == page) {
            return std::unexpected(std::make_unique<int>(17));
        }

        return {};
    });
    REQUIRE_FALSE(result);
    CHECK(17 == *result.error().error);
    CHECK(1 == result.error().resume);
    CHECK(1 == result.error().pages);
    CHECK(2 == reads);
}

TEST_CASE("fallible page readers and visitors share errors and preserve stop semantics")
{
    const auto stopped = traversal::for_each_page(0, fallible_reader {}, following {},
        [](const int page) -> std::expected<traversal::page_action, int> {
            return 1 == page ? traversal::page_action::stop : traversal::page_action::next;
        });
    REQUIRE(stopped);
    CHECK(2 == stopped->pages);
    CHECK(2 == stopped->resume);
    CHECK(stopped->outcome == traversal::page_traversal_outcome::stopped);

    const auto failed = traversal::for_each_page(0, fallible_reader {}, following {},
        [](int) -> std::expected<traversal::page_action, int> { return std::unexpected(23); });
    REQUIRE_FALSE(failed);
    CHECK(23 == failed.error().error);
    CHECK(0 == failed.error().pages);
    CHECK(0 == failed.error().resume);

    const auto finished = traversal::for_each_page(0, fallible_reader {}, following {},
        [](int) -> std::expected<void, int> { return {}; });
    REQUIRE(finished);
    CHECK(finished->complete());
    CHECK(4 == finished->pages);
    CHECK_FALSE(finished->resume);
}

TEST_CASE("fallible scans preserve successful stop positions")
{
    using domain = datum::interval::comparison_domain<int>;
    using bound = datum::interval::boundary<domain>;
    const auto result = traversal::scan_range(std::array {1, 2, 3},
        [](const int row) -> std::expected<datum::acceptance, int> {
            return 2 == row ? datum::acceptance::accept_and_stop : datum::acceptance::accept;
        }, traversal::ordered_positions {[](const int row) { return bound::closed(row); }});
    REQUIRE(result);
    REQUIRE(result->resume);
    CHECK(2 == result->resume->key());
    CHECK_FALSE(result->resume->inclusive());
    CHECK(2 == result->progress.consumed());
}

TEST_CASE("page read failure skips visitation and terminal visitor failure stays a failure")
{
    int visits = 0;
    const auto read_failure = traversal::for_each_page(0,
        [](const int state) -> std::expected<int, int> {
            if (1 == state) {
                return std::unexpected(31);
            }

            return state;
        }, following {}, [&visits](int) -> std::expected<void, int> {
            ++visits;

            return {};
        });
    REQUIRE_FALSE(read_failure);
    CHECK(31 == read_failure.error().error);
    CHECK(1 == read_failure.error().pages);
    CHECK(1 == read_failure.error().resume);
    CHECK(1 == visits);

    const auto terminal_failure = traversal::for_each_page(0, fallible_reader {}, following {},
        [](const int page) -> std::expected<void, int> {
            if (3 == page) {
                return std::unexpected(37);
            }

            return {};
        });
    REQUIRE_FALSE(terminal_failure);
    CHECK(37 == terminal_failure.error().error);
    CHECK(3 == terminal_failure.error().pages);
    CHECK(3 == terminal_failure.error().resume);
}

namespace {

using const_decision = const std::expected<datum::acceptance, std::unique_ptr<int>>;
static_assert(datum::detail::acceptance_result<const_decision>);
static_assert(!datum::detail::acceptance_result<const_decision&>);
static_assert(!datum::detail::acceptance_result<const_decision&&>);

struct const_page_reader final
{
    const std::expected<std::unique_ptr<int>, std::unique_ptr<int>> operator()(const int state) const
    {
        if (2 == state) {
            return std::unexpected(std::make_unique<int>(42));
        }

        return std::make_unique<int>(state);
    }
};

struct const_page_visitor final
{
    const std::expected<void, std::unique_ptr<int>> operator()(std::unique_ptr<int>) const { return {}; }
};

struct borrowed_page_reader final
{
    std::expected<int, int>& operator()(int) const;
};

struct borrowed_page_visitor final
{
    std::expected<void, int>&& operator()(int) const;
};

static_assert(traversal::page_traversal_detail::page_reader<const_page_reader, int>);
static_assert(traversal::page_traversal_detail::page_visitor<const_page_visitor, std::unique_ptr<int>>);
static_assert(!traversal::page_traversal_detail::page_reader<borrowed_page_reader, int>);
static_assert(!traversal::page_traversal_detail::page_visitor<borrowed_page_visitor, int>);

static_assert([] {
    const auto result = datum::visit_range(std::array {1, 2}, [](const int row)
        -> const std::expected<datum::acceptance, int> {
        if (2 == row) {
            return std::unexpected(17);
        }

        return datum::acceptance::accept;
    });

    return !result and 17 == result.error().error and 1 == result.error().progress.accepted;
}());

} // anonymous namespace

TEST_CASE("range and scan results own const-qualified values without requiring copies")
{
    const auto decide = [](const int row) -> const_decision {
        if (2 == row) {
            return std::unexpected(std::make_unique<int>(17));
        }

        return datum::acceptance::accept;
    };
    const auto result = datum::visit_range(std::array {1, 2, 3}, decide);
    REQUIRE_FALSE(result);
    CHECK(17 == *result.error().error);
    CHECK(1 == result.error().progress.accepted);
    CHECK(2 == result.error().progress.offered);

    struct positions final
    {
        int position(const int row) const { return row; }
        int next(const int row, const bool consumed) const { return row + consumed; }
    };
    const auto scanned = traversal::scan_range(std::array {1, 2, 3}, decide, positions {});
    REQUIRE_FALSE(scanned);
    CHECK(17 == *scanned.error().error);
    CHECK(1 == scanned.error().progress.accepted);
    CHECK(2 == scanned.error().resume);
}

TEST_CASE("page traversal owns const-qualified reader and visitor results")
{
    const auto next = [](const int state, std::unique_ptr<int>&) { return std::optional {state + 1}; };
    const auto read_failure = traversal::for_each_page(0, const_page_reader {}, next, const_page_visitor {});
    REQUIRE_FALSE(read_failure);
    CHECK(42 == *read_failure.error().error);
    CHECK(2 == read_failure.error().pages);
    CHECK(2 == read_failure.error().resume);

    const auto visit_failure = traversal::for_each_page(0, const_page_reader {}, next,
        [](std::unique_ptr<int> page) -> const std::expected<traversal::page_action, std::unique_ptr<int>> {
            if (1 == *page) {
                return std::unexpected(std::make_unique<int>(77));
            }

            return traversal::page_action::next;
        });
    REQUIRE_FALSE(visit_failure);
    CHECK(77 == *visit_failure.error().error);
    CHECK(1 == visit_failure.error().pages);
    CHECK(1 == visit_failure.error().resume);
}

namespace {

struct explicit_boolean final
{
    bool value;

    explicit operator bool() const { return value; }
};

struct minimal_page_state final
{
    int value;

    explicit minimal_page_state(const int input) : value(input)
    {}
    minimal_page_state(const minimal_page_state&) = delete;
    minimal_page_state(minimal_page_state&&) = default;
    void operator=(minimal_page_state&& other) { value = other.value; }
    explicit_boolean operator==(const minimal_page_state& other) const
    {
        return {value == other.value};
    }
    friend bool operator==(minimal_page_state&, const minimal_page_state&) = delete;
};

static_assert(std::move_constructible<minimal_page_state>);
static_assert(std::is_assignable_v<minimal_page_state&, minimal_page_state>);
static_assert(!std::assignable_from<minimal_page_state&, minimal_page_state>);
static_assert(!std::equality_comparable<minimal_page_state>);

template <typename PositionT>
concept scannable_position = requires(scan_contract_test::positions<PositionT> positions) {
    traversal::scan_range(std::array {1}, [](int) { return datum::acceptance::accept_and_stop; },
                          positions);
};

static_assert(std::is_move_constructible_v<scan_contract_test::owned_position>);
static_assert(!std::move_constructible<scan_contract_test::owned_position>);
static_assert(scannable_position<scan_contract_test::owned_position>);
static_assert(!scannable_position<scan_contract_test::pinned_position>);

} // anonymous namespace

TEST_CASE("page state needs only assignment and const Boolean equality")
{
    int visits = 0;
    const auto read = [](const minimal_page_state& state) { return state.value; };
    const auto result = traversal::for_each_page(minimal_page_state {0}, read,
        [](const minimal_page_state& state, int&) -> const std::optional<minimal_page_state> {
            if (2 == state.value) {
                return std::nullopt;
            }

            return std::optional<minimal_page_state> {std::in_place, state.value + 1};
        }, [&visits](int) { ++visits; });
    CHECK(result.complete());
    CHECK(3 == result.pages);
    CHECK(3 == visits);

    CHECK_THROWS_AS(traversal::for_each_page(minimal_page_state {0}, read,
        [](const minimal_page_state& state, int&) -> const std::optional<minimal_page_state> {
            return std::optional<minimal_page_state> {std::in_place, state.value};
        }, [&visits](int) { ++visits; }), std::logic_error);
    CHECK(3 == visits);
}

TEST_CASE("scan positions own const prvalues with explicit move construction")
{
    using position = scan_contract_test::owned_position;
    const auto stopped = traversal::scan_range(std::array {1, 2},
        [](int) { return datum::acceptance::accept_and_stop; },
        scan_contract_test::positions<position> {});
    REQUIRE(stopped.resume);
    CHECK(2 == stopped.resume->value);
    CHECK(1 == stopped.progress.accepted);

    const auto failed = traversal::scan_range(std::array {1, 2, 3},
        [](const int row) -> const std::expected<datum::acceptance, std::unique_ptr<int>> {
            if (2 == row) {
                return std::unexpected(std::make_unique<int>(17));
            }

            return datum::acceptance::accept;
        }, scan_contract_test::positions<position> {});
    REQUIRE_FALSE(failed);
    CHECK(2 == failed.error().resume.value);
    CHECK(1 == failed.error().progress.accepted);
    CHECK(17 == *failed.error().error);
}
