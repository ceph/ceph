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
