// Select a prefix range and compose ordinary Ranges ordering for listing records.
#include <datum/compare.h>
#include <datum/key.h>

#include <array>
#include <print>
#include <ranges>
#include <cassert>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <string_view>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

struct listed_object final
{
    std::string_view name;
    std::uint64_t generation;
};

int main()
{
    using namespace std::string_view_literals;

    const std::array names {
        "notes/01"sv, "photos/10"sv, "photos/20"sv, "photos/30"sv,
        "photos/70"sv, "photos/80"sv, "photos/90"sv, "videos/01"sv
    };
    auto selected = datum::key::byte_query::prefix("photos/");
    selected = datum::key::starting_after(selected, "photos/20");
    selected = datum::key::ending_before(selected, "photos/80");
    const auto intervals = iv::compile_intervals<1>(selected);
    assert(intervals.size() == 1);

    std::ranges::for_each(names | std::views::filter([&selected](const auto name) {
        return iv::contains(selected, name);
    }), [](const auto name) { std::println("{}", name); });

    std::array objects {
        listed_object {"photos/70", 4},
        listed_object {"photos/30", 2},
        listed_object {"photos/30", 9}
    };

    const auto unsorted = objects;
    std::ranges::sort(objects, [](const auto& lhs, const auto& rhs) {
        if (lhs.name != rhs.name) {
            return lhs.name < rhs.name;
        }
        return lhs.generation > rhs.generation;
    });
    const auto explicit_order = objects;
    objects = unsorted;

    std::ranges::sort(objects, datum::order_by(
        datum::ascending(&listed_object::name),
        datum::descending(&listed_object::generation)));
    assert(std::ranges::equal(objects, explicit_order, [](const auto& lhs, const auto& rhs) {
        return lhs.name == rhs.name and lhs.generation == rhs.generation;
    }));
    for (const auto& object : objects) {
        std::println("{}@{}", object.name, object.generation);
    }

    // A single tuple key needs only a standard algorithm projection.
    std::array pairs {std::pair {"photos/70"sv, 4}, std::pair {"photos/30"sv, 2}};
    std::ranges::sort(pairs, {}, datum::nth_key<0>);
    assert(pairs.front().first == "photos/30");
}
