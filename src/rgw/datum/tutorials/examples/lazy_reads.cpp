// Observe missing coverage as a standard range and stop after two planned reads:
#include <datum/interval/metric.h>
#include <datum/interval/range.h>

#include <array>
#include <print>
#include <ranges>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

int main()
{
    constexpr auto requested = iv::byte_query::closed_open(0, 24);
    const std::array cached_bounds {iv::byte_query::closed_open(4, 8),
                                    iv::byte_query::closed_open(12, 20)};
    const auto cached = iv::compile_intervals(cached_bounds);
    auto reads = iv::observe_intervals(iv::difference(requested, cached))
        | std::views::transform([](const auto& value) { return iv::extent_of(value); });
    unsigned planned = 0;
    const auto progress = datum::visit_range(reads, [&](const datum::byte_extent value) {
        std::println("read {}, length {}", value.offset, value.length);
        ++planned;
        if (2 == planned) {
            return datum::acceptance::accept_and_stop;
        }

        return datum::acceptance::accept;
    });
    std::println("planned: {}", progress.accepted);
}
