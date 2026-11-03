// Use a metric to take up to a quota of covered bytes without counting gaps.

#include <array>
#include <print>
#include <cstdint>

#include <datum/interval/metric.h>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

int main()
{
    using interval = iv::byte_query;

    // A covered byte has a position inside one of these intervals; gaps consume no quota.
    const std::array ranges {
        interval::closed_open(0, 3),
        interval::closed_open(10, 15)
    };
    const auto coverage = iv::compile_intervals(ranges);

    const std::uint64_t quota = 5;

    // Count only covered bytes toward the quota:
    const auto selected = iv::take(coverage, quota, iv::bytes);

    std::println("on the menu: {} bytes", iv::measure(coverage, iv::bytes));
    std::println("order: {} bytes", quota);

    for (const auto& span : selected) {
        std::println("[{}, {})", span.lower().key(), span.upper().key());
    }

    std::println("served: {} bytes", iv::measure(selected, iv::bytes));
}
