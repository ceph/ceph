// Use a metric to take up to a quota of covered bytes without counting gaps.

#include <array>
#include <print>
#include <cstddef>

#include <datum/interval/metric.h>

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

struct byte_positions final : iv::comparison_domain<std::size_t> {};

int main()
{
    using interval = iv::query<byte_positions>;

    // A covered byte has a position inside one of these intervals; gaps consume no quota.
    const std::array ranges {
        interval::closed_open(0, 3),
        interval::closed_open(10, 15)
    };
    const iv::canonical_set coverage {ranges};

    const iv::unsigned_metric<std::size_t> byte_metric;
    const std::size_t quota = 5;

    // Count only covered bytes toward the quota:
    const auto selected = iv::take(coverage, quota, byte_metric);

    std::println("on the menu: {} bytes", iv::measure(coverage, byte_metric));
    std::println("order: {} bytes", quota);

    for (const auto& span : selected) {
        std::println("[{}, {})", span.lower().key(), span.upper().key());
    }

    std::println("served: {} bytes", iv::measure(selected, byte_metric));
}
