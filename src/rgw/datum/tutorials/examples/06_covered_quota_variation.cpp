// Take a bounded covered-byte quota during constant evaluation:

#include <print>
#include <cstdint>

#include <datum/interval/metric.h>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

int main()
{
    using interval = iv::byte_query;

    // Retain two canonical pieces for measurement and selection; gaps consume no quota:
    constexpr auto coverage = iv::compile_intervals<2>(iv::set_union(
        interval::closed_open(0, 3),
        interval::closed_open(10, 15)
    ));

    constexpr std::uint64_t quota = 10;

    // Count only covered bytes toward the quota:
    constexpr auto selected = iv::take<2>(coverage, quota, iv::bytes);
    static_assert(iv::measure(selected, iv::bytes) == 8);

    std::println("on the menu: {} bytes", iv::measure(coverage, iv::bytes));
    std::println("order: {} bytes", quota);

    for (const auto& span : selected) {
        std::println("[{}, {})", span.lower().key(), span.upper().key());
    }

    std::println("served: {} bytes", iv::measure(selected, iv::bytes));
}
