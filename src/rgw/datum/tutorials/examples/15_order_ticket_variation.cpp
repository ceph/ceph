// Store a byte selection and quota, then select a bounded plan at compile time:

#include <print>
#include <cstdint>

#include <datum/interval/metric.h>
#include <datum/plan/query.h>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;
namespace plan = datum::plan;

int main()
{
    using interval = iv::byte_query;

    // Store the selection and quota without applying the quota:
    constexpr auto order = plan::with_control(
        plan::make_query(interval::closed_open(0, 8)), std::uint64_t {10});
    constexpr auto coverage = plan::compile_intervals(order);
    constexpr auto quota = plan::control<std::uint64_t>(order);

    std::println("requested: {} bytes", iv::measure(coverage, iv::bytes));
    std::println("quota: {} bytes", quota);

    // This example interprets its uint64_t control as a covered-byte quota:
    constexpr auto selected = iv::take(order.intervals, quota, iv::bytes);
    static_assert(iv::measure(selected, iv::bytes) == 8);
    for (const auto& span : selected) {
        std::println("[{}, {})", span.lower().key(), span.upper().key());
    }

    std::println("served: {} bytes", iv::measure(selected, iv::bytes));
}
