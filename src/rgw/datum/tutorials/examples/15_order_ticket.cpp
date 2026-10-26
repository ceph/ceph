// Store a byte selection and quota together, then explicitly take up to that many covered bytes.

#include <print>
#include <cstddef>

#include <datum/interval/metric.h>
#include <datum/plan/query.h>

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;
namespace plan = datum::plan;

struct byte_positions final : iv::comparison_domain<std::size_t> {};

int main()
{
    using interval = iv::query<byte_positions>;

    // Store the selection and quota without applying the quota:
    const auto order = plan::with_control(
        plan::make_query(interval::closed_open(0, 8)), std::size_t {5});
    const iv::canonical_set coverage {order.intervals};
    const auto quota = plan::control<std::size_t>(order);
    const iv::unsigned_metric<std::size_t> byte_metric;

    std::println("requested: {} bytes", iv::measure(coverage, byte_metric));
    std::println("quota: {} bytes", quota);

    // This example interprets its size_t control as a covered-byte quota:
    const auto selected = iv::take(coverage, quota, byte_metric);
    for (const auto& span : selected) {
        std::println("[{}, {})", span.lower().key(), span.upper().key());
    }

    std::println("served: {} bytes", iv::measure(selected, byte_metric));
}
