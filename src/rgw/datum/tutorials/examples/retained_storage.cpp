// Retain coverage with inferred storage, a hard entry limit, or a common dynamic type:
#include <datum/interval/metric.h>
#include <datum/plan/query.h>

#include <array>
#include <cstdint>
#include <print>
#include <vector>

namespace iv = ceph::salad::datum::interval;
namespace plan = ceph::salad::datum::plan;

constexpr auto previous = iv::byte_query::closed_open(0, 8);
constexpr auto desired = iv::byte_query::closed_open(5, 12);
constexpr auto changes = previous ^ desired;
constexpr auto changed = iv::compile_intervals(changes);
static_assert(changed.size() == 2 and iv::measure(changed, iv::bytes) == 9);

// The limit applies after normalization: two overlapping inputs fit in one output slot:
constexpr std::array overlapping {previous, desired};
constexpr auto limited = iv::compile_intervals<1>(overlapping);
static_assert(limited.size() == 1 and limited[0] == iv::byte_query::closed_open(0, 12));

constexpr auto batch = iv::take(changed, std::uint64_t {7}, iv::bytes);
static_assert(batch.size() == 2 and iv::measure(batch, iv::bytes) == 7);
constexpr auto order = plan::make_query(changes);
constexpr auto planned = plan::compile_intervals(order);
static_assert(iv::measure(planned, iv::bytes) == 9);

int main()
{
    // Different expression shapes share one owning type when dynamic storage is explicit:
    const std::array selections {
        iv::compile_intervals(previous, iv::dynamic_storage),
        iv::compile_intervals(changes, iv::dynamic_storage),
        plan::compile_intervals(order, iv::dynamic_storage)
    };
    const auto dynamic_batch = iv::take(changed, std::uint64_t {7}, iv::bytes, iv::dynamic_storage);

    // A vector's runtime size supplies no static bound, so its default result is dynamic:
    const std::vector runtime_entries {previous, desired};
    const auto runtime_coverage = iv::compile_intervals(runtime_entries);

    std::println("automatic: {} intervals, {} bytes", changed.size(), iv::measure(changed, iv::bytes));
    std::println("limit 1 after normalization: {} interval", limited.size());
    std::println("common dynamic type: {} selections", selections.size());
    std::println("runtime source: {} interval", runtime_coverage.size());
    std::println("quota: {} bytes, dynamic quota: {} bytes",
                 iv::measure(batch, iv::bytes), iv::measure(dynamic_batch, iv::bytes));
    std::println("resolved plan: {} bytes", iv::measure(planned, iv::bytes));
}
