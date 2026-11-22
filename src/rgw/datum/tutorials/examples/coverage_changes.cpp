// Select changed byte coverage without performing reads, writes, or repair.
#include <datum/interval/metric.h>

#include <cstdint>
#include <print>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

constexpr auto previous = iv::bounds({0, 8});
constexpr auto desired = iv::bounds({5, 7});

// N4950 [expr.const]: https://timsong-cpp.github.io/cppwp/n4950/expr.const
// Inline canonical storage retains the complete result during C++23 constant evaluation:
constexpr auto changed = iv::compile_intervals(previous ^ desired);
static_assert(changed.size() == 2);
static_assert(changed[0] == iv::bounds({0, 5}));
static_assert(changed[1] == iv::bounds({8, 4}));
static_assert(iv::measure(changed, iv::bytes) == 9);
static_assert(iv::contains(desired - previous, std::uint64_t {8}));
static_assert(iv::contains(previous - desired, std::uint64_t {0}));
static_assert(iv::is_empty_expression((previous ^ desired) ^ (desired ^ previous)));

int main()
{
    for (const auto& part : changed) {
        const auto extent = iv::extent_of(part);
        std::println("changed {}, length {}", extent.offset, extent.length);
    }

    std::println("components: {}, bytes: {}", changed.size(), iv::measure(changed, iv::bytes));
}
