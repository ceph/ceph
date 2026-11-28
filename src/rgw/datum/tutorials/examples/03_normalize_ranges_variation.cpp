// Merge overlapping intervals and report their coverage in order.

#include <array>
#include <print>
#include <ranges>

#include <datum/interval/range.h>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

struct positions final : iv::comparison_domain<int> {};

int main()
{
    const std::array ranges {
        iv::closed_between<positions>(8, 10),
        iv::closed_between<positions>(1, 4),
        iv::closed_between<positions>(3, 6)
    };

    // Add a bridging interval to a bounded collection:
    iv::interval_batch<positions, 4> pending {ranges};
    pending.append_range(std::views::single(iv::closed_between<positions>(6, 8)));

    // Merge overlaps and put the covered intervals in order:
    const auto coverage = iv::compile_intervals(pending);

    for (const auto& interval : coverage) {
        std::println("[{}, {}]", interval.lower().key(), interval.upper().key());
    }
}
