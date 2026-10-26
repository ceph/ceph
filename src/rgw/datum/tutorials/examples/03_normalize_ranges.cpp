// Merge overlapping intervals and report their coverage in order.

#include <array>
#include <print>

#include <datum/interval/query.h>

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

struct positions final : iv::comparison_domain<int> {};

int main()
{
    const std::array ranges {
        iv::closed_between<positions>(8, 10),
        iv::closed_between<positions>(1, 4),
        iv::closed_between<positions>(3, 6)
    };

    // Merge overlaps and put the covered intervals in order:
    const iv::canonical_set coverage {ranges};

    for (const auto& interval : coverage) {
        std::println("[{}, {}]", interval.lower().key(), interval.upper().key());
    }
}
