// Build and check a complete bounded read plan during C++23 constant evaluation:
#include <datum/interval/metric.h>

#include <print>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

// WG21 N4950 [expr.const]: https://timsong-cpp.github.io/cppwp/n4950/expr.const
// Inline entries can survive C++23 constant evaluation:
constexpr auto reads = iv::take<2>(
    iv::difference(iv::byte_query::closed_open(0, 15), iv::byte_query::closed_open(3, 10)),
    5, iv::bytes);

static_assert(reads.size() == 2);
static_assert(reads[0] == iv::byte_query::closed_open(0, 3));
static_assert(reads[1] == iv::byte_query::closed_open(10, 12));
static_assert(iv::measure(reads, iv::bytes) == 5);

int main()
{
    for (const auto& part : reads) {
        const auto bytes = iv::extent_of(part);
        std::println("read {}, length {}", bytes.offset, bytes.length);
    }

    std::println("planned: {} bytes", iv::measure(reads, iv::bytes));
}
