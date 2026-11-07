// Plan storage reads around cached data, with a limit on bytes read in one batch.
#include <datum/interval/metric.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <iostream>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

int main()
{
    constexpr std::uint64_t MiB = 1024 * 1024;
    const std::array request_ranges {
        iv::bounds({0, 16 * MiB}),
        iv::bounds({8 * MiB, 16 * MiB}),
        iv::bounds({40 * MiB, 16 * MiB})
    };
    const std::array cache_ranges {
        iv::bounds({4 * MiB, 8 * MiB}),
        iv::bounds({20 * MiB, 4 * MiB}),
        iv::bounds({44 * MiB, 8 * MiB})
    };

    const auto requested = iv::compile_intervals(request_ranges);
    const auto cached = iv::compile_intervals(cache_ranges);
    const auto missing = iv::compile_intervals(iv::difference(requested, cached));
    const auto batch = iv::take(missing, 14 * MiB, iv::bytes);

    std::ranges::for_each(batch, [](const auto& part) {
        std::cout << '[' << part.lower().key() / MiB << ", "
                  << part.upper().key() / MiB << ") MiB\n";
    });
}
