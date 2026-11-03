// Work out the logical read ranges around one cached block.
#include <datum/interval/metric.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <vector>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

int main()
{
    const datum::byte_extent wanted {0, 64};
    const datum::byte_extent held {16, 32};
    const iv::byte_query requested = iv::bounds(wanted);
    const iv::byte_query cached = iv::bounds(held);
    std::vector<datum::byte_extent> calls;
    auto read = [&](std::uint64_t offset, std::uint64_t length) { calls.emplace_back(offset, length); };

    const auto wanted_start = wanted.offset;
    const auto wanted_stop = datum::end_offset(wanted);
    const auto cache_start = held.offset;
    const auto cache_stop = datum::end_offset(held);

    // Explicit gap checks for a nonempty requested range and cached range.
    if (wanted_start < cache_start) {
        read(wanted_start, std::min(cache_start, wanted_stop) - wanted_start);
    }
    if (cache_stop < wanted_stop) {
        const auto start = std::max(cache_stop, wanted_start);
        read(start, wanted_stop - start);
    }
    const auto explicit_calls = calls;
    calls.clear();

    // The same byte ranges, calculated by subtraction.
    const auto missing = iv::difference(requested, cached);
    for (const auto& part : iv::compile_intervals<2>(missing)) {
        read(part.lower().key(), part.upper().key() - part.lower().key());
    }
    assert(calls == explicit_calls);
    assert((calls == std::vector<datum::byte_extent> {{0, 16}, {48, 16}}));
    for (const auto& [offset, length] : calls) {
        std::cout << "read " << offset << ".." << offset + length - 1
                  << " (" << length << " bytes)\n";
    }
}
