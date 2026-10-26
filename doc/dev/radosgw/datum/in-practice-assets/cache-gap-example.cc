// Work out the logical read ranges around one cached block.
#include <datum/interval/query.h>

#include <algorithm>
#include <cassert>
#include <iostream>
#include <utility>
#include <vector>

namespace iv = ::ceph::salad::datum::interval;

struct byte_offsets : iv::comparison_domain<unsigned> {};
using ByteRange = iv::query<byte_offsets>;

int main()
{
    const unsigned wanted_start = 0;
    const unsigned wanted_stop = 64;
    const unsigned cache_start = 16;
    const unsigned cache_stop = 48;
    const auto requested = ByteRange::closed_open(wanted_start, wanted_stop);
    const auto cached = ByteRange::closed_open(cache_start, cache_stop);
    std::vector<std::pair<unsigned, unsigned>> calls;
    auto read = [&](unsigned offset, unsigned length) { calls.emplace_back(offset, length); };

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
    assert((calls == std::vector<std::pair<unsigned, unsigned>> {{0, 16}, {48, 16}}));
    for (const auto& [offset, length] : calls) {
        std::cout << "read " << offset << ".." << offset + length - 1
                  << " (" << length << " bytes)\n";
    }
}
