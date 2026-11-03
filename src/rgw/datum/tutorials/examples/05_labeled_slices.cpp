// Clip records to selected intervals and identify the source of each resulting cut.

#include <array>
#include <print>
#include <cstdint>
#include <string_view>

#include <datum/interval/clip.h>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

struct text_record final
{
    std::string_view name;
    std::uint64_t offset;
    std::string_view text;
};

int main()
{
    using interval = iv::byte_query;

    const std::array records {
        text_record {"Miso", 0, "zzz meow"},
        text_record {"Mochi", 8, "purr zzz"}
    };
    const auto selection = iv::compile_intervals(interval::closed_open(4, 12));

    // Keep the source record attached to each clipped interval:
    const iv::clip_view cuts {records, selection, [](const text_record& row) {
        return datum::byte_extent {row.offset, row.text.size()};
    }};

    for (const auto& cut : cuts) {
        const auto& record = cut.source();
        const auto lower = cut.lower().key();
        const auto upper = cut.upper().key();

        // Convert global cut bounds to a substring within that source record:
        std::println("{} [{}, {}): \"{}\"", record.name, lower, upper,
                     record.text.substr(lower - record.offset, upper - lower));
    }
}
