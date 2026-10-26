// Clip records to selected intervals and identify the source of each resulting cut.

#include <array>
#include <print>
#include <cstddef>
#include <string_view>

#include <datum/interval/clip.h>

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

struct byte_positions final : iv::comparison_domain<std::size_t> {};

struct text_record final
{
    std::string_view name;
    std::size_t offset;
    std::string_view text;
};

int main()
{
    using interval = iv::query<byte_positions>;

    const std::array records {
        text_record {"Miso", 0, "zzz meow"},
        text_record {"Mochi", 8, "purr zzz"}
    };
    const iv::canonical_set selection {interval::closed_open(4, 12)};

    // Keep the source record attached to each clipped interval:
    const iv::clip_view cuts {records, selection, [](const text_record& row) {
        return interval::closed_open(row.offset, row.offset + row.text.size());
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
