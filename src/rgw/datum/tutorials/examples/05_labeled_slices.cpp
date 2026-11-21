// Visit selected record pieces while preserving the source of each cut:

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
    const std::array records {
        text_record {"Miso", 0, "zzz meow"},
        text_record {"Mochi", 8, "purr zzz"}
    };
    const std::array selection {datum::byte_extent {4, 8}};

    // Keep the source record attached to each clipped interval:
    const auto progress = iv::visit_overlaps(records, selection, [](const auto& cut, const auto&) {
        const auto& record = cut.source();
        const auto lower = cut.lower().key();
        const auto upper = cut.upper().key();

        // Convert global cut bounds to a substring within that source record:
        std::println("{} [{}, {}): \"{}\"", record.name, lower, upper,
                     record.text.substr(lower - record.offset, upper - lower));
        return datum::acceptance::accept;
    }, [](const text_record& row) {
        return datum::byte_extent {row.offset, row.text.size()};
    });

    return progress.accepted == 2 ? 0 : 1;
}
