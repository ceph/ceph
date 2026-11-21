// Plan mapped reads from value-producing layout descriptors and logical selections.
#include <datum/interval/clip.h>

#include <array>
#include <print>
#include <ranges>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

struct object_extent final
{
    unsigned object;
    datum::byte_mapping geometry;
};

int main()
{
    const std::array objects {object_extent {7, {{10, 8}, 100}},
                              object_extent {9, {{24, 4}, 200}}};
    auto layout = objects | std::views::transform([](const auto& source) { return source; });
    const std::array wanted {datum::byte_extent {12, 3}, datum::byte_extent {16, 10}};

    const auto result = iv::visit_overlaps(layout, wanted,
        [](const auto& source, const auto&) {
            const datum::byte_extent selected {source.lower().key(),
                                               source.upper().key() - source.lower().key()};
            const auto mapped = datum::clip(source.source().geometry, selected).value();
            std::println("object {}: logical {}, backing {}, length {}", source.source().object,
                         mapped.logical.offset, mapped.backing_offset, mapped.logical.length);

            return datum::acceptance::accept;
        }, [](const object_extent& source) { return source.geometry.logical; });

    std::println("pieces: {}", result.accepted);
}
