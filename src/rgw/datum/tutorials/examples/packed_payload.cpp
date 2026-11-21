// Select logical bytes from a fragmented packed payload while retaining sparse coordinates.
#include <datum/bytes.h>
#include <datum/interval/clip.h>

#include <array>
#include <print>
#include <string>
#include <string_view>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

int main()
{
    const std::array origins {datum::byte_extent {0, 4}, datum::byte_extent {10, 8},
                              datum::byte_extent {18, 4}, datum::byte_extent {40, 3}};
    const std::array wanted {datum::byte_extent {12, 2}, datum::byte_extent {16, 1},
                             datum::byte_extent {42, 1}};
    const std::array fragments {std::string_view {"ab"}, std::string_view {},
                                std::string_view {"cdefgh"}, std::string_view {"ijklmnopqrs"}};
    datum::byte_cursor payload {fragments};
    std::string selected;

    const auto result = iv::visit_packed_overlaps(origins, wanted,
        [&](const auto&, const auto&, const datum::byte_mapping mapped) {
            datum::write_fragments(
                payload.take(datum::byte_extent {mapped.backing_offset, mapped.logical.length}),
                [&](const auto bytes) { selected.append(bytes.data(), bytes.size()); });
            std::println("logical {}, packed {}, length {}", mapped.logical.offset,
                         mapped.backing_offset, mapped.logical.length);

            return datum::acceptance::accept;
        });

    std::println("selected: {}", selected);
    std::println("pieces: {}", result.accepted);
}
