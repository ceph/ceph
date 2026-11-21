// Copy a packed payload selection into independently segmented writable storage.
#include <datum/bytes.h>

#include <array>
#include <print>
#include <span>
#include <string_view>

namespace datum = ceph::salad::datum;

int main()
{
    const std::array source {std::string_view {"ab"}, std::string_view {"cdef"},
                             std::string_view {"gh"}};
    datum::byte_cursor payload {source};
    const auto selected = payload.take(datum::byte_extent {1, 6});
    std::array<char, 2> first {};
    std::array<char, 5> last {'.', '.', '.', '.', '.'};
    const std::array<std::span<char>, 2> destination {std::span {first}, std::span {last}};

    const auto copied = datum::scatter_bytes(selected, destination);
    std::println("copied: {}", copied);
    std::println("first: {}", std::string_view {first});
    std::println("last: {}", std::string_view {last});
}
