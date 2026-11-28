// Retain an independent Ceph snapshot and scatter its bytes into split ordinary storage:

#include <array>
#include <print>
#include <span>
#include <string_view>

#include <datum/bytes.h>
#include <ceph-datum.h>

namespace datum = ceph::salad::datum;

int main()
{
    ceph::buffer::list source;
    source.append("Miso: meow", 10);

    // Copying a native extent into a new list gives it independent payload:
    const ceph::buffer::list snapshot = datum::copy(source, datum::byte_extent {6, 4});
    source.clear();

    // A standard view of spans lets the pure core consume the native list's bytes:
    const auto fragments = datum::fragments(snapshot);
    std::array<char, 1> first {};
    std::array<char, 4> last {'.', '.', '.', '.'};
    const std::array<std::span<char>, 2> output {std::span {first}, std::span {last}};
    const auto copied = datum::scatter_bytes(fragments, output);

    std::println("snapshot: {}{}", std::string_view {first}, std::string_view {last}.substr(0, 3));

    return copied == 4 and last.back() == '.' ? 0 : 1;
}
