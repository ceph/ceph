// Copy a Ceph byte range into an independent bufferlist, then read its fragments with core copy.

#include <array>
#include <print>
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
    std::array<char, 4> output {};
    datum::copy(fragments, output);

    std::println("snapshot: {}", std::string_view {output});
}
