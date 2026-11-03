// Use Datum byte geometry to copy bytes from a Ceph bufferlist and retain a native slice.

#include <array>
#include <print>
#include <utility>
#include <string_view>

#include <ceph-datum.h>

namespace datum = ceph::salad::datum;

int main()
{
    // Native Ceph segments make the fragment boundary explicit:
    ceph::buffer::ptr head = ceph::buffer::copy("Miso: m", 7);
    ceph::buffer::ptr tail = ceph::buffer::copy("eow", 3);
    ceph::buffer::list stored;
    stored.append(std::move(head));
    stored.append(std::move(tail));
    std::println("stored: {} bytes in {} buffers", stored.length(), stored.get_num_buffers());

    // The list holds object bytes 100..109, starting at list offset zero:
    const datum::byte_mapping mapping {{100, 10}, 0};
    const datum::byte_extent wanted {106, 4};
    const auto selected = datum::clip(mapping, wanted);
    if (!selected) {
        return 1;
    }

    // List offset 6 selects the last byte of head, then all three bytes of tail:
    const datum::byte_extent bytes {selected->backing_offset, selected->logical.length};
    std::array<char, 4> output {};
    datum::copy(stored, bytes, output);

    // This is another Ceph bufferlist, retaining references to the selected payloads:
    const ceph::buffer::list retained = datum::slice(stored, bytes);
    stored.clear();

    std::println("copied: {}", std::string_view {output});
    std::println("retained: {}", retained.to_str());
}
