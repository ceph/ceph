// Encode and decode a postcard's recipient and message, excluding its local tracking number.

#include <print>
#include <string>

#include <datum/codec/zpp.h>

namespace datum = ::ceph::salad::datum;

struct postcard final
{
    std::string recipient;
    std::string message;
    int local_id = 0;
};

// Select the transmitted fields and their wire order:
using postcard_wire = datum::binary_fields<&postcard::recipient, &postcard::message>;

int main()
{
    const postcard sent {"Juniper", "wish you were here", 7};

    // Round-trip the selected fields; local_id is outside this schema:
    const auto bytes = datum::render::to_bytes<postcard_wire>(sent);
    const auto received = datum::render::from_bytes<postcard_wire>(bytes);

    std::println("to: {}", received.recipient);
    std::println("message: {}", received.message);
    std::println("local id: {} -> {}", sent.local_id, received.local_id);
}
