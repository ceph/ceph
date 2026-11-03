// Encode and decode a postcard's recipient and message, excluding its local tracking number.

#include <array>
#include <print>
#include <string>
#include <cstddef>

#include <datum/codec/zpp.h>
#include <datum/record_definition.h>

namespace datum = ceph::salad::datum;

#define POSTCARD_FIELDS(Field) \
    Field(std::string, recipient) \
    Field(std::string, message) \
    Field(int, local_id, 0)
DATUM_RECORD(postcard, POSTCARD_FIELDS);
#undef POSTCARD_FIELDS

// Select the transmitted fields and their wire order:
using postcard_wire = datum::binary_fields<&postcard::recipient, &postcard::message>;

int main()
{
    const postcard sent {"Juniper", "wish you were here", 7};

    // Encode into existing byte storage, returning only the written part:
    std::array<std::byte, 64> storage {};
    const auto bytes = datum::to_bytes<postcard_wire>(sent, storage);

    // Update only the selected fields in an existing record:
    postcard received {"old recipient", "old message", 42};
    datum::from_bytes<postcard_wire>(bytes, received);

    std::println("to: {}", received.recipient);
    std::println("message: {}", received.message);
    std::println("local id: {} -> {}", sent.local_id, received.local_id);
}
