// Encode and decode a postcard's recipient and message, excluding its local tracking number.

#include <print>
#include <string>

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

    // Round-trip the selected fields; local_id is outside this schema:
    const auto bytes = datum::to_bytes<postcard_wire>(sent);
    const auto received = datum::from_bytes<postcard_wire>(bytes);

    std::println("to: {}", received.recipient);
    std::println("message: {}", received.message);
    std::println("local id: {} -> {}", sent.local_id, received.local_id);
}
