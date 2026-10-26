// Read bytes across separate fragments without joining them into one buffer.

#include <array>
#include <print>
#include <string_view>

#include <datum/bytes.h>

namespace datum = ::ceph::salad::datum;

int main()
{
    using namespace std::string_view_literals;

    const std::array fragments {"hell"sv, "o, w"sv, "orld"sv, "!\n"sv};

    // Read all 14 bytes across the fragment boundaries:
    datum::byte_cursor input {fragments};
    const auto greeting = input.take(14);

    // Send each borrowed span to the output sink:
    datum::write_fragments(greeting, [](const auto block) {
        std::print("{}", std::string_view {block});
    });
}
