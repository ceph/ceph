// Keep a string and slice descriptor together so the slice can be recomputed after a move.

#include <print>
#include <string>
#include <utility>
#include <string_view>

#include <datum/bytes.h>
#include <datum/ownership.h>

namespace datum = ::ceph::salad::datum;

int main()
{

    // Store the owner together with an offset-and-length lens:
    datum::retained_view message {std::string {"Miso: meow"}, datum::byte_slice {0, 4}};
    std::println("before move: {}", std::string_view {message.get()});

    // Move the retained value, then recompute its slice:
    const auto moved = std::move(message);
    std::println("after move: {}", std::string_view {moved.get()});
}
