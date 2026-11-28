// Copy fragmented bytes into ordinary caller-owned storage.

#include <array>
#include <print>
#include <ranges>
#include <string>
#include <cassert>
#include <string_view>

#include <datum/bytes.h>
#include <datum/range.h>

namespace datum = ceph::salad::datum;

int main()
{
    using namespace std::string_view_literals;

    const std::array fragments {"hello, w"sv, "orld!\n"sv};

    std::array<char, 14> greeting {};

    // Copy the complete source after checking output capacity:
    datum::copy(fragments, greeting);
    std::print("{}", std::string_view {greeting});

    // Grow a string from a joined view when the destination size is not fixed:
    std::string contiguous;
    datum::append_range(contiguous, fragments | std::views::join);
    assert(contiguous == std::string_view {greeting});
}
