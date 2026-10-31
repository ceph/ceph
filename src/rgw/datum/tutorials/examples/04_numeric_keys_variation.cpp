// Encode integer keys so byte order matches numeric order.

#include <array>
#include <print>
#include <ranges>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>

#include <datum/key.h>

namespace datum = ::ceph::salad::datum;
namespace key = datum::key;

int main()
{
    using keyed_number = std::pair<key::encoded_key, std::uint16_t>;

    const std::array<std::uint16_t, 3> numbers {16, 2, 15};

    // Decimal segments sort as text:
    auto decimal_keys = numbers | std::views::transform([](const auto number) {
        return keyed_number {key::compose("item", key::decimal_segment(number)), number};
    }) | std::ranges::to<std::vector>();

    // Equal-width hexadecimal segments preserve unsigned numeric order:
    auto numeric_keys = numbers | std::views::transform([](const auto number) {
        return keyed_number {key::compose("item", key::fixed_width_hexadecimal_segment(number)), number};
    }) | std::ranges::to<std::vector>();

    // Sort by encoded bytes, keeping each number attached to its key:
    std::ranges::sort(decimal_keys, {}, &keyed_number::first);
    std::ranges::sort(numeric_keys, {}, &keyed_number::first);

    std::println("decimal: {:n}", decimal_keys | std::views::values);
    std::println("fixed-width hex: {:n}", numeric_keys | std::views::values);
}
