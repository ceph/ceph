// Select an interval of values and exclude a value with interval difference.

#include <print>
#include <ranges>

#include <datum/interval/query.h>

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

// Define the ordered integer domain used by the selection:
struct seat_numbers final : iv::comparison_domain<int> {};

int main()
{
    const auto seats = std::views::iota(1, 11);

    // Select seats 1–5, then exclude the reserved seat:
    const auto selection = iv::without(iv::closed_between<seat_numbers>(1, 5), 3);

    // Apply the selection with a standard range filter:
    auto available_seats = seats | std::views::filter([&selection](const int seat) {
        return iv::contains(selection, seat);
    });

    std::println("{:n}", available_seats);
}
