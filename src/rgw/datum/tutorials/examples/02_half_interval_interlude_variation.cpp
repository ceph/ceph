// Combine seat selections with union, intersection, and difference.

#include <print>
#include <ranges>

#include <datum/interval/query.h>

namespace datum = ceph::salad::datum;
namespace iv = datum::interval;

struct seat_numbers final : iv::comparison_domain<int> {};

int main()
{
    const auto seats = std::views::iota(1, 11);
    const auto a = iv::closed_between<seat_numbers>(1, 5);
    const auto b = iv::closed_between<seat_numbers>(7, 9);

    // Describe the three combinations:
    const auto together = iv::set_union(a, b);
    const auto overlap = iv::intersection(a, b);
    const auto only_a = iv::difference(a, b);

    // Print member seats; brackets also make an empty selection visible:
    const auto print_seats = [&seats](const auto label, const auto& selection) {
        auto selected = seats | std::views::filter([&selection](const int seat) {
            return iv::contains(selection, seat);
        });

        std::print("{}: [", label);
        auto separator = "";
        for (const auto seat : selected) {
            std::print("{}{}", separator, seat);
            separator = ", ";
        }
        std::println("]");
    };

    print_seats("A", a);
    print_seats("B", b);

    // Test membership in each combined selection:
    print_seats("A union B", together);
    print_seats("A intersection B", overlap);
    print_seats("A minus B", only_a);
}
