// Combine seat selections with union, intersection, and difference.

#include <print>
#include <ranges>

#include <datum/interval/query.h>

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

struct seat_numbers final : iv::comparison_domain<int> {};

int main()
{
    const auto seats = std::views::iota(1, 11);
    const auto a = iv::closed_between<seat_numbers>(1, 5);
    const auto b = iv::closed_between<seat_numbers>(3, 7);

    // Print member seats; brackets also make an empty selection visible:
    const auto print_seats = [&seats](const auto label, const auto& selection) {
        auto selected = seats | std::views::filter([&selection](const int seat) {
            return iv::contains(selection, seat);
        });

        std::println("{}: {}", label, selected);
    };

    print_seats("A", a);
    print_seats("B", b);

    // Combine the selections, then test membership in each result:
    print_seats("A union B", iv::set_union(a, b));
    print_seats("A intersection B", iv::intersection(a, b));
    print_seats("A minus B", iv::difference(a, b));
}
