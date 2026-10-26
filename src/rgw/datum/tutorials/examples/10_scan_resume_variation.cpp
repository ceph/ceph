// Save an owned resume position when a scan stops before or after consuming an entry.

#include <span>
#include <array>
#include <print>
#include <cstddef>
#include <iterator>
#include <string_view>

#include <datum/traversal/scan.h>

namespace datum = ::ceph::salad::datum;

struct toy final
{
    std::size_t index;
    std::string_view name;
};

// Datum's position() and next() operations, implemented here for array indices:
struct toy_positions final
{
    std::size_t position(const toy& entry) const { return entry.index; }

    std::size_t next(const std::size_t index, const bool consumed) const
    {
        return consumed ? 1 + index : index;
    }

};

int main()
{
    const std::array toys {
        toy {0, "felt mouse"},
        toy {1, "ball"},
        toy {2, "ribbon"}
    };
    const auto stop = datum::acceptance::accept_and_stop;

    // Count offers and consumption, retaining an owned resume index:
    const auto scan = datum::traversal::scan_range(toys, [stop](const toy& entry) {
        std::println("offered: {}", entry.name);
        return 2 == entry.index ? stop : datum::acceptance::accept;
    }, toy_positions {});

    std::println("offered: {}, consumed: {}", scan.progress.offered, scan.progress.consumed);

    if (scan.resume) {

        // Apply the saved index to the original array:
        const auto remaining = std::span {toys}.subspan(*scan.resume);
        std::println("resume index: {}", *scan.resume);
        std::println("remaining: {}", std::size(remaining));

        for (const auto& entry : remaining) {
            std::println("resumed: {}", entry.name);
        }
    }
}
