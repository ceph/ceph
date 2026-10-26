// Read a guest list across two pages and retain the next page's index when stopping early.

#include <array>
#include <print>
#include <cstddef>
#include <iterator>
#include <optional>

#include <datum/traversal.h>

namespace datum = ::ceph::salad::datum;

int main()
{
    const std::array pages {
        std::array {"Juniper", "Rowan"},
        std::array {"Hazel", "Wren"}
    };

    // Visit guest-list pages; return progress and an optional resume index:
    const auto result = datum::traversal::for_each_page(
        std::size_t {0},

        // Fetch the page at the current index:
        [&pages](const std::size_t index) { return pages[index]; },

        // Return the following index, or no index at the end:
        [&pages](const std::size_t index, const auto&) -> std::optional<std::size_t> {
            const auto following = 1 + index;

            return std::size(pages) > following ? std::optional {following} : std::nullopt;
        },

        // Print this page and decide whether to keep visiting:
        [](const auto& page) {
            for (const auto guest : page) {
                std::println("guest: {}", guest);
            }

            return datum::traversal::page_action::stop;
        });

    // Report completed page visits and any saved continuation:
    std::println("pages: {}", result.pages);
    std::println("complete: {}", result.complete());
    if (result.resume) {
        std::println("resume index: {}", *result.resume);
    }
}
