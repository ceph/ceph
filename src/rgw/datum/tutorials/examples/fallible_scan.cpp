// A handled row failure retains progress and the failing row's position.
#include <datum/traversal/scan.h>

#include <array>
#include <cstddef>
#include <expected>
#include <print>
#include <system_error>

namespace datum = ceph::salad::datum;

struct positions final
{
    std::size_t position(const std::size_t row) const { return row; }

    std::size_t next(const std::size_t row, const bool consumed) const { return row + consumed; }

};

int main()
{
    const std::array<std::size_t, 3> rows {0, 1, 2};
    const auto result = datum::traversal::scan_range(rows, [](const std::size_t row)
        -> std::expected<datum::acceptance, std::errc> {
        if (1 == row) {
            return std::unexpected(std::errc::invalid_argument);
        }

        return datum::acceptance::accept;
    }, positions {});

    if (!result) {
        const auto& failure = result.error();
        std::println("invalid row: {}", std::errc::invalid_argument == failure.error);
        std::println("offered: {}, consumed: {}", failure.progress.offered, failure.progress.consumed());
        std::println("resume index: {}", failure.resume);
    }
}
