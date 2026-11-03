// Decode a complete toy page, then prepare an owned reply and continuation before publication.

#include <charconv>
#include <cstddef>
#include <expected>
#include <print>
#include <system_error>

#include <array>
#include <span>
#include <string_view>
#include <vector>

#include <datum/traversal/scan.h>

namespace datum = ceph::salad::datum;

struct decoded_row final
{
    std::size_t source_index;
    unsigned value;
};

struct row_positions final
{
    std::size_t position(const decoded_row& row) const { return row.source_index; }

    std::size_t next(const std::size_t index, const bool consumed) const
    {
        return consumed ? 1 + index : index;
    }

};

struct page_reply final
{
    std::vector<unsigned> values;
    datum::traversal::scan_result<std::size_t> scan;
};

std::expected<unsigned, std::errc> decode(const std::string_view text)
{
    if (text.empty()) {
        return std::unexpected(std::errc::invalid_argument);
    }
    unsigned value;
    const auto last = text.data() + text.size();
    const auto parsed = std::from_chars(text.data(), last, value);
    if (std::errc {} != parsed.ec) {
        return std::unexpected(parsed.ec);
    }
    if (last != parsed.ptr) {
        return std::unexpected(std::errc::invalid_argument);
    }

    return value;
}

std::expected<page_reply, std::errc> prepare_reply(
    const std::span<const std::string_view> encoded, const std::size_t maximum)
{
    std::vector<decoded_row> records;
    for (const auto text : encoded) {
        auto value = decode(text);
        if (!value) {
            return std::unexpected(value.error());
        }
        records.push_back({records.size(), *value});
    }

    page_reply draft;
    draft.scan = datum::traversal::scan_range(records, [&draft, maximum](const decoded_row& row) {
        if (maximum == draft.values.size()) {
            return datum::acceptance::reject_and_stop;
        }
        if (0 == row.value) {
            return datum::acceptance::skip;
        }
        draft.values.push_back(row.value);

        return datum::acceptance::accept;
    }, row_positions {});

    return draft;
}

int main()
{
    using namespace std::string_view_literals;

    const std::array encoded {"0"sv, "7"sv, "9"sv};
    const auto reply = prepare_reply(encoded, 1);
    if (!reply || !reply->scan.resume) {
        return 1;
    }

    // Publication starts here; preparing the reply performed no external effects.
    std::println("returned: {}", reply->values.front());
    const auto& progress = reply->scan.progress;
    std::println("offered: {}, consumed: {}, accepted: {}",
                 progress.offered, progress.consumed(), progress.accepted);
    std::println("resume index: {}", *reply->scan.resume);

    const std::array malformed {"7"sv, "bad"sv};
    const auto rejected = prepare_reply(malformed, 1);
    if (rejected || std::errc::invalid_argument != rejected.error()) {
        return 1;
    }
    std::println("malformed page: no reply published");
}
