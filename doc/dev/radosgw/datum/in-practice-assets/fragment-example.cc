// Compare a staging string with a borrowed slice over the same split payload.
#include <datum/bytes.h>

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <ranges>
#include <string>
#include <string_view>

namespace datum = ::ceph::salad::datum;

int main()
{
    using namespace std::string_view_literals;

    const std::array fragments {"pay"sv, "load"sv, "tail"sv};
    const std::size_t length = 7;
    std::string received;
    bool check_borrows = false;
    std::size_t chunks = 0;
    auto consume = [&](const auto block) {
        const auto *bytes = std::ranges::data(block);
        const auto count = std::ranges::size(block);
        if (check_borrows) {
            assert(chunks < 2);
            assert(bytes == fragments[chunks].data());
            assert(count == fragments[chunks].size());
        }
        received.append(bytes, count);
        ++chunks;
    };

    {
        std::string contiguous;
        for (const auto fragment : fragments) {
            contiguous.append(fragment);
        }
        const auto payload = std::string_view {contiguous}.substr(0, length);
        consume(payload);
    }
    assert(received == "payload");
    assert(chunks == 1);

    received.clear();
    chunks = 0;
    check_borrows = true;
    {
        datum::byte_cursor input {fragments};
        const auto payload = input.take(length);
        datum::write_fragments(payload, consume);
        assert(input.remaining() == 4);
        assert(input.position() == length);
    }
    assert(received == "payload");
    assert(chunks == 2);
    std::puts(received.c_str());
}
