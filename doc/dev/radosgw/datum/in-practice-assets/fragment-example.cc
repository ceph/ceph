// Read split payloads and use shared byte vocabulary with native Ceph buffers.
#include <ceph-datum.h>
#include <datum/bytes.h>
#include <datum/range.h>

#include <array>
#include <cstdio>
#include <ranges>
#include <string>
#include <cassert>
#include <cstddef>
#include <utility>
#include <string_view>

namespace datum = ceph::salad::datum;

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
        datum::append_range(contiguous, fragments | std::views::join);
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
    std::array<char, 7> output {};
    const auto copied = datum::copy(fragments, 0, output);
    assert(std::string_view {copied} == "payload");

    // The same copy operation reads an ordinary Ceph reply, across its native buffers.
    ceph::buffer::list reply;
    reply.append(ceph::buffer::copy("pay", 3));
    reply.append(ceph::buffer::copy("loadtail", 8));
    datum::copy(reply, 0, output);

    const datum::byte_extent payload {0, 7};
    const ceph::buffer::list retained = datum::slice(reply, payload);
    const ceph::buffer::list independent = datum::copy(reply, payload);

    // The corresponding native operations choose and assemble the storage explicitly.
    ceph::buffer::list native_retained;
    native_retained.substr_of(reply, 0, 7);
    ceph::buffer::ptr native_bytes = ceph::buffer::create(7);
    auto native_input = reply.cbegin();
    native_input.copy(7, native_bytes.c_str());
    ceph::buffer::list native_independent;
    native_independent.append(std::move(native_bytes));
    assert(native_retained.to_str() == retained.to_str());
    assert(native_independent.to_str() == independent.to_str());
    assert(retained.buffers().front().c_str() == reply.buffers().front().c_str());
    assert(independent.buffers().front().c_str() != reply.buffers().front().c_str());
    native_retained.clear();
    reply.clear();
    assert(retained.to_str() == "payload");
    assert(independent.to_str() == "payload");

    // Keep the view descriptor and native list alive while the core consumes their spans.
    const auto native_fragments = datum::fragments(retained);
    datum::copy(native_fragments, output);
    assert(std::string_view {output} == "payload");

    // A consumer requiring one contiguous field uses the ordinary native cursor.
    std::array<char, 7> scratch {};
    auto fragmented_input = retained.cbegin();
    const auto gathered = datum::read_contiguous(fragmented_input, scratch);
    assert(std::string_view {gathered} == "payload");
    assert(gathered.data() == scratch.data());
    assert(fragmented_input.get_remaining() == 0);

    auto contiguous_input = independent.cbegin();
    const auto borrowed = datum::read_contiguous(contiguous_input, scratch);
    assert(std::string_view {borrowed} == "payload");
    assert(borrowed.data() == independent.buffers().front().c_str());
    assert(contiguous_input.get_remaining() == 0);

    auto short_input = retained.cbegin(1);
    bool rejected = false;
    try
     {
        (void)datum::read_contiguous(short_input, scratch);
     }
    catch (const ceph::buffer::end_of_buffer&)
     {
        rejected = true;
     }
    assert(rejected and short_input.get_remaining() == 6);

    // Map object positions into native storage, then retain only the selected bytes.
    const datum::byte_mapping stored {{4096, 7}, 0};
    const datum::byte_extent wanted {4098, 3};
    const auto selected = datum::clip(stored, wanted);
    if (!selected) {
        return 1;
    }

    const auto piece = datum::slice(retained, {selected->backing_offset, selected->logical.length});
    assert(piece.to_str() == "ylo");

    std::puts(received.c_str());
}
