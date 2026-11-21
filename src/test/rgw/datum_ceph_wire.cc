// Native framing must retain Ceph's wire representation when fragment sinks are shared.
#include <ceph/wire.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <catch2/catch_test_macros.hpp>

namespace wire = ceph::datum_wire;
namespace datum = ceph::salad::datum;

TEST_CASE("Ceph text sinks match native encoding across fragment boundaries")
{
    const std::array<std::string, 4> values {"", "x", "abcdef", std::string {"a\0bc\0d", 6}};
    for (const auto& value : values) {
        ceph::buffer::list source;
        // Preserve empty and single-byte descriptors as well as a longer fragment.
        source.push_back(ceph::buffer::create(0));
        if (!value.empty()) {
            source.push_back(ceph::buffer::copy(value.data(), 1));
            source.push_back(ceph::buffer::create(0));
            source.push_back(ceph::buffer::copy(value.data() + 1, value.size() - 1));
        }
        source.push_back(ceph::buffer::create(0));
        REQUIRE(std::ranges::distance(source.buffers()) >= 2);

        ceph::buffer::list expected;
        using ceph::encode;
        encode(std::uint8_t {7}, expected);
        encode(value, expected);
        encode(std::uint16_t {9}, expected);

        ceph::buffer::list actual;
        wire::writer writer {actual};
        writer.scalar(std::uint8_t {7});
        writer.text(source);
        writer.scalar(std::uint16_t {9});
        CHECK(actual.contents_equal(expected));
        CHECK(source.to_str() == value);

        auto cursor = actual.cbegin();
        wire::reader reader {cursor};
        CHECK(reader.scalar<std::uint8_t>() == 7);
        CHECK(reader.text() == value);
        CHECK(reader.scalar<std::uint16_t>() == 9);
        CHECK(reader.remaining() == 0);
    }
}

TEST_CASE("Ceph text sinks agree for native fragments and ordinary string views")
{
    const std::string value(4097, 'x');
    ceph::buffer::list source;
    source.append(ceph::buffer::copy(value.data(), 1));
    source.append(ceph::buffer::copy(value.data() + 1, value.size() - 1));
    const auto native = wire::encode_frame(1, [&](auto& output) { output.text(source); });
    const auto ordinary = wire::encode_frame(1, [&](auto& output) {
        output.text(std::string_view {value});
    });
    CHECK(native.contents_equal(ordinary));
    auto cursor = native.cbegin();
    wire::reader reader {cursor};
    CHECK(reader.frame(1, [](auto& fields, std::uint8_t) { return fields.text(); }) == value);
    CHECK(reader.remaining() == 0);
}

TEST_CASE("Ceph writer capacity checks preserve existing output")
{
    ceph::buffer::list output;
    output.append("prefix", 6);
    wire::writer writer {output};
    const auto remaining = std::numeric_limits<std::uint32_t>::max() - output.length();
    CHECK_NOTHROW(writer.ensure(remaining));
    CHECK_THROWS_AS(writer.ensure(std::uint64_t {remaining} + 1), std::length_error);
    CHECK(output.to_str() == "prefix");
}

TEST_CASE("Ceph fragments compose with standard byte storage and native output")
{
    ceph::buffer::list source;
    source.append(ceph::buffer::copy("\0\x80", 2));
    source.push_back(ceph::buffer::create(0));
    source.append(ceph::buffer::copy("\xFF\x7F", 2));
    std::vector<std::byte> collected;
    datum::write_fragments(datum::fragments(source), [&](const auto block) {
        datum::append_range(collected, std::as_bytes(block));
    });
    const std::array expected {std::byte {0}, std::byte {128}, std::byte {255}, std::byte {127}};
    CHECK(std::ranges::equal(collected, expected));
    ceph::buffer::list output;
    wire::writer {output}.payload(collected);
    CHECK(output.contents_equal(source));
    source.clear();
    CHECK(std::ranges::equal(collected, expected));
}

TEST_CASE("Ceph explicit field frames preserve native bytes and publish only complete records")
{
    struct metadata final
    {
        std::uint16_t tag;
        std::string name;
    };
    constexpr auto fields = datum::binary_fields<&metadata::tag, &metadata::name> {};
    const metadata original {7, std::string {"a\0bc", 4}};

    ceph::buffer::list expected;
    using ceph::encode;
    encode(std::uint8_t {3}, expected);
    encode(std::uint8_t {1}, expected);
    encode(static_cast<std::uint32_t>(sizeof(original.tag) + sizeof(std::uint32_t) +
                                      original.name.size()), expected);
    encode(original.tag, expected);
    encode(original.name, expected);

    ceph::buffer::list encoded;
    wire::writer {encoded}.frame(fields, 3, original);
    REQUIRE(encoded.contents_equal(expected));
    const auto decode = [](auto& cursor, auto& value) {
        wire::reader {cursor}.frame(3, [&](auto& input, std::uint8_t version) {
            CHECK(version == 3);
            value.tag = input.template scalar<std::uint16_t>();
            input.text(value.name);
        });
    };
    auto cursor = encoded.cbegin();
    metadata value {9, "unchanged"};
    wire::decode_record(value, cursor, decode);
    CHECK(value.tag == original.tag);
    CHECK(value.name == original.name);
    CHECK(cursor.get_remaining() == 0);

    // A consumer can reject a fully decoded candidate after its fields changed.
    cursor = encoded.cbegin();
    value = {9, "unchanged"};
    CHECK_THROWS_AS(wire::decode_record(value, cursor, [&](auto& input, auto& candidate) {
        decode(input, candidate);
        throw std::runtime_error {"consumer rejected the decoded record"};
    }), std::runtime_error);
    CHECK(value.tag == 9);
    CHECK(value.name == "unchanged");
    CHECK(cursor.get_remaining() == encoded.length());

    for (unsigned length = 0; length < encoded.length(); ++length) {
        ceph::buffer::list truncated;
        truncated.substr_of(encoded, 0, length);
        auto input = truncated.cbegin();
        metadata preserved {9, "unchanged"};
        CHECK_THROWS_AS(wire::decode_record(preserved, input, decode), ceph::buffer::malformed_input);
        CHECK(preserved.tag == 9);
        CHECK(preserved.name == "unchanged");
        CHECK(input.get_remaining() == length);
    }
}
