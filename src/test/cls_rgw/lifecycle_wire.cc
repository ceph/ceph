// RGW CLS: Independent lifecycle records and borrowed legacy association keys.
#include "test/cls_rgw/wire_test.h"
#include "rgw/datum/adapters/cls/lc.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <type_traits>

namespace {

namespace binding = ::cls::rgw::lc_datum;
using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;
using ::rgw_cls_test::fragmented;

bytes frame(const std::uint8_t version, const auto& body, const std::uint8_t compatible = 0)
{
    bytes output;
    ENCODE_START(version, compatible, output);
    body(output);
    if (version > 3) { ::ceph::encode(std::uint64_t {37}, output); }
    ENCODE_FINISH(output);

    return output;
}

auto native(const binding::marker_request& value)
{
    return ::cls_rgw_lc_get_entry_op {value.marker};
}

auto native(const binding::entry_message& value)
{
    ::cls_rgw_lc_set_entry_op result;
    result.entry = value.entry;

    return result;
}

auto native(const binding::head_message& value)
{
    ::cls_rgw_lc_put_head_op result;
    result.head = value.head;

    return result;
}

auto native(const binding::list_request& value)
{
    ::cls_rgw_lc_list_entries_op result;
    result.marker = value.marker;
    result.max_entries = value.max_entries;
    result.compat_v = value.compat_v;

    return result;
}

template <typename RecordT>
void compare(const bytes& input)
{
    RecordT actual;
    auto expected = native(actual);
    auto cursor = input.cbegin();
    ::decode(expected, cursor);
    const auto consumed = cursor.get_off();
    cursor = input.cbegin();
    binding::decode(actual, cursor);
    CHECK(cursor.get_off() == consumed);
    const auto preserved = canonical(native(actual));
    CHECK(preserved.contents_equal(canonical(expected)));
    const auto version = [&] {
        if constexpr (std::is_same_v<RecordT, binding::list_request>) { return actual.compat_v; }
        return std::uint8_t {0};
    }();
    if constexpr (std::is_same_v<RecordT, binding::list_request>) { CHECK(version == expected.compat_v); }
    for (unsigned size = 0; size < input.length(); ++size) {
        bytes prefix;
        prefix.substr_of(input, 0, size);
        cursor = prefix.cbegin();
        CHECK_THROWS_AS(binding::decode(actual, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
        CHECK(canonical(native(actual)).contents_equal(preserved));
        if constexpr (std::is_same_v<RecordT, binding::list_request>) { CHECK(actual.compat_v == version); }
    }
    bytes compound;
    ::ceph::encode(std::uint16_t {11}, compound);
    compound.append(input);
    ::ceph::encode(std::uint64_t {13}, compound);
    cursor = compound.cbegin(2);
    binding::decode(actual, cursor);
    CHECK(cursor.get_off() == consumed + 2);
}

bytes entry(const std::uint8_t version)
{
    return frame(version, [](bytes& output) {
        ::ceph::encode(std::string {"b\0t", 3}, output);
        ::ceph::encode(std::numeric_limits<std::uint64_t>::max(), output);
        ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
    });
}

bytes head(const std::uint8_t version)
{
    return frame(version, [&](bytes& output) {
        ::ceph::encode(std::uint64_t {17}, output);
        ::ceph::encode(std::string {"m\0r", 3}, output);
        if (version >= 2) { ::ceph::encode(std::uint64_t {19}, output); }
    });
}

} // anonymous namespace

TEST_CASE("Lifecycle marker and list requests preserve frames and request-version negotiation", "[rgw][cls][datum][lc]")
{
    for (const std::uint8_t version : {0, 1, 2, 3, 10}) {
        compare<binding::marker_request>(fragmented(frame(version, [](bytes& output) {
            ::ceph::encode(std::string {"m\0r", 3}, output);
        })));
        compare<binding::list_request>(fragmented(frame(version, [](bytes& output) {
            ::ceph::encode(std::string {"m\0r", 3}, output);
            ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
        })));
    }
    CHECK(binding::encode("marker").contents_equal(canonical(native(binding::marker_request {"marker"}))));
    CHECK(binding::encode("marker", 17).contents_equal(canonical(native(binding::list_request {"marker", 17, 3}))));
}

TEST_CASE("Lifecycle entry messages retain old pair requests and native leaf versions", "[rgw][cls][datum][lc]")
{
    for (const std::uint8_t version : {0, 1, 2, 3, 10}) {
        for (const std::uint8_t leaf : {0, 1, 2, 10}) {
            compare<binding::entry_message>(fragmented(frame(version, [&](bytes& output) {
                if (version < 2) {
                    ::ceph::encode(std::pair {std::string {"b\0t", 3}, -17}, output);
                }
                if (version >= 2) { output.append(entry(leaf)); }
            })));
        }
    }
    binding::entry_message value;
    value.entry = {std::string(80, 'b'), 17, std::numeric_limits<std::uint32_t>::max()};
    const auto expected = canonical(native(value));
    CHECK(binding::encode(value.entry).contents_equal(expected));
    bytes output;
    binding::encode(value, output);
    CHECK(output.contents_equal(expected));
}

TEST_CASE("Lifecycle head messages preserve old rollover defaults and native codecs", "[rgw][cls][datum][lc]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        for (const std::uint8_t leaf : {0, 1, 2, 10}) {
            compare<binding::head_message>(fragmented(frame(version, [&](bytes& output) { output.append(head(leaf)); })));
        }
    }
    binding::head_message value;
    value.head.start_date = 17;
    value.head.marker = std::string(80, 'm');
    value.head.shard_rollover_date = 19;
    CHECK(binding::encode(value.head).contents_equal(canonical(native(value))));
    bytes output;
    binding::encode(value, output);
    CHECK(output.contents_equal(canonical(native(value))));
}

TEST_CASE("Lifecycle legacy map replies borrow keys and retain sorted first-wins bytes", "[rgw][cls][datum][lc]")
{
    for (const std::uint8_t version : {0, 1, 2, 3, 10}) {
        binding::list_reply value;
        value.compat_v = version;
        value.is_truncated = true;
        value.entries = {{"z", 17, 11}, {std::string {"a\0b", 3}, 19, 13}, {"z", 23, 99},
                         {std::string(80, 'b'), 29, std::numeric_limits<std::uint32_t>::max()}};
        ::cls_rgw_lc_list_entries_ret expected {version};
        expected.entries = value.entries;
        expected.is_truncated = true;
        const auto encoded = canonical(expected);
        bytes actual;
        actual.append("prefix", 6);
        binding::encode(value, actual);
        bytes compound;
        compound.append("prefix", 6);
        compound.append(encoded);
        CHECK(actual.contents_equal(compound));
        value.entries.front().bucket.assign(80, 'x');
        CHECK(actual.contents_equal(compound));
        ::cls_rgw_lc_list_entries_ret decoded;
        auto cursor = actual.cbegin(6);
        ::decode(decoded, cursor);
        CHECK(cursor.end());
        CHECK(decoded.entries.size() == (version <= 2 ? 3 : 4));
        CHECK(decoded.is_truncated == (version >= 2));
    }
}

TEST_CASE("Lifecycle request candidates reject incompatible envelopes atomically", "[rgw][cls][datum][lc]")
{
    const auto input = frame(10, [](bytes&) {}, 20);
    const auto reject = [&]<typename T>(T value) {
        const auto preserved = canonical(native(value));
        auto cursor = input.cbegin();
        CHECK_THROWS_AS(binding::decode(value, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
        CHECK(canonical(native(value)).contents_equal(preserved));
    };
    reject(binding::marker_request {"saved"});
    reject(binding::entry_message {});
    reject(binding::head_message {});
    reject(binding::list_request {"saved", 17, 3});
}
