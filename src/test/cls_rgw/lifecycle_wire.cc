// RGW CLS: Independent lifecycle records and borrowed legacy association keys.
#include "test/cls_rgw/wire_test.h"
#include "rgw/datum/adapters/cls/lc.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "test/cls_rgw/new_limit.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <type_traits>

namespace {

namespace binding = cls::rgw::lc_datum;
using bytes = ceph::buffer::list;

using rgw_cls_test::canonical;
using rgw_cls_test::fragmented;

bytes frame(const std::uint8_t version, const auto& body, const std::uint8_t compatible = 0)
{
    bytes output;
    ENCODE_START(version, compatible, output);
    body(output);
    if (version > 3) { ceph::encode(std::uint64_t {37}, output); }
    ENCODE_FINISH(output);

    return output;
}

auto native(const binding::marker_request& value)
{
    return cls_rgw_lc_get_entry_op {value.marker};
}

auto native(const binding::entry_message& value)
{
    cls_rgw_lc_set_entry_op result;
    result.entry = value.entry;

    return result;
}

auto native(const binding::head_message& value)
{
    cls_rgw_lc_put_head_op result;
    result.head = value.head;

    return result;
}

auto native(const binding::list_request& value)
{
    cls_rgw_lc_list_entries_op result;
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
    const auto consumed = input.length() - cursor.get_remaining();
    cursor = input.cbegin();
    binding::decode(actual, cursor);
    CHECK(cursor.get_remaining() == input.length() - consumed);
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
        CHECK_THROWS_AS(binding::decode(actual, cursor), ceph::buffer::error);
        CHECK(cursor.get_remaining() == prefix.length());
        CHECK(canonical(native(actual)).contents_equal(preserved));
        if constexpr (std::is_same_v<RecordT, binding::list_request>) { CHECK(actual.compat_v == version); }
    }
    bytes compound;
    ceph::encode(std::uint16_t {11}, compound);
    compound.append(input);
    ceph::encode(std::uint64_t {13}, compound);
    cursor = compound.cbegin(2);
    binding::decode(actual, cursor);
    CHECK(cursor.get_remaining() == compound.length() - (consumed + 2));
}

template <typename NativeT, auto Member, typename DecodeT>
void compare_reply(const bytes& input, DecodeT&& decode_reply)
{
    NativeT expected;
    auto cursor = input.cbegin();
    ::decode(expected, cursor);
    const auto wanted = canonical(expected.*Member);
    for (const bool fragments : {false, true}) {
        const auto source = fragments ? fragmented(input) : input;
        CHECK(canonical(decode_reply(source)).contents_equal(wanted));
        bytes compound = source;
        ceph::encode(std::uint64_t {13}, compound);
        CHECK(canonical(decode_reply(compound)).contents_equal(wanted));
        for (unsigned size = 0; size < source.length(); ++size) {
            bytes prefix;
            prefix.substr_of(source, 0, size);
            CHECK_THROWS_AS(decode_reply(prefix), ceph::buffer::error);
        }
    }
}

bytes entry(const std::uint8_t version)
{
    return frame(version, [](bytes& output) {
        ceph::encode(std::string {"b\0t", 3}, output);
        ceph::encode(std::numeric_limits<std::uint64_t>::max(), output);
        ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
    });
}

bytes head(const std::uint8_t version)
{
    return frame(version, [&](bytes& output) {
        ceph::encode(std::uint64_t {17}, output);
        ceph::encode(std::string {"m\0r", 3}, output);
        if (version >= 2) { ceph::encode(std::uint64_t {19}, output); }
    });
}

} // anonymous namespace

TEST_CASE("Lifecycle marker and list requests preserve frames and request-version negotiation", "[rgw][cls][datum][lc]")
{
    for (const std::uint8_t version : {0, 1, 2, 3, 10}) {
        compare<binding::marker_request>(fragmented(frame(version, [](bytes& output) {
            ceph::encode(std::string {"m\0r", 3}, output);
        })));
        compare<binding::list_request>(fragmented(frame(version, [](bytes& output) {
            ceph::encode(std::string {"m\0r", 3}, output);
            ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
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
                    ceph::encode(std::pair {std::string {"b\0t", 3}, -17}, output);
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
        cls_rgw_lc_list_entries_ret expected {version};
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
        cls_rgw_lc_list_entries_ret decoded;
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
        CHECK_THROWS_AS(binding::decode(value, cursor), ceph::buffer::error);
        CHECK(cursor.get_remaining() == input.length());
        CHECK(canonical(native(value)).contents_equal(preserved));
    };
    reject(binding::marker_request {"saved"});
    reject(binding::entry_message {});
    reject(binding::head_message {});
    reject(binding::list_request {"saved", 17, 3});
}

TEST_CASE("Lifecycle entry reply values retain distinct old forms and native leaf versions",
          "[rgw][cls][datum][lc]")
{
    for (const std::uint8_t version : {0, 1, 2, 3, 10}) {
        if (version < 2) {
            const auto input = frame(version, [](bytes& output) {
                ceph::encode(std::pair {std::string {"b\0t", 3}, -17}, output);
            });
            compare_reply<cls_rgw_lc_get_entry_ret, &cls_rgw_lc_get_entry_ret::entry>(
                input, binding::decode_entry_reply);
            compare_reply<cls_rgw_lc_get_next_entry_ret, &cls_rgw_lc_get_next_entry_ret::entry>(
                input, binding::decode_next_entry_reply);
        }
        for (const std::uint8_t leaf : {0, 1, 2, 10}) {
            const auto input = frame(version, [&](bytes& output) { output.append(entry(leaf)); });
            compare_reply<cls_rgw_lc_get_entry_ret, &cls_rgw_lc_get_entry_ret::entry>(
                input, binding::decode_entry_reply);
            if (version >= 2) {
                compare_reply<cls_rgw_lc_get_next_entry_ret, &cls_rgw_lc_get_next_entry_ret::entry>(
                    input, binding::decode_next_entry_reply);
            }
            if (version < 2) {
                CHECK_THROWS_AS(binding::decode_next_entry_reply(input), ceph::buffer::error);
            }
        }
    }
}

TEST_CASE("Lifecycle head reply values retain rollover defaults and compatible frames",
          "[rgw][cls][datum][lc]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        for (const std::uint8_t leaf : {0, 1, 2, 10}) {
            const auto input = frame(version, [&](bytes& output) { output.append(head(leaf)); });
            compare_reply<cls_rgw_lc_get_head_ret, &cls_rgw_lc_get_head_ret::head>(
                input, binding::decode_head_reply);
        }
    }
}

TEST_CASE("Lifecycle list reply values retain map normalization and vector order",
          "[rgw][cls][datum][lc]")
{
    const std::vector<cls_rgw_lc_entry> entries {
        {"z", 17, 11}, {std::string {"a\0b", 3}, 19, 13}, {"z", 23, 99}};
    for (const std::uint8_t version : {0, 1, 2, 3, 10}) {
        for (const bool flag : {false, true}) {
            if (!flag && version >= 2) {
                continue;
            }
            const auto input = frame(version, [&](bytes& output) {
                if (version <= 2) {
                    ceph::encode(static_cast<std::uint32_t>(entries.size()), output);
                    for (const auto& value : entries) {
                        ceph::encode(value.bucket, output);
                        ceph::encode(static_cast<int>(value.status), output);
                    }
                }
                if (version > 2) {
                    ceph::encode(entries, output);
                }
                if (flag) {
                    ceph::encode(true, output);
                }
            });
            compare_reply<cls_rgw_lc_list_entries_ret, &cls_rgw_lc_list_entries_ret::entries>(
                input, binding::decode_list_reply);
        }
    }
    const auto missing_flag = frame(2, [](bytes& output) {
        ceph::encode(std::map<std::string, int> {}, output);
    });
    CHECK_THROWS_AS(binding::decode_list_reply(missing_flag), ceph::buffer::error);
}

TEST_CASE("Lifecycle list counts fail before oversized sequence allocation", "[rgw][cls][datum][lc]")
{
    for (const std::uint8_t version : {0, 1, 2, 3, 10}) {
        const auto impossible = frame(version, [](bytes& output) {
            ceph::encode(std::uint32_t {4096}, output);
            ceph::encode(false, output);
        });
        const auto decode = [&] {
            const rgw_cls_test::scoped_new_limit limit {65536};

            return binding::decode_list_reply(impossible);
        };
        CHECK_THROWS_AS(decode(), ceph::buffer::error);
    }
    for (const std::uint8_t leaf_version : {0, 1, 3}) {
        const auto smallest = frame(3, [&](bytes& output) {
            ceph::encode(std::uint32_t {1}, output);
            output.append(frame(leaf_version, [](bytes& leaf) {
                ceph::encode(std::string {}, leaf);
                ceph::encode(std::uint64_t {0}, leaf);
                ceph::encode(std::uint32_t {0}, leaf);
            }));
            ceph::encode(false, output);
        });
        compare_reply<cls_rgw_lc_list_entries_ret, &cls_rgw_lc_list_entries_ret::entries>(
            smallest, binding::decode_list_reply);
    }
}

TEST_CASE("Lifecycle duplicate associations reserve only their distinct output", "[rgw][cls][datum][lc]")
{
    const auto input = frame(2, [](bytes& output) {
        ceph::encode(std::uint32_t {4096}, output);
        for (unsigned index = 0; index < 4096; ++index) {
            ceph::encode(std::string(80, 'b'), output);
            ceph::encode(index == 0 ? -17 : static_cast<int>(index), output);
        }
        ceph::encode(true, output);
    });
    const auto decode = [&] {
        const rgw_cls_test::scoped_new_limit limit {65536};

        return binding::decode_list_reply(input);
    };
    const auto value = decode();
    REQUIRE(value.size() == 1);
    CHECK(value.front().bucket == std::string(80, 'b'));
    CHECK(value.front().start_time == 0);
    CHECK(value.front().status == static_cast<std::uint32_t>(-17));
}

TEST_CASE("Lifecycle old associations retain the first signed value under disorder and duplicates",
          "[rgw][cls][datum][lc]")
{
    for (const std::uint8_t version : {0, 1, 2}) {
        const auto input = frame(version, [](bytes& output) {
            ceph::encode(std::uint32_t {64}, output);
            for (int index = 63; index >= 0; --index) {
                ceph::encode(std::to_string(index % 17) + std::string(80, 'b'), output);
                ceph::encode(index % 2 ? -index - 1 : index, output);
            }
            ceph::encode(true, output);
        });
        cls_rgw_lc_list_entries_ret expected;
        auto cursor = input.cbegin();
        ::decode(expected, cursor);
        for (const bool fragments : {false, true}) {
            const auto value = binding::decode_list_reply(fragments ? fragmented(input) : input);
            CHECK(canonical(value).contents_equal(canonical(expected.entries)));
        }
    }
}

TEST_CASE("Lifecycle old entry replies reject a native leaf crossing the outer frame",
          "[rgw][cls][datum][lc]")
{
    // This leaf's length makes its first four bytes a plausible old string length.
    const auto leaf = canonical(cls_rgw_lc_entry {std::string(65520, 'b'), 17, 23});
    for (const std::uint8_t version : {0, 1}) {
        auto leaf_cursor = leaf.cbegin();
        auto input = frame(version, [&](bytes& output) { leaf_cursor.copy(265, output); });
        leaf_cursor.copy(leaf_cursor.get_remaining(), input);
        cls_rgw_lc_get_entry_ret expected;
        auto cursor = input.cbegin();
        CHECK_THROWS_AS(::decode(expected, cursor), ceph::buffer::error);
        CHECK_THROWS_AS(binding::decode_entry_reply(input), ceph::buffer::error);
    }
}
