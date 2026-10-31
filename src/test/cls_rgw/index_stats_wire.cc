// RGW CLS: Independent statistics records checked against native C++23 codecs.
#include "test/cls_rgw/wire_test.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "rgw/datum/adapters/cls/bucket.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <map>
#include <string>
#include <vector>

namespace {

namespace binding = ::cls::rgw::bucket_datum;
using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;
using ::rgw_cls_test::fragmented;

bytes envelope(const std::uint8_t version, const std::uint8_t first_framed, auto body)
{
    bytes output;
    if (version < first_framed) {
        ::ceph::encode(version, output);
        body(output);

        return output;
    }
    ENCODE_START(version, 1, output);
    body(output);
    ::ceph::encode(std::uint64_t {0xDEADBEEF}, output);
    ENCODE_FINISH(output);

    return output;
}

bytes stat_leaf(const std::uint8_t version, const std::uint64_t value)
{
    return envelope(version, 2, [&](bytes& output) {
        ::ceph::encode(value, output);
        ::ceph::encode(value + 1, output);
        ::ceph::encode(value + 2, output);
        if (version >= 3) { ::ceph::encode(value + 3, output); }
    });
}

bytes stat_map(const std::uint8_t leaf, const bool broken_duplicate = false)
{
    bytes output;
    const std::vector<std::uint8_t> categories {255, 2, 1, 2};
    ::ceph::encode(static_cast<std::uint32_t>(categories.size()), output);
    for (unsigned index = 0; index < categories.size(); ++index) {
        ::ceph::encode(categories[index], output);
        if (broken_duplicate && index + 1 == categories.size()) {
            ::ceph::encode(std::uint8_t {0}, output);
        } else {
            output.append(stat_leaf(leaf, 10 + index));
        }
    }

    return output;
}

bytes update(const std::uint8_t version, const std::uint8_t leaf, const bool broken = false)
{
    return envelope(version, 0, [&](bytes& output) {
        ::ceph::encode(std::uint8_t {0x80}, output);
        output.append(stat_map(leaf));
        if (version >= 2) { output.append(stat_map(leaf, broken)); }
    });
}

auto native(const binding::stats_request& value)
{
    ::rgw_cls_bucket_update_stats_op result;
    result.absolute = value.absolute;
    result.stats.insert(value.stats.begin(), value.stats.end());
    result.dec_stats.insert(value.dec_stats.begin(), value.dec_stats.end());

    return result;
}

auto native(const binding::timeout_request& value)
{
    return ::rgw_cls_tag_timeout_op {.tag_timeout = value.tag_timeout};
}

auto native(const binding::check_reply& value)
{
    ::rgw_cls_check_index_ret result;
    result.existing_header = value.existing_header;
    result.calculated_header = value.calculated_header;

    return result;
}

void reject_prefixes(const bytes& input, auto actual)
{
    const auto preserved = canonical(native(actual));
    for (unsigned size = 0; size < input.length(); ++size) {
        bytes prefix;
        prefix.substr_of(input, 0, size);
        auto cursor = prefix.cbegin();
        CHECK_THROWS_AS(binding::decode(actual, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
        CHECK(canonical(native(actual)).contents_equal(preserved));
    }
    auto invalid = input.to_str();
    invalid[1] = static_cast<char>(255);
    bytes incompatible;
    incompatible.append(invalid);
    auto cursor = incompatible.cbegin();
    CHECK_THROWS_AS(binding::decode(actual, cursor), ::ceph::buffer::error);
    CHECK(cursor.get_off() == 0);
    CHECK(canonical(native(actual)).contents_equal(preserved));
    bytes compound;
    ::ceph::encode(std::uint16_t {17}, compound);
    compound.append(input);
    ::ceph::encode(std::uint64_t {19}, compound);
    cursor = compound.cbegin(2);
    binding::decode(actual, cursor);
    CHECK(cursor.get_off() == 2 + input.length());
}

bytes header(const std::uint8_t version)
{
    return envelope(version, 2, [&](bytes& output) {
        output.append(stat_map(1));
        if (version > 2) { ::ceph::encode(std::uint64_t {17}, output); }
        if (version >= 4) {
            ::ceph::encode(std::uint64_t {19}, output);
            ::ceph::encode(std::uint64_t {23}, output);
        }
        if (version >= 5) { ::ceph::encode(std::string {"m\0r", 3}, output); }
        if (version >= 6) { ::encode(::cls_rgw_bucket_instance_entry {}, output); }
        if (version >= 7) { ::ceph::encode(std::uint8_t {0x80}, output); }
        if (version >= 8) { ::ceph::encode(std::uint32_t {29}, output); }
    });
}

} // anonymous namespace

TEST_CASE("Statistics update binds native maps to ordered first-wins vectors", "[rgw][cls][datum][stats]")
{
    static_assert(std::is_same_v<decltype(binding::stats_request::stats), binding::stat_associations>);
    for (std::uint8_t version = 0; version <= 4; ++version) {
        for (std::uint8_t leaf = 0; leaf <= 5; ++leaf) {
            INFO("outer=" << unsigned(version) << " leaf=" << unsigned(leaf));
            const auto input = fragmented(update(version, leaf));
            ::rgw_cls_bucket_update_stats_op expected;
            auto native_cursor = input.cbegin();
            ::decode(expected, native_cursor);
            binding::stats_request actual;
            auto cursor = input.cbegin();
            binding::decode(actual, cursor);
            CHECK(cursor.get_off() == native_cursor.get_off());
            CHECK(canonical(native(actual)).contents_equal(canonical(expected)));
            REQUIRE(actual.stats.size() == 3);
            CHECK(actual.stats.front().first == ::RGWObjCategory::Main);
            CHECK(actual.stats[1].second.total_size == 11);
            CHECK(actual.stats.back().first == static_cast<::RGWObjCategory>(255));
            reject_prefixes(input, actual);
        }
    }
}

TEST_CASE("Statistics update rejects impossible counts and validates discarded duplicates", "[rgw][cls][datum][stats]")
{
    for (const bool subtraction : {false, true}) {
        const auto input = envelope(2, 0, [&](bytes& output) {
            ::ceph::encode(false, output);
            if (subtraction) { ::ceph::encode(std::uint32_t {0}, output); }
            ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
            if (!subtraction) { ::ceph::encode(std::uint32_t {0}, output); }
        });
        binding::stats_request result;
        auto cursor = input.cbegin();
        CHECK_THROWS_AS(binding::decode(result, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
    }
    const auto broken = update(2, 1, true);
    binding::stats_request result;
    auto cursor = broken.cbegin();
    CHECK_THROWS_AS(binding::decode(result, cursor), ::ceph::buffer::error);
    CHECK(cursor.get_off() == 0);
}

TEST_CASE("Statistics vectors retain the full byte-key domain and consume duplicate-heavy input", "[rgw][cls][datum][stats]")
{
    const auto input = envelope(2, 0, [&](bytes& output) {
        ::ceph::encode(false, output);
        ::ceph::encode(std::uint32_t {768}, output);
        for (unsigned index = 0; index < 768; ++index) {
            ::ceph::encode(static_cast<std::uint8_t>(255 - index % 256), output);
            output.append(stat_leaf(3, index));
        }
        ::ceph::encode(std::uint32_t {0}, output);
    });
    binding::stats_request actual;
    auto cursor = input.cbegin();
    binding::decode(actual, cursor);
    CHECK(cursor.end());
    REQUIRE(actual.stats.size() == 256);
    for (unsigned index = 0; index < 256; ++index) {
        CHECK(static_cast<unsigned>(actual.stats[index].first) == index);
        CHECK(actual.stats[index].second.total_size == 255 - index);
    }
}

TEST_CASE("Tag timeout retains scalar framing and complete candidate publication", "[rgw][cls][datum][stats]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        const auto input = fragmented(envelope(version, 0, [&](bytes& output) {
            ::ceph::encode(std::numeric_limits<std::uint64_t>::max(), output);
        }));
        ::rgw_cls_tag_timeout_op expected;
        auto native_cursor = input.cbegin();
        ::decode(expected, native_cursor);
        binding::timeout_request actual;
        auto cursor = input.cbegin();
        binding::decode(actual, cursor);
        CHECK(actual.tag_timeout == expected.tag_timeout);
        CHECK(cursor.get_off() == native_cursor.get_off());
        reject_prefixes(input, actual);
        CHECK(binding::encode_timeout(actual.tag_timeout).contents_equal(canonical(expected)));
    }
}

TEST_CASE("Index check reply retains native header versions without native owning reply decode", "[rgw][cls][datum][stats]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        for (std::uint8_t leaf = 0; leaf <= 9; ++leaf) {
            INFO("outer=" << unsigned(version) << " header=" << unsigned(leaf));
            const auto input = fragmented(envelope(version, 0, [&](bytes& output) {
                output.append(header(leaf));
                output.append(header(8));
            }));
            ::rgw_cls_check_index_ret expected;
            auto cursor = input.cbegin();
            ::decode(expected, cursor);
            const auto actual = binding::decode_check_reply(input);
            CHECK(canonical(native(actual)).contents_equal(canonical(expected)));
            bytes encoded;
            binding::encode(actual, encoded);
            CHECK(encoded.contents_equal(canonical(expected)));
            for (unsigned size = 0; size < input.length(); ++size) {
                bytes prefix;
                prefix.substr_of(input, 0, size);
                CHECK_THROWS_AS(binding::decode_check_reply(prefix), ::ceph::buffer::error);
            }
        }
    }
}
