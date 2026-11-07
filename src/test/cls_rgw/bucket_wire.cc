// RGW CLS: Native C++23 oracles for bucket listing versions and map semantics.
#include "test/cls_rgw/wire_test.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "rgw/datum/adapters/cls/bucket.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>

#include <map>
#include <string>

namespace {

namespace binding = ::cls::rgw::bucket_datum;
using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;
using ::rgw_cls_test::fragmented;

bytes envelope(const std::uint8_t version, const std::uint8_t first_framed,
               const std::uint8_t compatible, auto body)
{
    bytes output;
    if (version < first_framed) {
        ::ceph::encode(version, output);
        body(output);

        return output;
    }
    ENCODE_START(version, compatible, output);
    body(output);
    ::ceph::encode(std::uint32_t {0xDEADBEEF}, output);
    ENCODE_FINISH(output);

    return output;
}

bytes request(const ::rgw_cls_list_op& value, const std::uint8_t version)
{
    return envelope(version, 2, 1, [&](bytes& output) {
        if (version < 4) { ::ceph::encode(value.start_obj.name, output); }
        ::ceph::encode(value.num_entries, output);
        if (version >= 3) { ::ceph::encode(value.filter_prefix, output); }
        if (version >= 4) { ::encode(value.start_obj, output); }
        if (version >= 5) { ::ceph::encode(std::uint8_t {0x80}, output); }
        if (version >= 6) { ::ceph::encode(value.delimiter, output); }
    });
}

auto native(const binding::request& value)
{
    ::rgw_cls_list_op result;
    result.start_obj = value.start_obj;
    result.filter_prefix = value.filter_prefix;
    result.delimiter = value.delimiter;
    result.num_entries = value.num_entries;
    result.list_versions = value.list_versions;

    return result;
}

auto native(const auto& value)
{
    ::rgw_cls_list_ret result;
    result.dir.header = value.dir.header;
    for (const auto& [key, entry] : value.dir.m) {
        result.dir.m.emplace(key, entry);
    }
    result.marker = value.marker;
    result.is_truncated = value.is_truncated;
    result.cls_filtered = value.cls_filtered;

    return result;
}

bytes entry(const std::uint8_t version)
{
    ::rgw_bucket_dir_entry value;
    value.key = {version < 8 ? "old" : "current", "retained-instance"};
    value.ver.epoch = 5;
    value.exists = true;
    value.meta.etag = "etag";
    value.index_ver = 17;
    value.tag = "retained-tag";
    value.flags = ::rgw_bucket_dir_entry::FLAG_VER | ::rgw_bucket_dir_entry::FLAG_CURRENT;
    value.versioned_epoch = 19;
    return envelope(version, 3, 1, [&](bytes& output) {
        ::ceph::encode(value.key.name, output);
        ::ceph::encode(value.ver.epoch, output);
        ::ceph::encode(value.exists, output);
        ::encode(value.meta, output);
        ::ceph::encode(value.pending_map, output);
        if (version >= 2) { ::ceph::encode(value.locator, output); }
        if (version >= 4) { ::encode(value.ver, output); }
        if (version >= 5) {
            ::encode_packed_val(value.index_ver, output);
            ::ceph::encode(value.tag, output);
        }
        if (version >= 6) { ::ceph::encode(value.key.instance, output); }
        if (version >= 7) { ::ceph::encode(value.flags, output); }
        if (version >= 8) { ::ceph::encode(value.versioned_epoch, output); }
    });
}

bytes response(const std::uint8_t outer, const std::uint8_t directory_version,
               const std::uint8_t old_leaf)
{
    return envelope(outer, 2, 1, [&](bytes& output) {
        output.append(envelope(directory_version, 2, 1, [&](bytes& directory) {
            ::rgw_bucket_dir_header header;
            header.ver = 23;
            header.max_marker = "marker";
            ::encode(header, directory);
            ::ceph::encode(std::uint32_t {3}, directory);
            ::ceph::encode(std::string {"z"}, directory);
            directory.append(entry(8));
            ::ceph::encode(std::string {"a"}, directory);
            directory.append(entry(6));
            ::ceph::encode(std::string {"z"}, directory);
            directory.append(entry(old_leaf));
        }));
        ::ceph::encode(std::uint8_t {0x80}, output);
        if (outer >= 4) {
            ::encode(::cls_rgw_obj_key {std::string {"n\0m", 3}, "version"}, output);
        }
    });
}

} // anonymous namespace

TEST_CASE("Bucket listing requests preserve all legacy gates and borrowed bytes", "[rgw][cls][datum][bucket-list]")
{
    ::rgw_cls_list_op value;
    value.start_obj = {std::string {"n\0m", 3}, "instance"};
    value.num_entries = std::numeric_limits<std::uint32_t>::max();
    value.filter_prefix = "prefix";
    value.delimiter = "/";
    value.list_versions = true;
    CHECK(binding::encode_request(value.start_obj, value.filter_prefix, value.delimiter,
                                  value.num_entries, value.list_versions).contents_equal(canonical(value)));
    for (std::uint8_t version = 0; version <= 8; ++version) {
        INFO("version=" << unsigned(version));
        const auto input = fragmented(request(value, version));
        ::rgw_cls_list_op expected;
        auto cursor = input.cbegin();
        ::decode(expected, cursor);
        binding::request actual;
        auto current = input.cbegin();
        binding::decode(actual, current);
        CHECK(current.get_off() == cursor.get_off());
        CHECK(canonical(native(actual)).contents_equal(canonical(expected)));
        const auto preserved = canonical(native(actual));
        for (unsigned size = 0; size < input.length(); ++size) {
            bytes prefix;
            prefix.substr_of(input, 0, size);
            current = prefix.cbegin();
            CHECK_THROWS_AS(binding::decode(actual, current), ::ceph::buffer::error);
            CHECK(current.get_off() == 0);
            CHECK(canonical(native(actual)).contents_equal(preserved));
        }
        if (version >= 2) {
            auto invalid = input.to_str();
            invalid[1] = static_cast<char>(255);
            bytes bad;
            bad.append(invalid);
            current = bad.cbegin();
            CHECK_THROWS_AS(binding::decode(actual, current), ::ceph::buffer::error);
            CHECK(current.get_off() == 0);
        }
        bytes compound;
        ::ceph::encode(std::uint16_t {17}, compound);
        compound.append(input);
        ::ceph::encode(std::uint64_t {19}, compound);
        current = compound.cbegin(2);
        binding::decode(actual, current);
        CHECK(current.get_off() == 2 + input.length());
    }
}

TEST_CASE("Bucket replies bind alternate associative containers with last-wins old values", "[rgw][cls][datum][bucket-list]")
{
    using tree_entries = std::map<std::string, ::rgw_bucket_dir_entry>;

    for (std::uint8_t outer = 0; outer <= 6; ++outer) {
        for (std::uint8_t directory = 0; directory <= 3; ++directory) {
            for (std::uint8_t old_leaf = 0; old_leaf <= 9; ++old_leaf) {
                INFO("outer=" << unsigned(outer) << " directory=" << unsigned(directory)
                     << " leaf=" << unsigned(old_leaf));
                const auto input = fragmented(response(outer, directory, old_leaf));
                ::rgw_cls_list_ret expected;
                auto cursor = input.cbegin();
                ::decode(expected, cursor);
                const auto actual = binding::decode_reply<binding::flat_entries>(input);
                const auto tree = binding::decode_reply<tree_entries>(input);
                CHECK(canonical(native(actual)).contents_equal(canonical(expected)));
                CHECK(canonical(native(tree)).contents_equal(canonical(expected)));
                CHECK(actual.cls_filtered == expected.cls_filtered);
                CHECK(tree.cls_filtered == expected.cls_filtered);
                REQUIRE(actual.dir.m.size() == 2);
                CHECK(actual.dir.m.at("z").key.name == (old_leaf < 8 ? "old" : "current"));
                CHECK(actual.dir.m.at("z").key.instance == "retained-instance");
                CHECK(actual.dir.m.at("z").tag == "retained-tag");
                CHECK(actual.dir.m.at("z").versioned_epoch == 19);
                bytes encoded;
                binding::encode(actual, encoded);
                CHECK(encoded.contents_equal(canonical(expected)));
                for (unsigned size = 0; size < input.length(); ++size) {
                    bytes prefix;
                    prefix.substr_of(input, 0, size);
                    CHECK_THROWS_AS(binding::decode_reply<binding::flat_entries>(prefix), ::ceph::buffer::error);
                }
            }
        }
    }
}

TEST_CASE("Bucket associative bounds reject impossible counts and incomplete overwritten values", "[rgw][cls][datum][bucket-list]")
{
    const auto impossible = envelope(4, 2, 2, [&](bytes& output) {
        output.append(envelope(2, 2, 2, [&](bytes& directory) {
            ::encode(::rgw_bucket_dir_header {}, directory);
            ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), directory);
        }));
        ::ceph::encode(false, output);
        ::encode(::cls_rgw_obj_key {}, output);
    });
    CHECK_THROWS_AS(binding::decode_reply<binding::flat_entries>(impossible), ::ceph::buffer::error);
    const auto incomplete_duplicate = envelope(4, 2, 2, [&](bytes& output) {
        output.append(envelope(2, 2, 2, [&](bytes& directory) {
            ::encode(::rgw_bucket_dir_header {}, directory);
            ::ceph::encode(std::uint32_t {2}, directory);
            ::ceph::encode(std::string {"z"}, directory);
            directory.append(entry(8));
            ::ceph::encode(std::string {"z"}, directory);
            ::ceph::encode(std::uint8_t {1}, directory);
        }));
        ::ceph::encode(false, output);
        ::encode(::cls_rgw_obj_key {}, output);
    });
    CHECK_THROWS_AS(binding::decode_reply<binding::flat_entries>(incomplete_duplicate), ::ceph::buffer::error);
    const auto empty = binding::decode_reply<binding::flat_entries>(canonical(::rgw_cls_list_ret {}));
    CHECK(empty.dir.m.empty());
    CHECK_FALSE(empty.is_truncated);
    auto invalid = response(4, 2, 1).to_str();
    invalid[1] = static_cast<char>(255);
    bytes incompatible;
    incompatible.append(invalid);
    CHECK_THROWS_AS(binding::decode_reply<binding::flat_entries>(incompatible), ::ceph::buffer::error);
}
