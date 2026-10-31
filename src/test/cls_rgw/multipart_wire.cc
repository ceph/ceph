// RGW CLS: Native multipart compatibility without normalizing decoded manifests.
#include "test/cls_rgw/wire_test.h"
#include "rgw/datum/adapters/cls/multipart.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <limits>

namespace {

namespace binding = ::cls::rgw::multipart_datum;
using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;
using ::rgw_cls_test::fragmented;

void fill(::RGWUploadPartInfo& value)
{
    value.num = std::numeric_limits<std::uint32_t>::max();
    value.size = std::numeric_limits<std::uint64_t>::max();
    value.accounted_size = 17;
    value.etag = std::string {"e\0t", 3};
    value.modified = ::ceph::real_time {std::chrono::seconds {23} + std::chrono::nanoseconds {29}};
    ::rgw_placement_rule placement;
    placement.name = "head-placement";
    placement.storage_class = "STANDARD";
    value.manifest.set_head(placement, {}, 31);
    value.manifest.set_trivial_rule(31, 4096);
    value.manifest.set_obj_size(73);
    value.manifest.set_prefix("current");
    value.cs_info.compression_type = "zstd";
    value.cs_info.orig_size = 37;
    value.cs_info.compressor_message = -41;
    value.cs_info.blocks = {{0, 0, 11}, {11, 7, 13}};
    value.past_prefixes = {"z", std::string {"a\0b", 3}};
    value.cksum.emplace(::rgw::cksum::Type::crc32c);
    value.cksum->digest.fill(0x37);
    value.crypt_salt = std::string {"s\0t", 3};
}

bytes frame(const std::uint8_t version, const auto& body, const std::uint8_t compatible = 0)
{
    bytes output;
    ENCODE_START(version, compatible, output);
    body(output);
    if (version > 7) { ::ceph::encode(std::uint64_t {47}, output); }
    ENCODE_FINISH(output);

    return output;
}

bytes leaf(const ::RGWUploadPartInfo& value, const std::uint8_t version)
{
    const auto body = [&](bytes& output) {
        ::ceph::encode(value.num, output);
        ::ceph::encode(value.size, output);
        ::ceph::encode(value.etag, output);
        ::ceph::encode(value.modified, output);
        if (version >= 3) { ::encode(value.manifest, output); }
        if (version >= 4) {
            ::encode(value.cs_info, output);
            ::ceph::encode(value.accounted_size, output);
        }
        if (version >= 5) { ::ceph::encode(value.past_prefixes, output); }
        if (version >= 6) { ::ceph::encode(value.cksum, output); }
        if (version >= 7) { ::ceph::encode(value.crypt_salt, output); }
    };
    if (version >= 2) { return frame(version, body, 2); }
    bytes output;
    ::ceph::encode(version, output);
    body(output);

    return output;
}

bytes request(const bytes& info, const std::uint8_t version = 1)
{
    return frame(version, [&](bytes& output) {
        ::ceph::encode(std::string {"p\0t", 3}, output);
        output.append(info);
    });
}

} // anonymous namespace

TEST_CASE("Multipart builder retains native manifest copy semantics while borrowing other fields", "[rgw][cls][datum][multipart]")
{
    ::RGWUploadPartInfo info;
    fill(info);
    ::cls_rgw_mp_upload_part_info_update_op expected;
    expected.part_key = std::string {"p\0t", 3};
    expected.info = info;
    CHECK(expected.info.manifest.get_head_placement_rule().name.empty());
    CHECK(info.manifest.get_head_placement_rule().name == "head-placement");
    const auto full = canonical(expected);
    auto actual = binding::encode_update(expected.part_key, info);
    CHECK(actual.contents_equal(full));
    CHECK_FALSE(request(canonical(info)).contents_equal(full));
    expected.part_key.assign(100, 'x');
    info.etag.assign(100, 'x');
    info.cs_info.blocks.clear();
    info.past_prefixes.clear();
    info.crypt_salt.clear();
    CHECK(actual.contents_equal(full));
}

TEST_CASE("Multipart module records preserve native old and future upload leaves in place", "[rgw][cls][datum][multipart]")
{
    ::RGWUploadPartInfo info;
    fill(info);
    for (const std::uint8_t outer : {0, 1, 10}) {
        for (const std::uint8_t version : {0, 1, 2, 3, 4, 5, 6, 7, 10}) {
            const auto full = fragmented(request(leaf(info, version), outer));
            ::cls_rgw_mp_upload_part_info_update_op expected;
            auto cursor = full.cbegin();
            ::decode(expected, cursor);
            const auto consumed = cursor.get_off();
            binding::update_request actual;
            cursor = full.cbegin();
            binding::decode(actual, cursor);
            CHECK(cursor.get_off() == consumed);
            CHECK(actual.part_key == expected.part_key);
            CHECK(canonical(actual.info).contents_equal(canonical(expected.info)));
            CHECK(actual.info.manifest.get_head_placement_rule().name == (version >= 3 ? "head-placement" : ""));
            CHECK(actual.info.accounted_size == (version >= 4 ? 17 : info.size));
            for (unsigned size = 0; size < full.length(); ++size) {
                bytes prefix;
                prefix.substr_of(full, 0, size);
                binding::update_request local;
                cursor = prefix.cbegin();
                CHECK_THROWS_AS(binding::decode(local, cursor), ::ceph::buffer::error);
                CHECK(cursor.get_off() == 0);
            }
            bytes compound;
            ::ceph::encode(std::uint16_t {11}, compound);
            compound.append(full);
            ::ceph::encode(std::uint64_t {13}, compound);
            cursor = compound.cbegin(2);
            binding::update_request local;
            binding::decode(local, cursor);
            CHECK(cursor.get_off() == consumed + 2);
        }
    }
}

TEST_CASE("Multipart rejects incompatible frames and nested reads beyond the outer boundary", "[rgw][cls][datum][multipart]")
{
    ::RGWUploadPartInfo info;
    fill(info);
    auto short_outer = request(canonical(info));
    auto raw = short_outer.to_str();
    const auto declared = short_outer.length() - 7;
    for (unsigned index = 0; index < 4; ++index) {
        raw[2 + index] = static_cast<char>(declared >> (8 * index));
    }
    short_outer.clear();
    short_outer.append(raw);
    for (const auto& input : {frame(10, [](bytes&) {}, 20), request(frame(10, [](bytes&) {}, 20)), short_outer}) {
        binding::update_request local;
        auto cursor = input.cbegin();
        CHECK_THROWS_AS(binding::decode(local, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
    }
}
