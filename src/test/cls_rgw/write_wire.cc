// RGW CLS: C++23 native codecs independently check private module bindings.
#include "test/cls_rgw/wire_test.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "rgw/datum/adapters/cls/write.h"

#include <catch2/catch_test_macros.hpp>

#include <concepts>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

namespace {

namespace binding = ::cls::rgw::write_datum;
using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;
using ::rgw_cls_test::fragmented;

bytes envelope(const std::uint8_t version, const bool old, auto body)
{
    bytes output;
    if (old) {
        ::ceph::encode(version, output);
        body(output);

        return output;
    }
    ENCODE_START(version, 1, output);
    body(output);
    if (version > 9) {
        ::ceph::encode(std::uint64_t {0xDEADBEEF}, output);
    }
    ENCODE_FINISH(output);

    return output;
}

bytes encoded(const ::rgw_cls_obj_prepare_op& value, const std::uint8_t version)
{
    return envelope(version, version < 3, [&](bytes& output) {
        ::ceph::encode(static_cast<std::uint8_t>(value.op), output);
        if (version < 5) { ::ceph::encode(value.key.name, output); }
        ::ceph::encode(value.tag, output);
        if (version >= 2) { ::ceph::encode(value.locator, output); }
        if (version >= 4) { ::ceph::encode(std::uint8_t {0x80}, output); }
        if (version >= 5) { ::encode(value.key, output); }
        if (version >= 6) { ::ceph::encode(value.bilog_flags, output); }
        if (version >= 7) { ::encode(value.zones_trace, output); }
    });
}

bytes encoded(const ::rgw_cls_obj_complete_op& value, const std::uint8_t version,
              const bool impossible_count = false)
{
    return envelope(version, version < 3, [&](bytes& output) {
        ::ceph::encode(static_cast<std::uint8_t>(value.op), output);
        if (version < 7) { ::ceph::encode(value.key.name, output); }
        ::ceph::encode(value.ver.epoch, output);
        ::encode(value.meta, output);
        ::ceph::encode(value.op_tag, output);
        if (version >= 2) { ::ceph::encode(value.locator, output); }
        ::ceph::encode(impossible_count ? std::numeric_limits<std::uint32_t>::max()
                                        : static_cast<std::uint32_t>(value.remove_objs.size()), output);
        if (!impossible_count) {
            for (const auto& key : value.remove_objs) {
                if (version >= 4 && version < 7) { ::ceph::encode(key.name, output); }
                if (version < 4 || version >= 7) { ::encode(key, output); }
            }
        }
        if (version >= 5) { ::encode(value.ver, output); }
        if (version >= 6) { ::ceph::encode(std::uint8_t {0x80}, output); }
        if (version >= 7) { ::encode(value.key, output); }
        if (version >= 8) { ::ceph::encode(value.bilog_flags, output); }
        if (version >= 9) { ::encode(value.zones_trace, output); }
    });
}

bytes encoded(const ::rgw_cls_link_olh_op& value, const std::uint8_t version)
{
    return envelope(version, false, [&](bytes& output) {
        ::encode(value.key, output);
        ::ceph::encode(value.olh_tag, output);
        ::ceph::encode(std::uint8_t {0x80}, output);
        ::ceph::encode(value.op_tag, output);
        ::encode(value.meta, output);
        ::ceph::encode(value.olh_epoch, output);
        ::ceph::encode(std::uint8_t {0x80}, output);
        ::ceph::encode(value.bilog_flags, output);
        if (version >= 2) {
            ::ceph::encode(std::uint64_t {123}, output);
        }
        if (version >= 3) { ::ceph::encode(value.unmod_since, output); }
        if (version >= 4) { ::ceph::encode(std::uint8_t {0x80}, output); }
        if (version >= 5) { ::encode(value.zones_trace, output); }
    });
}

bytes encoded(const ::rgw_cls_trim_olh_log_op& value, const std::uint8_t version)
{
    return envelope(version, false, [&](bytes& output) {
        ::encode(value.olh, output);
        ::ceph::encode(value.ver, output);
        ::ceph::encode(value.olh_tag, output);
    });
}

bytes encoded(const ::rgw_cls_unlink_instance_op& value, const std::uint8_t version)
{
    return envelope(version, false, [&](bytes& output) {
        ::encode(value.key, output);
        ::ceph::encode(value.op_tag, output);
        ::ceph::encode(value.olh_epoch, output);
        ::ceph::encode(std::uint8_t {0x80}, output);
        ::ceph::encode(value.bilog_flags, output);
        if (version >= 2) { ::ceph::encode(value.olh_tag, output); }
        if (version >= 3) { ::encode(value.zones_trace, output); }
    });
}

bytes encoded(const ::rgw_cls_bucket_clear_olh_op& value, const std::uint8_t version)
{
    return envelope(version, false, [&](bytes& output) {
        ::encode(value.key, output);
        ::ceph::encode(value.olh_tag, output);
    });
}

bytes encoded(const ::rgw_cls_refresh_instance_op& value, const std::uint8_t version)
{
    return envelope(version, false, [&](bytes& output) {
        ::encode(value.key, output);
    });
}

auto native(const binding::prepare_request& value)
{
    ::rgw_cls_obj_prepare_op result;
    result.key = value.key;
    result.tag = value.tag;
    result.locator = value.locator;
    result.op = value.op;
    result.log_op = value.log_op;
    result.bilog_flags = value.bilog_flags;
    result.zones_trace = value.zones_trace;

    return result;
}

auto native(const binding::complete_request& value)
{
    ::rgw_cls_obj_complete_op result;
    result.key = value.key;
    result.ver = value.ver;
    result.meta = value.meta;
    result.remove_objs.assign(value.remove_objs.begin(), value.remove_objs.end());
    result.op_tag = value.op_tag;
    result.locator = value.locator;
    result.zones_trace = value.zones_trace;
    result.op = value.op;
    result.log_op = value.log_op;
    result.bilog_flags = value.bilog_flags;

    return result;
}

auto native(const binding::link_request& value)
{
    ::rgw_cls_link_olh_op result;
    result.key = value.key;
    result.meta = value.meta;
    result.olh_tag = value.olh_tag;
    result.op_tag = value.op_tag;
    result.zones_trace = value.zones_trace;
    result.olh_epoch = value.olh_epoch;
    result.unmod_since = value.unmod_since;
    result.delete_marker = value.delete_marker;
    result.log_op = value.log_op;
    result.high_precision_time = value.high_precision_time;
    result.bilog_flags = value.bilog_flags;

    return result;
}

auto native(const binding::trim_request& value)
{
    ::rgw_cls_trim_olh_log_op result;
    result.olh = value.olh;
    result.olh_tag = value.olh_tag;
    result.ver = value.ver;

    return result;
}

auto native(const binding::unlink_request& value)
{
    ::rgw_cls_unlink_instance_op result;
    result.key = value.key;
    result.op_tag = value.op_tag;
    result.olh_tag = value.olh_tag;
    result.olh_epoch = value.olh_epoch;
    result.zones_trace = value.zones_trace;
    result.log_op = value.log_op;
    result.bilog_flags = value.bilog_flags;

    return result;
}

auto native(const binding::clear_request& value)
{
    ::rgw_cls_bucket_clear_olh_op result;
    result.key = value.key;
    result.olh_tag = value.olh_tag;

    return result;
}

auto native(const binding::refresh_request& value)
{
    ::rgw_cls_refresh_instance_op result;
    result.key = value.key;

    return result;
}

template <typename ModuleT, typename NativeT>
void check_versions(const NativeT& fixture, const unsigned maximum)
{
    for (unsigned version = 0; version <= maximum; ++version) {
        INFO("version=" << version);
        const auto input = fragmented(encoded(fixture, version));
        NativeT expected;
        auto expected_cursor = input.cbegin();
        ::decode(expected, expected_cursor);
        ModuleT actual;
        auto cursor = input.cbegin();
        binding::decode(actual, cursor);
        CHECK(cursor.get_off() == expected_cursor.get_off());
        CHECK(canonical(native(actual)).contents_equal(canonical(expected)));
        if constexpr (std::same_as<ModuleT, binding::complete_request>) {
            CHECK(actual.ver.epoch == expected.ver.epoch);
            CHECK(actual.ver.pool == expected.ver.pool);
            CHECK(std::ranges::equal(actual.remove_objs, expected.remove_objs));
        }
        if constexpr (std::same_as<ModuleT, binding::link_request>) {
            CHECK(actual.unmod_since == expected.unmod_since);
        }

        const auto preserved = canonical(native(actual));
        const bool has_header = version >= 3 ||
            (!std::same_as<ModuleT, binding::prepare_request> &&
             !std::same_as<ModuleT, binding::complete_request>);
        if (has_header) {
            for (const bool incompatible : {false, true}) {
                auto invalid = input.to_str();
                if (incompatible) {
                    invalid[1] = static_cast<char>(255);
                }
                if (!incompatible) {
                    std::fill_n(invalid.begin() + 2, 4, char {0});
                }
                bytes malformed;
                malformed.append(invalid);
                auto rejected = malformed.cbegin();
                CHECK_THROWS_AS(binding::decode(actual, rejected), ::ceph::buffer::error);
                CHECK(rejected.get_off() == 0);
                CHECK(canonical(native(actual)).contents_equal(preserved));
            }
        }
        for (unsigned length = 0; length < input.length(); ++length) {
            bytes prefix;
            prefix.substr_of(input, 0, length);
            auto truncated = prefix.cbegin();
            CHECK_THROWS_AS(binding::decode(actual, truncated), ::ceph::buffer::error);
            CHECK(truncated.get_off() == 0);
            CHECK(canonical(native(actual)).contents_equal(preserved));
        }

        bytes compound = input;
        ::ceph::encode(std::uint64_t {0x12345678}, compound);
        cursor = compound.cbegin();
        binding::decode(actual, cursor);
        CHECK(cursor.get_off() == input.length());

        bytes prefixed;
        ::ceph::encode(std::uint16_t {17}, prefixed);
        prefixed.append(input);
        cursor = prefixed.cbegin();
        cursor += 2;
        binding::decode(actual, cursor);
        CHECK(cursor.end());
        CHECK(canonical(native(actual)).contents_equal(preserved));
    }
}

} // anonymous namespace

TEST_CASE("Independent prepare requests preserve all native versions", "[rgw][cls][datum][write]")
{
    ::rgw_cls_obj_prepare_op request;
    request.op = ::CLS_RGW_OP_DEL;
    request.key = {std::string {"n\0m", 3}, "instance"};
    request.tag = std::string {"t\0g", 3};
    request.locator = "locator";
    request.bilog_flags = 0xFEDC;
    request.zones_trace.insert("zone", "location");
    check_versions<binding::prepare_request>(request, 10);
}

TEST_CASE("Independent complete requests decode the native list into a vector", "[rgw][cls][datum][write]")
{
    static_assert(std::same_as<decltype(binding::complete_request::remove_objs), std::vector<::cls_rgw_obj_key>>);
    ::rgw_cls_obj_complete_op request;
    request.key = {std::string {"n\0m", 3}, "instance"};
    request.op_tag = std::string {"t\0g", 3};
    request.locator = "locator";
    request.ver.pool = 123;
    request.ver.epoch = 65536;
    request.meta.size = 17;
    request.meta.accounted_size = 23;
    request.meta.owner = "owner";
    request.remove_objs = {{"remove", "v1"}, {"remove", "v1"}, {"last", "v2"}};
    request.bilog_flags = 0xFEDC;
    request.zones_trace.insert("zone", "location");
    check_versions<binding::complete_request>(request, 10);
    const auto input = encoded(request, 9, true);
    auto cursor = input.cbegin();
    binding::complete_request result;
    CHECK_THROWS_AS(binding::decode(result, cursor), ::ceph::buffer::error);
    CHECK(cursor.get_off() == 0);
}

TEST_CASE("Independent OLH link requests preserve timestamp version rules", "[rgw][cls][datum][write]")
{
    ::rgw_cls_link_olh_op request;
    request.key = {"object", "instance"};
    request.olh_tag = std::string {"o\0h", 3};
    request.op_tag = "operation";
    request.olh_epoch = std::numeric_limits<std::uint64_t>::max();
    request.meta.owner = "owner";
    request.meta.size = 17;
    request.unmod_since = ::ceph::real_clock::from_time_t(456) + std::chrono::nanoseconds {789};
    request.bilog_flags = 0xFEDC;
    request.zones_trace.insert("zone", "location");
    check_versions<binding::link_request>(request, 10);
}

TEST_CASE("Independent OLH trim requests retain native framing", "[rgw][cls][datum][write]")
{
    ::rgw_cls_trim_olh_log_op request;
    request.olh.name = std::string {"n\0m", 3};
    request.olh_tag = std::string {"t\0g", 3};
    request.ver = std::numeric_limits<std::uint64_t>::max();
    check_versions<binding::trim_request>(request, 10);
}

TEST_CASE("Independent OLH unlink requests preserve tag and zone versions", "[rgw][cls][datum][write]")
{
    ::rgw_cls_unlink_instance_op request;
    request.key = {std::string {"n\0m", 3}, "instance"};
    request.op_tag = std::string {"t\0g", 3};
    request.olh_tag = "olh-tag";
    request.olh_epoch = std::numeric_limits<std::uint64_t>::max();
    request.bilog_flags = 0xFEDC;
    request.zones_trace.insert("zone", "location");
    check_versions<binding::unlink_request>(request, 10);
}

TEST_CASE("Independent OLH clear and refresh requests reuse the key profile", "[rgw][cls][datum][write]")
{
    ::rgw_cls_bucket_clear_olh_op clear;
    clear.key.name = std::string {"n\0m", 3};
    clear.olh_tag = std::string {"t\0g", 3};
    check_versions<binding::clear_request>(clear, 10);
    ::rgw_cls_refresh_instance_op refresh;
    refresh.key = clear.key;
    refresh.key.instance = "instance";
    check_versions<binding::refresh_request>(refresh, 10);
}
