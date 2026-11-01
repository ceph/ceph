// RGW CLS: Native C++23 codecs check independent bilog messages and sequences.
#include "test/cls_rgw/wire_test.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "rgw/datum/adapters/cls/bilog.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <type_traits>

#include <algorithm>
#include <chrono>
#include <list>
#include <string>
#include <vector>

namespace {

namespace binding = ::cls::rgw::bilog_datum;
using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;
using ::rgw_cls_test::fragmented;

bytes envelope(const std::uint8_t version, auto body)
{
    bytes output;
    ENCODE_START(version, 1, output);
    body(output);
    if (version > 1) {
        ::ceph::encode(std::uint64_t {0xDEADBEEF}, output);
    }
    ENCODE_FINISH(output);

    return output;
}

template <typename ModuleT, typename NativeT>
void requests(const NativeT& fixture)
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        INFO("version=" << unsigned(version));
        const auto input = fragmented(envelope(version, [&](bytes& output) {
            if constexpr (std::is_same_v<NativeT, ::cls_rgw_bi_log_list_op>) {
                ::ceph::encode(fixture.marker, output);
                ::ceph::encode(fixture.max, output);
            } else {
                ::ceph::encode(fixture.start_marker, output);
                ::ceph::encode(fixture.end_marker, output);
            }
        }));
        NativeT expected;
        auto native_cursor = input.cbegin();
        ::decode(expected, native_cursor);
        ModuleT actual;
        auto cursor = input.cbegin();
        binding::decode(actual, cursor);
        CHECK(cursor.get_off() == native_cursor.get_off());
        if constexpr (std::is_same_v<NativeT, ::cls_rgw_bi_log_list_op>) {
            CHECK(actual.marker == expected.marker);
            CHECK(actual.max == expected.max);
        } else {
            CHECK(actual.start_marker == expected.start_marker);
            CHECK(actual.end_marker == expected.end_marker);
        }
        const auto original = actual;
        for (unsigned size = 0; size < input.length(); ++size) {
            bytes short_input;
            short_input.substr_of(input, 0, size);
            auto rejected = short_input.cbegin();
            CHECK_THROWS_AS(binding::decode(actual, rejected), ::ceph::buffer::error);
            CHECK(rejected.get_off() == 0);
            if constexpr (std::is_same_v<NativeT, ::cls_rgw_bi_log_list_op>) {
                CHECK(actual.marker == original.marker);
                CHECK(actual.max == original.max);
            } else {
                CHECK(actual.start_marker == original.start_marker);
                CHECK(actual.end_marker == original.end_marker);
            }
        }
        for (const bool compatible : {false, true}) {
            auto invalid = input.to_str();
            if (compatible) {
                invalid[1] = static_cast<char>(255);
            } else {
                std::fill_n(invalid.begin() + 2, 4, char {0});
            }
            bytes malformed;
            malformed.append(invalid);
            cursor = malformed.cbegin();
            CHECK_THROWS_AS(binding::decode(actual, cursor), ::ceph::buffer::error);
            CHECK(cursor.get_off() == 0);
        }
        bytes compound;
        ::ceph::encode(std::uint16_t {17}, compound);
        compound.append(input);
        ::ceph::encode(std::uint64_t {19}, compound);
        cursor = compound.cbegin(2);
        binding::decode(actual, cursor);
        CHECK(cursor.get_off() == 2 + input.length());
    }
}

bytes log_entry(const std::uint8_t version)
{
    return envelope(version, [&](bytes& output) {
        ::ceph::encode(std::string {"id\0x", 4}, output);
        ::ceph::encode(std::string {"n\0m", 3}, output);
        ::ceph::encode(::ceph::real_time {std::chrono::seconds {123}}, output);
        ::rgw_bucket_entry_ver ver;
        ver.pool = -1;
        ver.epoch = 65536;
        ::encode(ver, output);
        ::ceph::encode(std::string {"tag"}, output);
        ::ceph::encode(std::uint8_t {::CLS_RGW_OP_DEL}, output);
        ::ceph::encode(std::uint8_t {::CLS_RGW_STATE_COMPLETE}, output);
        ::encode_packed_val(std::uint64_t {65536}, output);
        if (version >= 2) {
            ::ceph::encode(std::string {"instance"}, output);
            ::ceph::encode(std::uint16_t {0xFEDC}, output);
        }
        if (version >= 3) {
            ::ceph::encode(std::string {"owner"}, output);
            ::ceph::encode(std::string {"display"}, output);
        }
        if (version >= 4) {
            ::rgw_zone_set zones;
            zones.insert("zone", "location");
            ::encode(zones, output);
        }
    });
}

} // anonymous namespace

TEST_CASE("Bilog list requests borrow native fields and publish independently", "[rgw][cls][datum][bilog]")
{
    ::cls_rgw_bi_log_list_op request;
    request.marker = std::string {"m\0r", 3};
    request.max = std::numeric_limits<std::uint32_t>::max();
    CHECK(binding::encode_list(request.marker, request.max).contents_equal(canonical(request)));
    requests<binding::list_request>(request);
}

TEST_CASE("Bilog trim requests preserve both marker encodings", "[rgw][cls][datum][bilog]")
{
    ::cls_rgw_bi_log_trim_op request;
    request.start_marker = std::string {"s\0t", 3};
    request.end_marker = std::string(80, 'e');
    CHECK(binding::encode_trim(request.start_marker, request.end_marker).contents_equal(canonical(request)));
    requests<binding::trim_request>(request);
}

TEST_CASE("Bilog counted replies bind vectors and lists with native leaf versions", "[rgw][cls][datum][bilog]")
{
    for (const std::uint8_t outer : {0, 1, 2, 10}) {
        for (const std::uint8_t leaf : {0, 1, 2, 3, 4, 5}) {
            INFO("outer=" << unsigned(outer) << " leaf=" << unsigned(leaf));
            const auto input = fragmented(envelope(outer, [&](bytes& output) {
                ::ceph::encode(std::uint32_t {2}, output);
                output.append(log_entry(leaf));
                output.append(log_entry(leaf));
                ::ceph::encode(std::uint8_t {0x80}, output);
            }));
            ::cls_rgw_bi_log_list_ret expected;
            auto cursor = input.cbegin();
            ::decode(expected, cursor);
            const auto [vector, vector_flag] = binding::decode_reply<std::vector<::rgw_bi_log_entry>>(input);
            auto [list, list_flag] = binding::decode_reply<std::list<::rgw_bi_log_entry>>(input);
            CHECK(vector.size() == 2);
            CHECK(list.size() == 2);
            CHECK(vector_flag == expected.truncated);
            CHECK(list_flag == expected.truncated);
            ::cls_rgw_bi_log_list_ret compatible;
            compatible.entries = std::move(list);
            compatible.truncated = list_flag;
            CHECK(canonical(compatible).contents_equal(canonical(expected)));
            binding::reply module {vector, vector_flag};
            bytes encoded;
            binding::encode(module, encoded);
            CHECK(encoded.contents_equal(canonical(expected)));
            for (unsigned size = 0; size < input.length(); ++size) {
                bytes prefix;
                prefix.substr_of(input, 0, size);
                CHECK_THROWS_AS(binding::decode_reply<std::vector<::rgw_bi_log_entry>>(prefix), ::ceph::buffer::error);
            }
        }
    }
    ::cls_rgw_bi_log_list_ret empty;
    const auto [entries, flag] = binding::decode_reply<std::vector<::rgw_bi_log_entry>>(canonical(empty));
    CHECK(entries.empty());
    CHECK_FALSE(flag);
    const auto impossible = envelope(1, [&](bytes& output) {
        ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
        ::ceph::encode(std::uint8_t {0}, output);
    });
    CHECK_THROWS_AS(binding::decode_reply<std::vector<::rgw_bi_log_entry>>(impossible), ::ceph::buffer::error);
}
