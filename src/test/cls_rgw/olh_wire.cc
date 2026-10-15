// RGW CLS: OLH native-wire compatibility with independent records/containers.
#include "olh_native_oracle.h"
#include "cls/rgw/cls_rgw_olh_datum.h"
#include "cls/rgw/cls_rgw_olh_wire.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <utility>

#include <map>
#include <random>
#include <string>
#include <vector>

namespace
{
namespace wire = ::cls::rgw::olh_wire;
namespace native = ::cls::rgw::olh_datum;
namespace datum = ::ceph::salad::datum;
namespace olh = ::cls::rgw::olh;
using byte_buffer = ::ceph::buffer::list;

auto fixture()
{
    return olh::reply {{{7, {{{std::string {"n\0m", 3}, "version"}, "tag", 99,
                              olh::action::link, true},
                             {{"other", ""}, "", 0, static_cast<olh::action>(255), false}}},
                       {42, {}}, {std::numeric_limits<std::uint64_t>::max(), {}}}, true};
}

byte_buffer fragmented(const byte_buffer& bytes)
{
    byte_buffer result;
    auto input = bytes.cbegin();
    while (input.get_remaining()) {
        char value;
        input.copy(1, &value);
        result.append(::ceph::buffer::ptr {::ceph::buffer::copy(&value, 1)});
    }

    return result;
}

auto encode(const olh::reply& value, const bool stales = true)
{
    return wire::encode_reply(value.epochs, value.truncated,
        datum::binary_fields<&olh::event::epoch, &olh::event::operation, &olh::event::tag,
                             &olh::event::object, &olh::event::delete_marker> {},
        datum::binary_fields<&olh::object_key::name, &olh::object_key::instance> {},
        [stales](const olh::event& entry) { return stales || olh::action::stale != entry.operation; }, stales);
}

void check(const olh::reply& actual, const olh::reply& expected)
{
    CHECK(actual.truncated == expected.truncated);
    REQUIRE(actual.epochs.size() == expected.epochs.size());
    for (std::size_t i = 0; i < expected.epochs.size(); ++i) {
        CHECK(actual.epochs[i].first == expected.epochs[i].first);
        const auto& rows = actual.epochs[i].second;
        const auto& wanted = expected.epochs[i].second;
        REQUIRE(rows.size() == wanted.size());
        for (std::size_t j = 0; j < wanted.size(); ++j) {
            CHECK(rows[j].epoch == wanted[j].epoch);
            CHECK(rows[j].operation == wanted[j].operation);
            CHECK(rows[j].tag == wanted[j].tag);
            CHECK(rows[j].object.name == wanted[j].object.name);
            CHECK(rows[j].object.instance == wanted[j].object.instance);
            CHECK(rows[j].delete_marker == wanted[j].delete_marker);
        }
    }
}

} // anonymous namespace

TEST_CASE("OLH requests preserve native bytes, versions and nested extensions", "[rgw][cls][datum][olh]")
{
    const rgw_olh_oracle::request expected {{std::string {"n\0m", 3}, "instance"},
                                            std::string {"t\0g", 3}, 0xFEDCBA9876543210, true};
    const auto bytes = wire::encode_request(expected.key.name, expected.key.instance,
                                            expected.marker, expected.tag, expected.stales);
    CHECK(bytes.contents_equal(rgw_olh_oracle::encode_request(expected)));
    const auto decoded = rgw_olh_oracle::decode_request(bytes);
    CHECK(decoded.key.name == expected.key.name);
    CHECK(decoded.key.instance == expected.key.instance);
    CHECK(decoded.marker == expected.marker);
    CHECK(decoded.tag == expected.tag);
    CHECK(decoded.stales);
    for (const auto version : {1, 2, 3}) {
        const auto input = fragmented(rgw_olh_oracle::encode_request(expected, version, true));
        auto cursor = input.cbegin();
        native::request result;
        native::decode(result, cursor);
        CHECK(result.olh.name == expected.key.name);
        CHECK(result.olh.instance == expected.key.instance);
        CHECK(result.olh_tag == expected.tag);
        CHECK(result.ver_marker == expected.marker);
        CHECK(result.get_stales == (version >= 2));
        CHECK(cursor.end());
    }
}

TEST_CASE("OLH event and nested map bindings preserve native encoding", "[rgw][cls][datum][olh]")
{
    const auto expected = fixture();
    const auto bytes = encode(expected);
    CHECK(bytes.contents_equal(rgw_olh_oracle::encode_reply(expected)));
    check(rgw_olh_oracle::decode_reply(bytes), expected);
    for (const bool extensions : {false, true}) {
        const auto input = fragmented(rgw_olh_oracle::encode_reply(expected, extensions));
        olh::reply result;
        native::decode_reply(input, result);
        check(result, expected);
        ::rgw_cls_read_olh_log_ret native_result;
        native::decode_reply(input, native_result);
        byte_buffer native_bytes;
        ::encode(native_result, native_bytes);
        CHECK(native_bytes.contents_equal(bytes));
        byte_buffer rebound;
        native::encode_reply({native_result.log.begin(), native_result.log.end(), native_result.log.size()}, true,
                             native_result.is_truncated, rebound);
        CHECK(rebound.contents_equal(bytes));
    }
}

TEST_CASE("OLH vector results retain native sorted first-wins map semantics", "[rgw][cls][datum][olh]")
{
    auto expected = fixture();
    expected.epochs[0].first = 42;
    expected.epochs[1].first = 7;
    expected.epochs.emplace_back(expected.epochs.front());
    expected.epochs.back().second.front().tag = "discard duplicate";
    const auto bytes = rgw_olh_oracle::encode_reply(expected);
    olh::reply result;
    native::decode_reply(bytes, result);
    check(result, rgw_olh_oracle::decode_reply(bytes));
    REQUIRE(result.epochs.size() == 3);
    CHECK(result.epochs[0].first == 7);
    CHECK(result.epochs[1].second.front().tag == "tag");
}

TEST_CASE("OLH framing rejects every proper truncation without publishing candidates", "[rgw][cls][datum][olh]")
{
    const auto full = encode(fixture());
    for (unsigned size = 0; size < full.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(full, 0, size);
        auto value = fixture();
        CHECK_THROWS_AS(native::decode_reply(prefix, value), ::ceph::buffer::error);
        check(value, fixture());
        ::rgw_cls_read_olh_log_ret compatible;
        compatible.log[123];
        CHECK_THROWS_AS(native::decode_reply(prefix, compatible), ::ceph::buffer::error);
        CHECK(compatible.log.contains(123));
        CHECK(compatible.log.size() == 1);
    }
    const auto request = wire::encode_request("name", "", 9, "tag", true);
    for (unsigned size = 0; size < request.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(request, 0, size);
        auto cursor = prefix.cbegin();
        native::request value;
        value.olh_tag = "preserve";
        CHECK_THROWS_AS(native::decode(value, cursor), ::ceph::buffer::error);
        CHECK(value.olh_tag == "preserve");
        CHECK(cursor.get_off() == 0);
    }
}

TEST_CASE("OLH bounds reject impossible group and event counts before allocation", "[rgw][cls][datum][olh]")
{
    for (const unsigned offset : {6, 18}) {
        auto bytes = encode(fixture());
        const char impossible[] = {'\xFF', '\xFF', '\xFF', '\xFF'};
        auto cursor = bytes.begin(offset);
        cursor.copy_in(4, impossible);
        olh::reply value;
        CHECK_THROWS_AS(native::decode_reply(bytes, value), ::ceph::buffer::error);
    }
    auto bytes = encode(fixture());
    // The first event frame cannot consume bytes outside its enclosing reply.
    auto cursor = bytes.begin(24);
    const char impossible[] = {'\xFF', '\xFF', '\xFF', '\xFF'};
    cursor.copy_in(4, impossible);
    olh::reply value;
    CHECK_THROWS_AS(native::decode_reply(bytes, value), ::ceph::buffer::error);
}

TEST_CASE("OLH preserves all action bytes and accepts native nonzero flags", "[rgw][cls][datum][olh]")
{
    for (unsigned raw = 0; raw < 256; ++raw) {
        olh::reply expected {{{1, {{{"n", "i"}, "t", 2, static_cast<olh::action>(raw), true}}}}, true};
        auto bytes = encode(expected);
        CHECK(bytes.contents_equal(rgw_olh_oracle::encode_reply(expected)));
        const char flag = static_cast<char>(raw);
        bytes.begin(bytes.length() - 1).copy_in(1, &flag);
        expected.truncated = 0 != raw;
        // The event's final byte immediately precedes the reply's final byte.
        bytes.begin(bytes.length() - 2).copy_in(1, &flag);
        expected.epochs.front().second.front().delete_marker = 0 != raw;
        olh::reply result;
        native::decode_reply(bytes, result);
        check(result, expected);
        check(result, rgw_olh_oracle::decode_reply(bytes));
    }
}

TEST_CASE("OLH filtering drops stale rows and empty groups after selection", "[rgw][cls][datum][olh]")
{
    auto value = fixture();
    value.epochs.front().second.front().operation = olh::action::stale;
    const auto bytes = encode(value, false);
    value.epochs.resize(1);
    value.epochs.front().second.erase(value.epochs.front().second.begin());
    check(rgw_olh_oracle::decode_reply(bytes), value);
}

TEST_CASE("OLH randomized nested pages agree with the native codec", "[rgw][cls][datum][olh]")
{
    std::mt19937 random {0xC15DA7};
    for (unsigned sample = 0; sample < 200; ++sample) {
        olh::reply expected;
        expected.truncated = random() % 2;
        for (unsigned group = 0, count = random() % 15; group < count; ++group) {
            auto& rows = expected.epochs.emplace_back(random() % 50, std::vector<olh::event> {}).second;
            for (unsigned row = 0, count = random() % 5; row < count; ++row) {
                rows.push_back({{std::string(random() % 40, static_cast<char>(random())), "instance"},
                                std::string(random() % 40, static_cast<char>(random())), random(),
                                static_cast<olh::action>(random() % 256), 0 != random() % 2});
            }
        }
        const auto bytes = encode(expected);
        CHECK(bytes.contents_equal(rgw_olh_oracle::encode_reply(expected)));
        olh::reply result;
        native::decode_reply(bytes, result);
        check(result, rgw_olh_oracle::decode_reply(bytes));
    }
}
