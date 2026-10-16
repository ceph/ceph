// RGW CLS: Native compatibility and independent Datum/zpp representations.
#include "bi_native_oracle.h"
#include "cls/rgw/cls_rgw_bi_datum.h"
#include "cls/rgw/cls_rgw_bi_wire.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>

#include <limits>
#include <map>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace
{
namespace wire = ::cls::rgw::bi_wire;
namespace native = ::cls::rgw::bi_datum;
namespace datum = ::ceph::salad::datum;
using byte_buffer = ::ceph::buffer::list;

byte_buffer payload()
{
    byte_buffer bytes;
    bytes.append("abc", 3);
    auto second = ::ceph::buffer::copy("\0\xFF!", 3);
    bytes.append(::ceph::buffer::ptr {std::move(second)});

    return bytes;
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

auto fixtures()
{
    return std::vector<rgw_bi_oracle::row> {
        {3, std::string {"a\0b", 3}, payload()},
        {255, "last", {}}
    };
}

struct client_row
{
    byte_buffer contents;
    std::uint8_t tag;
    std::string name;
};

constexpr auto client_row_fields = datum::binary_fields<&client_row::tag, &client_row::name, &client_row::contents> {};

struct counted_row final : client_row
{
    inline static unsigned constructions = 0;

    counted_row() { ++constructions; }

};

void check_rows(const auto& actual, const std::vector<rgw_bi_oracle::row>& expected)
{
    REQUIRE(actual.size() == expected.size());
    for (std::size_t index = 0; index < expected.size(); ++index) {
        CHECK(actual[index].tag == expected[index].kind);
        CHECK(actual[index].name == expected[index].key);
        CHECK(actual[index].contents.contents_equal(expected[index].payload));
    }
}

} // anonymous namespace

TEST_CASE("BI requests preserve native bytes and old version defaults", "[rgw][cls][datum]")
{
    const rgw_bi_oracle::request expected {997, std::string {"f\0x", 3}, "marker", true};
    auto bytes = wire::encode_request(expected.maximum, expected.filter, expected.marker, expected.reshard);
    CHECK(bytes.contents_equal(rgw_bi_oracle::encode_request(expected)));
    const auto old = rgw_bi_oracle::decode_request(bytes);
    CHECK(old.maximum == expected.maximum);
    CHECK(old.filter == expected.filter);
    CHECK(old.marker == expected.marker);
    CHECK(old.reshard);

    for (const auto version : {1, 2, 3}) {
        const auto input = fragmented(rgw_bi_oracle::encode_request(expected, version));
        native::request value;
        auto cursor = input.cbegin();
        native::decode(value, cursor);
        CHECK(value.max == expected.maximum);
        CHECK(value.name_filter == expected.filter);
        CHECK(value.marker == expected.marker);
        CHECK(value.reshardlog == (version >= 2));
        CHECK(cursor.end());
    }
}

TEST_CASE("BI replies use independent vector row types and preserve native frames", "[rgw][cls][datum]")
{
    const auto rows = fixtures();
    native::reply page;
    page.is_truncated = true;
    for (const auto& row : rows) {
        page.entries.emplace_back(static_cast<::BIIndexType>(row.kind), row.key, row.payload);
    }
    byte_buffer bytes;
    native::encode(page, bytes);
    CHECK(bytes.contents_equal(rgw_bi_oracle::encode_reply(rows, true)));
    bool old_truncated = false;
    const auto old_rows = rgw_bi_oracle::decode_reply(bytes, old_truncated);
    REQUIRE(old_rows.size() == rows.size());
    CHECK(old_truncated);
    CHECK(old_rows[0].payload.contents_equal(rows[0].payload));

    auto cursor = bytes.cbegin();
    auto result = wire::decode_entries<std::vector<client_row>>(cursor, client_row_fields);
    check_rows(result.first, rows);
    CHECK(result.second);
    CHECK(cursor.end());
    CHECK(wire::encode_entries(result.first, result.second, client_row_fields).contents_equal(bytes));

    // Decode retains the original opaque buffers rather than flattening the reply.
    const auto source = rows[0].payload.cbegin().get_current_ptr();
    const auto decoded = result.first[0].contents.cbegin().get_current_ptr();
    CHECK(source.c_str() == decoded.c_str());
}

TEST_CASE("BI decoding handles fragmentation and compatible frame extensions", "[rgw][cls][datum]")
{
    const auto rows = fixtures();
    const auto input = fragmented(rgw_bi_oracle::extended_reply(rows));
    auto cursor = input.cbegin();
    const auto result = wire::decode_entries<std::vector<client_row>>(cursor, client_row_fields);
    check_rows(result.first, rows);
    CHECK(result.second);
    CHECK(cursor.end());
}

TEST_CASE("BI native bridge rejects every proper reply truncation", "[rgw][cls][datum]")
{
    const auto bytes = rgw_bi_oracle::encode_reply(fixtures(), true);
    for (unsigned size = 0; size < bytes.length(); ++size) {
        byte_buffer short_input;
        short_input.substr_of(bytes, 0, size);
        CHECK_THROWS(native::decode_reply<std::vector<::rgw_cls_bi_entry>>(short_input));
    }
}

TEST_CASE("BI decoder rejects impossible counts before constructing rows", "[rgw][cls][datum]")
{
    auto bytes = rgw_bi_oracle::encode_reply({}, false);
    const char excessive[] = {char(0xFF), char(0xFF), char(0xFF), char(0xFF)};
    auto position = bytes.begin(6);
    position.copy_in(4, excessive);
    auto cursor = bytes.cbegin();
    counted_row::constructions = 0;
    CHECK_THROWS(wire::decode_entries<std::vector<counted_row>>(cursor, client_row_fields));
    CHECK(counted_row::constructions == 0);
}

TEST_CASE("BI native bool decoding accepts every byte value", "[rgw][cls][datum]")
{
    for (unsigned raw = 0; raw < 256; ++raw) {
        auto bytes = rgw_bi_oracle::encode_reply({}, false);
        const char value = static_cast<char>(raw);
        auto position = bytes.begin(bytes.length() - 1);
        position.copy_in(1, &value);
        auto cursor = bytes.cbegin();
        const auto result = wire::decode_entries<std::vector<client_row>>(cursor, client_row_fields);
        CHECK(result.second == (0 != raw));
    }
}

TEST_CASE("BI encoding accepts byte vectors and borrowed row ranges", "[rgw][cls][datum]")
{
    struct row final
    {
        int kind;
        std::string_view key;
        std::vector<std::byte> data;
    };
    const auto expected = fixtures();
    std::vector<row> rows;
    for (const auto& entry : expected) {
        const auto text = entry.payload.to_str();
        const auto bytes = std::as_bytes(std::span {text});
        rows.push_back({entry.kind, entry.key, {bytes.begin(), bytes.end()}});
    }
    using selected = datum::binary_fields<&row::kind, &row::key, &row::data>;

    CHECK(wire::encode_entries(std::span {rows}, true, selected {})
        .contents_equal(rgw_bi_oracle::encode_reply(expected, true)));
    CHECK(wire::encode_entries(std::span {rows}, true, selected {})
        .contents_equal(rgw_bi_oracle::encode_put_request(expected, true)));
    rows[0].kind = -1;
    CHECK_THROWS_AS(wire::encode_entries(rows, false, selected {}), std::length_error);
    rows[0].kind = 256;
    CHECK_THROWS_AS(wire::encode_entries(rows, false, selected {}), std::length_error);
}

TEST_CASE("BI decoded payload owners outlive their input envelope", "[rgw][cls][datum]")
{
    std::vector<client_row> rows;
    {
        auto bytes = rgw_bi_oracle::encode_reply(fixtures(), false);
        auto cursor = bytes.cbegin();
        rows = wire::decode_entries<std::vector<client_row>>(cursor, client_row_fields).first;
    }
    check_rows(rows, fixtures());
}

TEST_CASE("BI bounded frames reject invalid lengths and compatibility", "[rgw][cls][datum]")
{
    const auto valid = rgw_bi_oracle::encode_reply(fixtures(), false);
    // Outer compatibility, nested compatibility, outer/nested payload length.
    for (const auto [offset, value] : {std::pair {1u, 2u}, {11u, 2u},
                                     {2u, 4u}, {12u, 1u}}) {
        byte_buffer bytes;
        bytes.append(valid.to_str());
        const char raw = static_cast<char>(value);
        bytes.begin(offset).copy_in(1, &raw);
        auto cursor = bytes.cbegin();
        CHECK_THROWS(wire::decode_entries<std::vector<client_row>>(cursor, client_row_fields));
    }
}

TEST_CASE("BI native bridge keeps entry and request cursors on failure", "[rgw][cls][datum]")
{
    const auto full = rgw_bi_oracle::encode_request({11, "filter", "marker", true});
    for (unsigned size = 0; size < full.length(); ++size) {
        byte_buffer input;
        input.append("prefix", 6);
        byte_buffer tail;
        tail.substr_of(full, 0, size);
        input.append(tail);
        auto cursor = input.cbegin(6);
        native::request value {1, "preserve", "cursor", false};
        CHECK_THROWS(native::decode(value, cursor));
        CHECK(cursor.get_off() == 6);
        CHECK(value.name_filter == "preserve");
        CHECK(value.marker == "cursor");
    }
    native::reply empty;
    byte_buffer bytes;
    native::encode(empty, bytes);
    const auto result = native::decode_reply<std::vector<::rgw_cls_bi_entry>>(bytes);
    CHECK(result.first.empty());
    CHECK_FALSE(result.second);
}

TEST_CASE("BI randomized native envelopes preserve every field", "[rgw][cls][datum]")
{
    std::mt19937 random {0xB1157};
    for (unsigned trial = 0; trial < 80; ++trial) {
        std::vector<rgw_bi_oracle::row> rows;
        const auto size = trial == 0 ? 1000 : random() % 40;
        for (unsigned index = 0; index < size; ++index) {
            std::string key(random() % 300, '\0');
            for (auto& byte : key) {
                byte = static_cast<char>(random());
            }
            byte_buffer contents;
            for (unsigned fragment = 0, total = random() % 8; fragment < total; ++fragment) {
                std::string piece(random() % 160, '\0');
                for (auto& byte : piece) {
                    byte = static_cast<char>(random());
                }
                contents.append(::ceph::buffer::ptr {::ceph::buffer::copy(piece.data(), piece.size())});
            }
            rows.push_back({static_cast<std::uint8_t>(random()), std::move(key), std::move(contents)});
        }
        const bool truncated = random() % 2;
        const auto bytes = rgw_bi_oracle::encode_reply(rows, truncated);
        auto cursor = bytes.cbegin();
        const auto result = wire::decode_entries<std::vector<client_row>>(cursor, client_row_fields);
        check_rows(result.first, rows);
        CHECK(result.second == truncated);
        CHECK(cursor.end());
        CHECK(wire::encode_entries(result.first, result.second, client_row_fields).contents_equal(bytes));
    }
}

TEST_CASE("BI native entry decode preserves its destination on truncation", "[rgw][cls][datum]")
{
    const auto rows = fixtures();
    const auto bytes = rgw_bi_oracle::encode_reply(rows, false);
    const unsigned start = 10; // reply frame header followed by row count
    const unsigned length = 15 + rows[0].key.size() + rows[0].payload.length();
    for (unsigned size = 0; size < length; ++size) {
        byte_buffer input;
        input.substr_of(bytes, 0, start + size);
        auto cursor = input.cbegin(start);
        native::entry value {::BIIndexType::Invalid, "preserve", payload()};
        CHECK_THROWS(native::decode(value, cursor));
        CHECK(cursor.get_off() == start);
        CHECK(value.type == ::BIIndexType::Invalid);
        CHECK(value.idx == "preserve");
        CHECK(value.data.contents_equal(payload()));
    }
}

TEST_CASE("RGW native nested association bytes need no new container codec", "[rgw][cls][datum]")
{
    struct module_groups final
    {
        rgw_bi_oracle::epoch_groups epochs;
    };
    struct client_groups final
    {
        std::vector<std::pair<std::uint64_t, std::vector<std::uint32_t>>> epochs;
    };
    const module_groups original {{{1, {2, 3}}, {0xFFEEDDCCBBAA9988, {4}}}};
    std::vector<std::byte> bytes;
    zpp::bits::out output {bytes, zpp::bits::size4b {}, zpp::bits::endian::little {}};
    datum::serialize_schema(output, original, datum::binary_fields<&module_groups::epochs> {}).or_throw();
    const auto expected = rgw_bi_oracle::encode_groups(original.epochs).to_str();
    CHECK(std::ranges::equal(bytes, std::as_bytes(std::span {expected})));
    client_groups client;
    zpp::bits::in input {bytes, zpp::bits::size4b {}, zpp::bits::endian::little {}};
    datum::serialize_schema(input, client, datum::binary_fields<&client_groups::epochs> {}).or_throw();
    REQUIRE(client.epochs.size() == 2);
    CHECK(client.epochs[0].first == 1);
    CHECK(client.epochs[0].second == std::vector<std::uint32_t> {2, 3});

    // Counted pairs and a map have the same valid encoding. Duplicate keys
    // need an explicit policy: native and zpp maps keep the first value,
    // whereas a vector preserves duplicates. Container capability alone does
    // not establish equivalent application semantics.
    client.epochs.push_back({1, {9}});
    bytes.clear();
    zpp::bits::out duplicate_output {bytes, zpp::bits::size4b {}, zpp::bits::endian::little {}};
    datum::serialize_schema(duplicate_output, client,
        datum::binary_fields<&client_groups::epochs> {}).or_throw();
    byte_buffer duplicate_bytes;
    duplicate_bytes.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
    CHECK(rgw_bi_oracle::decode_groups(duplicate_bytes).at(1) == std::vector<std::uint32_t> {2, 3});
    module_groups zpp_groups;
    zpp::bits::in duplicate_input {bytes, zpp::bits::size4b {}, zpp::bits::endian::little {}};
    datum::serialize_schema(duplicate_input, zpp_groups,
        datum::binary_fields<&module_groups::epochs> {}).or_throw();
    CHECK(zpp_groups.epochs.at(1) == std::vector<std::uint32_t> {2, 3});
}

TEST_CASE("BI batch bindings use the native request profile with independent module rows", "[rgw][cls][datum][bi-put]")
{
    const auto expected = fixtures();
    std::vector<::rgw_cls_bi_entry> entries;
    for (const auto& row : expected) {
        entries.push_back({static_cast<::BIIndexType>(row.kind), row.key, row.payload});
    }
    for (const bool check_existing : {false, true}) {
        const auto bytes = native::encode_put_request(entries, check_existing);
        CHECK(bytes.contents_equal(rgw_bi_oracle::encode_put_request(expected, check_existing)));
        bool old_check = !check_existing;
        const auto old_rows = rgw_bi_oracle::decode_put_request(bytes, old_check);
        CHECK(old_check == check_existing);
        REQUIRE(old_rows.size() == expected.size());
        CHECK(old_rows[0].payload.contents_equal(expected[0].payload));
        const auto input = fragmented(bytes);
        auto cursor = input.cbegin();
        native::put_request value;
        native::decode(value, cursor);
        REQUIRE(value.entries.size() == expected.size());
        CHECK(value.check_existing == check_existing);
        CHECK(cursor.end());
        for (std::size_t index = 0; index < expected.size(); ++index) {
            CHECK(static_cast<std::uint8_t>(value.entries[index].type) == expected[index].kind);
            CHECK(value.entries[index].idx == expected[index].key);
            CHECK(value.entries[index].data.contents_equal(expected[index].payload));
        }
    }
    const auto future = fragmented(rgw_bi_oracle::extended_reply(expected));
    auto cursor = future.cbegin();
    native::put_request value;
    native::decode(value, cursor);
    REQUIRE(value.entries.size() == expected.size());
    CHECK(value.check_existing);
    CHECK(cursor.end());
    bool old_check = false;
    CHECK(rgw_bi_oracle::decode_put_request(future, old_check).size() == expected.size());
    CHECK(old_check);
}

TEST_CASE("BI batch request decoding publishes atomically on structural failure", "[rgw][cls][datum][bi-put]")
{
    const auto bytes = rgw_bi_oracle::encode_put_request(fixtures(), false);
    for (unsigned size = 0; size < bytes.length(); ++size) {
        byte_buffer input;
        input.append("prefix", 6);
        byte_buffer tail;
        tail.substr_of(bytes, 0, size);
        input.append(tail);
        auto cursor = input.cbegin(6);
        native::put_request value {{{::BIIndexType::Invalid, "preserve", payload()}}, true};
        CHECK_THROWS(native::decode(value, cursor));
        CHECK(cursor.get_off() == 6);
        REQUIRE(value.entries.size() == 1);
        CHECK(value.entries[0].idx == "preserve");
        CHECK(value.entries[0].data.contents_equal(payload()));
        CHECK(value.check_existing);
    }
    auto impossible = rgw_bi_oracle::encode_put_request({}, false);
    const char excessive[] = {char(0xFF), char(0xFF), char(0xFF), char(0xFF)};
    impossible.begin(6).copy_in(4, excessive);
    auto cursor = impossible.cbegin();
    native::put_request value;
    CHECK_THROWS(native::decode(value, cursor));
    CHECK(cursor.get_off() == 0);
    CHECK(value.entries.empty());
}

TEST_CASE("BI batch messages and decoded payloads retain their owners", "[rgw][cls][datum][bi-put]")
{
    byte_buffer input;
    const char *backing = nullptr;
    {
        std::vector<::rgw_cls_bi_entry> entries {{::BIIndexType::Plain, "key", payload()}};
        backing = entries[0].data.cbegin().get_current_ptr().c_str();
        input = native::encode_put_request(entries, false);
    }
    native::put_request value;
    auto cursor = input.cbegin();
    native::decode(value, cursor);
    input.clear();
    REQUIRE(value.entries.size() == 1);
    CHECK(value.entries[0].data.cbegin().get_current_ptr().c_str() == backing);
    CHECK(value.entries[0].data.contents_equal(payload()));
    CHECK(value.entries[0].idx == "key");
}

TEST_CASE("BI single puts share entry framing across independent records", "[rgw][cls][datum][single-put]")
{
    for (const auto& row : fixtures()) {
        const ::rgw_cls_bi_entry source {static_cast<::BIIndexType>(row.kind), row.key, row.payload};
        const auto bytes = native::encode_put_entry(source);
        CHECK(bytes.contents_equal(rgw_bi_oracle::encode_put_entry(row)));
        const auto old = rgw_bi_oracle::decode_put_entry(bytes);
        CHECK(old.kind == row.kind);
        CHECK(old.key == row.key);
        CHECK(old.payload.contents_equal(row.payload));
        const client_row independent {row.payload, row.kind, row.key};
        CHECK(wire::encode_entry_message(independent, client_row_fields).contents_equal(bytes));
        const auto future = fragmented(rgw_bi_oracle::extended_put_entry(row));
        auto cursor = future.cbegin();
        native::entry_put_request value;
        native::decode(value, cursor);
        CHECK(cursor.end());
        CHECK(static_cast<std::uint8_t>(value.item.type) == row.kind);
        CHECK(value.item.idx == row.key);
        CHECK(value.item.data.contents_equal(row.payload));
        CHECK(rgw_bi_oracle::decode_put_entry(future).payload.contents_equal(row.payload));
    }
}

TEST_CASE("BI single put decoding is bounded and retains owned results", "[rgw][cls][datum][single-put]")
{
    const auto bytes = rgw_bi_oracle::encode_put_entry(fixtures().front());
    for (unsigned size = 0; size < bytes.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(bytes, 0, size);
        auto cursor = prefix.cbegin();
        native::entry_put_request value {{::BIIndexType::Invalid, "preserve", payload()}};
        CHECK_THROWS(native::decode(value, cursor));
        CHECK(cursor.get_off() == 0);
        CHECK(value.item.idx == "preserve");
        CHECK(value.item.data.contents_equal(payload()));
    }
    native::entry_put_request owned;
    const char *backing = nullptr;
    {
        ::rgw_cls_bi_entry row {::BIIndexType::Plain, "key", payload()};
        backing = row.data.cbegin().get_current_ptr().c_str();
        auto input = native::encode_put_entry(row);
        auto cursor = input.cbegin();
        native::decode(owned, cursor);
    }
    CHECK(owned.item.data.cbegin().get_current_ptr().c_str() == backing);
    CHECK(owned.item.data.contents_equal(payload()));
}

TEST_CASE("BI get shares key and entry profiles without a shared operation record", "[rgw][cls][datum][bi-get]")
{
    for (unsigned kind = 0; kind < 256; ++kind) {
        const rgw_bi_oracle::get_request expected {std::string {"a\0b", 3}, std::string(80, 'i'),
                                                 static_cast<std::uint8_t>(kind)};
        const auto bytes = wire::encode_get_request(expected.name, expected.instance, expected.kind);
        CHECK(bytes.contents_equal(rgw_bi_oracle::encode_get_request(expected)));
        const auto old = rgw_bi_oracle::decode_get_request(bytes);
        CHECK(old.name == expected.name);
        CHECK(old.instance == expected.instance);
        CHECK(old.kind == expected.kind);
        for (const auto version : {1, 2}) {
            const auto input = fragmented(rgw_bi_oracle::encode_get_request(expected, version));
            auto cursor = input.cbegin();
            native::get_request value;
            native::decode(value, cursor);
            CHECK(value.key.name == expected.name);
            CHECK(value.key.instance == expected.instance);
            CHECK(static_cast<std::uint8_t>(value.type) == expected.kind);
            CHECK(cursor.end());
        }
    }
    for (const auto& row : fixtures()) {
        const native::entry source {static_cast<::BIIndexType>(row.kind), row.key, row.payload};
        byte_buffer bytes;
        native::encode_get_reply(source, bytes);
        CHECK(bytes.contents_equal(rgw_bi_oracle::encode_get_reply(row)));
        const auto old = rgw_bi_oracle::decode_get_reply(bytes);
        CHECK(old.kind == row.kind);
        CHECK(old.key == row.key);
        CHECK(old.payload.contents_equal(row.payload));
        for (const auto& input : {fragmented(bytes), fragmented(rgw_bi_oracle::extended_put_entry(row))}) {
            const auto decoded = native::decode_get_reply(input);
            CHECK(static_cast<std::uint8_t>(decoded.type) == row.kind);
            CHECK(decoded.idx == row.key);
            CHECK(decoded.data.contents_equal(row.payload));
        }
    }
}

TEST_CASE("BI get bounds key fields and preserves request publication on failure", "[rgw][cls][datum][bi-get]")
{
    const auto full = rgw_bi_oracle::encode_get_request({"key", "instance", 3});
    std::vector<byte_buffer> invalid;
    for (unsigned size = 0; size < full.length(); ++size) {
        byte_buffer short_input;
        short_input.substr_of(full, 0, size);
        invalid.push_back(std::move(short_input));
    }
    for (const unsigned offset : {1, 7}) {
        auto bytes = fragmented(full);
        const char newer = 2;
        bytes.begin(offset).copy_in(1, &newer);
        invalid.push_back(std::move(bytes));
    }
    auto outside = fragmented(full);
    const auto length = static_cast<char>(full.length() - 7);
    outside.begin(2).copy_in(1, &length); // kind lies outside the outer frame
    invalid.push_back(std::move(outside));
    auto excessive = fragmented(full);
    const char count[] = {char(0xFF), char(0xFF), char(0xFF), char(0xFF)};
    excessive.begin(12).copy_in(4, count);
    invalid.push_back(std::move(excessive));
    for (const auto& bytes : invalid) {
        byte_buffer input;
        input.append("prefix", 6);
        input.append(bytes);
        auto cursor = input.cbegin(6);
        native::get_request value {::BIIndexType::Invalid, {"preserve", "instance"}};
        CHECK_THROWS(native::decode(value, cursor));
        CHECK(cursor.get_off() == 6);
        CHECK(value.type == ::BIIndexType::Invalid);
        CHECK(value.key.name == "preserve");
        CHECK(value.key.instance == "instance");
    }
    const auto bytes = rgw_bi_oracle::encode_get_reply(fixtures().front());
    for (unsigned size = 0; size < bytes.length(); ++size) {
        byte_buffer short_input;
        short_input.substr_of(bytes, 0, size);
        CHECK_THROWS(native::decode_get_reply(short_input));
    }
}
