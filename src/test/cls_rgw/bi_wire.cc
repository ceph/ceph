// RGW CLS: Native compatibility and independent Datum/zpp representations.
#include "rgw/datum/adapters/ceph/wire.h"
#include "test/cls_rgw/wire_test.h"
#include "bi_native_oracle.h"
#include "rgw/datum/adapters/cls/bi.h"
#include "rgw/datum/adapters/cls/bi_wire.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include <array>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <random>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace rgw_metadata_test {

struct converted final
{
    std::uint64_t local_value;
    std::uint64_t unused;
};

struct rejected final {};

} // rgw_metadata_test

namespace ceph::salad::datum {

template <>
struct binary_field_codec<::rgw_metadata_test::converted> final
{
    static constexpr auto apply(auto& archive, const auto& value)
    {
        return archive(static_cast<std::uint16_t>(value.local_value));
    }

};

template <>
struct binary_field_codec<::rgw_metadata_test::rejected> final
{
    static constexpr zpp::bits::errc apply(auto&, const auto&)
    {
        return std::errc::invalid_argument;
    }

};

} // ceph::salad::datum

namespace {

namespace wire = ::cls::rgw::bi_wire;
namespace native = ::cls::rgw::bi_datum;
namespace datum = ::ceph::salad::datum;
using byte_buffer = ::ceph::buffer::list;

using ::rgw_cls_test::fragmented;

byte_buffer payload()
{
    byte_buffer bytes;
    bytes.append("abc", 3);
    auto second = ::ceph::buffer::copy("\0\xFF!", 3);
    bytes.append(::ceph::buffer::ptr {std::move(second)});

    return bytes;
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

struct client_request final
{
    std::string marker;
    std::string prefix;
    std::uint64_t maximum = 0;
    bool reshard = true;
};

constexpr auto client_request_fields = datum::binary_fields<&client_request::maximum, &client_request::prefix,
    &client_request::marker, &client_request::reshard> {};

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

        client_request client;
        cursor = input.cbegin();
        wire::decode_request(client_request_fields, cursor, client);
        CHECK(client.maximum == expected.maximum);
        CHECK(client.prefix == expected.filter);
        CHECK(client.marker == expected.marker);
        CHECK(client.reshard == (version >= 2));
        CHECK(cursor.end());
    }
}

TEST_CASE("RGW field archives preserve successful bytes and report undersized schemas", "[rgw][cls][datum]")
{
    using record = ::ceph::datum_wire::scalar_record<std::uint32_t>;

    const record original {0x01020304};
    const auto fields = datum::binary_fields<&record::value> {};
    byte_buffer bytes;
    ::ceph::datum_wire::writer output {bytes};
    CHECK_NOTHROW(output.fields(fields, original, 4));
    byte_buffer expected;
    ::ceph::encode(original.value, expected);
    CHECK(bytes.contents_equal(expected));

    auto cursor = bytes.cbegin();
    ::ceph::datum_wire::reader input {cursor};
    record decoded {0};
    CHECK_NOTHROW(input.fields<4>(fields, decoded));
    CHECK(decoded.value == original.value);
    CHECK(cursor.end());

    byte_buffer short_bytes;
    ::ceph::datum_wire::writer short_output {short_bytes};
    CHECK_THROWS_AS(short_output.fields(fields, original, 3), std::system_error);
    byte_buffer truncated;
    truncated.substr_of(bytes, 0, 3);
    cursor = truncated.cbegin();
    ::ceph::datum_wire::reader short_input {cursor};
    CHECK_THROWS_AS(short_input.fields<3>(fields, decoded), std::system_error);
}

TEST_CASE("RGW frame callbacks preserve metadata, reference returns, and extension bounds", "[rgw][cls][datum]")
{
    namespace shared = ::ceph::datum_wire;

    byte_buffer encoded;
    shared::writer output {encoded};
    output.frame(7, 2, [&] {
        output.scalar(std::uint32_t {0x01020304});
        output.scalar(std::uint16_t {0xFFFF});
    });
    const auto input = fragmented(encoded);
    auto cursor = input.cbegin();
    shared::reader archive {cursor};
    std::uint32_t value = 0;

    SECTION("The version-only callback returns the caller's reference") {
        auto& result = archive.frame(2, [&](auto& fields, const std::uint8_t version) -> std::uint32_t& {
            CHECK(7 == version);
            value = fields.template scalar<std::uint32_t>();

            return value;
        });
        CHECK(&result == &value);
    }

    SECTION("The compatibility-aware callback wins when both arities are supported") {
        std::uint8_t compatibility = 0;
        auto& result = archive.frame(2,
            [&](auto& fields, const std::uint8_t version, const auto ...compatible) -> std::uint32_t& {
                CHECK(7 == version);
                CHECK(1 == sizeof...(compatible));
                ((compatibility = compatible), ...);
                value = fields.template scalar<std::uint32_t>();

                return value;
            });
        CHECK(2 == compatibility);
        CHECK(&result == &value);
    }

    SECTION("The version-only callback needs no result") {
        archive.frame(2, [&](auto& fields, const std::uint8_t version) {
            CHECK(7 == version);
            value = fields.template scalar<std::uint32_t>();
        });
    }

    SECTION("The compatibility-aware callback needs no result") {
        archive.frame(2, [&](auto& fields, const std::uint8_t version, const std::uint8_t compatible) {
            CHECK(7 == version);
            CHECK(2 == compatible);
            value = fields.template scalar<std::uint32_t>();
        });
    }

    SECTION("A throwing callback leaves the unread extension in place") {
        CHECK_THROWS_AS(archive.frame(2, [](auto& fields, std::uint8_t) {
            fields.template scalar<std::uint32_t>();
            throw std::runtime_error {"late decoding failure"};
        }), std::runtime_error);
        CHECK(2 == cursor.get_remaining());
        return;
    }

    CHECK(0x01020304 == value);
    CHECK(cursor.end());
}

TEST_CASE("RGW fixed fields and frame bounds need only remaining, fragments, and skip", "[rgw][cls][datum]")
{
    struct minimal_cursor final
    {
        std::span<const char> bytes;
        bool empty_fragment = true;

        unsigned get_remaining() const { return bytes.size(); }

        std::size_t get_ptr_and_advance(const std::size_t wanted, const char **output)
        {
            const auto size = std::min({wanted, bytes.size(),
                                       std::size_t {empty_fragment ? 0u : 1u}});
            empty_fragment = !empty_fragment;
            *output = bytes.data();
            bytes = bytes.subspan(size);

            return size;
        }

        minimal_cursor& operator+=(const unsigned size)
        {
            bytes = bytes.subspan(size);

            return *this;
        }

    };

    byte_buffer encoded;
    ::ceph::datum_wire::writer output {encoded};
    output.frame(1, [&] {
        output.scalar(std::uint64_t {0x0102030405060708});
        output.scalar(std::uint16_t {0xFFFF});
    });
    auto stored = encoded.to_str() + "suffix";

    SECTION("Split scalars and skipped extensions preserve the following message") {
        minimal_cursor cursor {std::span<const char> {stored}};
        ::ceph::datum_wire::basic_reader archive {cursor};
        const auto value = archive.frame(1, [](auto& fields, std::uint8_t) {
            return fields.template scalar<std::uint64_t>();
        });
        CHECK(0x0102030405060708 == value);
        CHECK(std::string_view {cursor.bytes.data(), cursor.bytes.size()} == "suffix");
    }

    SECTION("Every proper frame truncation is rejected") {
        for (unsigned size = 0; size < encoded.length(); ++size) {
            minimal_cursor cursor {std::span<const char> {stored.data(), size}};
            ::ceph::datum_wire::basic_reader archive {cursor};
            CHECK_THROWS_AS(archive.frame(1, [](auto& fields, std::uint8_t) {
                fields.template scalar<std::uint64_t>();
            }), ::ceph::buffer::error);
        }
    }

    SECTION("A field cannot consume bytes beyond its declared frame") {
        stored[2] = 4; // The little-endian body length now admits only half the scalar.
        minimal_cursor cursor {std::span<const char> {stored}};
        ::ceph::datum_wire::basic_reader archive {cursor};
        CHECK_THROWS_AS(archive.frame(1, [](auto& fields, std::uint8_t) {
            fields.template scalar<std::uint64_t>();
        }), ::ceph::buffer::malformed_input);
        CHECK(stored.size() - 6 == cursor.get_remaining());
    }
}

TEST_CASE("RGW owning byte reads replace destinations and preserve payload owners", "[rgw][cls][datum]")
{
    for (const auto& expected : {std::string {}, std::string {"a\0b", 3}}) {
        byte_buffer encoded;
        ::ceph::encode(expected, encoded);
        auto input = fragmented(encoded);
        const auto *backing = input.buffers().back().c_str();

        auto cursor = input.cbegin();
        ::ceph::datum_wire::reader text_archive {cursor};
        std::string text = "old text";
        text_archive.text(text);
        CHECK(cursor.end());

        cursor = input.cbegin();
        ::ceph::datum_wire::reader payload_archive {cursor};
        auto bytes = payload();
        payload_archive.payload(bytes);
        CHECK(cursor.end());
        input.clear();

        CHECK(text == expected);
        CHECK(bytes.to_str() == expected);
        if (!expected.empty()) {
            REQUIRE(!bytes.buffers().empty());
            CHECK(bytes.buffers().back().c_str() == backing);
        }
    }
}

TEST_CASE("RGW scalar metadata preserves native bytes across append boundaries", "[rgw][cls][datum]")
{
    byte_buffer expected;
    ::ceph::encode(std::uint8_t {0xFF}, expected);
    ::ceph::encode(std::uint16_t {0xABCD}, expected);
    ::ceph::encode(std::uint32_t {0xABCDEF12}, expected);
    ::ceph::encode(std::uint64_t {0xABCDEF1234567890}, expected);
    for (const auto tail : {1u, 2u, 3u, 4u, 7u, 8u}) {
        byte_buffer bytes;
        {
            auto storage = bytes.get_contiguous_appender(tail);
        }
        ::ceph::datum_wire::writer output {bytes};
        output.scalar(std::uint8_t {0xFF});
        output.scalar(std::uint16_t {0xABCD});
        output.scalar(std::uint32_t {0xABCDEF12});
        output.scalar(std::uint64_t {0xABCDEF1234567890});
        CHECK(bytes.contents_equal(expected));
    }
}

TEST_CASE("RGW metadata spans preserve bytes across fresh and shared append storage", "[rgw][cls][datum]")
{
    struct record final
    {
        std::uint32_t generation;
        std::string_view name;
        std::uint8_t flags;
    };
    const auto fields = datum::binary_fields<&record::generation, &record::name, &record::flags> {};
    for (const auto size : {0u, 1u, 4095u, 4096u, 8193u, 65536u}) {
        for (const bool framed : {false, true}) {
            for (const bool append : {false, true}) {
                std::string name(size, 'n');
                if (!name.empty()) {
                    name[size / 2] = '\0';
                }
                const record value {0x01020304, name, 7};
                byte_buffer bytes;
                byte_buffer expected;
                byte_buffer shared;
                shared.append("opaque", 6);
                if (append) {
                    bytes.append("prefix", 6);
                    expected.append("prefix", 6);
                    bytes.append(shared);
                    expected.append(shared);
                }
                byte_buffer body;
                ::ceph::encode(value.generation, body);
                ::ceph::encode(name, body);
                ::ceph::encode(value.flags, body);
                if (framed) {
                    ::ceph::encode(std::uint8_t {3}, expected);
                    ::ceph::encode(std::uint8_t {1}, expected);
                    ::ceph::encode(static_cast<std::uint32_t>(body.length()), expected);
                }
                expected.append(body);
                ::ceph::datum_wire::writer output {bytes};
                if (framed) {
                    output.frame(fields, 3, value);
                }
                if (!framed) {
                    output.fields(fields, value);
                }
                name.assign("changed");
                CHECK(bytes.contents_equal(expected));
                CHECK(shared.to_str() == "opaque");
            }
        }
    }
}

TEST_CASE("RGW metadata sizes follow selected order and field conversions", "[rgw][cls][datum]")
{
    namespace metadata = ::ceph::datum_wire;
    struct fixed final
    {
        std::uint64_t epoch;
        std::uint8_t operation;
    };
    static_assert(9 == metadata::encoded_size(datum::binary_fields<&fixed::operation, &fixed::epoch> {}, fixed {}));
    static_assert(6 == metadata::encoded_size(metadata::header_fields {}, metadata::frame_header {}));
    static_assert(2 == metadata::encoded_size(
        datum::binary_fields<&metadata::scalar_record<::rgw_metadata_test::converted>::value> {},
        metadata::scalar_record<::rgw_metadata_test::converted> {}));

    struct record final
    {
        std::array<std::uint8_t, 5> unselected;
        std::string text;
        std::string_view tail;
        std::uint8_t flag;
        ::rgw_metadata_test::converted narrow;
    };
    const auto fields = datum::binary_fields<&record::narrow, &record::tail, &record::text,
        &record::tail, &record::flag> {};
    for (const auto& text : {std::string {"a\0b", 3}, std::string {}}) {
        const record value {{1, 2, 3, 4, 5}, text, "tail", 7, {0x1234, 0xFFFFFFFF}};
        byte_buffer expected;
        ::ceph::encode(std::uint16_t {0x1234}, expected);
        ::ceph::encode(std::string {value.tail}, expected);
        ::ceph::encode(value.text, expected);
        ::ceph::encode(std::string {value.tail}, expected);
        ::ceph::encode(value.flag, expected);

        CHECK(metadata::encoded_size(fields, value) == expected.length());
        byte_buffer bytes;
        metadata::writer output {bytes};
        output.fields(fields, value);
        CHECK(bytes.contents_equal(expected));
    }
}

TEST_CASE("RGW automatic metadata sizing preserves explicit codecs and rejects errors", "[rgw][cls][datum]")
{
    namespace metadata = ::ceph::datum_wire;
    using explicit_record = metadata::scalar_record<std::array<std::uint8_t, 3>>;

    const explicit_record value {{1, 2, 3}};
    byte_buffer bytes;
    metadata::writer output {bytes};
    output.fields(datum::binary_fields<&explicit_record::value> {}, value, 3);
    CHECK(bytes.to_str() == std::string {"\1\2\3", 3});

    using rejected_record = metadata::scalar_record<::rgw_metadata_test::rejected>;

    CHECK_THROWS_AS(output.fields(datum::binary_fields<&rejected_record::value> {}, rejected_record {}), std::system_error);
    CHECK(bytes.to_str() == std::string {"\1\2\3", 3});
    constexpr auto maximum = std::numeric_limits<std::uint32_t>::max();
    static_assert(maximum == metadata::checked_count(maximum));
    CHECK_THROWS_AS(metadata::checked_count(std::uint64_t {maximum} + 1), std::length_error);
    CHECK_THROWS_AS(output.fields(datum::binary_fields<&explicit_record::value> {},
                                  value, std::uint64_t {maximum} + 1), std::length_error);
    CHECK(bytes.to_str() == std::string {"\1\2\3", 3});
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
    auto result = wire::decode_entries<std::vector<client_row>>(client_row_fields, cursor);
    check_rows(result.first, rows);
    CHECK(result.second);
    CHECK(cursor.end());
    CHECK(wire::encode_entries(client_row_fields, result.first, result.second).contents_equal(bytes));

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
    const auto result = wire::decode_entries<std::vector<client_row>>(client_row_fields, cursor);
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
    auto position = bytes.begin(::ceph::encoding_detail::struct_header_len());
    position.copy_in(4, excessive);
    auto cursor = bytes.cbegin();
    counted_row::constructions = 0;
    CHECK_THROWS(wire::decode_entries<std::vector<counted_row>>(client_row_fields, cursor));
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
        const auto result = wire::decode_entries<std::vector<client_row>>(client_row_fields, cursor);
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

    CHECK(wire::encode_entries(selected {}, std::span {rows}, true)
        .contents_equal(rgw_bi_oracle::encode_reply(expected, true)));
    CHECK(wire::encode_entries(selected {}, std::span {rows}, true)
        .contents_equal(rgw_bi_oracle::encode_put_request(expected, true)));
    rows[0].kind = -1;
    CHECK_THROWS_AS(wire::encode_entries(selected {}, rows, false), std::length_error);
    rows[0].kind = 256;
    CHECK_THROWS_AS(wire::encode_entries(selected {}, rows, false), std::length_error);
}

TEST_CASE("BI decoded payload owners outlive their input envelope", "[rgw][cls][datum]")
{
    std::vector<client_row> rows;
    {
        auto bytes = rgw_bi_oracle::encode_reply(fixtures(), false);
        auto cursor = bytes.cbegin();
        rows = wire::decode_entries<std::vector<client_row>>(client_row_fields, cursor).first;
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
        CHECK_THROWS(wire::decode_entries<std::vector<client_row>>(client_row_fields, cursor));
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
        CHECK(value.max == 1);
        CHECK(value.name_filter == "preserve");
        CHECK(value.marker == "cursor");
        CHECK_FALSE(value.reshardlog);
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
        const auto result = wire::decode_entries<std::vector<client_row>>(client_row_fields, cursor);
        check_rows(result.first, rows);
        CHECK(result.second == truncated);
        CHECK(cursor.end());
        CHECK(wire::encode_entries(client_row_fields, result.first, result.second).contents_equal(bytes));
    }
}

TEST_CASE("RGW request publication accepts assign-only records and move-only decoders", "[rgw][cls][datum]")
{
    struct publication_record final
    {
        std::uint32_t number = 0;

    public:
        publication_record() = default;
        publication_record(const publication_record&) = delete;
        publication_record(publication_record&&) = delete;

        publication_record& operator=(publication_record&&) noexcept = default;

    };
    static_assert(!std::is_move_constructible_v<publication_record>);
    static_assert(std::is_nothrow_move_assignable_v<publication_record>);

    byte_buffer input;
    input.append("prefix", 6);
    ENCODE_START(1, 1, input);
    ::ceph::encode(std::uint32_t {42}, input);
    ENCODE_FINISH(input);
    for (const bool fail : {false, true}) {
        auto cursor = input.cbegin(6);
        publication_record value;
        value.number = 999;
        auto decode = [adjustment = std::make_unique<std::uint32_t>(1), fail](
                          auto& fields, auto& candidate, std::uint8_t) {
            candidate.number = fields.template scalar<std::uint32_t>() + *adjustment;
            if (fail) {
                throw std::runtime_error {"late decoding failure"};
            }
        };
        if (fail) {
            CHECK_THROWS_AS(::ceph::datum_wire::decode_record<1>(value, cursor, std::move(decode)), std::runtime_error);
            CHECK(999 == value.number);
            CHECK(6 == cursor.get_off());
            continue;
        }
        CHECK_NOTHROW(::ceph::datum_wire::decode_record<1>(value, cursor, std::move(decode)));
        CHECK(43 == value.number);
        CHECK(cursor.end());
    }
}

TEST_CASE("RGW publication borrows immobile mutable decoders", "[rgw][cls][datum]")
{
    namespace shared = ::ceph::datum_wire;

    struct record final { std::uint32_t number = 99; };
    struct decoder final
    {
        unsigned calls = 0;

        decoder() = default;
        decoder(const decoder&) = delete;
        decoder(decoder&&) = delete;

        void operator()(shared::reader& fields, record& value, std::uint8_t)
        {
            ++calls;
            value.number = fields.scalar<std::uint32_t>();
        }

        void operator()(byte_buffer::const_iterator& cursor, record& value)
        {
            shared::reader fields {cursor};
            (*this)(fields, value, 0);
        }

    };

    byte_buffer input;
    shared::writer output {input};
    record value;
    decoder decode;

    SECTION("The unframed decoder is borrowed") {
        output.scalar(std::uint32_t {42});
        auto cursor = input.cbegin();
        shared::decode_record(value, cursor, decode);
        CHECK(cursor.end());
    }

    SECTION("The framed decoder is borrowed") {
        input.append("prefix", 6);
        output.frame(1, [&] { output.scalar(std::uint32_t {42}); });
        input.append("suffix", 6);
        auto cursor = input.cbegin(6);
        shared::decode_record<1>(value, cursor, decode);
        CHECK(6 == cursor.get_remaining());
    }

    CHECK(42 == value.number);
    CHECK(1 == decode.calls);
}

TEST_CASE("BI native entry decode preserves its destination on truncation", "[rgw][cls][datum]")
{
    const auto rows = fixtures();
    const auto bytes = rgw_bi_oracle::encode_reply(rows, false);
    // Entries follow the reply header and row count.
    constexpr auto start = ::ceph::encoding_detail::struct_header_len() + sizeof(std::uint32_t);
    // Entry metadata is its frame, kind byte, key length, and payload length.
    const auto length = ::ceph::encoding_detail::struct_header_len() + sizeof(std::uint8_t) +
        2 * sizeof(std::uint32_t) + rows[0].key.size() + rows[0].payload.length();
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
    impossible.begin(::ceph::encoding_detail::struct_header_len())
        .copy_in(sizeof(excessive), excessive);
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
        CHECK(wire::encode_entry_message(client_row_fields, independent).contents_equal(bytes));
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
        native::encode(source, bytes);
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
    constexpr auto key_compat_offset =
        ::ceph::encoding_detail::struct_header_len() + sizeof(std::uint8_t);
    for (const auto offset : {sizeof(std::uint8_t), key_compat_offset}) {
        auto bytes = fragmented(full);
        const char newer = 2;
        bytes.begin(offset).copy_in(1, &newer);
        invalid.push_back(std::move(bytes));
    }
    auto outside = fragmented(full);
    const auto length = static_cast<char>(
        full.length() - ::ceph::encoding_detail::struct_header_len() - sizeof(std::uint8_t));
    outside.begin(2).copy_in(1, &length); // kind lies outside the outer frame
    invalid.push_back(std::move(outside));
    auto excessive = fragmented(full);
    const char count[] = {char(0xFF), char(0xFF), char(0xFF), char(0xFF)};
    // The name length follows the request frame header and nested key frame header.
    excessive.begin(2 * ::ceph::encoding_detail::struct_header_len()).copy_in(sizeof(count), count);
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

TEST_CASE("BI compact negotiation keeps old peers usable without another operation", "[rgw][cls][datum][compact]")
{
    const rgw_bi_oracle::request expected {19, std::string {"f\0x", 3}, "marker", true};
    const auto bytes = native::encode_request(native::reply_format::compact,
        expected.maximum, expected.filter, expected.marker, expected.reshard);
    const auto old = rgw_bi_oracle::decode_request(bytes);
    CHECK(old.maximum == expected.maximum);
    CHECK(old.filter == expected.filter);
    CHECK(old.marker == expected.marker);
    CHECK(old.reshard == expected.reshard);
    auto canonical = rgw_bi_oracle::encode_request(expected);
    const char version = 3;
    canonical.begin().copy_in(1, &version);
    CHECK(bytes.contents_equal(canonical));

    for (const auto format : {native::reply_format::framed, native::reply_format::compact}) {
        const auto input = native::encode_request(format,
            expected.maximum, expected.filter, expected.marker, expected.reshard);
        auto cursor = input.cbegin();
        native::request decoded;
        native::decode(decoded, cursor);
        CHECK(decoded.format == format);
        CHECK(cursor.end());
    }
    for (unsigned size = 0; size < bytes.length(); ++size) {
        byte_buffer short_input;
        short_input.substr_of(bytes, 0, size);
        auto cursor = short_input.cbegin();
        native::request preserved {17, "preserve", "cursor", false};
        CHECK_THROWS(native::decode(preserved, cursor));
        CHECK(preserved.max == 17);
        CHECK(preserved.format == native::reply_format::framed);
        CHECK(cursor.get_off() == 0);
    }

    const auto old_reply = rgw_bi_oracle::extended_reply(fixtures());
    const auto [rows, truncated] = native::decode_reply<std::vector<::rgw_cls_bi_entry>>(old_reply);
    REQUIRE(rows.size() == fixtures().size());
    CHECK(rows.front().idx == fixtures().front().key);
    CHECK(truncated);
}

TEST_CASE("BI compact replies match ordinary zpp container bytes and retain opaque owners", "[rgw][cls][datum][compact]")
{
    struct flat_row final
    {
        std::uint8_t kind;
        std::string_view key;
        std::string_view data;
    };
    for (const auto count : {0u, 1u, 2u, 1000u}) {
        native::reply page;
        page.is_truncated = true;
        const auto fixture = fixtures().front();
        for (unsigned index = 0; index < count; ++index) {
            page.entries.emplace_back(static_cast<::BIIndexType>(index % 256),
                fixture.key + std::to_string(index), fixture.payload);
        }
        byte_buffer framed;
        native::encode(page, framed);
        byte_buffer compact;
        native::encode(native::reply_format::compact, page, compact);
        CHECK(compact.length() + 6 * count == framed.length());
        if (0 == count) {
            CHECK(compact.contents_equal(framed));
        }
        if (0 < count) {
            std::vector<std::string> payloads;
            std::vector<flat_row> flat;
            payloads.reserve(count);
            flat.reserve(count);
            for (const auto& entry : page.entries) {
                payloads.push_back(entry.data.to_str());
                flat.emplace_back(static_cast<std::uint8_t>(entry.type), entry.idx, payloads.back());
            }
            std::vector<char> body;
            zpp::bits::out out {body, zpp::bits::size4b {}, zpp::bits::endian::little {}};
            out(flat, page.is_truncated).or_throw();
            byte_buffer expected;
            ENCODE_START(2, 2, expected);
            expected.append(body.data(), body.size());
            ENCODE_FINISH(expected);
            CHECK(compact.contents_equal(expected));
            bool old_truncated = false;
            CHECK_THROWS(rgw_bi_oracle::decode_reply(compact, old_truncated));
        }
        for (const auto& input : {compact, fragmented(compact)}) {
            const auto [vector_rows, vector_truncated] = native::decode_reply<std::vector<::rgw_cls_bi_entry>>(input);
            const auto [list_rows, list_truncated] = native::decode_reply<std::list<::rgw_cls_bi_entry>>(input);
            auto cursor = input.cbegin();
            const auto independent = wire::decode_entries<std::vector<client_row>, 2>(client_row_fields, cursor);
            REQUIRE(vector_rows.size() == count);
            REQUIRE(list_rows.size() == count);
            REQUIRE(independent.first.size() == count);
            CHECK(independent.second);
            CHECK(cursor.end());
            CHECK(vector_truncated);
            CHECK(list_truncated);
            unsigned index = 0;
            for (const auto& item : list_rows) {
                CHECK(item.type == page.entries[index].type);
                CHECK(item.idx == page.entries[index].idx);
                CHECK(item.data.contents_equal(page.entries[index].data));
                CHECK(vector_rows[index].idx == item.idx);
                CHECK(independent.first[index].tag == static_cast<std::uint8_t>(item.type));
                CHECK(independent.first[index].name == item.idx);
                CHECK(independent.first[index].contents.contents_equal(item.data));
                ++index;
            }
        }
        if (0 < count) {
            const auto source = page.entries.front().data.cbegin().get_current_ptr();
            auto [rows, truncated] = native::decode_reply<std::vector<::rgw_cls_bi_entry>>(compact);
            const auto decoded = rows.front().data.cbegin().get_current_ptr();
            CHECK(source.c_str() == decoded.c_str());
            compact.clear();
            page.entries.clear();
            CHECK(rows.front().data.contents_equal(fixture.payload));
        }
    }
}

TEST_CASE("BI compact replies reject truncations and impossible bounds before allocation", "[rgw][cls][datum][compact]")
{
    const auto rows = fixtures();
    native::reply page;
    for (const auto& row : rows) {
        page.entries.emplace_back(static_cast<::BIIndexType>(row.kind), row.key, row.payload);
    }
    byte_buffer bytes;
    native::encode(native::reply_format::compact, page, bytes);
    for (unsigned size = 0; size < bytes.length(); ++size) {
        byte_buffer shortened;
        shortened.substr_of(bytes, 0, size);
        CHECK_THROWS(native::decode_reply<std::vector<::rgw_cls_bi_entry>>(fragmented(shortened)));
    }
    for (const unsigned offset : {6u, 11u, 18u}) {
        auto invalid = fragmented(bytes);
        const char maximum[] = {char(0xFF), char(0xFF), char(0xFF), char(0xFF)};
        invalid.begin(offset).copy_in(4, maximum);
        auto cursor = invalid.cbegin();
        counted_row::constructions = 0;
        CHECK_THROWS((wire::decode_entries<std::vector<counted_row>, 2>(client_row_fields, cursor)));
        if (6 == offset) {
            CHECK(counted_row::constructions == 0);
        }
    }
    for (const auto [offset, value] : {std::pair {1u, char(3)}, std::pair {0u, char(1)}}) {
        auto invalid = fragmented(bytes);
        invalid.begin(offset).copy_in(1, &value);
        CHECK_THROWS(native::decode_reply<std::vector<::rgw_cls_bi_entry>>(invalid));
    }
    for (unsigned raw = 0; raw < 256; ++raw) {
        auto input = fragmented(bytes);
        const char flag = static_cast<char>(raw);
        input.begin(input.length() - 1).copy_in(1, &flag);
        const auto result = native::decode_reply<std::vector<::rgw_cls_bi_entry>>(input);
        CHECK(result.second == (0 != raw));
    }
}

TEST_CASE("BI compact payload owners survive every source record and envelope", "[rgw][cls][datum][compact]")
{
    auto [rows, address] = [] {
        native::reply page;
        page.entries.push_back({::BIIndexType::Plain, "owned", payload()});
        const auto address = reinterpret_cast<std::uintptr_t>(page.entries.front().data.cbegin().get_current_ptr().c_str());
        byte_buffer bytes;
        native::encode(native::reply_format::compact, page, bytes);
        auto result = native::decode_reply<std::vector<::rgw_cls_bi_entry>>(bytes);

        return std::pair {std::move(result.first), address};
    }();
    REQUIRE(rows.size() == 1);
    CHECK(rows.front().idx == "owned");
    CHECK(rows.front().data.to_str() == std::string {"abc\0\xFF!", 6});
    CHECK(reinterpret_cast<std::uintptr_t>(rows.front().data.cbegin().get_current_ptr().c_str()) == address);
}
