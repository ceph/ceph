// RGW CLS: Independent sequence/associative usage bindings and native codecs.
#include "test/cls_rgw/wire_test.h"
#include "rgw/datum/adapters/cls/usage.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <type_traits>

namespace {

namespace binding = ::cls::rgw::usage_datum;
using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;
using ::rgw_cls_test::fragmented;
using flat_entries = ::boost::container::flat_map<::rgw_user_bucket, ::rgw_usage_log_entry>;


template <typename T>
concept usage_reply_binding = requires { typename binding::read_reply<T>; };
static_assert(usage_reply_binding<binding::native_entries>);
static_assert(usage_reply_binding<flat_entries>);
static_assert(!usage_reply_binding<std::multimap<::rgw_user_bucket, ::rgw_usage_log_entry>>);

bytes frame(const std::uint8_t version, const auto& body, const std::uint8_t compatible = 0)
{
    bytes output;
    ENCODE_START(version, compatible, output);
    body(output);
    if (version > 4) { ::ceph::encode(std::uint64_t {37}, output); }
    ENCODE_FINISH(output);

    return output;
}

auto native(const binding::add_request& value)
{
    ::rgw_cls_usage_log_add_op result;
    result.info.entries = value.info.entries;

    return result;
}

auto native(const binding::read_request& value)
{
    ::rgw_cls_usage_log_read_op result;
    result.start_epoch = value.start_epoch;
    result.end_epoch = value.end_epoch;
    result.owner = value.owner;
    result.bucket = value.bucket;
    result.iter = value.iter;
    result.max_entries = value.max_entries;

    return result;
}

auto native(const binding::trim_request& value)
{
    ::rgw_cls_usage_log_trim_op result;
    result.start_epoch = value.start_epoch;
    result.end_epoch = value.end_epoch;
    result.user = value.user;
    result.bucket = value.bucket;

    return result;
}

auto native(const auto& value)
requires requires { value.usage; }
{
    ::rgw_cls_usage_log_read_ret result;
    for (const auto& [key, entry] : value.usage) { result.usage.emplace(key, entry); }
    result.truncated = value.truncated;
    result.next_iter = value.next_iter;

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
    if constexpr (std::is_same_v<RecordT, binding::add_request>) { expected.user.clear(); }
    cursor = input.cbegin();
    binding::decode(actual, cursor);
    CHECK(cursor.get_off() == consumed);
    const auto preserved = canonical(native(actual));
    CHECK(preserved.contents_equal(canonical(expected)));
    for (unsigned size = 0; size < input.length(); ++size) {
        bytes prefix;
        prefix.substr_of(input, 0, size);
        cursor = prefix.cbegin();
        CHECK_THROWS_AS(binding::decode(actual, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
        CHECK(canonical(native(actual)).contents_equal(preserved));
    }
    bytes compound;
    ::ceph::encode(std::uint16_t {11}, compound);
    compound.append(input);
    ::ceph::encode(std::uint64_t {13}, compound);
    cursor = compound.cbegin(2);
    binding::decode(actual, cursor);
    CHECK(cursor.get_off() == consumed + 2);
}

bytes entry(const std::uint8_t version, const std::uint64_t sent)
{
    return frame(version, [&](bytes& output) {
        ::ceph::encode(std::string {"tenant$ns$id"}, output);
        ::ceph::encode(std::string {"b\0t", 3}, output);
        ::ceph::encode(std::uint64_t {123}, output);
        ::ceph::encode(sent, output);
        for (const std::uint64_t count : {2, 3, 4}) { ::ceph::encode(count, output); }
        if (version >= 2) { ::ceph::encode(std::map<std::string, ::rgw_usage_data> {}, output); }
        if (version >= 3) { ::ceph::encode(std::string {"payer"}, output); }
        if (version >= 4) { ::encode(::rgw_s3select_usage_data {}, output); }
    });
}

bytes reply(const std::uint8_t version, const std::uint8_t leaf, const bool bad_duplicate = false)
{
    return frame(version, [&](bytes& output) {
        ::ceph::encode(std::uint32_t {3}, output);
        for (const auto& [key, sent] : std::vector<std::pair<std::string, std::uint64_t>> {{"z", 11}, {"a", 13}, {"z", 99}}) {
            output.append(frame(leaf, [&](bytes& fields) {
                ::ceph::encode(key, fields);
                ::ceph::encode(std::string {"bucket"}, fields);
            }));
            output.append(bad_duplicate && sent == 99 ? frame(4, [](bytes&) {}, 99) : entry(leaf, sent));
        }
        ::ceph::encode(std::uint8_t {0x80}, output);
        ::ceph::encode(std::string {"m\0r", 3}, output);
    });
}

} // anonymous namespace

TEST_CASE("Usage add preserves nested sequence and legacy leaf profiles without unused user ownership", "[rgw][cls][datum][usage]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        for (std::uint8_t leaf = 0; leaf <= 5; ++leaf) {
            const auto input = fragmented(frame(version, [&](bytes& output) {
                output.append(frame(1, [&](bytes& sequence) {
                    ::ceph::encode(std::uint32_t {2}, sequence);
                    sequence.append(entry(leaf, 11));
                    sequence.append(entry(leaf, 13));
                }));
                if (version >= 2) { ::ceph::encode(std::string {"$ns$id\0tail", 11}, output); }
            }));
            compare<binding::add_request>(input);
        }
    }
    ::rgw_cls_usage_log_add_op expected;
    ::rgw_usage_log_entry item;
    item.owner.from_str("tenant$ns$id");
    item.epoch = std::numeric_limits<std::uint64_t>::max();
    item.bucket = std::string(80, 'b');
    item.add_usage("category", {});
    expected.info.entries = {item, item};
    CHECK(binding::encode_add(expected.info.entries).contents_equal(canonical(expected)));
    CHECK(binding::encode_add({}).contents_equal(canonical(::rgw_cls_usage_log_add_op {})));
}

TEST_CASE("Usage read and trim retain field order and bucket version defaults", "[rgw][cls][datum][usage]")
{
    for (const std::uint8_t version : {0, 1, 2, 3, 10}) {
        compare<binding::read_request>(fragmented(frame(version, [&](bytes& output) {
            ::ceph::encode(std::uint64_t {17}, output);
            ::ceph::encode(std::numeric_limits<std::uint64_t>::max(), output);
            ::ceph::encode(std::string {"0user"}, output);
            ::ceph::encode(std::string {"m\0r", 3}, output);
            ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
            if (version >= 2) { ::ceph::encode(std::string {"b\0t", 3}, output); }
        })));
        compare<binding::trim_request>(fragmented(frame(version, [&](bytes& output) {
            ::ceph::encode(std::uint64_t {17}, output);
            ::ceph::encode(std::numeric_limits<std::uint64_t>::max(), output);
            ::ceph::encode(std::string {"u\0r", 3}, output);
            if (version >= 3) { ::ceph::encode(std::string {"b\0t", 3}, output); }
        })));
    }
    binding::read_request read {17, 19, "user", "bucket", "marker", 23};
    CHECK(binding::encode_read(read.owner, read.bucket, 17, 19, 23, read.iter).contents_equal(canonical(native(read))));
    binding::trim_request trim {17, 19, "user", "bucket"};
    CHECK(binding::encode_trim(trim.user, trim.bucket, 17, 19).contents_equal(canonical(native(trim))));
}

TEST_CASE("Usage associative replies retain first-wins decoding in tree and flat maps", "[rgw][cls][datum][usage]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        for (std::uint8_t leaf = 0; leaf <= 5; ++leaf) {
            const auto input = fragmented(reply(version, leaf));
            ::rgw_cls_usage_log_read_ret expected;
            auto cursor = input.cbegin();
            ::decode(expected, cursor);
            const auto tree = binding::decode_reply<binding::native_entries>(input);
            const auto flat = binding::decode_reply<flat_entries>(input);
            CHECK(canonical(native(tree)).contents_equal(canonical(expected)));
            CHECK(canonical(native(flat)).contents_equal(canonical(expected)));
            REQUIRE(tree.usage.size() == 2);
            CHECK(tree.usage.at(::rgw_user_bucket {"z", "bucket"}).total_usage.bytes_sent == 11);
            bytes encoded;
            binding::encode(tree, encoded);
            CHECK(encoded.contents_equal(canonical(expected)));
            for (unsigned size = 0; size < input.length(); ++size) {
                bytes prefix;
                prefix.substr_of(input, 0, size);
                CHECK_THROWS_AS(binding::decode_reply<binding::native_entries>(prefix), ::ceph::buffer::error);
                CHECK_THROWS_AS(binding::decode_reply<flat_entries>(prefix), ::ceph::buffer::error);
            }
        }
    }
    const auto broken = reply(1, 4, true);
    CHECK_THROWS_AS(binding::decode_reply<binding::native_entries>(broken), ::ceph::buffer::error);
    CHECK_THROWS_AS(binding::decode_reply<flat_entries>(broken), ::ceph::buffer::error);
}

TEST_CASE("Usage profiles reject impossible counts and incompatible envelopes", "[rgw][cls][datum][usage]")
{
    auto input = frame(2, [&](bytes& output) {
        output.append(frame(1, [](bytes& sequence) { ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), sequence); }));
        ::ceph::encode(std::string {}, output);
    });
    binding::add_request add;
    add.info.entries.emplace_back().bucket = "saved";
    auto cursor = input.cbegin();
    CHECK_THROWS_AS(binding::decode(add, cursor), ::ceph::buffer::error);
    CHECK(cursor.get_off() == 0);
    CHECK(add.info.entries.front().bucket == "saved");
    input = frame(1, [](bytes& output) {
        ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
        ::ceph::encode(false, output);
        ::ceph::encode(std::string {}, output);
    });
    CHECK_THROWS_AS(binding::decode_reply<binding::native_entries>(input), ::ceph::buffer::error);
    input = frame(10, [](bytes&) {}, 20);
    cursor = input.cbegin();
    binding::read_request read;
    CHECK_THROWS_AS(binding::decode(read, cursor), ::ceph::buffer::error);
    CHECK(cursor.get_off() == 0);
    binding::trim_request trim;
    CHECK_THROWS_AS(binding::decode(trim, cursor), ::ceph::buffer::error);
    CHECK(cursor.get_off() == 0);
}
