// RGW CLS: Native reshard wire compatibility with independent records/containers.
#include "test/cls_rgw/wire_test.h"
#include "rgw/datum/adapters/cls/reshard.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>

namespace {

namespace binding = ::cls::rgw::reshard_datum;
using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;
using ::rgw_cls_test::fragmented;

bytes frame(const std::uint8_t version, const auto& body, const std::uint8_t compatible = 0)
{
    bytes output;
    ENCODE_START(version, compatible, output);
    body(output);
    if (version > 3) { ::ceph::encode(std::uint64_t {37}, output); }
    ENCODE_FINISH(output);

    return output;
}

auto native(const binding::add_request& value)
{
    ::cls_rgw_reshard_add_op result;
    result.entry = value.entry;
    result.create_only = value.create_only;

    return result;
}

auto native(const binding::entry_message& value)
{
    ::cls_rgw_reshard_get_op result;
    result.entry = value.entry;

    return result;
}

auto native(const binding::instance_message& value)
{
    return ::cls_rgw_set_bucket_resharding_op {value.entry};
}

auto native(const binding::list_request& value)
{
    ::cls_rgw_reshard_list_op result;
    result.marker = value.marker;
    result.max = value.max;

    return result;
}

auto native(const binding::remove_request& value)
{
    ::cls_rgw_reshard_remove_op result;
    result.tenant = value.tenant;
    result.bucket_name = value.bucket_name;
    result.bucket_id = value.bucket_id;

    return result;
}

auto native(const binding::empty_request&) { return ::cls_rgw_clear_bucket_resharding_op {}; }
auto native(const binding::guard_request& value) { return ::cls_rgw_guard_bucket_resharding_op {value.ret_err}; }

template <typename RecordT>
void compare(const bytes& input)
{
    RecordT actual;
    auto expected = native(actual);
    auto cursor = input.cbegin();
    ::decode(expected, cursor);
    const auto consumed = cursor.get_off();
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

bytes entry(const std::uint8_t version)
{
    return frame(version, [&](bytes& output) {
        ::ceph::encode(::ceph::real_time {std::chrono::seconds {123}}, output);
        ::ceph::encode(std::string {"t\0t", 3}, output);
        ::ceph::encode(std::string(80, 'b'), output);
        ::ceph::encode(std::string {"id"}, output);
        if (version < 2) { ::ceph::encode(std::string {"removed"}, output); }
        ::ceph::encode(std::uint32_t {17}, output);
        ::ceph::encode(std::uint32_t {19}, output);
        if (version >= 3) { ::ceph::encode(std::uint8_t {255}, output); }
    });
}

bytes instance(const std::uint8_t version, const std::uint8_t status)
{
    return frame(version, [&](bytes& output) {
        ::ceph::encode(status, output);
        if (version != 2) {
            ::ceph::encode(std::string {"ignored"}, output);
            ::ceph::encode(std::int32_t {-1}, output);
        }
    });
}

} // anonymous namespace

TEST_CASE("Reshard queue requests preserve legacy leaves and borrowed wire profiles", "[rgw][cls][datum][reshard]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        for (std::uint8_t leaf = 0; leaf <= 4; ++leaf) {
            compare<binding::add_request>(fragmented(frame(version, [&](bytes& output) {
                output.append(entry(leaf));
                if (version >= 2) { ::ceph::encode(std::uint8_t {0x80}, output); }
            })));
            const auto input = fragmented(frame(version, [&](bytes& output) { output.append(entry(leaf)); }));
            compare<binding::entry_message>(input);
            ::cls_rgw_reshard_get_ret expected;
            auto cursor = input.cbegin();
            ::decode(expected, cursor);
            CHECK(canonical(binding::decode_entry(input)).contents_equal(canonical(expected.entry)));
        }
        compare<binding::list_request>(fragmented(frame(version, [](bytes& output) {
            ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
            ::ceph::encode(std::string {"m\0r", 3}, output);
        })));
        compare<binding::remove_request>(fragmented(frame(version, [](bytes& output) {
            for (const std::string& value : {std::string {"t\0t", 3}, std::string(80, 'b'), std::string {"id"}}) { ::ceph::encode(value, output); }
        })));
    }
    ::cls_rgw_reshard_entry value;
    const auto encoded = entry(3);
    auto cursor = encoded.cbegin();
    ::decode(value, cursor);
    binding::add_request add {value, true};
    CHECK(binding::encode_add(value, true).contents_equal(canonical(native(add))));
    CHECK(binding::encode_entry(value).contents_equal(canonical(native(binding::entry_message {value}))));
    binding::remove_request remove {value.tenant, value.bucket_name, value.bucket_id};
    CHECK(binding::encode_remove(value).contents_equal(canonical(native(remove))));
    CHECK(binding::encode_list("marker", 17).contents_equal(canonical(native(binding::list_request {"marker", 17}))));
}

TEST_CASE("Reshard control preserves empty frames, signed errors and all status bytes", "[rgw][cls][datum][reshard]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        compare<binding::empty_request>(fragmented(frame(version, [](bytes&) {})));
        for (const int error : {std::numeric_limits<int>::min(), -2300, -1, 0, 7, std::numeric_limits<int>::max()}) {
            compare<binding::guard_request>(fragmented(frame(version, [&](bytes& output) { ::ceph::encode(error, output); })));
            CHECK(binding::encode_guard(error).contents_equal(canonical(native(binding::guard_request {error}))));
        }
        for (std::uint8_t leaf = 0; leaf <= 4; ++leaf) {
            const auto input = fragmented(frame(version, [&](bytes& output) { output.append(instance(leaf, 255)); }));
            compare<binding::instance_message>(input);
            CHECK(binding::decode_instance(input).reshard_status == static_cast<::cls_rgw_reshard_status>(255));
        }
    }
    CHECK(binding::encode_empty().contents_equal(canonical(::cls_rgw_get_bucket_resharding_op {})));
    for (unsigned status = 0; status < 256; ++status) {
        const ::cls_rgw_bucket_instance_entry value {static_cast<::cls_rgw_reshard_status>(status)};
        CHECK(binding::encode_instance(value).contents_equal(canonical(native(binding::instance_message {value}))));
    }
}

TEST_CASE("Reshard list binds native list encoding to both sequence layouts", "[rgw][cls][datum][reshard]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        const auto input = fragmented(frame(version, [](bytes& output) {
            ::ceph::encode(std::uint32_t {5}, output);
            for (const std::uint8_t leaf : {3, 1, 2, 0, 4}) { output.append(entry(leaf)); }
            ::ceph::encode(std::uint8_t {0x80}, output);
        }));
        ::cls_rgw_reshard_list_ret expected;
        auto cursor = input.cbegin();
        ::decode(expected, cursor);
        auto [vector, truncated] = binding::decode_reply<std::vector<::cls_rgw_reshard_entry>>(input);
        auto [list, old_truncated] = binding::decode_reply<std::list<::cls_rgw_reshard_entry>>(input);
        CHECK(canonical(vector).contents_equal(canonical(expected.entries)));
        CHECK(canonical(list).contents_equal(canonical(expected.entries)));
        CHECK(truncated == expected.is_truncated);
        CHECK(old_truncated == truncated);
        bytes output;
        binding::encode(binding::reply {std::move(vector), truncated}, output);
        CHECK(output.contents_equal(canonical(expected)));
        for (unsigned size = 0; size < input.length(); ++size) {
            bytes prefix;
            prefix.substr_of(input, 0, size);
            CHECK_THROWS_AS(binding::decode_reply<std::vector<::cls_rgw_reshard_entry>>(prefix), ::ceph::buffer::error);
            CHECK_THROWS_AS(binding::decode_reply<std::list<::cls_rgw_reshard_entry>>(prefix), ::ceph::buffer::error);
        }
    }
    const auto input = frame(1, [](bytes& output) {
        ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
        ::ceph::encode(false, output);
    });
    CHECK_THROWS_AS(binding::decode_reply<std::vector<::cls_rgw_reshard_entry>>(input), ::ceph::buffer::error);
}

TEST_CASE("Reshard request candidates reject incompatible frames atomically", "[rgw][cls][datum][reshard]")
{
    const auto input = frame(10, [](bytes&) {}, 20);
    const auto reject = [&]<typename T>(T value) {
        const auto preserved = canonical(native(value));
        auto cursor = input.cbegin();
        CHECK_THROWS_AS(binding::decode(value, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
        CHECK(canonical(native(value)).contents_equal(preserved));
    };
    reject(binding::add_request {});
    reject(binding::entry_message {});
    reject(binding::instance_message {});
    reject(binding::list_request {"saved", 17});
    reject(binding::remove_request {"saved", "bucket", "id"});
    reject(binding::guard_request {-17});
    reject(binding::empty_request {});
}
