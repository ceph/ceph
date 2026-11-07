// RGW CLS: Independent object records checked against the C++23 native codec.
#include "test/cls_rgw/wire_test.h"
#include "rgw/datum/adapters/cls/object.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <type_traits>

namespace {

namespace binding = ::cls::rgw::object_datum;
using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;
using ::rgw_cls_test::fragmented;

auto native(const binding::remove_request& value)
{
    ::rgw_cls_obj_remove_op result;
    result.keep_attr_prefixes.assign(value.keep_attr_prefixes.begin(), value.keep_attr_prefixes.end());

    return result;
}

auto native(const binding::version_request& value)
{
    return ::rgw_cls_obj_store_pg_ver_op {value.attr};
}

auto native(const binding::prefix_request& value)
{
    ::rgw_cls_obj_check_attrs_prefix result;
    result.check_prefix = value.check_prefix;
    result.fail_if_exist = value.fail_if_exist;

    return result;
}

auto native(const binding::mtime_request& value)
{
    ::rgw_cls_obj_check_mtime result;
    result.mtime = value.mtime;
    result.type = static_cast<::RGWCheckMTimeType>(value.type);
    result.high_precision_time = value.high_precision_time;

    return result;
}

bytes frame(const std::uint8_t version, const std::uint8_t compatible, const auto& body)
{
    bytes output;
    ENCODE_START(version, compatible, output);
    body(output);
    if (version > 2) { ::ceph::encode(std::uint64_t {37}, output); }
    ENCODE_FINISH(output);

    return output;
}

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
    CHECK(canonical(native(actual)).contents_equal(preserved));
}

} // anonymous namespace

TEST_CASE("Object removal binds native list bytes to ordered vector prefixes", "[rgw][cls][datum][object]")
{
    static_assert(std::is_same_v<decltype(binding::remove_request::keep_attr_prefixes), std::vector<std::string>>);
    const std::list<std::string> prefixes {"", "z", std::string {"a\0b", 3}, "z", std::string(80, 'x')};
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        compare<binding::remove_request>(fragmented(frame(version, 0, [&](bytes& output) {
            ::ceph::encode(prefixes, output);
        })));
    }
    ::rgw_cls_obj_remove_op expected;
    expected.keep_attr_prefixes = prefixes;
    CHECK(binding::encode_remove(prefixes).contents_equal(canonical(expected)));
    CHECK(binding::encode_remove({}).contents_equal(canonical(::rgw_cls_obj_remove_op {})));
}

TEST_CASE("Object string requests retain arbitrary bytes and nonzero boolean semantics", "[rgw][cls][datum][object]")
{
    for (const std::string& text : {std::string {}, std::string {"a\0b", 3}, std::string(80, 'x')}) {
        for (const std::uint8_t version : {0, 1, 2, 10}) {
            compare<binding::version_request>(fragmented(frame(version, 0, [&](bytes& output) {
                ::ceph::encode(text, output);
            })));
            compare<binding::prefix_request>(fragmented(frame(version, 0, [&](bytes& output) {
                ::ceph::encode(text, output);
                ::ceph::encode(std::uint8_t {0x80}, output);
            })));
        }
        CHECK(binding::encode_version(text).contents_equal(canonical(::rgw_cls_obj_store_pg_ver_op {text})));
        for (const bool flag : {false, true}) {
            binding::prefix_request request {text, flag};
            CHECK(binding::encode_prefix(text, flag).contents_equal(canonical(native(request))));
        }
    }
}

TEST_CASE("Mtime requests retain old precision defaults and native timestamp encoding", "[rgw][cls][datum][object]")
{
    for (const auto time : {::ceph::real_time {}, ::ceph::real_time {std::chrono::seconds {123} + std::chrono::nanoseconds {987654321}}}) {
        for (const std::uint8_t version : {0, 1, 2, 3, 10}) {
            for (std::uint8_t kind = 0; kind < 8; ++kind) {
                compare<binding::mtime_request>(fragmented(frame(version, 0, [&](bytes& output) {
                    ::ceph::encode(time, output);
                    ::ceph::encode(kind, output);
                    if (version >= 2) { ::ceph::encode(std::uint8_t {0x80}, output); }
                })));
                binding::mtime_request request {time, kind, true};
                CHECK(binding::encode_mtime(time, true, kind).contents_equal(canonical(native(request))));
            }
        }
    }
    // All bytes have a safe representation in the independent module record.
    for (unsigned kind = 0; kind < 256; ++kind) {
        const auto input = binding::encode_mtime({}, false, kind);
        auto cursor = input.cbegin();
        binding::mtime_request request;
        binding::decode(request, cursor);
        CHECK(cursor.end());
        CHECK(request.type == kind);
    }
}

TEST_CASE("Object decoders reject incompatible frames and impossible counts before publication", "[rgw][cls][datum][object]")
{
    const auto reject = []<typename RecordT>(RecordT value, const bytes& input) {
        const auto preserved = canonical(native(value));
        auto cursor = input.cbegin();
        CHECK_THROWS_AS(binding::decode(value, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
        CHECK(canonical(native(value)).contents_equal(preserved));
    };
    const auto incompatible = frame(10, 20, [](bytes&) {});
    reject(binding::remove_request {{"saved"}}, incompatible);
    reject(binding::version_request {"saved"}, incompatible);
    reject(binding::prefix_request {"saved", true}, incompatible);
    reject(binding::mtime_request {{}, 1, true}, incompatible);
    for (const std::uint32_t count : {1u, std::numeric_limits<std::uint32_t>::max()}) {
        reject(binding::remove_request {{"saved"}}, frame(1, 1, [&](bytes& output) {
            ::ceph::encode(count, output);
        }));
    }
    reject(binding::version_request {"saved"}, frame(1, 1, [](bytes& output) {
        ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
    }));
}
