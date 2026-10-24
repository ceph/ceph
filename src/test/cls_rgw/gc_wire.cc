// RGW CLS: Independent legacy GC requests/replies checked by native C++23 codecs.
#include "test/cls_rgw/wire_test.h"
#include "rgw/datum/adapters/cls/gc.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>

namespace {

namespace binding = ::cls::rgw::gc_datum;
using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;
using ::rgw_cls_test::fragmented;

bytes frame(const std::uint8_t version, const auto& body, const std::uint8_t compatible = 0)
{
    bytes output;
    ENCODE_START(version, compatible, output);
    body(output);
    if (version > 2) { ::ceph::encode(std::uint64_t {37}, output); }
    ENCODE_FINISH(output);

    return output;
}

auto native(const binding::set_request& value)
{
    ::cls_rgw_gc_set_entry_op result;
    result.expiration_secs = value.expiration_secs;
    result.info = value.info;

    return result;
}

auto native(const binding::defer_request& value)
{
    ::cls_rgw_gc_defer_entry_op result;
    result.expiration_secs = value.expiration_secs;
    result.tag = value.tag;

    return result;
}

auto native(const binding::list_request& value)
{
    ::cls_rgw_gc_list_op result;
    result.marker = value.marker;
    result.max = value.max;
    result.expired_only = value.expired_only;

    return result;
}

auto native(const binding::remove_request& value)
{
    ::cls_rgw_gc_remove_op result;
    result.tags = value.tags;

    return result;
}

template <typename EntriesT>
auto native(const binding::list_reply<EntriesT>& value)
{
    ::cls_rgw_gc_list_ret result;
    result.entries.assign(value.entries.begin(), value.entries.end());
    result.next_marker = value.next_marker;
    result.truncated = value.truncated;

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

bytes info(const std::uint8_t version, const std::uint8_t object_version)
{
    return frame(version, [&](bytes& output) {
        ::ceph::encode(std::string {"t\0g", 3}, output);
        output.append(frame(version, [&](bytes& chain) {
            ::ceph::encode(std::uint32_t {2}, chain);
            for (unsigned index = 0; index < 2; ++index) {
                chain.append(frame(object_version, [&](bytes& object) {
                    ::ceph::encode(std::string(80, 'p'), object);
                    ::ceph::encode(std::string {"old-name"}, object);
                    ::ceph::encode(std::string {"l\0c", 3}, object);
                    if (object_version >= 2) { ::encode(::cls_rgw_obj_key {"new-name", "instance"}, object); }
                }));
            }
        }));
        ::ceph::encode(::ceph::real_time {std::chrono::seconds {123}}, output);
    });
}

} // anonymous namespace

TEST_CASE("GC set borrows nested native object chains and retains old leaf encodings", "[rgw][cls][datum][gc]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        for (const std::uint8_t leaf : {0, 1, 2, 10}) {
            for (const std::uint8_t object : {0, 1, 2, 10}) {
                compare<binding::set_request>(fragmented(frame(version, [&](bytes& output) {
                    ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
                    output.append(info(leaf, object));
                })));
            }
        }
    }
    const auto input = info(1, 2);
    ::cls_rgw_gc_obj_info value;
    auto cursor = input.cbegin();
    ::decode(value, cursor);
    binding::set_request request {value, 17};
    CHECK(binding::encode_set(17, value).contents_equal(canonical(native(request))));
}

TEST_CASE("GC defer remove and list retain field order and old expired-only default", "[rgw][cls][datum][gc]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        compare<binding::defer_request>(fragmented(frame(version, [](bytes& output) {
            ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
            ::ceph::encode(std::string {"t\0g", 3}, output);
        })));
        compare<binding::remove_request>(fragmented(frame(version, [](bytes& output) {
            ::ceph::encode(std::vector<std::string> {"", "z", std::string {"a\0b", 3}, "z"}, output);
        })));
        for (const bool expired : {false, true}) {
            const auto input = fragmented(frame(version, [&](bytes& output) {
                ::ceph::encode(std::string {"m\0r", 3}, output);
                ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
                if (version >= 2) { ::ceph::encode(static_cast<std::uint8_t>(expired ? 0x80 : 0), output); }
            }));
            compare<binding::list_request>(input);
            binding::list_request request;
            auto cursor = input.cbegin();
            binding::decode(request, cursor);
            CHECK(request.expired_only == (version < 2 || expired));
        }
    }
    binding::defer_request defer {std::string {"t\0g", 3}, 17};
    CHECK(binding::encode_defer(17, defer.tag).contents_equal(canonical(native(defer))));
    binding::remove_request remove {{"", "a", "a"}};
    CHECK(binding::encode_remove(remove.tags).contents_equal(canonical(native(remove))));
    binding::list_request list {"marker", 19, false};
    CHECK(binding::encode_list(list.marker, 19, false).contents_equal(canonical(native(list))));
}

TEST_CASE("GC reply binds vector and list while retaining old marker defaults", "[rgw][cls][datum][gc]")
{
    for (const std::uint8_t version : {0, 1, 2, 10}) {
        for (const std::uint8_t leaf : {0, 1, 2, 10}) {
            const auto input = fragmented(frame(version, [&](bytes& output) {
                ::ceph::encode(std::uint32_t {2}, output);
                output.append(info(leaf, 1));
                output.append(info(leaf, 2));
                if (version >= 2) { ::ceph::encode(std::string {"m\0r", 3}, output); }
                ::ceph::encode(std::uint8_t {0x80}, output);
            }));
            ::cls_rgw_gc_list_ret expected;
            auto cursor = input.cbegin();
            ::decode(expected, cursor);
            const auto vector = binding::decode_reply<std::vector<::cls_rgw_gc_obj_info>>(input);
            const auto list = binding::decode_reply<std::list<::cls_rgw_gc_obj_info>>(input);
            CHECK(canonical(native(vector)).contents_equal(canonical(expected)));
            CHECK(canonical(native(list)).contents_equal(canonical(expected)));
            bytes output;
            binding::encode(vector, output);
            CHECK(output.contents_equal(canonical(expected)));
            for (unsigned size = 0; size < input.length(); ++size) {
                bytes prefix;
                prefix.substr_of(input, 0, size);
                CHECK_THROWS_AS(binding::decode_reply<std::vector<::cls_rgw_gc_obj_info>>(prefix), ::ceph::buffer::error);
                CHECK_THROWS_AS(binding::decode_reply<std::list<::cls_rgw_gc_obj_info>>(prefix), ::ceph::buffer::error);
            }
        }
    }
}

TEST_CASE("GC adapters reject incompatible frames and impossible outer sequence counts", "[rgw][cls][datum][gc]")
{
    const auto input = frame(10, [](bytes&) {}, 20);
    const auto reject = [&]<typename T>(T value) {
        const auto preserved = canonical(native(value));
        auto cursor = input.cbegin();
        CHECK_THROWS_AS(binding::decode(value, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
        CHECK(canonical(native(value)).contents_equal(preserved));
    };
    reject(binding::set_request {});
    reject(binding::defer_request {"saved", 17});
    reject(binding::list_request {"saved", 19, false});
    reject(binding::remove_request {{"saved"}});
    const auto impossible = frame(1, [](bytes& output) {
        ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), output);
    });
    binding::remove_request request {{"saved"}};
    auto cursor = impossible.cbegin();
    CHECK_THROWS_AS(binding::decode(request, cursor), ::ceph::buffer::error);
    CHECK(cursor.get_off() == 0);
    CHECK(request.tags == std::vector<std::string> {"saved"});
    CHECK_THROWS_AS(binding::decode_reply<std::vector<::cls_rgw_gc_obj_info>>(impossible), ::ceph::buffer::error);
}
