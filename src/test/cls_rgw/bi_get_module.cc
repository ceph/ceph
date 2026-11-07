// RGW CLS: Actual BI-get handler with an exact, opaque OMAP boundary.
#include "module_loader.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstdint>
#include <cstdlib>

#include <fstream>
#include <string>
#include <utility>
#include <vector>

namespace {

using byte_buffer = ::ceph::buffer::list;

struct storage final
{
    std::vector<std::string> reads;
    byte_buffer payload;
    int status = 0;
};

auto invoke(storage& state, byte_buffer input)
{
    const auto [call, flags] = ::rgw_cls_test::get_method("bi_get");
    REQUIRE(flags == CLS_METHOD_RD);
    byte_buffer output;
    output.append("prefix", 6);
    const int status = call(&state, &input, &output);
    if (const char *path = std::getenv("CEPH_TEST_CLS_RGW_TRANSCRIPT")) {
        static std::ofstream transcript {path, std::ios::binary};
        byte_buffer record;
        ::ceph::encode(input, record);
        ::ceph::encode(status, record);
        ::ceph::encode(output, record);
        ::ceph::encode(state.reads, record);
        record.write_stream(transcript);
        REQUIRE(transcript.good());
    }

    return std::pair {status, std::move(output)};
}

byte_buffer request(const ::BIIndexType type, const ::cls_rgw_obj_key& key = {"key", "instance"})
{
    ::rgw_cls_bi_get_op value;
    value.type = type;
    value.key = key;
    byte_buffer bytes;
    ::encode(value, bytes);

    return bytes;
}

} // anonymous namespace

int cls_cxx_map_get_val(cls_method_context_t context, const std::string& key, byte_buffer *output)
{
    auto& state = *static_cast<storage *>(context);
    state.reads.push_back(key);
    if (state.status < 0) {
        return state.status;
    }
    *output = state.payload;

    return state.status;
}

TEST_CASE("BI get selects native namespaces and returns opaque stored bytes", "[rgw][cls][bi-get]")
{
    const std::string prefix(1, static_cast<char>(0x80));
    const std::string name {"a\0b", 3};
    struct lookup final
    {
        ::BIIndexType type;
        ::cls_rgw_obj_key key;
        std::string index;
    };
    const std::vector<lookup> cases {
        {::BIIndexType::Plain, {"", ""}, ""},
        {::BIIndexType::Plain, {name, "ignored"}, name},
        {::BIIndexType::Instance, {name, ""}, name},
        {::BIIndexType::Instance, {name, "null"}, prefix + "1000_" + name + std::string {"\0inull", 6}},
        {::BIIndexType::Instance, {name, std::string {"x\0y", 3}},
         prefix + "1000_" + name + std::string {"\0ix\0y", 5}},
        {::BIIndexType::OLH, {name, "ignored"}, prefix + "1001_" + name},
        {::BIIndexType::OLH, {"", "null"}, prefix + "1001_"}
    };
    for (const auto& item : cases) {
        for (const bool empty : {false, true}) {
            storage state;
            if (!empty) {
                // Deliberately not a native directory or OLH record.
                state.payload.append(::ceph::buffer::ptr {::ceph::buffer::copy("a\0", 2)});
                state.payload.append(::ceph::buffer::ptr {::ceph::buffer::copy("\xFF!", 2)});
            }
            const auto [status, output] = invoke(state, request(item.type, item.key));
            REQUIRE(status == 0);
            CHECK(state.reads == std::vector<std::string> {item.index});
            ::rgw_cls_bi_get_ret expected;
            expected.entry = {item.type, item.index, state.payload};
            byte_buffer encoded;
            encoded.append("prefix", 6);
            ::encode(expected, encoded);
            CHECK(output.contents_equal(encoded));
            ::rgw_cls_bi_get_ret result;
            auto cursor = output.cbegin(6);
            ::decode(result, cursor);
            CHECK(cursor.end());
            CHECK(result.entry.type == item.type);
            CHECK(result.entry.idx == item.index);
            CHECK(result.entry.data.contents_equal(state.payload));
        }
    }
}

TEST_CASE("BI get preserves storage errors and rejects invalid kinds before lookup", "[rgw][cls][bi-get]")
{
    for (const auto type : {::BIIndexType::Plain, ::BIIndexType::Instance, ::BIIndexType::OLH}) {
        for (const int result : {-ENOENT, -EIO, -EAGAIN, -ENOMEM, 7}) {
            storage state;
            state.status = result;
            const auto [status, output] = invoke(state, request(type));
            CHECK(status == (result < 0 ? result : 0));
            CHECK(state.reads.size() == 1);
            if (result < 0) {
                CHECK(output.to_str() == "prefix");
            }
        }
    }
    for (unsigned raw = 0; raw < 256; ++raw) {
        const auto type = static_cast<::BIIndexType>(raw);
        if (type == ::BIIndexType::Plain || type == ::BIIndexType::Instance || type == ::BIIndexType::OLH) {
            continue;
        }
        storage state;
        state.status = -EAGAIN;
        const auto [status, output] = invoke(state, request(type));
        CHECK(status == -EINVAL);
        CHECK(state.reads.empty());
        CHECK(output.to_str() == "prefix");
    }
}

TEST_CASE("BI get rejects every request truncation before storage access", "[rgw][cls][bi-get]")
{
    const auto full = request(::BIIndexType::Instance);
    for (unsigned size = 0; size < full.length(); ++size) {
        byte_buffer bytes;
        bytes.substr_of(full, 0, size);
        storage state;
        state.status = -EAGAIN;
        const auto [status, output] = invoke(state, std::move(bytes));
        CHECK(status == -EINVAL);
        CHECK(state.reads.empty());
        CHECK(output.to_str() == "prefix");
    }
}
