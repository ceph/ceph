// RGW CLS: Execute the actual client through a controlled transport boundary.
// This checks request bytes and atomic publication without a live OSD.
#include "cls/rgw/cls_rgw_client.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <new>
#include <string_view>

namespace
{
using byte_buffer = ::ceph::buffer::list;
byte_buffer request_bytes;
byte_buffer reply_bytes;
int transport_status = 0;
bool allocation_failure = false;

byte_buffer response()
{
    ::rgw_cls_bi_list_ret value;
    value.entries.push_back({::BIIndexType::Plain, "row", {}});
    value.is_truncated = true;
    byte_buffer bytes;
    ::encode(value, bytes);

    return bytes;
}

} // anonymous namespace

int librados::IoCtx::exec_impl(const std::string& oid, const char *cls, const char *method,
                              byte_buffer& input, byte_buffer& output)
{
    CHECK(oid == "object");
    CHECK(std::string_view {cls} == "rgw");
    CHECK(std::string_view {method} == "bi_list");
    if (allocation_failure) {
        throw std::bad_alloc {};
    }
    request_bytes = input;
    output = reply_bytes;

    return transport_status;
}

TEST_CASE("BI client preserves native requests and both output interfaces", "[rgw][cls][datum]")
{
    ::librados::IoCtx context;
    reply_bytes = response();
    transport_status = 0;
    allocation_failure = false;
    ::rgw_cls_bi_list_op request;
    request.max = 19;
    request.name_filter = std::string {"a\0b", 3};
    request.marker = "marker";
    request.reshardlog = true;
    byte_buffer expected;
    ::encode(request, expected);
    std::vector<::rgw_cls_bi_entry> entries;
    bool truncated = false;

    CHECK(0 == ::cls_rgw_bi_list(context, "object", request.name_filter, request.marker,
                               request.max, entries, truncated, true));
    CHECK(request_bytes.contents_equal(expected));
    REQUIRE(entries.size() == 1);
    CHECK(entries[0].idx == "row");
    CHECK(truncated);

    std::list<::rgw_cls_bi_entry> compatible;
    CHECK(0 == ::cls_rgw_bi_list(context, "object", request.name_filter, request.marker,
                               request.max, &compatible, &truncated, true));
    CHECK(request_bytes.contents_equal(expected));
    REQUIRE(compatible.size() == 1);
    CHECK(compatible.front().idx == "row");
}

TEST_CASE("BI client failures preserve caller outputs and native status", "[rgw][cls][datum]")
{
    ::librados::IoCtx context;
    std::vector<::rgw_cls_bi_entry> entries {{::BIIndexType::Invalid, "preserve", {}}};
    std::list<::rgw_cls_bi_entry> compatible {{::BIIndexType::Invalid, "preserve", {}}};
    bool truncated = false;
    allocation_failure = false;
    transport_status = 0;
    const auto full = response();
    for (unsigned size = 0; size < full.length(); ++size) {
        reply_bytes.clear();
        reply_bytes.substr_of(full, 0, size);
        CHECK(-EIO == ::cls_rgw_bi_list(context, "object", "", "", 9, entries, truncated));
        CHECK(-EIO == ::cls_rgw_bi_list(context, "object", "", "", 9, &compatible, &truncated));
        CHECK(-EIO == ::cls_rgw_bi_list(context, "object", "", "", 9, nullptr, nullptr));
        REQUIRE(compatible.size() == 1);
        CHECK(compatible.front().idx == "preserve");
        REQUIRE(entries.size() == 1);
        CHECK(entries[0].idx == "preserve");
        CHECK_FALSE(truncated);
    }
    transport_status = -EAGAIN;
    CHECK(-EAGAIN == ::cls_rgw_bi_list(context, "object", "", "", 9, entries, truncated));
    // Existing pointer outputs are not required on a failed transport call.
    CHECK(-EAGAIN == ::cls_rgw_bi_list(context, "object", "", "", 9, nullptr, nullptr));
    allocation_failure = true;
    CHECK(-ENOMEM == ::cls_rgw_bi_list(context, "object", "", "", 9, entries, truncated));
    allocation_failure = false;
    transport_status = 0;
    REQUIRE(entries.size() == 1);
    CHECK(entries[0].idx == "preserve");
    CHECK_FALSE(truncated);
}

void librados::ObjectOperation::exec_impl(const char *cls, const char *method, byte_buffer& input)
{
    CHECK(std::string_view {cls} == "rgw");
    CHECK(std::string_view {method} == "bi_put_entries");
    request_bytes = input;
}

TEST_CASE("BI batch client retains encoded bytes after consuming its input", "[rgw][cls][bi-put]")
{
    byte_buffer expected;
    {
        ::rgw_cls_bi_put_entries_op reference;
        reference.check_existing = true;
        byte_buffer payload;
        payload.append("opaque", 6);
        reference.entries.push_back({::BIIndexType::Plain, std::string(80, 'k'), payload});
        ::encode(reference, expected);
        ::librados::ObjectWriteOperation operation;
        ::cls_rgw_bi_put_entries(operation, std::move(reference.entries), true);
    }
    CHECK(request_bytes.contents_equal(expected));
    ::rgw_cls_bi_put_entries_op decoded;
    auto cursor = request_bytes.cbegin();
    ::decode(decoded, cursor);
    REQUIRE(decoded.entries.size() == 1);
    CHECK(decoded.entries[0].idx == std::string(80, 'k'));
    CHECK(decoded.entries[0].data.to_str() == "opaque");
    CHECK(decoded.check_existing);
    CHECK(cursor.end());
}
