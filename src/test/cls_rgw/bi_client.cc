// RGW CLS: Execute the actual client through a controlled transport boundary.
// This checks request bytes and atomic publication without a live OSD.
#include "cls/rgw/cls_rgw_client.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <limits>
#include <new>
#include <utility>

#include <chrono>
#include <string>
#include <string_view>

namespace
{
using byte_buffer = ::ceph::buffer::list;
byte_buffer request_bytes;
byte_buffer reply_bytes;
int transport_status = 0;
bool allocation_failure = false;
std::string request_method;
int put_transport_status = 0;
byte_buffer *request_output = nullptr;

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
    request_method = method;
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
    CHECK(request_method == "bi_list");
    CHECK(request_bytes.contents_equal(expected));
    REQUIRE(entries.size() == 1);
    CHECK(entries[0].idx == "row");
    CHECK(truncated);

    std::list<::rgw_cls_bi_entry> compatible;
    CHECK(0 == ::cls_rgw_bi_list(context, "object", request.name_filter, request.marker,
                               request.max, &compatible, &truncated, true));
    CHECK(request_method == "bi_list");
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
        CHECK(request_method == "bi_list");
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
    request_method = method;
    request_bytes = input;
    request_output = nullptr;
}

void librados::ObjectOperation::exec_impl(const char *cls, const char *method,
                                          byte_buffer& input, byte_buffer *output, int *status)
{
    exec_impl(cls, method, input);
    request_output = output;
    CHECK(status == nullptr);
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
    CHECK(request_method == "bi_put_entries");
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

int librados::IoCtx::operate(const std::string& oid, ObjectWriteOperation *)
{
    CHECK(oid == "object");

    return put_transport_status;
}

TEST_CASE("BI single put clients share request bytes and own borrowed inputs", "[rgw][cls][single-put]")
{
    byte_buffer expected;
    ::rgw_cls_bi_entry row {::BIIndexType::Plain, std::string(80, 'k'), {}};
    row.data.append("opaque", 6);
    ::rgw_cls_bi_put_op reference;
    reference.entry = row;
    ::encode(reference, expected);
    ::librados::IoCtx context;
    for (const int status : {-EACCES, 0, 7}) {
        put_transport_status = status;
        CHECK(::cls_rgw_bi_put(context, "object", row) == (status < 0 ? status : 0));
        CHECK(request_method == "bi_put");
        CHECK(request_bytes.contents_equal(expected));
    }
    put_transport_status = 0;
    {
        ::librados::ObjectWriteOperation operation;
        ::cls_rgw_bi_put(operation, "ignored", row);
        row.idx.clear();
        row.data.clear();
    }
    CHECK(request_method == "bi_put");
    CHECK(request_bytes.contents_equal(expected));
}

TEST_CASE("BI get client owns its result and preserves synchronous error contracts", "[rgw][cls][bi-get]")
{
    ::librados::IoCtx context;
    const ::cls_rgw_obj_key key {std::string {"a\0b", 3}, std::string(80, 'i')};
    allocation_failure = false;
    for (const auto type : {::BIIndexType::Plain, ::BIIndexType::Instance, ::BIIndexType::OLH,
                           static_cast<::BIIndexType>(255)}) {
        ::rgw_cls_bi_get_op request;
        request.key = key;
        request.type = type;
        byte_buffer expected;
        ::encode(request, expected);
        for (const int status : {0, 7}) {
            transport_status = status;
            ::rgw_cls_bi_entry result;
            const char *backing = nullptr;
            {
                ::rgw_cls_bi_get_ret reply;
                reply.entry = {type, "stored-key", {}};
                reply.entry.data.append(::ceph::buffer::ptr {::ceph::buffer::copy("a\0b", 3)});
                backing = reply.entry.data.cbegin().get_current_ptr().c_str();
                reply_bytes.clear();
                ::encode(reply, reply_bytes);
                CHECK(0 == ::cls_rgw_bi_get(context, "object", type, key, &result));
            }
            reply_bytes.clear();
            CHECK(request_method == "bi_get");
            CHECK(request_bytes.contents_equal(expected));
            CHECK(result.type == type);
            CHECK(result.idx == "stored-key");
            CHECK(result.data.to_str() == std::string {"a\0b", 3});
            CHECK(result.data.cbegin().get_current_ptr().c_str() == backing);
        }
    }
    ::rgw_cls_bi_get_ret reply;
    reply.entry = {::BIIndexType::Plain, "key", {}};
    byte_buffer full;
    ::encode(reply, full);
    ::rgw_cls_bi_entry result {::BIIndexType::Invalid, "preserve", {}};
    result.data.append("owned", 5);
    transport_status = 0;
    for (unsigned size = 0; size < full.length(); ++size) {
        reply_bytes.clear();
        reply_bytes.substr_of(full, 0, size);
        CHECK(-EIO == ::cls_rgw_bi_get(context, "object", ::BIIndexType::Plain, key, &result));
        CHECK(-EIO == ::cls_rgw_bi_get(context, "object", ::BIIndexType::Plain, key, nullptr));
        CHECK(request_method == "bi_get");
        CHECK(result.type == ::BIIndexType::Invalid);
        CHECK(result.idx == "preserve");
        CHECK(result.data.to_str() == "owned");
    }
    transport_status = -EAGAIN;
    CHECK(-EAGAIN == ::cls_rgw_bi_get(context, "object", ::BIIndexType::Plain, key, nullptr));
    CHECK(-EAGAIN == ::cls_rgw_bi_get(context, "object", ::BIIndexType::Plain, key, &result));
    allocation_failure = true;
    CHECK_THROWS_AS(::cls_rgw_bi_get(context, "object", ::BIIndexType::Plain, key, &result), std::bad_alloc);
    allocation_failure = false;
    transport_status = 0;
    CHECK(result.type == ::BIIndexType::Invalid);
    CHECK(result.idx == "preserve");
    CHECK(result.data.to_str() == "owned");
}

TEST_CASE("Index prepare and complete builders borrow arguments with native wire bytes", "[rgw][cls][write]")
{
    for (const unsigned length : {0u, 3u, 80u}) {
        const ::cls_rgw_obj_key key {std::string(length, 'n'), std::string(length, 'i')};
        const std::string tag(length, 't');
        const std::string locator {"a\0b", 3};
        for (const auto kind : {::CLS_RGW_OP_ADD, ::CLS_RGW_OP_DEL, ::CLS_RGW_OP_CANCEL,
                               ::CLS_RGW_OP_UNKNOWN, ::CLS_RGW_OP_LINK_OLH}) {
            ::rgw_cls_obj_prepare_op reference;
            reference.op = kind;
            reference.key = key;
            reference.tag = tag;
            reference.locator = locator;
            byte_buffer expected;
            ::encode(reference, expected);
            ::librados::ObjectWriteOperation operation;
            ::cls_rgw_bucket_prepare_op(operation, kind, tag, key, locator);
            CHECK(request_method == "bucket_prepare_op");
            CHECK(request_bytes.contents_equal(expected));
            ::rgw_cls_obj_prepare_op decoded;
            auto cursor = request_bytes.cbegin();
            ::decode(decoded, cursor);
            CHECK(decoded.key == key);
            CHECK(decoded.tag == tag);
            CHECK_FALSE(decoded.log_op);
            CHECK(decoded.bilog_flags == 0);
            CHECK(decoded.zones_trace.entries.empty());
            CHECK(cursor.end());
        }
    }

    for (const auto epoch : {0ull, 127ull, 128ull, 256ull, 65536ull,
                              std::numeric_limits<unsigned long long>::max()}) {
        for (const bool populated : {false, true}) {
            byte_buffer expected;
            {
                ::rgw_cls_obj_complete_op reference;
                reference.op = ::CLS_RGW_OP_ADD;
                reference.key = {std::string(80, 'n'), std::string(32, 'i')};
                reference.op_tag = std::string(32, 't');
                reference.locator = std::string {"a\0b", 3};
                reference.ver.pool = populated ? -1 : 128;
                reference.ver.epoch = epoch;
                reference.log_op = populated;
                reference.bilog_flags = 0xABCD;
                reference.meta.category = ::RGWObjCategory::Main;
                reference.meta.size = 65536;
                reference.meta.accounted_size = 12345;
                reference.meta.mtime = ::ceph::real_time {std::chrono::seconds {123} + std::chrono::nanoseconds {456}};
                reference.meta.restore_expiry_date = reference.meta.mtime;
                reference.meta.etag = std::string(32, 'e');
                reference.meta.owner = std::string(80, 'o');
                reference.meta.owner_display_name = std::string(80, 'd');
                reference.meta.content_type = "application/octet-stream";
                reference.meta.user_data = std::string {"u\0v", 3};
                reference.meta.storage_class = "STANDARD";
                reference.meta.appendable = true;
                reference.meta.restore_status = 255;
                if (populated) {
                    reference.remove_objs.push_back({std::string(80, 'r'), std::string(32, 'v')});
                    reference.remove_objs.push_back({"second", ""});
                    reference.zones_trace.insert(std::string(36, 'z'), std::string(80, 'l'));
                    reference.zones_trace.insert("other", std::nullopt);
                }
                ::encode(reference, expected);
                ::librados::ObjectWriteOperation operation;
                ::cls_rgw_bucket_complete_op(operation, reference.op, reference.op_tag, reference.ver,
                    reference.key, reference.meta, populated ? &reference.remove_objs : nullptr,
                    reference.log_op, reference.bilog_flags, populated ? &reference.zones_trace : nullptr,
                    reference.locator);
            }
            CHECK(request_method == "bucket_complete_op");
            CHECK(request_bytes.contents_equal(expected));
            ::rgw_cls_obj_complete_op decoded;
            auto cursor = request_bytes.cbegin();
            ::decode(decoded, cursor);
            // The native packed codec truncates 65536 to uint16 at its <= boundary.
            // Borrowing must preserve its wire behavior rather than change the codec:
            CHECK(decoded.ver.epoch == (65536 == epoch ? 0 : epoch));
            CHECK(decoded.bilog_flags == 0xABCD);
            CHECK(decoded.meta.owner == std::string(80, 'o'));
            CHECK(cursor.end());
        }
    }
}

TEST_CASE("OLH link and trim builders own encoded bytes from borrowed fragmented arguments", "[rgw][cls][write]")
{
    for (const unsigned length : {0u, 3u, 32u, 4096u}) {
        for (const bool populated : {false, true}) {
            byte_buffer expected;
            byte_buffer receipt;
            {
                ::rgw_cls_link_olh_op reference;
                reference.key = {std::string {"n\0m", 3}, std::string(32, 'i')};
                reference.olh_tag = std::string(length, 't');
                reference.op_tag = std::string(32, 'o');
                reference.delete_marker = populated;
                reference.log_op = populated;
                reference.high_precision_time = populated;
                reference.olh_epoch = std::numeric_limits<std::uint64_t>::max();
                reference.unmod_since = ::ceph::real_time {std::chrono::seconds {123} + std::chrono::nanoseconds {456}};
                if (populated) {
                    reference.meta.etag = std::string(32, 'e');
                    reference.meta.owner = std::string(80, 'o');
                    reference.meta.restore_status = 255;
                    reference.zones_trace.insert(std::string(36, 'z'), std::string(80, 'l'));
                }
                ::encode(reference, expected);
                byte_buffer tag;
                for (const char c : reference.olh_tag) {
                    tag.append(::ceph::buffer::copy(&c, 1));
                }
                ::librados::ObjectWriteOperation operation;
                ::cls_rgw_bucket_link_olh(operation, reference.key, tag, reference.delete_marker,
                    reference.op_tag, populated ? &reference.meta : nullptr, reference.olh_epoch,
                    reference.unmod_since, reference.high_precision_time, reference.log_op,
                    reference.zones_trace, populated ? &receipt : nullptr);
            }
            CHECK(request_method == "bucket_link_olh");
            CHECK(request_output == (populated ? &receipt : nullptr));
            CHECK(request_bytes.contents_equal(expected));
            ::rgw_cls_link_olh_op decoded;
            auto cursor = request_bytes.cbegin();
            ::decode(decoded, cursor);
            CHECK(decoded.olh_tag == std::string(length, 't'));
            CHECK(decoded.bilog_flags == 0);
            CHECK(cursor.end());

            ::rgw_cls_trim_olh_log_op reference;
            reference.olh = decoded.key;
            reference.ver = decoded.olh_epoch;
            reference.olh_tag = decoded.olh_tag;
            expected.clear();
            ::encode(reference, expected);
            ::librados::ObjectWriteOperation operation;
            ::cls_rgw_trim_olh_log(operation, reference.olh, reference.ver, reference.olh_tag);
            CHECK(request_method == "bucket_trim_olh_log");
            CHECK(request_bytes.contents_equal(expected));
        }
    }
}
