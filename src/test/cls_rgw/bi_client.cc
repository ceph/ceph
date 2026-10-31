// RGW CLS: Execute the actual client through a controlled transport boundary.
// This checks request bytes and atomic publication without a live OSD.
#include "cls/rgw/cls_rgw_client.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>

namespace {

using byte_buffer = ::ceph::buffer::list;

byte_buffer request_bytes;
byte_buffer reply_bytes;
int transport_status = 0;
bool allocation_failure = false;
std::string request_method;
int put_transport_status = 0;
std::vector<int> write_statuses;
std::size_t write_next = 0;
bool assert_pending = false;
bool captured_assert = false;
byte_buffer *request_output = nullptr;
std::unique_ptr<::librados::ObjectOperationCompletion> header_completion;
int submission_status = 0;

byte_buffer response(const bool compact = false)
{
    ::rgw_cls_bi_list_ret value;
    value.entries.push_back({::BIIndexType::Plain, "row", {}});
    value.is_truncated = true;
    byte_buffer bytes;
    if (compact) {
        ENCODE_START(2, 2, bytes);
        ::ceph::encode(std::uint32_t {1}, bytes);
        ::ceph::encode(static_cast<std::uint8_t>(value.entries.front().type), bytes);
        ::ceph::encode(value.entries.front().idx, bytes);
        ::ceph::encode(value.entries.front().data, bytes);
        ::ceph::encode(value.is_truncated, bytes);
        ENCODE_FINISH(bytes);

        return bytes;
    }
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

TEST_CASE("BI client advertises compact replies and accepts native fallback for both outputs", "[rgw][cls][datum]")
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
    const char version = 3;
    expected.begin().copy_in(1, &version);
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

    reply_bytes = response(true);
    truncated = false;
    CHECK(0 == ::cls_rgw_bi_list(context, "object", request.name_filter, request.marker,
                               request.max, entries, truncated, true));
    REQUIRE(entries.size() == 1);
    CHECK(entries.front().idx == "row");
    CHECK(truncated);
    CHECK(0 == ::cls_rgw_bi_list(context, "object", request.name_filter, request.marker,
                               request.max, &compatible, &truncated, true));
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

TEST_CASE("BI compact reply failures preserve both output interfaces", "[rgw][cls][datum]")
{
    ::librados::IoCtx context;
    transport_status = 0;
    allocation_failure = false;
    const auto full = response(true);
    for (unsigned size = 0; size < full.length(); ++size) {
        std::vector<::rgw_cls_bi_entry> entries {{::BIIndexType::Invalid, "preserve", {}}};
        std::list<::rgw_cls_bi_entry> compatible {{::BIIndexType::Invalid, "preserve", {}}};
        bool truncated = false;
        reply_bytes.clear();
        reply_bytes.substr_of(full, 0, size);
        CHECK(-EIO == ::cls_rgw_bi_list(context, "object", "", "", 9, entries, truncated));
        CHECK(-EIO == ::cls_rgw_bi_list(context, "object", "", "", 9, &compatible, &truncated));
        REQUIRE(entries.size() == 1);
        REQUIRE(compatible.size() == 1);
        CHECK(entries.front().idx == "preserve");
        CHECK(compatible.front().idx == "preserve");
        CHECK_FALSE(truncated);
    }
}

void librados::ObjectOperation::exec_impl(const char *cls, const char *method, byte_buffer& input)
{
    captured_assert = std::exchange(assert_pending, false);
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

    if (!write_statuses.empty()) {
        return write_statuses.at(write_next++);
    }

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
        for (unsigned controls = 0; controls < 8; ++controls) {
            const bool removals = controls & 1;
            const bool zones = controls & 2;
            byte_buffer expected;
            {
                ::rgw_cls_obj_complete_op reference;
                reference.op = ::CLS_RGW_OP_ADD;
                reference.key = {std::string(80, 'n'), std::string(32, 'i')};
                reference.op_tag = std::string(32, 't');
                reference.locator = std::string {"a\0b", 3};
                reference.ver.pool = zones ? -1 : 128;
                reference.ver.epoch = epoch;
                reference.log_op = controls & 4;
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
                if (removals) {
                    reference.remove_objs.push_back({std::string(80, 'r'), std::string(32, 'v')});
                    reference.remove_objs.push_back({"second", ""});
                }
                if (zones) {
                    reference.zones_trace.insert(std::string(36, 'z'), std::string(80, 'l'));
                    reference.zones_trace.insert("other", std::nullopt);
                }
                ::encode(reference, expected);
                ::librados::ObjectWriteOperation operation;
                ::cls_rgw_bucket_complete_op(operation, reference.op, reference.op_tag, reference.ver,
                    reference.key, reference.meta, removals ? &reference.remove_objs : nullptr,
                    reference.log_op, reference.bilog_flags,
                    zones ? &reference.zones_trace : nullptr,
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
        for (unsigned controls = 0; controls < 16; ++controls) {
            const bool populated = controls & 8;
            byte_buffer expected;
            byte_buffer receipt;
            {
                ::rgw_cls_link_olh_op reference;
                reference.key = {std::string {"n\0m", 3}, std::string(32, 'i')};
                reference.olh_tag = std::string(length, 't');
                reference.op_tag = std::string(32, 'o');
                reference.delete_marker = controls & 1;
                reference.log_op = controls & 4;
                reference.high_precision_time = controls & 2;
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
                tag.zero(); // A string field must not retain mutable input bytes.
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

TEST_CASE("OLH unlink clear and refresh builders preserve native bytes and receipt paths", "[rgw][cls][write]")
{
    for (const bool populated : {false, true}) {
        byte_buffer expected;
        byte_buffer receipt;
        {
            ::rgw_cls_unlink_instance_op reference;
            reference.key = {std::string {"n\0m", 3}, std::string(80, 'i')};
            reference.op_tag = std::string(80, 't');
            reference.olh_tag = std::string {"o\0h", 3};
            reference.olh_epoch = std::numeric_limits<std::uint64_t>::max();
            reference.log_op = populated;
            reference.bilog_flags = 0xFEDC;
            if (populated) { reference.zones_trace.insert("zone", "location"); }
            ::encode(reference, expected);
            ::librados::ObjectWriteOperation operation;
            ::cls_rgw_bucket_unlink_instance(operation, reference.key, reference.op_tag,
                reference.olh_tag, reference.olh_epoch, reference.log_op, reference.bilog_flags,
                reference.zones_trace, populated ? &receipt : nullptr);
        }
        CHECK(request_method == "bucket_unlink_instance");
        CHECK(request_output == (populated ? &receipt : nullptr));
        CHECK(request_bytes.contents_equal(expected));
    }

    ::cls_rgw_obj_key key {"object", "instance"};
    ::rgw_cls_refresh_instance_op refresh;
    refresh.key = key;
    byte_buffer expected;
    ::encode(refresh, expected);
    ::librados::ObjectWriteOperation operation;
    ::cls_rgw_bucket_refresh_instance(operation, key);
    CHECK(request_method == "bucket_refresh_instance");
    CHECK(request_bytes.contents_equal(expected));
    ::rgw_cls_bucket_clear_olh_op clear;
    clear.key.name = "object";
    clear.olh_tag = std::string {"t\0g", 3};
    expected.clear();
    ::encode(clear, expected);
    ::cls_rgw_clear_olh(operation, clear.key, clear.olh_tag);
    CHECK(request_method == "bucket_clear_olh");
    CHECK(request_bytes.contents_equal(expected));

    ::librados::IoCtx context;
    std::string oid = "object";
    const ::rgw_zone_set zones;
    for (const int status : {-EAGAIN, 0, 7}) {
        put_transport_status = status;
        CHECK(::cls_rgw_bucket_unlink_instance(context, oid, key, "op", "olh", 7, false, 0, zones) ==
              (status < 0 ? status : 0));
        CHECK(::cls_rgw_clear_olh(context, oid, clear.key, clear.olh_tag) == status);
    }
    put_transport_status = 0;
}

TEST_CASE("Usage client borrows input entries and publishes owned native map results", "[rgw][cls][usage]")
{
    allocation_failure = false;
    transport_status = 0;
    ::librados::ObjectWriteOperation operation;
    ::rgw_cls_usage_log_add_op add;
    auto& entry = add.info.entries.emplace_back();
    entry.owner.from_str("tenant$ns$id");
    entry.bucket = std::string(80, 'b');
    entry.epoch = std::numeric_limits<std::uint64_t>::max();
    entry.add_usage("category", {});
    byte_buffer expected;
    ::encode(add, expected);
    ::cls_rgw_usage_log_add(operation, add.info);
    add.info.entries.clear();
    CHECK(request_method == "user_usage_log_add");
    CHECK(request_bytes.contents_equal(expected));

    ::rgw_cls_usage_log_read_ret response;
    response.usage.emplace(::rgw_user_bucket {"user", "bucket"}, ::rgw_usage_log_entry {});
    response.usage.begin()->second.bucket = "result";
    response.truncated = true;
    response.next_iter = std::string(80, 'm');
    reply_bytes.clear();
    ::encode(response, reply_bytes);
    ::librados::IoCtx context;
    for (const int result : {0, 7}) {
        transport_status = result;
        ::rgw_cls_usage_log_read_op call;
        call.start_epoch = 17;
        call.end_epoch = 19;
        call.owner = "user";
        call.bucket = std::string {"b\0t", 3};
        call.max_entries = 23;
        call.iter = std::string {"m\0r", 3};
        expected.clear();
        ::encode(call, expected);
        std::map<::rgw_user_bucket, ::rgw_usage_log_entry> usage;
        auto marker = call.iter;
        bool truncated = false;
        CHECK(::cls_rgw_usage_log_read(context, "object", call.owner, call.bucket, 17, 19, 23, marker, usage, &truncated) == 0);
        CHECK(request_method == "user_usage_log_read");
        CHECK(request_bytes.contents_equal(expected));
        CHECK(marker == response.next_iter);
        CHECK(truncated);
        REQUIRE(usage.size() == 1);
        CHECK(usage.begin()->second.bucket == "result");
        CHECK(::cls_rgw_usage_log_read(context, "object", marker, marker, 17, 19, 23, marker, usage, nullptr) == 0);
    }
    transport_status = 0;
}

TEST_CASE("Usage client preserves its malformed, transport and allocation error contracts", "[rgw][cls][usage]")
{
    ::librados::IoCtx context;
    ::rgw_cls_usage_log_read_ret response;
    response.truncated = true;
    response.next_iter = "next";
    response.usage[::rgw_user_bucket {"user", "bucket"}].bucket = "result";
    byte_buffer full;
    ::encode(response, full);
    std::map<::rgw_user_bucket, ::rgw_usage_log_entry> usage;
    usage[::rgw_user_bucket {"saved", "bucket"}].bucket = "preserved";
    std::string marker = "saved";
    bool truncated = true;
    allocation_failure = false;
    transport_status = 0;
    for (unsigned size = 0; size < full.length(); ++size) {
        reply_bytes.clear();
        reply_bytes.substr_of(full, 0, size);
        CHECK(::cls_rgw_usage_log_read(context, "object", "user", "", 0, 19, 23, marker, usage, &truncated) == -EINVAL);
        CHECK(marker == "saved");
        CHECK_FALSE(truncated);
        REQUIRE(usage.size() == 1);
        CHECK(usage.begin()->second.bucket == "preserved");
    }
    for (const int error : {-EAGAIN, -ENOMEM, -ENOENT}) {
        transport_status = error;
        truncated = true;
        CHECK(::cls_rgw_usage_log_read(context, "object", "user", "", 0, 19, 23, marker, usage, &truncated) == error);
        CHECK_FALSE(truncated);
        CHECK(marker == "saved");
    }
    allocation_failure = true;
    CHECK_THROWS_AS(::cls_rgw_usage_log_read(context, "object", "user", "", 0, 19, 23, marker, usage, &truncated), std::bad_alloc);
    allocation_failure = false;
    transport_status = 0;
}

TEST_CASE("Usage trim builders retain bytes and synchronous ENODATA completion", "[rgw][cls][usage]")
{
    ::rgw_cls_usage_log_trim_op call;
    call.start_epoch = 17;
    call.end_epoch = std::numeric_limits<std::uint64_t>::max();
    call.user = std::string {"u\0r", 3};
    call.bucket = std::string(80, 'b');
    byte_buffer expected;
    ::encode(call, expected);
    ::librados::ObjectWriteOperation operation;
    ::cls_rgw_usage_log_trim(operation, call.user, call.bucket, call.start_epoch, call.end_epoch);
    CHECK(request_method == "user_usage_log_trim");
    CHECK(request_bytes.contents_equal(expected));
    for (const auto& statuses : std::vector<std::vector<int>> {{-ENODATA}, {0, 0, -ENODATA}, {-EIO}, {0, -EAGAIN}}) {
        write_statuses = statuses;
        write_next = 0;
        ::librados::IoCtx context;
        CHECK(::cls_rgw_usage_log_trim(context, "object", call.user, call.bucket, call.start_epoch, call.end_epoch) == (statuses.back() == -ENODATA ? 0 : statuses.back()));
        CHECK(write_next == statuses.size());
        CHECK(request_method == "user_usage_log_trim");
        CHECK(request_bytes.contents_equal(expected));
        write_statuses.clear();
    }
    ::cls_rgw_usage_log_clear(operation);
    CHECK(request_method == "usage_log_clear");
    CHECK(request_bytes.length() == 0);
}

void librados::ObjectOperation::assert_exists() { assert_pending = true; }

TEST_CASE("Reshard builders borrow native leaves and retain assert-exists ordering", "[rgw][cls][reshard]")
{
    ::librados::ObjectWriteOperation operation;
    ::cls_rgw_reshard_add_op add;
    add.entry.time = ::ceph::real_time {std::chrono::seconds {123}};
    add.entry.tenant = std::string {"t\0t", 3};
    add.entry.bucket_name = std::string(80, 'b');
    add.entry.bucket_id = "id";
    add.entry.old_num_shards = 3;
    add.entry.new_num_shards = 7;
    add.entry.initiator = ::cls_rgw_reshard_initiator::Dynamic;
    add.create_only = true;
    byte_buffer expected;
    ::encode(add, expected);
    ::cls_rgw_reshard_add(operation, add.entry, true);
    CHECK(request_method == "reshard_add");
    CHECK(request_bytes.contents_equal(expected));
    ::cls_rgw_reshard_remove_op remove;
    remove.tenant = add.entry.tenant;
    remove.bucket_name = add.entry.bucket_name;
    remove.bucket_id = add.entry.bucket_id;
    expected.clear();
    ::encode(remove, expected);
    ::cls_rgw_reshard_remove(operation, add.entry);
    add.entry = {};
    CHECK(request_method == "reshard_remove");
    CHECK(request_bytes.contents_equal(expected));
    for (const unsigned status : {0, 1, 2, 3, 255}) {
        ::cls_rgw_set_bucket_resharding_op set;
        set.entry.reshard_status = static_cast<::cls_rgw_reshard_status>(status);
        expected.clear();
        ::encode(set, expected);
        ::cls_rgw_set_bucket_resharding(operation, set.entry.reshard_status);
        CHECK(captured_assert);
        CHECK_FALSE(assert_pending);
        CHECK(request_method == "set_bucket_resharding");
        CHECK(request_bytes.contents_equal(expected));
    }
    ::cls_rgw_clear_bucket_resharding(operation);
    CHECK_FALSE(captured_assert);
    expected.clear();
    ::encode(::cls_rgw_clear_bucket_resharding_op {}, expected);
    CHECK(request_method == "clear_bucket_resharding");
    CHECK(request_bytes.contents_equal(expected));
    for (const int error : {std::numeric_limits<int>::min(), -2300, -1, 0, 7, std::numeric_limits<int>::max()}) {
        expected.clear();
        ::encode(::cls_rgw_guard_bucket_resharding_op {error}, expected);
        ::cls_rgw_guard_bucket_resharding(operation, error);
        CHECK(request_method == "guard_bucket_resharding");
        CHECK(request_bytes.contents_equal(expected));
    }
    ::librados::ObjectReadOperation read;
    byte_buffer reply;
    ::cls_rgw_get_bucket_resharding(read, reply);
    expected.clear();
    ::encode(::cls_rgw_get_bucket_resharding_op {}, expected);
    CHECK(request_method == "get_bucket_resharding");
    CHECK(request_output == &reply);
    CHECK(request_bytes.contents_equal(expected));
    ::cls_rgw_get_bucket_resharding_ret returned;
    returned.new_instance.reshard_status = ::cls_rgw_reshard_status::IN_PROGRESS;
    reply.clear();
    ::encode(returned, reply);
    ::cls_rgw_bucket_instance_entry output;
    ::cls_rgw_get_bucket_resharding_decode(reply, output);
    CHECK(output.reshard_status == ::cls_rgw_reshard_status::IN_PROGRESS);
    for (unsigned size = 0; size < reply.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(reply, 0, size);
        CHECK_THROWS_AS(::cls_rgw_get_bucket_resharding_decode(prefix, output), ::ceph::buffer::error);
        CHECK(output.reshard_status == ::cls_rgw_reshard_status::IN_PROGRESS);
    }
}

TEST_CASE("Reshard clients retain list interfaces, markers and transport/error contracts", "[rgw][cls][reshard]")
{
    ::librados::IoCtx context;
    ::cls_rgw_reshard_entry row;
    row.tenant = "tenant";
    row.bucket_name = std::string(80, 'b');
    row.bucket_id = "id";
    row.initiator = ::cls_rgw_reshard_initiator::Dynamic;
    ::cls_rgw_reshard_list_ret listed;
    listed.entries.push_back(row);
    listed.is_truncated = true;
    reply_bytes.clear();
    ::encode(listed, reply_bytes);
    const auto full_list = reply_bytes;
    std::string marker = "marker";
    std::list<::cls_rgw_reshard_entry> entries;
    bool truncated = false;
    allocation_failure = false;
    for (const int status : {0, 7}) {
        transport_status = status;
        CHECK(::cls_rgw_reshard_list(context, "object", marker, 17, entries, &truncated) == 0);
        CHECK(request_method == "reshard_list");
        CHECK(marker == "marker");
        CHECK(truncated);
        REQUIRE(entries.size() == 1);
        CHECK(entries.front().bucket_name == row.bucket_name);
        ::cls_rgw_reshard_list_op request;
        request.marker = marker;
        request.max = 17;
        byte_buffer expected;
        ::encode(request, expected);
        CHECK(request_bytes.contents_equal(expected));
    }
    transport_status = 0;
    for (unsigned size = 0; size < full_list.length(); ++size) {
        reply_bytes.clear();
        reply_bytes.substr_of(full_list, 0, size);
        CHECK(::cls_rgw_reshard_list(context, "object", marker, 17, entries, &truncated) == -EIO);
        CHECK(marker == "marker");
        CHECK(truncated);
        REQUIRE(entries.size() == 1);
        CHECK(entries.front().bucket_name == row.bucket_name);
    }
    ::cls_rgw_reshard_get_ret got;
    got.entry = row;
    reply_bytes.clear();
    ::encode(got, reply_bytes);
    const auto full_get = reply_bytes;
    ::cls_rgw_reshard_entry output;
    output.tenant = "request";
    output.bucket_name = "bucket";
    ::cls_rgw_reshard_get_op expected_get;
    expected_get.entry = output;
    byte_buffer expected;
    ::encode(expected_get, expected);
    CHECK(::cls_rgw_reshard_get(context, "object", output) == 0);
    CHECK(request_method == "reshard_get");
    CHECK(request_bytes.contents_equal(expected));
    CHECK(output.bucket_name == row.bucket_name);
    for (unsigned size = 0; size < full_get.length(); ++size) {
        reply_bytes.clear();
        reply_bytes.substr_of(full_get, 0, size);
        CHECK(::cls_rgw_reshard_get(context, "object", output) == -EIO);
        CHECK(output.bucket_name == row.bucket_name);
    }
    transport_status = -EAGAIN;
    CHECK(::cls_rgw_reshard_get(context, "object", output) == -EAGAIN);
    CHECK(::cls_rgw_reshard_list(context, "object", marker, 17, entries, &truncated) == -EAGAIN);
    allocation_failure = true;
    CHECK_THROWS_AS(::cls_rgw_reshard_get(context, "object", output), std::bad_alloc);
    CHECK_THROWS_AS(::cls_rgw_reshard_list(context, "object", marker, 17, entries, &truncated), std::bad_alloc);
    allocation_failure = false;
    transport_status = 0;
}

TEST_CASE("GC clients borrow chains and tags while retaining native reply publication", "[rgw][cls][gc]")
{
    ::librados::ObjectWriteOperation write;
    ::cls_rgw_gc_set_entry_op set;
    set.expiration_secs = std::numeric_limits<std::uint32_t>::max();
    set.info.tag = std::string(80, 't');
    set.info.time = ::ceph::real_time {std::chrono::seconds {123}};
    set.info.chain.push_obj(std::string(80, 'p'), {std::string(80, 'o'), "instance"}, std::string(80, 'l'));
    byte_buffer expected;
    ::encode(set, expected);
    ::cls_rgw_gc_set_entry(write, set.expiration_secs, set.info);
    CHECK(request_method == "gc_set_entry");
    set.info.tag.clear();
    set.info.chain.objs.clear();
    CHECK(request_bytes.contents_equal(expected));
    ::cls_rgw_gc_defer_entry_op defer;
    defer.expiration_secs = 17;
    defer.tag = std::string {"t\0g", 3};
    expected.clear();
    ::encode(defer, expected);
    ::cls_rgw_gc_defer_entry(write, 17, defer.tag);
    defer.tag.clear();
    CHECK(request_method == "gc_defer_entry");
    CHECK(request_bytes.contents_equal(expected));
    ::cls_rgw_gc_remove_op remove;
    remove.tags = {"", "a", std::string {"t\0g", 3}, "a"};
    expected.clear();
    ::encode(remove, expected);
    ::cls_rgw_gc_remove(write, remove.tags);
    remove.tags.clear();
    CHECK(request_method == "gc_remove");
    CHECK(request_bytes.contents_equal(expected));
    ::librados::ObjectReadOperation read;
    byte_buffer output;
    ::cls_rgw_gc_list_op list;
    list.marker = std::string {"m\0r", 3};
    list.max = 19;
    list.expired_only = false;
    expected.clear();
    ::encode(list, expected);
    ::cls_rgw_gc_list(read, list.marker, 19, false, output);
    list.marker.clear();
    CHECK(request_method == "gc_list");
    CHECK(request_output == &output);
    CHECK(request_bytes.contents_equal(expected));
    set.info.chain.push_obj("pool", {"object", "instance"}, "location");
    ::cls_rgw_gc_list_ret reply;
    reply.entries.push_back(set.info);
    reply.entries.back().tag = "result";
    reply.truncated = true;
    reply.next_marker = std::string(80, 'm');
    output.clear();
    ::encode(reply, output);
    std::list<::cls_rgw_gc_obj_info> entries;
    bool truncated = false;
    std::string marker = "saved";
    REQUIRE(::cls_rgw_gc_list_decode(output, entries, truncated, marker) == 0);
    REQUIRE(entries.size() == 1);
    CHECK(entries.front().tag == "result");
    REQUIRE(entries.front().chain.objs.size() == 1);
    CHECK(entries.front().chain.objs.front().key.name == "object");
    CHECK(truncated);
    CHECK(marker == reply.next_marker);
    for (unsigned size = 0; size < output.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(output, 0, size);
        CHECK(::cls_rgw_gc_list_decode(prefix, entries, truncated, marker) == -EIO);
        REQUIRE(entries.size() == 1);
        CHECK(entries.front().tag == "result");
        CHECK(truncated);
        CHECK(marker == reply.next_marker);
    }
}

TEST_CASE("Lifecycle entry retains copy semantics and transfers bucket ownership on moves", "[rgw][cls][lc]")
{
    static_assert(std::is_copy_constructible_v<::cls_rgw_lc_entry>);
    static_assert(std::is_copy_assignable_v<::cls_rgw_lc_entry>);
    static_assert(std::is_nothrow_move_constructible_v<::cls_rgw_lc_entry>);
    static_assert(std::is_nothrow_move_assignable_v<::cls_rgw_lc_entry>);
    ::cls_rgw_lc_entry original {std::string(4096, 'b'), 17, 23};
    const auto *storage = original.bucket.data();
    ::cls_rgw_lc_entry moved {std::move(original)};
    CHECK(moved.bucket.data() == storage);
    CHECK(moved.start_time == 17);
    CHECK(moved.status == 23);
    ::cls_rgw_lc_entry assigned;
    assigned = std::move(moved);
    CHECK(assigned.bucket.data() == storage);
    ::cls_rgw_lc_entry copied {assigned};
    CHECK(copied.bucket == assigned.bucket);
    CHECK(copied.bucket.data() != assigned.bucket.data());
    ::cls_rgw_lc_entry copy_assigned;
    copy_assigned = assigned;
    assigned.bucket.front() = 'x';
    CHECK(copy_assigned.bucket.front() == 'b');
    CHECK(copied.bucket.front() == 'b');
    CHECK(copy_assigned.start_time == 17);
    CHECK(copy_assigned.status == 23);
}

TEST_CASE("Lifecycle builders borrow marker entry and head data while preserving wire bytes", "[rgw][cls][lc]")
{
    ::librados::ObjectReadOperation read;
    ::librados::ObjectWriteOperation write;
    byte_buffer output, expected;
    ::cls_rgw_lc_entry value {std::string(80, 'b'), 17, 23};
    ::cls_rgw_lc_set_entry_op set;
    set.entry = value;
    ::encode(set, expected);
    ::cls_rgw_lc_set_entry(write, value);
    CHECK(request_method == "lc_set_entry");
    CHECK(request_bytes.contents_equal(expected));
    ::cls_rgw_lc_rm_entry(write, value);
    value.bucket.clear();
    CHECK(request_method == "lc_rm_entry");
    CHECK(request_bytes.contents_equal(expected));
    ::cls_rgw_lc_get_entry_op marker;
    marker.marker = std::string {"m\0r", 3};
    expected.clear();
    ::encode(marker, expected);
    ::cls_rgw_lc_get_entry(read, marker.marker, output);
    CHECK(request_method == "lc_get_entry");
    CHECK(request_bytes.contents_equal(expected));
    CHECK(request_output == &output);
    ::cls_rgw_lc_get_next_entry(read, marker.marker, output);
    marker.marker.clear();
    CHECK(request_method == "lc_get_next_entry");
    CHECK(request_bytes.contents_equal(expected));
    ::cls_rgw_lc_put_head_op head;
    head.head.start_date = 17;
    head.head.marker = std::string(80, 'm');
    head.head.shard_rollover_date = 19;
    expected.clear();
    ::encode(head, expected);
    ::cls_rgw_lc_put_head(write, head.head);
    head.head.marker.clear();
    CHECK(request_method == "lc_put_head");
    CHECK(request_bytes.contents_equal(expected));
    ::cls_rgw_lc_get_head(read, output);
    CHECK(request_method == "lc_get_head");
    CHECK(request_bytes.length() == 0);
    ::cls_rgw_lc_list_entries_op list;
    list.marker = std::string(80, 'm');
    list.max_entries = 23;
    expected.clear();
    ::encode(list, expected);
    ::cls_rgw_lc_list(read, list.marker, 23, output);
    list.marker.clear();
    CHECK(request_method == "lc_list_entries");
    CHECK(request_bytes.contents_equal(expected));
}

TEST_CASE("Lifecycle clients retain ambiguous v1 entry replies and distinct next-entry compatibility", "[rgw][cls][lc]")
{
    for (const bool native_leaf : {false, true}) {
        byte_buffer input;
        ENCODE_START(1, 1, input);
        if (native_leaf) { ::encode(::cls_rgw_lc_entry {std::string(80, 'b'), 17, 23}, input); }
        if (!native_leaf) { ::ceph::encode(std::pair {std::string(80, 'b'), -17}, input); }
        ENCODE_FINISH(input);
        ::cls_rgw_lc_entry value;
        CHECK(::cls_rgw_lc_get_entry_decode(input, value) == 0);
        CHECK(value.bucket == std::string(80, 'b'));
        CHECK(value.start_time == (native_leaf ? 17 : 0));
        CHECK(value.status == (native_leaf ? 23 : static_cast<std::uint32_t>(-17)));
        CHECK(::cls_rgw_lc_get_next_entry_decode(input, value) == (native_leaf ? -EIO : 0));
    }
    ::cls_rgw_lc_get_entry_ret response;
    response.entry = {std::string(80, 'b'), 17, 23};
    byte_buffer full;
    ::encode(response, full);
    ::cls_rgw_lc_entry value {"saved", 29, 31};
    for (unsigned size = 0; size < full.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(full, 0, size);
        CHECK(::cls_rgw_lc_get_entry_decode(prefix, value) == -EIO);
        CHECK(::cls_rgw_lc_get_next_entry_decode(prefix, value) == -EIO);
        CHECK(value.bucket == "saved");
        CHECK(value.start_time == 29);
        CHECK(value.status == 31);
    }
    CHECK(::cls_rgw_lc_get_entry_decode(full, value) == 0);
    CHECK(value.bucket == response.entry.bucket);
}

TEST_CASE("Lifecycle list and head clients retain sorting and malformed-output preservation", "[rgw][cls][lc]")
{
    for (const std::uint8_t version : {1, 2, 3}) {
        ::cls_rgw_lc_list_entries_ret response {version};
        response.entries = {{"z", 17, 11}, {"a", 19, 13}, {"z", 23, 99}};
        response.is_truncated = true;
        byte_buffer input;
        ::encode(response, input);
        std::vector<::cls_rgw_lc_entry> entries;
        REQUIRE(::cls_rgw_lc_list_decode(input, entries) == 0);
        REQUIRE(entries.size() == (version < 3 ? 2 : 3));
        CHECK(entries.front().bucket == "a");
        CHECK(entries.back().bucket == "z");
        if (version < 3) { CHECK(entries.back().status == 11); }
        for (unsigned size = 0; size < input.length(); ++size) {
            byte_buffer prefix;
            prefix.substr_of(input, 0, size);
            CHECK(::cls_rgw_lc_list_decode(prefix, entries) == -EIO);
            CHECK(entries.front().bucket == "a");
            CHECK(entries.back().bucket == "z");
        }
    }
    ::cls_rgw_lc_list_entries_ret unsorted;
    std::map<std::string, std::pair<std::uint64_t, std::uint32_t>> expected;
    for (unsigned index = 0; index < 100; ++index) {
        const auto bucket = std::to_string(100 - index) + std::string(80, 'b');
        unsorted.entries.emplace_back(bucket, 17 + index, 23 + index);
        expected.emplace(bucket, std::pair {17 + index, 23 + index});
    }
    byte_buffer encoded;
    ::encode(unsorted, encoded);
    std::vector<::cls_rgw_lc_entry> sorted;
    REQUIRE(::cls_rgw_lc_list_decode(encoded, sorted) == 0);
    REQUIRE(sorted.size() == expected.size());
    auto found = expected.begin();
    for (const auto& value : sorted) {
        CHECK(value.bucket == found->first);
        CHECK(value.start_time == found->second.first);
        CHECK(value.status == found->second.second);
        ++found;
    }
    ::cls_rgw_lc_get_head_ret response;
    response.head.start_date = 17;
    response.head.marker = std::string(80, 'm');
    response.head.shard_rollover_date = 19;
    byte_buffer input;
    ::encode(response, input);
    ::cls_rgw_lc_obj_head head;
    REQUIRE(::cls_rgw_lc_get_head_decode(input, head) == 0);
    CHECK(head.marker == response.head.marker);
    for (unsigned size = 0; size < input.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(input, 0, size);
        CHECK(::cls_rgw_lc_get_head_decode(prefix, head) == -EIO);
        CHECK(head.marker == response.head.marker);
        CHECK(head.start_date == 17);
        CHECK(head.shard_rollover_date == 19);
    }
}

TEST_CASE("Multipart client preserves native manifest normalization and owns encoded bytes", "[rgw][cls][multipart]")
{
    ::RGWUploadPartInfo info;
    info.num = 17;
    info.size = 4096;
    info.accounted_size = 8192;
    info.etag = std::string(100, 'e');
    ::rgw_placement_rule placement;
    placement.name = "head-placement";
    info.manifest.set_head(placement, {}, 31);
    info.manifest.set_trivial_rule(31, 4096);
    info.manifest.set_prefix("current");
    info.cs_info.compression_type = "zstd";
    info.cs_info.blocks = {{0, 0, 4096}};
    info.past_prefixes = {std::string(100, 'p'), std::string {"a\0b", 3}};
    info.crypt_salt = std::string(100, 's');
    std::string key {"p\0t", 3};
    ::cls_rgw_mp_upload_part_info_update_op expected;
    expected.part_key = key;
    expected.info = info;
    byte_buffer encoded;
    ::encode(expected, encoded);
    ::librados::ObjectWriteOperation operation;
    ::cls_rgw_mp_upload_part_info_update(operation, key, info);
    CHECK(request_method == "mp_upload_part_info_update");
    CHECK(request_bytes.contents_equal(encoded));
    CHECK(info.manifest.get_head_placement_rule().name == "head-placement");
    CHECK(info.past_prefixes.size() == 2);
    key.assign(100, 'x');
    info.etag.clear();
    info.cs_info.blocks.clear();
    info.past_prefixes.clear();
    info.crypt_salt.clear();
    CHECK(request_bytes.contents_equal(encoded));
}

void librados::ObjectOperation::exec_impl(const char *cls, const char *method,
                                          byte_buffer& input, ObjectOperationCompletion *completion)
{
    exec_impl(cls, method, input);
    REQUIRE_FALSE(header_completion);
    header_completion.reset(completion);
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *,
                                ObjectReadOperation *, byte_buffer *output)
{
    CHECK(oid == "object");
    CHECK(output == nullptr);

    return submission_status;
}

TEST_CASE("Directory header client reuses list requests and preserves native callback results", "[rgw][cls][bucket-list]")
{
    struct header_callback final : ::RGWGetDirHeader_CB
    {
        unsigned calls = 0;
        int status = 123;
        ::rgw_bucket_dir_header header;

        void handle_response(const int result, const ::rgw_bucket_dir_header& value) override
        {
            ++calls;
            status = result;
            header = value;
        }

    };
    ::rgw_cls_list_op request;
    byte_buffer expected;
    ::encode(request, expected);
    ::rgw_cls_list_ret reply;
    reply.dir.header.ver = 41;
    reply.dir.header.tag_timeout = 23;
    reply.marker = {"tail", "version"};
    byte_buffer bytes;
    ::encode(reply, bytes);
    byte_buffer prefix;
    ::encode(reply.dir, prefix);
    ::ceph::encode(reply.is_truncated, prefix);
    ::librados::IoCtx context;
    submission_status = 0;
    for (const auto [status, malformed] : {std::pair {0, false}, std::pair {-EAGAIN, false},
                                          std::pair {0, true}}) {
        boost::intrusive_ptr<header_callback> callback {new header_callback};
        CHECK(0 == ::cls_rgw_get_dir_header_async(context, "object", callback));
        CHECK(request_method == "bucket_list");
        CHECK(request_bytes.contents_equal(expected));
        CHECK(callback->calls == 0);
        REQUIRE(header_completion);
        auto response = bytes;
        if (malformed) {
            // Reject the trailing key's compatibility after the directory header decodes.
            const char incompatible = char(0xFF);
            response.begin(6 + prefix.length() + 1).copy_in(1, &incompatible);
        }
        auto completion = std::move(header_completion);
        completion->handle_completion(status, response);
        CHECK(callback->calls == 1);
        CHECK(callback->status == (malformed ? -EIO : status));
        CHECK(callback->header.ver == 41);
        CHECK(callback->header.tag_timeout == 23);
    }
    boost::intrusive_ptr<header_callback> callback {new header_callback};
    submission_status = -ECANCELED;
    CHECK(-ECANCELED == ::cls_rgw_get_dir_header_async(context, "object", callback));
    CHECK(callback->calls == 0);
    header_completion.reset();
    submission_status = 0;
}
