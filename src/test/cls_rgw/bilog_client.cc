// RGW CLS: Actual index-read builders and deferred completion contracts.
#include "test/cls_rgw/wire_test.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_const.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <new>
#include <utility>

#include <memory>
#include <string>
#include <string_view>

namespace {

using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;

std::unique_ptr<::librados::ObjectOperationCompletion> pending;
bytes request_bytes;
std::string request_method;
bool fail_allocation = false;

auto response()
{
    ::cls_rgw_bi_log_list_ret value;
    value.entries.emplace_back();
    auto& entry = value.entries.back();
    entry.id = std::string {"id\0x", 4};
    entry.object = std::string(80, 'o');
    entry.tag = std::string(80, 't');
    entry.op = ::CLS_RGW_OP_DEL;
    entry.state = ::CLS_RGW_STATE_COMPLETE;
    entry.zones_trace.insert("zone", "location");
    value.truncated = true;

    return value;
}

void schedule(::cls_rgw_bi_log_list_ret& output, int *status)
{
    ::librados::ObjectReadOperation operation;
    std::string marker {"m\0r", 3};
    ::cls_rgw_bilog_list(operation, marker, 17, &output, status);
    marker.assign(80, 'x');
}

void complete(const int status, bytes input, const bool allocation_failure = false)
{
    REQUIRE(pending != nullptr);
    auto callback = std::move(pending);
    fail_allocation = allocation_failure;
    callback->handle_completion(status, input);
    CHECK_FALSE(fail_allocation);
}

} // anonymous namespace

void *operator new(const std::size_t size)
{
    if (std::exchange(fail_allocation, false)) {
        throw std::bad_alloc {};
    }
    if (void *p = std::malloc(size ? size : 1)) {
        return p;
    }

    throw std::bad_alloc {};
}

void operator delete(void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }

void librados::ObjectOperation::exec_impl(const char *cls, const char *method,
                                         bytes& input, ObjectOperationCompletion *completion)
{
    CHECK(std::string_view {cls} == "rgw");
    REQUIRE(pending == nullptr);
    request_method = method;
    request_bytes = input;
    pending.reset(completion);
}

void librados::ObjectOperation::exec_impl(const char *cls, const char *method, bytes& input)
{
    CHECK(std::string_view {cls} == "rgw");
    request_method = method;
    request_bytes = input;
}

TEST_CASE("Bilog client builders borrow markers and retain deferred result status", "[rgw][cls][bilog]")
{
    for (const int result : {0, 7, ::RGWBIAdvanceAndRetryError}) {
        ::cls_rgw_bi_log_list_ret output;
        int status = 123;
        schedule(output, &status);
        CHECK(status == 123);
        CHECK(request_method == "bi_log_list");
        ::cls_rgw_bi_log_list_op expected;
        expected.marker = std::string {"m\0r", 3};
        expected.max = 17;
        CHECK(request_bytes.contents_equal(canonical(expected)));
        const auto reply = response();
        complete(result, canonical(reply));
        CHECK(status == result);
        CHECK(canonical(output).contents_equal(canonical(reply)));
    }
    ::cls_rgw_bi_log_list_ret output;
    schedule(output, nullptr);
    complete(0, canonical(response()));
    CHECK(output.entries.size() == 1);

    bytes expected;
    {
        ::cls_rgw_bi_log_trim_op native;
        native.start_marker = std::string {"s\0t", 3};
        native.end_marker = std::string(80, 'e');
        expected = canonical(native);
        ::librados::ObjectWriteOperation operation;
        ::cls_rgw_bilog_trim(operation, native.start_marker, native.end_marker);
        native.start_marker.clear();
        native.end_marker.clear();
    }
    CHECK(request_method == "bi_log_trim");
    CHECK(request_bytes.contents_equal(expected));
}

TEST_CASE("Bilog completion retains transport errors and contains malformed replies", "[rgw][cls][bilog]")
{
    ::cls_rgw_bi_log_list_ret output = response();
    const auto preserved = canonical(output);
    int status = 123;
    schedule(output, &status);
    complete(-EAGAIN, {});
    CHECK(status == -EAGAIN);
    CHECK(canonical(output).contents_equal(preserved));

    const auto full = canonical(response());
    for (unsigned size = 0; size < full.length(); ++size) {
        output = response();
        bytes prefix;
        prefix.substr_of(full, 0, size);
        schedule(output, &status);
        complete(0, prefix);
        CHECK(status == -EIO);
        CHECK(canonical(output).contents_equal(preserved));
    }
}

TEST_CASE("Bilog Datum completion preserves output on allocation failure", "[rgw][cls][datum][bilog]")
{
    auto output = response();
    const auto preserved = canonical(output);
    int status = 123;
    schedule(output, &status);
    complete(0, canonical(response()), true);
    CHECK(status == -ENOMEM);
    CHECK(canonical(output).contents_equal(preserved));
}

namespace {

auto bucket_response()
{
    ::rgw_cls_list_ret value;
    value.dir.header.ver = 17;
    value.dir.header.max_marker = "max-marker";
    ::rgw_bucket_dir_entry entry;
    entry.key = {std::string {"n\0m", 3}, "instance"};
    entry.exists = true;
    entry.meta.etag = std::string(80, 'e');
    value.dir.m.emplace(std::string {"map\0key", 7}, std::move(entry));
    value.marker = {"marker", "version"};
    value.is_truncated = true;

    return value;
}

void schedule_bucket(::rgw_cls_list_ret& output)
{
    ::librados::ObjectReadOperation operation;
    ::cls_rgw_obj_key start {std::string {"n\0m", 3}, "instance"};
    std::string prefix(80, 'p');
    std::string delimiter {"/"};
    ::cls_rgw_bucket_list_op(operation, start, prefix, delimiter, 17, true, &output);
    start.name.clear();
    prefix.clear();
    delimiter.clear();
}

} // anonymous namespace

TEST_CASE("Bucket list client owns borrowed requests and publishes native API results", "[rgw][cls][bucket-list]")
{
    for (const int result : {0, 7, ::RGWBIAdvanceAndRetryError}) {
        ::rgw_cls_list_ret output;
        schedule_bucket(output);
        CHECK(request_method == "bucket_list");
        ::rgw_cls_list_op expected;
        expected.start_obj = {std::string {"n\0m", 3}, "instance"};
        expected.filter_prefix = std::string(80, 'p');
        expected.delimiter = "/";
        expected.num_entries = 17;
        expected.list_versions = true;
        CHECK(request_bytes.contents_equal(canonical(expected)));
        const auto reply = bucket_response();
        complete(result, canonical(reply));
        CHECK(canonical(output).contents_equal(canonical(reply)));
        CHECK(output.cls_filtered);
    }
    auto output = bucket_response();
    const auto preserved = canonical(output);
    schedule_bucket(output);
    complete(-EAGAIN, {});
    CHECK(canonical(output).contents_equal(preserved));
}

TEST_CASE("Bucket list completion contains malformed replies", "[rgw][cls][bucket-list]")
{
    auto output = bucket_response();
    const auto preserved = canonical(output);
    const auto full = canonical(output);
    for (unsigned size = 0; size < full.length(); ++size) {
        output = bucket_response();
        bytes prefix;
        prefix.substr_of(full, 0, size);
        schedule_bucket(output);
        complete(0, prefix);
        CHECK(canonical(output).contents_equal(preserved));
    }
    output = bucket_response();
    schedule_bucket(output);
    complete(0, full, true);
    CHECK(canonical(output).contents_equal(preserved));
}

TEST_CASE("Index stat and timeout clients encode borrowed native arguments", "[rgw][cls][stats]")
{
    for (const bool absolute : {false, true}) {
        for (const bool subtract : {false, true}) {
            bytes expected;
            {
                ::rgw_cls_bucket_update_stats_op reference;
                reference.absolute = absolute;
                reference.stats[::RGWObjCategory::Main].total_size = 123;
                reference.stats[static_cast<::RGWObjCategory>(255)].actual_size = 456;
                if (subtract) {
                    reference.dec_stats[::RGWObjCategory::Main].num_entries = 3;
                }
                expected = canonical(reference);
                ::librados::ObjectWriteOperation operation;
                ::cls_rgw_bucket_update_stats(operation, absolute, reference.stats,
                    subtract ? &reference.dec_stats : nullptr);
                reference.stats.clear();
                reference.dec_stats.clear();
            }
            CHECK(request_method == "bucket_update_stats");
            CHECK(request_bytes.contents_equal(expected));
        }
    }
    for (const std::uint64_t timeout : {std::uint64_t {0}, std::uint64_t {120},
                                       std::numeric_limits<std::uint64_t>::max()}) {
        ::librados::ObjectWriteOperation operation;
        ::cls_rgw_bucket_set_tag_timeout(operation, timeout);
        CHECK(request_method == "bucket_set_tag_timeout");
        CHECK(request_bytes.contents_equal(canonical(::rgw_cls_tag_timeout_op {.tag_timeout = timeout})));
    }
}

TEST_CASE("Index check client preserves synchronous decode and candidate error contracts", "[rgw][cls][stats]")
{
    ::rgw_cls_check_index_ret expected;
    expected.existing_header.ver = 17;
    expected.existing_header.stats[::RGWObjCategory::Main].total_size = 23;
    expected.calculated_header.ver = 29;
    expected.calculated_header.stats[::RGWObjCategory::MultiPart].actual_size = 31;
    const auto full = canonical(expected);
    ::rgw_cls_check_index_ret output;
    ::cls_rgw_bucket_check_index_decode(full, output);
    CHECK(canonical(output).contents_equal(full));
    for (unsigned size = 0; size < full.length(); ++size) {
        output = expected;
        bytes prefix;
        prefix.substr_of(full, 0, size);
        CHECK_THROWS_AS(::cls_rgw_bucket_check_index_decode(prefix, output), ::ceph::buffer::error);
        CHECK(canonical(output).contents_equal(full));
    }
    output = expected;
    bool failed = false;
    fail_allocation = true;
    try {
        ::cls_rgw_bucket_check_index_decode(full, output);
    } catch (const std::bad_alloc&) {
        failed = true;
    }
    fail_allocation = false;
    CHECK(failed);
    CHECK(canonical(output).contents_equal(full));
}

TEST_CASE("Object client builders retain native wire bytes after borrowed inputs change", "[rgw][cls][object]")
{
    ::librados::ObjectWriteOperation operation;
    ::rgw_cls_obj_remove_op removal;
    removal.keep_attr_prefixes = {"", "z", std::string {"a\0b", 3}, "z", std::string(80, 'x')};
    auto expected = canonical(removal);
    ::cls_rgw_remove_obj(operation, removal.keep_attr_prefixes);
    removal.keep_attr_prefixes.clear();
    CHECK(request_method == "obj_remove");
    CHECK(request_bytes.contents_equal(expected));

    for (const auto& text : {std::string {}, std::string {"a\0b", 3}, std::string(80, 'x')}) {
        ::rgw_cls_obj_store_pg_ver_op version {text};
        expected = canonical(version);
        ::cls_rgw_obj_store_pg_ver(operation, version.attr);
        version.attr.clear();
        CHECK(request_method == "obj_store_pg_ver");
        CHECK(request_bytes.contents_equal(expected));
        for (const bool fail : {false, true}) {
            ::rgw_cls_obj_check_attrs_prefix prefix;
            prefix.check_prefix = text;
            prefix.fail_if_exist = fail;
            expected = canonical(prefix);
            ::cls_rgw_obj_check_attrs_prefix(operation, prefix.check_prefix, fail);
            prefix.check_prefix.clear();
            CHECK(request_method == "obj_check_attrs_prefix");
            CHECK(request_bytes.contents_equal(expected));
        }
    }
    for (unsigned kind = 0; kind < 8; ++kind) {
        for (const bool precise : {false, true}) {
            ::rgw_cls_obj_check_mtime check;
            check.mtime = ::ceph::real_time {std::chrono::seconds {123} + std::chrono::nanoseconds {987654321}};
            check.type = static_cast<::RGWCheckMTimeType>(kind);
            check.high_precision_time = precise;
            expected = canonical(check);
            ::cls_rgw_obj_check_mtime(operation, check.mtime, precise, check.type);
            check.mtime = {};
            CHECK(request_method == "obj_check_mtime");
            CHECK(request_bytes.contents_equal(expected));
        }
    }
}
