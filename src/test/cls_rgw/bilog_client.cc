// RGW CLS: Actual index-read builders and deferred completion contracts.
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_const.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstdlib>
#include <new>
#include <utility>

#include <memory>
#include <string>
#include <string_view>

namespace {

using bytes = ::ceph::buffer::list;
std::unique_ptr<::librados::ObjectOperationCompletion> pending;
bytes request_bytes;
std::string request_method;
bool fail_allocation = false;

bytes canonical(const auto& value)
{
    bytes output;
    ::encode(value, output);

    return output;
}

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
#ifdef TEST_RGW_CLS_DATUM
        CHECK(canonical(output).contents_equal(preserved));
#endif
    }
}

#ifdef TEST_RGW_CLS_DATUM
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
#endif

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
#ifdef TEST_RGW_CLS_DATUM
        CHECK(canonical(output).contents_equal(preserved));
#endif
    }
#ifdef TEST_RGW_CLS_DATUM
    output = bucket_response();
    schedule_bucket(output);
    complete(0, full, true);
    CHECK(canonical(output).contents_equal(preserved));
#endif
}
