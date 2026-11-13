// RGW CLS: Real OLH client request construction and completion publication.
#include "olh_native_oracle.h"
#include "cls/rgw/cls_rgw_client.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <new>
#include <tuple>
#include <utility>

#include <memory>
#include <string>
#include <string_view>
#include <thread>

namespace {

namespace olh = cls::rgw::olh;
using byte_buffer = ceph::buffer::list;

std::unique_ptr<librados::ObjectOperationCompletion> pending;
byte_buffer request_bytes;
byte_buffer reply_bytes;
int operation_status = 0;
int transport_status = 0;
std::atomic_bool fail_allocation = false;
bool complete_on_worker = false;
bool fail_after_transport = false;
std::size_t completion_allocations = 0;
thread_local bool count_completion_allocations = false;

auto response()
{
    return olh::reply {{{7, {{{"object", "version"}, "tag", 99,
                             static_cast<olh::action>(255), true}}}}, true};
}

auto request()
{
    cls_rgw_obj_key key;
    key.name = std::string {"n\0m", 3};
    key.instance = "instance";

    return key;
}

void complete(const int status, byte_buffer bytes)
{
    REQUIRE(pending != nullptr);
    auto callback = std::move(pending);
    if (!complete_on_worker) {
        callback->handle_completion(status, bytes);

        return;
    }
    std::thread worker {[callback = std::move(callback), bytes = std::move(bytes), status]() mutable {
        count_completion_allocations = true;
        callback->handle_completion(status, bytes);
        count_completion_allocations = false;
    }};
    worker.join();
}

template <typename ReplyT>
void schedule(ReplyT& output, int& status)
{
    librados::ObjectReadOperation op;
    cls_rgw_get_olh_log(op, request(), 0xABCDEF, std::string {"t\0g", 3}, output, status);
}

} // anonymous namespace

// Fail one armed allocation; reset before throwing so Catch2 and
// exception reporting can allocate normally.
void *operator new(const std::size_t size)
{
    if (count_completion_allocations) {
        ++completion_allocations;
    }
    if (fail_allocation.exchange(false, std::memory_order_relaxed)) {
        throw std::bad_alloc {};
    }
    if (void *p = std::malloc(size ? size : 1)) {
        return p;
    }

    throw std::bad_alloc {};
}

// Keep GCC from inlining free into callers of the matched malloc-backed replacement.
[[gnu::noinline]] void operator delete(void *p) noexcept { std::free(p); }
[[gnu::noinline]] void operator delete(void *p, std::size_t) noexcept { std::free(p); }

void librados::ObjectOperation::exec_impl(const char *cls, const char *method,
                                         byte_buffer& input, ObjectOperationCompletion *completion)
{
    CHECK(std::string_view {cls} == "rgw");
    CHECK(std::string_view {method} == "bucket_read_olh_log");
    REQUIRE(pending == nullptr);
    pending.reset(completion);
    request_bytes = input;
}

int librados::IoCtx::operate(const std::string& oid, ObjectReadOperation *, byte_buffer *)
{
    CHECK(oid == "object");
    complete(operation_status, reply_bytes);
    if (fail_after_transport) {
        fail_allocation = true;
    }

    return transport_status;
}

TEST_CASE("OLH clients share native request bytes and independent owning replies", "[rgw][cls][olh]")
{
    rgw_cls_read_olh_log_ret compatible;
    olh::reply output;
    int status = 123;
    const auto bytes = rgw_olh_oracle::encode_reply(response());
    const auto key = request();
    const auto expected = rgw_olh_oracle::encode_request({{key.name, key.instance},
                                                       std::string {"t\0g", 3}, 0xABCDEF, true});
    schedule(compatible, status);
    CHECK(status == 123);
    CHECK(request_bytes.contents_equal(expected));
    complete(3, bytes);
    CHECK(status == 3);
    byte_buffer native_bytes;
    ::encode(compatible, native_bytes);
    CHECK(native_bytes.contents_equal(bytes));

    schedule(output, status);
    CHECK(request_bytes.contents_equal(expected));
    complete(0, bytes);
    CHECK(status == 0);
    CHECK(rgw_olh_oracle::encode_reply(output).contents_equal(bytes));
}

TEST_CASE("OLH completions preserve outputs on decode, transport and allocation failures", "[rgw][cls][olh]")
{
    const auto bytes = rgw_olh_oracle::encode_reply(response());
    olh::reply output = response();
    rgw_cls_read_olh_log_ret compatible;
    auto cursor = bytes.cbegin();
    ::decode(compatible, cursor);
    int status = 0;
    const auto check_preserved = [&] {
        CHECK(rgw_olh_oracle::encode_reply(output).contents_equal(bytes));
        byte_buffer actual;
        ::encode(compatible, actual);
        CHECK(actual.contents_equal(bytes));
    };
    for (unsigned size = 0; size < bytes.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(bytes, 0, size);
        schedule(output, status);
        complete(0, prefix);
        CHECK(status == -EIO);
        schedule(compatible, status);
        complete(0, prefix);
        CHECK(status == -EIO);
        check_preserved();
    }
    for (const int result : {-EAGAIN, -ECANCELED}) {
        schedule(output, status);
        complete(result, {});
        CHECK(status == result);
        schedule(compatible, status);
        complete(result, {});
        CHECK(status == result);
        check_preserved();
    }
    schedule(output, status);
    auto callback = std::move(pending);
    auto input = bytes;
    fail_allocation = true;
    callback->handle_completion(0, input);
    CHECK_FALSE(fail_allocation);
    CHECK(status == -ENOMEM);
    check_preserved();
    schedule(compatible, status);
    callback = std::move(pending);
    fail_allocation = true;
    callback->handle_completion(0, input);
    CHECK_FALSE(fail_allocation);
    CHECK(status == -ENOMEM);
    check_preserved();
}

TEST_CASE("OLH completions retain retry status and sync transport precedence", "[rgw][cls][olh]")
{
    olh::reply output;
    int status = 0;
    reply_bytes = rgw_olh_oracle::encode_reply(response());
    schedule(output, status);
    complete(RGWBIAdvanceAndRetryError, reply_bytes);
    CHECK(status == RGWBIAdvanceAndRetryError);
    CHECK(rgw_olh_oracle::encode_reply(output).contents_equal(reply_bytes));
    librados::IoCtx context;
    std::string oid = "object";
    rgw_cls_read_olh_log_ret compatible;
    for (const auto& [transport, operation, expected] : {
             std::tuple {0, 0, 0}, std::tuple {2, 0, 2}, std::tuple {0, -EAGAIN, -EAGAIN},
             std::tuple {-ECANCELED, -EAGAIN, -ECANCELED},
             std::tuple {-ECANCELED, 0, -ECANCELED},
             std::tuple {0, RGWBIAdvanceAndRetryError, RGWBIAdvanceAndRetryError},
             std::tuple {0, 3, 0}}) {
        transport_status = transport;
        operation_status = operation;
        CHECK(cls_rgw_get_olh_log(context, oid, request(), 1, "tag", output) == expected);
        CHECK(cls_rgw_get_olh_log(context, oid, request(), 1, "tag", compatible) == expected);
    }
}

TEST_CASE("Synchronous OLH reads allocate decoded replies on the calling thread", "[rgw][cls][olh]")
{
    reply_bytes = rgw_olh_oracle::encode_reply(response());
    operation_status = transport_status = 0;
    librados::IoCtx context;
    std::string oid = "object";
    const auto read = [&context, &oid](auto& output) {
        completion_allocations = 0;
        complete_on_worker = true;
        const auto result = cls_rgw_get_olh_log(context, oid, request(), 1, "tag", output);
        complete_on_worker = false;
        CHECK(result == 0);
        CHECK(completion_allocations == 0);
    };
    olh::reply output;
    read(output);
    CHECK(rgw_olh_oracle::encode_reply(output).contents_equal(reply_bytes));
    rgw_cls_read_olh_log_ret compatible;
    read(compatible);
    byte_buffer actual;
    ::encode(compatible, actual);
    CHECK(actual.contents_equal(reply_bytes));
}

TEST_CASE("Synchronous OLH reads preserve outputs on decoding failures", "[rgw][cls][olh]")
{
    auto output = response();
    output.epochs.front().second.front().tag = "preserve";
    const auto before = rgw_olh_oracle::encode_reply(output);
    rgw_cls_read_olh_log_ret compatible;
    auto cursor = before.cbegin();
    ::decode(compatible, cursor);
    const auto encoded = rgw_olh_oracle::encode_reply(response());
    operation_status = transport_status = 0;
    librados::IoCtx context;
    std::string oid = "object";
    const auto check_preserved = [&output, &before, &compatible] {
        CHECK(rgw_olh_oracle::encode_reply(output).contents_equal(before));
        byte_buffer actual;
        ::encode(compatible, actual);
        CHECK(actual.contents_equal(before));
    };
    for (unsigned size = 0; size < encoded.length(); ++size) {
        reply_bytes.clear();
        reply_bytes.substr_of(encoded, 0, size);
        CHECK(cls_rgw_get_olh_log(context, oid, request(), 1, "tag", output) == -EIO);
        CHECK(cls_rgw_get_olh_log(context, oid, request(), 1, "tag", compatible) == -EIO);
        check_preserved();
    }
    reply_bytes = encoded;
    const auto allocation_failure = [&context, &oid](auto& value) {
        fail_after_transport = true;
        const auto result = cls_rgw_get_olh_log(context, oid, request(), 1, "tag", value);
        fail_after_transport = false;
        const auto unconsumed = fail_allocation.exchange(false, std::memory_order_relaxed);
        CHECK_FALSE(unconsumed);
        CHECK(result == -ENOMEM);
    };
    allocation_failure(output);
    allocation_failure(compatible);
    check_preserved();
}
