// RGW CLS: Real OLH client request construction and completion publication.
#include "olh_native_oracle.h"
#include "cls/rgw/cls_rgw_client.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstdlib>
#include <new>
#include <tuple>
#include <utility>

#include <memory>
#include <string>
#include <string_view>

namespace {

namespace olh = ::cls::rgw::olh;
using byte_buffer = ::ceph::buffer::list;

std::unique_ptr<::librados::ObjectOperationCompletion> pending;
byte_buffer request_bytes;
byte_buffer reply_bytes;
int operation_status = 0;
int transport_status = 0;
bool fail_allocation = false;

auto response()
{
    return olh::reply {{{7, {{{"object", "version"}, "tag", 99,
                             static_cast<olh::action>(255), true}}}}, true};
}

auto request()
{
    ::cls_rgw_obj_key key;
    key.name = std::string {"n\0m", 3};
    key.instance = "instance";

    return key;
}

void complete(const int status, byte_buffer bytes)
{
    REQUIRE(pending != nullptr);
    auto callback = std::move(pending);
    callback->handle_completion(status, bytes);
}

template <typename ReplyT>
void schedule(ReplyT& output, int& status)
{
    ::librados::ObjectReadOperation op;
    ::cls_rgw_get_olh_log(op, request(), 0xABCDEF, std::string {"t\0g", 3}, output, status);
}

} // anonymous namespace

// Arm one allocation inside completion; reset before throwing so Catch2 and
// exception reporting can allocate normally.
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

    return transport_status;
}

TEST_CASE("OLH clients share native request bytes and independent owning replies", "[rgw][cls][olh]")
{
    ::rgw_cls_read_olh_log_ret compatible;
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
    ::rgw_cls_read_olh_log_ret compatible;
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
    complete(::RGWBIAdvanceAndRetryError, reply_bytes);
    CHECK(status == ::RGWBIAdvanceAndRetryError);
    CHECK(rgw_olh_oracle::encode_reply(output).contents_equal(reply_bytes));
    ::librados::IoCtx context;
    std::string oid = "object";
    ::rgw_cls_read_olh_log_ret compatible;
    for (const auto [transport, operation, expected] : {
             std::tuple {0, 0, 0}, std::tuple {2, 0, 2}, std::tuple {0, -EAGAIN, -EAGAIN},
             std::tuple {-ECANCELED, -EAGAIN, -ECANCELED}}) {
        transport_status = transport;
        operation_status = operation;
        CHECK(::cls_rgw_get_olh_log(context, oid, request(), 1, "tag", output) == expected);
        CHECK(::cls_rgw_get_olh_log(context, oid, request(), 1, "tag", compatible) == expected);
    }
}
