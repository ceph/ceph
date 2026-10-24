// Exercise the actual user builders and deferred native completion contracts.
#include "datum_test.h"
#include "cls/user/cls_user_client.h"
#include "include/rados/librados.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cerrno>
#include <memory>

namespace {

using namespace user_cls_test;

bytes request;
std::string method;
std::unique_ptr<::librados::ObjectOperationCompletion> pending;
int submission_status = 0;

void complete(const int status, bytes reply)
{
    REQUIRE(pending);
    auto completion = std::move(pending);
    completion->handle_completion(status, reply);
}

struct callback_state final
{
    unsigned calls = 0;
    unsigned destroyed = 0;
    int status = 123;
    ::cls_user_header header;
};

class header_callback final : public ::RGWGetUserHeader_CB
{
    callback_state& state;

public:
    explicit header_callback(callback_state& state) : state(state)
    {}

    ~header_callback() override { ++state.destroyed; }

public:
    void handle_response(const int status, ::cls_user_header& header) override
    {
        ++state.calls;
        state.status = status;
        state.header = header;
    }

};

} // anonymous namespace

void librados::ObjectOperation::exec_impl(const char *cls, const char *name, bytes& input)
{
    CHECK(std::string_view {cls} == "user");
    method = name;
    request = input;
}

void librados::ObjectOperation::exec_impl(const char *cls, const char *name, bytes& input,
                                         ObjectOperationCompletion *completion)
{
    exec_impl(cls, name, input);
    REQUIRE_FALSE(pending);
    pending.reset(completion);
}

int librados::IoCtx::aio_operate(const std::string&, AioCompletion *, ObjectReadOperation *, bytes *)
{
    return submission_status;
}

TEST_CASE("User clients borrow builder inputs and retain timestamp and request bytes", "[rgw][cls][user]")
{
    ::librados::ObjectWriteOperation operation;
    ::cls_user_set_buckets_op bulk;
    bulk.entries.push_back(bucket_entry());
    bulk.add = true;
    const auto before = ::ceph::real_clock::now();
    ::cls_user_set_buckets(operation, bulk.entries, bulk.add);
    const auto after = ::ceph::real_clock::now();
    auto cursor = request.cbegin();
    ::cls_user_set_buckets_op decoded;
    ::decode(decoded, cursor);
    CHECK(decoded.time >= before);
    CHECK(decoded.time <= after);
    bulk.time = decoded.time;
    CHECK(method == "set_buckets_info");
    CHECK(request.contents_equal(canonical(bulk)));
    const auto saved = request;
    bulk.entries.clear();
    CHECK(request.contents_equal(saved));
    auto entry = bucket_entry();
    ::cls_user_remove_bucket_op remove;
    remove.bucket = entry.bucket;
    ::cls_user_remove_bucket(operation, entry.bucket);
    entry.bucket.name.clear();
    CHECK(method == "remove_bucket");
    CHECK(request.contents_equal(canonical(remove)));
    ::cls_user_complete_stats_sync(operation);
    ::cls_user_complete_stats_sync_op sync;
    cursor = request.cbegin();
    ::decode(sync, cursor);
    CHECK(method == "complete_stats_sync");
    CHECK(sync.time >= before);
    CHECK(sync.time <= ::ceph::real_clock::now());
    ::cls_user_reset_stats(operation);
    ::cls_user_reset_stats_op reset;
    cursor = request.cbegin();
    ::decode(reset, cursor);
    CHECK(method == "reset_user_stats");
    CHECK(reset.time >= before);
    CHECK(reset.time <= ::ceph::real_clock::now());
    ::cls_user_account_resource_add_op add;
    add.entry = resource();
    add.exclusive = true;
    add.limit = 0xFFFFFFFF;
    ::cls_user_account_resource_add(operation, add.entry, add.exclusive, add.limit);
    const auto expected = canonical(add);
    add.entry.name.clear();
    add.entry.metadata.clear();
    CHECK(method == "account_resource_add");
    CHECK(request.contents_equal(expected));
    ::cls_user_account_resource_rm_op rm;
    rm.name = std::string {"R\0name", 6};
    ::cls_user_account_resource_rm(operation, rm.name);
    CHECK(method == "account_resource_rm");
    CHECK(request.contents_equal(canonical(rm)));
}

TEST_CASE("User bucket list completion publishes only a complete native reply", "[rgw][cls][user]")
{
    ::cls_user_list_buckets_ret expected;
    expected.entries.push_back(bucket_entry());
    expected.marker = std::string(80, 'm');
    expected.truncated = true;
    const auto full = canonical(expected);
    for (unsigned size = 0; size <= full.length(); ++size) {
        ::librados::ObjectReadOperation operation;
        std::list<::cls_user_bucket_entry> entries;
        std::string marker = "preserved";
        bool truncated = false;
        int status = 123;
        ::cls_user_bucket_list(operation, "start", "end", -1, entries, &marker, &truncated, &status);
        ::cls_user_list_buckets_op call;
        call.marker = "start";
        call.end_marker = "end";
        call.max_entries = -1;
        CHECK(method == "list_buckets");
        CHECK(request.contents_equal(canonical(call)));
        bytes prefix;
        prefix.substr_of(full, 0, size);
        complete(7, prefix);
        CHECK(status == (size == full.length() ? 7 : -EIO));
        if (size < full.length()) {
            CHECK(entries.empty());
            CHECK(marker == "preserved");
            CHECK_FALSE(truncated);
        }
        if (size == full.length()) {
            CHECK(canonical(entries).contents_equal(canonical(expected.entries)));
            CHECK(marker == expected.marker);
            CHECK(truncated);
        }
    }
    ::librados::ObjectReadOperation operation;
    std::list<::cls_user_bucket_entry> entries;
    ::cls_user_bucket_list(operation, "", "", 0, entries, nullptr, nullptr, nullptr);
    complete(-EAGAIN, {});
    CHECK(entries.empty());
}

TEST_CASE("User header completion retains partial decode callback data and reference lifetime", "[rgw][cls][user]")
{
    ::cls_user_get_header_ret reply;
    reply.header.stats.total_entries = 103;
    reply.header.stats.total_bytes = 107;
    reply.header.last_stats_sync = bucket_entry().creation_time;
    reply.header.last_stats_update = reply.header.last_stats_sync;
    const auto full = canonical(reply);
    ::librados::IoCtx context;
    std::string oid = "oid";
    submission_status = 0;
    for (unsigned size = 0; size <= full.length(); ++size) {
        callback_state state;
        CHECK(::cls_user_get_header_async(context, oid, new header_callback {state}) == 0);
        CHECK(method == "get_header");
        CHECK(request.contents_equal(canonical(::cls_user_get_header_op {})));
        bytes prefix;
        prefix.substr_of(full, 0, size);
        ::cls_user_get_header_ret native;
        int status = 0;
        try {
            auto cursor = prefix.cbegin();
            ::decode(native, cursor);
        } catch (const ::ceph::buffer::error&) { status = -EIO; }
        complete(0, prefix);
        CHECK(state.calls == 1);
        CHECK(state.destroyed == 1);
        CHECK(state.status == status);
        CHECK(canonical(state.header).contents_equal(canonical(native.header)));
    }
    // Correct outer and header lengths allow stats and sync time to decode
    // before the absent last-update field fails. Retain that partial payload.
    auto raw = full.to_str();
    raw.resize(raw.size() - 8);
    for (const unsigned offset : {0, 6}) {
        const auto length = static_cast<std::uint32_t>(raw.size() - offset - 6);
        for (unsigned index = 0; index < 4; ++index) {
            raw[offset + 2 + index] = static_cast<char>(length >> (8 * index));
        }
    }
    bytes malformed;
    malformed.append(raw);
    callback_state partial;
    CHECK(::cls_user_get_header_async(context, oid, new header_callback {partial}) == 0);
    complete(0, malformed);
    CHECK(partial.calls == 1);
    CHECK(partial.destroyed == 1);
    CHECK(partial.status == -EIO);
    CHECK(partial.header.stats.total_entries == 103);
    CHECK(partial.header.stats.total_bytes == 107);
    CHECK(partial.header.last_stats_sync == reply.header.last_stats_sync);
    for (const int transport : {-EAGAIN, 0}) {
        callback_state state;
        CHECK(::cls_user_get_header_async(context, oid, new header_callback {state}) == 0);
        complete(transport, full);
        CHECK(state.calls == (transport < 0 ? 0 : 1));
        CHECK(state.destroyed == 1);
    }
    callback_state rejected;
    submission_status = -EBUSY;
    CHECK(::cls_user_get_header_async(context, oid, new header_callback {rejected}) == -EBUSY);
    pending.reset();
    CHECK(rejected.calls == 0);
    CHECK(rejected.destroyed == 1);
    submission_status = 0;
    ::librados::ObjectReadOperation operation;
    ::cls_user_header header;
    header.stats.total_entries = 109;
    int status = 123;
    ::cls_user_get_header(operation, &header, &status);
    complete(0, {});
    CHECK(status == -EIO);
    CHECK(header.stats.total_entries == 109);
}

TEST_CASE("User resource callbacks retain requests opaque metadata errors and optional outputs", "[rgw][cls][user]")
{
    ::cls_user_account_resource_get_ret get;
    get.entry = resource();
    const auto full = canonical(get);
    for (unsigned size = 0; size <= full.length(); ++size) {
        ::librados::ObjectReadOperation operation;
        auto entry = resource("preserved");
        int status = 123;
        ::cls_user_account_resource_get(operation, "Resource", entry, &status);
        ::cls_user_account_resource_get_op call;
        call.name = "Resource";
        CHECK(method == "account_resource_get");
        CHECK(request.contents_equal(canonical(call)));
        bytes prefix;
        prefix.substr_of(full, 0, size);
        complete(0, prefix);
        CHECK(status == (size == full.length() ? 0 : -EIO));
        CHECK(canonical(entry).contents_equal(canonical(size == full.length() ? get.entry : resource("preserved"))));
    }
    ::cls_user_account_resource_list_ret list;
    list.entries.push_back(resource());
    list.marker = "last";
    list.truncated = true;
    const auto full_list = canonical(list);
    for (unsigned size = 0; size <= full_list.length(); ++size) {
        ::librados::ObjectReadOperation operation;
        std::vector<::cls_user_account_resource> entries;
        std::string marker = "preserved";
        bool truncated = false;
        int status = 123;
        ::cls_user_account_resource_list(operation, "start", "/path/", 17, entries, &truncated, &marker, &status);
        ::cls_user_account_resource_list_op call;
        call.marker = "start";
        call.path_prefix = "/path/";
        call.max_entries = 17;
        CHECK(method == "account_resource_list");
        CHECK(request.contents_equal(canonical(call)));
        bytes prefix;
        prefix.substr_of(full_list, 0, size);
        complete(0, prefix);
        CHECK(status == (size == full_list.length() ? 0 : -EIO));
        CHECK(entries.size() == (size == full_list.length() ? 1 : 0));
        CHECK(marker == (size == full_list.length() ? "last" : "preserved"));
        CHECK(truncated == (size == full_list.length()));
    }
}
