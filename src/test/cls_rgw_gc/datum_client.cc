// RGW-specific queue client; shared queue capacity protocol stays native.
#include "test/cls_rgw/wire_test.h"
#include "cls/rgw_gc/cls_rgw_gc_client.h"
#include "cls/rgw_gc/cls_rgw_gc_ops.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "rgw/datum/adapters/cls/queued_gc.h"
#include <catch2/catch_test_macros.hpp>
#include <cerrno>
#include <limits>

namespace {

using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;

bytes request, reply;
std::string method, class_name;
int status = 0;

} // anonymous namespace

void librados::ObjectOperation::exec_impl(const char *cls, const char *name, bytes& input)
{
    class_name = cls;
    method = name;
    request = input;
}

int librados::IoCtx::exec_impl(const std::string&, const char *cls, const char *name, bytes& input, bytes& output)
{
    class_name = cls;
    method = name;
    request = input;
    output = reply;
    return status;
}

TEST_CASE("Queued GC builders retain native widths and borrowed info bytes", "[rgw][cls][queued-gc]")
{
    ::librados::ObjectWriteOperation operation;
    ::cls_rgw_gc_queue_init_op init;
    init.size = std::numeric_limits<std::uint64_t>::max();
    init.num_deferred_entries = (1ull << 32) + 17;
    ::cls_rgw_gc_queue_init(operation, init.size, init.num_deferred_entries);
    CHECK(class_name == "rgw_gc");
    CHECK(method == "rgw_gc_queue_init");
    CHECK(request.contents_equal(canonical(init)));
    ::cls_rgw_gc_obj_info info;
    info.tag = std::string(80, 't');
    info.chain.objs.emplace_back().key.name = std::string(80, 'n');
    ::cls_rgw_gc_set_entry_op enqueue;
    enqueue.expiration_secs = 19;
    enqueue.info = info;
    ::cls_rgw_gc_queue_enqueue(operation, 19, info);
    CHECK(method == "rgw_gc_queue_enqueue");
    CHECK(request.contents_equal(canonical(enqueue)));
    ::cls_rgw_gc_queue_defer_entry_op defer;
    defer.expiration_secs = 23;
    defer.info = info;
    ::cls_rgw_gc_queue_defer_entry(operation, 23, info);
    const auto expected = canonical(defer);
    CHECK(method == "rgw_gc_queue_update_entry");
    CHECK(request.contents_equal(expected));
    CHECK(info.tag == std::string(80, 't'));
    info.tag.clear();
    info.chain.objs.clear();
    CHECK(request.contents_equal(expected));
    ::cls_rgw_gc_queue_remove_entries_op remove;
    remove.num_entries = std::numeric_limits<std::uint32_t>::max();
    ::cls_rgw_gc_queue_remove_entries(operation, remove.num_entries);
    CHECK(method == "rgw_gc_queue_remove_entries");
    CHECK(request.contents_equal(canonical(remove)));
}

TEST_CASE("Queued GC list retains requests output publication and transport errors", "[rgw][cls][queued-gc]")
{
    ::librados::IoCtx context;
    ::cls_rgw_gc_list_ret result;
    result.entries.emplace_back().tag = "row";
    result.truncated = true;
    result.next_marker = "next";
    reply = canonical(result);
    status = 0;
    std::list<::cls_rgw_gc_obj_info> entries;
    bool truncated = false;
    std::string next;
    CHECK(::cls_rgw_gc_queue_list_entries(context, "oid", "marker", 17, false, entries, truncated, next) == 0);
    REQUIRE(entries.size() == 1);
    CHECK(entries.front().tag == "row");
    CHECK(truncated);
    CHECK(next == "next");
    ::cls_rgw_gc_list_op expected;
    expected.marker = "marker";
    expected.max = 17;
    expected.expired_only = false;
    CHECK(request.contents_equal(canonical(expected)));
    const auto full = reply;
    for (unsigned size = 0; size < full.length(); ++size) {
        reply.clear();
        reply.substr_of(full, 0, size);
        CHECK(::cls_rgw_gc_queue_list_entries(context, "oid", "marker", 17, false, entries, truncated, next) == -EIO);
        REQUIRE(entries.size() == 1);
        CHECK(entries.front().tag == "row");
        CHECK(truncated);
        CHECK(next == "next");
    }
    status = -EAGAIN;
    CHECK(::cls_rgw_gc_queue_list_entries(context, "oid", "marker", 17, false, entries, truncated, next) == -EAGAIN);
    status = 0;
}

TEST_CASE("Queued GC capacity continues to use shared queue class and codec", "[rgw][cls][queued-gc]")
{
    ::cls_queue_get_capacity_ret result;
    result.queue_capacity = 8192;
    reply = canonical(result);
    status = 0;
    ::librados::IoCtx context;
    std::uint64_t capacity = 17;
    CHECK(::cls_rgw_gc_queue_get_capacity(context, "oid", capacity) == 0);
    CHECK(class_name == "queue");
    CHECK(capacity == 8192);
    CHECK(request.length() == 0);
    reply.clear();
    CHECK(::cls_rgw_gc_queue_get_capacity(context, "oid", capacity) == -EIO);
    CHECK(capacity == 8192);
}

TEST_CASE("Queued GC scalar profiles preserve old future frames and reject every prefix atomically", "[rgw][cls][queued-gc]")
{
    namespace binding = ::cls::rgw::queued_gc_datum;
    for (const std::uint8_t version : {0, 1, 10}) {
        bytes full;
        ENCODE_START(version, 1, full);
        ::ceph::encode(std::uint64_t {37}, full);
        ::ceph::encode(std::numeric_limits<std::uint64_t>::max(), full);
        if (version > 1) { ::ceph::encode(std::uint64_t {41}, full); }
        ENCODE_FINISH(full);
        binding::init_request value;
        auto cursor = full.cbegin();
        binding::decode(value, cursor);
        CHECK(cursor.end());
        CHECK(value.size == 37);
        CHECK(value.num_deferred_entries == std::numeric_limits<std::uint64_t>::max());
        for (unsigned size = 0; size < full.length(); ++size) {
            bytes prefix;
            prefix.substr_of(full, 0, size);
            cursor = prefix.cbegin();
            CHECK_THROWS_AS(binding::decode(value, cursor), ::ceph::buffer::error);
            CHECK(cursor.get_off() == 0);
            CHECK(value.size == 37);
            CHECK(value.num_deferred_entries == std::numeric_limits<std::uint64_t>::max());
        }
    }
    ::cls_rgw_gc_queue_init_op init;
    init.size = 37;
    init.num_deferred_entries = 41;
    CHECK(binding::encode_init(37, 41).contents_equal(canonical(init)));
    ::cls_rgw_gc_queue_remove_entries_op remove;
    remove.num_entries = std::numeric_limits<std::uint64_t>::max();
    const auto full = binding::encode_remove(remove.num_entries);
    CHECK(full.contents_equal(canonical(remove)));
    binding::remove_request value;
    auto cursor = full.cbegin();
    binding::decode(value, cursor);
    CHECK(value.num_entries == remove.num_entries);
    for (unsigned size = 0; size < full.length(); ++size) {
        bytes prefix;
        prefix.substr_of(full, 0, size);
        cursor = prefix.cbegin();
        CHECK_THROWS_AS(binding::decode(value, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
        CHECK(value.num_entries == remove.num_entries);
    }
}
