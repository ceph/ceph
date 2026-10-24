// Actual RGW-specific wrapper with controlled shared-queue calls and xattrs.
#include "test/cls_rgw/wire_test.h"
#include <catch2/catch_test_macros.hpp>
#include RGW_GC_TEST_IMPLEMENTATION

#include <cstdlib>
#include <fstream>
#include <limits>
#include <map>
#include <sys/syscall.h>
#include <unistd.h>

namespace {

using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;

struct storage final
{
    ::cls_queue_head head;
    std::vector<::cls_queue_list_ret> pages;

    std::vector<::cls_queue_list_op> lists;
    std::vector<std::pair<std::string, bytes>> effects;
    std::vector<bytes> enqueued;

    std::string removed;
    bytes xattr;
    bool has_xattr = false;

    unsigned page = 0;

    int fail_at = -1;
    int failure = -EAGAIN;

public:
    storage()
    {
        ::cls_rgw_gc_urgent_data urgent;
        urgent.num_urgent_data_entries = 100;
        head.bl_urgent_data = canonical(urgent);
        head.max_urgent_data_size = 1024 * 1024;
    }

};

using method = std::pair<int, ::cls_method_cxx_call_t>;

std::map<std::string, method> methods;

int effect(storage& state, std::string name, bytes value = {})
{
    state.effects.emplace_back(std::move(name), std::move(value));
    return int(state.effects.size()) == state.fail_at ? state.failure : 0;
}

int invoke(storage& state, bytes request, const std::string& name, bytes *reply = nullptr)
{
    static const bool initialized = [] { __cls_init(); return true; }();
    static_cast<void>(initialized);
    const auto [flags, call] = methods.at(name);
    CHECK(flags == (name == "rgw_gc_queue_list_entries" ? CLS_METHOD_RD : CLS_METHOD_RD | CLS_METHOD_WR));
    bytes output;
    const int result = call(&state, &request, &output);
    if (reply) { *reply = output; }
    if (const char *path = std::getenv("CEPH_TEST_QUEUED_GC_TRANSCRIPT")) {
        static std::ofstream transcript {path, std::ios::binary};
        bytes record;
        ::ceph::encode(name, record);
        ::ceph::encode(request, record);
        ::ceph::encode(result, record);
        ::ceph::encode(output, record);
        ::ceph::encode(state.effects, record);
        ::encode(state.head, record);
        ::ceph::encode(state.xattr, record);
        ::ceph::encode(state.has_xattr, record);
        ::ceph::encode(state.enqueued, record);
        ::ceph::encode(state.removed, record);
        record.write_stream(transcript);
        REQUIRE(transcript.good());
    }
    return result;
}

::ceph::real_time time_at(const int offset = 0)
{
    return ::ceph::real_time {std::chrono::seconds {1234567890 + offset} + std::chrono::nanoseconds {123}};
}

::cls_rgw_gc_obj_info info(const std::string& tag, const int offset = 0)
{
    ::cls_rgw_gc_obj_info result;
    result.tag = tag;
    result.time = time_at(offset);
    result.chain.objs.emplace_back().key.name = std::string(80, 'n');
    return result;
}

::cls_queue_list_ret page(std::initializer_list<::cls_rgw_gc_obj_info> values, const bool more = false)
{
    ::cls_queue_list_ret result;
    result.is_truncated = more;
    result.next_marker = "next";
    for (const auto& value : values) {
        result.entries.push_back({canonical(value), value.tag});
    }
    return result;
}

::cls_rgw_gc_urgent_data urgent(const storage& state)
{
    ::cls_rgw_gc_urgent_data result;
    auto cursor = state.head.bl_urgent_data.cbegin();
    ::decode(result, cursor);
    return result;
}

bytes set_request(const ::cls_rgw_gc_obj_info& value, const unsigned expiration = 17)
{
    ::cls_rgw_gc_set_entry_op request;
    request.info = value;
    request.expiration_secs = expiration;
    return canonical(request);
}

} // anonymous namespace

int cls_register(const char *name, cls_handle_t *handle)
{
    CHECK(std::string_view {name} == "rgw_gc");
    *handle = nullptr;
    return 0;
}

int cls_log(int, const char *, ...) { return 0; }

namespace detail {

int cls_register_cxx_method_impl(cls_handle_t, const char *name, const int flags,
                                cls_method_cxx_call_t call, cls_method_handle_t *handle)
{
    methods.emplace(name, method {flags, call});
    *handle = nullptr;
    return 0;
}

} // detail

extern "C" int clock_gettime(clockid_t clock, timespec *value) noexcept
{
    if (clock == CLOCK_REALTIME || clock == CLOCK_REALTIME_COARSE) {
        *value = {1234567890, 123};
        return 0;
    }
    return int(::syscall(SYS_clock_gettime, clock, value));
}

int queue_init(cls_method_context_t context, const ::cls_queue_init_op& op)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "init", canonical(op)); status < 0) { return status; }
    state.head.queue_size = op.queue_size;
    state.head.bl_urgent_data = op.bl_urgent_data;
    return 0;
}

int queue_read_head(cls_method_context_t context, ::cls_queue_head& head)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "read-head"); status < 0) { return status; }
    head = state.head;
    return 0;
}

int queue_write_head(cls_method_context_t context, ::cls_queue_head& head)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "write-head", canonical(head)); status < 0) { return status; }
    state.head = head;
    return 0;
}

int queue_enqueue(cls_method_context_t context, ::cls_queue_enqueue_op& op, ::cls_queue_head&)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "enqueue", canonical(op)); status < 0) { return status; }
    state.enqueued = op.bl_data_vec;
    return 0;
}

int queue_list_entries(cls_method_context_t context, const ::cls_queue_list_op& op,
                       ::cls_queue_list_ret& result, ::cls_queue_head&)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "list", canonical(op)); status < 0) { return status; }
    state.lists.push_back(op);
    if (state.page < state.pages.size()) { result = state.pages[state.page++]; }
    else { result.is_truncated = false; result.entries.clear(); result.next_marker = "end"; }
    return 0;
}

int queue_remove_entries(cls_method_context_t context, const ::cls_queue_remove_op& op, ::cls_queue_head&)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "remove", canonical(op)); status < 0) { return status; }
    state.removed = op.end_marker;
    return 0;
}

int cls_cxx_getxattr(cls_method_context_t context, const char *, bytes *value)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "get-xattr"); status < 0) { return status; }
    if (!state.has_xattr) { return -ENODATA; }
    *value = state.xattr;
    return 0;
}

int cls_cxx_setxattr(cls_method_context_t context, const char *, bytes *value)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "set-xattr", *value); status < 0) { return status; }
    state.xattr = *value;
    state.has_xattr = true;
    return 0;
}

TEST_CASE("Queued GC init retains deferred count narrowing and enqueue time and queue calls", "[rgw][cls][queued-gc]")
{
    storage state;
    ::cls_rgw_gc_queue_init_op init;
    init.size = 4096;
    init.num_deferred_entries = (1ull << 32) + 7;
    REQUIRE(invoke(state, canonical(init), "rgw_gc_queue_init") == 0);
    CHECK(urgent(state).num_urgent_data_entries == 7);
    CHECK(state.head.queue_size == 4096);
    state.effects.clear();
    REQUIRE(invoke(state, set_request(info("tag")), "rgw_gc_queue_enqueue") == 0);
    REQUIRE(state.enqueued.size() == 1);
    ::cls_rgw_gc_obj_info stored;
    auto cursor = state.enqueued.front().cbegin();
    ::decode(stored, cursor);
    CHECK(stored.time == time_at(17));
    CHECK(stored.tag == "tag");
    CHECK(state.effects.size() == 3);
    CHECK(state.effects[1].first == "enqueue");
    CHECK(state.effects[2].first == "write-head");
}

TEST_CASE("Queued GC listing counts unexpired rows, skips deferred rows and retains equality expiry", "[rgw][cls][queued-gc]")
{
    storage state;
    auto metadata = urgent(state);
    metadata.urgent_data_map["deferred"] = time_at(1);
    metadata.num_head_urgent_entries = 1;
    state.head.bl_urgent_data = canonical(metadata);
    state.pages = {page({info("deferred"), info("future", 1)}, true), page({info("equal"), info("expired", -1)})};
    ::cls_rgw_gc_list_op list;
    list.max = 3;
    list.expired_only = true;
    bytes output;
    REQUIRE(invoke(state, canonical(list), "rgw_gc_queue_list_entries", &output) == 0);
    ::cls_rgw_gc_list_ret result;
    auto cursor = output.cbegin();
    ::decode(result, cursor);
    REQUIRE(result.entries.size() == 2);
    CHECK(result.entries.front().tag == "equal");
    CHECK(result.entries.back().tag == "expired");
    CHECK_FALSE(result.truncated);
    CHECK(result.next_marker.empty());
    REQUIRE(state.lists.size() == 2);
    CHECK(state.lists[0].max == 3);
    CHECK(state.lists[1].max == 2);
    CHECK(state.lists[1].start_marker == "next");
}

TEST_CASE("Queued GC default listing and urgent head precedence retain native marker and xattr behavior", "[rgw][cls][queued-gc]")
{
    for (const bool head_hit : {false, true}) {
        storage state;
        auto metadata = urgent(state);
        metadata.num_xattr_urgent_entries = 1;
        if (head_hit) { metadata.urgent_data_map["tag"] = time_at(-1); }
        state.head.bl_urgent_data = canonical(metadata);
        state.has_xattr = true;
        state.xattr = canonical(std::unordered_map<std::string, ::ceph::real_time> {{"tag", time_at(1)}});
        state.pages = {page({info("tag")}, true), page({})};
        ::cls_rgw_gc_list_op list;
        list.max = 0;
        list.expired_only = false;
        bytes output;
        REQUIRE(invoke(state, canonical(list), "rgw_gc_queue_list_entries", &output) == 0);
        ::cls_rgw_gc_list_ret result;
        auto cursor = output.cbegin();
        ::decode(result, cursor);
        CHECK(result.entries.size() == (head_hit ? 1 : 0));
        CHECK(state.lists.front().max == 128);
        CHECK(state.effects.size() == (head_hit ? 3 : 4));
    }
}

TEST_CASE("Queued GC remove preserves lookahead marker and urgent equality counters", "[rgw][cls][queued-gc]")
{
    storage state;
    auto metadata = urgent(state);
    metadata.urgent_data_map["first"] = time_at();
    metadata.num_head_urgent_entries = 1;
    state.head.bl_urgent_data = canonical(metadata);
    state.pages = {page({info("first"), info("lookahead")}, true)};
    ::cls_rgw_gc_queue_remove_entries_op remove;
    remove.num_entries = 1;
    REQUIRE(invoke(state, canonical(remove), "rgw_gc_queue_remove_entries") == 0);
    REQUIRE(state.lists.size() == 1);
    CHECK(state.lists[0].max == 2);
    CHECK(state.removed == "lookahead");
    CHECK(urgent(state).urgent_data_map.empty());
    CHECK(urgent(state).num_head_urgent_entries == 0);
    storage empty;
    remove.num_entries = 0;
    REQUIRE(invoke(empty, canonical(remove), "rgw_gc_queue_remove_entries") == 0);
    CHECK(empty.lists[0].max == 129);
    CHECK(empty.removed.empty());
    CHECK(empty.effects.back().first == "write-head");
}

TEST_CASE("Queued GC update preserves head update spill and ENOSPC staging without enqueue", "[rgw][cls][queued-gc]")
{
    for (const bool spill : {false, true}) {
        storage state;
        if (spill) { state.head.max_urgent_data_size = 0; }
        REQUIRE(invoke(state, set_request(info("tag")), "rgw_gc_queue_update_entry") == 0);
        CHECK(state.enqueued.empty());
        const auto metadata = urgent(state);
        CHECK(metadata.num_head_urgent_entries == (spill ? 0 : 1));
        CHECK(metadata.num_xattr_urgent_entries == (spill ? 1 : 0));
        CHECK(state.has_xattr == spill);
        REQUIRE(invoke(state, set_request(info("tag"), 23), "rgw_gc_queue_update_entry") == 0);
        CHECK(urgent(state).num_head_urgent_entries == metadata.num_head_urgent_entries);
        CHECK(urgent(state).num_xattr_urgent_entries == metadata.num_xattr_urgent_entries);
    }
    storage full;
    full.head.max_urgent_data_size = 0;
    auto metadata = urgent(full);
    metadata.num_urgent_data_entries = 0;
    full.head.bl_urgent_data = canonical(metadata);
    CHECK(invoke(full, set_request(info("tag")), "rgw_gc_queue_update_entry") == -ENOSPC);
    CHECK(full.has_xattr);
    CHECK(full.effects.back().first == "set-xattr");
}

TEST_CASE("Queued GC structural and queue failures retain admission and status", "[rgw][cls][queued-gc]")
{
    ::cls_rgw_gc_queue_init_op init;
    init.size = 4096;
    ::cls_rgw_gc_list_op list;
    ::cls_rgw_gc_queue_remove_entries_op remove;
    remove.num_entries = 1;
    const std::map<std::string, bytes> requests {
        {"rgw_gc_queue_init", canonical(init)}, {"rgw_gc_queue_enqueue", set_request(info("tag"))},
        {"rgw_gc_queue_list_entries", canonical(list)}, {"rgw_gc_queue_remove_entries", canonical(remove)},
        {"rgw_gc_queue_update_entry", set_request(info("tag"))}
    };
    for (const auto& [name, full] : requests) {
        for (unsigned size = 0; size < full.length(); ++size) {
            bytes prefix;
            prefix.substr_of(full, 0, size);
            storage state;
            CHECK(invoke(state, prefix, name) == -EINVAL);
            CHECK(state.effects.empty());
        }
        storage state;
        state.fail_at = 1;
        CHECK(invoke(state, full, name) == -EAGAIN);
    }
    for (const int stage : {2, 3}) {
        storage state;
        state.fail_at = stage;
        CHECK(invoke(state, set_request(info("tag")), "rgw_gc_queue_enqueue") == -EAGAIN);
    }
    storage corrupt;
    corrupt.head.bl_urgent_data.clear();
    corrupt.head.bl_urgent_data.append("bad", 3);
    CHECK(invoke(corrupt, canonical(list), "rgw_gc_queue_list_entries") == -EINVAL);
    storage bad_row;
    auto rows = page({info("tag")});
    rows.entries.front().data.clear();
    rows.entries.front().data.append("bad", 3);
    bad_row.pages = {rows};
    CHECK(invoke(bad_row, canonical(list), "rgw_gc_queue_list_entries") == -EINVAL);
}
