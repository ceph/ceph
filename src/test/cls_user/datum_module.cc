// Run the actual user plugin against controlled OMAP storage, including errors.
#include "datum_test.h"
#include "../cls_rgw/module_loader.h"

#include <catch2/catch_test_macros.hpp>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <map>
#include <tuple>
#include <vector>

namespace {

using namespace user_cls_test;

struct storage final
{
    std::map<std::string, bytes> values;
    bytes header;
    bytes reply;
    std::vector<std::pair<std::string, bytes>> effects;
    int failure_call = -1;
    int failure = -EAGAIN;
};

int effect(storage& state, std::string operation, bytes arguments = {})
{
    state.effects.emplace_back(std::move(operation), std::move(arguments));

    return static_cast<int>(state.effects.size()) == state.failure_call ? state.failure : 0;
}

int invoke(storage& state, const std::string_view method, bytes input)
{
    const auto [call, flags] = ::rgw_cls_test::get_method(method);
    const bool read_only = method == "list_buckets" || method == "get_header" ||
                           method == "account_resource_get" || method == "account_resource_list";
    REQUIRE(flags == (read_only ? CLS_METHOD_RD : CLS_METHOD_RD | CLS_METHOD_WR));
    bytes output;
    const int status = call(&state, &input, &output);
    state.reply = output;
    if (const char *path = std::getenv("CEPH_TEST_CLS_RGW_TRANSCRIPT")) {
        static std::ofstream transcript {path, std::ios::binary};
        const auto record = canonical(std::tuple {std::string {method}, input, status, output,
                                                 state.effects, state.values, state.header});
        record.write_stream(transcript);
        REQUIRE(transcript.good());
    }

    return status;
}

int invoke(storage& state, const std::string_view method, const auto& request)
{
    return invoke(state, method, canonical(request));
}

template <typename T>
T decoded(const bytes& input)
{
    T value;
    auto cursor = input.cbegin();
    ::decode(value, cursor);

    return value;
}

void check_failures(const storage& initial, const std::string_view method, const auto& request)
{
    auto success = initial;
    success.effects.clear();
    REQUIRE(invoke(success, method, request) == 0);
    for (unsigned index = 1; index <= success.effects.size(); ++index) {
        auto failed = initial;
        failed.effects.clear();
        failed.failure_call = index;
        CHECK(invoke(failed, method, request) == -EAGAIN);
        CHECK(failed.effects.size() == index);
    }
}

} // anonymous namespace

int cls_cxx_map_read_header(cls_method_context_t context, bytes *output)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "read-header"); status < 0) { return status; }
    *output = state.header;

    return 0;
}

int cls_cxx_map_write_header(cls_method_context_t context, bytes *input)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "write-header", *input); status < 0) { return status; }
    state.header = *input;

    return 0;
}

int cls_cxx_map_get_val(cls_method_context_t context, const std::string& key, bytes *output)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "get-value", canonical(key)); status < 0) { return status; }
    const auto found = state.values.find(key);
    if (found == state.values.end()) { return -ENOENT; }
    *output = found->second;

    return 0;
}

int cls_cxx_map_set_val(cls_method_context_t context, const std::string& key, bytes *input)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "set-value", canonical(std::pair {key, *input})); status < 0) { return status; }
    state.values.insert_or_assign(key, *input);

    return 0;
}

int cls_cxx_map_remove_key(cls_method_context_t context, const std::string& key)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "remove-key", canonical(key)); status < 0) { return status; }
    state.values.erase(key);

    return 0;
}

int cls_cxx_map_get_vals(cls_method_context_t context, const std::string& after,
                         const std::string& prefix, const std::uint64_t maximum,
                         std::map<std::string, bytes> *output, bool *more)
{
    auto& state = *static_cast<storage *>(context);
    if (const int status = effect(state, "get-values", canonical(std::tuple {after, prefix, maximum})); status < 0) { return status; }
    *more = false;
    for (auto it = state.values.upper_bound(after); it != state.values.end(); ++it) {
        if (!it->first.starts_with(prefix)) { continue; }
        if (output->size() == maximum) { *more = true; break; }
        output->insert(*it);
    }

    return 0;
}

TEST_CASE("User plugin validates all eleven outer requests before any storage IO", "[rgw][cls][user]")
{
    ::cls_user_set_buckets_op bulk;
    bulk.entries.push_back(bucket_entry());
    ::cls_user_remove_bucket_op remove;
    remove.bucket = bucket_entry().bucket;
    ::cls_user_account_resource_add_op add;
    add.entry = resource();
    const std::vector<std::pair<std::string, bytes>> requests {
        {"set_buckets_info", canonical(bulk)},
        {"complete_stats_sync", canonical(::cls_user_complete_stats_sync_op {})},
        {"remove_bucket", canonical(remove)},
        {"list_buckets", canonical(::cls_user_list_buckets_op {})},
        {"get_header", canonical(::cls_user_get_header_op {})},
        {"reset_user_stats", canonical(::cls_user_reset_stats_op {})},
        {"reset_user_stats2", canonical(::cls_user_reset_stats2_op {})},
        {"account_resource_add", canonical(add)},
        {"account_resource_get", canonical(::cls_user_account_resource_get_op {})},
        {"account_resource_rm", canonical(::cls_user_account_resource_rm_op {})},
        {"account_resource_list", canonical(::cls_user_account_resource_list_op {})}
    };
    for (const auto& [method, full] : requests) {
        for (unsigned size = 0; size < full.length(); ++size) {
            storage state;
            bytes prefix;
            prefix.substr_of(full, 0, size);
            CHECK(invoke(state, method, prefix) == -EINVAL);
            CHECK(state.effects.empty());
            CHECK(state.reply.length() == 0);
        }
    }
}

TEST_CASE("User bulk update retains add sync removal timestamp and native IO order", "[rgw][cls][user]")
{
    storage initial;
    ::cls_user_set_buckets_op bulk;
    bulk.entries.push_back(bucket_entry("a"));
    bulk.entries.push_back(bucket_entry("b"));
    bulk.add = true;
    bulk.time = bucket_entry().creation_time;
    check_failures(initial, "set_buckets_info", bulk);
    storage state;
    CHECK(invoke(state, "set_buckets_info", bulk) == 0);
    auto header = decoded<::cls_user_header>(state.header);
    CHECK(header.stats.total_entries == 6);
    CHECK(header.stats.total_bytes == 74);
    CHECK(header.last_stats_update == bulk.time);
    CHECK(decoded<::cls_user_bucket_entry>(state.values.at("a")).user_stats_sync);
    bulk.entries.front().size = 101;
    bulk.entries.front().bucket.bucket_id = "resharded";
    CHECK(invoke(state, "set_buckets_info", bulk) == 0);
    CHECK(decoded<::cls_user_header>(state.header).stats.total_bytes == 74);
    CHECK(decoded<::cls_user_bucket_entry>(state.values.at("a")).bucket.bucket_id == "resharded");
    bulk.add = false;
    bulk.entries.push_back(bucket_entry("missing"));
    CHECK(invoke(state, "set_buckets_info", bulk) == 0);
    CHECK_FALSE(state.values.contains("missing"));
    CHECK(decoded<::cls_user_header>(state.header).stats.total_bytes == 138);
    ::cls_user_complete_stats_sync_op sync;
    sync.time = bulk.time;
    check_failures(state, "complete_stats_sync", sync);
    CHECK(invoke(state, "complete_stats_sync", sync) == 0);
    CHECK(decoded<::cls_user_header>(state.header).last_stats_sync == sync.time);
    sync.time = ::ceph::real_time {};
    CHECK(invoke(state, "complete_stats_sync", sync) == 0);
    CHECK(decoded<::cls_user_header>(state.header).last_stats_sync == bulk.time);
    ::cls_user_remove_bucket_op remove;
    remove.bucket = bucket_entry("a").bucket;
    state.effects.clear();
    check_failures(state, "remove_bucket", remove);
    CHECK(invoke(state, "remove_bucket", remove) == 0);
    CHECK(decoded<::cls_user_header>(state.header).stats.total_bytes == 37);
    CHECK(invoke(state, "remove_bucket", remove) == 0);
    storage unsynced;
    unsynced.values["a"] = canonical(bucket_entry("a"));
    CHECK(invoke(unsynced, "remove_bucket", remove) == 0);
    CHECK(unsynced.header.length() == 0);
    CHECK(unsynced.effects.size() == 3);
    storage corrupt;
    corrupt.header.append("x", 1);
    CHECK(invoke(corrupt, "set_buckets_info", bulk) == -EIO);
}

TEST_CASE("User bucket paging preserves corrupt skips end marker and signed limits", "[rgw][cls][user]")
{
    storage initial;
    initial.values["a"] = canonical(bucket_entry("a"));
    initial.values["b"].append("corrupt", 7);
    initial.values["c"] = canonical(bucket_entry("c"));
    ::cls_user_list_buckets_op list;
    list.max_entries = 2;
    auto state = initial;
    CHECK(invoke(state, "list_buckets", list) == 0);
    auto reply = decoded<::cls_user_list_buckets_ret>(state.reply);
    REQUIRE(reply.entries.size() == 1);
    CHECK(reply.entries.front().bucket.name == "a");
    CHECK(reply.marker == "b");
    CHECK(reply.truncated);
    list.marker = reply.marker;
    CHECK(invoke(state, "list_buckets", list) == 0);
    reply = decoded<::cls_user_list_buckets_ret>(state.reply);
    REQUIRE(reply.entries.size() == 1);
    CHECK(reply.entries.front().bucket.name == "c");
    CHECK(reply.marker.empty());
    CHECK_FALSE(reply.truncated);
    for (const int maximum : {0, -1, 1001}) {
        state = initial;
        list.marker.clear();
        list.max_entries = maximum;
        CHECK(invoke(state, "list_buckets", list) == 0);
        reply = decoded<::cls_user_list_buckets_ret>(state.reply);
        CHECK(reply.entries.size() == (maximum == 0 ? 0 : 2));
    }
    state = initial;
    list.end_marker = "c";
    CHECK(invoke(state, "list_buckets", list) == 0);
    reply = decoded<::cls_user_list_buckets_ret>(state.reply);
    CHECK(reply.entries.size() == 1);
    CHECK_FALSE(reply.truncated);
    CHECK(reply.marker.empty());
    storage corrupt_only;
    corrupt_only.values["a"].append("x", 1);
    corrupt_only.values["b"].append("x", 1);
    list.end_marker.clear();
    list.max_entries = 1;
    CHECK(invoke(corrupt_only, "list_buckets", list) == 0);
    reply = decoded<::cls_user_list_buckets_ret>(corrupt_only.reply);
    CHECK(reply.entries.empty());
    CHECK(reply.marker == "a");
    CHECK(reply.truncated);
    check_failures(initial, "list_buckets", list);
}

TEST_CASE("User header read retains empty defaults corrupt errors and storage failures", "[rgw][cls][user]")
{
    storage state;
    const ::cls_user_get_header_op request;
    check_failures(state, "get_header", request);
    CHECK(invoke(state, "get_header", request) == 0);
    CHECK(decoded<::cls_user_get_header_ret>(state.reply).header.stats.total_entries == 0);
    state.header.append("x", 1);
    CHECK(invoke(state, "get_header", request) == -EIO);
}

TEST_CASE("User reset paths retain page sums corruption and final reply on write failure", "[rgw][cls][user]")
{
    storage initial;
    for (unsigned index = 0; index < 1001; ++index) {
        const auto name = std::to_string(10000 + index);
        initial.values[name] = canonical(bucket_entry(name));
    }
    ::cls_user_reset_stats_op reset;
    reset.time = bucket_entry().creation_time;
    check_failures(initial, "reset_user_stats", reset);
    auto state = initial;
    CHECK(invoke(state, "reset_user_stats", reset) == 0);
    CHECK(decoded<::cls_user_header>(state.header).stats.total_entries == 3003);
    CHECK(decoded<::cls_user_header>(state.header).last_stats_update == reset.time);
    ::cls_user_reset_stats2_op reset2;
    reset2.time = reset.time;
    reset2.acc_stats.total_entries = 999999;
    state = initial;
    CHECK(invoke(state, "reset_user_stats2", reset2) == 0);
    auto reply = decoded<::cls_user_reset_stats2_ret>(state.reply);
    CHECK(reply.truncated);
    CHECK(reply.acc_stats.total_entries == 3000);
    CHECK(state.header.length() == 0);
    reply.update_call(reset2);
    state.effects.clear();
    state.failure_call = 2;
    CHECK(invoke(state, "reset_user_stats2", reset2) == -EAGAIN);
    reply = decoded<::cls_user_reset_stats2_ret>(state.reply);
    CHECK_FALSE(reply.truncated);
    // Match main: the incoming accumulated statistics are currently ignored.
    CHECK(reply.acc_stats.total_entries == 3);
    CHECK(state.header.length() == 0);
    state.failure_call = -1;
    CHECK(invoke(state, "reset_user_stats2", reset2) == 0);
    CHECK(decoded<::cls_user_header>(state.header).stats.total_entries == 3);
    storage corrupt;
    corrupt.values["bad"].append("x", 1);
    CHECK(invoke(corrupt, "reset_user_stats", reset) == -EIO);
    reset2.marker.clear();
    CHECK(invoke(corrupt, "reset_user_stats2", reset2) == -EIO);
}

TEST_CASE("Account resource add get remove retain case folding limits metadata and IO failures", "[rgw][cls][user]")
{
    storage initial;
    ::cls_user_account_resource_add_op add;
    add.entry = resource();
    add.limit = 1;
    check_failures(initial, "account_resource_add", add);
    storage state;
    CHECK(invoke(state, "account_resource_add", add) == 0);
    CHECK(state.values.contains("resource"));
    CHECK(decoded<::cls_user_account_header>(state.header).count == 1);
    add.exclusive = true;
    state.effects.clear();
    CHECK(invoke(state, "account_resource_add", add) == -EEXIST);
    CHECK(state.effects.size() == 1);
    add.exclusive = false;
    add.limit = 0;
    state.effects.clear();
    CHECK(invoke(state, "account_resource_add", add) == 0);
    CHECK(state.effects.size() == 2);
    add.entry.name = "Second";
    CHECK(invoke(state, "account_resource_add", add) == -EUSERS);
    ::cls_user_account_resource_get_op get;
    get.name = "RESOURCE";
    state.effects.clear();
    check_failures(state, "account_resource_get", get);
    CHECK(invoke(state, "account_resource_get", get) == 0);
    CHECK(canonical(decoded<::cls_user_account_resource_get_ret>(state.reply).entry).contents_equal(canonical(resource())));
    ::cls_user_account_resource_rm_op remove;
    remove.name = get.name;
    state.effects.clear();
    check_failures(state, "account_resource_rm", remove);
    CHECK(invoke(state, "account_resource_rm", remove) == 0);
    CHECK(decoded<::cls_user_account_header>(state.header).count == 0);
    CHECK(invoke(state, "account_resource_rm", remove) == -ENOENT);
    storage corrupt;
    corrupt.values["resource"].append("x", 1);
    CHECK(invoke(corrupt, "account_resource_get", get) == -EIO);
}

TEST_CASE("Account resource list keeps fetched marker prefix filtering corruption and page cap", "[rgw][cls][user]")
{
    storage initial;
    initial.values["a"] = canonical(resource("A", "/accepted/"));
    initial.values["b"] = canonical(resource("B", "/excluded/"));
    initial.values["c"] = canonical(resource("C", "/accepted/"));
    ::cls_user_account_resource_list_op list;
    list.max_entries = 2;
    list.path_prefix = "/accepted/";
    check_failures(initial, "account_resource_list", list);
    auto state = initial;
    CHECK(invoke(state, "account_resource_list", list) == 0);
    auto reply = decoded<::cls_user_account_resource_list_ret>(state.reply);
    REQUIRE(reply.entries.size() == 1);
    CHECK(reply.entries.front().name == "A");
    CHECK(reply.marker == "b");
    CHECK(reply.truncated);
    list.marker = "b";
    CHECK(invoke(state, "account_resource_list", list) == 0);
    reply = decoded<::cls_user_account_resource_list_ret>(state.reply);
    CHECK(reply.marker == "c");
    CHECK_FALSE(reply.truncated);
    list.marker.clear();
    list.path_prefix = "/none/";
    list.max_entries = 0xFFFFFFFF;
    CHECK(invoke(state, "account_resource_list", list) == 0);
    reply = decoded<::cls_user_account_resource_list_ret>(state.reply);
    CHECK(reply.entries.empty());
    CHECK(reply.marker == "c");
    list.max_entries = 0;
    CHECK(invoke(state, "account_resource_list", list) == 0);
    reply = decoded<::cls_user_account_resource_list_ret>(state.reply);
    CHECK(reply.entries.empty());
    CHECK(reply.marker.empty());
    CHECK(reply.truncated);
    list.max_entries = 1000;
    state.values["b"].clear();
    state.values["b"].append("corrupt", 7);
    CHECK(invoke(state, "account_resource_list", list) == -EIO);
}
