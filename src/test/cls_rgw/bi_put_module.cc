// RGW CLS: Actual index handlers with controlled OMAP effects and time.
#include "module_loader.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "common/config_proxy.h"
#include "osd/osd_types.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>

#include <algorithm>
#include <chrono>
#include <ctime>
#include <fstream>
#include <map>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <sys/syscall.h>
#include <unistd.h>

namespace {

using byte_buffer = ::ceph::buffer::list;

struct storage final
{
    std::map<std::string, byte_buffer> values;
    byte_buffer header;
    byte_buffer reply;
    std::vector<std::pair<std::string, byte_buffer>> effects;

    std::uint64_t maximum = 4;
    std::uint32_t reshard_threshold = 4;
    bool usage_key_transition = false;
    int stat_status = 0;

    int failure_call = -1;
    int failure = -EAGAIN;
    int secondary_failure_call = -1;
    int secondary_failure = -EIO;
};

int effect(storage& state, std::string operation, byte_buffer arguments = {})
{
    state.effects.emplace_back(std::move(operation), std::move(arguments));

    if (static_cast<int>(state.effects.size()) == state.secondary_failure_call) {
        return state.secondary_failure;
    }

    return static_cast<int>(state.effects.size()) == state.failure_call ? state.failure : 0;
}

auto invoke(storage& state, byte_buffer input, const std::string_view method = "bi_put_entries")
{
    const auto [call, flags] = ::rgw_cls_test::get_method(method);
    const bool read_only = method == "bi_log_list" || method == "bucket_list" || method == "bucket_check_index" || method == "user_usage_log_read" || method == "reshard_list" ||
                           method == "reshard_get" || method == "guard_bucket_resharding" || method == "get_bucket_resharding" || method == "gc_list" || method == "lc_get_entry" ||
                           method == "lc_get_next_entry" || method == "lc_list_entries" || method == "lc_get_head";
    REQUIRE(flags == (read_only ? CLS_METHOD_RD : CLS_METHOD_RD | CLS_METHOD_WR));
    byte_buffer output;
    const int status = call(&state, &input, &output);
    state.reply = output;
    if (const char *path = std::getenv("CEPH_TEST_CLS_RGW_TRANSCRIPT")) {
        static std::ofstream transcript {path, std::ios::binary};
        byte_buffer record;
        ::ceph::encode(std::string {method}, record);
        ::ceph::encode(input, record);
        ::ceph::encode(status, record);
        ::ceph::encode(output, record);
        ::ceph::encode(state.effects, record);
        ::ceph::encode(state.values, record);
        ::ceph::encode(state.header, record);
        record.write_stream(transcript);
        REQUIRE(transcript.good());
    }

    return status;
}

auto invoke(storage& state, const ::rgw_cls_bi_put_entries_op& request)
{
    byte_buffer input;
    ::encode(request, input);

    return invoke(state, std::move(input));
}

auto invoke(storage& state, const ::rgw_cls_bi_put_op& request)
{
    byte_buffer input;
    ::encode(request, input);

    return invoke(state, std::move(input), "bi_put");
}

auto entry(const std::string& key, const std::uint64_t size)
{
    ::rgw_bucket_dir_entry value;
    value.key.name = key;
    value.exists = true;
    value.meta.category = ::RGWObjCategory::Main;
    value.meta.size = size;
    value.meta.accounted_size = size;
    ::rgw_cls_bi_entry result;
    result.type = ::BIIndexType::Plain;
    result.idx = key;
    ::encode(value, result.data);

    return result;
}

auto effect_names(const storage& state)
{
    return state.effects | std::views::keys | std::ranges::to<std::vector<std::string>>();
}

auto header(const storage& state)
{
    ::rgw_bucket_dir_header value;
    auto cursor = state.header.cbegin();
    ::decode(value, cursor);

    return value;
}

} // anonymous namespace

// Keep timestamped pending/log records deterministic across actual plugins.
extern "C" int clock_gettime(clockid_t clock, timespec *value) noexcept
{
    if (clock == CLOCK_REALTIME || clock == CLOCK_REALTIME_COARSE) {
        *value = {1234567890, 123};

        return 0;
    }

    return static_cast<int>(::syscall(SYS_clock_gettime, clock, value));
}

const ::object_info_t& cls_get_object_info(cls_method_context_t)
{
    static const ::object_info_t info;

    return info;
}

std::uint64_t cls_current_version(cls_method_context_t) { return 123; }
int cls_current_subop_num(cls_method_context_t) { return 2; }

const ::ConfigProxy& cls_get_config(cls_method_context_t context)
{
    static ::ConfigProxy configuration {false};
    const auto& state = *static_cast<storage *>(context);
    configuration->osd_max_omap_entries_per_request = state.maximum;
    configuration->rgw_reshardlog_threshold = state.reshard_threshold;
    configuration->rgw_usage_log_key_transition = state.usage_key_transition;

    return configuration;
}

int cls_cxx_map_read_header(cls_method_context_t context, byte_buffer *output)
{
    auto& state = *static_cast<storage *>(context);
    if (const int result = effect(state, "read-header"); result < 0) {
        return result;
    }
    *output = state.header;

    return 0;
}

int cls_cxx_map_write_header(cls_method_context_t context, byte_buffer *input)
{
    auto& state = *static_cast<storage *>(context);
    if (const int result = effect(state, "write-header", *input); result < 0) {
        return result;
    }
    state.header = *input;

    return 0;
}

int cls_cxx_map_get_vals_by_keys(cls_method_context_t context, const std::set<std::string>& keys,
                                 std::map<std::string, byte_buffer> *output)
{
    auto& state = *static_cast<storage *>(context);
    byte_buffer arguments;
    ::ceph::encode(keys, arguments);
    if (const int result = effect(state, "get-by-keys", arguments); result < 0) {
        return result;
    }
    for (const auto& key : keys) {
        if (const auto found = state.values.find(key); found != state.values.end()) {
            output->insert(*found);
        }
    }

    return 0;
}

int cls_cxx_map_get_val(cls_method_context_t context, const std::string& key, byte_buffer *output)
{
    auto& state = *static_cast<storage *>(context);
    byte_buffer arguments;
    ::ceph::encode(key, arguments);
    if (const int result = effect(state, "get-value", arguments); result < 0) {
        return result;
    }
    const auto found = state.values.find(key);
    if (found == state.values.end()) {
        return -ENOENT;
    }
    *output = found->second;

    return 0;
}

int cls_cxx_map_get_vals(cls_method_context_t context, const std::string& after,
                         const std::string& prefix, const std::uint64_t maximum,
                         std::map<std::string, byte_buffer> *output, bool *more)
{
    auto& state = *static_cast<storage *>(context);
    byte_buffer arguments;
    ::ceph::encode(std::tuple {after, prefix, maximum}, arguments);
    if (const int result = effect(state, "get-values", arguments); result < 0) {
        return result;
    }
    *more = false;
    for (auto it = state.values.upper_bound(after); it != state.values.end(); ++it) {
        if (!it->first.starts_with(prefix)) {
            continue;
        }
        if (output->size() == maximum) {
            *more = true;
            break;
        }
        output->insert(*it);
    }

    return 0;
}

int cls_cxx_map_get_keys(cls_method_context_t context, const std::string& after,
                         const std::uint64_t maximum, std::set<std::string> *output, bool *more)
{
    auto& state = *static_cast<storage *>(context);
    byte_buffer arguments;
    ::ceph::encode(std::tuple {after, maximum}, arguments);
    if (const int result = effect(state, "get-keys", arguments); result < 0) {
        return result;
    }
    *more = false;
    for (auto it = state.values.upper_bound(after); it != state.values.end(); ++it) {
        if (output->size() == maximum) {
            *more = true;
            break;
        }
        output->insert(it->first);
    }

    return 0;
}

int cls_cxx_map_remove_range(cls_method_context_t context, const std::string& first,
                             const std::string& last)
{
    auto& state = *static_cast<storage *>(context);
    byte_buffer arguments;
    ::ceph::encode(std::pair {first, last}, arguments);
    if (const int result = effect(state, "remove-range", arguments); result < 0) {
        return result;
    }
    const auto lower = state.values.lower_bound(first);
    const auto upper = state.values.lower_bound(last);
    state.values.erase(lower, upper);

    return 0;
}

int cls_cxx_map_remove_key(cls_method_context_t context, const std::string& key)
{
    auto& state = *static_cast<storage *>(context);
    byte_buffer arguments;
    ::ceph::encode(key, arguments);
    if (const int result = effect(state, "remove", arguments); result < 0) {
        return result;
    }

    return state.values.erase(key) ? 0 : -ENOENT;
}

int cls_cxx_map_set_val(cls_method_context_t context, const std::string& key, byte_buffer *value)
{
    auto& state = *static_cast<storage *>(context);
    byte_buffer arguments;
    ::ceph::encode(std::pair {key, *value}, arguments);
    if (const int result = effect(state, "set-value", arguments); result < 0) {
        return result;
    }
    state.values.insert_or_assign(key, *value);

    return 0;
}

int cls_cxx_map_set_vals(cls_method_context_t context, const std::map<std::string, byte_buffer> *values)
{
    auto& state = *static_cast<storage *>(context);
    byte_buffer arguments;
    ::ceph::encode(*values, arguments);
    if (const int result = effect(state, "set-values", arguments); result < 0) {
        return result;
    }
    for (const auto& [key, value] : *values) {
        state.values.insert_or_assign(key, value);
    }

    return 0;
}

TEST_CASE("BI batch writes preserve accounting, first values and overwrite checks", "[rgw][cls][bi-put]")
{
    for (const bool check_existing : {false, true}) {
        storage state;
        ::rgw_cls_bi_put_entries_op request;
        request.entries.push_back(entry("key", 11));
        REQUIRE(invoke(state, request) == 0);
        state.effects.clear();
        request.entries = {entry("key", 3), entry("key", 7)};
        request.check_existing = check_existing;
        REQUIRE(invoke(state, request) == 0);
        REQUIRE(state.values.size() == 1);
        CHECK(state.values.at("key").contents_equal(request.entries.front().data));
        const auto result = header(state);
        CHECK(result.ver == 2);
        const auto& stats = result.stats.at(::RGWObjCategory::Main);
        CHECK(stats.num_entries == (check_existing ? 2 : 3));
        CHECK(stats.total_size == (check_existing ? 10 : 21));
        CHECK(stats.actual_size == stats.total_size);
        CHECK(stats.total_size_rounded == (check_existing ? 8192 : 12288));
        CHECK(state.effects.size() == (check_existing ? 4 : 3));
    }
}

TEST_CASE("BI batch quota and request truncations precede storage effects", "[rgw][cls][bi-put]")
{
    storage state;
    ::rgw_cls_bi_put_entries_op request;
    request.entries.resize(5);
    CHECK(invoke(state, request) == -E2BIG);
    CHECK(state.effects.empty());
    request.entries = {entry("key", 3)};
    byte_buffer input;
    ::encode(request, input);
    for (unsigned size = 0; size < input.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(input, 0, size);
        CHECK(invoke(state, std::move(prefix)) == -EINVAL);
        CHECK(state.effects.empty());
    }
}

TEST_CASE("BI batch reshard deletion ignores payload and missing-key errors", "[rgw][cls][bi-put]")
{
    storage state;
    state.values["gone"] = entry("gone", 3).data;
    ::rgw_cls_bi_put_entries_op request;
    auto deleted = entry("gone", 1);
    deleted.type = ::BIIndexType::ReshardDeleted;
    deleted.data.clear();
    request.entries.push_back(std::move(deleted));
    REQUIRE(invoke(state, request) == 0);
    CHECK(state.values.empty());
    CHECK(header(state).stats.empty());
    CHECK(invoke(state, request) == 0);
    CHECK(header(state).ver == 2);
}

TEST_CASE("BI batch malformed incoming payload returns an error", "[rgw][cls][bi-put][guard]")
{
    storage state;
    ::rgw_cls_bi_put_entries_op request;
    request.entries.push_back({::BIIndexType::Plain, "key", {}});
    CHECK(invoke(state, request) == -EINVAL);
    CHECK(state.values.empty());
    CHECK(state.header.length() == 0);
}

TEST_CASE("BI empty batches retain header and overwrite-check effects", "[rgw][cls][bi-put]")
{
    for (const bool check_existing : {false, true}) {
        storage state;
        ::rgw_cls_bi_put_entries_op request;
        request.check_existing = check_existing;
        REQUIRE(invoke(state, request) == 0);
        CHECK(header(state).ver == 1);
        CHECK(header(state).stats.empty());
        if (check_existing) {
            CHECK(effect_names(state) == std::vector<std::string> {"read-header", "get-by-keys", "set-values", "write-header"});
        } else {
            CHECK(effect_names(state) == std::vector<std::string> {"read-header", "set-values", "write-header"});
        }
    }
}

TEST_CASE("BI batch storage failures retain their stage and status", "[rgw][cls][bi-put]")
{
    for (const int failure_call : {1, 2, 3, 4}) {
        storage state;
        state.failure_call = failure_call;
        ::rgw_cls_bi_put_entries_op request;
        request.entries.push_back(entry("key", 3));
        request.check_existing = true;
        CHECK(invoke(state, request) == -EAGAIN);
        CHECK(state.effects.size() == static_cast<std::size_t>(failure_call));
        CHECK(state.header.length() == 0);
        // The host records staged effects. Actual transaction rollback is a
        // separate live-OSD check; a header-write failure follows staged values.
        CHECK(state.values.size() == (failure_call == 4 ? 1 : 0));
    }
}

TEST_CASE("BI batch reshard deletion errors are nonfatal and ordered", "[rgw][cls][bi-put]")
{
    storage state;
    state.failure_call = 2;
    ::rgw_cls_bi_put_entries_op request;
    request.entries = {entry("key", 11), {::BIIndexType::ReshardDeleted, "key", {}}, entry("key", 7)};
    REQUIRE(invoke(state, request) == 0);
    REQUIRE(state.values.size() == 1);
    CHECK(state.values.at("key").contents_equal(request.entries.front().data));
    CHECK(header(state).stats.at(::RGWObjCategory::Main).total_size == 18);
    CHECK(header(state).stats.at(::RGWObjCategory::Main).num_entries == 2);
    CHECK(effect_names(state) == std::vector<std::string> {"read-header", "remove", "set-values", "write-header"});
}

TEST_CASE("BI batch structural admission precedes quota and header checks", "[rgw][cls][bi-put]")
{
    storage state;
    ::rgw_cls_bi_put_entries_op request;
    request.entries.assign(5, entry("key", 3));
    byte_buffer bytes;
    ::encode(request, bytes);
    byte_buffer truncated;
    truncated.substr_of(bytes, 0, bytes.length() - 1);
    CHECK(invoke(state, truncated) == -EINVAL);
    CHECK(invoke(state, bytes) == -E2BIG);
    CHECK(state.effects.empty());
    request.entries.resize(4);
    REQUIRE(invoke(state, request) == 0);
    CHECK(header(state).stats.at(::RGWObjCategory::Main).num_entries == 4);
}

TEST_CASE("BI batch reshard guards and corrupt headers precede write effects", "[rgw][cls][bi-put]")
{
    for (const auto status : {::cls_rgw_reshard_status::NOT_RESHARDING, ::cls_rgw_reshard_status::IN_PROGRESS,
                              ::cls_rgw_reshard_status::DONE, ::cls_rgw_reshard_status::IN_LOGRECORD}) {
        for (const unsigned count : {3u, 4u}) {
            storage state;
            ::rgw_bucket_dir_header original;
            original.new_instance.reshard_status = status;
            original.reshardlog_entries = count;
            ::encode(original, state.header);
            const bool blocked = status == ::cls_rgw_reshard_status::IN_PROGRESS ||
                (status == ::cls_rgw_reshard_status::IN_LOGRECORD && count >= state.reshard_threshold);
            CHECK(invoke(state, ::rgw_cls_bi_put_entries_op {}) == (blocked ? -CLS_RGW_ERR_BUSY_RESHARDING : 0));
            CHECK(state.effects.size() == (blocked ? 1 : 3));
            CHECK(header(state).ver == (blocked ? 0 : 1));
        }
    }
    storage state;
    state.header.append("bad", 3);
    CHECK(invoke(state, ::rgw_cls_bi_put_entries_op {}) == -EIO);
    CHECK(effect_names(state) == std::vector<std::string> {"read-header"});
}

TEST_CASE("BI batch accounting keeps native kind, flags, existence and sizes", "[rgw][cls][bi-put]")
{
    for (const auto type : {::BIIndexType::Plain, ::BIIndexType::Instance, ::BIIndexType::Invalid,
                            static_cast<::BIIndexType>(255)}) {
        for (const bool exists : {false, true}) {
            for (const unsigned flags : {0u, 1u}) {
                for (const auto category : {::RGWObjCategory::Main, ::RGWObjCategory::Shadow}) {
                    storage state;
                    ::rgw_bucket_dir_entry value;
                    value.key.name = "key";
                    value.exists = exists;
                    value.flags = flags;
                    value.meta.category = category;
                    value.meta.size = 5;
                    value.meta.accounted_size = 7;
                    ::rgw_cls_bi_put_entries_op request;
                    auto& incoming = request.entries.emplace_back(type, "key", byte_buffer {});
                    ::encode(value, incoming.data);
                    REQUIRE(invoke(state, request) == 0);
                    const bool account = exists && (type == ::BIIndexType::Instance ||
                        (type == ::BIIndexType::Plain && flags == 0));
                    const auto result = header(state);
                    CHECK(result.stats.size() == (account ? 1 : 0));
                    CHECK(state.values.at("key").contents_equal(incoming.data));
                    if (account) {
                        const auto& stats = result.stats.at(category);
                        CHECK(stats.num_entries == 1);
                        CHECK(stats.total_size == 7);
                        CHECK(stats.actual_size == 5);
                        CHECK(stats.total_size_rounded == 4096);
                    }
                }
            }
        }
    }
    storage state;
    ::rgw_bucket_olh_entry olh;
    olh.key.name = "object";
    ::rgw_cls_bi_put_entries_op request;
    auto& incoming = request.entries.emplace_back(::BIIndexType::OLH, "olh", byte_buffer {});
    ::encode(olh, incoming.data);
    REQUIRE(invoke(state, request) == 0);
    CHECK(header(state).stats.empty());
}

TEST_CASE("BI batch malformed stored payload returns an I/O error", "[rgw][cls][bi-put][guard]")
{
    storage state;
    state.values["key"] = {};
    ::rgw_cls_bi_put_entries_op request;
    request.check_existing = true;
    request.entries.push_back(entry("key", 3));
    CHECK(invoke(state, request) == -EIO);
    CHECK(effect_names(state) == std::vector<std::string> {"read-header", "get-by-keys"});
    CHECK(state.header.length() == 0);
    CHECK(state.values.at("key").length() == 0);
}

TEST_CASE("Single BI puts preserve opaque payloads and arbitrary keys", "[rgw][cls][single-put]")
{
    for (const auto kind : {0u, 1u, 2u, 3u, 4u, 255u}) {
        for (const auto& key : {std::string {}, std::string {"a\0b", 3}, std::string(80, 'k')}) {
            storage state;
            state.values[key].append("before", 6);
            ::rgw_cls_bi_put_op request;
            request.entry = {static_cast<::BIIndexType>(kind), key, {}};
            request.entry.data.append("opaque\0\xFF", 8);
            REQUIRE(invoke(state, request) == 0);
            CHECK(state.header.length() == 0);
            if (kind == 4) {
                CHECK(state.values.empty());
                CHECK(effect_names(state) == std::vector<std::string> {"remove"});
            } else {
                REQUIRE(state.values.size() == 1);
                CHECK(state.values.at(key).contents_equal(request.entry.data));
                CHECK(effect_names(state) == std::vector<std::string> {"set-value"});
            }
        }
    }
}

TEST_CASE("Single BI put storage errors retain native nonfatal status", "[rgw][cls][single-put]")
{
    for (const auto kind : {::BIIndexType::Plain, ::BIIndexType::ReshardDeleted}) {
        storage state;
        state.values["key"].append("before", 6);
        state.failure_call = 1;
        ::rgw_cls_bi_put_op request;
        request.entry = {kind, "key", {}};
        CHECK(invoke(state, request) == 0);
        CHECK(state.effects.size() == 1);
        CHECK(state.values.at("key").to_str() == "before");
        CHECK(state.header.length() == 0);
    }
}

TEST_CASE("Single BI put structural errors precede storage effects", "[rgw][cls][single-put]")
{
    ::rgw_cls_bi_put_op request;
    request.entry = entry("key", 3);
    byte_buffer full;
    ::encode(request, full);
    for (unsigned size = 0; size < full.length(); ++size) {
        storage state;
        byte_buffer input;
        input.substr_of(full, 0, size);
        CHECK(invoke(state, std::move(input), "bi_put") == -EINVAL);
        CHECK(state.effects.empty());
        CHECK(state.values.empty());
        CHECK(state.header.length() == 0);
    }
}

namespace {

int invoke_write(storage& state, const auto& request, const std::string_view operation)
{
    byte_buffer input;
    ::encode(request, input);

    return invoke(state, std::move(input), operation);
}

::rgw_bucket_dir_entry object_entry(const storage& state)
{
    ::rgw_bucket_dir_entry result;
    auto cursor = state.values.at("object").cbegin();
    ::decode(result, cursor);

    return result;
}

auto complete_request()
{
    ::rgw_cls_obj_complete_op request;
    request.key.name = "object";
    request.ver.pool = 7;
    request.ver.epoch = 5;
    request.meta.category = ::RGWObjCategory::Main;
    request.meta.size = 17;
    request.meta.accounted_size = 23;

    return request;
}

auto olh_key() { return std::string {"\x80" "1001_object"}; }

auto olh_entry(const storage& state)
{
    ::rgw_bucket_olh_entry result;
    auto cursor = state.values.at(olh_key()).cbegin();
    ::decode(result, cursor);

    return result;
}

auto link_request()
{
    ::rgw_cls_link_olh_op request;
    request.key = {"object", "v1"};
    request.olh_tag = "olh-tag";
    request.op_tag = "operation";
    request.delete_marker = true;
    request.olh_epoch = 5;
    request.meta.category = ::RGWObjCategory::Main;
    request.meta.mtime = ::ceph::real_clock::from_time_t(100);

    return request;
}

} // anonymous namespace

TEST_CASE("Index prepare and complete preserve pending state and accounting", "[rgw][cls][write]")
{
    storage state;
    ::rgw_cls_obj_prepare_op prepare;
    prepare.key.name = "object";
    prepare.tag = "transaction";
    REQUIRE(invoke_write(state, prepare, "bucket_prepare_op") == 0);
    const auto pending = object_entry(state);
    REQUIRE(pending.pending_map.contains(prepare.tag));
    CHECK(pending.pending_map.find(prepare.tag)->second.timestamp ==
          ::ceph::real_clock::from_time_t(1234567890) + std::chrono::nanoseconds {123});
    CHECK_FALSE(pending.exists);
    auto complete = complete_request();
    complete.op_tag = prepare.tag;
    complete.remove_objs = {{"missing", ""}, {"missing", ""}};
    REQUIRE(invoke_write(state, complete, "bucket_complete_op") == 0);
    const auto result = object_entry(state);
    CHECK(result.pending_map.empty());
    CHECK(result.exists);
    CHECK(result.meta.size == 17);
    CHECK(header(state).stats.at(::RGWObjCategory::Main).total_size == 23);
    CHECK(header(state).stats.at(::RGWObjCategory::Main).num_entries == 1);
}

TEST_CASE("Index complete cancellation and deletion preserve native effects", "[rgw][cls][write]")
{
    storage state;
    ::rgw_cls_obj_prepare_op prepare;
    prepare.key.name = "object";
    prepare.tag = "transaction";
    REQUIRE(invoke_write(state, prepare, "bucket_prepare_op") == 0);
    auto complete = complete_request();
    complete.op_tag = prepare.tag;
    complete.op = ::CLS_RGW_OP_CANCEL;
    REQUIRE(invoke_write(state, complete, "bucket_complete_op") == 0);
    CHECK(state.values.empty());
    REQUIRE(invoke_write(state, prepare, "bucket_prepare_op") == 0);
    complete.op = ::CLS_RGW_OP_ADD;
    REQUIRE(invoke_write(state, complete, "bucket_complete_op") == 0);
    complete.op_tag.clear();
    complete.op = ::CLS_RGW_OP_DEL;
    ++complete.ver.epoch;
    REQUIRE(invoke_write(state, complete, "bucket_complete_op") == 0);
    CHECK(state.values.empty());
    CHECK(header(state).stats.at(::RGWObjCategory::Main).num_entries == 0);
}

TEST_CASE("Index write decode and semantic rejection precede storage", "[rgw][cls][write]")
{
    const auto check = [](const auto& request, const std::string_view operation) {
        byte_buffer full;
        ::encode(request, full);
        for (unsigned size = 0; size < full.length(); ++size) {
            storage state;
            byte_buffer input;
            input.substr_of(full, 0, size);
            CHECK(invoke(state, std::move(input), operation) == -EINVAL);
            CHECK(state.effects.empty());
        }
    };
    check(complete_request(), "bucket_complete_op");
    check(link_request(), "bucket_link_olh");
    ::rgw_cls_obj_prepare_op prepare;
    check(prepare, "bucket_prepare_op");
    storage state;
    CHECK(invoke_write(state, prepare, "bucket_prepare_op") == -EINVAL);
    CHECK(state.effects.empty());
    ::rgw_cls_trim_olh_log_op trim;
    trim.olh = {"object", "bad-instance"};
    check(trim, "bucket_trim_olh_log");
    CHECK(invoke_write(state, trim, "bucket_trim_olh_log") == -EINVAL);
    CHECK(state.effects.empty());
}

TEST_CASE("Index complete preserves storage error stages and reshard rejection", "[rgw][cls][write]")
{
    for (const int call : {1, 2, 3, 4}) {
        storage state;
        ::rgw_cls_obj_prepare_op prepare;
        prepare.key.name = "object";
        prepare.tag = "transaction";
        REQUIRE(invoke_write(state, prepare, "bucket_prepare_op") == 0);
        state.effects.clear();
        state.failure_call = call;
        auto complete = complete_request();
        complete.op_tag = prepare.tag;
        CHECK(invoke_write(state, complete, "bucket_complete_op") == (call == 1 ? -EINVAL : -EAGAIN));
        CHECK(state.effects.size() == static_cast<unsigned>(call));
    }
    storage state;
    ::rgw_bucket_dir_header blocked;
    blocked.new_instance.reshard_status = ::cls_rgw_reshard_status::IN_PROGRESS;
    ::encode(blocked, state.header);
    CHECK(invoke_write(state, complete_request(), "bucket_complete_op") == -CLS_RGW_ERR_BUSY_RESHARDING);
    CHECK(effect_names(state) == std::vector<std::string> {"read-header"});
}

TEST_CASE("Index suggestions retain live pending entries and expire the final entry",
          "[rgw][cls][suggest]")
{
    for (const bool retain_pending : {true, false}) {
        storage state;
        ::rgw_bucket_dir_entry current;
        current.key.name = "object";
        ::rgw_bucket_pending_info pending;
        pending.op = ::CLS_RGW_OP_ADD;
        current.pending_map.emplace("expired", pending);
        if (retain_pending) {
            pending.op = ::CLS_RGW_OP_DEL;
            pending.timestamp = ::ceph::real_clock::from_time_t(1234567890);
            current.pending_map.emplace("retained", pending);
        }
        ::encode(current, state.values[current.key.name]);

        ::rgw_bucket_dir_entry suggestion;
        suggestion.key = current.key;
        byte_buffer input;
        ::ceph::encode(static_cast<std::uint8_t>(CEPH_RGW_REMOVE), input);
        ::encode(suggestion, input);
        REQUIRE(invoke(state, std::move(input), "dir_suggest_changes") == 0);
        CHECK(state.values.contains(current.key.name) == retain_pending);
        if (retain_pending) {
            CHECK(effect_names(state) == std::vector<std::string> {"read-header", "get-value"});
        } else {
            CHECK(effect_names(state) ==
                  std::vector<std::string> {"read-header", "get-value", "remove"});
        }
    }
}

TEST_CASE("OLH trim retains its inclusive epoch and tag guards", "[rgw][cls][write]")
{
    storage state;
    ::rgw_bucket_olh_entry original;
    original.key.name = "object";
    original.tag = "olh-tag";
    original.pending_log = {{1, {}}, {2, {}}, {3, {}}};
    ::encode(original, state.values[olh_key()]);
    ::rgw_cls_trim_olh_log_op trim;
    trim.olh.name = "object";
    trim.olh_tag = original.tag;
    trim.ver = 2;
    REQUIRE(invoke_write(state, trim, "bucket_trim_olh_log") == 0);
    CHECK(olh_entry(state).pending_log.size() == 1);
    CHECK(olh_entry(state).pending_log.contains(3));
    state.effects.clear();
    trim.olh_tag = "wrong";
    CHECK(invoke_write(state, trim, "bucket_trim_olh_log") == -ECANCELED);
    CHECK(effect_names(state) == std::vector<std::string> {"get-value"});
}

TEST_CASE("OLH trim handles sparse epochs and the maximum inclusive boundary", "[rgw][cls][write]")
{
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    for (const auto limit : {std::uint64_t {0}, std::uint64_t {1}, std::uint64_t {2},
                            maximum - 2, maximum - 1, maximum}) {
        storage state;
        ::rgw_bucket_olh_entry original;
        original.key.name = "object";
        original.tag = "olh-tag";
        original.pending_log = {{1, {}}, {maximum - 1, {}}, {maximum, {}}};
        ::encode(original, state.values[olh_key()]);
        ::rgw_cls_trim_olh_log_op trim;
        trim.olh.name = original.key.name;
        trim.olh_tag = original.tag;
        trim.ver = limit;
        REQUIRE(invoke_write(state, trim, "bucket_trim_olh_log") == 0);
        const auto retained = olh_entry(state);
        CHECK(retained.tag == original.tag);
        CHECK(retained.pending_log.contains(1) == (limit < 1));
        CHECK(retained.pending_log.contains(maximum - 1) == (limit < maximum - 1));
        CHECK(retained.pending_log.contains(maximum) == (limit < maximum));
        const std::size_t expected_size =
            (limit < 1) + (limit < maximum - 1) + (limit < maximum);
        CHECK(retained.pending_log.size() == expected_size);
    }
}

TEST_CASE("OLH link preserves version, time and tag behavior", "[rgw][cls][write]")
{
    storage state;
    auto request = link_request();
    REQUIRE(invoke_write(state, request, "bucket_link_olh") == 0);
    CHECK(olh_entry(state).tag == request.olh_tag);
    CHECK(olh_entry(state).key.instance == "v1");
    CHECK(olh_entry(state).delete_marker);
    request.olh_tag = "wrong";
    CHECK(invoke_write(state, request, "bucket_link_olh") == -ECANCELED);
    request.olh_tag = "olh-tag";
    request.unmod_since = ::ceph::real_clock::from_time_t(99);
    REQUIRE(invoke_write(state, request, "bucket_link_olh") == 0);
    CHECK(olh_entry(state).pending_log.rbegin()->second.back().op == ::CLS_RGW_OLH_OP_STALE);
    request.unmod_since = {};
    request.log_op = true;
    request.zones_trace.insert("zone", "location");
    ++request.olh_epoch;
    REQUIRE(invoke_write(state, request, "bucket_link_olh") == 0);
}

TEST_CASE("OLH refresh copies instance metadata to its listing only", "[rgw][cls][write]")
{
    storage state;
    const auto link = link_request();
    REQUIRE(invoke_write(state, link, "bucket_link_olh") == 0);
    const auto instance = std::ranges::find_if(state.values, [](const auto& item) {
        return item.first.starts_with("\x80" "1000_");
    });
    REQUIRE(instance != state.values.end());
    ::rgw_bucket_dir_entry updated;
    auto cursor = instance->second.cbegin();
    ::decode(updated, cursor);
    updated.meta.etag = "updated";
    instance->second.clear();
    ::encode(updated, instance->second);
    const auto stored_instance = instance->second;
    const auto stored_head = state.values.at(olh_key());
    state.effects.clear();
    ::rgw_cls_refresh_instance_op refresh;
    refresh.key = link.key;
    REQUIRE(invoke_write(state, refresh, "bucket_refresh_instance") == 0);
    CHECK(state.values.at(instance->first).contents_equal(stored_instance));
    CHECK(state.values.at(olh_key()).contents_equal(stored_head));
    CHECK(header(state).stats.empty());
    CHECK(effect_names(state) == std::vector<std::string> {"read-header", "get-value", "set-value", "write-header"});
    std::pair<std::string, byte_buffer> written;
    cursor = state.effects[2].second.cbegin();
    ::ceph::decode(written, cursor);
    CHECK(written.first != instance->first);
    CHECK(written.second.contents_equal(stored_instance));
}

TEST_CASE("OLH refresh skips missing and ordinary unversioned entries", "[rgw][cls][write]")
{
    for (const bool exists : {false, true}) {
        storage state;
        if (exists) { state.values["object"] = entry("object", 17).data; }
        const auto before = state.values;
        ::rgw_cls_refresh_instance_op request;
        request.key.name = "object";
        REQUIRE(invoke_write(state, request, "bucket_refresh_instance") == 0);
        CHECK(effect_names(state) == std::vector<std::string> {"read-header", "get-value"});
        CHECK(state.values.size() == before.size());
        CHECK(state.header.length() == 0);
    }
}

TEST_CASE("OLH clear removes version markers while preserving ordinary entries", "[rgw][cls][write]")
{
    for (const bool marker : {false, true}) {
        storage state;
        ::rgw_bucket_olh_entry head;
        head.key.name = "object";
        head.tag = "olh-tag";
        ::encode(head, state.values[olh_key()]);
        ::rgw_bucket_dir_entry plain;
        plain.key.name = "object";
        plain.flags = marker ? ::rgw_bucket_dir_entry::FLAG_VER_MARKER : 0;
        ::encode(plain, state.values["object"]);
        ::rgw_cls_bucket_clear_olh_op request;
        request.key.name = "object";
        request.olh_tag = "wrong";
        CHECK(invoke_write(state, request, "bucket_clear_olh") == -ECANCELED);
        CHECK(state.values.size() == 2);
        request.olh_tag = head.tag;
        REQUIRE(invoke_write(state, request, "bucket_clear_olh") == 0);
        CHECK_FALSE(state.values.contains(olh_key()));
        CHECK(state.values.contains("object") == !marker);
    }
}

TEST_CASE("OLH unlink and clear retain last-version removal behavior", "[rgw][cls][write]")
{
    storage state;
    const auto link = link_request();
    REQUIRE(invoke_write(state, link, "bucket_link_olh") == 0);
    ::rgw_cls_unlink_instance_op unlink;
    unlink.key = link.key;
    unlink.op_tag = "unlink";
    unlink.olh_epoch = 6;
    // The existing unlink operation does not compare this tag for an existing
    // head. Retain that behavior;clear has a separate tag guard.
    unlink.olh_tag = "different";
    REQUIRE(invoke_write(state, unlink, "bucket_unlink_instance") == 0);
    CHECK(olh_entry(state).tag == link.olh_tag);
    CHECK_FALSE(olh_entry(state).exists);
    CHECK(olh_entry(state).pending_removal);
    CHECK(olh_entry(state).key.name == link.key.name);
    CHECK(olh_entry(state).key.instance.empty());
    ::rgw_cls_bucket_clear_olh_op clear;
    clear.key.name = "object";
    clear.olh_tag = link.olh_tag;
    REQUIRE(invoke_write(state, clear, "bucket_clear_olh") == 0);
    CHECK(state.values.empty());
}

TEST_CASE("OLH unlink promotes the next version and keeps the stored head tag", "[rgw][cls][write]")
{
    storage state;
    auto link = link_request();
    REQUIRE(invoke_write(state, link, "bucket_link_olh") == 0);
    link.key.instance = "v2";
    ++link.olh_epoch;
    REQUIRE(invoke_write(state, link, "bucket_link_olh") == 0);
    REQUIRE(olh_entry(state).key.instance == "v2");
    ::rgw_cls_unlink_instance_op request;
    request.key = link.key;
    request.op_tag = "unlink";
    request.olh_tag = "different";
    request.olh_epoch = 7;
    request.log_op = true;
    request.zones_trace.insert("zone", "location");
    REQUIRE(invoke_write(state, request, "bucket_unlink_instance") == 0);
    CHECK(olh_entry(state).key.instance == "v1");
    CHECK(olh_entry(state).exists);
    CHECK(olh_entry(state).delete_marker);
    CHECK(olh_entry(state).tag == link.olh_tag);
}

TEST_CASE("OLH successor lookup distinguishes another object from invalid stored data",
          "[rgw][cls][write]")
{
    for (const bool malformed : {false, true}) {
        INFO("malformed=" << malformed);
        storage state;
        auto link = link_request();
        REQUIRE(invoke_write(state, link, "bucket_link_olh") == 0);
        link.key.instance = "v2";
        ++link.olh_epoch;
        REQUIRE(invoke_write(state, link, "bucket_link_olh") == 0);
        const auto previous = std::ranges::find_if(state.values, [](const auto& item) {
            if (!item.first.starts_with("object")) {
                return false;
            }
            ::rgw_bucket_dir_entry entry;
            auto cursor = item.second.cbegin();
            ::decode(entry, cursor);
            return entry.key.instance == "v1";
        });
        REQUIRE(previous != state.values.end());
        previous->second.clear();
        if (!malformed) {
            ::rgw_bucket_dir_entry other;
            other.key = {"object-other", "v1"};
            ::encode(other, previous->second);
        }
        const auto stored_head = state.values.at(olh_key());
        const auto stored_header = state.header;
        ::rgw_cls_unlink_instance_op request;
        request.key = link.key;
        request.op_tag = "unlink";
        request.olh_epoch = 7;
        if (malformed) {
            CHECK(invoke_write(state, request, "bucket_unlink_instance") == -EIO);
            CHECK(state.values.at(olh_key()).contents_equal(stored_head));
            CHECK(state.header.contents_equal(stored_header));
            continue;
        }
        REQUIRE(invoke_write(state, request, "bucket_unlink_instance") == 0);
        CHECK_FALSE(olh_entry(state).exists);
        CHECK(olh_entry(state).pending_removal);
        CHECK(olh_entry(state).key.name == link.key.name);
        CHECK(olh_entry(state).key.instance.empty());
    }
}

TEST_CASE("Remaining OLH request rejection and unlink failures preserve native stages", "[rgw][cls][write]")
{
    const auto reject_prefixes = [](const auto& request, const std::string_view operation) {
        byte_buffer full;
        ::encode(request, full);
        for (unsigned size = 0; size < full.length(); ++size) {
            storage state;
            byte_buffer input;
            input.substr_of(full, 0, size);
            CHECK(invoke(state, std::move(input), operation) == -EINVAL);
            CHECK(state.effects.empty());
        }
    };
    ::rgw_cls_refresh_instance_op refresh;
    refresh.key.name = "object";
    reject_prefixes(refresh, "bucket_refresh_instance");
    ::rgw_cls_bucket_clear_olh_op clear;
    clear.key = {"object", "bad-instance"};
    reject_prefixes(clear, "bucket_clear_olh");
    storage invalid;
    CHECK(invoke_write(invalid, clear, "bucket_clear_olh") == -EINVAL);
    CHECK(invalid.effects.empty());
    ::rgw_cls_unlink_instance_op unlink;
    unlink.key = link_request().key;
    unlink.olh_epoch = 6;
    reject_prefixes(unlink, "bucket_unlink_instance");
    CHECK(invoke_write(invalid, unlink, "bucket_unlink_instance") == -ENOENT);

    storage baseline;
    REQUIRE(invoke_write(baseline, link_request(), "bucket_link_olh") == 0);
    baseline.effects.clear();
    REQUIRE(invoke_write(baseline, unlink, "bucket_unlink_instance") == 0);
    for (unsigned call = 1; call <= baseline.effects.size(); ++call) {
        storage state;
        REQUIRE(invoke_write(state, link_request(), "bucket_link_olh") == 0);
        state.effects.clear();
        state.failure_call = call;
        CHECK(invoke_write(state, unlink, "bucket_unlink_instance") == -EAGAIN);
        CHECK(state.effects.size() == call);
    }
}

namespace {

const std::string log_prefix {"\x80" "0_"};
const std::string instance_prefix {"\x80" "1000_"};

storage log_storage()
{
    storage state;
    for (const std::string id : {"a", "b", "c"}) {
        ::rgw_bi_log_entry row;
        row.id = id;
        row.object = std::string {"n\0m", 3};
        row.tag = std::string(80, 't');
        row.op = ::CLS_RGW_OP_DEL;
        row.state = ::CLS_RGW_STATE_COMPLETE;
        row.zones_trace.insert("zone", "location");
        ::encode(row, state.values[log_prefix + id]);
    }
    state.values["plain"] = {};
    state.values[instance_prefix + "unrelated"] = {};

    return state;
}

auto log_reply(const storage& state)
{
    ::cls_rgw_bi_log_list_ret reply;
    auto cursor = state.reply.cbegin();
    ::decode(reply, cursor);
    CHECK(cursor.end());

    return reply;
}

} // anonymous namespace

TEST_CASE("Bilog module list preserves page markers truncation and namespace bounds", "[rgw][cls][bilog]")
{
    storage state = log_storage();
    const auto original = state.values;
    ::cls_rgw_bi_log_list_op request;
    request.max = 1;
    for (const std::string id : {"a", "b", "c"}) {
        REQUIRE(invoke_write(state, request, "bi_log_list") == 0);
        const auto reply = log_reply(state);
        REQUIRE(reply.entries.size() == 1);
        CHECK(reply.entries.front().id == id);
        CHECK(reply.entries.front().tag == std::string(80, 't'));
        CHECK(reply.truncated);
        request.marker = id;
    }
    REQUIRE(invoke_write(state, request, "bi_log_list") == 0);
    CHECK(log_reply(state).entries.empty());
    CHECK_FALSE(log_reply(state).truncated);
    request.marker.clear();
    request.max = 100;
    REQUIRE(invoke_write(state, request, "bi_log_list") == 0);
    const auto all = log_reply(state);
    CHECK(all.entries.size() == 3);
    CHECK_FALSE(all.truncated);
    CHECK(state.values == original);
    CHECK(std::ranges::all_of(effect_names(state), [](const auto& name) { return name == "get-values"; }));
}

TEST_CASE("Bilog module list retains zero limits empty results corruption and read errors", "[rgw][cls][bilog]")
{
    ::cls_rgw_bi_log_list_op request;
    request.max = 0;
    storage state = log_storage();
    REQUIRE(invoke_write(state, request, "bi_log_list") == 0);
    CHECK(log_reply(state).entries.empty());
    CHECK(log_reply(state).truncated);
    state = {};
    request.max = 100;
    REQUIRE(invoke_write(state, request, "bi_log_list") == 0);
    CHECK(log_reply(state).entries.empty());
    CHECK_FALSE(log_reply(state).truncated);
    state = log_storage();
    state.values[log_prefix + "b"].clear();
    CHECK(invoke_write(state, request, "bi_log_list") == -EIO);
    CHECK(state.reply.length() == 0);
    state = log_storage();
    // Native iteration compares > rather than >= at the namespace sentinel.
    state.values[instance_prefix] = {};
    CHECK(invoke_write(state, request, "bi_log_list") == -EIO);
    CHECK(state.reply.length() == 0);
    state = log_storage();
    state.failure_call = 1;
    CHECK(invoke_write(state, request, "bi_log_list") == -EAGAIN);
    CHECK(state.effects.size() == 1);
}

TEST_CASE("Bilog module trim preserves exclusive start inclusive end and empty-end namespace", "[rgw][cls][bilog]")
{
    ::cls_rgw_bi_log_trim_op request;
    request.start_marker = "a";
    request.end_marker = "b";
    storage state = log_storage();
    REQUIRE(invoke_write(state, request, "bi_log_trim") == 0);
    CHECK(state.values.contains(log_prefix + "a"));
    CHECK_FALSE(state.values.contains(log_prefix + "b"));
    CHECK(state.values.contains(log_prefix + "c"));
    CHECK((effect_names(state) == std::vector<std::string> {"get-keys", "remove-range"}));
    request.start_marker.clear();
    request.end_marker.clear();
    REQUIRE(invoke_write(state, request, "bi_log_trim") == 0);
    CHECK(state.values.size() == 2);
    CHECK(state.values.contains("plain"));
    CHECK(state.values.contains(instance_prefix + "unrelated"));

    state = log_storage();
    state.values[log_prefix + std::string {"b\0", 2}] = {};
    request.end_marker = "b";
    REQUIRE(invoke_write(state, request, "bi_log_trim") == 0);
    CHECK_FALSE(state.values.contains(log_prefix + "b"));
    CHECK(state.values.contains(log_prefix + std::string {"b\0", 2}));
}

TEST_CASE("Bilog module trim retains empty-range and storage failure statuses", "[rgw][cls][bilog]")
{
    ::cls_rgw_bi_log_trim_op request;
    request.start_marker = "z";
    request.end_marker = "zz";
    storage state = log_storage();
    CHECK(invoke_write(state, request, "bi_log_trim") == -ENODATA);
    CHECK((effect_names(state) == std::vector<std::string> {"get-keys"}));
    state = {};
    request.start_marker.clear();
    request.end_marker.clear();
    CHECK(invoke_write(state, request, "bi_log_trim") == -ENODATA);
    for (const unsigned failure : {1, 2}) {
        state = log_storage();
        const auto original = state.values;
        state.failure_call = failure;
        CHECK(invoke_write(state, request, "bi_log_trim") == -EAGAIN);
        CHECK(state.effects.size() == failure);
        CHECK(state.values == original);
    }
}

TEST_CASE("Bilog module rejects every incomplete request before storage access", "[rgw][cls][bilog]")
{
    const auto check = [](const auto& request, const std::string_view operation) {
        byte_buffer full;
        ::encode(request, full);
        for (unsigned size = 0; size < full.length(); ++size) {
            byte_buffer prefix;
            prefix.substr_of(full, 0, size);
            storage state;
            CHECK(invoke(state, prefix, operation) == -EINVAL);
            CHECK(state.effects.empty());
            CHECK(state.reply.length() == 0);
        }
    };
    ::cls_rgw_bi_log_list_op list;
    list.marker = std::string {"m\0r", 3};
    list.max = 100;
    check(list, "bi_log_list");
    ::cls_rgw_bi_log_trim_op trim;
    trim.start_marker = "start";
    trim.end_marker = "end";
    check(trim, "bi_log_trim");
}

namespace {

storage directory_storage()
{
    storage state;
    for (const std::string name : {"a", "dir/x", "dir/y", "z"}) {
        state.values[name] = entry(name, 11).data;
    }
    state.values[log_prefix + "unrelated"] = {};
    ::rgw_bucket_dir_header header;
    header.ver = 17;
    header.max_marker = "max-marker";
    header.stats[::RGWObjCategory::Main].num_entries = 4;
    ::encode(header, state.header);

    return state;
}

auto directory_reply(const storage& state)
{
    ::rgw_cls_list_ret result;
    auto cursor = state.reply.cbegin();
    ::decode(result, cursor);
    CHECK(cursor.end());

    return result;
}

} // anonymous namespace

TEST_CASE("Bucket directory module preserves header-only reads and ordinary page markers", "[rgw][cls][bucket-list]")
{
    storage state = directory_storage();
    const auto original = state.values;
    ::rgw_cls_list_op request;
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    CHECK(directory_reply(state).dir.header.ver == 17);
    CHECK(directory_reply(state).dir.m.empty());
    CHECK_FALSE(directory_reply(state).is_truncated);
    CHECK((effect_names(state) == std::vector<std::string> {"read-header"}));
    request.num_entries = 2;
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    auto result = directory_reply(state);
    REQUIRE(result.dir.m.size() == 2);
    CHECK(result.dir.m.contains("a"));
    CHECK(result.dir.m.contains("dir/x"));
    CHECK(result.is_truncated);
    CHECK(result.marker.name == "dir/x");
    request.start_obj = result.marker;
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    result = directory_reply(state);
    REQUIRE(result.dir.m.size() == 2);
    CHECK(result.dir.m.contains("dir/y"));
    CHECK(result.dir.m.contains("z"));
    request.start_obj = result.marker;
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    CHECK(directory_reply(state).dir.m.empty());
    CHECK_FALSE(directory_reply(state).is_truncated);
    CHECK(state.values == original);
}

TEST_CASE("Bucket directory module retains delimiter proxies prefix and namespace skipping", "[rgw][cls][bucket-list]")
{
    storage state = directory_storage();
    ::rgw_cls_list_op request;
    request.num_entries = 10;
    request.delimiter = "/";
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    auto result = directory_reply(state);
    REQUIRE(result.dir.m.size() == 3);
    CHECK(result.dir.m.contains("a"));
    CHECK(result.dir.m.contains("z"));
    REQUIRE(result.dir.m.contains("dir/"));
    CHECK(result.dir.m.at("dir/").is_common_prefix());
    CHECK_FALSE(result.is_truncated);
    request.filter_prefix = "dir/";
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    result = directory_reply(state);
    REQUIRE(result.dir.m.size() == 2);
    CHECK(result.dir.m.contains("dir/x"));
    CHECK(result.dir.m.contains("dir/y"));
    request.filter_prefix = "missing";
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    CHECK(directory_reply(state).dir.m.empty());

    request = {};
    request.num_entries = 10;
    const std::string unicode {"\xC3\xA9"};
    state.values[unicode] = entry(unicode, 11).data;
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    result = directory_reply(state);
    CHECK(result.dir.m.size() == 5);
    CHECK(result.dir.m.contains(unicode));
    CHECK_FALSE(result.dir.m.contains(log_prefix + "unrelated"));
}

TEST_CASE("Bucket directory module retains version filtering and missing instance markers", "[rgw][cls][bucket-list]")
{
    storage state;
    auto link = link_request();
    REQUIRE(invoke_write(state, link, "bucket_link_olh") == 0);
    link.key.instance = "v2";
    ++link.olh_epoch;
    REQUIRE(invoke_write(state, link, "bucket_link_olh") == 0);
    ::rgw_cls_list_op request;
    request.num_entries = 10;
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    CHECK(directory_reply(state).dir.m.empty());
    request.list_versions = true;
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    const auto result = directory_reply(state);
    REQUIRE(result.dir.m.size() == 2);
    CHECK(std::ranges::all_of(result.dir.m, [](const auto& row) {
        return row.second.is_delete_marker() && row.second.key.name == "object";
    }));
    request.start_obj = {"object", "missing"};
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    CHECK(directory_reply(state).dir.m.empty());
}

TEST_CASE("Bucket directory module preserves bounded empty-page advance and retry", "[rgw][cls][bucket-list]")
{
    storage state;
    for (unsigned index = 0; index < 9; ++index) {
        const auto name = "a" + std::to_string(index);
        ::rgw_bucket_dir_entry row;
        row.key.name = name;
        row.flags = ::rgw_bucket_dir_entry::FLAG_VER_MARKER;
        ::encode(row, state.values[name]);
    }
    ::rgw_cls_list_op request;
    request.num_entries = 1;
    REQUIRE(invoke_write(state, request, "bucket_list") == ::RGWBIAdvanceAndRetryError);
    const auto result = directory_reply(state);
    CHECK(result.dir.m.empty());
    CHECK(result.is_truncated);
    CHECK(result.marker.name == "a7");
    CHECK(state.effects.size() == 9);
    CHECK(std::ranges::count(effect_names(state), std::string {"get-values"}) == 8);
    request.start_obj = result.marker;
    REQUIRE(invoke_write(state, request, "bucket_list") == 0);
    CHECK(directory_reply(state).dir.m.empty());
    CHECK_FALSE(directory_reply(state).is_truncated);
}

TEST_CASE("Bucket directory module retains corruption storage errors and request rejection", "[rgw][cls][bucket-list]")
{
    ::rgw_cls_list_op request;
    request.num_entries = 10;
    storage state = directory_storage();
    state.values.at("a").clear();
    CHECK(invoke_write(state, request, "bucket_list") == -EINVAL);
    CHECK(state.reply.length() == 0);
    state = directory_storage();
    state.header.clear();
    state.header.append("x", 1);
    CHECK(invoke_write(state, request, "bucket_list") == -EIO);
    for (const unsigned failure : {1, 2}) {
        state = directory_storage();
        state.failure_call = failure;
        CHECK(invoke_write(state, request, "bucket_list") == -EAGAIN);
        CHECK(state.effects.size() == failure);
    }
    byte_buffer full;
    ::encode(request, full);
    for (unsigned size = 0; size < full.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(full, 0, size);
        state = directory_storage();
        CHECK(invoke(state, prefix, "bucket_list") == -EINVAL);
        CHECK(state.effects.empty());
    }
}

namespace {

storage stat_storage()
{
    storage state;
    ::rgw_bucket_dir_header value;
    value.ver = 7;
    value.master_ver = 19;
    value.tag_timeout = 3;
    value.syncstopped = true;
    value.max_marker = "retained";
    auto& stats = value.stats[::RGWObjCategory::Main];
    stats.total_size = 20;
    stats.total_size_rounded = 30;
    stats.num_entries = 4;
    stats.actual_size = 50;
    ::encode(value, state.header);

    return state;
}

auto stat_value(const std::uint64_t total, const std::uint64_t rounded,
                const std::uint64_t count, const std::uint64_t actual)
{
    ::rgw_bucket_category_stats result;
    result.total_size = total;
    result.total_size_rounded = rounded;
    result.num_entries = count;
    result.actual_size = actual;

    return result;
}

auto check_reply(const storage& state)
{
    ::rgw_cls_check_index_ret result;
    auto cursor = state.reply.cbegin();
    ::decode(result, cursor);
    CHECK(cursor.end());

    return result;
}

storage check_storage()
{
    auto state = stat_storage();
    auto add = [&](const std::string& index, const std::string& name, const std::uint64_t size,
                   const std::uint64_t accounted, const std::uint16_t flags, const bool exists) {
        ::rgw_bucket_dir_entry row;
        row.key.name = name;
        row.exists = exists;
        row.meta.category = ::RGWObjCategory::Main;
        row.meta.size = size;
        row.meta.accounted_size = accounted;
        row.flags = flags;
        ::encode(row, state.values[index]);
    };
    add("plain", "plain", 100, 80, 0, true);
    add("\xC3\xA9", "\xC3\xA9", 11, 11, 0, true);
    add("version-listing", "version-listing", 999, 999, ::rgw_bucket_dir_entry::FLAG_VER, true);
    add("absent", "absent", 999, 999, 0, false);
    add(instance_prefix + "version", "instance", 200, 160, ::rgw_bucket_dir_entry::FLAG_VER, true);
    state.values[std::string {"\x80" "1001_unrelated"}] = {};

    return state;
}

} // anonymous namespace

TEST_CASE("Index statistics module retains absolute incremental subtraction and metadata rules", "[rgw][cls][stats]")
{
    ::rgw_cls_bucket_update_stats_op request;
    request.stats[::RGWObjCategory::Main] = stat_value(10, 20, 2, 30);
    request.dec_stats[::RGWObjCategory::Main] = stat_value(3, 5, 1, 7);
    storage state = stat_storage();
    REQUIRE(invoke_write(state, request, "bucket_update_stats") == 0);
    auto result = header(state);
    CHECK(result.stats.at(::RGWObjCategory::Main) == stat_value(27, 45, 5, 73));
    CHECK(result.ver == 8);
    CHECK(result.master_ver == 19);
    CHECK(result.tag_timeout == 3);
    CHECK(result.syncstopped);
    CHECK(result.max_marker == "retained");
    CHECK((effect_names(state) == std::vector<std::string> {"read-header", "write-header"}));

    state = stat_storage();
    request.absolute = true;
    const auto original = state.header;
    CHECK(invoke_write(state, request, "bucket_update_stats") == -EINVAL);
    CHECK(state.header.contents_equal(original));
    CHECK((effect_names(state) == std::vector<std::string> {"read-header"}));
    request.dec_stats.clear();
    REQUIRE(invoke_write(state, request, "bucket_update_stats") == 0);
    CHECK(header(state).stats.at(::RGWObjCategory::Main) == stat_value(10, 20, 2, 30));

    state = {};
    request.absolute = false;
    request.stats.clear();
    request.dec_stats[static_cast<::RGWObjCategory>(255)] = stat_value(1, 1, 1, 1);
    REQUIRE(invoke_write(state, request, "bucket_update_stats") == 0);
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    CHECK(header(state).stats.at(static_cast<::RGWObjCategory>(255)) ==
          stat_value(maximum, maximum, maximum, maximum));
    request.dec_stats.clear();
    REQUIRE(invoke_write(state, request, "bucket_update_stats") == 0);
    CHECK(header(state).ver == 2);
}

TEST_CASE("Tag timeout changes only timeout and header generation", "[rgw][cls][stats]")
{
    for (const std::uint64_t timeout : {std::uint64_t {0}, std::numeric_limits<std::uint64_t>::max()}) {
        storage state = stat_storage();
        ::rgw_cls_tag_timeout_op request {.tag_timeout = timeout};
        REQUIRE(invoke_write(state, request, "bucket_set_tag_timeout") == 0);
        const auto value = header(state);
        CHECK(value.tag_timeout == timeout);
        CHECK(value.ver == 8);
        CHECK(value.master_ver == 19);
        CHECK(value.syncstopped);
        CHECK(value.stats.at(::RGWObjCategory::Main) == stat_value(20, 30, 4, 50));
    }
}

TEST_CASE("Index stat request errors preserve storage and native I/O stages", "[rgw][cls][stats]")
{
    const auto check = [](const auto& request, const std::string_view operation) {
        byte_buffer full;
        ::encode(request, full);
        for (unsigned size = 0; size < full.length(); ++size) {
            auto state = stat_storage();
            byte_buffer prefix;
            prefix.substr_of(full, 0, size);
            CHECK(invoke(state, prefix, operation) == -EINVAL);
            CHECK(state.effects.empty());
        }
        for (const unsigned failure : {1, 2}) {
            auto state = stat_storage();
            const auto original = state.header;
            state.failure_call = failure;
            CHECK(invoke_write(state, request, operation) == -EAGAIN);
            CHECK(state.effects.size() == failure);
            CHECK(state.header.contents_equal(original));
        }
        auto state = stat_storage();
        state.header.clear();
        state.header.append("x", 1);
        CHECK(invoke_write(state, request, operation) == -EIO);
    };
    ::rgw_cls_bucket_update_stats_op update;
    update.stats[::RGWObjCategory::Main] = stat_value(1, 2, 3, 4);
    check(update, "bucket_update_stats");
    check(::rgw_cls_tag_timeout_op {.tag_timeout = 17}, "bucket_set_tag_timeout");
}

TEST_CASE("Index check and rebuild retain accounting metadata and storage effects", "[rgw][cls][stats]")
{
    auto state = check_storage();
    const auto original = state.header;
    REQUIRE(invoke(state, {}, "bucket_check_index") == 0);
    const auto result = check_reply(state);
    byte_buffer existing;
    ::encode(result.existing_header, existing);
    CHECK(existing.contents_equal(original));
    const auto& calculated = result.calculated_header;
    CHECK(calculated.stats.at(::RGWObjCategory::Main) == stat_value(251, 12288, 3, 311));
    CHECK(calculated.ver == 7);
    CHECK(calculated.tag_timeout == 3);
    CHECK(calculated.syncstopped);
    CHECK(calculated.master_ver == 0);
    CHECK(calculated.max_marker.empty());
    CHECK(state.header.contents_equal(original));
    state.effects.clear();
    REQUIRE(invoke(state, {}, "bucket_rebuild_index") == 0);
    CHECK(header(state).stats.at(::RGWObjCategory::Main) == calculated.stats.at(::RGWObjCategory::Main));
    CHECK(header(state).ver == 8);
    CHECK(header(state).master_ver == 0);
    CHECK(effect_names(state).back() == "write-header");
}

TEST_CASE("Index check and rebuild page through more than one thousand entries", "[rgw][cls][stats]")
{
    storage state;
    for (unsigned index = 0; index < 1003; ++index) {
        const auto name = "object-" + std::to_string(index);
        state.values[name] = entry(name, 1).data;
    }
    REQUIRE(invoke(state, {}, "bucket_check_index") == 0);
    CHECK(check_reply(state).calculated_header.stats.at(::RGWObjCategory::Main) ==
          stat_value(1003, 1003 * 4096, 1003, 1003));
    CHECK(std::ranges::count(effect_names(state), std::string {"get-values"}) >= 3);
    REQUIRE(invoke(state, {}, "bucket_rebuild_index") == 0);
    CHECK(header(state).stats.at(::RGWObjCategory::Main).num_entries == 1003);
}

TEST_CASE("Index check and rebuild retain corruption reshard guards and failure stages", "[rgw][cls][stats]")
{
    for (const std::string operation : {"bucket_check_index", "bucket_rebuild_index"}) {
        auto baseline = check_storage();
        REQUIRE(invoke(baseline, {}, operation) == 0);
        for (unsigned failure = 1; failure <= baseline.effects.size(); ++failure) {
            auto state = check_storage();
            const auto original = state.header;
            state.failure_call = failure;
            CHECK(invoke(state, {}, operation) == -EAGAIN);
            CHECK(state.effects.size() == failure);
            CHECK(state.header.contents_equal(original));
        }
        for (const auto& corrupt : {std::string {"plain"}, instance_prefix + "version"}) {
            auto state = check_storage();
            state.values.at(corrupt).clear();
            CHECK(invoke(state, {}, operation) == -EIO);
        }
    }
    auto state = check_storage();
    auto value = header(state);
    value.new_instance.reshard_status = ::cls_rgw_reshard_status::IN_PROGRESS;
    state.header.clear();
    ::encode(value, state.header);
    CHECK(invoke(state, {}, "bucket_rebuild_index") == -CLS_RGW_ERR_BUSY_RESHARDING);
    CHECK((effect_names(state) == std::vector<std::string> {"read-header"}));
    CHECK(invoke(state, {}, "bucket_check_index") == 0);
}

int cls_cxx_stat(cls_method_context_t context, std::uint64_t *size, time_t *mtime)
{
    auto& state = *static_cast<storage *>(context);
    CHECK(size == nullptr);
    CHECK(mtime == nullptr);
    if (const int result = effect(state, "stat"); result < 0) { return result; }

    return state.stat_status;
}

namespace {

::rgw_usage_log_entry usage_entry(const std::string& user, const std::string& bucket,
                                  const std::uint64_t epoch, const std::uint64_t sent,
                                  const std::string& payer = {})
{
    ::rgw_usage_log_entry entry;
    entry.owner.from_str(user);
    entry.payer.from_str(payer);
    entry.bucket = bucket;
    entry.epoch = epoch;
    ::rgw_usage_data data;
    data.bytes_sent = sent;
    data.bytes_received = 2 * sent;
    data.ops = 1;
    data.successful_ops = 1;
    entry.add_usage("get_obj", data);

    return entry;
}

byte_buffer usage_bytes(const auto& value)
{
    using ::ceph::encode;

    byte_buffer output;
    encode(value, output);

    return output;
}

int add_usage(storage& state, const std::vector<::rgw_usage_log_entry>& entries)
{
    ::rgw_cls_usage_log_add_op request;
    request.info.entries = entries;
    request.user.from_str("$ns$unused");

    return invoke(state, usage_bytes(request), "user_usage_log_add");
}

::rgw_cls_usage_log_read_op read_usage(const std::string& owner = {}, const std::uint32_t maximum = 0)
{
    ::rgw_cls_usage_log_read_op request;
    request.start_epoch = 10;
    request.end_epoch = 20;
    request.owner = owner;
    request.max_entries = maximum;

    return request;
}

::rgw_cls_usage_log_read_ret usage_result(const storage& state)
{
    ::rgw_cls_usage_log_read_ret result;
    auto cursor = state.reply.cbegin();
    ::decode(result, cursor);
    REQUIRE(cursor.end());

    return result;
}

::rgw_cls_usage_log_trim_op trim_usage(const std::string& user = {})
{
    ::rgw_cls_usage_log_trim_op request;
    request.start_epoch = 10;
    request.end_epoch = 20;
    request.user = user;

    return request;
}

} // anonymous namespace

TEST_CASE("Usage add retains aggregation, payer keys and ignored user semantics", "[rgw][cls][usage]")
{
    storage state;
    CHECK(add_usage(state, {usage_entry("owner", "bucket", 11, 3, "payer"),
                            usage_entry("owner", "bucket", 11, 5, "payer")}) == 0);
    REQUIRE(state.values.size() == 2);
    CHECK(state.values.at("00000000011_payer_bucket").contents_equal(state.values.at("payer_00000000011_bucket")));
    ::rgw_usage_log_entry result;
    auto cursor = state.values.at("00000000011_payer_bucket").cbegin();
    ::decode(result, cursor);
    CHECK(result.owner.to_str() == "owner");
    CHECK(result.payer.to_str() == "payer");
    CHECK(result.total_usage.bytes_sent == 8);
    CHECK(result.total_usage.ops == 2);
    state.effects.clear();
    CHECK(add_usage(state, {}) == 0);
    CHECK(state.effects.empty());
    CHECK(state.values.size() == 2);
}

TEST_CASE("Usage read retains exclusive end, bucket filtering, scanned markers and aggregation", "[rgw][cls][usage]")
{
    storage state;
    REQUIRE(add_usage(state, {usage_entry("user", "bucket", 9, 99),
                             usage_entry("user", "bucket", 10, 3),
                             usage_entry("user", "bucket", 11, 5),
                             usage_entry("user", "other", 12, 7),
                             usage_entry("user", "bucket", 20, 99)}) == 0);
    for (const std::string& owner : {std::string {}, std::string {"user"}}) {
        auto request = read_usage(owner);
        request.bucket = "bucket";
        state.effects.clear();
        REQUIRE(invoke(state, usage_bytes(request), "user_usage_log_read") == 0);
        const auto result = usage_result(state);
        REQUIRE(result.usage.size() == 1);
        CHECK(result.usage.begin()->second.total_usage.bytes_sent == 8);
        CHECK_FALSE(result.truncated);
        CHECK(result.next_iter.empty());
        request.max_entries = 1;
        std::uint64_t total = 0;
        unsigned pages = 0;
        do {
            REQUIRE(invoke(state, usage_bytes(request), "user_usage_log_read") == 0);
            const auto page = usage_result(state);
            for (const auto& [key, value] : page.usage) { total += value.total_usage.bytes_sent; }
            ++pages;
            REQUIRE(pages < 10);
            if (!page.truncated) { break; }
            REQUIRE(!page.next_iter.empty());
            request.iter = page.next_iter;
        } while (true);
        CHECK(total == 8);
        CHECK(pages >= 3);
    }
}

TEST_CASE("Usage key transition preserves two-pass reads and old-key cleanup", "[rgw][cls][usage]")
{
    for (const bool transition : {false, true}) {
        storage state;
        state.usage_key_transition = transition;
        REQUIRE(add_usage(state, {usage_entry("0user", "bucket", 11, 3)}) == 0);
        state.values["0user_00000000012_bucket"] = usage_bytes(usage_entry("0user", "bucket", 12, 5));
        auto request = read_usage("0user", 1);
        std::uint64_t total = 0;
        unsigned pages = 0;
        do {
            REQUIRE(invoke(state, usage_bytes(request), "user_usage_log_read") == 0);
            const auto result = usage_result(state);
            for (const auto& [key, value] : result.usage) { total += value.total_usage.bytes_sent; }
            REQUIRE(++pages < 10);
            if (!result.truncated) { break; }
            request.iter = result.next_iter;
        } while (true);
        CHECK(total == (transition ? 8 : 3));
    }
    for (const bool fail_cleanup : {false, true}) {
        storage state;
        state.usage_key_transition = true;
        state.values["0user_00000000011_bucket"] = usage_bytes(usage_entry("0user", "bucket", 11, 1));
        state.failure_call = fail_cleanup ? 4 : -1;
        CHECK(add_usage(state, {usage_entry("0user", "bucket", 11, 3)}) == 0);
        CHECK(state.values.contains("0user_00000000011_bucket") == fail_cleanup);
        CHECK(state.values.contains("~0user_00000000011_bucket"));
        CHECK(effect_names(state) == std::vector<std::string> {"get-value", "set-value", "set-value", "remove"});
    }
}

TEST_CASE("Usage trim retains native pagination, paired removals and ENODATA", "[rgw][cls][usage]")
{
    for (const bool transition : {false, true}) {
        storage state;
        state.usage_key_transition = transition;
        REQUIRE(add_usage(state, {usage_entry("0user", "bucket", 11, 3), usage_entry("other", "bucket", 12, 5)}) == 0);
        if (transition) {
            state.values["0user_00000000011_bucket"] = state.values.at("~0user_00000000011_bucket");
        }
        state.effects.clear();
        auto request = trim_usage("0user");
        CHECK(invoke(state, usage_bytes(request), "user_usage_log_trim") == 0);
        CHECK(state.values.size() == 2);
        CHECK(invoke(state, usage_bytes(request), "user_usage_log_trim") == -ENODATA);
        CHECK(invoke(state, usage_bytes(trim_usage()), "user_usage_log_trim") == 0);
        CHECK(state.values.empty());
        CHECK(invoke(state, usage_bytes(trim_usage()), "user_usage_log_trim") == -ENODATA);
    }
    storage state;
    std::vector<::rgw_usage_log_entry> entries;
    for (unsigned index = 0; index < 1003; ++index) {
        entries.push_back(usage_entry("user", std::to_string(index), 11, 1));
    }
    REQUIRE(add_usage(state, entries) == 0);
    state.effects.clear();
    CHECK(invoke(state, usage_bytes(trim_usage("user")), "user_usage_log_trim") == 0);
    CHECK(state.values.size() == 6);
    CHECK(invoke(state, usage_bytes(trim_usage("user")), "user_usage_log_trim") == 0);
    CHECK(state.values.empty());
    CHECK(invoke(state, usage_bytes(trim_usage("user")), "user_usage_log_trim") == -ENODATA);
}

TEST_CASE("Usage failures retain native stage-specific statuses and corrupt-record handling", "[rgw][cls][usage]")
{
    for (int stage = 1; stage <= 3; ++stage) {
        storage state;
        state.failure_call = stage;
        state.failure = -EAGAIN;
        CHECK(add_usage(state, {usage_entry("user", "bucket", 11, 3)}) == (stage == 1 ? -EINVAL : -EAGAIN));
        CHECK(state.effects.size() == static_cast<unsigned>(stage));
    }
    storage initial;
    REQUIRE(add_usage(initial, {usage_entry("user", "bucket", 11, 3)}) == 0);
    initial.effects.clear();
    for (int stage = 1; stage <= 4; ++stage) {
        storage state = initial;
        state.failure_call = stage;
        CHECK(invoke(state, usage_bytes(trim_usage("user")), "user_usage_log_trim") == -EAGAIN);
        CHECK(state.effects.size() == static_cast<unsigned>(stage));
    }
    storage failed_read = initial;
    failed_read.failure_call = 1;
    CHECK(invoke(failed_read, usage_bytes(read_usage()), "user_usage_log_read") == -EAGAIN);
    initial.values.at("00000000011_user_bucket") = usage_bytes(std::string {"corrupt"});
    CHECK(add_usage(initial, {usage_entry("user", "bucket", 11, 3)}) == -EINVAL);
    CHECK(invoke(initial, usage_bytes(read_usage()), "user_usage_log_read") == -EINVAL);
    CHECK(invoke(initial, usage_bytes(trim_usage()), "user_usage_log_trim") == -EINVAL);
}

TEST_CASE("Usage request rejection preserves trim's stat-before-decode ordering", "[rgw][cls][usage]")
{
    ::rgw_cls_usage_log_add_op add;
    add.info.entries = {usage_entry("user", "bucket", 11, 3)};
    const std::map<std::string, byte_buffer> requests {
        {"user_usage_log_add", usage_bytes(add)},
        {"user_usage_log_read", usage_bytes(read_usage())},
        {"user_usage_log_trim", usage_bytes(trim_usage())}
    };
    for (const auto& [method, full] : requests) {
        for (unsigned size = 0; size < full.length(); ++size) {
            byte_buffer input;
            input.substr_of(full, 0, size);
            storage state;
            CHECK(invoke(state, input, method) == -EINVAL);
            CHECK(effect_names(state) == (method == "user_usage_log_trim" ? std::vector<std::string> {"stat"} : std::vector<std::string> {}));
        }
    }
    for (const int error : {-ENOENT, -EIO, -EAGAIN}) {
        storage state;
        state.stat_status = error;
        CHECK(invoke(state, byte_buffer {}, "user_usage_log_trim") == error);
        CHECK(effect_names(state) == std::vector<std::string> {"stat"});
    }
}

namespace {

::cls_rgw_reshard_entry reshard_entry(const std::string& name = "bucket")
{
    ::cls_rgw_reshard_entry entry;
    entry.time = ::ceph::real_time {std::chrono::seconds {123}};
    entry.tenant = "tenant";
    entry.bucket_name = name;
    entry.bucket_id = "id";
    entry.old_num_shards = 3;
    entry.new_num_shards = 7;
    entry.initiator = ::cls_rgw_reshard_initiator::Dynamic;

    return entry;
}

byte_buffer old_reshard_entry(const ::cls_rgw_reshard_entry& entry, const std::uint8_t version)
{
    byte_buffer output;
    ENCODE_START(version, 1, output);
    ::ceph::encode(entry.time, output);
    ::ceph::encode(entry.tenant, output);
    ::ceph::encode(entry.bucket_name, output);
    ::ceph::encode(entry.bucket_id, output);
    if (version < 2) { ::ceph::encode(std::string {"removed"}, output); }
    ::ceph::encode(entry.old_num_shards, output);
    ::ceph::encode(entry.new_num_shards, output);
    if (version >= 3) { ::ceph::encode(entry.initiator, output); }
    ENCODE_FINISH(output);

    return output;
}

byte_buffer reshard_list_request(const std::uint32_t maximum, const std::string& marker = {})
{
    ::cls_rgw_reshard_list_op request;
    request.max = maximum;
    request.marker = marker;

    return usage_bytes(request);
}

} // anonymous namespace

TEST_CASE("Reshard queue retains create-only checks, get ownership and bucket-id removal", "[rgw][cls][reshard]")
{
    storage state;
    ::cls_rgw_reshard_add_op add;
    add.entry = reshard_entry();
    add.create_only = true;
    REQUIRE(invoke(state, usage_bytes(add), "reshard_add") == 0);
    CHECK(effect_names(state) == std::vector<std::string> {"get-value", "set-value"});
    CHECK(state.values.at("tenant:bucket").contents_equal(usage_bytes(add.entry)));
    CHECK(invoke(state, usage_bytes(add), "reshard_add") == -EEXIST);
    add.create_only = false;
    add.entry.new_num_shards = 11;
    CHECK(invoke(state, usage_bytes(add), "reshard_add") == 0);
    ::cls_rgw_reshard_get_op get;
    get.entry = reshard_entry();
    CHECK(invoke(state, usage_bytes(get), "reshard_get") == 0);
    ::cls_rgw_reshard_get_ret result;
    auto cursor = state.reply.cbegin();
    ::decode(result, cursor);
    CHECK(result.entry.new_num_shards == 11);
    ::cls_rgw_reshard_remove_op remove;
    remove.tenant = "tenant";
    remove.bucket_name = "bucket";
    remove.bucket_id = "different";
    CHECK(invoke(state, usage_bytes(remove), "reshard_remove") == 0);
    CHECK(state.values.size() == 1);
    remove.bucket_id.clear();
    CHECK(invoke(state, usage_bytes(remove), "reshard_remove") == 0);
    CHECK(state.values.empty());
    CHECK(invoke(state, usage_bytes(remove), "reshard_remove") == -ENOENT);
}

TEST_CASE("Reshard listing preserves zero and large limits, pagination and mixed old leaves", "[rgw][cls][reshard]")
{
    storage state;
    for (unsigned index = 0; index < 1003; ++index) {
        const auto key = std::to_string(index);
        state.values[key] = usage_bytes(reshard_entry(key));
    }
    for (const std::uint32_t maximum : {0u, 1u, 2u, 1000u, 1001u, 2147483647u, 2147483648u, 4294967295u}) {
        state.effects.clear();
        REQUIRE(invoke(state, reshard_list_request(maximum), "reshard_list") == 0);
        ::cls_rgw_reshard_list_ret result;
        auto cursor = state.reply.cbegin();
        ::decode(result, cursor);
        const auto fetched = maximum && maximum < 1000 ? maximum : 1000;
        CHECK(result.entries.size() == (maximum && maximum <= 2147483647u ? fetched : 0));
        CHECK(result.is_truncated);
        CHECK(state.effects.size() == 1);
    }
    REQUIRE(invoke(state, reshard_list_request(2, "999"), "reshard_list") == 0);
    ::cls_rgw_reshard_list_ret empty;
    auto cursor = state.reply.cbegin();
    ::decode(empty, cursor);
    CHECK(empty.entries.empty());
    CHECK_FALSE(empty.is_truncated);

    state.values.clear();
    for (const std::uint8_t version : {3, 1, 2, 0}) {
        const auto key = std::to_string(state.values.size());
        state.values[key] = old_reshard_entry(reshard_entry(key), version);
    }
    REQUIRE(invoke(state, reshard_list_request(4), "reshard_list") == 0);
    ::cls_rgw_reshard_list_ret result;
    cursor = state.reply.cbegin();
    ::decode(result, cursor);
    REQUIRE(result.entries.size() == 4);
    CHECK(result.entries.front().initiator == ::cls_rgw_reshard_initiator::Dynamic);
    for (const auto& entry : result.entries | std::views::drop(1)) {
        CHECK(entry.initiator == ::cls_rgw_reshard_initiator::Unknown);
        CHECK(entry.tenant == "tenant");
        CHECK(entry.bucket_id == "id");
        CHECK(entry.new_num_shards == 7);
    }
}

TEST_CASE("Reshard queue preserves stage-specific failures and corrupt data", "[rgw][cls][reshard]")
{
    ::cls_rgw_reshard_add_op add;
    add.entry = reshard_entry();
    add.create_only = true;
    for (int stage = 1; stage <= 2; ++stage) {
        storage state;
        state.failure_call = stage;
        CHECK(invoke(state, usage_bytes(add), "reshard_add") == -EAGAIN);
        CHECK(state.effects.size() == static_cast<unsigned>(stage));
    }
    for (const bool malformed : {false, true}) {
        storage state;
        state.values["tenant:bucket"] = malformed ? usage_bytes(std::string {"corrupt"}) : usage_bytes(add.entry);
        CHECK(invoke(state, usage_bytes(add), "reshard_add") == -EEXIST);
        ::cls_rgw_reshard_get_op get;
        get.entry = add.entry;
        CHECK(invoke(state, usage_bytes(get), "reshard_get") == (malformed ? -EIO : 0));
        ::cls_rgw_reshard_remove_op remove;
        remove.tenant = "tenant";
        remove.bucket_name = "bucket";
        if (malformed) {
            CHECK(invoke(state, usage_bytes(remove), "reshard_remove") == -EIO);
            CHECK(invoke(state, reshard_list_request(1), "reshard_list") == -EIO);
        }
        if (!malformed) {
            state.effects.clear();
            state.failure_call = 2;
            CHECK(invoke(state, usage_bytes(remove), "reshard_remove") == 0);
            CHECK(state.values.size() == 1);
        }
    }
    storage state;
    state.failure_call = 1;
    CHECK(invoke(state, reshard_list_request(1), "reshard_list") == -EAGAIN);
}

TEST_CASE("Bucket reshard controls preserve header metadata and exact guard return values", "[rgw][cls][reshard]")
{
    for (const unsigned status : {0, 1, 2, 3, 255}) {
        storage state;
        ::rgw_bucket_dir_header initial;
        initial.ver = 9;
        initial.master_ver = 13;
        initial.tag_timeout = 17;
        state.header = usage_bytes(initial);
        ::cls_rgw_set_bucket_resharding_op set;
        set.entry.reshard_status = static_cast<::cls_rgw_reshard_status>(status);
        CHECK(invoke(state, usage_bytes(set), "set_bucket_resharding") == 0);
        auto changed = header(state);
        CHECK(changed.new_instance.reshard_status == set.entry.reshard_status);
        CHECK(changed.ver == 10);
        CHECK(changed.master_ver == 13);
        CHECK(changed.tag_timeout == 17);
        CHECK(invoke(state, usage_bytes(::cls_rgw_get_bucket_resharding_op {}), "get_bucket_resharding") == 0);
        ::cls_rgw_get_bucket_resharding_ret result;
        auto cursor = state.reply.cbegin();
        ::decode(result, cursor);
        CHECK(result.new_instance.reshard_status == set.entry.reshard_status);
        for (const std::uint32_t count : {0, 3, 4, 7}) {
            for (const int error : {std::numeric_limits<int>::min(), -2300, -1, 0, 7, std::numeric_limits<int>::max()}) {
                changed.reshardlog_entries = count;
                state.header = usage_bytes(changed);
                ::cls_rgw_guard_bucket_resharding_op guard {error};
                const bool blocked = status == 1 || (status == 3 && count >= state.reshard_threshold);
                CHECK(invoke(state, usage_bytes(guard), "guard_bucket_resharding") == (blocked ? error : 0));
            }
        }
        CHECK(invoke(state, usage_bytes(::cls_rgw_clear_bucket_resharding_op {}), "clear_bucket_resharding") == 0);
        CHECK(header(state).new_instance.reshard_status == ::cls_rgw_reshard_status::NOT_RESHARDING);
        CHECK(header(state).ver == 11);
    }
}

TEST_CASE("Reshard controls retain header failure stages and request admission", "[rgw][cls][reshard]")
{
    ::cls_rgw_reshard_add_op add;
    add.entry = reshard_entry();
    ::cls_rgw_reshard_get_op get;
    get.entry = add.entry;
    ::cls_rgw_reshard_remove_op remove;
    remove.tenant = "tenant";
    remove.bucket_name = "bucket";
    ::cls_rgw_set_bucket_resharding_op set;
    set.entry.reshard_status = ::cls_rgw_reshard_status::IN_PROGRESS;
    const std::map<std::string, byte_buffer> requests {
        {"reshard_add", usage_bytes(add)}, {"reshard_get", usage_bytes(get)},
        {"reshard_list", reshard_list_request(1)}, {"reshard_remove", usage_bytes(remove)},
        {"set_bucket_resharding", usage_bytes(set)},
        {"clear_bucket_resharding", usage_bytes(::cls_rgw_clear_bucket_resharding_op {})},
        {"get_bucket_resharding", usage_bytes(::cls_rgw_get_bucket_resharding_op {})},
        {"guard_bucket_resharding", usage_bytes(::cls_rgw_guard_bucket_resharding_op {-2300})}
    };
    for (const auto& [method, full] : requests) {
        for (unsigned size = 0; size < full.length(); ++size) {
            byte_buffer input;
            input.substr_of(full, 0, size);
            storage state;
            CHECK(invoke(state, input, method) == -EINVAL);
            CHECK(state.effects.empty());
        }
        if (method.find("bucket_resharding") == std::string::npos) { continue; }
        const bool writes = method == "set_bucket_resharding" || method == "clear_bucket_resharding";
        for (int stage = 1; stage <= (writes ? 2 : 1); ++stage) {
            storage state;
            state.failure_call = stage;
            CHECK(invoke(state, full, method) == -EAGAIN);
            CHECK(state.effects.size() == static_cast<unsigned>(stage));
        }
        storage corrupt;
        corrupt.header = usage_bytes(std::string {"corrupt"});
        CHECK(invoke(corrupt, full, method) == -EIO);
    }
}

namespace {

::cls_rgw_gc_obj_info gc_info(const std::string& tag = "tag", const std::uint64_t seconds = 1234567889)
{
    ::cls_rgw_gc_obj_info info;
    info.tag = tag;
    info.time = ::ceph::real_time {std::chrono::seconds {seconds} + std::chrono::nanoseconds {123}};
    for (unsigned index = 0; index < 3; ++index) {
        info.chain.push_obj(std::string(80, 'p'), {std::string(80, 'o'), std::to_string(index)}, std::string(80, 'l'));
    }

    return info;
}

std::string gc_time_key(const ::cls_rgw_gc_obj_info& info)
{
    char result[32];
    const auto time = ::ceph::real_clock::to_ceph_timespec(info.time);
    std::snprintf(result, sizeof(result), "%011llu.%09u",
                  static_cast<unsigned long long>(time.tv_sec), static_cast<unsigned>(time.tv_nsec));

    return "1_" + std::string {result};
}

int set_gc(storage& state, const ::cls_rgw_gc_obj_info& info, const std::uint32_t expiration = 1)
{
    ::cls_rgw_gc_set_entry_op request;
    request.info = info;
    request.expiration_secs = expiration;

    return invoke(state, usage_bytes(request), "gc_set_entry");
}

byte_buffer gc_list_request(const bool expired, const std::uint32_t maximum = 0, const std::string& marker = {})
{
    ::cls_rgw_gc_list_op request;
    request.expired_only = expired;
    request.max = maximum;
    request.marker = marker;

    return usage_bytes(request);
}

void seed_gc(storage& state, const ::cls_rgw_gc_obj_info& info)
{
    state.values["0_" + info.tag] = usage_bytes(info);
    state.values[gc_time_key(info)] = usage_bytes(info);
}

::cls_rgw_gc_list_ret gc_result(const storage& state)
{
    ::cls_rgw_gc_list_ret result;
    auto cursor = state.reply.cbegin();
    ::decode(result, cursor);
    REQUIRE(cursor.end());

    return result;
}

} // anonymous namespace

TEST_CASE("Legacy GC set/defer retain native time indexes and complete object chains", "[rgw][cls][gc]")
{
    storage state;
    const auto info = gc_info();
    REQUIRE(set_gc(state, info) == 0);
    CHECK(effect_names(state) == std::vector<std::string> {"get-value", "set-value", "set-value"});
    REQUIRE(state.values.size() == 2);
    CHECK(state.values.at("0_tag").contents_equal(state.values.at("1_01234567891.000000123")));
    REQUIRE(invoke(state, gc_list_request(false), "gc_list") == 0);
    const auto result = gc_result(state);
    REQUIRE(result.entries.size() == 1);
    CHECK(result.entries.front().tag == "tag");
    REQUIRE(result.entries.front().chain.objs.size() == 3);
    CHECK(result.entries.front().chain.objs.back().key.instance == "2");
    CHECK_FALSE(result.truncated);
    CHECK(result.next_marker.empty());
    ::cls_rgw_gc_defer_entry_op defer;
    defer.tag = "tag";
    defer.expiration_secs = 2;
    state.effects.clear();
    CHECK(invoke(state, usage_bytes(defer), "gc_defer_entry") == 0);
    CHECK(effect_names(state) == std::vector<std::string> {"get-value", "get-value", "remove", "set-value", "set-value"});
    CHECK_FALSE(state.values.contains("1_01234567891.000000123"));
    CHECK(state.values.at("0_tag").contents_equal(state.values.at("1_01234567892.000000123")));
    CHECK(invoke(state, gc_list_request(true), "gc_list") == 0);
    CHECK(gc_result(state).entries.empty());
    CHECK(set_gc(state, gc_info(""), 0) == 0);
}

TEST_CASE("Legacy GC list retains strict expiry, namespace cutoff, markers and default page size", "[rgw][cls][gc]")
{
    storage state;
    seed_gc(state, gc_info("past", 1234567889));
    seed_gc(state, gc_info("equal", 1234567890));
    seed_gc(state, gc_info("future", 1234567891));
    REQUIRE(invoke(state, gc_list_request(true), "gc_list") == 0);
    auto result = gc_result(state);
    REQUIRE(result.entries.size() == 1);
    CHECK(result.entries.front().tag == "past");
    CHECK_FALSE(result.truncated);
    CHECK(result.next_marker.empty());
    state.values["2_invalid"] = usage_bytes(std::string {"corrupt"});
    REQUIRE(invoke(state, gc_list_request(false, 1), "gc_list") == 0);
    result = gc_result(state);
    REQUIRE(result.entries.size() == 1);
    CHECK(result.truncated);
    CHECK(result.next_marker == "1_01234567889.000000123");
    REQUIRE(invoke(state, gc_list_request(false, 2, result.next_marker), "gc_list") == 0);
    result = gc_result(state);
    CHECK(result.entries.size() == 2);
    CHECK(result.truncated);
    REQUIRE(invoke(state, gc_list_request(false, 2, result.next_marker), "gc_list") == 0);
    result = gc_result(state);
    CHECK(result.entries.empty());
    CHECK_FALSE(result.truncated);
    CHECK(result.next_marker.empty());
    state.values.clear();
    for (unsigned index = 0; index < 130; ++index) { seed_gc(state, gc_info(std::to_string(index), 1000 + index)); }
    REQUIRE(invoke(state, gc_list_request(false), "gc_list") == 0);
    result = gc_result(state);
    CHECK(result.entries.size() == 128);
    CHECK(result.truncated);
    REQUIRE(invoke(state, gc_list_request(false, 0, result.next_marker), "gc_list") == 0);
    CHECK(gc_result(state).entries.size() == 2);
    CHECK_FALSE(gc_result(state).truncated);
    CHECK(gc_result(state).next_marker.empty());
}

TEST_CASE("Legacy GC removal retains duplicate and missing-index behavior", "[rgw][cls][gc]")
{
    for (const bool missing_time : {false, true}) {
        storage state;
        const auto info = gc_info(std::string {"t\0g", 3});
        seed_gc(state, info);
        if (missing_time) { state.values.erase(gc_time_key(info)); }
        ::cls_rgw_gc_remove_op request;
        request.tags = {"missing", info.tag, info.tag};
        CHECK(invoke(state, usage_bytes(request), "gc_remove") == 0);
        CHECK(state.values.empty());
    }
    storage state;
    CHECK(invoke(state, usage_bytes(::cls_rgw_gc_remove_op {}), "gc_remove") == 0);
    CHECK(state.effects.empty());
}

TEST_CASE("Legacy GC set keeps ignored initial reads and original cleanup errors", "[rgw][cls][gc]")
{
    for (const int error : {-EAGAIN, -EIO, -ENOMEM}) {
        storage state;
        state.failure_call = 1;
        state.failure = error;
        CHECK(set_gc(state, gc_info()) == 0);
        CHECK(state.values.size() == 2);
    }
    for (const bool fail_cleanup : {false, true}) {
        storage state;
        state.failure_call = 3;
        if (fail_cleanup) { state.secondary_failure_call = 4; }
        CHECK(set_gc(state, gc_info()) == -EAGAIN);
        CHECK(effect_names(state) == std::vector<std::string> {"get-value", "set-value", "set-value", "remove"});
        CHECK(state.values.contains("0_tag") == fail_cleanup);
    }
    storage state;
    state.values["0_tag"] = usage_bytes(std::string {"corrupt"});
    CHECK(set_gc(state, gc_info()) == 0);
}

TEST_CASE("Legacy GC defer remove and list retain failure stages and corruption errors", "[rgw][cls][gc]")
{
    storage initial;
    seed_gc(initial, gc_info());
    ::cls_rgw_gc_defer_entry_op defer;
    defer.tag = "tag";
    defer.expiration_secs = 2;
    for (int stage = 1; stage <= 5; ++stage) {
        storage state = initial;
        state.failure_call = stage;
        CHECK(invoke(state, usage_bytes(defer), "gc_defer_entry") == (stage == 2 ? 0 : -EAGAIN));
    }
    ::cls_rgw_gc_remove_op remove;
    remove.tags = {"tag"};
    for (int stage = 1; stage <= 3; ++stage) {
        storage state = initial;
        state.failure_call = stage;
        CHECK(invoke(state, usage_bytes(remove), "gc_remove") == -EAGAIN);
        CHECK(state.effects.size() == static_cast<unsigned>(stage));
    }
    storage failed = initial;
    failed.failure_call = 1;
    CHECK(invoke(failed, gc_list_request(false), "gc_list") == -EAGAIN);
    initial.values[gc_time_key(gc_info())] = usage_bytes(std::string {"corrupt"});
    CHECK(invoke(initial, gc_list_request(false), "gc_list") == -EIO);
    initial.values["0_tag"] = usage_bytes(std::string {"corrupt"});
    CHECK(invoke(initial, usage_bytes(defer), "gc_defer_entry") == -EIO);
    CHECK(invoke(initial, usage_bytes(remove), "gc_remove") == -EIO);
}

TEST_CASE("Legacy GC structural request errors precede storage", "[rgw][cls][gc]")
{
    ::cls_rgw_gc_set_entry_op set;
    set.info = gc_info();
    ::cls_rgw_gc_defer_entry_op defer;
    defer.tag = "tag";
    ::cls_rgw_gc_remove_op remove;
    remove.tags = {"tag", "other"};
    const std::map<std::string, byte_buffer> requests {
        {"gc_set_entry", usage_bytes(set)}, {"gc_defer_entry", usage_bytes(defer)},
        {"gc_list", gc_list_request(true)}, {"gc_remove", usage_bytes(remove)}
    };
    for (const auto& [method, full] : requests) {
        for (unsigned size = 0; size < full.length(); ++size) {
            byte_buffer prefix;
            prefix.substr_of(full, 0, size);
            storage state;
            CHECK(invoke(state, prefix, method) == -EINVAL);
            CHECK(state.effects.empty());
        }
    }
}

namespace {

byte_buffer lc_list_request(const std::uint8_t version, const std::uint32_t maximum = 3, const std::string& marker = {})
{
    byte_buffer output;
    ENCODE_START(version, 1, output);
    ::ceph::encode(marker, output);
    ::ceph::encode(maximum, output);
    ENCODE_FINISH(output);

    return output;
}

} // anonymous namespace

TEST_CASE("Lifecycle set get next and remove preserve native keys and entry values", "[rgw][cls][lc]")
{
    storage state;
    ::cls_rgw_lc_set_entry_op set;
    set.entry = {std::string {"b\0t", 3}, 17, 23};
    CHECK(invoke(state, usage_bytes(set), "lc_set_entry") == 0);
    CHECK(state.values.at(set.entry.bucket).contents_equal(usage_bytes(set.entry)));
    ::cls_rgw_lc_get_entry_op get {set.entry.bucket};
    REQUIRE(invoke(state, usage_bytes(get), "lc_get_entry") == 0);
    ::cls_rgw_lc_get_entry_ret result;
    auto cursor = state.reply.cbegin();
    ::decode(result, cursor);
    CHECK(usage_bytes(result.entry).contents_equal(usage_bytes(set.entry)));
    ::cls_rgw_lc_get_next_entry_op next;
    REQUIRE(invoke(state, usage_bytes(next), "lc_get_next_entry") == 0);
    ::cls_rgw_lc_get_next_entry_ret next_result;
    cursor = state.reply.cbegin();
    ::decode(next_result, cursor);
    CHECK(next_result.entry.bucket == set.entry.bucket);
    next.marker = set.entry.bucket;
    REQUIRE(invoke(state, usage_bytes(next), "lc_get_next_entry") == 0);
    cursor = state.reply.cbegin();
    ::decode(next_result, cursor);
    CHECK(next_result.entry.bucket.empty());
    CHECK(next_result.entry.start_time == 0);
    CHECK(next_result.entry.status == 0);
    ::cls_rgw_lc_rm_entry_op remove;
    remove.entry = set.entry;
    CHECK(invoke(state, usage_bytes(remove), "lc_rm_entry") == 0);
    CHECK(state.values.empty());
    CHECK(invoke(state, usage_bytes(remove), "lc_rm_entry") == -ENOENT);
    CHECK(invoke(state, usage_bytes(get), "lc_get_entry") == -ENOENT);
}

TEST_CASE("Lifecycle list retains request versions, legacy normalization and exact pagination", "[rgw][cls][lc]")
{
    storage state;
    state.values["a"] = usage_bytes(::cls_rgw_lc_entry {"z", 17, 11});
    state.values["b"] = usage_bytes(::cls_rgw_lc_entry {"a", 19, 13});
    state.values["c"] = usage_bytes(::cls_rgw_lc_entry {"z", 23, 99});
    state.values["d"] = usage_bytes(std::pair {std::string {"old"}, -17});
    for (const std::uint8_t version : {0, 1, 2, 3, 10}) {
        REQUIRE(invoke(state, lc_list_request(version), "lc_list_entries") == 0);
        auto header_cursor = state.reply.cbegin();
        std::uint8_t actual_version;
        ::ceph::decode(actual_version, header_cursor);
        CHECK(actual_version == version);
        ::cls_rgw_lc_list_entries_ret result;
        auto cursor = state.reply.cbegin();
        ::decode(result, cursor);
        REQUIRE(result.entries.size() == (version <= 2 ? 2 : 3));
        CHECK(result.is_truncated == (version >= 2));
        if (version <= 2) {
            CHECK(result.entries.front().bucket == "a");
            CHECK(result.entries.back().bucket == "z");
            CHECK(result.entries.back().status == 11);
            CHECK(result.entries.back().start_time == 0);
        }
        if (version > 2) {
            CHECK(result.entries.front().bucket == "z");
            CHECK(result.entries.front().start_time == 17);
            CHECK(result.entries.back().status == 99);
        }
    }
    REQUIRE(invoke(state, lc_list_request(3, 3, "c"), "lc_list_entries") == 0);
    ::cls_rgw_lc_list_entries_ret result;
    auto cursor = state.reply.cbegin();
    ::decode(result, cursor);
    REQUIRE(result.entries.size() == 1);
    CHECK(result.entries.front().bucket == "old");
    CHECK(result.entries.front().start_time == 0);
    CHECK(result.entries.front().status == static_cast<std::uint32_t>(-17));
    CHECK_FALSE(result.is_truncated);
    REQUIRE(invoke(state, lc_list_request(3, 0), "lc_list_entries") == 0);
    cursor = state.reply.cbegin();
    ::decode(result, cursor);
    CHECK(result.entries.empty());
    CHECK(result.is_truncated);
}

TEST_CASE("Lifecycle head storage preserves fields, defaults, versioning and ignored input", "[rgw][cls][lc]")
{
    storage state;
    CHECK(invoke(state, usage_bytes(std::string {"ignored input"}), "lc_get_head") == 0);
    ::cls_rgw_lc_get_head_ret result;
    auto cursor = state.reply.cbegin();
    ::decode(result, cursor);
    CHECK(result.head.start_date == 0);
    CHECK(result.head.marker.empty());
    CHECK(result.head.shard_rollover_date == 0);
    ::cls_rgw_lc_put_head_op put;
    put.head.start_date = 17;
    put.head.marker = std::string(80, 'm');
    put.head.shard_rollover_date = 19;
    CHECK(invoke(state, usage_bytes(put), "lc_put_head") == 0);
    CHECK(state.header.contents_equal(usage_bytes(put.head)));
    CHECK(invoke(state, {}, "lc_get_head") == 0);
    cursor = state.reply.cbegin();
    ::decode(result, cursor);
    CHECK(result.head.marker == put.head.marker);
    CHECK(result.head.shard_rollover_date == 19);
}

TEST_CASE("Lifecycle old pair requests remain distinct from stored-entry compatibility", "[rgw][cls][lc]")
{
    for (const std::uint8_t version : {0, 1}) {
        byte_buffer request;
        ENCODE_START(version, 1, request);
        ::ceph::encode(std::pair {std::string {"bucket"}, -17}, request);
        ENCODE_FINISH(request);
        storage state;
        CHECK(invoke(state, request, "lc_set_entry") == 0);
        ::cls_rgw_lc_entry stored;
        auto cursor = state.values.at("bucket").cbegin();
        ::decode(stored, cursor);
        CHECK(stored.start_time == 0);
        CHECK(stored.status == static_cast<std::uint32_t>(-17));
        CHECK(invoke(state, request, "lc_rm_entry") == 0);
        CHECK(state.values.empty());
    }
    storage state;
    state.values["bucket"] = usage_bytes(std::pair {std::string {"old"}, -17});
    CHECK(invoke(state, usage_bytes(::cls_rgw_lc_get_entry_op {"bucket"}), "lc_get_entry") == -EIO);
    ::cls_rgw_lc_get_next_entry_op next;
    CHECK(invoke(state, usage_bytes(next), "lc_get_next_entry") == -EIO);
    CHECK(invoke(state, lc_list_request(3), "lc_list_entries") == 0);
}

TEST_CASE("Lifecycle failures retain stage-specific errors and request admission", "[rgw][cls][lc]")
{
    ::cls_rgw_lc_set_entry_op set;
    set.entry = {"bucket", 17, 23};
    ::cls_rgw_lc_rm_entry_op remove;
    remove.entry = set.entry;
    ::cls_rgw_lc_put_head_op head;
    const std::map<std::string, byte_buffer> requests {
        {"lc_set_entry", usage_bytes(set)}, {"lc_rm_entry", usage_bytes(remove)},
        {"lc_get_entry", usage_bytes(::cls_rgw_lc_get_entry_op {"bucket"})},
        {"lc_get_next_entry", usage_bytes(::cls_rgw_lc_get_next_entry_op {})},
        {"lc_list_entries", lc_list_request(3)}, {"lc_put_head", usage_bytes(head)}
    };
    for (const auto& [method, full] : requests) {
        for (unsigned size = 0; size < full.length(); ++size) {
            byte_buffer input;
            input.substr_of(full, 0, size);
            storage state;
            CHECK(invoke(state, input, method) == -EINVAL);
            CHECK(state.effects.empty());
        }
        storage state;
        state.failure_call = 1;
        CHECK(invoke(state, full, method) == -EAGAIN);
    }
    storage state;
    state.failure_call = 1;
    CHECK(invoke(state, {}, "lc_get_head") == -EAGAIN);
    state.failure_call = -1;
    state.header = usage_bytes(std::string {"corrupt"});
    CHECK(invoke(state, {}, "lc_get_head") == -EINVAL);
    state.values["a"] = state.header;
    CHECK(invoke(state, lc_list_request(3), "lc_list_entries") == -EIO);
}

namespace {

void multipart_info(::RGWUploadPartInfo& value, const std::string& prefix)
{
    value.num = 17;
    value.size = 4096;
    value.accounted_size = 8192;
    value.etag = "etag";
    ::rgw_placement_rule placement;
    placement.name = "raw-head-placement";
    value.manifest.set_head(placement, {}, 31);
    value.manifest.set_trivial_rule(31, 4096);
    value.manifest.set_prefix(prefix);
    value.past_prefixes = {"older", "shared"};
    value.crypt_salt = std::string {"s\0t", 3};
}

byte_buffer multipart_request(const ::RGWUploadPartInfo& value, const std::uint8_t version = 1)
{
    byte_buffer output;
    ENCODE_START(version, 1, output);
    ::ceph::encode(std::string {"p\0t", 3}, output);
    ::encode(value, output);
    if (version > 1) { ::ceph::encode(std::uint64_t {37}, output); }
    ENCODE_FINISH(output);

    return output;
}

} // anonymous namespace

TEST_CASE("Multipart update stores raw manifest head placement and merges previous prefixes", "[rgw][cls][multipart]")
{
    ::RGWUploadPartInfo incoming;
    multipart_info(incoming, "current");
    ::RGWUploadPartInfo old;
    multipart_info(old, "previous");
    old.past_prefixes = {"oldest", "shared"};
    for (const std::uint8_t version : {0, 1, 10}) {
        storage state;
        state.values[std::string {"p\0t", 3}] = usage_bytes(old);
        REQUIRE(invoke(state, multipart_request(incoming, version), "mp_upload_part_info_update") == 0);
        ::RGWUploadPartInfo stored;
        auto cursor = state.values.at(std::string {"p\0t", 3}).cbegin();
        ::decode(stored, cursor);
        CHECK(stored.manifest.get_head_placement_rule().name == "raw-head-placement");
        CHECK(stored.past_prefixes == std::set<std::string> {"older", "shared", "oldest", "previous"});
        CHECK(stored.crypt_salt == incoming.crypt_salt);
        CHECK(stored.accounted_size == 8192);
        CHECK(state.reply.length() == 0);
        const auto effects = effect_names(state);
        REQUIRE(effects.size() == 2);
        CHECK(effects.front() == "get-value");
        CHECK(effects.back() == "set-value");
    }
}

TEST_CASE("Multipart missing entry succeeds and prefix collisions never write", "[rgw][cls][multipart]")
{
    ::RGWUploadPartInfo incoming;
    multipart_info(incoming, "current");
    storage state;
    REQUIRE(invoke(state, multipart_request(incoming), "mp_upload_part_info_update") == 0);
    CHECK(state.values.at(std::string {"p\0t", 3}).contents_equal(usage_bytes(incoming)));
    // Both a stored current manifest prefix and an older prefix reject reuse.
    for (const bool manifest_collision : {false, true}) {
        ::RGWUploadPartInfo old;
        multipart_info(old, manifest_collision ? "current" : "previous");
        if (!manifest_collision) { old.past_prefixes.insert("current"); }
        storage collision;
        const auto original = usage_bytes(old);
        collision.values[std::string {"p\0t", 3}] = original;
        CHECK(invoke(collision, multipart_request(incoming), "mp_upload_part_info_update") == -EEXIST);
        CHECK(collision.values.at(std::string {"p\0t", 3}).contents_equal(original));
        CHECK(effect_names(collision) == std::vector<std::string> {"get-value"});
    }
    incoming.past_prefixes.insert("current");
    storage self_collision;
    CHECK(invoke(self_collision, multipart_request(incoming), "mp_upload_part_info_update") == -EEXIST);
    CHECK(self_collision.values.empty());
}

TEST_CASE("Multipart decode and storage failures preserve native stage and status", "[rgw][cls][multipart]")
{
    ::RGWUploadPartInfo incoming;
    multipart_info(incoming, "current");
    const auto full = multipart_request(incoming);
    for (unsigned size = 0; size < full.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(full, 0, size);
        storage state;
        CHECK(invoke(state, prefix, "mp_upload_part_info_update") == -EINVAL);
        CHECK(state.effects.empty());
    }
    for (const int stage : {1, 2}) {
        storage state;
        state.failure_call = stage;
        CHECK(invoke(state, full, "mp_upload_part_info_update") == -EAGAIN);
        CHECK(state.values.empty());
    }
    storage corrupt;
    corrupt.values[std::string {"p\0t", 3}] = usage_bytes(std::string {"bad"});
    CHECK(invoke(corrupt, full, "mp_upload_part_info_update") == -EIO);
    CHECK(effect_names(corrupt) == std::vector<std::string> {"get-value"});
}
