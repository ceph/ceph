// RGW CLS: Actual index-write handlers with controlled OMAP effects and time.
#include "module_loader.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "common/config_proxy.h"
#include "osd/osd_types.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

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

namespace
{
using byte_buffer = ::ceph::buffer::list;

struct storage final
{
    std::map<std::string, byte_buffer> values;
    byte_buffer header;
    std::vector<std::pair<std::string, byte_buffer>> effects;

    std::uint64_t maximum = 4;
    std::uint32_t reshard_threshold = 4;

    int failure_call = -1;
    int failure = -EAGAIN;
};

int effect(storage& state, std::string operation, byte_buffer arguments = {})
{
    state.effects.emplace_back(std::move(operation), std::move(arguments));

    return static_cast<int>(state.effects.size()) == state.failure_call ? state.failure : 0;
}

auto invoke(storage& state, byte_buffer input, const std::string_view method = "bi_put_entries")
{
    const auto [call, flags] = ::rgw_cls_test::get_method(method);
    REQUIRE(flags == (CLS_METHOD_RD | CLS_METHOD_WR));
    byte_buffer output;
    const int status = call(&state, &input, &output);
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

namespace
{
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
