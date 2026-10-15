// RGW CLS: Actual batch-write handler with controlled OMAP effects.
#include "module_loader.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "common/config_proxy.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include <fstream>
#include <map>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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
