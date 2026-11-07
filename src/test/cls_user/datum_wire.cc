// Independent user records must accept the same concrete native encoding.
#include "datum_test.h"
#include "rgw/datum/adapters/cls/user.h"

#include <catch2/catch_test_macros.hpp>
#include <limits>
#include <type_traits>

namespace {

using namespace user_cls_test;
namespace binding = ::cls::rgw::user_datum;

template <typename NativeT, typename WireT, typename ProjectT>
void check_request(const NativeT& native, const bytes& encoded, const ProjectT project)
{
    CHECK(encoded.contents_equal(canonical(native)));
    for (const auto version : {std::uint8_t {0}, static_cast<std::uint8_t>(encoded[0]), std::uint8_t {31}}) {
        auto raw = encoded.to_str();
        raw[0] = version;
        if (version == 31) {
            raw.append("tail\0", 5);
            const auto length = static_cast<std::uint32_t>(raw.size() - 6);
            for (unsigned index = 0; index < 4; ++index) {
                raw[2 + index] = static_cast<char>(length >> (8 * index));
            }
        }
        bytes full;
        // Individual allocations exercise every scalar/header across fragments.
        for (const char ch : raw) { full.append(::ceph::buffer::copy(&ch, 1)); }
        NativeT expected;
        auto cursor = full.cbegin();
        ::decode(expected, cursor);
        const auto expected_bytes = canonical(expected);
        WireT value;
        cursor = full.cbegin();
        binding::decode(value, cursor);
        CHECK(cursor.end());
        CHECK(canonical(project(value)).contents_equal(expected_bytes));
        for (unsigned size = 0; size < full.length(); ++size) {
            bytes prefix;
            prefix.substr_of(full, 0, size);
            cursor = prefix.cbegin();
            CHECK_THROWS_AS(binding::decode(value, cursor), ::ceph::buffer::error);
            CHECK(cursor.get_off() == 0);
            CHECK(canonical(project(value)).contents_equal(expected_bytes));
        }
        full.append("outside", 7);
        cursor = full.cbegin();
        binding::decode(value, cursor);
        CHECK(cursor.get_remaining() == 7);
        raw[1] = 127;
        bytes incompatible;
        incompatible.append(raw);
        cursor = incompatible.cbegin();
        CHECK_THROWS_AS(binding::decode(value, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
    }
}

} // anonymous namespace

TEST_CASE("User request bindings retain native versions fragments widths and atomic errors", "[rgw][cls][user][datum]")
{
    static_assert(std::is_nothrow_move_assignable_v<::cls_user_bucket_entry>);
    static_assert(std::is_nothrow_move_assignable_v<::cls_user_account_resource>);
    ::cls_user_set_buckets_op bulk;
    bulk.entries.push_back(bucket_entry());
    bulk.add = true;
    bulk.time = bulk.entries.front().creation_time;
    check_request<decltype(bulk), binding::bulk_request>(bulk,
        binding::encode_bulk(bulk.entries, bulk.add, bulk.time), [](const auto& value) {
            ::cls_user_set_buckets_op result;
            result.entries.assign(value.entries.begin(), value.entries.end());
            result.add = value.add;
            result.time = value.time;
            return result;
        });
    ::cls_user_remove_bucket_op remove;
    remove.bucket = bucket_entry().bucket;
    check_request<decltype(remove), binding::bucket_request>(remove,
        binding::encode_bucket(remove.bucket), [](const auto& value) {
            ::cls_user_remove_bucket_op result;
            result.bucket = value.bucket;
            return result;
        });
    for (const int maximum : {0, -1, std::numeric_limits<int>::min(), std::numeric_limits<int>::max()}) {
        ::cls_user_list_buckets_op list;
        list.marker = std::string {"m\0r", 3};
        list.end_marker = std::string(80, 'e');
        list.max_entries = maximum;
        check_request<decltype(list), binding::bucket_list_request>(list,
            binding::encode_bucket_list(list.marker, list.end_marker, maximum), [](const auto& value) {
                ::cls_user_list_buckets_op result;
                result.marker = value.marker;
                result.end_marker = value.end_marker;
                result.max_entries = value.max_entries;
                return result;
            });
    }
    ::cls_user_get_header_op empty;
    check_request<decltype(empty), binding::empty_request>(empty,
        binding::encode_get_header(), [](const auto&) { return ::cls_user_get_header_op {}; });
    ::cls_user_complete_stats_sync_op sync;
    sync.time = bulk.time;
    check_request<decltype(sync), binding::time_request>(sync,
        binding::encode_time(sync.time), [](const auto& value) {
            ::cls_user_complete_stats_sync_op result;
            result.time = value.time;
            return result;
        });
    ::cls_user_reset_stats_op reset;
    reset.time = bulk.time;
    CHECK(binding::encode_time(reset.time).contents_equal(canonical(reset)));
    ::cls_user_reset_stats2_op reset2;
    reset2.time = bulk.time;
    reset2.marker = "continuation";
    reset2.acc_stats.total_entries = 987;
    check_request<decltype(reset2), binding::reset_request>(reset2, canonical(reset2), [](const auto& value) {
        ::cls_user_reset_stats2_op result;
        result.time = value.time;
        result.marker = value.marker;
        result.acc_stats = value.acc_stats;
        return result;
    });
    ::cls_user_account_resource_add_op add;
    add.entry = resource();
    add.exclusive = true;
    add.limit = std::numeric_limits<std::uint32_t>::max();
    check_request<decltype(add), binding::resource_add_request>(add,
        binding::encode_resource_add(add.entry, add.exclusive, add.limit), [](const auto& value) {
            ::cls_user_account_resource_add_op result;
            result.entry = value.entry;
            result.exclusive = value.exclusive;
            result.limit = value.limit;
            return result;
        });
    ::cls_user_account_resource_get_op get;
    get.name = std::string {"R\0name", 6};
    check_request<decltype(get), binding::name_request>(get, binding::encode_name(get.name), [](const auto& value) {
        ::cls_user_account_resource_get_op result;
        result.name = value.name;
        return result;
    });
    ::cls_user_account_resource_rm_op rm;
    rm.name = get.name;
    CHECK(binding::encode_name(rm.name).contents_equal(canonical(rm)));
    ::cls_user_account_resource_list_op list;
    list.marker = "marker";
    list.path_prefix = "/path/";
    list.max_entries = std::numeric_limits<std::uint32_t>::max();
    check_request<decltype(list), binding::resource_list_request>(list,
        binding::encode_resource_list(list.marker, list.path_prefix, list.max_entries), [](const auto& value) {
            ::cls_user_account_resource_list_op result;
            result.marker = value.marker;
            result.path_prefix = value.path_prefix;
            result.max_entries = value.max_entries;
            return result;
        });
}

TEST_CASE("User module reply records encode native list and vector clients identically", "[rgw][cls][user][datum]")
{
    ::cls_user_list_buckets_ret buckets;
    buckets.entries.push_back(bucket_entry());
    buckets.marker = "marker";
    buckets.truncated = true;
    binding::bucket_list_reply independent;
    independent.entries.assign(buckets.entries.begin(), buckets.entries.end());
    independent.marker = buckets.marker;
    independent.truncated = buckets.truncated;
    CHECK(canonical(independent).contents_equal(canonical(buckets)));
    ::cls_user_get_header_ret header;
    header.header.stats.total_entries = 79;
    CHECK(canonical(binding::header_reply {header.header}).contents_equal(canonical(header)));
    ::cls_user_reset_stats2_ret reset;
    reset.marker = "next";
    reset.acc_stats.total_entries = 83;
    reset.truncated = true;
    CHECK(canonical(binding::reset_reply {reset.marker, reset.acc_stats, reset.truncated}).contents_equal(canonical(reset)));
    ::cls_user_account_resource_get_ret get;
    get.entry = resource();
    CHECK(canonical(binding::resource_reply {get.entry}).contents_equal(canonical(get)));
    ::cls_user_account_resource_list_ret list;
    list.entries.push_back(resource());
    list.truncated = true;
    list.marker = "resource";
    CHECK(canonical(binding::resource_list_reply {list.entries, list.truncated, list.marker}).contents_equal(canonical(list)));
}

TEST_CASE("Bulk user decoding accepts oldest entry and rejects impossible counts before allocation", "[rgw][cls][user][datum]")
{
    for (const std::uint8_t version : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
        bytes leaf;
        auto body = [&] {
            ::ceph::encode(std::string {}, leaf);
            ::ceph::encode(std::uint64_t {37}, leaf);
            ::ceph::encode(std::uint32_t {17}, leaf);
            if (version >= 2) { ::ceph::encode(std::uint64_t {3}, leaf); }
            if (version >= 3) { ::encode(bucket_entry().bucket, leaf); }
            if (version >= 4) { ::ceph::encode(std::uint64_t {4096}, leaf); }
            if (version >= 6) { ::ceph::encode(true, leaf); }
            if (version >= 7) { ::ceph::encode(bucket_entry().creation_time, leaf); }
            if (version == 8) { ::ceph::encode(std::string {"old-placement"}, leaf); }
        };
        if (version < 5) {
            ::ceph::encode(version, leaf);
            body();
        }
        if (version >= 5) {
            ENCODE_START(version, 5, leaf);
            body();
            ENCODE_FINISH(leaf);
        }
        bytes input;
        ENCODE_START(1, 1, input);
        ::ceph::encode(std::uint32_t {1}, input);
        input.append(leaf);
        ::ceph::encode(false, input);
        ::ceph::encode(::ceph::real_time {}, input);
        ENCODE_FINISH(input);
        ::cls_user_set_buckets_op native;
        auto cursor = input.cbegin();
        ::decode(native, cursor);
        binding::bulk_request decoded;
        cursor = input.cbegin();
        binding::decode(decoded, cursor);
        REQUIRE(decoded.entries.size() == 1);
        CHECK(canonical(decoded.entries.front()).contents_equal(canonical(native.entries.front())));
    }
    bytes impossible;
    ENCODE_START(1, 1, impossible);
    ::ceph::encode(std::numeric_limits<std::uint32_t>::max(), impossible);
    ::ceph::encode(false, impossible);
    ::ceph::encode(::ceph::real_time {}, impossible);
    ENCODE_FINISH(impossible);
    binding::bulk_request value;
    auto cursor = impossible.cbegin();
    CHECK_THROWS_AS(binding::decode(value, cursor), ::ceph::buffer::error);
    CHECK(cursor.get_off() == 0);
    CHECK(value.entries.empty());
}

TEST_CASE("User native leaf hooks preserve placement branches and enclosing frame bounds", "[rgw][cls][user][datum]")
{
    for (const bool named_placement : {false, true}) {
        auto value = bucket_entry().bucket;
        if (!named_placement) { value.placement_id.clear(); }
        value.explicit_placement.data_pool = "data";
        value.explicit_placement.index_pool = "index";
        value.explicit_placement.data_extra_pool = "extra";
        ::cls_user_remove_bucket_op native;
        native.bucket = value;
        auto encoded = binding::encode_bucket(value);
        CHECK(encoded.contents_equal(canonical(native)));
        binding::bucket_request decoded;
        auto cursor = encoded.cbegin();
        binding::decode(decoded, cursor);
        CHECK(canonical(decoded.bucket).contents_equal(canonical(value)));
        // The native leaf has sufficient bytes outside its deliberately short
        // parent. The outer binding must still reject it without publication.
        auto raw = encoded.to_str();
        raw[2] = 1;
        raw[3] = raw[4] = raw[5] = 0;
        bytes short_parent;
        short_parent.append(raw);
        cursor = short_parent.cbegin();
        CHECK_THROWS_AS(binding::decode(decoded, cursor), ::ceph::buffer::error);
        CHECK(cursor.get_off() == 0);
        CHECK(canonical(decoded.bucket).contents_equal(canonical(value)));
    }
}
