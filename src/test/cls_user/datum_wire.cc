// Independent user records must accept the same concrete native encoding.
#include "datum_test.h"
#include "rgw/datum/adapters/cls/user.h"
#include "test/cls_rgw/new_limit.h"

#include <catch2/catch_test_macros.hpp>
#include <limits>
#include <type_traits>

namespace {

using namespace user_cls_test;
namespace binding = cls::rgw::user_datum;

bytes reply_frame(const std::uint8_t version, const auto& body,
                  const std::uint8_t compatible = 1)
{
    bytes output;
    ENCODE_START(version, compatible, output);
    body(output);
    if (version > 1) {
        output.append("extension", 9);
    }
    ENCODE_FINISH(output);

    return output;
}

bytes bucket_leaf(const std::uint8_t version)
{
    bytes leaf;
    const auto body = [&] {
        ceph::encode(std::string {}, leaf);
        ceph::encode(std::uint64_t {37}, leaf);
        ceph::encode(std::uint32_t {17}, leaf);
        if (version >= 2) { ceph::encode(std::uint64_t {3}, leaf); }
        if (version >= 3) { ::encode(bucket_entry().bucket, leaf); }
        if (version >= 4) { ceph::encode(std::uint64_t {4096}, leaf); }
        if (version >= 6) { ceph::encode(true, leaf); }
        if (version >= 7) { ceph::encode(bucket_entry().creation_time, leaf); }
        if (version == 8) { ceph::encode(std::string {"old-placement"}, leaf); }
    };
    if (version < 5) {
        ceph::encode(version, leaf);
        body();

        return leaf;
    }
    ENCODE_START(version, 5, leaf);
    body();
    if (version > 9) { leaf.append("extension", 9); }
    ENCODE_FINISH(leaf);

    return leaf;
}

template <typename NativeT>
void check_reply(const bytes& input, const auto& decode_reply, const auto& project)
{
    NativeT expected;
    auto cursor = input.cbegin();
    ::decode(expected, cursor);
    const auto wanted = canonical(project(expected));
    for (const bool fragments : {false, true}) {
        auto source = fragments ? rgw_cls_test::fragmented(input) : input;
        CHECK(canonical(decode_reply(source)).contents_equal(wanted));
        for (unsigned size = 0; size < source.length(); ++size) {
            bytes prefix;
            prefix.substr_of(source, 0, size);
            CHECK_THROWS_AS(decode_reply(prefix), ceph::buffer::error);
        }
        source.append("outside", 7);
        CHECK(canonical(decode_reply(source)).contents_equal(wanted));
    }
}

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
        for (const char ch : raw) { full.append(ceph::buffer::copy(&ch, 1)); }
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
            CHECK_THROWS_AS(binding::decode(value, cursor), ceph::buffer::error);
            CHECK(cursor.get_remaining() == prefix.length());
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
        CHECK_THROWS_AS(binding::decode(value, cursor), ceph::buffer::error);
        CHECK(cursor.get_remaining() == incompatible.length());
    }
}

} // anonymous namespace

TEST_CASE("Account resource replies retain native versions opaque metadata and page values",
          "[rgw][cls][user][datum][resource]")
{
    static_assert(std::is_nothrow_move_assignable_v<binding::resource_list_reply>);
    for (const std::uint8_t version : {0, 1, 31}) {
        for (const std::uint8_t leaf_version : {0, 1, 31}) {
            const auto value = resource(std::string {"R\0name", 6}, std::string {"/p\0/", 4});
            const auto leaf = reply_frame(leaf_version, [&](bytes& output) {
                ceph::encode(value.name, output);
                ceph::encode(value.path, output);
                ceph::encode(value.metadata, output);
            });
            const auto get = reply_frame(version, [&](bytes& output) { output.append(leaf); });
            check_reply<cls_user_account_resource_get_ret>(get, binding::decode_resource_reply,
                [](const auto& reply) -> const auto& { return reply.entry; });
            for (const std::uint8_t truncated : {0, 1, 2}) {
                const auto list = reply_frame(version, [&](bytes& output) {
                    ceph::encode(std::uint32_t {2}, output);
                    output.append(leaf);
                    output.append(leaf);
                    ceph::encode(truncated, output);
                    ceph::encode(std::string {"m\0r", 3}, output);
                });
                check_reply<cls_user_account_resource_list_ret>(list,
                    binding::decode_resource_list_reply, [](const auto& reply) -> const auto& {
                        return reply;
                    });
            }
        }
    }
    cls_user_account_resource_list_ret empty;
    check_reply<decltype(empty)>(canonical(empty), binding::decode_resource_list_reply,
        [](const auto& reply) -> const auto& { return reply; });
}

TEST_CASE("User bucket replies decode directly into native lists across legacy leaf versions",
          "[rgw][cls][user][datum][bucket-list]")
{
    using page_type = binding::bucket_list_reply<std::list<cls_user_bucket_entry>>;
    static_assert(std::is_nothrow_move_assignable_v<page_type>);
    const auto decode_reply = [](const bytes& input) {
        auto page = binding::decode_bucket_list_reply(input);
        cls_user_list_buckets_ret native;
        native.entries = std::move(page.entries);
        native.marker = std::move(page.marker);
        native.truncated = page.truncated;

        return native;
    };
    for (const std::uint8_t version : {0, 1, 31}) {
        for (const std::uint8_t leaf_version : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 31}) {
            const auto leaf = bucket_leaf(leaf_version);
            const auto input = reply_frame(version, [&](bytes& output) {
                ceph::encode(std::uint32_t {2}, output);
                output.append(leaf);
                output.append(leaf);
                ceph::encode(std::string {"m\0r", 3}, output);
                ceph::encode(std::uint8_t {2}, output);
            });
            check_reply<cls_user_list_buckets_ret>(input, decode_reply,
                [](const auto& page) -> const auto& { return page; });
        }
    }
    const auto incompatible = reply_frame(31, [](bytes&) {}, 2);
    CHECK_THROWS_AS(binding::decode_bucket_list_reply(incompatible), ceph::buffer::error);
    const auto missing_flag = reply_frame(1, [](bytes& output) {
        ceph::encode(std::list<cls_user_bucket_entry> {}, output);
        ceph::encode(std::string {}, output);
    });
    CHECK_THROWS_AS(binding::decode_bucket_list_reply(missing_flag), ceph::buffer::error);
    const auto empty = binding::decode_bucket_list_reply(canonical(cls_user_list_buckets_ret {}));
    CHECK(empty.entries.empty());
    CHECK(empty.marker.empty());
    CHECK_FALSE(empty.truncated);
}

TEST_CASE("User header replies retain native versions and partial error fields in place",
          "[rgw][cls][user][datum][header]")
{
    const auto decode_reply = [](const bytes& input) {
        cls_user_get_header_ret value;
        binding::decode_header_reply(input, value.header);

        return value;
    };
    cls_user_header value;
    value.stats.total_entries = 17;
    value.stats.total_bytes = std::uint64_t {1} << 40;
    value.last_stats_sync = bucket_entry().creation_time;
    value.last_stats_update = value.last_stats_sync;
    for (const std::uint8_t version : {0, 1, 31}) {
        for (const std::uint8_t leaf_version : {0, 1, 31}) {
            const auto leaf = reply_frame(leaf_version, [&](bytes& output) {
                ::encode(value.stats, output);
                ceph::encode(value.last_stats_sync, output);
                ceph::encode(value.last_stats_update, output);
            });
            const auto input = reply_frame(version, [&](bytes& output) { output.append(leaf); });
            check_reply<cls_user_get_header_ret>(input, decode_reply,
                [](const auto& page) -> const auto& { return page; });
        }
    }
    const auto partial = reply_frame(1, [&](bytes& output) {
        output.append(reply_frame(1, [&](bytes& fields) {
            ::encode(value.stats, fields);
            ceph::encode(value.last_stats_sync, fields); // Absent last-update field.
        }));
    });
    auto raw = canonical(value).to_str();
    bytes prefix;
    prefix.append(raw.data(), raw.size() - 1);
    auto overrun = reply_frame(1, [&](bytes& output) { output.append(prefix); });
    overrun.append(raw.data() + raw.size() - 1, 1);
    const auto incompatible = reply_frame(31, [](bytes&) {}, 2);
    for (const auto& input : {partial, overrun, incompatible}) {
        cls_user_get_header_ret expected;
        expected.header.stats.total_entries = 103;
        expected.header.last_stats_update = ceph::real_time {std::chrono::seconds {123}};
        auto actual = expected.header;
        auto cursor = input.cbegin();
        CHECK_THROWS_AS(::decode(expected, cursor), ceph::buffer::error);
        CHECK_THROWS_AS(binding::decode_header_reply(input, actual), ceph::buffer::error);
        CHECK(canonical(actual).contents_equal(canonical(expected.header)));
    }
}

TEST_CASE("User reset replies retain accumulated widths native leaf versions and frame bounds",
          "[rgw][cls][user][datum][reset-accumulator]")
{
    static_assert(std::is_nothrow_move_assignable_v<binding::reset_reply>);
    for (const std::uint8_t version : {0, 1, 31}) {
        for (const std::uint8_t leaf_version : {0, 1, 31}) {
            const auto accumulated = reply_frame(leaf_version, [](bytes& output) {
                ceph::encode(std::numeric_limits<std::uint64_t>::max(), output);
                ceph::encode(std::uint64_t {1} << 40, output);
                ceph::encode(std::uint64_t {1} << 42, output);
            });
            for (const std::uint8_t flag : {0, 1, 2}) {
                const auto input = reply_frame(version, [&](bytes& output) {
                    ceph::encode(std::string {"m\0r", 3}, output);
                    output.append(accumulated);
                    ceph::encode(flag, output);
                });
                check_reply<cls_user_reset_stats2_ret>(input, binding::decode_reset_reply,
                    [](const auto& page) -> const auto& { return page; });
            }
        }
    }
    const auto incompatible = reply_frame(31, [](bytes&) {}, 2);
    CHECK_THROWS_AS(binding::decode_reset_reply(incompatible), ceph::buffer::error);
    const auto missing_flag = reply_frame(1, [](bytes& output) {
        ceph::encode(std::string {"next"}, output);
        ::encode(cls_user_stats {}, output);
    });
    CHECK_THROWS_AS(binding::decode_reset_reply(missing_flag), ceph::buffer::error);
}

TEST_CASE("Account resource replies reject incompatible frames and enclosed-field overruns",
          "[rgw][cls][user][datum][resource]")
{
    const auto incompatible = reply_frame(31, [](bytes&) {}, 2);
    CHECK_THROWS_AS(binding::decode_resource_reply(incompatible), ceph::buffer::error);
    CHECK_THROWS_AS(binding::decode_resource_list_reply(incompatible), ceph::buffer::error);
    const auto leaf = canonical(resource());
    bytes prefix;
    prefix.substr_of(leaf, 0, leaf.length() - 1);
    auto overrun = reply_frame(1, [&](bytes& output) { output.append(prefix); });
    const char last = leaf[leaf.length() - 1];
    overrun.append(&last, 1);
    cls_user_account_resource_get_ret native;
    auto cursor = overrun.cbegin();
    CHECK_THROWS_AS(::decode(native, cursor), ceph::buffer::error);
    CHECK_THROWS_AS(binding::decode_resource_reply(overrun), ceph::buffer::error);
    const auto missing_marker = reply_frame(1, [&](bytes& output) {
        ceph::encode(std::vector {resource()}, output);
        ceph::encode(true, output);
    });
    CHECK_THROWS_AS(binding::decode_resource_list_reply(missing_marker), ceph::buffer::error);
}

TEST_CASE("Resource page counts are bounded before native vector allocation", "[rgw][cls][user][datum]")
{
    for (const std::uint8_t version : {0, 1, 3}) {
        const auto impossible = reply_frame(version, [](bytes& output) {
            ceph::encode(std::uint32_t {4096}, output);
            ceph::encode(false, output);
            ceph::encode(std::string {}, output);
        });
        const auto decode = [&] {
            const rgw_cls_test::scoped_new_limit limit {65536};

            return binding::decode_resource_list_reply(impossible);
        };
        // The guard exposes a late count check by turning an oversized reserve into bad_alloc.
        CHECK_THROWS_AS(decode(), ceph::buffer::error);

        const auto smallest = reply_frame(version, [](bytes& output) {
            ceph::encode(std::uint32_t {1}, output);
            output.append(reply_frame(0, [](bytes& leaf) {
                ceph::encode(std::string {}, leaf);
                ceph::encode(std::string {}, leaf);
                ceph::encode(bytes {}, leaf);
            }));
            ceph::encode(false, output);
            ceph::encode(std::string {}, output);
        });
        check_reply<cls_user_account_resource_list_ret>(smallest,
            binding::decode_resource_list_reply, [](const auto& reply) -> const auto& { return reply; });
    }
}

TEST_CASE("User request bindings retain native versions fragments widths and atomic errors", "[rgw][cls][user][datum]")
{
    static_assert(std::is_nothrow_move_assignable_v<cls_user_bucket_entry>);
    static_assert(std::is_nothrow_move_assignable_v<cls_user_account_resource>);
    cls_user_set_buckets_op bulk;
    bulk.entries.push_back(bucket_entry());
    bulk.add = true;
    bulk.time = bulk.entries.front().creation_time;
    check_request<decltype(bulk), binding::bulk_request>(bulk,
        binding::encode_bulk(bulk.entries, bulk.add, bulk.time), [](const auto& value) {
            cls_user_set_buckets_op result;
            result.entries.assign(value.entries.begin(), value.entries.end());
            result.add = value.add;
            result.time = value.time;
            return result;
        });
    cls_user_remove_bucket_op remove;
    remove.bucket = bucket_entry().bucket;
    check_request<decltype(remove), binding::bucket_request>(remove,
        binding::encode_bucket(remove.bucket), [](const auto& value) {
            cls_user_remove_bucket_op result;
            result.bucket = value.bucket;
            return result;
        });
    for (const int maximum : {0, -1, std::numeric_limits<int>::min(), std::numeric_limits<int>::max()}) {
        cls_user_list_buckets_op list;
        list.marker = std::string {"m\0r", 3};
        list.end_marker = std::string(80, 'e');
        list.max_entries = maximum;
        check_request<decltype(list), binding::bucket_list_request>(list,
            binding::encode_bucket_list(list.marker, list.end_marker, maximum), [](const auto& value) {
                cls_user_list_buckets_op result;
                result.marker = value.marker;
                result.end_marker = value.end_marker;
                result.max_entries = value.max_entries;
                return result;
            });
    }
    cls_user_get_header_op empty;
    check_request<decltype(empty), binding::empty_request>(empty,
        binding::encode_get_header(), [](const auto&) { return cls_user_get_header_op {}; });
    cls_user_complete_stats_sync_op sync;
    sync.time = bulk.time;
    check_request<decltype(sync), binding::time_request>(sync,
        binding::encode_time(sync.time), [](const auto& value) {
            cls_user_complete_stats_sync_op result;
            result.time = value.time;
            return result;
        });
    cls_user_reset_stats_op reset;
    reset.time = bulk.time;
    CHECK(binding::encode_time(reset.time).contents_equal(canonical(reset)));
    cls_user_reset_stats2_op reset2;
    reset2.time = bulk.time;
    reset2.marker = "continuation";
    reset2.acc_stats.total_entries = 987;
    check_request<decltype(reset2), binding::reset_request>(reset2,
        binding::encode_reset(reset2.time, reset2.marker, reset2.acc_stats), [](const auto& value) {
            cls_user_reset_stats2_op result;
            result.time = value.time;
            result.marker = value.marker;
            result.acc_stats = value.acc_stats;
            return result;
        });
    cls_user_account_resource_add_op add;
    add.entry = resource();
    add.exclusive = true;
    add.limit = std::numeric_limits<std::uint32_t>::max();
    check_request<decltype(add), binding::resource_add_request>(add,
        binding::encode_resource_add(add.entry, add.exclusive, add.limit), [](const auto& value) {
            cls_user_account_resource_add_op result;
            result.entry = value.entry;
            result.exclusive = value.exclusive;
            result.limit = value.limit;
            return result;
        });
    cls_user_account_resource_get_op get;
    get.name = std::string {"R\0name", 6};
    check_request<decltype(get), binding::name_request>(get, binding::encode_name(get.name), [](const auto& value) {
        cls_user_account_resource_get_op result;
        result.name = value.name;
        return result;
    });
    cls_user_account_resource_rm_op rm;
    rm.name = get.name;
    CHECK(binding::encode_name(rm.name).contents_equal(canonical(rm)));
    cls_user_account_resource_list_op list;
    list.marker = "marker";
    list.path_prefix = "/path/";
    list.max_entries = std::numeric_limits<std::uint32_t>::max();
    check_request<decltype(list), binding::resource_list_request>(list,
        binding::encode_resource_list(list.marker, list.path_prefix, list.max_entries), [](const auto& value) {
            cls_user_account_resource_list_op result;
            result.marker = value.marker;
            result.path_prefix = value.path_prefix;
            result.max_entries = value.max_entries;
            return result;
        });
}

TEST_CASE("User module reply records encode native list and vector clients identically", "[rgw][cls][user][datum]")
{
    cls_user_list_buckets_ret buckets;
    buckets.entries.push_back(bucket_entry());
    buckets.marker = "marker";
    buckets.truncated = true;
    binding::bucket_list_reply<> independent;
    independent.entries.assign(buckets.entries.begin(), buckets.entries.end());
    independent.marker = buckets.marker;
    independent.truncated = buckets.truncated;
    CHECK(canonical(independent).contents_equal(canonical(buckets)));
    cls_user_get_header_ret header;
    header.header.stats.total_entries = 79;
    CHECK(canonical(binding::header_reply {header.header}).contents_equal(canonical(header)));
    cls_user_reset_stats2_ret reset;
    reset.marker = "next";
    reset.acc_stats.total_entries = 83;
    reset.truncated = true;
    CHECK(canonical(binding::reset_reply {reset.marker, reset.acc_stats, reset.truncated}).contents_equal(canonical(reset)));
    cls_user_account_resource_get_ret get;
    get.entry = resource();
    CHECK(canonical(binding::resource_reply {get.entry}).contents_equal(canonical(get)));
    cls_user_account_resource_list_ret list;
    list.entries.push_back(resource());
    list.truncated = true;
    list.marker = "resource";
    CHECK(canonical(binding::resource_list_reply {list.entries, list.truncated, list.marker}).contents_equal(canonical(list)));
}

TEST_CASE("Bulk user decoding accepts oldest entry and rejects impossible counts before allocation", "[rgw][cls][user][datum]")
{
    for (const std::uint8_t version : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
        const auto leaf = bucket_leaf(version);
        bytes input;
        ENCODE_START(1, 1, input);
        ceph::encode(std::uint32_t {1}, input);
        input.append(leaf);
        ceph::encode(false, input);
        ceph::encode(ceph::real_time {}, input);
        ENCODE_FINISH(input);
        cls_user_set_buckets_op native;
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
    ceph::encode(std::numeric_limits<std::uint32_t>::max(), impossible);
    ceph::encode(false, impossible);
    ceph::encode(ceph::real_time {}, impossible);
    ENCODE_FINISH(impossible);
    binding::bulk_request value;
    auto cursor = impossible.cbegin();
    CHECK_THROWS_AS(binding::decode(value, cursor), ceph::buffer::error);
    CHECK(cursor.get_remaining() == impossible.length());
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
        cls_user_remove_bucket_op native;
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
        CHECK_THROWS_AS(binding::decode(decoded, cursor), ceph::buffer::error);
        CHECK(cursor.get_remaining() == short_parent.length());
        CHECK(canonical(decoded.bucket).contents_equal(canonical(value)));
    }
}
