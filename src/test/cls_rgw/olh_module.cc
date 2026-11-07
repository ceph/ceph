// RGW CLS: Exercise the registered OLH handler through controlled OMAP reads.
// This runs the real module; the host fixture supplies storage, not an OSD.
#include "cls/rgw/cls_rgw_ops.h"
#include "module_loader.h"
#include "test/cls_rgw/wire_test.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstdint>
#include <cstdlib>

#include <algorithm>
#include <array>
#include <fstream>
#include <limits>
#include <map>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using byte_buffer = ::ceph::buffer::list;

struct storage final
{
    std::map<std::string, byte_buffer> values;
    std::vector<std::string> reads;

    int failure = 0;
};

auto invoke(storage& context, byte_buffer input)
{
    const auto [olh_handler, olh_flags] = ::rgw_cls_test::get_method("bucket_read_olh_log");
    REQUIRE(olh_handler != nullptr);
    REQUIRE(olh_flags == CLS_METHOD_RD);
    byte_buffer output;
    const auto previous_reads = context.reads.size();
    const int status = olh_handler(&context, &input, &output);
    if (const char *path = std::getenv("CEPH_TEST_CLS_RGW_TRANSCRIPT")) {
        static std::ofstream transcript {path, std::ios::binary};
        byte_buffer record;
        ::ceph::encode(input, record);
        ::ceph::encode(status, record);
        ::ceph::encode(output, record);
        const std::vector<std::string> reads {context.reads.begin() + previous_reads, context.reads.end()};
        ::ceph::encode(reads, record);
        record.write_stream(transcript);
        REQUIRE(transcript.good());
    }

    return std::pair {status, std::move(output)};
}

auto query(storage& context, const ::rgw_cls_read_olh_log_op& request)
{
    byte_buffer input;
    ::encode(request, input);

    return invoke(context, std::move(input));
}

storage fixture(const ::rgw_bucket_olh_entry& value)
{
    storage context;
    byte_buffer bytes;
    ::encode(value, bytes);
    context.values.emplace(std::string {"\x80" "1001_"} + value.key.name, std::move(bytes));

    return context;
}

::rgw_bucket_olh_entry log_fixture(const unsigned groups, const bool stale)
{
    ::rgw_bucket_olh_entry value;
    value.key.name = "object";
    value.tag = "tag";
    for (unsigned epoch = 1; epoch <= groups; ++epoch) {
        ::rgw_bucket_olh_log_entry entry;
        entry.epoch = epoch + 100; // Row epoch is independent of its map key.
        entry.op = stale ? ::CLS_RGW_OLH_OP_STALE : ::CLS_RGW_OLH_OP_LINK_OLH;
        entry.op_tag = std::string {"t\0g", 3};
        entry.key = {"name", "version"};
        entry.delete_marker = epoch % 2;
        value.pending_log[epoch].push_back(std::move(entry));
    }

    return value;
}

::rgw_cls_read_olh_log_op request()
{
    ::rgw_cls_read_olh_log_op value;
    value.olh.name = "object";
    value.olh_tag = "tag";

    return value;
}

::rgw_cls_read_olh_log_ret decode_reply(const byte_buffer& bytes)
{
    ::rgw_cls_read_olh_log_ret value;
    auto cursor = bytes.cbegin();
    ::decode(value, cursor);
    REQUIRE(cursor.end());

    return value;
}

} // anonymous namespace

int cls_cxx_map_get_val(cls_method_context_t context, const std::string& key, byte_buffer *output)
{
    auto& state = *static_cast<storage *>(context);
    state.reads.push_back(key);
    if (state.failure) {
        return state.failure;
    }
    const auto found = state.values.find(key);
    if (found == state.values.end()) {
        return -ENOENT;
    }
    *output = found->second;

    return 0;
}

TEST_CASE("OLH empty and missing logs have a defined empty response", "[rgw][cls][olh]")
{
    const auto value = log_fixture(0, false);
    auto context = fixture(value);
    auto op = request();
    for (const auto marker : {std::uint64_t {0}, std::numeric_limits<std::uint64_t>::max()}) {
        op.ver_marker = marker;
        const auto [status, bytes] = query(context, op);
        REQUIRE(status == 0);
        const auto reply = decode_reply(bytes);
        CHECK(reply.log.empty());
        CHECK_FALSE(reply.is_truncated);
    }
    context.values.clear();
    op.olh_tag.clear();
    const auto [status, bytes] = query(context, op);
    REQUIRE(status == 0);
    CHECK(decode_reply(bytes).log.empty());
}

TEST_CASE("OLH pages count epochs before stale filtering", "[rgw][cls][olh]")
{
    auto context = fixture(log_fixture(1002, true));
    auto op = request();
    auto [status, bytes] = query(context, op);
    REQUIRE(status == 0);
    auto reply = decode_reply(bytes);
    CHECK(reply.log.empty());
    CHECK(reply.is_truncated);

    op.get_stales = true;
    std::tie(status, bytes) = query(context, op);
    REQUIRE(status == 0);
    reply = decode_reply(bytes);
    REQUIRE(reply.log.size() == 1000);
    CHECK(reply.log.begin()->first == 1);
    CHECK(reply.log.rbegin()->first == 1000);
    CHECK(reply.log.begin()->second.front().epoch == 101);
    CHECK(reply.is_truncated);

    op.ver_marker = 1000;
    std::tie(status, bytes) = query(context, op);
    REQUIRE(status == 0);
    reply = decode_reply(bytes);
    REQUIRE(reply.log.size() == 2);
    CHECK(reply.log.begin()->first == 1001);
    CHECK_FALSE(reply.is_truncated);
}

TEST_CASE("OLH input and storage errors retain native status and read boundaries", "[rgw][cls][olh]")
{
    auto context = fixture(log_fixture(1, false));
    auto op = request();
    op.olh.instance = "invalid";
    CHECK(query(context, op).first == -EINVAL);
    CHECK(context.reads.empty());
    op.olh.instance.clear();
    op.olh_tag = "mismatch";
    CHECK(query(context, op).first == -ECANCELED);
    REQUIRE(context.reads.size() == 1);
    CHECK(context.reads.front() == std::string {"\x80" "1001_object"});
    op.olh_tag = "tag";
    context.failure = -EAGAIN;
    CHECK(query(context, op).first == -EAGAIN);
    context.failure = 0;
    context.values.begin()->second.clear();
    CHECK(query(context, op).first == -EIO);

    byte_buffer full;
    ::encode(op, full);
    for (unsigned size = 0; size < full.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(full, 0, size);
        context.reads.clear();
        CHECK(invoke(context, std::move(prefix)).first == -EINVAL);
        CHECK(context.reads.empty());
    }
}

TEST_CASE("OLH native action type preserves every wire byte", "[rgw][cls][olh]")
{
    static_assert(std::is_same_v<std::underlying_type_t<::OLHLogOp>, std::uint32_t>);
    static_assert(sizeof(::OLHLogOp) == 4);
    for (unsigned raw = 0; raw < 256; ++raw) {
        auto value = log_fixture(1, false);
        value.pending_log.begin()->second.front().op = static_cast<::OLHLogOp>(raw);
        auto context = fixture(value);
        auto op = request();
        op.get_stales = true;
        const auto [status, bytes] = query(context, op);
        REQUIRE(status == 0);
        const auto reply = decode_reply(bytes);
        REQUIRE(reply.log.size() == 1);
        CHECK(static_cast<unsigned>(reply.log.begin()->second.front().op) == raw);
    }
}

TEST_CASE("OLH page boundaries preserve exact native replies across log shapes", "[rgw][cls][olh]")
{
    for (const unsigned count : {0, 1, 2, 17, 999, 1000, 1001, 1002}) {
        for (const unsigned shape : {0, 1, 2, 3}) {
            auto value = log_fixture(count, shape == 1);
            for (auto& [epoch, entries] : value.pending_log) {
                if (shape == 2 && 0 == epoch % 2) {
                    entries.front().op = ::CLS_RGW_OLH_OP_STALE;
                }
                if (shape == 3 && 0 == epoch % 3) {
                    entries.clear();
                }
            }
            auto context = fixture(value);
            for (const std::uint64_t marker : {0ULL, 1ULL, 2ULL, 16ULL, 998ULL, 999ULL,
                                               1000ULL, 1001ULL, 1002ULL, 0xFFFFFFFFFFFFFFFFULL}) {
                for (const bool stales : {false, true}) {
                    auto op = request();
                    op.ver_marker = marker;
                    op.get_stales = stales;
                    ::rgw_cls_read_olh_log_ret expected;
                    auto cursor = value.pending_log.upper_bound(marker);
                    for (unsigned selected = 0; selected < 1000 && cursor != value.pending_log.end();
                         ++selected, ++cursor) {
                        auto entries = cursor->second;
                        if (!stales) {
                            std::erase_if(entries, [](const auto& entry) {
                                return ::CLS_RGW_OLH_OP_STALE == entry.op;
                            });
                        }
                        if (stales || !entries.empty()) {
                            expected.log.emplace(cursor->first, std::move(entries));
                        }
                    }
                    expected.is_truncated = cursor != value.pending_log.end();
                    byte_buffer wanted;
                    ::encode(expected, wanted);
                    context.reads.clear();
                    const auto [status, bytes] = query(context, op);
                    REQUIRE(status == 0);
                    CHECK(bytes.contents_equal(wanted));
                    CHECK(context.reads == std::vector<std::string> {"\x80" "1001_object"});
                }
            }
        }
    }
}

TEST_CASE("OLH sparse epochs preserve exclusive markers through the largest key", "[rgw][cls][olh]")
{
    constexpr auto last_epoch = std::numeric_limits<std::uint64_t>::max();
    auto value = log_fixture(1, false);
    const auto events = value.pending_log.begin()->second;
    value.pending_log.clear();
    for (const auto epoch : std::array<std::uint64_t, 4> {0, 1, last_epoch - 1, last_epoch}) {
        value.pending_log.emplace(epoch, events);
    }

    for (const bool dense_fragments : {false, true}) {
        auto context = fixture(value);
        if (dense_fragments) {
            auto& stored = context.values.begin()->second;
            stored = ::rgw_cls_test::fragmented(stored);
        }
        for (const auto marker :
             std::array<std::uint64_t, 5> {0, 1, 2, last_epoch - 1, last_epoch}) {
            ::rgw_cls_read_olh_log_ret expected;
            for (const auto& [epoch, rows] : value.pending_log) {
                if (epoch > marker) {
                    expected.log.emplace(epoch, rows);
                }
            }
            byte_buffer wanted;
            ::encode(expected, wanted);
            for (const bool stales : {false, true}) {
                auto op = request();
                op.ver_marker = marker;
                op.get_stales = stales;
                const auto [status, bytes] = query(context, op);
                REQUIRE(status == 0);
                CHECK(bytes.contents_equal(wanted));
            }
        }
    }
}

TEST_CASE("Stored OLH forwarding validates all rows and retains fragmented storage", "[rgw][cls][olh][stored]")
{
    auto value = log_fixture(1002, false);
    value.tag = std::string {"t\0g", 3};
    auto context = fixture(value);
    auto& stored = context.values.begin()->second;
    byte_buffer fragments;
    auto cursor = stored.cbegin();
    while (!cursor.end()) {
        char byte;
        cursor.copy(1, &byte);
        fragments.append(::ceph::buffer::copy(&byte, 1));
    }
    stored = std::move(fragments);
    auto op = request();
    op.get_stales = true;
    op.olh_tag = value.tag;
    op.ver_marker = 1000;
    const auto [status, output] = query(context, op);
    REQUIRE(status == 0);
    REQUIRE(context.reads.size() == 1);
    context.values.clear();
    const auto reply = decode_reply(output);
    REQUIRE(reply.log.size() == 2);
    CHECK(reply.log.begin()->first == 1001);
    CHECK_FALSE(reply.is_truncated);

    context = fixture(log_fixture(1, false));
    op = request();
    op.get_stales = true;
    const auto full = context.values.begin()->second;
    for (unsigned size = 0; size < full.length(); ++size) {
        auto& encoded = context.values.begin()->second;
        encoded.clear();
        encoded.substr_of(full, 0, size);
        context.reads.clear();
        const auto [failure, bytes] = query(context, op);
        CHECK(failure == -EIO);
        CHECK(bytes.length() == 0);
        CHECK(context.reads.size() == 1);
    }
}

TEST_CASE("Stored OLH noncanonical representations preserve native normalization", "[rgw][cls][olh][stored]")
{
    const auto value = log_fixture(1, false);
    const auto events = value.pending_log.begin()->second;
    for (const unsigned shape : {0, 1, 2, 3, 4}) {
        byte_buffer stored;
        ENCODE_START(1, 1, stored);
        ::encode(value.key, stored);
        ::ceph::encode(value.delete_marker, stored);
        ::ceph::encode(value.epoch, stored);
        ::ceph::encode(std::uint32_t {2}, stored);
        for (const std::uint64_t epoch : {1ULL, 2ULL}) {
            ::ceph::encode(0 == shape ? std::uint64_t {1} : (4 == shape ? 3 - epoch : epoch), stored);
            ::ceph::encode(std::uint32_t {1}, stored);
            byte_buffer row;
            auto event = events.front();
            event.op_tag = 1 == epoch ? "first" : "second";
            ::encode(event, row);
            auto bytes = row.to_str();
            if (shape == 1) {
                bytes.back() = 7; // Native bool decoding normalizes nonzero bytes.
            }
            if (shape == 2) {
                bytes[0] = 2; // Compatible future event frame.
            }
            if (shape == 3) {
                bytes[1] = 0; // Native output restores compatibility byte1.
            }
            stored.append(bytes);
        }
        ::ceph::encode(value.tag, stored);
        ::ceph::encode(value.exists, stored);
        ::ceph::encode(value.pending_removal, stored);
        ENCODE_FINISH(stored);

        ::rgw_bucket_olh_entry native;
        auto cursor = stored.cbegin();
        ::decode(native, cursor);
        ::rgw_cls_read_olh_log_ret expected;
        expected.log = native.pending_log;
        byte_buffer wanted;
        ::encode(expected, wanted);
        auto context = fixture(value);
        context.values.begin()->second = stored;
        auto op = request();
        op.get_stales = true;
        const auto [status, output] = query(context, op);
        REQUIRE(status == 0);
        CHECK(output.contents_equal(wanted));
        CHECK(context.reads.size() == 1);

        auto broken = stored.to_str();
        broken.pop_back();
        context.values.begin()->second.clear();
        context.values.begin()->second.append(broken);
        op.ver_marker = std::numeric_limits<std::uint64_t>::max();
        CHECK(query(context, op).first == -EIO);
    }
}

TEST_CASE("Stored OLH projection validates malformed events beyond its returned page", "[rgw][cls][olh][stored]")
{
    const auto value = log_fixture(1002, false);
    auto context = fixture(value);
    auto& stored = context.values.begin()->second;
    auto cursor = stored.cbegin();
    cursor += 6;
    ::cls_rgw_obj_key key;
    bool deleted;
    std::uint64_t head_epoch;
    std::uint32_t groups;
    ::decode(key, cursor);
    ::ceph::decode(deleted, cursor);
    ::ceph::decode(head_epoch, cursor);
    ::ceph::decode(groups, cursor);
    unsigned last = 0;
    for (std::uint32_t group = 0; group < groups; ++group) {
        std::uint64_t epoch;
        std::uint32_t count;
        ::ceph::decode(epoch, cursor);
        ::ceph::decode(count, cursor);
        for (std::uint32_t item = 0; item < count; ++item) {
            last = cursor.get_off();
            ::rgw_bucket_olh_log_entry event;
            ::decode(event, cursor);
        }
    }
    auto broken = stored.to_str();
    broken[last + 1] = 2; // Unsupported compatibility byte on the last event.
    stored.clear();
    stored.append(broken);
    auto op = request();
    op.get_stales = true;
    for (const std::uint64_t marker : {0ULL, 1000ULL, 0xFFFFFFFFFFFFFFFFULL}) {
        op.ver_marker = marker;
        context.reads.clear();
        const auto [status, bytes] = query(context, op);
        CHECK(status == -EIO);
        CHECK(bytes.length() == 0);
        CHECK(context.reads.size() == 1);
    }
}
