// RGW CLS: OLH native-wire compatibility with independent records/containers.
#include "test/cls_rgw/wire_test.h"
#include "olh_native_oracle.h"
#include "rgw/datum/adapters/cls/olh.h"
#include "rgw/datum/adapters/cls/olh_wire.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstdint>
#include <limits>
#include <utility>

#include <algorithm>
#include <array>
#include <map>
#include <random>
#include <string>
#include <vector>

namespace {

namespace wire = ::cls::rgw::olh_wire;
namespace native = ::cls::rgw::olh_datum;
namespace datum = ::ceph::salad::datum;
namespace olh = ::cls::rgw::olh;
using byte_buffer = ::ceph::buffer::list;

using ::rgw_cls_test::fragmented;

constexpr auto event_fields = datum::binary_fields<&olh::event::epoch, &olh::event::operation,
    &olh::event::tag, &olh::event::object, &olh::event::delete_marker> {};
constexpr auto key_fields = datum::binary_fields<&olh::object_key::name, &olh::object_key::instance> {};

auto fixture()
{
    return olh::reply {{{7, {{{std::string {"n\0m", 3}, "version"}, "tag", 99,
                              olh::action::link, true},
                             {{"other", ""}, "", 0, static_cast<olh::action>(255), false}}},
                       {42, {}}, {std::numeric_limits<std::uint64_t>::max(), {}}}, true};
}

auto encode(const olh::reply& value, const bool stales = true)
{
    return wire::encode_reply(event_fields, key_fields, value.epochs, value.truncated,
        [stales](const olh::event& entry) { return stales || olh::action::stale != entry.operation; }, stales);
}

void check(const olh::reply& actual, const olh::reply& expected)
{
    CHECK(actual.truncated == expected.truncated);
    REQUIRE(actual.epochs.size() == expected.epochs.size());
    for (std::size_t i = 0; i < expected.epochs.size(); ++i) {
        CHECK(actual.epochs[i].first == expected.epochs[i].first);
        const auto& rows = actual.epochs[i].second;
        const auto& wanted = expected.epochs[i].second;
        REQUIRE(rows.size() == wanted.size());
        for (std::size_t j = 0; j < wanted.size(); ++j) {
            CHECK(rows[j].epoch == wanted[j].epoch);
            CHECK(rows[j].operation == wanted[j].operation);
            CHECK(rows[j].tag == wanted[j].tag);
            CHECK(rows[j].object.name == wanted[j].object.name);
            CHECK(rows[j].object.instance == wanted[j].object.instance);
            CHECK(rows[j].delete_marker == wanted[j].delete_marker);
        }
    }
}

} // anonymous namespace

TEST_CASE("OLH filtering borrows an immobile mutable predicate", "[rgw][cls][datum][olh]")
{
    struct predicate final
    {
        unsigned calls = 0;

        predicate() = default;
        predicate(const predicate&) = delete;
        predicate(predicate&&) = delete;

        bool operator()(const olh::event& entry)
        {
            ++calls;

            return olh::action::stale != entry.operation;
        }

    };

    auto expected = fixture();
    auto stale = expected.epochs.front().second.front();
    stale.operation = olh::action::stale;
    expected.epochs.front().second.push_back(stale);
    predicate include;
    const auto bytes = wire::encode_reply(event_fields, key_fields, expected.epochs,
                                         expected.truncated, include, false);
    CHECK(bytes.contents_equal(encode(expected, false)));
    CHECK(0 < include.calls);
}

TEST_CASE("OLH requests preserve native bytes, versions and nested extensions", "[rgw][cls][datum][olh]")
{
    const rgw_olh_oracle::request expected {{std::string {"n\0m", 3}, "instance"},
                                            std::string {"t\0g", 3}, 0xFEDCBA9876543210, true};
    const auto bytes = wire::encode_request(expected.key.name, expected.key.instance,
                                            expected.marker, expected.tag, expected.stales);
    CHECK(bytes.contents_equal(rgw_olh_oracle::encode_request(expected)));
    const auto decoded = rgw_olh_oracle::decode_request(bytes);
    CHECK(decoded.key.name == expected.key.name);
    CHECK(decoded.key.instance == expected.key.instance);
    CHECK(decoded.marker == expected.marker);
    CHECK(decoded.tag == expected.tag);
    CHECK(decoded.stales);
    for (const auto version : {1, 2, 3}) {
        const auto input = fragmented(rgw_olh_oracle::encode_request(expected, version, true));
        auto cursor = input.cbegin();
        native::request result;
        native::decode(result, cursor);
        CHECK(result.olh.name == expected.key.name);
        CHECK(result.olh.instance == expected.key.instance);
        CHECK(result.tag() == expected.tag);
        CHECK(result.ver_marker == expected.marker);
        CHECK(result.get_stales == (version >= 2));
        CHECK(cursor.end());
    }
}

TEST_CASE("OLH request tags retain storage across field boundaries", "[rgw][cls][datum][olh][request]")
{
    for (const auto& tag : {std::string {}, std::string {"t\0g", 3},
                            std::string(32, 't'), std::string(129, 't')}) {
        const auto original = wire::encode_request("object", "", 9, tag, true);
        const auto bytes = original.to_str();
        for (unsigned split = 0; split <= bytes.size(); ++split) {
            byte_buffer input;
            if (split) {
                input.append(::ceph::buffer::copy(bytes.data(), split));
            }
            if (split < bytes.size()) {
                input.append(::ceph::buffer::copy(bytes.data() + split, bytes.size() - split));
            }
            auto cursor = input.cbegin();
            native::request result;
            native::decode(result, cursor);
            CHECK(cursor.end());
            if (!tag.empty() && 1 == input.get_num_buffers()) {
                const auto& source = input.buffers().front();
                const auto lower = reinterpret_cast<std::uintptr_t>(source.c_str());
                const auto upper = lower + source.length();
                const auto retained = reinterpret_cast<std::uintptr_t>(result.tag().data());
                CHECK(lower <= retained);
                CHECK(retained + result.tag().size() <= upper);
            }
            input.clear();
            CHECK(result.tag() == tag);
            CHECK(result.olh.name == "object");
        }
        auto cursor = original.cbegin();
        const auto owned = wire::decode_request(cursor,
            [](auto&&, auto&&, std::uint64_t, std::string&& value, bool) {
                return std::move(value);
            });
        CHECK(owned == tag);
    }
}

TEST_CASE("Retained wire text replaces an existing value, including empty text", "[rgw][cls][datum][olh][request]")
{
    byte_buffer bytes;
    ::ceph::datum_wire::writer output {bytes};
    output.text("previous");
    output.text("replacement");
    output.text("");
    auto cursor = bytes.cbegin();
    ::ceph::datum_wire::reader input {cursor};
    ::ceph::buffer::ptr value;
    input.text(value);
    CHECK(std::string_view {value.c_str(), value.length()} == "previous");
    input.text(value);
    CHECK(std::string_view {value.c_str(), value.length()} == "replacement");
    input.text(value);
    CHECK(value.length() == 0);
    CHECK(cursor.end());
}

TEST_CASE("OLH event and nested map bindings preserve native encoding", "[rgw][cls][datum][olh]")
{
    const auto expected = fixture();
    const auto bytes = encode(expected);
    CHECK(bytes.contents_equal(rgw_olh_oracle::encode_reply(expected)));
    check(rgw_olh_oracle::decode_reply(bytes), expected);
    for (const bool extensions : {false, true}) {
        const auto input = fragmented(rgw_olh_oracle::encode_reply(expected, extensions));
        olh::reply result;
        native::decode_reply(input, result);
        check(result, expected);
        ::rgw_cls_read_olh_log_ret native_result;
        native::decode_reply(input, native_result);
        byte_buffer native_bytes;
        ::encode(native_result, native_bytes);
        CHECK(native_bytes.contents_equal(bytes));
        byte_buffer rebound;
        native::encode_reply({native_result.log.begin(), native_result.log.end(), native_result.log.size()}, true,
                             native_result.is_truncated, rebound);
        CHECK(rebound.contents_equal(bytes));
    }
}

TEST_CASE("OLH vector results retain native sorted first-wins map semantics", "[rgw][cls][datum][olh]")
{
    auto expected = fixture();
    expected.epochs[0].first = 42;
    expected.epochs[1].first = 7;
    expected.epochs.emplace_back(expected.epochs.front());
    expected.epochs.back().second.front().tag = "discard duplicate";
    const auto bytes = rgw_olh_oracle::encode_reply(expected);
    olh::reply result;
    native::decode_reply(bytes, result);
    check(result, rgw_olh_oracle::decode_reply(bytes));
    REQUIRE(result.epochs.size() == 3);
    CHECK(result.epochs[0].first == 7);
    CHECK(result.epochs[1].second.front().tag == "tag");
}

TEST_CASE("OLH framing rejects every proper truncation without publishing candidates", "[rgw][cls][datum][olh]")
{
    const auto full = encode(fixture());
    for (unsigned size = 0; size < full.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(full, 0, size);
        auto value = fixture();
        CHECK_THROWS_AS(native::decode_reply(prefix, value), ::ceph::buffer::error);
        check(value, fixture());
        ::rgw_cls_read_olh_log_ret compatible;
        compatible.log[123];
        CHECK_THROWS_AS(native::decode_reply(prefix, compatible), ::ceph::buffer::error);
        CHECK(compatible.log.contains(123));
        CHECK(compatible.log.size() == 1);
    }
    const auto request = wire::encode_request("name", "", 9, "tag", true);
    auto storage = wire::encode_request("held", "", 3, "preserve", true);
    auto preserved_cursor = storage.cbegin();
    native::request preserved;
    native::decode(preserved, preserved_cursor);
    storage.clear();
    for (unsigned size = 0; size < request.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(request, 0, size);
        auto cursor = prefix.cbegin();
        auto value = preserved;
        CHECK_THROWS_AS(native::decode(value, cursor), ::ceph::buffer::error);
        CHECK(value.tag() == "preserve");
        CHECK(cursor.get_off() == 0);
    }
}

TEST_CASE("OLH bounds reject impossible group and event counts before allocation", "[rgw][cls][datum][olh]")
{
    constexpr auto group_count_offset = ::ceph::encoding_detail::struct_header_len();
    // The first event count follows the group count and the first group's epoch.
    constexpr auto event_count_offset =
        group_count_offset + sizeof(std::uint32_t) + sizeof(std::uint64_t);
    for (const unsigned offset : {group_count_offset, event_count_offset}) {
        auto bytes = encode(fixture());
        const char impossible[] = {'\xFF', '\xFF', '\xFF', '\xFF'};
        auto cursor = bytes.begin(offset);
        cursor.copy_in(4, impossible);
        olh::reply value;
        CHECK_THROWS_AS(native::decode_reply(bytes, value), ::ceph::buffer::error);
    }
    auto bytes = encode(fixture());
    // Skip the event count and the first event frame's version and compatibility bytes.
    // Its length must not permit reads outside the enclosing reply.
    constexpr auto event_length_offset =
        event_count_offset + sizeof(std::uint32_t) + 2 * sizeof(std::uint8_t);
    auto cursor = bytes.begin(event_length_offset);
    const char impossible[] = {'\xFF', '\xFF', '\xFF', '\xFF'};
    cursor.copy_in(4, impossible);
    olh::reply value;
    CHECK_THROWS_AS(native::decode_reply(bytes, value), ::ceph::buffer::error);
}

TEST_CASE("OLH preserves all action bytes and accepts native nonzero flags", "[rgw][cls][datum][olh]")
{
    for (unsigned raw = 0; raw < 256; ++raw) {
        olh::reply expected {{{1, {{{"n", "i"}, "t", 2, static_cast<olh::action>(raw), true}}}}, true};
        auto bytes = encode(expected);
        CHECK(bytes.contents_equal(rgw_olh_oracle::encode_reply(expected)));
        const char flag = static_cast<char>(raw);
        bytes.begin(bytes.length() - 1).copy_in(1, &flag);
        expected.truncated = 0 != raw;
        // The event's final byte immediately precedes the reply's final byte.
        bytes.begin(bytes.length() - 2).copy_in(1, &flag);
        expected.epochs.front().second.front().delete_marker = 0 != raw;
        olh::reply result;
        native::decode_reply(bytes, result);
        check(result, expected);
        check(result, rgw_olh_oracle::decode_reply(bytes));
    }
}

TEST_CASE("OLH filtering drops stale rows and empty groups after selection", "[rgw][cls][datum][olh]")
{
    auto value = fixture();
    value.epochs.front().second.front().operation = olh::action::stale;
    const auto bytes = encode(value, false);
    value.epochs.resize(1);
    value.epochs.front().second.erase(value.epochs.front().second.begin());
    check(rgw_olh_oracle::decode_reply(bytes), value);
}

TEST_CASE("OLH randomized nested pages agree with the native codec", "[rgw][cls][datum][olh]")
{
    std::mt19937 random {0xC15DA7};
    for (unsigned sample = 0; sample < 200; ++sample) {
        olh::reply expected;
        expected.truncated = random() % 2;
        for (unsigned group = 0, count = random() % 15; group < count; ++group) {
            auto& rows = expected.epochs.emplace_back(random() % 50, std::vector<olh::event> {}).second;
            for (unsigned row = 0, count = random() % 5; row < count; ++row) {
                rows.push_back({{std::string(random() % 40, static_cast<char>(random())), "instance"},
                                std::string(random() % 40, static_cast<char>(random())), random(),
                                static_cast<olh::action>(random() % 256), 0 != random() % 2});
            }
        }
        const auto bytes = encode(expected);
        CHECK(bytes.contents_equal(rgw_olh_oracle::encode_reply(expected)));
        olh::reply result;
        native::decode_reply(bytes, result);
        check(result, rgw_olh_oracle::decode_reply(bytes));
    }
}

TEST_CASE("Stored OLH projection retains canonical bytes and their owners", "[rgw][cls][olh][stored]")
{
    ::rgw_bucket_olh_entry stored;
    stored.key = {"object", ""};
    stored.tag = std::string {"t\0g", 3};
    for (const std::uint64_t epoch : {7ULL, 42ULL, 0xFFFFFFFFFFFFFFFFULL}) {
        ::rgw_bucket_olh_log_entry event;
        event.epoch = epoch - 1;
        event.op = static_cast<::OLHLogOp>(255);
        event.op_tag = std::string(80, 't');
        event.key = {std::string {"n\0m", 3}, std::string(32, 'i')};
        event.delete_marker = true;
        stored.pending_log[epoch].push_back(std::move(event));
    }
    byte_buffer original;
    ::encode(stored, original);
    for (const bool fragments : {false, true}) {
        for (const std::uint64_t marker : {0ULL, 7ULL, 42ULL, 0xFFFFFFFFFFFFFFFFULL}) {
            byte_buffer input = fragments ? fragmented(original) : original;
            ::rgw_cls_read_olh_log_ret expected;
            expected.log.insert(stored.pending_log.upper_bound(marker), stored.pending_log.end());
            byte_buffer wanted;
            ::encode(expected, wanted);
            byte_buffer output;
            const auto status = native::try_encode_stored_reply(input, marker, stored.tag, output);
            if (fragments) {
                CHECK_FALSE(status.has_value());
                CHECK(output.length() == 0);
                continue;
            }
            REQUIRE(status.has_value());
            CHECK(*status == 0);
            input.clear();
            CHECK(output.contents_equal(wanted));
        }
    }

    auto large = stored;
    large.pending_log.begin()->second.front().op_tag.assign(8192, 't');
    byte_buffer source;
    ::encode(large, source);
    std::vector<std::pair<std::uintptr_t, std::uintptr_t>> owners;
    for (const auto& fragment : source.buffers()) {
        const auto start = reinterpret_cast<std::uintptr_t>(fragment.c_str());
        owners.emplace_back(start, start + fragment.length());
    }
    byte_buffer retained;
    REQUIRE(native::try_encode_stored_reply(source, 0, stored.tag, retained) == 0);
    CHECK(std::ranges::any_of(retained.buffers(), [&owners](const auto& fragment) {
        const auto start = reinterpret_cast<std::uintptr_t>(fragment.c_str());
        return std::ranges::any_of(owners, [start, size = fragment.length()](const auto& owner) {
            return owner.first <= start && start + size <= owner.second && 0 != size;
        });
    }));
    source.clear();
    ::rgw_cls_read_olh_log_ret expected;
    expected.log = std::move(large.pending_log);
    byte_buffer wanted;
    ::encode(expected, wanted);
    CHECK(retained.contents_equal(wanted));

    byte_buffer preserved;
    preserved.append("preserve", 8);
    auto output = preserved;
    const auto mismatch = native::try_encode_stored_reply(original, 0, "wrong", output);
    REQUIRE(mismatch.has_value());
    CHECK(*mismatch == -ECANCELED);
    CHECK(output.contents_equal(preserved));
    for (unsigned size = 0; size < original.length(); ++size) {
        byte_buffer prefix;
        prefix.substr_of(original, 0, size);
        CHECK_FALSE(native::try_encode_stored_reply(prefix, 0, stored.tag, output).has_value());
        CHECK(output.contents_equal(preserved));
    }
    auto compatible = original.to_str();
    compatible[0] = 2;
    byte_buffer future;
    future.append(compatible);
    CHECK_FALSE(native::try_encode_stored_reply(future, 0, stored.tag, output).has_value());
    compatible[0] = 1;
    compatible[1] = 0;
    future.clear();
    future.append(compatible);
    CHECK_FALSE(native::try_encode_stored_reply(future, 0, stored.tag, output).has_value());
    CHECK(output.contents_equal(preserved));
}

TEST_CASE("Stored OLH projection handles every physical field boundary", "[rgw][cls][olh][stored]")
{
    ::rgw_bucket_olh_entry stored;
    stored.key = {"object", ""};
    stored.tag.assign(32, 't');
    ::rgw_bucket_olh_log_entry event;
    event.epoch = 5;
    event.op = ::CLS_RGW_OLH_OP_LINK_OLH;
    event.op_tag.assign(67, 't');
    event.key = {std::string(129, 'n'), std::string(31, 'i')};
    event.delete_marker = true;
    stored.pending_log[7].push_back(std::move(event));
    stored.pending_log[9];
    byte_buffer original;
    ::encode(stored, original);
    const auto bytes = original.to_str();
    ::rgw_cls_read_olh_log_ret expected;
    expected.log = stored.pending_log;
    byte_buffer wanted;
    ::encode(expected, wanted);
    for (unsigned split = 0; split <= bytes.size(); ++split) {
        byte_buffer input;
        if (split) {
            input.append(::ceph::buffer::copy(bytes.data(), split));
        }
        if (split < bytes.size()) {
            input.append(::ceph::buffer::copy(bytes.data() + split, bytes.size() - split));
        }
        byte_buffer output;
        REQUIRE(native::try_encode_stored_reply(input, 0, stored.tag, output) == 0);
        CHECK(native::try_encode_stored_reply(input, 0, "wrong", output) == -ECANCELED);
        input.clear();
        CHECK(output.contents_equal(wanted));
    }
}

TEST_CASE("Stored OLH pagination agrees across native and sparse fragment chains", "[rgw][cls][olh][stored]")
{
    ::rgw_bucket_olh_entry stored;
    stored.key = {"object", ""};
    stored.tag.assign(32, 't');
    for (std::uint64_t epoch = 0; epoch < 1002; ++epoch) {
        auto& entries = stored.pending_log[epoch];
        if (epoch % 11) {
            ::rgw_bucket_olh_log_entry event;
            event.epoch = epoch;
            event.op = ::CLS_RGW_OLH_OP_LINK_OLH;
            event.op_tag.assign(3 + 64 * (epoch % 3), 't');
            event.key = {std::string(129, 'n'), std::string(31, 'i')};
            event.delete_marker = epoch % 2;
            entries.push_back(std::move(event));
        }
    }
    byte_buffer original;
    ::encode(stored, original);
    auto contiguous = original;
    contiguous.rebuild();
    REQUIRE(contiguous.get_num_buffers() == 1);
    const std::array inputs {original, contiguous, fragmented(original, 32)};
    for (const std::uint64_t marker : {0ULL, 17ULL, 1000ULL, 0xFFFFFFFFFFFFFFFFULL}) {
        ::rgw_cls_read_olh_log_ret expected;
        auto first = stored.pending_log.upper_bound(marker);
        for (unsigned count = 0; first != stored.pending_log.end() && count < 1000; ++count, ++first) {
            expected.log.insert(*first);
        }
        expected.is_truncated = first != stored.pending_log.end();
        byte_buffer wanted;
        ::encode(expected, wanted);
        for (const auto& input : inputs) {
            byte_buffer output;
            REQUIRE(native::try_encode_stored_reply(input, marker, stored.tag, output) == 0);
            CHECK(output.contents_equal(wanted));
        }
    }
}
