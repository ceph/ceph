// Exercise the production RADOS stats readers and reset loop with controlled operation results.
#include "datum_test.h"
#include "common/async/yield_context.h"
#include "include/rados/librados.hpp"
#include "rgw/driver/rados/buckets.h"
#include "rgw/driver/rados/rgw_tools.h"

#include <catch2/catch_test_macros.hpp>
#include <boost/asio/detached.hpp>

#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using namespace user_cls_test;

struct response final
{
    int transport = 0;
    int method = 0;
    bytes reply;
};

struct scenario;
scenario *active = nullptr;

struct scenario final
{
    int lookup_status = 0;
    unsigned lookups = 0;
    unsigned operations = 0;
    std::vector<response> responses;
    std::vector<cls_user_reset_stats2_op> requests;
    bytes *output = nullptr;
    int *method_status = nullptr;
    std::unique_ptr<librados::ObjectOperationCompletion> header_completion;
    ceph::real_time earliest;
    boost::asio::yield_context *yield = nullptr;

public:
    scenario() { active = this; }

    ~scenario() { active = nullptr; }
};

cls_user_stats totals(const std::uint64_t entries)
{
    cls_user_stats value;
    value.total_entries = entries;
    value.total_bytes = entries + 17;
    value.total_bytes_rounded = entries + 4096;

    return value;
}

bytes reply(const std::string& marker, const cls_user_stats& accumulated, const bool truncated)
{
    cls_user_reset_stats2_ret value;
    value.marker = marker;
    value.acc_stats = accumulated;
    value.truncated = truncated;

    return canonical(value);
}

int run(const optional_yield y = null_yield)
{
    librados::Rados rados;
    rgw_raw_obj object;
    object.oid = "stats";

    return rgwrados::buckets::reset_stats(nullptr, y, rados, object);
}

int read_stats(RGWStorageStats& stats, ceph::real_time *synced = nullptr,
               ceph::real_time *updated = nullptr, const optional_yield y = null_yield)
{
    librados::Rados rados;
    rgw_raw_obj object;
    object.oid = "stats";

    return rgwrados::buckets::read_stats(nullptr, y, rados, object, stats, synced, updated);
}

} // anonymous namespace

int rgw_get_rados_ref(const DoutPrefixProvider *, librados::Rados *, rgw_raw_obj object,
                      rgw_rados_ref *ref)
{
    REQUIRE(active);
    ++active->lookups;
    ref->obj = std::move(object);
    active->earliest = ceph::real_clock::now();

    return active->lookup_status;
}

void librados::ObjectOperation::exec_impl(const char *cls, const char *method, bytes& input,
                                         bytes *output, int *status)
{
    REQUIRE(active);
    CHECK(std::string_view {cls} == "user");
    CHECK(std::string_view {method} == "reset_user_stats2");
    REQUIRE_FALSE(active->output);
    REQUIRE(output);
    REQUIRE(status);
    cls_user_reset_stats2_op request;
    auto cursor = input.cbegin();
    ::decode(request, cursor);
    CHECK(cursor.end());
    CHECK(input.contents_equal(canonical(request)));
    CHECK(request.time >= active->earliest);
    CHECK(request.time <= ceph::real_clock::now());
    active->requests.push_back(std::move(request));
    active->output = output;
    active->method_status = status;
}

int rgw_rados_operate(const DoutPrefixProvider *, librados::IoCtx&, const std::string& oid,
                      librados::ObjectWriteOperation&&, optional_yield y, const int flags,
                      const jspan_context *, version_t *)
{
    REQUIRE(active);
    REQUIRE(active->output);
    REQUIRE(active->method_status);
    CHECK(oid == "stats");
    CHECK(flags == librados::OPERATION_RETURNVEC);
    CHECK(static_cast<bool>(y) == (active->yield != nullptr));
    if (active->yield) {
        CHECK(&y.get_yield_context() == active->yield);
    }
    REQUIRE(active->operations < active->responses.size());
    const auto& result = active->responses[active->operations++];
    *std::exchange(active->output, nullptr) = result.reply;
    *std::exchange(active->method_status, nullptr) = result.method;
    active->earliest = ceph::real_clock::now();

    return result.transport;
}

void librados::ObjectOperation::exec_impl(const char *cls, const char *method, bytes& input,
                                         ObjectOperationCompletion *completion)
{
    REQUIRE(active);
    CHECK(std::string_view {cls} == "user");
    CHECK(std::string_view {method} == "get_header");
    CHECK(input.contents_equal(canonical(cls_user_get_header_op {})));
    REQUIRE_FALSE(active->header_completion);
    REQUIRE(completion);
    active->header_completion.reset(completion);
}

int rgw_rados_operate(const DoutPrefixProvider *, librados::IoCtx&, const std::string& oid,
                      librados::ObjectReadOperation&&, bytes *, optional_yield y, const int flags,
                      const jspan_context *, version_t *)
{
    REQUIRE(active);
    REQUIRE(active->header_completion);
    CHECK(oid == "stats");
    CHECK(flags == 0);
    CHECK(static_cast<bool>(y) == (active->yield != nullptr));
    if (active->yield) {
        CHECK(&y.get_yield_context() == active->yield);
    }
    REQUIRE(active->operations < active->responses.size());
    const auto& result = active->responses[active->operations++];
    auto completion = std::move(active->header_completion);
    auto reply = result.reply;
    completion->handle_completion(result.method, reply);

    return result.transport;
}

TEST_CASE("RADOS user stats reader publishes a complete header", "[rgw][cls][user][stats-reader]")
{
    for (const int status : {0, 7}) {
        scenario state;
        cls_user_get_header_ret expected;
        expected.header.stats = totals(std::uint64_t {1} << 40);
        expected.header.last_stats_sync = ceph::real_clock::from_time_t(31);
        expected.header.last_stats_update = ceph::real_clock::from_time_t(37);
        state.responses = {{0, status, canonical(expected)}};
        RGWStorageStats stats;
        ceph::real_time synced, updated;
        CHECK(read_stats(stats, &synced, &updated) == 0);
        CHECK(stats.num_objects == expected.header.stats.total_entries);
        CHECK(stats.size == expected.header.stats.total_bytes);
        CHECK(stats.size_rounded == expected.header.stats.total_bytes_rounded);
        CHECK(synced == expected.header.last_stats_sync);
        CHECK(updated == expected.header.last_stats_update);
        CHECK(state.operations == 1);
    }
}

TEST_CASE("RADOS user stats reader rejects malformed replies before publication",
          "[rgw][cls][user][stats-reader]")
{
    cls_user_get_header_ret expected;
    expected.header.stats = totals(41);
    const auto full = canonical(expected);
    for (unsigned size = 0; size < full.length(); ++size) {
        scenario state;
        bytes prefix;
        prefix.substr_of(full, 0, size);
        state.responses = {{0, 0, std::move(prefix)}};
        RGWStorageStats stats;
        stats.num_objects = 43;
        stats.size = 47;
        stats.size_rounded = 53;
        auto synced = ceph::real_clock::from_time_t(59);
        auto updated = ceph::real_clock::from_time_t(61);
        CHECK(read_stats(stats, &synced, &updated) == -EIO);
        CHECK(stats.num_objects == 43);
        CHECK(stats.size == 47);
        CHECK(stats.size_rounded == 53);
        CHECK(synced == ceph::real_clock::from_time_t(59));
        CHECK(updated == ceph::real_clock::from_time_t(61));
        CHECK(state.operations == 1);
    }
}

TEST_CASE("RADOS user stats reader retains error precedence and missing-object behavior",
          "[rgw][cls][user][stats-reader]")
{
    scenario state;
    int expected = 0;
    SECTION("Reference lookup failure") {
        state.lookup_status = -ENOENT;
        expected = -ENOENT;
    }
    SECTION("Transport error precedes malformed reply") {
        state.responses = {{-EAGAIN, 0, {}}};
        expected = -EAGAIN;
    }
    SECTION("Method error precedes malformed reply") {
        state.responses = {{0, -EPERM, {}}};
        expected = -EPERM;
    }
    SECTION("Missing object from transport is empty stats") {
        state.responses = {{-ENOENT, -ENOENT, {}}};
    }
    SECTION("Missing object from method is empty stats") {
        state.responses = {{0, -ENOENT, {}}};
    }
    RGWStorageStats stats;
    stats.num_objects = 67;
    stats.size = 71;
    stats.size_rounded = 73;
    auto synced = ceph::real_clock::from_time_t(79);
    auto updated = ceph::real_clock::from_time_t(83);
    CHECK(read_stats(stats, &synced, &updated) == expected);
    CHECK(stats.num_objects == (expected < 0 ? 67 : 0));
    CHECK(stats.size == (expected < 0 ? 71 : 0));
    CHECK(stats.size_rounded == (expected < 0 ? 73 : 0));
    CHECK(synced == ceph::real_clock::from_time_t(expected < 0 ? 79 : 0));
    CHECK(updated == ceph::real_clock::from_time_t(expected < 0 ? 83 : 0));
    CHECK(state.operations == (state.lookup_status < 0 ? 0 : 1));
}

TEST_CASE("RADOS user stats reader accepts omitted timestamps and forwards yield",
          "[rgw][cls][user][stats-reader]")
{
    scenario state;
    cls_user_get_header_ret expected;
    expected.header.stats = totals(89);
    state.responses = {{0, 0, canonical(expected)}};
    RGWStorageStats stats;
    boost::asio::io_context context;
    boost::asio::spawn(context, [&](boost::asio::yield_context yield) {
        state.yield = &yield;
        CHECK(read_stats(stats, nullptr, nullptr, optional_yield {yield}) == 0);
    }, boost::asio::detached);
    context.run();
    CHECK(stats.num_objects == 89);
    CHECK(state.operations == 1);
}

TEST_CASE("RADOS user reset forwards page markers and totals without accumulating them twice",
          "[rgw][cls][user][reset-caller]")
{
    scenario state;
    const auto first = totals(std::uint64_t {1} << 40);
    const auto second = totals((std::uint64_t {1} << 40) + 99);
    state.responses = {{0, 7, reply("a", first, true)},
                       {0, 0, reply("z", second, true)},
                       {0, 5, reply("", second, false)}};
    CHECK(run() == 5);
    CHECK(state.lookups == 1);
    REQUIRE(state.requests.size() == 3);
    CHECK(state.requests[0].marker.empty());
    CHECK(canonical(state.requests[0].acc_stats).contents_equal(canonical(cls_user_stats {})));
    CHECK(state.requests[1].marker == "a");
    CHECK(canonical(state.requests[1].acc_stats).contents_equal(canonical(first)));
    CHECK(state.requests[2].marker == "z");
    CHECK(canonical(state.requests[2].acc_stats).contents_equal(canonical(second)));
}

TEST_CASE("RADOS user reset retains reference transport method and malformed-reply precedence",
          "[rgw][cls][user][reset-caller]")
{
    scenario state;
    SECTION("Reference failure precedes any request") {
        state.lookup_status = -ENOENT;
        CHECK(run() == -ENOENT);
        CHECK(state.requests.empty());
        CHECK(state.operations == 0);
    }
    SECTION("Transport failure precedes malformed output") {
        state.responses = {{-EAGAIN, -EIO, {}}};
        CHECK(run() == -EAGAIN);
        CHECK(state.operations == 1);
    }
    SECTION("A valid final reply retains its negative method status") {
        state.responses = {{0, -EIO, reply("", totals(17), false)}};
        CHECK(run() == -EIO);
        CHECK(state.operations == 1);
    }
    SECTION("An empty final page retains positive method status") {
        state.responses = {{0, 7, reply("", {}, false)}};
        CHECK(run() == 7);
        CHECK(state.operations == 1);
    }
    SECTION("Malformed output retains EINVAL even with negative method status") {
        state.responses = {{0, -EIO, {}}};
        CHECK(run() == -EINVAL);
        CHECK(state.operations == 1);
    }
}

TEST_CASE("RADOS user reset stops on a malformed later page without another continuation",
          "[rgw][cls][user][reset-caller]")
{
    scenario state;
    const auto accumulated = totals(19);
    bytes malformed;
    ENCODE_START(1, 1, malformed);
    ceph::encode(std::string {"bad-next"}, malformed);
    ::encode(totals(23), malformed); // Missing final truncation flag.
    ENCODE_FINISH(malformed);
    state.responses = {{0, 0, reply("next", accumulated, true)}, {0, 0, malformed}};
    CHECK(run() == -EINVAL);
    REQUIRE(state.requests.size() == 2);
    CHECK(state.requests[1].marker == "next");
    CHECK(canonical(state.requests[1].acc_stats).contents_equal(canonical(accumulated)));
    CHECK(state.operations == 2);
}

TEST_CASE("RADOS user reset forwards the caller yield context", "[rgw][cls][user][reset-caller]")
{
    scenario state;
    state.responses = {{0, 0, reply("", {}, false)}};
    boost::asio::io_context context;
    boost::asio::spawn(context, [&](boost::asio::yield_context yield) {
        state.yield = &yield;
        CHECK(run(optional_yield {yield}) == 0);
    }, boost::asio::detached);
    context.run();
    CHECK(state.operations == 1);
}
